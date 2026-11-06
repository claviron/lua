//! The applications `lua.yaml` declares, loaded in every worker process and
//! mounted on hosts by the edge; managed as the `lua` runtime through the
//! application manager (start, stop, restart — by name, over gRPC as well).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use async_trait::async_trait;
use claviron_app::environment::{
    ApplicationEnvironments, EnvironmentContext, ExecutionEnvironment,
};
use claviron_app::management::{
    ApplicationFailed, ApplicationStarted, ApplicationStopped,
};
use claviron_app::{
    ApplicationControlResult, ApplicationController, ApplicationId,
    ApplicationLifecycle, ApplicationManager, ApplicationName,
    ApplicationPoolStatus, ApplicationRuntime, ApplicationSnapshot,
};
#[cfg(feature = "orbit")]
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{
    self, Bootable, Component, Reloadable, Resolve, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext, ServiceStartPolicy};

use crate::app::{ApplicationConfig, LuaApplication};

mod adapter;
pub use adapter::LuaApplicationAdapter;

/// The host provides Olua's module contract, not an arbitrary Lua framework.
fn discover_frameworks() -> Vec<String> {
    vec!["Olua".to_owned()]
}

/// One declared application and where it stands.
struct Managed {
    config: ApplicationConfig,
    /// Stopped by the operator: not loaded again until started.
    enabled: bool,
    /// Counts the states this application has had; each load is one.
    generation: u64,
    last_error: Option<String>,
    loaded: Option<Arc<LuaApplication>>,
    environment: Option<Arc<ExecutionEnvironment>>,
}

/// The applications of this worker process: declared at registration and on
/// every reload, loaded when the service runs, stopped when it ends.
///
/// A reload or a restart loads the module into a new state and puts it in
/// place of the old one; requests that arrive after are served by the new
/// state. The old one is not interrupted — what it is serving finishes there,
/// and it closes with its last request. An application whose new module does
/// not load keeps its old state, and the error is logged.
pub struct LuaApplications {
    applications: RwLock<HashMap<String, Managed>>,
    /// States a reload, a restart or a stop replaced and that still serve a
    /// request; they are told to stop with the rest when the worker stops.
    retired: Mutex<Vec<Weak<LuaApplication>>>,
    /// Loading happens once the service runs; a reload before that only reads
    /// the file.
    running: AtomicBool,
    environment_changed: tokio::sync::Notify,
    loading: tokio::sync::Mutex<()>,
}

impl LuaApplications {
    pub fn is_declared(
        &self,
        name: &str,
    ) -> bool {
        self.applications.read().unwrap().contains_key(name)
    }

    /// The application's current state, when it has one.
    pub fn get(
        &self,
        name: &str,
    ) -> Option<Arc<LuaApplication>> {
        self.applications
            .read()
            .unwrap()
            .get(name)
            .and_then(|managed| managed.loaded.clone())
    }

    /// Load `name`'s module into a new state and put it in place. What it
    /// replaces is retired; when it does not load, what serves now serves on.
    async fn load(
        &self,
        state: &AppState,
        name: &str,
    ) -> Result<(), String> {
        self.load_prepared(state, name, None).await
    }

    /// Reuse an already prepared notification: an env-only change must not
    /// reread its files or install a second subscription for the same owner.
    async fn load_prepared(
        &self,
        state: &AppState,
        name: &str,
        prepared: Option<Arc<ExecutionEnvironment>>,
    ) -> Result<(), String> {
        let _loading = self.loading.lock().await;
        let Some(config) = self
            .applications
            .read()
            .unwrap()
            .get(name)
            .map(|managed| managed.config.clone())
        else {
            return Err(format!(
                "Lua application '{name}' is not declared in lua.yaml"
            ));
        };
        let environment = match prepared {
            Some(environment) => environment,
            None => Arc::new(
                EnvironmentContext::new(state)
                    .and_then(|context| {
                        context.observe(&config.env_file, config.env.clone())
                    })
                    .map_err(|error| error.to_string())?,
            ),
        };
        let values =
            environment.snapshot().map_err(|error| error.to_string())?;
        let engine = crate::host::engine(state, name)
            .with_environment(values.as_ref().clone());
        if let Some(managed) = self.applications.write().unwrap().get_mut(name)
        {
            managed.environment = Some(environment);
        }
        self.environment_changed.notify_one();
        let loaded = LuaApplication::load(&engine, name, &config).await;
        let mut applications = self.applications.write().unwrap();
        let Some(managed) = applications.get_mut(name) else {
            return Err(format!(
                "Lua application '{name}' was removed while it loaded"
            ));
        };
        // A stop received while a module loads must not publish a fresh state.
        if !managed.enabled {
            return Ok(());
        }
        match loaded {
            Ok(application) => {
                managed.generation += 1;
                managed.last_error = None;
                let replaced = managed.loaded.replace(Arc::new(application));
                let generation = managed.generation;
                drop(applications);
                if state.is_worker() {
                    tracing::debug!(application = %name, module = %config.module.display(), generation, "🌙 Lua application loaded");
                } else {
                    tracing::info!(application = %name, module = %config.module.display(), generation, "🌙 Lua application loaded");
                }
                if let Some(replaced) = replaced {
                    self.retire(replaced);
                    emit(
                        state,
                        ApplicationStopped {
                            id: id(name),
                            generation: generation - 1,
                        },
                    );
                }
                emit(state, ApplicationStarted { id: id(name), generation });
                Ok(())
            }
            Err(error) => {
                let error = error.to_string();
                managed.last_error = Some(error.clone());
                let kept = managed.loaded.is_some();
                let generation = managed.generation;
                drop(applications);
                tracing::error!(
                    application = %name,
                    module = %config.module.display(),
                    "Lua application did not load; {}: {error}",
                    if kept { "the previous state serves on" } else { "its routes answer 503" }
                );
                if !kept {
                    emit(
                        state,
                        ApplicationFailed {
                            id: id(name),
                            generation,
                            error: error.clone(),
                        },
                    );
                }
                Err(error)
            }
        }
    }

    /// Load every enabled application.
    async fn load_all(
        &self,
        state: &AppState,
    ) {
        let names: Vec<String> = self
            .applications
            .read()
            .unwrap()
            .iter()
            .filter(|(_, managed)| managed.enabled)
            .map(|(name, _)| name.clone())
            .collect();
        for name in names {
            let _ = self.load(state, &name).await;
        }
    }

    /// Take `lua.yaml`'s applications: new ones are declared, removed ones
    /// retired, and every one keeps whether the operator stopped it.
    fn declare(
        &self,
        state: &AppState,
        declared: HashMap<String, ApplicationConfig>,
    ) {
        let removed: Vec<(String, Managed)> = {
            let mut applications = self.applications.write().unwrap();
            let gone: Vec<String> = applications
                .keys()
                .filter(|name| !declared.contains_key(*name))
                .cloned()
                .collect();
            let removed = gone
                .into_iter()
                .filter_map(|name| applications.remove_entry(&name))
                .collect();
            for (name, config) in declared {
                match applications.get_mut(&name) {
                    Some(managed) => managed.config = config,
                    None => {
                        applications.insert(
                            name,
                            Managed {
                                config,
                                enabled: true,
                                generation: 0,
                                last_error: None,
                                loaded: None,
                                environment: None,
                            },
                        );
                    }
                }
            }
            removed
        };
        self.environment_changed.notify_one();
        for (name, managed) in removed {
            tracing::info!(application = %name, "🌙 Lua application removed");
            if let Some(application) = managed.loaded {
                self.retire(application);
                emit(
                    state,
                    ApplicationStopped {
                        id: id(&name),
                        generation: managed.generation,
                    },
                );
            }
        }
    }

    /// Mark `name` started; `true` when it has no state yet and needs one.
    fn start(
        &self,
        name: &str,
    ) -> Result<bool, String> {
        let mut applications = self.applications.write().unwrap();
        let managed = applications.get_mut(name).ok_or_else(|| {
            format!("Lua application '{name}' is not declared in lua.yaml")
        })?;
        managed.enabled = true;
        Ok(managed.loaded.is_none())
    }

    /// Stop taking requests for `name`; what its state serves finishes there.
    fn stop(
        &self,
        state: &AppState,
        name: &str,
    ) -> Result<(), String> {
        let (retired, generation) = {
            let mut applications = self.applications.write().unwrap();
            let managed = applications.get_mut(name).ok_or_else(|| {
                format!("Lua application '{name}' is not declared in lua.yaml")
            })?;
            managed.enabled = false;
            (managed.loaded.take(), managed.generation)
        };
        if let Some(application) = retired {
            tracing::info!(application = %name, generation, "🌙 Lua application stopped");
            self.retire(application);
            emit(state, ApplicationStopped { id: id(name), generation });
        }
        Ok(())
    }

    /// Keep a weak hold on a replaced state, so the worker's stop reaches it
    /// while it still serves; it closes when its last request lets go.
    fn retire(
        &self,
        application: Arc<LuaApplication>,
    ) {
        let mut retired = self.retired.lock().unwrap();
        retired.retain(|held| held.strong_count() > 0);
        retired.push(Arc::downgrade(&application));
    }

    fn retiring(&self) -> u32 {
        self.retired
            .lock()
            .unwrap()
            .iter()
            .filter(|held| held.strong_count() > 0)
            .count() as u32
    }

    fn snapshot(
        &self,
        name: &str,
        managed: &Managed,
    ) -> ApplicationSnapshot {
        let lifecycle =
            match (&managed.loaded, managed.enabled, &managed.last_error) {
                (Some(_), true, _) => ApplicationLifecycle::Running,
                (None, true, Some(_)) => ApplicationLifecycle::Failed,
                (None, true, None) => ApplicationLifecycle::Starting,
                (_, false, _) => ApplicationLifecycle::Stopped,
            };
        ApplicationSnapshot {
            id: id(name),
            description: claviron_app::ApplicationDescription {
                frameworks: managed
                    .config
                    .frameworks
                    .clone()
                    .unwrap_or_else(discover_frameworks),
                icon: managed.config.icon.clone(),
                ..claviron_app::ApplicationDescription::new("lua", "fetch")
                    .runner("claviron")
            },
            lifecycle,
            enabled: managed.enabled,
            generation: managed.generation,
            // One state per worker process; its permits are the pool.
            pool: managed.loaded.as_ref().map(|application| {
                let (concurrency, busy) = application.permits();
                ApplicationPoolStatus {
                    accepting: Some(managed.enabled),
                    instances: 1,
                    ready: Some((concurrency - busy) as u32),
                    busy: Some(busy as u32),
                    retiring: Some(self.retiring()),
                    backing_off: None,
                    exhausted: None,
                }
            }),
            last_error: managed.last_error.clone(),
        }
    }
}

fn runtime() -> ApplicationRuntime {
    ApplicationRuntime::new("lua").expect("static Lua runtime name")
}

/// The application's id under the `lua` runtime. Names are checked against
/// the application manager's rules when `lua.yaml` is read ([`declared`]).
fn id(name: &str) -> ApplicationId {
    ApplicationId::new(
        runtime(),
        ApplicationName::new(name)
            .expect("names are checked when lua.yaml is read"),
    )
}

fn emit<E: claviron_runtime::Event>(
    state: &AppState,
    event: E,
) {
    state.events().emit(event);
}

impl Component<AppState> for LuaApplications {
    /// `lua.yaml`'s applications and search path, which the new states use.
    type Deps = (Arc<crate::config::LuaConfig>,);

    fn name(&self) -> &'static str {
        "LuaApplications"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    #[cfg(feature = "orbit")]
    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<crate::config::LuaConfig>,)> for LuaApplications {
    /// Declares what `lua.yaml` named once Congif has applied it to
    /// `LuaConfig`; nothing is loaded until `run`.
    async fn boot(
        &self,
        (config,): (Arc<crate::config::LuaConfig>,),
        state: &AppState,
    ) -> registry::Result<()> {
        self.declare(state, declared(&config)?);
        Ok(())
    }
}

/// Applications serve requests, so they load in workers (or a standalone
/// process); the master declares them and loads none.
#[cfg(feature = "orbit")]
impl Scoped for LuaApplications {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<crate::config::LuaConfig>,)> for LuaApplications {
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        ServiceStartPolicy::Automatic
    }

    async fn run(
        self: Arc<Self>,
        _deps: (Arc<crate::config::LuaConfig>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        // One application that does not load is that application's problem:
        // it is logged and its routes answer `503`, as a PHP application's
        // do, while the worker and every other application serve.
        self.running.store(true, Ordering::Release);
        self.load_all(&state).await;

        loop {
            let mut environments = ApplicationEnvironments::default();
            for (name, managed) in self.applications.read().unwrap().iter() {
                if let Some(environment) = &managed.environment {
                    environments
                        .insert_shared(name.clone(), Arc::clone(environment));
                }
            }
            tokio::select! {
                () = context.cancelled() => break,
                () = self.environment_changed.notified() => {},
                (name, result) = environments.changed() => match result {
                    Ok(_) => {
                        let environment = self.applications.read().unwrap().get(&name)
                            .filter(|managed| managed.enabled).and_then(|managed| managed.environment.clone())
                            .filter(|current| environments.get(&name).is_some_and(|notified| Arc::ptr_eq(current, notified)));
                        if let Some(environment) = environment && let Err(error) = self.load_prepared(&state, &name, Some(environment)).await {
                            tracing::error!(application = %name, %error, "Lua environment renewal failed; previous state retained");
                        }
                    }
                    Err(error) => tracing::error!(application = %name, %error, "Lua environment replacement rejected; previous state retained"),
                }
            }
        }

        self.running.store(false, Ordering::Release);
        let loaded: Vec<Arc<LuaApplication>> = self
            .applications
            .write()
            .unwrap()
            .values_mut()
            .filter_map(|managed| {
                managed.environment = None;
                managed.loaded.take()
            })
            .collect();
        for application in loaded {
            application.stop();
        }
        for retired in std::mem::take(&mut *self.retired.lock().unwrap()) {
            if let Some(application) = retired.upgrade() {
                application.stop();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for LuaApplications {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let (config,) = <(Arc<crate::config::LuaConfig>,) as Resolve<
            AppState,
        >>::resolve(state.registry_ref())?;
        self.declare(state, declared(&config)?);
        if self.running.load(Ordering::Acquire) {
            self.load_all(state).await;
        }
        Ok(())
    }
}

/// `lua.yaml`'s applications, each name one the application manager accepts.
fn declared(
    config: &crate::config::LuaConfig
) -> registry::Result<HashMap<String, ApplicationConfig>> {
    let declared = config.applications();
    for name in declared.keys() {
        ApplicationName::new(name.as_str()).map_err(|error| {
            registry::Error::msg(format!(
                "lua.yaml: application '{name}': {error}"
            ))
        })?;
    }
    Ok(declared)
}

/// Register the applications `lua.yaml` declares and hand them to the
/// application manager as the `lua` runtime, in every process. `Scoped`
/// keeps the loading in workers: the master serves no requests and loads
/// none.
pub fn register(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<LuaApplications>().is_some() {
        return Ok(());
    }
    let service = Arc::new(LuaApplications {
        applications: RwLock::new(HashMap::new()),
        retired: Mutex::new(Vec::new()),
        running: AtomicBool::new(false),
        environment_changed: tokio::sync::Notify::new(),
        loading: tokio::sync::Mutex::new(()),
    });
    if let Some(manager) = state.registry_ref().resolve::<ApplicationManager>()
    {
        manager
            .add(Arc::new(LuaApplicationController {
                state: state.clone(),
                service: Arc::clone(&service),
            }))
            .map_err(|error| registry::Error::msg(error.to_string()))?;
    }
    state.registry_ref().register(service).map(|_| ())
}

/// The `lua` runtime's side of the application manager.
struct LuaApplicationController {
    state: AppState,
    service: Arc<LuaApplications>,
}

fn refused(error: String) -> Box<dyn std::error::Error + Send + Sync> {
    error.into()
}

#[async_trait]
impl ApplicationController for LuaApplicationController {
    fn runtime(&self) -> &ApplicationRuntime {
        static RUNTIME: std::sync::OnceLock<ApplicationRuntime> =
            std::sync::OnceLock::new();
        RUNTIME.get_or_init(runtime)
    }

    fn list(&self) -> Vec<ApplicationSnapshot> {
        let applications = self.service.applications.read().unwrap();
        applications
            .iter()
            .map(|(name, managed)| self.service.snapshot(name, managed))
            .collect()
    }

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot> {
        let applications = self.service.applications.read().unwrap();
        applications
            .get(name.as_str())
            .map(|managed| self.service.snapshot(name.as_str(), managed))
    }

    async fn start(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        if self.service.start(name.as_str()).map_err(refused)? {
            self.service
                .load(&self.state, name.as_str())
                .await
                .map_err(refused)?;
        }
        Ok(())
    }

    async fn stop(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.service.stop(&self.state, name.as_str()).map_err(refused)
    }

    /// A new state in place of the old, with no gap between them.
    async fn restart(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.service.start(name.as_str()).map_err(refused)?;
        self.service.load(&self.state, name.as_str()).await.map_err(refused)
    }
}
