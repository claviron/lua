//! `lua.yaml` in the configuration directory: where `require` searches for
//! Lua source beyond Claviron's own modules.
//!
//! ```yaml
//! path: [lua_modules/share/lua/5.5, lib/lua]
//! ```
//!
//! Relative directories are the process's working directory's, as other
//! Claviron paths are. Without the file there is no search path: a script
//! reaches only the extensions. Congif prepares the file before boot and in
//! the reload batch; this provider applies what was prepared.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{self, Bootable, Component, Reloadable};
use claviron_runtime::{AppState, Register};
use serde::Deserialize;

/// What the configuration directory's file is called.
pub const CONFIG_FILE: &str = "lua.yaml";

/// `lua.yaml` as Congif prepares it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct LuaFile {
    path: Vec<PathBuf>,
    /// Read by [`applications`], once per worker process.
    #[cfg(feature = "app")]
    applications:
        std::collections::HashMap<String, crate::app::ApplicationConfig>,
    #[cfg(not(feature = "app"))]
    applications: serde_yaml::Value,
    #[cfg(feature = "fetch")]
    fetch: crate::fetch::FetchConfig,
}

/// The configured search path, as the last boot or reload applied it.
pub struct LuaConfig {
    config_path: PathBuf,
    path: RwLock<Vec<PathBuf>>,
    #[cfg(feature = "app")]
    applications: RwLock<
        std::collections::HashMap<String, crate::app::ApplicationConfig>,
    >,
    #[cfg(feature = "fetch")]
    fetcher: RwLock<Option<Arc<crate::fetch::Fetcher>>>,
}

impl LuaConfig {
    fn new(state: &AppState) -> Self {
        Self {
            config_path: state.config_dir().join(CONFIG_FILE),
            path: RwLock::new(Vec::new()),
            #[cfg(feature = "app")]
            applications: RwLock::new(std::collections::HashMap::new()),
            #[cfg(feature = "fetch")]
            fetcher: RwLock::new(None),
        }
    }

    pub fn path(&self) -> Vec<PathBuf> {
        self.path.read().unwrap().clone()
    }

    /// The fetch client `lua.yaml` opened, if it opened one.
    #[cfg(feature = "fetch")]
    pub fn fetcher(&self) -> Option<Arc<crate::fetch::Fetcher>> {
        self.fetcher.read().unwrap().clone()
    }

    fn apply(
        &self,
        read: &LuaFile,
    ) -> registry::Result<()> {
        #[cfg(feature = "fetch")]
        {
            let fetcher =
                crate::fetch::Fetcher::new(&read.fetch).map_err(|error| {
                    registry::Error::msg(format!(
                        "{}: {error}",
                        self.config_path.display()
                    ))
                })?;
            *self.fetcher.write().unwrap() = fetcher;
        }
        *self.path.write().unwrap() = read.path.clone();
        #[cfg(feature = "app")]
        {
            *self.applications.write().unwrap() = read.applications.clone();
        }
        Ok(())
    }
}

#[async_trait]
impl Configurable for LuaConfig {
    type Config = LuaFile;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// Without the file there is no search path and no application.
    fn absent_source(&self) -> Option<LuaFile> {
        Some(LuaFile::default())
    }

    async fn configure(
        &self,
        config: &LuaFile,
    ) -> congif::Result<()> {
        self.apply(config).map_err(congif::Error::msg)
    }
}

impl LuaConfig {
    /// The applications `lua.yaml` declares, as the last boot or reload
    /// applied them.
    #[cfg(feature = "app")]
    pub fn applications(
        &self
    ) -> std::collections::HashMap<String, crate::app::ApplicationConfig> {
        self.applications.read().unwrap().clone()
    }
}

impl Component<AppState> for LuaConfig {
    type Deps = ();

    fn name(&self) -> &'static str {
        "LuaConfig"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for LuaConfig {
    async fn boot(
        &self,
        _deps: (),
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.apply(&config)
    }
}

#[async_trait]
impl Reloadable<AppState> for LuaConfig {
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.apply(&config)
    }
}

impl Register<AppState> for LuaConfig {
    fn register(state: &AppState) -> registry::Result<()> {
        if state.registry_ref().resolve::<LuaConfig>().is_some() {
            return Ok(());
        }
        state.bind_config_environment::<Self>()?;
        state.registry_ref().register(Arc::new(LuaConfig::new(state)))?;
        Ok(())
    }
}

/// Register the configuration, once.
pub fn register(state: &AppState) -> registry::Result<()> {
    <LuaConfig as Register<AppState>>::register(state)
}
