//! Native Lua HTTP dispatch and optional socket/session bindings.
//!
//! Host preparation resolves configured owners and native session bindings.
//! Socket ownership and admission remain with the native transport registries;
//! their precedence does not follow the ordinary HTTP backend's position.

mod config;
#[cfg(any(feature = "websocket", feature = "webtransport"))]
mod origin;
#[cfg(feature = "websocket")]
mod websocket;
#[cfg(feature = "webtransport")]
mod webtransport;

use std::sync::Arc;

use crate::serve::{LuaApplicationAdapter, LuaApplications};
use claviron_http::ext::PathPrefix;
use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{RouteBody, ServiceResult};
use http::Request;

use claviron_edge::pipeline::{DispatchCall, DispatchStep};

pub use config::{LuaRouteConfig, LuaSocketOriginPolicy};

/// A scope's `lua:` directive as declared: one mapping or a list of them.
/// The edge keeps it as written; this is where it is decoded. An absent or
/// null directive declares nothing.
fn declared(
    scope: &claviron_edge::Site
) -> Result<Vec<LuaRouteConfig>, serde_yaml::Error> {
    match scope.directive("lua") {
        None | Some(serde_yaml::Value::Null) => Ok(Vec::new()),
        Some(serde_yaml::Value::Sequence(items)) => {
            items.iter().cloned().map(serde_yaml::from_value).collect()
        }
        Some(one) => Ok(vec![serde_yaml::from_value(one.clone())?]),
    }
}

struct Mount {
    path: Option<PathPrefix>,
    adapter: LuaApplicationAdapter,
}

/// One prepared Lua backend. An absent declaration installs no backend step.
/// The most specific mount claims the request, including unavailable/error
/// responses. A non-match yields the same unread request to the next owner.
#[derive(Clone)]
pub struct LuaBackend {
    mounts: Arc<[Mount]>,
}

impl LuaBackend {
    pub fn new(
        service: Option<Arc<LuaApplications>>,
        routes: &[LuaRouteConfig],
        prefix: Option<&PathPrefix>,
        policy: HttpPolicyOverride,
    ) -> Option<Self> {
        if routes.is_empty() {
            return None;
        }
        let mut mounts: Vec<_> = routes
            .iter()
            .map(|route| Mount {
                path: prefix.cloned(),
                adapter: LuaApplicationAdapter::new(
                    service.clone(),
                    route.application.as_str(),
                    policy,
                ),
            })
            .collect();
        // Preserve the former stable mount precedence: longer first, explicit
        // root before a catch-all, and later declarations first on equal keys.
        mounts.sort_by(|a, b| match (&a.path, &b.path) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
        });
        mounts.reverse();
        Some(Self { mounts: mounts.into() })
    }

    fn select(
        &self,
        path: &str,
    ) -> Option<&Mount> {
        self.mounts.iter().find(|mount| {
            mount
                .path
                .as_ref()
                .is_none_or(|prefix| prefix.has_path_segment(path))
        })
    }
}

impl claviron_edge::pipeline::BackendHandler for LuaBackend {
    fn contract(&self) -> claviron_edge::pipeline::BackendContract {
        claviron_edge::pipeline::BackendContract {
            identity: claviron_edge::pipeline::BackendIdentity::None,
            reply: claviron_edge::pipeline::BackendReply::Owner,
        }
    }
}

impl DispatchStep for LuaBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<claviron_edge::pipeline::host::HostContext<'_>>,
    ) -> DispatchCall {
        let Some(mount) = self.select(request.uri().path()) else {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        };
        let adapter = mount.adapter.clone();
        // Copied while the context is lent: the call outlives it.
        let edge = context.and_then(|context| context.policy()).copied();
        DispatchCall::new(async move {
            ServiceResult::Handled(adapter.handle(request, edge).await)
        })
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
fn mounted_websocket_path(
    mount: Option<&claviron_http::ext::PathPrefix>,
    path: &claviron_http::ext::PathPrefix,
) -> Arc<str> {
    match mount.map(|mount| mount.as_str()) {
        None | Some("/") => Arc::from(path.as_str()),
        Some(mount) if path.as_str() == "/" => Arc::from(mount),
        Some(mount) => Arc::from(format!("{mount}{}", path.as_str())),
    }
}

/// One socket endpoint as it was prepared. Its handler keeps this, so that
/// admitting a socket interprets no configuration.
#[cfg(any(feature = "websocket", feature = "webtransport"))]
#[derive(Clone)]
struct Endpoint {
    /// Where the endpoint is mounted on the host.
    path: Arc<str>,
    application: Arc<str>,
    origin: LuaSocketOriginPolicy,
    /// The directive the endpoint was decoded from, as its scope declared it.
    source: Arc<serde_yaml::Value>,
    /// The mount of the scope that declared it.
    mount: Option<PathPrefix>,
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
impl Endpoint {
    /// Whether `host` still declares, for this endpoint's path, exactly what
    /// the endpoint was prepared from.
    ///
    /// A registry that outlives a publication keeps a binding after its route
    /// was removed, masked by a more specific route or given to another
    /// application. Such an entry must not admit. A route that was only
    /// edited has been prepared again by then, and its new binding replaced
    /// this one.
    fn still_declared(
        &self,
        host: &claviron_edge::Site,
    ) -> bool {
        let selected =
            host.routes.find(&self.path).map(|route| &**route).unwrap_or(host);
        selected.directive("lua") == Some(&*self.source)
            && selected.files.mapping.as_ref().map(|mapping| mapping.prefix())
                == self.mount.as_ref()
    }
}

/// Every socket endpoint the host's scopes declare, decoded once.
#[cfg(any(feature = "websocket", feature = "webtransport"))]
fn socket_endpoints(
    host: &claviron_edge::Site
) -> Result<Vec<Endpoint>, serde_yaml::Error> {
    let mut endpoints = Vec::new();
    for (prefix, scope) in host.scopes() {
        let Some(source) = scope.directive("lua") else {
            continue;
        };
        let source = Arc::new(source.clone());
        let mount = scope
            .files
            .mapping
            .as_ref()
            .map(|mapping| mapping.prefix().clone());
        for route in declared(scope)? {
            for socket in route.websockets() {
                endpoints.push(Endpoint {
                    path: mounted_websocket_path(prefix, &socket.path),
                    application: Arc::from(route.application.as_str()),
                    origin: route.origin,
                    source: source.clone(),
                    mount: mount.clone(),
                });
            }
        }
    }
    Ok(endpoints)
}

/// The decode failure of a host's `lua:` directive, as a preparation error.
#[cfg(any(feature = "websocket", feature = "webtransport"))]
fn invalid(
    host: &claviron_types::Host,
    error: serde_yaml::Error,
) -> claviron_runtime::registry::Error {
    claviron_runtime::registry::Error::msg(format!(
        "lua directive of host '{host}': {error}"
    ))
}

#[cfg(all(test, feature = "websocket"))]
#[path = "../tests/edge/socket_path.rs"]
mod socket_path_tests;

#[cfg(test)]
#[path = "../tests/edge.rs"]
mod tests;

/// Register configured HTTP backends and optional native session contributions.
/// Native Lua application and Runner use do not select this Edge capability.
pub fn register(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    let registry = state
        .registry_ref()
        .resolve::<claviron_edge::HostAppRegistry>()
        .ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "Lua requires the Edge host registry",
            )
        })?;
    registry.register_backend_factory(Arc::new(Factory))?;
    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    claviron_edge::register_routable(
        state,
        Arc::new(Sessions {
            #[cfg(feature = "websocket")]
            state: state.clone(),
        }),
    )?;
    Ok(())
}

struct Factory;
impl claviron_edge::pipeline::backend::BackendFactory for Factory {
    fn name(&self) -> &'static str {
        "lua"
    }
    fn placement(&self) -> claviron_edge::pipeline::backend::BackendPlacement {
        claviron_edge::pipeline::backend::BackendPlacement::AfterFiles
    }
    fn prepare(
        &self,
        context: &claviron_edge::pipeline::backend::BackendContext<'_>,
    ) -> claviron_runtime::registry::Result<
        Vec<Arc<dyn claviron_edge::pipeline::BackendHandler>>,
    > {
        let policy = claviron_edge::extension::RouteContext {
            state: context.state,
            host: context.host,
            config: context.site,
            route: context.route,
            prefix: context.prefix,
        }
        .policy();
        let routes = declared(context.route).map_err(|error| {
            claviron_runtime::registry::Error::msg(format!(
                "backend 'lua' at '{}': {error}",
                context.prefix
            ))
        })?;
        Ok(LuaBackend::new(
            context.state.registry_ref().resolve::<LuaApplications>(),
            &routes,
            Some(context.prefix),
            policy,
        )
        .into_iter()
        .map(|owner| {
            Arc::new(owner) as Arc<dyn claviron_edge::pipeline::BackendHandler>
        })
        .collect())
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
struct Sessions {
    #[cfg(feature = "websocket")]
    state: claviron_runtime::AppState,
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
impl claviron_runtime::registry::Component<claviron_runtime::AppState>
    for Sessions
{
    type Deps = ();
    fn name(&self) -> &str {
        "LuaEdgeSessions"
    }
}

#[cfg(any(feature = "websocket", feature = "webtransport"))]
impl claviron_edge::Routable for Sessions {
    fn match_host(
        &self,
        _: &claviron_types::Host,
        host: &claviron_edge::HostConfig,
    ) -> bool {
        // A declaration that does not decode selects nothing here; the
        // backend's own preparation of the same scope reports it.
        socket_endpoints(host).is_ok_and(|endpoints| !endpoints.is_empty())
    }
    fn prepare_sessions(
        &self,
        state: &claviron_runtime::AppState,
        host: &claviron_types::Host,
        config: &claviron_edge::HostConfig,
    ) -> claviron_runtime::registry::Result<()> {
        #[cfg(feature = "webtransport")]
        webtransport::append_routes(state, host, config)?;
        #[cfg(not(feature = "webtransport"))]
        let _ = (state, host, config);
        Ok(())
    }
    #[cfg(feature = "websocket")]
    fn h1_upgrades(
        &self,
        host: &claviron_types::Host,
        config: Arc<claviron_edge::HostConfig>,
    ) -> claviron_runtime::registry::Result<
        Vec<claviron_edge::pipeline::H1UpgradeRoute>,
    > {
        let mut routes = Vec::new();
        websocket::append_routes(&self.state, host, &config, &mut routes)?;
        Ok(routes)
    }
}
