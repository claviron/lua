//! Edge integration for native Lua WebTransport sessions.

#[cfg(feature = "webtransport")]
mod webtransport;

#[cfg(feature = "webtransport")]
use std::sync::Arc;

#[cfg(feature = "webtransport")]
use claviron_http::ext::PathPrefix;
#[cfg(feature = "webtransport")]
use crate::backend::{declared, mounted_websocket_path};

pub use crate::backend::{LuaBackend, LuaRouteConfig, LuaSocketOriginPolicy};

struct Registered;

/// One socket endpoint as it was prepared. Its handler keeps this, so that
/// admitting a socket interprets no configuration.
#[cfg(feature = "webtransport")]
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

#[cfg(feature = "webtransport")]
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
        host: &claviron_pipeline::Site,
    ) -> bool {
        let selected =
            host.routes.find(&self.path).map(|route| &**route).unwrap_or(host);
        selected.directive("lua") == Some(&*self.source)
            && selected.files.mapping.as_ref().map(|mapping| mapping.prefix())
                == self.mount.as_ref()
    }
}

/// Every socket endpoint the host's scopes declare, decoded once.
#[cfg(feature = "webtransport")]
fn socket_endpoints(
    host: &claviron_pipeline::Site
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
#[cfg(feature = "webtransport")]
fn invalid(
    host: &claviron_types::Host,
    error: serde_yaml::Error,
) -> claviron_runtime::registry::Error {
    claviron_runtime::registry::Error::msg(format!(
        "lua directive of host '{host}': {error}"
    ))
}

#[cfg(all(test, feature = "webtransport"))]
#[path = "../tests/edge/socket_path.rs"]
mod socket_path_tests;

/// Register Lua routes and any enabled native WebTransport bindings on Edge.
/// To compose them separately, use [`crate::backend::register`] for routes.
pub fn register(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    let registry = state.registry_ref();
    if registry.resolve::<Registered>().is_some() {
        return Ok(());
    }
    let hosts = registry
        .resolve::<claviron_edge::HostAppRegistry>()
        .ok_or_else(|| {
            claviron_runtime::registry::Error::msg(
                "Lua requires the Edge host registry",
            )
        })?;
    crate::backend::register(hosts.site())?;
    #[cfg(feature = "webtransport")]
    register_webtransport(state)?;
    registry.insert(std::sync::Arc::new(Registered))?;
    Ok(())
}

/// Contribute Lua WebTransport endpoints to Edge's native session registry.
#[cfg(feature = "webtransport")]
fn register_webtransport(
    state: &claviron_runtime::AppState,
) -> claviron_runtime::registry::Result<()> {
    claviron_edge::register_routable(state, Arc::new(Sessions))
}

#[cfg(feature = "webtransport")]
struct Sessions;

#[cfg(feature = "webtransport")]
impl claviron_runtime::registry::Component<claviron_runtime::AppState>
    for Sessions
{
    type Deps = ();
    fn name(&self) -> &str {
        "LuaEdgeSessions"
    }
}

#[cfg(feature = "webtransport")]
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
        webtransport::append_routes(state, host, config)
    }
}
