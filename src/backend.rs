//! Compile site directives for embedded Lua applications.

mod config;
#[cfg(any(feature = "websocket", all(feature = "edge", feature = "webtransport")))]
pub(crate) mod origin;
#[cfg(feature = "websocket")]
mod websocket;

use std::sync::Arc;

use crate::serve::{LuaApplicationAdapter, LuaApplications};
use claviron_http::ext::PathPrefix;
use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{RouteBody, ServiceResult};
use http::Request;

use claviron_pipeline::{DispatchCall, DispatchStep};

pub use config::{LuaRouteConfig, LuaSocketOriginPolicy};

/// A scope's `lua:` directive as declared: one mapping or a list of them.
/// The edge keeps it as written; this is where it is decoded. An absent or
/// null directive declares nothing.
pub(crate) fn declared(
    scope: &claviron_pipeline::Site
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

impl claviron_pipeline::BackendHandler for LuaBackend {
    fn contract(&self) -> claviron_pipeline::BackendContract {
        claviron_pipeline::BackendContract {
            execution: claviron_runtime::ExecutorSpec::Inherit,
            identity: claviron_pipeline::BackendIdentity::None,
            reply: claviron_pipeline::BackendReply::Owner,
        }
    }
}

impl DispatchStep for LuaBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<claviron_pipeline::HostContext<'_>>,
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

#[cfg(any(feature = "websocket", all(feature = "edge", feature = "webtransport")))]
pub(crate) fn mounted_websocket_path(
    mount: Option<&claviron_http::ext::PathPrefix>,
    path: &claviron_http::ext::PathPrefix,
) -> Arc<str> {
    match mount.map(|mount| mount.as_str()) {
        None | Some("/") => Arc::from(path.as_str()),
        Some(mount) if path.as_str() == "/" => Arc::from(mount),
        Some(mount) => Arc::from(format!("{mount}{}", path.as_str())),
    }
}

/// Install `lua:` route preparation in the site's registry.
pub fn register(
    registry: &claviron_pipeline::SiteRegistry,
) -> claviron_runtime::registry::Result<()> {
    registry.register_backend(Arc::new(Factory))
}

struct Factory;
impl claviron_pipeline::factory::BackendFactory for Factory {
    fn name(&self) -> &'static str {
        "lua"
    }
    fn placement(&self) -> claviron_pipeline::factory::BackendPlacement {
        claviron_pipeline::factory::BackendPlacement::AfterFiles
    }
    fn prepare(
        &self,
        context: &claviron_pipeline::factory::BackendContext<'_>,
    ) -> claviron_runtime::registry::Result<
        Vec<Arc<dyn claviron_pipeline::BackendHandler>>,
    > {
        let policy = claviron_pipeline::extension::RouteContext {
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
        #[allow(unused_mut)]
        let mut owners: Vec<
            Arc<dyn claviron_pipeline::BackendHandler>,
        > = Vec::new();
        // A socket's path is its endpoint's, whatever an HTTP mount would say.
        #[cfg(feature = "websocket")]
        if let Some(sockets) = websocket::Sockets::prepare(context, &routes) {
            owners.push(Arc::new(sockets));
        }
        owners.extend(
            LuaBackend::new(
                context.state.registry_ref().resolve::<LuaApplications>(),
                &routes,
                Some(context.prefix),
                policy,
            )
            .into_iter()
            .map(|owner| {
                Arc::new(owner) as Arc<dyn claviron_pipeline::BackendHandler>
            }),
        );
        Ok(owners)
    }
}

#[cfg(test)]
#[path = "../tests/edge.rs"]
mod tests;
