//! Native request entry for a named, managed Lua application.

use std::sync::Arc;

use claviron_http::policy::HttpPolicyOverride;
use claviron_http::{RouteBody, RouteResponse, err_status};
use http::{Request, StatusCode};

use super::LuaApplications;

/// A prepared application name and route policy, independent of any router.
///
/// The service is captured at preparation; the current application generation
/// is acquired for each call, so stop/reload keeps its existing behavior. A
/// missing or stopped application owns the matched request and answers 503.
#[derive(Clone)]
pub struct LuaApplicationAdapter {
    service: Option<Arc<LuaApplications>>,
    application: Arc<str>,
    route: HttpPolicyOverride,
}

impl LuaApplicationAdapter {
    pub fn new(
        service: Option<Arc<LuaApplications>>,
        application: impl Into<Arc<str>>,
        route: HttpPolicyOverride,
    ) -> Self {
        let application = application.into();
        if service
            .as_ref()
            .is_some_and(|service| !service.is_declared(&application))
        {
            tracing::warn!(application = %application, "Lua route names an application absent from lua.yaml; it answers 503 until one is declared");
        }
        Self { service, application, route }
    }

    pub fn application(&self) -> &str {
        &self.application
    }

    /// Transfer the existing body directly into the current Lua application.
    /// Body delivery, admission, execution and cancellation remain engine-owned.
    /// `edge` is the policy of the scope that selected this application, which
    /// the route's own narrows; `None` when no edge stands in front of it.
    pub async fn handle(
        &self,
        request: Request<RouteBody>,
        edge: Option<claviron_http::policy::HttpPolicy>,
    ) -> RouteResponse {
        let Some(application) = self
            .service
            .as_ref()
            .and_then(|service| service.get(&self.application))
        else {
            let mut response = err_status(StatusCode::SERVICE_UNAVAILABLE);
            response.headers_mut().insert(
                http::header::RETRY_AFTER,
                http::HeaderValue::from_static("1"),
            );
            return response;
        };
        let policy = crate::app::route_policy(edge, &self.route);
        application.handle(request, policy).await
    }
}

#[cfg(test)]
#[path = "../../tests/serve/adapter.rs"]
mod tests;
