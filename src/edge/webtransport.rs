//! Bind configured Lua socket endpoints to the existing native H3 registry.
use std::sync::Arc;

use crate::webtransport::SessionEnd;
use crate::{app::LuaApplication, serve::LuaApplications};
use async_trait::async_trait;
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use http::StatusCode;

use super::Endpoint;
use claviron_edge::Site;
use claviron_edge::pipeline::{
    AnyAuthenticated, AuthContext, H3WebTransportSession, WebTransportAccept,
    WebTransportHandler, WebTransportRegistry, WebTransportReject,
    WebTransportResult,
};

pub(crate) fn append_routes(
    state: &AppState,
    host: &Host,
    config: &Site,
) -> Result<()> {
    let Some(service) = state.registry_ref().resolve::<LuaApplications>()
    else {
        return Ok(());
    };
    let registry = state
        .registry_ref()
        .resolve::<WebTransportRegistry>()
        .ok_or_else(|| Error::msg("Lua requires the WebTransport registry"))?;
    let endpoints = super::socket_endpoints(config)
        .map_err(|error| super::invalid(host, error))?;
    for endpoint in endpoints {
        registry.bind_handler(
            host.clone(),
            endpoint.path.clone(),
            Arc::new(Handler { service: service.clone(), endpoint }),
        )?;
    }
    Ok(())
}

struct Handler {
    service: Arc<LuaApplications>,
    /// Prepared with the host; admission compares against it and decodes
    /// nothing.
    endpoint: Endpoint,
}

#[async_trait]
impl WebTransportHandler for Handler {
    fn name(&self) -> &'static str {
        "lua_webtransport"
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &AuthContext,
    ) -> WebTransportResult {
        if ctx.admission.is_none() {
            return Err(WebTransportReject::new(
                StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        // Retained registry entries cannot revive removed or masked routes.
        if *ctx.path != *self.endpoint.path
            || !self.endpoint.still_declared(&ctx.host_config)
        {
            return Err(WebTransportReject::new(StatusCode::NOT_FOUND));
        }
        // Native CONNECT and H1 upgrade share the same browser-origin boundary.
        if !super::origin::allows_origin(
            self.endpoint.origin,
            &ctx.headers,
            &ctx.uri,
            &ctx.conn.scheme(),
        ) {
            return Err(WebTransportReject::forbidden());
        }
        let app = crate::webtransport::prepare(
            &self.service,
            &self.endpoint.application,
        )
        .map_err(|_| {
            WebTransportReject::new(StatusCode::SERVICE_UNAVAILABLE)
        })?;
        Ok(WebTransportAccept::authenticated(app))
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> Result<()> {
        let application = accepted
            .ok_or_else(|| Error::msg("Lua WebTransport application missing"))?
            .take::<Arc<LuaApplication>>()?;
        let shutdown = async {
            tokio::select! {
                () = ctx.wait_graceful_shutdown() => SessionEnd::ServerStopping,
                () = state.on_shutdown() => SessionEnd::ServerStopping,
                closed = session.closed() => match closed {
                    Ok(_) => SessionEnd::PeerClosed,
                    Err(error) => {
                        tracing::debug!(%error, "Lua WebTransport CONNECT ended");
                        SessionEnd::TransportLost
                    }
                },
            }
        };
        let headers = ctx
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        crate::webtransport::serve_session(
            || session.accept_bi(),
            session.incoming_capacity(),
            ctx.uri.to_string(),
            headers,
            application,
            shutdown,
        )
        .await;
        drop(session);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/edge/webtransport.rs"]
mod tests;
