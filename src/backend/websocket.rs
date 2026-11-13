//! An embedded Lua application's WebSocket, on HTTP/1.1 and HTTP/2 alike: the
//! offer says what is being opened and carries it.

use std::collections::HashMap;
use std::sync::Arc;

use claviron_http::ext::ConnCtx;
use claviron_http::socket::{Carrier, Offer, SocketIo, SocketKind};
use claviron_http::{RouteBody, ServiceResult, err_status};
use claviron_pipeline::factory::BackendContext;
use claviron_pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep, HostContext,
};
use http::{Request, StatusCode};

use super::{LuaRouteConfig, LuaSocketOriginPolicy, origin::allows_origin};
use crate::serve::LuaApplications;

struct Socket {
    application: Arc<str>,
    origin: LuaSocketOriginPolicy,
}

/// A scope's socket endpoints, in front of its HTTP mounts.
pub(super) struct Sockets {
    service: Arc<LuaApplications>,
    endpoints: HashMap<Arc<str>, Socket>,
    /// The transport's `h1.upgrade`: what the connection's terms allow.
    h1_upgrade: bool,
}

impl Sockets {
    pub(super) fn prepare(
        context: &BackendContext<'_>,
        routes: &[LuaRouteConfig],
    ) -> Option<Self> {
        let mut endpoints = HashMap::new();
        for route in routes {
            for socket in route.websockets() {
                endpoints.insert(
                    super::mounted_websocket_path(
                        Some(context.prefix),
                        &socket.path,
                    ),
                    Socket {
                        application: Arc::from(route.application.as_str()),
                        origin: route.origin,
                    },
                );
            }
        }
        if endpoints.is_empty() {
            return None;
        }
        Some(Self {
            service: context
                .state
                .registry_ref()
                .resolve::<LuaApplications>()?,
            endpoints,
            h1_upgrade: context.transport.h1.upgrade,
        })
    }
}

impl BackendHandler for Sockets {
    fn contract(&self) -> BackendContract {
        BackendContract {
            execution: claviron_runtime::ExecutorSpec::Inherit,
            // The origin check reads the connection's scheme.
            identity: BackendIdentity::Request,
            reply: BackendReply::Owner,
        }
    }
}

fn answered(status: StatusCode) -> DispatchCall {
    DispatchCall::ready(ServiceResult::Handled(err_status(status)))
}

impl DispatchStep for Sockets {
    fn call(
        &self,
        request: Request<RouteBody>,
        _: Option<HostContext<'_>>,
    ) -> DispatchCall {
        let offer = request
            .extensions()
            .get::<Offer>()
            .filter(|offer| offer.info().kind == SocketKind::WebSocket);
        let socket = self.endpoints.get(request.uri().path());
        let (Some(offer), Some(socket)) = (offer, socket) else {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        };
        let carrier = offer.info().carrier;
        if carrier == Carrier::H1 && !self.h1_upgrade {
            return answered(StatusCode::FORBIDDEN);
        }
        let scheme = request
            .extensions()
            .get::<Arc<ConnCtx>>()
            .map(|connection| connection.scheme());
        if !scheme.is_some_and(|scheme| {
            allows_origin(
                socket.origin,
                request.headers(),
                request.uri(),
                &scheme,
            )
        }) {
            return answered(StatusCode::FORBIDDEN);
        }
        // Checked before the handshake is answered.
        let Ok(application) =
            crate::websocket::prepare(&self.service, &socket.application)
        else {
            return answered(StatusCode::SERVICE_UNAVAILABLE);
        };
        let Some(claim) = offer.claim() else {
            return answered(StatusCode::BAD_REQUEST);
        };
        let authority = request
            .uri()
            .authority()
            .map(ToString::to_string)
            .or_else(|| {
                request
                    .headers()
                    .get(http::header::HOST)
                    .and_then(|host| host.to_str().ok())
                    .map(str::to_owned)
            })
            .unwrap_or_default();
        let url = format!(
            "wss://{authority}{}",
            request.uri().path_and_query().map_or("/", |path| path.as_str())
        );
        let headers = crate::websocket::header_pairs(request.headers());
        let version = match carrier {
            Carrier::H2 => "HTTP/2.0",
            _ => "HTTP/1.1",
        };
        DispatchCall::ready(ServiceResult::Handled(claim.accept_with(
            move |io| async move {
                let SocketIo::Stream(upgraded) = io else {
                    tracing::debug!(
                        "a Lua WebSocket has no use for this session's I/O"
                    );
                    return;
                };
                crate::websocket::serve_websocket(
                    upgraded,
                    url,
                    headers,
                    application,
                    version,
                )
                .await;
            },
        )))
    }
}
