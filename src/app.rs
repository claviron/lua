//! Lua applications: a state loaded once per worker process and woken by
//! requests. See `ARCHITECTURE.md`, Applications.
//!
//! Each request is a task on Tokio and a coroutine in the application's state,
//! admitted through two semaphores: `max_waiting` bounds the queue for a
//! permit, `concurrency` the requests the state runs at once.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use claviron_http::policy::{BodyNeed, HttpPolicy, HttpPolicyOverride};
use claviron_http::{
    RouteBody, RouteResponse, boxed_body, bytes_body, err_status,
};
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue, RETRY_AFTER};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use mlua::chunk::ChunkMode;
use mlua::{Function, Lua, LuaString, ObjectLike, Table, Value};
use serde::Deserialize;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{Engine, Execution};

/// One application in `lua.yaml`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    /// The module that returns what serves requests. Relative: the working
    /// directory's.
    pub module: PathBuf,
    /// Application framework metadata; when absent, management reports Olua.
    #[serde(
        default,
        alias = "framework",
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    pub frameworks: Option<Vec<String>>,
    #[serde(default)]
    pub icon: Option<String>,
    /// Raw execution sources; the application owner resolves and watches them.
    #[serde(default, deserialize_with = "claviron_helpers::de::one_or_many")]
    pub env_file: Vec<PathBuf>,
    /// Inline values win over files and the selected deployment variant.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// Requests the state runs at once: the permits of its semaphore.
    #[serde(default = "one")]
    pub concurrency: usize,
    /// Requests queued for a permit before one more is refused.
    #[serde(default = "max_waiting")]
    pub max_waiting: usize,
    /// Its WebSockets, when a `lua:` route declares any.
    #[serde(default)]
    pub websocket: SocketConfig,
    /// Reliable WebTransport sessions use the same message/queue limits.
    #[serde(default)]
    pub webtransport: SocketConfig,
}

/// An application's WebSockets: the same two limits a JavaScript
/// application's have, with the same defaults.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SocketConfig {
    /// The largest message a client may send, in bytes; a larger one closes
    /// the socket with 1009.
    #[serde(default = "max_message_size")]
    pub max_message_size: usize,
    /// Messages `socket:send` queues before it waits for the client.
    #[serde(default = "send_queue")]
    pub send_queue: usize,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self { max_message_size: max_message_size(), send_queue: send_queue() }
    }
}

/// 1 MiB, JavaScript's `websocket.max_message_size`.
fn max_message_size() -> usize {
    1024 * 1024
}

/// 64, JavaScript's `websocket.action_queue_capacity`.
fn send_queue() -> usize {
    64
}

fn one() -> usize {
    1
}

/// JavaScript applications' default, so the two queue alike.
fn max_waiting() -> usize {
    256
}

/// A loaded application: its state and what serves requests in it.
pub struct LuaApplication {
    name: Arc<str>,
    lua: Lua,
    execution: Arc<Execution>,
    served: Value,
    fetch: Function,
    permits: Arc<Semaphore>,
    concurrency: usize,
    waiting: Semaphore,
    #[cfg(feature = "websocket")]
    socket: SocketConfig,
    #[cfg(feature = "webtransport")]
    webtransport: SocketConfig,
}

/// Pieces of a streamed reply between the handler and the client: one being
/// written out while the handler produces the next. The count is not a
/// buffer size an operator tunes — how much of a reply is held is the
/// response policy's `buffering`, applied after this — it only lets the two
/// sides overlap, the same two a JavaScript reply uses.
const REPLY_PIECES: usize = 2;

impl LuaApplication {
    /// Open the application's state and run its module once. What the module
    /// returns serves requests: a table or userdata with a `fetch` method —
    /// `claviron.router()` is one.
    pub async fn load(
        engine: &Engine,
        name: &str,
        config: &ApplicationConfig,
    ) -> Result<Self, crate::Error> {
        let source =
            std::fs::read_to_string(&config.module).map_err(|error| {
                mlua::Error::runtime(format!(
                    "{}: {error}",
                    config.module.display()
                ))
            })?;
        let target: Arc<str> = Arc::from(name);
        let printed = Arc::clone(&target);
        let (lua, execution) = engine.open(
            &config.module.display().to_string(),
            Some(Arc::new(move |line: &[u8]| {
                tracing::info!(
                    application = %printed,
                    "{}",
                    String::from_utf8_lossy(line).trim_end()
                );
            })),
        )?;
        let served: Value = lua
            .load(source)
            .set_name(format!("@{}", config.module.display()))
            .set_mode(ChunkMode::Text)
            .call_async(())
            .await?;
        let fetch: Function = match &served {
            Value::Table(table) => table.get("fetch")?,
            Value::UserData(data) => data.get("fetch")?,
            other => {
                return Err(mlua::Error::runtime(format!(
                    "{} returned a {}, not something with a `fetch` method",
                    config.module.display(),
                    other.type_name()
                ))
                .into());
            }
        };
        if config.concurrency == 0 {
            return Err(
                mlua::Error::runtime("concurrency is at least 1").into()
            );
        }
        Ok(Self {
            name: target,
            lua,
            execution,
            served,
            fetch,
            permits: Arc::new(Semaphore::new(config.concurrency)),
            concurrency: config.concurrency,
            #[cfg(feature = "websocket")]
            socket: config.websocket,
            #[cfg(feature = "webtransport")]
            webtransport: config.webtransport,
            waiting: Semaphore::new(config.max_waiting),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Observe this application's retirement while a native session opens.
    #[cfg(feature = "webtransport")]
    pub fn execution_stopping(&self) -> tokio_util::sync::CancellationToken {
        self.execution.stopping().clone()
    }

    /// Requests the state may run at once, and how many it runs now.
    pub fn permits(&self) -> (usize, usize) {
        (self.concurrency, self.concurrency - self.permits.available_permits())
    }

    /// The handlers its sockets are served by — `websocket` on the table the
    /// module returned, or what `router:websocket` was given — looked up
    /// when a socket opens, so a module may set them after it returns.
    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    pub(crate) fn socket_handlers(
        &self,
        transport: crate::socket::Transport,
    ) -> mlua::Result<Option<Table>> {
        let name = transport.name();
        match &self.served {
            Value::Table(table) => table.get(name),
            Value::UserData(data) => {
                let router = data.borrow::<crate::router::Router>()?;
                Ok(match transport {
                    #[cfg(feature = "websocket")]
                    crate::socket::Transport::WebSocket => {
                        router.websocket.clone()
                    }
                    #[cfg(feature = "webtransport")]
                    crate::socket::Transport::WebTransport => {
                        router.webtransport.clone()
                    }
                })
            }
            _ => Ok(None),
        }
    }

    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    pub(crate) fn socket_config(
        &self,
        transport: crate::socket::Transport,
    ) -> SocketConfig {
        match transport {
            #[cfg(feature = "websocket")]
            crate::socket::Transport::WebSocket => self.socket,
            #[cfg(feature = "webtransport")]
            crate::socket::Transport::WebTransport => self.webtransport,
        }
    }

    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    pub(crate) fn lua(&self) -> &Lua {
        &self.lua
    }

    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    pub(crate) fn execution(&self) -> &Arc<Execution> {
        &self.execution
    }

    #[cfg(any(feature = "websocket", feature = "webtransport"))]
    pub(crate) fn semaphore(&self) -> &Arc<Semaphore> {
        &self.permits
    }

    /// End every wait a handler is parked on: the worker is stopping.
    pub fn stop(&self) {
        self.execution.interrupt();
    }

    /// Serve one request under `policy`: its `requests.buffering` says
    /// whether the body is taken whole before the handler runs, its
    /// `requests.queue_timeout` how long the request may wait for a permit
    /// (`None` waits for one), and its `responses` how the reply is delivered.
    ///
    /// A body taken whole is taken before the permit, so a slow upload holds
    /// a place in the queue, never one of the state's permits.
    pub async fn handle(
        self: &Arc<Self>,
        request: Request<RouteBody>,
        policy: HttpPolicy,
    ) -> RouteResponse {
        let Ok(queued) = self.waiting.try_acquire() else {
            return unavailable();
        };
        let (parts, body) = request.into_parts();
        let declared = http_body::Body::size_hint(&body).exact();
        let whole = http_body::Body::is_end_stream(&body)
            || policy
                .requests
                .delivery(BodyNeed::Stream, declared, false)
                .takes_whole(declared);
        let body = if whole {
            match body.collect().await {
                Ok(collected) => bytes_body(collected.to_bytes()),
                Err(error) => {
                    tracing::warn!(application = %self.name, "request body: {error}");
                    return err_status(StatusCode::BAD_REQUEST);
                }
            }
        } else {
            body
        };
        let acquire = Arc::clone(&self.permits).acquire_owned();
        let queue_timeout: Option<Duration> = policy.requests.queue_timeout;
        let permit = match queue_timeout {
            Some(limit) => match tokio::time::timeout(limit, acquire).await {
                Ok(permit) => permit,
                Err(_) => return unavailable(),
            },
            None => acquire.await,
        };
        let Ok(permit) = permit else {
            return unavailable();
        };
        drop(queued);
        match self.call(parts, body, &policy, permit).await {
            Ok(reply) => reply,
            Err(error) => {
                tracing::error!(application = %self.name, "{error}");
                err_status(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// Run the handler. The permit is held until the reply is complete: with
    /// a reply that streams, that is after this returns.
    async fn call(
        self: &Arc<Self>,
        parts: http::request::Parts,
        body: RouteBody,
        policy: &HttpPolicy,
        permit: OwnedSemaphorePermit,
    ) -> mlua::Result<RouteResponse> {
        let table = self.lua.create_table()?;
        table.set("method", parts.method.as_str())?;
        table.set("path", parts.uri.path())?;
        table.set("query", parts.uri.query())?;
        let headers = self.lua.create_table()?;
        for name in parts.headers.keys() {
            let joined = parts
                .headers
                .get_all(name)
                .iter()
                .map(|value| {
                    String::from_utf8_lossy(value.as_bytes()).into_owned()
                })
                .collect::<Vec<_>>()
                .join(", ");
            headers.set(name.as_str(), joined)?;
        }
        table.set("headers", headers)?;
        let body = Arc::new(RequestBody {
            body: Mutex::new(Some(body)),
            execution: Arc::clone(&self.execution),
        });
        let reading = Arc::clone(&body);
        table.set(
            "read",
            self.lua.create_async_function(move |lua, _: Value| {
                let body = Arc::clone(&reading);
                async move { bytes_or_nil(&lua, body.read().await?) }
            })?,
        )?;
        table.set(
            "body",
            self.lua.create_async_function(move |lua, _: Value| {
                let body = Arc::clone(&body);
                async move { lua.create_string(body.rest().await?) }
            })?,
        )?;

        let reply = tokio::select! {
            reply = self.fetch.call_async::<Value>((self.served.clone(), table)) => reply?,
            () = self.execution.stopping().cancelled() => return Ok(unavailable())
        };
        self.reply_of(reply, policy, permit).await
    }

    /// A handler's reply as a response: a string is `200 text/plain`, a table
    /// is `{ status, headers, body }`, and its body a string or a function
    /// called for each piece until it returns `nil`.
    async fn reply_of(
        self: &Arc<Self>,
        reply: Value,
        policy: &HttpPolicy,
        permit: OwnedSemaphorePermit,
    ) -> mlua::Result<RouteResponse> {
        let (status, mut headers, body) = match reply {
            Value::String(text) => {
                let mut headers = http::HeaderMap::new();
                headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                (
                    StatusCode::OK,
                    headers,
                    bytes_body(Bytes::copy_from_slice(&text.as_bytes())),
                )
            }
            Value::Table(table) => {
                let status: Option<u16> = table.get("status")?;
                let status = StatusCode::from_u16(status.unwrap_or(200))
                    .map_err(|error| {
                        mlua::Error::runtime(format!("status: {error}"))
                    })?;
                let headers =
                    headers_of(table.get::<Option<Table>>("headers")?)?;
                let body = match table.get::<Value>("body")? {
                    Value::Nil => bytes_body(Bytes::new()),
                    Value::String(text) => {
                        bytes_body(Bytes::copy_from_slice(&text.as_bytes()))
                    }
                    Value::Function(next) => {
                        return self
                            .stream(status, headers, next, policy, permit)
                            .await;
                    }
                    other => {
                        return Err(mlua::Error::runtime(format!(
                            "body is a {}, not a string or a function",
                            other.type_name()
                        )));
                    }
                };
                (status, headers, body)
            }
            other => {
                return Err(mlua::Error::runtime(format!(
                    "fetch returned a {}, not a string or a reply table",
                    other.type_name()
                )));
            }
        };
        let body = claviron_http::body::deliver_reply(
            &mut headers,
            body,
            &policy.responses,
        )
        .await;
        drop(permit);
        Ok(response(status, headers, body))
    }

    /// A reply whose body the handler produces piece by piece. The head goes
    /// now; the pieces follow from a task that holds the permit until the
    /// last one, or until the client is gone, or the worker stops.
    async fn stream(
        self: &Arc<Self>,
        status: StatusCode,
        mut headers: http::HeaderMap,
        next: Function,
        policy: &HttpPolicy,
        permit: OwnedSemaphorePermit,
    ) -> mlua::Result<RouteResponse> {
        let (pieces, receiver) = mpsc::channel(REPLY_PIECES);
        let application = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit;
            loop {
                let piece = tokio::select! {
                    piece = next.call_async::<Option<LuaString>>(()) => piece,
                    () = pieces.closed() => return,
                    () = application.execution.stopping().cancelled() => {
                        Err(mlua::Error::runtime("the reply ended: the application is stopping"))
                    }
                };
                match piece {
                    Ok(Some(piece)) => {
                        let piece = Bytes::copy_from_slice(&piece.as_bytes());
                        if pieces.send(Ok(piece)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(error) => {
                        tracing::error!(application = %application.name, "{error}");
                        let _ = pieces
                            .send(Err(std::io::Error::other(error.to_string())))
                            .await;
                        return;
                    }
                }
            }
        });
        let body = claviron_http::body::deliver_reply(
            &mut headers,
            boxed_body(ReplyStream(receiver)),
            &policy.responses,
        )
        .await;
        Ok(response(status, headers, body))
    }
}

/// A request body as a handler reads it: in pieces, or the rest at once.
struct RequestBody {
    body: Mutex<Option<RouteBody>>,
    execution: Arc<Execution>,
}

impl RequestBody {
    /// The next piece, or `None` once the body has ended.
    async fn read(&self) -> mlua::Result<Option<Bytes>> {
        let mut guard = self.body.lock().await;
        loop {
            let Some(body) = guard.as_mut() else {
                return Ok(None);
            };
            let frame = tokio::select! {
                frame = body.frame() => frame,
                () = self.execution.stopping().cancelled() => {
                    return Err(mlua::Error::runtime("the read ended: the application is stopping"));
                }
            };
            match frame {
                None => *guard = None,
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        return Ok(Some(data));
                    }
                }
                Some(Err(error)) => {
                    return Err(mlua::Error::runtime(format!(
                        "request body: {error}"
                    )));
                }
            }
        }
    }

    /// What is left of the body, whole.
    async fn rest(&self) -> mlua::Result<Vec<u8>> {
        let mut rest = Vec::new();
        while let Some(piece) = self.read().await? {
            rest.extend_from_slice(&piece);
        }
        Ok(rest)
    }
}

fn bytes_or_nil(
    lua: &Lua,
    bytes: Option<Bytes>,
) -> mlua::Result<Value> {
    match bytes {
        Some(bytes) => Ok(Value::String(lua.create_string(&bytes)?)),
        None => Ok(Value::Nil),
    }
}

/// A streamed reply's pieces as a body. An error ends it cut short, so the
/// client sees a broken reply rather than a complete one.
struct ReplyStream(mpsc::Receiver<std::io::Result<Bytes>>);

impl http_body::Body for ReplyStream {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>>
    {
        self.0
            .poll_recv(cx)
            .map(|piece| piece.map(|piece| piece.map(http_body::Frame::data)))
    }
}

fn response(
    status: StatusCode,
    headers: http::HeaderMap,
    body: RouteBody,
) -> RouteResponse {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

/// A reply's headers: a list is one header line per value, as `set-cookie`
/// needs.
fn headers_of(given: Option<Table>) -> mlua::Result<http::HeaderMap> {
    let mut headers = http::HeaderMap::new();
    let Some(given) = given else {
        return Ok(headers);
    };
    for pair in given.pairs::<String, Value>() {
        let (name, value) = pair?;
        let name = HeaderName::try_from(name.as_str()).map_err(|error| {
            mlua::Error::runtime(format!("header {name}: {error}"))
        })?;
        let values = match value {
            Value::Table(list) => list
                .sequence_values::<String>()
                .collect::<mlua::Result<Vec<_>>>()?,
            other => vec![other.to_string()?],
        };
        for value in values {
            let value = HeaderValue::try_from(value).map_err(|error| {
                mlua::Error::runtime(format!("header {name}: {error}"))
            })?;
            headers.append(&name, value);
        }
    }
    Ok(headers)
}

fn unavailable() -> RouteResponse {
    let mut response = err_status(StatusCode::SERVICE_UNAVAILABLE);
    response.headers_mut().insert(RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

/// The route's policy narrows the one the edge lends. With no edge in front
/// there is none to narrow.
pub(crate) fn route_policy(
    edge: Option<HttpPolicy>,
    route: &HttpPolicyOverride,
) -> HttpPolicy {
    edge.unwrap_or(HttpPolicy::unlimited()).narrow(route)
}

#[cfg(test)]
#[path = "../tests/app.rs"]
mod tests;
