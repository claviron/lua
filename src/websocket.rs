//! WebSockets for Lua applications, the way JavaScript applications have
//! them: the edge holds the socket — handshake, frames, ping, close — and the
//! application sees three events.
//!
//! ```lua
//! router:websocket({
//!   open    = function(socket) socket:send("hello") end,
//!   message = function(socket, data, binary) socket:send(data) end,
//!   close   = function(socket, code, reason, clean) end,
//! })
//! ```
//!
//! A socket's events run one after another, each in its own coroutine and
//! each holding one of the application's `concurrency` permits only while it
//! runs, so a socket that is open and quiet costs no permit. `socket:send`
//! queues for the client and waits while the queue is full. The same session
//! serves an HTTP/1.1 `Upgrade` and an HTTP/2 extended CONNECT: both hand an
//! upgraded transport to [`serve_websocket`].

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::socket::{
    Event, LuaSocket, NEXT_ID, Outbound, Transport, handle_events,
};
use fastwebsockets::{
    FragmentCollector, Frame, OpCode, Payload, Role, WebSocket,
};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;
use tracing::debug;

use crate::app::LuaApplication;
use crate::serve::LuaApplications;

/// Why a socket cannot be opened for an application.
#[derive(Debug, thiserror::Error)]
pub enum Refused {
    #[error("Lua application '{0}' is not loaded")]
    NotLoaded(String),
    #[error("Lua application '{0}' serves no WebSocket")]
    NoHandlers(String),
}

/// The application a socket is for, checked before the handshake is
/// answered: loaded, and with handlers to hand its events to.
pub fn prepare(
    service: &LuaApplications,
    application: &str,
) -> Result<Arc<LuaApplication>, Refused> {
    let loaded = service
        .get(application)
        .ok_or_else(|| Refused::NotLoaded(application.to_owned()))?;
    match loaded.socket_handlers(Transport::WebSocket) {
        Ok(Some(_)) => Ok(loaded),
        _ => Err(Refused::NoHandlers(application.to_owned())),
    }
}

/// The request headers as a socket shows them.
pub fn header_pairs(headers: &http::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

/// Serve one socket over an upgraded transport until either side closes it
/// or the application stops. HTTP/1.1 `Upgrade` and HTTP/2 extended CONNECT
/// both end here.
pub async fn serve_websocket(
    upgraded: Upgraded,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
    http_version: &'static str,
) {
    let config = application.socket_config(Transport::WebSocket);
    let (outbound, mut queued) = mpsc::channel(config.send_queue.max(1));
    let socket = LuaSocket {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        #[cfg(feature = "webtransport")]
        session_id: None,
        url,
        headers,
        outbound,
        transport: Transport::WebSocket,
        http_version,
    };
    let id = socket.id;
    let (events, happened) = mpsc::channel::<Event>(1);
    let handling =
        tokio::spawn(handle_events(Arc::clone(&application), socket, happened));

    let mut raw =
        WebSocket::after_handshake(TokioIo::new(upgraded), Role::Server);
    raw.set_auto_close(true);
    raw.set_auto_pong(true);
    raw.set_max_message_size(config.max_message_size);
    let mut frames = FragmentCollector::new(raw);

    // One event waits here for the handlers to take it; frames are not read
    // meanwhile, so a client that sends faster than the application reads
    // is held back by its own connection. What the application sends keeps
    // flowing out whatever the handlers are doing.
    let mut pending: Option<Event> = None;
    let stopping = application.execution().stopping().clone();
    loop {
        tokio::select! {
            out = queued.recv() => match out {
                Some(Outbound::Text(text)) => {
                    if frames.write_frame(Frame::text(Payload::Owned(text.into_bytes()))).await.is_err() {
                        pending = Some(Event::Close(1006, String::new(), false));
                        break;
                    }
                }
                Some(Outbound::Binary(bytes)) => {
                    if frames.write_frame(Frame::binary(Payload::Owned(bytes))).await.is_err() {
                        pending = Some(Event::Close(1006, String::new(), false));
                        break;
                    }
                }
                Some(Outbound::Close(code, reason)) => {
                    let _ = frames.write_frame(Frame::close(code, reason.as_bytes())).await;
                    pending = Some(Event::Close(code, reason, true));
                    break;
                }
                // The handlers are gone: an error closed the socket.
                None => break
            },
            permit = events.reserve(), if pending.is_some() => match permit {
                Ok(permit) => permit.send(pending.take().expect("guarded")),
                Err(_) => break
            },
            frame = frames.read_frame(), if pending.is_none() => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        debug!(socket = id, %error, "Lua WebSocket transport ended");
                        pending = Some(Event::Close(1006, String::new(), false));
                        break;
                    }
                };
                if frame.payload.len() > config.max_message_size {
                    let _ = frames.write_frame(Frame::close(1009, b"message too large")).await;
                    pending = Some(Event::Close(1009, "message too large".to_owned(), false));
                    break;
                }
                match frame.opcode {
                    OpCode::Text => {
                        if std::str::from_utf8(&frame.payload).is_err() {
                            let _ = frames.write_frame(Frame::close(1007, b"text is not UTF-8")).await;
                            pending = Some(Event::Close(1007, "text is not UTF-8".to_owned(), false));
                            break;
                        }
                        pending = Some(Event::Message(Vec::from(frame.payload), false));
                    }
                    OpCode::Binary => pending = Some(Event::Message(Vec::from(frame.payload), true)),
                    OpCode::Close => {
                        let (code, reason) = close_details(&frame.payload);
                        pending = Some(Event::Close(code, reason, true));
                        break;
                    }
                    _ => {}
                }
            },
            () = stopping.cancelled() => {
                let _ = frames.write_frame(Frame::close(1001, b"application stopping")).await;
                break;
            }
        }
    }
    // The close event, when there is one, is the handlers' last; they are
    // waited for, so a close handler finishes before the session ends.
    if let Some(event) = pending.take()
        && let Ok(permit) = events.reserve().await
    {
        permit.send(event);
    }
    drop(events);
    let _ = handling.await;
}

fn close_details(payload: &[u8]) -> (u16, String) {
    if payload.len() < 2 {
        return (1005, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    (code, String::from_utf8_lossy(&payload[2..]).into_owned())
}
