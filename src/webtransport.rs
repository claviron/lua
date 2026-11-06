//! Independent Lua callbacks over reliable WebTransport bidirectional streams.
//!
//! The native H3 driver owns acceptance and drain. Records use the existing
//! Text/Binary/Close codec, not WebSocket frames or unreliable datagrams.
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use claviron_orbit::link::application::{
    Message, MessageReader, write_message,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};

use crate::app::LuaApplication;
use crate::serve::LuaApplications;
use crate::socket::{
    Event, LuaSocket, NEXT_ID, Outbound, Transport, handle_events,
};

/// Resolve the application generation before accepting the native session.
pub fn prepare(
    service: &LuaApplications,
    name: &str,
) -> mlua::Result<Arc<LuaApplication>> {
    let application = service
        .get(name)
        .ok_or_else(|| mlua::Error::runtime("Lua application is not loaded"))?;
    if application.socket_handlers(Transport::WebTransport)?.is_none() {
        return Err(mlua::Error::runtime(
            "Lua application serves no WebTransport",
        ));
    }
    Ok(application)
}

/// Serve a stream owned by the accepting WebTransport session.
pub async fn serve<S>(
    stream: S,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
    shutdown: impl Future<Output = ()> + Send,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let stopping = application.execution().stopping().clone();
    let shutdown = async {
        tokio::pin!(shutdown);
        tokio::select! {
            biased;
            () = stopping.cancelled() => Close(1001, "application stopping".to_owned(), false),
            () = &mut shutdown => Close(1001, "server stopping".to_owned(), false),
        }
    };
    serve_stream(stream, url, headers, application, None, shutdown).await;
}

/// Why a native WebTransport session stops accepting new streams.
pub enum SessionEnd {
    /// The peer finished its CONNECT direction normally.
    PeerClosed,
    /// The CONNECT stream was reset or the native connection failed.
    TransportLost,
    /// The accepting listener, host binding or server is retiring.
    ServerStopping,
}

impl SessionEnd {
    fn close(self) -> Close {
        match self {
            Self::PeerClosed => Close(1000, "session ended".to_owned(), true),
            Self::TransportLost => Close(1006, String::new(), false),
            Self::ServerStopping => {
                Close(1001, "server stopping".to_owned(), false)
            }
        }
    }
}

/// Own all incoming bidi stream handlers for one accepted native session.
///
/// Each stream gets separate socket callbacks and bounded queues. `max_streams`
/// is supplied by the native owner's configured admission bound, not inferred
/// here. Ending a stream does not end the session. Session shutdown stops input
/// admission, signals every stream and joins their callback and output drain.
/// The incoming accept future must be cancel-safe; it is polled alongside
/// stream completion and shutdown, without a detached accept task.
pub async fn serve_session<S, A, N>(
    incoming: A,
    max_streams: NonZeroUsize,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
    shutdown: impl Future<Output = SessionEnd>,
) where
    A: FnMut() -> N,
    N: Future<Output = Option<S>>,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let session_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let stopping = application.execution().stopping().clone();
    let shutdown = async {
        tokio::pin!(shutdown);
        tokio::select! {
            biased;
            () = stopping.cancelled() => Close(1001, "application stopping".to_owned(), false),
            end = &mut shutdown => end.close(),
        }
    };
    run_streams(incoming, max_streams, shutdown, |stream, ended| {
        serve_stream(
            stream,
            url.clone(),
            headers.clone(),
            Arc::clone(&application),
            Some(session_id),
            wait_end(ended),
        )
    })
    .await;
}

async fn run_streams<S, A, N, R, F>(
    mut incoming: A,
    max_streams: NonZeroUsize,
    shutdown: impl Future<Output = Close>,
    mut serve: R,
) where
    A: FnMut() -> N,
    N: Future<Output = Option<S>>,
    R: FnMut(S, watch::Receiver<Option<Close>>) -> F,
    F: Future<Output = ()>,
{
    let (end, ended) = watch::channel(None);
    let mut streams = FuturesUnordered::new();
    tokio::pin!(shutdown);
    let reason = loop {
        tokio::select! {
            biased;
            close = &mut shutdown => break close,
            Some(()) = streams.next(), if !streams.is_empty() => {},
            // Defer even constructing an accept future until this branch is
            // polled: select evaluates disabled branch expressions as well.
            next = async { incoming().await }, if streams.len() < max_streams.get() => {
                match next {
                    Some(stream) => streams.push(serve(stream, ended.clone())),
                    None => break SessionEnd::TransportLost.close(),
                }
            }
        }
    };
    end.send_replace(Some(reason));
    // GRACEFUL_OWNERSHIP: retain all admitted streams until their owned work
    // drains. The native owner alone may enforce its explicit grace policy.
    while streams.next().await.is_some() {}
}

async fn wait_end(mut ended: watch::Receiver<Option<Close>>) -> Close {
    loop {
        if let Some(close) = ended.borrow().clone() {
            return close;
        }
        ended
            .changed()
            .await
            .expect("session owns the end signal through drain");
    }
}

async fn serve_stream<S>(
    stream: S,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
    session_id: Option<u64>,
    shutdown: impl Future<Output = Close>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let config = application.socket_config(Transport::WebTransport);
    let (outbound, queued) = mpsc::channel(config.send_queue.max(1));
    let socket = LuaSocket {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        session_id,
        url,
        headers,
        outbound,
        transport: Transport::WebTransport,
        http_version: "HTTP/3.0",
    };
    // One pending event overlaps the callback; further reads wait for it.
    let (events, happened) = mpsc::channel(1);
    let handling = handle_events(Arc::clone(&application), socket, happened);
    let driving =
        drive(stream, queued, events, config.max_message_size, shutdown);
    tokio::join!(handling, driving);
}

#[derive(Clone)]
struct Close(u16, String, bool);

async fn drive<S>(
    stream: S,
    queued: mpsc::Receiver<Outbound>,
    events: mpsc::Sender<Event>,
    max_message_size: usize,
    shutdown: impl Future<Output = Close>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, writer) = tokio::io::split(stream);
    let (finish, closing) = oneshot::channel();
    let writing = write_loop(writer, queued, closing);
    tokio::pin!(writing, shutdown);
    let mut writer_done = false;
    let mut records = MessageReader::with_limit(max_message_size);
    let mut pending = None;
    let close = loop {
        tokio::select! {
            biased;
            close = &mut shutdown => break close,
            close = &mut writing => {
                writer_done = true;
                break close;
            },
            permit = events.reserve(), if pending.is_some() => {
                match permit {
                    Ok(permit) => permit.send(pending.take().expect("pending event")),
                    Err(_) => break Close(1011, "handler ended".to_owned(), false),
                }
            },
            record = records.read(&mut reader), if pending.is_none() => {
                match record {
                    Ok(Some(Message::Text(text))) => pending = Some(Event::Message(text.into_bytes(), false)),
                    Ok(Some(Message::Binary(bytes))) => pending = Some(Event::Message(bytes, true)),
                    Ok(Some(Message::Close { code, reason })) => break Close(code, reason, true),
                    Ok(None) => break Close(1000, String::new(), true),
                    Err(error) => {
                        tracing::debug!(%error, "Lua WebTransport stream ended");
                        break Close(1006, String::new(), false);
                    }
                }
            },
        }
    };
    let _ = finish.send(close.clone());
    let notifying = async move {
        if let Some(event) = pending {
            let _ = events.send(event).await;
        }
        let Close(code, reason, clean) = close;
        let _ = events.send(Event::Close(code, reason, clean)).await;
    };
    // Keep polling the writer while callbacks finish: closing its receiver
    // wakes producers blocked on a full queue, even if the peer cannot read.
    let draining = async {
        if !writer_done {
            writing.await;
        }
    };
    tokio::join!(notifying, draining);
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut queued: mpsc::Receiver<Outbound>,
    mut closing: oneshot::Receiver<Close>,
) -> Close {
    let mut close = None;
    loop {
        let out = tokio::select! {
            biased;
            Ok(reason) = &mut closing, if close.is_none() => {
                queued.close();
                close = Some(reason);
                continue;
            }
            out = queued.recv() => out,
        };
        let message = match out {
            Some(Outbound::Text(text)) => Message::Text(text),
            Some(Outbound::Binary(bytes)) => Message::Binary(bytes),
            Some(Outbound::Close(code, reason)) => {
                close.get_or_insert(Close(code, reason, true));
                break;
            }
            None => break,
        };
        let sending = async {
            write_message(&mut writer, &message).await?;
            writer.flush().await
        };
        tokio::pin!(sending);
        let result = tokio::select! {
            biased;
            Ok(reason) = &mut closing, if close.is_none() => {
                queued.close();
                close = Some(reason);
                // Never cancel a partially written record and start another.
                sending.await
            }
            result = &mut sending => result,
        };
        if result.is_err() {
            return Close(1006, String::new(), false);
        }
    }
    queued.close();
    let close =
        close.unwrap_or_else(|| Close(1011, "handler ended".to_owned(), false));
    if close.0 != 1006 {
        let message = Message::Close { code: close.0, reason: close.1.clone() };
        if write_message(&mut writer, &message).await.is_err()
            || writer.shutdown().await.is_err()
        {
            return Close(1006, String::new(), false);
        }
    }
    close
}

#[cfg(test)]
#[path = "../tests/webtransport.rs"]
mod tests;
