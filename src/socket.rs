//! Shared Lua socket callbacks; native transports own framing and lifetime.
use crate::app::LuaApplication;
use mlua::{Function, LuaString, Table, UserData, UserDataMethods, Value};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Clone, Copy)]
pub(crate) enum Transport {
    #[cfg(feature = "websocket")]
    WebSocket,
    #[cfg(feature = "webtransport")]
    WebTransport,
}
impl Transport {
    pub(crate) fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "websocket")]
            Self::WebSocket => "websocket",
            #[cfg(feature = "webtransport")]
            Self::WebTransport => "webtransport",
        }
    }
}

/// What the application writes to a socket.
pub(crate) enum Outbound {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
}

/// What a socket's events are made of, in the order they happened.
pub(crate) enum Event {
    Message(Vec<u8>, bool),
    Close(u16, String, bool),
}

/// Ids tell sockets apart within this worker process.
pub(crate) static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// One socket as a handler holds it.
pub(crate) struct LuaSocket {
    pub(crate) id: u64,
    #[cfg(feature = "webtransport")]
    pub(crate) session_id: Option<u64>,
    pub(crate) url: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) outbound: mpsc::Sender<Outbound>,
    pub(crate) transport: Transport,
    pub(crate) http_version: &'static str,
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("socket: {error}"))
}

impl LuaSocket {
    async fn queue(
        &self,
        message: Outbound,
    ) -> mlua::Result<()> {
        self.outbound
            .send(message)
            .await
            .map_err(|_| failure("the socket is closed"))
    }
}

impl UserData for LuaSocket {
    fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("transport", |_, this| {
            Ok(this.transport.name())
        });
        fields.add_field_method_get("http_version", |_, this| {
            Ok(this.http_version)
        });
        fields.add_field_method_get("id", |_, this| Ok(this.id));
        #[cfg(feature = "webtransport")]
        fields
            .add_field_method_get("session_id", |_, this| Ok(this.session_id));
        fields.add_field_method_get("url", |_, this| Ok(this.url.clone()));
        fields.add_field_method_get("headers", |lua, this| {
            let table = lua.create_table()?;
            for (name, value) in &this.headers {
                table.set(name.as_str(), value.as_str())?;
            }
            Ok(table)
        });
    }

    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // A text message: the string must be UTF-8, as the protocol says.
        methods.add_async_method(
            "send",
            |_, this: mlua::UserDataRef<LuaSocket>, text: LuaString| async move {
                let text = text
                    .to_str()
                    .map_err(|_| failure("text is UTF-8; send bytes with send_binary"))?
                    .to_string();
                this.queue(Outbound::Text(text)).await
            }
        );
        #[cfg(feature = "orbit")]
        methods.add_function(
            "listen",
            |_,
             (socket, listener): (
                mlua::AnyUserData,
                Option<mlua::AnyUserData>,
            )| {
                socket.borrow::<LuaSocket>()?;
                if let Some(listener) = &listener {
                    listener.borrow::<crate::events::Listener>()?;
                }
                socket.set_named_user_value("claviron.events", listener)?;
                Ok(socket)
            },
        );
        methods.add_async_method(
            "send_binary",
            |_, this: mlua::UserDataRef<LuaSocket>, bytes: LuaString| async move {
                this.queue(Outbound::Binary(bytes.as_bytes().to_vec())).await
            }
        );
        methods.add_async_method(
            "close",
            |_,
             this: mlua::UserDataRef<LuaSocket>,
             (code, reason): (Option<u16>, Option<String>)| async move {
                this.queue(Outbound::Close(
                    code.unwrap_or(1000),
                    reason.unwrap_or_default(),
                ))
                .await
            },
        );
    }
}

/// Run a socket's handlers: `open`, then each event in order.
pub(crate) async fn handle_events(
    application: Arc<LuaApplication>,
    socket: LuaSocket,
    mut happened: mpsc::Receiver<Event>,
) {
    let outbound = socket.outbound.clone();
    let id = socket.id;
    let run = async {
        let lua = application.lua();
        let handlers =
            application.socket_handlers(socket.transport)?.ok_or_else(
                || failure("the application no longer serves this transport"),
            )?;
        let socket = lua.create_userdata(socket)?;
        let ending = async {
            call(&application, &handlers, "open", (socket.clone(),)).await?;
            loop {
                #[cfg(feature = "orbit")]
                let listener = socket
                    .named_user_value::<Option<mlua::AnyUserData>>(
                        "claviron.events",
                    )?
                    .map(|listener| {
                        listener
                            .borrow::<crate::events::Listener>()
                            .map(|listener| listener.clone())
                    })
                    .transpose()?;
                #[cfg(feature = "orbit")]
                let incoming =
                    next_input(&mut happened, listener.as_ref()).await;
                #[cfg(not(feature = "orbit"))]
                let incoming = Input::Socket(happened.recv().await);
                match incoming {
                    Input::Socket(Some(Event::Message(data, binary))) => {
                        let data = lua.create_string(data)?;
                        call(
                            &application,
                            &handlers,
                            "message",
                            (socket.clone(), data, binary),
                        )
                        .await?;
                    }
                    Input::Socket(Some(Event::Close(code, reason, clean))) => {
                        break Ok::<_, mlua::Error>((code, reason, clean));
                    }
                    Input::Socket(None) => {
                        break Ok((1006, "transport ended".to_owned(), false));
                    }
                    #[cfg(feature = "orbit")]
                    Input::Application(Ok(event)) => {
                        let event = crate::events::event_table(lua, event)?;
                        call(
                            &application,
                            &handlers,
                            "event",
                            (socket.clone(), event),
                        )
                        .await?;
                    }
                    #[cfg(feature = "orbit")]
                    Input::Application(Err(error)) => {
                        if application.execution().stopping().is_cancelled() {
                            // The native owner still delivers its close event. Stop
                            // event admission while that transport drains.
                            socket.set_named_user_value(
                                "claviron.events",
                                Value::Nil,
                            )?;
                        } else {
                            return Err(error);
                        }
                    }
                }
            }
        }
        .await;
        #[cfg(feature = "orbit")]
        socket.set_named_user_value("claviron.events", Value::Nil)?;
        let (code, reason, clean) = match &ending {
            Ok(close) => close.clone(),
            Err(_) if application.execution().stopping().is_cancelled() => {
                (1001, "application stopping".to_owned(), false)
            }
            Err(_) => (1011, "application error".to_owned(), false),
        };
        // Close is cleanup, including after execution cancellation. No event
        // listener or request coroutine is spawned outside this socket owner.
        let close = match handlers.get::<Option<Function>>("close")? {
            Some(close) => {
                let _permit = Arc::clone(application.semaphore())
                    .acquire_owned()
                    .await
                    .map_err(|_| failure("the application stopped"))?;
                close
                    .call_async::<Value>((socket, code, reason, clean))
                    .await
                    .map(|_| ())
            }
            None => Ok(()),
        };
        ending?;
        close?;
        Ok::<(), mlua::Error>(())
    };
    if let Err(error) = run.await {
        warn!(application = %application.name(), socket = id, "Lua socket handler failed: {error}");
        let _ = outbound
            .send(Outbound::Close(1011, "application error".to_owned()))
            .await;
    }
}

enum Input {
    Socket(Option<Event>),
    #[cfg(feature = "orbit")]
    Application(mlua::Result<claviron_app::ApplicationEvent>),
}

#[cfg(all(test, feature = "orbit"))]
#[path = "../tests/socket_events.rs"]
mod tests;

#[cfg(feature = "orbit")]
async fn next_input(
    happened: &mut mpsc::Receiver<Event>,
    listener: Option<&crate::events::Listener>,
) -> Input {
    match listener {
        Some(listener) => tokio::select! {
            event = happened.recv() => Input::Socket(event),
            event = listener.receive() => Input::Application(event),
        },
        None => Input::Socket(happened.recv().await),
    }
}

/// Call one handler, when the application has it, holding a permit while it
/// runs. A handler waits for a permit as long as it takes: a socket's events
/// are not refused, they are only ordered behind the application's work.
async fn call(
    application: &Arc<LuaApplication>,
    handlers: &Table,
    name: &str,
    arguments: impl mlua::IntoLuaMulti,
) -> mlua::Result<()> {
    let Some(handler) = handlers.get::<Option<Function>>(name)? else {
        return Ok(());
    };
    let _permit = tokio::select! {
        permit = Arc::clone(application.semaphore()).acquire_owned() => {
            permit.map_err(|_| failure("the application stopped"))?
        }
        () = application.execution().stopping().cancelled() => {
            return Err(failure("the application is stopping"));
        }
    };
    tokio::select! {
        done = handler.call_async::<Value>(arguments) => done.map(|_| ()),
        () = application.execution().stopping().cancelled() => {
            Err(failure("the application is stopping"))
        }
    }
}
