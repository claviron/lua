//! `claviron.orbit.stream`: a byte stream between two holders over the
//! fleet's shared memory — the table the Orbit transport's bodies ride on.
//!
//! ```lua
//! local s = claviron.stream()                 -- this process's lane
//! claviron.events.publish("pipe", { ticket = s:ticket() })
//! s:send("hello")                             -- one message
//! s:finish()
//!
//! local peer = claviron.stream.open(ticket)   -- another script, any process
//! local message = peer:receive()              -- "hello"; nil once finished
//! ```
//!
//! A stream is two bounded rings, one each way. A writer that fills its ring
//! waits for the reader; a reader waits for bytes or the end. Both waits end
//! when the script is told to stop, never after an interval.
//!
//! `read` and `write` are the bytes as they are; `send` and `receive` frame
//! them as messages, in the record the transport's message streams use. One
//! stream takes one of the two: raw bytes read as messages are garbage.

use std::sync::Arc;

use claviron_orbit::link::application::{
    Message, MessageReader, write_message,
};
use claviron_orbit::stream::{Endpoint, ReadHalf, Streams, Ticket, WriteHalf};
use mlua::{Lua, Table, UserData, UserDataMethods, UserDataRef, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::{Execution, Extension};

/// At most one ring's worth per `read`: the most a writer can have put there.
const READ_MAX: usize = claviron_orbit::stream::STREAM_BUFFER_BYTES;

/// `claviron.orbit.stream`, over the fleet's stream table.
pub struct StreamExtension {
    streams: Arc<Streams>,
}

impl StreamExtension {
    pub fn new(streams: Arc<Streams>) -> Self {
        Self { streams }
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("stream: {error}"))
}

impl Extension for StreamExtension {
    fn name(&self) -> &'static str {
        "claviron.orbit.stream"
    }

    fn global(&self) -> Option<&'static str> {
        Some("stream")
    }

    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let streams = Arc::clone(&self.streams);
        let opening = Arc::clone(execution);
        module.set(
            "open",
            lua.create_function(move |_, ticket: String| {
                let ticket: Ticket = ticket.parse().map_err(failure)?;
                let endpoint = streams.open(ticket).map_err(failure)?;
                Ok(LuaStream::new(endpoint, None, &opening))
            })?,
        )?;
        let streams = Arc::clone(&self.streams);
        let execution = Arc::clone(execution);
        let meta = lua.create_table()?;
        meta.set(
            "__call",
            lua.create_function(move |_, _: Table| {
                let (endpoint, ticket) = streams.create().map_err(failure)?;
                Ok(LuaStream::new(endpoint, Some(ticket), &execution))
            })?,
        )?;
        module.set_metatable(Some(meta))?;
        Ok(module)
    }
}

/// One side of a stream as a script holds it. Dropped with the state, which
/// ends both directions for the far side.
struct LuaStream {
    id: String,
    /// The far side's, when this side created the stream.
    ticket: Option<String>,
    execution: Arc<Execution>,
    reader: Mutex<(ReadHalf, MessageReader)>,
    /// Gone once finished: a write after `finish` is an error, not a wait.
    writer: Mutex<Option<WriteHalf>>,
}

impl LuaStream {
    fn new(
        endpoint: Endpoint,
        ticket: Option<Ticket>,
        execution: &Arc<Execution>,
    ) -> Self {
        let id = endpoint.id().to_string();
        let (reader, writer) = endpoint.split();
        Self {
            id,
            ticket: ticket.map(|ticket| ticket.to_string()),
            execution: Arc::clone(execution),
            reader: Mutex::new((reader, MessageReader::new())),
            writer: Mutex::new(Some(writer)),
        }
    }

    /// Wait for `work`, or fail when the script is told to stop.
    async fn until_stopped<T>(
        &self,
        work: impl Future<Output = std::io::Result<T>>,
    ) -> mlua::Result<T> {
        tokio::select! {
            done = work => done.map_err(failure),
            () = self.execution.stopping().cancelled() => {
                Err(failure("the wait ended: this script is stopping"))
            }
        }
    }

    async fn write(
        &self,
        bytes: &[u8],
    ) -> mlua::Result<()> {
        let mut writer = self.writer.lock().await;
        let half = writer
            .as_mut()
            .ok_or_else(|| failure("this side has finished writing"))?;
        self.until_stopped(half.write_all(bytes)).await
    }

    async fn send(
        &self,
        bytes: Vec<u8>,
    ) -> mlua::Result<()> {
        let mut writer = self.writer.lock().await;
        let half = writer
            .as_mut()
            .ok_or_else(|| failure("this side has finished writing"))?;
        self.until_stopped(write_message(half, &Message::Binary(bytes))).await
    }

    async fn read(&self) -> mlua::Result<Option<Vec<u8>>> {
        let mut reader = self.reader.lock().await;
        let mut buffer = vec![0; READ_MAX];
        let read = self.until_stopped(reader.0.read(&mut buffer)).await?;
        if read == 0 {
            return Ok(None);
        }
        buffer.truncate(read);
        Ok(Some(buffer))
    }

    async fn receive(&self) -> mlua::Result<Option<Vec<u8>>> {
        let mut guard = self.reader.lock().await;
        let (half, messages) = &mut *guard;
        match self.until_stopped(messages.read(half)).await? {
            Some(Message::Binary(bytes)) => Ok(Some(bytes)),
            Some(Message::Text(text)) => Ok(Some(text.into_bytes())),
            Some(Message::Close { .. }) | None => Ok(None),
        }
    }

    async fn finish(&self) -> mlua::Result<()> {
        if let Some(mut half) = self.writer.lock().await.take() {
            half.finish().map_err(failure)?;
        }
        Ok(())
    }
}

impl UserData for LuaStream {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("id", |_, this, ()| Ok(this.id.clone()));
        methods.add_method("ticket", |_, this, ()| {
            this.ticket.clone().ok_or_else(|| {
                failure("only the side that created a stream holds its ticket")
            })
        });
        methods.add_async_method(
            "write",
            |_, this: UserDataRef<LuaStream>, bytes: mlua::LuaString| async move {
                this.write(&bytes.as_bytes()).await
            }
        );
        methods.add_async_method(
            "send",
            |_, this: UserDataRef<LuaStream>, bytes: mlua::LuaString| async move {
                this.send(bytes.as_bytes().to_vec()).await
            }
        );
        methods.add_async_method(
            "read",
            |lua, this: UserDataRef<LuaStream>, ()| async move {
                bytes_or_nil(&lua, this.read().await?)
            },
        );
        methods.add_async_method(
            "receive",
            |lua, this: UserDataRef<LuaStream>, ()| async move {
                bytes_or_nil(&lua, this.receive().await?)
            },
        );
        methods.add_async_method("finish", |_, this: UserDataRef<LuaStream>, ()| async move {
            this.finish().await
        });
        methods.add_async_method(
            "reset",
            |_, this: UserDataRef<LuaStream>, ()| async move {
                if let Some(mut half) = this.writer.lock().await.take() {
                    half.reset();
                }
                Ok(())
            },
        );
    }
}

fn bytes_or_nil(
    lua: &Lua,
    bytes: Option<Vec<u8>>,
) -> mlua::Result<Value> {
    match bytes {
        Some(bytes) => Ok(Value::String(lua.create_string(bytes)?)),
        None => Ok(Value::Nil),
    }
}

#[cfg(test)]
#[path = "../tests/stream.rs"]
mod tests;
