//! `claviron.invoke`: the fleet's invocations — work sent to an operation and
//! taken by whoever serves it, in any language, on any process.
//!
//! ```lua
//! local id = claviron.invoke.send("image.resize", { path = "a.png", width = 320 })
//!
//! claviron.invoke.serve("image.resize", function(payload, invocation)
//!   -- … the work; an outcome, if wanted, is an event keyed by invocation.id
//! end)
//!
//! local work = claviron.invoke.subscribe("report.build")
//! local invocation = work:next()   -- { id, operation, payload, submitted_at }
//! ```
//!
//! Sending waits for nothing. A subscription is its own cursor on the
//! operation's ring, open from `subscribe`: two subscribers both receive every
//! invocation, and claiming one is theirs. Waiting ends when an invocation
//! comes or when the execution is told to stop, never on a clock. Payloads
//! are the same event values `claviron.events` carries.

use std::sync::Arc;

use claviron_app::EventValue;
use claviron_orbit::{Invocation, InvocationBus};
use mlua::{
    Function, Lua, Table, UserData, UserDataMethods, UserDataRef, Value,
};
use tokio::sync::Mutex;

use crate::events::{from_event, to_event};
use crate::{Execution, Extension};

pub struct InvokeExtension {
    bus: Arc<InvocationBus>,
}

impl InvokeExtension {
    pub fn new(bus: Arc<InvocationBus>) -> Self {
        Self { bus }
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("invoke: {error}"))
}

fn operation(name: String) -> mlua::Result<String> {
    let name = name.trim().to_owned();
    if name.is_empty() {
        return Err(failure("an operation cannot be empty"));
    }
    Ok(name)
}

impl Extension for InvokeExtension {
    fn name(&self) -> &'static str {
        "claviron.invoke"
    }

    fn global(&self) -> Option<&'static str> {
        Some("invoke")
    }

    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let bus = Arc::clone(&self.bus);
        module.set(
            "send",
            lua.create_function(move |_, (name, payload): (String, Value)| {
                let payload =
                    to_event(payload, 0)?.to_bytes().map_err(failure)?;
                bus.submit(&operation(name)?, &payload)
                    .map(|id| id.to_string())
                    .map_err(failure)
            })?,
        )?;
        let subscribe = {
            let bus = Arc::clone(&self.bus);
            let execution = Arc::clone(execution);
            move |name: String| -> mlua::Result<Subscription> {
                let name = operation(name)?;
                let cursor = bus.subscribe(name.clone()).map_err(failure)?;
                Ok(Subscription {
                    operation: name,
                    cursor: Arc::new(Mutex::new(cursor)),
                    execution: Arc::clone(&execution),
                })
            }
        };
        let subscribing = subscribe.clone();
        module.set(
            "subscribe",
            lua.create_function(move |_, name: String| subscribing(name))?,
        )?;
        module.set(
            "serve",
            lua.create_async_function(move |lua, (name, handler): (String, Function)| {
                let subscription = subscribe(name);
                async move {
                    let subscription = subscription?;
                    loop {
                        let invocation = match subscription.receive(&lua).await {
                            Ok(invocation) => invocation,
                            Err(Missed::Stopping) => return Ok(()),
                            Err(Missed::Lagged(missed)) => {
                                tracing::warn!(
                                    operation = %subscription.operation,
                                    missed,
                                    "Lua serve fell behind the invocation ring"
                                );
                                continue;
                            }
                            Err(Missed::Error(error)) => return Err(error)
                        };
                        let payload: Value = invocation.get("payload")?;
                        handler.call_async::<()>((payload, invocation)).await?;
                    }
                }
            })?
        )?;
        Ok(module)
    }
}

/// A cursor on one operation's ring, open until it is dropped.
struct Subscription {
    operation: String,
    cursor: Arc<Mutex<claviron_orbit::invoke::InvocationSubscription>>,
    execution: Arc<Execution>,
}

enum Missed {
    Stopping,
    Lagged(u64),
    Error(mlua::Error),
}

impl Subscription {
    async fn receive(
        &self,
        lua: &Lua,
    ) -> Result<Table, Missed> {
        let mut cursor = self.cursor.lock().await;
        let received = tokio::select! {
            received = cursor.receive() => received,
            () = self.execution.stopping().cancelled() => return Err(Missed::Stopping)
        };
        match received {
            Ok(invocation) => {
                invocation_table(lua, &invocation).map_err(Missed::Error)
            }
            Err(claviron_orbit::invoke::Error::Lagged(missed)) => {
                Err(Missed::Lagged(missed))
            }
            Err(error) => Err(Missed::Error(failure(error))),
        }
    }
}

fn invocation_table(
    lua: &Lua,
    invocation: &Invocation,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("id", invocation.id.to_string())?;
    table.set("operation", invocation.operation.as_str())?;
    // The ring carries event values; bytes in another codec come through as
    // they are, a Lua string holding them.
    let payload = match EventValue::from_bytes(&invocation.payload) {
        Ok(value) => from_event(lua, &value)?,
        Err(_) => Value::String(lua.create_string(&invocation.payload)?),
    };
    table.set("payload", payload)?;
    table.set("submitted_at", invocation.submitted_at.as_unix_ms())?;
    Ok(table)
}

impl UserData for Subscription {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method(
            "next",
            |lua, this: UserDataRef<Subscription>, ()| async move {
                match this.receive(&lua).await {
                    Ok(invocation) => Ok(invocation),
                    Err(Missed::Stopping) => {
                        Err(failure("the wait ended: this script is stopping"))
                    }
                    // Said, not skipped; the subscription stays usable.
                    Err(Missed::Lagged(missed)) => Err(failure(format!(
                        "fell behind: {missed} invocations were missed"
                    ))),
                    Err(Missed::Error(error)) => Err(error),
                }
            },
        );
        methods
            .add_method("operation", |_, this, ()| Ok(this.operation.clone()));
    }
}

#[cfg(test)]
#[path = "../tests/invoke.rs"]
mod tests;
