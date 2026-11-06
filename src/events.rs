//! `claviron.events`: the fleet's application events, the ones PHP, Python
//! and JavaScript publish and receive.
//!
//! ```lua
//! claviron.events.publish("order.paid", { id = 7 })                 -- to everyone
//! claviron.events.publish("reindex", {}, { to = "php:octane-app" }) -- to one application
//!
//! local paid = claviron.events.listen({ "order.paid", "order.refunded" })
//! local event = paid:next()        -- { id, topic, payload }; parks until one comes
//! local once = claviron.events.wait("job.done")
//! ```
//!
//! A listener is subscribed when `listen` returns, so a script that starts
//! work and then waits for its answer cannot miss it. A wait ends when an
//! event comes, or when the execution is told to stop — never on a clock.
//! A script with no application identity of its own receives the events sent
//! to everyone; one sent to an application is that application's.
//!
//! Payloads cross languages as Claviron's event values: a Lua sequence is a
//! list with keys 0, 1, … as PHP and JavaScript see one, and comes back a
//! sequence; strings are bytes, binary included; `claviron.json.null` is null.

use std::sync::Arc;

use claviron_app::{
    ApplicationEvent, ApplicationId, ApplicationName, ApplicationRuntime,
    EventArrayKey, EventValue,
};
use claviron_runtime::events::ProcessEventBus;
use mlua::{
    Lua, LuaSerdeExt, Table, UserData, UserDataMethods, UserDataRef, Value,
};
use tokio::sync::{Mutex, broadcast};

use crate::{Execution, Extension};

pub struct EventsExtension {
    bus: ProcessEventBus,
}

impl EventsExtension {
    pub fn new(bus: ProcessEventBus) -> Self {
        Self { bus }
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("events: {error}"))
}

impl Extension for EventsExtension {
    fn name(&self) -> &'static str {
        "claviron.events"
    }

    fn global(&self) -> Option<&'static str> {
        Some("events")
    }

    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let bus = self.bus.clone();
        module.set(
            "publish",
            lua.create_function(
                move |_,
                      (topic, payload, options): (
                    String,
                    Value,
                    Option<Table>,
                )| {
                    let payload = to_event(payload, 0)?;
                    let target = match options {
                        Some(options) => options.get::<Option<String>>("to")?,
                        None => None,
                    };
                    let event = match target {
                        Some(target) => ApplicationEvent::for_application(
                            application(&target)?,
                            topic,
                            payload,
                        ),
                        None => ApplicationEvent::new(topic, payload),
                    }
                    .map_err(failure)?;
                    bus.emit(event);
                    Ok(())
                },
            )?,
        )?;
        let listen = {
            let bus = self.bus.clone();
            let execution = Arc::clone(execution);
            move |topics: Value| -> mlua::Result<Listener> {
                let topics = match topics {
                    Value::String(topic) => {
                        vec![topic.to_str()?.trim().to_owned()]
                    }
                    Value::Table(topics) => topics
                        .sequence_values::<String>()
                        .map(|topic| topic.map(|topic| topic.trim().to_owned()))
                        .collect::<mlua::Result<_>>()?,
                    other => {
                        return Err(failure(format!(
                            "listen takes a topic or a list of them, not {}",
                            other.type_name()
                        )));
                    }
                };
                if topics.is_empty() || topics.iter().any(String::is_empty) {
                    return Err(failure("an event topic cannot be empty"));
                }
                Ok(Listener::subscribe(&bus, topics, Arc::clone(&execution)))
            }
        };
        let listening = listen.clone();
        module.set(
            "listen",
            lua.create_function(move |_, topics: Value| listening(topics))?,
        )?;
        module.set(
            "wait",
            lua.create_async_function(move |lua, topic: Value| {
                let listener = listen(topic);
                async move { listener?.next(&lua).await }
            })?,
        )?;
        Ok(module)
    }
}

/// `runtime:name`, as PHP and JavaScript name an application.
fn application(target: &str) -> mlua::Result<ApplicationId> {
    let (runtime, name) = target.split_once(':').ok_or_else(|| {
        failure(format!("'{target}' is not runtime:name, as php:octane-app"))
    })?;
    Ok(ApplicationId::new(
        ApplicationRuntime::new(runtime).map_err(failure)?,
        ApplicationName::new(name).map_err(failure)?,
    ))
}

/// A subscription, open from `listen` until the listener is dropped.
#[derive(Clone)]
pub(crate) struct Listener {
    topics: Vec<String>,
    receiver: Arc<Mutex<broadcast::Receiver<ApplicationEvent>>>,
    execution: Arc<Execution>,
}

impl Listener {
    pub(crate) fn subscribe(
        bus: &ProcessEventBus,
        topics: Vec<String>,
        execution: Arc<Execution>,
    ) -> Self {
        Self {
            topics,
            receiver: Arc::new(Mutex::new(bus.subscribe::<ApplicationEvent>())),
            execution,
        }
    }

    pub(crate) async fn receive(&self) -> mlua::Result<ApplicationEvent> {
        let mut receiver = self.receiver.lock().await;
        loop {
            let received = tokio::select! {
                received = receiver.recv() => received,
                () = self.execution.stopping().cancelled() => {
                    return Err(failure("the wait ended: this script is stopping"));
                }
            };
            match received {
                Ok(event)
                    if event.target().is_none()
                        && self
                            .topics
                            .iter()
                            .any(|topic| topic == event.topic().as_str()) =>
                {
                    return Ok(event);
                }
                Ok(_) => continue,
                // Said, not skipped: events were lost, and the script decides
                // what that means. The listener stays usable.
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    return Err(failure(format!(
                        "fell behind: {missed} events were missed"
                    )));
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(failure("the event bus closed"));
                }
            }
        }
    }

    async fn next(
        &self,
        lua: &Lua,
    ) -> mlua::Result<Table> {
        event_table(lua, self.receive().await?)
    }
}

pub(crate) fn event_table(
    lua: &Lua,
    event: ApplicationEvent,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("id", event.id().to_string())?;
    table.set("topic", event.topic().as_str())?;
    table.set("payload", from_event(lua, event.payload())?)?;
    Ok(table)
}

impl UserData for Listener {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method(
            "next",
            |lua, this: UserDataRef<Listener>, ()| async move {
                this.next(&lua).await
            },
        );
        methods.add_method("topics", |_, this, ()| Ok(this.topics.clone()));
    }
}

/// How deep a payload may nest; the event format bounds it the same way.
const MAX_DEPTH: usize = 64;

pub(crate) fn to_event(
    value: Value,
    depth: usize,
) -> mlua::Result<EventValue> {
    if depth > MAX_DEPTH {
        return Err(failure(
            "the payload nests too deep (or refers to itself)",
        ));
    }
    Ok(match value {
        Value::Nil => EventValue::Null,
        Value::LightUserData(pointer) if pointer.0.is_null() => {
            EventValue::Null
        }
        Value::Boolean(value) => EventValue::Bool(value),
        Value::Integer(value) => EventValue::Int(value),
        Value::Number(value) => EventValue::Float(value),
        Value::String(value) => EventValue::String(value.as_bytes().to_vec()),
        Value::Table(table) => {
            let length = table.raw_len();
            let mut entries = Vec::new();
            let mut sequence = true;
            let mut count = 0usize;
            for pair in table.pairs::<Value, Value>() {
                let (key, value) = pair?;
                count += 1;
                let key = match key {
                    Value::Integer(index) => {
                        if !(1..=length as i64).contains(&index) {
                            sequence = false;
                        }
                        EventArrayKey::Int(index)
                    }
                    Value::String(key) => {
                        sequence = false;
                        EventArrayKey::String(key.to_str()?.to_owned())
                    }
                    other => {
                        return Err(failure(format!(
                            "a payload key is a string or an integer, not {}",
                            other.type_name()
                        )));
                    }
                };
                entries.push((key, to_event(value, depth + 1)?));
            }
            // A Lua sequence travels as a list, keyed from 0 as PHP and
            // JavaScript key one.
            if sequence && count == length {
                entries.sort_by_key(|(key, _)| match key {
                    EventArrayKey::Int(index) => *index,
                    EventArrayKey::String(_) => i64::MAX,
                });
                for (key, _) in &mut entries {
                    if let EventArrayKey::Int(index) = key {
                        *index -= 1;
                    }
                }
            }
            EventValue::Array(entries)
        }
        other => {
            return Err(failure(format!(
                "an event cannot carry {}",
                other.type_name()
            )));
        }
    })
}

pub(crate) fn from_event(
    lua: &Lua,
    value: &EventValue,
) -> mlua::Result<Value> {
    Ok(match value {
        EventValue::Null => lua.null(),
        EventValue::Bool(value) => Value::Boolean(*value),
        EventValue::Int(value) => Value::Integer(*value),
        EventValue::Float(value) => Value::Number(*value),
        EventValue::String(value) => Value::String(lua.create_string(value)?),
        EventValue::Array(entries) => {
            let table = lua.create_table()?;
            // A list keyed 0, 1, … is a Lua sequence from 1.
            let list = entries.iter().enumerate().all(
                |(index, (key, _))| matches!(key, EventArrayKey::Int(key) if *key == index as i64)
            );
            for (key, value) in entries {
                let value = from_event(lua, value)?;
                match key {
                    EventArrayKey::Int(index) if list => {
                        table.raw_set(index + 1, value)?
                    }
                    EventArrayKey::Int(index) => {
                        table.raw_set(*index, value)?
                    }
                    EventArrayKey::String(key) => {
                        table.raw_set(key.as_str(), value)?
                    }
                }
            }
            Value::Table(table)
        }
    })
}

/// Carry application events across the fleet, as the other engines do.
/// Idempotent; a process outside a fleet has no bridge and keeps its events.
pub fn register(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    claviron_orbit::register(state)?;
    if let Some(events) = state
        .registry_ref()
        .resolve::<claviron_orbit::ApplicationEventRegistry>()
    {
        events.register_bridge::<ApplicationEvent>(state)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/events.rs"]
mod tests;
