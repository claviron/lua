//! `claviron.log`: records to the log channels `logging.yaml` declares —
//! the fleet's one writer formats and writes them, the way it does PHP's,
//! Python's and JavaScript's.
//!
//! ```lua
//! local log = claviron.log.channel("app")              -- refused if undeclared
//! log:warn("order failed", { order = 7, customer = { id = 3 } })
//! local billing = claviron.log.channel("app", { target = "billing" })
//! ```
//!
//! A record's target is the host's name for the script (a task's name) unless
//! the channel names one. Context tables become fields: nested tables as
//! dotted keys (`customer.id`), numbers and booleans typed. A record goes into
//! this process's lane of the log ring and the call returns.

use std::sync::Arc;

use claviron_logging::pipeline::{
    ChannelId, Level, LogChannels, Record, Value as Field,
};
use mlua::{Lua, Table, UserData, UserDataMethods, Value};

use crate::{Execution, Extension};

pub struct LogExtension {
    channels: Option<Arc<LogChannels>>,
    target: Arc<str>,
}

impl LogExtension {
    /// `channels` is `None` when this process has no log pipeline; every
    /// channel then reads as undeclared. `target` names the script's records.
    pub fn new(
        channels: Option<Arc<LogChannels>>,
        target: impl Into<Arc<str>>,
    ) -> Self {
        Self { channels, target: target.into() }
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("log: {error}"))
}

impl Extension for LogExtension {
    fn name(&self) -> &'static str {
        "claviron.log"
    }

    fn global(&self) -> Option<&'static str> {
        Some("log")
    }

    fn module(
        &self,
        lua: &Lua,
        _: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let channels = self.channels.clone();
        let default_target = Arc::clone(&self.target);
        module.set(
            "channel",
            lua.create_function(
                move |_, (name, options): (String, Option<Table>)| {
                    let undeclared = || {
                        failure(format!(
                            "channel \"{name}\" is not declared in logging.yaml"
                        ))
                    };
                    let channels = channels.clone().ok_or_else(undeclared)?;
                    let id = channels.channel(&name).ok_or_else(undeclared)?;
                    let target = match options {
                        Some(options) => {
                            options.get::<Option<String>>("target")?
                        }
                        None => None,
                    };
                    Ok(Channel {
                        channels,
                        id,
                        target: target
                            .map(Arc::from)
                            .unwrap_or_else(|| Arc::clone(&default_target)),
                    })
                },
            )?,
        )?;
        Ok(module)
    }
}

struct Channel {
    channels: Arc<LogChannels>,
    id: ChannelId,
    target: Arc<str>,
}

impl Channel {
    fn write(
        &self,
        level: Level,
        message: Value,
        context: Option<Table>,
    ) -> mlua::Result<bool> {
        let message = message.to_string()?;
        let mut owned: Vec<(String, Owned)> = Vec::new();
        if let Some(context) = context {
            flatten(&context, "", &mut owned, 0)?;
        }
        let fields: Vec<(&str, Field<'_>)> = owned
            .iter()
            .map(|(key, value)| (key.as_str(), value.borrow()))
            .collect();
        let record = Record {
            level,
            target: &self.target,
            message: &message,
            fields: &fields,
        };
        Ok(self.channels.emit(self.id, &record).is_ok())
    }
}

impl UserData for Channel {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        for (name, level) in [
            ("trace", Level::Trace),
            ("debug", Level::Debug),
            ("info", Level::Info),
            ("warn", Level::Warn),
            ("error", Level::Error),
        ] {
            methods.add_method(
                name,
                move |_, this, (message, context): (Value, Option<Table>)| {
                    this.write(level, message, context)
                },
            );
        }
    }
}

enum Owned {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl Owned {
    fn borrow(&self) -> Field<'_> {
        match self {
            Self::Str(text) => Field::Str(text),
            Self::I64(number) => Field::I64(*number),
            Self::F64(number) => Field::F64(*number),
            Self::Bool(flag) => Field::Bool(*flag),
        }
    }
}

/// How deep context may nest before the rest is written as text.
const MAX_DEPTH: usize = 16;

fn flatten(
    table: &Table,
    prefix: &str,
    out: &mut Vec<(String, Owned)>,
    depth: usize,
) -> mlua::Result<()> {
    for pair in table.pairs::<Value, Value>() {
        let (key, value) = pair?;
        let key = match key {
            Value::String(key) => key.to_str()?.to_owned(),
            Value::Integer(index) => index.to_string(),
            other => other.to_string()?,
        };
        let name =
            if prefix.is_empty() { key } else { format!("{prefix}.{key}") };
        match value {
            Value::Table(nested) if depth < MAX_DEPTH => {
                flatten(&nested, &name, out, depth + 1)?
            }
            Value::Integer(number) => out.push((name, Owned::I64(number))),
            Value::Number(number) => out.push((name, Owned::F64(number))),
            Value::Boolean(flag) => out.push((name, Owned::Bool(flag))),
            Value::String(text) => {
                out.push((name, Owned::Str(text.to_string_lossy())))
            }
            Value::Nil => {}
            other => out.push((name, Owned::Str(other.to_string()?))),
        }
    }
    Ok(())
}
