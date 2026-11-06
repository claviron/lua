//! `claviron.regex`: regular expressions from Rust's `regex` crate, over
//! bytes, as Lua strings are.
//!
//! ```lua
//! local date = claviron.regex("(?<year>\\d{4})-(\\d{2})")
//! date:is_match("2026-09")                  --> true
//! date:find("on 2026-09")                   --> 4, 10   (1-based, inclusive)
//! local m = date:match("2026-09")           --> m[0] = whole, m[1], m[2], m.year
//! for m in date:gmatch(text) do … end
//! date:replace("2026-09", "$2/$year")       --> "09/2026"
//! claviron.regex(",\\s*"):split("a, b,c")   --> { "a", "b", "c" }
//! ```
//!
//! The engine matches in linear time and never backtracks, so a pattern
//! cannot hang a script however it is written.

use std::sync::Arc;

use mlua::{Lua, MetaMethod, Table, UserData, UserDataMethods, Value};
use regex::bytes::{Captures, Regex};

use crate::{Execution, Extension};

pub struct RegexExtension;

impl Extension for RegexExtension {
    fn name(&self) -> &'static str {
        "claviron.regex"
    }

    fn module(
        &self,
        lua: &Lua,
        _: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let compile = |pattern: String| {
            Regex::new(&pattern).map(LuaRegex).map_err(|error| {
                mlua::Error::runtime(format!("regex: {error}"))
            })
        };
        module.set(
            "new",
            lua.create_function(move |_, pattern: String| compile(pattern))?,
        )?;
        let meta = lua.create_table()?;
        meta.set(
            "__call",
            lua.create_function(move |_, (_, pattern): (Table, String)| {
                compile(pattern)
            })?,
        )?;
        module.set_metatable(Some(meta))?;
        Ok(module)
    }

    fn global(&self) -> Option<&'static str> {
        Some("regex")
    }
}

struct LuaRegex(Regex);

/// A 1-based position, as Lua counts, into a 0-based byte offset.
fn offset(
    init: Option<i64>,
    length: usize,
) -> usize {
    match init {
        None => 0,
        Some(init) if init > 0 => ((init - 1) as usize).min(length),
        // Lua's negative positions count from the end.
        Some(init) => length.saturating_sub(init.unsigned_abs() as usize),
    }
}

fn captures(
    lua: &Lua,
    regex: &Regex,
    found: &Captures<'_>,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    for (index, group) in found.iter().enumerate() {
        if let Some(group) = group {
            table.set(index, lua.create_string(group.as_bytes())?)?;
        }
    }
    for name in regex.capture_names().flatten() {
        if let Some(group) = found.name(name) {
            table.set(name, lua.create_string(group.as_bytes())?)?;
        }
    }
    Ok(table)
}

impl UserData for LuaRegex {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("is_match", |_, this, text: mlua::LuaString| {
            Ok(this.0.is_match(&text.as_bytes()))
        });
        methods.add_method(
            "find",
            |_, this, (text, init): (mlua::LuaString, Option<i64>)| {
                let bytes = text.as_bytes();
                let start = offset(init, bytes.len());
                Ok(match this.0.find_at(&bytes, start) {
                    Some(found) => (
                        Some(found.start() as i64 + 1),
                        Some(found.end() as i64),
                    ),
                    None => (None, None),
                })
            },
        );
        methods.add_method(
            "match",
            |lua, this, (text, init): (mlua::LuaString, Option<i64>)| {
                let bytes = text.as_bytes();
                let start = offset(init, bytes.len());
                match this.0.captures_at(&bytes, start) {
                    Some(found) => {
                        Ok(Value::Table(captures(lua, &this.0, &found)?))
                    }
                    None => Ok(Value::Nil),
                }
            },
        );
        methods.add_method("gmatch", |lua, this, text: mlua::LuaString| {
            let bytes = text.as_bytes().to_vec();
            let regex = this.0.clone();
            let mut position = 0;
            lua.create_function_mut(move |lua, ()| {
                if position > bytes.len() {
                    return Ok(Value::Nil);
                }
                let Some(found) = regex.captures_at(&bytes, position) else {
                    position = bytes.len() + 1;
                    return Ok(Value::Nil);
                };
                let whole = found.get(0).expect("group 0 is the match");
                // An empty match moves on by one, or it would be found again.
                position = if whole.is_empty() {
                    whole.end() + 1
                } else {
                    whole.end()
                };
                Ok(Value::Table(captures(lua, &regex, &found)?))
            })
        });
        methods.add_method(
            "replace",
            |lua,
             this,
             (text, replacement, limit): (
                mlua::LuaString,
                mlua::LuaString,
                Option<usize>,
            )| {
                let text = text.as_bytes();
                let replacement = replacement.as_bytes();
                let replaced =
                    this.0.replacen(&text, limit.unwrap_or(0), &*replacement);
                lua.create_string(replaced.as_ref())
            },
        );
        methods.add_method("split", |lua, this, text: mlua::LuaString| {
            let table = lua.create_table()?;
            for (index, part) in this.0.split(&text.as_bytes()).enumerate() {
                table.set(index + 1, lua.create_string(part)?)?;
            }
            Ok(table)
        });
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(this.0.as_str().to_owned())
        });
    }
}
