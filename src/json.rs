//! `claviron.json`: JSON in and out, through `serde_json`.
//!
//! ```lua
//! local text = claviron.json.encode({ id = 7, tags = claviron.json.array({}) })
//! local value = claviron.json.decode(text)
//! if value.missing == claviron.json.null then … end
//! ```
//!
//! Lua cannot hold `nil` in a table, so JSON's `null` is `claviron.json.null`;
//! and a Lua table does not say whether it is a list, so an empty one encodes
//! as an object unless `claviron.json.array` marked it. Decoded arrays carry
//! that mark, so they encode back as arrays, empty or not.

use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Table, Value};

use crate::{Execution, Extension};

pub struct JsonExtension;

impl Extension for JsonExtension {
    fn name(&self) -> &'static str {
        "claviron.json"
    }

    fn module(
        &self,
        lua: &Lua,
        _: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        module.set(
            "encode",
            lua.create_function(
                |lua, (value, options): (Value, Option<Table>)| {
                    let pretty = match options {
                        Some(options) => options
                            .get::<Option<bool>>("pretty")?
                            .unwrap_or(false),
                        None => false,
                    };
                    let json: serde_json::Value = lua.from_value(value)?;
                    let text = if pretty {
                        serde_json::to_string_pretty(&json)
                    } else {
                        serde_json::to_string(&json)
                    };
                    text.map_err(|error| {
                        mlua::Error::runtime(format!("json.encode: {error}"))
                    })
                },
            )?,
        )?;
        module.set(
            "decode",
            lua.create_function(|lua, text: mlua::LuaString| {
                let json: serde_json::Value = serde_json::from_slice(
                    &text.as_bytes(),
                )
                .map_err(|error| {
                    mlua::Error::runtime(format!("json.decode: {error}"))
                })?;
                lua.to_value(&json)
            })?,
        )?;
        module.set("null", lua.null())?;
        module.set(
            "array",
            lua.create_function(|lua, table: Option<Table>| {
                let table = match table {
                    Some(table) => table,
                    None => lua.create_table()?,
                };
                table.set_metatable(Some(lua.array_metatable()))?;
                Ok(table)
            })?,
        )?;
        Ok(module)
    }

    fn global(&self) -> Option<&'static str> {
        Some("json")
    }
}
