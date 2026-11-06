//! The fleet's shared state as Lua modules.
//!
//! `claviron.orbit.cell` is the first: a typed fleet cell as a Lua value, the
//! counterpart of PHP's `Claviron\Orbital` and JavaScript's
//! `claviron:orbit/cell`.
//!
//! ```lua
//! local progress = claviron.cell(0)          -- an int cell: its type is 0's
//! progress:add(1)
//! local same = claviron.cell.open(progress:id())
//! progress:wait(progress:version())
//! ```
//!
//! A cell is a handle, not a copy: it holds the id, and every read and write
//! goes to the fleet's shared memory.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use claviron_orbit::cell::{CellId, Claim, Orbital, TextId};
use claviron_orbit::{Cells, Text};
use mlua::{
    Lua, MetaMethod, Table, UserData, UserDataMethods, UserDataRef, Value,
};

use crate::{Execution, Extension};

/// `claviron.orbit.cell`, over the fleet's cell tables.
pub struct CellExtension {
    cells: Arc<Cells>,
}

impl CellExtension {
    pub fn new(cells: Arc<Cells>) -> Self {
        Self { cells }
    }
}

impl Extension for CellExtension {
    fn name(&self) -> &'static str {
        "claviron.orbit.cell"
    }

    fn global(&self) -> Option<&'static str> {
        Some("cell")
    }

    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        for kind in ["int", "float", "bool", "string"] {
            let cells = Arc::clone(&self.cells);
            let execution = Arc::clone(execution);
            module.set(
                kind,
                lua.create_function(move |_, value: Value| {
                    let cell = Cell::allocate(&cells, kind, value)?;
                    Ok(LuaCell {
                        cells: Arc::clone(&cells),
                        execution: Arc::clone(&execution),
                        id: cell.id(),
                    })
                })?,
            )?;
        }
        let cells = Arc::clone(&self.cells);
        let opening = Arc::clone(execution);
        module.set(
            "open",
            lua.create_function(move |_, id: String| {
                Cell::open(&cells, &id)?;
                Ok(LuaCell {
                    cells: Arc::clone(&cells),
                    execution: Arc::clone(&opening),
                    id,
                })
            })?,
        )?;
        // `claviron.cell(value)`: the cell's type is its first value's.
        let cells = Arc::clone(&self.cells);
        let execution = Arc::clone(execution);
        let meta = lua.create_table()?;
        meta.set(
            "__call",
            lua.create_function(move |_, (_, value): (Table, Value)| {
                let kind = match &value {
                    Value::Integer(_) => "int",
                    Value::Number(_) => "float",
                    Value::Boolean(_) => "bool",
                    Value::String(_) => "string",
                    Value::Nil => {
                        return Err(failure("a cell starts with a value; its type is the value's"));
                    }
                    other => {
                        return Err(failure(format!("a cell does not hold {}", other.type_name())));
                    }
                };
                let cell = Cell::allocate(&cells, kind, value)?;
                Ok(LuaCell {
                    cells: Arc::clone(&cells),
                    execution: Arc::clone(&execution),
                    id: cell.id()
                })
            })?
        )?;
        module.set_metatable(Some(meta))?;
        Ok(module)
    }
}

/// A cell as a script holds it: the id and nothing else. Every operation
/// opens the cell by id, so a released cell fails as stale in every state
/// that still holds it.
struct LuaCell {
    cells: Arc<Cells>,
    execution: Arc<Execution>,
    id: String,
}

impl LuaCell {
    fn cell(&self) -> mlua::Result<Cell> {
        Cell::open(&self.cells, &self.id)
    }
}

impl UserData for LuaCell {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("id", |_, this, ()| Ok(this.id.clone()));
        methods.add_method("kind", |_, this, ()| Ok(this.cell()?.kind()));
        methods.add_method("get", |lua, this, ()| this.cell()?.load(lua));
        methods.add_method("set", |_, this, value: Value| {
            this.cell()?.store(value)
        });
        methods.add_method("add", |lua, this, by: Value| {
            this.cell()?.add(lua, by)
        });
        methods.add_method("add_until", |_, this, (by, limit): (i64, i64)| {
            this.cell()?.add_until(by, limit)
        });
        methods.add_method("append", |_, this, text: String| {
            this.cell()?.append(&text)
        });
        methods.add_method("version", |_, this, ()| this.cell()?.version());
        methods.add_method("release", |_, this, ()| this.cell()?.release());
        // Parks the coroutine, not a thread of the host's: a blocking-pool
        // thread waits on the cell's change count until something writes it,
        // or until the execution is told to stop, which sets the word the
        // wait also watches and then wakes the cell. Nothing here measures
        // time.
        methods.add_async_method(
            "wait",
            |_, this: UserDataRef<LuaCell>, since: i64| {
                let cells = Arc::clone(&this.cells);
                let execution = Arc::clone(&this.execution);
                let id = this.id.clone();
                async move {
                    let wake_cells = Arc::clone(&cells);
                    let wake_id = id.clone();
                    let _parked = execution.park(move || {
                        if let Ok(cell) = Cell::open(&wake_cells, &wake_id) {
                            let _ = cell.wake_waiters();
                        }
                    });
                    let waiting = Arc::clone(&execution);
                    let outcome = tokio::task::spawn_blocking(move || {
                        Cell::open(&cells, &id)?
                            .wait_changed_until(since, waiting.interrupted())
                    })
                    .await
                    .map_err(|error| {
                        failure(format!("wait interrupted: {error}"))
                    })??;
                    outcome.ok_or_else(|| {
                        failure("cell wait ended: this script is stopping")
                    })
                }
            },
        );
        // Comparisons and `tostring` read the cell. Arithmetic is left out:
        // `cell + 1` would read like an update and not be one.
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(match this.cell()?.load_text()? {
                Some(text) => text,
                None => this.id.clone(),
            })
        });
        // Lua calls `__eq` only between two userdata, so `==` compares two
        // cells' values; against a plain value it is `false`, as it is for any
        // userdata — `cell:get() == 3` is the spelling there.
        methods.add_meta_method(
            MetaMethod::Eq,
            |lua, this, other: UserDataRef<LuaCell>| {
                Ok(this.cell()?.load(lua)? == other.cell()?.load(lua)?)
            },
        );
        // `n > 3` is `3 < n` to Lua, so either side may be the cell.
        methods.add_meta_function(
            MetaMethod::Lt,
            |lua, (left, right): (Value, Value)| {
                Ok(number(lua, left)? < number(lua, right)?)
            },
        );
        methods.add_meta_function(
            MetaMethod::Le,
            |lua, (left, right): (Value, Value)| {
                Ok(number(lua, left)? <= number(lua, right)?)
            },
        );
    }
}

/// A side of a comparison as a number: a cell's value, or the plain value.
fn number(
    lua: &Lua,
    value: Value,
) -> mlua::Result<f64> {
    let value = match value {
        Value::UserData(cell) => cell.borrow::<LuaCell>()?.cell()?.load(lua)?,
        value => value,
    };
    match value {
        Value::Integer(value) => Ok(value as f64),
        Value::Number(value) => Ok(value),
        _ => Err(failure("only an int or a float cell compares")),
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("Orbital: {error}"))
}

enum Cell {
    Int(Orbital<i64>),
    Float(Orbital<f64>),
    Bool(Orbital<bool>),
    Text(Text),
}

impl Cell {
    fn allocate(
        cells: &Cells,
        kind: &str,
        value: Value,
    ) -> mlua::Result<Self> {
        Ok(match (kind, value) {
            ("int", Value::Integer(value)) => {
                Self::Int(cells.allocate(value).map_err(failure)?)
            }
            ("float", Value::Number(value)) => {
                Self::Float(cells.allocate(value).map_err(failure)?)
            }
            ("float", Value::Integer(value)) => {
                Self::Float(cells.allocate(value as f64).map_err(failure)?)
            }
            ("bool", Value::Boolean(value)) => {
                Self::Bool(cells.allocate(value).map_err(failure)?)
            }
            ("string", Value::String(value)) => Self::Text(
                cells.allocate_text(&value.to_str()?).map_err(failure)?,
            ),
            (kind, value) => {
                return Err(failure(format!(
                    "a {kind} cell does not take {}",
                    value.type_name()
                )));
            }
        })
    }

    fn open(
        cells: &Cells,
        id: &str,
    ) -> mlua::Result<Self> {
        if let Some(rest) = id.strip_prefix("cell:") {
            // The scalar id does not say which type; the cell does.
            let cell_id: CellId =
                format!("cell:{rest}").parse().map_err(failure)?;
            return cells
                .open::<i64>(cell_id)
                .map(Self::Int)
                .or_else(|_| cells.open::<f64>(cell_id).map(Self::Float))
                .or_else(|_| cells.open::<bool>(cell_id).map(Self::Bool))
                .map_err(failure);
        }
        let text_id: TextId = id.parse().map_err(failure)?;
        cells.open_text(text_id).map(Self::Text).map_err(failure)
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Bool(_) => "bool",
            Self::Text(_) => "string",
        }
    }

    fn id(&self) -> String {
        match self {
            Self::Int(cell) => cell.id().to_string(),
            Self::Float(cell) => cell.id().to_string(),
            Self::Bool(cell) => cell.id().to_string(),
            Self::Text(cell) => cell.id().to_string(),
        }
    }

    fn load(
        &self,
        lua: &Lua,
    ) -> mlua::Result<Value> {
        Ok(match self {
            Self::Int(cell) => Value::Integer(cell.load().map_err(failure)?),
            Self::Float(cell) => Value::Number(cell.load().map_err(failure)?),
            Self::Bool(cell) => Value::Boolean(cell.load().map_err(failure)?),
            Self::Text(cell) => {
                Value::String(lua.create_string(cell.load().map_err(failure)?)?)
            }
        })
    }

    /// The value as `tostring` shows it; `None` never, kept total.
    fn load_text(&self) -> mlua::Result<Option<String>> {
        Ok(Some(match self {
            Self::Int(cell) => cell.load().map_err(failure)?.to_string(),
            Self::Float(cell) => cell.load().map_err(failure)?.to_string(),
            Self::Bool(cell) => cell.load().map_err(failure)?.to_string(),
            Self::Text(cell) => cell.load().map_err(failure)?,
        }))
    }

    fn store(
        &self,
        value: Value,
    ) -> mlua::Result<()> {
        match (self, value) {
            (Self::Int(cell), Value::Integer(value)) => cell.store(value),
            (Self::Float(cell), Value::Number(value)) => cell.store(value),
            (Self::Float(cell), Value::Integer(value)) => {
                cell.store(value as f64)
            }
            (Self::Bool(cell), Value::Boolean(value)) => cell.store(value),
            (Self::Text(cell), Value::String(value)) => {
                cell.store(&value.to_str()?)
            }
            (cell, value) => {
                return Err(failure(format!(
                    "a {} cell does not take {}",
                    cell.kind(),
                    value.type_name()
                )));
            }
        }
        .map_err(failure)
    }

    /// Atomic on the cell; the value after comes back.
    fn add(
        &self,
        lua: &Lua,
        by: Value,
    ) -> mlua::Result<Value> {
        let _ = lua;
        match (self, by) {
            (Self::Int(cell), Value::Integer(by)) => {
                cell.fetch_add(by).map(Value::Integer).map_err(failure)
            }
            (Self::Float(cell), Value::Number(by)) => {
                cell.fetch_add(by).map(Value::Number).map_err(failure)
            }
            (Self::Float(cell), Value::Integer(by)) => {
                cell.fetch_add(by as f64).map(Value::Number).map_err(failure)
            }
            (Self::Text(_), _) => {
                Err(failure("a string cell has no arithmetic; use append()"))
            }
            (cell, by) => Err(failure(format!(
                "a {} cell does not add {}",
                cell.kind(),
                by.type_name()
            ))),
        }
    }

    /// Take up to `by` while the value stays at or under `limit`, in one swap:
    /// N callers racing this never pass the limit, which `add` followed by a
    /// test cannot promise. `took` false is the whole answer.
    fn add_until(
        &self,
        by: i64,
        limit: i64,
    ) -> mlua::Result<(bool, i64, i64)> {
        let Self::Int(cell) = self else {
            return Err(failure("only an int cell can be claimed from"));
        };
        Ok(match cell.add_until(by, limit).map_err(failure)? {
            Claim::Took { taken, after } => (true, taken, after),
            Claim::Exhausted => (false, 0, limit),
        })
    }

    fn append(
        &self,
        text: &str,
    ) -> mlua::Result<String> {
        let Self::Text(cell) = self else {
            return Err(failure("only a string cell can be appended to"));
        };
        cell.append(text).map_err(failure)?;
        cell.load().map_err(failure)
    }

    fn version(&self) -> mlua::Result<i64> {
        match self {
            Self::Int(cell) => cell.version().map(i64::from),
            Self::Float(cell) => cell.version().map(i64::from),
            Self::Bool(cell) => cell.version().map(i64::from),
            // Lua's signed integer retains every bit of the opaque u64 token.
            Self::Text(cell) => cell.version().map(|revision| revision as i64),
        }
        .map_err(failure)
    }

    fn wait_changed_until(
        &self,
        since: i64,
        interrupt: &AtomicBool,
    ) -> mlua::Result<Option<i64>> {
        let scalar = || {
            u32::try_from(since)
                .map_err(|_| failure("wait takes a version from version()"))
        };
        match self {
            Self::Int(cell) => cell
                .wait_changed_until(scalar()?, interrupt)
                .map(|revision| revision.map(i64::from)),
            Self::Float(cell) => cell
                .wait_changed_until(scalar()?, interrupt)
                .map(|revision| revision.map(i64::from)),
            Self::Bool(cell) => cell
                .wait_changed_until(scalar()?, interrupt)
                .map(|revision| revision.map(i64::from)),
            Self::Text(cell) => cell
                .wait_changed_until(since as u64, interrupt)
                .map(|revision| revision.map(|revision| revision as i64)),
        }
        .map_err(failure)
    }

    fn wake_waiters(&self) -> mlua::Result<()> {
        match self {
            Self::Int(cell) => cell.wake_waiters(),
            Self::Float(cell) => cell.wake_waiters(),
            Self::Bool(cell) => cell.wake_waiters(),
            Self::Text(cell) => cell.wake_waiters(),
        }
        .map_err(failure)
    }

    fn release(self) -> mlua::Result<()> {
        match self {
            Self::Int(cell) => cell.release(),
            Self::Float(cell) => cell.release(),
            Self::Bool(cell) => cell.release(),
            Self::Text(cell) => cell.release(),
        }
        .map_err(failure)
    }
}

#[cfg(test)]
#[path = "../tests/orbit.rs"]
mod tests;
