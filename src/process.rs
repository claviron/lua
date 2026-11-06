//! `claviron.process`: host-supplied process role and worker slot, not credentials.

use std::sync::Arc;

use claviron_runtime::RunMode;
use mlua::{Lua, Table};

use crate::{Execution, Extension};

/// Captures the owning process's identity when its managed engine is built.
pub struct ProcessExtension {
    mode: RunMode,
}

impl ProcessExtension {
    pub fn new(mode: RunMode) -> Self {
        Self { mode }
    }
}

impl Extension for ProcessExtension {
    fn name(&self) -> &'static str {
        "claviron.process"
    }

    fn global(&self) -> Option<&'static str> {
        Some("process")
    }

    fn module(
        &self,
        lua: &Lua,
        _: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        module.set("role", self.mode.as_env_value())?;
        module.set("worker_id", self.mode.worker_id())?;
        Ok(module)
    }
}
