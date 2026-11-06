//! The engine: fresh sandboxed states, the extensions that go into them, and
//! running one script to its end.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use mlua::chunk::ChunkMode;
use mlua::{Function, Lua, LuaOptions, StdLib, Table, Value, Variadic};
use tokio_util::sync::CancellationToken;

use crate::execution::Execution;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Lua(#[from] mlua::Error),
}

/// What a crate adds for Lua: a module scripts `require` by name.
pub trait Extension: Send + Sync + 'static {
    /// The name a script requires (`claviron.orbit.cell`).
    fn name(&self) -> &'static str;

    /// Build the module's table in `lua`, for the execution it belongs to.
    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table>;

    /// Its name under the `claviron` global as well (`cell` for
    /// `claviron.cell`), when it has one. The same module `require` returns,
    /// built on first use.
    fn global(&self) -> Option<&'static str> {
        None
    }
}

/// Where a script's `print` writes.
pub type Output = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// One script to run.
pub struct Script {
    /// How errors and tracebacks name it: its path, usually.
    pub name: String,
    pub source: String,
    /// The script's arguments, as Lua's `arg` table: `arg[0]` is the name.
    pub args: Vec<String>,
    /// Where `print` writes; `None` is the process's standard output.
    pub output: Option<Output>,
}

impl Script {
    pub fn new(
        name: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            source: source.into(),
            args: Vec::new(),
            output: None,
        }
    }
}

/// The extensions every state gets. Cheap to clone; a host builds one and runs
/// every script through it.
#[derive(Clone, Default)]
pub struct Engine {
    extensions: Arc<HashMap<&'static str, Arc<dyn Extension>>>,
    path: Arc<Vec<PathBuf>>,
    environment: Arc<HashMap<String, String>>,
}

impl Engine {
    /// Values belonging to this execution, never the host process environment.
    pub fn with_environment(
        mut self,
        values: HashMap<String, String>,
    ) -> Self {
        self.environment = Arc::new(values);
        self
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(
        mut self,
        extension: Arc<dyn Extension>,
    ) -> Self {
        Arc::make_mut(&mut self.extensions).insert(extension.name(), extension);
        self
    }

    /// Directories `require` searches for Lua source after the extensions:
    /// `a.b` is `a/b.lua` or `a/b/init.lua` in each, in order. Source only —
    /// a C module is never loaded from here.
    pub fn with_path(
        mut self,
        path: Vec<PathBuf>,
    ) -> Self {
        self.path = Arc::new(path);
        self
    }

    /// Run `script` to its end in a fresh state. When `cancellation` fires,
    /// every wait the script is parked on ends with an error the script can
    /// see; the script itself decides how it finishes, and this waits for it.
    pub async fn execute(
        &self,
        script: Script,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let execution = Execution::new();
        let lua = self.state(
            &execution,
            &script.name,
            &script.args,
            script.output.clone(),
        )?;
        let running = lua
            .load(script.source.as_str())
            .set_name(format!("@{}", script.name))
            .set_mode(ChunkMode::Text)
            .exec_async();
        tokio::pin!(running);
        tokio::select! {
            result = &mut running => return result.map_err(Error::from),
            () = cancellation.cancelled() => execution.interrupt()
        }
        running.await.map_err(Error::from)
    }

    /// A state that stays: an application's, loaded once and called for every
    /// request. The same sandbox and extensions as a script's; interrupting
    /// the execution ends every wait anything in the state is parked on.
    pub fn open(
        &self,
        name: &str,
        output: Option<Output>,
    ) -> Result<(Lua, Arc<Execution>), Error> {
        let execution = Execution::new();
        let lua = self.state(&execution, name, &[], output)?;
        Ok((lua, execution))
    }

    /// A state with only what a script may have: the chosen standard
    /// libraries, text-only `load`, and `require` over the registered
    /// extensions.
    fn state(
        &self,
        execution: &Arc<Execution>,
        name: &str,
        arguments: &[String],
        output: Option<Output>,
    ) -> mlua::Result<Lua> {
        let lua = Lua::new_with(
            StdLib::STRING
                | StdLib::TABLE
                | StdLib::MATH
                | StdLib::UTF8
                | StdLib::COROUTINE,
            LuaOptions::default(),
        )?;
        let globals = lua.globals();
        // Nothing reads the host's files.
        globals.set("dofile", Value::Nil)?;
        globals.set("loadfile", Value::Nil)?;
        // Source only: a binary chunk is loaded unverified.
        let load: Function = globals.get("load")?;
        globals.set(
            "load",
            lua.create_function(
                move |_,
                      (chunk, name, _mode, env): (
                    Value,
                    Value,
                    Value,
                    Value,
                )| {
                    load.call::<mlua::MultiValue>((chunk, name, "t", env))
                },
            )?,
        )?;
        if let Some(output) = output {
            globals.set(
                "print",
                lua.create_function(move |_, values: Variadic<Value>| {
                    let mut line = Vec::new();
                    for (index, value) in values.iter().enumerate() {
                        if index > 0 {
                            line.push(b'\t');
                        }
                        line.extend_from_slice(value.to_string()?.as_bytes());
                    }
                    line.push(b'\n');
                    output(&line);
                    Ok(())
                })?,
            )?;
        }
        let args = lua.create_table()?;
        args.set(0, name)?;
        for (index, value) in arguments.iter().enumerate() {
            args.set(index + 1, value.as_str())?;
        }
        globals.set("arg", args)?;
        let require = self.require(&lua, execution)?;
        globals.set("claviron", self.namespace(&lua, require.clone())?)?;
        globals.set("require", require)?;
        Ok(lua)
    }

    /// The `claviron` global: each extension that names itself there, built
    /// through `require` on first use.
    fn namespace(
        &self,
        lua: &Lua,
        require: Function,
    ) -> mlua::Result<Table> {
        let names: HashMap<&'static str, &'static str> = self
            .extensions
            .values()
            .filter_map(|extension| {
                Some((extension.global()?, extension.name()))
            })
            .collect();
        let namespace = lua.create_table()?;
        let environment = Arc::clone(&self.environment);
        namespace.set(
            "getenv",
            lua.create_function(move |_, key: String| {
                Ok(environment.get(&key).cloned())
            })?,
        )?;
        let meta = lua.create_table()?;
        meta.set(
            "__index",
            lua.create_function(
                move |_, (namespace, key): (Table, String)| {
                    let Some(module) = names.get(key.as_str()) else {
                        return Ok(Value::Nil);
                    };
                    let module: Table = require.call(*module)?;
                    namespace.raw_set(key, &module)?;
                    Ok(Value::Table(module))
                },
            )?,
        )?;
        namespace.set_metatable(Some(meta))?;
        Ok(namespace)
    }

    /// `require` over the registered extensions, then the packages the binary
    /// carries, then the search path: each
    /// module is loaded once per state, on its first `require`. Nothing else
    /// is searched — no `package.path` of the host's, no C modules.
    fn require(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Function> {
        let loaded = lua.create_table()?;
        let extensions = Arc::clone(&self.extensions);
        let path = Arc::clone(&self.path);
        let execution = Arc::clone(execution);
        lua.create_function(move |lua, name: String| {
            let cached = loaded.get::<Value>(name.as_str())?;
            if !cached.is_nil() {
                return Ok(cached);
            }
            let module = match extensions.get(name.as_str()) {
                Some(extension) => {
                    Value::Table(extension.module(lua, &execution)?)
                }
                None if crate::packages::source(&name).is_some() => {
                    let source =
                        crate::packages::source(&name).expect("checked");
                    let chunk = lua
                        .load(source)
                        .set_name(format!("@{name}.lua"))
                        .set_mode(ChunkMode::Text)
                        .into_function()?;
                    match chunk
                        .call::<Value>((name.as_str(), format!("{name}.lua")))?
                    {
                        Value::Nil => Value::Boolean(true),
                        module => module,
                    }
                }
                None => {
                    let file = find(&path, &name).ok_or_else(|| {
                        mlua::Error::runtime(format!(
                            "module '{name}' is not available here"
                        ))
                    })?;
                    let source =
                        std::fs::read_to_string(&file).map_err(|error| {
                            mlua::Error::runtime(format!(
                                "module '{name}': {}: {error}",
                                file.display()
                            ))
                        })?;
                    let chunk = lua
                        .load(source)
                        .set_name(format!("@{}", file.display()))
                        .set_mode(ChunkMode::Text)
                        .into_function()?;
                    // Lua's own convention: the module gets its name and file,
                    // and one that returns nothing is loaded as `true`.
                    match chunk.call::<Value>((
                        name.as_str(),
                        file.display().to_string(),
                    ))? {
                        Value::Nil => Value::Boolean(true),
                        module => module,
                    }
                }
            };
            loaded.set(name.as_str(), &module)?;
            Ok(module)
        })
    }
}

/// `a.b` in the search path: `a/b.lua`, then `a/b/init.lua`, directory by
/// directory.
fn find(
    path: &[PathBuf],
    name: &str,
) -> Option<PathBuf> {
    if name.is_empty()
        || name
            .split('.')
            .any(|part| part.is_empty() || part.contains(['/', '\\']))
    {
        return None;
    }
    let relative: PathBuf = name.split('.').collect();
    path.iter().find_map(|dir| {
        let file = dir.join(&relative).with_extension("lua");
        if file.is_file() {
            return Some(file);
        }
        let init = dir.join(&relative).join("init.lua");
        init.is_file().then_some(init)
    })
}

#[cfg(test)]
#[path = "../tests/engine.rs"]
mod tests;
