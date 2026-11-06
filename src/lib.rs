//! Lua 5.5 embedded in Claviron.
//!
//! Every script runs the way PHP runs one: a fresh sandboxed state, the
//! registered extensions installed into it, the script driven to its end as a
//! future on Tokio, and the state dropped. Nothing a script keeps outlives it;
//! what must be shared goes through Orbit. See `ARCHITECTURE.md`.

#[cfg(feature = "app")]
pub mod app;
#[cfg(feature = "server")]
pub mod config;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub mod db;
#[cfg(feature = "edge")]
pub mod edge;
mod engine;
#[cfg(feature = "orbit")]
pub mod events;
mod execution;
#[cfg(feature = "fetch")]
pub mod fetch;
#[cfg(feature = "server")]
pub mod host;
#[cfg(feature = "orbit")]
pub mod invoke;
pub mod json;
#[cfg(feature = "log")]
pub mod log;
#[cfg(feature = "orbit")]
pub mod orbit;
mod packages;
#[cfg(feature = "server")]
pub mod process;
pub mod regex;
pub mod router;
#[cfg(feature = "app")]
pub mod serve;
#[cfg(any(feature = "websocket", feature = "webtransport"))]
mod socket;
#[cfg(feature = "orbit")]
pub mod stream;
#[cfg(feature = "tasks")]
pub mod task;
#[cfg(feature = "websocket")]
pub mod websocket;
#[cfg(feature = "webtransport")]
pub mod webtransport;

/// Register what a host's Lua needs, in order, each from its own module:
/// the fleet's cells and streams, the applications `lua.yaml` declares, the
/// application events, the invocations, the search path and `lua:` tasks.
/// Host-selected routes are separate: `edge::register`, after Edge's own.
#[cfg(all(feature = "app", feature = "orbit"))]
pub fn register(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    claviron_orbit::register_cells(state)?;
    claviron_orbit::register_streams(state)?;
    serve::register(state)?;
    events::register(state)?;
    claviron_orbit::register_invoke(state)?;
    config::register(state)?;
    #[cfg(feature = "tasks")]
    task::register(state).map_err(claviron_runtime::registry::Error::msg)?;
    Ok(())
}

pub use engine::{Engine, Error, Extension, Output, Script};
pub use execution::Execution;
pub use json::JsonExtension;
pub use mlua;
pub use regex::RegexExtension;
pub use router::RouterExtension;

#[cfg(test)]
#[path = "../tests/tools.rs"]
mod tools_tests;
