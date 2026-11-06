use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{Engine, JsonExtension, RegexExtension, Script};

async fn run(source: &str) {
    Engine::new()
        .with(Arc::new(JsonExtension))
        .with(Arc::new(RegexExtension))
        .execute(Script::new("tools.lua", source), &CancellationToken::new())
        .await
        .unwrap();
}

/// JSON round-trips what Lua cannot say on its own: `null` in a table, and
/// an empty list that stays a list.
#[tokio::test(flavor = "current_thread")]
async fn json_keeps_null_and_empty_arrays() {
    run(include_str!("fixtures/tools/json_keeps_null_and_empty_arrays.lua"))
        .await;
}

/// Regex positions are Lua's (1-based, inclusive), captures come by number
/// and by name, and matching works on bytes.
#[tokio::test(flavor = "current_thread")]
async fn regex_speaks_lua_positions() {
    run(include_str!("fixtures/tools/regex_speaks_lua_positions.lua")).await;
}

/// A channel `logging.yaml` does not declare is refused when it is opened —
/// and without a pipeline, none is declared.
#[cfg(feature = "log")]
#[tokio::test(flavor = "current_thread")]
async fn an_undeclared_log_channel_is_refused() {
    Engine::new()
        .with(Arc::new(crate::log::LogExtension::new(None, "script")))
        .execute(
            Script::new("log.lua", include_str!("fixtures/tools/log.lua")),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
}

/// The packages the binary carries load on Lua 5.5 inside the sandbox and
/// work; what in them needs a closed library fails when called, not on load.
#[tokio::test(flavor = "current_thread")]
async fn carried_packages_load_and_work() {
    run(include_str!("fixtures/tools/carried_packages_load_and_work.lua"))
        .await;
}
