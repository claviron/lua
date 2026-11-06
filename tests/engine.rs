use std::sync::{Arc, Mutex};

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn environment_is_frozen_per_execution_and_does_not_expose_the_process() {
    let script = |value: &str| {
        let mut script = Script::new(
            "environment.lua",
            include_str!("fixtures/engine/environment.lua"),
        );
        script.args = vec![value.to_owned()];
        script
    };
    let original = Engine::new().with_environment(
        [("VALUE".into(), "original".into()), ("EMPTY".into(), String::new())]
            .into(),
    );
    let replacement = original.clone().with_environment(
        [
            ("VALUE".into(), "replacement".into()),
            ("EMPTY".into(), String::new()),
        ]
        .into(),
    );
    let cancellation = CancellationToken::new();
    original.execute(script("original"), &cancellation).await.unwrap();
    replacement.execute(script("replacement"), &cancellation).await.unwrap();
    original.execute(script("original"), &cancellation).await.unwrap();
}

/// A script gets only what the engine gives it: no files, no process, no
/// binary chunks, no module it was not handed — and its `print` goes where the
/// host said.
#[tokio::test(flavor = "current_thread")]
async fn a_script_runs_sandboxed_and_prints_to_its_host() {
    let printed = Arc::new(Mutex::new(Vec::<u8>::new()));
    let sink = Arc::clone(&printed);
    let mut script =
        Script::new("sandbox.lua", include_str!("fixtures/engine/sandbox.lua"));
    script.args = vec!["first".into()];
    script.output = Some(Arc::new(move |bytes: &[u8]| {
        sink.lock().unwrap().extend_from_slice(bytes)
    }));

    Engine::new()
        .execute(script, &tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8(printed.lock().unwrap().clone()).unwrap(),
        format!("sandbox.lua\tfirst\t{}\n", i64::MAX)
    );
}

/// `require` finds Lua source in the search path after the extensions, loads
/// it once per state as text, and hands it its name and file.
#[tokio::test(flavor = "current_thread")]
async fn require_loads_source_from_the_search_path() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/engine/path");
    let script =
        Script::new("path.lua", include_str!("fixtures/engine/path.lua"));
    Engine::new()
        .with_path(vec![dir])
        .execute(script, &tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
}
