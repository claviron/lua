use std::sync::Arc;

use claviron_orbit::Cells;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{Engine, Script};

fn engine() -> Engine {
    let fleet = Arc::new(
        claviron_orbit::Fleet::join("lua-cell-test", 1).expect("fleet"),
    );
    Engine::new().with(Arc::new(CellExtension::new(Arc::new(
        Cells::new(fleet).expect("cells"),
    ))))
}

/// A cell is the same place through every handle: one script allocates and
/// updates it, another opens it by id and sees the value, integers stay
/// 64-bit, and comparisons read the cell.
#[tokio::test(flavor = "current_thread")]
async fn a_cell_is_shared_by_id_between_scripts() {
    let engine = engine();
    let stop = CancellationToken::new();
    let id = Arc::new(std::sync::Mutex::new(String::new()));
    let sink = Arc::clone(&id);
    let mut first =
        Script::new("first.lua", include_str!("fixtures/orbit/first.lua"));
    first.output = Some(Arc::new(move |bytes: &[u8]| {
        *sink.lock().unwrap() = String::from_utf8_lossy(bytes).trim().to_owned()
    }));
    engine.execute(first, &stop).await.unwrap();

    let id = id.lock().unwrap().clone();
    let mut second =
        Script::new("second.lua", include_str!("fixtures/orbit/second.lua"));
    second.args = vec![id];
    engine.execute(second, &stop).await.unwrap();
}

/// A script parked on a cell is released when its execution is told to stop,
/// with an error it can see — not by a clock.
#[tokio::test(flavor = "multi_thread")]
async fn stopping_ends_a_parked_wait() {
    let engine = engine();
    let stop = CancellationToken::new();
    let script =
        Script::new("wait.lua", include_str!("fixtures/orbit/wait.lua"));
    let stopper = stop.clone();
    let running =
        tokio::spawn(async move { engine.execute(script, &stopper).await });
    tokio::task::yield_now().await;
    stop.cancel();
    running.await.unwrap().unwrap();
}

#[test]
fn cell_revision_preserves_scalar_validation_and_signed_text_tokens() {
    let cells = Cells::new(Arc::new(
        claviron_orbit::Fleet::join("revision", 1).unwrap(),
    ))
    .unwrap();
    let interrupt = std::sync::atomic::AtomicBool::new(false);
    let scalar = cells.allocate(7_i64).unwrap();
    let handle = Cell::Int(scalar.clone());
    let before = handle.version().unwrap();
    scalar.store(8).unwrap();
    assert_eq!(handle.wait_changed_until(before, &interrupt).unwrap(), Some(1));
    for invalid in [-1, i64::from(u32::MAX) + 1] {
        assert!(handle.wait_changed_until(invalid, &interrupt).is_err());
    }
    let text = cells.allocate_text("before").unwrap();
    let handle = Cell::Text(text.clone());
    let before = handle.version().unwrap();
    text.store("after").unwrap();
    let after = text.version().unwrap() as i64;
    assert_eq!(
        handle.wait_changed_until(before, &interrupt).unwrap(),
        Some(after)
    );
    assert_eq!(
        handle.wait_changed_until(i64::MIN, &interrupt).unwrap(),
        Some(after)
    );
}
