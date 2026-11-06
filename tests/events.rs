use std::sync::Arc;

use claviron_runtime::events::ProcessEventBus;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{Engine, JsonExtension, Script};

fn engine(bus: &ProcessEventBus) -> Engine {
    Engine::new()
        .with(Arc::new(EventsExtension::new(bus.clone())))
        .with(Arc::new(JsonExtension))
}

/// A listener opened before the work that answers it cannot miss the answer;
/// payloads keep their shape across the event value, lists included; an
/// event sent to an application is not a script's.
#[tokio::test(flavor = "current_thread")]
async fn a_listener_receives_what_is_published_after_it() {
    let bus = ProcessEventBus::new();
    let script =
        Script::new("events.lua", include_str!("fixtures/events/events.lua"));
    engine(&bus).execute(script, &CancellationToken::new()).await.unwrap();
}

/// A script waiting for an event is released when its execution is told to
/// stop — not by a clock.
#[tokio::test(flavor = "current_thread")]
async fn stopping_ends_an_event_wait() {
    let bus = ProcessEventBus::new();
    let stop = CancellationToken::new();
    let script =
        Script::new("wait.lua", include_str!("fixtures/events/wait.lua"));
    let engine = engine(&bus);
    let running = engine.execute(script, &stop);
    tokio::pin!(running);
    tokio::select! {
        _ = &mut running => panic!("the wait ended on its own"),
        () = tokio::task::yield_now() => {}
    }
    stop.cancel();
    running.await.unwrap();
}
