use std::sync::Arc;

use claviron_orbit::{Fleet, InvocationBus};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{Engine, Script};

fn engine(name: &'static str) -> Engine {
    let bus = Arc::new(InvocationBus::new(Arc::new(
        Fleet::join(name, 1).expect("fleet"),
    )));
    Engine::new().with(Arc::new(InvokeExtension::new(bus)))
}

/// A subscription opened before the send receives it, with the id `send`
/// returned and the payload's shape kept; other operations pass it by.
#[tokio::test(flavor = "current_thread")]
async fn a_subscriber_receives_what_is_sent_after_it() {
    let script =
        Script::new("invoke.lua", include_str!("fixtures/invoke/invoke.lua"));
    engine("lua-invoke-send")
        .execute(script, &CancellationToken::new())
        .await
        .unwrap();
}

/// `serve` hands each invocation to its handler until the execution is told
/// to stop, and then returns rather than failing.
#[tokio::test(flavor = "current_thread")]
async fn serve_runs_until_stopped() {
    let stop = CancellationToken::new();
    let printed = Arc::new(std::sync::Mutex::new(String::new()));
    let sink = Arc::clone(&printed);
    let mut script =
        Script::new("serve.lua", include_str!("fixtures/invoke/serve.lua"));
    script.output = Some(Arc::new(move |bytes: &[u8]| {
        sink.lock().unwrap().push_str(&String::from_utf8_lossy(bytes))
    }));
    let engine = engine("lua-invoke-serve");
    let running = engine.execute(script, &stop);
    tokio::pin!(running);
    tokio::select! {
        _ = &mut running => panic!("serve ended on its own"),
        () = tokio::task::yield_now() => {}
    }
    stop.cancel();
    running.await.unwrap();
    // Sent before `serve` subscribed, so they are not its: a subscription
    // starts at the ring's head.
    assert_eq!(printed.lock().unwrap().as_str(), "serving\nstopped serving\n");
}
