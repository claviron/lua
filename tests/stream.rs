use std::sync::Arc;

use claviron_orbit::stream::{Incarnation, Streams};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{Engine, Script};

fn engine() -> Engine {
    let fleet = Arc::new(
        claviron_orbit::Fleet::join("lua-stream-test", 1).expect("fleet"),
    );
    let streams = Streams::new(fleet, Incarnation::new(1)).expect("streams");
    Engine::new().with(Arc::new(StreamExtension::new(Arc::new(streams))))
}

/// Two scripts over one stream: the creator hands its ticket out, sends two
/// messages and finishes; the other side opens the ticket, echoes each back
/// in upper case until the end, and finishes in turn.
#[tokio::test(flavor = "current_thread")]
async fn two_scripts_exchange_messages_over_a_stream() {
    let engine = engine();
    let stop = CancellationToken::new();
    let (ticket_tx, ticket_rx) = tokio::sync::oneshot::channel::<String>();
    let ticket_tx = std::sync::Mutex::new(Some(ticket_tx));

    let mut creator =
        Script::new("creator.lua", include_str!("fixtures/stream/creator.lua"));
    creator.output = Some(Arc::new(move |bytes: &[u8]| {
        if let Some(tx) = ticket_tx.lock().unwrap().take() {
            let _ = tx.send(String::from_utf8_lossy(bytes).trim().to_owned());
        }
    }));

    let echo = async {
        let mut echo =
            Script::new("echo.lua", include_str!("fixtures/stream/echo.lua"));
        echo.args = vec![ticket_rx.await.expect("ticket")];
        engine.execute(echo, &stop).await
    };
    let (created, echoed) = tokio::join!(engine.execute(creator, &stop), echo);
    created.unwrap();
    echoed.unwrap();
}

/// A script parked on a stream nobody writes to is released when its
/// execution is told to stop.
#[tokio::test(flavor = "current_thread")]
async fn a_parked_read_ends_when_the_script_stops() {
    let engine = engine();
    let stop = CancellationToken::new();
    let script =
        Script::new("parked.lua", include_str!("fixtures/stream/parked.lua"));
    let stopping = async {
        tokio::task::yield_now().await;
        stop.cancel();
    };
    let (ran, ()) = tokio::join!(engine.execute(script, &stop), stopping);
    ran.unwrap();
}
