use super::*;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::{cell::RefCell, collections::VecDeque};

fn incoming<T>(
    queue: &RefCell<VecDeque<T>>
) -> impl Future<Output = Option<T>> + use<T> {
    let next = queue.borrow_mut().pop_front();
    async move {
        match next {
            Some(value) => Some(value),
            None => std::future::pending().await,
        }
    }
}

fn assert_pending(future: Pin<&mut impl Future>) {
    assert!(future.poll(&mut Context::from_waker(Waker::noop())).is_pending());
}

#[tokio::test]
async fn slow_write_preserves_read_shutdown_and_admitted_records() {
    let (stream, mut peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    outbound.send(Outbound::Text("x".repeat(64))).await.unwrap();
    let driving = drive(stream, queued, events, 128, async {
        stopped.await.unwrap();
        Close(1001, "server stopping".to_owned(), false)
    });
    tokio::pin!(driving);
    assert_pending(driving.as_mut()); // The first record is partially written.
    outbound.send(Outbound::Binary(vec![1, 2, 3])).await.unwrap();
    let blocked = outbound.send(Outbound::Text("not admitted".to_owned()));
    tokio::pin!(blocked);
    assert_pending(blocked.as_mut());

    write_message(&mut peer, &Message::Text("ping".to_owned())).await.unwrap();
    assert_pending(driving.as_mut());
    assert!(
        matches!(happened.try_recv(), Ok(Event::Message(data, false)) if data == b"ping")
    );

    stop.send(()).unwrap();
    assert_pending(driving.as_mut());
    assert!(outbound.is_closed());
    assert!(blocked.await.is_err());
    assert!(
        matches!(happened.try_recv(), Ok(Event::Close(1001, reason, false)) if reason == "server stopping")
    );

    let receiving = async {
        let mut records = MessageReader::new();
        assert_eq!(
            records.read(&mut peer).await.unwrap(),
            Some(Message::Text("x".repeat(64)))
        );
        assert_eq!(
            records.read(&mut peer).await.unwrap(),
            Some(Message::Binary(vec![1, 2, 3]))
        );
        assert_eq!(
            records.read(&mut peer).await.unwrap(),
            Some(Message::Close {
                code: 1001,
                reason: "server stopping".to_owned()
            })
        );
        assert_eq!(records.read(&mut peer).await.unwrap(), None);
    };
    tokio::join!(driving, receiving);
}

#[tokio::test]
async fn full_event_queue_keeps_pending_message_before_shutdown_close() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let (_outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    let driving = drive(stream, queued, events, 128, async {
        stopped.await.unwrap();
        Close(1001, "stopping".to_owned(), false)
    });
    tokio::pin!(driving);
    for text in ["first", "pending", "not admitted"] {
        write_message(&mut peer, &Message::Text(text.to_owned()))
            .await
            .unwrap();
    }
    assert_pending(driving.as_mut());
    stop.send(()).unwrap();
    assert_pending(driving.as_mut());
    let receiving = async {
        for expected in [b"first".as_slice(), b"pending".as_slice()] {
            assert!(
                matches!(happened.recv().await, Some(Event::Message(data, false)) if data == expected)
            );
        }
        assert!(matches!(
            happened.recv().await,
            Some(Event::Close(1001, _, false))
        ));
        assert!(happened.recv().await.is_none());
    };
    tokio::join!(driving, receiving);
}

#[tokio::test]
async fn failed_writer_releases_producers_and_notifies_close() {
    let (stream, peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    outbound.send(Outbound::Text("hello".to_owned())).await.unwrap();
    drop(peer);
    let driving = drive(stream, queued, events, 128, std::future::pending());
    tokio::pin!(driving);
    assert!(matches!(
        driving.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(())
    ));
    assert!(outbound.is_closed());
    assert!(matches!(happened.try_recv(), Ok(Event::Close(1006, _, false))));
}

#[tokio::test]
async fn multistream_admission_is_bounded_and_shutdown_joins_every_drain() {
    let (finish1, done1) = oneshot::channel();
    let (finish2, done2) = oneshot::channel();
    let (finish3, done3) = oneshot::channel();
    let queue =
        RefCell::new(VecDeque::from([(1, done1), (2, done2), (3, done3)]));
    let started = RefCell::new(Vec::new());
    let stopped = RefCell::new(Vec::new());
    let completed = RefCell::new(Vec::new());
    let (stop, stopping) = oneshot::channel();
    let running = run_streams(
        || incoming(&queue),
        NonZeroUsize::new(2).unwrap(),
        async {
            stopping.await.unwrap();
            SessionEnd::ServerStopping.close()
        },
        |(id, done), ended| {
            started.borrow_mut().push(id);
            let stopped = &stopped;
            let completed = &completed;
            async move {
                tokio::pin!(done);
                tokio::select! {
                    _ = &mut done => {},
                    close = wait_end(ended) => {
                        assert_eq!(close.0, 1001);
                        stopped.borrow_mut().push(id);
                        let _ = done.await;
                    }
                }
                completed.borrow_mut().push(id);
            }
        },
    );
    tokio::pin!(running);
    assert_pending(running.as_mut());
    assert_eq!(*started.borrow(), [1, 2]);
    assert_eq!(queue.borrow().len(), 1);

    finish2.send(()).unwrap();
    assert_pending(running.as_mut());
    assert_eq!(*started.borrow(), [1, 2, 3]);
    assert_eq!(*completed.borrow(), [2]);
    stop.send(()).unwrap();
    assert_pending(running.as_mut());
    let mut stopping_ids = stopped.borrow().clone();
    stopping_ids.sort_unstable();
    assert_eq!(stopping_ids, [1, 3]);
    finish1.send(()).unwrap();
    assert_pending(running.as_mut()); // Stream 3 still owns admitted work.
    finish3.send(()).unwrap();
    running.await;
    assert_eq!(completed.borrow().len(), 3);
}

#[tokio::test]
async fn multistream_slow_output_does_not_block_sibling_io_or_session_shutdown()
{
    let (slow, mut slow_peer) = tokio::io::duplex(16);
    let (fast, mut fast_peer) = tokio::io::duplex(64);
    let (slow_send, slow_queue) = mpsc::channel(1);
    let (fast_send, fast_queue) = mpsc::channel(1);
    let (slow_events, mut slow_happened) = mpsc::channel(1);
    let (fast_events, mut fast_happened) = mpsc::channel(1);
    slow_send.send(Outbound::Text("x".repeat(64))).await.unwrap();
    fast_send.send(Outbound::Text("ready".to_owned())).await.unwrap();
    let queue = RefCell::new(VecDeque::from([
        (slow, slow_queue, slow_events),
        (fast, fast_queue, fast_events),
    ]));
    let (stop, stopping) = oneshot::channel();
    let running = run_streams(
        || incoming(&queue),
        NonZeroUsize::new(2).unwrap(),
        async {
            stopping.await.unwrap();
            SessionEnd::ServerStopping.close()
        },
        |(stream, queued, events), ended| {
            drive(stream, queued, events, 128, wait_end(ended))
        },
    );
    tokio::pin!(running);
    assert_pending(running.as_mut());
    let mut fast_records = MessageReader::new();
    assert_eq!(
        fast_records.read(&mut fast_peer).await.unwrap(),
        Some(Message::Text("ready".to_owned()))
    );
    write_message(&mut fast_peer, &Message::Text("ping".to_owned()))
        .await
        .unwrap();
    assert_pending(running.as_mut());
    assert!(
        matches!(fast_happened.try_recv(), Ok(Event::Message(data, false)) if data == b"ping")
    );

    slow_send.try_send(Outbound::Binary(vec![1, 2, 3])).unwrap();
    assert!(slow_send.try_send(Outbound::Text("full".to_owned())).is_err());
    stop.send(()).unwrap();
    assert_pending(running.as_mut());
    assert!(slow_send.is_closed());
    assert!(fast_send.is_closed());
    assert!(matches!(
        slow_happened.try_recv(),
        Ok(Event::Close(1001, _, false))
    ));
    assert!(matches!(
        fast_happened.try_recv(),
        Ok(Event::Close(1001, _, false))
    ));
    assert!(matches!(
        fast_records.read(&mut fast_peer).await.unwrap(),
        Some(Message::Close { code: 1001, .. })
    ));

    let receiving = async {
        let mut records = MessageReader::new();
        assert_eq!(
            records.read(&mut slow_peer).await.unwrap(),
            Some(Message::Text("x".repeat(64)))
        );
        assert_eq!(
            records.read(&mut slow_peer).await.unwrap(),
            Some(Message::Binary(vec![1, 2, 3]))
        );
        assert!(matches!(
            records.read(&mut slow_peer).await.unwrap(),
            Some(Message::Close { code: 1001, .. })
        ));
        assert_eq!(records.read(&mut slow_peer).await.unwrap(), None);
    };
    tokio::join!(running, receiving);
}

#[tokio::test]
async fn multistream_shutdown_before_admission_never_starts_a_handler() {
    let queue = RefCell::new(VecDeque::from([1]));
    run_streams(
        || incoming(&queue),
        NonZeroUsize::new(1).unwrap(),
        std::future::ready(SessionEnd::PeerClosed.close()),
        |_, _| async { panic!("closed session admitted a stream") },
    )
    .await;
    assert_eq!(queue.borrow().len(), 1);
}
