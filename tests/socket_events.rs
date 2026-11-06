use std::future::{Future, poll_fn};
use std::task::Poll;

use claviron_app::{ApplicationEvent, EventValue};
use claviron_runtime::events::ProcessEventBus;

use super::*;
use crate::Execution;
use crate::events::Listener;

fn listener(
    bus: &ProcessEventBus,
    execution: Arc<Execution>,
) -> Listener {
    Listener::subscribe(bus, vec!["example.chat".to_owned()], execution)
}

#[tokio::test]
async fn application_event_reaches_a_socket_without_socket_input() {
    let bus = ProcessEventBus::new();
    let listener = listener(&bus, Execution::new());
    let (_sender, mut input) = mpsc::channel(1);
    let event =
        ApplicationEvent::new("example.chat", EventValue::Null).unwrap();
    let id = event.id();
    bus.emit(event);
    match next_input(&mut input, Some(&listener)).await {
        Input::Application(Ok(event)) => assert_eq!(event.id(), id),
        _ => panic!("expected the subscribed application event"),
    }
    // The same local publication is not received a second time.
    let next = listener.receive();
    tokio::pin!(next);
    poll_fn(|cx| {
        assert!(next.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn socket_close_preserves_the_event_subscription() {
    let bus = ProcessEventBus::new();
    let listener = listener(&bus, Execution::new());
    let (sender, mut input) = mpsc::channel(1);
    {
        let next = next_input(&mut input, Some(&listener));
        tokio::pin!(next);
        poll_fn(|cx| {
            assert!(next.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        sender.send(Event::Close(1000, "done".to_owned(), true)).await.unwrap();
        assert!(matches!(
            next.await,
            Input::Socket(Some(Event::Close(1000, _, true)))
        ));
    }
    let event =
        ApplicationEvent::new("example.chat", EventValue::Null).unwrap();
    let id = event.id();
    bus.emit(event);
    assert_eq!(listener.receive().await.unwrap().id(), id);
}

#[tokio::test]
async fn stopping_detaches_events_without_hiding_native_close() {
    let bus = ProcessEventBus::new();
    let execution = Execution::new();
    let listener = listener(&bus, Arc::clone(&execution));
    let (sender, mut input) = mpsc::channel(1);
    {
        let next = next_input(&mut input, Some(&listener));
        tokio::pin!(next);
        poll_fn(|cx| {
            assert!(next.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        execution.interrupt();
        assert!(matches!(next.await, Input::Application(Err(_))));
    }
    sender
        .send(Event::Close(1001, "stopping".to_owned(), false))
        .await
        .unwrap();
    assert!(matches!(
        next_input(&mut input, None).await,
        Input::Socket(Some(Event::Close(1001, _, false)))
    ));
}
