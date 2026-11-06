use super::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use http_body::{Body, Frame};

struct UnreadBody(Arc<AtomicUsize>);

impl Body for UnreadBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        panic!("an unavailable application must not read the request body");
    }
}

impl Drop for UnreadBody {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn unavailable_application_releases_the_unread_body_and_returns_503() {
    let drops = Arc::new(AtomicUsize::new(0));
    let adapter = LuaApplicationAdapter::new(
        None,
        "example",
        HttpPolicyOverride::default(),
    );
    let request =
        Request::new(claviron_http::boxed_body(UnreadBody(drops.clone())));
    let mut call = Box::pin(adapter.handle(request, None));
    let Poll::Ready(response) =
        call.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!(
            "unavailable applications must answer without engine or body work"
        );
    };
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[http::header::RETRY_AFTER], "1");
    drop(call);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
