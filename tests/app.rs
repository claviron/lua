use std::sync::Arc;

use claviron_http::policy::HttpPolicy;
use claviron_http::{RouteBody, boxed_body, bytes_body};
use http_body_util::BodyExt;

use super::*;

async fn load(fixture: &str) -> Arc<LuaApplication> {
    let module = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/app")
        .join(fixture);
    let engine = Engine::new()
        .with(Arc::new(crate::RouterExtension))
        .with(Arc::new(crate::JsonExtension));
    let config = ApplicationConfig {
        module,
        frameworks: None,
        icon: None,
        env_file: Vec::new(),
        env: Default::default(),
        concurrency: 1,
        max_waiting: 4,
        websocket: SocketConfig::default(),
        webtransport: SocketConfig::default(),
    };
    Arc::new(
        LuaApplication::load(&engine, "test", &config).await.expect("load"),
    )
}

/// A body that arrives in these pieces, as a client's upload does.
fn pieces(pieces: &[&'static str]) -> RouteBody {
    let (sender, receiver) = mpsc::channel(pieces.len());
    for piece in pieces {
        sender.try_send(Ok(Bytes::from_static(piece.as_bytes()))).unwrap();
    }
    boxed_body(ReplyStream(receiver))
}

async fn send(
    application: &Arc<LuaApplication>,
    method: &str,
    uri: &str,
    body: RouteBody,
) -> (u16, http::HeaderMap, Vec<Bytes>) {
    let request = http::Request::builder()
        .method(method)
        .uri(uri)
        .header("x-seen", "a")
        .header("x-seen", "b")
        .body(body)
        .unwrap();
    let response = application.handle(request, HttpPolicy::unlimited()).await;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let mut body = response.into_body();
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.unwrap().into_data() {
            frames.push(data);
        }
    }
    (status, headers, frames)
}

fn text(frames: &[Bytes]) -> String {
    frames.iter().map(|frame| String::from_utf8_lossy(frame)).collect()
}

/// The module runs once and its router serves every request: parameters, the
/// request as the handler sees it, state kept between requests, `404`, `405`
/// with `Allow`, a string reply, and a handler's error as `500` that leaves
/// the state serving.
#[tokio::test(flavor = "current_thread")]
async fn a_router_module_serves_requests_from_one_state() {
    let application = load("router.lua").await;
    let get = |uri: &'static str| {
        let application = Arc::clone(&application);
        async move {
            send(
                &application,
                "GET",
                uri,
                bytes_body(Bytes::from_static(b"payload")),
            )
            .await
        }
    };

    let (status, headers, frames) = get("/users/7?full=1").await;
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_str(&text(&frames)).unwrap();
    assert_eq!(body["id"], "7");
    assert_eq!(body["served"], 1);
    assert_eq!(body["query"], "full=1");
    assert_eq!(body["seen"], "a, b");
    assert_eq!(body["body"], "payload");
    assert_eq!(headers.get_all("set-cookie").iter().count(), 2);

    assert!(
        text(&get("/users/8").await.2).contains("\"served\":2"),
        "state stays between requests"
    );
    let (_, _, created) =
        send(&application, "POST", "/users", bytes_body(Bytes::new())).await;
    assert_eq!(text(&created), "created");
    assert_eq!(get("/nowhere").await.0, 404);
    let (status, headers, _) =
        send(&application, "DELETE", "/users/7", bytes_body(Bytes::new()))
            .await;
    assert_eq!(
        (status, headers.get("allow").unwrap().to_str().unwrap()),
        (405, "GET")
    );
    assert_eq!(get("/fail").await.0, 500);
    let (status, _, _) =
        send(&application, "HEAD", "/users/9", bytes_body(Bytes::new())).await;
    assert_eq!(status, 200, "HEAD falls back to the GET route");
}

/// A body that streams reaches the handler piece by piece, and a reply whose
/// body is a function leaves as the pieces it returns.
#[tokio::test(flavor = "current_thread")]
async fn bodies_stream_both_ways() {
    let application = load("stream.lua").await;
    let (status, _, frames) =
        send(&application, "POST", "/echo", pieces(&["a", "b", "c"])).await;
    assert_eq!(status, 200);
    assert_eq!(
        frames,
        vec![Bytes::from("A"), Bytes::from("B"), Bytes::from("C")]
    );
}
