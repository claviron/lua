use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{Engine, Script};

/// A server that answers each connection by its request line: `/ok` echoes
/// the method and the body, `/away` redirects to a host fetch does not allow.
async fn server() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut request = vec![0; 4096];
                let read = stream.read(&mut request).await.unwrap();
                let request =
                    String::from_utf8_lossy(&request[..read]).into_owned();
                let line =
                    request.lines().next().unwrap_or_default().to_owned();
                let body = request
                    .split("\r\n\r\n")
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                let reply = if line.contains("/away") {
                    "HTTP/1.1 302 Found\r\nlocation: http://elsewhere.invalid/\r\ncontent-length: 0\r\n\r\n".to_owned()
                } else {
                    let text = format!(
                        "{} {}",
                        line.split(' ').next().unwrap_or_default(),
                        body
                    );
                    format!(
                        "HTTP/1.1 200 OK\r\nx-seen: one\r\nx-seen: two\r\ncontent-length: {}\r\n\r\n{text}",
                        text.len()
                    )
                };
                let _ = stream.write_all(reply.as_bytes()).await;
            });
        }
    });
    address
}

/// The allowed host answers; a host not allowed, and a redirect to one, are
/// refused — the first before any request is made.
#[tokio::test(flavor = "current_thread")]
async fn fetch_reaches_only_allowed_hosts() {
    let address = server().await;
    let config: FetchConfig =
        serde_yaml::from_str("allow: [127.0.0.1]").unwrap();
    let fetcher = Fetcher::new(&config).unwrap().expect("allow opens fetch");
    let mut script =
        Script::new("fetch.lua", include_str!("fixtures/fetch/fetch.lua"));
    script.args = vec![format!("http://{address}")];
    Engine::new()
        .with(Arc::new(FetchExtension::new(fetcher)))
        .execute(script, &CancellationToken::new())
        .await
        .unwrap();
}

/// Nothing allowed is nothing opened.
#[test]
fn fetch_is_closed_unless_allowed() {
    assert!(Fetcher::new(&FetchConfig::default()).unwrap().is_none());
}
