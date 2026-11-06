use super::*;
use http::HeaderValue;

#[test]
fn route_origin_choice_applies_to_both_socket_transports() {
    use super::super::LuaRouteConfig;

    let target = "/socket".parse::<Uri>().unwrap();
    for (setting, policy) in [
        ("", LuaSocketOriginPolicy::Any),
        ("origin: any\n", LuaSocketOriginPolicy::Any),
        ("origin: same-origin\n", LuaSocketOriginPolicy::SameOrigin),
    ] {
        let route: LuaRouteConfig = serde_yaml::from_str(&format!(
            "application: demo\n{setting}websocket: {{path: /socket}}"
        ))
        .unwrap();
        assert_eq!(route.origin, policy);
        for origin in [None, Some("null"), Some("https://other.test")] {
            assert_eq!(
                allows_origin(
                    route.origin,
                    &request_headers(origin),
                    &target,
                    &Scheme::HTTPS
                ),
                policy == LuaSocketOriginPolicy::Any,
            );
        }
        assert!(allows_origin(
            route.origin,
            &request_headers(Some("https://example.test")),
            &target,
            &Scheme::HTTPS,
        ));
    }
    assert!(
        serde_yaml::from_str::<LuaRouteConfig>(
            "application: demo\norigin: same-orign"
        )
        .is_err()
    );
}

fn request_headers(origin: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, HeaderValue::from_static("example.test"));
    if let Some(origin) = origin {
        headers.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
    }
    headers
}

#[test]
fn h1_and_native_connect_use_the_same_origin_decision() {
    // H1 has an origin-form target; H2 WS and H3 WT carry native authority.
    let targets =
        ["/socket", "https://example.test/socket", "https://example.test/wt"];
    for target in targets {
        let target = target.parse::<Uri>().unwrap();
        for (origin, expected) in [
            (Some("https://example.test"), true),
            (Some("https://EXAMPLE.test:443"), true),
            (Some("http://example.test"), false),
            (Some("https://example.test:444"), false),
            (Some("https://other.test"), false),
            (Some("https://example.test.other.test"), false),
            (None, false),
            (Some("null"), false),
        ] {
            assert_eq!(
                same_origin(&request_headers(origin), &target, &Scheme::HTTPS),
                expected,
                "target={target}, origin={origin:?}"
            );
        }
    }
}

#[test]
fn malformed_or_ambiguous_origin_is_not_a_browser_origin() {
    let target = "/socket".parse::<Uri>().unwrap();
    for origin in [
        "",
        "https://example.test/",
        "https://example.test/path",
        "https://example.test?query",
        "https://example.test#fragment",
        "https://user@example.test",
        "https://example.test:",
        "https://example.test:+443",
        "https://example.test:65536",
        "https://example.test https://other.test",
        "https://example.test,https://other.test",
        " https://example.test",
        "https://example.test ",
        "https://*.test",
    ] {
        assert!(
            !same_origin(
                &request_headers(Some(origin)),
                &target,
                &Scheme::HTTPS
            ),
            "{origin}"
        );
    }
    let mut headers = request_headers(Some("https://example.test"));
    headers.append(
        header::ORIGIN,
        HeaderValue::from_static("https://example.test"),
    );
    assert!(!same_origin(&headers, &target, &Scheme::HTTPS));
    headers.insert(header::ORIGIN, HeaderValue::from_bytes(b"\xff").unwrap());
    assert!(!same_origin(&headers, &target, &Scheme::HTTPS));
}

#[test]
fn requested_alias_and_port_define_the_origin() {
    let mut headers = request_headers(Some("https://alias.test:8443"));
    headers.insert(header::HOST, HeaderValue::from_static("alias.test:8443"));
    assert!(same_origin(&headers, &"/socket".parse().unwrap(), &Scheme::HTTPS));
    headers.insert(
        header::ORIGIN,
        HeaderValue::from_static("https://example.test:8443"),
    );
    assert!(!same_origin(
        &headers,
        &"/socket".parse().unwrap(),
        &Scheme::HTTPS
    ));
}

#[test]
fn physical_scheme_and_default_ports_are_shared_across_transports() {
    let target = "/socket".parse::<Uri>().unwrap();
    let mut headers = request_headers(Some("http://example.test:80"));
    assert!(same_origin(&headers, &target, &Scheme::HTTP));
    assert!(!same_origin(&headers, &target, &Scheme::HTTPS));
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    headers.insert(
        header::ORIGIN,
        HeaderValue::from_static("https://example.test"),
    );
    assert!(!same_origin(&headers, &target, &Scheme::HTTP));
}

#[test]
fn ipv6_addresses_compare_by_value_and_preserve_the_port_boundary() {
    let mut headers = request_headers(Some("https://[::1]:443"));
    headers.insert(header::HOST, HeaderValue::from_static("[0:0:0:0:0:0:0:1]"));
    assert!(same_origin(&headers, &"/socket".parse().unwrap(), &Scheme::HTTPS));
    headers
        .insert(header::ORIGIN, HeaderValue::from_static("https://[::1]:8443"));
    assert!(!same_origin(
        &headers,
        &"/socket".parse().unwrap(),
        &Scheme::HTTPS
    ));
}

#[test]
fn absent_or_duplicate_target_host_fails_closed_without_native_authority() {
    let target = "/socket".parse::<Uri>().unwrap();
    let mut headers = request_headers(Some("https://example.test"));
    headers.append(header::HOST, HeaderValue::from_static("example.test"));
    assert!(!same_origin(&headers, &target, &Scheme::HTTPS));
    headers.remove(header::HOST);
    assert!(!same_origin(&headers, &target, &Scheme::HTTPS));
    // A native authority is sufficient; canonical host fallback is not used.
    assert!(same_origin(
        &headers,
        &"https://example.test/socket".parse().unwrap(),
        &Scheme::HTTPS
    ));
}
