use claviron_edge::HostConfig;

/// The endpoint prepared for `application` at `path`, as a registry would
/// keep it.
fn endpoint(
    host: &HostConfig,
    path: &str,
    application: &str,
) -> Option<super::super::Endpoint> {
    super::super::socket_endpoints(host).unwrap().into_iter().find(|endpoint| {
        &*endpoint.path == path && &*endpoint.application == application
    })
}

#[test]
fn lua_webtransport_rejects_removed_or_reassigned_socket_bindings() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /lua:\n    lua: { application: demo, websocket: { path: /socket } }",
    ).unwrap();
    let bound = endpoint(&host, "/lua/socket", "demo").unwrap();
    assert!(bound.still_declared(&host));
    assert!(endpoint(&host, "/lua/socket/extra", "demo").is_none());
    assert!(endpoint(&host, "/lua/socket", "other").is_none());

    // The registry still holds the binding after a reload took the route
    // away or gave the path to another application.
    let removed: HostConfig = serde_yaml::from_str("root: public").unwrap();
    assert!(!bound.still_declared(&removed));
    let reassigned: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /lua:\n    lua: { application: other, websocket: { path: /socket } }",
    ).unwrap();
    assert!(!bound.still_declared(&reassigned));
}

#[test]
fn lua_webtransport_respects_more_specific_route_ownership() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /lua:\n    lua: { application: demo, websocket: { path: /socket } }\n  /lua/socket:\n    lua: { application: other, websocket: { path: / } }",
    ).unwrap();
    // Both scopes declare the path; only the more specific one owns it.
    assert!(
        !endpoint(&host, "/lua/socket", "demo").unwrap().still_declared(&host)
    );
    assert!(
        endpoint(&host, "/lua/socket", "other").unwrap().still_declared(&host)
    );
}

#[test]
fn lua_webtransport_uses_the_effective_routes_origin_policy() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /lua:\n    lua: { application: demo, origin: any, websocket: { path: /socket } }\n  /lua/socket:\n    lua: { application: demo, origin: same-origin, websocket: { path: / } }",
    ).unwrap();
    let owners: Vec<_> = super::super::socket_endpoints(&host)
        .unwrap()
        .into_iter()
        .filter(|endpoint| {
            &*endpoint.path == "/lua/socket" && endpoint.still_declared(&host)
        })
        .collect();
    assert_eq!(owners.len(), 1);
    assert_eq!(
        owners[0].origin,
        super::super::LuaSocketOriginPolicy::SameOrigin
    );
}
