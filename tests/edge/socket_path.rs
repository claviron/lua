use claviron_http::ext::PathPrefix;

use super::{mounted_websocket_path, socket_endpoints};

fn prefix(value: &str) -> PathPrefix {
    serde_yaml::from_str(value).unwrap()
}

#[test]
fn native_socket_selection_rejects_removed_and_masked_lua_endpoints() {
    let host: claviron_edge::HostConfig = serde_yaml::from_str(
        "lua: {application: demo, websocket: {path: /socket}}\nroutes:\n  /socket: {static: {}}\n",
    ).unwrap();
    // The root declares the endpoint, but a more specific route without Lua
    // owns its path.
    let endpoints = socket_endpoints(&host).unwrap();
    let [declared] = endpoints.as_slice() else {
        panic!("one declared endpoint");
    };
    assert_eq!(&*declared.path, "/socket");
    assert!(!declared.still_declared(&host));
    let removed = claviron_edge::HostConfig::new("public");
    assert!(!declared.still_declared(&removed));
}

#[test]
fn websocket_path_is_relative_to_the_application_mount() {
    let mount = prefix("/api");
    let socket = prefix("/socket");
    assert_eq!(
        mounted_websocket_path(Some(&mount), &socket).as_ref(),
        "/api/socket"
    );
    assert_eq!(mounted_websocket_path(None, &socket).as_ref(), "/socket");

    let root = prefix("/");
    assert_eq!(mounted_websocket_path(Some(&mount), &root).as_ref(), "/api");
}
