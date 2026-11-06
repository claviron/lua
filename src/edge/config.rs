//! The `lua:` route directive. The edge keeps a route's declaration as it was
//! written; this crate owns its shape and decodes it when a host is prepared.

use claviron_helpers::de::opt_one_or_many;
use claviron_http::config::routes::JavaScriptWebSocketConfig;
use serde::Deserialize;

/// Browser-origin admission for both socket transports of a Lua route.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LuaSocketOriginPolicy {
    /// No Origin restriction, including for clients without the header.
    #[default]
    Any,
    /// Require one HTTP(S) Origin matching the accepted scheme and authority.
    SameOrigin,
}

/// One route mapping to a Lua application declared in `lua.yaml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LuaRouteConfig {
    /// The application's name in `lua.yaml`.
    #[serde(deserialize_with = "deserialize_lua_application_name")]
    pub application: String,
    /// Shared WS/WT admission; omission imposes no Origin restriction.
    #[serde(default)]
    pub origin: LuaSocketOriginPolicy,
    /// Exact WebSocket endpoints relative to this application's mount.
    #[serde(
        default,
        alias = "websocket",
        deserialize_with = "opt_one_or_many"
    )]
    websockets: Option<Vec<JavaScriptWebSocketConfig>>,
}

impl LuaRouteConfig {
    /// Its WebSocket endpoints; the same shape a JavaScript route's are.
    pub fn websockets(&self) -> &[JavaScriptWebSocketConfig] {
        self.websockets.as_deref().unwrap_or(&[])
    }
}

fn deserialize_lua_application_name<'de, D>(
    deserializer: D
) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let name = String::deserialize(deserializer)?;
    if name.trim().is_empty() {
        return Err(serde::de::Error::custom(
            "Lua route application must not be empty",
        ));
    }
    Ok(name)
}
