//! `claviron.fetch`: HTTP out of a script, to the hosts `lua.yaml` allows.
//!
//! ```lua
//! local response = claviron.fetch("https://api.example.com/orders/7", {
//!   method = "POST", headers = { ["content-type"] = "application/json" },
//!   body = claviron.json.encode({ paid = true }),
//! })
//! if response.ok then print(response.status, response.body) end
//! ```
//!
//! ```yaml
//! # lua.yaml
//! fetch:
//!   allow: [api.example.com, "*.internal"]   # unwritten: no fetch; "*": any host
//!   ca: [certs/internal-ca.pem]              # trusted besides the system's
//!   max_body_size: 10485760                  # bytes; unwritten, no bound
//! ```
//!
//! Closed unless the operator opens it: a script reaches only the hosts
//! listed, and a redirect elsewhere is refused. A request waits for its
//! answer or for the script to stop; nothing here invents a timeout.
//! Certificates are the system's trust store and the listed CAs.

use std::path::PathBuf;
use std::sync::Arc;

use mlua::{Lua, Table, Value};
use serde::Deserialize;

use crate::{Execution, Extension};

/// `lua.yaml`'s `fetch:` section.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FetchConfig {
    allow: Vec<String>,
    ca: Vec<PathBuf>,
    max_body_size: Option<usize>,
}

/// How many redirects one request follows: reqwest's own default policy's
/// limit, kept because a custom policy has to state its own.
const REDIRECTS: usize = 10;

/// The hosts a script may reach.
#[derive(Clone, Debug)]
struct Allow(Arc<Vec<Rule>>);

#[derive(Debug)]
enum Rule {
    Any,
    Host(String),
    Suffix(String),
}

impl Allow {
    fn new(rules: &[String]) -> Self {
        Self(Arc::new(
            rules
                .iter()
                .map(|rule| {
                    let rule = rule.trim().to_ascii_lowercase();
                    if rule == "*" {
                        Rule::Any
                    } else if let Some(suffix) = rule.strip_prefix("*.") {
                        Rule::Suffix(format!(".{suffix}"))
                    } else {
                        Rule::Host(rule)
                    }
                })
                .collect(),
        ))
    }

    fn permits(
        &self,
        host: &str,
    ) -> bool {
        let host = host.to_ascii_lowercase();
        self.0.iter().any(|rule| match rule {
            Rule::Any => true,
            Rule::Host(allowed) => *allowed == host,
            Rule::Suffix(suffix) => host.ends_with(suffix.as_str()),
        })
    }
}

/// The client and its rules, built when the configuration is read.
pub struct Fetcher {
    client: reqwest::Client,
    allow: Allow,
    max_body_size: Option<usize>,
}

impl Fetcher {
    /// `None` when the section allows nothing: fetch is closed.
    pub fn new(config: &FetchConfig) -> Result<Option<Arc<Self>>, String> {
        if config.allow.is_empty() {
            return Ok(None);
        }
        let allow = Allow::new(&config.allow);
        let mut certificates = Vec::new();
        for path in &config.ca {
            let pem = std::fs::read(path).map_err(|error| {
                format!("fetch.ca {}: {error}", path.display())
            })?;
            certificates.extend(
                reqwest::Certificate::from_pem_bundle(&pem).map_err(
                    |error| format!("fetch.ca {}: {error}", path.display()),
                )?,
            );
        }
        let redirects = allow.clone();
        let client = reqwest::Client::builder()
            .tls_certs_merge(certificates)
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= REDIRECTS {
                    return attempt.error("too many redirects");
                }
                match attempt.url().host_str() {
                    Some(host) if redirects.permits(host) => attempt.follow(),
                    _ => attempt
                        .error("a redirect to a host fetch does not allow"),
                }
            }))
            .build()
            .map_err(|error| format!("fetch: {error}"))?;
        Ok(Some(Arc::new(Self {
            client,
            allow,
            max_body_size: config.max_body_size,
        })))
    }
}

pub struct FetchExtension {
    fetcher: Arc<Fetcher>,
}

impl FetchExtension {
    pub fn new(fetcher: Arc<Fetcher>) -> Self {
        Self { fetcher }
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("fetch: {error}"))
}

impl Extension for FetchExtension {
    fn name(&self) -> &'static str {
        "claviron.fetch"
    }

    fn global(&self) -> Option<&'static str> {
        Some("fetch")
    }

    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let fetcher = Arc::clone(&self.fetcher);
        let execution = Arc::clone(execution);
        let request = lua.create_async_function(
            move |lua, (_, url, options): (Table, String, Option<Table>)| {
                let fetcher = Arc::clone(&fetcher);
                let execution = Arc::clone(&execution);
                async move {
                    let request = build(&fetcher, &url, options)?;
                    let answered = tokio::select! {
                        answered = fetch(&fetcher, request) => answered?,
                        () = execution.stopping().cancelled() => {
                            return Err(failure("the request ended: this script is stopping"));
                        }
                    };
                    response(&lua, answered)
                }
            }
        )?;
        let meta = lua.create_table()?;
        meta.set("__call", request)?;
        module.set_metatable(Some(meta))?;
        Ok(module)
    }
}

fn build(
    fetcher: &Fetcher,
    url: &str,
    options: Option<Table>,
) -> mlua::Result<reqwest::Request> {
    let url = reqwest::Url::parse(url)
        .map_err(|error| failure(format!("{url}: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(failure(format!("{url}: only http and https")));
    }
    let host =
        url.host_str().ok_or_else(|| failure(format!("{url}: no host")))?;
    if !fetcher.allow.permits(host) {
        return Err(failure(format!("{host} is not a host fetch allows")));
    }
    let mut method = reqwest::Method::GET;
    let mut builder_headers = reqwest::header::HeaderMap::new();
    let mut body = None;
    if let Some(options) = options {
        if let Some(name) = options.get::<Option<String>>("method")? {
            method = reqwest::Method::from_bytes(
                name.to_ascii_uppercase().as_bytes(),
            )
            .map_err(|_| failure(format!("{name} is not a method")))?;
        }
        if let Some(headers) = options.get::<Option<Table>>("headers")? {
            for pair in headers.pairs::<String, String>() {
                let (name, value) = pair?;
                let name =
                    reqwest::header::HeaderName::from_bytes(name.as_bytes())
                        .map_err(|_| {
                            failure(format!("{name} is not a header name"))
                        })?;
                let value = reqwest::header::HeaderValue::from_str(&value)
                    .map_err(|_| {
                        failure(format!(
                            "the {name} header's value is not valid"
                        ))
                    })?;
                builder_headers.append(name, value);
            }
        }
        body = options
            .get::<Option<mlua::LuaString>>("body")?
            .map(|body| body.as_bytes().to_vec());
    }
    let mut request = reqwest::Request::new(method, url);
    *request.headers_mut() = builder_headers;
    if let Some(body) = body {
        *request.body_mut() = Some(body.into());
    }
    Ok(request)
}

struct Answer {
    status: u16,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

async fn fetch(
    fetcher: &Fetcher,
    request: reqwest::Request,
) -> mlua::Result<Answer> {
    let mut response =
        fetcher.client.execute(request).await.map_err(failure)?;
    if let (Some(limit), Some(length)) =
        (fetcher.max_body_size, response.content_length())
        && length as usize > limit
    {
        return Err(failure(format!(
            "the body is {length} bytes; fetch allows {limit}"
        )));
    }
    let status = response.status().as_u16();
    let url = response.url().to_string();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(failure)? {
        body.extend_from_slice(&chunk);
        if let Some(limit) = fetcher.max_body_size
            && body.len() > limit
        {
            return Err(failure(format!(
                "the body passed the {limit} bytes fetch allows"
            )));
        }
    }
    Ok(Answer { status, url, headers, body })
}

/// `{ status, ok, url, headers, body }`; a header sent more than once is its
/// values joined with ", ", as HTTP allows for most.
fn response(
    lua: &Lua,
    answer: Answer,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("status", answer.status)?;
    table.set("ok", (200..300).contains(&answer.status))?;
    table.set("url", answer.url)?;
    let headers = lua.create_table()?;
    for (name, value) in answer.headers {
        let joined = match headers.get::<Value>(name.as_str())? {
            Value::String(previous) => {
                format!("{}, {value}", previous.to_str()?)
            }
            _ => value,
        };
        headers.set(name, joined)?;
    }
    table.set("headers", headers)?;
    table.set("body", lua.create_string(&answer.body)?)?;
    Ok(table)
}

#[cfg(test)]
#[path = "../tests/fetch.rs"]
mod tests;
