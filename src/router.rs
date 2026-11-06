//! `claviron.router`: paths to handlers, matched in Rust.
//!
//! ```lua
//! local router = claviron.router()
//! router:get("/users/{id}", function(request, params) return "user " .. params.id end)
//! router:post("/users", create)
//! return router                        -- it has `fetch`, so it serves an application
//! ```
//!
//! Paths use `matchit`'s syntax, as axum's routes do: `{name}` is one segment,
//! `{*rest}` the remainder. A path matched under another method is `405` with
//! `Allow`; no match is `404`. `HEAD` falls back to the `GET` route.

use std::collections::BTreeMap;

use mlua::{
    AnyUserData, Function, Lua, Table, UserData, UserDataMethods, Value,
};

use crate::{Execution, Extension};

/// `claviron.router`.
pub struct RouterExtension;

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("router: {error}"))
}

impl Extension for RouterExtension {
    fn name(&self) -> &'static str {
        "claviron.router"
    }

    fn global(&self) -> Option<&'static str> {
        Some("router")
    }

    fn module(
        &self,
        lua: &Lua,
        _execution: &std::sync::Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        module
            .set("new", lua.create_function(|_, ()| Ok(Router::default()))?)?;
        let meta = lua.create_table()?;
        meta.set(
            "__call",
            lua.create_function(|_, _: Table| Ok(Router::default()))?,
        )?;
        module.set_metatable(Some(meta))?;
        Ok(module)
    }
}

/// Methods to routes, each route an index into the handlers. `*` is `any`.
#[derive(Default)]
pub(crate) struct Router {
    methods: BTreeMap<String, matchit::Router<usize>>,
    handlers: Vec<Function>,
    /// `router:websocket({ open, message, close })`: what an application
    /// serving this router does with its sockets.
    pub(crate) websocket: Option<Table>,
    pub(crate) webtransport: Option<Table>,
}

/// What a request finds.
enum Found {
    Handler(Function, Vec<(String, String)>),
    NotAllowed(Vec<String>),
    Nowhere,
}

impl Router {
    fn add(
        &mut self,
        method: &str,
        path: &str,
        handler: Function,
    ) -> mlua::Result<()> {
        self.methods
            .entry(method.to_owned())
            .or_default()
            .insert(path, self.handlers.len())
            .map_err(|error| failure(format!("{method} {path}: {error}")))?;
        self.handlers.push(handler);
        Ok(())
    }

    fn find(
        &self,
        method: &str,
        path: &str,
    ) -> Found {
        let fallback = (method == "HEAD").then_some("GET");
        for candidate in
            [Some(method), fallback, Some("*")].into_iter().flatten()
        {
            if let Some(matched) = self
                .methods
                .get(candidate)
                .and_then(|routes| routes.at(path).ok())
            {
                let params = matched
                    .params
                    .iter()
                    .map(|(name, value)| (name.to_owned(), value.to_owned()))
                    .collect();
                return Found::Handler(
                    self.handlers[*matched.value].clone(),
                    params,
                );
            }
        }
        let allowed: Vec<String> = self
            .methods
            .iter()
            .filter(|(_, routes)| routes.at(path).is_ok())
            .map(|(method, _)| method.clone())
            .collect();
        if allowed.is_empty() {
            Found::Nowhere
        } else {
            Found::NotAllowed(allowed)
        }
    }
}

impl UserData for Router {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        for (name, method) in [
            ("get", "GET"),
            ("post", "POST"),
            ("put", "PUT"),
            ("patch", "PATCH"),
            ("delete", "DELETE"),
            ("options", "OPTIONS"),
            ("head", "HEAD"),
            ("any", "*"),
        ] {
            // Returns the router, so routes chain.
            methods.add_function(
                name,
                move |_,
                      (this, path, handler): (
                    AnyUserData,
                    String,
                    Function,
                )| {
                    this.borrow_mut::<Router>()?.add(method, &path, handler)?;
                    Ok(this)
                },
            );
        }
        methods.add_function(
            "websocket",
            |_, (this, handlers): (AnyUserData, Table)| {
                this.borrow_mut::<Router>()?.websocket = Some(handlers);
                Ok(this)
            },
        );
        methods.add_function(
            "webtransport",
            |_, (this, handlers): (AnyUserData, Table)| {
                this.borrow_mut::<Router>()?.webtransport = Some(handlers);
                Ok(this)
            },
        );
        methods.add_function(
            "route",
            |_,
             (this, method, path, handler): (
                AnyUserData,
                String,
                String,
                Function,
            )| {
                this.borrow_mut::<Router>()?.add(
                    &method.to_ascii_uppercase(),
                    &path,
                    handler,
                )?;
                Ok(this)
            },
        );
        methods.add_async_function(
            "fetch",
            |lua, (this, request): (AnyUserData, Table)| async move {
                let method: String = request.get("method")?;
                let path: String = request.get("path")?;
                // The borrow ends before the handler runs: a handler may add
                // routes, and another request may be routed meanwhile.
                let found = this.borrow::<Router>()?.find(&method, &path);
                match found {
                    Found::Handler(handler, params) => {
                        let table = lua.create_table()?;
                        for (name, value) in params {
                            table.set(name, value)?;
                        }
                        handler.call_async::<Value>((request, table)).await
                    }
                    Found::NotAllowed(allowed) => {
                        // `any` would have matched: only named methods are here.
                        let allow = allowed.join(", ");
                        let reply = lua.create_table()?;
                        reply.set("status", 405)?;
                        let headers = lua.create_table()?;
                        headers.set("allow", allow)?;
                        reply.set("headers", headers)?;
                        Ok(Value::Table(reply))
                    }
                    Found::Nowhere => {
                        let reply = lua.create_table()?;
                        reply.set("status", 404)?;
                        Ok(Value::Table(reply))
                    }
                }
            },
        );
    }
}
