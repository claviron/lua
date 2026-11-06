# claviron-lua

Lua 5.5 scripting and managed applications embedded in Claviron.

The engine runs text scripts with explicit Rust-provided capabilities. Short
scripts get a fresh state per execution; HTTP applications retain one state per
application per worker. Those are different lifetime contracts, not a pool of
interchangeable request states.

Applications use this crate beside `claviron` and call `claviron_lua::register`,
plus `claviron_lua::edge::register` for host-selected routes. Lua is
vendored through `mlua`: the build compiles its interpreter rather than depending
on a system Lua installation. This crate is an engine, not a standalone runner
binary, and does not require Edge to execute scripts.

## Script execution and extensions

`Engine` holds registered extensions, a module search path, and an explicit
execution environment. `Script` carries a name, source, arguments, and optional
output callback. `Engine::execute` creates a state, runs the script as a future,
and drops its state when execution finishes.

The `Extension` trait supplies a named module to `require`; it builds a table
against the current Lua state and `Execution`. An extension can also expose its
module under the `claviron` global. Register extensions once on the engine, not
by performing runtime discovery inside each script operation.

`Engine::new()` is a minimal engine. The managed `host::engine` builder installs
JSON, regex, and routing support, plus enabled capabilities for which the host
has supplied runtime resources. A low-level caller can choose its own extensions.

The sandbox opens selected standard libraries: base, string, table, math, UTF-8,
and coroutines. It removes `dofile` and `loadfile`, restricts `load` to text, and
does not expose `io`, `os`, or `debug`. `require` searches registered extensions,
bundled pure-Lua packages, and explicitly configured source directories, in that
order. It does not load arbitrary C modules or use the host's `package.path`.

This is an in-process capability boundary, not process isolation. Async waits
can yield; CPU-bound Lua is still CPU-bound. Cancellation wakes registered waits
and lets execution finish cooperatively; it is not preemption of arbitrary Lua
code and does not create an implicit execution deadline.

## Managed HTTP applications

`lua.yaml` declares application modules and source search directories. Relative
paths are process-working-directory-relative, not relative to the YAML file.
A minimal application declaration is:

```yaml
applications:
  example:
    module: apps/example.lua
```

The module returns a value with a `fetch` method, including a `claviron.router`:

```lua
local router = claviron.router()
router:get("/hello/{name}", function(request, params)
  return "Hello " .. params.name
end)
return router
```

Declaring the application does not mount an HTTP path by itself. Edge or another
HTTP consumer supplies typed Lua route declarations naming that application.
The `app` implementation surface owns the raw `Request<RouteBody>` handler.
Host routing and HTTP upgrade admission belong to the consumer, not the engine.

`serve::LuaApplicationAdapter` is the native entry for a named managed
application. It retains the prepared service and route policy, acquires the
current application generation for each call, and forwards `Request<RouteBody>`
without adapting or collecting its body. The optional `edge` module registers
its BackendFactory and H1/H2 WebSocket or H3 WebTransport contributions. The
facade selects this integration when both Edge and Lua are enabled; native
engine and Runner use remain Edge-free. Lua has no Axum dependency or binding.

Requests carry method, path, query, headers, and body-reading methods. Responses
can be strings or tables with status, headers, and a body; a function-valued body
produces streamed chunks until it returns `nil`. Route request/response buffering
policy determines buffering at the HTTP boundary.

Globals and loaded modules survive between application requests. Keep
request-specific values in locals. `concurrency` bounds in-flight coroutines
and defaults to one; `max_waiting` bounds the permit queue and defaults to 256.
Excess queued work is rejected rather than growing an unbounded backlog.
Concurrency overlaps waiting coroutines; it does not make Lua computation
parallel within one state. A streamed reply retains its permit through completion.

The application manager reports runtime `lua` and, by default, framework `Olua`.
Start, stop, restart, and reload operate on application generations. Replacement
loads a new state before publication; a failed replacement leaves the previous
state serving. Requests using a retired state finish there. An explicitly stopped
application stays stopped across reloads.

## Shared capabilities and configuration

The root facade composes the engine's local features rather than exposing every
implementation feature as a separate application choice:

| Facade selection | Capability |
| --- | --- |
| `lua` | Engine, managed configuration/applications, and Orbit integration. |
| `lua` + `tasks` | Lua scripts as task-service executions. |
| `lua` + `websocket` | HTTP/1 Upgrade and HTTP/2 extended-CONNECT socket handlers. |
| `lua` + `webtransport` | Reliable HTTP/3 WebTransport stream handlers. |
| `lua` | Includes outbound HTTP, subject to `lua.yaml` host policy. |
| `lua` + `postgres`, `mysql` or `sqlite` | Query adapters using Claviron's configured database pools. |
| `lua` + `log-pipeline` | Records to declared log channels. |

Orbit extensions provide cells, streams, events, and invocations over the host's
registered resources. Shared state belongs there, not in another script's Lua
globals. Bounded event and invocation history is not a durable exactly-once queue.

Managed applications and tasks also receive `claviron.process` (or
`require "claviron.process"`): `role` is `master`, `worker` or `standalone`, and
`worker_id` is the owning process's one-based worker slot, or `nil` outside a
worker. This host-supplied snapshot does not read environment variables and
does not identify a user or a WebTransport session. Worker slots are local to
one process family, not globally unique identities.

`router:webtransport(handlers)` uses the same `open`, `message` and `close`
callbacks as `router:websocket(handlers)`. Both can share one handlers table.
Edge binds the route's declared `websocket` endpoints for either selected
transport; WebTransport additionally requires WebTransport callbacks. Each
socket exposes `transport` and `http_version` from the accepting server path.
WebTransport accepts client-opened reliable bidirectional streams carrying
Claviron Text/Binary/Close records (`u8 kind`, little-endian `u32 length`,
payload); it does not carry WebSocket frames. Datagram handling is not included.
The application's `webtransport` configuration uses the same `max_message_size`
and `send_queue` settings as `websocket`. No new facade feature is needed.
Each stream has separate `open/message/close` callbacks, bounded queues and
independent reads and writes. `socket.id` identifies a stream; `session_id`
groups its siblings within this worker (it is not the wire CONNECT ID).
Closing one socket leaves its siblings and the session available. Active stream
handlers are bounded by the accepting listener's configured QUIC bidi limit;
the native driver's pending input queue remains bounded too. Lua callbacks
retain the application's `concurrency` limit, so a value of one serializes
callbacks even though transport IO progresses independently.
CONNECT closure, application stop and native retirement stop new admission.
Shutdown closes outbound admission and delivers callback events without waiting for a slow
writer; accepted output drains before the close record. The session owns and
joins every admitted stream and both directions, without adding a drain deadline.

With `orbit`, `socket:listen(claviron.events.listen("example.chat"))` attaches
one subscription to that socket's existing callback loop. Its optional
`event(socket, event)` handler receives the usual `{ id, topic, payload }`.
Publish once through `claviron.events`; a separate local broadcast would
duplicate delivery. This remains a bounded event bus, not a durable queue.
Use a separate listener for each socket; do not also call `next()` on it.
`socket:listen(nil)` detaches it. Waiting consumes no application permit;
callbacks share the application's concurrency limit.

The socket owner detaches the listener before its final `close` callback,
including on application shutdown. No detached listener task is spawned.
A retired application generation retains its accepted sockets and their
subscriptions until they close; replacement does not silently terminate them.

Outbound fetch is closed unless `fetch.allow` permits a destination; redirects
are checked too. Its pooled client and optional response-size bound come from
configuration. Host allow rules are not a complete network-isolation mechanism.
Database statements use bind parameters and configured pools.
SQLite uses `claviron.db.sqlite(name)` on the configured pools, with the same
`query`, `first`, `execute` and callback-based `transaction` methods. Parameters
use `?`; result values retain SQLite's storage classes. Transactions accept
serializable isolation; other isolation levels and per-transaction `read_only`
are rejected rather than silently ignored.
If SQLite automatically rolls back a transaction after an error, its Lua
transaction handle rejects further operations and cannot report a successful
commit. Errors that leave the native transaction active remain recoverable.
Unsupported SQL value types fail explicitly rather than receiving guessed Lua
representations.

Application `env_file` and inline `env` declarations prepare detached execution
values through the shared [Dotenw](https://crates.io/crates/dotenw) infrastructure.
`claviron.getenv` reads those values, not the server's process environment. An
environment update replaces only the affected application's state; invalid
replacement data leaves the previous generation serving.

## Tools and current boundaries

The managed engine installs `claviron.json`, Rust-backed byte regexes, and the
router. Bundled `inspect`, `lume`, `middleclass`, and `serpent` modules require no
runtime installation; functions needing unavailable standard libraries still
cannot bypass the sandbox. Their sources retain their own MIT notices.

[claviron.d.lua](claviron.d.lua) supplies lua-language-server annotations for
editor completion. This is tooling metadata, not a runtime module.

WebSocket behavior is opt-in and uses declared endpoint paths and queue/message
limits. WebTransport and a dedicated Lua runner owner are not implemented by
this crate. Compiling an optional capability does not configure its external
resources or mount it publicly.

## Related crates

- [App](../app/README.md): managed application identity and lifecycle.
- [Tasks](../tasks/README.md): triggers, placement, and execution policy.
- [Orbit](../orbit/README.md): shared state and same-host coordination.
- [HTTP](../http/README.md): transport-neutral policy and routing declarations.
- [DB](../db/README.md): runtime-owned connection pools.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
Bundled third-party Lua sources retain their own license notices.


### Cell revision tokens

Cell `version()` and `wait()` return opaque integer tokens. Pass a token back
unchanged; do not increment it or convert it through a float. Text cells retain
all 64 revision bits in the language's signed 64-bit integer, so a valid token
can be negative. Scalar cell tokens remain unsigned 32-bit counts represented
as nonnegative integers; out-of-range scalar wait tokens are rejected.
