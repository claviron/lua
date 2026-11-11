# Lua architecture

`claviron-lua` embeds Lua 5.5 in Claviron and makes it easy to extend: a Rust
crate adds a module Lua scripts `require`, the way a `claviron-js` extension
adds a `claviron:*` module. Claviron's own first extension is Orbit — a
fleet's shared state, reachable from a script as plainly as a local value.

Status: in use — `lua:` tasks, the extensions below, and applications.

## What it is, and what it is not

- **An engine, not a runner.** There is no Lua application process and no
  runner shell for one; an application runs inside a host (see Applications). Lua runs inside whatever hosts it — the edge, a
  language runner, a downstream binary — behind a `lua` feature. A runner
  that wants scripting takes the feature; one that does not carries nothing.
- **The exception to "no more embedding".** Other languages join the fleet as
  processes of their own. Lua does not need to: its VM is small, a state
  belongs to one thread, there is no global interpreter lock, and a state can
  be sandboxed. What scripts share lives in Orbit, not in a state, so the
  fleet model holds.
- Not yet: signed bytecode over the wire, the browser build, edge scripting
  phases. They come after the engine and its first extension, and each gets
  its own section here when it is designed.

## Optional Edge integration

`edge.rs` registers the configured BackendFactory. Its prepared owners select
mounts and forward to `LuaApplicationAdapter`; an absent declaration installs no
HTTP action. The owner declares no identity export and execution-owned replies
through the common Backend contract; it copies its scope's policy from the
call's `HostContext`, and the route's own policy narrows that.

`edge/config.rs` owns the `lua:` route directive: `LuaRouteConfig` and its
Origin policy (`any`, the default, imposes no restriction; `same-origin` asks
for strict browser-origin admission, enforced by the native acceptance
handlers, not by Lua callbacks or HTTP CORS). Edge keeps a route's directive
as it was written and knows nothing of its shape; this crate decodes it once,
when a host is prepared, for the HTTP backend and for the socket bindings.

`edge/websocket.rs` and `edge/webtransport.rs` own native session adapters;
`edge/origin.rs` shares browser-origin validation. A cold Routable contribution
prepares session bindings for matching hosts. Each binding carries the
endpoint it was prepared from, so admitting a socket decodes nothing. The H2
CONNECT and WebTransport registries outlive a publication and keep a binding
after its route is removed, masked by a more specific route or reassigned;
admission therefore compares the selected host declaration with the one the
binding was prepared from and refuses when they differ. An edited route has
been prepared again by then and its new binding has replaced the old one.
`Routable::prepare_sessions` and H1 upgrade preparation run before publication;
there is no engine-specific registration call inside Edge.

The `edge` feature is independent of native application/Runner use. WebSocket
and WebTransport extend an already-selected Edge integration through weak
feature forwarding. Pure ownership/path/origin tests live in `tests/edge` and
start no Lua engine or listener.

## The engine

- **Lua 5.5**, through `mlua` with `lua55` and `vendored`: the C source is
  compiled into the binary rather than linked from the system. Distributions
  do not carry 5.5, and every host must run the same build — bytecode depends
  on the version and on the integer and float sizes it was compiled with.
- `mlua` is not 1.0 and each minor release has broken its API; the version is
  pinned and upgraded deliberately.
- **One state per execution, the way PHP runs a script.** An execution opens
  a fresh sandboxed state, installs the extensions, runs the script as a
  future on Tokio and drops the state when it ends. There is no application
  model, no state kept per thread and no pool: nothing a script leaves in its
  state outlives it or is seen by another, and there is nothing to reset. What
  must be shared goes through Orbit. Opening a state costs microseconds.
- `mlua`'s `send` feature, so an execution is a `Send` future the
  multi-threaded runtime can run like any other.
- **Async where Orbit waits.** A script calls an Orbit wait as a plain
  function; underneath, the call yields the coroutine it runs in and resumes
  when Orbit wakes it. The wait ends because something was written, never
  because an interval passed.

## The sandbox

A script gets what Claviron gives it, and nothing of the host process:

- Only the chosen standard libraries are opened (5.5's
  `luaL_openselectedlibs`): base, string, table, math, utf8, coroutine.
- No `io`, no `os`, no `debug`. From base, no `dofile`, `loadfile`, or
  `load` of a binary chunk: text only, and only where the host allows it.
- `require` resolves Claviron's modules, then the directories `lua.yaml`
  lists (`path: [...]`, source only, `a.b` as `a/b.lua` or `a/b/init.lua`),
  nothing else: no `package.path` of the host's, no C modules. Pure-Lua
  packages from LuaRocks work from there
  (`luarocks --lua-version 5.5 --tree lua_modules install …`); one that needs
  `io`, `os` or `debug` does not, by design. C modules, listed by the operator
  the way `php.ini` lists extensions, are a later step.
- Everything else — time, logging, the fleet's state — is an extension, so
  what a script can do is the list of extensions its host registered.

## Extensions

A crate that wants Lua to reach something registers an extension:

- An extension has a module name (`claviron.orbit.cell`) and builds that
  module's table in a state: functions, and userdata with metatables for the
  values it hands out.
- Extensions are registered with the host once and installed into every state
  it creates, so a script's `require` finds the same module on every thread.
- The host's context — the registry, the fleet, the application's name —
  reaches an extension when it builds its module, the way `claviron-js`
  extensions receive theirs; a module never looks anything up by itself.
- Downstream crates write extensions the same way Claviron's own are
  written. That is the first thing this crate has to make easy.

## Process identity

The managed host installs `claviron.process`, also available through `require`.
Its `role` and optional `worker_id` are captured from AppState's `RunMode` when
the engine is built, not from application environment or request headers.
Worker slots remain process-family-local and one-based; master and standalone
report their role with no worker id. The snapshot exposes no process control or
credentials. Editing a Lua table cannot change the runtime's identity.
Raw `Engine::new()` has no host identity unless its embedding composition
explicitly supplies `ProcessExtension`. No sandbox capability is added there
implicitly.

## The first extension: Orbit cells

`claviron.orbit.cell` — a typed fleet cell as a Lua value, the counterpart of
PHP's `Claviron\Orbital` and JavaScript's `claviron:orbit/cell`:

```lua
local progress = claviron.cell(0)   -- an int cell: its type is its first value's
local writer   = claviron.cell("")  -- a string cell; 0.5 makes a float, true a bool

progress:add(1)                     -- the value after
progress:set(10)
local now = progress:get()
local took, taken, after = progress:add_until(5, 100)   -- bounded claim
writer:append("step done\n")

local id = progress:id()                 -- crosses an event or a request by reference
local same = claviron.cell.open(id)      -- another process, another thread, same cell

local seen = progress:version()
progress:wait(seen)                 -- parks until the cell changes
```

- A cell is a handle, not a cache and not a counter. It has no string key: it
  is an anonymous value whose identity is the id Orbit gives it, passed by
  reference. A counter is the keyed one (`claviron.counter(name)`), found by
  whoever names it.
- Extensions may name themselves under the `claviron` global as well as by
  module: `claviron.cell` is the same table `require "claviron.orbit.cell"`
  returns, built on first use.
- A cell starts with a value, and its type is that value's; there is no
  untyped cell and no default type.
- The userdata holds only the id. Every operation opens the cell by id, so a
  released cell fails as stale in every state that still holds it.
- Lua 5.5 integers are 64-bit: an int cell is a Lua integer, with none of the
  string round trip JavaScript needs for `BigInt`.
- Comparisons and `tostring` read the cell (`__lt`, `__le`, `__tostring`), so
  `progress > 3` compares values. Lua calls `__eq` only between two userdata,
  so `==` compares two cells' values and is `false` against a plain value, as
  for any userdata — `progress:get() == 3` is the spelling there. Arithmetic
  metamethods are left out on purpose: `progress + 1` would read like an update
  and not be one.

Counters and locks follow as further extensions of the same shape.

## Streams

`claviron.stream` is a byte stream between two holders over the fleet's
stream table (kind `246`) — the same `orbit-stream` rings the Orbit transport
carries request and response bodies on.

```lua
local s = claviron.stream()                  -- side A, in this process's lane
claviron.events.publish("pipe", { ticket = s:ticket() })
s:send("hello")                              -- one message
s:finish()                                   -- no more from this side
local echo = s:receive()                     -- nil once the far side finished

local peer = claviron.stream.open(ticket)    -- side B: another script, any process
```

- Two bounded rings, one each way. A writer that fills its ring waits for the
  reader, so a message larger than the ring crosses as the reader drains it.
  Both waits end on bytes, on the end, or when the script is told to stop.
- The ticket is the far side's address and is held only by the creator; it
  travels by an event, an invocation or anything else. A side is held once:
  a second `open` of the same ticket is refused, which is what lets several
  workers race for one ticket and exactly one win.
- `send` / `receive` frame messages in the record the transport's message
  streams use; `read` / `write` are the bytes as they are, for a peer that
  speaks something else. One stream takes one of the two.
- A stream lives as long as its holders: dropping a state ends its sides, and
  the far side reads the end.

## Events

Sockets may attach one ordinary events listener with `socket:listen(listener)`.
The existing native socket owner selects input or subscription readiness and
invokes `event(socket, event)` under the same concurrency permit as other
callbacks. Waiting holds no permit and spawns no detached task. The owner drops
the subscription before final close cleanup, which also runs on application
cancellation. A retired generation retains subscriptions for its still-accepted
sockets until those sockets close; replacement creates no extra global listener.
Each socket needs its own subscription, consumed only by its owner loop.

`claviron.events` carries the fleet's application events — the same ones PHP,
Python and JavaScript publish and receive, bridged across processes by Orbit.

```lua
claviron.events.publish("order.paid", { id = 7 })                  -- to everyone
claviron.events.publish("reindex", {}, { to = "php:octane-app" })  -- to one application
local paid = claviron.events.listen({ "order.paid", "order.refunded" })
local event = paid:next()            -- { id, topic, payload }
local once = claviron.events.wait("job.done")
```

- A listener is subscribed when `listen` returns, so work started after it
  cannot answer before it listens.
- A wait ends when an event comes or when the execution is told to stop;
  there is no timeout argument, by design.
- A listener that fell behind says how many events it missed, as an error
  the script can catch; the listener stays usable.
- A script has no application identity: it receives events sent to
  everyone, not those sent to an application.
- Payloads cross as Claviron's event values: a Lua sequence is a list keyed
  0, 1, … as PHP and JavaScript see it, and comes back a sequence; strings are
  bytes, binary included; `claviron.json.null` is null; 64-bit integers
  round-trip.

## Invocations

`claviron.invoke` sends work to an operation and serves it — the fleet's
invocation ring, shared with PHP and JavaScript.

```lua
local id = claviron.invoke.send("image.resize", { path = "a.png" })   -- waits for nothing
claviron.invoke.serve("image.resize", function(payload, call) … end)  -- until stopped
local work = claviron.invoke.subscribe("report.build")
local call = work:next()          -- { id, operation, payload, submitted_at }
```

- A subscription is its own cursor, starting at the ring's head when
  `subscribe` (or `serve`) returns; two subscribers both receive every
  invocation, and claiming one is theirs.
- `serve` returns when the execution is told to stop; a daemon `lua:` task
  that serves an operation is how an invocation runs Lua. `next()` reports a
  subscription that fell behind as an error; `serve` logs it and goes on.
- An outcome, when wanted, is an event keyed by the invocation's id.

## Logging

`claviron.log` writes to the log channels `logging.yaml` declares, through the
fleet's one writer, as PHP's Monolog handler, Python's `LogHandler` and
JavaScript's `claviron:log` do.

```lua
local log = claviron.log.channel("app")               -- refused if undeclared
log:warn("order failed", { order = 7, customer = { id = 3 } })
claviron.log.channel("app", { target = "billing" }):error("refund failed")
```

- Levels are `trace`, `debug`, `info`, `warn`, `error`.
- A record's target is the host's name for the script — a task's name —
  unless the channel names one.
- Context becomes fields: nested tables as dotted keys, numbers and booleans
  typed. Behind the `log` feature, on with the facade's `log-pipeline`.

## Packages the binary carries

Four pure-Lua packages come with every build, so a script has them without
installing anything; `require` finds them after the extensions and before
`lua.yaml`'s path. All are MIT-licensed; each licence sits beside its source
in `crates/lua/lua/`.

| package | for |
|---|---|
| `inspect` | tables as readable text: debugging, logs |
| `lume` | everyday helpers: `map`, `filter`, `reduce`, `merge`, `split`, `uuid` |
| `middleclass` | classes and inheritance |
| `serpent` | Lua values as Lua source, for what JSON cannot carry |

Each loads on Lua 5.5 inside the sandbox, which a test holds. In `lume`,
`time` (needs `os`), `trace` (needs `debug`) and `hotswap` (needs `package`)
fail when called. Penlight is left out (much of it is `io` and `os`), and so is
a JSON package (`claviron.json` is the one).

## HTTP out

`claviron.fetch(url [, { method, headers, body }])` returns
`{ status, ok, url, headers, body }`, over reqwest with rustls — behind the
facade's `lua` feature (the engine crate's `fetch` feature), and closed unless
`lua.yaml` opens it:

```yaml
fetch:
  allow: [api.example.com, "*.internal"]   # unwritten: no fetch; "*": any host
  ca: [certs/internal-ca.pem]              # trusted besides the system's store
  max_body_size: 10485760                  # bytes; unwritten, no bound
```

- A host not listed is refused before any request, and so is a redirect to
  one; a redirect chain stops where reqwest's own default policy stops (10).
- A request waits for its answer or for the script to stop; there is no
  timeout unless one is declared.
- The client is built when `lua.yaml` is read, so connections are pooled
  across executions; a reload rebuilds it.

## SQL

`claviron.db` runs statements on the pools `db.yaml` declares — the same
`claviron-db` pools Rust, PHP and JavaScript use in this process — behind the
engine's explicit `postgres`, `mysql` and `sqlite` features. The facade forwards the
matching database feature only when Lua is already selected; Lua alone never
selects a database backend. The shared `db` feature does not install a query
adapter by itself. SQLx defaults are disabled at workspace
level, and Lua does not inherit `claviron-db`'s default backend.

```lua
local pg = claviron.db.postgres()                 -- postgres.default; or ("analytics")
local rows = pg:query("select id, name from users where active = $1", true)
local user = pg:first("select id, name from users where id = $1", 7)   -- or nil
print(pg:execute("delete from sessions where user_id = $1", 7).affected)
pg:transaction(function(tx) … end, { isolation = "serializable", read_only = false })
local sqlite = claviron.db.sqlite()              -- sqlite.default; or ("demo")
local record = sqlite:first("select name from records where id = ?", 7)
```

- A handle names a connection, not a pool: the pool is looked up per
  statement, so a reloaded `db.yaml` is what the next one uses. Naming a
  connection `db.yaml` does not declare is an error at `db.postgres(name)`.
- A statement awaits on Tokio: the coroutine parks, the worker thread serves
  others. It waits for its answer or for the script to stop; the pool's
  `acquire_timeout` and PostgreSQL's `statement_timeout` are the only bounds.
- Values: integers, floats, booleans and strings both ways; bytes
  (`bytea`, `BLOB`) as byte strings; `json`/`jsonb` as tables; SQL `NULL` as
  `claviron.db.null`, so every asked-for column is in the row. Any other
  column type (`numeric`, `uuid`, timestamps) is refused with the column's
  name rather than guessed at — cast it in SQL, where its format is decided.
  So is a MySQL `BIGINT UNSIGNED` past Lua's largest integer, and two columns
  with one name. This is the JavaScript driver's contract.
- A driver error carries the server's message and SQLSTATE.
- `transaction` commits when its function returns, rolls back when it
  raises (the function's error is what the script sees), and returns what
  the function returned. The handle it passed is dead afterwards. A script
  stopped mid-transaction drops it, and sqlx rolls it back.
- Rows come back whole; there is no cursor yet.

SQLite decodes each value's storage class, even when successive rows in one
column hold different types. INTEGER, REAL, TEXT, BLOB and NULL map directly;
booleans bind as integers and JSON tables as text. `insert_id` is the signed
last rowid on the connection, not necessarily a new id for the current statement.
Transactions use SQLite's normal `BEGIN`; serializable isolation is accepted,
other isolation levels and per-transaction `read_only` are refused. No pooled
connection PRAGMA is changed to emulate those options. Lock waits follow the
pool's configured `busy_timeout`; the Lua adapter adds no timeout.

## Tools: JSON and regular expressions

Two plain extensions every host installs, written in Rust rather than taken
from pure-Lua packages, because they sit on hot paths:

- `claviron.json` — `encode(value [, { pretty = true }])`, `decode(text)`,
  over `serde_json`. JSON's `null` is `claviron.json.null` (a table cannot hold
  `nil`), and `claviron.json.array(t)` marks a table as a list so an empty one
  encodes as `[]`; decoded arrays carry the mark. 64-bit integers round-trip.
- `claviron.regex(pattern)` — Rust's `regex` over bytes, as Lua strings are:
  `is_match`, `find` (1-based, inclusive, like `string.find`), `match`
  (captures by number and by name), `gmatch`, `replace`, `split`. The engine
  never backtracks, so no pattern can hang a script.

## Tasks

A Lua script is also a task, beside `command:`, `php:` and `js:` in
`claviron-tasks`: a `lua:` executor that runs a script in a sandboxed state
with the host's extensions, on the task's schedule, dispatch and log routing.
It is the smallest host there is — no request, no route — which makes it the
natural first one: a task writes a cell, and a PHP or JavaScript application
reads the same cell.

## Applications

Olua is Claviron's Lua application framework: the sandboxed module contract
and Rust-provided capabilities described here, not a general-purpose Lua runner.
Common Application management reports runtime `lua`, frameworks `[Olua]`, runner
`claviron` and request mode `fetch`. The adapter's discovery function identifies
this host-owned module contract. An explicit `frameworks` list (including `[]`)
overrides it; scalar `framework` remains a single-label shorthand. No module name
is interpreted as a framework. Optional `icon` is a symbolic client code; absent
or unknown codes use the GUI's runtime default.
Local and Link observations use the same description without changing request
dispatch. The crate, feature and `claviron.*` module names remain unchanged.

Each application may declare `env_file` (one cwd-relative path or an ordered
list) and inline `env`. Its execution owner prepares the detached Dotenw recipe:
files and selected deployment variants, then inline overrides. `claviron.getenv`
reads those execution-owned values, returns nil for an absent key, and never
reads the host process environment. Lua task executions also receive their
TaskService-prepared `spec.env` through this same engine capability.

The application runnable multiplexes environment notifications in its existing
cancellation loop, with no per-application spawned watcher task. Changed values
load only that application's module into a fresh state; requests already using
the previous state finish there. Invalid env or a module-load failure keeps the
old state, and a stopped application is not started by an env change. An env-only
renewal reuses the prepared snapshot rather than rereading its files. Configuration
reload replaces the recipe and its file membership; removing the application
or stopping the aggregate runnable releases registrations.

The `app` feature owns application state, lifecycle and the raw
`Request<RouteBody>` handler. There is no Axum feature, dependency or router
binding in the engine. serve::LuaApplicationAdapter owns a prepared service,
application name and narrowed route policy. It acquires the current loaded
generation for each native call, retains it through execution/cancellation and
passes the request body directly to that generation. Missing/stopped applications
answer 503 without reading the body. Existing streaming, admission and lifecycle
behavior remains with LuaApplication.

The optional edge.rs integration prepares only declared mounts, in stable longest
prefix order, with later equal-prefix declarations winning as before. A
nonmatching mount returns the unread request capability; a matching error is
handled, never a signal to try another backend. HostConfig and registry lookup
belong to cold composition. Native execution remains independent of host pipelines and backend order.

The websocket feature selects app and native framing dependencies and weakly
extends an already-selected Edge adapter. H1UpgradeHandler, H2 CONNECT and HTTP
handshake validation live in edge/websocket.rs; WebTransport is beside it.
Both share origin admission in edge/origin.rs. Host preparation calls their
native registration contracts. Native engine prepare and
serve_websocket remain shared by H1 and H2. This
separates wire admission from Lua callbacks without adding tasks or changing
accepted generation ownership, stream draining or engine shutdown.

A Lua application stays up and is woken by requests, the way a Laravel
application runs under Octane and a Hono application in a persistent V8
isolate. It is configured in `lua.yaml` and mounted on a host like a
JavaScript application:

```yaml
# lua.yaml
applications:
  shop:
    module: apps/shop/app.lua   # relative: the working directory's
    concurrency: 8              # requests one state runs at once; 1 when unwritten
    max_queued: 256            # requests queued for a permit; 256 when unwritten
```

```yaml
# sites/shop.yaml
lua: { path: /api, application: shop }
```

The module returns what serves requests: a table with `fetch(request)`, or a
`claviron.router`, which has one.

```lua
local router = claviron.router()
router:get("/users/{id}", function(request, params)
  return { status = 200, headers = { ["content-type"] = "application/json" },
           body = claviron.json.encode({ id = params.id }) }
end)
return router
```

- **One state per application per worker process**, loaded once when the
  worker starts. No pool and no thread of its own: a state is not bound to an
  OS thread, so each request is a task on Tokio and a coroutine in the state.
  Parallelism comes from worker processes.
- **`concurrency` is a semaphore** in front of the state. With `1` a state runs
  one request at a time, as a JavaScript isolate or a PHP worker does. Above
  it, a request that waits — on `fetch`, a cell, a stream, an event — lets
  another run; coroutines switch only there, so a handler that computes holds
  its turn. Raising it is a decision for applications that wait.
- A request takes an idle permit directly. `max_queued` bounds requests
  waiting for a permit. At zero, a busy state replies with `503` immediately. How long a request may wait for a permit is the route's
  `requests.queue_timeout`, as for JavaScript: the wait ends on a permit, or
  on that declared time.
- **State is shared across requests**, as under Octane: globals and module
  tables a handler changes stay changed. What belongs to one request lives in
  its locals.
- A request is `{ method, path, query, headers }` — `headers` with
  lower-case names, repeated ones joined with `", "` — and two methods for
  its body: `request:read()`, the next piece or `nil` at the end, and
  `request:body()`, the rest at once.
- Whether the body is taken whole before the handler runs is the route's
  request `buffering`, as for every backend that can stream
  (`BodyNeed::Stream`); taken whole, `read` hands it over as one piece. A body
  taken whole is taken before the request's permit, so a slow upload holds a
  place in the queue, never one of the state's permits.
- A reply is a table `{ status, headers, body }` or a string (`200`,
  `text/plain`). `body` is a string, or a function called for each piece
  until it returns `nil`: the head goes at once and the pieces follow, and
  the function may wait between them — on `fetch`, a cell, a stream, an
  event. How much of a reply is held before it is sent is the route's
  response `buffering`. The permit is held until the last piece; a client
  that leaves ends the reply before the next call, and an error cuts the
  body short so the client sees a broken reply, not a complete one.
- An error the handler raises before its reply is a `500` and is logged; the
  state serves the next request.
- An application that does not load — a syntax error, a module that returns
  nothing with `fetch` — is logged and its routes answer `503`; the worker
  and every other application serve on.
- **Reload** reads `lua.yaml` again and loads every application's module into
  a new state, which takes the old one's place: requests that arrive after
  are the new state's. The old state is not interrupted — what it serves
  finishes there, and it closes with its last request; the worker's stop
  still reaches it while it serves. A module that does not load leaves the
  old state serving, and the error is logged. An application added to
  `lua.yaml` is loaded, one removed is retired the same way. A route may
  name an application `lua.yaml` does not declare yet; it answers `503` until
  a reload declares it.
- **Managed as the `lua` runtime** through the application manager, like
  `php` and `js`: `list`, `status`, `start`, `stop`, `restart` by name — over
  gRPC too (`claviron-client restart-application lua shop`); the runtime is a
  string there, so nothing in the protocol changed. `restart` loads a new
  state and puts it in place, the same way a reload does, so there is no gap
  in which the application answers `503`; it is observed as the old
  generation stopping and the new one starting (`ApplicationStopped`,
  `ApplicationStarted`). `stop` takes the application out of service — its
  routes answer `503` — and lets what its state serves finish; a stopped
  application stays stopped across reloads until it is started.
- No execution timeout: a request ends when its handler returns. The
  application's state closes when the worker stops, which ends every wait a
  handler is parked on.
- **WebSockets**, as a JavaScript application has them (feature
  `websocket`, facade `lua` + `websocket`). The route declares the endpoints,
  relative to its mount; the application gives the handlers, on the table it
  returns or with `router:websocket`:

  ```yaml
  lua: { path: /lua, application: demo, websocket: { path: /socket } }
  ```

  ```lua
  router:websocket({
    open    = function(socket) socket:send("hello") end,
    message = function(socket, data, binary) socket:send(data) end,
    close   = function(socket, code, reason, clean) end,
  })
  ```

  The edge holds the socket — handshake, frames, ping, close — over an
  HTTP/1.1 `Upgrade` or an HTTP/2 extended CONNECT, both ending in one
  session. A socket's events run in order, each in its own coroutine and each
  holding a `concurrency` permit only while it runs: an open, quiet socket
  costs none. `socket:send(text)` (UTF-8) and `socket:send_binary(bytes)`
  queue for the client and wait while the queue is full, and what the
  application sends keeps flowing while a handler runs, so a handler waiting
  on a full queue never stalls the socket that would empty it. `socket.id`,
  `socket.url` and `socket.headers` tell sockets apart; `socket:close(code,
  reason)` ends one. A disconnect the client never announced still reaches
  `close`, with 1006 and `clean = false`. The application stopping closes
  its sockets with 1001; a handler's error closes its socket with 1011.
  `lua.yaml` may bound them per application:

  ```yaml
  applications:
    demo:
      websocket: { max_message_size: 1048576, send_queue: 64 }   # JavaScript's defaults
  ```
  The configuration schema remains available without the feature; the stored
  transport-specific socket settings compile only with their selected feature;
  callback execution is shared by the two transports.
- The local `webtransport` feature selects `app` and the existing Orbit message
  codec, not Lua's Orbit extensions. The facade and Edge forward
  their existing `webtransport` feature through this crate's `webtransport`,
  enabling it only when Lua is also selected, without selecting Lua or WebSockets
  on its own. `router:webtransport` accepts the same callback table as
  `router:websocket`. Declaring callbacks remains available without the
  transport feature, so one module can retain its WebSocket fallback in a
  WebSocket-only build. No WT binding or IO driver is enabled by that declaration.
  Socket userdata includes the server-owned `transport`
  and `http_version`. Edge binds configured socket paths through its existing
  WebTransport registry, validates the selected Lua route and Origin before
  acceptance, and hands the application's accepted generation client-opened
  reliable bidirectional streams. Each stream has its own socket id, callback
  sequence, pending input and bounded output queue; a worker-local `session_id`
  groups siblings without exposing the wire CONNECT id. A socket close ends
  only that stream. The session retains a bounded FuturesUnordered of stream
  futures, reusing the listener's validated QUIC bidi input capacity as its
  concurrent handler bound. At capacity it stops receiving, leaving the native
  input queue bounded; native overflow refuses only the excess stream. There
  are no detached stream tasks. Callback execution retains the existing
  application-wide concurrency permits, including deliberate serialization
  when concurrency is one. Records use the existing Text/Binary/Close
  codec (`u8 kind`, little-endian `u32 length`, payload), bounded by the
  application's `webtransport.max_message_size`; `webtransport.send_queue`
  bounds outbound messages. No WebSocket framing or unreliable datagram API
  is implied. Native listener/host retirement and application stopping reach
  the session. H3's CONNECT closure observer distinguishes peer FIN and transport
  loss from listener/server retirement. Shutdown stops stream admission, signals
  all current streams through one retained watch value, and joins every stream's
  IO and callbacks before releasing the native session. Closing the last data
  stream alone does not close the session; subsequent streams remain admissible.
  The read and write halves progress independently. Shutdown closes outbound
  admission even during a blocked write and delivers pending input followed by
  the close event independently of output drain. The writer completes each
  admitted record without cancellation, drains its bounded queue, then sends
  the close record. No deadline is introduced; the accepting native owner
  retains escalation authority. All futures remain joined in the session.
  Dropping that task cannot leave detached callbacks. Quiet sockets hold no
  application concurrency permit.
- Later: WebTransport datagrams and a Lua owner for the runner shell.

## Editor support

`crates/lua/claviron.d.lua` describes `claviron.*` in lua-language-server's
annotations (`---@meta`), the way `claviron.d.ts` does for JavaScript: point
the server's `workspace.library` at it and the API completes and checks in the
editor. It runs nothing, and it grows with every extension.

## Placement

- This crate, `claviron-lua`; the Orbit extensions under `lua`
  with `orbit`. No dependency on the edge.
- Tests live under `crates/lua/tests`, as everywhere in Claviron.

## Open before building

1. **The first host.** A `lua:` task is the candidate (see Tasks); a route
   handler in a runner comes after.
2. **Script loading.** Settled by the model: an execution reads its script,
   so a changed script is simply the next execution's.
