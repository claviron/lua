---@meta
-- Type definitions for Claviron's Lua API, for lua-language-server.
-- Point the language server at this file ("workspace.library") and
-- `claviron.*` completes and checks in the editor. Nothing here runs.

---@class claviron
---@field cell claviron.cell
---@field stream claviron.stream
---@field events claviron.events
---@field invoke claviron.invoke
---@field log claviron.log
---@field json claviron.json
---@field regex claviron.regex
---@field fetch claviron.fetch
---@field db claviron.db
---@field router claviron.router
---@field process claviron.process
claviron = {}

---Read this execution's environment. Does not expose the host process.
---@param key string
---@return string|nil
function claviron.getenv(key) end

---Host-supplied process identity; a worker slot is not a global user identity.
---@class claviron.process
---@field role 'master'|'worker'|'standalone'
---@field worker_id integer?  one-based worker slot; nil in master/standalone

-- Cells ----------------------------------------------------------------------

---A fleet cell: a handle holding the cell's id; every read and write goes to
---the fleet's shared memory. Its type is its first value's.
---@class claviron.Cell
local Cell = {}

---@return string id  crosses an event or an invocation by reference
function Cell:id() end

---@return "int"|"float"|"bool"|"string"
function Cell:kind() end

---@return integer|number|boolean|string
function Cell:get() end

---@param value integer|number|boolean|string
function Cell:set(value) end

---Atomic on the cell; the value after.
---@param by integer|number
---@return integer|number
function Cell:add(by) end

---Take up to `by` while the value stays at or under `limit`, in one swap.
---@param by integer
---@param limit integer
---@return boolean took
---@return integer taken
---@return integer after
function Cell:add_until(by, limit) end

---@param text string
---@return string after
function Cell:append(text) end

---@return integer
function Cell:version() end

---Parks until the cell changes from `version`; errors when the script is
---stopping.
---@param version integer
---@return integer changes
function Cell:wait(version) end

function Cell:release() end

---@class claviron.cell
---@overload fun(value: integer|number|boolean|string): claviron.Cell
local cell = {}

---@param id string
---@return claviron.Cell
function cell.open(id) end

-- Streams --------------------------------------------------------------------

---One side of a fleet byte stream. Waits end on bytes, on the end, or when the
---script is stopping.
---@class claviron.Stream
local Stream = {}

---@return string
function Stream:id() end

---The far side's address; only the side that created the stream holds it.
---@return string
function Stream:ticket() end

---One message; waits while the ring is full.
---@param message string
function Stream:send(message) end

---The next message, or nil once the far side finished.
---@return string?
function Stream:receive() end

---Bytes as they are; waits while the ring is full.
---@param bytes string
function Stream:write(bytes) end

---The bytes there are now, or nil once the far side finished.
---@return string?
function Stream:read() end

---No more from this side; the far side reads the end.
function Stream:finish() end

---End this side's writing as failed.
function Stream:reset() end

---@class claviron.stream
---@overload fun(): claviron.Stream
local stream = {}

---Hold the side a ticket names; refused when another holder has it.
---@param ticket string
---@return claviron.Stream
function stream.open(ticket) end

-- Events ---------------------------------------------------------------------

---@class claviron.Event
---@field id string
---@field topic string
---@field payload any

---@class claviron.Listener
local Listener = {}

---Parks until the next event on one of the topics; errors when the script is
---stopping, or when the listener fell behind.
---@return claviron.Event
function Listener:next() end

---@return string[]
function Listener:topics() end

---@class claviron.events
local events = {}

---@param topic string
---@param payload? any
---@param options? { to?: string }  `to` is `runtime:name`, as `php:octane-app`
function events.publish(topic, payload, options) end

---Subscribed when it returns.
---@param topics string|string[]
---@return claviron.Listener
function events.listen(topics) end

---@param topics string|string[]
---@return claviron.Event
function events.wait(topics) end

-- Invocations ----------------------------------------------------------------

---@class claviron.Invocation
---@field id string
---@field operation string
---@field payload any
---@field submitted_at integer  milliseconds since the Unix epoch

---@class claviron.Subscription
local Subscription = {}

---@return claviron.Invocation
function Subscription:next() end

---@return string
function Subscription:operation() end

---@class claviron.invoke
local invoke = {}

---Waits for nothing.
---@param operation string
---@param payload? any
---@return string id
function invoke.send(operation, payload) end

---@param operation string
---@return claviron.Subscription
function invoke.subscribe(operation) end

---Runs `handler` for each invocation until the script is stopping.
---@param operation string
---@param handler fun(payload: any, invocation: claviron.Invocation)
function invoke.serve(operation, handler) end

-- Logging --------------------------------------------------------------------

---@class claviron.LogChannel
local LogChannel = {}

---@param message any
---@param context? table
---@return boolean written
function LogChannel:trace(message, context) end
---@param message any
---@param context? table
---@return boolean written
function LogChannel:debug(message, context) end
---@param message any
---@param context? table
---@return boolean written
function LogChannel:info(message, context) end
---@param message any
---@param context? table
---@return boolean written
function LogChannel:warn(message, context) end
---@param message any
---@param context? table
---@return boolean written
function LogChannel:error(message, context) end

---@class claviron.log
local log = {}

---Refused when `logging.yaml` does not declare the channel.
---@param name string
---@param options? { target?: string }
---@return claviron.LogChannel
function log.channel(name, options) end

-- JSON -----------------------------------------------------------------------

---@class claviron.json
---@field null lightuserdata  JSON's null
local json = {}

---@param value any
---@param options? { pretty?: boolean }
---@return string
function json.encode(value, options) end

---@param text string
---@return any
function json.decode(text) end

---Mark a table as a list, so an empty one encodes as `[]`.
---@param t? table
---@return table
function json.array(t) end

-- Regular expressions --------------------------------------------------------

---@class claviron.Regex
local Regex = {}

---@param text string
---@return boolean
function Regex:is_match(text) end

---1-based and inclusive, like `string.find`.
---@param text string
---@param init? integer
---@return integer? start
---@return integer? finish
function Regex:find(text, init) end

---Captures by number (`[0]` is the whole match) and by name.
---@param text string
---@param init? integer
---@return table?
function Regex:match(text, init) end

---@param text string
---@return fun(): table?
function Regex:gmatch(text) end

---@param text string
---@param replacement string  `$1`, `$name`
---@param limit? integer  0, or none, replaces all
---@return string
function Regex:replace(text, replacement, limit) end

---@param text string
---@return string[]
function Regex:split(text) end

---@class claviron.regex
---@overload fun(pattern: string): claviron.Regex
local regex = {}

---@param pattern string
---@return claviron.Regex
function regex.new(pattern) end

-- HTTP out --------------------------------------------------------------------

---@class claviron.Response
---@field status integer
---@field ok boolean  status 2xx
---@field url string  after redirects
---@field headers table<string, string>  lower-case names; repeated ones joined with ", "
---@field body string

---To the hosts `lua.yaml`'s `fetch.allow` lists; absent there, fetch is absent.
---@class claviron.fetch
---@overload fun(url: string, options?: { method?: string, headers?: table<string, string>, body?: string }): claviron.Response

-- SQL ------------------------------------------------------------------------

---What `execute` answers.
---@class claviron.Done
---@field affected integer  rows the statement changed
---@field insert_id? integer  MySQL AUTO_INCREMENT or SQLite last rowid; PostgreSQL uses RETURNING

---Statements on one pooled connection: a named connection, or a transaction's.
---Parameters are `$1…` (PostgreSQL) or `?` (MySQL/SQLite). `nil` and `db.null` are
---NULL, a table is JSON, a string that is not UTF-8 is bytes.
---@class claviron.Sql
local Sql = {}

---Every row, each a table keyed by column name; NULL is `db.null`.
---@param sql string
---@param ... any
---@return table[]
function Sql:query(sql, ...) end

---The first row, or nil when there is none.
---@param sql string
---@param ... any
---@return table?
function Sql:first(sql, ...) end

---@param sql string
---@param ... any
---@return claviron.Done
function Sql:execute(sql, ...) end

---A connection `db.yaml` declares.
---@class claviron.Database: claviron.Sql
local Database = {}

---Run `body` in a transaction: it commits when `body` returns and rolls back
---when it raises. Returns what `body` returns.
---@generic T
---@param body fun(tx: claviron.Sql): T
---@param options? { isolation?: "read uncommitted"|"read committed"|"repeatable read"|"serializable", read_only?: boolean }
---@return T
function Database:transaction(body, options) end

---@return string
function Database:name() end

---The pools `db.yaml` declares; requires a database backend feature.
---@class claviron.db
---@field null lightuserdata  SQL NULL (the same value as `claviron.json.null`)
local db = {}

---@param name? string  default "default"
---@return claviron.Database
function db.postgres(name) end

---With the `mysql` feature.
---@param name? string  default "default"
---@return claviron.Database
function db.mysql(name) end

---With the `sqlite` feature. Values use SQLite storage classes: INTEGER,
---REAL, TEXT and BLOB. Booleans bind as integers; JSON tables bind as text.
---Transactions accept serializable isolation, not per-transaction read_only.
---@param name? string  default "default"
---@return claviron.Database
function db.sqlite(name) end

-- Applications -----------------------------------------------------------------

---A request as an application's `fetch` receives it.
---@class claviron.Request
---@field method string
---@field path string
---@field query string?  the raw query string, without `?`
---@field headers table<string, string>  lower-case names; repeated ones joined with ", "
local Request = {}

---The next piece of the body, or nil once it has ended.
---@return string?
function Request:read() end

---The rest of the body, whole.
---@return string
function Request:body() end

---What `fetch` or a route handler returns: a string is `200 text/plain`. A
---`body` function is called for each piece until it returns nil.
---@alias claviron.Reply string|{ status?: integer, headers?: table<string, string|string[]>, body?: string|fun(): string? }

---Paths to handlers. `{name}` is one segment, `{*rest}` the remainder; a path
---matched under another method is `405`, no match `404`, and `HEAD` falls back
---to `GET`. Each method returns the router, so routes chain.
---@class claviron.Router
local Router = {}

---@alias claviron.Handler fun(request: claviron.Request, params: table<string, string>): claviron.Reply

---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:get(path, handler) end
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:post(path, handler) end
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:put(path, handler) end
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:patch(path, handler) end
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:delete(path, handler) end
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:options(path, handler) end
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:head(path, handler) end
---Every method.
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:any(path, handler) end
---@param method string
---@param path string
---@param handler claviron.Handler
---@return claviron.Router
function Router:route(method, path, handler) end

---Handlers for the application's WebSockets; each is optional.
---@class claviron.SocketHandlers
---@field open? fun(socket: claviron.Socket)
---@field message? fun(socket: claviron.Socket, data: string, binary: boolean)
---@field event? fun(socket: claviron.Socket, event: claviron.Event)
---@field close? fun(socket: claviron.Socket, code: integer, reason: string, clean: boolean)

---One WebSocket or reliable WebTransport stream, held by the edge.
---@class claviron.Socket
---@field id integer  unique within this worker
---@field session_id integer?  worker-local WT session identity, shared by its streams; nil for WebSockets
---@field url string
---@field headers table<string, string>
---@field transport 'websocket'|'webtransport'
---@field http_version 'HTTP/1.1'|'HTTP/2.0'|'HTTP/3.0'
local Socket = {}

---A text message; `text` must be UTF-8. Waits while the send queue is full.
---@param text string
function Socket:send(text) end

---Attach one events listener to this socket's managed callback loop (requires orbit).
---Pass nil to detach. Do not also consume this listener with next().
---@param listener? claviron.Listener
---@return claviron.Socket
function Socket:listen(listener) end

---A binary message. Waits while the send queue is full.
---@param bytes string
function Socket:send_binary(bytes) end

---@param code? integer  1000 when omitted
---@param reason? string
function Socket:close(code, reason) end

---Serve the application's WebSockets with these handlers.
---@param handlers claviron.SocketHandlers
---@return claviron.Router
function Router:websocket(handlers) end

---Declare reliable WebTransport callbacks, run in order per incoming bidi stream.
---Each stream has its own socket; close ends that stream, not sibling streams.
---Serving requires the webtransport feature; application concurrency still applies.
---@param handlers claviron.SocketHandlers
---@return claviron.Router
function Router:webtransport(handlers) end

---What an application's module may return: it serves requests.
---@param request claviron.Request
---@return claviron.Reply
function Router:fetch(request) end

---@class claviron.router
---@overload fun(): claviron.Router
local router = {}

---@return claviron.Router
function router.new() end
