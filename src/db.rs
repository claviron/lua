//! `claviron.db`: SQL on the pools `db.yaml` declares.
//!
//! ```lua
//! local pg = claviron.db.postgres()            -- `postgres.default`; or ("analytics")
//! local users = pg:query("select id, name from users where active = $1", true)
//! local user = pg:first("select id, name from users where id = $1", 7)  -- or nil
//! local done = pg:execute("update users set seen = now() where id = $1", 7)
//! print(done.affected)
//!
//! pg:transaction(function(tx)
//!   tx:execute("update accounts set balance = balance - $1 where id = $2", 10, 1)
//!   tx:execute("update accounts set balance = balance + $1 where id = $2", 10, 2)
//! end, { isolation = "serializable" })
//!
//! local my = claviron.db.mysql()               -- `?` placeholders; `insert_id`
//! local sqlite = claviron.db.sqlite()          -- `?` placeholders; signed rowid
//! ```
//!
//! The pools are Claviron's own: the same connections Rust, PHP and JavaScript
//! use in this process, rebuilt when `db.yaml` is reloaded. A query awaits on
//! Tokio; the script's coroutine parks and the worker thread serves others.
//! It waits for its answer or for the script to stop; the pool's own
//! `acquire_timeout` and PostgreSQL's `statement_timeout` are the only bounds.
//!
//! Values cross as Lua's own types: integers, floats, booleans, strings,
//! `bytea`/`BLOB` as byte strings, `json`/`jsonb` as tables. SQL `NULL` is
//! `claviron.db.null` (the same value as `claviron.json.null`), so a row keeps
//! every column it was asked for. A column of any other type — `numeric`,
//! `uuid`, timestamps — is refused rather than guessed at: cast it in SQL
//! (`created_at::text`), which is also where its format is decided.
//!
//! A transaction commits when its function returns and rolls back when it
//! raises; what the function returns, `transaction` returns. Its handle is
//! unusable once it has ended.

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::sync::Arc;

use claviron_db::DbService;
use mlua::{
    Function, Lua, LuaSerdeExt, MultiValue, Table, UserData, UserDataMethods,
    UserDataRef, Value, Variadic,
};
use tokio::sync::Mutex;

use crate::{Execution, Extension};

pub struct DbExtension {
    service: Arc<DbService>,
}

impl DbExtension {
    pub fn new(service: Arc<DbService>) -> Self {
        Self { service }
    }
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("db: {error}"))
}

/// A driver error as the script sees it: the server's message and, when the
/// server gave one, its SQLSTATE, which is what a retry decision reads.
fn database_failure(error: sqlx::Error) -> mlua::Error {
    match &error {
        sqlx::Error::Database(database) => match database.code() {
            Some(code) => {
                failure(format!("{} (SQLSTATE {code})", database.message()))
            }
            None => failure(database.message()),
        },
        _ => failure(error),
    }
}

fn stopped() -> mlua::Error {
    failure("the query ended: this script is stopping")
}

impl Extension for DbExtension {
    fn name(&self) -> &'static str {
        "claviron.db"
    }

    fn global(&self) -> Option<&'static str> {
        Some("db")
    }

    fn module(
        &self,
        lua: &Lua,
        execution: &Arc<Execution>,
    ) -> mlua::Result<Table> {
        let module = lua.create_table()?;
        let backends: &[Backend] = &[
            #[cfg(feature = "postgres")]
            Backend::Postgres,
            #[cfg(feature = "mysql")]
            Backend::MySql,
            #[cfg(feature = "sqlite")]
            Backend::Sqlite,
        ];
        for &backend in backends {
            let service = Arc::clone(&self.service);
            let execution = Arc::clone(execution);
            module.set(
                backend.name(),
                lua.create_function(move |_, name: Option<String>| {
                    let name = name.unwrap_or_else(|| "default".to_owned());
                    if !backend.declared(&service, &name) {
                        return Err(failure(format!(
                            "{} connection \"{name}\" is not declared in db.yaml",
                            backend.name()
                        )));
                    }
                    Ok(Database {
                        service: Arc::clone(&service),
                        backend,
                        name,
                        execution: Arc::clone(&execution)
                    })
                })?
            )?;
        }
        module.set("null", lua.null())?;
        Ok(module)
    }
}

#[derive(Clone, Copy)]
enum Backend {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
    #[cfg(feature = "sqlite")]
    Sqlite,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres => "postgres",
            #[cfg(feature = "mysql")]
            Self::MySql => "mysql",
            #[cfg(feature = "sqlite")]
            Self::Sqlite => "sqlite",
        }
    }

    fn declared(
        self,
        service: &DbService,
        name: &str,
    ) -> bool {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres => service.postgres_named(name).is_some(),
            #[cfg(feature = "mysql")]
            Self::MySql => service.mysql_named(name).is_some(),
            #[cfg(feature = "sqlite")]
            Self::Sqlite => service.sqlite_named(name).is_some(),
        }
    }
}

/// A bind parameter, taken from a Lua value.
enum Param {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

fn params(
    lua: &Lua,
    values: Variadic<Value>,
) -> mlua::Result<Vec<Param>> {
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            Ok(match value {
                Value::Nil => Param::Null,
                value if value.is_null() => Param::Null,
                Value::Boolean(value) => Param::Boolean(value),
                Value::Integer(value) => Param::Integer(value),
                Value::Number(value) => Param::Float(value),
                // A string that is not UTF-8 is bytes (`bytea`, `BLOB`).
                Value::String(value) => match value.to_str() {
                    Ok(text) => Param::Text(text.to_owned()),
                    Err(_) => Param::Bytes(value.as_bytes().to_vec()),
                },
                Value::Table(_) => Param::Json(lua.from_value(value)?),
                other => {
                    return Err(failure(format!(
                        "parameter {} is a {}, which SQL has no value for",
                        index + 1,
                        other.type_name()
                    )));
                }
            })
        })
        .collect()
}

/// A column value, decoded before it reaches Lua.
enum Cell {
    Null,
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    Json(serde_json::Value),
}

/// What one statement answered.
#[derive(Default)]
struct Outcome {
    columns: Vec<String>,
    rows: Vec<Vec<Cell>>,
    affected: u64,
    insert_id: Option<InsertId>,
}

#[derive(Clone, Copy)]
enum InsertId {
    #[cfg(feature = "mysql")]
    Unsigned(u64),
    #[cfg(feature = "sqlite")]
    Signed(i64),
}

fn unsupported(
    backend: &str,
    column: &str,
    type_name: &str,
) -> sqlx::Error {
    sqlx::Error::Decode(
        format!(
            "{backend} column \"{column}\" is {type_name}, which has no Lua value; cast it in \
             SQL (for example {column}::text)"
        )
        .into()
    )
}

fn cell(
    lua: &Lua,
    cell: Cell,
) -> mlua::Result<Value> {
    Ok(match cell {
        Cell::Null => lua.null(),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        Cell::Boolean(value) => Value::Boolean(value),
        Cell::Integer(value) => Value::Integer(value),
        Cell::Float(value) => Value::Number(value),
        Cell::Text(value) => Value::String(lua.create_string(value)?),
        Cell::Bytes(value) => Value::String(lua.create_string(value)?),
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        Cell::Json(value) => lua.to_value(&value)?,
    })
}

/// Rows as a list of tables keyed by column name. The list is marked as an
/// array, so `claviron.json.encode` writes `[]` for no rows.
fn rows(
    lua: &Lua,
    outcome: Outcome,
) -> mlua::Result<Table> {
    for (index, column) in outcome.columns.iter().enumerate() {
        if outcome.columns[..index].contains(column) {
            return Err(failure(format!(
                "two columns are named \"{column}\"; name them apart with AS"
            )));
        }
    }
    let list = lua.create_table_with_capacity(outcome.rows.len(), 0)?;
    list.set_metatable(Some(lua.array_metatable()))?;
    for values in outcome.rows {
        let row = lua.create_table_with_capacity(0, outcome.columns.len())?;
        for (column, value) in outcome.columns.iter().zip(values) {
            row.set(column.as_str(), cell(lua, value)?)?;
        }
        list.push(row)?;
    }
    Ok(list)
}

/// `{ affected, insert_id }`: MySQL's `AUTO_INCREMENT` or SQLite's last rowid
/// on this connection. PostgreSQL says it with `RETURNING`.
fn done(
    lua: &Lua,
    outcome: &Outcome,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("affected", outcome.affected)?;
    if let Some(id) = outcome.insert_id {
        match id {
            #[cfg(feature = "mysql")]
            InsertId::Unsigned(id) => table.set("insert_id", id)?,
            #[cfg(feature = "sqlite")]
            InsertId::Signed(id) => table.set("insert_id", id)?,
        }
    }
    Ok(table)
}

/// `transaction`'s options: `{ isolation = "serializable", read_only = true }`.
#[derive(Clone, Copy, Default)]
struct Settings {
    isolation: Option<&'static str>,
    read_only: bool,
}

fn settings(options: Option<Table>) -> mlua::Result<Settings> {
    let Some(options) = options else {
        return Ok(Settings::default());
    };
    let isolation = match options.get::<Option<String>>("isolation")? {
        None => None,
        Some(level) => Some(match level.to_ascii_lowercase().as_str() {
            "read uncommitted" => "READ UNCOMMITTED",
            "read committed" => "READ COMMITTED",
            "repeatable read" => "REPEATABLE READ",
            "serializable" => "SERIALIZABLE",
            _ => {
                return Err(failure(format!(
                    "\"{level}\" is not an isolation level"
                )));
            }
        }),
    };
    let read_only = options.get::<Option<bool>>("read_only")?.unwrap_or(false);
    Ok(Settings { isolation, read_only })
}

/// A named connection of one backend: `claviron.db.postgres("analytics")`.
/// It holds the name, not a pool, so a reloaded `db.yaml` is what the next
/// query uses.
struct Database {
    service: Arc<DbService>,
    backend: Backend,
    name: String,
    execution: Arc<Execution>,
}

impl Database {
    async fn run(
        &self,
        sql: &str,
        params: Vec<Param>,
    ) -> mlua::Result<Outcome> {
        let run = async {
            match self.backend {
                #[cfg(feature = "postgres")]
                Backend::Postgres => {
                    let pool = self
                        .service
                        .postgres_named(&self.name)
                        .ok_or_else(|| {
                            failure(format!(
                                "postgres connection \"{}\" is gone",
                                self.name
                            ))
                        })?;
                    let mut connection =
                        pool.acquire().await.map_err(database_failure)?;
                    postgres::run(&mut connection, sql, params)
                        .await
                        .map_err(database_failure)
                }
                #[cfg(feature = "mysql")]
                Backend::MySql => {
                    let pool = self
                        .service
                        .mysql_named(&self.name)
                        .ok_or_else(|| {
                            failure(format!(
                                "mysql connection \"{}\" is gone",
                                self.name
                            ))
                        })?;
                    let mut connection =
                        pool.acquire().await.map_err(database_failure)?;
                    mysql::run(&mut connection, sql, params)
                        .await
                        .map_err(database_failure)
                }
                #[cfg(feature = "sqlite")]
                Backend::Sqlite => {
                    let pool = self
                        .service
                        .sqlite_named(&self.name)
                        .ok_or_else(|| {
                            failure(format!(
                                "sqlite connection \"{}\" is gone",
                                self.name
                            ))
                        })?;
                    let mut connection =
                        pool.acquire().await.map_err(database_failure)?;
                    sqlite::run(&mut connection, sql, params)
                        .await
                        .map_err(database_failure)
                }
            }
        };
        tokio::select! {
            outcome = run => outcome,
            () = self.execution.stopping().cancelled() => Err(stopped())
        }
    }

    async fn begin(
        &self,
        settings: Settings,
    ) -> mlua::Result<Open> {
        let begin = async {
            match self.backend {
                #[cfg(feature = "postgres")]
                Backend::Postgres => {
                    let pool = self
                        .service
                        .postgres_named(&self.name)
                        .ok_or_else(|| {
                            failure(format!(
                                "postgres connection \"{}\" is gone",
                                self.name
                            ))
                        })?;
                    postgres::begin(&pool, settings)
                        .await
                        .map(Open::Postgres)
                        .map_err(database_failure)
                }
                #[cfg(feature = "mysql")]
                Backend::MySql => {
                    let pool = self
                        .service
                        .mysql_named(&self.name)
                        .ok_or_else(|| {
                            failure(format!(
                                "mysql connection \"{}\" is gone",
                                self.name
                            ))
                        })?;
                    mysql::begin(&pool, settings)
                        .await
                        .map(Open::MySql)
                        .map_err(database_failure)
                }
                #[cfg(feature = "sqlite")]
                Backend::Sqlite => {
                    let pool = self
                        .service
                        .sqlite_named(&self.name)
                        .ok_or_else(|| {
                            failure(format!(
                                "sqlite connection \"{}\" is gone",
                                self.name
                            ))
                        })?;
                    sqlite::begin(&pool, settings)
                        .await
                        .map(Open::Sqlite)
                        .map_err(database_failure)
                }
            }
        };
        tokio::select! {
            open = begin => open,
            () = self.execution.stopping().cancelled() => Err(stopped())
        }
    }
}

impl UserData for Database {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method(
            "query",
            |lua,
             this: UserDataRef<Database>,
             (sql, values): (String, Variadic<Value>)| async move {
                let outcome = this.run(&sql, params(&lua, values)?).await?;
                rows(&lua, outcome)
            },
        );
        methods.add_async_method(
            "first",
            |lua,
             this: UserDataRef<Database>,
             (sql, values): (String, Variadic<Value>)| async move {
                let outcome = this.run(&sql, params(&lua, values)?).await?;
                rows(&lua, outcome)?.get::<Value>(1)
            },
        );
        methods.add_async_method(
            "execute",
            |lua,
             this: UserDataRef<Database>,
             (sql, values): (String, Variadic<Value>)| async move {
                let outcome = this.run(&sql, params(&lua, values)?).await?;
                done(&lua, &outcome)
            },
        );
        methods.add_async_method(
            "transaction",
            |lua,
             this: UserDataRef<Database>,
             (body, options): (Function, Option<Table>)| async move {
                let open = this.begin(settings(options)?).await?;
                Transaction::call(lua, Arc::clone(&this.execution), open, body)
                    .await
            },
        );
        methods.add_method("name", |_, this, ()| Ok(this.name.clone()));
    }
}

/// An open transaction on one pooled connection.
enum Open {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::Transaction<'static, sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlite::Transaction),
}

impl Open {
    async fn run(
        &mut self,
        sql: &str,
        params: Vec<Param>,
    ) -> Result<Outcome, sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => {
                postgres::run(transaction, sql, params).await
            }
            #[cfg(feature = "mysql")]
            Self::MySql(transaction) => {
                mysql::run(transaction, sql, params).await
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => {
                sqlite::run_transaction(transaction, sql, params).await
            }
        }
    }

    async fn commit(self) -> Result<(), sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => transaction.commit().await,
            #[cfg(feature = "mysql")]
            Self::MySql(transaction) => transaction.commit().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => transaction.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => transaction.rollback().await,
            #[cfg(feature = "mysql")]
            Self::MySql(transaction) => transaction.rollback().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => transaction.rollback().await,
        }
    }
}

/// The handle a transaction's function receives: `query`, `first` and
/// `execute`, on the transaction's connection.
struct Transaction {
    state: Arc<Mutex<Option<Open>>>,
    execution: Arc<Execution>,
}

impl Transaction {
    async fn call(
        lua: Lua,
        execution: Arc<Execution>,
        open: Open,
        body: Function,
    ) -> mlua::Result<MultiValue> {
        let state = Arc::new(Mutex::new(Some(open)));
        let handle =
            lua.create_userdata(Self { state: Arc::clone(&state), execution })?;
        let returned = body.call_async::<MultiValue>(handle).await;
        // A retained handle must be unusable after its callback finishes.
        let open = state.lock().await.take();
        match (returned, open) {
            (Ok(values), Some(open)) => {
                open.commit().await.map_err(database_failure)?;
                Ok(values)
            }
            (Err(error), Some(open)) => {
                // Preserve the callback's error if rollback also fails.
                let _ = open.rollback().await;
                Err(error)
            }
            (Ok(_), None) => Err(failure("the transaction has ended")),
            (Err(error), None) => Err(error),
        }
    }

    async fn run(
        &self,
        sql: &str,
        params: Vec<Param>,
    ) -> mlua::Result<Outcome> {
        let mut state = self.state.lock().await;
        let open = state
            .as_mut()
            .ok_or_else(|| failure("the transaction has ended"))?;
        let outcome = tokio::select! {
            outcome = open.run(sql, params) => outcome,
            () = self.execution.stopping().cancelled() => return Err(stopped())
        };
        #[cfg(feature = "sqlite")]
        if outcome.is_err() {
            let ended = match open {
                Open::Sqlite(transaction) => {
                    transaction.retire_if_ended().await
                }
                #[cfg(any(feature = "postgres", feature = "mysql"))]
                _ => false,
            };
            if ended {
                state.take();
            }
        }
        outcome.map_err(database_failure)
    }
}

impl UserData for Transaction {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method(
            "query",
            |lua,
             this: UserDataRef<Transaction>,
             (sql, values): (String, Variadic<Value>)| async move {
                let outcome = this.run(&sql, params(&lua, values)?).await?;
                rows(&lua, outcome)
            },
        );
        methods.add_async_method(
            "first",
            |lua,
             this: UserDataRef<Transaction>,
             (sql, values): (String, Variadic<Value>)| async move {
                let outcome = this.run(&sql, params(&lua, values)?).await?;
                rows(&lua, outcome)?.get::<Value>(1)
            },
        );
        methods.add_async_method(
            "execute",
            |lua,
             this: UserDataRef<Transaction>,
             (sql, values): (String, Variadic<Value>)| async move {
                let outcome = this.run(&sql, params(&lua, values)?).await?;
                done(&lua, &outcome)
            },
        );
    }
}
