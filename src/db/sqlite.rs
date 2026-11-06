//! SQLite: bound parameters, runtime value types and pooled transactions.

use std::ffi::{c_int, c_void};

use futures_util::TryStreamExt;
use sqlx::pool::PoolConnection;
use sqlx::query::Query;
use sqlx::sqlite::{
    SqliteArguments, SqliteConnection, SqliteRow, SqliteTransactionManager,
};
use sqlx::{Column, Either, Row, Sqlite, SqlitePool, TypeInfo, ValueRef};
use sqlx_core::transaction::TransactionManager;

use super::{Cell, InsertId, Outcome, Param, Settings, unsupported};

// Stable SQLite C API, linked by SQLx. Its locked handle excludes concurrent
// worker access while inspecting the native transaction state.
unsafe extern "C" {
    fn sqlite3_get_autocommit(connection: *mut c_void) -> c_int;
}

/// Own the pool lease so an automatically rolled-back connection can be closed
/// instead of returning SQLx's stale transaction depth to the next borrower.
pub(super) struct Transaction {
    connection: PoolConnection<Sqlite>,
    open: bool,
}

impl Transaction {
    pub(super) async fn commit(mut self) -> Result<(), sqlx::Error> {
        if let Err(error) =
            SqliteTransactionManager::commit(&mut self.connection).await
        {
            self.connection.close_on_drop();
            return Err(error);
        }
        self.open = false;
        Ok(())
    }

    pub(super) async fn rollback(mut self) -> Result<(), sqlx::Error> {
        if let Err(error) =
            SqliteTransactionManager::rollback(&mut self.connection).await
        {
            self.connection.close_on_drop();
            return Err(error);
        }
        self.open = false;
        Ok(())
    }

    pub(super) async fn retire_if_ended(&mut self) -> bool {
        let ended = match self.connection.lock_handle().await {
            Ok(mut handle) => {
                // SAFETY: the connection is live and SQLx's worker is excluded
                // until this guard is dropped. SQLite does not mutate here.
                unsafe {
                    sqlite3_get_autocommit(
                        handle.as_raw_handle().as_ptr().cast(),
                    ) != 0
                }
            }
            // A connection whose native state cannot be inspected must not be
            // used to continue the transaction or returned to the pool.
            Err(_) => true,
        };
        if ended {
            self.connection.close_on_drop();
            self.open = false;
        }
        ended
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.open {
            // Dropping the owner can race a native automatic rollback before
            // its error reaches Lua. Queue cleanup, then discard the lease so
            // an unobserved failure cannot leak stale depth into the pool.
            self.connection.close_on_drop();
            SqliteTransactionManager::start_rollback(&mut self.connection);
        }
    }
}

pub(super) async fn begin(
    pool: &SqlitePool,
    settings: Settings,
) -> Result<Transaction, sqlx::Error> {
    // Do not change pooled connection PRAGMAs to imitate per-transaction
    // options: those changes would leak to the next borrower.
    if settings.read_only {
        return Err(sqlx::Error::Protocol(
            "sqlite does not support a per-transaction read_only option".into(),
        ));
    }
    if settings.isolation.is_some_and(|level| level != "SERIALIZABLE") {
        return Err(sqlx::Error::Protocol(
            "sqlite transactions support only serializable isolation".into(),
        ));
    }
    // Arm drop cleanup before awaiting BEGIN, including cancellation while
    // SQLx's worker is opening the transaction.
    let mut transaction =
        Transaction { connection: pool.acquire().await?, open: true };
    SqliteTransactionManager::begin(&mut transaction.connection, None).await?;
    Ok(transaction)
}

pub(super) async fn run_transaction(
    transaction: &mut Transaction,
    sql: &str,
    params: Vec<Param>,
) -> Result<Outcome, sqlx::Error> {
    run(&mut transaction.connection, sql, params).await
}

pub(super) async fn run(
    connection: &mut SqliteConnection,
    sql: &str,
    params: Vec<Param>,
) -> Result<Outcome, sqlx::Error> {
    // The script owns the SQL statement; values remain bound parameters.
    let mut statement = sqlx::query(sqlx::AssertSqlSafe(sql));
    for param in params {
        statement = bind(statement, param);
    }
    #[allow(deprecated)]
    let mut stream = statement.fetch_many(connection);
    let mut outcome = Outcome::default();
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(done) => {
                outcome.affected =
                    outcome.affected.saturating_add(done.rows_affected());
                outcome.insert_id =
                    Some(InsertId::Signed(done.last_insert_rowid()));
            }
            Either::Right(row) => push(&mut outcome, &row)?,
        }
    }
    Ok(outcome)
}

fn bind(
    query: Query<'_, Sqlite, SqliteArguments>,
    param: Param,
) -> Query<'_, Sqlite, SqliteArguments> {
    match param {
        Param::Null => query.bind(Option::<String>::None),
        Param::Boolean(value) => query.bind(value),
        Param::Integer(value) => query.bind(value),
        Param::Float(value) => query.bind(value),
        Param::Text(value) => query.bind(value),
        Param::Bytes(value) => query.bind(value),
        Param::Json(value) => query.bind(sqlx::types::Json(value)),
    }
}

fn push(
    outcome: &mut Outcome,
    row: &SqliteRow,
) -> Result<(), sqlx::Error> {
    if outcome.columns.is_empty() {
        outcome.columns = row
            .columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect();
    }
    let values = row
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let raw = row.try_get_raw(index)?;
            if raw.is_null() {
                return Ok(Cell::Null);
            }
            // SQLite permits different storage classes in successive rows of the
            // same column. Decode each value, not the column's declared affinity.
            Ok(match raw.type_info().name() {
                "INTEGER" => Cell::Integer(row.try_get(index)?),
                "REAL" => Cell::Float(row.try_get(index)?),
                "TEXT" => Cell::Text(row.try_get(index)?),
                "BLOB" => Cell::Bytes(row.try_get(index)?),
                other => {
                    return Err(unsupported("sqlite", column.name(), other));
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    outcome.rows.push(values);
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/db/sqlite.rs"]
mod tests;
