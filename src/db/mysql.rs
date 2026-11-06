//! MySQL: binds, the row decoder and `START TRANSACTION`.

use futures_util::TryStreamExt;
use sqlx::mysql::{MySqlArguments, MySqlConnection, MySqlRow};
use sqlx::pool::MaybePoolConnection;
use sqlx::query::Query;
use sqlx::{
    Column, Either, MySql, MySqlPool, Row, SqlSafeStr, Transaction, TypeInfo,
    ValueRef,
};

use super::{Cell, InsertId, Outcome, Param, Settings, unsupported};

pub(super) async fn begin(
    pool: &MySqlPool,
    settings: Settings,
) -> Result<Transaction<'static, MySql>, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    // MySQL sets the level for the next transaction with its own statement.
    if let Some(level) = settings.isolation {
        let statement = format!("SET TRANSACTION ISOLATION LEVEL {level}");
        // Built only from the fixed words `settings` allows.
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&mut *connection)
            .await?;
    }
    let statement = if settings.read_only {
        "START TRANSACTION READ ONLY"
    } else {
        "START TRANSACTION"
    };
    Transaction::begin(
        MaybePoolConnection::PoolConnection(connection),
        Some(sqlx::AssertSqlSafe(statement).into_sql_str()),
    )
    .await
}

pub(super) async fn run(
    connection: &mut MySqlConnection,
    sql: &str,
    params: Vec<Param>,
) -> Result<Outcome, sqlx::Error> {
    // SQL is the script's own input to this driver; values stay bind
    // parameters and are never interpolated.
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
                if done.last_insert_id() != 0 {
                    outcome.insert_id =
                        Some(InsertId::Unsigned(done.last_insert_id()));
                }
            }
            Either::Right(row) => push(&mut outcome, &row)?,
        }
    }
    Ok(outcome)
}

fn bind(
    query: Query<'_, MySql, MySqlArguments>,
    param: Param,
) -> Query<'_, MySql, MySqlArguments> {
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
    row: &MySqlRow,
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
            decode(row, index, column.name(), column.type_info().name())
        })
        .collect::<Result<Vec<_>, _>>()?;
    outcome.rows.push(values);
    Ok(())
}

fn decode(
    row: &MySqlRow,
    index: usize,
    column: &str,
    type_name: &str,
) -> Result<Cell, sqlx::Error> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(Cell::Null);
    }
    Ok(match type_name {
        "BOOLEAN" => Cell::Boolean(row.try_get(index)?),
        "TINYINT" => Cell::Integer(i64::from(row.try_get::<i8, _>(index)?)),
        "SMALLINT" => Cell::Integer(i64::from(row.try_get::<i16, _>(index)?)),
        "MEDIUMINT" | "INT" => {
            Cell::Integer(i64::from(row.try_get::<i32, _>(index)?))
        }
        "BIGINT" => Cell::Integer(row.try_get(index)?),
        "TINYINT UNSIGNED" => {
            Cell::Integer(i64::from(row.try_get::<u8, _>(index)?))
        }
        "SMALLINT UNSIGNED" => {
            Cell::Integer(i64::from(row.try_get::<u16, _>(index)?))
        }
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            Cell::Integer(i64::from(row.try_get::<u32, _>(index)?))
        }
        // Lua's integers are signed 64-bit: a larger value is refused, not
        // rounded into a float.
        "BIGINT UNSIGNED" => {
            let value = row.try_get::<u64, _>(index)?;
            Cell::Integer(i64::try_from(value).map_err(|_| {
                sqlx::Error::Decode(
                    format!(
                        "mysql column \"{column}\" holds {value}, past Lua's largest integer; \
                         cast it in SQL (CAST({column} AS CHAR))"
                    )
                    .into()
                )
            })?)
        }
        "FLOAT" => Cell::Float(f64::from(row.try_get::<f32, _>(index)?)),
        "DOUBLE" => Cell::Float(row.try_get(index)?),
        "JSON" => Cell::Json(
            row.try_get::<sqlx::types::Json<serde_json::Value>, _>(index)?.0,
        ),
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB"
        | "LONGBLOB" => Cell::Bytes(row.try_get(index)?),
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT"
        | "LONGTEXT" | "ENUM" | "SET" => Cell::Text(row.try_get(index)?),
        _ => return Err(unsupported("mysql", column, type_name)),
    })
}
