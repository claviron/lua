//! PostgreSQL: binds, the row decoder and `BEGIN`.

use futures_util::TryStreamExt;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{
    PgArgumentBuffer, PgArguments, PgConnection, PgRow, PgTypeInfo,
};
use sqlx::query::Query;
use sqlx::{
    Column, Either, PgPool, Postgres, Row, Transaction, Type, TypeInfo,
    ValueRef,
};

use super::{Cell, Outcome, Param, Settings, unsupported};

pub(super) async fn begin(
    pool: &PgPool,
    settings: Settings,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let mut statement = String::from("BEGIN");
    if let Some(level) = settings.isolation {
        statement.push_str(" ISOLATION LEVEL ");
        statement.push_str(level);
    }
    if settings.read_only {
        statement.push_str(" READ ONLY");
    }
    // Built only from the fixed words above.
    pool.begin_with(sqlx::AssertSqlSafe(statement)).await
}

pub(super) async fn run(
    connection: &mut PgConnection,
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
            }
            Either::Right(row) => push(&mut outcome, &row)?,
        }
    }
    Ok(outcome)
}

fn bind(
    query: Query<'_, Postgres, PgArguments>,
    param: Param,
) -> Query<'_, Postgres, PgArguments> {
    match param {
        Param::Null => query.bind(PgNull),
        Param::Boolean(value) => query.bind(value),
        Param::Integer(value) => query.bind(value),
        Param::Float(value) => query.bind(value),
        Param::Text(value) => query.bind(value),
        Param::Bytes(value) => query.bind(value),
        Param::Json(value) => query.bind(sqlx::types::Json(value)),
    }
}

/// OID zero asks the server to infer the parameter's type from the SQL, so an
/// untyped `nil` fits any nullable column instead of being declared TEXT.
struct PgNull;

impl Type<Postgres> for PgNull {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(0))
    }
}

impl Encode<'_, Postgres> for PgNull {
    fn encode_by_ref(
        &self,
        _buffer: &mut PgArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        Ok(IsNull::Yes)
    }
}

fn push(
    outcome: &mut Outcome,
    row: &PgRow,
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
    row: &PgRow,
    index: usize,
    column: &str,
    type_name: &str,
) -> Result<Cell, sqlx::Error> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(Cell::Null);
    }
    Ok(match type_name {
        "BOOL" => Cell::Boolean(row.try_get(index)?),
        "INT2" => Cell::Integer(i64::from(row.try_get::<i16, _>(index)?)),
        "INT4" => Cell::Integer(i64::from(row.try_get::<i32, _>(index)?)),
        "INT8" => Cell::Integer(row.try_get(index)?),
        "FLOAT4" => Cell::Float(f64::from(row.try_get::<f32, _>(index)?)),
        "FLOAT8" => Cell::Float(row.try_get(index)?),
        "BYTEA" => Cell::Bytes(row.try_get(index)?),
        "JSON" | "JSONB" => Cell::Json(
            row.try_get::<sqlx::types::Json<serde_json::Value>, _>(index)?.0,
        ),
        "CHAR" | "NAME" | "TEXT" | "VARCHAR" | "BPCHAR" | "UNKNOWN" => {
            Cell::Text(row.try_get(index)?)
        }
        _ => return Err(unsupported("postgres", column, type_name)),
    })
}
