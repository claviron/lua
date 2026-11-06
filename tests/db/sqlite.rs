use std::sync::Arc;
use std::time::Duration;

use mlua::{Function, Lua, LuaSerdeExt, MultiValue};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::{begin, run, run_transaction};
use crate::db::{DbExtension, Open, Param, Settings, Transaction, rows};
use crate::{Execution, Extension};

async fn pool() -> sqlx::SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .in_memory(true)
                .busy_timeout(Duration::ZERO),
        )
        .await
        .unwrap()
}

async fn callback(
    lua: &Lua,
    pool: &sqlx::SqlitePool,
    source: &str,
) -> mlua::Result<MultiValue> {
    let body: Function = lua.load(source).eval()?;
    let open = begin(pool, Settings::default()).await.unwrap();
    Transaction::call(lua.clone(), Execution::new(), Open::Sqlite(open), body)
        .await
}

#[tokio::test]
async fn sqlite_lua_queries_bind_values_and_commit_callback_results() {
    let pool = pool().await;
    let lua = Lua::new();
    lua.globals().set("null", lua.null()).unwrap();
    let values = callback(&lua, &pool, r#"
        return function(tx)
            saved = tx
            tx:execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT, flag INTEGER, n, payload TEXT, bytes BLOB, missing)")
            local inserted = tx:execute("INSERT INTO sample VALUES (?, ?, ?, ?, ?, ?, ?)",
                -7, "x'); DROP TABLE sample; --", true, 2.5, {answer = 42}, string.char(255, 0), null)
            assert(inserted.affected == 1 and inserted.insert_id == -7)
            local row = tx:first("SELECT *, json_extract(payload, '$.answer') AS answer FROM sample WHERE id = ?", -7)
            assert(row.id == -7 and row.flag == 1 and row.n == 2.5)
            assert(row.name == "x'); DROP TABLE sample; --" and row.answer == 42)
            assert(row.bytes == string.char(255, 0) and row.missing == null)
            assert(tx:first("SELECT * FROM sample WHERE id = ?", 99) == nil)
            assert(#tx:query("SELECT * FROM sample WHERE id = ?", 99) == 0)
            assert(tx:execute("UPDATE sample SET name = ? WHERE id = ?", "changed", -7).affected == 1)
            assert(tx:query("SELECT name FROM sample")[1].name == "changed")
            assert(tx:execute("INSERT INTO sample (id) VALUES (?)", 0).insert_id == 0)
            assert(tx:execute("DELETE FROM sample WHERE id = ?", 0).affected == 1)
            return "committed", 42
        end
    "#).await.unwrap();
    assert_eq!(
        lua.unpack_multi::<(String, i64)>(values).unwrap(),
        ("committed".into(), 42)
    );
    let name: String = sqlx::query_scalar("SELECT name FROM sample")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(name, "changed");
    let error = lua
        .load("return saved:first('SELECT 1')")
        .eval_async::<mlua::Value>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("the transaction has ended"));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_callback_and_statement_errors_roll_back() {
    let pool = pool().await;
    sqlx::query("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let lua = Lua::new();
    for source in [
        "return function(tx) tx:execute('INSERT INTO sample VALUES (1)'); error('callback failed') end",
        "return function(tx) tx:execute('INSERT INTO sample VALUES (1)'); tx:execute('INSERT INTO sample VALUES (1)') end",
    ] {
        assert!(callback(&lua, &pool, source).await.is_err());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sample")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    pool.close().await;
}

#[tokio::test]
async fn sqlite_auto_rollback_retires_stale_transaction_connection() {
    use sqlx::Connection;

    let pool = pool().await;
    sqlx::query("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let mut open = begin(&pool, Settings::default()).await.unwrap();
    run_transaction(&mut open, "INSERT INTO sample VALUES (1)", vec![])
        .await
        .unwrap();
    let error = run_transaction(
        &mut open,
        "INSERT OR ROLLBACK INTO sample VALUES (1)",
        vec![],
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("1555")
    );
    assert!(open.retire_if_ended().await);
    drop(open);

    // Capacity one requires release of the retired lease. A replacement must
    // not inherit the driver's stale transaction depth.
    let mut connection = pool.acquire().await.unwrap();
    assert!(!connection.is_in_transaction());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT 42")
            .fetch_one(&mut *connection)
            .await
            .unwrap(),
        42
    );
    drop(connection);
    begin(&pool, Settings::default()).await.unwrap().rollback().await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn sqlite_auto_rollback_check_preserves_recoverable_constraint() {
    let pool = pool().await;
    sqlx::query("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let mut open = begin(&pool, Settings::default()).await.unwrap();
    run_transaction(&mut open, "INSERT INTO sample VALUES (1)", vec![])
        .await
        .unwrap();
    assert!(
        run_transaction(&mut open, "INSERT INTO sample VALUES (1)", vec![])
            .await
            .is_err()
    );
    assert!(!open.retire_if_ended().await);
    run_transaction(&mut open, "INSERT INTO sample VALUES (2)", vec![])
        .await
        .unwrap();
    open.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sample")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_auto_rollback_unfinished_owner_discards_lease() {
    use sqlx::Connection;

    let pool = pool().await;
    sqlx::query("CREATE TEMP TABLE previous_owner (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    let mut open = begin(&pool, Settings::default()).await.unwrap();
    run_transaction(&mut open, "INSERT INTO previous_owner VALUES (1)", vec![])
        .await
        .unwrap();
    drop(open);

    let mut connection = pool.acquire().await.unwrap();
    assert!(!connection.is_in_transaction());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sqlite_temp_master WHERE name = 'previous_owner'",
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap(),
        0,
        "an unfinished owner must close its connection, including session state",
    );
    drop(connection);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_decodes_each_rows_storage_class_and_rejects_duplicate_columns()
{
    let pool = pool().await;
    let mut connection = pool.acquire().await.unwrap();
    let outcome = run(&mut connection,
        "SELECT ? AS value UNION ALL SELECT ? UNION ALL SELECT ? UNION ALL SELECT ? UNION ALL SELECT ?",
        vec![Param::Integer(i64::MAX), Param::Text("text".into()), Param::Float(1.25), Param::Bytes(vec![255, 0]), Param::Null],
    ).await.unwrap();
    let lua = Lua::new();
    lua.globals().set("rows", rows(&lua, outcome).unwrap()).unwrap();
    lua.globals().set("null", lua.null()).unwrap();
    lua.load(
        r#"
        assert(rows[1].value == math.maxinteger)
        assert(rows[2].value == "text" and rows[3].value == 1.25)
        assert(rows[4].value == string.char(255, 0) and rows[5].value == null)
    "#,
    )
    .exec()
    .unwrap();
    let outcome = run(&mut connection, "SELECT 1 AS value, 2 AS value", vec![])
        .await
        .unwrap();
    assert!(
        rows(&lua, outcome).unwrap_err().to_string().contains("two columns")
    );
    drop(connection);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_transaction_options_are_not_silently_ignored() {
    let pool = pool().await;
    for settings in [
        Settings { isolation: None, read_only: true },
        Settings { isolation: Some("READ UNCOMMITTED"), read_only: false },
        Settings { isolation: Some("READ COMMITTED"), read_only: false },
        Settings { isolation: Some("REPEATABLE READ"), read_only: false },
    ] {
        assert!(begin(&pool, settings).await.is_err());
    }
    begin(
        &pool,
        Settings { isolation: Some("SERIALIZABLE"), read_only: false },
    )
    .await
    .unwrap()
    .rollback()
    .await
    .unwrap();
    pool.close().await;
}

#[test]
fn sqlite_requires_a_declared_connection() {
    let lua = Lua::new();
    let extension = DbExtension::new(Arc::new(claviron_db::DbService::empty(
        Default::default(),
    )));
    let module = extension.module(&lua, &Execution::new()).unwrap();
    let sqlite: Function = module.get("sqlite").unwrap();
    let error = sqlite.call::<mlua::Value>("missing").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sqlite connection \"missing\" is not declared")
    );
}
