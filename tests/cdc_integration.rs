//! CDC integration tests — create their own Firebird databases.
//! Requires: Firebird 5 on localhost:3050 (SYSDBA/masterkey) + PostgreSQL.
//!
//! Optional env vars (defaults shown):
//!   FBCLIENT_PATH  — full path to fbclient.dll  (auto-detected)
//!   TEST_FDB_DIR   — dir Firebird can write to   (C:\Temp)
//!   TEST_PG_HOST   — postgres host               (localhost)
//!   TEST_PG_PORT   — postgres port               (5432)
//!   TEST_PG_USER   — postgres user               (postgres)
//!   TEST_PG_PASS   — postgres password           (postgres)
//!   TEST_PG_DB     — postgres database           (postgres)
//!
//! Run:
//!   cargo test --release --test cdc_integration -- --nocapture
//!   (--release required: rsfbclient 0.23 XSQLDA has UB caught by Rust debug checks)

use std::collections::HashMap;
use fdb_extract::cdc::{sync_table, TableCache};
use fdb_extract::ods::OdsReader;
use postgres::{Client, NoTls};
use rsfbclient::prelude::*;

// ── Shared helpers ────────────────────────────────────────────────────────────

fn evar(key: &str) -> Option<String> { std::env::var(key).ok() }

fn fbclient_path() -> String {
    evar("FBCLIENT_PATH").unwrap_or_else(|| {
        for path in &[
            r"C:\Firebird-5\fbclient.dll",
            r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
            r"C:\Program Files\Firebird\Firebird_5_0\fbclient.dll",
            r"C:\Program Files (x86)\Firebird\Firebird_4_0\fbclient.dll",
            r"C:\Firebird\fbclient.dll",
        ] {
            if std::path::Path::new(path).exists() { return path.to_string(); }
        }
        "fbclient.dll".to_string()
    })
}

fn temp_fdb_path(tag: &str) -> String {
    let dir = evar("TEST_FDB_DIR").unwrap_or_else(|| r"C:\Temp".to_string());
    std::fs::create_dir_all(&dir).ok();
    format!("{}\\test_cdc_{}_{}.fdb", dir, std::process::id(), tag)
}

fn pg_connect() -> Client {
    let host = evar("TEST_PG_HOST").unwrap_or_else(|| "localhost".into());
    let port: u16 = evar("TEST_PG_PORT").unwrap_or_else(|| "5432".into()).parse().unwrap();
    let user = evar("TEST_PG_USER").unwrap_or_else(|| "postgres".into());
    let pass = evar("TEST_PG_PASS").unwrap_or_else(|| "postgres".into());
    let db   = evar("TEST_PG_DB").unwrap_or_else(|| "postgres".into());
    Client::configure()
        .host(&host).port(port).user(&user).password(&pass).dbname(&db)
        .connect(NoTls)
        .unwrap_or_else(|e| panic!("PG connect failed: {e:#?}"))
}

fn pg_count(pg: &mut Client, table: &str, pk_col: &str, pk_val: i32) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" = $1",
                      table.to_lowercase(), pk_col.to_lowercase());
    pg.query_one(&sql, &[&pk_val]).unwrap().get::<_, i64>(0)
}

fn pg_int(pg: &mut Client, table: &str, pk_col: &str, pk_val: i32, col: &str) -> Option<i32> {
    let sql = format!("SELECT \"{}\" FROM \"{}\" WHERE \"{}\" = $1",
                      col.to_lowercase(), table.to_lowercase(), pk_col.to_lowercase());
    pg.query_opt(&sql, &[&pk_val]).unwrap().map(|r| r.get::<_, i32>(0))
}

/// Build PG mirror table (DROP + CREATE) and run initial sync. Returns (stats, pk_fields, page_gens).
fn initial_sync(
    fdb_path: &str,
    table:    &str,
    pg:       &mut Client,
) -> (fdb_extract::cdc::DeltaStats, Vec<String>, HashMap<u32, u32>) {
    let pg_table = table.to_lowercase();
    pg.execute(&format!("DROP TABLE IF EXISTS \"{pg_table}\""), &[]).ok();

    let db  = OdsReader::open(fdb_path).unwrap();
    let sql = fdb_extract::schema::create_table_sql_from(&db, table, false).unwrap();
    pg.batch_execute(&sql).unwrap_or_else(|e| panic!("CREATE TABLE failed: {e}"));

    let pk_fields = db.read_primary_key_fields(table)
        .unwrap_or_else(|e| panic!("read PK fields: {e}"));
    let mut gens: HashMap<u32, u32> = HashMap::new();
    let mut cache: Option<TableCache> = None;

    let db   = OdsReader::open(fdb_path).unwrap();
    let stat = sync_table(&db, pg, table, 0, &pk_fields, &mut gens, &mut cache, None, false)
        .unwrap_or_else(|e| panic!("Initial sync failed: {e}"));
    (stat, pk_fields, gens)
}

fn delta_sync(
    fdb_path:  &str,
    table:     &str,
    last_txn:  u64,
    pk_fields: &[String],
    gens:      &mut HashMap<u32, u32>,
    pg:        &mut Client,
    debug:     bool,
) -> fdb_extract::cdc::DeltaStats {
    let db = OdsReader::open(fdb_path).unwrap();
    let mut cache: Option<TableCache> = None;
    sync_table(&db, pg, table, last_txn, pk_fields, gens, &mut cache, None, debug)
        .unwrap_or_else(|e| panic!("Delta sync failed: {e}"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────
// Each test uses a unique table name to avoid PG conflicts when running in parallel.
// Run with: cargo test --release --test cdc_integration -- --nocapture

/// INSERT into Firebird → CDC → row appears in PostgreSQL.
#[test]
fn cdc_insert_syncs_to_postgres() {
    // Table name unique to this test to avoid parallel conflicts in PG
    let table  = "CDC_TEST_INSERT";
    let pk_col = "CODCLIENTE";
    let pk: i32 = -100_001;
    let fdb    = temp_fdb_path("insert");

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute("CREATE TABLE CDC_TEST_INSERT (CODCLIENTE INTEGER NOT NULL PRIMARY KEY)", ())
        .unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));

    let mut pg = pg_connect();
    pg.batch_execute("SET synchronous_commit = off").ok();

    // initial sync (empty table)
    let (s0, pk_fields, mut gens) = initial_sync(&fdb, table, &mut pg);
    let last_txn = s0.max_txn;
    eprintln!("Initial: {} upserts, last_txn={last_txn}", s0.upserts);

    // insert
    fb.execute("INSERT INTO CDC_TEST_INSERT (CODCLIENTE) VALUES (?)", (pk,))
        .unwrap_or_else(|e| panic!("INSERT: {e}"));
    eprintln!("Inserted pk={pk}");
    std::thread::sleep(std::time::Duration::from_millis(200));

    // delta CDC
    let s1 = delta_sync(&fdb, table, last_txn, &pk_fields, &mut gens, &mut pg, true);
    eprintln!("Delta: {} upserts, {} deletes", s1.upserts, s1.deletes);

    let rows = pg.query(
        &format!("SELECT \"{}\" FROM \"{}\" ORDER BY 1", pk_col.to_lowercase(), table.to_lowercase()),
        &[],
    ).unwrap();
    eprintln!("PG rows after CDC ({}):", rows.len());
    for row in &rows { eprintln!("  {pk_col}={}", row.get::<_, i32>(0)); }

    let count = pg_count(&mut pg, table, pk_col, pk);

    pg.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table.to_lowercase()), &[]).ok();
    fb.drop_database().ok();

    assert_eq!(s1.upserts, 1, "must detect 1 upsert");
    assert_eq!(s1.deletes, 0);
    assert_eq!(count, 1, "row must be in PG");
}

/// DELETE from Firebird → CDC → row disappears from PostgreSQL.
#[test]
fn cdc_delete_syncs_to_postgres() {
    let table  = "CDC_TEST_DELETE";
    let pk_col = "CODCLIENTE";
    let pk: i32 = -100_002;
    let fdb    = temp_fdb_path("delete");

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute("CREATE TABLE CDC_TEST_DELETE (CODCLIENTE INTEGER NOT NULL PRIMARY KEY)", ())
        .unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));

    let mut pg = pg_connect();
    pg.batch_execute("SET synchronous_commit = off").ok();

    // insert + initial sync
    fb.execute("INSERT INTO CDC_TEST_DELETE (CODCLIENTE) VALUES (?)", (pk,)).unwrap();
    let (s0, pk_fields, mut gens) = initial_sync(&fdb, table, &mut pg);
    let last_txn = s0.max_txn;
    eprintln!("Initial: {} upserts, last_txn={last_txn}", s0.upserts);
    assert_eq!(pg_count(&mut pg, table, pk_col, pk), 1, "must be in PG before delete");

    // delete
    fb.execute("DELETE FROM CDC_TEST_DELETE WHERE CODCLIENTE = ?", (pk,))
        .unwrap_or_else(|e| panic!("DELETE: {e}"));
    eprintln!("Deleted pk={pk}");
    std::thread::sleep(std::time::Duration::from_millis(200));

    // delta CDC
    let s1 = delta_sync(&fdb, table, last_txn, &pk_fields, &mut gens, &mut pg, true);
    eprintln!("Delta: {} upserts, {} deletes", s1.upserts, s1.deletes);

    let rows = pg.query(
        &format!("SELECT \"{}\" FROM \"{}\" ORDER BY 1", pk_col.to_lowercase(), table.to_lowercase()),
        &[],
    ).unwrap();
    eprintln!("PG rows after CDC ({}):", rows.len());
    for row in &rows { eprintln!("  {pk_col}={}", row.get::<_, i32>(0)); }

    let count = pg_count(&mut pg, table, pk_col, pk);

    pg.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table.to_lowercase()), &[]).ok();
    fb.drop_database().ok();

    assert_eq!(s1.deletes, 1, "must detect 1 delete");
    assert_eq!(s1.upserts, 0);
    assert_eq!(count, 0, "row must be GONE from PG");
}

/// UPDATE in Firebird → CDC → updated value appears in PostgreSQL.
#[test]
fn cdc_update_syncs_to_postgres() {
    let table  = "CDC_TEST_UPDATE";
    let pk_col = "CODCLIENTE";
    let pk: i32 = -100_003;
    let fdb    = temp_fdb_path("update");

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute(
        "CREATE TABLE CDC_TEST_UPDATE (CODCLIENTE INTEGER NOT NULL PRIMARY KEY, VALOR INTEGER)",
        (),
    ).unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));

    let mut pg = pg_connect();
    pg.batch_execute("SET synchronous_commit = off").ok();

    // insert valor=10 + initial sync
    fb.execute("INSERT INTO CDC_TEST_UPDATE (CODCLIENTE, VALOR) VALUES (?, ?)", (pk, 10i32))
        .unwrap_or_else(|e| panic!("INSERT: {e}"));
    let (s0, pk_fields, mut gens) = initial_sync(&fdb, table, &mut pg);
    let last_txn = s0.max_txn;
    eprintln!("Initial: {} upserts, last_txn={last_txn}", s0.upserts);
    assert_eq!(pg_int(&mut pg, table, pk_col, pk, "VALOR"), Some(10), "initial valor must be 10");

    // update valor → 20
    fb.execute("UPDATE CDC_TEST_UPDATE SET VALOR = ? WHERE CODCLIENTE = ?", (20i32, pk))
        .unwrap_or_else(|e| panic!("UPDATE: {e}"));
    eprintln!("Updated pk={pk} VALOR 10→20");
    std::thread::sleep(std::time::Duration::from_millis(200));

    // delta CDC
    let s1 = delta_sync(&fdb, table, last_txn, &pk_fields, &mut gens, &mut pg, true);
    eprintln!("Delta: {} upserts, {} deletes", s1.upserts, s1.deletes);

    let rows = pg.query(
        &format!("SELECT \"{}\", \"valor\" FROM \"{}\" ORDER BY 1", pk_col.to_lowercase(), table.to_lowercase()),
        &[],
    ).unwrap();
    eprintln!("PG rows after CDC ({}):", rows.len());
    for row in &rows {
        eprintln!("  {pk_col}={} VALOR={}", row.get::<_, i32>(0), row.get::<_, i32>(1));
    }

    let valor = pg_int(&mut pg, table, pk_col, pk, "VALOR");

    pg.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table.to_lowercase()), &[]).ok();
    fb.drop_database().ok();

    assert_eq!(s1.upserts, 1, "must detect 1 upsert");
    assert_eq!(s1.deletes, 0);
    assert_eq!(valor, Some(20), "VALOR must be 20 in PG");
}

/// DELETE in txn A + INSERT in txn B (two separate transactions) → single CDC cycle captures both.
#[test]
fn cdc_two_separate_txns_single_cycle() {
    let table  = "CDC_TEST_2TXN";
    let pk_col = "CODCLIENTE";
    let pk_a: i32 = -888_881;  // deleted in txn A
    let pk_b: i32 = -888_882;  // inserted in txn B
    let fdb    = temp_fdb_path("2txn");

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");
    eprintln!("pk_a={pk_a} (delete txn)  pk_b={pk_b} (insert txn)");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute(
        "CREATE TABLE CDC_TEST_2TXN (CODCLIENTE INTEGER NOT NULL PRIMARY KEY)", (),
    ).unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));

    let mut pg = pg_connect();
    pg.batch_execute("SET synchronous_commit = off").ok();

    // insert pk_a + initial sync
    fb.execute("INSERT INTO CDC_TEST_2TXN (CODCLIENTE) VALUES (?)", (pk_a,)).unwrap();
    let (s0, pk_fields, mut gens) = initial_sync(&fdb, table, &mut pg);
    let last_txn = s0.max_txn;
    eprintln!("Initial: {} upserts, last_txn={last_txn}", s0.upserts);
    assert_eq!(pg_count(&mut pg, table, pk_col, pk_a), 1);

    // txn A: DELETE pk_a  (separate transaction)
    fb.execute("DELETE FROM CDC_TEST_2TXN WHERE CODCLIENTE = ?", (pk_a,))
        .unwrap_or_else(|e| panic!("DELETE txn A: {e}"));
    eprintln!("Txn A: DELETE pk_a={pk_a}");

    // txn B: INSERT pk_b  (separate transaction)
    fb.execute("INSERT INTO CDC_TEST_2TXN (CODCLIENTE) VALUES (?)", (pk_b,))
        .unwrap_or_else(|e| panic!("INSERT txn B: {e}"));
    eprintln!("Txn B: INSERT pk_b={pk_b}");

    std::thread::sleep(std::time::Duration::from_millis(200));

    // single CDC cycle — must capture both transactions
    let s1 = delta_sync(&fdb, table, last_txn, &pk_fields, &mut gens, &mut pg, true);
    eprintln!("Delta (single cycle): {} upserts, {} deletes, max_txn={}", s1.upserts, s1.deletes, s1.max_txn);

    // SELECT all rows from PG table to see actual state
    let rows = pg.query(
        &format!("SELECT \"{}\" FROM \"{}\" ORDER BY 1", pk_col.to_lowercase(), table.to_lowercase()),
        &[],
    ).unwrap();
    eprintln!("PG rows after CDC ({}):", rows.len());
    for row in &rows {
        let v: i32 = row.get(0);
        eprintln!("  {pk_col}={v}");
    }

    let count_a = pg_count(&mut pg, table, pk_col, pk_a);
    let count_b = pg_count(&mut pg, table, pk_col, pk_b);

    pg.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table.to_lowercase()), &[]).ok();
    fb.drop_database().ok();

    assert_eq!(s1.deletes, 1, "CDC must detect 1 delete (pk_a)");
    assert_eq!(s1.upserts, 1, "CDC must detect 1 upsert (pk_b)");
    assert_eq!(count_a, 0, "pk_a={pk_a} must be GONE from PG");
    assert_eq!(count_b, 1, "pk_b={pk_b} must be IN PG");
}

/// Multi-page: fill 3+ pages, then DELETE from page 1 + INSERT on last page in same txn.
/// Proves CDC scans ALL pages for deletes, not just pag_gen-changed pages.
#[test]
fn cdc_multipage_delete_insert_same_txn() {
    let table  = "CDC_TEST_MULTIPG";
    let pk_col = "CODCLIENTE";
    let fdb    = temp_fdb_path("multipg");

    // ~156 records per 4096-byte page → 500 records fills ~3 pages
    let bulk_count: i32 = 500;
    let delete_pk: i32  = 1;           // on page 1
    let insert_pk: i32  = bulk_count + 1; // beyond all existing → different page

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");
    eprintln!("bulk={bulk_count}  delete_pk={delete_pk}  insert_pk={insert_pk}");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute(
        "CREATE TABLE CDC_TEST_MULTIPG (CODCLIENTE INTEGER NOT NULL PRIMARY KEY)", (),
    ).unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));

    // bulk insert in one transaction to fill 3+ pages
    fb.with_transaction(|tr| {
        for i in 1i32..=bulk_count {
            tr.execute("INSERT INTO CDC_TEST_MULTIPG (CODCLIENTE) VALUES (?)", (i,))?;
        }
        Ok(())
    }).unwrap_or_else(|e| panic!("bulk insert: {e}"));
    eprintln!("Inserted {bulk_count} records");

    let mut pg = pg_connect();
    pg.batch_execute("SET synchronous_commit = off").ok();

    let (s0, pk_fields, mut gens) = initial_sync(&fdb, table, &mut pg);
    let last_txn = s0.max_txn;
    eprintln!("Initial sync: {} upserts, last_txn={last_txn}", s0.upserts);
    assert_eq!(s0.upserts as i32, bulk_count, "all bulk records must sync");
    assert_eq!(pg_count(&mut pg, table, pk_col, delete_pk), 1, "delete_pk must be in PG");

    // same transaction: DELETE from page 1, INSERT beyond all pages
    fb.with_transaction(|tr| {
        tr.execute("DELETE FROM CDC_TEST_MULTIPG WHERE CODCLIENTE = ?", (delete_pk,))?;
        tr.execute("INSERT INTO CDC_TEST_MULTIPG (CODCLIENTE) VALUES (?)", (insert_pk,))?;
        Ok(())
    }).unwrap_or_else(|e| panic!("delete+insert txn: {e}"));
    eprintln!("Same txn: DELETE pk={delete_pk} (page 1) + INSERT pk={insert_pk} (last page)");
    std::thread::sleep(std::time::Duration::from_millis(300));

    // single CDC cycle
    let s1 = delta_sync(&fdb, table, last_txn, &pk_fields, &mut gens, &mut pg, true);
    eprintln!("Delta: {} upserts, {} deletes, pages_total={} pages_skipped={}",
              s1.upserts, s1.deletes, s1.pages_total, s1.pages_skipped);

    let rows = pg.query(
        &format!("SELECT \"{pk_col_lower}\" FROM \"{tbl}\" WHERE \"{pk_col_lower}\" IN ($1,$2) ORDER BY 1",
                 pk_col_lower = pk_col.to_lowercase(), tbl = table.to_lowercase()),
        &[&delete_pk, &insert_pk],
    ).unwrap();
    eprintln!("PG rows for delete_pk/insert_pk ({}):", rows.len());
    for row in &rows { eprintln!("  {pk_col}={}", row.get::<_, i32>(0)); }

    let count_del = pg_count(&mut pg, table, pk_col, delete_pk);
    let count_ins = pg_count(&mut pg, table, pk_col, insert_pk);

    pg.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table.to_lowercase()), &[]).ok();
    fb.drop_database().ok();

    assert!(s1.pages_total > 3,
        "must span >3 pages (got {}), delete and insert on different pages", s1.pages_total);
    assert_eq!(s1.deletes, 1, "must detect 1 delete (pk={delete_pk})");
    assert_eq!(s1.upserts, 1, "must detect 1 upsert (pk={insert_pk})");
    assert_eq!(count_del, 0, "pk={delete_pk} must be GONE from PG");
    assert_eq!(count_ins, 1, "pk={insert_pk} must be IN PG");
}

/// Views must NOT appear in list_tables() — only base tables.
#[test]
fn views_excluded_from_list_tables() {
    let base = "VIEW_TEST_BASE";
    let view = "VIEW_TEST_VIEW";
    let fdb  = temp_fdb_path("views");

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute(
        "CREATE TABLE VIEW_TEST_BASE (CODCLIENTE INTEGER NOT NULL PRIMARY KEY, VALOR INTEGER)", (),
    ).unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));
    fb.execute(
        "CREATE VIEW VIEW_TEST_VIEW AS SELECT CODCLIENTE, VALOR FROM VIEW_TEST_BASE", (),
    ).unwrap_or_else(|e| panic!("CREATE VIEW: {e}"));

    let db = OdsReader::open(&fdb).unwrap();
    let tables = db.list_tables();
    eprintln!("list_tables: {tables:?}");

    let has_base = tables.iter().any(|t| t.eq_ignore_ascii_case(base));
    let has_view = tables.iter().any(|t| t.eq_ignore_ascii_case(view));

    fb.drop_database().ok();

    assert!(has_base, "base table {base} must be listed");
    assert!(!has_view, "view {view} must NOT be listed as a table");
}

/// DELETE + INSERT in same Firebird transaction → both synced correctly.
#[test]
fn cdc_delete_insert_same_txn_syncs_to_postgres() {
    let table  = "CDC_TEST_TXNDELINS";
    let pk_col = "CODCLIENTE";
    let pk_a: i32 = -999_991;
    let pk_b: i32 = -999_992;
    let fdb    = temp_fdb_path("del_ins_txn");

    let fbclient = fbclient_path();
    eprintln!("fbclient: {fbclient}  FDB: {fdb}");
    eprintln!("pk_a={pk_a} (delete)  pk_b={pk_b} (insert)");

    let mut fb = rsfbclient::builder_native()
        .with_dyn_load(&fbclient)
        .with_remote().host("localhost").port(3050)
        .db_name(&fdb).user("SYSDBA").pass("masterkey")
        .create_database()
        .unwrap_or_else(|e| panic!("create_database: {e}"));
    fb.execute(
        "CREATE TABLE CDC_TEST_TXNDELINS (CODCLIENTE INTEGER NOT NULL PRIMARY KEY)", (),
    ).unwrap_or_else(|e| panic!("CREATE TABLE: {e}"));

    let mut pg = pg_connect();
    pg.batch_execute("SET synchronous_commit = off; SET work_mem = '64MB'").ok();

    // insert pk_a + initial sync
    fb.execute("INSERT INTO CDC_TEST_TXNDELINS (CODCLIENTE) VALUES (?)", (pk_a,)).unwrap();
    let (s0, pk_fields, mut gens) = initial_sync(&fdb, table, &mut pg);
    let last_txn = s0.max_txn;
    eprintln!("Initial: {} upserts, last_txn={last_txn}", s0.upserts);
    assert_eq!(pg_count(&mut pg, table, pk_col, pk_a), 1);

    // delete pk_a + insert pk_b — same transaction
    fb.with_transaction(|tr| {
        tr.execute("DELETE FROM CDC_TEST_TXNDELINS WHERE CODCLIENTE = ?", (pk_a,))?;
        tr.execute("INSERT INTO CDC_TEST_TXNDELINS (CODCLIENTE) VALUES (?)", (pk_b,))?;
        Ok(())
    }).unwrap_or_else(|e| panic!("transaction: {e}"));
    eprintln!("DELETE pk_a={pk_a} + INSERT pk_b={pk_b} (same txn)");
    std::thread::sleep(std::time::Duration::from_millis(300));

    // delta CDC
    let s1 = delta_sync(&fdb, table, last_txn, &pk_fields, &mut gens, &mut pg, true);
    eprintln!("Delta: {} upserts, {} deletes, max_txn={}", s1.upserts, s1.deletes, s1.max_txn);

    let rows = pg.query(
        &format!("SELECT \"{}\" FROM \"{}\" ORDER BY 1", pk_col.to_lowercase(), table.to_lowercase()),
        &[],
    ).unwrap();
    eprintln!("PG rows after CDC ({}):", rows.len());
    for row in &rows { eprintln!("  {pk_col}={}", row.get::<_, i32>(0)); }

    let count_a = pg_count(&mut pg, table, pk_col, pk_a);
    let count_b = pg_count(&mut pg, table, pk_col, pk_b);

    pg.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table.to_lowercase()), &[]).ok();
    fb.drop_database().ok();

    assert_eq!(s1.deletes, 1, "must detect 1 delete");
    assert_eq!(count_a, 0, "pk_a must be GONE from PG");
    assert_eq!(count_b, 1, "pk_b must be IN PG");
}
