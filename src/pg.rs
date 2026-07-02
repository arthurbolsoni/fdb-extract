use anyhow::Result;
use postgres::{Client, NoTls, error::SqlState};
use std::io::{BufWriter, Write};
use std::time::Instant;

use crate::ods::OdsReader;

// ── Timing wrapper ────────────────────────────────────────────────────────────
// Sits between BufWriter (128 MB buffer) and CopyInWriter (network).
// write() is called only on actual network flushes — negligible Instant overhead.

struct TimingWrite<W: Write> {
    inner:   W,
    pg_ns:   u64,
    bytes:   u64,
}

impl<W: Write> Write for TimingWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let t = Instant::now();
        let n = self.inner.write(buf)?;
        self.pg_ns += t.elapsed().as_nanos() as u64;
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let t = Instant::now();
        let r = self.inner.flush();
        self.pg_ns += t.elapsed().as_nanos() as u64;
        r
    }
}

// ── Stats ─────────────────────────────────────────────────────────────────────

pub struct MigrateStats {
    pub rows:   u64,
    pub bytes:  u64,   // bytes written to COPY stream
    pub fdb_ns: u64,   // time decoding ODS (total - pg)
    pub pg_ns:  u64,   // time writing to PostgreSQL
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn connect_pub(args: &crate::Args, dbname: &str) -> Result<Client> {
    connect(args, dbname)
}

fn connect(args: &crate::Args, dbname: &str) -> Result<Client> {
    connect_to(&args.pg_host, args.pg_port, &args.pg_user, &args.pg_password, dbname)
}

/// Connect with explicit params (used by the CDC `LocalPgSink`, decoupled from `Args`).
pub fn connect_to(host: &str, port: u16, user: &str, password: &str, dbname: &str) -> Result<Client> {
    let mut c = Client::configure()
        .host(host).port(port)
        .user(user).password(password)
        .dbname(dbname)
        .connect(NoTls)?;
    // Per-session WAL optimisations — safe for bulk load, no global impact.
    c.batch_execute("SET synchronous_commit = off; SET work_mem = '256MB';")?;
    Ok(c)
}

fn ensure_database(args: &crate::Args) -> Result<()> {
    let pg_db = args.pg_database.as_deref().unwrap();
    let mut admin = connect(args, "postgres")?;
    match admin.execute(&format!("CREATE DATABASE \"{}\"", pg_db), &[]) {
        Err(e) if e.code() == Some(&SqlState::DUPLICATE_DATABASE) => {}
        Err(e) => return Err(e.into()),
        Ok(_)  => eprintln!("Created database {pg_db}"),
    }
    Ok(())
}

fn fmt_speed(rows: u64, ns: u64, bytes: u64) -> String {
    if ns == 0 { return "—".into(); }
    let secs = ns as f64 / 1e9;
    let mb   = bytes as f64 / (1024.0 * 1024.0);
    format!("{:.0} rows/s  {:.1} MB/s", rows as f64 / secs, mb / secs)
}

// ── Core migration ────────────────────────────────────────────────────────────

pub fn migrate_table(
    db:       &OdsReader,
    client:   &mut Client,
    table:    &str,
    drop:     bool,
    unlogged: bool,
    quiet:    bool,
) -> Result<MigrateStats> {
    let pg_table = table.to_lowercase();

    if drop {
        client.execute(&format!("DROP TABLE IF EXISTS \"{}\"", pg_table), &[])?;
    }
    let create_sql = crate::schema::create_table_sql_from(db, table, unlogged)?;
    client.batch_execute(&create_sql)?;

    let relation_id       = db.find_relation_id(table)?;
    let (field_order, n_fields) = crate::extract::copy_slots(db, relation_id, table)?;
    let bar               = crate::make_spinner(quiet);

    // Scope ensures CopyInWriter (which borrows `client`) is dropped before ALTER TABLE.
    let (count, bytes, fdb_ns, pg_ns) = {
        let copy_sql    = format!("COPY \"{}\" FROM STDIN WITH (FORMAT BINARY)", pg_table);
        let copy_writer = client.copy_in(copy_sql.as_str())?;
        let timed       = TimingWrite { inner: copy_writer, pg_ns: 0, bytes: 0 };
        let mut out     = BufWriter::with_capacity(128 * 1024 * 1024, timed);

        let t_start = Instant::now();

        let count = crate::extract::write_copy_stream(
            db, relation_id, &field_order, n_fields, &mut out, |c| bar.set_position(c),
        )?;

        out.flush()?;

        let total_ns = t_start.elapsed().as_nanos() as u64;
        let timed    = out.into_inner().map_err(|e| anyhow::anyhow!("{}", e.into_error()))?;
        let bytes    = timed.bytes;
        let pg_partial = timed.pg_ns;

        let t_finish = Instant::now();
        timed.inner.finish()?;
        let pg_ns  = pg_partial + t_finish.elapsed().as_nanos() as u64;
        let fdb_ns = total_ns.saturating_sub(pg_partial);

        (count, bytes, fdb_ns, pg_ns)
    }; // CopyInWriter dropped here — client borrow released

    bar.finish_and_clear();
    Ok(MigrateStats { rows: count, bytes, fdb_ns, pg_ns })
}

// ── Public entry points ───────────────────────────────────────────────────────

pub fn migrate(args: &crate::Args) -> Result<MigrateStats> {
    ensure_database(args)?;
    let db     = OdsReader::open(args.database.as_deref().unwrap())?;
    let mut pg = connect(args, args.pg_database.as_deref().unwrap())?;
    migrate_table(&db, &mut pg, args.table.as_deref().unwrap(), args.drop, args.unlogged, args.no_progress)
}

fn run_table_list(args: &crate::Args, tables: &[String]) -> Result<()> {
    ensure_database(args)?;
    let db     = OdsReader::open(args.database.as_deref().unwrap())?;
    let mut pg = connect(args, args.pg_database.as_deref().unwrap())?;

    let run_start     = Instant::now();
    let mut tot_rows  = 0u64;
    let mut tot_bytes = 0u64;
    let mut tot_fdb   = 0u64;
    let mut tot_pg    = 0u64;
    let mut ok        = 0usize;
    let mut failed    = 0usize;

    for table in tables {
        eprint!("  {:<35} ", table);
        match migrate_table(&db, &mut pg, table, args.drop, args.unlogged, true) {
            Ok(s) => {
                let secs  = (s.fdb_ns + s.pg_ns) as f64 / 1e9;
                let mb    = s.bytes as f64 / (1024.0 * 1024.0);
                eprintln!(
                    "{:>8} rows  {:>6.1} MB  {:>5.2}s  fdb: {}  pg: {}",
                    s.rows, mb, secs,
                    fmt_speed(s.rows, s.fdb_ns, s.bytes),
                    fmt_speed(s.rows, s.pg_ns,  s.bytes),
                );
                tot_rows  += s.rows;
                tot_bytes += s.bytes;
                tot_fdb   += s.fdb_ns;
                tot_pg    += s.pg_ns;
                ok        += 1;
            }
            Err(e) => {
                eprintln!("FAILED — {e:#}");
                failed += 1;
                if let Ok(new_pg) = connect(args, args.pg_database.as_deref().unwrap()) {
                    pg = new_pg;
                }
            }
        }
    }

    let elapsed = run_start.elapsed().as_secs_f64();
    let tot_mb  = tot_bytes as f64 / (1024.0 * 1024.0);
    eprintln!("\n{}", "=".repeat(80));
    eprintln!(
        "Total  {:>8} rows  {:>6.1} MB  {:>5.2}s  {} OK  {} FAILED",
        tot_rows, tot_mb, elapsed, ok, failed
    );
    eprintln!(
        "       fdb: {}  pg: {}",
        fmt_speed(tot_rows, tot_fdb, tot_bytes),
        fmt_speed(tot_rows, tot_pg,  tot_bytes),
    );
    Ok(())
}

pub fn migrate_all(args: &crate::Args) -> Result<()> {
    let db     = OdsReader::open(args.database.as_deref().unwrap())?;
    let tables = db.list_tables();
    drop(db);
    if tables.is_empty() { eprintln!("No user tables found."); return Ok(()); }
    run_table_list(args, &tables)
}

pub fn migrate_list(args: &crate::Args, tables: &[String]) -> Result<()> {
    run_table_list(args, tables)
}
