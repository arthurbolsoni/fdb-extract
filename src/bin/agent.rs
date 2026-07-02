//! fdb-agent — the client-side pusher of the push integrador.
//!
//! Runs on the customer's server, next to the `.fdb` file. Reads tables directly via
//! ODS pages and pushes them to `fdb-ingest` over mTLS. Holds only its client cert +
//! key — never a PostgreSQL credential.
//!
//! This binary currently does the FULL (batch snapshot) path: for each table it sends
//! the CREATE TABLE DDL plus a streamed PGCOPY BINARY dump. Continuous CDC/DELTA push
//! is added on top of the shared `Sink` abstraction.

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use fdb_extract::ods::OdsReader;
use fdb_extract::{extract, schema, tlsauth, wire};
use rustls::pki_types::ServerName;
use rustls::{ClientConnection, StreamOwned};
use std::io::BufWriter;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "fdb-agent", about = "Push Firebird tables to fdb-ingest over mTLS")]
struct Args {
    /// Gateway address, host:port.
    #[arg(long)]
    gateway: String,
    /// TLS server name to validate (must match a SAN on the ingest server cert).
    #[arg(long)]
    server_name: String,

    #[arg(long)]
    ca: String,
    #[arg(long)]
    cert: String,
    #[arg(long)]
    key: String,

    /// Path to the .fdb file.
    #[arg(short = 'd', long)]
    database: String,

    #[arg(long)]
    all_tables: bool,
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    tables: Vec<String>,
    #[arg(short = 't', long)]
    table: Option<String>,

    /// Create UNLOGGED tables on the target (faster initial load).
    #[arg(long)]
    unlogged: bool,

    /// After the initial FULL seed, stream continuous CDC deltas to the gateway.
    #[arg(long)]
    watch: bool,
    /// CDC poll interval in seconds.
    #[arg(long, default_value = "5")]
    watch_interval: u32,
    /// CDC state file (default: <database>.cdc.json).
    #[arg(long)]
    state_file: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    tlsauth::install_crypto_provider();

    let ca   = std::fs::read_to_string(&args.ca).context("read ca")?;
    let cert = std::fs::read_to_string(&args.cert).context("read client cert")?;
    let key  = std::fs::read_to_string(&args.key).context("read client key")?;
    let tls_cfg = tlsauth::client_config(&ca, &cert, &key)?;

    let db = OdsReader::open(&args.database)
        .with_context(|| format!("open {}", args.database))?;

    let tables: Vec<String> = if args.all_tables {
        db.list_tables()
    } else if !args.tables.is_empty() {
        args.tables.clone()
    } else if let Some(t) = &args.table {
        vec![t.clone()]
    } else {
        return Err(anyhow!("provide --table, --tables, or --all-tables"));
    };

    let run = Instant::now();
    let (mut ok, mut failed) = (0u32, 0u32);
    let (mut rows_read, mut rows_sent, mut bytes_sent) = (0u64, 0u64, 0u64);
    for table in &tables {
        match push_full(&args, &tls_cfg, &db, table) {
            Ok(s) => {
                eprintln!(
                    "  {:<35} {:>9} read  {:>9} sent  {:>8.2} MB",
                    table, s.rows_read, s.rows_sent, s.bytes as f64 / 1_048_576.0
                );
                ok += 1;
                rows_read  += s.rows_read;
                rows_sent  += s.rows_sent;
                bytes_sent += s.bytes;
            }
            Err(e) => {
                eprintln!("  {:<35} FAILED — {e:#}", table);
                failed += 1;
            }
        }
    }
    eprintln!(
        "\n{} table(s)  {} rows read  {} rows sent  {:.2} MB  {:.2}s  {} OK  {} FAILED",
        tables.len(), rows_read, rows_sent,
        bytes_sent as f64 / 1_048_576.0, run.elapsed().as_secs_f64(), ok, failed
    );
    if failed > 0 { std::process::exit(1); }

    if args.watch {
        // FULL above seeds the target; now stream deltas continuously via RemoteSink.
        let state_path = args.state_file.clone()
            .unwrap_or_else(|| format!("{}.cdc.json", args.database));
        let interval = std::time::Duration::from_secs(args.watch_interval.max(1) as u64);
        let mut sink = fdb_extract::sink::RemoteSink::new(
            Arc::clone(&tls_cfg), &args.gateway, &args.server_name);
        fdb_extract::cdc::run_watch(&args.database, &tables, &state_path, interval, false, &mut sink)?;
    }
    Ok(())
}

/// Open one mTLS connection to the gateway.
fn connect(args: &Args, cfg: &Arc<rustls::ClientConfig>)
    -> Result<StreamOwned<ClientConnection, TcpStream>>
{
    let server = ServerName::try_from(args.server_name.clone())
        .with_context(|| format!("invalid server name {}", args.server_name))?;
    let conn = ClientConnection::new(Arc::clone(cfg), server).context("new TLS session")?;
    let tcp = TcpStream::connect(&args.gateway)
        .with_context(|| format!("connect {}", args.gateway))?;
    Ok(StreamOwned::new(conn, tcp))
}

/// Per-table push stats: rows decoded from the .fdb, rows the gateway accepted,
/// and on-wire body bytes (frame headers + PGCOPY payload).
struct PushStats { rows_read: u64, rows_sent: u64, bytes: u64 }

/// Push one table as a FULL snapshot.
fn push_full(
    args: &Args,
    cfg:  &Arc<rustls::ClientConfig>,
    db:   &OdsReader,
    table: &str,
) -> Result<PushStats> {
    let create_sql = schema::create_table_sql_from(db, table, args.unlogged)?;
    let relation_id = db.find_relation_id(table)?;
    let (field_order, n_fields) = extract::copy_slots(db, relation_id, table)?;

    let mut tls = connect(args, cfg)?;

    let header = serde_json::to_vec(&wire::FullHeader {
        table:      table.to_string(),
        create_sql,
    })?;
    wire::write_request_header(&mut tls, wire::Op::Full, &header)?;

    // Stream the dump: write_copy_stream → 8 MB BufWriter → ChunkWriter → TLS.
    let rows_read;
    let bytes;
    {
        let chunker = wire::ChunkWriter::new(&mut tls);
        let mut out = BufWriter::with_capacity(8 * 1024 * 1024, chunker);
        rows_read = extract::write_copy_stream(db, relation_id, &field_order, n_fields, &mut out, |_| {})?;
        let chunker = out.into_inner().map_err(|e| anyhow!("{}", e.into_error()))?;
        bytes = chunker.bytes_written();
        chunker.finish()?; // emits terminator, releases the &mut tls borrow
    }

    let resp = wire::read_response(&mut tls).context("read response")?;
    if let Some(err) = resp.error {
        return Err(anyhow!("gateway rejected: {err}"));
    }
    Ok(PushStats { rows_read, rows_sent: resp.rows, bytes })
}
