//! fdb-ingest — the VPS-side gateway of the push integrador.
//!
//! Holds the PostgreSQL credentials (nowhere else). Terminates and validates mTLS in
//! Rust (no nginx): every connection must present a client cert chained to our private
//! CA; the tenant is the cert CN, mapped to a target database. Two operations:
//!
//!   FULL  — receive a streamed PGCOPY BINARY dump, load into a staging table, then
//!           atomically swap it in (no empty-table window).
//!   DELTA — receive changed rows + deleted PKs and apply via `merge::apply_delta`.
//!
//! Also bundles the certificate tooling (`ca-init`, `issue-server`, `issue-client`)
//! so an operator can stand up the CA and issue tenant certs without extra tools.

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use fdb_extract::{certgen, merge, tlsauth, wire};
use postgres::{Client, NoTls};
use rustls::{ServerConnection, StreamOwned};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "fdb-ingest", about = "Push integrador gateway (mTLS) + cert tooling")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a fresh private CA (ca.crt + ca.key).
    CaInit(CaInitArgs),
    /// Issue the ingest server certificate (SANs = host/IP the agent dials).
    IssueServer(IssueServerArgs),
    /// Issue a per-tenant client certificate (CN = tenant).
    IssueClient(IssueClientArgs),
    /// Run the ingest server.
    Serve(ServeArgs),
}

#[derive(Parser)]
struct CaInitArgs {
    #[arg(long, default_value = "fdb-integrador CA")]
    cn: String,
    #[arg(long, default_value = "pki")]
    out_dir: PathBuf,
}

#[derive(Parser)]
struct IssueServerArgs {
    #[arg(long, default_value = "pki")]
    pki: PathBuf,
    /// Hostname or IP the agent connects to. Repeatable.
    #[arg(long = "san", required = true)]
    sans: Vec<String>,
    #[arg(long, default_value = "pki")]
    out_dir: PathBuf,
}

#[derive(Parser)]
struct IssueClientArgs {
    #[arg(long, default_value = "pki")]
    pki: PathBuf,
    #[arg(long)]
    tenant: String,
    #[arg(long, default_value = "certs")]
    out_dir: PathBuf,
}

#[derive(Parser, Clone)]
struct ServeArgs {
    #[arg(long, default_value = "0.0.0.0:8443")]
    bind: String,
    #[arg(long, default_value = "pki/ca.crt")]
    ca: PathBuf,
    #[arg(long, default_value = "pki/server.crt")]
    cert: PathBuf,
    #[arg(long, default_value = "pki/server.key")]
    key: PathBuf,

    #[arg(long, default_value = "127.0.0.1")]
    pg_host: String,
    #[arg(long, default_value = "5432")]
    pg_port: u16,
    #[arg(long, default_value = "postgres")]
    pg_user: String,
    /// PG password; falls back to $PGPASSWORD.
    #[arg(long, default_value = "")]
    pg_password: String,

    /// tenant=database mapping. Repeatable, e.g. --tenant acme=replica_acme
    #[arg(long = "tenant", required = true)]
    tenants: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::CaInit(a)       => ca_init(a),
        Cmd::IssueServer(a)  => issue_server(a),
        Cmd::IssueClient(a)  => issue_client(a),
        Cmd::Serve(a)        => serve(a),
    }
}

// ── Cert tooling ───────────────────────────────────────────────────────────────

fn write_pem(dir: &Path, stem: &str, pem: &certgen::Pem) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let crt = dir.join(format!("{stem}.crt"));
    let key = dir.join(format!("{stem}.key"));
    std::fs::write(&crt, &pem.cert_pem).with_context(|| format!("write {}", crt.display()))?;
    std::fs::write(&key, &pem.key_pem).with_context(|| format!("write {}", key.display()))?;
    eprintln!("  wrote {}", crt.display());
    eprintln!("  wrote {}", key.display());
    Ok(())
}

fn ca_init(a: CaInitArgs) -> Result<()> {
    let ca = certgen::generate_ca(&a.cn)?;
    write_pem(&a.out_dir, "ca", &ca)?;
    eprintln!("CA created (CN={}).", a.cn);
    Ok(())
}

fn issue_server(a: IssueServerArgs) -> Result<()> {
    let ca_crt = std::fs::read_to_string(a.pki.join("ca.crt")).context("read ca.crt")?;
    let ca_key = std::fs::read_to_string(a.pki.join("ca.key")).context("read ca.key")?;
    let pem = certgen::issue_server(&ca_crt, &ca_key, &a.sans)?;
    write_pem(&a.out_dir, "server", &pem)?;
    eprintln!("Server cert issued (SANs: {}).", a.sans.join(", "));
    Ok(())
}

fn issue_client(a: IssueClientArgs) -> Result<()> {
    let ca_crt = std::fs::read_to_string(a.pki.join("ca.crt")).context("read ca.crt")?;
    let ca_key = std::fs::read_to_string(a.pki.join("ca.key")).context("read ca.key")?;
    let pem = certgen::issue_client(&ca_crt, &ca_key, &a.tenant)?;
    write_pem(&a.out_dir, &a.tenant, &pem)?;
    eprintln!("Client cert issued for tenant '{}'.", a.tenant);
    Ok(())
}

// ── Server ─────────────────────────────────────────────────────────────────────

struct ServeCtx {
    tls:     Arc<rustls::ServerConfig>,
    pg_host: String,
    pg_port: u16,
    pg_user: String,
    pg_pass: String,
    tenants: HashMap<String, String>, // tenant CN → database
}

fn serve(a: ServeArgs) -> Result<()> {
    tlsauth::install_crypto_provider();

    let ca   = std::fs::read_to_string(&a.ca).with_context(|| format!("read {}", a.ca.display()))?;
    let cert = std::fs::read_to_string(&a.cert).with_context(|| format!("read {}", a.cert.display()))?;
    let key  = std::fs::read_to_string(&a.key).with_context(|| format!("read {}", a.key.display()))?;
    let tls  = tlsauth::server_config(&ca, &cert, &key)?;

    let mut tenants = HashMap::new();
    for t in &a.tenants {
        let (cn, db) = t.split_once('=')
            .ok_or_else(|| anyhow!("bad --tenant '{t}', expected tenant=database"))?;
        tenants.insert(cn.to_string(), db.to_string());
    }

    let pg_pass = if a.pg_password.is_empty() {
        std::env::var("PGPASSWORD").unwrap_or_default()
    } else {
        a.pg_password.clone()
    };

    let ctx = Arc::new(ServeCtx {
        tls,
        pg_host: a.pg_host.clone(),
        pg_port: a.pg_port,
        pg_user: a.pg_user.clone(),
        pg_pass,
        tenants,
    });

    let listener = TcpListener::bind(&a.bind).with_context(|| format!("bind {}", a.bind))?;
    eprintln!("fdb-ingest listening on {} — {} tenant(s)", a.bind, ctx.tenants.len());

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let ctx = Arc::clone(&ctx);
                std::thread::spawn(move || {
                    let peer = s.peer_addr().map(|p| p.to_string()).unwrap_or_default();
                    if let Err(e) = handle_conn(&ctx, s) {
                        eprintln!("[{peer}] connection error: {e:#}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e:#}"),
        }
    }
    Ok(())
}

fn handle_conn(ctx: &ServeCtx, tcp: TcpStream) -> Result<()> {
    let conn = ServerConnection::new(Arc::clone(&ctx.tls)).context("new TLS session")?;
    let mut tls = StreamOwned::new(conn, tcp);

    // Reading the header drives the TLS handshake (and thus client-cert verification);
    // if the cert doesn't chain to our CA the handshake fails here.
    let (op, header) = wire::read_request_header(&mut tls).context("read request header")?;

    let tenant = {
        let certs = tls.conn.peer_certificates()
            .ok_or_else(|| anyhow!("client presented no certificate"))?;
        let cn = tlsauth::tenant_from_cert(certs.first()
            .ok_or_else(|| anyhow!("empty client cert chain"))?)?;
        cn
    };

    let dbname = match ctx.tenants.get(&tenant) {
        Some(db) => db.clone(),
        None => {
            let _ = wire::write_response(&mut tls, &wire::Resp::err(format!("unknown tenant '{tenant}'")));
            return Err(anyhow!("unknown tenant '{tenant}'"));
        }
    };

    let result = dispatch(ctx, &mut tls, op, &header, &dbname);

    let resp = match &result {
        Ok(r)  => r.clone(),
        Err(e) => wire::Resp::err(format!("{e:#}")),
    };
    wire::write_response(&mut tls, &resp).context("write response")?;
    result.map(|_| ())
}

fn dispatch(
    ctx:    &ServeCtx,
    tls:    &mut StreamOwned<ServerConnection, TcpStream>,
    op:     wire::Op,
    header: &[u8],
    dbname: &str,
) -> Result<wire::Resp> {
    match op {
        wire::Op::Full  => handle_full(ctx, tls, header, dbname),
        wire::Op::Delta => handle_delta(ctx, tls, header, dbname),
    }
}

fn handle_full(
    ctx:    &ServeCtx,
    tls:    &mut StreamOwned<ServerConnection, TcpStream>,
    header: &[u8],
    dbname: &str,
) -> Result<wire::Resp> {
    let h: wire::FullHeader = serde_json::from_slice(header).context("parse FullHeader")?;
    let pg_table = h.table.to_lowercase();
    let staging  = format!("{pg_table}__load");

    let mut client = connect(ctx, dbname)?;

    // Build staging DDL by retargeting the agent's CREATE TABLE to the staging name.
    let staging_ddl = retarget_create(&h.create_sql, &pg_table, &staging)
        .ok_or_else(|| anyhow!("could not derive staging DDL for {pg_table}"))?;
    client.batch_execute(&format!("DROP TABLE IF EXISTS \"{staging}\""))?;
    client.batch_execute(&staging_ddl)?;

    // Stream the chunked PGCOPY body straight into COPY.
    let rows = {
        let writer = client.copy_in(
            &format!("COPY \"{staging}\" FROM STDIN WITH (FORMAT BINARY)"))?;
        let mut writer = writer;
        let mut body = wire::ChunkReader::new(&mut *tls);
        std::io::copy(&mut body, &mut writer).context("relay COPY body")?;
        if !body.is_done() { return Err(anyhow!("body ended before terminator")); }
        writer.finish()?
    };

    // Atomic swap — readers never observe an empty table.
    client.batch_execute(&format!(
        "BEGIN; \
         DROP TABLE IF EXISTS \"{pg_table}\"; \
         ALTER TABLE \"{staging}\" RENAME TO \"{pg_table}\"; \
         COMMIT;"
    ))?;

    eprintln!("  [{dbname}] FULL {pg_table}: {rows} rows");
    Ok(wire::Resp { rows, ..Default::default() })
}

fn handle_delta(
    ctx:    &ServeCtx,
    tls:    &mut StreamOwned<ServerConnection, TcpStream>,
    header: &[u8],
    dbname: &str,
) -> Result<wire::Resp> {
    let h: wire::DeltaHeader = serde_json::from_slice(header).context("parse DeltaHeader")?;

    let mut client = connect(ctx, dbname)?;
    let meta = merge::MergeMeta {
        pg_table:     h.table.to_lowercase(),
        cols_csv:     h.cols_csv,
        pk_csv:       h.pk_csv,
        pk_col_names: h.pk_col_names,
    };
    // Stream both delta bodies straight off the socket into COPY — never buffer the
    // whole delta (a full-table delta would otherwise OOM-kill the gateway).
    merge::apply_delta_streaming(&mut client, &meta, tls, h.n_upserts, h.n_deletes)?;

    eprintln!("  [{dbname}] DELTA {}: +{} ~{}", meta.pg_table, h.n_upserts, h.n_deletes);
    Ok(wire::Resp { upserts: h.n_upserts, deletes: h.n_deletes, ..Default::default() })
}

// ── PG helpers ──────────────────────────────────────────────────────────────────

fn connect(ctx: &ServeCtx, dbname: &str) -> Result<Client> {
    // Ensure the target database exists (idempotent).
    {
        let mut admin = Client::configure()
            .host(&ctx.pg_host).port(ctx.pg_port)
            .user(&ctx.pg_user).password(&ctx.pg_pass)
            .dbname("postgres").connect(NoTls)
            .context("connect postgres db")?;
        use postgres::error::SqlState;
        match admin.execute(&format!("CREATE DATABASE \"{dbname}\""), &[]) {
            Err(e) if e.code() == Some(&SqlState::DUPLICATE_DATABASE) => {}
            Err(e) => return Err(e).context("create database"),
            Ok(_)  => eprintln!("  created database {dbname}"),
        }
    }
    let mut c = Client::configure()
        .host(&ctx.pg_host).port(ctx.pg_port)
        .user(&ctx.pg_user).password(&ctx.pg_pass)
        .dbname(dbname).connect(NoTls)
        .with_context(|| format!("connect {dbname}"))?;
    c.batch_execute("SET synchronous_commit = off; SET work_mem = '256MB';")?;
    Ok(c)
}

/// Retarget a `CREATE [UNLOGGED ]TABLE "from" (...)` statement to `to`.
fn retarget_create(create_sql: &str, from: &str, to: &str) -> Option<String> {
    let needle = format!("TABLE \"{from}\"");
    let repl   = format!("TABLE \"{to}\"");
    create_sql.find(&needle).map(|_| create_sql.replacen(&needle, &repl, 1))
}
