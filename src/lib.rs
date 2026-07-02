use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Instant;

pub mod bench;
pub mod cdc;
pub mod certgen;
pub mod extract;
pub mod merge;
pub mod ods;
pub mod pg;
pub mod schema;
pub mod sink;
pub mod sqz;
pub mod tlsauth;
pub mod wire;

#[derive(Parser, Debug)]
#[command(name = "fdb-extract", about = "Fast Firebird extractor — direct ODS page reading, no server needed")]
pub struct Args {
    #[arg(short = 'd', long)]
    pub database: Option<String>,

    #[arg(short = 't', long)]
    pub table: Option<String>,

    #[arg(short = 'o', long)]
    pub output: Option<String>,

    #[arg(long)]
    pub no_progress: bool,

    #[arg(long)]
    pub list_tables: bool,

    #[arg(long)]
    pub create_table: bool,

    #[arg(long)]
    pub bench: bool,

    #[arg(long)]
    pub pg_database: Option<String>,

    #[arg(long)]
    pub all_tables: bool,

    #[arg(long, value_delimiter = ',', num_args = 1..)]
    pub tables: Vec<String>,

    #[arg(long, default_value = "localhost")]
    pub pg_host: String,

    #[arg(long, default_value = "5432")]
    pub pg_port: u16,

    #[arg(long, default_value = "postgres")]
    pub pg_user: String,

    #[arg(long, default_value = "")]
    pub pg_password: String,

    #[arg(long)]
    pub drop: bool,

    #[arg(long)]
    pub unlogged: bool,

    #[arg(long)]
    pub watch: bool,

    #[arg(long, default_value = "5")]
    pub watch_interval: u32,

    #[arg(long)]
    pub state_file: Option<String>,

    #[arg(long)]
    pub debug: bool,

    /// Dump every slot of every data page for a relation (txn, flags, len, b_page, b_line).
    /// Use with --table TABLE.  Skips system relations and reads no PostgreSQL.
    #[arg(long)]
    pub inspect: bool,
}

pub fn inspect_relation(args: &Args) -> Result<()> {
    let db_path = args.database.as_deref()
        .ok_or_else(|| anyhow::anyhow!("--database required"))?;
    let table = args.table.as_deref()
        .ok_or_else(|| anyhow::anyhow!("--table TABLE required for --inspect"))?;

    let db = ods::OdsReader::open(db_path)?;
    let rid = db.find_relation_id(table)?;
    let Some(pp) = db.find_first_pp(rid) else {
        eprintln!("No pointer page for {table} (relation_id={rid})");
        return Ok(());
    };
    let pages = db.data_pages_for(pp);

    eprintln!("relation={table} rid={rid} first_pp={pp} data_pages={}", pages.len());
    for &dp_n in &pages {
        let dp = db.page(dp_n);
        if dp[0] != ods::PAG_DATA { continue; }
        let gen = u32::from_le_bytes(dp[4..8].try_into().unwrap());
        let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
        eprintln!("─── page {dp_n} pag_gen={gen} slots={cnt} ───");
        for s in 0..cnt {
            let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
            let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
            if off == 0 || len == 0 {
                eprintln!("  slot {s:4}: empty");
                continue;
            }
            if off + len > db.page_size {
                eprintln!("  slot {s:4}: OOB off={off} len={len}");
                continue;
            }
            let raw = &dp[off..off+len];
            if raw.len() < 13 {
                eprintln!("  slot {s:4}: short raw_len={}", raw.len());
                continue;
            }
            let lo  = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as u64;
            let bp  = u32::from_le_bytes(raw[4..8].try_into().unwrap());
            let bl  = u16::from_le_bytes([raw[8], raw[9]]);
            let fl  = u16::from_le_bytes([raw[10], raw[11]]);
            let fmt = raw[12];
            let txn = if fl & 1024 != 0 && raw.len() >= 16 {
                let hi = u16::from_le_bytes([raw[14], raw[15]]) as u64;
                (hi << 32) | lo
            } else {
                lo
            };
            let tag = if fl & 1    != 0 { "DEL " } else { "    " };
            let chn = if fl & 2    != 0 { "CHN " } else { "    " };
            let frg = if fl & 4    != 0 { "FRG " } else { "    " };
            let inc = if fl & 8    != 0 { "INC " } else { "    " };
            let lng = if fl & 1024 != 0 { "LNG " } else { "    " };
            let npk = if fl & 2048 != 0 { "NPK " } else { "    " };
            eprintln!(
                "  slot {s:4}: txn={txn:<10} flags=0x{fl:04x} {tag}{chn}{frg}{inc}{lng}{npk} fmt={fmt:<3} \
                 b_page={bp:<8} b_line={bl:<4} raw_len={}",
                raw.len(),
            );
        }
    }
    Ok(())
}

pub fn make_spinner(no_progress: bool) -> ProgressBar {
    if no_progress {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {pos} rows  ({per_sec})")
            .unwrap(),
    );
    pb
}

pub fn run() -> Result<()> {
    let args = Args::parse();

    if args.bench {
        bench::run();
        return Ok(());
    }

    if args.watch {
        return cdc::watch(&args);
    }

    if args.inspect {
        return inspect_relation(&args);
    }

    if let Some(ref tbl) = args.table {
        if tbl.starts_with("__debug_constraints:") {
            let name = &tbl["__debug_constraints:".len()..];
            let db = ods::OdsReader::open(args.database.as_deref().unwrap())?;
            let nl = if db.ods_ver >= 13 { 252usize } else { 63 };
            let ctypes = db.list_constraints(name);
            eprintln!("ODS version: {}  name_len: {}", db.ods_ver, nl);
            eprintln!("Constraints for '{}':", name);
            for c in &ctypes { eprintln!("  '{}'", c); }
            if ctypes.is_empty() { eprintln!("  (none found — checking all records in rel 22)"); }
            return Ok(());
        }
    }

    if args.list_tables {
        let db = ods::OdsReader::open(args.database.as_deref()
            .ok_or_else(|| anyhow::anyhow!("--database required"))?)?;
        for t in db.list_tables() { println!("{t}"); }
        return Ok(());
    }

    if args.create_table {
        print!("{}", schema::create_table_sql(&args)?);
        return Ok(());
    }

    let start = Instant::now();

    if args.pg_database.is_some() {
        if args.all_tables {
            pg::migrate_all(&args)?;
        } else if !args.tables.is_empty() {
            pg::migrate_list(&args, &args.tables.clone())?;
        } else {
            args.table.as_deref()
                .ok_or_else(|| anyhow::anyhow!("Provide --table TABLE, --tables T1 T2 ..., or --all-tables"))?;
            let s       = pg::migrate(&args)?;
            let elapsed = start.elapsed().as_secs_f64();
            let mb      = s.bytes as f64 / (1024.0 * 1024.0);
            eprintln!("\n{} rows  {mb:.1} MB  {elapsed:.2}s → {}",
                s.rows, args.pg_database.as_deref().unwrap());
        }
        return Ok(());
    }

    let table = args.table.as_deref()
        .ok_or_else(|| anyhow::anyhow!("Provide --table TABLE or --list-tables"))?;

    let output = args.output.clone()
        .unwrap_or_else(|| format!("{}.bin", table.to_lowercase()));
    let stdout_mode = output == "-";

    let rows    = extract::extract_ods_binary(&args)?;
    let elapsed = start.elapsed().as_secs_f64();

    if !stdout_mode {
        eprintln!("\n{rows} rows → {output} in {elapsed:.2}s  ({:.0} rows/sec)",
            rows as f64 / elapsed.max(0.001));
    } else {
        eprintln!("\n{rows} rows in {elapsed:.2}s  ({:.0} rows/sec)",
            rows as f64 / elapsed.max(0.001));
    }

    Ok(())
}
