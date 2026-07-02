# fdb-extract

**Read Firebird `.fdb` files directly — no Firebird server required — and replicate them into PostgreSQL.**

`fdb-extract` memory-maps the database file and parses Firebird's on-disk structure (ODS)
itself: pointer pages, data pages, RLE-compressed records, fragmented rows, MVCC record
versions. Rows are decoded straight into PostgreSQL `COPY BINARY` format — no Firebird
server, no client library, no ODBC, no SQL layer in between.

On top of that base it implements:

- **Trigger-less, log-less CDC** — continuously detects inserts, updates *and deletes* in a
  live `.fdb` by watching page generations and transaction numbers, and merges them into
  PostgreSQL. No triggers, no replication log, no changes to the source database at all.
- **Push replication over mTLS** — an agent/gateway pair for the common real-world setup
  where Firebird sits on a customer's on-premises machine (behind NAT, no public IP) and
  PostgreSQL lives on a cloud server. The agent makes outbound-only connections and never
  holds a PostgreSQL credential.

The `.fdb` file is always opened **read-only**, and it is safe to read while a live Firebird
server owns the file — the integration test suite does exactly that against a running
Firebird 5 server.

## Why

A lot of legacy ERP / POS / desktop systems keep their data in Firebird. Getting that data
into PostgreSQL for analytics or migration usually means a Firebird server install, client
libraries, and an ETL pipeline polling with `SELECT`s — slow, invasive, and painful to run
on hundreds of customer machines.

Parsing the file format directly removes the whole stack: copy one static binary next to
the `.fdb` and stream the data out at disk speed.

## What's in the box

Three binaries from one crate:

| Binary | Runs on | Purpose |
|---|---|---|
| `fdb-extract` | anywhere | CLI: list tables, extract to `COPY BINARY` files, load/migrate into PostgreSQL, local CDC watch, inspect pages, benchmark |
| `fdb-agent` | customer machine (Windows) | Reads the `.fdb` and pushes FULL snapshots + CDC deltas to the gateway over mTLS. Holds only a client certificate |
| `fdb-ingest` | cloud VPS (Linux) | mTLS gateway: validates client certs against a private CA, maps cert CN → tenant database, loads into PostgreSQL. Also bundles the PKI tooling (`ca-init`, `issue-server`, `issue-client`) — no openssl needed |

## How it works

**Extraction.** The file is memory-mapped and metadata is discovered by walking the system
catalog pages themselves (`RDB$PAGES`, `RDB$RELATIONS`, `RDB$FORMATS`, …) — tables, columns,
types and primary keys, all without a server. Records are decompressed with a port of
Firebird's SQZ RLE codec, fragmented records are reassembled across pages, and MVCC
back-versions are skipped so each row comes out exactly once. Every record is decoded with
the format version it was written under, so tables that lived through `ALTER TABLE` decode
correctly. Data pages are read in sorted physical order and rows are encoded directly into
`COPY BINARY` with a zero-allocation hot path.

**CDC.** A change cycle is triggered by the header page's generation counter; within a
cycle only data pages whose page generation changed are scanned, and only records whose
transaction number exceeds the persisted watermark count as changes. Deletes are recovered
from MVCC delete stubs (chasing the back-version chain to reconstruct the primary key) and,
for rows garbage-collected without a stub, by diffing compact per-page primary-key
snapshots. Deltas are applied to PostgreSQL in a single transaction: `COPY` into temp
staging tables, then `DELETE … USING` + `INSERT … ON CONFLICT DO UPDATE`. Watermark and
snapshots persist across restarts via crash-safe (write-tmp-then-rename) state files.

**Push replication.** The agent seeds each table with a FULL snapshot — streamed into a
staging table on the gateway and atomically swapped in, so readers never see an empty
table — then streams CDC deltas. TLS is terminated and client certificates are validated
inside the Rust binary (rustls): the client cert must chain to your private CA, and the
cert's CN selects the tenant database. The gateway never buffers a delta in memory; bodies
stream from the socket straight into PostgreSQL `COPY`.

## Supported input

- Firebird **3.0+** (ODS 12, 13 and 14 layouts — including the identifier-length and
  header changes in ODS 13/14). Firebird 2.5 and InterBase files are rejected.
- Single-file databases (no secondary files, no encrypted pages).
- Tested end-to-end against Firebird 5 and PostgreSQL.

### Type mapping

| Firebird | PostgreSQL |
|---|---|
| `CHAR` / `VARCHAR` | `VARCHAR(n)` (Windows-1252 → UTF-8) |
| `SMALLINT` / `INTEGER` / `BIGINT` (scale 0) | `SMALLINT` / `INTEGER` / `BIGINT` |
| `NUMERIC` / `DECIMAL` (scaled storage) | `DOUBLE PRECISION` |
| `FLOAT` / `DOUBLE PRECISION` | `REAL` / `DOUBLE PRECISION` |
| `DATE` / `TIME` / `TIMESTAMP` | `DATE` / `TIME` / `TIMESTAMP` |
| `TIME/TIMESTAMP WITH TIME ZONE` | `TIME` / `TIMESTAMP` (zone dropped) |
| `BOOLEAN` | `BOOLEAN` |
| `RDB$DB_KEY` | `BYTEA` |

### Known limitations (read before relying on it)

- **`BLOB` and `ARRAY` columns are not extracted** — they come out as `NULL` (the column is
  created as `TEXT`).
- `DECFLOAT(16/34)` and `INT128` columns come out as `NULL`.
- Scaled `NUMERIC`/`DECIMAL` values are converted through `double` — exact decimal precision
  beyond ~15 significant digits is not preserved.
- Text is assumed to be Windows-1252 and transcoded to UTF-8; the column's declared charset
  is not consulted. Embedded `NUL` bytes are dropped (PostgreSQL text can't hold them).
- CDC watch **skips tables without a primary key** (a warning is printed).
- Express-delete detection (rows garbage-collected without a delete stub) is disabled for
  tables whose PK contains a variable-length column; stub-based deletes still work.

## Quick start

```bash
cargo build --release
# binaries: target/release/fdb-extract (+ fdb-agent, fdb-ingest)
```

### List tables

```bash
fdb-extract --database mydb.fdb --list-tables
```

### Extract a table to a PGCOPY binary file

```bash
fdb-extract --database mydb.fdb --table CLIENTES            # -> clientes.bin
fdb-extract --database mydb.fdb --table CLIENTES --output -  # stream to stdout
```

The output is the exact payload for `COPY "clientes" FROM STDIN WITH (FORMAT BINARY)`.

### Migrate to PostgreSQL

```bash
# single table
fdb-extract --database mydb.fdb --table CLIENTES \
  --pg-database mydb --pg-host localhost --pg-user postgres --pg-password secret --drop

# everything
fdb-extract --database mydb.fdb --all-tables \
  --pg-database mydb --pg-user postgres --pg-password secret --drop

# specific tables, in order
fdb-extract --database mydb.fdb --tables CLIENTES,PEDIDOS,ITENS --pg-database mydb
```

The target database and tables are created automatically (DDL derived from the Firebird
metadata, including the primary key). Use `--unlogged` for UNLOGGED tables (no WAL —
faster initial load, not crash-safe).

### Watch (CDC)

Polls the `.fdb` and continuously merges inserts/updates/deletes into PostgreSQL.
Seed the target with a full migration first.

```bash
fdb-extract --database mydb.fdb --watch --watch-interval 5 \
  --pg-database mydb --pg-user postgres --pg-password secret
```

State persists in `<database>.cdc.json` (override with `--state-file`) — restarts resume
from the watermark instead of re-scanning. Add `--debug` for per-cycle page-scan and
timing stats.

### Debug helpers

```bash
fdb-extract --database mydb.fdb --table CLIENTES --inspect   # dump page slots: txn, flags, back-pointers
fdb-extract --database mydb.fdb --table CLIENTES --create-table  # print the generated CREATE TABLE
fdb-extract --bench                                          # ns/op per field encoder + encoded bytes (no db needed)
```

## Options

| Flag | Default | Description |
|---|---|---|
| `-d`, `--database` | — | Path to `.fdb` file |
| `-t`, `--table` | — | Single table name |
| `--tables` | — | Table list (comma- or space-separated) |
| `--all-tables` | false | All user tables |
| `-o`, `--output` | `<table>.bin` (lowercased) | Output file; `-` for stdout |
| `--pg-database` | — | Target PG database (enables the PG path) |
| `--pg-host` / `--pg-port` | localhost / 5432 | PG endpoint |
| `--pg-user` / `--pg-password` | postgres / empty | PG credentials |
| `--drop` | false | `DROP TABLE IF EXISTS` before load |
| `--unlogged` | false | Create UNLOGGED tables |
| `--watch` | false | CDC mode |
| `--watch-interval` | 5 | Poll interval, seconds (min 1) |
| `--state-file` | `<db>.cdc.json` | CDC state file path |
| `--debug` | false | Per-cycle scan/timing stats |
| `--list-tables` | false | List user tables (views excluded) and exit |
| `--create-table` | false | Print CREATE TABLE SQL and exit |
| `--inspect` | false | Dump data-page slots for `--table` and exit |
| `--bench` | false | Benchmark field conversion and exit |
| `--no-progress` | false | Suppress progress spinner |

## Push replication (mTLS): `fdb-agent` + `fdb-ingest`

For production, when Firebird is on the customer's machine and PostgreSQL is on a cloud
VPS, and **no PostgreSQL credential may live on the customer machine**:

```
[customer — Firebird, NAT, no public IP]        [VPS — public IP]
 .fdb ──mmap──▶ fdb-agent ──outbound mTLS:8443──▶ fdb-ingest ──▶ postgres
                (client cert only)                (PG creds)      127.0.0.1
```

```bash
# VPS: stand up the CA + certs, then serve
fdb-ingest ca-init --cn "my CA"
fdb-ingest issue-server --san ingest.example.com
fdb-ingest issue-client --tenant acme
fdb-ingest serve --pg-user fdb_writer --tenant acme=replica_acme   # PGPASSWORD in env

# Customer: FULL seed + continuous CDC
fdb-agent --gateway ingest.example.com:8443 --server-name ingest.example.com \
  --ca ca.crt --cert acme.crt --key acme.key \
  --database BANCO.FDB --all-tables --watch
```

Security model:

- The agent's **only secret is its client certificate**. If it leaks, the blast radius is
  that one tenant's replica — never PostgreSQL access.
- Certificate validation happens **in the Rust binary** (chain to your private CA,
  CN = tenant). Certs from another CA fail the handshake; unknown CNs are rejected.
  Any fronting proxy must be pure L4 passthrough — no TLS-terminating reverse proxy.
- FULL reloads land in a staging table and are swapped in atomically — the target table
  is never observed empty.
- Tenants are authenticated, not sandboxed: the gateway runs each tenant's generated DDL
  under its own PG user. Issue certificates only to machines you operate.

Full runbook (PKI, systemd unit, Windows service/Task Scheduler, PostgreSQL hardening,
rotation/revocation): [`deploy/DEPLOY.md`](deploy/DEPLOY.md). Tenant onboarding walkthrough
(Portuguese): [`deploy/ONBOARDING.md`](deploy/ONBOARDING.md).

## Building

Native build is a plain `cargo build --release`. Windows binaries are statically linked
(`+crt-static`, no VC++ redist). The Linux gateway binary cross-compiles **from Windows**
as a fully static musl ELF with `cargo-zigbuild` — see [`BUILDER.md`](BUILDER.md).

## Testing

- Unit tests run against synthetic, hand-built ODS pages (fragment chains, delete stubs,
  format changes, express deletes) — no Firebird or PostgreSQL needed: `cargo test`
- The integration suite creates real databases on a live Firebird 5 server, mutates them
  over SQL, and asserts the CDC pipeline's row-level results in a real PostgreSQL:
  `cargo test --release --test cdc_integration` (see [`.env.example`](.env.example))

## Status

Built for and battle-tested on real-world ERP replication workloads (large multi-GB
databases, tables with decades of `ALTER TABLE` history, fragmented records). It is not
affiliated with the Firebird project. ODS parsing is derived from the Firebird source; if
you hit a page layout it doesn't understand, `--inspect` output makes a great bug report.

## License

[MIT](LICENSE)
