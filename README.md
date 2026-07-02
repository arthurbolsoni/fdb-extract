# fdb-extract

Fast Firebird extractor — reads `.fdb` files directly via ODS page parsing, no Firebird server required.

## Build

```bash
cargo build --release
# binary: target/release/fdb-extract
```

## Usage

### List tables

```bash
fdb-extract --database mydb.fdb --list-tables
```

### Extract to null (benchmark / verify)

Reads all rows and discards output. Useful for measuring throughput.

```bash
# Windows
fdb-extract --database mydb.fdb --table CLIENTES --output - > NUL

# Linux/macOS
fdb-extract --database mydb.fdb --table CLIENTES --output - > /dev/null
```

### Benchmark / verify field conversion

Runs `write_field_binary` for every Firebird dtype and prints `ns/op` plus the
encoded PostgreSQL `COPY BINARY` bytes. No database needed — uses a synthetic
record. The hex output is the conversion result, so it doubles as a correctness
check (e.g. DOUBLE π → `40 09 21 FB 54 44 2D 18`, BOOLEAN → `01`).

```bash
fdb-extract --bench
```

```
dtype                     ns/op   encoded (first 8 bytes)
----------------------------------------------------------------------
DOUBLE                     3.64 ns/op   bytes=8  hex=40 09 21 FB 54 44 2D 18
BOOLEAN                    3.14 ns/op   bytes=1  hex=01
DBKEY                      4.04 ns/op   bytes=8  hex=DE AD BE EF 01 02 03 04
...
```

### Inspect a relation (debug)

Dumps every slot of every data page for a relation: transaction number, record
flags (`DEL/CHN/FRG/INC/LNG/NPK`), format version, back-pointers and raw length.
Reads no PostgreSQL.

```bash
fdb-extract --database mydb.fdb --table CLIENTES --inspect
```

### Extract to PostgreSQL

Single table:

```bash
fdb-extract \
  --database mydb.fdb \
  --table CLIENTES \
  --pg-database mydb \
  --pg-host localhost \
  --pg-user postgres \
  --pg-password secret \
  --drop
```

All tables at once:

```bash
fdb-extract \
  --database mydb.fdb \
  --all-tables \
  --pg-database mydb \
  --pg-host localhost \
  --pg-user postgres \
  --pg-password secret \
  --drop
```

Specific tables in order:

```bash
fdb-extract \
  --database mydb.fdb \
  --tables CLIENTES,PEDIDOS,ITENS \
  --pg-database mydb \
  --pg-host localhost \
  --pg-user postgres \
  --pg-password secret
```

Use `--unlogged` to create UNLOGGED tables (no WAL, faster initial load):

```bash
fdb-extract --database mydb.fdb --all-tables --pg-database mydb --drop --unlogged
```

### Watch (CDC)

Polls the `.fdb` file for changes and syncs deltas to PostgreSQL continuously.
Requires a prior full extraction to seed the target tables.

```bash
fdb-extract \
  --database mydb.fdb \
  --watch \
  --watch-interval 5 \
  --pg-database mydb \
  --pg-host localhost \
  --pg-user postgres \
  --pg-password secret
```

With per-cycle debug stats (page scan counts, timings, per-table last transaction):

```bash
fdb-extract \
  --database mydb.fdb \
  --watch \
  --watch-interval 5 \
  --pg-database mydb \
  --pg-host localhost \
  --pg-user postgres \
  --pg-password secret \
  --debug
```

State is persisted in `<database>.cdc.json` between restarts. Override with `--state-file`:

```bash
fdb-extract --database mydb.fdb --watch --pg-database mydb --state-file /var/lib/cdc/mydb.json
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--database` | — | Path to `.fdb` file |
| `--table` | — | Single table name |
| `--tables` | — | Comma-separated table list |
| `--all-tables` | false | Migrate all user tables |
| `--output` | `<table>.bin` | Output file; `-` for stdout |
| `--pg-database` | — | Target PG database name |
| `--pg-host` | localhost | PG host |
| `--pg-port` | 5432 | PG port |
| `--pg-user` | postgres | PG user |
| `--pg-password` | — | PG password |
| `--drop` | false | DROP TABLE IF EXISTS before load |
| `--unlogged` | false | Create UNLOGGED tables |
| `--watch` | false | CDC mode |
| `--watch-interval` | 5 | Poll interval in seconds |
| `--state-file` | `<db>.cdc.json` | CDC state file path |
| `--debug` | false | Per-cycle scan/timing stats |
| `--list-tables` | false | List user tables (views excluded) and exit |
| `--create-table` | false | Print CREATE TABLE SQL and exit |
| `--bench` | false | Benchmark/verify field conversion and exit |
| `--inspect` | false | Dump data-page slots for `--table` and exit |
| `--no-progress` | false | Suppress progress spinner |

## Requirements

- Firebird 4+ (ODS 13+)
- Database file must be readable (Firebird server can be running; file is opened read-only)

## Push integrador (mTLS): `fdb-agent` + `fdb-ingest`

For production, when Firebird is on the customer's server and PostgreSQL is on a
cloud VPS, and **no PG credential may live on the customer machine**, the work is
split across two extra binaries built from this same crate:

- **`fdb-agent`** (customer) — reads the `.fdb` and pushes tables to the gateway
  over mTLS. Holds only a client cert + key; never a PG credential. Does FULL
  snapshots (batch) and continuous CDC (`--watch`, reusing the same scan as the
  local watch path through the `DeltaSink` abstraction).
- **`fdb-ingest`** (VPS) — terminates and validates mTLS **in Rust** (client cert
  must chain to a private CA; the tenant is the cert CN), then loads into PostgreSQL.
  FULL loads into a staging table and atomically swaps it in; DELTA reuses the same
  `merge::apply_delta` as the local CDC path. Holds the PG creds (localhost only).
  Also bundles the cert tooling: `ca-init`, `issue-server`, `issue-client`.

No nginx — an L4 proxy/firewall, if present, only forwards the port. The agent
connects **outbound** only, so it works behind NAT with no public IP.

See [`deploy/DEPLOY.md`](deploy/DEPLOY.md) for the full runbook (PKI, systemd unit,
Windows Task Scheduler / service, PostgreSQL hardening, rotation, verification).

```bash
# VPS: stand up the CA + certs, then serve
fdb-ingest ca-init --cn "my CA"
fdb-ingest issue-server --san ingest.example.com
fdb-ingest issue-client --tenant acme
fdb-ingest serve --pg-user fdb_writer --tenant acme=replica_acme   # PGPASSWORD in env

# Customer: batch snapshot, then continuous CDC
fdb-agent --gateway ingest.example.com:8443 --server-name ingest.example.com \
  --ca ca.crt --cert acme.crt --key acme.key \
  --database BANCO.FDB --all-tables --watch
```
