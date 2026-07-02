# Push integrador — deployment runbook

Replicate a customer's on-prem Firebird into a cloud PostgreSQL **without any
PostgreSQL credential on the customer machine**. The customer runs `fdb-agent`
(holds only a client cert); the VPS runs `fdb-ingest` (holds the PG creds).
TLS is mutually authenticated and **validated in Rust** — no nginx/TLS proxy.

```
[customer server — NAT, no public IP]        [VPS — public IP]
 .fdb ──mmap──▶ fdb-agent ──outbound mTLS:8443──▶ fdb-ingest ──▶ postgres
                (client cert only)                (PG creds)      127.0.0.1
```

- Customer makes only **outbound** connections (works behind NAT/CGNAT).
- VPS exposes only the ingest port. PostgreSQL binds `127.0.0.1`, never the internet.
- A proxy/firewall in front of the port, if any, must be **L4 passthrough** — it
  must NOT terminate TLS (the ingest verifies the client cert itself).

## 1. PKI (do the CA on a trusted machine; keep `ca.key` offline)

```bash
# Create the private CA once.
fdb-ingest ca-init --cn "ACME integrador CA" --out-dir pki
#   -> pki/ca.crt  pki/ca.key   (guard ca.key; it is NOT needed at runtime)

# Server cert — SANs must include every host/IP the agent dials.
fdb-ingest issue-server --pki pki --san ingest.acme.com --san 203.0.113.10 --out-dir pki
#   -> pki/server.crt  pki/server.key

# One client cert per tenant (CN = tenant id used in the --tenant map).
fdb-ingest issue-client --pki pki --tenant acme --out-dir certs
#   -> certs/acme.crt  certs/acme.key
```

Ship to the VPS: `ca.crt`, `server.crt`, `server.key` (NOT `ca.key`).
Ship to the customer: `ca.crt`, `<tenant>.crt`, `<tenant>.key`.

## 2. PostgreSQL on the VPS

```conf
# postgresql.conf
listen_addresses = 'localhost'      # never expose PG to the internet
```
```sql
-- a writer role scoped to the replica DBs (not superuser)
CREATE ROLE fdb_writer LOGIN PASSWORD '...';
-- fdb-ingest auto-creates per-tenant databases on first FULL; if you pre-create
-- them, grant ownership to fdb_writer.
```

## 3. Run the gateway (systemd)

See `fdb-ingest.service`. Put `PGPASSWORD=...` in `/etc/fdb-ingest/env` (chmod 600),
one `--tenant CN=database` per customer, then:

```bash
sudo systemctl enable --now fdb-ingest
sudo journalctl -u fdb-ingest -f
```

Firewall: allow inbound only on the ingest port (e.g. 8443). Nothing else.

## 4. Run the agent on the customer (Windows)

Copy the binary + `ca.crt`, `<tenant>.crt`, `<tenant>.key` next to the `.fdb`.

**Batch snapshot** (e.g. every 15 min) via Task Scheduler:

```bat
schtasks /Create /TN fdb-agent-batch /SC MINUTE /MO 15 /RL LIMITED /TR ^
 "C:\fdb\fdb-agent.exe --gateway ingest.acme.com:8443 --server-name ingest.acme.com ^
  --ca C:\fdb\ca.crt --cert C:\fdb\acme.crt --key C:\fdb\acme.key ^
  --database C:\firebird\BANCO.FDB --all-tables --unlogged"
```

**Continuous CDC** (seed FULL once, then stream deltas) — run as a long-lived
service (e.g. via [nssm](https://nssm.cc) or `sc create`):

```
fdb-agent.exe --gateway ingest.acme.com:8443 --server-name ingest.acme.com \
  --ca ca.crt --cert acme.crt --key acme.key \
  --database C:\firebird\BANCO.FDB --all-tables \
  --watch --watch-interval 5
```

State/watermark persists in `<database>.cdc.json` on the customer side, so a
restart resumes without re-scanning. The DB file is opened read-only.

## 5. Rotation / revocation

- **Rotate a tenant**: re-issue with `issue-client` and replace the cert/key on the
  customer. The old cert stops working once removed from the CA trust (or via CRL).
- **Onboard a tenant**: `issue-client --tenant X`, add `--tenant X=replica_X` to the
  ingest unit, `systemctl reload`/restart.
- A client cert is the agent's only secret. If it leaks, the blast radius is that
  one tenant's data — never PostgreSQL access. PG creds live solely on the VPS.

## 6. Verification checklist

- [ ] `psql` from outside the VPS to PG **fails** (PG bound to localhost).
- [ ] Agent with the right cert pushes; counts match in the tenant DB.
- [ ] During a FULL re-load the table is never empty (atomic staging swap).
- [ ] A cert from a **different CA** is rejected at the TLS handshake
      (`fdb-ingest` logs `invalid peer certificate: UnknownIssuer`).
- [ ] An unknown tenant CN is rejected with `unknown tenant '<cn>'`.
