//! Where a CDC delta gets applied.
//!
//! `cdc::sync_table` does the pure Firebird scan and then hands the resulting delta
//! (changed rows + deleted PKs, already PGCOPY-encoded) to a [`DeltaSink`]:
//!
//!   - [`LocalPgSink`] applies it to a local PostgreSQL via `merge::apply_delta` — the
//!     original CLI `--watch` behaviour, unchanged.
//!   - [`RemoteSink`] ships it to `fdb-ingest` over mTLS — the push integrador's
//!     continuous-stream path, where no PG credential exists on the client.

use anyhow::{anyhow, Result};
use postgres::Client;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

use crate::merge::{self, ApplyTiming, MergeMeta};
use crate::wire;

pub trait DeltaSink {
    fn apply_delta(
        &mut self,
        meta:       &MergeMeta,
        upsert_buf: &[u8],
        delete_buf: &[u8],
        n_upserts:  u64,
        n_deletes:  u64,
        debug:      bool,
    ) -> Result<ApplyTiming>;

    /// Re-establish the underlying connection after an error. Best-effort.
    fn reconnect(&mut self) -> Result<()> { Ok(()) }
}

// ── Local PostgreSQL ────────────────────────────────────────────────────────────

pub struct LocalPgSink {
    client:   Client,
    host:     String,
    port:     u16,
    user:     String,
    password: String,
    dbname:   String,
}

impl LocalPgSink {
    pub fn new(host: &str, port: u16, user: &str, password: &str, dbname: &str) -> Result<Self> {
        let client = crate::pg::connect_to(host, port, user, password, dbname)?;
        Ok(Self {
            client,
            host: host.into(), port, user: user.into(),
            password: password.into(), dbname: dbname.into(),
        })
    }
}

impl DeltaSink for LocalPgSink {
    fn apply_delta(
        &mut self, meta: &MergeMeta, upsert_buf: &[u8], delete_buf: &[u8],
        n_upserts: u64, n_deletes: u64, debug: bool,
    ) -> Result<ApplyTiming> {
        merge::apply_delta(&mut self.client, meta, upsert_buf, delete_buf, n_upserts, n_deletes, debug)
    }

    fn reconnect(&mut self) -> Result<()> {
        self.client = crate::pg::connect_to(&self.host, self.port, &self.user, &self.password, &self.dbname)?;
        Ok(())
    }
}

// ── Remote gateway (mTLS) ───────────────────────────────────────────────────────

pub struct RemoteSink {
    cfg:         Arc<ClientConfig>,
    gateway:     String,
    server_name: String,
}

impl RemoteSink {
    pub fn new(cfg: Arc<ClientConfig>, gateway: &str, server_name: &str) -> Self {
        Self { cfg, gateway: gateway.into(), server_name: server_name.into() }
    }

    fn open(&self) -> Result<StreamOwned<ClientConnection, TcpStream>> {
        let sn = ServerName::try_from(self.server_name.clone())
            .map_err(|_| anyhow!("invalid server name {}", self.server_name))?;
        let conn = ClientConnection::new(Arc::clone(&self.cfg), sn)?;
        let tcp = TcpStream::connect(&self.gateway)?;
        Ok(StreamOwned::new(conn, tcp))
    }
}

impl DeltaSink for RemoteSink {
    fn apply_delta(
        &mut self, meta: &MergeMeta, upsert_buf: &[u8], delete_buf: &[u8],
        n_upserts: u64, n_deletes: u64, _debug: bool,
    ) -> Result<ApplyTiming> {
        let mut tls = self.open()?;
        let header = serde_json::to_vec(&wire::DeltaHeader {
            table:        meta.pg_table.clone(),
            cols_csv:     meta.cols_csv.clone(),
            pk_csv:       meta.pk_csv.clone(),
            pk_col_names: meta.pk_col_names.clone(),
            n_upserts,
            n_deletes,
        })?;
        wire::write_request_header(&mut tls, wire::Op::Delta, &header)?;
        wire::write_u32_prefixed(&mut tls, upsert_buf)?;
        wire::write_u32_prefixed(&mut tls, delete_buf)?;
        tls.flush()?;
        let resp = wire::read_response(&mut tls)?;
        if let Some(e) = resp.error {
            return Err(anyhow!("gateway rejected delta: {e}"));
        }
        // Timing breakdown is server-side and not reported back; return zeros.
        Ok(ApplyTiming::default())
    }
}
