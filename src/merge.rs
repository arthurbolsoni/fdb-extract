//! Shared PostgreSQL delta-merge logic.
//!
//! Applies a CDC delta (changed rows + deleted PKs, both already encoded as PGCOPY
//! BINARY) to a target table via the temp-table dance: COPY into staging temps,
//! `DELETE ... USING` the deleted-PK temp, then `INSERT ... ON CONFLICT DO UPDATE`
//! from the upsert temp. The whole thing runs in one transaction.
//!
//! This was lifted verbatim out of `cdc::sync_table` so the same code path can be
//! driven both locally (CDC watch against a local PG) and server-side by `fdb-ingest`
//! (which receives the buffers over the wire). Behaviour is byte-for-byte identical.

use anyhow::Result;
use postgres::Client;
use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::time::Instant;

use crate::wire;

/// Everything the merge needs about the target table — all derivable from the
/// Firebird schema, so the agent can ship it to the gateway with the buffers.
pub struct MergeMeta {
    pub pg_table:     String,      // lowercase target table name
    pub cols_csv:     String,      // "col_a", "col_b", ... in PG column order
    pub pk_csv:       String,      // PK columns, comma separated
    pub pk_col_names: Vec<String>, // PK columns, each quoted, for the match clause
}

/// Granular timing of the PG apply phase. Fed into `cdc::DeltaStats` for `--debug`;
/// ignored by `fdb-ingest`.
#[derive(Default)]
pub struct ApplyTiming {
    pub setup_ns:       u64,
    pub copy_start_ns:  u64,
    pub copy_finish_ns: u64,
    pub del_copy_ns:    u64,
    pub delete_sql_ns:  u64,
    pub upsert_sql_ns:  u64,
    pub cleanup_ns:     u64,
    pub commit_ns:      u64,
}

/// Build the `INSERT ... SELECT ... FROM <src> ON CONFLICT` statement that merges a
/// staging table into the target. Non-PK columns become `col = EXCLUDED.col`; when
/// every column is part of the PK there is nothing to update, so it degrades to
/// `DO NOTHING`. Pure (no DB) so the upsert shape is unit-testable.
fn build_upsert_sql(meta: &MergeMeta, src: &str) -> String {
    let MergeMeta { pg_table, cols_csv, pk_csv, pk_col_names } = meta;
    let pk_set: HashSet<&str> = pk_col_names.iter().map(|s| s.as_str()).collect();
    let update_cols: Vec<String> = cols_csv.split(", ")
        .filter(|cn| !pk_set.contains(*cn))
        .map(|cn| format!("{cn} = EXCLUDED.{cn}"))
        .collect();
    if update_cols.is_empty() {
        format!(
            "INSERT INTO \"{pg_table}\" ({cols_csv}) SELECT {cols_csv} FROM \"{src}\" \
             ON CONFLICT ({pk_csv}) DO NOTHING")
    } else {
        let updates = update_cols.join(", ");
        format!(
            "INSERT INTO \"{pg_table}\" ({cols_csv}) SELECT {cols_csv} FROM \"{src}\" \
             ON CONFLICT ({pk_csv}) DO UPDATE SET {updates}")
    }
}

/// Apply one delta to `meta.pg_table`. `upsert_buf` / `delete_buf` are full PGCOPY
/// BINARY streams; `n_upserts` / `n_deletes` are their row counts (used only to skip
/// empty branches). Caller is expected to invoke this only when there is work to do.
pub fn apply_delta(
    client:     &mut Client,
    meta:       &MergeMeta,
    upsert_buf: &[u8],
    delete_buf: &[u8],
    n_upserts:  u64,
    n_deletes:  u64,
    debug:      bool,
) -> Result<ApplyTiming> {
    let pg_table  = &meta.pg_table;
    let cols_csv  = &meta.cols_csv;
    let pk_csv    = &meta.pk_csv;
    let delta_tmp = format!("_fdb_delta_{}", pg_table.replace('"', ""));
    let del_tmp   = format!("_fdb_del_{}",   pg_table.replace('"', ""));

    let t_setup = Instant::now();
    client.execute("BEGIN", &[])?;

    let apply: Result<[u64; 7]> = (|| {
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{delta_tmp}\""), &[]);
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{del_tmp}\""),   &[]);
        client.execute(
            &format!("CREATE TEMP TABLE \"{delta_tmp}\" (LIKE \"{pg_table}\")"),
            &[],
        )?;
        client.execute(
            &format!(
                "CREATE TEMP TABLE \"{del_tmp}\" AS SELECT {pk_csv} FROM \"{pg_table}\" WHERE false"
            ),
            &[],
        )?;
        let sn = t_setup.elapsed().as_nanos() as u64;

        let t_cs = Instant::now();
        let cw = client.copy_in(
            &format!("COPY \"{delta_tmp}\" ({cols_csv}) FROM STDIN WITH (FORMAT BINARY)")
        )?;
        let cs = t_cs.elapsed().as_nanos() as u64;

        let t_cf = Instant::now();
        let mut cw = cw;
        cw.write_all(upsert_buf)?;
        cw.flush()?;
        // into_inner not available on CopyInWriter; finish() consumes it
        cw.finish()?;
        let cf = t_cf.elapsed().as_nanos() as u64;

        let mut dc = 0u64;
        let mut dl = 0u64;
        if n_deletes > 0 {
            let t_dc = Instant::now();
            let mut dcw = client.copy_in(
                &format!("COPY \"{del_tmp}\" ({pk_csv}) FROM STDIN WITH (FORMAT BINARY)")
            )?;
            dcw.write_all(delete_buf)?;
            dcw.flush()?;
            dcw.finish()?;
            dc = t_dc.elapsed().as_nanos() as u64;

            if debug {
                let row: postgres::Row = client.query_one(
                    &format!("SELECT COUNT(*) FROM \"{del_tmp}\""), &[])?;
                let n: i64 = row.get(0);
                eprintln!("    [del copy] temp \"{del_tmp}\" has {n} rows after COPY");
            }
            let match_clause = meta.pk_col_names.iter()
                .map(|cn| format!("\"{pg_table}\".{cn} = \"{del_tmp}\".{cn}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let t_dl = Instant::now();
            client.execute(
                &format!("DELETE FROM \"{pg_table}\" USING \"{del_tmp}\" WHERE {match_clause}"),
                &[],
            )?;
            dl = t_dl.elapsed().as_nanos() as u64;
        }

        let mut up = 0u64;
        if n_upserts > 0 {
            let sql = build_upsert_sql(meta, &delta_tmp);
            let t_up = Instant::now();
            client.execute(&sql, &[])?;
            up = t_up.elapsed().as_nanos() as u64;
        }

        let t_cl = Instant::now();
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{delta_tmp}\""), &[]);
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{del_tmp}\""),   &[]);
        let cl = t_cl.elapsed().as_nanos() as u64;

        Ok([sn, cs, cf, dc, dl, up, cl])
    })();

    match apply {
        Ok([sn, cs, cf, dc, dl, up, cl]) => {
            let t_co = Instant::now();
            client.execute("COMMIT", &[])?;
            let commit_ns = t_co.elapsed().as_nanos() as u64;
            Ok(ApplyTiming {
                setup_ns: sn, copy_start_ns: cs, copy_finish_ns: cf,
                del_copy_ns: dc, delete_sql_ns: dl, upsert_sql_ns: up,
                cleanup_ns: cl, commit_ns,
            })
        }
        Err(e) => {
            let _ = client.execute("ROLLBACK", &[]);
            Err(e)
        }
    }
}

/// Streaming twin of [`apply_delta`] for the gateway: instead of taking the upsert /
/// delete buffers as in-memory slices, it pulls them straight off the wire (`body`,
/// positioned at `[u32 ulen][upsert][u32 dlen][delete]`) directly into the COPY
/// streams. Memory stays bounded regardless of delta size — a whole-table delta no
/// longer requires allocating the whole table on the gateway.
///
/// Both length-prefixed bodies are always consumed (even when `n_deletes == 0` the
/// delete body's framing must be drained) so the connection stays in sync.
pub fn apply_delta_streaming<R: Read>(
    client:    &mut Client,
    meta:      &MergeMeta,
    body:      &mut R,
    n_upserts: u64,
    n_deletes: u64,
) -> Result<()> {
    let pg_table  = &meta.pg_table;
    let cols_csv  = &meta.cols_csv;
    let pk_csv    = &meta.pk_csv;
    let delta_tmp = format!("_fdb_delta_{}", pg_table.replace('"', ""));
    let del_tmp   = format!("_fdb_del_{}",   pg_table.replace('"', ""));

    client.execute("BEGIN", &[])?;

    let apply: Result<()> = (|| {
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{delta_tmp}\""), &[]);
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{del_tmp}\""),   &[]);
        client.execute(
            &format!("CREATE TEMP TABLE \"{delta_tmp}\" (LIKE \"{pg_table}\")"), &[])?;
        client.execute(
            &format!("CREATE TEMP TABLE \"{del_tmp}\" AS SELECT {pk_csv} FROM \"{pg_table}\" WHERE false"),
            &[])?;

        // Upsert body → delta temp (streamed). Always present on the wire.
        {
            let mut cw = client.copy_in(
                &format!("COPY \"{delta_tmp}\" ({cols_csv}) FROM STDIN WITH (FORMAT BINARY)"))?;
            wire::copy_u32_prefixed(body, &mut cw)?;
            cw.finish()?;
        }

        // Delete body → delete temp (streamed) when there are deletes; otherwise the
        // empty body is still framed on the wire and must be drained.
        if n_deletes > 0 {
            {
                let mut dcw = client.copy_in(
                    &format!("COPY \"{del_tmp}\" ({pk_csv}) FROM STDIN WITH (FORMAT BINARY)"))?;
                wire::copy_u32_prefixed(body, &mut dcw)?;
                dcw.finish()?;
            }
            let match_clause = meta.pk_col_names.iter()
                .map(|cn| format!("\"{pg_table}\".{cn} = \"{del_tmp}\".{cn}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            client.execute(
                &format!("DELETE FROM \"{pg_table}\" USING \"{del_tmp}\" WHERE {match_clause}"),
                &[])?;
        } else {
            wire::copy_u32_prefixed(body, &mut io::sink())?;
        }

        if n_upserts > 0 {
            client.execute(&build_upsert_sql(meta, &delta_tmp), &[])?;
        }

        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{delta_tmp}\""), &[]);
        let _ = client.execute(&format!("DROP TABLE IF EXISTS \"{del_tmp}\""),   &[]);
        Ok(())
    })();

    match apply {
        Ok(()) => { client.execute("COMMIT", &[])?; Ok(()) }
        Err(e) => { let _ = client.execute("ROLLBACK", &[]); Err(e) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn meta(cols: &[&str], pks: &[&str]) -> MergeMeta {
        let q = |c: &str| format!("\"{c}\"");
        MergeMeta {
            pg_table:     "vendas".into(),
            cols_csv:     cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", "),
            pk_csv:       pks.iter().map(|c| q(c)).collect::<Vec<_>>().join(", "),
            pk_col_names: pks.iter().map(|c| q(c)).collect(),
        }
    }

    #[test]
    fn upsert_sql_updates_only_non_pk_columns() {
        let m = meta(&["nrovenda", "cliente", "total"], &["nrovenda"]);
        let sql = build_upsert_sql(&m, "_stg");
        assert!(sql.contains("ON CONFLICT (\"nrovenda\") DO UPDATE SET"));
        assert!(sql.contains("\"cliente\" = EXCLUDED.\"cliente\""));
        assert!(sql.contains("\"total\" = EXCLUDED.\"total\""));
        // The PK column must never appear in the SET list.
        assert!(!sql.contains("\"nrovenda\" = EXCLUDED"));
        assert!(sql.contains("FROM \"_stg\""));
    }

    #[test]
    fn upsert_sql_degrades_to_do_nothing_when_all_columns_are_pk() {
        // Composite PK covering every column → nothing left to update.
        let m = meta(&["nrovenda", "nroitem"], &["nrovenda", "nroitem"]);
        let sql = build_upsert_sql(&m, "_stg");
        assert!(sql.contains("ON CONFLICT (\"nrovenda\", \"nroitem\") DO NOTHING"));
        assert!(!sql.contains("DO UPDATE"));
    }

    /// The streaming apply relies on `copy_u32_prefixed` peeling exactly one framed
    /// body per call so the upsert body and the delete body land in the right COPY
    /// stream. Drive two back-to-back frames through it and confirm the split.
    #[test]
    fn copy_u32_prefixed_splits_sequential_bodies_in_order() {
        let upsert = b"PGCOPY-upsert-bytes".to_vec();
        let delete = b"del".to_vec();
        let mut wire = Vec::new();
        wire.extend_from_slice(&(upsert.len() as u32).to_be_bytes());
        wire.extend_from_slice(&upsert);
        wire.extend_from_slice(&(delete.len() as u32).to_be_bytes());
        wire.extend_from_slice(&delete);

        let mut r = Cursor::new(wire);
        let mut got_u = Vec::new();
        let mut got_d = Vec::new();
        let nu = crate::wire::copy_u32_prefixed(&mut r, &mut got_u).unwrap();
        let nd = crate::wire::copy_u32_prefixed(&mut r, &mut got_d).unwrap();

        assert_eq!(nu, upsert.len() as u64);
        assert_eq!(nd, delete.len() as u64);
        assert_eq!(got_u, upsert);
        assert_eq!(got_d, delete);
    }

    /// When `n_deletes == 0` the gateway still drains the (empty) delete frame into a
    /// sink so the connection stays byte-aligned for the next request.
    #[test]
    fn copy_u32_prefixed_drains_empty_delete_frame() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&0u32.to_be_bytes()); // zero-length delete body
        wire.extend_from_slice(b"NEXT");             // would be the next request
        let mut r = Cursor::new(wire);
        let n = crate::wire::copy_u32_prefixed(&mut r, &mut std::io::sink()).unwrap();
        assert_eq!(n, 0);
        // Reader is now positioned exactly at the next request's bytes.
        let mut rest = Vec::new();
        std::io::Read::read_to_end(&mut r, &mut rest).unwrap();
        assert_eq!(rest, b"NEXT");
    }

    /// A truncated frame (header promises more than the stream holds) must error, not
    /// silently apply a partial COPY.
    #[test]
    fn copy_u32_prefixed_errors_on_truncated_body() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&10u32.to_be_bytes()); // claims 10 bytes
        wire.extend_from_slice(b"only4");             // supplies 5
        let mut r = Cursor::new(wire);
        let err = crate::wire::copy_u32_prefixed(&mut r, &mut std::io::sink()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
