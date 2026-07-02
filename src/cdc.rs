use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{
    extract::write_field_binary,
    ods::{self, OdsReader},
};

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default)]
pub struct CdcState {
    pub version:   u32,
    pub last_txn:  u64,
    #[serde(default)]
    pub tables:    HashMap<String, TableState>,
    #[serde(default)]
    pub page_gens: HashMap<String, HashMap<u32, u32>>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct TableState {
    pub last_txn:  u64,
    pub pk_fields: Vec<String>,
}

impl CdcState {
    pub fn load(path: &str) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Self { version: 1, ..Default::default() })
    }

    // Atomic save — write to .tmp then rename so a crash mid-write never corrupts state.
    pub fn save(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        let tmp  = format!("{path}.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// P4: persist per-table PK snapshots in a binary side file so a process
    /// restart doesn't trigger a full first_cycle table scan.
    /// Format: "FDBS"[4] ver(u32) n_tables(u64)
    ///         repeat: name_len(u32) name(utf8) pk_byte_len(u32)
    ///                 n_pages(u64) repeat: page_num(u32) flat_len(u32) flat
    pub fn save_snapshots(
        path:   &str,
        caches: &HashMap<String, Option<TableCache>>,
    ) -> Result<()> {
        let snap_path = format!("{path}.snap");
        let tmp       = format!("{snap_path}.tmp");
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        buf.extend_from_slice(b"FDBS");
        buf.extend_from_slice(&1u32.to_le_bytes());
        let n_tables: u64 = caches.values()
            .filter(|c| c.as_ref().map(|x| x.pk_byte_len > 0 && !x.prev_pks_per_page.is_empty()).unwrap_or(false))
            .count() as u64;
        buf.extend_from_slice(&n_tables.to_le_bytes());
        for (name, copt) in caches {
            let Some(c) = copt else { continue; };
            if c.pk_byte_len == 0 || c.prev_pks_per_page.is_empty() { continue; }
            let nb = name.as_bytes();
            buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
            buf.extend_from_slice(nb);
            buf.extend_from_slice(&(c.pk_byte_len as u32).to_le_bytes());
            buf.extend_from_slice(&(c.prev_pks_per_page.len() as u64).to_le_bytes());
            for (page_num, flat) in &c.prev_pks_per_page {
                buf.extend_from_slice(&page_num.to_le_bytes());
                buf.extend_from_slice(&(flat.len() as u32).to_le_bytes());
                buf.extend_from_slice(flat);
            }
        }
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, &snap_path)?;
        Ok(())
    }

    /// Returns `table_name -> (pk_byte_len, page_num -> flat_pks)`.
    /// Returns empty map on any error / missing file / version mismatch (no panic).
    pub fn load_snapshots(path: &str) -> HashMap<String, (usize, HashMap<u32, Vec<u8>>)> {
        let snap_path = format!("{path}.snap");
        let data = match std::fs::read(&snap_path) {
            Ok(d) => d,
            Err(_) => return HashMap::new(),
        };
        let mut out = HashMap::new();
        let read = || -> Option<HashMap<String, (usize, HashMap<u32, Vec<u8>>)>> {
            if data.len() < 16 || &data[0..4] != b"FDBS" { return None; }
            let ver = u32::from_le_bytes(data[4..8].try_into().ok()?);
            if ver != 1 { return None; }
            let n_tables = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
            let mut o = HashMap::with_capacity(n_tables);
            let mut i = 16usize;
            for _ in 0..n_tables {
                if i + 4 > data.len() { return None; }
                let nl = u32::from_le_bytes(data[i..i+4].try_into().ok()?) as usize;
                i += 4;
                if i + nl > data.len() { return None; }
                let name = std::str::from_utf8(&data[i..i+nl]).ok()?.to_string();
                i += nl;
                if i + 4 > data.len() { return None; }
                let pk_byte_len = u32::from_le_bytes(data[i..i+4].try_into().ok()?) as usize;
                i += 4;
                if i + 8 > data.len() { return None; }
                let n_pages = u64::from_le_bytes(data[i..i+8].try_into().ok()?) as usize;
                i += 8;
                let mut pages = HashMap::with_capacity(n_pages);
                for _ in 0..n_pages {
                    if i + 8 > data.len() { return None; }
                    let pn = u32::from_le_bytes(data[i..i+4].try_into().ok()?);
                    i += 4;
                    let fl = u32::from_le_bytes(data[i..i+4].try_into().ok()?) as usize;
                    i += 4;
                    if i + fl > data.len() { return None; }
                    pages.insert(pn, data[i..i+fl].to_vec());
                    i += fl;
                }
                o.insert(name, (pk_byte_len, pages));
            }
            Some(o)
        };
        if let Some(parsed) = read() { out = parsed; }
        out
    }
}

// ── Table cache (runtime-only, not serialized) ────────────────────────────────

#[derive(Clone)]
pub(crate) struct SlotInfo {
    pub(crate) desc:      ods::Desc,
    pub(crate) null_byte: usize,
    pub(crate) null_mask: u8,
}

/// Build PG-column-ordered slots for one format's descriptors. A column whose field
/// id is absent from this format (added by a later ALTER TABLE) gets a zero descriptor
/// (dtype 0) → `write_field_binary` emits the PGCOPY NULL sentinel.
pub(crate) fn build_slots(field_order: &[(usize, String)], descs: &[ods::Desc]) -> Vec<SlotInfo> {
    field_order.iter().map(|(fid, _)| {
        let desc = descs.get(*fid).cloned().unwrap_or_else(ods::Desc::default_zero);
        SlotInfo { desc, null_byte: fid / 8, null_mask: 1 << (fid % 8) }
    }).collect()
}

impl TableCache {
    /// Slots for a record's on-disk format byte, falling back to the latest-format
    /// slots when that format wasn't pre-cached (should not happen for live records).
    #[inline]
    pub(crate) fn slots_for(&self, fmt: u8) -> &[SlotInfo] {
        match self.fmt_slots.get(fmt as usize).and_then(|o| o.as_ref()) {
            Some(s) => s,
            None    => &self.slots,
        }
    }
}

pub struct TableCache {
    pub(crate) relation_id:  u16,
    pub(crate) slots:        Vec<SlotInfo>,
    /// Slots indexed by on-disk format byte (rhd_format). `None` = no such format;
    /// resolve a record via `slots_for(raw[12])`. Built once at cache init.
    pub(crate) fmt_slots:    Vec<Option<Vec<SlotInfo>>>,
    pub(crate) col_names:    Vec<String>,
    pub(crate) pk_col_names: Vec<String>,
    pub(crate) n_fields:     i16,
    pub(crate) n_pk:         i16,
    // Fix #6: Option<usize> instead of usize::MAX sentinel
    pub(crate) pk_indices:   Vec<Option<usize>>,
    pub(crate) first_pp:     Option<u32>,
    pub(crate) pp_gen:       u32,
    pub(crate) data_pages:   Vec<u32>,

    /// Per-page PGCOPY-encoded primary-key snapshot for express-delete detection.
    ///
    /// Storage: flat sorted `Vec<u8>` with fixed stride `pk_byte_len` — one
    /// contiguous byte array per page instead of HashSet<Vec<u8>>.
    /// Memory: `pk_byte_len` bytes/row vs. ~48 bytes/row (Vec overhead + HashSet
    /// bucket), a ~6-10x reduction for typical integer PKs.
    /// `pk_byte_len == 0` means variable-length PKs (VARCHAR etc.) — snapshot
    /// disabled for those tables.
    pub(crate) prev_pks_per_page: HashMap<u32, Vec<u8>>,
    pub(crate) pk_tracked:        bool,
    /// Fixed PGCOPY-encoded byte length per PK tuple. 0 = variable (no snapshot).
    pub(crate) pk_byte_len:       usize,
}

// ── Flat sorted PK snapshot helpers ──────────────────────────────────────────
//
// Per-page PK sets are stored as flat sorted byte arrays instead of
// HashSet<Vec<u8>>.  For fixed-stride PKs (all numeric/date types) this cuts
// memory ~6-10x: no per-entry Vec allocation (24 bytes overhead) and no HashSet
// bucket table (~24 bytes/slot).  A 3M-row table with INTEGER PKs goes from
// ~200 MB per snapshot to ~24 MB.

/// Compute the fixed PGCOPY-encoded byte length for the PK tuple of a table.
/// Returns 0 if any PK field is variable-length (TEXT, VARCHAR, BLOB etc.),
/// which disables flat storage and express-delete tracking for that table.
pub(crate) fn pk_pgcopy_stride(pk_indices: &[Option<usize>], slots: &[SlotInfo]) -> usize {
    let mut total = 0usize;
    for &pi in pk_indices {
        let Some(pi) = pi else { return 0 };
        let desc = &slots[pi].desc;
        // PGCOPY format: i32 length-prefix (4 bytes) + payload.
        // Scaled integers are emitted as f64 (8 bytes payload).
        let payload = match desc.dtype {
            ods::DTYPE_SHORT    => if desc.scale == 0 { 2 } else { 8 },
            ods::DTYPE_LONG     => if desc.scale == 0 { 4 } else { 8 },
            ods::DTYPE_INT64 | ods::DTYPE_QUAD => 8,
            ods::DTYPE_REAL     => 4,
            ods::DTYPE_DOUBLE | ods::DTYPE_D_FLOAT => 8,
            ods::DTYPE_SQL_DATE => 4,
            ods::DTYPE_SQL_TIME | ods::DTYPE_SQL_TIME_TZ => 8,
            ods::DTYPE_TIMESTAMP | ods::DTYPE_TIMESTAMP_TZ => 8,
            ods::DTYPE_BOOLEAN  => 1,
            _ => return 0, // TEXT / VARCHAR / BLOB / unknown → variable
        };
        total += 4 + payload; // length prefix + payload
    }
    total
}

/// Binary search for `target` in a flat sorted PK array with fixed `stride`.
#[inline]
pub(crate) fn flat_pk_contains(flat: &[u8], stride: usize, target: &[u8]) -> bool {
    if stride == 0 || flat.is_empty() { return false; }
    let n = flat.len() / stride;
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let chunk = &flat[mid * stride..(mid + 1) * stride];
        match chunk.cmp(target) {
            std::cmp::Ordering::Equal   => return true,
            std::cmp::Ordering::Less    => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    false
}

/// Sort and deduplicate a flat PK byte array in-place by fixed `stride`.
///
/// Common strides (4, 8, 12, 16, 20, 24) use a const-generic fast path that
/// reinterprets the buffer as `&mut [[u8; S]]` and calls `slice::sort_unstable`
/// — zero extra allocation, std-quality sort. Exotic strides fall through to
/// an in-place heapsort that uses only a fixed-size stack scratch buffer.
pub(crate) fn flat_pk_sort_dedup(flat: &mut Vec<u8>, stride: usize) {
    match stride {
        0  => {}
        4  => sort_dedup_const::<4>(flat),
        8  => sort_dedup_const::<8>(flat),
        12 => sort_dedup_const::<12>(flat),
        16 => sort_dedup_const::<16>(flat),
        20 => sort_dedup_const::<20>(flat),
        24 => sort_dedup_const::<24>(flat),
        _  => sort_dedup_generic(flat, stride),
    }
}

#[inline]
fn sort_dedup_const<const S: usize>(flat: &mut Vec<u8>) {
    if flat.len() < 2 * S { return; }
    // SAFETY: caller invariant — flat.len() is a multiple of S, and S matches stride.
    let n = flat.len() / S;
    let slice: &mut [[u8; S]] = unsafe {
        std::slice::from_raw_parts_mut(flat.as_mut_ptr() as *mut [u8; S], n)
    };
    slice.sort_unstable();
    // In-place dedup: write-pointer pattern, no allocation.
    let mut w = 1usize;
    let mut r = 1usize;
    while r < n {
        if slice[r] != slice[w - 1] {
            if r != w { slice[w] = slice[r]; }
            w += 1;
        }
        r += 1;
    }
    flat.truncate(w * S);
}

fn sort_dedup_generic(flat: &mut Vec<u8>, stride: usize) {
    if stride == 0 || stride > 64 || flat.len() <= stride { return; }
    let n = flat.len() / stride;
    if n < 2 { return; }
    let mut tmp = [0u8; 64];
    // Heapsort — O(N log N) worst case, O(1) extra stack (besides tmp).
    let buf = flat.as_mut_slice();
    let mut i = n / 2;
    while i > 0 {
        i -= 1;
        heap_sift_down(buf, stride, i, n, &mut tmp[..stride]);
    }
    let mut end = n;
    while end > 1 {
        end -= 1;
        chunk_swap_at(buf, stride, 0, end, &mut tmp[..stride]);
        heap_sift_down(buf, stride, 0, end, &mut tmp[..stride]);
    }
    // In-place dedup
    let mut w = stride;
    let mut r = stride;
    while r < flat.len() {
        if flat[r..r + stride] != flat[w - stride..w] {
            if r != w { flat.copy_within(r..r + stride, w); }
            w += stride;
        }
        r += stride;
    }
    flat.truncate(w);
}

#[inline]
fn chunk_swap_at(buf: &mut [u8], stride: usize, a: usize, b: usize, tmp: &mut [u8]) {
    if a == b { return; }
    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
    let (left, right) = buf.split_at_mut(hi * stride);
    tmp.copy_from_slice(&left[lo * stride..lo * stride + stride]);
    left[lo * stride..lo * stride + stride].copy_from_slice(&right[..stride]);
    right[..stride].copy_from_slice(tmp);
}

fn heap_sift_down(buf: &mut [u8], stride: usize, start: usize, end: usize, tmp: &mut [u8]) {
    let mut root = start;
    loop {
        let mut child = 2 * root + 1;
        if child >= end { break; }
        if child + 1 < end {
            let go_right = buf[child * stride..child * stride + stride]
                < buf[(child + 1) * stride..(child + 1) * stride + stride];
            if go_right { child += 1; }
        }
        let need_swap = buf[root * stride..root * stride + stride]
            < buf[child * stride..child * stride + stride];
        if need_swap {
            chunk_swap_at(buf, stride, root, child, tmp);
            root = child;
        } else {
            break;
        }
    }
}

// ── Table delta ───────────────────────────────────────────────────────────────

pub struct DeltaStats {
    pub upserts:         u64,
    pub deletes:         u64,
    pub max_txn:         u64,
    pub pages_total:     u32,
    pub pages_skipped:   u32,
    pub pages_scanned:   u32,
    pub records_checked: u64,
    pub scan_ns:         u64,
    pub pg_ns:           u64,
    pub express_deletes: u64,
    pub sort_page_ns:    u64,
    pub sort_alive_ns:   u64,
    pub express_ns:      u64,
    // Granular per-table breakdown (sum should ≈ scan_ns + pg_ns + setup + commit).
    pub setup_ns:        u64, // BEGIN + 2x DROP IF EXISTS + 2x CREATE TEMP TABLE
    pub copy_start_ns:   u64, // client.copy_in() handshake for delta COPY
    pub scan_pure_ns:    u64, // scan_changes_into (no PG I/O)
    pub copy_finish_ns:  u64, // flush + into_inner.finish() for delta COPY
    pub del_copy_ns:     u64, // del_tmp COPY (start + write + finish)
    pub delete_sql_ns:   u64, // DELETE FROM ... USING
    pub upsert_sql_ns:   u64, // INSERT ... ON CONFLICT
    pub cleanup_ns:      u64, // 2x DROP at end
    pub commit_ns:       u64, // COMMIT
}

pub fn sync_table(
    db:            &OdsReader,
    sink:          &mut dyn crate::sink::DeltaSink,
    table:         &str,
    last_txn:      u64,
    pk_fields:     &[String],
    page_gens:     &mut HashMap<u32, u32>,
    cache:         &mut Option<TableCache>,
    snapshot_init: Option<HashMap<u32, Vec<u8>>>,
    debug:         bool,
) -> Result<DeltaStats> {
    let pg_table = table.to_lowercase();

    // Initialize schema cache once per process lifetime
    if cache.is_none() {
        let relation_id = db.find_relation_id(table)?;
        let descs = db.read_format(relation_id, u16::MAX)?;
        let field_order: Vec<(usize, String)> = db.read_field_names(relation_id, table)
            .unwrap_or_else(|_| (0..descs.len()).map(|i| (i, format!("col_{i}"))).collect());

        // Slots for the latest format — used for the stride/PK metadata below.
        let slots: Vec<SlotInfo> = build_slots(&field_order, &descs);

        // Per-record-format slots. Every record stamps its format in rhd_format
        // (raw[12]); rows written before a later ALTER TABLE keep the old layout.
        // Decoding them with the latest format reads wrong offsets (corrupt upsert
        // values, wrong delete PKs). Cache slots for every available format version;
        // absent columns (field id ≥ that format's field count) decode as NULL.
        let mut fmt_slots: Vec<Option<Vec<SlotInfo>>> = (0..256).map(|_| None).collect();
        for v in 0..=255u16 {
            if let Ok(d) = db.read_format(relation_id, v) {
                fmt_slots[v as usize] = Some(build_slots(&field_order, &d));
            }
        }

        // Fix #6: Option<usize> instead of usize::MAX sentinel
        let pk_indices: Vec<Option<usize>> = pk_fields.iter().map(|pk| {
            field_order.iter().position(|(_, n)| n.eq_ignore_ascii_case(pk))
        }).collect();

        let col_names: Vec<String> = field_order.iter()
            .map(|(_, n)| format!("\"{}\"", n.to_lowercase()))
            .collect();
        let pk_col_names: Vec<String> = pk_fields.iter()
            .map(|n| format!("\"{}\"", n.to_lowercase()))
            .collect();

        let n_fields   = slots.len() as i16;
        let n_pk       = pk_indices.len() as i16;
        let pk_tracked = pk_indices.iter().all(|i| i.is_some());
        let pk_byte_len = pk_pgcopy_stride(&pk_indices, &slots);
        let first_pp   = db.find_first_pp(relation_id);
        let (pp_gen, data_pages) = if let Some(pp) = first_pp {
            let raw = db.page(pp);
            let gen = u32::from_le_bytes(raw[4..8].try_into().unwrap());
            (gen, db.data_pages_for(pp))
        } else {
            (0, vec![])
        };

        // P4: hydrate snapshot from disk on first cycle so we don't have to
        // re-scan the entire table just to rebuild prev_pks_per_page.
        let prev_pks_per_page = match snapshot_init {
            Some(snap) if pk_byte_len > 0 => snap,
            _ => HashMap::new(),
        };

        *cache = Some(TableCache {
            relation_id,
            slots, fmt_slots, col_names, pk_col_names,
            n_fields, n_pk, pk_indices,
            first_pp, pp_gen, data_pages,
            prev_pks_per_page,
            pk_tracked,
            pk_byte_len,
        });
    }

    let c = cache.as_mut().unwrap();

    // Refresh data_pages — and re-resolve first_pp when previously None so
    // CREATE TABLE after startup is detected (B3).
    let mut data_pages_refreshed = false;
    match c.first_pp {
        None => {
            if let Some(pp) = db.find_first_pp(c.relation_id) {
                let raw = db.page(pp);
                c.first_pp   = Some(pp);
                c.pp_gen     = u32::from_le_bytes(raw[4..8].try_into().unwrap());
                c.data_pages = db.data_pages_for(pp);
                data_pages_refreshed = true;
            } else {
                return Ok(DeltaStats { upserts: 0, deletes: 0, max_txn: last_txn,
                    pages_total: 0, pages_skipped: 0, pages_scanned: 0,
                    records_checked: 0, scan_ns: 0, pg_ns: 0, express_deletes: 0,
                    sort_page_ns: 0, sort_alive_ns: 0, express_ns: 0,
                    setup_ns: 0, copy_start_ns: 0, scan_pure_ns: 0,
                    copy_finish_ns: 0, del_copy_ns: 0, delete_sql_ns: 0,
                    upsert_sql_ns: 0, cleanup_ns: 0, commit_ns: 0 });
            }
        }
        Some(pp) => {
            let raw     = db.page(pp);
            let cur_gen = u32::from_le_bytes(raw[4..8].try_into().unwrap());
            if cur_gen != c.pp_gen {
                c.data_pages = db.data_pages_for(pp);
                c.pp_gen     = cur_gen;
                data_pages_refreshed = true;
            }
        }
    }

    let cols_csv = c.col_names.join(", ");
    let pk_csv   = c.pk_col_names.join(", ");

    // Scan first — pure Rust, no PG involvement.
    // Upserts are buffered in memory; deletes were always in-memory (delete_buf).
    // In steady-state CDC most cycles have 0 upserts + 0 deletes, so the buffer
    // stays at 19 bytes (PGCOPY header + trailer). We only open a PG transaction
    // when there is actual data to apply — eliminating the ~49ms COPY finish
    // round-trip per table that dominates idle cycles.
    let mut upsert_buf = Vec::<u8>::with_capacity(64 * 1024);
    let t_sp  = Instant::now();
    let scan  = scan_changes_into(db, c, last_txn, page_gens, &mut upsert_buf, debug)?;
    let scan_pure_ns = t_sp.elapsed().as_nanos() as u64;

    // Timers for the PG apply phase; 0 when the apply is skipped.
    let mut setup_ns      = 0u64;
    let mut copy_start_ns = 0u64;
    let mut copy_finish_ns= 0u64;
    let mut del_copy_ns   = 0u64;
    let mut delete_sql_ns = 0u64;
    let mut upsert_sql_ns = 0u64;
    let mut cleanup_ns    = 0u64;
    let mut commit_ns     = 0u64;

    if scan.upserts > 0 || scan.deletes > 0 {
        let meta = crate::merge::MergeMeta {
            pg_table:     pg_table.clone(),
            cols_csv:     cols_csv.clone(),
            pk_csv:       pk_csv.clone(),
            pk_col_names: c.pk_col_names.clone(),
        };
        let t = sink.apply_delta(
            &meta, &upsert_buf, &scan.delete_buf, scan.upserts, scan.deletes, debug,
        )?;
        setup_ns       = t.setup_ns;
        copy_start_ns  = t.copy_start_ns;
        copy_finish_ns = t.copy_finish_ns;
        del_copy_ns    = t.del_copy_ns;
        delete_sql_ns  = t.delete_sql_ns;
        upsert_sql_ns  = t.upsert_sql_ns;
        cleanup_ns     = t.cleanup_ns;
        commit_ns      = t.commit_ns;
    }

    // Back-compat aggregate totals.
    let scan_ns = copy_start_ns + scan_pure_ns + copy_finish_ns;
    let pg_ns   = del_copy_ns + delete_sql_ns + upsert_sql_ns + cleanup_ns;

    // Merge new_gens only after successful PG apply
    page_gens.extend(scan.new_gens);
    // Advance PK snapshot incrementally — only replace entries for changed pages so
    // prev_pks_per_page never holds two full copies of the table simultaneously.
    if c.pk_byte_len > 0 {
        for (dp_n, new_flat) in scan.new_pks_per_page {
            c.prev_pks_per_page.insert(dp_n, new_flat);
        }
    }

    // B4+P10: drop entries for pages no longer in c.data_pages so the maps don't
    // grow unbounded as Firebird GCs old pages. Only run when data_pages was
    // refreshed — otherwise the live set is identical to last cycle and the
    // retain pass is pure overhead (HashSet of 200k+ u32 takes 5-10ms).
    if data_pages_refreshed {
        let live: HashSet<u32> = c.data_pages.iter().copied().collect();
        c.prev_pks_per_page.retain(|k, _| live.contains(k));
        page_gens.retain(|k, _| live.contains(k));
    }

    let pages_scanned = scan.pages_total.saturating_sub(scan.pages_skipped);

    Ok(DeltaStats {
        upserts:         scan.upserts,
        deletes:         scan.deletes,
        max_txn:         scan.max_txn,
        pages_total:     scan.pages_total,
        pages_skipped:   scan.pages_skipped,
        pages_scanned,
        records_checked: scan.records_checked,
        scan_ns,
        pg_ns,
        express_deletes: scan.express_deletes,
        sort_page_ns:    scan.sort_page_ns,
        sort_alive_ns:   scan.sort_alive_ns,
        express_ns:      scan.express_ns,
        setup_ns, copy_start_ns, scan_pure_ns, copy_finish_ns,
        del_copy_ns, delete_sql_ns, upsert_sql_ns, cleanup_ns, commit_ns,
    })
}

// ── Scan (pure — no PG dependency) ───────────────────────────────────────────

pub(crate) struct ScanOutput {
    pub(crate) delete_buf:       Vec<u8>,
    pub(crate) upserts:          u64,
    pub(crate) deletes:          u64,
    pub(crate) max_txn:          u64,
    pub(crate) new_gens:         HashMap<u32, u32>,
    pub(crate) new_pks_per_page: HashMap<u32, Vec<u8>>,
    pub(crate) pages_total:      u32,
    pub(crate) pages_skipped:    u32,
    pub(crate) records_checked:  u64,
    pub(crate) express_deletes:  u64,
    /// Sum of per-page `flat_pk_sort_dedup` calls (snapshot rebuild path).
    pub(crate) sort_page_ns:     u64,
    /// `flat_pk_sort_dedup` on the global `alive_flat` set (express-delete prep).
    pub(crate) sort_alive_ns:    u64,
    /// Full express-delete block: alive_flat build + sort + diff loop.
    pub(crate) express_ns:       u64,
}

/// Encode the primary-key fields of `rec_buf` in PostgreSQL COPY BINARY layout.
fn encode_pk_pgcopy(
    rec_buf:  &[u8],
    c:        &TableCache,
    fmt:      u8,
    out:      &mut Vec<u8>,
    text_buf: &mut Vec<u8>,
) -> Result<()> {
    out.clear();
    let slots = c.slots_for(fmt);
    // Fix #6: Option<usize> — bail if any PK index is missing
    for pi in &c.pk_indices {
        let pi = pi.ok_or_else(|| anyhow::anyhow!("pk index missing"))?;
        let slot = &slots[pi];
        // Fix #7: remove redundant `nb != 0` check; `nb & null_mask != 0` is sufficient
        let nb = rec_buf.get(slot.null_byte).copied().unwrap_or(0xFF);
        if nb & slot.null_mask != 0 {
            out.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            write_field_binary(rec_buf, &slot.desc, out, text_buf)?;
        }
    }
    Ok(())
}

/// Maximum depth for back-version chain following on delete stubs.
/// Limits follow depth for corrupted or deeply-nested version chains.
const MAX_CHAIN_DEPTH: usize = 8;

/// Backward-compat wrapper used by tests — buffers upserts internally.
#[cfg(test)]
pub(crate) fn scan_changes(
    db:        &OdsReader,
    c:         &TableCache,
    last_txn:  u64,
    page_gens: &HashMap<u32, u32>,
    debug:     bool,
) -> Result<ScanOutput> {
    let mut sink = Vec::<u8>::with_capacity(64 * 1024);
    scan_changes_into(db, c, last_txn, page_gens, &mut sink, debug)
}

/// Streaming scan: upserts are written to `upsert_writer` (a PG COPY stream in
/// production, an in-memory `Vec<u8>` in tests). Deletes are kept in a small
/// in-memory buffer inside `ScanOutput` (delete volume is usually << upserts).
pub(crate) fn scan_changes_into<W: Write>(
    db:            &OdsReader,
    c:             &TableCache,
    last_txn:      u64,
    page_gens:     &HashMap<u32, u32>,
    upsert_writer: &mut W,
    debug:         bool,
) -> Result<ScanOutput> {
    let mut delete_buf      = Vec::<u8>::with_capacity(16 * 1024);
    let mut rec_buf         = Vec::<u8>::with_capacity(4096);
    let mut text_buf        = Vec::<u8>::with_capacity(512);
    let mut pk_tmp          = Vec::<u8>::with_capacity(32);
    let mut upserts         = 0u64;
    let mut deletes         = 0u64;
    let mut express_deletes = 0u64;
    let mut max_txn         = last_txn;
    let mut pages_total     = 0u32;
    let mut pages_skipped   = 0u32;
    let mut records_checked = 0u64;
    let mut new_gens        = HashMap::<u32, u32>::new();
    let mut sort_page_ns    = 0u64;
    let mut sort_alive_ns   = 0u64;
    let mut express_ns      = 0u64;

    let     is_first_cycle   = c.pk_byte_len > 0 && c.prev_pks_per_page.is_empty();
    // Flat sorted Vec<u8> per page; stride = c.pk_byte_len. Zero alloc per row.
    let mut new_pks_per_page = HashMap::<u32, Vec<u8>>::new();

    upsert_writer.write_all(b"PGCOPY\n\xff\r\n\0")?;
    upsert_writer.write_all(&0i32.to_be_bytes())?;
    upsert_writer.write_all(&0i32.to_be_bytes())?;

    delete_buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    delete_buf.extend_from_slice(&0i32.to_be_bytes());
    delete_buf.extend_from_slice(&0i32.to_be_bytes());

    for &dp_n in &c.data_pages {
        let dp = db.page(dp_n);
        if dp[0] != ods::PAG_DATA { continue; }
        pages_total += 1;

        let gen          = u32::from_le_bytes(dp[4..8].try_into().unwrap());
        let page_changed = page_gens.get(&dp_n).copied() != Some(gen);
        if page_changed {
            new_gens.insert(dp_n, gen);
        } else {
            pages_skipped += 1;
        }

        let rebuild_pks = c.pk_byte_len > 0 && (page_changed || is_first_cycle);
        let mut page_pks_now: Vec<u8> = Vec::new();

        // On unchanged pages where PKs don't need rebuilding only RHD_DELETED
        // slots can carry new information (upserts are impossible, snapshot is
        // already up-to-date).  Skip every non-deleted slot without calling
        // record_transaction — saves ~20 ns × millions of live records per cycle.
        let delete_only = !page_changed && !rebuild_pks;

        let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
        for s in 0..cnt {
            let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
            let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
            if off == 0 || len == 0 || off + len > db.page_size { continue; }
            let raw = &dp[off..off+len];

            if raw.len() < 13 { continue; }
            let flags_raw = u16::from_le_bytes([raw[10], raw[11]]);

            // Fast path: skip non-deleted slots immediately on unchanged pages.
            if delete_only && (flags_raw & ods::RHD_DELETED == 0) { continue; }

            if debug && (flags_raw & ods::RHD_DELETED != 0) {
                let raw_txn = u32::from_le_bytes(raw[0..4].try_into().unwrap());
                eprintln!("    [del probe] page={dp_n} slot={s} raw_txn={raw_txn} raw_len={} flags=0x{flags_raw:04x} last_txn={last_txn}", raw.len());
            }

            let Some(txn) = ods::record_transaction(raw) else { continue };
            records_checked += 1;

            // Single-shot unpack: RLE decompression is the dominant cost per record.
            // Both PK-snapshot rebuild and upsert/delete handling need the same
            // decompressed buffer, so decompress at most once per record.
            let pk_skip   = ods::RHD_DELETED | ods::RHD_CHAIN | ods::RHD_FRAGMENT
                          | ods::RHD_INCOMPLETE | ods::RHD_BLOB;
            let want_pk     = rebuild_pks && (flags_raw & pk_skip == 0);
            let want_change = txn > last_txn;
            if !want_pk && !want_change { continue; }

            let unpacked = ods::unpack_record_cdc(raw, &mut rec_buf);

            if want_pk && matches!(unpacked, Some(false))
                && encode_pk_pgcopy(&rec_buf, c, raw[12], &mut pk_tmp, &mut text_buf).is_ok()
                && pk_tmp.len() == c.pk_byte_len
            {
                page_pks_now.extend_from_slice(&pk_tmp);
            }

            if !want_change { continue; }
            if txn > max_txn { max_txn = txn; }

            let Some(deleted) = unpacked else {
                if debug && (flags_raw & 1 != 0) {
                    eprintln!("    [del skip] page={dp_n} txn={txn} raw_len={} flags=0x{flags_raw:04x} (unpack→None)", raw.len());
                }
                continue;
            };

            if deleted {
                if debug {
                    eprintln!("    [del scan] page={dp_n} txn={txn} raw_len={} flags=0x{flags_raw:04x}", raw.len());
                }
                // Fix #6: skip if any PK index unresolved
                if !c.pk_tracked { continue; }

                // Fix #2: depth-limited chain follow.
                // Delete stubs have zeroed payload; follow back-version chain until
                // we reach a record that unpack_record_cdc can decompress.
                // Only continue following when the intermediate record has a valid
                // chain-type flag (RHD_CHAIN | RHD_DELETED) — FRAGMENT/INCOMPLETE
                // have different b_page/b_line semantics and are not followed.
                let del_fmt;
                {
                    let mut chain_page = u32::from_le_bytes(raw[4..8].try_into().unwrap());
                    let mut chain_line = u16::from_le_bytes([raw[8], raw[9]]) as usize;
                    let mut resolved   = false;
                    let mut found_fmt  = raw[12];

                    // b_page=0 means no back-version exists (insert+delete in same
                    // transaction, or Firebird GC already cleared the pointer).
                    // The row was never visible in a committed state we could have
                    // captured, so skip the chain-follow entirely.
                    if chain_page == 0 {
                        continue;
                    }

                    'chain: for depth in 0..MAX_CHAIN_DEPTH {
                        match db.read_slot_raw(chain_page, chain_line) {
                            None => {
                                if debug {
                                    eprintln!("    [del chain] slot not found page={chain_page} line={chain_line} depth={depth}");
                                }
                                break 'chain;
                            }
                            Some(back_raw) => {
                                match ods::unpack_record_cdc(back_raw, &mut rec_buf) {
                                    Some(_) => {
                                        if debug && depth > 0 {
                                            eprintln!("    [del chain ok] resolved at depth={depth} rec_buf_len={}", rec_buf.len());
                                        }
                                        // rec_buf now holds the back-version's data; decode
                                        // the PK with THAT record's on-disk format.
                                        found_fmt = back_raw[12];
                                        resolved = true;
                                        break 'chain;
                                    }
                                    None => {
                                        // Unpack failed — continue following only for chain-type records
                                        if back_raw.len() < 13 { break 'chain; }
                                        let bflags = u16::from_le_bytes([back_raw[10], back_raw[11]]);
                                        let chain_flags = ods::RHD_CHAIN | ods::RHD_DELETED;
                                        if bflags & chain_flags == 0 {
                                            if debug {
                                                eprintln!("    [del chain] stopped: flags=0x{bflags:04x} depth={depth}");
                                            }
                                            break 'chain;
                                        }
                                        chain_page = u32::from_le_bytes(back_raw[4..8].try_into().unwrap());
                                        chain_line = u16::from_le_bytes([back_raw[8], back_raw[9]]) as usize;
                                    }
                                }
                            }
                        }
                    }

                    if !resolved {
                        if debug {
                            eprintln!("    [del chain] exhausted without resolving page={dp_n} txn={txn}");
                        }
                        continue;
                    }
                    del_fmt = found_fmt;
                }

                let del_slots = c.slots_for(del_fmt);
                delete_buf.extend_from_slice(&c.n_pk.to_be_bytes());
                for pi in &c.pk_indices {
                    let pi   = pi.unwrap(); // safe: pk_tracked guarantees all Some
                    let slot = &del_slots[pi];
                    // Fix #7: redundant `nb != 0` removed
                    let nb = rec_buf.get(slot.null_byte).copied().unwrap_or(0xFF);
                    if nb & slot.null_mask != 0 {
                        delete_buf.extend_from_slice(&(-1i32).to_be_bytes());
                    } else {
                        write_field_binary(&rec_buf, &slot.desc, &mut delete_buf, &mut text_buf)?;
                    }
                }
                deletes += 1;
            } else if page_changed {
                // rec_buf holds this live record (unpacked from `raw` above); decode
                // every column with the record's own on-disk format.
                upsert_writer.write_all(&c.n_fields.to_be_bytes())?;
                for slot in c.slots_for(raw[12]) {
                    // Fix #7: redundant `nb != 0` removed
                    let nb = rec_buf.get(slot.null_byte).copied().unwrap_or(0xFF);
                    if nb & slot.null_mask != 0 {
                        upsert_writer.write_all(&(-1i32).to_be_bytes())?;
                    } else {
                        write_field_binary(&rec_buf, &slot.desc, upsert_writer, &mut text_buf)?;
                    }
                }
                upserts += 1;
            }
        }

        // Only store changed pages — express delete can only occur when pag_gen
        // bumped (GC rewriting a record always modifies the page).
        if rebuild_pks && c.pk_byte_len > 0 {
            let t = Instant::now();
            flat_pk_sort_dedup(&mut page_pks_now, c.pk_byte_len);
            sort_page_ns += t.elapsed().as_nanos() as u64;
            new_pks_per_page.insert(dp_n, page_pks_now);
        }
    }

    // Express-delete detection: PKs that vanished from the entire table since the
    // last snapshot must have been GC'd by Firebird (no RHD_DELETED stub on disk).
    //
    // O(N log N): build one sorted "alive" set across all changed pages
    // (new snapshot) plus all unchanged pages (previous snapshot is still truth
    // for those). Then iterate prev PKs from changed pages once; emit when
    // absent from alive. HashSet<Vec<u8>> tracks emitted-this-cycle (typically
    // small).
    // Express-delete only possible when at least one page bumped pag_gen this cycle
    // (GC rewriting always bumps gen). Zero changed pages → zero express deletes,
    // and building alive_flat would sort the whole table for nothing.
    if c.pk_byte_len > 0 && !is_first_cycle && !new_pks_per_page.is_empty() {
        let t_express = Instant::now();
        let stride = c.pk_byte_len;

        let mut alive_cap = 0usize;
        for f in new_pks_per_page.values() { alive_cap += f.len(); }
        for (n, f) in &c.prev_pks_per_page {
            if !new_pks_per_page.contains_key(n) { alive_cap += f.len(); }
        }
        let mut alive_flat: Vec<u8> = Vec::with_capacity(alive_cap);
        for f in new_pks_per_page.values() {
            alive_flat.extend_from_slice(f);
        }
        for (n, f) in &c.prev_pks_per_page {
            if !new_pks_per_page.contains_key(n) {
                alive_flat.extend_from_slice(f);
            }
        }
        let t_sort = Instant::now();
        flat_pk_sort_dedup(&mut alive_flat, stride);
        sort_alive_ns = t_sort.elapsed().as_nanos() as u64;

        let mut emitted: HashSet<Vec<u8>> = HashSet::new();
        for (&dp_n, prev_flat) in &c.prev_pks_per_page {
            // Express delete requires GC to have rewritten the page → pag_gen bumped.
            // Unchanged pages are authoritative; their PKs are still alive by definition.
            if !new_pks_per_page.contains_key(&dp_n) { continue; }
            for pk in prev_flat.chunks_exact(stride) {
                if flat_pk_contains(&alive_flat, stride, pk) { continue; }
                if !emitted.insert(pk.to_vec()) { continue; }
                if debug {
                    eprintln!("    [del express] pk_bytes={:02x?}", &pk[..pk.len().min(24)]);
                }
                delete_buf.extend_from_slice(&c.n_pk.to_be_bytes());
                delete_buf.extend_from_slice(pk);
                deletes         += 1;
                express_deletes += 1;
            }
        }
        express_ns = t_express.elapsed().as_nanos() as u64;
    }

    upsert_writer.write_all(&(-1i16).to_be_bytes())?;
    delete_buf.extend_from_slice(&(-1i16).to_be_bytes());

    Ok(ScanOutput {
        delete_buf,
        upserts, deletes, express_deletes,
        max_txn, new_gens, new_pks_per_page,
        pages_total, pages_skipped, records_checked,
        sort_page_ns, sort_alive_ns, express_ns,
    })
}

// PG application logic is inlined into sync_table — see P3 streaming refactor.

// ── Watch loop ────────────────────────────────────────────────────────────────

/// CLI entry point: local PostgreSQL `--watch`. Builds a `LocalPgSink` and runs the
/// generic watch loop — behaviour identical to before the sink refactor.
pub fn watch(args: &crate::Args) -> Result<()> {
    let db_path    = args.database.as_deref().unwrap();
    let state_path = args.state_file.as_deref()
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}.cdc.json", db_path));
    let interval   = Duration::from_secs(args.watch_interval.max(1) as u64);
    let pg_db      = args.pg_database.as_deref().unwrap();

    let tables: Vec<String> = if args.all_tables {
        let db = OdsReader::open(db_path)?;
        db.list_tables()
    } else if !args.tables.is_empty() {
        args.tables.clone()
    } else {
        vec![args.table.as_deref()
            .ok_or_else(|| anyhow::anyhow!("Provide --table, --tables, or --all-tables"))?
            .to_string()]
    };

    let mut sink = crate::sink::LocalPgSink::new(
        &args.pg_host, args.pg_port, &args.pg_user, &args.pg_password, pg_db)?;
    run_watch(db_path, &tables, &state_path, interval, args.debug, &mut sink)
}

/// Generic CDC watch loop: polls the `.fdb`, scans deltas, and applies each via the
/// provided [`DeltaSink`]. Driven locally (`LocalPgSink`) by the CLI and remotely
/// (`RemoteSink`) by `fdb-agent --watch`.
pub fn run_watch(
    db_path:    &str,
    tables:     &[String],
    state_path: &str,
    interval:   Duration,
    debug:      bool,
    sink:       &mut dyn crate::sink::DeltaSink,
) -> Result<()> {
    let state_path = state_path.to_string();
    let mut state = CdcState::load(&state_path);
    // P4: hydrate per-table PK snapshots from disk so first cycle after a
    // restart doesn't re-scan the whole DB to rebuild them.
    let mut pending_snapshots = CdcState::load_snapshots(&state_path);
    // Fix #5: track header page pag_generation instead of mtime.
    let mut last_hdr_gen: Option<u32> = None;
    let mut table_caches: HashMap<String, Option<TableCache>> = HashMap::new();

    // Fix #8: graceful shutdown — Ctrl+C sets stop flag; loop saves state before exit.
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        }).unwrap_or_else(|e| eprintln!("WARN: could not set Ctrl+C handler: {e}"));
    }

    eprintln!("Watching {} — {} table(s) — interval {}s", db_path, tables.len(), interval.as_secs());
    eprintln!("State file: {}", state_path);
    eprintln!("Watermark:  txn={}", state.last_txn);

    // Keep OdsReader (mmap) alive across cycles.
    let mut db          = OdsReader::open(db_path)?;
    let mut db_filesize = std::fs::metadata(db_path)?.len();
    let mut cycle_num   = 0u64;

    loop {
        // Fix #8: check stop flag at top of loop
        if stop.load(Ordering::Relaxed) {
            eprintln!("Shutting down — saving state…");
            state.save(&state_path)?;
            if let Err(e) = CdcState::save_snapshots(&state_path, &table_caches) {
                eprintln!("WARN: snapshot save failed: {e:#}");
            }
            return Ok(());
        }

        let fsize    = std::fs::metadata(db_path)?.len();
        let remapped = fsize != db_filesize;
        if remapped {
            db          = OdsReader::open(db_path)?;
            db_filesize = fsize;
            table_caches.clear();
        }

        // Fix #5: use header page pag_generation as change signal — reliable on all
        // filesystems (no mtime sub-second precision issue, no NFS noatime problem).
        let cur_hdr_gen = {
            let hdr = db.page(0);
            u32::from_le_bytes(hdr[4..8].try_into().unwrap())
        };
        let changed = last_hdr_gen != Some(cur_hdr_gen);
        last_hdr_gen = Some(cur_hdr_gen);

        if changed {
            cycle_num += 1;
            let t              = Instant::now();
            let mut total_ups      = 0u64;
            let mut total_del      = 0u64;
            let mut total_pages    = 0u32;
            let mut total_skipped  = 0u32;
            let mut total_setup    = 0u64;
            let mut total_copy_s   = 0u64;
            let mut total_scan_p   = 0u64;
            let mut total_copy_f   = 0u64;
            let mut total_del_cp   = 0u64;
            let mut total_del_sql  = 0u64;
            let mut total_ups_sql  = 0u64;
            let mut total_cleanup  = 0u64;
            let mut total_commit   = 0u64;
            let mut new_max        = state.last_txn;
            let mut state_dirty    = false;

            if debug {
                let hdr     = db.page(0);
                let hdr_gen = u32::from_le_bytes(hdr[ 4.. 8].try_into().unwrap());
                let oat     = u32::from_le_bytes(hdr[28..32].try_into().unwrap());
                let ost     = u32::from_le_bytes(hdr[32..36].try_into().unwrap());
                let fmb     = db_filesize as f64 / (1024.0 * 1024.0);
                eprintln!(
                    "  [dbg] cycle={cycle_num}  hdr_gen={hdr_gen}  OAT={oat}  OST={ost}  \
                     global_wm={}  fsize={fmb:.0}MB{}",
                    state.last_txn,
                    if remapped { "  [remapped]" } else { "" },
                );
            }

            for table in tables {
                let tstate = state.tables.entry(table.clone()).or_default();
                if tstate.pk_fields.is_empty() {
                    match db.read_primary_key_fields(table) {
                        Ok(pks) => { tstate.pk_fields = pks; }
                        Err(e)  => {
                            eprintln!("  WARN {table}: no PK — {e:#}");
                            continue;
                        }
                    }
                }
                let pk_fields       = tstate.pk_fields.clone();
                let per_table_txn   = tstate.last_txn;
                let table_last_txn  = per_table_txn.max(state.last_txn);

                let gens  = state.page_gens.entry(table.clone()).or_default();
                let cache = table_caches.entry(table.clone()).or_insert(None);

                // P4: hand over the loaded-from-disk snapshot the first time this
                // table's cache is created in this process. After that, the cache
                // holds the live snapshot.
                let snap_init = if cache.is_none() {
                    pending_snapshots.remove(table).map(|(_, m)| m)
                } else { None };

                match sync_table(&db, sink, table, table_last_txn, &pk_fields, gens, cache, snap_init, debug) {
                    Ok(stats) => {
                        if stats.upserts > 0 || stats.deletes > 0 {
                            eprintln!("  {:<30}  +{} ~{}  (txn {} → {})",
                                table, stats.upserts, stats.deletes,
                                table_last_txn, stats.max_txn);
                        }
                        if debug {
                            eprintln!(
                                "  [dbg] {:<28}  last_txn={}  pages {}/{} scanned ({} skipped)  \
                                 recs_checked={}  express_del={}",
                                table,
                                per_table_txn,
                                stats.pages_scanned, stats.pages_total, stats.pages_skipped,
                                stats.records_checked,
                                stats.express_deletes,
                            );
                            eprintln!(
                                "         setup={:.1}  copy_start={:.1}  scan_pure={:.1}  \
                                 copy_finish={:.1}  del_copy={:.1}  del_sql={:.1}  \
                                 upsert_sql={:.1}  cleanup={:.1}  commit={:.1}  \
                                 (sort_page={:.2}  sort_alive={:.2}  express={:.2})  [ms]",
                                stats.setup_ns       as f64 / 1e6,
                                stats.copy_start_ns  as f64 / 1e6,
                                stats.scan_pure_ns   as f64 / 1e6,
                                stats.copy_finish_ns as f64 / 1e6,
                                stats.del_copy_ns    as f64 / 1e6,
                                stats.delete_sql_ns  as f64 / 1e6,
                                stats.upsert_sql_ns  as f64 / 1e6,
                                stats.cleanup_ns     as f64 / 1e6,
                                stats.commit_ns      as f64 / 1e6,
                                stats.sort_page_ns   as f64 / 1e6,
                                stats.sort_alive_ns  as f64 / 1e6,
                                stats.express_ns     as f64 / 1e6,
                            );
                        }
                        state_dirty |= stats.pages_scanned > 0 || stats.max_txn > table_last_txn;
                        total_ups     += stats.upserts;
                        total_del     += stats.deletes;
                        total_pages   += stats.pages_total;
                        total_skipped += stats.pages_skipped;
                        total_setup   += stats.setup_ns;
                        total_copy_s  += stats.copy_start_ns;
                        total_scan_p  += stats.scan_pure_ns;
                        total_copy_f  += stats.copy_finish_ns;
                        total_del_cp  += stats.del_copy_ns;
                        total_del_sql += stats.delete_sql_ns;
                        total_ups_sql += stats.upsert_sql_ns;
                        total_cleanup += stats.cleanup_ns;
                        total_commit  += stats.commit_ns;
                        if stats.max_txn > new_max { new_max = stats.max_txn; }
                        state.tables.get_mut(table).unwrap().last_txn = stats.max_txn;
                    }
                    Err(e) => {
                        eprintln!("  ERROR {table}: {e:#}");
                        // Log reconnect failure explicitly rather than silently swallowing it.
                        if let Err(re) = sink.reconnect() {
                            eprintln!("  ERROR reconnect failed: {re:#}");
                        }
                    }
                }
            }

            if total_ups > 0 || total_del > 0 {
                state.last_txn = new_max;
                state_dirty    = true;
                eprintln!("  → {} upserts  {} deletes  {:.3}s  txn={new_max}",
                    total_ups, total_del, t.elapsed().as_secs_f64());
            }
            if debug {
                let scanned = total_pages.saturating_sub(total_skipped);
                let wall_ms = t.elapsed().as_secs_f64() * 1000.0;
                let acct = total_setup + total_copy_s + total_scan_p + total_copy_f
                         + total_del_cp + total_del_sql + total_ups_sql
                         + total_cleanup + total_commit;
                eprintln!(
                    "  [dbg] cycle total  pages {}/{} scanned ({} skipped, {:.0}%)  wall={:.1}ms  \
                     accounted={:.1}ms",
                    scanned, total_pages, total_skipped,
                    if total_pages > 0 { total_skipped as f64 / total_pages as f64 * 100.0 } else { 0.0 },
                    wall_ms, acct as f64 / 1e6,
                );
                eprintln!(
                    "  [dbg] stage totals  setup={:.0}  copy_start={:.0}  scan_pure={:.0}  \
                     copy_finish={:.0}  del_copy={:.0}  del_sql={:.0}  upsert_sql={:.0}  \
                     cleanup={:.0}  commit={:.0}  [ms]",
                    total_setup   as f64 / 1e6,
                    total_copy_s  as f64 / 1e6,
                    total_scan_p  as f64 / 1e6,
                    total_copy_f  as f64 / 1e6,
                    total_del_cp  as f64 / 1e6,
                    total_del_sql as f64 / 1e6,
                    total_ups_sql as f64 / 1e6,
                    total_cleanup as f64 / 1e6,
                    total_commit  as f64 / 1e6,
                );
            }
            if state_dirty {
                state.save(&state_path)?;
                if let Err(e) = CdcState::save_snapshots(&state_path, &table_caches) {
                    eprintln!("WARN: snapshot save failed: {e:#}");
                }
            }
        }

        // Fix #8: interruptible sleep — check stop flag every 100 ms so Ctrl+C
        // exits promptly without waiting for the full interval.
        let ticks = (interval.as_millis() as u64 / 100).max(1);
        for _ in 0..ticks {
            if stop.load(Ordering::Relaxed) { break; }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

// ── Scan unit tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod scan_tests {
    use super::*;
    use crate::ods::{self, OdsReader};

    const PAGE_SIZE: usize = 4096;
    const ODS13_ENC: u16 = 0x800D;

    // ── FDB builder ──────────────────────────────────────────────────────────

    fn write_fdb(extra_pages: Vec<Vec<u8>>) -> std::path::PathBuf {
        use std::io::Write as _;
        let path = std::env::temp_dir()
            .join(format!("fdb_scan_test_{}.fdb", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos()));
        let mut hdr = vec![0u8; PAGE_SIZE];
        hdr[16..18].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
        hdr[18..20].copy_from_slice(&ODS13_ENC.to_le_bytes());
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&hdr).unwrap();
        for pg in &extra_pages {
            let mut buf = vec![0u8; PAGE_SIZE];
            let n = pg.len().min(PAGE_SIZE);
            buf[..n].copy_from_slice(&pg[..n]);
            f.write_all(&buf).unwrap();
        }
        path
    }

    fn data_page(pag_gen: u32, records: &[Vec<u8>]) -> Vec<u8> {
        let mut pg = vec![0u8; PAGE_SIZE];
        pg[0] = ods::PAG_DATA;
        pg[4..8].copy_from_slice(&pag_gen.to_le_bytes());
        pg[22..24].copy_from_slice(&(records.len() as u16).to_le_bytes());
        let mut end = PAGE_SIZE;
        for (i, rec) in records.iter().enumerate() {
            end -= rec.len();
            let off = end as u16;
            let len = rec.len() as u16;
            pg[24 + i*4] = (off & 0xFF) as u8;
            pg[25 + i*4] = (off >> 8)   as u8;
            pg[26 + i*4] = (len & 0xFF) as u8;
            pg[27 + i*4] = (len >> 8)   as u8;
            pg[end..end + rec.len()].copy_from_slice(rec);
        }
        pg
    }

    // ── Record builders ───────────────────────────────────────────────────────

    fn sqz(data: &[u8]) -> Vec<u8> {
        assert!(data.len() <= 127);
        let mut v = vec![data.len() as u8];
        v.extend_from_slice(data);
        v
    }

    fn payload_i32(value: i32) -> Vec<u8> {
        let mut d = vec![0u8; 4];
        d.extend_from_slice(&value.to_le_bytes());
        sqz(&d)
    }

    fn live_record(txn: u32, pk: i32) -> Vec<u8> {
        let mut r = vec![0u8; 13];
        r[0..4].copy_from_slice(&txn.to_le_bytes());
        r.extend(payload_i32(pk));
        r
    }

    fn delete_stub_with_back(txn: u32, b_page: u32, b_line: u16) -> Vec<u8> {
        let mut r = vec![0u8; 13];
        r[0..4].copy_from_slice(&txn.to_le_bytes());
        r[4..8].copy_from_slice(&b_page.to_le_bytes());
        r[8..10].copy_from_slice(&b_line.to_le_bytes());
        r[10..12].copy_from_slice(&1u16.to_le_bytes());
        r.extend(sqz(&[0u8; 4]));
        r
    }

    fn delete_stub_old_txn(txn: u32) -> Vec<u8> {
        let mut r = vec![0u8; 13];
        r[0..4].copy_from_slice(&txn.to_le_bytes());
        r[10..12].copy_from_slice(&1u16.to_le_bytes());
        r.extend(sqz(&[0u8; 4]));
        r
    }

    // ── Cache builder ─────────────────────────────────────────────────────────

    /// Single DTYPE_LONG (i32) PK at decompressed offset 4.
    /// pk_byte_len = 4 (i32 length prefix) + 4 (i32 payload) = 8.
    fn make_cache(data_pages: Vec<u32>) -> TableCache {
        let desc = ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4,
                               sub_type: 0, flags: 0, offset: 4 };
        let slot = SlotInfo { desc, null_byte: 0, null_mask: 1 };
        TableCache {
            relation_id:    0,
            slots:          vec![slot],
            fmt_slots:      vec![],
            col_names:      vec!["\"id\"".to_string()],
            pk_col_names:   vec!["\"id\"".to_string()],
            n_fields:       1,
            n_pk:           1,
            pk_indices:     vec![Some(0)],
            first_pp:       None,
            pp_gen:         0,
            data_pages,
            prev_pks_per_page: HashMap::new(),
            pk_tracked:        true,
            pk_byte_len:       8, // 4-byte length prefix + 4-byte i32
        }
    }

    // ── Existing regression tests ─────────────────────────────────────────────

    #[test]
    fn delete_on_unchanged_page_detected() {
        let pag_gen = 7u32;
        let back = live_record(50, 42);
        let del  = delete_stub_with_back(101, 1, 1);
        let pg   = data_page(pag_gen, &[del, back]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, pag_gen);

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.deletes, 1, "delete stub on unchanged page must be detected");
        assert_eq!(out.upserts, 0);
    }

    #[test]
    fn upsert_ignored_on_unchanged_page() {
        let pag_gen = 5u32;
        let pg = data_page(pag_gen, &[live_record(101, 99)]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, pag_gen);

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.upserts, 0, "live record on unchanged page must not produce upsert");
        assert_eq!(out.deletes, 0);
    }

    #[test]
    fn upsert_on_changed_page_detected() {
        let pg = data_page(8, &[live_record(101, 99)]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, 7u32);

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.upserts, 1);
        assert_eq!(out.deletes, 0);
    }

    #[test]
    fn delete_and_upsert_different_pages_same_txn() {
        let back = live_record(50, 42);
        let del  = delete_stub_with_back(101, 1, 1);
        let pg1  = data_page(7, &[del, back]);
        let pg2  = data_page(8, &[live_record(101, 99)]);
        let path = write_fdb(vec![pg1, pg2]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, 7u32);
        page_gens.insert(2u32, 7u32);

        let out = scan_changes(&db, &make_cache(vec![1, 2]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.deletes, 1, "delete on unchanged page must be found");
        assert_eq!(out.upserts, 1, "upsert on changed page must be found");
    }

    #[test]
    fn delete_long_tranum_chain_follow_unchanged_page() {
        let back_rec = live_record(50, 42);
        let pg2 = data_page(6, &[back_rec]);

        let mut stub = vec![0u8; 16];
        stub[0..4].copy_from_slice(&101u32.to_le_bytes());
        stub[4..8].copy_from_slice(&2u32.to_le_bytes());
        stub[8..10].copy_from_slice(&0u16.to_le_bytes());
        stub[10..12].copy_from_slice(&(1u16 | 1024u16).to_le_bytes());
        stub.extend(sqz(&[0u8; 5]));

        let pg1 = data_page(7, &[stub]);
        let path = write_fdb(vec![pg1, pg2]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, 7u32);

        let out = scan_changes(&db, &make_cache(vec![1, 2]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.deletes, 1, "RHD_LONG_TRANUM delete stub on unchanged page must be detected");
        assert_eq!(out.upserts, 0);
    }

    #[test]
    fn old_txn_ignored() {
        let pg = data_page(9, &[delete_stub_old_txn(50)]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &HashMap::new(), false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.deletes, 0, "old-txn delete must be ignored");
        assert_eq!(out.upserts, 0);
    }

    #[test]
    fn max_txn_advances() {
        let pg = data_page(10, &[live_record(200, 1), live_record(150, 2)]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &HashMap::new(), false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.max_txn, 200);
        assert_eq!(out.upserts, 2);
    }

    #[test]
    fn express_delete_detected_via_pk_snapshot_diff() {
        let pg_initial = data_page(7, &[live_record(50, 10), live_record(50, 20)]);
        let path = write_fdb(vec![pg_initial]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut cache = make_cache(vec![1]);
        let mut page_gens = HashMap::new();

        let out1 = scan_changes(&db, &cache, 100, &page_gens, false).unwrap();
        assert_eq!(out1.deletes, 0, "first cycle must not emit deletes");
        page_gens.extend(out1.new_gens);
        cache.prev_pks_per_page = out1.new_pks_per_page;
        // flat Vec<u8>: 2 PKs × 8 bytes each = 16 bytes
        assert_eq!(cache.prev_pks_per_page.get(&1).map(|v| v.len() / 8), Some(2));

        let pg_after = data_page(8, &[live_record(50, 10)]);
        let path2 = write_fdb(vec![pg_after]);
        let db2   = OdsReader::open(path2.to_str().unwrap()).unwrap();

        let out2 = scan_changes(&db2, &cache, 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&path2).ok();

        assert_eq!(out2.express_deletes, 1, "vanished PK must be detected as express-delete");
        assert_eq!(out2.deletes, 1);
    }

    #[test]
    fn express_delete_skipped_when_pk_still_present_elsewhere() {
        let pg1 = data_page(7, &[live_record(50, 10), live_record(50, 20)]);
        let pg2 = data_page(7, &[]);
        let path = write_fdb(vec![pg1, pg2]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut cache = make_cache(vec![1, 2]);
        let mut page_gens = HashMap::new();

        let out1 = scan_changes(&db, &cache, 100, &page_gens, false).unwrap();
        page_gens.extend(out1.new_gens);
        cache.prev_pks_per_page = out1.new_pks_per_page;

        let pg1b = data_page(8, &[live_record(50, 10)]);
        let pg2b = data_page(8, &[live_record(50, 20)]);
        let path2 = write_fdb(vec![pg1b, pg2b]);
        let db2   = OdsReader::open(path2.to_str().unwrap()).unwrap();

        let out2 = scan_changes(&db2, &cache, 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&path2).ok();

        assert_eq!(out2.express_deletes, 0, "PK present on another page must not count as delete");
    }

    #[test]
    fn new_gens_only_for_changed_pages() {
        let pg1 = data_page(7, &[delete_stub_old_txn(50)]);
        let pg2 = data_page(9, &[live_record(101, 2)]);
        let path = write_fdb(vec![pg1, pg2]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, 7u32);
        page_gens.insert(2u32, 7u32);

        let out = scan_changes(&db, &make_cache(vec![1, 2]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert!(!out.new_gens.contains_key(&1), "unchanged page must not enter new_gens");
        assert!(out.new_gens.contains_key(&2),  "changed page must enter new_gens");
        assert_eq!(out.new_gens[&2], 9u32);
    }

    // ── Fix #2: multi-level back-version chain ────────────────────────────────

    /// Delete stub → intermediate RHD_CHAIN record (unpack succeeds but data too small
    /// for the PK field test, simulated by using a RHD_CHAIN record whose b_page/b_line
    /// is followed) → level-2 back version with full record.
    ///
    /// In this test we directly verify that when the first back-version slot has
    /// RHD_FRAGMENT (unpack→None) and points to a second page, the chain loop
    /// continues and resolves the PK from the second page.
    #[test]
    fn multi_level_chain_follow_through_fragment() {
        // Page 3 (page index 2 in write_fdb extra_pages): full record with pk=77
        let full_rec = live_record(50, 77);
        let pg3 = data_page(5, &[full_rec]);

        // Page 2 (page index 1): an INCOMPLETE/FRAGMENT record pointing to page 3.
        // RHD_FRAGMENT (flag 4) makes unpack_record_cdc return None, triggering depth follow.
        // b_page=3 (physical page 3 = extra_pages[2]), b_line=0.
        let mut fragment = vec![0u8; 13];
        fragment[0..4].copy_from_slice(&50u32.to_le_bytes()); // txn
        fragment[4..8].copy_from_slice(&3u32.to_le_bytes());   // b_page = 3
        fragment[8..10].copy_from_slice(&0u16.to_le_bytes());  // b_line = 0
        // RHD_CHAIN (2) | RHD_FRAGMENT (4) — CHAIN allows the loop to follow further
        fragment[10..12].copy_from_slice(&(ods::RHD_CHAIN | ods::RHD_FRAGMENT).to_le_bytes());
        fragment.extend(sqz(&[0u8; 4]));
        let pg2 = data_page(5, &[fragment]);

        // Page 1 (extra_pages[0]): delete stub pointing to page 2, slot 0.
        let del = delete_stub_with_back(101, 2, 0);
        let pg1 = data_page(7, &[del]);

        let path = write_fdb(vec![pg1, pg2, pg3]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, 7u32); // unchanged gen

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.deletes, 1,
            "delete must be resolved via 2-level chain follow through RHD_CHAIN|RHD_FRAGMENT");
        assert_eq!(out.upserts, 0);
    }

    /// Chain follow stops and skips the delete when the intermediate has neither
    /// RHD_CHAIN nor RHD_DELETED — avoids following unrelated pointers.
    #[test]
    fn chain_follow_stops_on_non_chain_flags() {
        // Page 2: record with RHD_BLOB (not a chain-type) → chain loop must abort
        let mut blob_rec = vec![0u8; 13];
        blob_rec[0..4].copy_from_slice(&50u32.to_le_bytes());
        blob_rec[10..12].copy_from_slice(&ods::RHD_BLOB.to_le_bytes()); // blob, not chain
        blob_rec.extend(sqz(&[0u8; 4]));
        let pg2 = data_page(5, &[blob_rec]);

        let del = delete_stub_with_back(101, 2, 0);
        let pg1 = data_page(7, &[del]);

        let path = write_fdb(vec![pg1, pg2]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut page_gens = HashMap::new();
        page_gens.insert(1u32, 7u32);

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path).ok();

        // Cannot resolve PK → delete is skipped, not emitted as garbage
        assert_eq!(out.deletes, 0,
            "delete with unresolvable chain must be skipped, not emitted");
    }

    // ── Fix #3: express-delete without all_pks_now global set ────────────────

    /// Verify express-delete detection works correctly when PKs span multiple pages
    /// (tests the new_pks_per_page.values().any() lookup that replaced all_pks_now).
    #[test]
    fn express_delete_multi_page_no_false_positive() {
        // 3 pages; PK 10 on pg1, PK 20 on pg2, PK 30 on pg3
        let pg1 = data_page(7, &[live_record(50, 10)]);
        let pg2 = data_page(7, &[live_record(50, 20)]);
        let pg3 = data_page(7, &[live_record(50, 30)]);
        let path = write_fdb(vec![pg1, pg2, pg3]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut cache = make_cache(vec![1, 2, 3]);
        let mut page_gens = HashMap::new();

        // Cycle 1: establish baseline
        let out1 = scan_changes(&db, &cache, 100, &page_gens, false).unwrap();
        page_gens.extend(out1.new_gens);
        cache.prev_pks_per_page = out1.new_pks_per_page;
        std::fs::remove_file(&path).ok();

        // Cycle 2: PK 20 (page 2) deleted; PKs 10 and 30 move to new pages (gen bump)
        let pg1b = data_page(8, &[live_record(50, 10)]);
        let pg2b = data_page(8, &[]);                        // PK 20 gone
        let pg3b = data_page(8, &[live_record(50, 30)]);
        let path2 = write_fdb(vec![pg1b, pg2b, pg3b]);
        let db2   = OdsReader::open(path2.to_str().unwrap()).unwrap();

        let out2 = scan_changes(&db2, &cache, 100, &page_gens, false).unwrap();
        std::fs::remove_file(&path2).ok();

        assert_eq!(out2.express_deletes, 1, "only PK 20 (truly gone) should be express-deleted");
        assert_eq!(out2.deletes, 1);
    }

    // ── Fix #5: hdr_gen change detection ─────────────────────────────────────

    /// Verify the header page pag_generation field is readable and monotonically
    /// increases or stays stable — the watch loop uses it as the change trigger.
    #[test]
    fn hdr_gen_readable_from_fdb() {
        let path = write_fdb(vec![]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let hdr = db.page(0);
        // pag_generation at hdr[4..8] — must not panic and must be a valid u32
        let gen = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        std::fs::remove_file(&path).ok();

        // The synthetic FDB has hdr_gen=0 (zeroed); just verify the field is accessible
        let _ = gen; // no assertion on value — just prove it doesn't panic/OOB
    }

    /// Two FDBs with different pag_gen values at offset 4 should be detected as changed.
    #[test]
    fn hdr_gen_change_triggers_scan() {
        use std::io::Write as _;

        // Build two FDB files that differ only in hdr pag_gen
        let make_fdb_with_gen = |gen: u32| -> std::path::PathBuf {
            let path = std::env::temp_dir()
                .join(format!("fdb_gen_test_{}.fdb",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos()));
            let mut hdr = vec![0u8; PAGE_SIZE];
            hdr[16..18].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
            hdr[18..20].copy_from_slice(&ODS13_ENC.to_le_bytes());
            hdr[4..8].copy_from_slice(&gen.to_le_bytes()); // pag_generation
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&hdr).unwrap();
            path
        };

        let path_a = make_fdb_with_gen(5);
        let path_b = make_fdb_with_gen(6);

        let db_a = OdsReader::open(path_a.to_str().unwrap()).unwrap();
        let db_b = OdsReader::open(path_b.to_str().unwrap()).unwrap();

        let gen_a = u32::from_le_bytes(db_a.page(0)[4..8].try_into().unwrap());
        let gen_b = u32::from_le_bytes(db_b.page(0)[4..8].try_into().unwrap());

        std::fs::remove_file(&path_a).ok();
        std::fs::remove_file(&path_b).ok();

        assert_ne!(gen_a, gen_b, "different hdr_gen values must produce different reads");
        // Simulates the watch() condition: changed = last_hdr_gen != Some(cur_hdr_gen)
        let mut last = None::<u32>;
        let changed_a = last != Some(gen_a); last = Some(gen_a);
        let changed_b = last != Some(gen_b); last = Some(gen_b);
        let changed_b2 = last != Some(gen_b);
        assert!(changed_a,  "first read must be changed");
        assert!(changed_b,  "different gen must be changed");
        assert!(!changed_b2, "same gen repeated must not be changed");
    }

    // ── Fix #6: Option<usize> pk_indices ─────────────────────────────────────

    /// A cache with a missing PK index (None) must set pk_tracked=false
    /// and not attempt any PK operations (no panic on usize::MAX dereference).
    #[test]
    fn missing_pk_index_not_tracked() {
        let desc = ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4,
                               sub_type: 0, flags: 0, offset: 4 };
        let slot = SlotInfo { desc, null_byte: 0, null_mask: 1 };
        let cache = TableCache {
            relation_id:    0,
            slots:          vec![slot],
            fmt_slots:      vec![],
            col_names:      vec!["\"id\"".to_string()],
            pk_col_names:   vec!["\"nonexistent\"".to_string()],
            n_fields:       1,
            n_pk:           1,
            pk_indices:     vec![None],   // field not found in schema
            first_pp:       None,
            pp_gen:         0,
            data_pages:     vec![],
            prev_pks_per_page: HashMap::new(),
            pk_tracked:        false,
            pk_byte_len:       0,   // no stride → express delete disabled
        };

        assert!(!cache.pk_tracked, "missing pk index must disable pk_tracked");

        // With pk_tracked=false, scan_changes must not attempt PK operations
        // (no usize::MAX dereference = no panic). Use a delete stub to trigger
        // the pk_tracked guard.
        let del = delete_stub_with_back(101, 1, 0);
        let pg  = data_page(9, &[del]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        // cache with non-tracked pk — pass a mutable clone for this test
        let mut c = cache;
        c.data_pages = vec![1];

        let out = scan_changes(&db, &c, 100, &HashMap::new(), false).unwrap();
        std::fs::remove_file(&path).ok();

        // No panic, no delete emitted (pk_tracked=false guard hit)
        assert_eq!(out.deletes, 0, "untracked-pk table must not emit deletes");
    }

    // ── Flat PK snapshot (memory fix) ────────────────────────────────────────

    /// pk_pgcopy_stride returns correct sizes for common numeric PK types.
    #[test]
    fn pk_stride_computed_correctly() {
        let make_slot = |dtype: u8, scale: i8| -> SlotInfo {
            SlotInfo {
                desc: ods::Desc { dtype, scale, length: 0, sub_type: 0, flags: 0, offset: 0 },
                null_byte: 0,
                null_mask: 1,
            }
        };

        let slots_i32 = vec![make_slot(ods::DTYPE_LONG, 0)];
        assert_eq!(pk_pgcopy_stride(&[Some(0)], &slots_i32), 8,  "INTEGER = 4+4");

        let slots_i64 = vec![make_slot(ods::DTYPE_INT64, 0)];
        assert_eq!(pk_pgcopy_stride(&[Some(0)], &slots_i64), 12, "INT64 = 4+8");

        let slots_date = vec![make_slot(ods::DTYPE_SQL_DATE, 0)];
        assert_eq!(pk_pgcopy_stride(&[Some(0)], &slots_date), 8, "DATE = 4+4");

        // Composite: INTEGER + INT64 = 8 + 12 = 20
        let slots_comp = vec![make_slot(ods::DTYPE_LONG, 0), make_slot(ods::DTYPE_INT64, 0)];
        assert_eq!(pk_pgcopy_stride(&[Some(0), Some(1)], &slots_comp), 20, "composite");

        // VARCHAR → variable → 0
        let slots_varchar = vec![make_slot(ods::DTYPE_VARYING, 0)];
        assert_eq!(pk_pgcopy_stride(&[Some(0)], &slots_varchar), 0, "VARCHAR = 0 (variable)");

        // Missing index → 0
        assert_eq!(pk_pgcopy_stride(&[None], &slots_i32), 0, "None index = 0");
    }

    /// flat_pk_sort_dedup: sorts entries and removes exact duplicates.
    #[test]
    fn flat_pk_sort_dedup_works() {
        let stride = 4;
        // Entries: [3, 1, 2, 1] (as 4-byte big-endian chunks for ordering)
        let mut flat: Vec<u8> = vec![];
        flat.extend_from_slice(&3u32.to_be_bytes());
        flat.extend_from_slice(&1u32.to_be_bytes());
        flat.extend_from_slice(&2u32.to_be_bytes());
        flat.extend_from_slice(&1u32.to_be_bytes()); // duplicate

        flat_pk_sort_dedup(&mut flat, stride);

        assert_eq!(flat.len() / stride, 3, "duplicate removed");
        let vals: Vec<u32> = flat.chunks_exact(stride)
            .map(|c| u32::from_be_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(vals, vec![1, 2, 3], "sorted ascending");
    }

    /// flat_pk_contains: binary search finds present and rejects absent entries.
    #[test]
    fn flat_pk_contains_binary_search() {
        let stride = 4;
        let mut flat: Vec<u8> = vec![];
        for v in [10u32, 20, 30, 40, 50] {
            flat.extend_from_slice(&v.to_be_bytes());
        }
        // already sorted

        assert!( flat_pk_contains(&flat, stride, &20u32.to_be_bytes()), "present");
        assert!( flat_pk_contains(&flat, stride, &10u32.to_be_bytes()), "first");
        assert!( flat_pk_contains(&flat, stride, &50u32.to_be_bytes()), "last");
        assert!(!flat_pk_contains(&flat, stride, &15u32.to_be_bytes()), "absent");
        assert!(!flat_pk_contains(&flat, stride, &0u32.to_be_bytes()),  "before first");
        assert!(!flat_pk_contains(&flat, stride, &99u32.to_be_bytes()), "after last");
        assert!(!flat_pk_contains(&[], stride, &10u32.to_be_bytes()),   "empty flat");
    }

    /// Memory layout: a table with N rows should produce exactly N×stride bytes
    /// in the snapshot, not N×(24+stride) like HashSet<Vec<u8>> would.
    #[test]
    fn snapshot_memory_is_flat_not_per_vec() {
        let n_records: usize = 100; // 100 × 22 bytes/record < 4096-byte page
        let stride = 8; // DTYPE_LONG PK encoding

        // Build a page with n_records live records
        let records: Vec<Vec<u8>> = (1..=n_records as i32)
            .map(|pk| live_record(50, pk))
            .collect();
        let pg = data_page(5, &records);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let out = scan_changes(&db, &make_cache(vec![1]), 100, &HashMap::new(), false).unwrap();
        std::fs::remove_file(&path).ok();

        let snapshot = out.new_pks_per_page.get(&1).expect("snapshot must exist");
        assert_eq!(snapshot.len(), n_records * stride,
            "snapshot must be exactly n_records×stride bytes (flat), got {} bytes for {} records",
            snapshot.len(), n_records);
    }

    // ── Fix #7: null-mask check ───────────────────────────────────────────────

    /// Null bitmap byte = 0x00 means all fields non-null.
    /// The old `nb != 0 && nb & mask != 0` and new `nb & mask != 0` behave identically
    /// for nb=0 (both false). Verify field is encoded, not NULL-sentinel.
    #[test]
    fn null_mask_zero_byte_encodes_value_not_null() {
        let mut rec_buf = vec![0u8; 8]; // 4-byte null bitmap (all 0) + 4-byte i32
        let value: i32 = 42;
        rec_buf[4..8].copy_from_slice(&value.to_le_bytes());

        let desc = ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4,
                               sub_type: 0, flags: 0, offset: 4 };
        let slot = SlotInfo { desc: desc.clone(), null_byte: 0, null_mask: 1 };

        // Fix #7 logic: `nb & slot.null_mask != 0` → false → encode value
        let nb = rec_buf.get(slot.null_byte).copied().unwrap_or(0xFF);
        assert_eq!(nb, 0u8);
        assert!(nb & slot.null_mask == 0, "null_byte=0 must not trigger null sentinel");

        let mut out = Vec::new();
        let mut text_buf = Vec::new();
        write_field_binary(&rec_buf, &desc, &mut out, &mut text_buf).unwrap();

        // PGCOPY i32: 4-byte length + 4-byte value
        assert_eq!(out.len(), 8);
        let encoded = i32::from_be_bytes(out[4..8].try_into().unwrap());
        assert_eq!(encoded, value);
    }

    // ── Per-record format selection (CDC) ─────────────────────────────────────
    //
    // Records stamp their on-disk format in rhd_format (raw[12]). Rows written before
    // a later ALTER TABLE keep the old layout; decoding them with the latest format
    // reads wrong offsets → corrupt upsert values / wrong delete PKs. CDC must pick
    // slots per record via TableCache::slots_for(raw[12]).

    #[test]
    fn build_slots_marks_columns_added_after_format_as_null() {
        // Current schema: field ids 0,1,5. The record's format only knew ids 0..3, so
        // the column with id 5 must decode as a zero descriptor (→ NULL), not a wrong
        // offset read.
        let field_order = vec![
            (0usize, "a".to_string()),
            (1, "b".to_string()),
            (5, "added_later".to_string()),
        ];
        let descs = vec![
            ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4, sub_type: 0, flags: 0, offset: 4 },
            ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4, sub_type: 0, flags: 0, offset: 8 },
            ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4, sub_type: 0, flags: 0, offset: 12 },
        ];
        let slots = build_slots(&field_order, &descs);
        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].desc.dtype, ods::DTYPE_LONG);
        assert_eq!(slots[2].desc.dtype, 0, "absent column → zero desc → write_field_binary NULL");
    }

    #[test]
    fn scan_upsert_decodes_with_record_format_not_latest() {
        // Build a cache whose LATEST `slots` deliberately point the column at a bogus
        // offset (99, beyond the record). The record is written in format 0, for which
        // fmt_slots[0] holds the CORRECT offset (4). If scan honours rhd_format the
        // upsert carries 42; if it used the latest `slots` it would emit NULL.
        let good = SlotInfo {
            desc: ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4, sub_type: 0, flags: 0, offset: 4 },
            null_byte: 0, null_mask: 1,
        };
        let bogus = SlotInfo {
            desc: ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4, sub_type: 0, flags: 0, offset: 99 },
            null_byte: 0, null_mask: 1,
        };
        let mut fmt_slots: Vec<Option<Vec<SlotInfo>>> = (0..256).map(|_| None).collect();
        fmt_slots[0] = Some(vec![good]);

        let cache = TableCache {
            relation_id: 0,
            slots: vec![bogus],          // latest format = wrong offset
            fmt_slots,
            col_names: vec!["\"id\"".to_string()],
            pk_col_names: vec!["\"id\"".to_string()],
            n_fields: 1, n_pk: 1,
            pk_indices: vec![Some(0)],
            first_pp: None, pp_gen: 0,
            data_pages: vec![1],
            prev_pks_per_page: HashMap::new(),
            pk_tracked: true, pk_byte_len: 8,
        };

        // One live record (format byte 0), pk=42, on a changed page → upsert.
        let pag_gen = 5u32;
        let pg   = data_page(pag_gen, &[live_record(200, 42)]);
        let path = write_fdb(vec![pg]);
        let db   = OdsReader::open(path.to_str().unwrap()).unwrap();

        let mut upserts = Vec::<u8>::new();
        let page_gens = HashMap::new(); // unseen page → page_changed → upsert
        let out = scan_changes_into(&db, &cache, 100, &page_gens, &mut upserts, false).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(out.upserts, 1, "changed page with a new txn must emit one upsert");
        // PGCOPY row: [i16 field count][i32 len][payload]. 42 BE = 00 00 00 2A.
        let needle = 42i32.to_be_bytes();
        assert!(upserts.windows(4).any(|w| w == needle),
            "upsert must decode pk via fmt_slots[0] (offset 4 → 42), not latest slots (offset 99 → NULL)");
    }
}
