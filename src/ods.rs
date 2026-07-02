//! Direct Firebird ODS reader — no server, no fbclient.dll.
//!
//! ODS reference: src/jrd/ods.h
//! RLE compress:  src/jrd/sqz.cpp
//! Field align:   src/jrd/met.epp  MET_align()
//! NULL bitmap:   src/jrd/val.h    FLAG_BYTES(n)
//!
//! System relation IDs (relations.h, stable since ODS 8):
//!   0  RDB$PAGES          fields: page_num(i32), relation_id(i16), seq(i32), type(i16)
//!   2  RDB$FIELDS
//!   5  RDB$RELATION_FIELDS
//!   6  RDB$RELATIONS
//!   8  RDB$FORMATS        fields: rid(i16), format(i16), desc_blob(8 bytes)

use anyhow::{bail, Result};
use memmap2::MmapOptions;
use std::fs::File;

pub const PAG_DATA   : u8 = 5;
pub const PAG_POINTER: u8 = 4;

// rhd_flags
pub const RHD_DELETED    : u16 = 1;
pub const RHD_CHAIN      : u16 = 2;
pub const RHD_FRAGMENT   : u16 = 4;
pub const RHD_INCOMPLETE : u16 = 8;
pub const RHD_BLOB       : u16 = 16;
pub const RHD_LONG_TRANUM: u16 = 1024;
pub const RHD_NOT_PACKED : u16 = 2048;

// dtype constants — from src/include/firebird/impl/dsc_pub.h
pub const DTYPE_TEXT        : u8 = 1;
pub const DTYPE_CSTRING     : u8 = 2;
pub const DTYPE_VARYING     : u8 = 3;
pub const DTYPE_SHORT       : u8 = 8;
pub const DTYPE_LONG        : u8 = 9;
pub const DTYPE_QUAD        : u8 = 10;  // legacy 8-byte int
pub const DTYPE_REAL        : u8 = 11;
pub const DTYPE_DOUBLE      : u8 = 12;
pub const DTYPE_D_FLOAT     : u8 = 13;
pub const DTYPE_SQL_DATE    : u8 = 14;
pub const DTYPE_SQL_TIME    : u8 = 15;
pub const DTYPE_TIMESTAMP   : u8 = 16;
#[allow(dead_code)]
pub const DTYPE_BLOB        : u8 = 17;
#[allow(dead_code)]
pub const DTYPE_ARRAY       : u8 = 18;
pub const DTYPE_INT64       : u8 = 19;
pub const DTYPE_DBKEY       : u8 = 20;
pub const DTYPE_BOOLEAN     : u8 = 21;
#[allow(dead_code)]
pub const DTYPE_DEC64       : u8 = 22;
#[allow(dead_code)]
pub const DTYPE_DEC128      : u8 = 23;
#[allow(dead_code)]
pub const DTYPE_INT128      : u8 = 24;
pub const DTYPE_SQL_TIME_TZ : u8 = 25;
pub const DTYPE_TIMESTAMP_TZ: u8 = 26;

// ── Disk descriptor: ods.h Ods::Descriptor (12 bytes) ────────────────────────
#[derive(Debug, Clone)]
pub struct Desc {
    pub dtype   : u8,
    pub scale   : i8,
    pub length  : u16,
    #[allow(dead_code)]
    pub sub_type: i16,
    #[allow(dead_code)]
    pub flags   : u16,
    pub offset  : u32,
}

impl Desc {
    pub fn from_bytes(b: &[u8]) -> Self {
        assert!(b.len() >= 12);
        Self {
            dtype:    b[0],
            scale:    b[1] as i8,
            length:   u16::from_le_bytes([b[2], b[3]]),
            sub_type: i16::from_le_bytes([b[4], b[5]]),
            flags:    u16::from_le_bytes([b[6], b[7]]),
            offset:   u32::from_le_bytes([b[8], b[9], b[10], b[11]]),
        }
    }

    pub fn default_zero() -> Self {
        Self { dtype: 0, scale: 0, length: 0, sub_type: 0, flags: 0, offset: 0 }
    }
}

// ── ODS reader ────────────────────────────────────────────────────────────────

pub struct OdsReader {
    // Memory-mapped — OS pages in only what's needed, no full-RAM load
    mmap         : memmap2::Mmap,
    pub page_size: usize,
    pub ods_ver  : u16,
}

impl OdsReader {
    pub fn open(path: &str) -> Result<Self> {
        let f    = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&f)? };

        if mmap.len() < 256 {
            bail!("File too small");
        }

        // hdr_page_size at offset 16 (USHORT)
        let page_size = u16::from_le_bytes([mmap[16], mmap[17]]) as usize;

        // hdr_ods_version at offset 18 (USHORT)
        // Stored as: ODS_MAJOR | ODS_FIREBIRD_FLAG (0x8000)
        // e.g. FB5 (ODS 13) = 13 | 0x8000 = 0x800D
        // e.g. FB6 (ODS 14) = 14 | 0x8000 = 0x800E
        let ods_enc = u16::from_le_bytes([mmap[18], mmap[19]]);
        let ods_ver = (ods_enc & 0x7FFF) as u16;

        if !(1024..=65536).contains(&page_size) {
            bail!("Invalid page size {page_size}");
        }
        if ods_ver < 10 || ods_ver > 100 {
            bail!("ODS {ods_ver} (raw 0x{ods_enc:04X}) not recognised — is this a Firebird .fdb file?");
        }
        if ods_ver < 12 {
            bail!("ODS {ods_ver} not supported (need Firebird 3+ / ODS 12+)");
        }

        Ok(Self { mmap, page_size, ods_ver })
    }

    #[inline]
    pub fn page(&self, n: u32) -> &[u8] {
        let off = n as usize * self.page_size;
        &self.mmap[off..off + self.page_size]
    }

    /// hdr_PAGES: first pointer page of RDB$PAGES.
    ///
    /// Struct layout change between ODS versions:
    ///   ODS 13 (FB4/5): hdr_PAGES at offset 20 (no hdr_ods_minor field)
    ///   ODS 14 (FB6):   hdr_ods_minor at offset 20, hdr_PAGES at offset 28
    pub fn hdr_pages(&self) -> u32 {
        let h = self.page(0);
        let offset = if self.ods_ver >= 14 { 28 } else { 20 };
        u32::from_le_bytes(h[offset..offset+4].try_into().unwrap())
    }

    /// Walk pointer page chain for a relation, collecting non-empty data page numbers.
    ///
    /// pointer_page (ods.h):
    ///   +16: ppg_sequence u32
    ///   +20: ppg_next     u32   (0 = last)
    ///   +24: ppg_count    u16
    ///   +26: ppg_relation u16
    ///   +32: ppg_page[]   u32 array (count entries)
    pub fn data_pages_for(&self, first_pp: u32) -> Vec<u32> {
        let mut pages = Vec::new();
        let mut pp    = first_pp;

        while pp != 0 {
            let p = self.page(pp);
            if p[0] != PAG_POINTER { break; }

            let count = u16::from_le_bytes([p[24], p[25]]) as usize;
            let next  = u32::from_le_bytes(p[20..24].try_into().unwrap());

            for i in 0..count {
                let dp = u32::from_le_bytes(p[32 + i*4..36 + i*4].try_into().unwrap());
                if dp == 0 { continue; }
                pages.push(dp);
            }

            if p[1] & 0x01 != 0 || next == 0 { break; }
            pp = next;
        }
        // Sort by physical page number → sequential disk access (critical for HDD/cold cache)
        pages.sort_unstable();
        pages
    }

    /// Scan RDB$PAGES to find the first pointer page for a given relation_id.
    ///
    /// RDB$PAGES hard-coded format (all non-nullable):
    ///   null_bitmap : 4 bytes at 0
    ///   page_num    : i32 at 4
    ///   relation_id : i16 at 8
    ///   pad         : 2 bytes
    ///   seq         : i32 at 12
    ///   page_type   : i16 at 16
    pub fn find_first_pp(&self, relation_id: u16) -> Option<u32> {
        let pp0 = self.hdr_pages();
        let dps = self.data_pages_for(pp0);

        for dp_n in dps {
            let dp    = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let count = u16::from_le_bytes([dp[22], dp[23]]) as usize;

            for s in 0..count {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }

                if let Some(rec) = self.unpack_record(&dp[off..off+len]) {
                    if rec.len() < 18 { continue; }
                    let rid   = i16::from_le_bytes([rec[8],  rec[9]])  as u16;
                    let ptype = i16::from_le_bytes([rec[16], rec[17]]);
                    let seq   = i32::from_le_bytes(rec[12..16].try_into().unwrap());
                    if rid == relation_id && ptype == 4 && seq == 0 {
                        let pn = i32::from_le_bytes(rec[4..8].try_into().unwrap()) as u32;
                        return Some(pn);
                    }
                }
            }
        }
        None
    }

    /// Find relation_id for a user table by scanning RDB$RELATIONS (relation 6).
    ///
    /// Hard-coded field offsets from MET_align() rules (met.epp) and FLAG_BYTES:
    ///   null_bitmap = 4 bytes; f_rel_id (i16) at 32; f_rel_name (char) at 42.
    ///   MAX_SQL_IDENTIFIER_LEN: 63 bytes ODS ≤12, 252 bytes ODS 13+.
    pub fn find_relation_id(&self, table: &str) -> Result<u16> {
        let pp = self.find_first_pp(6)
            .ok_or_else(|| anyhow::anyhow!("Cannot find RDB$RELATIONS pointer page"))?;
        let dps = self.data_pages_for(pp);
        let target = table.to_uppercase();

        let name_len: usize = if self.ods_ver >= 13 { 252 } else { 63 };
        const ID_OFF  : usize = 32;
        const NAME_OFF: usize = 42;

        for dp_n in dps {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;

            for s in 0..cnt {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }

                if let Some(rec) = self.unpack_record_meta(&dp[off..off+len]) {
                    if rec.len() < NAME_OFF + name_len { continue; }
                    if rec[1] & 0x01 != 0 { continue; }
                    let name = std::str::from_utf8(&rec[NAME_OFF..NAME_OFF + name_len])
                        .unwrap_or("").trim_end();
                    if name.eq_ignore_ascii_case(&target) {
                        if rec.len() < ID_OFF + 2 { continue; }
                        let rid = i16::from_le_bytes([rec[ID_OFF], rec[ID_OFF+1]]) as u16;
                        return Ok(rid);
                    }
                }
            }
        }
        bail!("Table '{}' not found in RDB$RELATIONS", table)
    }

    /// List all user table names (RDB$SYSTEM_FLAG = 0).
    pub fn list_tables(&self) -> Vec<String> {
        let name_len: usize = if self.ods_ver >= 13 { 252 } else { 63 };
        const NAME_OFF: usize = 42;
        const SYS_OFF : usize = 34;

        let Some(pp) = self.find_first_pp(6) else { return vec![]; };
        let dps = self.data_pages_for(pp);
        let mut tables = Vec::new();

        for dp_n in dps {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            for s in 0..cnt {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }
                if let Some(rec) = self.unpack_record_meta(&dp[off..off+len]) {
                    if rec.len() < NAME_OFF + name_len { continue; }
                    if rec[1] & 0x01 != 0 { continue; }
                    // Skip views: field 0 = RDB$VIEW_BLR. Null bit clear ⇒ view_blr
                    // present ⇒ relation is a VIEW, not a base table.
                    if rec[0] & 0x01 == 0 { continue; }
                    if rec.len() >= SYS_OFF + 2 {
                        let sys = i16::from_le_bytes([rec[SYS_OFF], rec[SYS_OFF+1]]);
                        if sys != 0 { continue; }
                    }
                    let name = std::str::from_utf8(&rec[NAME_OFF..NAME_OFF+name_len])
                        .unwrap_or("").trim_end_matches('\0').trim().to_string();
                    if !name.is_empty() { tables.push(name); }
                }
            }
        }
        tables.sort();
        tables
    }

    /// Read format descriptor from RDB$FORMATS (relation 8).
    ///
    /// RDB$FORMATS layout (non-nullable):
    ///   null_bitmap(4) | rid(i16 at 4) | format_ver(i16 at 6) | desc_blob(8 bytes at 8)
    ///
    /// The blob contains N × 12-byte Ods::Descriptor entries.
    ///
    /// BID layout (RecordNumber.h, 8 bytes LE):
    ///   [0-1] bid_relation_id  [2] reserved  [3] bid_number_up  [4-7] bid_number
    ///   record_number = (bid_number_up << 32) | bid_number
    ///   dpg_sequence  = rec_num / max_rec_per_page
    pub fn read_format(&self, relation_id: u16, fmt_ver: u16) -> Result<Vec<Desc>> {
        let mut entries = self.scan_formats(relation_id);
        if entries.is_empty() {
            bail!("No format entries found for relation {relation_id}");
        }
        entries.sort_by_key(|(v, _)| *v);

        let bid = if fmt_ver == u16::MAX {
            entries.last().unwrap().1
        } else {
            entries.iter().find(|(v, _)| *v == fmt_ver)
                .map(|(_, b)| *b)
                .ok_or_else(|| anyhow::anyhow!(
                    "Format ver {fmt_ver} not found for relation {relation_id}"
                ))?
        };

        let blob_data = self.read_blob_from_bid(&bid)?;
        // Format blob: [0..2] count u16 LE | [2..2+count*12] N×Descriptor
        if blob_data.len() < 2 {
            bail!("Format blob too short: {} bytes", blob_data.len());
        }
        let count  = u16::from_le_bytes([blob_data[0], blob_data[1]]) as usize;
        let needed = 2 + count * 12;
        if blob_data.len() < needed {
            bail!("Format blob length {} < needed {} (count={})", blob_data.len(), needed, count);
        }
        Ok(blob_data[2..needed].chunks_exact(12).map(Desc::from_bytes).collect())
    }

    /// Read ordered field names from RDB$RELATION_FIELDS (relation 5).
    ///
    /// ODS 13 layout (name_len = 252):
    ///   null_bitmap(4) | fname[0](252) | rname[1](252) | fields 2-4(252 each)
    ///   | estring[5] varying(127) | position[6] short | qheader[7] blob(8)
    ///   | flag[8] short | id[9] short (RDB$FIELD_ID)
    pub fn read_field_names(&self, relation_id: u16, table_name: &str) -> Result<Vec<(usize, String)>> {
        let name_len  = if self.ods_ver >= 13 { 252usize } else { 63 };
        let fname_off = 4usize;
        let rname_off = fname_off + name_len;
        let field4_end = rname_off + name_len * 4;
        let f5_start  = (field4_end + 1) & !1;
        let f5_end    = f5_start + 2 + 127;
        let pos_off   = (f5_end + 1) & !1;
        let f7_start  = ((pos_off + 2) + 7) & !7;
        let f8_off    = f7_start + 8;
        let id_off    = f8_off + 2;

        let target = table_name.to_uppercase();
        let pp = self.find_first_pp(5)
            .ok_or_else(|| anyhow::anyhow!("No pointer page for RDB$RELATION_FIELDS"))?;

        let mut entries: Vec<(i16, usize, String)> = Vec::new();

        for rec in self.records_meta(pp) {
            let null0 = if rec.is_empty() { 0u8 } else { rec[0] };

            if null0 & 0x02 != 0 { continue; }
            if rname_off + name_len > rec.len() { continue; }
            let rname = std::str::from_utf8(&rec[rname_off..rname_off+name_len])
                .unwrap_or("").trim_end_matches('\0').trim();
            if rname.to_uppercase() != target { continue; }

            if null0 & 0x01 != 0 { continue; }
            if fname_off + name_len > rec.len() { continue; }
            let fname = std::str::from_utf8(&rec[fname_off..fname_off+name_len])
                .unwrap_or("").trim_end_matches('\0').trim().to_string();
            if fname.is_empty() { continue; }

            if id_off + 2 > rec.len() { continue; }
            let fid = i16::from_le_bytes([rec[id_off], rec[id_off+1]]) as usize;
            let pos: i16 = if pos_off + 2 <= rec.len() && (null0 & 0x40 == 0) {
                i16::from_le_bytes([rec[pos_off], rec[pos_off+1]])
            } else { fid as i16 };

            entries.push((pos, fid, fname));
        }

        if entries.is_empty() {
            bail!("No fields found for relation {relation_id} ({table_name})");
        }
        entries.sort_by_key(|(pos, _, _)| *pos);
        Ok(entries.into_iter().map(|(_, fid, name)| (fid, name)).collect())
    }

    // ── Record unpacking ──────────────────────────────────────────────────────

    /// Unpack record: parse rhd/rhde header, RLE decompress if needed.
    pub fn unpack_record_ex(&self, raw: &[u8], allow_chain: bool) -> Option<Vec<u8>> {
        if raw.len() < 13 { return None; }
        let flags = u16::from_le_bytes([raw[10], raw[11]]);
        let mut skip = RHD_DELETED | RHD_FRAGMENT | RHD_INCOMPLETE | RHD_BLOB;
        if !allow_chain { skip |= RHD_CHAIN; }
        if flags & skip != 0 { return None; }
        let data_start = if flags & RHD_LONG_TRANUM != 0 { 16 } else { 13 };
        if raw.len() <= data_start { return None; }
        let compressed = &raw[data_start..];
        if flags & RHD_NOT_PACKED != 0 {
            Some(compressed.to_vec())
        } else {
            crate::sqz::decompress(compressed)
        }
    }

    /// Unpack for metadata scanning: accepts head fragments and chain records.
    ///
    /// Large records (e.g. RDB$RELATIONS rows with many 252-byte identifier columns)
    /// don't fit a single data-page slot and are split into fragments. The head slot
    /// carries `rhd_incomplete`; reassemble it via `reassemble_fragments`. Continuation
    /// slots carry `rhd_fragment` and are skipped (reached only through the head's chain).
    ///
    /// `raw` must be a slice borrowed from this reader's mmap (the caller passes
    /// `&self.page(n)[..]`) so fragment slices on other pages share its lifetime.
    pub fn unpack_record_meta<'a>(&'a self, raw: &'a [u8]) -> Option<Vec<u8>> {
        if raw.len() < 13 { return None; }
        let flags = u16::from_le_bytes([raw[10], raw[11]]);
        let skip = RHD_DELETED | RHD_FRAGMENT | RHD_BLOB;
        if flags & skip != 0 { return None; }
        if flags & RHD_INCOMPLETE != 0 {
            return self.reassemble_fragments(raw);
        }
        let data_start = if flags & RHD_LONG_TRANUM != 0 { 16 } else { 13 };
        if raw.len() <= data_start { return None; }
        let compressed = &raw[data_start..];
        if flags & RHD_NOT_PACKED != 0 {
            Some(compressed.to_vec())
        } else {
            crate::sqz::decompress(compressed)
        }
    }

    /// Reassemble a fragmented record starting from its head slot.
    ///
    /// Mirrors `jrd/vio.cpp` VIO_data: each fragment is an independent SQZ stream and
    /// the decompressed pieces are concatenated. Header sizes follow `dpm.epp get_header`:
    ///   - an `rhd_incomplete` fragment uses the rhdf header → data at RHDF_SIZE (22),
    ///     next fragment at f_page (bytes 16..20) / f_line (bytes 20..22);
    ///   - the final fragment (`rhd_fragment`, not incomplete) uses the plain header
    ///     (16 with rhd_long_tranum, else 13).
    pub fn reassemble_fragments<'a>(&'a self, head: &'a [u8]) -> Option<Vec<u8>> {
        const RHDF_SIZE: usize = 22;
        let mut out = Vec::new();
        let mut cur: &[u8] = head;

        for _ in 0..4096 {              // guard against a corrupt cyclic chain
            if cur.len() < 13 { return None; }
            let fl = u16::from_le_bytes([cur[10], cur[11]]);
            let incomplete = fl & RHD_INCOMPLETE != 0;
            let hsize = if incomplete { RHDF_SIZE }
                        else if fl & RHD_LONG_TRANUM != 0 { 16 } else { 13 };
            if cur.len() <= hsize { return None; }
            let comp = &cur[hsize..];
            if fl & RHD_NOT_PACKED != 0 {
                out.extend_from_slice(comp);
            } else {
                out.extend_from_slice(&crate::sqz::decompress(comp)?);
            }

            if !incomplete { return Some(out); }

            let f_page = u32::from_le_bytes(cur[16..20].try_into().unwrap());
            let f_line = u16::from_le_bytes([cur[20], cur[21]]) as usize;
            cur = self.read_slot_raw(f_page, f_line)?;
        }
        None
    }

    pub fn unpack_record(&self, raw: &[u8]) -> Option<Vec<u8>> {
        self.unpack_record_ex(raw, false)
    }

    pub fn records_meta(&self, first_pp: u32) -> impl Iterator<Item = Vec<u8>> + '_ {
        self.data_pages_for(first_pp).into_iter().flat_map(move |dp_n| {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { return vec![]; }
            let count = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            let mut recs = Vec::new();
            for s in 0..count {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }
                if let Some(r) = self.unpack_record_meta(&dp[off..off+len]) {
                    recs.push(r);
                }
            }
            recs
        })
    }

    // ── Blob reading ──────────────────────────────────────────────────────────

    fn resolve_bid(&self, bid: &[u8; 8]) -> (u16, usize, usize) {
        let rel_id  = u16::from_le_bytes([bid[0], bid[1]]);
        let num_up  = bid[3] as u64;
        let num_lo  = u32::from_le_bytes([bid[4], bid[5], bid[6], bid[7]]) as u64;
        let rec_num = ((num_up << 32) | num_lo) as usize;
        // maxRecsPerDP = (page_size - sizeof(data_page)) / (sizeof(dpg_repeat) + RHD_SIZE)
        //              = (page_size - 28) / 17
        let max_rec = (self.page_size - 28) / 17;
        (rel_id, rec_num / max_rec, rec_num % max_rec)
    }

    fn read_blob_from_bid(&self, bid: &[u8; 8]) -> Result<Vec<u8>> {
        let (rel_id, target_seq, target_slot) = self.resolve_bid(bid);

        let pp  = self.find_first_pp(rel_id)
            .ok_or_else(|| anyhow::anyhow!("No pointer page for blob relation {rel_id}"))?;
        let dps = self.data_pages_for(pp);

        for dp_n in dps {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let seq = u32::from_le_bytes(dp[16..20].try_into().unwrap()) as usize;
            if seq != target_seq { continue; }

            let slot_count = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            if target_slot >= slot_count { continue; }

            let off = u16::from_le_bytes([dp[24+target_slot*4], dp[25+target_slot*4]]) as usize;
            let len = u16::from_le_bytes([dp[26+target_slot*4], dp[27+target_slot*4]]) as usize;
            if off == 0 || len == 0 || off + len > self.page_size { continue; }

            let raw = &dp[off..off+len];
            if raw.len() < 32 { continue; }

            let flags = u16::from_le_bytes([raw[10], raw[11]]);
            if flags & RHD_BLOB == 0 { continue; }

            // blh struct layout (ods.h) — changed between ODS 13 and 14:
            //
            // ODS 13 (FB5):                    ODS 14 (FB6):
            //   +0:  blh_lead_page u32            +0:  blh_lead_page u32
            //   +4:  blh_max_seq   u32            +4:  blh_max_seq   u32
            //   +8:  blh_max_seg   u16            +8:  blh_max_seg   u16
            //   +10: blh_flags     u16            +10: blh_flags     u16
            //   +12: blh_level     u8  ←          +12: blh_count     u32
            //   +16: blh_count     u32            +16: blh_length    u64
            //   +20: blh_length    u32 ←          +24: blh_sub_type  u16
            //   +24: blh_sub_type  u16            +26: blh_charset   u8
            //   +26: blh_charset   u8             +27: blh_level     u8  ←
            //   +28: blh_page[]                   +28: blh_page[]
            let blh = raw;
            let (blh_lead_page, blh_max_seq, blh_level, blh_length, blh_count) =
                if self.ods_ver >= 14 {
                    let lead  = u32::from_le_bytes(blh[0..4].try_into().unwrap());
                    let mseq  = u32::from_le_bytes(blh[4..8].try_into().unwrap());
                    let level = blh[27];
                    let len   = u64::from_le_bytes(blh[16..24].try_into().unwrap()) as usize;
                    let cnt   = u32::from_le_bytes(blh[12..16].try_into().unwrap());
                    (lead, mseq, level, len, cnt)
                } else {
                    let lead  = u32::from_le_bytes(blh[0..4].try_into().unwrap());
                    let mseq  = u32::from_le_bytes(blh[4..8].try_into().unwrap());
                    let level = blh[12];
                    let len   = u32::from_le_bytes(blh[20..24].try_into().unwrap()) as usize;
                    let cnt   = u32::from_le_bytes(blh[16..20].try_into().unwrap());
                    (lead, mseq, level, len, cnt)
                };
            let stream_blob = flags & 0x0020 != 0;

            return if blh_length == 0 {
                Ok(vec![])
            } else if blh_max_seq == 0 {
                if stream_blob {
                    if blh.len() < 28 + blh_length {
                        bail!("Stream blob truncated: have {}, need {}", blh.len(), 28 + blh_length);
                    }
                    Ok(blh[28..28 + blh_length].to_vec())
                } else {
                    let mut data = Vec::with_capacity(blh_length);
                    let mut pos = 28usize;
                    for _ in 0..blh_count {
                        if pos + 2 > blh.len() { break; }
                        let seg_len = u16::from_le_bytes([blh[pos], blh[pos+1]]) as usize;
                        pos += 2;
                        if pos + seg_len > blh.len() { break; }
                        data.extend_from_slice(&blh[pos..pos+seg_len]);
                        pos += seg_len;
                    }
                    Ok(data)
                }
            } else if blh_level == 0 {
                let mut data = Vec::with_capacity(blh_length);
                for i in 0..(blh_max_seq as usize).min(1000) {
                    if blh.len() < 28 + (i+1)*4 { break; }
                    let pg = u32::from_le_bytes(blh[28+i*4..32+i*4].try_into().unwrap());
                    if pg == 0 { break; }
                    data.extend_from_slice(&self.read_blob_page_raw(pg));
                }
                data.truncate(blh_length);
                Ok(data)
            } else if blh_level == 1 {
                let ptr_page = u32::from_le_bytes(blh[28..32].try_into().unwrap());
                let pp = self.page(ptr_page);
                let n_pages = ((blh_length + self.page_size - 1) / self.page_size).max(1);
                let mut data = Vec::with_capacity(blh_length);
                for i in 0..n_pages.min(1000) {
                    let off = 4 + i * 4;
                    if off + 4 > self.page_size { break; }
                    let pg = u32::from_le_bytes(pp[off..off+4].try_into().unwrap());
                    if pg == 0 { break; }
                    data.extend_from_slice(&self.read_blob_page_raw(pg));
                }
                data.truncate(blh_length);
                Ok(data)
            } else {
                self.read_blob_page(blh_lead_page, blh_length)
            };
        }
        bail!("Blob header not found at seq={target_seq} slot={target_slot} rel={rel_id}")
    }

    fn read_blob_page(&self, page_num: u32, length: usize) -> Result<Vec<u8>> {
        if page_num as usize * self.page_size + self.page_size > self.mmap.len() {
            bail!("Blob page {page_num} out of file bounds");
        }
        let bp = self.page(page_num);
        if bp[0] != 8 {
            bail!("Expected pag_blob(8) at page {page_num}, got {}", bp[0]);
        }
        let raw = self.read_blob_page_raw(page_num);
        Ok(raw[..raw.len().min(length)].to_vec())
    }

    fn read_blob_page_raw(&self, page_num: u32) -> Vec<u8> {
        let bp = self.page(page_num);
        // blob_page: +24: blp_length u16, +28: data
        let dlen = u16::from_le_bytes([bp[24], bp[25]]) as usize;
        bp[28..28 + dlen.min(self.page_size - 28)].to_vec()
    }

    fn scan_formats(&self, relation_id: u16) -> Vec<(u16, [u8; 8])> {
        let Some(pp) = self.find_first_pp(8) else { return vec![]; };
        let dps = self.data_pages_for(pp);
        let mut found = Vec::new();
        for dp_n in dps {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let count = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            for s in 0..count {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }
                if let Some(rec) = self.unpack_record_meta(&dp[off..off+len]) {
                    if rec.len() < 16 { continue; }
                    let rid = i16::from_le_bytes([rec[4], rec[5]]) as u16;
                    if rid != relation_id { continue; }
                    let fv = i16::from_le_bytes([rec[6], rec[7]]) as u16;
                    let mut bid = [0u8; 8];
                    bid.copy_from_slice(&rec[8..16]);
                    found.push((fv, bid));
                }
            }
        }
        found
    }
}

/// Decompress into caller buffer — zero allocation per record.
/// Returns false if record should be skipped (deleted/chained/blob/fragment/malformed).
///
/// RHD_CHAIN marks an *old version* of an updated row (a back-version, often stored
/// as a delta). A full-state snapshot must emit only the current/primary version, so
/// chained records are skipped — otherwise an updated row appears twice (duplicate PK)
/// and back-version deltas decode to garbage (e.g. NUL bytes in text columns).
#[inline]
pub fn unpack_record_into(raw: &[u8], buf: &mut Vec<u8>) -> bool {
    if raw.len() < 13 { return false; }
    let flags = u16::from_le_bytes([raw[10], raw[11]]);
    let skip = RHD_DELETED | RHD_CHAIN | RHD_FRAGMENT | RHD_INCOMPLETE | RHD_BLOB;
    if flags & skip != 0 { return false; }
    let data_start = if flags & RHD_LONG_TRANUM != 0 { 16 } else { 13 };
    if raw.len() <= data_start { return false; }
    let compressed = &raw[data_start..];
    if flags & RHD_NOT_PACKED != 0 {
        buf.clear();
        buf.extend_from_slice(compressed);
        true
    } else {
        crate::sqz::decompress_into(compressed, buf)
    }
}

// ── CDC helpers ───────────────────────────────────────────────────────────────

/// Read the transaction ID from a raw record without unpacking.
/// Returns None for blobs, fragments, or malformed records.
#[inline]
pub fn record_transaction(raw: &[u8]) -> Option<u64> {
    if raw.len() < 13 { return None; }
    let flags = u16::from_le_bytes([raw[10], raw[11]]);
    if flags & (RHD_FRAGMENT | RHD_INCOMPLETE | RHD_BLOB) != 0 { return None; }
    let lo = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as u64;
    if flags & RHD_LONG_TRANUM != 0 {
        if raw.len() < 16 { return None; }
        let hi = u16::from_le_bytes([raw[14], raw[15]]) as u64;
        Some((hi << 32) | lo)
    } else {
        Some(lo)
    }
}

/// Like unpack_record_into but also unpacks DELETED records (for CDC delete detection).
/// Returns None for blobs, fragments, and malformed records. Returns Some(deleted).
pub fn unpack_record_cdc(raw: &[u8], buf: &mut Vec<u8>) -> Option<bool> {
    if raw.len() < 13 { return None; }
    let flags = u16::from_le_bytes([raw[10], raw[11]]);
    if flags & (RHD_FRAGMENT | RHD_INCOMPLETE | RHD_BLOB) != 0 { return None; }
    let deleted = flags & RHD_DELETED != 0;
    let data_start = if flags & RHD_LONG_TRANUM != 0 { 16 } else { 13 };
    if raw.len() <= data_start { return None; }
    let compressed = &raw[data_start..];
    let ok = if flags & RHD_NOT_PACKED != 0 {
        buf.clear();
        buf.extend_from_slice(compressed);
        true
    } else {
        crate::sqz::decompress_into(compressed, buf)
    };
    if ok { Some(deleted) } else { None }
}

// ── PK reading from system tables ─────────────────────────────────────────────

#[allow(dead_code)]
fn read_text_field(rec: &[u8], desc: &Desc) -> Option<String> {
    let s = desc.offset as usize;
    match desc.dtype {
        DTYPE_TEXT | DTYPE_CSTRING => {
            let end = (s + desc.length as usize).min(rec.len());
            if end <= s { return None; }
            Some(String::from_utf8_lossy(&rec[s..end]).trim_end_matches(|c: char| c == '\0' || c == ' ').to_string())
        }
        DTYPE_VARYING => {
            if s + 2 > rec.len() { return None; }
            let vlen = u16::from_le_bytes([rec[s], rec[s+1]]) as usize;
            let end = (s + 2 + vlen).min(rec.len());
            Some(String::from_utf8_lossy(&rec[s+2..end]).to_string())
        }
        _ => None,
    }
}

impl OdsReader {
    /// Read PRIMARY KEY field names for a table by scanning system tables.
    ///
    /// System tables don't have entries in RDB$FORMATS (Relation.cpp line 1147).
    /// Offsets are hardcoded from fields.h / relations.h / FLAG_BYTES macro.
    ///
    /// FLAG_BYTES(n) = (((n + BITS_PER_LONG) & ~(BITS_PER_LONG-1)) >> 3)
    /// BITS_PER_LONG=32 → rounds up to 4 bytes for any table with 1-31 fields.
    /// All fields are dtype_text (2-byte aligned) or dtype_short (2-byte aligned).
    pub fn read_primary_key_fields(&self, table_name: &str) -> Result<Vec<String>> {
        let index_name = self.find_pk_index_name(table_name)?;
        self.find_index_field_names(&index_name)
    }

    /// List all constraint types found for a table (for debugging).
    pub fn list_constraints(&self, table_name: &str) -> Vec<String> {
        let nl        = if self.ods_ver >= 13 { 252usize } else { 63 };
        let ctype_off = 4 + nl;
        let rname_off = ctype_off + 11;

        let target = table_name.to_uppercase();
        let mut out = Vec::new();

        let Some(pp) = self.find_first_pp(22) else { return out; };
        for dp_n in self.data_pages_for(pp) {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            for s in 0..cnt {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }
                let Some(rec) = self.unpack_record_meta(&dp[off..off+len]) else { continue };
                if rname_off + nl > rec.len() { continue; }
                let rname = std::str::from_utf8(&rec[rname_off..rname_off+nl])
                    .unwrap_or("").trim_end_matches(|c: char| c == '\0' || c == ' ');
                if !rname.eq_ignore_ascii_case(&target) { continue; }
                if ctype_off + 11 > rec.len() { continue; }
                let ctype = std::str::from_utf8(&rec[ctype_off..ctype_off+11])
                    .unwrap_or("").trim().to_string();
                out.push(ctype);
            }
        }
        out
    }

    fn find_pk_index_name(&self, table_name: &str) -> Result<String> {
        // RDB$RELATION_CONSTRAINTS (rel 22) hardcoded offsets.
        //
        // All fields are dtype_text (1-byte aligned). No padding between consecutive CHAR fields.
        // FLAG_BYTES(6)=4. null_bitmap=4 bytes.
        //
        // ODS 12 (MAX_SQL_IDENTIFIER_LEN=63):
        //   +4   constraint_name  (63)
        //   +67  constraint_type  (11)
        //   +78  relation_name    (63)
        //   +141 deferrable       (3)
        //   +144 init_deferred    (3)
        //   +147 index_name       (63)
        //
        // ODS 13+ (MAX_SQL_IDENTIFIER_LEN=252):
        //   +4   constraint_name  (252)
        //   +256 constraint_type  (11)
        //   +267 relation_name    (252)
        //   +519 deferrable       (3)
        //   +522 init_deferred    (3)
        //   +525 index_name       (252)

        let pp = self.find_first_pp(22)
            .ok_or_else(|| anyhow::anyhow!("No pointer page for RDB$RELATION_CONSTRAINTS (rel 22)"))?;

        // All fields are CHAR (dtype_text, 1-byte aligned) — no alignment padding between them.
        // Record layout: null_bitmap(4) | cname(nl) | ctype(11) | rname(nl) | dfr(3) | idfr(3) | iname(nl)
        let nl        = if self.ods_ver >= 13 { 252usize } else { 63 };
        let ctype_off = 4 + nl;
        let rname_off = ctype_off + 11;
        let dfr_off   = rname_off + nl;
        let idfr_off  = dfr_off + 3;
        let iname_off = idfr_off + 3;

        let target = table_name.to_uppercase();

        for dp_n in self.data_pages_for(pp) {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            for s in 0..cnt {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }
                let Some(rec) = self.unpack_record_meta(&dp[off..off+len]) else { continue };

                // constraint_type at ctype_off, length 11
                if ctype_off + 11 > rec.len() { continue; }
                let ctype = std::str::from_utf8(&rec[ctype_off..ctype_off+11])
                    .unwrap_or("").trim();
                if !ctype.eq_ignore_ascii_case("PRIMARY KEY") { continue; }

                // relation_name at rname_off, length nl
                if rname_off + nl > rec.len() { continue; }
                let rname = std::str::from_utf8(&rec[rname_off..rname_off+nl])
                    .unwrap_or("").trim_end_matches(|c: char| c == '\0' || c == ' ');
                if !rname.eq_ignore_ascii_case(&target) { continue; }

                // index_name at iname_off, length nl
                if iname_off + nl > rec.len() { continue; }
                let iname = std::str::from_utf8(&rec[iname_off..iname_off+nl])
                    .unwrap_or("").trim_end_matches(|c: char| c == '\0' || c == ' ');
                return Ok(iname.to_string());
            }
        }
        anyhow::bail!("No PRIMARY KEY constraint found for table '{}'", table_name)
    }

    /// Read raw slot bytes from a data page (for chain-following on delete stubs).
    pub fn read_slot_raw(&self, page_num: u32, slot: usize) -> Option<&[u8]> {
        let dp = self.page(page_num);
        if dp[0] != PAG_DATA { return None; }
        let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
        if slot >= cnt { return None; }
        let off = u16::from_le_bytes([dp[24+slot*4], dp[25+slot*4]]) as usize;
        let len = u16::from_le_bytes([dp[26+slot*4], dp[27+slot*4]]) as usize;
        if off == 0 || len == 0 || off + len > self.page_size { return None; }
        Some(&dp[off..off+len])
    }

    fn find_index_field_names(&self, index_name: &str) -> Result<Vec<String>> {
        // RDB$INDEX_SEGMENTS (rel 3) hardcoded offsets.
        //
        // Fields: index_name(text,nl), field_name(text,nl), position(short,2),
        //         statistics(double,8,8-byte aligned) [ODS≥11], ...
        // FLAG_BYTES(3)=4. null_bitmap=4 bytes.
        //
        // ODS 12 (nl=63):  iname@4, fname@68, pos@132, stats@136
        // ODS 13+ (nl=252): iname@4, fname@256, pos@508, stats@512

        let pp = self.find_first_pp(3)
            .ok_or_else(|| anyhow::anyhow!("No pointer page for RDB$INDEX_SEGMENTS (rel 3)"))?;

        // CHAR iname + CHAR fname (byte-aligned, no padding); SSHORT pos needs 2-byte align.
        // 4 + nl + nl is always even so no padding needed before pos either.
        let nl        = if self.ods_ver >= 13 { 252usize } else { 63 };
        let iname_off = 4;
        let fname_off = 4 + nl;
        let pos_off   = 4 + nl + nl;   // 4 + 2*nl is always even → no alignment padding

        let target = index_name.to_uppercase();
        let mut entries: Vec<(i16, String)> = Vec::new();

        for dp_n in self.data_pages_for(pp) {
            let dp = self.page(dp_n);
            if dp[0] != PAG_DATA { continue; }
            let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            for s in 0..cnt {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > self.page_size { continue; }
                let Some(rec) = self.unpack_record_meta(&dp[off..off+len]) else { continue };

                if iname_off + nl > rec.len() { continue; }
                let iname = std::str::from_utf8(&rec[iname_off..iname_off+nl])
                    .unwrap_or("").trim_end_matches(|c: char| c == '\0' || c == ' ');
                if !iname.eq_ignore_ascii_case(&target) { continue; }

                if fname_off + nl > rec.len() { continue; }
                let fname = std::str::from_utf8(&rec[fname_off..fname_off+nl])
                    .unwrap_or("").trim_end_matches(|c: char| c == '\0' || c == ' ').to_string();

                let pos: i16 = if pos_off + 2 <= rec.len() {
                    i16::from_le_bytes([rec[pos_off], rec[pos_off+1]])
                } else { entries.len() as i16 };

                entries.push((pos, fname));
            }
        }

        if entries.is_empty() {
            anyhow::bail!("No segments found for index '{}'", index_name);
        }
        entries.sort_by_key(|(pos, _)| *pos);
        Ok(entries.into_iter().map(|(_, name)| name).collect())
    }
}

// ── CDC delete-stub unit tests ────────────────────────────────────────────────
//
// These tests reproduce the delete-not-working bug without needing a real .fdb:
//
//   1. Firebird creates a small "delete stub" on the data page (22 bytes for
//      typical records: 16-byte RHD_LONG_TRANUM header + 6 bytes of SQZ data).
//   2. The actual record (with the PK) lives in the BACK VERSION pointed to by
//      rhd_b_page (raw[4..8]) and rhd_b_line (raw[8..10]).
//   3. unpack_record_cdc(stub, buf) correctly returns Some(true) but buf is only
//      as large as the stub's compressed payload — NOT the full record.
//   4. write_field_binary then reads buf[pk_offset..pk_offset+pk_len]; when that
//      range is beyond buf.len() it writes the PGCOPY NULL sentinel (-1i32).
//   5. DELETE FROM t WHERE pk = NULL matches 0 rows.
//
// Fix: when deleted=true and buf is too small for any PK field, follow the chain
// via read_slot_raw(rhd_b_page, rhd_b_line) to get the full back-version data.

#[cfg(test)]
mod cdc_delete_stub_tests {
    use super::*;

    // Build a synthetic Firebird record header + optional payload.
    //
    //   raw[0..4]   rhd_transaction lo32
    //   raw[4..8]   rhd_b_page  (back-version page)
    //   raw[8..10]  rhd_b_line  (back-version slot index)
    //   raw[10..12] rhd_flags
    //   raw[12]     rhd_format
    //   raw[13..16] padding (short header) OR hi_tranum at [14..16] (long header)
    //   raw[header_size..]  compressed payload
    fn make_stub(txn: u64, b_page: u32, b_line: u16, flags: u16, payload: &[u8]) -> Vec<u8> {
        let long = flags & RHD_LONG_TRANUM != 0;
        let hdr  = if long { 16 } else { 13 };
        let mut raw = vec![0u8; hdr + payload.len()];
        raw[0..4].copy_from_slice(&(txn as u32).to_le_bytes());
        raw[4..8].copy_from_slice(&b_page.to_le_bytes());
        raw[8..10].copy_from_slice(&b_line.to_le_bytes());
        raw[10..12].copy_from_slice(&flags.to_le_bytes());
        if long {
            raw[14..16].copy_from_slice(&((txn >> 32) as u16).to_le_bytes());
        }
        raw[hdr..].copy_from_slice(payload);
        raw
    }

    // SQZ: ctrl=+N then N literal bytes.
    fn sqz_literal(data: &[u8]) -> Vec<u8> {
        assert!(data.len() <= 127);
        let mut v = vec![data.len() as u8];
        v.extend_from_slice(data);
        v
    }

    // ── record_transaction ────────────────────────────────────────────────────

    #[test]
    fn stub_transaction_short_header() {
        // 13-byte header, no RHD_LONG_TRANUM
        let stub = make_stub(12345, 0, 0, 0, &sqz_literal(&[1, 2, 3]));
        assert_eq!(record_transaction(&stub), Some(12345));
    }

    #[test]
    fn stub_transaction_long_header() {
        // RHD_DELETED | RHD_LONG_TRANUM = 0x0801, txn spans hi word
        let txn   = 18065u64;
        let flags = RHD_DELETED | RHD_LONG_TRANUM;
        let stub  = make_stub(txn, 3340534, 3, flags, &[0u8; 6]);
        assert_eq!(record_transaction(&stub), Some(txn),
            "RHD_LONG_TRANUM: hi word at raw[14..16] must be OR-ed into txn");
    }

    #[test]
    fn stub_transaction_fragment_returns_none() {
        let flags = RHD_FRAGMENT;
        let stub  = make_stub(999, 0, 0, flags, &[0u8; 4]);
        assert_eq!(record_transaction(&stub), None,
            "FRAGMENT records must not be counted as transactions");
    }

    // ── unpack_record_cdc ─────────────────────────────────────────────────────

    #[test]
    fn live_record_returns_some_false() {
        let stub = make_stub(1000, 0, 0, 0, &sqz_literal(&[0xAA, 0xBB, 0xCC]));
        let mut buf = Vec::new();
        assert_eq!(unpack_record_cdc(&stub, &mut buf), Some(false));
        assert_eq!(buf, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn delete_stub_returns_some_true() {
        // Mirrors real output: raw_len=22 flags=0x0801
        let flags   = RHD_DELETED | RHD_LONG_TRANUM;
        let payload = sqz_literal(&[0u8; 5]); // 6-byte SQZ → 5 decompressed bytes
        let stub    = make_stub(18065, 3340534, 3, flags, &payload);
        assert_eq!(stub.len(), 22, "matches observed raw_len=22");
        let mut buf = Vec::new();
        assert_eq!(unpack_record_cdc(&stub, &mut buf), Some(true),
            "delete stub must return deleted=true");
    }

    // ── BUG: stub data too small to hold PK → write_field_binary emits NULL ───

    #[test]
    fn delete_stub_buf_too_small_for_int64_pk() {
        // Scenario: CLIENTES.CODCLIENTE is INT64 (8 bytes) at decompressed offset 4.
        // The delete stub decompresses to only 5 bytes — field spans [4..12] which
        // is beyond the buffer → write_field_binary writes the PGCOPY NULL sentinel.
        let flags   = RHD_DELETED | RHD_LONG_TRANUM;
        let payload = sqz_literal(&[0u8; 5]); // decompresses to 5 bytes
        let stub    = make_stub(18065, 3340534, 3, flags, &payload);

        let mut buf = Vec::new();
        unpack_record_cdc(&stub, &mut buf).unwrap();

        let pk_offset: usize = 4; // typical INT64 PK offset after 4-byte null bitmap
        let pk_length: usize = 8; // INT64

        // This is the bug: the stub data cannot contain the PK field.
        // The caller must follow the back-version chain instead.
        assert!(
            pk_offset + pk_length > buf.len(),
            "stub buf ({} bytes) must be too small for PK at [{}..{}]; \
             delete path must follow rhd_b_page/rhd_b_line chain",
            buf.len(), pk_offset, pk_offset + pk_length,
        );
    }

    // ── Chain pointer extraction ──────────────────────────────────────────────

    #[test]
    fn delete_stub_chain_pointers_correct() {
        // When delete stub is too small to extract PK, the fix is to follow:
        //   rhd_b_page = u32 at raw[4..8]
        //   rhd_b_line = u16 at raw[8..10]
        // then call db.read_slot_raw(b_page, b_line) to get the full back version.
        let b_page: u32 = 3_340_534;
        let b_line: u16 = 3;
        let flags = RHD_DELETED | RHD_LONG_TRANUM;
        let stub  = make_stub(18065, b_page, b_line, flags, &[0u8; 6]);

        let got_b_page = u32::from_le_bytes(stub[4..8].try_into().unwrap());
        let got_b_line = u16::from_le_bytes([stub[8], stub[9]]);
        assert_eq!(got_b_page, b_page);
        assert_eq!(got_b_line, b_line);
    }

    #[test]
    fn chain_follow_logic_on_back_version() {
        // Simulate: back-version raw record (RHD_CHAIN, not DELETED) contains full data.
        // After following b_page/b_line, we call unpack_record_cdc on the back version.
        // The back version has RHD_CHAIN set so unpack_record_cdc returns Some(false).
        // The caller must accept Some(false) here and treat the data as the PK source.
        let full_pk_value: i64 = 42;
        let mut full_record = vec![0u8; 12]; // 4 null bitmap + 8 INT64
        full_record[4..12].copy_from_slice(&full_pk_value.to_le_bytes());
        let payload      = sqz_literal(&full_record);
        let back_version = make_stub(18065, 0, 0, RHD_CHAIN, &payload);

        let mut buf = Vec::new();
        // unpack_record_cdc must unpack chain records (they are skipped by unpack_record
        // but CDC delete-fix code needs them as PK source)
        let result = unpack_record_cdc(&back_version, &mut buf);
        assert_eq!(result, Some(false), "back version (CHAIN) should decompress OK");
        assert!(buf.len() >= 12, "back version must contain full record data");
        let pk = i64::from_le_bytes(buf[4..12].try_into().unwrap());
        assert_eq!(pk, full_pk_value, "PK extracted from back version must match");
    }
}

// ── Fragmented-record reassembly tests ────────────────────────────────────────
//
// Large records (RDB$RELATIONS rows with many 252-byte identifier columns) don't fit
// a single data-page slot. Firebird splits them: the head slot carries rhd_incomplete
// and the rhdf header (data at offset 22, next fragment at f_page=[16..20]/f_line=
// [20..22]); the tail slot carries rhd_fragment with the plain 13-byte header. Each
// fragment is its own SQZ stream; decompressed pieces concatenate (jrd/vio.cpp).
//
// Before the fix, unpack_record_meta read the head at offset 13/16 (inside the rhdf
// header) → SQZ decode failed → the relation was invisible → "Table X not found in
// RDB$RELATIONS" even though it exists.
#[cfg(test)]
mod fragment_tests {
    use super::*;
    use std::io::Write;

    // SQZ literal run: ctrl = +N then N literal bytes (sqz.cpp), decodes to those bytes.
    fn sqz_literal(data: &[u8]) -> Vec<u8> {
        assert!(data.len() <= 127);
        let mut v = vec![data.len() as u8];
        v.extend_from_slice(data);
        v
    }

    // Build a minimal two-page ODS file: page 0 = header, page 1 = one data page whose
    // slot 0 is the head fragment (→ "HELLO") and slot 1 the tail fragment (→ "WORLD").
    fn write_fragmented_db() -> std::path::PathBuf {
        const PS: usize = 1024;
        let mut buf = vec![0u8; PS * 2];

        // ── page 0: header (OdsReader::open reads page_size@16, ods_ver@18) ──
        buf[16..18].copy_from_slice(&(PS as u16).to_le_bytes());
        buf[18..20].copy_from_slice(&(13u16 | 0x8000).to_le_bytes()); // ODS 13 + FB flag

        // ── page 1: data page ──
        let p1 = PS; // file offset of page 1
        buf[p1] = PAG_DATA;
        buf[p1 + 22..p1 + 24].copy_from_slice(&2u16.to_le_bytes()); // dpg_count = 2 slots

        // head fragment at page-relative offset 64
        let head_off = 64usize;
        let head_payload = sqz_literal(b"HELLO");
        let head_len = 22 + head_payload.len(); // rhdf header (22) + SQZ piece
        {
            let h = p1 + head_off;
            buf[h + 10..h + 12].copy_from_slice(&RHD_INCOMPLETE.to_le_bytes());
            buf[h + 16..h + 20].copy_from_slice(&1u32.to_le_bytes()); // f_page = 1
            buf[h + 20..h + 22].copy_from_slice(&1u16.to_le_bytes()); // f_line = 1 (slot 1)
            buf[h + 22..h + 22 + head_payload.len()].copy_from_slice(&head_payload);
        }

        // tail fragment at page-relative offset 256
        let tail_off = 256usize;
        let tail_payload = sqz_literal(b"WORLD");
        let tail_len = 13 + tail_payload.len(); // plain header (13) + SQZ piece
        {
            let t = p1 + tail_off;
            buf[t + 10..t + 12].copy_from_slice(&RHD_FRAGMENT.to_le_bytes());
            buf[t + 13..t + 13 + tail_payload.len()].copy_from_slice(&tail_payload);
        }

        // slot vector: slot s → off@(24+s*4), len@(26+s*4)
        buf[p1 + 24..p1 + 26].copy_from_slice(&(head_off as u16).to_le_bytes());
        buf[p1 + 26..p1 + 28].copy_from_slice(&(head_len as u16).to_le_bytes());
        buf[p1 + 28..p1 + 30].copy_from_slice(&(tail_off as u16).to_le_bytes());
        buf[p1 + 30..p1 + 32].copy_from_slice(&(tail_len as u16).to_le_bytes());

        let path = std::env::temp_dir().join(format!("fdbfrag_{}.fdb", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&buf).unwrap();
        path
    }

    #[test]
    fn reassembles_multi_fragment_record() {
        let path = write_fragmented_db();
        let db = OdsReader::open(path.to_str().unwrap()).unwrap();

        // head slot lives at page 1, slot 0
        let head = db.read_slot_raw(1, 0).expect("head slot");
        let flags = u16::from_le_bytes([head[10], head[11]]);
        assert!(flags & RHD_INCOMPLETE != 0, "head must carry rhd_incomplete");

        let full = db.reassemble_fragments(head).expect("reassembly");
        assert_eq!(full, b"HELLOWORLD",
            "decompressed fragments must concatenate in order");

        // unpack_record_meta must route incomplete heads through reassembly too
        let via_meta = db.unpack_record_meta(head).expect("meta reassembly");
        assert_eq!(via_meta, b"HELLOWORLD");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tail_fragment_alone_is_skipped_by_meta_scan() {
        // A continuation slot (rhd_fragment) is reached only through the head's chain;
        // a metadata scan that lands on it directly must skip it, not treat it as a row.
        let path = write_fragmented_db();
        let db = OdsReader::open(path.to_str().unwrap()).unwrap();

        let tail = db.read_slot_raw(1, 1).expect("tail slot");
        assert!(db.unpack_record_meta(tail).is_none(),
            "rhd_fragment continuation must be skipped by unpack_record_meta");

        let _ = std::fs::remove_file(&path);
    }
}
