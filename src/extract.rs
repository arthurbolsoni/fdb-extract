use anyhow::Result;
use std::io::{BufWriter, Write};

use crate::ods::{self, OdsReader};

// Windows-1252 → UTF-8 codepoints for the 0x80-0x9F range (undefined → same value).
static WIN1252_EXTRA: [u32; 32] = [
    0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
    0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
    0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
];

/// Convert a Win1252/Latin-1 byte slice to UTF-8.
/// Pure ASCII fast path: one scan + memcpy, no per-byte branch.
/// Mixed: finds first non-ASCII with position(), memcpy leading ASCII, then encodes remainder.
#[inline]
fn win1252_to_utf8(bytes: &[u8], out: &mut Vec<u8>) {
    out.clear();
    // Fast path also bails on NUL: PostgreSQL text cannot hold 0x00 in any encoding,
    // so a single embedded NUL would otherwise abort the whole table's COPY.
    let split = match bytes.iter().position(|&b| b >= 0x80 || b == 0) {
        None => { out.extend_from_slice(bytes); return; }  // all ASCII, no NUL — memcpy and done
        Some(i) => i,
    };
    out.reserve(bytes.len() + 16);
    out.extend_from_slice(&bytes[..split]);  // leading clean block via memcpy
    let mut i = split;
    while i < bytes.len() {
        let b = bytes[i];
        if b == 0 {
            i += 1; // drop NUL — invalid in PG text
        } else if b < 0x80 {
            // Find end of ASCII run (no NUL), flush as memcpy
            let start = i;
            while i < bytes.len() && bytes[i] < 0x80 && bytes[i] != 0 { i += 1; }
            out.extend_from_slice(&bytes[start..i]);
        } else {
            let cp: u32 = if b < 0xA0 { WIN1252_EXTRA[(b - 0x80) as usize] } else { b as u32 };
            if cp < 0x800 {
                out.push(0xC0 | (cp >> 6) as u8);
                out.push(0x80 | (cp & 0x3F) as u8);
            } else {
                out.push(0xE0 | (cp >> 12) as u8);
                out.push(0x80 | ((cp >> 6) & 0x3F) as u8);
                out.push(0x80 | (cp & 0x3F) as u8);
            }
            i += 1;
        }
    }
}

// Powers of 10 as f64 — avoids powi() libm call for scaled integer conversion.
static SCALE_DIV: [f64; 19] = [
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
];

// Firebird MJD of 2000-01-01 (PostgreSQL epoch)
const PG_EPOCH_MJD: i32 = 51544;

/// Encode one Firebird field into PostgreSQL COPY BINARY wire format.
#[inline]
pub fn write_field_binary<W: std::io::Write>(rec: &[u8], desc: &ods::Desc, out: &mut W, text_buf: &mut Vec<u8>) -> std::io::Result<()> {
    let s = desc.offset as usize;

    macro_rules! null { () => { return out.write_all(&(-1i32).to_be_bytes()) } }
    macro_rules! len4 { ($n:expr) => { out.write_all(&($n as i32).to_be_bytes())? } }

    match desc.dtype {
        ods::DTYPE_TEXT | ods::DTYPE_CSTRING => {
            let end = (s + desc.length as usize).min(rec.len());
            if end <= s { null!(); }
            let raw = &rec[s..end];
            let tl = raw.iter().rposition(|&b| b > b' ').map(|i| i+1).unwrap_or(0);
            win1252_to_utf8(&raw[..tl], text_buf);
            len4!(text_buf.len()); out.write_all(text_buf)?;
        }
        ods::DTYPE_VARYING => {
            if s + 2 > rec.len() { null!(); }
            let vlen = u16::from_le_bytes([rec[s], rec[s+1]]) as usize;
            let end  = (s + 2 + vlen).min(rec.len());
            let data = &rec[s+2..end];
            let tl = data.iter().rposition(|&b| b > b' ').map(|i| i+1).unwrap_or(data.len());
            win1252_to_utf8(&data[..tl], text_buf);
            len4!(text_buf.len()); out.write_all(text_buf)?;
        }
        ods::DTYPE_SHORT => {
            if s + 2 > rec.len() { null!(); }
            let v = i16::from_le_bytes([rec[s], rec[s+1]]);
            if desc.scale == 0 {
                len4!(2); out.write_all(&v.to_be_bytes())?;
            } else {
                let f = v as f64 / SCALE_DIV[(-desc.scale) as usize];
                len4!(8); out.write_all(&f.to_bits().to_be_bytes())?;
            }
        }
        ods::DTYPE_LONG => {
            if s + 4 > rec.len() { null!(); }
            let v = i32::from_le_bytes(rec[s..s+4].try_into().unwrap());
            if desc.scale == 0 {
                len4!(4); out.write_all(&v.to_be_bytes())?;
            } else {
                let f = v as f64 / SCALE_DIV[(-desc.scale) as usize];
                len4!(8); out.write_all(&f.to_bits().to_be_bytes())?;
            }
        }
        ods::DTYPE_INT64 | ods::DTYPE_QUAD => {
            if s + 8 > rec.len() { null!(); }
            let v = i64::from_le_bytes(rec[s..s+8].try_into().unwrap());
            if desc.scale == 0 {
                len4!(8); out.write_all(&v.to_be_bytes())?;
            } else {
                let f = v as f64 / SCALE_DIV[(-desc.scale) as usize];
                len4!(8); out.write_all(&f.to_bits().to_be_bytes())?;
            }
        }
        ods::DTYPE_REAL => {
            if s + 4 > rec.len() { null!(); }
            let bits = u32::from_le_bytes(rec[s..s+4].try_into().unwrap());
            len4!(4); out.write_all(&bits.to_be_bytes())?;
        }
        ods::DTYPE_DOUBLE | ods::DTYPE_D_FLOAT => {
            if s + 8 > rec.len() { null!(); }
            let bits = u64::from_le_bytes(rec[s..s+8].try_into().unwrap());
            len4!(8); out.write_all(&bits.to_be_bytes())?;
        }
        ods::DTYPE_SQL_DATE => {
            if s + 4 > rec.len() { null!(); }
            let mjd = i32::from_le_bytes(rec[s..s+4].try_into().unwrap());
            len4!(4); out.write_all(&(mjd - PG_EPOCH_MJD).to_be_bytes())?;
        }
        ods::DTYPE_SQL_TIME | ods::DTYPE_SQL_TIME_TZ => {
            if s + 4 > rec.len() { null!(); }
            let ticks = u32::from_le_bytes(rec[s..s+4].try_into().unwrap());
            let us = ticks as i64 * 100;
            len4!(8); out.write_all(&us.to_be_bytes())?;
        }
        ods::DTYPE_TIMESTAMP | ods::DTYPE_TIMESTAMP_TZ => {
            if s + 8 > rec.len() { null!(); }
            let mjd   = i32::from_le_bytes(rec[s..s+4].try_into().unwrap());
            let ticks = u32::from_le_bytes(rec[s+4..s+8].try_into().unwrap());
            let us = (mjd as i64 - PG_EPOCH_MJD as i64) * 86_400_000_000 + ticks as i64 * 100;
            len4!(8); out.write_all(&us.to_be_bytes())?;
        }
        ods::DTYPE_BOOLEAN => {
            if s >= rec.len() { null!(); }
            len4!(1); out.write_all(&[if rec[s] != 0 { 1u8 } else { 0u8 }])?;
        }
        ods::DTYPE_DBKEY => {
            let end = (s + desc.length as usize).min(rec.len());
            let data = &rec[s..end];
            len4!(data.len()); out.write_all(data)?;
        }
        _ => { null!(); }
    }
    Ok(())
}

// ── Shared PGCOPY stream builder ────────────────────────────────────────────
//
// One field slot in PG column order: its Firebird descriptor plus the null-bitmap
// position. `present` is false when the column did not yet exist in the record's
// on-disk format version (added by a later ALTER TABLE) — such a field is always
// emitted as NULL.
pub struct CopySlot { pub desc: ods::Desc, pub null_byte: usize, pub null_mask: u8, pub present: bool }

/// Build the PG-column-ordered slots for a *specific* on-disk format's descriptors.
///
/// Firebird versions every record: the header byte `rhd_format` (raw[12]) names the
/// format the row was written with. Columns added by a later ALTER TABLE have a
/// field id ≥ that format's field count and did not exist physically in the row, so
/// they are marked `present: false` and decoded as NULL.
pub fn slots_for_format(field_order: &[(usize, String)], descs: &[ods::Desc]) -> Vec<CopySlot> {
    field_order.iter().map(|(fid, _)| match descs.get(*fid) {
        Some(d) => CopySlot { desc: d.clone(), null_byte: fid / 8, null_mask: 1 << (fid % 8), present: true },
        None    => CopySlot { desc: ods::Desc::default_zero(), null_byte: 0, null_mask: 0, present: false },
    }).collect()
}

/// Resolve the PG output column order (field id + name) and field count for a relation.
/// The per-record format descriptors are resolved later, inside `write_copy_stream`.
pub fn copy_slots(db: &OdsReader, relation_id: u16, table: &str) -> Result<(Vec<(usize, String)>, i16)> {
    let descs = db.read_format(relation_id, u16::MAX)?;
    let field_order: Vec<(usize, String)> = db.read_field_names(relation_id, table)
        .unwrap_or_else(|_| (0..descs.len()).map(|i| (i, format!("col_{i}"))).collect());
    let n_fields = field_order.len() as i16;
    Ok((field_order, n_fields))
}

/// Write the full PostgreSQL COPY BINARY stream (header + every live row + trailer)
/// for a relation into `out`. Returns the row count. `progress` is invoked every
/// 10k rows with the running count.
pub fn write_copy_stream<W: Write>(
    db:          &OdsReader,
    relation_id: u16,
    field_order: &[(usize, String)],
    n_fields:    i16,
    out:         &mut W,
    mut progress: impl FnMut(u64),
) -> std::io::Result<u64> {
    // PostgreSQL COPY BINARY header
    out.write_all(b"PGCOPY\n\xff\r\n\0")?;
    out.write_all(&0i32.to_be_bytes())?;
    out.write_all(&0i32.to_be_bytes())?;

    // Per-record format resolution. Every record stores its format version in
    // rhd_format (raw[12]); rows written before a later ALTER TABLE keep the older
    // layout. Decoding them all with the latest format reads garbage offsets
    // (bogus duplicate PKs, varchar length prefixes that overflow the target type).
    // Cache the PG-ordered slots per format byte; most tables use one or two.
    let latest = db.read_format(relation_id, u16::MAX).ok();
    let mut fmt_cache: Vec<Option<Vec<CopySlot>>> = (0..256).map(|_| None).collect();

    let count = if let Some(pp) = db.find_first_pp(relation_id) {
        let mut rec_buf  = Vec::<u8>::with_capacity(4096);
        let mut text_buf = Vec::<u8>::with_capacity(512);
        let mut count = 0u64;

        for &dp_n in &db.data_pages_for(pp) {
            let dp = db.page(dp_n);
            if dp[0] != ods::PAG_DATA { continue; }
            let cnt = u16::from_le_bytes([dp[22], dp[23]]) as usize;
            for s in 0..cnt {
                let off = u16::from_le_bytes([dp[24+s*4], dp[25+s*4]]) as usize;
                let len = u16::from_le_bytes([dp[26+s*4], dp[27+s*4]]) as usize;
                if off == 0 || len == 0 || off + len > db.page_size { continue; }
                let raw = &dp[off..off+len];
                let fmt = raw[12];
                let fl  = u16::from_le_bytes([raw[10], raw[11]]);
                // A large data row split across slots has an rhd_incomplete head; reassemble
                // it from its fragment chain instead of dropping it. Everything else goes
                // through the zero-copy fast path.
                let got = if fl & ods::RHD_INCOMPLETE != 0
                    && fl & (ods::RHD_DELETED | ods::RHD_FRAGMENT | ods::RHD_BLOB | ods::RHD_CHAIN) == 0
                {
                    match db.reassemble_fragments(raw) {
                        Some(v) => { rec_buf.clear(); rec_buf.extend_from_slice(&v); true }
                        None    => false,
                    }
                } else {
                    ods::unpack_record_into(raw, &mut rec_buf)
                };
                if !got { continue; }

                if fmt_cache[fmt as usize].is_none() {
                    let descs = db.read_format(relation_id, fmt as u16).ok()
                        .or_else(|| latest.clone());
                    let slots = descs.map(|d| slots_for_format(field_order, &d))
                        .unwrap_or_default();
                    fmt_cache[fmt as usize] = Some(slots);
                }
                let slots = fmt_cache[fmt as usize].as_ref().unwrap();

                out.write_all(&n_fields.to_be_bytes())?;
                for slot in slots {
                    let nb = rec_buf.get(slot.null_byte).copied().unwrap_or(0xFF);
                    if !slot.present || (nb != 0 && nb & slot.null_mask != 0) {
                        out.write_all(&(-1i32).to_be_bytes())?;
                    } else {
                        write_field_binary(&rec_buf, &slot.desc, out, &mut text_buf)?;
                    }
                }
                count += 1;
                if count % 10_000 == 0 { progress(count); }
            }
        }
        count
    } else { 0 };

    // Trailer
    out.write_all(&(-1i16).to_be_bytes())?;
    Ok(count)
}

pub fn extract_ods_binary(args: &crate::Args) -> Result<u64> {
    let table = args.table.as_deref().unwrap();
    let output = args.output.clone()
        .unwrap_or_else(|| format!("{}.bin", table.to_lowercase()));

    let db = ods::OdsReader::open(args.database.as_deref().unwrap())?;
    let relation_id = db.find_relation_id(table)?;
    let (field_order, n_fields) = copy_slots(&db, relation_id, table)?;

    let stdout_mode = output == "-";
    let bar = crate::make_spinner(args.no_progress || stdout_mode);

    if stdout_mode {
        // Windows opens stdout in text mode — LF→CRLF corrupts binary data.
        #[cfg(windows)]
        unsafe {
            extern "C" { fn _setmode(fd: i32, mode: i32) -> i32; }
            _setmode(1, 0x8000); // _O_BINARY
        }
    }

    let sink: Box<dyn Write> = if stdout_mode {
        Box::new(std::io::stdout())
    } else {
        Box::new(std::fs::File::create(&output)?)
    };
    let mut out = BufWriter::with_capacity(128 * 1024 * 1024, sink);

    let count = write_copy_stream(&db, relation_id, &field_order, n_fields, &mut out, |c| bar.set_position(c))?;

    bar.finish_with_message(format!("{count} rows"));
    out.flush()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc_int64(offset: u32) -> ods::Desc {
        ods::Desc { dtype: ods::DTYPE_INT64, scale: 0, length: 8, sub_type: 0, flags: 0, offset }
    }

    fn desc_long(offset: u32) -> ods::Desc {
        ods::Desc { dtype: ods::DTYPE_LONG, scale: 0, length: 4, sub_type: 0, flags: 0, offset }
    }

    fn desc_text(offset: u32, length: u16) -> ods::Desc {
        ods::Desc { dtype: ods::DTYPE_TEXT, scale: 0, length, sub_type: 0, flags: 0, offset }
    }

    // ── Bug reproduction: delete stub → NULL pk ───────────────────────────────
    //
    // When Firebird deletes a record it creates a tiny stub on the data page.
    // The stub decompresses to a few bytes — not enough to hold the PK field.
    // write_field_binary detects offset+len > rec.len() and emits the PGCOPY
    // NULL sentinel (-1i32).  DELETE WHERE pk = NULL → 0 rows affected.

    #[test]
    fn int64_pk_beyond_buf_emits_null() {
        // Simulate: CODCLIENTE INT64 at offset 4, record buf only 5 bytes
        // (typical delete stub decompressed payload)
        let rec  = vec![0u8; 5]; // stub decompressed: only 5 bytes
        let desc = desc_int64(4); // pk at offset 4, needs bytes 4..12
        let mut out      = Vec::new();
        let mut text_buf = Vec::new();

        write_field_binary(&rec, &desc, &mut out, &mut text_buf).unwrap();

        // PGCOPY NULL sentinel is -1i32 big-endian = [0xFF, 0xFF, 0xFF, 0xFF]
        assert_eq!(out, (-1i32).to_be_bytes(),
            "INT64 pk beyond buf must emit PGCOPY NULL; DELETE WHERE pk=NULL → 0 rows");
    }

    #[test]
    fn long_pk_beyond_buf_emits_null() {
        let rec  = vec![0u8; 3]; // only 3 bytes, field needs [2..6]
        let desc = desc_long(2);
        let mut out      = Vec::new();
        let mut text_buf = Vec::new();

        write_field_binary(&rec, &desc, &mut out, &mut text_buf).unwrap();

        assert_eq!(out, (-1i32).to_be_bytes());
    }

    // ── Correct path: full record → real pk value ─────────────────────────────

    #[test]
    fn int64_pk_within_buf_emits_value() {
        // pk = 42, stored LE at offset 4 (after 4-byte null bitmap)
        let pk: i64 = 42;
        let mut rec = vec![0u8; 12];
        rec[4..12].copy_from_slice(&pk.to_le_bytes());

        let desc = desc_int64(4);
        let mut out      = Vec::new();
        let mut text_buf = Vec::new();

        write_field_binary(&rec, &desc, &mut out, &mut text_buf).unwrap();

        // PGCOPY INT8: 4-byte len (8) + 8-byte value BE
        assert_eq!(&out[0..4], &8i32.to_be_bytes(), "length prefix must be 8");
        let got = i64::from_be_bytes(out[4..12].try_into().unwrap());
        assert_eq!(got, pk);
    }

    #[test]
    fn text_field_truncated_at_buf_end() {
        // Field declared as 20 chars but record only has 10 bytes at that offset.
        // Should not panic; writes what's available (or empty).
        let rec  = vec![b'A'; 10];
        let desc = desc_text(0, 20);
        let mut out      = Vec::new();
        let mut text_buf = Vec::new();

        write_field_binary(&rec, &desc, &mut out, &mut text_buf).unwrap();

        // Should not emit NULL — DTYPE_TEXT uses min(end, rec.len())
        assert_ne!(out, (-1i32).to_be_bytes(), "TEXT truncated at buf end must not emit NULL");
    }

    #[test]
    fn int64_at_offset_zero_within_buf() {
        let pk: i64 = -1;
        let mut rec = vec![0u8; 8];
        rec[0..8].copy_from_slice(&pk.to_le_bytes());

        let desc = desc_int64(0);
        let mut out      = Vec::new();
        let mut text_buf = Vec::new();

        write_field_binary(&rec, &desc, &mut out, &mut text_buf).unwrap();

        let got = i64::from_be_bytes(out[4..12].try_into().unwrap());
        assert_eq!(got, pk);
    }

    // ── Record format-version bug ─────────────────────────────────────────────
    //
    // Firebird stamps every record with the format it was written under (rhd_format,
    // raw[12]). After an ALTER TABLE, old rows keep the OLD layout. Decoding them with
    // the LATEST format reads every field at the wrong offset, which produced the two
    // production failures:
    //   • VENDAS        — a wrong offset read text bytes as the INT PK → bogus values
    //                     collided → "duplicate key (nrovenda)=(39882)".
    //   • VENDAS_PRODS  — a wrong offset read a varchar length prefix as 16672 →
    //                     "value too long for type character varying(80)".

    fn desc_varying(offset: u32, length: u16) -> ods::Desc {
        ods::Desc { dtype: ods::DTYPE_VARYING, scale: 0, length, sub_type: 0, flags: 0, offset }
    }

    // The written PGCOPY field's declared length prefix (first 4 bytes, big-endian).
    fn written_len(out: &[u8]) -> i32 { i32::from_be_bytes(out[0..4].try_into().unwrap()) }

    #[test]
    fn wrong_format_offset_overflows_varchar_but_record_format_is_correct() {
        // A VENDAS_PRODS-style record physically written in an OLD format: a VARCHAR(5)
        // lives at offset 8 holding "HELLO". A LATER ALTER inserted columns, so the
        // newest format places that column at offset 20 instead.
        let mut rec = vec![0u8; 64];
        // correct (old-format) location: 2-byte len=5 then "HELLO"
        rec[8..10].copy_from_slice(&5u16.to_le_bytes());
        rec[10..15].copy_from_slice(b"HELLO");
        // at the latest-format offset (20) sit unrelated bytes whose first two read as a
        // huge length prefix (0x4120 = 16672) — exactly the production symptom. The bytes
        // that follow are real text (here 'X'), so the bogus read returns a long string.
        rec[20] = 0x20;
        rec[21] = 0x41;
        for b in &mut rec[22..64] { *b = b'X'; }

        let declared = 7u16; // VARCHAR(5) → 5 + 2-byte varying prefix

        // Decoding with the record's OWN (old) format → correct, fits the column.
        let mut out = Vec::new();
        let mut tb  = Vec::new();
        write_field_binary(&rec, &desc_varying(8, declared), &mut out, &mut tb).unwrap();
        assert_eq!(written_len(&out), 5, "old-format offset must yield the real 'HELLO'");
        assert_eq!(&out[4..9], b"HELLO");

        // Decoding the same bytes with the LATEST format's offset → overflow garbage.
        let mut bad = Vec::new();
        write_field_binary(&rec, &desc_varying(20, declared), &mut bad, &mut tb).unwrap();
        assert!(written_len(&bad) as usize > (declared - 2) as usize,
            "latest-format offset reads a bogus length that overflows VARCHAR(5) — the bug");
    }

    #[test]
    fn slots_for_format_marks_columns_added_after_record_as_null() {
        // Current schema has 4 columns (field ids 0,1,2,5). The record's on-disk format
        // only knew 3 fields (descriptors for ids 0..3). Column with field id 5 was added
        // by a later ALTER TABLE → it must be emitted as NULL, not read from a wrong offset.
        let field_order = vec![
            (0usize, "a".to_string()),
            (1, "b".to_string()),
            (2, "c".to_string()),
            (5, "added_later".to_string()),
        ];
        let old_format_descs = vec![desc_long(4), desc_long(8), desc_long(12)];

        let slots = slots_for_format(&field_order, &old_format_descs);
        assert_eq!(slots.len(), 4);
        assert!(slots[0].present && slots[1].present && slots[2].present);
        assert!(!slots[3].present, "field id 5 absent from old format must be NULL");
        // present columns map to their field id for the null-bitmap lookup
        assert_eq!(slots[2].null_byte, 0);
        assert_eq!(slots[2].null_mask, 1 << 2);
    }
}
