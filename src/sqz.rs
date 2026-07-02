//! Firebird SQZ decompressor — mirrors Compressor::unpack() in src/jrd/sqz.cpp
//!
//! Format (signed ctrl byte):
//!   ctrl > 0  : copy next ctrl bytes verbatim
//!   ctrl == 0 : NOP (copy 0 bytes)
//!   ctrl == -1: repeat count in next 2 bytes (u16 LE), then 1 byte to repeat
//!   ctrl == -2: repeat count in next 4 bytes (u32 LE), then 1 byte to repeat
//!   ctrl < 0  : repeat next byte |ctrl| times

const MAX_RECORD_SIZE: usize = 256 * 1024;

/// Decompress into a caller-provided buffer (zero allocation).
/// Clears `out` first. Returns false on malformed input.
pub fn decompress_into(input: &[u8], out: &mut Vec<u8>) -> bool {
    out.clear();
    decompress_impl(input, out)
}

/// Decompress a Firebird SQZ-compressed record (allocating version).
/// Returns None if the input is malformed or output exceeds MAX_RECORD_SIZE.
pub fn decompress(input: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 4);
    if decompress_impl(input, &mut out) { Some(out) } else { None }
}

fn decompress_impl(input: &[u8], out: &mut Vec<u8>) -> bool {
    let mut i = 0usize;

    while i < input.len() {
        let ctrl = input[i] as i8;
        i += 1;

        if ctrl >= 0 {
            let n = ctrl as usize;
            if i + n > input.len() { return false; }
            out.extend_from_slice(&input[i..i + n]);
            i += n;
        } else {
            let count: usize = match ctrl {
                -1 => {
                    if i + 2 > input.len() { return false; }
                    let c = u16::from_le_bytes([input[i], input[i + 1]]) as usize;
                    i += 2;
                    c
                }
                -2 => {
                    if i + 4 > input.len() { return false; }
                    let c = u32::from_le_bytes([input[i], input[i + 1], input[i + 2], input[i + 3]]) as usize;
                    i += 4;
                    if c > MAX_RECORD_SIZE { return false; }
                    c
                }
                n => n.unsigned_abs() as usize,
            };

            if i >= input.len() { return false; }
            let byte = input[i];
            i += 1;

            if out.len() + count > MAX_RECORD_SIZE { return false; }
            out.resize(out.len() + count, byte);
        }

        if out.len() > MAX_RECORD_SIZE { return false; }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Unit tests ────────────────────────────────────────────────────────────

    #[test]
    fn empty_input() {
        assert_eq!(decompress(&[]), Some(vec![]));
    }

    #[test]
    fn nop_ctrl_zero() {
        // ctrl=0 is NOP — produces no output, advances past the byte
        assert_eq!(decompress(&[0]), Some(vec![]));
        assert_eq!(decompress(&[0, 0, 0]), Some(vec![]));
    }

    #[test]
    fn copy_run() {
        // ctrl=3: copy next 3 bytes
        let input = [3u8, 0x07, 0x14, 0x30];
        assert_eq!(decompress(&input), Some(vec![0x07, 0x14, 0x30]));
    }

    #[test]
    fn repeat_run() {
        // ctrl=-5 (0xFB): repeat 5 times, byte=0x2A
        let input = [0xFBu8, 0x2A];
        assert_eq!(decompress(&input), Some(vec![0x2A; 5]));
    }

    #[test]
    fn repeat_i8_min() {
        // ctrl=-128 (0x80): should repeat 128 times without overflow
        let input = [0x80u8, 0xFF];
        let out = decompress(&input).unwrap();
        assert_eq!(out.len(), 128);
        assert!(out.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn extended_u16_count() {
        // ctrl=-1 (0xFF): next 2 bytes = u16 count, then repeat byte
        let count: u16 = 500;
        let mut input = vec![0xFFu8];
        input.extend_from_slice(&count.to_le_bytes());
        input.push(0xAB);
        let out = decompress(&input).unwrap();
        assert_eq!(out.len(), 500);
        assert!(out.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn mixed_runs() {
        // Decompress the typical first bytes of a RDB$RELATIONS fragmented record
        let input = [
            0x03u8, 0x07, 0x14, 0x30, // ctrl=+3: copy [07 14 30]
            0x87, 0x2A,                // ctrl=-121: repeat 0x2A × 121
            0x00,                      // ctrl=0: NOP
            0x01, 0x41,                // ctrl=+1: copy [0x41]
        ];
        let out = decompress(&input).unwrap();
        assert_eq!(out.len(), 3 + 121 + 1);
        assert_eq!(&out[..3], &[0x07, 0x14, 0x30]);
        assert!(out[3..3+121].iter().all(|&b| b == 0x2A));
        assert_eq!(out[3+121], 0x41);
    }

    #[test]
    fn truncated_copy_returns_none() {
        // ctrl=10 but only 3 bytes follow — malformed
        let input = [10u8, 0x01, 0x02, 0x03];
        assert_eq!(decompress(&input), None);
    }

    #[test]
    fn truncated_repeat_returns_none() {
        // ctrl=-5 but no byte follows
        let input = [0xFBu8];
        assert_eq!(decompress(&input), None);
    }

    #[test]
    fn truncated_u16_count_returns_none() {
        // ctrl=-1 but only 1 byte follows for count
        let input = [0xFFu8, 0x01];
        assert_eq!(decompress(&input), None);
    }

    // ── Integration tests against BASE.FDB ────────────────────────────────────

    #[cfg(feature = "test_base_fdb")]
    mod base_fdb {
        // These tests require BASE.FDB in the crate root directory.
        // Enable with: cargo test --features test_base_fdb
        // They test the full ODS read + decompress pipeline.

        use crate::ods::OdsReader;

        const DB: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/BASE.FDB");

        #[test]
        fn list_tables_includes_estados() {
            let db = OdsReader::open(DB).expect("open BASE.FDB");
            let tables = db.list_tables();
            assert!(
                tables.iter().any(|t| t == "ESTADOS"),
                "ESTADOS not found in list_tables: {tables:?}"
            );
        }

        #[test]
        fn estados_has_rows() {
            let db = OdsReader::open(DB).expect("open BASE.FDB");
            let rid = db.find_relation_id("ESTADOS").expect("find ESTADOS");
            let descs = db.read_format(rid, u16::MAX).expect("read format");
            let pp = db.find_first_pp(rid).expect("find pointer page");
            let row_count = db.records_meta(pp).count();
            assert!(row_count > 0, "ESTADOS should have at least 1 row");
        }
    }
}
