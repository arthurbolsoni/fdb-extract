use std::time::Instant;
use crate::ods;

pub fn run() {
    const ITERS: u64 = 5_000_000;

    // Synthetic record: 16 bytes null bitmap (all non-null) + 512 bytes data.
    let mut rec = vec![0u8; 528];
    for i in 16..528usize { rec[i] = (i % 251) as u8; }

    rec[16..24].copy_from_slice(&3.141592653589793f64.to_le_bytes());  // DOUBLE
    rec[24..28].copy_from_slice(&2.71828f32.to_le_bytes());             // REAL
    rec[28..36].copy_from_slice(&123456789i64.to_le_bytes());           // INT64
    rec[36..40].copy_from_slice(&42i32.to_le_bytes());                  // LONG
    rec[40..42].copy_from_slice(&(-7i16).to_le_bytes());                // SHORT
    rec[42..46].copy_from_slice(&60_000i32.to_le_bytes());              // SQL_DATE
    let ticks: u32 = (10 * 3600 + 30 * 60 + 45) * 10_000 + 1234;
    rec[46..50].copy_from_slice(&ticks.to_le_bytes());                  // SQL_TIME
    let vdata = b"Hello, World!";
    rec[50] = vdata.len() as u8; rec[51] = 0;
    rec[52..52+vdata.len()].copy_from_slice(vdata);                     // VARYING
    let tdata = b"FIXED TEXT    ";
    rec[70..70+tdata.len()].copy_from_slice(tdata);                     // TEXT
    rec[140] = 1;                                                        // BOOLEAN
    rec[141..149].copy_from_slice(&[0xDE,0xAD,0xBE,0xEF,0x01,0x02,0x03,0x04]); // DBKEY

    macro_rules! bench {
        ($label:expr, $dtype:expr, $off:expr, $len:expr, $scale:expr) => {{
            let desc = ods::Desc {
                dtype: $dtype, scale: $scale,
                length: $len, sub_type: 0, flags: 0, offset: $off,
            };
            let mut out      = Vec::with_capacity(64);
            let mut text_buf = Vec::with_capacity(64);
            for _ in 0..10_000 { out.clear(); crate::extract::write_field_binary(&rec, &desc, &mut out, &mut text_buf).ok(); }
            let t0 = Instant::now();
            for _ in 0..ITERS { out.clear(); crate::extract::write_field_binary(&rec, &desc, &mut out, &mut text_buf).ok(); }
            let ns = t0.elapsed().as_nanos() as f64 / ITERS as f64;
            let payload = if out.len() > 4 { &out[4..] } else { &out[..] };
            let hex: String = payload.iter().take(8).map(|b| format!("{b:02X}")).collect::<Vec<_>>().join(" ");
            println!("{:<22} {:>8.2} ns/op   bytes={:<2} hex={}", $label, ns, payload.len(), hex);
        }};
    }

    println!("{:<22} {:>8}   {}", "dtype", "ns/op", "encoded (first 8 bytes)");
    println!("{}", "-".repeat(70));
    bench!("DOUBLE",         ods::DTYPE_DOUBLE,      16, 8,  0);
    bench!("REAL",           ods::DTYPE_REAL,         24, 4,  0);
    bench!("INT64 scale=0",  ods::DTYPE_INT64,        28, 8,  0);
    bench!("INT64 scale=-2", ods::DTYPE_INT64,        28, 8, -2);
    bench!("LONG scale=0",   ods::DTYPE_LONG,         36, 4,  0);
    bench!("LONG scale=-2",  ods::DTYPE_LONG,         36, 4, -2);
    bench!("SHORT scale=0",  ods::DTYPE_SHORT,        40, 2,  0);
    bench!("SQL_DATE",       ods::DTYPE_SQL_DATE,     42, 4,  0);
    bench!("SQL_TIME",       ods::DTYPE_SQL_TIME,     46, 4,  0);
    bench!("TIMESTAMP",      ods::DTYPE_TIMESTAMP,    42, 8,  0);
    bench!("VARYING",        ods::DTYPE_VARYING,      50, 65, 0);
    bench!("TEXT",           ods::DTYPE_TEXT,         70, 63, 0);
    bench!("BOOLEAN",        ods::DTYPE_BOOLEAN,     140, 1,  0);
    bench!("DBKEY",          ods::DTYPE_DBKEY,       141, 8,  0);
}
