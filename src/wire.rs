//! Framed wire protocol shared by `fdb-agent` (client) and `fdb-ingest` (server).
//!
//! Runs over a single mTLS-authenticated TCP connection (rustls, sync). One request
//! per connection:
//!
//! ```text
//! request:  MAGIC(4) VERSION(1) OP(1) [u32 hdr_len][hdr_json]  <body>
//!   FULL body:  chunked PGCOPY stream — repeated {[u32 len][bytes]}, ended by len=0
//!   DELTA body: [u32 ulen][upsert PGCOPY] [u32 dlen][delete PGCOPY]
//! response: MAGIC(4) STATUS(1: 0=ok 1=err) [u32 json_len][resp_json]
//! ```
//!
//! Headers/response are JSON so the schema can evolve without breaking the framing.
//! The FULL body is chunk-framed so the agent can stream `write_copy_stream` output
//! straight to the socket without knowing the total length up front.

use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

pub const MAGIC: [u8; 4] = *b"FDBI";
pub const VERSION: u8 = 1;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op { Full, Delta }

impl Op {
    pub fn to_u8(self) -> u8 { match self { Op::Full => 1, Op::Delta => 2 } }
    pub fn from_u8(b: u8) -> io::Result<Op> {
        match b { 1 => Ok(Op::Full), 2 => Ok(Op::Delta), _ => Err(inval("unknown op")) }
    }
}

#[derive(Serialize, Deserialize)]
pub struct FullHeader {
    pub table:      String,
    pub create_sql: String,
}

#[derive(Serialize, Deserialize)]
pub struct DeltaHeader {
    pub table:        String,
    pub cols_csv:     String,
    pub pk_csv:       String,
    pub pk_col_names: Vec<String>,
    pub n_upserts:    u64,
    pub n_deletes:    u64,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Resp {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error:   Option<String>,
    pub rows:    u64,
    pub upserts: u64,
    pub deletes: u64,
}

impl Resp {
    pub fn err(msg: impl Into<String>) -> Self {
        Resp { error: Some(msg.into()), ..Default::default() }
    }
}

fn inval(m: &str) -> io::Error { io::Error::new(io::ErrorKind::InvalidData, m.to_string()) }

// ── Length-prefixed blobs ──────────────────────────────────────────────────────

pub fn write_u32_prefixed<W: Write>(w: &mut W, b: &[u8]) -> io::Result<()> {
    w.write_all(&(b.len() as u32).to_be_bytes())?;
    w.write_all(b)
}

pub fn read_u32_prefixed<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut l = [0u8; 4];
    r.read_exact(&mut l)?;
    let n = u32::from_be_bytes(l) as usize;
    let mut v = vec![0u8; n];
    r.read_exact(&mut v)?;
    Ok(v)
}

/// Stream a `[u32 len][len bytes]` body from `r` into `w` without ever holding the
/// whole body in memory — bounded by `io::copy`'s internal buffer. Used by the
/// ingest so a whole-table delta (hundreds of MB) can flow socket→PG COPY instead
/// of being slurped into one `vec![0u8; n]` (which OOM-killed the gateway).
pub fn copy_u32_prefixed<R: Read, W: Write>(r: &mut R, w: &mut W) -> io::Result<u64> {
    let mut l = [0u8; 4];
    r.read_exact(&mut l)?;
    let n = u32::from_be_bytes(l) as u64;
    let mut limited = (&mut *r).take(n);
    let copied = io::copy(&mut limited, w)?;
    if copied != n {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short delta body"));
    }
    Ok(copied)
}

// ── Request header ─────────────────────────────────────────────────────────────

pub fn write_request_header<W: Write>(w: &mut W, op: Op, header_json: &[u8]) -> io::Result<()> {
    w.write_all(&MAGIC)?;
    w.write_all(&[VERSION, op.to_u8()])?;
    write_u32_prefixed(w, header_json)
}

pub fn read_request_header<R: Read>(r: &mut R) -> io::Result<(Op, Vec<u8>)> {
    let mut m = [0u8; 4];
    r.read_exact(&mut m)?;
    if m != MAGIC { return Err(inval("bad magic")); }
    let mut vo = [0u8; 2];
    r.read_exact(&mut vo)?;
    if vo[0] != VERSION { return Err(inval("unsupported version")); }
    let op = Op::from_u8(vo[1])?;
    let header = read_u32_prefixed(r)?;
    Ok((op, header))
}

// ── Response ───────────────────────────────────────────────────────────────────

pub fn write_response<W: Write>(w: &mut W, resp: &Resp) -> io::Result<()> {
    let j = serde_json::to_vec(resp).map_err(|e| inval(&e.to_string()))?;
    w.write_all(&MAGIC)?;
    w.write_all(&[u8::from(resp.error.is_some())])?;
    write_u32_prefixed(w, &j)?;
    w.flush()
}

pub fn read_response<R: Read>(r: &mut R) -> io::Result<Resp> {
    let mut m = [0u8; 4];
    r.read_exact(&mut m)?;
    if m != MAGIC { return Err(inval("bad magic")); }
    let mut s = [0u8; 1];
    r.read_exact(&mut s)?;
    let j = read_u32_prefixed(r)?;
    serde_json::from_slice(&j).map_err(|e| inval(&e.to_string()))
}

// ── Chunked body (FULL streaming) ──────────────────────────────────────────────

/// Frames every `write()` as `[u32 len][bytes]`. Call `finish()` to emit the
/// terminating zero-length chunk. Wrap a large `BufWriter` around this so each
/// flushed block becomes one frame rather than one frame per tiny field write.
pub struct ChunkWriter<W: Write> { inner: W, written: u64 }

impl<W: Write> ChunkWriter<W> {
    pub fn new(inner: W) -> Self { Self { inner, written: 0 } }
    /// Total bytes pushed downstream so far: frame headers + payload (excludes the
    /// terminator, which `finish` writes). This is the on-wire body size.
    pub fn bytes_written(&self) -> u64 { self.written }
    pub fn finish(mut self) -> io::Result<W> {
        self.inner.write_all(&0u32.to_be_bytes())?;
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for ChunkWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() { return Ok(0); }
        self.inner.write_all(&(buf.len() as u32).to_be_bytes())?;
        self.inner.write_all(buf)?;
        self.written += 4 + buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> { self.inner.flush() }
}

/// Reads a stream written by [`ChunkWriter`], presenting the unframed bytes and
/// returning EOF (`Ok(0)`) after the zero-length terminator chunk.
pub struct ChunkReader<R: Read> { inner: R, remaining: usize, done: bool }

impl<R: Read> ChunkReader<R> {
    pub fn new(inner: R) -> Self { Self { inner, remaining: 0, done: false } }
    pub fn into_inner(self) -> R { self.inner }
    pub fn is_done(&self) -> bool { self.done }
}

impl<R: Read> Read for ChunkReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done { return Ok(0); }
        if self.remaining == 0 {
            let mut l = [0u8; 4];
            self.inner.read_exact(&mut l)?;
            let n = u32::from_be_bytes(l) as usize;
            if n == 0 { self.done = true; return Ok(0); }
            self.remaining = n;
        }
        let to = self.remaining.min(buf.len());
        let got = self.inner.read(&mut buf[..to])?;
        if got == 0 { return Err(io::ErrorKind::UnexpectedEof.into()); }
        self.remaining -= got;
        Ok(got)
    }
}
