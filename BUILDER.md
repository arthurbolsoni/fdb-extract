# BUILDER — how to build the three binaries

This crate produces three binaries from one source tree:

| Binary       | Runs on            | Who has it                | Built how                          |
|--------------|--------------------|---------------------------|------------------------------------|
| `fdb-extract`| Windows (dev/CLI)  | local                     | native `cargo build`               |
| `fdb-agent`  | Windows (customer) | next to the `.fdb`        | native `cargo build`               |
| `fdb-ingest` | **Linux** (VPS/dev)| gateway + PostgreSQL host | **cross-compiled** from Windows    |

All builds happen **on Windows**. No WSL, no Docker, no remote build box.

---

## 1. Windows binaries (`fdb-extract`, `fdb-agent`)

Native target, nothing special:

```powershell
cargo build --release --bin fdb-agent
cargo build --release --bin fdb-extract
# output: target\release\fdb-agent.exe , target\release\fdb-extract.exe
```

`.cargo/config.toml` already sets `+crt-static` for `x86_64-pc-windows-msvc`, so the
`.exe` is self-contained (no VC++ redist needed on the customer machine).

---

## 2. Linux binary (`fdb-ingest`) — cross-compile from Windows

### Why `cargo build --target x86_64-unknown-linux-musl` is NOT enough

`rustc --print target-list` lists every target rustc can *emit code* for — it is
**not** the set you can actually *build*. Three layers are needed, and only the first
two are free:

1. **Codegen** — in `target-list` by default. rustc emits `.o` for any of them.
2. **Target std** — `rustup target add <triple>` downloads the precompiled `rust-std`.
3. **Linker + C toolchain for the target** — the missing piece on Windows.

This crate depends on **`ring`** (the rustls crypto backend), which compiles **C/asm**
with a C compiler that must *target* Linux. Windows has no Linux linker and no Linux
cross C compiler, so a plain cross build dies with:

```
error: failed to find tool "x86_64-linux-musl-gcc"
```

A pure-Rust crate with no C deps can sometimes link via the self-contained `rust-lld`
that ships with the musl target — but `ring`'s C code breaks that. We need a real
cross C toolchain.

### The fix: `cargo-zigbuild` (zig provides the cross C toolchain)

`zig cc` is a complete cross C compiler + linker for every target. `cargo-zigbuild`
wires it into cargo, satisfying both `ring`'s C compile and the final link.

**Target OS:** the `dev` gateway is Oracle Linux 8.7 (glibc 2.28). We build **musl
static** so the binary has zero libc dependency and runs on any Linux regardless of
glibc version. (A `-gnu` binary built on a newer glibc would fail to start there.)

#### One-time setup

```powershell
# zig (via pip — no PATH juggling; cargo-zigbuild auto-detects `python -m ziglang`)
pip install ziglang

# the cargo wrapper
cargo install cargo-zigbuild

# the musl std for the target
rustup target add x86_64-unknown-linux-musl
```

#### Build

```powershell
cargo zigbuild --release --target x86_64-unknown-linux-musl --bin fdb-ingest
# output: target\x86_64-unknown-linux-musl\release\fdb-ingest
```

Verify it is a static ELF:

```powershell
# on the Linux box after copying:  file fdb-ingest
# expect: ELF 64-bit LSB executable, x86-64, statically linked, stripped
```

---

## 3. Deploy `fdb-ingest` to the `dev` gateway

`dev` is an SSH alias in `~/.ssh/config` pointing at your gateway host. The service
lives in `/root/fdb-ingest/`.

```powershell
# copy next to the old binary, smoke-test, then swap in (keep a backup)
scp target\x86_64-unknown-linux-musl\release\fdb-ingest dev:/root/fdb-ingest/fdb-ingest.new
ssh dev "cd /root/fdb-ingest && chmod +x fdb-ingest.new && ./fdb-ingest.new --help | head -1 && cp -f fdb-ingest fdb-ingest.bak && mv -f fdb-ingest.new fdb-ingest"
```

Restart the gateway (it crashed → it is not running, so just start it):

```bash
ssh dev
cd /root/fdb-ingest && ./run-ingest.sh        # or: systemctl restart fdb-ingest
```

Roll back if needed: `mv -f fdb-ingest.bak fdb-ingest`.

---

## 4. Refresh the repo artifacts (`dist/`)

`dist/` ships ready-to-run bundles. After a build, refresh both:

```powershell
Copy-Item target\x86_64-unknown-linux-musl\release\fdb-ingest dist\linux-x86_64\fdb-ingest -Force
Copy-Item target\release\fdb-agent.exe                          dist\windows\fdb-agent.exe   -Force
```

---

## TL;DR

```powershell
# Windows agent
cargo build --release --bin fdb-agent

# Linux ingest (cross, musl static) — one-time: pip install ziglang; cargo install cargo-zigbuild; rustup target add x86_64-unknown-linux-musl
cargo zigbuild --release --target x86_64-unknown-linux-musl --bin fdb-ingest

# deploy
scp target\x86_64-unknown-linux-musl\release\fdb-ingest dev:/root/fdb-ingest/fdb-ingest
```
