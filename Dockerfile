# syntax=docker/dockerfile:1.7
# Build every shippable binary from one Linux container.
#
#   SERVER  fdb-ingest      — Linux only            (musl static)
#   CLIENT  fdb-agent       — Linux  (musl static)
#           fdb-agent.exe   — Windows (mingw, static CRT)
#
# Cross toolchains, all inside Linux (no zig needed here):
#   - musl-tools           → musl-gcc           → Linux musl static  (ring C + link)
#   - gcc-mingw-w64        → x86_64-w64-mingw32 → Windows .exe        (ring C + link)
#
# ── Usage ───────────────────────────────────────────────────────────────────────
#   # everything onto the host:
#   docker build --target export --output dist .
#     -> dist/linux-x86_64/fdb-ingest
#     -> dist/linux-x86_64/fdb-agent
#     -> dist/windows/fdb-agent.exe
#
#   # just the server binary:
#   docker build --target export-server --output dist/linux-x86_64 .
#
#   # just the client pair (linux + windows):
#   docker build --target export-client --output dist .
#
#   # runnable gateway image:
#   docker build --target runtime -t fdb-ingest .

ARG LINUX_TARGET=x86_64-unknown-linux-musl
ARG WIN_TARGET=x86_64-pc-windows-gnu

# ── builder ────────────────────────────────────────────────────────────────────
FROM rust:slim AS builder
ARG LINUX_TARGET
ARG WIN_TARGET

RUN apt-get update -qq \
 && apt-get install -y -qq musl-tools gcc-mingw-w64-x86-64 \
 && rm -rf /var/lib/apt/lists/* \
 && rustup target add ${LINUX_TARGET} ${WIN_TARGET}

# ring's C code is built per target; point each at its cross compiler/linker.
# crt-static on the .exe so it carries no libgcc/libwinpthread DLL dependency.
ENV CC_x86_64_unknown_linux_musl=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc \
    CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc \
    CARGO_TARGET_X86_64_PC_WINDOWS_GNU_RUSTFLAGS="-Ctarget-feature=+crt-static"

WORKDIR /src
COPY . .

# One layer builds all three artifacts. Cache mounts keep the dep registry and the
# target/ dir warm across rebuilds, so only changed code recompiles. The binaries
# must be copied out to /out *inside* this RUN — the target cache mount is not
# present in later stages.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    cargo build --release --target ${LINUX_TARGET} --bin fdb-ingest; \
    cargo build --release --target ${LINUX_TARGET} --bin fdb-agent; \
    cargo build --release --target ${WIN_TARGET}   --bin fdb-agent; \
    mkdir -p /out/linux-x86_64 /out/windows; \
    cp target/${LINUX_TARGET}/release/fdb-ingest /out/linux-x86_64/fdb-ingest; \
    cp target/${LINUX_TARGET}/release/fdb-agent  /out/linux-x86_64/fdb-agent; \
    cp target/${WIN_TARGET}/release/fdb-agent.exe /out/windows/fdb-agent.exe; \
    strip /out/linux-x86_64/fdb-ingest /out/linux-x86_64/fdb-agent; \
    x86_64-w64-mingw32-strip /out/windows/fdb-agent.exe

# ── export stages (use with `--output`) ─────────────────────────────────────────
# Server only: Linux fdb-ingest.
FROM scratch AS export-server
COPY --from=builder /out/linux-x86_64/fdb-ingest /fdb-ingest

# Client only: Linux + Windows fdb-agent.
FROM scratch AS export-client
COPY --from=builder /out/linux-x86_64/fdb-agent /linux-x86_64/fdb-agent
COPY --from=builder /out/windows/fdb-agent.exe  /windows/fdb-agent.exe

# Everything, laid out like dist/.
FROM scratch AS export
COPY --from=builder /out/ /

# ── runtime: the gateway, static binary on scratch ──────────────────────────────
FROM scratch AS runtime
COPY --from=builder /out/linux-x86_64/fdb-ingest /usr/local/bin/fdb-ingest
# PKI + tenant map provided at run time, e.g.:
#   docker run -p 8443:8443 -v $PWD/pki:/pki fdb-ingest \
#     serve --ca /pki/ca.crt --cert /pki/server.crt --key /pki/server.key \
#     --tenant cliente1=replica_cliente1
EXPOSE 8443
ENTRYPOINT ["/usr/local/bin/fdb-ingest"]
