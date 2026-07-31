# AkiDB Server - CPU Build
# Standalone Dockerfile for CI/CD pipelines
#
# Build: docker build -f deploy/docker/akidb-server.Dockerfile -t akidb-server:latest .
# Run:   docker run -p 50051:50051 -p 9090:9090 akidb-server:latest

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:1-bookworm AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    librocksdb-dev \
    clang \
    libclang-dev \
    cmake \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# NumKong 7.7 probes optional ARM extensions successfully with GCC 12, but its
# combined dispatch translation unit is compiled at the ARMv8 baseline and then
# fails on those intrinsics. Keep the portable baseline NEON path enabled while
# disabling optional ARM dispatch variants for reproducible multi-arch images.
ENV NK_TARGET_NEONHALF=0 \
    NK_TARGET_NEONSDOT=0 \
    NK_TARGET_NEONBFDOT=0 \
    NK_TARGET_NEONFHM=0 \
    NK_TARGET_SVE=0 \
    NK_TARGET_SVEHALF=0 \
    NK_TARGET_SVEBFDOT=0 \
    NK_TARGET_SVESDOT=0 \
    NK_TARGET_SVE2=0 \
    NK_TARGET_SVE2P1=0 \
    NK_TARGET_NEONFP8=0 \
    NK_TARGET_SME=0 \
    NK_TARGET_SME2=0 \
    NK_TARGET_SME2P1=0 \
    NK_TARGET_SMEF64=0 \
    NK_TARGET_SMEHALF=0 \
    NK_TARGET_SMEBF16=0 \
    NK_TARGET_SMEBI32=0 \
    NK_TARGET_SMELUT2=0 \
    NK_TARGET_SMEFA64=0

# Build release binary
RUN cargo build --release --locked -p akidb-server

# Verify binary was built
RUN test -f /build/target/release/akidb-server

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="AkiDB Server"
LABEL org.opencontainers.image.description="AkiDB vector database shard server"
LABEL org.opencontainers.image.vendor="AkiDB"
LABEL org.opencontainers.image.version="0.10.0"

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    librocksdb7.8 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user (ADR-021: Security Hardening)
RUN groupadd -r -g 1000 akidb && \
    useradd -r -u 1000 -g akidb -d /app -s /sbin/nologin akidb

# Create data directories
RUN mkdir -p /var/lib/akidb/data /var/lib/akidb/wal /var/lib/akidb/snapshots && \
    chown -R akidb:akidb /var/lib/akidb

# Copy binary from builder
COPY --from=builder /build/target/release/akidb-server /usr/local/bin/akidb-server
RUN chmod +x /usr/local/bin/akidb-server && \
    chown akidb:akidb /usr/local/bin/akidb-server

# Switch to non-root user
USER akidb
WORKDIR /var/lib/akidb

# Expose ports
# 50051: gRPC service
# 9090:  Prometheus metrics
EXPOSE 50051 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:9090/healthz || exit 1

# Volume for persistent data
VOLUME ["/var/lib/akidb"]

# Default environment variables
ENV RUST_LOG=info
ENV AKIDB_DATA_DIR=/var/lib/akidb/data
ENV AKIDB_WAL_DIR=/var/lib/akidb/wal
ENV AKIDB_LISTEN_ADDR=0.0.0.0:50051
ENV AKIDB_METRICS_ADDR=0.0.0.0:9090

ENTRYPOINT ["akidb-server"]
