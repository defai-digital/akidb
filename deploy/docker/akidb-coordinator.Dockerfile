# AkiDB Coordinator
# Standalone Dockerfile for CI/CD pipelines
#
# Build: docker build -f deploy/docker/akidb-coordinator.Dockerfile -t akidb-coordinator:latest .
# Run:   docker run -p 50052:50052 -p 9091:9091 akidb-coordinator:latest

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:1-bookworm AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    libclang-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build release binary
RUN cargo build --release --locked -p akidb-coordinator

# Verify binary was built
RUN test -f /build/target/release/akidb-coordinator

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="AkiDB Coordinator"
LABEL org.opencontainers.image.description="AkiDB distributed query coordinator"
LABEL org.opencontainers.image.vendor="AkiDB"
LABEL org.opencontainers.image.version="1.0.0"

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user (ADR-021: Security Hardening)
RUN groupadd -r -g 1000 akidb && \
    useradd -r -u 1000 -g akidb -d /app -s /sbin/nologin akidb

# Create config directory
RUN mkdir -p /etc/akidb && \
    chown -R akidb:akidb /etc/akidb

# Copy binary from builder
COPY --from=builder /build/target/release/akidb-coordinator /usr/local/bin/akidb-coordinator
RUN chmod +x /usr/local/bin/akidb-coordinator && \
    chown akidb:akidb /usr/local/bin/akidb-coordinator

# Switch to non-root user
USER akidb

# Expose ports
# 50052: gRPC service (coordinator)
# 9091:  Prometheus metrics
EXPOSE 50052 9091

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:9091/health || exit 1

# Default environment variables
ENV RUST_LOG=info
ENV AKIDB_COORDINATOR_LISTEN_ADDR=0.0.0.0:50052
ENV AKIDB_COORDINATOR_METRICS_HOST=0.0.0.0
ENV AKIDB_COORDINATOR_METRICS_PORT=9091

ENTRYPOINT ["akidb-coordinator"]
