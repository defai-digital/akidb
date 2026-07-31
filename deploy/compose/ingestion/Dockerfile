# AkiDB Ingestion Orchestrator
# Rust-based document processing pipeline

FROM rust:1-bookworm AS builder

WORKDIR /app

# Install dependencies for building
RUN apt-get update && apt-get install -y \
    clang \
    cmake \
    libclang-dev \
    pkg-config \
    protobuf-compiler \
    libssl-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build release binary
RUN cargo build --release --locked -p akidb-ingestion

# Final stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3 \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary
COPY --from=builder /app/target/release/akidb-ingestion /app/

# Create state directory
RUN mkdir -p /var/lib/akidb && \
    useradd -m -u 1000 ingestion && \
    chown -R ingestion:ingestion /app /var/lib/akidb

USER ingestion

EXPOSE 9093

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:9093/health || exit 1

ENTRYPOINT ["/app/akidb-ingestion"]
