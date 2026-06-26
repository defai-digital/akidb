# AkiDB Ingestion Orchestrator
# Rust-based document processing pipeline

FROM rust:1.83-bookworm as builder

WORKDIR /app

# Install dependencies for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build release binary
RUN cargo build --release -p akidb-ingestion

# Final stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
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

# Health check via process existence
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD pgrep akidb-ingestion || exit 1

ENTRYPOINT ["/app/akidb-ingestion"]
