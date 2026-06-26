# AkiDB Thor Edition Documentation

> A distributed vector search engine optimized for NVIDIA Jetson Thor edge clusters

**Version:** 0.1.0
**Last Updated:** January 22, 2026

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Project Overview](#project-overview)
3. [Architecture](#architecture)
4. [Getting Started](#getting-started)
5. [Configuration Reference](#configuration-reference)
6. [API Reference](#api-reference)
7. [Deployment Guide](#deployment-guide)
8. [TUI Dashboard Guide](#tui-dashboard-guide)
9. [Operations Guide](#operations-guide)
10. [Troubleshooting](#troubleshooting)
11. [Glossary](#glossary)

---

## Quick Start

```bash
# Development build (Mac/Linux - CPU mode)
cargo build --features cpu

# Start a single shard server
./target/debug/akidb-server --config config/default.toml

# Insert a vector via gRPC
grpcurl -plaintext -d '{
  "collection": "documents",
  "id": "doc-001",
  "vector": [0.1, 0.2, 0.3, ...]
}' localhost:50051 akidb.v1.Akidb/Insert

# Search for similar vectors
grpcurl -plaintext -d '{
  "collection": "documents",
  "query": [0.1, 0.2, 0.3, ...],
  "top_k": 10
}' localhost:50051 akidb.v1.Akidb/Search
```

---

## Project Overview

### What is AkiDB Thor Edition?

AkiDB Thor Edition is a high-performance distributed vector search engine designed specifically for NVIDIA Jetson Thor edge clusters. It enables real-time Retrieval Augmented Generation (RAG) applications with:

- **GPU-accelerated FAISS** indexing for sub-50ms search latency
- **Distributed architecture** with stateless coordinators and sharded storage
- **Edge-optimized design** minimizing network dependencies
- **Production-ready** with WAL, snapshots, and graceful degradation

### Use Cases

| Use Case | Description |
|----------|-------------|
| **Edge RAG** | Real-time document retrieval for LLM applications at the edge |
| **Semantic Search** | Fast similarity search across embedded documents |
| **Recommendation** | Real-time recommendation based on vector similarity |
| **Anomaly Detection** | Finding outliers by nearest neighbor distance |

### Key Features

- **Sub-50ms P95 latency** on Jetson Thor hardware
- **Horizontal scaling** via sharding across multiple Thor nodes
- **Automatic failover** with partial result support
- **MinIO snapshots** for durability without replication overhead
- **TUI Dashboard** for real-time cluster monitoring

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Applications                       │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Coordinator (Stateless)                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Fan-out    │  │   Result    │  │    Backpressure         │  │
│  │  Router     │  │   Merger    │  │    Controller           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│   Shard 0        │  │   Shard 1        │  │   Shard N        │
│  (Thor Node 1)   │  │  (Thor Node 2)   │  │  (Thor Node N)   │
│ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │
│ │ FAISS Index  │ │  │ │ FAISS Index  │ │  │ │ FAISS Index  │ │
│ │   (GPU)      │ │  │ │   (GPU)      │ │  │ │   (GPU)      │ │
│ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │
│ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │
│ │   RocksDB    │ │  │ │   RocksDB    │ │  │ │   RocksDB    │ │
│ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │
└──────────────────┘  └──────────────────┘  └──────────────────┘
           │                    │                    │
           └────────────────────┼────────────────────┘
                                ▼
                    ┌──────────────────┐
                    │      MinIO       │
                    │   (Snapshots)    │
                    └──────────────────┘
```

### Crate Dependency Graph

```
akidb-server (binary - shard server)
├── akidb-grpc (gRPC service layer)
│   ├── akidb-faiss (vector index abstraction)
│   │   └── akidb-common (types, errors)
│   └── akidb-storage (persistence layer)
│       └── akidb-common
└── akidb-common

akidb-coordinator (binary - stateless coordinator)
├── akidb-grpc
└── akidb-common

akidb-tui (binary - monitoring dashboard)
├── akidb-grpc
└── akidb-common
```

### Crate Descriptions

| Crate | Purpose |
|-------|---------|
| **akidb-common** | Shared types (`Vector`, `VectorId`, `SearchResult`), error types (`AkiDbError`), configuration parsing |
| **akidb-faiss** | Vector index trait (`VectorIndex`) with CPU, GPU, cuVS, and Mock implementations. Handles tombstone bitsets and index rebuilds |
| **akidb-storage** | RocksDB backend, Write-Ahead Log (WAL), ID mapping (external ↔ internal), S3/MinIO snapshot storage |
| **akidb-grpc** | gRPC service implementing `akidb.v1.Akidb` proto. Proto definition at `crates/grpc-server/proto/akidb.proto` |
| **akidb-coordinator** | Stateless query coordinator: fan-out search, min-heap result merging, shard routing, backpressure, read-your-writes consistency |
| **akidb-tui** | Terminal UI for monitoring cluster topology, node health, and real-time metrics |

### Key Design Patterns

#### ID Mapping
External string IDs are mapped to internal `i64` IDs for FAISS compatibility:
```
External ID: "doc-abc-123" → Internal ID: 42
```
The storage layer maintains bidirectional mapping for lookups.

#### Tombstone Deletes
Vectors are soft-deleted using a GPU bitset filter rather than removed from the index:
- Delete marks vector as tombstone
- Search excludes tombstoned vectors via bitset
- Periodic rebuilds compact tombstones

#### WAL + Snapshots
- **WAL**: Write-ahead log ensures durability for recent writes
- **Snapshots**: Periodic full snapshots to MinIO for disaster recovery
- **Recovery**: Restore from snapshot, then replay WAL

#### Partial Results
When shards are unavailable, the coordinator returns partial results with coverage metrics:
```json
{
  "results": [...],
  "coverage": 0.67,  // 2 of 3 shards responded
  "within_slo": true
}
```

---

## Getting Started

### Prerequisites

- **Rust** 1.75+ (with cargo)
- **RocksDB** development libraries
- **Protocol Buffers** compiler (protoc)
- For GPU builds: **CUDA 12.x** and **NVIDIA driver**

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/akidb.git
cd akidb

# Development build (CPU mode)
cargo build --features cpu

# Production build (GPU mode - on Jetson Thor)
cargo build --release --features gpu
```

### Running Tests

```bash
# Run all tests (CPU mode)
cargo test --features cpu

# Run specific crate tests
cargo test -p akidb-storage --features cpu
cargo test -p akidb-faiss --features cpu

# Run a single test
cargo test --features cpu test_search_basic
```

### Local Development Setup

1. **Start a shard server:**
   ```bash
   ./target/debug/akidb-server \
     --listen 0.0.0.0:50051 \
     --config config/default.toml
   ```

2. **Start the coordinator:**
   ```bash
   ./target/debug/akidb-coordinator \
     --listen 0.0.0.0:50050 \
     --shards 127.0.0.1:50051
   ```

3. **Launch the TUI dashboard:**
   ```bash
   ./target/debug/akidb-tui --coordinator 127.0.0.1:50050
   ```

---

## Configuration Reference

### Main Configuration (`config/default.toml`)

```toml
# =============================================================================
# AkiDB Thor Edition Configuration
# =============================================================================

[server]
# gRPC server bind address
listen = "0.0.0.0:50051"
# Maximum concurrent requests
max_connections = 1000

# =============================================================================
# Index Configuration
# =============================================================================
[index]
# FAISS index type: "Flat", "IVF4096,Flat", "IVF16384,Flat"
index_type = "IVF4096,Flat"
# Vector dimension (must match your embeddings)
dimension = 1024
# Number of clusters to search (higher = more accurate, slower)
nprobe = 32
# Distance metric: "L2" or "InnerProduct"
metric = "L2"

[index.gpu]
# GPU device ID (0 for first GPU)
device_id = 0
# Fraction of GPU memory to use (0.0-1.0)
memory_fraction = 0.6
# Enable GPU index (requires --features gpu)
enabled = true

# =============================================================================
# Storage Configuration
# =============================================================================
[storage]
# RocksDB data directory
data_dir = "/var/lib/akidb/data"
# Write-ahead log directory
wal_dir = "/var/lib/akidb/wal"
# Enable WAL for durability
wal_enabled = true

[storage.minio]
# MinIO endpoint for snapshots
endpoint = "http://minio.local:9000"
# Bucket name for snapshots
bucket = "akidb-snapshots"
# Access credentials (use environment variables in production)
access_key = "${MINIO_ACCESS_KEY}"
secret_key = "${MINIO_SECRET_KEY}"
# Snapshot interval in seconds (0 to disable)
snapshot_interval = 3600

# =============================================================================
# SLO Configuration
# =============================================================================
[slo]
# Target P95 latency in milliseconds
p95_target_ms = 50
# Enable adaptive backpressure
backpressure_enabled = true
# Maximum in-flight requests before backpressure
max_in_flight = 1000
# Request queue size
max_queue_size = 5000
```

### TUI Configuration (`/opt/akidb/config/tui.json`)

```json
{
  "refresh_interval_ms": 500,
  "show_gpu_metrics": true,
  "discovery_addresses": [
    "127.0.0.1:50050",
    "192.168.1.61:50050",
    "192.168.1.62:50050"
  ],
  "theme": {
    "name": "default"
  },
  "layout": {
    "show_topology": true,
    "show_metrics": true,
    "show_health": true
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `refresh_interval_ms` | integer | 500 | Dashboard refresh rate in milliseconds |
| `show_gpu_metrics` | boolean | true | Display GPU memory and temperature |
| `discovery_addresses` | string[] | localhost + Thor IPs | Coordinator addresses for auto-discovery |
| `coordinator_address` | string | null | Explicit coordinator (overrides discovery) |
| `theme.name` | string | "default" | Theme: "default", "minimal", "high-contrast" |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AKIDB_CONFIG` | Path to configuration file |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `RUST_LOG` | Log level (trace, debug, info, warn, error) |
| `CUDA_VISIBLE_DEVICES` | GPU device selection |

---

## API Reference

### gRPC Service: `akidb.v1.Akidb`

Proto file: `crates/grpc-server/proto/akidb.proto`

#### Insert

Insert a single vector.

```protobuf
rpc Insert(InsertRequest) returns (InsertResponse);

message InsertRequest {
  string collection = 1;
  string id = 2;
  repeated float vector = 3;
  map<string, string> metadata = 4;
}

message InsertResponse {
  bool success = 1;
  string message = 2;
}
```

**Example:**
```bash
grpcurl -plaintext -d '{
  "collection": "documents",
  "id": "doc-001",
  "vector": [0.1, 0.2, 0.3],
  "metadata": {"source": "web", "title": "Example"}
}' localhost:50051 akidb.v1.Akidb/Insert
```

#### Search

Search for similar vectors.

```protobuf
rpc Search(SearchRequest) returns (SearchResponse);

message SearchRequest {
  string collection = 1;
  repeated float query = 2;
  uint32 top_k = 3;
  float threshold = 4;  // Optional distance threshold
}

message SearchResponse {
  repeated SearchResult results = 1;
  float coverage = 2;      // Fraction of shards that responded
  bool within_slo = 3;     // Whether latency was within SLO
}

message SearchResult {
  string id = 1;
  float distance = 2;
  map<string, string> metadata = 3;
}
```

**Example:**
```bash
grpcurl -plaintext -d '{
  "collection": "documents",
  "query": [0.1, 0.2, 0.3],
  "top_k": 10
}' localhost:50050 akidb.v1.Akidb/Search
```

#### Delete

Delete a vector by ID.

```protobuf
rpc Delete(DeleteRequest) returns (DeleteResponse);

message DeleteRequest {
  string collection = 1;
  string id = 2;
}

message DeleteResponse {
  bool success = 1;
  bool found = 2;
}
```

#### Get

Retrieve a vector by ID.

```protobuf
rpc Get(GetRequest) returns (GetResponse);

message GetRequest {
  string collection = 1;
  string id = 2;
}

message GetResponse {
  string id = 1;
  repeated float vector = 2;
  map<string, string> metadata = 3;
  bool found = 4;
}
```

#### InsertBatch

Insert multiple vectors in a batch.

```protobuf
rpc InsertBatch(InsertBatchRequest) returns (InsertBatchResponse);

message InsertBatchRequest {
  string collection = 1;
  repeated InsertItem items = 2;
}

message InsertItem {
  string id = 1;
  repeated float vector = 2;
  map<string, string> metadata = 3;
}

message InsertBatchResponse {
  uint32 success_count = 1;
  uint32 failure_count = 2;
  repeated string failed_ids = 3;
}
```

#### SearchBatch

Execute multiple searches in a batch.

```protobuf
rpc SearchBatch(SearchBatchRequest) returns (SearchBatchResponse);

message SearchBatchRequest {
  string collection = 1;
  repeated SearchQuery queries = 2;
  uint32 top_k = 3;
}

message SearchBatchResponse {
  repeated SearchResponse results = 1;
}
```

#### Health

Health check endpoint.

```protobuf
rpc Health(HealthRequest) returns (HealthResponse);

message HealthRequest {}

message HealthResponse {
  bool healthy = 1;
  string version = 2;
  uint64 uptime_seconds = 3;
}
```

#### GetClusterState

Get cluster topology and metrics (coordinator only).

```protobuf
rpc GetClusterState(GetClusterStateRequest) returns (GetClusterStateResponse);

message GetClusterStateRequest {}

message GetClusterStateResponse {
  repeated CoordinatorNode coordinators = 1;
  repeated ShardNode shards = 2;
  optional string leader_id = 3;
  string local_peer_id = 4;
  ClusterMetrics metrics = 5;
}

message CoordinatorNode {
  string id = 1;
  string peer_id = 2;
  string address = 3;
  bool is_leader = 4;
  bool is_self = 5;
  NodeStatus status = 6;
}

message ShardNode {
  string id = 1;
  string address = 2;
  uint64 vector_count = 3;
  float health_score = 4;
  optional float gpu_memory_percent = 5;
  optional float temperature = 6;
  NodeStatus status = 7;
}

message ClusterMetrics {
  double qps = 1;
  double p50_latency_ms = 2;
  double p95_latency_ms = 3;
  double p99_latency_ms = 4;
  float coverage = 5;
  float backpressure = 6;
  bool within_slo = 7;
}

enum NodeStatus {
  NODE_STATUS_UNKNOWN = 0;
  NODE_STATUS_HEALTHY = 1;
  NODE_STATUS_UNHEALTHY = 2;
}
```

---

## Deployment Guide

### Production Deployment (Jetson Thor)

#### Prerequisites

- 2+ NVIDIA Jetson Thor nodes
- Network connectivity between nodes
- MinIO or S3-compatible storage for snapshots

#### Node Setup

1. **Install dependencies:**
   ```bash
   sudo apt update
   sudo apt install -y build-essential librocksdb-dev protobuf-compiler
   ```

2. **Build with GPU support:**
   ```bash
   cargo build --release --features gpu
   ```

3. **Install binaries:**
   ```bash
   sudo cp target/release/akidb-server /usr/local/bin/
   sudo cp target/release/akidb-coordinator /usr/local/bin/akidb-coordinator-new
   sudo cp target/release/akidb-tui /usr/local/bin/
   ```

4. **Create configuration:**
   ```bash
   sudo mkdir -p /opt/akidb/config
   sudo cp config/default.toml /opt/akidb/config/
   sudo cp config/tui.json /opt/akidb/config/
   ```

5. **Create data directories:**
   ```bash
   sudo mkdir -p /var/lib/akidb/{data,wal}
   sudo chown -R $USER:$USER /var/lib/akidb
   ```

#### Systemd Services

**Shard Service (`/etc/systemd/system/akidb-shard.service`):**
```ini
[Unit]
Description=AkiDB Shard Server
After=network.target

[Service]
Type=simple
User=devop
ExecStart=/usr/local/bin/akidb-server \
    --listen 0.0.0.0:50051 \
    --config /opt/akidb/config/default.toml
Restart=always
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

**Coordinator Service (`/etc/systemd/system/akidb-coordinator.service`):**
```ini
[Unit]
Description=AkiDB Coordinator Service
After=network.target

[Service]
Type=simple
User=devop
ExecStart=/usr/local/bin/akidb-coordinator-new \
    --listen 0.0.0.0:50050 \
    --shards 192.168.1.61:50051,192.168.1.62:50051 \
    --pool-size 4 \
    --timeout 5000 \
    --log-level info
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**Enable and start services:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable akidb-shard akidb-coordinator
sudo systemctl start akidb-shard akidb-coordinator
```

### Two-Node Cluster Example

**Thor 1 (192.168.1.61):**
- Shard server on port 50051
- Coordinator on port 50050

**Thor 2 (192.168.1.62):**
- Shard server on port 50051
- Coordinator on port 50050 (for HA)

Both coordinators know about both shards and can handle requests independently.

---

## TUI Dashboard Guide

### Launching the Dashboard

```bash
# Auto-discovery (uses config file)
akidb-tui

# Explicit coordinator
akidb-tui --coordinator 192.168.1.61:50050

# Test connection (no TUI)
akidb-tui --test-connection --coordinator 127.0.0.1:50050

# Mock mode for testing
akidb-tui --mock
```

### Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                    AkiDB Thor Edition TUI                        │
├─────────────────────────────────────────────────────────────────┤
│  Topology                    │  Health                          │
│  ─────────                   │  ──────                          │
│  Coordinators:               │  shard-0  ████████████  95%      │
│  ● coord-0 (leader)          │  shard-1  ██████████░░  92%      │
│    192.168.1.61:50050        │                                  │
│  ○ coord-1                   │  QPS: 125.3                      │
│    192.168.1.62:50050        │  P50: 22.5ms  P95: 38.2ms        │
│                              │  Coverage: 100%                  │
│  Shards:                     │  Backpressure: 5%                │
│  ● shard-0  38 vectors       │                                  │
│  ● shard-1  42 vectors       │  [Within SLO ✓]                  │
├─────────────────────────────────────────────────────────────────┤
│  Status: Connected to 192.168.1.61:50050                        │
│  Press 'q' to quit, '?' for help                                │
└─────────────────────────────────────────────────────────────────┘
```

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `q` / `Ctrl+C` | Quit |
| `?` / `h` | Show help |
| `↑` / `k` | Move selection up |
| `↓` / `j` | Move selection down |
| `Tab` | Switch panel |
| `r` | Force refresh |

### Configuration File

The TUI loads configuration from these locations (in order):
1. `/opt/akidb/config/tui.json`
2. `/opt/akidb/config/tui.toml`
3. `/etc/akidb/tui.toml`
4. `~/.config/akidb/tui.toml`
5. `./config/tui.toml`

---

## Operations Guide

### Monitoring

#### Key Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| QPS | Queries per second | Application-dependent |
| P50 Latency | Median latency | < 25ms |
| P95 Latency | 95th percentile latency | < 50ms |
| Coverage | Fraction of shards responding | 100% |
| Backpressure | Request queue pressure | < 10% |

#### Health Checks

```bash
# Via gRPC
grpcurl -plaintext localhost:50051 akidb.v1.Akidb/Health

# Via TUI test mode
akidb-tui --test-connection --coordinator 127.0.0.1:50050
```

### Backup and Recovery

#### Manual Snapshot

```bash
# Trigger snapshot via MinIO CLI
mc cp /var/lib/akidb/data minio/akidb-snapshots/manual-$(date +%Y%m%d)/
```

#### Restore from Snapshot

```bash
# Stop the shard
sudo systemctl stop akidb-shard

# Restore data
mc cp minio/akidb-snapshots/latest/ /var/lib/akidb/data/

# Start the shard
sudo systemctl start akidb-shard
```

### Index Rebuild

Tombstones accumulate over time. Periodic rebuilds compact the index:

```bash
# Rebuilds happen automatically based on tombstone ratio
# Configure in default.toml:
[index]
rebuild_tombstone_threshold = 0.2  # Rebuild when 20% tombstones
```

---

## Troubleshooting

### Common Issues

#### Connection Refused

**Symptom:** `Connection refused` when connecting to coordinator or shard.

**Solutions:**
1. Check service is running: `systemctl status akidb-coordinator`
2. Check firewall: `sudo ufw allow 50050/tcp`
3. Verify bind address in config is `0.0.0.0`, not `127.0.0.1`

#### GetClusterState Unimplemented

**Symptom:** TUI shows "GetClusterState RPC failed: Unimplemented"

**Solution:** The coordinator binary is outdated. Rebuild and redeploy:
```bash
cargo build --release -p akidb-coordinator
sudo systemctl stop akidb-coordinator
sudo cp target/release/akidb-coordinator /usr/local/bin/akidb-coordinator-new
sudo systemctl start akidb-coordinator
```

#### TUI "No such device or address"

**Symptom:** Error when running TUI remotely.

**Solution:** The TUI requires a proper TTY. SSH directly to the machine:
```bash
ssh devop@192.168.1.61
akidb-tui
```

Or use `--test-connection` mode for headless testing.

#### High Latency

**Symptom:** P95 latency exceeds 50ms SLO.

**Solutions:**
1. Reduce `nprobe` in config (tradeoff: lower recall)
2. Increase `memory_fraction` for GPU index
3. Check GPU temperature (throttling occurs above 85°C)
4. Review backpressure metrics - may need more shards

#### Partial Coverage

**Symptom:** Coverage below 100% in search results.

**Solutions:**
1. Check shard health in TUI
2. Verify network connectivity between coordinator and shards
3. Check shard logs: `journalctl -u akidb-shard -f`

### Log Locations

| Component | Log Command |
|-----------|-------------|
| Shard | `journalctl -u akidb-shard -f` |
| Coordinator | `journalctl -u akidb-coordinator -f` |
| TUI | Logs to stderr (redirect with `2>tui.log`) |

---

## Glossary

| Term | Definition |
|------|------------|
| **Coordinator** | Stateless service that routes queries to shards and merges results |
| **Coverage** | Fraction of shards that responded to a query (1.0 = all shards) |
| **Fan-out** | Pattern of sending a query to all shards in parallel |
| **FAISS** | Facebook AI Similarity Search - GPU-accelerated vector index library |
| **IVF** | Inverted File Index - FAISS index type that partitions vectors into clusters |
| **Min-heap** | Data structure used to efficiently merge top-K results from multiple shards |
| **nprobe** | Number of IVF clusters to search; higher = more accurate but slower |
| **Shard** | A partition of the vector index stored on a single node |
| **SLO** | Service Level Objective - target latency guarantee (e.g., P95 < 50ms) |
| **Tombstone** | Soft-deleted vector; excluded from search via bitset until index rebuild |
| **WAL** | Write-Ahead Log - ensures durability by logging writes before applying |

---

## Feature Flags

| Flag | Description |
|------|-------------|
| `cpu` | CPU-only FAISS implementation (development) |
| `gpu` | CUDA-enabled GPU FAISS (production on Jetson Thor) |

Build with features:
```bash
cargo build --features cpu      # Development
cargo build --features gpu      # Production
```

---

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `scripts/thor-validate.sh` | Validate Jetson Thor hardware |
| `scripts/faiss-benchmark.sh` | Run FAISS GPU benchmarks |
| `scripts/minio-setup.sh` | Setup MinIO for snapshot storage |
| `scripts/build-on-thor.sh` | Build on Jetson Thor with GPU support |

---

## Support

- **Issues:** https://github.com/your-org/akidb/issues
- **Documentation:** This file and `CLAUDE.md` in repository root

---

*Generated by AkiDB Documentation Agent - January 22, 2026*
