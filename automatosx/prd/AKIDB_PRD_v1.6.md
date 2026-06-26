# AkiDB Thor Edition - Product Requirements Document (PRD)
## Version 1.6

**Version:** 1.6
**Date:** 2026-01-21
**Author:** AkiDB Team
**Status:** Approved
**Changes from v1.5:** Docker deployment (was Podman), security hardening requirements, version unified
**Review:** Multi-model synthesis (Claude, Gemini, Grok) - Container strategy re-evaluation

---

## Change Log from v1.5

| Section | Change | Rationale |
|---------|--------|-----------|
| §11 | Docker + docker-compose (was Podman + quadlets) | NVIDIA Jetson Thor ecosystem alignment |
| §15 | Enhanced Docker security requirements | ADR-021 hardening controls |
| §16 | Updated production checklist | Docker-specific validation |
| All | Version unified to 1.6 | Align PRD, ADR, Implementation Plan |

---

## Executive Summary

### Product Vision

**AkiDB Thor Edition** is a production-ready distributed vector search engine for **NVIDIA Jetson Thor** edge clusters with:
- **Hybrid document ingestion** (Rust orchestration + Python parsing)
- **Fault-tolerant architecture** (circuit breaker, backpressure, memory coordination)
- **30-minute batch SLO** from upload to searchable
- **Docker-based deployment** aligned with NVIDIA's official Jetson documentation

### v1.6 Key Features

| Feature | Description |
|---------|-------------|
| **Docker Deployment** | docker-compose with nvidia-container-runtime (NVIDIA recommended) |
| **Hybrid Ingestion** | Rust orchestrator (60-70%) + Python parser (30-40%) |
| **Circuit Breaker** | Python parser failures isolated from orchestrator |
| **Backpressure** | AkiDB saturation throttles NATS consumption |
| **Memory Coordination** | Unified memory pressure detection via tegrastats |
| **Security Hardening** | Non-root containers, capability drop, secrets management |

---

## 11. Deployment Architecture (v1.6)

### 11.1 Container Runtime

**Decision:** Docker + docker-compose with nvidia-container-runtime

**Rationale:**
- NVIDIA official Thor documentation specifies Docker
- jetson-containers ecosystem provides tested vLLM/CUDA 13.x configurations
- GPU passthrough reliability is highest on vendor-supported configuration

```
┌─────────────────────────────────────────────────────────────────┐
│                    AKIDB THOR CLUSTER (v1.6)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Container Runtime: Docker with nvidia-container-runtime        │
│  Management: docker-compose.yml per node                        │
│  Orchestration: Ansible for multi-node deployment               │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Thor 1    │  │   Thor 2    │  │   Thor 3    │             │
│  │  (Shard 0)  │  │  (Shard 1)  │  │  (Shard 2)  │             │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤             │
│  │ akidb-shard │  │ akidb-shard │  │ akidb-shard │             │
│  │ (GPU)       │  │ (GPU)       │  │ (GPU)       │             │
│  │ ingestion-  │  │             │  │             │             │
│  │ orchestrator│  │             │  │             │             │
│  │ doc-parser  │  │             │  │             │             │
│  │ NATS (R1)   │  │ NATS (R2)   │  │ NATS (R3)   │             │
│  │ minio       │  │ minio       │  │ minio       │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│         │               │               │                       │
│         └───────────────┴───────────────┘                       │
│              NATS Raft Cluster (3-node)                         │
│              Quorum: 2 | Can lose: 1 node                       │
│                         │                                       │
│                    ┌─────────────┐                              │
│                    │   Thor 4    │                              │
│                    │(Coordinator)│                              │
│                    ├─────────────┤                              │
│                    │akidb-coord  │                              │
│                    │upload-gateway│                             │
│                    │(NATS client)│ ← Connects to Thor 1-3      │
│                    │minio        │                              │
│                    └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 11.2 Docker Configuration

#### /etc/docker/daemon.json (All Nodes)

```json
{
  "default-runtime": "nvidia",
  "runtimes": {
    "nvidia": {
      "path": "nvidia-container-runtime",
      "runtimeArgs": []
    }
  },
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  },
  "storage-driver": "overlay2"
}
```

### 11.3 Service Distribution

| Node | Services | GPU Required |
|------|----------|--------------|
| Thor 1 | akidb-shard, ingestion-orchestrator, doc-parser, nats, minio | Yes (shard + orchestrator) |
| Thor 2 | akidb-shard, nats, minio | Yes |
| Thor 3 | akidb-shard, nats, minio | Yes |
| Thor 4 | akidb-coordinator, upload-gateway, minio | No |

---

## 12. Docker Compose Specifications

### 12.1 Shard Node (Thor 1-3)

```yaml
# deploy/docker/docker-compose.shard.yml
version: "3.8"

services:
  akidb-shard:
    image: ghcr.io/akidb/akidb-server:${AKIDB_VERSION:-latest}
    container_name: akidb-shard
    restart: unless-stopped
    user: "1000:1000"
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    deploy:
      resources:
        limits:
          memory: 48G
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
    environment:
      - AKIDB_ROLE=shard
      - AKIDB_SHARD_ID=${SHARD_ID}
      - AKIDB_DATA_DIR=/data
      - RUST_LOG=info
    volumes:
      - akidb-data:/data
      - ./config/akidb.toml:/etc/akidb/akidb.toml:ro
    ports:
      - "50051:50051"
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=:50051"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 60s
    networks:
      - akidb-network

  nats:
    image: nats:2.10-alpine
    container_name: nats
    restart: unless-stopped
    user: "1000:1000"
    security_opt:
      - no-new-privileges:true
    command: ["-c", "/etc/nats/nats.conf"]
    volumes:
      - nats-data:/data
      - ./config/nats.conf:/etc/nats/nats.conf:ro
    ports:
      - "4222:4222"
      - "6222:6222"
      - "8222:8222"
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8222/healthz"]
      interval: 10s
      timeout: 5s
      retries: 3
    networks:
      - akidb-network

  minio:
    image: minio/minio:latest
    container_name: minio
    restart: unless-stopped
    user: "1000:1000"
    security_opt:
      - no-new-privileges:true
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER_FILE=/run/secrets/minio-root-user
      - MINIO_ROOT_PASSWORD_FILE=/run/secrets/minio-root-password
    secrets:
      - minio-root-user
      - minio-root-password
    volumes:
      - minio-data:/data
    ports:
      - "9000:9000"
      - "9001:9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - akidb-network

volumes:
  akidb-data:
  nats-data:
  minio-data:

networks:
  akidb-network:
    driver: bridge

secrets:
  minio-root-user:
    file: ./secrets/minio-root-user
  minio-root-password:
    file: ./secrets/minio-root-password
```

### 12.2 Ingestion Services (Thor 1 Only)

```yaml
# deploy/docker/docker-compose.ingestion.yml
version: "3.8"

services:
  ingestion-orchestrator:
    image: ghcr.io/akidb/ingestion-orchestrator:${AKIDB_VERSION:-latest}
    container_name: ingestion-orchestrator
    restart: unless-stopped
    user: "1000:1000"
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    environment:
      - NATS_URL=nats://nats:4222
      - MINIO_ENDPOINT=minio:9000
      - AKIDB_COORDINATOR=akidb-coordinator:50051
      - TENSORRT_URL=http://tensorrt:8001
      - DOC_PARSER_URL=http://doc-parser:8080
      - CIRCUIT_BREAKER_THRESHOLD=3
      - CIRCUIT_BREAKER_RESET_SECS=30
      - BACKPRESSURE_LATENCY_THRESHOLD_MS=500
      - MEMORY_PAUSE_THRESHOLD_PCT=70
      - RUST_LOG=info
    secrets:
      - minio-access-key
      - minio-secret-key
    volumes:
      - orchestrator-state:/var/lib/akidb
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9090/health"]
      interval: 10s
      timeout: 5s
      retries: 3
    networks:
      - akidb-network
    depends_on:
      nats:
        condition: service_healthy
      doc-parser:
        condition: service_healthy

  doc-parser:
    image: ghcr.io/akidb/doc-parser:${AKIDB_VERSION:-latest}
    container_name: doc-parser
    restart: unless-stopped
    user: "1000:1000"
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    deploy:
      resources:
        limits:
          memory: 2G
    environment:
      - PYTHONUNBUFFERED=1
      - MAX_FILE_SIZE_MB=100
      - PARSE_TIMEOUT_SECS=60
    tmpfs:
      - /tmp:size=500M
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - akidb-network

volumes:
  orchestrator-state:

secrets:
  minio-access-key:
    file: ./secrets/minio-access-key
  minio-secret-key:
    file: ./secrets/minio-secret-key
```

### 12.3 Coordinator Node (Thor 4)

```yaml
# deploy/docker/docker-compose.coordinator.yml
version: "3.8"

services:
  akidb-coordinator:
    image: ghcr.io/akidb/akidb-coordinator:${AKIDB_VERSION:-latest}
    container_name: akidb-coordinator
    restart: unless-stopped
    user: "1000:1000"
    read_only: true
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    deploy:
      resources:
        limits:
          memory: 4G
    environment:
      - AKIDB_ROLE=coordinator
      - AKIDB_SHARDS=thor1:50051,thor2:50051,thor3:50051
      - RUST_LOG=info
    volumes:
      - ./config/coordinator.toml:/etc/akidb/coordinator.toml:ro
    tmpfs:
      - /tmp:size=100M
    ports:
      - "50051:50051"
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=:50051"]
      interval: 10s
      timeout: 5s
      retries: 3
    networks:
      - akidb-network

  upload-gateway:
    image: ghcr.io/akidb/upload-gateway:${AKIDB_VERSION:-latest}
    container_name: upload-gateway
    restart: unless-stopped
    user: "1000:1000"
    read_only: true
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    deploy:
      resources:
        limits:
          memory: 1G
    environment:
      - MINIO_ENDPOINT=minio:9000
      - UPLOAD_BUCKET=uploads
      - PRESIGNED_URL_EXPIRY=900
      - MAX_FILE_SIZE_MB=100
    secrets:
      - minio-access-key
      - minio-secret-key
    tmpfs:
      - /tmp:size=100M
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 3
    networks:
      - akidb-network

  minio:
    image: minio/minio:latest
    container_name: minio
    restart: unless-stopped
    user: "1000:1000"
    security_opt:
      - no-new-privileges:true
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER_FILE=/run/secrets/minio-root-user
      - MINIO_ROOT_PASSWORD_FILE=/run/secrets/minio-root-password
    secrets:
      - minio-root-user
      - minio-root-password
    volumes:
      - minio-data:/data
    ports:
      - "9000:9000"
      - "9001:9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - akidb-network

volumes:
  minio-data:

networks:
  akidb-network:
    driver: bridge

secrets:
  minio-root-user:
    file: ./secrets/minio-root-user
  minio-root-password:
    file: ./secrets/minio-root-password
  minio-access-key:
    file: ./secrets/minio-access-key
  minio-secret-key:
    file: ./secrets/minio-secret-key
```

---

## 13. Ingestion Pipeline Architecture

*(Unchanged from v1.5)*

### 13.1 Overview

The v1.6 Hybrid Ingestion Pipeline includes:

1. **Rust Orchestrator** - Memory-efficient, long-running core
2. **Python Parser Service** - Complex formats with fault isolation
3. **Circuit Breaker** - Prevents cascade failures
4. **Backpressure Controller** - Throttles when AkiDB saturated
5. **Memory Coordinator** - Manages unified memory contention
6. **Semantic Chunker** - Sentence-boundary-aware splitting
7. **Dynamic Batcher** - Queue-depth-adaptive embedding batches

### 13.2 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID INGESTION PIPELINE (v1.6)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  MinIO Event                                                                │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    NATS JetStream (3-node)                           │   │
│  │  Stream: AKIDB_INGEST | Replicas: 3 | max_deliver: 3                │   │
│  └──────────────────────────────────┬──────────────────────────────────┘   │
│                                     │                                       │
│                                     ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    RUST ORCHESTRATOR (Docker container)              │   │
│  │                    Memory: 512MB-2GB (dynamic)                       │   │
│  │                                                                      │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  MEMORY COORDINATOR                             │ │   │
│  │  │  Monitor: tegrastats | Pause threshold: 70% unified memory     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                                     │                                │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  BACKPRESSURE CONTROLLER                        │ │   │
│  │  │  Monitor: AkiDB insert latency | Pause if >500ms               │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                                     │                                │   │
│  │  ┌──────────────────────────────────┴───────────────────────────┐   │   │
│  │  │                    FORMAT ROUTER                              │   │   │
│  │  │  Route by extension → Rust (60-70%) or Python (30-40%)       │   │   │
│  │  └─────────────┬────────────────────────────┬───────────────────┘   │   │
│  │                │                            │                        │   │
│  │                ▼                            ▼                        │   │
│  │  ┌─────────────────────────┐  ┌─────────────────────────────────┐  │   │
│  │  │   RUST PARSERS (60-70%) │  │   CIRCUIT BREAKER               │  │   │
│  │  │   • JSON (serde_json)   │  │   ┌───────────────────────────┐ │  │   │
│  │  │   • CSV (csv crate)     │  │   │ State: CLOSED/OPEN/HALF  │ │  │   │
│  │  │   • HTML (scraper)      │  │   │ Failures: 0/3            │ │  │   │
│  │  │   • XML (quick-xml)     │  │   │ Reset: 30s               │ │  │   │
│  │  │   • XLSX (calamine)     │  │   └───────────┬───────────────┘ │  │   │
│  │  │   • DOCX-simple (docx-rs)│ │               │                  │  │   │
│  │  └──────────┬──────────────┘  │               ▼                  │  │   │
│  │             │                  │   ┌───────────────────────────┐ │  │   │
│  │             │                  │   │ PYTHON PARSER (30-40%)   │ │  │   │
│  │             │                  │   │ (Docker container)       │ │  │   │
│  │             │                  │   │ • PDF (pdfplumber)       │ │  │   │
│  │             │                  │   │ • DOCX-complex           │ │  │   │
│  │             │                  │   │ • ENL (custom)           │ │  │   │
│  │             │                  │   │ Memory: 2GB | Timeout: 60s│ │  │   │
│  │             │                  │   └───────────┬───────────────┘ │  │   │
│  │             │                  └───────────────┼─────────────────┘  │   │
│  │             │                                  │                     │   │
│  │             └──────────────┬──────────────────┘                     │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  SEMANTIC CHUNKER                               │ │   │
│  │  │  Target: ~512 tokens | Boundary: sentence | Overlap: 20-50     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  DYNAMIC BATCHER                                │ │   │
│  │  │  Range: 16-64 | Based on: queue depth + GPU utilization        │ │   │
│  │  │                                                                 │ │   │
│  │  │                  TensorRT-LLM Embedding                         │ │   │
│  │  │                  Model: BGE-base-en-v1.5 (768-dim)             │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  IDEMPOTENCY LAYER                              │ │   │
│  │  │  Key: content_hash | Dedup: skip if exists                     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  AKIDB GRPC CLIENT (tonic)                      │ │   │
│  │  │  Backpressure signal → throttle NATS ack rate                  │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 14. Monitoring Requirements

*(Unchanged from v1.5)*

### 14.1 Required Metrics

```yaml
# Ingestion Throughput
ingestion_documents_total{format, status}
ingestion_chunks_total{format}
embedding_batches_total{size_bucket}

# Latency (histograms)
ingestion_e2e_duration_seconds{format, quantile}
parser_duration_seconds{format, parser}
embedding_batch_duration_seconds{quantile}

# Resource Utilization
memory_usage_bytes{component}
gpu_utilization_percent
gpu_memory_used_bytes
unified_memory_used_bytes

# Queue Health
nats_pending_messages{stream}
dead_letter_queue_depth
queue_processing_rate

# Resilience State
circuit_breaker_state{service}
backpressure_active
memory_pressure_level

# Docker-specific (NEW in v1.6)
docker_container_cpu_usage_seconds_total
docker_container_memory_usage_bytes
docker_container_restart_count
```

### 14.2 Alerting Thresholds

| Alert | Condition | Severity |
|-------|-----------|----------|
| SLO Breach | `ingestion_e2e_duration_seconds{p95} > 1800` | Critical |
| High DLQ | `dead_letter_queue_depth > 100` | Warning |
| Memory Pressure | `unified_memory_used_bytes > 0.7 * 64GB` | Warning |
| Circuit Open | `circuit_breaker_state == "open"` | Warning |
| Parser Down | `parser_failures_total rate > 10/min` | Critical |
| Queue Backlog | `nats_pending_messages > 10000` | Warning |
| Container Restart | `docker_container_restart_count > 3 in 1h` | Warning |

---

## 15. Security Requirements (v1.6)

### 15.1 Docker Security Hardening

| Control | Requirement | Validation |
|---------|-------------|------------|
| Non-root containers | `user: "1000:1000"` in all services | `docker exec <c> id` returns uid=1000 |
| Read-only filesystem | `read_only: true` where possible | Write attempts fail |
| No privilege escalation | `security_opt: no-new-privileges:true` | seccomp audit |
| Capability drop | `cap_drop: ALL` + explicit adds | `docker inspect` shows minimal caps |
| Secrets management | No secrets in environment variables | `docker inspect` shows no sensitive env |
| Resource limits | Memory limits on all containers | `docker stats` shows limits |
| Log rotation | `max-size: 100m, max-file: 3` | `/var/lib/docker/containers` bounded |

### 15.2 Pre-signed URL Security

| Control | Specification |
|---------|---------------|
| URL expiry | 15 minutes (configurable) |
| Permissions | PUT-only to specific key |
| Size validation | Reject files > 100MB |
| Content-Type | Validate matches declared type |
| Path traversal | Sanitize filenames before storage |

### 15.3 Network Security

```yaml
networks:
  akidb-internal:
    driver: bridge
    internal: true  # No external access for internal services
  akidb-external:
    driver: bridge
    # Only coordinator and upload-gateway exposed
```

---

## 16. Production Readiness Checklist (v1.6)

### Critical (Must Pass for Release)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| C-01 | Docker + nvidia-runtime installed | Infra | `docker run --gpus all nvidia/cuda nvidia-smi` |
| C-02 | NATS 3-node cluster deployed | Infra | `nats cluster info` shows 3 nodes |
| C-03 | Circuit breaker implemented | Dev | Unit tests for all state transitions |
| C-04 | Backpressure mechanism tested | QA | Load test: AkiDB saturated → queue bounded |
| C-05 | Memory coordinator active | Dev | tegrastats integration verified |
| C-06 | Core metrics exported | Ops | Prometheus scraping all targets |
| C-07 | 30-min SLO validated | QA | End-to-end test passes |
| C-08 | GPU passthrough working | Infra | `nvidia-smi` inside container |
| C-09 | Docker security hardening | Security | ADR-021 checklist passed |

### High Priority (Strongly Recommended)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| H-01 | Semantic chunking | Dev | A/B test vs fixed chunking |
| H-02 | Dynamic batching | Dev | Queue depth → batch size logged |
| H-03 | XLSX in Rust (calamine) | Dev | Parse 1000 XLSX files |
| H-04 | Idempotency layer | Dev | Duplicate detection tests |
| H-05 | Document state tracking | Dev | `GET /status/{id}` returns history |
| H-06 | Pre-signed URL hardening | Security | Penetration test passed |
| H-07 | GPU metrics via DCGM | Ops | Grafana dashboard working |
| H-08 | jetson-containers base image | Dev | vLLM builds successfully |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial PRD |
| 1.1 | 2025-01-20 | AkiDB Team | SLO boundaries, consistency guarantees |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets) |
| 1.3 | 2026-01-21 | AkiDB Team | Ingestion pipeline (Python sidecar) |
| 1.4 | 2026-01-21 | AkiDB Team | Hybrid ingestion (Rust orchestrator + Python parser) |
| 1.5 | 2026-01-21 | AkiDB Team | NATS 3-node, resilience patterns, monitoring |
| 1.6 | 2026-01-21 | AkiDB Team | **Docker deployment**, security hardening, version unified |

---

*End of PRD v1.6*
