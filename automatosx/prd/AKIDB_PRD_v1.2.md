# AkiDB Thor Edition - Product Requirements Document (PRD)
## Version 1.2

**Version:** 1.2
**Date:** 2026-01-21
**Author:** AkiDB Team
**Status:** Approved
**Changes from v1.1:** Added container orchestration requirements (Podman + quadlets), deployment architecture, infrastructure requirements
**Review:** Multi-model synthesis (Claude, Gemini, Grok) addressing deployment architecture

---

## Change Log from v1.1

| Section | Change | Rationale |
|---------|--------|-----------|
| §11 | NEW: Deployment Architecture | Container strategy decision |
| §11 | Podman + quadlets specified | Memory efficiency for edge GPU workloads |
| §12 | NEW: Infrastructure Requirements | Hardware and software prerequisites |
| §7 | Updated deployment-related FRs | Align with container strategy |
| §8 | Updated operational NFRs | Container-based recovery targets |

---

## Table of Contents

*Sections 1-10 remain largely unchanged from v1.1. New sections 11-12 added.*

1. [Executive Summary](#1-executive-summary) *(minor update)*
2-10. *(See v1.1 for unchanged sections)*
11. [Deployment Architecture (NEW)](#11-deployment-architecture)
12. [Infrastructure Requirements (NEW)](#12-infrastructure-requirements)
13-19. *(Renumbered from v1.1)*

---

## 1. Executive Summary

### 1.1 Product Vision

**AkiDB Thor Edition** is a distributed vector search engine for **NVIDIA Jetson Thor** edge clusters.

### 1.2 Key Performance Targets (v1.2)

> **IMPORTANT:** All targets apply ONLY at the reference configuration. See §8 for SLO boundary conditions.

| Metric | Target | Reference Config | Validation Status |
|--------|--------|------------------|-------------------|
| E2E Search Latency (P95) | < 50ms | D=768, N=1M, topK=10 | **ESTIMATED** |
| FAISS Search (per shard, P95) | < 10ms | nprobe=32, batch=1 | **ESTIMATED** |
| Embedding Latency (P95) | < 10ms | TensorRT-LLM | **ESTIMATED** |
| Throughput | 100 QPS | Reference config | **ESTIMATED** |
| Recall@10 | > 95% | Reference config | **ESTIMATED** |
| Recovery Time (RTO) | < 60s | 1M vectors | **ESTIMATED** |
| Read-Your-Writes Visibility | < 100ms | After insert success | **SPECIFIED** |
| **Container Restart** | < 30s | Podman + systemd | **SPECIFIED** |
| **Rolling Update** | Zero downtime | Per-node sequential | **SPECIFIED** |

### 1.3 v1.2 Key Additions

1. **Container Orchestration:** Podman + systemd quadlets for edge deployment
2. **Deployment Architecture:** Quadlet configurations for shard/coordinator
3. **Infrastructure Requirements:** Hardware, software, and network prerequisites
4. **Dev/Prod Parity:** Docker Compose for development, Podman for production

---

## 11. Deployment Architecture (NEW in v1.2)

### 11.1 Container Strategy

AkiDB Thor Edition uses **Podman with systemd quadlets** for container orchestration on edge devices.

#### Why Podman + Quadlets

| Requirement | Podman + Quadlets | Kubernetes | Docker Compose |
|-------------|-------------------|------------|----------------|
| Memory overhead | ~0 (daemonless) | ~500MB-1GB | ~100-200MB |
| GPU memory for FAISS | Maximized | Reduced | Slightly reduced |
| Operational complexity | Low-moderate | High | Low |
| Recovery mechanism | systemd native | Pod restart | Manual/external |
| Multi-node orchestration | Ansible/scripts | Built-in | Manual |

#### Decision Rationale

1. **Memory efficiency is critical**: FAISS GPU workloads require ~38GB (0.6 × 64GB). Every MB saved for orchestration goes to vector indexing.
2. **Single-shard-per-node design**: Kubernetes scheduling benefits don't apply when each node has exactly one shard.
3. **Familiar tooling**: systemd integration means `journalctl`, `systemctl` work as expected.
4. **Rootless security**: Edge devices may have physical access risks; rootless containers reduce attack surface.

### 11.2 Deployment Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    AKIDB THOR CLUSTER                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Thor 1    │  │   Thor 2    │  │   Thor 3    │         │
│  │  (Shard 0)  │  │  (Shard 1)  │  │  (Shard 2)  │         │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤         │
│  │ akidb-shard │  │ akidb-shard │  │ akidb-shard │         │
│  │   (Podman)  │  │   (Podman)  │  │   (Podman)  │         │
│  │  GPU: 0.6   │  │  GPU: 0.6   │  │  GPU: 0.6   │         │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤         │
│  │    minio    │  │    minio    │  │    minio    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│         │               │               │                   │
│         └───────────────┴───────────────┘                   │
│                         │                                   │
│                    ┌─────────────┐                          │
│                    │   Thor 4    │                          │
│                    │(Coordinator)│                          │
│                    ├─────────────┤                          │
│                    │akidb-coord  │                          │
│                    │  (Podman)   │                          │
│                    │  No GPU     │                          │
│                    ├─────────────┤                          │
│                    │   minio     │                          │
│                    └─────────────┘                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 11.3 Container Specifications

#### Shard Server Container

| Attribute | Specification |
|-----------|---------------|
| Image | `ghcr.io/akidb/akidb-server:latest` |
| Base | `debian:bookworm-slim` |
| GPU | Required (NVIDIA CDI passthrough) |
| Ports | 50051 (gRPC), 9090 (metrics) |
| Volumes | `/var/lib/akidb` (data), `/etc/akidb` (config) |
| Network | Host mode |
| User | Non-root (UID 1000) |
| Health check | `grpc_health_probe -addr=localhost:50051` |
| Restart policy | Always (via systemd) |

#### Coordinator Container

| Attribute | Specification |
|-----------|---------------|
| Image | `ghcr.io/akidb/akidb-coordinator:latest` |
| Base | `debian:bookworm-slim` |
| GPU | Not required |
| Ports | 50051 (gRPC), 9090 (metrics) |
| Volumes | `/etc/akidb` (config, read-only) |
| Network | Host mode |
| User | Non-root (UID 1000) |
| Health check | `grpc_health_probe -addr=localhost:50051` |
| Restart policy | Always (via systemd) |

#### MinIO Container

| Attribute | Specification |
|-----------|---------------|
| Image | `minio/minio:latest` |
| Ports | 9000 (S3 API), 9001 (console) |
| Volumes | `/var/lib/minio` (data) |
| Network | Host mode |
| Mode | Distributed (4 nodes) |
| Health check | `curl -f http://localhost:9000/minio/health/live` |

### 11.4 Quadlet File Structure

```
/etc/containers/systemd/
├── akidb-shard.container      # Shard server quadlet
├── akidb-coordinator.container # Coordinator quadlet (node 4 only)
└── minio.container            # MinIO quadlet

/etc/akidb/
├── akidb.toml                 # Shard configuration
└── coordinator.toml           # Coordinator configuration

/var/lib/akidb/
├── rocksdb/                   # RocksDB data
├── wal/                       # Write-ahead log
└── snapshots/                 # Local snapshot cache
```

### 11.5 Deployment Workflow

#### Initial Deployment

```
1. Provision Thor hardware (4 nodes)
2. Install JetPack + NVIDIA Container Toolkit
3. Install Podman
4. Configure NVIDIA CDI for GPU passthrough
5. Deploy quadlet files via Ansible
6. Start services: systemctl start akidb-shard minio
7. Verify health: grpc_health_probe -addr=localhost:50051
```

#### Rolling Update

```
For each node in [thor1, thor2, thor3, thor4]:
  1. Pull new image: podman pull ghcr.io/akidb/akidb-server:$VERSION
  2. Restart service: systemctl restart akidb-shard
  3. Wait for health check: grpc_health_probe (timeout 60s)
  4. If healthy: proceed to next node
  5. If unhealthy: rollback and abort
```

#### Rollback

```
1. Pull previous image: podman pull ghcr.io/akidb/akidb-server:$PREVIOUS
2. Restart service: systemctl restart akidb-shard
3. Verify health
```

### 11.6 Development Environment

For local development, use Docker Compose for parity with production containers:

| Environment | Tool | Purpose |
|-------------|------|---------|
| Local dev | Docker Compose | Same container images, fast iteration |
| CI/CD | Docker Compose | Integration testing |
| Staging | Podman + quadlets | Pre-production validation |
| Production | Podman + quadlets | Edge deployment |

```yaml
# docker-compose.yml provides dev environment
# Mirrors production container configuration
# GPU optional (CPU fallback for Mac development)
```

---

## 12. Infrastructure Requirements (NEW in v1.2)

### 12.1 Hardware Requirements

#### Jetson Thor Nodes (4 units)

| Component | Specification | Notes |
|-----------|---------------|-------|
| **CPU** | ARM64 (Thor SoC) | Blackwell architecture |
| **GPU** | Integrated (Thor) | CUDA 12.x, ~128 TOPS |
| **Memory** | 64GB unified | Shared CPU/GPU |
| **Storage** | 500GB NVMe SSD | For RocksDB + snapshots |
| **Network** | 10Gbps Ethernet | For cluster communication |

#### Network Infrastructure

| Component | Specification | Notes |
|-----------|---------------|-------|
| **Switch** | 10Gbps managed | VLAN support recommended |
| **Latency** | < 1ms intra-cluster | Critical for fan-out |
| **DNS** | Local resolver | Or /etc/hosts entries |

### 12.2 Software Requirements

#### Operating System

| Component | Version | Notes |
|-----------|---------|-------|
| **JetPack** | 6.2+ | NVIDIA Jetson SDK |
| **Ubuntu** | 22.04 LTS | JetPack base |
| **Kernel** | 5.15+ | Jetson-specific |

#### Container Runtime

| Component | Version | Notes |
|-----------|---------|-------|
| **Podman** | 4.0+ | Daemonless container engine |
| **NVIDIA Container Toolkit** | Latest | GPU passthrough |
| **CDI** | Enabled | Container Device Interface |

#### Development Tools

| Component | Version | Notes |
|-----------|---------|-------|
| **Rust** | 1.75+ | Nightly for some features |
| **CUDA** | 12.x | Must match JetPack |
| **protoc** | 3.x | Protocol buffers |

### 12.3 Software Dependencies (Container)

| Dependency | Version | Purpose |
|------------|---------|---------|
| RocksDB | 7.8+ | Metadata storage |
| grpc_health_probe | 0.4+ | Health checking |
| curl | Latest | MinIO health checks |

### 12.4 Network Ports

| Port | Protocol | Service | Direction |
|------|----------|---------|-----------|
| 50051 | TCP/gRPC | AkiDB API | Inbound |
| 9090 | TCP/HTTP | Prometheus metrics | Inbound |
| 8080 | TCP/HTTP | HTTP API (optional) | Inbound |
| 9000 | TCP/HTTP | MinIO S3 API | Inbound/Internal |
| 9001 | TCP/HTTP | MinIO Console | Inbound |

### 12.5 Storage Layout

```
/var/lib/akidb/           # 200GB minimum
├── rocksdb/              # ~50GB (metadata, ID mapping)
├── wal/                  # ~10GB (write-ahead log)
└── cache/                # ~50GB (snapshot cache)

/var/lib/minio/           # 200GB minimum
└── akidb-snapshots/      # FAISS index snapshots
```

### 12.6 Resource Limits

| Resource | Limit | Rationale |
|----------|-------|-----------|
| GPU memory | 60% (configurable) | Reserve for FAISS index |
| System memory | 90% | Leave headroom for OS |
| File descriptors | 65536 | High connection count |
| Memory lock | Unlimited | GPU memory pinning |

---

## 7. Functional Requirements (UPDATED)

### 7.6 Deployment Requirements (NEW in v1.2)

| ID | Requirement | Priority | Specification |
|----|-------------|----------|---------------|
| FR-D01 | Container-based deployment | P0 | Podman with systemd quadlets |
| FR-D02 | GPU passthrough | P0 | NVIDIA CDI for FAISS GPU |
| FR-D03 | Health checking | P0 | gRPC health probe integration |
| FR-D04 | Rolling updates | P0 | Zero-downtime sequential updates |
| FR-D05 | Rollback capability | P0 | Previous image tag restoration |
| FR-D06 | Log aggregation | P1 | journald to external collector |
| FR-D07 | Secrets management | P1 | systemd credentials or external vault |
| FR-D08 | Dev environment | P1 | Docker Compose for local dev |

---

## 8. Non-Functional Requirements (UPDATED)

### 8.6 Operational Requirements (NEW in v1.2)

| ID | Requirement | Target | Notes |
|----|-------------|--------|-------|
| NFR-O01 | Container start time | < 30s | Including GPU initialization |
| NFR-O02 | Service restart (systemd) | < 10s | After container ready |
| NFR-O03 | Rolling update duration | < 10 min | 4-node cluster |
| NFR-O04 | Rollback time | < 5 min | Per node |
| NFR-O05 | Log retention | 7 days | journald default |
| NFR-O06 | Metrics scrape interval | 15s | Prometheus default |

### 8.7 Security Requirements (NEW in v1.2)

| ID | Requirement | Target | Notes |
|----|-------------|--------|-------|
| NFR-S01 | Rootless containers | Required | Podman default |
| NFR-S02 | Read-only root filesystem | Recommended | Except data volumes |
| NFR-S03 | Non-root user | UID 1000 | Inside container |
| NFR-S04 | Network isolation | Host mode | For simplicity; consider CNI later |
| NFR-S05 | Secrets in files | Required | Not environment variables |

---

## 14. Success Metrics (UPDATED)

### 14.3 Operational Metrics (NEW in v1.2)

| Metric | Target | Phase |
|--------|--------|-------|
| Container startup time | < 30s | Phase 1+ |
| Rolling update success rate | > 99% | Phase 4 |
| Mean Time to Recovery (container) | < 60s | Phase 2+ |
| Deployment frequency | Weekly | Phase 4 |
| Change failure rate | < 5% | Phase 4 |

---

## 17. Risks and Mitigations (UPDATED)

### 17.2 Deployment Risks (NEW in v1.2)

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Podman/Jetson compatibility | Medium | High | Test CDI early in Phase 0 |
| GPU passthrough failures | Medium | High | Fallback to CPU mode |
| Rolling update failures | Low | Medium | Automated rollback script |
| Image registry unavailable | Low | Medium | Local image cache |
| systemd service conflicts | Low | Low | Dedicated quadlet directory |

---

## 18. Open Questions (UPDATED)

### 18.3 Deployment Questions (NEW in v1.2)

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q8 | Image registry location? | GHCR vs self-hosted | Phase 0 |
| Q9 | Secrets management approach? | systemd credentials vs Vault | Phase 1 |
| Q10 | Network mode (host vs bridge)? | Host (recommended) vs CNI | Phase 1 |
| Q11 | Log aggregation solution? | Loki vs Elasticsearch | Phase 2 |

---

## Summary of v1.2 Changes

| Section | Key Change | User Impact |
|---------|------------|-------------|
| §11 | Deployment architecture defined | Clear container strategy |
| §11 | Podman + quadlets specified | Memory-efficient edge deployment |
| §11 | Rolling update workflow | Zero-downtime updates |
| §12 | Infrastructure requirements | Hardware/software prerequisites |
| §7 | Deployment FRs added | Containerization requirements |
| §8 | Operational NFRs added | Container performance targets |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial PRD |
| 1.1 | 2025-01-20 | AkiDB Team | SLO boundaries, delete/update contracts, consistency guarantees, cuVS gate |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets), deployment architecture, infrastructure requirements |

---

*End of PRD v1.2*
