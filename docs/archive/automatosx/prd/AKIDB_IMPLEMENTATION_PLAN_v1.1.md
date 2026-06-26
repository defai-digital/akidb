# AkiDB Thor Edition - Implementation Plan

**Version:** 1.1
**Date:** 2026-01-21
**Status:** Approved
**Based On:** ADR v1.2, PRD v1.2
**Review:** Multi-model synthesis (Claude, Gemini, Grok)
**Changes from v1.0:** Added Podman + quadlets deployment tasks, updated Phase 0 and Phase 4

---

## Change Log from v1.0

| Section | Change | Rationale |
|---------|--------|-----------|
| Phase 0 | Added Podman + NVIDIA CDI setup | Container infrastructure validation |
| Phase 1 | Added Dockerfile creation | Container build pipeline |
| Phase 4 | Replaced Kubernetes with Podman quadlets | Deployment architecture decision |
| All | Added container-specific tasks | Align with ADR-017 |

---

## Executive Summary

This implementation plan covers the development of AkiDB Thor Edition over **~19 weeks (~5 months)** across 4 phases plus a validation sprint. The plan prioritizes early hardware validation, FAISS-rs GPU binding stability, and distributed systems correctness over ML optimizations.

**Key Updates in v1.1:**
- Container deployment via Podman + systemd quadlets (not Kubernetes)
- Docker Compose for development environment
- NVIDIA CDI for GPU passthrough validation in Phase 0

---

## Timeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AKIDB THOR IMPLEMENTATION TIMELINE                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Week 0          │ Weeks 1-6        │ Weeks 7-12      │ Weeks 13-18  │ 19-22│
│  ┌─────────────┐ │ ┌──────────────┐ │ ┌─────────────┐ │ ┌──────────┐ │ ┌───┐│
│  │ VALIDATION  │ │ │   PHASE 1    │ │ │   PHASE 2   │ │ │  PHASE 3 │ │ │P4 ││
│  │   SPRINT    │ │ │  Foundation  │ │ │ Distribution│ │ │Optimization│ │ │   ││
│  │  (1 week)   │ │ │  (6 weeks)   │ │ │  (6 weeks)  │ │ │ (6 weeks)│ │ │4wk││
│  └─────────────┘ │ └──────────────┘ │ └─────────────┘ │ └──────────┘ │ └───┘│
│                                                                             │
│  Hardware        │ Single-node      │ Multi-node      │ TensorRT     │ cuVS │
│  Podman + CDI    │ FAISS GPU        │ Fan-out         │ Rebuild      │ Prod │
│  CI/CD           │ gRPC + RocksDB   │ Tombstones      │ Performance  │      │
│  Dockerfile      │                  │                 │              │      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Total Duration:** 22-23 weeks (~5.5 months)

---

## Phase 0: Validation Sprint (Week 0) - UPDATED

### Objectives
- Validate hardware compatibility before writing application code
- **Validate Podman + NVIDIA CDI for GPU passthrough (NEW)**
- Establish CI/CD pipeline with GPU support
- Confirm CUDA version compatibility matrix
- Set security baseline

### Tasks

| ID | Task | Owner | Duration | Exit Criteria |
|----|------|-------|----------|---------------|
| V-01 | Procure Jetson Thor hardware (4 units) | Infra | - | Hardware delivered |
| V-02 | GPU driver + CUDA installation | Infra | 1 day | nvidia-smi reports expected GPU |
| V-03 | FAISS standalone benchmark (IVF-Flat, 1M vectors) | Dev | 2 days | Benchmark completes, latencies recorded |
| V-04 | MinIO cluster deployment (4 nodes) | Infra | 1 day | S3 API responds, latency < 10ms |
| V-05 | CUDA compatibility matrix validation | ML | 1 day | FAISS 1.8+ and TensorRT compatible |
| V-06 | CI/CD pipeline with GPU runners | DevOps | 2 days | GPU tests run in CI |
| V-07 | Security baseline (TLS config, cargo-audit) | DevOps | 1 day | No critical vulnerabilities |
| **V-08** | **Install Podman on Thor nodes** | **DevOps** | **0.5 day** | **podman --version succeeds** |
| **V-09** | **Configure NVIDIA CDI for Podman** | **DevOps** | **1 day** | **nvidia-ctk cdi list shows GPU** |
| **V-10** | **Validate GPU passthrough in container** | **DevOps** | **0.5 day** | **nvidia-smi works inside Podman container** |
| **V-11** | **Create base Dockerfile** | **Dev** | **1 day** | **Multi-stage build compiles** |

### Container Validation Tasks (NEW)

#### V-08: Podman Installation

```bash
# Install Podman on each Thor node
sudo apt-get update
sudo apt-get install -y podman podman-compose

# Verify
podman --version
# Expected: podman version 4.x.x
```

#### V-09: NVIDIA CDI Configuration

```bash
# Install NVIDIA Container Toolkit
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
    sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit

# Generate CDI specification
sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml

# Verify CDI
nvidia-ctk cdi list
# Expected: nvidia.com/gpu=0
```

#### V-10: GPU Passthrough Validation

```bash
# Test GPU access inside container
podman run --rm --device nvidia.com/gpu=all \
    nvidia/cuda:12.0-base-ubuntu22.04 nvidia-smi

# Expected: GPU info displayed
```

#### V-11: Base Dockerfile

```dockerfile
# Dockerfile.base (validation only)
FROM rust:1.75-bookworm AS builder
RUN apt-get update && apt-get install -y cmake libclang-dev
WORKDIR /app
COPY . .
RUN cargo build --release --features cpu

FROM debian:bookworm-slim AS runtime
COPY --from=builder /app/target/release/akidb-server /usr/local/bin/
CMD ["/usr/local/bin/akidb-server", "--help"]
```

### Deliverables (UPDATED)
- [ ] Hardware benchmark report (FAISS IVF-Flat latencies at reference config)
- [ ] MinIO latency baseline document
- [ ] CUDA compatibility matrix (FAISS ↔ TensorRT)
- [ ] CI/CD pipeline operational
- [ ] Security scan report
- [ ] **Podman + CDI validated on Thor nodes (NEW)**
- [ ] **Base Dockerfile compiling (NEW)**

### Exit Gate (UPDATED)
All tasks complete. FAISS GPU IVF-Flat confirmed working on Thor hardware. **Podman GPU passthrough validated.**

---

## Phase 1: Foundation (Weeks 1-6) - UPDATED

### Objectives
- Establish single-node vector search with GPU acceleration
- Implement core gRPC API
- Set up RocksDB for metadata storage
- Instrument observability from day one
- **Create production Dockerfile and docker-compose.yml (NEW)**

### Sprint Breakdown

#### Sprint 1 (Weeks 1-2): Scaffolding & FAISS Integration

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-01 | Initialize Cargo workspace with modular crates | P0 | 2d | - |
| P1-02 | Create `faiss-wrapper` crate with GPU IVF-Flat bindings | P0 | 5d | P1-01 |
| P1-03 | Implement basic insert/search operations via FFI | P0 | 3d | P1-02 |
| P1-04 | Define gRPC protobuf schemas (v1) | P0 | 2d | - |
| P1-05 | Design storage abstraction interface | P1 | 2d | - |

**Sprint 1 Exit Criteria:**
- [ ] FFI calls to FAISS GPU succeed
- [ ] Basic insert/search works in unit tests
- [ ] Protobuf schemas defined and compiling

#### Sprint 2 (Weeks 3-4): RocksDB & gRPC Service

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-06 | Implement RocksDB storage backend | P0 | 4d | P1-05 |
| P1-07 | ID mapping: external → internal | P0 | 3d | P1-06 |
| P1-08 | Implement gRPC InsertVector endpoint | P0 | 2d | P1-03, P1-04 |
| P1-09 | Implement gRPC SearchVector endpoint | P0 | 2d | P1-03, P1-04 |
| P1-10 | Minimal gRPC streaming prototype (fan-out stub) | P1 | 2d | P1-09 |
| P1-11 | Error propagation across FFI boundary | P0 | 2d | P1-03 |

**Sprint 2 Exit Criteria:**
- [ ] Persistence verified (restart preserves data)
- [ ] gRPC endpoints functional
- [ ] Streaming POC demonstrates fan-out semantics
- [ ] FFI errors propagate to gRPC responses

#### Sprint 3 (Weeks 5-6): GPU Memory, Observability & Containers

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-12 | GPU memory management (60% budget enforcement) | P0 | 3d | P1-02 |
| P1-13 | Memory pressure handling (CPU fallback trigger) | P1 | 2d | P1-12 |
| P1-14 | Observability: OpenTelemetry tracing | P0 | 3d | P1-08, P1-09 |
| P1-15 | Metrics: GPU memory, latency percentiles | P0 | 2d | P1-14 |
| P1-16 | Benchmarking at reference config (D=768, N=1M) | P0 | 3d | P1-12 |
| P1-17 | Load test: 10M+ vectors without OOM | P0 | 2d | P1-12 |
| **P1-18** | **Create production Dockerfile (multi-stage)** | **P0** | **2d** | **P1-01** |
| **P1-19** | **Create docker-compose.yml for development** | **P0** | **1d** | **P1-18** |
| **P1-20** | **Add grpc_health_probe to container** | **P0** | **0.5d** | **P1-18** |
| **P1-21** | **CI: Build and push container images** | **P1** | **1d** | **P1-18** |

**Sprint 3 Exit Criteria:**
- [ ] 10M+ vectors without GPU OOM
- [ ] P50/P95/P99 latencies baselined
- [ ] Tracing spans visible in Jaeger/similar
- [ ] Metrics exported to Prometheus
- [ ] **Dockerfile builds successfully (NEW)**
- [ ] **docker-compose up starts local environment (NEW)**
- [ ] **Health probe works in container (NEW)**

### Container Tasks Detail (NEW)

#### P1-18: Production Dockerfile

```dockerfile
# Dockerfile
# Multi-stage build for AkiDB

# ============================================
# Stage 1: Builder
# ============================================
FROM rust:1.75-bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    libclang-dev \
    librocksdb-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/*/Cargo.toml crates/
RUN mkdir -p crates/common/src && echo "pub fn dummy() {}" > crates/common/src/lib.rs \
    # ... (create dummy files for all crates)
    && cargo build --release --features cpu \
    && rm -rf target/release/.fingerprint/akidb*

# Build actual application
COPY crates crates
COPY proto proto
RUN cargo build --release --features cpu

# ============================================
# Stage 2: Runtime
# ============================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    librocksdb7.8 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install grpc_health_probe
RUN curl -fsSL https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.24/grpc_health_probe-linux-amd64 \
    -o /usr/bin/grpc_health_probe && chmod +x /usr/bin/grpc_health_probe

RUN useradd -r -u 1000 -g root akidb
RUN mkdir -p /data /etc/akidb && chown akidb:root /data /etc/akidb

COPY --from=builder /app/target/release/akidb-server /usr/local/bin/
COPY --from=builder /app/target/release/akidb-coordinator /usr/local/bin/
COPY config/default.toml /etc/akidb/akidb.toml

USER akidb
EXPOSE 50051 8080 9090

ENTRYPOINT ["/usr/local/bin/akidb-server"]
CMD ["--config", "/etc/akidb/akidb.toml"]
```

#### P1-19: Development docker-compose.yml

```yaml
# docker-compose.yml
version: '3.8'

services:
  akidb-shard:
    build:
      context: .
      dockerfile: Dockerfile
    image: akidb-server:dev
    environment:
      - AKIDB_ROLE=shard
      - RUST_LOG=debug
    volumes:
      - ./data:/data
      - ./config:/etc/akidb:ro
    ports:
      - "50051:50051"
      - "9090:9090"
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=localhost:50051"]
      interval: 10s
      timeout: 5s
      retries: 3

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER=akidb-admin
      - MINIO_ROOT_PASSWORD=akidb-secret
    volumes:
      - minio-data:/data
    ports:
      - "9000:9000"
      - "9001:9001"

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./deploy/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9091:9090"

  grafana:
    image: grafana/grafana:latest
    volumes:
      - ./deploy/grafana:/etc/grafana/provisioning:ro
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin

volumes:
  minio-data:
```

### Phase 1 Deliverables (UPDATED)
- [ ] Single-node AkiDB binary with gRPC API
- [ ] Insert, Search, Get operations functional
- [ ] RocksDB persistence layer
- [ ] GPU memory management with CPU fallback
- [ ] Observability (tracing + metrics)
- [ ] Benchmark report at reference configuration
- [ ] **Production Dockerfile (NEW)**
- [ ] **docker-compose.yml for development (NEW)**
- [ ] **CI container build pipeline (NEW)**

### Phase 1 Exit Gate (UPDATED)

| Criteria | Target | Validated |
|----------|--------|-----------|
| Single-node insert latency | < 5ms | [ ] |
| Single-node search P95 (ref config) | < 10ms | [ ] |
| Vectors without OOM | 10M+ | [ ] |
| Tracing instrumented | Yes | [ ] |
| Storage abstraction | Interface defined | [ ] |
| Security | TLS on gRPC | [ ] |
| **Dockerfile builds** | Yes | [ ] |
| **docker-compose up works** | Yes | [ ] |

---

## Phase 2: Distribution (Weeks 7-12)

*(Sprints 4-6 remain largely unchanged from v1.0)*

### Additional Container Tasks

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| **P2-20** | **Create coordinator Dockerfile** | **P0** | **1d** | **Phase 1** |
| **P2-21** | **Update docker-compose for multi-service** | **P1** | **1d** | **P2-20** |

---

## Phase 3: Optimization (Weeks 13-18)

*(Sprints 7-9 remain largely unchanged from v1.0)*

---

## Phase 4: Production (Weeks 19-22) - SIGNIFICANTLY UPDATED

### Objectives
- Evaluate and integrate cuVS (if gate criteria met)
- **Deploy with Podman + systemd quadlets (CHANGED from Kubernetes)**
- Production hardening
- Security audit and documentation

### Sprint Breakdown

#### Sprint 10 (Weeks 19-20): cuVS Integration

*(Unchanged from v1.0)*

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P4-01 | cuVS integration behind feature flag | P1 | 4d | Phase 1 |
| P4-02 | Shadow mode validation (24h) | P0 | 3d | P4-01 |
| P4-03 | cuVS vs FAISS benchmark comparison | P0 | 2d | P4-01 |
| P4-04 | Rollback mechanism testing | P0 | 2d | P4-01 |
| P4-05 | cuVS gate decision documentation | P0 | 1d | P4-02, P4-03 |

#### Sprint 11 (Weeks 21-22): Production Hardening - UPDATED

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P4-06 | Security audit (external or internal) | P0 | 3d | All |
| P4-07 | Penetration testing | P1 | 2d | P4-06 |
| ~~P4-08~~ | ~~Deployment automation (Kubernetes manifests)~~ | - | - | **REMOVED** |
| **P4-08** | **Create Podman quadlet files** | **P0** | **2d** | **All** |
| **P4-09** | **Create Ansible playbook for quadlet deployment** | **P0** | **2d** | **P4-08** |
| **P4-10** | **Create rolling update script** | **P0** | **1d** | **P4-08** |
| **P4-11** | **Create rollback script** | **P0** | **1d** | **P4-08** |
| P4-12 | Runbook documentation | P0 | 2d | All |
| P4-13 | Operational playbooks (incident response) | P0 | 2d | All |
| P4-14 | Final load testing (production simulation) | P0 | 2d | All |

### Quadlet Deployment Tasks (NEW)

#### P4-08: Podman Quadlet Files

Create quadlet files for production deployment:

```
deploy/podman/
├── akidb-shard.container      # Shard server quadlet
├── akidb-coordinator.container # Coordinator quadlet
├── minio.container            # MinIO quadlet
└── README.md                  # Deployment instructions
```

**akidb-shard.container:**

```ini
[Unit]
Description=AkiDB Shard Server
After=network-online.target minio.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/akidb-server:latest
ContainerName=akidb-shard
AddDevice=nvidia.com/gpu=all
Environment=NVIDIA_VISIBLE_DEVICES=all
Environment=NVIDIA_DRIVER_CAPABILITIES=compute,utility
Environment=AKIDB_CONFIG=/etc/akidb/akidb.toml
Environment=AKIDB_ROLE=shard
Environment=RUST_LOG=info
Volume=/var/lib/akidb:/data:Z
Volume=/etc/akidb:/etc/akidb:ro,Z
Network=host
HealthCmd=/usr/bin/grpc_health_probe -addr=localhost:50051
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3
HealthStartPeriod=30s

[Service]
Restart=always
RestartSec=10
TimeoutStartSec=300
TimeoutStopSec=60
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

#### P4-09: Ansible Deployment Playbook

```yaml
# deploy/ansible/playbooks/deploy-podman.yml
---
- name: Deploy AkiDB with Podman Quadlets
  hosts: thor_cluster
  become: yes
  vars:
    akidb_version: "latest"
    quadlet_dir: /etc/containers/systemd

  tasks:
    - name: Ensure Podman is installed
      apt:
        name: [podman, podman-compose]
        state: present

    - name: Ensure NVIDIA Container Toolkit is installed
      apt:
        name: nvidia-container-toolkit
        state: present

    - name: Generate NVIDIA CDI spec
      command: nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml
      args:
        creates: /etc/cdi/nvidia.yaml

    - name: Create quadlet directory
      file:
        path: "{{ quadlet_dir }}"
        state: directory
        mode: '0755'

    - name: Create AkiDB config directory
      file:
        path: /etc/akidb
        state: directory
        mode: '0755'

    - name: Create AkiDB data directory
      file:
        path: /var/lib/akidb
        state: directory
        owner: 1000
        group: root
        mode: '0755'

    - name: Deploy AkiDB configuration
      template:
        src: akidb.toml.j2
        dest: /etc/akidb/akidb.toml
        mode: '0644'

    - name: Deploy shard quadlet (shard nodes)
      template:
        src: akidb-shard.container.j2
        dest: "{{ quadlet_dir }}/akidb-shard.container"
        mode: '0644'
      when: akidb_role == 'shard'
      notify: Reload systemd

    - name: Deploy coordinator quadlet (coordinator node)
      template:
        src: akidb-coordinator.container.j2
        dest: "{{ quadlet_dir }}/akidb-coordinator.container"
        mode: '0644'
      when: akidb_role == 'coordinator'
      notify: Reload systemd

    - name: Deploy MinIO quadlet
      template:
        src: minio.container.j2
        dest: "{{ quadlet_dir }}/minio.container"
        mode: '0644'
      notify: Reload systemd

    - name: Pull container images
      command: "podman pull ghcr.io/akidb/akidb-server:{{ akidb_version }}"
      when: akidb_role == 'shard'

    - name: Pull coordinator image
      command: "podman pull ghcr.io/akidb/akidb-coordinator:{{ akidb_version }}"
      when: akidb_role == 'coordinator'

  handlers:
    - name: Reload systemd
      systemd:
        daemon_reload: yes

- name: Start services
  hosts: thor_cluster
  become: yes
  tasks:
    - name: Start MinIO
      systemd:
        name: minio
        state: started
        enabled: yes

    - name: Wait for MinIO
      wait_for:
        port: 9000
        delay: 5
        timeout: 60

    - name: Start AkiDB shard
      systemd:
        name: akidb-shard
        state: started
        enabled: yes
      when: akidb_role == 'shard'

    - name: Start AkiDB coordinator
      systemd:
        name: akidb-coordinator
        state: started
        enabled: yes
      when: akidb_role == 'coordinator'

    - name: Wait for AkiDB health
      command: grpc_health_probe -addr=localhost:50051
      register: health
      retries: 12
      delay: 5
      until: health.rc == 0
```

#### P4-10: Rolling Update Script

```bash
#!/bin/bash
# deploy/scripts/rolling-update.sh
set -euo pipefail

VERSION=${1:-latest}
NODES=(thor1 thor2 thor3 thor4)
HEALTH_TIMEOUT=60

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

wait_for_health() {
    local node=$1
    local timeout=$HEALTH_TIMEOUT

    log "Waiting for $node to be healthy..."
    while ! ssh "$node" "grpc_health_probe -addr=localhost:50051" 2>/dev/null; do
        ((timeout--))
        if [[ $timeout -le 0 ]]; then
            log "ERROR: $node failed health check"
            return 1
        fi
        sleep 1
    done
    log "$node is healthy"
}

main() {
    log "Starting rolling update to version $VERSION"

    for node in "${NODES[@]}"; do
        log "=== Updating $node ==="

        # Pull new image
        log "Pulling new image on $node..."
        ssh "$node" "podman pull ghcr.io/akidb/akidb-server:$VERSION"

        # Restart service
        log "Restarting service on $node..."
        ssh "$node" "systemctl restart akidb-shard.service 2>/dev/null || systemctl restart akidb-coordinator.service"

        # Wait for health
        if ! wait_for_health "$node"; then
            log "ROLLBACK REQUIRED: $node failed health check"
            log "Run: ./rollback.sh $node"
            exit 1
        fi

        log "$node updated successfully"
        log ""
    done

    log "=== Rolling update complete ==="
}

main "$@"
```

#### P4-11: Rollback Script

```bash
#!/bin/bash
# deploy/scripts/rollback.sh
set -euo pipefail

NODE=${1:-}
PREVIOUS_VERSION=${2:-previous}

if [[ -z "$NODE" ]]; then
    echo "Usage: $0 <node> [previous_version]"
    exit 1
fi

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "Rolling back $NODE to version $PREVIOUS_VERSION"

# Pull previous image
ssh "$NODE" "podman pull ghcr.io/akidb/akidb-server:$PREVIOUS_VERSION"

# Restart service
ssh "$NODE" "systemctl restart akidb-shard.service 2>/dev/null || systemctl restart akidb-coordinator.service"

# Wait for health
timeout=60
while ! ssh "$NODE" "grpc_health_probe -addr=localhost:50051" 2>/dev/null; do
    ((timeout--))
    if [[ $timeout -le 0 ]]; then
        log "ERROR: Rollback failed - $NODE still unhealthy"
        exit 1
    fi
    sleep 1
done

log "Rollback complete - $NODE is healthy"
```

**Sprint 11 Exit Criteria (UPDATED):**
- [ ] Security audit passed
- [ ] **Podman quadlets deployed to all Thor nodes (CHANGED)**
- [ ] **Rolling update script tested (NEW)**
- [ ] **Rollback script tested (NEW)**
- [ ] Runbooks complete
- [ ] Production simulation successful

### Phase 4 Deliverables (UPDATED)
- [ ] cuVS integration (if gate passed) or documented exclusion
- [ ] Security audit report
- [ ] **Podman quadlet files (CHANGED from Kubernetes)**
- [ ] **Ansible deployment playbook (NEW)**
- [ ] **Rolling update and rollback scripts (NEW)**
- [ ] Operational runbooks
- [ ] Production readiness certification

### Phase 4 Exit Gate (UPDATED)

| Criteria | Target | Validated |
|----------|--------|-----------|
| cuVS decision | Documented | [ ] |
| Security audit | Passed | [ ] |
| Production simulation | 100 QPS, < 50ms P95 | [ ] |
| Runbooks | Complete | [ ] |
| **Podman deployment** | All nodes running | [ ] |
| **Rolling update** | Zero-downtime tested | [ ] |
| **Rollback** | < 5 min tested | [ ] |

---

## Critical Path Dependencies (UPDATED)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CRITICAL PATH DEPENDENCY DAG                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Week 0: Hardware Validation + Podman/CDI ──────────────────────────┐      │
│              │                                                       │      │
│              ▼                                                       │      │
│  Phase 1: FAISS-rs GPU ──► Storage ──► gRPC ──► Dockerfile          │      │
│              │                │           │           │              │      │
│              │                ▼           │           │              │      │
│              │         RocksDB Integration◄───────────┘              │      │
│              │                │                                      │      │
│              ▼                ▼                                      │      │
│  Phase 2: Fan-out Coordinator ◄───┘                                  │      │
│              │                                                       │      │
│              ├──► Tombstones ──► Rebuild (Phase 3)                  │      │
│              │                                                       │      │
│              ▼                                                       │      │
│  Phase 3: TensorRT ◄──────────────────────────────────────┘         │      │
│              │                                                       │      │
│              ▼                                                       │      │
│  Phase 4: cuVS Gate + Podman Quadlets + Production                  │      │
│                                                                      │      │
└──────────────────────────────────────────────────────────────────────┘

NEW DEPENDENCY: Podman/CDI validation (Week 0) → Dockerfile (Phase 1) →
               docker-compose (Phase 1) → Quadlets (Phase 4)
```

---

## Team Allocation (UPDATED)

| Role | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|------|---------|---------|---------|---------|---------|
| **Rust Engineer 1** | FAISS bench | FAISS wrapper, FFI | Coordinator | Rebuild | cuVS |
| **Rust Engineer 2** | - | gRPC, RocksDB | Tombstones | WAL | Hardening |
| **ML Engineer** | CUDA compat | - | - | TensorRT | cuVS validation |
| **DevOps** | CI/CD, MinIO, **Podman/CDI** | Observability, **Dockerfile** | Load testing | Automation | **Quadlets, Scripts** |

---

## Deliverables Summary (UPDATED)

### Container Deliverables (NEW)

| Phase | Deliverable | Description |
|-------|-------------|-------------|
| 0 | Podman + CDI validated | GPU passthrough working |
| 1 | Dockerfile | Multi-stage production build |
| 1 | docker-compose.yml | Development environment |
| 1 | CI container pipeline | Automated image builds |
| 4 | Quadlet files | Production systemd integration |
| 4 | Ansible playbook | Automated deployment |
| 4 | Rolling update script | Zero-downtime updates |
| 4 | Rollback script | Quick recovery |

---

## Open Questions (UPDATED)

### Resolved in v1.1

| ID | Question | Resolution |
|----|----------|------------|
| Q1 | Deployment orchestration? | Podman + systemd quadlets (not Kubernetes) |
| Q2 | Development environment? | Docker Compose |
| Q3 | GPU passthrough method? | NVIDIA CDI |

### Remaining Questions

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q4 | Image registry? | GHCR vs self-hosted | Phase 0 |
| Q5 | Secrets management? | systemd credentials vs Vault | Phase 1 |
| Q6 | Log aggregation? | Loki vs Elasticsearch | Phase 2 |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial implementation plan |
| 1.1 | 2026-01-21 | AkiDB Team | Added Podman + quadlets deployment, updated Phase 0 and Phase 4 |

---

*End of Implementation Plan v1.1*
