# AkiDB Thor Edition - Architecture Decision Records (ADR)
## Version 1.2

**Version:** 1.2
**Date:** 2026-01-21
**Status:** Approved
**Changes from v1.1:** Added ADR-017 Container Orchestration Strategy (Podman + quadlets)
**Review:** Multi-model synthesis (Claude, Gemini, Grok) addressing deployment architecture

---

## Change Log from v1.1

| Section | Change | Rationale |
|---------|--------|-----------|
| ADR-017 | NEW: Container Orchestration Strategy | Deployment architecture decision needed |
| ADR-017 | Podman + quadlets selected over K8s/Docker | Memory efficiency critical for GPU workloads |
| All | Updated deployment references | Align with container strategy |

---

## Table of Contents

- [ADR-002: Vector Index Strategy (FAISS GPU IVF-Flat)](#adr-002-vector-index-strategy-revised) *(unchanged from v1.1)*
- [ADR-009: Index Lifecycle - Delete, Update, Rebuild](#adr-009-index-lifecycle-revised) *(unchanged from v1.1)*
- [ADR-015: ID Management Contract](#adr-015-id-management-contract) *(unchanged from v1.1)*
- [ADR-016: Consistency and Visibility Guarantees](#adr-016-consistency-guarantees) *(unchanged from v1.1)*
- [ADR-017: Container Orchestration Strategy (NEW)](#adr-017-container-orchestration-strategy)

*Note: ADRs 002, 009, 015, 016 remain unchanged from v1.1. Only new ADR-017 included below.*

---

## ADR-017: Container Orchestration Strategy (NEW)

### Status
**Accepted**

### Context

AkiDB Thor Edition deploys on NVIDIA Jetson Thor edge devices (ARM64, ~64GB unified memory). The deployment architecture must balance:

1. **Memory efficiency**: GPU/FAISS workloads require maximum available RAM (~38GB for vector indices at 0.6 memory fraction)
2. **Operational simplicity**: 4-node cluster with single shard per node
3. **GPU passthrough**: NVIDIA Container Toolkit integration
4. **Dev/prod parity**: Containerized environments for reproducibility
5. **Recovery**: systemd-based process supervision

The existing Ansible-based deployment (`deploy/ansible/`) uses plain systemd services, which lacks containerization benefits (reproducibility, dependency isolation, rollback).

### Options Considered

| Option | Memory Overhead | Complexity | GPU Support | Recovery |
|--------|-----------------|------------|-------------|----------|
| **Kubernetes (K3s)** | ~500MB-1GB | High | Excellent | Automatic |
| **Docker Compose** | ~100-200MB | Low | Good | Manual |
| **Podman + quadlets** | ~0 (daemonless) | Moderate | Good | systemd |
| **Plain systemd** | ~0 | Low | N/A | systemd |

### Decision

We adopt **Podman with systemd quadlets** for container orchestration.

```
┌─────────────────────────────────────────────────────────────┐
│              CONTAINER ORCHESTRATION DECISION               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  SELECTED: Podman + systemd quadlets                       │
│                                                             │
│  Rationale:                                                 │
│  • Daemonless architecture = ~0 memory overhead            │
│  • Native systemd integration (journalctl, systemctl)      │
│  • Rootless by default (security on edge devices)          │
│  • Docker CLI compatible (familiar tooling)                │
│  • CDI GPU support (cleaner than --gpus flag)              │
│                                                             │
│  Trade-offs accepted:                                       │
│  • No built-in multi-node orchestration (use Ansible)      │
│  • Rolling updates require scripting                        │
│  • Less mature NVIDIA Jetson documentation than Docker      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Deployment Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT TOPOLOGY                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Thor Node 1-3: Shard Servers                               │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Container: akidb-shard                               │ │
│  │  • Podman quadlet: /etc/containers/systemd/           │ │
│  │  • GPU passthrough via CDI                            │ │
│  │  • Persistent volume: /var/lib/akidb                  │ │
│  │  • systemd service: akidb-shard.service               │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  Thor Node 4: Coordinator                                   │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Container: akidb-coordinator                         │ │
│  │  • Stateless, can run on any node                     │ │
│  │  • No GPU required (CPU only)                         │ │
│  │  • systemd service: akidb-coordinator.service         │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  All Nodes: MinIO                                           │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Container: minio                                      │ │
│  │  • Distributed mode across 4 nodes                    │ │
│  │  • Persistent volume: /var/lib/minio                  │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Quadlet Configuration

#### Shard Server Quadlet

```ini
# /etc/containers/systemd/akidb-shard.container
[Unit]
Description=AkiDB Shard Server
After=network-online.target minio.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/akidb-server:latest
ContainerName=akidb-shard

# GPU passthrough via CDI
AddDevice=nvidia.com/gpu=all
Environment=NVIDIA_VISIBLE_DEVICES=all
Environment=NVIDIA_DRIVER_CAPABILITIES=compute,utility

# Configuration
Environment=AKIDB_CONFIG=/etc/akidb/akidb.toml
Environment=AKIDB_ROLE=shard
Environment=RUST_LOG=info

# Volumes
Volume=/var/lib/akidb:/data:Z
Volume=/etc/akidb:/etc/akidb:ro,Z

# Networking
Network=host

# Health check
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

# Resource limits
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

#### Coordinator Quadlet

```ini
# /etc/containers/systemd/akidb-coordinator.container
[Unit]
Description=AkiDB Coordinator
After=network-online.target
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/akidb-coordinator:latest
ContainerName=akidb-coordinator

# No GPU needed for coordinator
Environment=AKIDB_CONFIG=/etc/akidb/coordinator.toml
Environment=AKIDB_ROLE=coordinator
Environment=RUST_LOG=info

# Shard endpoints (configured via toml or env)
Environment=AKIDB_SHARDS=thor1:50051,thor2:50051,thor3:50051

# Volumes
Volume=/etc/akidb:/etc/akidb:ro,Z

# Networking
Network=host

# Health check
HealthCmd=/usr/bin/grpc_health_probe -addr=localhost:50051
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3

[Service]
Restart=always
RestartSec=5
TimeoutStartSec=60

[Install]
WantedBy=multi-user.target
```

#### MinIO Quadlet

```ini
# /etc/containers/systemd/minio.container
[Unit]
Description=MinIO Object Storage
After=network-online.target

[Container]
Image=minio/minio:latest
ContainerName=minio

Environment=MINIO_ROOT_USER=akidb-admin
Environment=MINIO_ROOT_PASSWORD_FILE=/run/secrets/minio-password

Volume=/var/lib/minio:/data:Z
Volume=/run/secrets:/run/secrets:ro

Network=host

Exec=server /data --console-address ":9001"

HealthCmd=curl -f http://localhost:9000/minio/health/live
HealthInterval=30s
HealthTimeout=10s
HealthRetries=3

[Service]
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Deployment Workflow

#### Initial Deployment (Ansible)

```yaml
# deploy/ansible/playbooks/deploy-containers.yml
- name: Deploy AkiDB Containers
  hosts: thor_cluster
  become: yes
  tasks:
    - name: Install Podman
      apt:
        name: [podman, podman-compose]
        state: present

    - name: Install NVIDIA Container Toolkit
      include_tasks: nvidia-cdi-setup.yml

    - name: Create quadlet directory
      file:
        path: /etc/containers/systemd
        state: directory

    - name: Deploy shard quadlet (shard nodes)
      template:
        src: akidb-shard.container.j2
        dest: /etc/containers/systemd/akidb-shard.container
      when: akidb_role == 'shard'
      notify: Reload systemd

    - name: Deploy coordinator quadlet (coordinator node)
      template:
        src: akidb-coordinator.container.j2
        dest: /etc/containers/systemd/akidb-coordinator.container
      when: akidb_role == 'coordinator'
      notify: Reload systemd

    - name: Start services
      systemd:
        name: "{{ item }}"
        state: started
        enabled: yes
        daemon_reload: yes
      loop:
        - minio.service
        - "{{ 'akidb-shard' if akidb_role == 'shard' else 'akidb-coordinator' }}.service"
```

#### Rolling Update Script

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
    local port=${2:-50051}
    local timeout=$HEALTH_TIMEOUT

    log "Waiting for $node to be healthy..."
    until ssh "$node" "grpc_health_probe -addr=localhost:$port" 2>/dev/null; do
        ((timeout--))
        if [[ $timeout -le 0 ]]; then
            log "ERROR: $node failed health check"
            return 1
        fi
        sleep 1
    done
    log "$node is healthy"
}

for node in "${NODES[@]}"; do
    log "Updating $node..."

    # Pull new image
    ssh "$node" "podman pull ghcr.io/akidb/akidb-server:$VERSION"

    # Restart service (quadlet handles image update)
    ssh "$node" "systemctl restart akidb-shard.service || systemctl restart akidb-coordinator.service"

    # Wait for health
    if ! wait_for_health "$node"; then
        log "ROLLBACK: $node failed, stopping update"
        ssh "$node" "podman pull ghcr.io/akidb/akidb-server:previous && systemctl restart akidb-shard.service"
        exit 1
    fi

    log "$node updated successfully"
done

log "Rolling update complete"
```

### Development Environment

For local development, use Docker Compose for parity:

```yaml
# docker-compose.yml (development)
version: '3.8'

services:
  akidb-shard:
    build:
      context: .
      dockerfile: Dockerfile
      target: runtime
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

volumes:
  minio-data:
```

### Dockerfile

```dockerfile
# Dockerfile
# Multi-stage build for AkiDB

# ============================================
# Stage 1: Builder
# ============================================
FROM rust:1.75-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    libclang-dev \
    librocksdb-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml crates/common/
COPY crates/faiss-wrapper/Cargo.toml crates/faiss-wrapper/
COPY crates/storage/Cargo.toml crates/storage/
COPY crates/grpc-server/Cargo.toml crates/grpc-server/
COPY crates/coordinator/Cargo.toml crates/coordinator/
COPY crates/server/Cargo.toml crates/server/

# Create dummy source files for dependency compilation
RUN mkdir -p crates/common/src && echo "pub fn dummy() {}" > crates/common/src/lib.rs \
    && mkdir -p crates/faiss-wrapper/src && echo "pub fn dummy() {}" > crates/faiss-wrapper/src/lib.rs \
    && mkdir -p crates/storage/src && echo "pub fn dummy() {}" > crates/storage/src/lib.rs \
    && mkdir -p crates/grpc-server/src && echo "pub fn dummy() {}" > crates/grpc-server/src/lib.rs \
    && mkdir -p crates/coordinator/src && echo "pub fn dummy() {}" > crates/coordinator/src/lib.rs \
    && mkdir -p crates/server/src && echo "fn main() {}" > crates/server/src/main.rs

# Build dependencies only
RUN cargo build --release --features cpu && rm -rf target/release/.fingerprint/akidb*

# Copy actual source code
COPY crates crates
COPY proto proto

# Build the application
RUN cargo build --release --features cpu

# ============================================
# Stage 2: Runtime
# ============================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    librocksdb7.8 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install grpc_health_probe
RUN curl -fsSL https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.24/grpc_health_probe-linux-amd64 \
    -o /usr/bin/grpc_health_probe && chmod +x /usr/bin/grpc_health_probe

# Create non-root user
RUN useradd -r -u 1000 -g root akidb

# Create directories
RUN mkdir -p /data /etc/akidb && chown akidb:root /data /etc/akidb

# Copy binaries
COPY --from=builder /app/target/release/akidb-server /usr/local/bin/
COPY --from=builder /app/target/release/akidb-coordinator /usr/local/bin/

# Copy default config
COPY config/default.toml /etc/akidb/akidb.toml

USER akidb

EXPOSE 50051 8080 9090

ENTRYPOINT ["/usr/local/bin/akidb-server"]
CMD ["--config", "/etc/akidb/akidb.toml"]
```

### GPU Support on Jetson Thor

#### NVIDIA Container Device Interface (CDI)

```bash
# Install NVIDIA Container Toolkit with CDI support
# /deploy/scripts/setup-nvidia-cdi.sh

#!/bin/bash
set -euo pipefail

# Add NVIDIA repository
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
    gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

apt-get update
apt-get install -y nvidia-container-toolkit

# Generate CDI specification
nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml

# Verify
nvidia-ctk cdi list
# Should show: nvidia.com/gpu=0
```

### Monitoring Integration

```yaml
# Prometheus scrape config for Podman containers
# /etc/prometheus/prometheus.yml (addition)

scrape_configs:
  - job_name: 'akidb-shards'
    static_configs:
      - targets:
        - 'thor1:9090'
        - 'thor2:9090'
        - 'thor3:9090'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):.*'
        replacement: '${1}'

  - job_name: 'akidb-coordinator'
    static_configs:
      - targets: ['thor4:9090']
```

### Rejected Alternatives

#### Kubernetes (K3s)

**Why rejected:**
- ~500MB-1GB memory overhead per node
- Etcd quorum issues in 4-node clusters
- Scheduling benefits don't align with single-shard-per-node design
- Operational complexity not justified at this scale

**When to reconsider:**
- Scaling beyond 8 nodes
- Multi-tenancy requirements
- Service mesh (mTLS) needs

#### Docker Compose

**Why rejected:**
- Docker daemon is a single point of failure
- ~100-200MB daemon overhead
- Less secure (requires root daemon)

**When appropriate:**
- Local development (use for dev/test parity)

#### Plain systemd

**Why rejected:**
- No container isolation (dependency conflicts)
- Poor dev/prod parity
- No image-based rollback
- Security: processes run directly on host

### Consequences

**Positive:**
- Maximum memory available for GPU/FAISS workloads
- Familiar Linux tooling (systemctl, journalctl)
- Rootless security by default
- Native systemd recovery and logging
- Easy rollback via image tags

**Negative:**
- Multi-node coordination requires Ansible/scripts
- Rolling updates not automatic
- Less official NVIDIA Jetson documentation than Docker
- Team may need to learn Podman/quadlet specifics

**Neutral:**
- Similar developer experience to Docker (CLI compatible)
- Monitoring integration unchanged (Prometheus scraping)

---

## Validation Checklist for v1.2

Before signing off on architecture:

- [ ] **Hardware:** Jetson Thor acquired and operational
- [ ] **FAISS:** GPU IVF-Flat benchmark at reference config
- [ ] **SLO:** Actual latency/recall documented
- [ ] **cuVS:** 24h shadow mode (if pursuing)
- [ ] **Delete:** Tombstone filtering validated
- [ ] **Rebuild:** Dual-index swap tested with concurrent ingest
- [ ] **Consistency:** Read-your-writes <100ms validated
- [ ] **Containers:** Podman + quadlets deployed on Thor (NEW)
- [ ] **GPU passthrough:** CDI working with FAISS GPU (NEW)
- [ ] **Rolling updates:** Script tested (NEW)

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial ADRs |
| 1.1 | 2025-01-20 | AkiDB Team | cuVS gate, SLO boundaries, delete/update contract, ID management, consistency guarantees |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration strategy (Podman + quadlets) |

---

*End of ADR v1.2*
