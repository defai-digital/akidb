# AkiDB Thor Edition - Implementation Plan

**Version:** 1.6
**Date:** 2026-01-21
**Status:** Approved
**Based On:** ADR v1.6, PRD v1.6
**Review:** Multi-model synthesis (Claude, Gemini, Grok)
**Changes from v1.3:** Docker deployment (was Podman), security hardening, version unified

---

## Change Log from v1.3

| Section | Change | Rationale |
|---------|--------|-----------|
| All | Podman + quadlets → Docker + docker-compose | ADR-017 amendment (NVIDIA ecosystem) |
| Phase 0 | Docker validation tasks | nvidia-container-runtime setup |
| Phase 2 | Dockerfile instead of quadlet tasks | Docker-based builds |
| Phase 4 | docker-compose deployment | Production deployment |
| All | Version unified to 1.6 | Align PRD, ADR, Implementation Plan |

---

## Executive Summary

This implementation plan covers the development of AkiDB Thor Edition over **~26 weeks (~6.5 months)** across 4 phases plus a validation sprint. This version uses **Docker + docker-compose** deployment aligned with NVIDIA's official Jetson Thor documentation.

**Key Updates in v1.6:**
- Docker + nvidia-container-runtime (NVIDIA recommended path)
- docker-compose.yml for service orchestration
- Docker security hardening (ADR-021)
- Reference to jetson-containers ecosystem for vLLM builds
- Ansible for multi-node Docker deployment

---

## Current Progress Assessment

### Completed (Phase 0-1)

| Component | Status | Location |
|-----------|--------|----------|
| FAISS GPU wrapper | **DONE** | `crates/faiss-wrapper/` |
| RocksDB storage | **DONE** | `crates/storage/` |
| gRPC server | **DONE** | `crates/grpc-server/` |
| Coordinator (fanout, merger) | **DONE** | `crates/coordinator/` |
| Generic backpressure | **DONE** | `crates/coordinator/src/backpressure.rs` |
| Embedding service | **DONE** | `crates/coordinator/src/embedding.rs` |
| Batch processing | **DONE** | `crates/coordinator/src/batch.rs` |
| Common types/config | **DONE** | `crates/common/` |
| Benchmark crate | **DONE** | `crates/benchmark/` |
| K8s manifests (reference) | **DONE** | `deploy/kubernetes/` |
| Ansible structure | **PARTIAL** | `deploy/ansible/` |

### Not Started (Phase 2 Hybrid Ingestion + Docker)

| Component | Status | New Location |
|-----------|--------|--------------|
| Ingestion orchestrator (Rust) | **NOT STARTED** | `crates/ingestion-orchestrator/` |
| Rust parsers | **NOT STARTED** | `crates/ingestion-orchestrator/src/parsers/` |
| Python parser service | **NOT STARTED** | `services/doc-parser/` |
| Upload gateway | **NOT STARTED** | `services/upload-gateway/` |
| Docker compose files | **NOT STARTED** | `deploy/docker/` |
| Dockerfiles | **NOT STARTED** | Various `Dockerfile` locations |

---

## Timeline Overview

```
------------------------------------------------------------------------------
                      AKIDB THOR IMPLEMENTATION TIMELINE v1.6
------------------------------------------------------------------------------

  Week 0       | Weeks 1-6     | Weeks 7-16     | Weeks 17-22  | Weeks 23-26
  +---------+  | +----------+  | +------------+ | +----------+ | +----------+
  |VALIDATION|  | | PHASE 1  |  | |  PHASE 2   | | | PHASE 3  | | | PHASE 4  |
  | SPRINT  |  | |Foundation|  | |  HYBRID    | | | Optimize | | |Production|
  | (1 week)|  | | (6 weeks)|  | | INGESTION  | | | (6 weeks)| | | (4 weeks)|
  +---------+  | +----------+  | | (10 weeks) | | +----------+ | +----------+
               |               | +------------+ |              |
  Hardware     | ~70% COMPLETE | Rust Orch.    | TensorRT     | Docker
  Docker+NVIDIA| (verify only) | + Python Parse| Rebuild      | Production
  NATS 3-node  |               | + Resilience  | Performance  | Compose
               |               | + Dockerfiles |              |

------------------------------------------------------------------------------
```

**Total Duration:** 26-27 weeks (~6.5 months)

---

## Phase 0: Validation Sprint (Week 0)

### Objectives
- Validate hardware environment
- **Setup Docker with nvidia-container-runtime**
- Verify existing Phase 1 implementation
- Test NATS on ARM64

### Validation Tasks

| ID | Task | Owner | Duration | Exit Criteria |
|----|------|-------|----------|---------------|
| V-01 | Verify Thor hardware specs | DevOps | 0.5 day | 64GB unified memory confirmed |
| V-02 | **Install Docker + nvidia-container-runtime** | DevOps | 1 day | `docker run --gpus all nvidia/cuda nvidia-smi` works |
| V-03 | **Configure /etc/docker/daemon.json** | DevOps | 0.5 day | `default-runtime: nvidia` set |
| V-04 | Test NATS 2.10 on ARM64 | DevOps | 0.5 day | NATS server runs, JetStream enabled |
| V-05 | Verify existing FAISS build | Rust Eng | 1 day | cargo test in faiss-wrapper passes |
| V-06 | Verify existing coordinator | Rust Eng | 1 day | cargo test in coordinator passes |
| V-07 | Benchmark single-node FAISS | ML Eng | 1 day | IVF-Flat search <10ms for 1M vectors |
| V-08 | Test Python 3.11 runtime | DevOps | 0.5 day | python3 --version succeeds |
| V-09 | Validate network latency | DevOps | 0.5 day | <1ms inter-node latency |
| V-10 | Test tegrastats availability | DevOps | 0.5 day | tegrastats returns memory stats |
| V-11 | **Pull jetson-containers base images** | DevOps | 0.5 day | vLLM base image available |

### Docker Setup Validation

```bash
# V-02: Install Docker + NVIDIA runtime
sudo apt-get update
sudo apt-get install -y docker.io nvidia-container-toolkit

# V-03: Configure daemon.json
cat /etc/docker/daemon.json
# Should show: "default-runtime": "nvidia"

# Restart Docker
sudo systemctl restart docker

# Validate GPU passthrough
docker run --rm --gpus all nvidia/cuda:12.0-base nvidia-smi
```

### Phase 0 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| Docker + nvidia-runtime | `nvidia-smi` inside container | [ ] |
| NATS on ARM64 | JetStream operational | [ ] |
| Existing tests pass | cargo test all-green | [ ] |
| Network latency | <1ms inter-node | [ ] |
| tegrastats access | Memory stats readable | [ ] |
| jetson-containers | Base images pulled | [ ] |

---

## Phase 1: Foundation Completion (Weeks 1-6)

### Status: ~70% Complete

Most of Phase 1 is already implemented. This phase focuses on verification, documentation, and Docker setup.

### Objectives
- Verify and document existing implementation
- **Create Dockerfiles for existing services**
- Prepare for Phase 2 hybrid ingestion

### Sprint 1-2 (Weeks 1-4): Verification & Dockerfiles

| ID | Task | Priority | Estimate | Status |
|----|------|----------|----------|--------|
| P1-01 | Document existing FAISS wrapper | P1 | 2d | NEW |
| P1-02 | Document coordinator architecture | P1 | 2d | NEW |
| P1-03 | Verify gRPC API contract | P0 | 1d | NEW |
| P1-04 | **Create Dockerfile for akidb-server** | P0 | 2d | NEW |
| P1-05 | **Create Dockerfile for akidb-coordinator** | P0 | 2d | NEW |
| P1-06 | Test distributed fan-out | P0 | 3d | VERIFY |
| P1-07 | Performance baseline benchmarks | P0 | 3d | NEW |
| P1-08 | Fix any failing tests | P0 | 2d | AS NEEDED |
| P1-09 | CI/CD pipeline setup | P0 | 3d | NEW |

### Sprint 3 (Weeks 5-6): Pre-Ingestion Setup

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-10 | Create ingestion-orchestrator crate scaffold | P0 | 2d | - |
| P1-11 | Setup services/ directory structure | P0 | 1d | - |
| P1-12 | **Create deploy/docker/ directory structure** | P0 | 1d | - |
| P1-13 | NATS configuration for 3-node cluster | P0 | 2d | V-04 |
| P1-14 | MinIO bucket notification setup | P0 | 2d | - |
| P1-15 | Prometheus/Grafana dashboards | P1 | 2d | - |

### Dockerfile: akidb-server

```dockerfile
# crates/server/Dockerfile
FROM dustynv/l4t-pytorch:r36.4.0-torch2.5 AS builder

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Build dependencies
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin akidb-server

# Runtime image
FROM dustynv/l4t-pytorch:r36.4.0-torch2.5

# Create non-root user
RUN groupadd -r akidb && useradd -r -g akidb akidb

# Copy binary
COPY --from=builder /app/target/release/akidb-server /usr/local/bin/
COPY --from=builder /app/target/release/grpc_health_probe /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown akidb:akidb /data

USER akidb
WORKDIR /data

EXPOSE 50051
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD grpc_health_probe -addr=:50051

ENTRYPOINT ["akidb-server"]
```

### Phase 1 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| All existing tests pass | 100% green | [ ] |
| Single-node search P95 | < 10ms | [ ] |
| gRPC API documented | OpenAPI spec | [ ] |
| CI/CD operational | GitHub Actions | [ ] |
| akidb-server Dockerfile | Builds + runs | [ ] |
| akidb-coordinator Dockerfile | Builds + runs | [ ] |
| ingestion-orchestrator scaffold | Cargo builds | [ ] |

---

## Phase 2: Hybrid Ingestion Pipeline (Weeks 7-16)

### Objectives
- Implement Rust ingestion orchestrator
- Create Python parser service
- Deploy NATS 3-node JetStream cluster
- Implement all resilience patterns
- **Create all Dockerfiles and docker-compose files**
- Achieve 30-minute upload-to-searchable SLO

### Directory Structure

```
deploy/
├── docker/
│   ├── docker-compose.shard.yml       # Thor 1-3: shard services
│   ├── docker-compose.ingestion.yml   # Thor 1: ingestion services
│   ├── docker-compose.coordinator.yml # Thor 4: coordinator services
│   ├── docker-compose.monitoring.yml  # All: Prometheus + Grafana
│   ├── config/
│   │   ├── akidb.toml
│   │   ├── coordinator.toml
│   │   └── nats.conf
│   └── secrets/
│       ├── .gitkeep
│       └── README.md  # Instructions for secrets
├── ansible/
│   ├── playbooks/
│   │   ├── deploy-docker.yml
│   │   └── update-services.yml
│   └── inventory.yml
└── podman/  # Preserved for future JetPack 8.x evaluation
    └── README.md
```

### Sprint Breakdown

#### Sprint 4 (Weeks 7-8): NATS + Rust Orchestrator Foundation

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-01 | Deploy NATS 3-node JetStream cluster | P0 | 2d | P1-13 |
| P2-02 | **Create NATS docker-compose service** | P0 | 1d | P2-01 |
| P2-03 | NATS consumer in Rust (async_nats) | P0 | 3d | P2-01 |
| P2-04 | MinIO event notification → NATS | P0 | 1d | P2-01 |
| P2-05 | Basic orchestrator pipeline scaffold | P0 | 2d | P1-10 |
| P2-06 | Configuration loading (envconfig) | P1 | 1d | P2-05 |

**Sprint 4 Exit Criteria:**
- [ ] NATS 3-node cluster running via Docker
- [ ] MinIO uploads trigger NATS events
- [ ] Rust consumer receives messages

#### Sprint 5 (Weeks 9-10): Rust Parsers + Format Router

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-07 | Format router (extension-based) | P0 | 1d | P2-05 |
| P2-08 | JSON parser (serde_json) | P0 | 1d | P2-07 |
| P2-09 | CSV parser (csv crate) | P0 | 1d | P2-07 |
| P2-10 | HTML parser (scraper) | P0 | 2d | P2-07 |
| P2-11 | XML parser (quick-xml) | P0 | 2d | P2-07 |
| P2-12 | XLSX parser (calamine) | P0 | 2d | P2-07 |
| P2-13 | Simple DOCX parser (docx-rs) | P1 | 2d | P2-07 |
| P2-14 | Parser unit tests | P0 | 1d | P2-08..P2-13 |

**Sprint 5 Exit Criteria:**
- [ ] JSON, CSV, HTML, XML, XLSX parsed in Rust
- [ ] Format router correctly routes by extension
- [ ] Parser tests passing

#### Sprint 6 (Weeks 11-12): Python Parser Service + Dockerfiles

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-15 | Create doc-parser FastAPI scaffold | P0 | 1d | - |
| P2-16 | PDF parser (pdfplumber) | P0 | 2d | P2-15 |
| P2-17 | Complex DOCX parser (python-docx) | P0 | 2d | P2-15 |
| P2-18 | ENL parser (custom) | P1 | 2d | P2-15 |
| P2-19 | HTTP client in Rust orchestrator | P0 | 2d | P2-15 |
| P2-20 | **doc-parser Dockerfile** | P0 | 1d | P2-15 |
| P2-21 | **ingestion-orchestrator Dockerfile** | P0 | 1d | P2-05 |
| P2-22 | **upload-gateway Dockerfile** | P0 | 1d | - |

### Dockerfile: doc-parser (Python)

```dockerfile
# services/doc-parser/Dockerfile
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r parser && useradd -r -g parser parser

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY parser/ ./parser/

# Create temp directory
RUN mkdir -p /tmp/parser && chown parser:parser /tmp/parser

USER parser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "parser.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### Dockerfile: ingestion-orchestrator (Rust)

```dockerfile
# crates/ingestion-orchestrator/Dockerfile
FROM dustynv/l4t-pytorch:r36.4.0-torch2.5 AS builder

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin ingestion-orchestrator

FROM dustynv/l4t-pytorch:r36.4.0-torch2.5

RUN groupadd -r akidb && useradd -r -g akidb akidb

COPY --from=builder /app/target/release/ingestion-orchestrator /usr/local/bin/

RUN mkdir -p /var/lib/akidb && chown akidb:akidb /var/lib/akidb

USER akidb
WORKDIR /var/lib/akidb

EXPOSE 9090

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD curl -f http://localhost:9090/health || exit 1

ENTRYPOINT ["ingestion-orchestrator"]
```

**Sprint 6 Exit Criteria:**
- [ ] PDF, complex DOCX, ENL parsed in Python
- [ ] Rust orchestrator calls Python parser via HTTP
- [ ] All Dockerfiles build successfully

#### Sprint 7 (Weeks 13-14): Resilience Patterns (ADR-020)

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-23 | Circuit breaker for Python parser | P0 | 3d | P2-19 |
| P2-24 | Circuit breaker state metrics | P0 | 1d | P2-23 |
| P2-25 | AkiDB-latency backpressure controller | P0 | 3d | P2-05 |
| P2-26 | NATS consumption throttling | P0 | 1d | P2-25 |
| P2-27 | Memory coordinator (tegrastats) | P0 | 2d | - |
| P2-28 | Resilience integration tests | P0 | 2d | P2-23..P2-27 |

**Sprint 7 Exit Criteria:**
- [ ] Circuit breaker transitions work (CLOSED → OPEN → HALF-OPEN → CLOSED)
- [ ] Backpressure pauses on high AkiDB latency
- [ ] Memory coordinator pauses at 70% unified memory

#### Sprint 8 (Weeks 15-16): Chunking, Batching, Integration

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-29 | Semantic chunker (unicode-segmentation) | P0 | 3d | - |
| P2-30 | Dynamic batcher (queue-depth adaptive) | P0 | 2d | - |
| P2-31 | TensorRT-LLM embedding client | P0 | 2d | - |
| P2-32 | Idempotency layer (content-hash) | P0 | 2d | - |
| P2-33 | Document state tracker (SQLite) | P1 | 2d | - |
| P2-34 | Dead letter queue handler | P1 | 1d | P2-03 |
| P2-35 | Upload gateway (FastAPI) | P0 | 3d | - |
| P2-36 | Pre-signed URL generation | P0 | 1d | P2-35 |
| P2-37 | **docker-compose.shard.yml** | P0 | 1d | ALL |
| P2-38 | **docker-compose.ingestion.yml** | P0 | 1d | ALL |
| P2-39 | **docker-compose.coordinator.yml** | P0 | 1d | ALL |
| P2-40 | End-to-end integration test | P0 | 3d | ALL |

**Sprint 8 Exit Criteria:**
- [ ] Semantic chunking produces sentence-boundary chunks
- [ ] Dynamic batching adjusts to queue depth
- [ ] All docker-compose files work
- [ ] End-to-end: Upload → Parse → Chunk → Embed → Search works
- [ ] 30-minute SLO validated

### Phase 2 Deliverables

| Deliverable | Description |
|-------------|-------------|
| `crates/ingestion-orchestrator/` | Rust orchestrator with all resilience patterns |
| `crates/ingestion-orchestrator/Dockerfile` | Orchestrator Docker image |
| `services/doc-parser/` | Python parser service (FastAPI) |
| `services/doc-parser/Dockerfile` | Parser Docker image |
| `services/upload-gateway/` | Upload gateway with pre-signed URLs |
| `services/upload-gateway/Dockerfile` | Gateway Docker image |
| `deploy/docker/docker-compose.shard.yml` | Shard node services |
| `deploy/docker/docker-compose.ingestion.yml` | Ingestion services |
| `deploy/docker/docker-compose.coordinator.yml` | Coordinator services |

### Phase 2 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| NATS 3-node cluster | Quorum operational | [ ] |
| Rust parsing ratio | 60-70% of documents | [ ] |
| Circuit breaker | All state transitions tested | [ ] |
| Backpressure | Throttles on AkiDB latency >500ms | [ ] |
| Memory coordinator | Pauses at 70% unified memory | [ ] |
| Semantic chunking | ~512 tokens, sentence boundaries | [ ] |
| Upload → Search SLO | < 30 minutes (P95) | [ ] |
| Docker images | All build and run | [ ] |
| docker-compose | Services start correctly | [ ] |

---

## Phase 3: Optimization (Weeks 17-22)

### Objectives
- Integrate TensorRT-optimized models
- Implement index rebuild
- Performance tuning
- Ingestion optimization
- **Security hardening (ADR-021)**

### Sprint 9-10 (Weeks 17-20): TensorRT + Index Rebuild

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-01 | TensorRT model optimization | P0 | 4d | Phase 2 |
| P3-02 | Index rebuild strategy | P0 | 5d | Phase 1 |
| P3-03 | Async rebuild with zero downtime | P0 | 4d | P3-02 |
| P3-04 | Compaction scheduling | P1 | 3d | P3-02 |
| P3-05 | Performance profiling | P0 | 2d | ALL |

### Sprint 11-12 (Weeks 21-22): Security Hardening + Load Testing

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-06 | Optimize embedding batch sizes | P0 | 2d | Phase 2 |
| P3-07 | Add ENL parser to Python service | P2 | 2d | Phase 2 |
| P3-08 | Ingestion load testing (1000 docs/hr) | P0 | 3d | Phase 2 |
| P3-09 | Thermal throttling (batch reduction) | P1 | 2d | P2-27 |
| P3-10 | Cold start handling (503 until ready) | P1 | 1d | - |
| P3-11 | DLQ auto-recovery | P1 | 1d | P2-34 |
| P3-12 | **Docker security hardening (ADR-021)** | P0 | 3d | Phase 2 |
| P3-13 | Pre-signed URL hardening | P0 | 2d | P2-36 |

### Phase 3 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| Search P95 latency | < 10ms | [ ] |
| Ingestion throughput | 1000 docs/hr | [ ] |
| Index rebuild | Zero-downtime | [ ] |
| TensorRT inference | < 20ms per batch | [ ] |
| Docker security | ADR-021 checklist passed | [ ] |

---

## Phase 4: Production Deployment (Weeks 23-26)

### Objectives
- Complete Docker deployment automation
- Production monitoring
- Documentation
- Handoff

### Sprint 13-14 (Weeks 23-26): Production Readiness

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P4-01 | **Finalize all docker-compose files** | P0 | 2d | Phase 2 |
| P4-02 | **Ansible playbook for Docker deployment** | P0 | 3d | P4-01 |
| P4-03 | Grafana dashboards (all 4 required) | P0 | 2d | - |
| P4-04 | Alerting rules (all thresholds from PRD) | P0 | 2d | P4-03 |
| P4-05 | Production load testing | P0 | 3d | ALL |
| P4-06 | Runbook documentation | P0 | 2d | - |
| P4-07 | cuVS evaluation gate | P1 | 3d | - |
| P4-08 | Security penetration test | P0 | 2d | - |
| P4-09 | Final sign-off checklist | P0 | 1d | ALL |

### Ansible Playbook: Deploy Docker Services

```yaml
# deploy/ansible/playbooks/deploy-docker.yml
---
- name: Deploy AkiDB Thor Cluster
  hosts: thor_nodes
  become: yes
  vars:
    akidb_version: "{{ lookup('env', 'AKIDB_VERSION') | default('latest', true) }}"

  tasks:
    - name: Ensure Docker is running
      service:
        name: docker
        state: started
        enabled: yes

    - name: Copy docker-compose files
      copy:
        src: "{{ item.src }}"
        dest: "/opt/akidb/{{ item.dest }}"
        mode: '0644'
      loop:
        - { src: 'docker-compose.shard.yml', dest: 'docker-compose.yml' }
      when: "'shard' in group_names"

    - name: Copy docker-compose files (coordinator)
      copy:
        src: "{{ item.src }}"
        dest: "/opt/akidb/{{ item.dest }}"
        mode: '0644'
      loop:
        - { src: 'docker-compose.coordinator.yml', dest: 'docker-compose.yml' }
      when: "'coordinator' in group_names"

    - name: Copy secrets
      copy:
        src: "secrets/"
        dest: "/opt/akidb/secrets/"
        mode: '0600'

    - name: Pull Docker images
      command: docker-compose pull
      args:
        chdir: /opt/akidb

    - name: Start services
      command: docker-compose up -d
      args:
        chdir: /opt/akidb

    - name: Wait for services to be healthy
      command: docker-compose ps
      args:
        chdir: /opt/akidb
      register: services_status
      until: "'unhealthy' not in services_status.stdout"
      retries: 30
      delay: 10
```

### Phase 4 Exit Gate (Production Readiness Checklist)

#### Critical (Must Pass)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| C-01 | Docker + nvidia-runtime on all nodes | Infra | `docker run --gpus all nvidia-smi` |
| C-02 | NATS 3-node cluster deployed | Infra | `nats cluster info` shows 3 nodes |
| C-03 | Circuit breaker implemented | Dev | Unit tests pass |
| C-04 | Backpressure tested | QA | Load test validates |
| C-05 | Memory coordinator active | Dev | tegrastats integration works |
| C-06 | Core metrics exported | Ops | Prometheus scraping |
| C-07 | 30-min SLO validated | QA | E2E test passes |
| C-08 | Docker security hardening | Security | ADR-021 checklist |

#### High Priority (Strongly Recommended)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| H-01 | Semantic chunking | Dev | A/B test |
| H-02 | Dynamic batching | Dev | Logged |
| H-03 | XLSX in Rust (calamine) | Dev | 1000 files |
| H-04 | Idempotency layer | Dev | Tests pass |
| H-05 | Pre-signed URL hardening | Security | Pen test |
| H-06 | GPU metrics via DCGM | Ops | Dashboard |
| H-07 | jetson-containers base | Dev | vLLM builds |
| H-08 | Ansible playbook | DevOps | Full deploy |

---

## Critical Path Dependencies (v1.6)

```
------------------------------------------------------------------------------
                  CRITICAL PATH DEPENDENCY DAG v1.6
------------------------------------------------------------------------------

  Week 0: Hardware + Docker + nvidia-runtime + NATS + tegrastats Validation
              │
              ▼
  Phase 1: Verify existing code ──► Dockerfiles ──► Scaffold ingestion
              │
              ▼
  Phase 2: ┌──────────────────────────────────────────────────────────┐
           │                                                          │
           │  NATS 3-node (Docker) ──► MinIO Events ──┐              │
           │                                           │              │
           │  Rust Parsers ───────────────────────────┼──► Router   │
           │                                           │      │       │
           │  Python Parser (Docker) ─────────────────┼──► CB       │
           │                                           │      │       │
           │                                           ▼      ▼       │
           │                    Orchestrator Pipeline                 │
           │                            │                             │
           │    ┌───────────────────────┼───────────────────┐        │
           │    │                       │                   │        │
           │    ▼                       ▼                   ▼        │
           │  Backpressure       Memory Coord      Semantic Chunker  │
           │         │                 │                 │            │
           │         └─────────────────┴─────────────────┘            │
           │                           │                              │
           │                           ▼                              │
           │                   Dynamic Batcher ──► TensorRT          │
           │                           │                              │
           │                           ▼                              │
           │                   docker-compose files                   │
           │                                                          │
           └──────────────────────────────────────────────────────────┘
              │
              ▼
  Phase 3: TensorRT ──► Rebuild ──► Security Hardening (ADR-021)
              │
              ▼
  Phase 4: Ansible ──► docker-compose deploy ──► Monitoring ──► Sign-off

------------------------------------------------------------------------------
```

---

## Team Allocation (v1.6)

| Role | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|------|---------|---------|---------|---------|---------|
| **Rust Engineer 1** | Verify FAISS | Dockerfile, CI/CD | **Orchestrator, Parsers** | Rebuild | Docker |
| **Rust Engineer 2** | Verify coord | Tests | **Circuit breaker, Backpressure** | Perf | Hardening |
| **Python Engineer** | - | - | **doc-parser, upload-gateway** | ENL | Testing |
| **ML Engineer** | FAISS bench | - | Chunker, Batcher | TensorRT | cuVS |
| **DevOps** | **Docker+NVIDIA**, NATS | CI/CD | **docker-compose, Ansible** | Load test | Deploy |

---

## Technology Stack (v1.6)

### Core (Rust)
- FAISS 1.8+ (GPU IVF-Flat)
- RocksDB 7.8+
- Tonic (gRPC)
- Tokio (async runtime)
- async_nats (NATS client)
- calamine (XLSX)
- scraper (HTML)
- quick-xml (XML)
- unicode-segmentation (sentence splitting)
- SQLite (document state)

### Ingestion Services (Python)
- Python 3.11
- FastAPI 0.109+
- pdfplumber 0.10+
- python-docx 1.1+
- Uvicorn

### Infrastructure
- **Docker + nvidia-container-runtime** (NVIDIA recommended)
- **docker-compose** for service orchestration
- **Ansible** for multi-node deployment
- NATS JetStream 2.10+ (3-node)
- MinIO (distributed)
- Prometheus + Grafana

### Base Images
- `dustynv/l4t-pytorch:r36.4.0-torch2.5` (GPU services)
- `python:3.11-slim` (Python services)
- `nats:2.10-alpine` (NATS)
- `minio/minio:latest` (MinIO)

---

## Deliverables Summary (v1.6)

| Phase | Deliverable | Description |
|-------|-------------|-------------|
| 0 | Validation report | Hardware + Docker + NATS + tegrastats confirmed |
| 1 | Dockerfiles | akidb-server, akidb-coordinator |
| 1 | Verified codebase | Existing code documented + tested |
| 2 | Rust orchestrator | `crates/ingestion-orchestrator/` |
| 2 | Python parser | `services/doc-parser/` |
| 2 | Upload gateway | `services/upload-gateway/` |
| 2 | docker-compose files | All service definitions |
| 2 | NATS 3-node | JetStream cluster operational |
| 2 | Resilience patterns | Circuit breaker, backpressure, memory |
| 3 | TensorRT models | Optimized inference |
| 3 | Security hardening | ADR-021 implemented |
| 4 | Ansible playbooks | Full cluster deployment |
| 4 | Dashboards | 4 Grafana dashboards |
| 4 | Runbook | Operations documentation |

---

## Open Questions (v1.6)

### Resolved

| ID | Question | Resolution |
|----|----------|------------|
| Q7 | Document parsing approach? | Hybrid: Rust orchestrator + Python parser |
| Q8 | Message queue? | NATS JetStream 3-node |
| Q9 | Chunking strategy? | Semantic (sentence-boundary) |
| Q10 | NATS cluster size? | 3-node (not 4) |
| Q11 | XLSX parser? | Rust (calamine) |
| Q12 | Container runtime? | **Docker + nvidia-container-runtime** |

### Remaining

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q13 | TensorRT vs vLLM for embedding? | TensorRT (primary) | Phase 3 |
| Q14 | Malware scanning for uploads? | ClamAV vs cloud API | Phase 3 |
| Q15 | OCR for scanned PDFs? | Tesseract vs cloud | Phase 3 |
| Q16 | cuVS replacement for FAISS? | Depends on benchmarks | Phase 4 |
| Q17 | Podman re-evaluation? | Wait for JetPack 8.x | Future |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial implementation plan |
| 1.1 | 2026-01-21 | AkiDB Team | Added Podman + quadlets deployment |
| 1.2 | 2026-01-21 | AkiDB Team | Added Python Ingestion Service, NATS, Upload Gateway |
| 1.3 | 2026-01-21 | AkiDB Team | Hybrid architecture, NATS 3-node, resilience patterns |
| 1.6 | 2026-01-21 | AkiDB Team | **Docker deployment**, security hardening, version unified |

---

*End of Implementation Plan v1.6*
