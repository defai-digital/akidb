# AkiDB Thor Edition - Architecture Decision Records (ADR)
## Version 1.6

**Version:** 1.6
**Date:** 2026-01-21
**Status:** Approved
**Changes from v1.4:** ADR-017 amended to Docker (was Podman), added ADR-021 Docker security hardening
**Review:** Multi-model synthesis (Claude, Gemini, Grok) - Container strategy re-evaluation

---

## Change Log from v1.4

| Section | Change | Rationale |
|---------|--------|-----------|
| ADR-017 | **AMENDED**: Podman → Docker for Jetson Thor | NVIDIA ecosystem alignment, jetson-containers support |
| ADR-021 | NEW: Docker Security Hardening | Mitigate Docker daemon attack surface |
| All | Version unified to 1.6 | Align PRD, ADR, Implementation Plan versions |

---

## Table of Contents

- [ADR-002: Vector Index Strategy](#adr-002) *(unchanged)*
- [ADR-009: Index Lifecycle](#adr-009) *(unchanged)*
- [ADR-015: ID Management Contract](#adr-015) *(unchanged)*
- [ADR-016: Consistency Guarantees](#adr-016) *(unchanged)*
- [ADR-017: Container Orchestration Strategy (AMENDED)](#adr-017-container-orchestration-strategy-amended)
- [ADR-018: Hybrid Ingestion Pipeline](#adr-018) *(unchanged from v1.4)*
- [ADR-019: NATS Cluster Sizing](#adr-019) *(unchanged from v1.4)*
- [ADR-020: Resilience Patterns](#adr-020) *(unchanged from v1.4)*
- [ADR-021: Docker Security Hardening (NEW)](#adr-021-docker-security-hardening)

---

## ADR-017: Container Orchestration Strategy (AMENDED)

### Status
**Amended** (January 2026)

### Original Decision (v1.2)
Podman + systemd quadlets for container orchestration.

### Amendment Context

New evidence from multi-model synthesis (Claude, Gemini, Grok) challenged the original Podman decision for **Jetson Thor specifically**:

1. **NVIDIA Official Documentation**: Thor Dev Kit documentation explicitly guides Docker with `default-runtime=nvidia`
2. **jetson-containers Ecosystem**: The `dusty-nv/jetson-containers` project is Docker-first with JetPack 7/CUDA 13.x support
3. **vLLM Source Build Requirements**: vLLM on Thor requires source compilation; Docker configurations are battle-tested
4. **nvidia-container-toolkit Issues**: JetPack 7.1 had apt dependency issues; Docker path has more community fixes
5. **Podman+CDI on Jetson**: Less community testing, more troubleshooting reports

### Amended Decision

**Docker + docker-compose** for all services on Jetson Thor (JetPack 7.x).

```
┌─────────────────────────────────────────────────────────────────┐
│           CONTAINER ORCHESTRATION DECISION (AMENDED)             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ORIGINAL (v1.2): Podman + systemd quadlets                    │
│  AMENDED (v1.6):  Docker + docker-compose                      │
│                                                                 │
│  Rationale for Amendment:                                       │
│  • NVIDIA official docs specify Docker for Jetson Thor         │
│  • jetson-containers provides tested vLLM/CUDA 13.x configs    │
│  • GPU passthrough reliability is highest on Docker            │
│  • Community knowledge concentration = faster issue resolution │
│  • Memory overhead (~100MB) is negligible on 64GB unified RAM  │
│                                                                 │
│  Trade-offs accepted:                                           │
│  • Docker daemon runs as root (mitigated via ADR-021)          │
│  • Less systemd integration than quadlets                       │
│  • Requires docker-compose for service management              │
│                                                                 │
│  Preserved for future:                                          │
│  • Podman configs in deploy/podman/ for JetPack 8.x evaluation │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Options Re-evaluated

| Option | Memory Overhead | Jetson Thor Support | GPU Reliability | Decision |
|--------|-----------------|---------------------|-----------------|----------|
| **Docker + compose** | ~100MB | **Official NVIDIA docs** | **Highest** | **SELECTED** |
| Podman + quadlets | ~0 | Community only | Moderate | DEFERRED |
| Kubernetes (K3s) | ~500MB-1GB | Unofficial | Good | REJECTED |
| Hybrid (Docker+Podman) | Mixed | Partial | Complex | REJECTED |

### Deployment Architecture (Amended)

```
┌─────────────────────────────────────────────────────────────────┐
│                 DEPLOYMENT TOPOLOGY (v1.6)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Thor Node 1-3: Shard Servers                                   │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  Container Runtime: Docker (nvidia-container-runtime)     │ │
│  │  Management: docker-compose.yml                           │ │
│  │  Services:                                                 │ │
│  │    • akidb-shard (GPU via --gpus all)                    │ │
│  │    • nats (JetStream, nodes 1-3 only)                    │ │
│  │    • minio                                                │ │
│  │  Persistent volumes: /var/lib/akidb, /var/lib/nats       │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Thor Node 1 Only: Ingestion Services                           │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  • ingestion-orchestrator (GPU for embedding)            │ │
│  │  • doc-parser (Python, no GPU)                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Thor Node 4: Coordinator                                       │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  Container Runtime: Docker                                │ │
│  │  Services:                                                 │ │
│  │    • akidb-coordinator (no GPU)                          │ │
│  │    • upload-gateway                                       │ │
│  │    • minio                                                │ │
│  │  Connects to NATS cluster on Thor 1-3                    │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Docker Configuration

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

### Review Triggers for Podman Re-evaluation

| Trigger | Condition |
|---------|-----------|
| JetPack 8.x release | NVIDIA adds official Podman/CDI documentation |
| jetson-containers | Adds mature Podman support |
| Community maturity | >50% of Jetson GPU container discussions use Podman |
| Security requirement | Rootless becomes mandatory for compliance |

### Consequences (Amended)

**Positive:**
- Aligned with NVIDIA's official documentation and QA testing
- Leverages jetson-containers ecosystem for vLLM builds
- Maximum GPU passthrough reliability
- Larger community for troubleshooting

**Negative:**
- Docker daemon attack surface (mitigated via ADR-021)
- ~100MB memory overhead (negligible on 64GB)
- Less native systemd integration
- Requires explicit security hardening

---

## ADR-021: Docker Security Hardening (NEW)

### Status
**Accepted**

### Context

The amendment of ADR-017 from Podman to Docker introduces security considerations. Docker daemon runs as root by default, creating an attack surface on edge devices with potential physical access.

### Decision

Implement mandatory security hardening measures for Docker deployments.

### Security Controls

#### 1. Container-Level Controls

```yaml
# All containers must include these settings
services:
  akidb-shard:
    user: "1000:1000"  # Non-root user
    read_only: true     # Read-only root filesystem
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    cap_add:
      - NET_BIND_SERVICE  # Only if needed
    tmpfs:
      - /tmp:size=100M
```

#### 2. GPU Service Exception

```yaml
# GPU services require specific capabilities
services:
  akidb-shard:
    # Standard hardening applies, plus:
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
    # Cannot use read_only due to CUDA cache
    volumes:
      - type: tmpfs
        target: /tmp
        tmpfs:
          size: 1073741824  # 1GB for CUDA cache
```

#### 3. Network Isolation

```yaml
networks:
  akidb-internal:
    driver: bridge
    internal: true  # No external access
  akidb-external:
    driver: bridge
    # Only coordinator and upload-gateway exposed
```

#### 4. Secrets Management

```yaml
secrets:
  minio-access-key:
    file: ./secrets/minio-access-key
  minio-secret-key:
    file: ./secrets/minio-secret-key

services:
  akidb-shard:
    secrets:
      - minio-access-key
      - minio-secret-key
    # NOT environment variables
```

#### 5. Resource Limits

```yaml
services:
  akidb-shard:
    deploy:
      resources:
        limits:
          memory: 48G      # Leave 16GB for system + other services
        reservations:
          memory: 32G
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
      memlock:
        soft: -1
        hard: -1
```

### Hardening Checklist

| Control | Requirement | Validation |
|---------|-------------|------------|
| Non-root containers | `user: "1000:1000"` in all services | `docker exec <container> id` returns non-root |
| Read-only filesystem | `read_only: true` where possible | Write attempts fail |
| No privilege escalation | `no-new-privileges:true` | seccomp audit |
| Capability drop | `cap_drop: ALL` + explicit adds | `docker inspect` shows minimal caps |
| Secret files | No secrets in environment variables | `docker inspect` shows no sensitive env vars |
| Resource limits | Memory and CPU limits set | `docker stats` shows limits |
| Log rotation | `max-size` and `max-file` set | `/var/lib/docker/containers` bounded |

### Consequences

**Positive:**
- Reduced attack surface despite Docker daemon running as root
- Defense in depth for edge deployment
- Compliance with security best practices
- Clear validation checklist

**Negative:**
- Additional complexity in docker-compose files
- GPU services cannot use full read-only mode
- Some performance overhead from seccomp

---

## ADR-018: Hybrid Ingestion Pipeline

*(Unchanged from v1.4 - see previous version)*

### Summary

- Rust orchestrator (tokio) for 60-70% of documents
- Python parser service for complex formats (PDF, DOCX-complex, ENL)
- Format-aware routing by file extension
- XLSX moved to Rust (calamine)

---

## ADR-019: NATS Cluster Sizing

*(Unchanged from v1.4 - see previous version)*

### Summary

- 3-node NATS JetStream cluster (not 4)
- Deployed on Thor nodes 1-3
- Thor 4 (coordinator) connects as client
- Quorum: 2 of 3 nodes

---

## ADR-020: Resilience Patterns

*(Unchanged from v1.4 - see previous version)*

### Summary

- Circuit breaker for Python parser (3 failures → open, 30s reset)
- Backpressure controller (AkiDB latency > 500ms → pause NATS)
- Memory coordinator (tegrastats, pause at 70% unified memory)

---

## v1.6 Production Readiness Checklist

### Critical (Must Pass)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| C-01 | Docker + nvidia-runtime installed | Infra | `docker run --gpus all nvidia/cuda nvidia-smi` |
| C-02 | NATS 3-node cluster deployed | Infra | `nats cluster info` shows 3 nodes |
| C-03 | Circuit breaker implemented | Dev | Unit tests for all state transitions |
| C-04 | Backpressure tested | QA | Load test: AkiDB saturated → queue bounded |
| C-05 | Memory coordinator active | Dev | tegrastats integration verified |
| C-06 | Core metrics exported | Ops | Prometheus scraping all targets |
| C-07 | 30-min SLO validated | QA | End-to-end test passes |
| C-08 | Docker security hardening | Security | ADR-021 checklist passed |

### High Priority (Strongly Recommended)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| H-01 | Semantic chunking | Dev | A/B test vs fixed chunking |
| H-02 | Dynamic batching | Dev | Queue depth → batch size logged |
| H-03 | XLSX in Rust (calamine) | Dev | Parse 1000 XLSX files |
| H-04 | Idempotency layer | Dev | Duplicate detection tests |
| H-05 | Pre-signed URL hardening | Security | Penetration test passed |
| H-06 | GPU metrics via DCGM | Ops | Grafana dashboard working |
| H-07 | jetson-containers base image | Dev | vLLM builds successfully |

### Review Triggers

| Trigger | Action |
|---------|--------|
| JetPack 8.x release | Re-evaluate Podman viability |
| NVIDIA Podman docs for Jetson | POC Podman deployment |
| Security audit finding | Review ADR-021 controls |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial ADRs |
| 1.1 | 2025-01-20 | AkiDB Team | cuVS gate, SLO boundaries |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets) |
| 1.3 | 2026-01-21 | AkiDB Team | Hybrid ingestion pipeline |
| 1.4 | 2026-01-21 | AkiDB Team | NATS 3-node, resilience patterns |
| 1.6 | 2026-01-21 | AkiDB Team | **Docker amendment**, security hardening |

---

*End of ADR v1.6*
