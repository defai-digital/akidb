# AkiDB Product Requirements Document

**Version:** 2.0
**Date:** 2026-06-26
**Status:** Draft
**License:** Apache-2.0
**Related ADR:** [ADR-0001: Mac-First Cell Architecture](../adr/ADR-0001-mac-first-cell-architecture.md)

---

## 1. Executive Summary

AkiDB is a Mac-first vector database for private, local, and edge retrieval workloads. The product must run well on a single Apple Silicon Mac, scale up to a validated four-Mac Thunderbolt cell, and scale horizontally by adding additional cells.

The core product bet is not "distributed by default." The core bet is that one Mac can be a strong, low-ops vector search appliance, and that a four-Mac Thunderbolt cell can deliver a useful step function in capacity, resilience, and throughput without introducing data-center-grade operational complexity.

AkiDB v2 supports macOS Apple Silicon only. Thor, CUDA, NVIDIA GPU, and Linux ARM deployment paths are out of scope.

---

## 2. Product Positioning

### 2.1 Primary Users

| User | Need | AkiDB Value |
| --- | --- | --- |
| AI application developer | Run private RAG locally with low setup cost | One-command local vector DB on Apple Silicon |
| Small AI team | Keep sensitive embeddings on-prem or in office hardware | Single-Mac or four-Mac deployment without managed cloud |
| Edge operator | Search local documents, logs, media, or device data | Appliance-style deployment with snapshots and recovery |
| Research / prototyping team | Scale from local development to a small cluster | Same API from one Mac to one Thunderbolt cell |

### 2.2 Market Gap

Existing vector databases generally fall into two groups:

- Cloud/Kubernetes-first distributed systems.
- Local/embedded systems that are developer-friendly but do not offer a strong local cluster story.

AkiDB targets the gap between them: Mac-first local production, with an explicit four-Mac cell design for users who want more capacity or resilience without adopting a full Kubernetes platform.

---

## 3. Deployment Model

### 3.1 Supported Topologies

| Topology | Status | Purpose | Product Promise |
| --- | --- | --- | --- |
| One Mac | P0 | Baseline production appliance and development target | Simple, reliable, measurable local vector DB |
| Four-Mac Thunderbolt cell | P0 | First distributed production shape | More capacity, higher read throughput, node-loss tolerance when configured with replicas |
| Multiple cells | P1 | Horizontal growth beyond one cell | Route by collection, tenant, or shard group |
| Arbitrary N-node mesh | Non-goal for v2.0 | Avoid unbounded topology complexity | Not promised |

### 3.2 One-Mac Appliance

The one-Mac deployment is the default product experience.

Requirements:

| ID | Requirement | Priority |
| --- | --- | --- |
| MAC-001 | AkiDB must run on Apple Silicon macOS without Docker for the core server path. | P0 |
| MAC-002 | A single Mac deployment must support local persistent storage, WAL, snapshots, and restart recovery. | P0 |
| MAC-003 | A single Mac deployment must expose the same client API as a cell deployment. | P0 |
| MAC-004 | The local process must fail fast on unsupported CPU architecture or insufficient memory. | P0 |
| MAC-005 | The local deployment must support import, search, delete, reindex, and snapshot workflows. | P0 |

### 3.3 Four-Mac Thunderbolt Cell

A cell is the smallest distributed production unit. It contains four Macs connected with a validated Thunderbolt networking topology.

Requirements:

| ID | Requirement | Priority |
| --- | --- | --- |
| CELL-001 | A cell must contain exactly four data nodes in v2.0. | P0 |
| CELL-002 | A cell must support direct search fan-out across shards within the cell. | P0 |
| CELL-003 | A cell must support replica placement so one node can fail without losing searchable coverage. | P0 |
| CELL-004 | A cell must expose one logical endpoint through a coordinator. | P0 |
| CELL-005 | A cell must continuously measure link health, per-node load, and tail latency. | P0 |
| CELL-006 | A cell must degrade predictably when one node or one link fails. | P0 |
| CELL-007 | A cell must not require Kubernetes for the initial production path. | P0 |

### 3.4 Multi-Cell Horizontal Scaling

Horizontal scaling happens by adding cells, not by growing one unbounded Thunderbolt mesh.

Requirements:

| ID | Requirement | Priority |
| --- | --- | --- |
| SCALE-001 | AkiDB must model each four-Mac cell as an independent failure and capacity domain. | P1 |
| SCALE-002 | Coordinators must route requests by collection, tenant, or shard group before fan-out. | P1 |
| SCALE-003 | Cross-cell fan-out must be explicit and observable, not the default for every query. | P1 |
| SCALE-004 | Cell addition must not require reformatting existing on-disk data. | P1 |

---

## 4. Hardware Compatibility

### 4.1 Reference Mac SKUs

The product must define benchmarked reference SKUs. Exact models may change as Apple releases new hardware, but every release must document:

- SoC generation.
- CPU core count.
- Unified memory capacity.
- Memory bandwidth.
- Internal SSD size.
- Thunderbolt generation.
- macOS version.

### 4.2 Homogeneous and Heterogeneous Cells

The first production cell must be homogeneous: same SoC class, same RAM, same SSD class, same Thunderbolt generation.

Heterogeneous nodes are allowed only under explicit capacity-aware placement.

Requirements:

| ID | Requirement | Priority |
| --- | --- | --- |
| HW-001 | The default four-Mac cell documentation must require matching reference SKUs. | P0 |
| HW-002 | AkiDB must detect CPU, memory, disk, and link capability at node join time. | P0 |
| HW-003 | A node that cannot hold an assigned shard replica must be rejected for that placement. | P0 |
| HW-004 | Heterogeneous placement must use node weights for shard count, replica count, and query admission. | P1 |
| HW-005 | Hot shard replica groups should use same-class nodes unless the operator overrides policy. | P1 |

Rationale: in a fan-out search path, the slowest node often controls P95/P99. Mixed hardware is useful for cold storage, ingestion, background indexing, and overflow capacity, but it should not silently enter a latency-critical replica group.

---

## 5. Core Functional Requirements

### 5.1 Vector Search

| ID | Requirement | Priority |
| --- | --- | --- |
| SEARCH-001 | Support dense vector search with topK. | P0 |
| SEARCH-002 | Support metadata filtering before or during vector search where possible. | P0 |
| SEARCH-003 | Support tombstoned vector exclusion. | P0 |
| SEARCH-004 | Support exact or approximate backend selection behind a stable API. | P1 |
| SEARCH-005 | Return per-shard timing diagnostics in debug mode. | P1 |

### 5.2 Ingestion and Lifecycle

| ID | Requirement | Priority |
| --- | --- | --- |
| ING-001 | Support batch ingestion with durable WAL before vectors become visible. | P0 |
| ING-002 | Support idempotent ingestion using content hash and source identity. | P0 |
| ING-003 | Support soft delete and delayed compaction. | P0 |
| ING-004 | Support versioned reindexing with shadow insert before tombstoning old vectors. | P0 |
| ING-005 | Protect search SLOs with ingestion backpressure. | P0 |

### 5.3 Snapshots and Recovery

| ID | Requirement | Priority |
| --- | --- | --- |
| REC-001 | Single Mac must support local snapshot and restore. | P0 |
| REC-002 | Cell deployment must support per-node snapshots with a cell-level manifest. | P0 |
| REC-003 | Restore must verify shard identity, replica identity, and index version before serving traffic. | P0 |
| REC-004 | Recovery from one failed node in a replicated cell must not require full cell downtime. | P1 |

### 5.4 API Compatibility

| ID | Requirement | Priority |
| --- | --- | --- |
| API-001 | Client APIs must not expose whether the target is one Mac or a cell for normal operations. | P0 |
| API-002 | Administrative APIs must expose topology, placement, health, and recovery state. | P0 |
| API-003 | The wire API must be stable across one-Mac and four-Mac cell deployments. | P1 |

---

## 6. Performance Requirements

Performance targets must be measured separately for one Mac and four-Mac cell deployments. Claims are not accepted without benchmark artifacts.

### 6.1 Reference Workload

The default benchmark workload is:

- Dimension: 768 and 1536.
- Dataset sizes: 1M, 10M, and maximum resident set for the reference SKU.
- topK: 10.
- Query mix: single-query interactive and batched.
- Metadata filter selectivity: none, 10 percent, 1 percent.

### 6.2 SLO Targets

| Metric | One Mac Target | Four-Mac Cell Target | Priority |
| --- | --- | --- | --- |
| Search P95, 1M vectors, topK 10 | < 50 ms | < 50 ms | P0 |
| Search P99, 1M vectors, topK 10 | < 100 ms | < 100 ms | P0 |
| Read throughput | Baseline | >= 2.5x one Mac on same dataset class | P0 |
| One-node failure read availability | Snapshot restore path | Full search coverage with RF >= 2 | P0 |
| Restart recovery | < 5 minutes for 1M vectors | < 10 minutes per cell | P1 |

The four-Mac cell must prove either better throughput, better availability, or larger resident capacity than one Mac. If it does not, the product should recommend one Mac.

---

## 7. Safety and Correctness

### 7.1 Safety Requirements

| ID | Requirement | Priority |
| --- | --- | --- |
| SAFE-001 | A vector must not become visible before its metadata and WAL entry are durable. | P0 |
| SAFE-002 | A shard must not be assigned to a node that lacks enough memory or disk headroom. | P0 |
| SAFE-003 | A query must return an explicit degraded status if any required shard is unavailable and no replica is available. | P0 |
| SAFE-004 | A replicated cell must keep shard replicas on different physical Macs. | P0 |
| SAFE-005 | Cell metadata consensus must avoid even-quorum deadlock. | P0 |

### 7.2 Consistency Model

AkiDB uses pragmatic read visibility:

- Writes are durable before acknowledgement.
- Search visibility may lag until index publication.
- Per-document read-your-writes is required after the ingest operation reports visible.
- Cross-cell global transactions are a non-goal for v2.0.

---

## 8. Observability

Requirements:

| ID | Requirement | Priority |
| --- | --- | --- |
| OBS-001 | Expose node, shard, replica, and cell health. | P0 |
| OBS-002 | Expose Thunderbolt link throughput, error, and latency metrics where available. | P0 |
| OBS-003 | Expose search fan-out timing by shard in debug mode. | P0 |
| OBS-004 | Emit structured logs for node join, node leave, placement change, failover, and restore. | P0 |
| OBS-005 | Provide a local TUI or CLI status command for one Mac and cell deployments. | P1 |

---

## 9. Security

Requirements:

| ID | Requirement | Priority |
| --- | --- | --- |
| SEC-001 | Default local deployment must bind only to localhost unless explicitly configured. | P0 |
| SEC-002 | Cell internode traffic must support authentication. | P0 |
| SEC-003 | Administrative APIs must require explicit credentials outside localhost. | P0 |
| SEC-004 | Snapshots must include integrity checks. | P0 |
| SEC-005 | Optional at-rest encryption is P1, not a v2.0 release blocker. | P1 |

---

## 10. Non-Goals

- Arbitrary-size Thunderbolt mesh.
- Kubernetes-first deployment.
- Cross-cell distributed transactions.
- Claiming high availability for a two-node cluster.
- Treating heterogeneous nodes as equal in a hot search cell.
- Replacing cloud-scale vector databases for multi-region enterprise workloads.
- CUDA, FAISS GPU, cuVS, Thor, or Linux ARM support.

---

## 11. Release Gates

### 11.1 v2.0 Alpha

- One-Mac server runs on Apple Silicon.
- Durable local storage, ingestion, search, delete, snapshot.
- Basic coordinator API shape preserved for future cell use.
- Benchmarks published for one reference Mac.

### 11.2 v2.0 Beta

- Four-Mac Thunderbolt cell forms reliably.
- Shard placement and replica placement implemented.
- One-node read failover demonstrated.
- Cell benchmark shows measurable value over one Mac.

### 11.3 v2.0 GA

- Documented reference SKU.
- Reproducible benchmark suite.
- Recovery runbook.
- Security checklist.
- Apache-2.0 metadata and LICENSE file complete.

---

## 12. Open Questions

| Question | Owner | Target |
| --- | --- | --- |
| Which Mac SKU is the first reference cell? | Product / Engineering | Alpha |
| Which Thunderbolt topology is validated first: full mesh or constrained direct links? | Engineering | Alpha |
| Should the first backend be exact CPU, HNSW, FAISS CPU, or a custom portable backend? | Engineering | Alpha |
| How much query fan-out should be allowed across cells by default? | Product / Engineering | Beta |
| What is the minimum supported macOS version? | Engineering | Alpha |
