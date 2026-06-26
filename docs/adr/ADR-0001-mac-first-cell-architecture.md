# AkiDB Architecture Decision Records

**Version:** 2.0
**Date:** 2026-06-26
**Status:** Draft
**License:** Apache-2.0
**Related PRD:** [AkiDB PRD](../product/PRD.md)

---

## ADR-001: Mac-First Product Center

**Status:** Accepted

### Context

AkiDB previously centered on hardware-specific accelerator deployments. User feedback and product review identified a stronger near-term wedge: Apple Silicon Macs provide high memory bandwidth, fast local SSDs, strong developer availability, and a simpler local deployment story.

The market already has distributed vector databases and local vector databases, but there is a gap for a Mac-first vector DB that scales from one local Mac to a small high-speed local cell.

### Decision

AkiDB v2.0 will be Mac-first.

The primary product targets are:

1. One Apple Silicon Mac as a production-capable local vector DB appliance.
2. Four Apple Silicon Macs as a Thunderbolt-connected cell.
3. Multiple cells for horizontal growth.

AkiDB supports macOS Apple Silicon only as an active target.

### Consequences

Positive:

- Better developer and buyer accessibility.
- Strong privacy and local-first story.
- Lower operational complexity than Kubernetes-first systems.
- Clear benchmark baseline: one Mac before any distributed claims.

Negative:

- Hardware-specific acceleration is not part of the active product path.
- Apple GPU acceleration remains a future research area, not a release dependency.
- Production Linux edge users are out of scope for v2.

---

## ADR-002: One Mac Is the Baseline Deployment

**Status:** Accepted

### Context

Distributed systems add latency, failure modes, and operational complexity. A four-Mac cell is only valuable if it is faster, safer, or larger than one Mac for real workloads.

### Decision

All product and benchmark claims must start with the one-Mac baseline. Distributed mode is an extension, not a prerequisite.

The one-Mac deployment must provide:

- Durable WAL.
- Local persistent storage.
- Snapshot and restore.
- Ingestion and reindexing.
- Stable client API.
- Local-only secure defaults.

### Consequences

Positive:

- Keeps the product useful before distributed mode is complete.
- Provides an honest performance baseline.
- Reduces release risk.

Negative:

- Some distributed code paths will arrive later.
- Marketing must avoid overclaiming cluster value before benchmarks prove it.

---

## ADR-003: Four-Mac Thunderbolt Cell as the First Distributed Unit

**Status:** Accepted

### Context

The proposed distributed shape is four Macs connected over Thunderbolt. Thunderbolt 5 offers high bandwidth, but it is still a network/interconnect path with topology constraints. It should not be treated as a general-purpose data-center fabric.

### Decision

AkiDB will define a "cell" as exactly four data nodes for v2.0.

A cell is:

- One capacity domain.
- One failure domain.
- One benchmark unit.
- One local operational unit.

The product will not support arbitrary N-node meshes in v2.0.

### Consequences

Positive:

- The topology is small enough to test rigorously.
- Capacity planning is understandable.
- Failure behavior can be documented precisely.

Negative:

- Users cannot add a fifth Mac directly into a v2.0 hot cell.
- Larger deployments require multiple cells and routing.

---

## ADR-004: Scale by Adding Cells, Not by Growing a Mesh

**Status:** Accepted

### Context

Adding Macs indefinitely to one cluster would create unclear topology, uneven link behavior, hard-to-debug tail latency, and failure domains that are too large for the product goal.

### Decision

Horizontal scaling beyond four Macs happens by adding another cell.

The coordinator routes to cells by:

- Collection.
- Tenant.
- Shard group.
- Explicit cross-cell search request.

Cross-cell fan-out is allowed only when requested or configured. It is not the default query path.

### Consequences

Positive:

- Keeps search fan-out bounded.
- Makes failure domains explicit.
- Allows each cell to use homogeneous hardware.

Negative:

- Some workloads need collection or tenant placement planning.
- Global topK across many cells requires additional merge latency.

---

## ADR-005: Homogeneous Hot Cells, Weighted Heterogeneous Support

**Status:** Accepted

### Context

A search query that fans out across shards is often limited by the slowest responding shard. Mixed Mac SKUs can be useful, but treating them equally would damage tail latency and make capacity planning misleading.

### Decision

Production hot cells should use matching reference SKUs.

AkiDB may support heterogeneous nodes only through explicit weighted placement:

- More capable Macs receive more or hotter shards.
- Smaller Macs receive fewer, colder, or background workloads.
- Replica groups for hot shards prefer same-class nodes.
- Node admission rejects placements that exceed memory or disk headroom.

### Consequences

Positive:

- Predictable P95/P99 latency.
- Cleaner benchmarks.
- Safer failover behavior.

Negative:

- Operators with mixed hardware need a more explicit placement plan.
- The first release cannot promise ideal utilization of every spare Mac.

---

## ADR-006: Replicated Shards for Safety in a Four-Mac Cell

**Status:** Accepted

### Context

No-replication sharding increases capacity but does not make the system safer. If any shard exists only on one Mac, losing that Mac loses searchable coverage until restore.

### Decision

The four-Mac production cell must support replica placement. The recommended production policy is replication factor 2 for hot collections.

Rules:

- Primary and replica for a shard must be on different Macs.
- Search can use a replica when the primary is unavailable or overloaded.
- Write acknowledgement requires the configured durability policy.
- The system must report degraded status if required replicas are unavailable.

### Consequences

Positive:

- A four-Mac cell can be safer than one Mac.
- Failover behavior becomes testable.
- Product claims can distinguish capacity-only sharding from HA sharding.

Negative:

- RF=2 halves effective resident capacity for replicated collections.
- Write paths need replica durability and publication logic.

---

## ADR-007: Three-Voter Metadata Consensus in a Four-Node Cell

**Status:** Accepted

### Context

Even-sized consensus groups are awkward because they do not improve failure tolerance compared with the next lower odd number, and they can make quorum behavior harder to reason about.

### Decision

If AkiDB uses an internal consensus group for cell metadata, a four-Mac cell must use three voting members and one learner or data-only member.

Consensus covers:

- Cell membership.
- Shard placement.
- Replica placement.
- Index publication epochs.
- Recovery state.

Vector payload replication and query execution are separate data-plane concerns.

### Consequences

Positive:

- Avoids four-voter quorum anti-patterns.
- One voter can fail while metadata operations continue.
- The fourth Mac still contributes data capacity.

Negative:

- Voter placement must be explicit.
- If two voters fail, metadata changes stop until recovery.

---

## ADR-008: Thunderbolt Is a Network Transport, Not Shared Memory

**Status:** Accepted

### Context

Thunderbolt has high bandwidth, but AkiDB cannot assume remote memory access, cache coherence, or uniform link behavior. The software must behave as a distributed system over a fast transport.

### Decision

Within a cell, Thunderbolt is treated as a measured network transport.

The system must:

- Measure link throughput and latency.
- Detect link failures.
- Avoid assuming symmetric performance.
- Keep shard ownership and replica state explicit.
- Continue to work over a slower network for development, with lower SLO expectations.

### Consequences

Positive:

- Prevents invalid architecture assumptions.
- Allows test environments without Thunderbolt.
- Makes SLOs conditional on validated topology.

Negative:

- Requires more observability work.
- Some theoretical bandwidth may not translate into query throughput.

---

## ADR-009: Portable CPU Backend First

**Status:** Accepted

### Context

Specialized accelerator APIs should not block the Mac-only product. The first Mac path needs correctness, durability, and benchmarkability before specialized acceleration.

### Decision

AkiDB v2.0 starts with a portable CPU vector backend behind a stable trait. Backend options can include exact search, HNSW, FAISS CPU, or other portable implementations.

The backend abstraction must preserve:

- Add.
- Search.
- Delete/tombstone.
- Snapshot.
- Restore.
- Backend-specific diagnostics.

GPU, Metal, FAISS GPU, CUDA, and cuVS are not part of the supported v2 active path.

### Consequences

Positive:

- Avoids blocking on GPU integration.
- Keeps one-Mac path simple.
- Allows backend benchmarking without API churn.

Negative:

- Initial peak throughput may be lower than GPU systems.
- Metal acceleration requires a future ADR.

---

## ADR-010: Stable API Across One Mac, Cell, and Optional Accelerators

**Status:** Accepted

### Context

The product should let users start on one Mac and move to a cell without changing client code.

### Decision

The client API must remain topology-neutral for normal operations. Administrative APIs expose topology and placement, but search and ingestion APIs should work across one Mac and cell deployments.

### Consequences

Positive:

- Smooth migration from local to cell.
- Easier SDK support.
- Cleaner tests.

Negative:

- Some topology-specific optimizations need admin or debug APIs rather than client API changes.

---

## ADR-011: Apache-2.0 Project License

**Status:** Accepted

### Context

The project is moving from MIT to Apache-2.0. Apache-2.0 provides an explicit patent grant and is widely accepted for infrastructure software.

### Decision

AkiDB source code and documentation are licensed under Apache License 2.0 unless a file states otherwise.

Cargo metadata, Python package metadata, README, and the repository LICENSE file must use Apache-2.0.

### Consequences

Positive:

- Clear patent grant.
- Familiar license for infrastructure users.
- Aligns Rust and Python service metadata.

Negative:

- Slightly more compliance overhead than MIT.
- Existing downstream users of older MIT snapshots may need guidance if the project has already been distributed externally.

---

## Superseded Documents

This ADR set supersedes the prior hardware-specific versioned ADR series. Subsystem-specific ADRs may remain valid only where they do not conflict with this document.
