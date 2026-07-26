# Ubuntu AMD64 Knowledge-Cell Qualification

**Qualification date:** 2026-07-25

**Status:** Passed for the bounded profile in this document

**Support floor:** Ubuntu 24.04 or newer on AMD64

**Measured host OS:** Ubuntu 26.04 on `x86_64`

## Decision

The PostgreSQL-led AkiDB knowledge cell is qualified for a single logical
knowledge shard replicated in full to three AkiDB data-plane VMs. Two
stateless AX Fabric gateways route reads only to replicas that prove the
active generation, digest, sequence, readiness, and heartbeat.

The qualified retrieval envelope is 100,000 vectors at 768 dimensions, F32,
on an 8-vCPU/32-GB VM. The authoritative generation/failover exercise used a
smaller deterministic 1,000-vector/1,000-edge corpus so that generation
publication, exact convergence, graph quality, replacement, backup, and
failure drills could be repeated many times.

This decision does not qualify:

- PostgreSQL or MinIO as highly available in the four-VM lab;
- one million vectors, five million vectors, or 1,536 dimensions;
- Linux ARM64, NVIDIA Thor, CUDA, or another Linux distribution;
- AkiDB sharding, automatic resharding, or a stateful Kubernetes profile;
- `graph_hybrid` without a configured embedding provider.

## Product and data ownership

AkiDB remains a rebuildable retrieval projection. MinIO/OpenWiki hold
canonical knowledge, while HA PostgreSQL is the authority for generations,
ordered mutations, activation, audit, and replica checkpoints. Each AkiDB
replica owns independent RocksDB, HNSW, BM25, and bounded-graph state on its
local data volume. NATS is not an authority and was not required by the
measured control-plane load.

## Topology and host profile

| Role | Count | Placement |
| --- | ---: | --- |
| Full AkiDB retrieval replica | 3 | VMs 1–3, independent failure domains |
| Stateless AX retrieval gateway | 2 | VMs 1–2 |
| Lab PostgreSQL and MinIO | 1 each | VM 4 |
| WireGuard service network | 4 peers | `10.77.0.0/24` |

Every supplied VM reported:

| Property | Measured value |
| --- | --- |
| OS/architecture | Ubuntu 26.04, `x86_64` |
| CPU | 8 vCPU |
| RAM | 32,569,462,784–32,569,470,976 bytes |
| Root volume | 514,840,973,312-byte virtual disk, ext4 |
| Service exposure | AkiDB, gateway, PostgreSQL, MinIO, and metrics restricted to WireGuard |

The hypervisor exposed `/dev/vda2`; the underlying media was not independently
verified as NVMe. The result therefore qualifies the measured virtual-disk
profile, not a physical-NVMe claim. A 320-GB usable data disk remains
acceptable for the qualified envelope because admission reserves 25 GiB and
requires active-plus-shadow headroom.

## Immutable artifacts and active generation

| Artifact | Release ID | SHA-256 |
| --- | --- | --- |
| AkiDB Linux AMD64 qualification build | `6ef800ac5f3680fa41698e93d9bb3ae979a319b2` | `1d07f6cc1b72c445d7a5d3bf77076b1303f7ae9c57a6df4df45ab6b1c11b03a7` |
| AX knowledge gateway Linux AMD64 build | `214ad4d41d0f60aa4a16d93316806cd0fcc343fc` | `b96e18e39e408ab355accee02389197d35b5134f0ddd711f63776bd811f5d75a` |
| Backup archive | `qualification-20260725-01` | `17f74f7e7abfc6318a931ce141ad1e9d82f7c13d595f04230bf9dfb06d863b0c` |

The two release IDs are the exact source commit SHAs. The final artifacts were
produced from those committed trees, checksum-verified before deployment,
rolled across the cell, and then reconciled by a second complete Ansible site
run with zero changes and zero failures. The final AkiDB archive also passed
GitHub build-provenance attestation verification before deployment.

The final AkiDB build upgrades tonic, prost, and rustls-webpki beyond the
RustSec-affected dependency set and explicitly selects the portable ring
CryptoProvider before creating TLS clients or listeners. The first upgraded
candidate exposed the otherwise ambiguous ring/AWS-LC feature combination:
the rolling readiness gate kept the replica drained, stopped the rollout, and
the installed prior release restored all three replicas before the corrected
commit-derived artifact was rebuilt and deployed. The corrected production
feature set, workspace tests, startup smoke, rolling deployment, golden
queries, and final convergence checks all passed.

The first CI packaging attempt exposed a Clang 18 compiler crash while
generating numkong dynamic-dispatch code. The artifact script now defaults to
the Ubuntu LTS GCC/G++ toolchain, while retaining explicit `CC`/`CXX`
overrides for separate compiler qualification. The replacement workflow built
the complete immutable archive, executed each Linux binary on Ubuntu 24.04,
verified its checksum and manifest, attached provenance, and uploaded it. That
CI-produced archive is the release rolled across all three replicas.

The active authority state after all drills was:

| Field | Value |
| --- | --- |
| Generation | `qualification-generation-20260725b` |
| Manifest SHA-256 | `3235c9365f7313d380fc0fec7d0b332316a9f7dfb587cc6644d991ccfbaf69ca` |
| Materialization digest | `ed934f8cedd1e7c7d3905661b0611efc776238f45a668ecdc83843e2089ba208` |
| Applied mutation sequence | 0 |
| Materialized vectors / edges | 1,000 / 1,000 |
| Activation policy | At least 2 ready replicas in 2 failure domains |

All three replica checkpoints reported the same generation, sequence, digest,
vector count, and edge count. All were undrained, process-ready, and serving
when final verification ran.

## Retrieval performance

The standalone performance corpus uses deterministic F32 vectors, dimension
768, HNSW, cosine distance, top-10, `nprobe=32`, 1,000 queries, and concurrency
1. Mandatory workspace filtering is enabled. The initial result exposed an
implementation defect: any filter requested the entire HNSW corpus as
candidates. Bounding the candidate request while retaining fail-closed
post-filtering removed the accidental linear scan.

| Result | Before fix | After fix |
| --- | ---: | ---: |
| Corpus at search | 100,100 vectors | 100,201 vectors |
| P50 | 861.344 ms | 5.594 ms |
| P95 | 900.185 ms | 6.476 ms |
| P99 | 923.736 ms | 6.848 ms |
| Average | 863.382 ms | 5.656 ms |
| Throughput | 1.16 QPS | 176.34 QPS |
| Queries under 50 ms | 0% | 100% |

The 101-vector difference came from the benchmark's insert phase and is 0.101%
of the corpus. The final after row used the new query-only mode and did not
mutate its 100,201-vector corpus. Its retained JSON evidence is
[`linux-amd64-100k-query-only.json`](evidence/linux-amd64-100k-query-only.json)
(SHA-256
`fbb46d53227898ba1d030c944327f5f62e8850bc9c684ed1c45fefcc580ac328`).

Persisted index size after the final run was 658,971,882 bytes. Rebuilding
100,201 persisted vectors into the in-memory HNSW index after process restart
took 457.9 seconds and reached a systemd-reported peak of 1,656,414,208 bytes.
Startup rebuild time is therefore a current operational constraint even though
steady query latency passes the 50-ms reference SLO.

## Graph and evidence quality

A live 100-query set used exact resolution chunks as lexical seeds. Each case
expected all four chunks from the same canonical document; the other three
chunks require graph expansion. Both modes used top-10, a two-hop maximum,
fanout 8, maximum 64 expanded nodes, a 4,096-token context budget, and
generation-stable citations.

| Mode | Evidence recall | Related-evidence recall | Citation errors | Budget violations | Request failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| BM25 | 100/400 (25%) | 0/300 (0%) | 0 | 0 | 0 |
| Graph | 400/400 (100%) | 300/300 (100%) | 0 | 0 | 0 |

The final golden graph request returned HTTP 200 through both gateways. Each
used one replica attempt, returned 10 hits, expanded 48 nodes within the
configured limit, packed 10 cited items, and returned canonical `s3://` source
identity, source version, content hash, chunk ID, document ID, offsets,
generation ID, and per-edge evidence paths. The two measured route latencies
were 35 ms and 30 ms.

The lab intentionally disabled an embedding endpoint. Explicit `hybrid` and
`graph_hybrid` requests therefore failed closed with a provider-not-configured
error, while BM25-seeded `graph` retrieval was qualified live. Vector/hybrid
and graph-hybrid contracts remain covered by Rust and TypeScript tests; an
embedding-backed live qualification is required before making a model-specific
quality claim.

## Availability and recovery drills

| Drill | Result |
| --- | --- |
| One replica stopped | Gateway automatically routed to another replica; first observed response 48–54 ms, subsequent responses 14–15 ms |
| PostgreSQL stopped | Existing reads remained HTTP 200 with `controlStale=true`; publication failed closed |
| MinIO stopped | Existing graph reads remained HTTP 200; publication failed and authority state did not change |
| Blank-node replacement | Replica 3 was drained, its generation volume removed, rebuilt from authority/object storage, digest-checked, and re-admitted |
| Rolling upgrade | Passed one replica at a time with quorum gates |
| Compatible rollback | Passed from candidate E to D with re-admission gates |
| Incompatible rollback | Correctly rejected because the old build lacked required restart-persistent disk metrics |
| Backup/restore | 478,089-byte archive restored into an isolated PostgreSQL database; control tables and MinIO objects validated |
| Full site replay | Final Ansible run: `changed=0`, `failed=0` on four hosts and localhost |

The current generation continued serving during PostgreSQL and MinIO outages
because replicas own local materialized state. Publication and recovery still
depend on those canonical/control systems.

## Security and operations gates

The qualification verified:

- TLS for AkiDB gRPC, AX gateway HTTPS, PostgreSQL, and MinIO;
- bearer credentials split by read, generation control, and gateway boundary;
- enforced workspace metadata and tenant-scoped graph filtering at every hop;
- UFW rules and private AkiDB binds restricted to the WireGuard overlay;
- least-privilege MinIO read and publisher identities;
- checksum-pinned immutable releases and protected systemd environments;
- provenance-bearing citations and edge evidence;
- capacity, checkpoint, disk, route, failure, and graph metrics;
- generation GC with audit/dry-run controls, drain/replacement workflows,
  backup/restore, rolling upgrade, and rollback runbooks.

No public IP or credential is part of this report or committed inventory.

## Capacity envelope

The conservative planner reserves 30% of host RAM and 25 GiB of disk, includes
HNSW links and record overhead, applies a 1.65× build-RAM multiplier, and
requires active-plus-shadow disk space.

| Corpus | Planner peak RAM | Active + shadow disk | Decision |
| --- | ---: | ---: | --- |
| 100k × 768 F32 | 0.610 GiB | 1.626 GiB | Admitted and measured |
| 1M × 768 F32 | 6.098 GiB | 16.260 GiB | Admitted by planning only; not qualified |
| 5M × 1,536 F32 | 54.091 GiB | 144.243 GiB | Rejected on 32-GB host RAM |

Only the 100k × 768 row is a support claim. A larger corpus requires its own
load, restart, failure, quality, and latency qualification.

## Phase 6/7 decision

A 30.055-second control-plane sample processed 1,203 transactions (about
40 TPS), with 1,203 commits, zero rollbacks, 7,033 cache hits, zero block
reads, three idle connections, no sustained active connection, and about 1.9%
PostgreSQL CPU. No PostgreSQL notification-load trigger was measured, so NATS
is deferred. PostgreSQL remains authoritative and replicas continue polling
idempotently.

No measured capacity or business trigger currently justifies logical
sharding, online resharding, Kafka/Redpanda, or stateful Kubernetes. Phase 7 is
therefore closed by its designed no-trigger outcome: these paths remain
deferred and require a new evidence-led plan.

## Final limitations

The qualified cell provides retrieval read availability across one AkiDB
replica failure. It is not full-system HA while PostgreSQL and MinIO each run
on one lab host. Production must supply HA PostgreSQL and durable,
failure-domain-aware canonical object storage, external secret management,
certificate lifecycle management, alert delivery, and scheduled restore
drills.

This report does **not** claim market-aligned ANN parity, Milvus/Weaviate
competitor comparison, public SIFT1M/GIST1M release matrices, G2/G3 graph
market tiers, or multi-hour soak at market scale. Those remain an active
release gate under
[market-readiness-qualification.md](market-readiness-qualification.md).

The design deliberately favors full replicas over early sharding: it is
simple, independently rebuildable, and gives deterministic failover, but
multiplies index storage and rebuild work by the replica count. Scale out only
after a measured single-replica limit.
