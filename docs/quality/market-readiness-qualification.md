# Market-Aligned Product Readiness Qualification

Status: active release gate (automation ready; full evidence verdict not complete)

Primary targets: Mac Studio / AMD64 PC (single user); Mac Studio cluster /
AMD64 cloud cluster (enterprise)

Also supported: Mac Mini / MacBook standalone

Outside the release matrix: macOS Intel, Linux ARM64/NVIDIA Thor, Ubuntu older
than 24.04, CUDA/GPU index acceleration, and unqualified Linux distributions

## Current status (2026-07-26)

| Lane | Automation | Evidence status |
| --- | --- | --- |
| A — AkiDB SIFT1M matrix | `knowledge-market-ann.yml` + `akidb-ann-bench` + `summarize_market_ann.py` | Ready to run on the isolated lab; not a checked-in release pass |
| A — mutable crash recovery | `knowledge-market-recovery.yml` + `akidb-recovery-probe` + `summarize_market_recovery.py` | Automated fsynced-ack/SIGKILL/restart gate; not yet a checked-in release pass |
| A — Milvus / Weaviate parity | `knowledge-market-competitors.yml` + `competitor_ann_bench.py` + `summarize_market_parity.py` | Ready to run on pinned images; not a checked-in release pass |
| B — native graph matrix | `knowledge-market-graph.yml` + `akidb-graph-bench` + `summarize_market_graph.py` | Ready to run; G1–G3 not a checked-in market pass |
| C — knowledge-serving cell | knowledge-site / load / failure playbooks | Bounded Ubuntu AMD64 cell already qualified separately at 100k × 768; full market soak and scale matrix still open |

Related but separate from this market gate:

- The PostgreSQL-led three-replica knowledge cell passed a bounded Ubuntu AMD64
  qualification (100,000 vectors × 768 dimensions, plus smaller generation and
  failover drills). See
  [linux-amd64-knowledge-cell-qualification.md](linux-amd64-knowledge-cell-qualification.md).
- That cell pass is necessary serving-system evidence, not a substitute for
  public-dataset ANN parity or competitor comparison.
- CI syntax-checks the market Ansible playbooks and tests their local
  summarizers and probe state machines. It does not execute live SIFT1M,
  SIGKILL, competitor, or soak workloads.

A market release verdict remains `not-ready` until one immutable candidate
produces a single evidence manifest that passes every required gate in this
document. Missing, stale, mismatched, or failed reports never count as pass.

## Decision

AkiDB is ready for a market-aligned release claim only when one immutable
release candidate passes three independent evidence lanes:

1. **Vector retrieval:** exact-ground-truth ANN quality and performance using
   public datasets and market-standard metrics.
2. **Bounded graph retrieval:** exact known-answer graph operations and
   GraphRAG evidence quality for the graph contract AkiDB actually exposes.
3. **Serving system:** security, generation consistency, sustained load,
   restart, backup/restore, and failure-under-load behavior for a complete
   knowledge-serving cell.

Passing a synthetic throughput test alone is not a release verdict. A report
must identify the source dataset, immutable AkiDB artifact, configuration,
hardware, driver location, warm-up, concurrency, query count, and all gate
values.

## What "market comparable" means

The vector lane follows the methodology common to
[VectorDBBench](https://github.com/zilliztech/VectorDBBench),
[ANN-Benchmarks](https://github.com/erikbern/ann-benchmarks), the
[Weaviate ANN benchmark](https://docs.weaviate.io/weaviate/benchmarks/ann),
and the [Milvus index guidance](https://milvus.io/docs/index-explained.md):

- use public vectors with exact neighbor ground truth;
- report Recall@K together with throughput and latency;
- include import/build time and final storage size;
- test multiple concurrency and index-search breadth settings;
- include filtered workloads and insertion while searching;
- use a separate client and server with the same network placement; and
- compare products on identical hardware, data, distance metric, top-k, and
  recall band.

Vendor-published numbers are context, not parity evidence. AkiDB, Milvus, and
Weaviate must be rerun by us on the same isolated qualification hosts.

The pinned comparison set was reviewed on 2026-07-26:

- Milvus server `v2.6.21` with `pymilvus==2.6.17`;
- Weaviate server `1.38.6` with `weaviate-client==4.22.0`; and
- VectorDBBench `v1.0.22` methodology at commit
  `191b7106a08a3e6f9f9ffe9bf5604d8f5daa8270`.

The playbook pins the reviewed multi-architecture image digests and the release
evidence independently records resolved `RepoDigest` values; tags alone are not
immutable evidence. `scripts/competitor_ann_bench.py` uses the public SIFT files
directly so all three products receive exactly the same 1,000,000 vectors and
10,000 queries. VectorDBBench remains a methodology reference because its
built-in SIFT capacity does not match this exact SIFT1M release matrix.

The graph lane is intentionally narrower. AkiDB provides a bounded retrieval
graph, not a general Cypher/GQL database. It applies known-answer, persistence,
concurrency, mutation-integrity, and percentile-latency principles from the
[LDBC benchmark suite](https://ldbcouncil.org/benchmarks/) to AkiDB's shipped
API. Passing this lane must never be described as LDBC, SNB, Graphalytics,
Cypher, or general graph-database compliance.

## Reference AMD64 topology

The repeatable qualification topology is:

```text
node 1: AkiDB replica 1 + knowledge gateway 1
node 2: AkiDB replica 2 + knowledge gateway 2
node 3: AkiDB replica 3
node 4: load driver + qualification dependencies
```

All nodes must run Ubuntu 24.04 or later on AMD64. Record the exact image,
kernel, CPU model, vCPU count, RAM, disk model, filesystem, mount options, and
network path in the evidence manifest.

For the current 1M and 10M qualification tiers, each data node needs at least:

- 8 vCPU;
- 32 GiB RAM;
- 320 GB local NVMe; and
- 1 Gbit/s private networking.

The preferred capacity configuration remains 16 vCPU, 64 GiB RAM, and 500 GB
local NVMe per data node. Extra CPU is not required for correctness; it reduces
index-build time, raises concurrent-query capacity, and leaves headroom for
compaction, checkpointing, and rebuild traffic. A 100M-vector qualification
must use a separately sized disk budget derived from measured bytes per vector.

For fair single-engine comparisons, use one isolated server host and one
isolated client host. Do not run PostgreSQL, MinIO, gateways, or a competing
database on the benchmark server during that run. Run each product separately,
recreate the data volume between products, and retain resource telemetry.

The three AkiDB data nodes in the knowledge-serving cell are full replicas of
one logical shard. This topology validates read availability; it must not be
represented as a horizontally sharded cluster.

## Lane A: vector retrieval

### Public datasets

The minimum release matrix is:

| Tier | Dataset | Purpose |
| --- | --- | --- |
| A1 | SIFT1M, 128 dimensions, L2 | Public exact-ground-truth compatibility |
| A2 | GIST1M, 960 dimensions, L2 | High-dimensional public ANN behavior |
| A3 | Public 768-dimensional embedding corpus | Current private-RAG shape |
| A4 | Public 1536-dimensional embedding corpus | Larger embedding shape |
| A5 | 10M vectors at a supported dimension | Capacity and build/restart behavior |

Every converted file is SHA-256 identified. The preparation utility
`scripts/convert_ann_benchmarks_hdf5.py` converts ANN-Benchmarks HDF5 data into
streaming `fvecs`/`ivecs`; the release benchmark binary does not depend on
Python, HDF5, or NumPy.

### Query matrix

For every dataset, run at least:

- top-k: 10 and 100;
- search breadth: 32, 64, 128, and 256 where supported;
- concurrency: 1, 8, 32, and 64;
- 1,000 warm-up queries; and
- a 60-second post-import quiescence window; and
- three complete 10,000-query measurement rounds per point.

Use `akidb-ann-bench` to record:

- exact Recall@K;
- successful and failed requests;
- QPS;
- mean, P50, P95, P99, and maximum latency;
- import duration and vectors/second;
- before/after health counts; and
- dataset path, byte count, and SHA-256.

The server must start from an empty, isolated data directory for a load run.
Any pre-existing active vector causes the harness to fail closed.

### Filter and mixed-workload matrix

Test exact metadata filters at approximately 1%, 5%, and 50% selectivity.
The SIFT harness labels each train row deterministically and chooses each
query's filter label from its exact nearest neighbor. Filtering the ordered
official neighbor list then gives exact filtered ground truth without treating
unfiltered recall as filtered recall. Every returned row is independently
checked against the requested label.

The current post-filter implementation doubles its ANN candidate window only
when the filtered result is incomplete and caps the largest window at 16,384.
Geometric retries keep cumulative predicate work below three times that
maximum window. The cap prevents an impossible or hostile predicate from silently
turning every large-index query into an unbounded full-index scan; exhausting
it returns fewer results and fails this qualification's result-count gate
instead of hiding the limitation.

Also run:

- search while inserting at 10% and 50% of measured ingest capacity;
- update and delete while searching;
- process restart after index build;
- WAL recovery after an unclean stop; and
- snapshot restore followed by the full query set.

The Ansible ANN gate derives the two paced rates from the candidate's measured
SIFT1M import throughput. Each scheduled cycle performs an insert, an update,
and an immediate delete of a deliberately distant transient vector while eight
workers continue exact-ground-truth searches. Each 10% and 50% phase lasts five
minutes, must complete at least 90% of its requested cycle rate, must return to
the original active-vector count, and must preserve Recall@10, result integrity,
and P99 gates. The cycle rate is based on insert capacity; update and delete
traffic are additional load.

### Absolute vector gates

The chosen production configuration must meet all of these:

- insert failures: 0;
- measured query failures: 0;
- Recall@10: at least 0.95;
- Recall@100: at least 0.95;
- returned filter violations: 0;
- missing or duplicate IDs after restart: 0;
- dataset and health counts exactly reconcile; and
- all recovery runs reproduce the same accuracy result.

Latency and QPS are configuration-specific and must be published as a Pareto
curve, never as a single best number obtained at a lower recall setting.

### Competitor parity gate

Run current stable Milvus and Weaviate releases on the same isolated server
shape. Record exact image/version digests and tune only documented production
settings. At a common Recall@10 of at least 0.95:

- AkiDB QPS must be at least 70% of the median competitor QPS;
- AkiDB P99 must be no more than 150% of median competitor P99;
- AkiDB build time must be no more than 200% of the median; and
- AkiDB final on-disk bytes must be no more than 200% of the median.

These relative limits define "similar" for the first AMD64 release. A failed
relative performance gate is a release blocker unless the release explicitly
narrows its published scale or workload claim.

## Lane B: bounded graph and GraphRAG

### Native graph kernel

`akidb-graph-bench` materializes a deterministic persistent topology:

```text
Document --contains--> Chunk --mentions--> Entity
```

It then closes and reopens RocksDB before running a concurrent mix of:

- filtered one-hop neighbors;
- two-hop paths;
- bounded path existence;
- related chunks;
- negative paths;
- cross-workspace atomic rejection;
- excessive-depth rejection; and
- node deletion with incident-edge cleanup.

Run at these minimum sizes:

| Tier | Documents | Chunks/document | Entities | Approximate nodes |
| --- | ---: | ---: | ---: | ---: |
| G1 | 10,000 | 4 | 1,000 | 51,000 |
| G2 | 100,000 | 4 | 10,000 | 510,000 |
| G3 | 1,000,000 | 4 | 100,000 | 5,100,000 |

Run 10,000 known-answer queries for G1 and at least 100,000 for G2/G3 at
concurrency 1, 8, 32, and 64.

Native graph release gates are:

- known-answer accuracy: 1.0;
- query and integrity errors: 0;
- persistent node and edge counts: exact;
- cross-workspace mutation acceptance: 0;
- graph depth above the product limit accepted: 0;
- orphan incident edges after deletion: 0; and
- local-NVMe P99 at G2/concurrency 8: no more than 50 ms.

### GraphRAG quality

Use at least 100 human-reviewed or deterministically generated enterprise-style
questions covering email threads, attachments, tickets, products, invoices,
contracts, versions, and evidence citations. Compare:

1. vector only;
2. vector + BM25 + metadata;
3. hybrid + one-hop graph;
4. hybrid + two-hop graph; and
5. hybrid + graph + rerank.

Report Evidence Recall@K, multi-hop answer accuracy, relationship precision and
recall, entity-resolution accuracy, citation correctness, graph-expansion token
cost, and hallucination rate.

The GraphRAG release configuration must have:

- expected evidence recall: 1.0 for deterministic known-answer cases;
- expected document recall: 1.0 for deterministic known-answer cases;
- citation correctness: 1.0;
- forbidden evidence returned: 0;
- cross-workspace or ACL leakage: 0;
- stale generation routes: 0; and
- no regression versus hybrid-only retrieval on single-hop questions.

The graph feature passes only if it materially improves the multi-hop set over
hybrid-only retrieval. The result must state the absolute delta and confidence
interval, not only that the graph path is faster.

## Lane C: complete serving system

### Correctness and security

The gateway harness validates both HTTPS gateways, exact generation/manifest
evidence, minimum mutation sequence, bounded graph options, typed/legacy
context agreement, citations, and forbidden evidence. Security probes require:

- health is intentionally available without credentials;
- ready and search reject missing or incorrect credentials;
- unsupported content type is rejected;
- workspace override is rejected;
- graph depth above the bound is rejected;
- a query over 16 KiB is rejected before routing; and
- a request body over 1 MiB is rejected.

All expected statuses must match on both gateways.

### Load levels

Run the following sequence without rebuilding data:

| Stage | Duration | Traffic |
| --- | --- | --- |
| correctness | at least 2 complete fixture passes | closed loop |
| baseline | 10 minutes | 25 QPS |
| step | 5 minutes each | 25, 50, 100, 200, then saturation |
| steady | 2 hours | 70% of sustainable QPS |
| soak | 24 hours | 50% of sustainable QPS |

The harness reports service latency and schedule-to-completion latency so
client backlog cannot hide coordinated omission. Sustainable QPS is the
highest step with zero contract failures, error rate no more than 0.01%, and
P99 within the published service objective.

### Failure matrix

Inject one bounded fault at a time while paced traffic is already running:

- stop each AkiDB replica process in turn;
- SIGKILL one replica during a write and verify WAL recovery;
- reboot one replica host;
- stop each gateway in turn behind the client/load-balancer path;
- interrupt one replica's PostgreSQL control-plane connection;
- make one replica's MinIO source temporarily unavailable;
- fill a dedicated test volume to its configured watermark;
- replace one replica from a blank volume;
- restore from a backup to a separate path; and
- roll forward and roll back one immutable artifact.

Every action must identify one exact qualification target, have a bounded hold
time, and use an `always` recovery block. The single-replica process fault gate
requires zero failed reads, at least two serving replicas in the measured
report, at least one request-level retry, exact generation readiness after
recovery, and no stale route.

### Serving-system gates

- correctness/security run: 0 failed requests and 0 failed probes;
- two-hour steady run: error rate at most 0.01%, 0 contract failures;
- 24-hour soak: 0 data/citation/tenant correctness errors;
- single-replica fault: 0 failed reads and at least one observed retry;
- single-gateway fault: 0 failed reads through the published client path;
- blank rebuild: exact manifest, generation, sequence, vector count, and graph
  count;
- backup/restore: full known-answer suite passes from the restored path; and
- cross-tenant leakage: exactly 0 in every phase.

The mutable crash gate is stronger than a health-only restart check.
`akidb-recovery-probe` fsyncs a client-side journal after each successful
insert, update, or delete response. The server is then SIGKILLed while those
operations are active. After systemd automatic restart, every allocated ID is
checked against its last acknowledged state. A state may advance by only the
single operation whose response was in flight; an acknowledged state may
never disappear or regress. Probe IDs are deleted, the active count must
return to exactly 1,000,000, and full SIFT1M Recall@10 is rerun before the
crash, after crash recovery, and after a graceful restart.

The lab's PostgreSQL and MinIO may remain single-node only for AkiDB process
qualification. A production-HA claim additionally requires managed or
independently qualified HA PostgreSQL and object storage.

## Phased execution plan

### Phase 0 — Reproducible candidate

- CI passes on macOS ARM64 and Ubuntu 24.04 AMD64.
- Release tag equals the Cargo workspace version.
- Linux artifact uses the `generation-postgres` feature.
- Required binaries are present; packaging may not ignore missing binaries.
- Artifact SHA-256 and build provenance attestation are retained.

Exit: one immutable candidate is selected; no source rebuild is allowed during
the remaining phases.

### Phase 1 — Deterministic correctness

- Run unit/integration/doc/Python tests.
- Run native graph G1 and gateway known-answer/security gates.
- Verify idempotent ingest, delete propagation, restart, and generation
  consistency.

Exit: all correctness gates pass with zero leakage and zero contract failures.

### Phase 2 — Market vector qualification

- Run SIFT1M, GIST1M, 768d, and 1536d matrices.
- Run filter and mixed insert/search matrices.
- Repeat matched Milvus and Weaviate runs.
- Publish recall/throughput/latency Pareto tables and resource telemetry.

Exit: absolute accuracy and competitor-parity gates pass.

### Phase 3 — Graph and GraphRAG qualification

- Run G1/G2/G3 native graph matrices.
- Run the multi-hop GraphRAG evaluation against all retrieval baselines.
- Verify provenance, ACL enforcement at each hop, and deletion propagation.

Exit: graph accuracy gates pass and multi-hop quality shows a material,
statistically reported improvement.

### Phase 4 — Load, recovery, and HA

- Determine saturation and sustainable QPS.
- Run two-hour steady and 24-hour soak.
- Complete every failure, blank-rebuild, rolling-upgrade, rollback, and
  backup/restore drill.

Exit: availability and recovery gates pass for all three replicas and both
gateways.

### Phase 5 — Release decision

- Generate a single evidence manifest containing artifact, configuration,
  dataset, inventory, and report SHA-256 values.
- Review every required gate; no missing report is treated as a pass.
- Publish supported OS/architecture, tested capacity, and explicit exclusions.
- Create the release only from the already-qualified artifact.

Exit: the release verdict is `pass`. Any missing, stale, mismatched, or failed
evidence keeps the candidate in `not-ready` state.

## Automation inventory

| Tool | Role |
| --- | --- |
| `scripts/convert_ann_benchmarks_hdf5.py` | Convert official ANN-Benchmarks HDF5 to streaming `fvecs`/`ivecs` with SHA-256 manifests |
| `akidb-ann-bench` | Exact-ground-truth AkiDB ANN driver (in the Linux AMD64 release archive) |
| `scripts/summarize_market_ann.py` | Fail-closed summary for a complete AkiDB market ANN evidence set |
| `akidb-recovery-probe` | Fsynced acknowledged-mutation journal and post-crash state verifier |
| `scripts/summarize_market_recovery.py` | Fail-closed SIGKILL, automatic restart, durability, and ANN regression summary |
| `scripts/competitor_ann_bench.py` | Qualification-only Milvus / Weaviate driver using the same SIFT files |
| `scripts/summarize_market_parity.py` | Fail-closed three-engine parity verdict |
| `akidb-graph-bench` | Known-answer native graph matrix driver |
| `scripts/summarize_market_graph.py` | Fail-closed graph-matrix summary |
| `deploy/ansible/playbooks/knowledge-market-ann.yml` | Isolate one replica, run SIFT1M, always restore the cell |
| `deploy/ansible/playbooks/knowledge-market-recovery.yml` | Reuse a passed SIFT1M run, SIGKILL during writes, verify durable recovery, always restore the cell |
| `deploy/ansible/playbooks/knowledge-market-competitors.yml` | Sequential pinned Milvus then Weaviate, then parity summary |
| `deploy/ansible/playbooks/knowledge-market-graph.yml` | Isolate one replica for the G1/G2/G3 graph matrix |

Market playbooks are deliberately separate from production reconciliation
(`knowledge-site.yml`). They require explicit confirmation strings, path
allowlists, and WireGuard-only exposure.

## Reproducible commands

Convert an official ANN-Benchmarks HDF5 dataset:

```bash
python3 scripts/convert_ann_benchmarks_hdf5.py \
  --input /qualification/sift-128-euclidean.hdf5 \
  --output-dir /var/tmp/akidb-market-data/sift1m-fvecs
```

Place the converted files under a `/var/tmp/akidb-market-data/` path so the
Ansible market playbooks accept the dataset directory.

### AkiDB SIFT1M matrix (Lane A absolute gates)

From `deploy/ansible`, after the knowledge cell is healthy and the immutable
candidate artifact variables are exported:

```bash
AKIDB_MARKET_RUN_ID=<unique-run-id> \
AKIDB_MARKET_SERVER=akidb-amd64-3 \
AKIDB_MARKET_DRIVER=akidb-amd64-4 \
AKIDB_MARKET_DATASET_DIR=/var/tmp/akidb-market-data/sift1m-fvecs \
AKIDB_MARKET_OUTPUT_DIR=/qualification/evidence/akidb \
AKIDB_MARKET_CONFIRM=yes-isolate-one-qualification-replica \
ansible-playbook playbooks/knowledge-market-ann.yml
```

The playbook stages the same immutable candidate on the server and driver,
isolates one replica into a dedicated market data directory, runs the full
SIFT1M point matrix through `akidb-ann-bench`, always restores generation
readiness, and writes a fail-closed summary with `summarize_market_ann.py`.

For a single manual point against an already isolated L2 server:

```bash
akidb-ann-bench \
  --server https://10.77.0.13:50061 \
  --dataset-name sift-128-euclidean \
  --train-fvecs /var/tmp/akidb-market-data/sift1m-fvecs/train.fvecs \
  --query-fvecs /var/tmp/akidb-market-data/sift1m-fvecs/test.fvecs \
  --neighbors-ivecs /var/tmp/akidb-market-data/sift1m-fvecs/neighbors.ivecs \
  --metric l2 \
  --collection default \
  --workspace qualification \
  --top-k 10 \
  --nprobe 128 \
  --concurrency 8 \
  --warmup-queries 1000 \
  --measurement-rounds 3 \
  --post-load-settle-seconds 60 \
  --min-recall 0.95 \
  --output-json /qualification/evidence/sift1m-c8-n128.json
```

The mutable standalone shard currently has one physical active collection,
named `default`. Creating a registry entry does not create another physical
index; market qualification therefore uses `--collection default`.

### Mutable SIGKILL recovery (Lane A durability gate)

The source run must already have a passing SIFT1M summary for the same
immutable artifact. From `deploy/ansible`:

```bash
AKIDB_RECOVERY_RUN_ID=<unique-recovery-run-id> \
AKIDB_RECOVERY_SOURCE_MARKET_RUN_ID=<passed-akidb-run-id> \
AKIDB_RECOVERY_SERVER=akidb-amd64-3 \
AKIDB_RECOVERY_DRIVER=akidb-amd64-4 \
AKIDB_RECOVERY_DATASET_DIR=/var/tmp/akidb-market-data/sift1m-fvecs \
AKIDB_RECOVERY_ANN_EVIDENCE_DIR=/qualification/evidence/akidb \
AKIDB_RECOVERY_OUTPUT_DIR=/qualification/evidence/recovery \
AKIDB_RECOVERY_CONFIRM=yes-sigkill-isolated-market-replica \
ansible-playbook playbooks/knowledge-market-recovery.yml
```

The gate requires at least 100 acknowledged inserts, 50 updates, and 25
deletes before SIGKILL; systemd must record an automatic restart with a new
PID and invocation identity. Crash and graceful recovery must each finish
within 900 seconds, every acknowledged state must survive, cleanup must return
to exactly 1,000,000 active vectors, and all three 10,000-query ANN checks must
retain Recall@10 of at least 0.95 with P99 no more than 250 ms.

### Competitor parity (Lane A relative gates)

After the immutable AkiDB SIFT1M matrix passes, run both competitors
sequentially on the same isolated server and driver. Inject
`AKIDB_COMPETITOR_MINIO_ACCESS_KEY` and
`AKIDB_COMPETITOR_MINIO_SECRET_KEY` from the CI secret store or an ephemeral
lab credential helper first; never put either value in the command line or an
inventory file.

```bash
AKIDB_COMPETITOR_RUN_ID=<unique-run-id> \
AKIDB_COMPETITOR_SERVER=akidb-amd64-3 \
AKIDB_COMPETITOR_DRIVER=akidb-amd64-4 \
AKIDB_COMPETITOR_DATASET_DIR=/var/tmp/akidb-market-data/sift1m-fvecs \
AKIDB_COMPETITOR_OUTPUT_DIR=/qualification/evidence/competitors \
AKIDB_COMPETITOR_CONFIRM=yes-run-isolated-market-competitors \
AKIDB_PARITY_AKI_EVIDENCE_DIR=/qualification/evidence/akidb \
AKIDB_PARITY_AKI_RUN_ID=<passed-akidb-run-id> \
ansible-playbook playbooks/knowledge-market-competitors.yml
```

The playbook installs a qualification-only Docker runtime, binds database
ports only to the WireGuard address, runs one database at a time, captures
resolved image digests and resource/storage evidence, removes every container,
and restores exact-generation AkiDB readiness in an unconditional recovery
block. Anonymous database access is accepted only inside this isolated
comparison network and is recorded in the evidence. `summarize_market_parity.py`
then compares AkiDB, Milvus, and Weaviate on the same dataset digests and fails
closed on the relative QPS, P99, build-time, and storage gates.

Run the native graph G1 gate:

```bash
akidb-graph-bench \
  --data-dir /qualification/graph-g1 \
  --documents 10000 \
  --chunks-per-document 4 \
  --entities 1000 \
  --queries 10000 \
  --concurrency 8 \
  --min-accuracy 1 \
  --max-p99-ms 50 \
  --output-json /qualification/evidence/graph-g1-c8.json
```

Run the complete persistent G1/G2/G3 concurrency matrix from the Ansible
controller:

```bash
AKIDB_GRAPH_RUN_ID=<unique-run-id> \
AKIDB_GRAPH_SERVER=akidb-amd64-3 \
AKIDB_GRAPH_OUTPUT_DIR=/qualification/evidence/graph \
AKIDB_GRAPH_CONFIRM=yes-isolate-one-qualification-replica \
ansible-playbook playbooks/knowledge-market-graph.yml
```

Each tier is built once. Concurrency 8, 32, and 64 reopen the same RocksDB
graph through `--skip-build`, so query comparisons do not silently rebuild a
different corpus.

Run gateway correctness and security from the Ansible controller:

```bash
ansible-playbook \
  -i inventories/lab/hosts.yml \
  playbooks/knowledge-load-test.yml
```

Run one explicitly authorized replica fault under load:

```bash
AKIDB_READINESS_FAULT_REPLICA=akidb-amd64-1 \
AKIDB_READINESS_FAULT_CONFIRM=yes-stop-one-qualification-replica \
ansible-playbook \
  -i inventories/lab/hosts.yml \
  playbooks/knowledge-failure-under-load.yml
```

Credentials remain environment-only and must never appear in inventory,
fixtures, reports, shell history, or committed evidence.
