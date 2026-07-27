# Linux AMD64 Authoritative Memory Qualification

**Qualification date:** 2026-07-27

**Status:** Passed for the bounded synthetic systems profile in this document

**Measured source commit:** `f61f0ba7c5113ac6b061e6684f00d846ef1e2b1a`

**Product status:** Experimental single-process developer preview

## Decision

The Authoritative Memory implementation passes its Linux AMD64 technical
systems profile at 1,000, 10,000, and 100,000 committed memory versions. The
matrix used five fresh release runs per size across all four requested
`akidb-amd64-1` through `akidb-amd64-4` hosts.

Across 555,000 synced commits and 15,000 measured known-answer recalls, the
matrix recorded:

- zero commit failures;
- zero recall failures;
- zero incorrect known-answer recalls; and
- zero maximum commit-to-mandatory-projection visibility lag.

This is a bounded technical qualification, not a production release or a
product-market claim. AkiDB Memory remains an experimental
one-authoritative-workspace-per-process preview. The result does not authorize
system-of-record, HA, fleet, semantic-quality, competitor, or SOTA wording.

The complete aggregate and raw evidence are retained under the
[evidence manifest](evidence/authoritative-memory-amd64-f61f0ba7c511/README.md).

## Source and host profile

Every run report independently recorded the same full source commit, a clean
Git tree, Linux `x86_64`, and a PASS verdict. The source commit was tested
before the benchmark matrix; this later report-only commit does not change the
measured implementation.

All four VMs reported:

| Property | Measured value |
| --- | --- |
| OS | Ubuntu 26.04 LTS |
| Kernel / architecture | Linux 7.0.0-28-generic / `x86_64` |
| CPU | Intel Xeon Processor (Cascadelake), 8 logical cores |
| RAM | 32,569,462,784–32,569,470,976 bytes |
| Root filesystem | `/dev/vda2`, ext4, 514,840,973,312-byte virtual disk |
| Rust / C++ toolchain | Rust 1.93.1, GCC/G++ 15.2.0 |

The hypervisor exposed a virtual disk; the underlying medium was not verified
as physical NVMe. These were shared lab hosts. Existing AkiDB, PostgreSQL,
MinIO, and other background services remained running where present, and the
captured load/process snapshots are part of the evidence. Results must
therefore not be represented as isolated, dedicated, bare-metal performance.

## Exact-SHA validation

The four-host validation split exercised the complete workspace and the
language/tooling surfaces without treating a benchmark run as a substitute
for tests:

| Host | Exact command or validation role | Result |
| --- | --- | --- |
| `akidb-amd64-1` | `cargo test --workspace --locked -j 2` | PASS — full Rust workspace, integration, and doc-test command completed |
| `akidb-amd64-2` | `cargo clippy --workspace --all-targets --locked -j 2` | PASS — two existing coordinator `too_many_arguments` warnings retained in the log |
| `akidb-amd64-3` | `cargo check --workspace --all-targets --locked -j 2` | PASS |
| `akidb-amd64-4` | Changed Memory packages plus Python SDK, script, protobuf/codegen, TypeScript, shell, and sensitive-file gates | PASS — Python SDK 37 passed/2 skipped, script tests 39 passed, TypeScript 16 passed/1 skipped |

Each environment record binds the result to the full source SHA and confirms
that the source remained clean. Host 2 needed the distro `rust-clippy`
component and host 4 needed the distro Python venv component during preflight;
the final checksum-bound commands started afterward and passed from a clean
source tree.

The retained [validation directories](evidence/authoritative-memory-amd64-f61f0ba7c511/validation/)
contain the command logs, environment records, and SHA-256 manifests.

## Benchmark method

Each run used
[`scripts/qualify-agentic-memory-amd64.sh`](../../scripts/qualify-agentic-memory-amd64.sh)
with:

| Setting | Value |
| --- | ---: |
| Build | Locked release build |
| Active committed versions | 1,000 / 10,000 / 100,000 |
| Fresh runs per size | 5 |
| Synced Remember concurrency | 8 |
| Measured Recall requests per run | 1,000 |
| Warm-up Recall requests per run | 20 |
| Recall concurrency | 8 |
| Recall recipe | `preview-bounded-bm25-v1` |
| `top_k` / context budget | 10 / 256 tokens |
| Transport | Loopback gRPC |
| Workspace topology | One authoritative workspace per process |
| RSS observation interval | 100 ms |

Every run required a clean Linux AMD64 source tree, unused loopback ports, and
new absolute work and evidence directories. It generated a deterministic
known-answer corpus bound to the run ID and corpus size, launched a fresh
server and RocksDB state, committed all versions with `SYNCED` durability,
then issued the warm-up and measured recalls. It retained every commit and
recall latency sample, disk observations, continuous RSS samples, a live
content-free metrics scrape, and server log.

The checksum-validating aggregator required exactly 15 reports, five per size,
the exact four host labels mapped one-to-one to four machine identity digests,
the exact source commit and clean-tree state, the fixed configuration, complete
sample distributions, all-zero correctness counters, and required live
metrics. Re-aggregation produced a byte-identical `summary.json`.

## Performance envelope

Values below are medians across the five independent runs at each size.
Brackets contain the deterministic bootstrap 95% interval for that run-level
median. Latency columns are medians of the five per-run percentile values, not
percentiles formed by pooling requests across machines.

### Synced commit acknowledgement through visibility

| Versions | Throughput, versions/s | P50, ms | P95, ms | P99, ms |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 561.98 [491.98, 589.50] | 13.821 [13.326, 16.001] | 15.911 [14.686, 18.251] | 19.191 [15.740, 26.608] |
| 10,000 | 505.70 [443.66, 538.97] | 15.591 [14.543, 17.209] | 18.349 [17.161, 23.181] | 20.433 [20.035, 26.131] |
| 100,000 | 444.49 [438.98, 462.94] | 17.570 [16.717, 17.859] | 21.358 [20.480, 22.405] | 25.219 [23.328, 26.950] |

The latency starts before the Remember RPC and ends only after the returned
receipt proves the commit visible through the mandatory projection set.

### Known-answer bounded-BM25 recall

| Versions | Throughput, recalls/s | P50, ms | P95, ms | P99, ms |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 848.24 [652.78, 895.78] | 9.184 [8.838, 12.296] | 10.700 [9.402, 13.887] | 12.407 [9.896, 16.118] |
| 10,000 | 751.93 [588.21, 811.49] | 10.470 [9.652, 13.580] | 11.954 [10.773, 15.165] | 12.562 [11.797, 16.383] |
| 100,000 | 730.07 [677.65, 748.77] | 10.871 [10.474, 11.671] | 11.995 [11.672, 13.310] | 12.537 [11.971, 14.184] |

### Observed process and storage footprint

| Versions | Disk growth, MiB | Peak observed server RSS, MiB |
| ---: | ---: | ---: |
| 1,000 | 25.62 [25.61, 25.62] | 51.24 [51.14, 51.50] |
| 10,000 | 56.94 [56.93, 57.00] | 103.00 [102.14, 112.05] |
| 100,000 | 365.40 [365.33, 365.45] | 211.00 [206.97, 216.88] |

Disk growth is the run's fresh data-directory size after measured recalls
minus its initial size. RSS is the greatest 100-ms server sample observed
during that run. Neither is a capacity planner for arbitrary content or
retention policies.

Five run-level observations produce a deliberately small-sample interval.
These intervals communicate observed repeatability; they are not evidence for
the wider population of AMD64 machines or workloads.

## Correctness, observability, and credential handling

The measured path verified, for each fresh run:

- sequences started at 1 and ended at the requested version count;
- every requested synced commit succeeded and became visible with no observed
  mandatory-projection lag;
- all measured recalls succeeded and returned the deterministic expected
  memory;
- the active canonical and persisted incremental BM25 projection checkpoints
  advanced to the commit sequence;
- every recall persisted its snapshot;
- required commit, projection, recall, snapshot, and authorization metrics
  were live; and
- metrics did not contain the run namespace.

The harness generated distinct principal and legacy bearer tokens in
mode-`0600` files outside the evidence directory. It scanned the report,
metrics, and server log for both exact credential values before checksumming
the run. No token or generated server configuration was copied into the
evidence tree. This is evidence for the harness's credential handling and
content-free metrics checks, not a general penetration-test certification.

## Qualification boundary

This report does not qualify:

- production, system-of-record, multi-tenant, or fleet use;
- Memory HA, quorum, placement, failover, resharding, or cross-process
  convergence;
- remote-listener TLS, external identity providers, secret lifecycle, or a
  production deployment topology;
- finite retention enforcement or artifact garbage collection;
- more than 100,000 active versions, long-duration soak, or a dedicated-host
  capacity limit;
- dense retrieval, an embedding model, natural-language compiler quality,
  unified Memory-plus-Knowledge answer quality, or model-generated answers;
- semantic benchmark advantage over no-memory, hybrid RAG, Hindsight, Mem0,
  Graphiti, or any other system;
- external quickstart success, pilots, design-partner retention, competitive
  wins, incident-time improvement, or independent reproduction; or
- SOTA, production availability, physical-NVMe, or universal performance
  claims.

Those exclusions are gates, not implied failures. The current supported claim
is narrower: the exact source commit passed this reproducible synthetic Linux
AMD64 correctness and systems envelope while retaining its experimental
single-process preview label.
