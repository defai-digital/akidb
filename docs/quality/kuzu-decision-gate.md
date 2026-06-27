# Kuzu Adapter Decision Gate

AkiDB keeps the native RocksDB-backed graph index on the default hot path until
Kuzu proves it is worth adding as more than an optional adapter. The Kuzu
decision must be based on a native-vs-Kuzu benchmark artifact, not on feature
appeal alone.

The optional Rust adapter is available behind `akidb-graph/kuzu` for evaluation.
On macOS, use the shared Homebrew Kuzu library when running adapter tests:

```bash
KUZU_SHARED=1 \
KUZU_LIBRARY_DIR=/opt/homebrew/lib \
KUZU_INCLUDE_DIR=/opt/homebrew/include \
cargo test -p akidb-graph --features kuzu
```

Generate and validate a native-vs-Kuzu decision artifact:

```bash
./scripts/benchmark-kuzu-decision.sh
```

For a quick toolchain smoke run:

```bash
NODES=50 EDGES=150 QUERIES_PER_KIND=5 \
OUTPUT=/tmp/akidb-kuzu-decision-smoke.json \
./scripts/benchmark-kuzu-decision.sh
```

The current Homebrew formula marks Kuzu as deprecated because the upstream
repository is archived. Treat that as a maintenance risk: Kuzu can remain an
optional adapter only if the adoption artifact records the packaging source,
upstream status, rollback plan, and owner for monitoring future breakage.

Validate an artifact for optional adapter readiness:

```bash
python3 scripts/validate-kuzu-decision.py docs/reports/kuzu-decision-YYYYMMDD.json
```

Validate the stricter hot-path promotion gate:

```bash
python3 scripts/validate-kuzu-decision.py \
  docs/reports/kuzu-decision-YYYYMMDD.json \
  --mode hot-path
```

## Required Evidence

The artifact must compare native and Kuzu on the same Apple Silicon Mac, same
AkiDB commit, same dataset, and same query mix. It must include:

- hardware and software metadata
- node, edge, and related-chunk edge counts
- query mix containing `neighbors`, `two_hop`, `path_exists`, and
  `related_chunks`
- native and Kuzu ingest throughput
- native and Kuzu storage and peak RSS
- native and Kuzu query QPS and P50/P95/P99 latency
- result parity and matching node/edge counts
- an explicit recommendation and rationale
- Kuzu packaging source, upstream maintenance status, and rollback plan

## Default Gates

Optional adapter mode for `ship_optional_kuzu`:

- result parity >= 99.5%
- Kuzu P95/P99 <= 3.0x native
- Kuzu ingest wall time <= 4.0x native
- Kuzu storage <= 5.0x native
- Kuzu peak RSS <= 4.0x native
- Kuzu QPS >= 0.25x native
- recommendation is `ship_optional_kuzu`

A `reject_kuzu` artifact is also valid in optional-adapter mode when the
artifact is structurally complete and includes a rationale. In that case, the
validator records a defensible rejection rather than requiring Kuzu to pass the
adoption thresholds.

Hot-path promotion mode:

- result parity >= 99.9%
- Kuzu P95/P99 <= 1.25x native
- Kuzu ingest wall time <= 2.0x native
- Kuzu storage <= 2.0x native
- Kuzu peak RSS <= 2.0x native
- Kuzu QPS >= 0.80x native
- recommendation is `promote_kuzu_hot_path`
- rollback plan is present

If optional mode passes but hot-path mode does not, Kuzu can be kept as an
optional adapter for complex graph workloads while native remains the default
GraphRAG retrieval index.

## Artifact Shape

```json
{
  "schema_version": 1,
  "generated_at": "2026-06-27T00:00:00Z",
  "hardware": {
    "os": "macOS 15.5",
    "arch": "arm64",
    "mac_model": "Mac15,9",
    "memory_bytes": 68719476736
  },
  "software": {
    "akidb_commit": "abcdef0",
    "rustc": "rustc 1.88.0",
    "kuzu_version": "0.x"
  },
  "dataset": {
    "shape": "code_graph",
    "nodes": 1000000,
    "edges": 5000000,
    "related_chunk_edges": 1000000
  },
  "query_mix": [
    {"kind": "neighbors", "count": 1000},
    {"kind": "two_hop", "count": 1000},
    {"kind": "path_exists", "count": 1000},
    {"kind": "related_chunks", "count": 1000}
  ],
  "backends": {
    "native": {
      "available": true,
      "implementation": "akidb-native-graph",
      "ingest": {"wall_time_ms": 1000, "nodes_per_sec": 100000, "edges_per_sec": 500000},
      "storage": {"bytes": 1000000000},
      "memory": {"peak_rss_bytes": 2000000000},
      "queries": {
        "qps": 10000,
        "errors": 0,
        "latency": {"count": 4000, "min_ms": 0.1, "p50_ms": 0.4, "p95_ms": 1.0, "p99_ms": 2.0, "max_ms": 5.0}
      }
    },
    "kuzu": {
      "available": true,
      "implementation": "kuzu-rust",
      "ingest": {"wall_time_ms": 2500, "nodes_per_sec": 40000, "edges_per_sec": 200000},
      "storage": {"bytes": 3000000000},
      "memory": {"peak_rss_bytes": 5000000000},
      "queries": {
        "qps": 3500,
        "errors": 0,
        "latency": {"count": 4000, "min_ms": 0.2, "p50_ms": 0.9, "p95_ms": 2.5, "p99_ms": 5.0, "max_ms": 12.0}
      }
    }
  },
  "correctness": {
    "result_parity_percent": 99.8,
    "native_node_count": 1000000,
    "kuzu_node_count": 1000000,
    "native_edge_count": 5000000,
    "kuzu_edge_count": 5000000
  },
  "decision": {
    "recommendation": "ship_optional_kuzu",
    "rationale": "Passes parity and optional-adapter gates; native remains faster for hot-path GraphRAG.",
    "rollback_plan": "Keep native graph as the default hot path and disable akidb-graph/kuzu.",
    "packaging_source": "Homebrew shared Kuzu library plus kuzu Rust crate",
    "upstream_status": "Homebrew marks Kuzu deprecated because upstream is archived.",
    "maintenance_owner": "AkiDB maintainers"
  }
}
```
