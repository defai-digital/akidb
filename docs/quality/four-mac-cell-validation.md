# Four-Mac Cell Validation

The four-Mac Thunderbolt cell is deferred until the one-Mac product has
traction, but any future validation claim must be backed by a machine-readable
artifact. Do not mark the cell production-ready from prose notes alone.

Validate an artifact:

```bash
python3 scripts/validate-four-mac-cell.py docs/reports/four-mac-cell-YYYYMMDD.json
```

Create a measured-input template:

```bash
python3 scripts/build-four-mac-cell-artifact.py \
  --write-template docs/reports/four-mac-input-template.json
```

After replacing the template values with real node inventory, six Thunderbolt
link measurements, and failure-test results, build and validate the artifact:

```bash
python3 scripts/build-four-mac-cell-artifact.py \
  --input docs/reports/four-mac-input-template.json \
  --one-mac-qps 586.434 \
  --cell-qps 2600 \
  --cell-p95-ms 45 \
  --cell-p99-ms 90 \
  --output docs/reports/four-mac-cell-YYYYMMDD.json \
  --validate
```

The builder also supports split measured-input files with `--nodes`,
`--links`, and `--failure-tests` when the collection workflow stores those
measurements separately.

The validator checks:

- exactly four healthy Apple Silicon nodes
- three metadata voters and one learner/data-only node
- homogeneous hot-cell hardware unless `--allow-heterogeneous` is explicit
- complete Thunderbolt link coverage across all six node pairs
- RF>=2 shard placement with primary and replicas on distinct Macs
- node-loss and link-loss degraded-mode tests
- throughput ratio versus the one-Mac reference benchmark
- no Kubernetes dependency for the initial production path

Default gates:

- link P95 <= 500 microseconds
- link bandwidth >= 10 Gbps
- packet loss <= 0.01%
- cell throughput >= 2.5x one-Mac throughput

Example artifact shape:

```json
{
  "schema_version": 1,
  "generated_at": "2026-06-27T00:00:00Z",
  "cell": {
    "id": "cell-a",
    "nodes": [
      {"id": "mac-1", "host": "mac-1.local", "arch": "arm64", "mac_model": "Mac15,9", "memory_bytes": 68719476736, "role": "voter", "healthy": true},
      {"id": "mac-2", "host": "mac-2.local", "arch": "arm64", "mac_model": "Mac15,9", "memory_bytes": 68719476736, "role": "voter", "healthy": true},
      {"id": "mac-3", "host": "mac-3.local", "arch": "arm64", "mac_model": "Mac15,9", "memory_bytes": 68719476736, "role": "voter", "healthy": true},
      {"id": "mac-4", "host": "mac-4.local", "arch": "arm64", "mac_model": "Mac15,9", "memory_bytes": 68719476736, "role": "learner", "healthy": true}
    ]
  },
  "deployment": {"orchestrator": "none"},
  "network": {
    "links": [
      {"from": "mac-1", "to": "mac-2", "transport": "thunderbolt", "healthy": true, "latency_p95_us": 120, "bandwidth_gbps": 20, "packet_loss_percent": 0}
    ]
  },
  "placement": {
    "collections": [
      {
        "name": "default",
        "replication_factor": 2,
        "shards": [
          {"id": "shard-0", "primary": "mac-1", "replicas": ["mac-2"]}
        ]
      }
    ]
  },
  "failure_tests": [
    {"kind": "node_loss", "passed": true, "observed_status": "degraded", "recovery_time_ms": 500},
    {"kind": "link_loss", "passed": true, "observed_status": "degraded", "recovery_time_ms": 250}
  ],
  "benchmark": {
    "one_mac_qps": 1000,
    "cell_qps": 2600,
    "throughput_ratio": 2.6,
    "cell_p95_ms": 45,
    "cell_p99_ms": 90
  }
}
```

The example omits five of the six required Thunderbolt links for brevity; a real
artifact must include every pair. The generated input template includes all six
node pairs so it can be used directly with `--input` after replacing the example
measurements.
