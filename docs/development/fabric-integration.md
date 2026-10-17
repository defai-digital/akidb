# AX Fabric integration verification

AkiDB runs independently and is AX Fabric's preferred retrieval runtime.
Fabric owns canonical source revisions, access-policy semantics, publication
control and gateway receipts. AkiDB materializes rebuildable retrieval projections
and returns typed context, citations, diagnostics and serving-generation evidence.

The `ax-fabric/packages/akidb*` packages are a different packaging boundary from
this repository's Rust server and Python/TypeScript SDKs. Record the exact adapter
and engine revisions instead of assuming matching product names imply compatibility.

## Reproduce the lifecycle

Start with the public [single-node preview](generation-serving-preview.md),
[knowledge-serving runbook](../runbooks/knowledge-serving.md), and
[architecture and ownership](../architecture/knowledge-serving.md).
Use only synthetic records and credential-configured SeaweedFS instances.

The existing public tooling includes:

```bash
./scripts/test-generation-serving-local-mirror.sh
./scripts/test-generation-serving-seaweedfs.sh
python3 scripts/qa_generation_serving.py --help
```

Read each script's prerequisites before running it. The local-mirror test does
not establish S3, PostgreSQL convergence or gateway failover. The SeaweedFS path
requires the documented pinned binary and explicit credentials. Never disable
those checks to make the example work.

The local-mirror harness builds this checkout with `generation-s3`. When setting
`AKIDB_BIN` explicitly, supply a generation-enabled binary: the default standalone
build is insufficient. A binary's presence does not establish feature support.

For PostgreSQL replica convergence, configure the test database through the
documented environment variable and explicitly run the ignored gate:

```bash
cargo test -p akidb-grpc --features generation-postgres \
  three_blank_replicas_converge_rebuild_and_isolate_a_gap -- --ignored --nocapture
```

## Compatibility record

For each candidate, retain the engine commit/artifact checksum, SDK versions,
Fabric commit/adapter version, proto/fixture version, OS/architecture and the
commands actually run. Record each scenario as pass, fail or not run:

| Scenario | Required observation |
| --- | --- |
| Stage and retrieve | Generation, manifest, sequence and citations identify the intended snapshot |
| Activate a replacement | Vector, lexical, payload and graph results do not mix generations |
| Reject invalid input | Bad digest/model/schema or sequence gap cannot become ready |
| Restart/rebuild | Local indexes reconstruct from the canonical bundle and ordered mutations |
| Replica loss | The gateway only selects an eligible replica; timeout and stale-control limits apply |
| Delete/access change | The replacement generation enforces the new eligible record set |
| Roll back | The retained, qualified generation is served according to Fabric's activation contract |

The public known-answer manifests live under `contracts/fixtures/knowledge/`.
Extend those fixtures and integration tests when fixing a contract failure.
Do not publish credentials, private datasets, inventories or internal records.

## Qualification limits

The qualified cell envelope remains Ubuntu AMD64, three replicas, 100k x 768.
Single-node generation serving on Mac remains preview; Mac clusters and larger
capacity are separate unqualified targets. PostgreSQL and object-store HA are
external dependencies. See [Platform Support](../platform/SUPPORT.md).
No new compatibility or capacity result is implied by this checklist.
