# Authoritative Memory AMD64 Qualification Evidence

**Qualification date:** 2026-07-27

**Source commit:** `f61f0ba7c5113ac6b061e6684f00d846ef1e2b1a`

**Aggregate verdict:** `PASS`

This directory is the checksum-bound evidence for the bounded Authoritative
Memory Linux AMD64 systems qualification. The measured source tree was clean
and detached at the full commit above on every host. The later documentation
commit that publishes this directory does not change the measured binaries or
source.

## Evidence layout

- [`summary.json`](summary.json) is the deterministic four-host aggregate. It
  binds all 15 reports to the source commit, profile, host labels, machine
  identity digests, run IDs, and per-run checksums.
- `runs/<host>/<run-id>/` contains one `report.json`, content-free Prometheus
  scrape, server log, and `SHA256SUMS` for each fresh run.
- `runs/<host>/host-environment.txt` records the source state, platform,
  filesystem, load, and process snapshot taken before the host's benchmark
  sequence. `HOST_SHA256SUMS` covers it and the build/driver log.
- `validation/<host>/` contains the exact-SHA validation environment, command
  log, and checksum manifest for that host.
- [`EVIDENCE_SHA256SUMS`](EVIDENCE_SHA256SUMS) covers every other file in this
  directory. It intentionally does not include itself.

The run allocation was:

| Host | Fresh run IDs |
| --- | --- |
| `akidb-amd64-1` | `amd64-1-v1000-r1`, `amd64-1-v10000-r1`, `amd64-1-v100000-r1`, `amd64-1-v100000-r5` |
| `akidb-amd64-2` | `amd64-2-v1000-r2`, `amd64-2-v10000-r2`, `amd64-2-v10000-r5`, `amd64-2-v100000-r2` |
| `akidb-amd64-3` | `amd64-3-v1000-r3`, `amd64-3-v1000-r5`, `amd64-3-v10000-r3`, `amd64-3-v100000-r3` |
| `akidb-amd64-4` | `amd64-4-v1000-r4`, `amd64-4-v10000-r4`, `amd64-4-v100000-r4` |

This gives five independent fresh process/data runs at each of 1,000, 10,000,
and 100,000 versions, and uses all four distinct machine identities.

## Reproduce the aggregate

From the repository root:

```bash
aggregate_dir=$(mktemp -d)
python3 scripts/summarize_agentic_memory_benchmarks.py \
  --evidence-dir docs/quality/evidence/authoritative-memory-amd64-f61f0ba7c511/runs \
  --expected-git-commit f61f0ba7c5113ac6b061e6684f00d846ef1e2b1a \
  --expected-host akidb-amd64-1 \
  --expected-host akidb-amd64-2 \
  --expected-host akidb-amd64-3 \
  --expected-host akidb-amd64-4 \
  --output "$aggregate_dir/summary.json"

cmp \
  docs/quality/evidence/authoritative-memory-amd64-f61f0ba7c511/summary.json \
  "$aggregate_dir/summary.json"
```

The aggregator fails closed on missing reports, mismatched checksums or source
state, unexpected host/machine mappings, profile drift, incomplete latency or
RSS samples, correctness failures, content-bearing metric labels, or a
non-PASS run verdict.

Verify the complete tree from the repository root:

```bash
evidence_root=docs/quality/evidence/authoritative-memory-amd64-f61f0ba7c511
(cd "$evidence_root" && shasum -a 256 -c EVIDENCE_SHA256SUMS)
```

The nested manifests remain useful for verifying an individual host or run.
The root manifest also covers those manifests.

## Security and claim boundary

The harness generated separate principal and legacy bearer tokens in
mode-`0600` work files, checked both values against every exported run
artifact, and did not copy credentials or configuration files into this
directory. Metrics were also rejected if they contained a run namespace.
Repository sensitive-file policy was rerun against the final tracked tree.

Host labels, a generic reported hostname, and one-way machine identity digests
are retained to prove four distinct machines. No public IP address or
credential is included.

These artifacts establish only the synthetic, single-process,
one-authoritative-workspace systems profile described in the
[qualification report](../../linux-amd64-authoritative-memory-qualification.md).
They do not establish semantic answer quality, competitor or hybrid-RAG
advantage, production system-of-record suitability, high availability, fleet
behavior, isolated bare-metal performance, or a SOTA claim.
