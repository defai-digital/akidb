# AkiDB Linux AMD64 Cluster Deployment

This directory provides two reproducible Ubuntu 24.04-or-newer AMD64 paths,
plus isolated market-qualification playbooks:

| Profile | Playbooks | Status and purpose |
| --- | --- | --- |
| Knowledge-serving cell | `knowledge-preflight.yml`, `knowledge-site.yml`, `knowledge-verify.yml`, load/failure/backup drills | Supported availability design: three independently rebuilt full replicas, two AX gateways, PostgreSQL authority, MinIO artifacts. Bounded Ubuntu AMD64 envelope is qualified. |
| Market qualification | `knowledge-market-ann.yml`, `knowledge-market-recovery.yml`, `knowledge-market-competitors.yml`, `knowledge-market-graph.yml` | Active release gate automation. Isolates one replica or runs competitors; not production reconciliation. |
| Independent-shard lab (N=1, 2, or 4+) | `preflight.yml`, `network.yml`, `deploy.yml`, `verify.yml`, `site.yml` | Capacity path: 1 standalone, 2 dual-shard, or 4+ multi-shard with one coordinator. Not HA and not the agent-facing replica design. N=3 is not a supported size. |

These playbooks target the **enterprise AMD64 cloud/lab path**. Product
targets also include single-user Mac Studio or AMD64 PC, and enterprise Mac
Studio clusters. Linux ARM64, NVIDIA Thor, CUDA, and older Ubuntu releases are
outside the supported matrix. See
[`docs/platform/SUPPORT.md`](../../docs/platform/SUPPORT.md),
[`docs/architecture/knowledge-serving.md`](../../docs/architecture/knowledge-serving.md),
and
[`docs/quality/market-readiness-qualification.md`](../../docs/quality/market-readiness-qualification.md).

## Design decision

The deployment unit is one immutable, checksum-pinned tar archive built once
by CI. Ansible installs that exact archive on every node, renders versioned
configuration, switches an atomic `current` symlink, restarts one shard at a
time, and gates each step on the gRPC health API.

This is preferable to compiling on each server:

- every node runs byte-identical binaries;
- the source commit, artifact checksum, deployment, and rollback target are
  directly traceable;
- build tools are not installed on production-like nodes;
- rerunning a playbook is idempotent.

It is also a better first qualification path than Kubernetes:

- four systemd hosts are easy to inspect and benchmark;
- RocksDB ownership and local NVMe behavior remain explicit;
- there is no orchestrator overhead hiding product failures.

The tradeoffs are deliberate:

- Ansible controls host state, so operating-system consistency still matters;
- these four servers are independent shards, not a replicated high-
  availability cluster. A failed node makes that shard's data unavailable;
- coordinator high availability is not implemented;
- the current coordinator does not propagate bearer token or workspace
  metadata to shards. Cluster mode therefore uses `auth.mode=disabled` only
  on a WireGuard-only service network. Public interfaces never bind AkiDB
  ports. Coordinator authentication propagation is a production blocker, not
  a waived security requirement.

## Target cloud architecture

The current WireGuard lab reuses already-provisioned public VMs. The
repeatable cloud baseline should be provisioned with Terraform or OpenTofu:

```text
OpenTofu
├── VPC and private subnets
├── dedicated AkiDB data VMs
├── local-NVMe data/WAL placement
├── firewall and load-balancer boundaries
└── object-storage backup target

Ansible
├── immutable AkiDB release
├── systemd units and versioned configuration
├── rolling health gates
└── rollback and qualification
```

Docker images may remain packaging and CI artifacts, but Docker Compose is not
the cross-host production orchestrator. The medium-term deployment is hybrid:
Kubernetes runs stateless document parsers, upload gateways, ingestion
workers, and monitoring; dedicated VMs run RocksDB, vector indexes, and WAL.
An orchestrator can restart a process, but it cannot substitute for database-
level replication, durable shard placement, or failover semantics.

## Supported cluster sizes (independent-shard profile)

Inventory length drives topology. Playbooks loop over
`groups['akidb_shards']` / `groups['akidb_cluster']` — do **not** write
`if n==1 / elif n==2 / elif n==4` trees in roles.

| Size | Inventory shape | Client entrypoint |
| --- | --- | --- |
| **N=1** | 1 host in `akidb_shards`; omit `akidb_coordinators` | Shard `:50051` |
| **N=2** | 2 shards + exactly 1 coordinator (often co-located) | Coordinator `:50050` |
| **N=4+** | N shards (4, 5, 6, …) + exactly 1 coordinator | Coordinator `:50050` |
| **N=3** | Not supported for this profile | — |

Examples (documentation IPs only):

- `inventories/example/hosts.single.yml` — N=1
- `inventories/example/hosts.dual.yml` — N=2
- `inventories/example/hosts.yml` — N=4 (comment shows how to add N>4)

Real host maps stay in gitignored `inventories/lab/`.

## Network and host model

SSH uses each host's public (or management) IP. AkiDB data/control ports bind
only to a **trusted service plane**, never to `0.0.0.0` / the public NIC.

Two service-plane modes are supported via `akidb_service_network_mode`:

| Mode | When to use | `akidb_overlay_address` | WireGuard |
| --- | --- | --- | --- |
| `private` | Provider private backplane exists (VPC, vRack, private NIC) | Private NIC IPs | Off |
| `wireguard` (default) | Hosts only share public reachability | Synthetic overlay IPs (e.g. `10.77.0.x`) | Full mesh |

### WireGuard mode (no private back network)

The public endpoints below use the RFC 5737 documentation range; real host
addresses stay only in the gitignored lab inventory.

```text
documentation SSH             WireGuard service plane
192.0.2.11       ────────>     10.77.0.11
192.0.2.12       ────────>     10.77.0.12
192.0.2.13       ────────>     10.77.0.13
192.0.2.14       ────────>     10.77.0.14
```

UFW permits WireGuard UDP only from the other declared peer public IPs. Shards
bind `10.77.0.x:50051`; the coordinator binds `10.77.0.11:50050`. WireGuard
private keys are generated and retained on their respective hosts.

### Private mode (trusted backplane)

```yaml
# Set on each host (host vars beat playbook group_vars defaults):
akidb_service_network_mode: private
akidb_overlay_address: 10.1.0.132          # this host's private NIC
akidb_overlay_cidr: 10.1.0.0/16            # cluster private CIDR
akidb_service_network_interface: ens4      # optional UFW interface bind
```

Preflight requires the overlay address to be present on a local interface.
`network.yml` skips WireGuard, opens AkiDB ports only from `akidb_overlay_cidr`
(optionally on the private NIC), and verifies peer reachability over the
backplane. Systemd units do **not** `Require=` WireGuard in this mode.

The real lab inventory is stored under `inventories/lab/` and is intentionally
gitignored. Legacy inventories may still set `akidb_manage_wireguard: false`
instead of mode; roles treat that as private when mode is empty.

## Artifact flow

`.github/workflows/linux-amd64-artifact.yml` builds on the Ubuntu 24.04 glibc
baseline and emits:

```text
akidb-linux-amd64-<git-sha>.tar.gz
akidb-linux-amd64-<git-sha>.tar.gz.sha256
```

The archive contains `akidb`, `akidb-server`, `akidb-coordinator`,
`akidb-bench`, `akidb-ann-bench`, `akidb-graph-bench`, and a build manifest.
The Linux AMD64 server is compiled with the optional `generation-postgres`
control surface; it remains disabled at runtime unless generation serving is
explicitly configured. The package script defaults to the Ubuntu LTS GCC/G++
toolchain and allows explicit `CC`/`CXX` overrides for separate compiler
qualification. GitHub attaches a provenance attestation.

Download a completed CI artifact:

```bash
gh run list --workflow linux-amd64-artifact.yml --limit 1
gh run download <run-id> \
  --name akidb-linux-amd64-<git-sha> \
  --dir dist
```

Export the immutable release inputs:

```bash
export AKIDB_RELEASE_ID=<git-sha>
export AKIDB_ARTIFACT_PATH="$PWD/dist/akidb-linux-amd64-<git-sha>.tar.gz"
export AKIDB_ARTIFACT_SHA256="$(cut -d ' ' -f 1 "$AKIDB_ARTIFACT_PATH.sha256")"
```

Do not put tokens, SSH private keys, or real infrastructure credentials in
committed inventory.

## Reproducible operations

Run from `deploy/ansible`.

Run the non-mutating host capacity and security gates first:

```bash
ansible-playbook playbooks/preflight.yml
```

Bootstrap or reconcile the private overlay only after preflight passes:

```bash
ansible-playbook playbooks/network.yml
```

Stage one artifact everywhere and perform a rolling deployment:

```bash
ansible-playbook playbooks/deploy.yml
```

Verify the release ID and checksum on every host, local shard health,
coordinator-to-shard reachability, coordinator health, and public bind policy:

```bash
ansible-playbook playbooks/verify.yml
```

The complete, safely rerunnable path for the **independent-shard lab**
(N=1, 2, or 4+) is:

```bash
ansible-playbook playbooks/site.yml
```

Rollback requires an already installed release and its original artifact
checksum, and still rolls one shard at a time:

```bash
export AKIDB_ROLLBACK_RELEASE_ID=<previous-git-sha>
export AKIDB_ROLLBACK_ARTIFACT_SHA256=<previous-artifact-sha256>
ansible-playbook playbooks/rollback.yml
```

Configuration is stored per release under `/etc/akidb/releases/`. Binaries are
stored under `/opt/akidb/releases/`; `/opt/akidb/current` is the atomic
activation pointer. Persistent RocksDB, WAL, and snapshots stay under
`/var/lib/akidb` and are never removed by deploy or rollback.

## Knowledge-serving cell

The knowledge cell is the supported availability profile. Operator detail lives
in [`docs/runbooks/knowledge-serving.md`](../../docs/runbooks/knowledge-serving.md).
Bounded Ubuntu AMD64 evidence is recorded in
[`docs/quality/linux-amd64-knowledge-cell-qualification.md`](../../docs/quality/linux-amd64-knowledge-cell-qualification.md).

From `deploy/ansible`, with immutable AkiDB and gateway artifacts exported:

```bash
ansible-galaxy collection install -r requirements.yml
ansible-playbook playbooks/knowledge-preflight.yml
ansible-playbook playbooks/knowledge-site.yml
ansible-playbook playbooks/knowledge-verify.yml
```

`knowledge-site.yml` is safely rerunnable. It composes preflight, network,
optional lab dependencies, deploy, and verify. Lab dependency management is
gated by inventory (`akidb_knowledge_manage_lab_dependencies`); production
points at managed PostgreSQL and S3/MinIO instead.

Additional knowledge drills (separate from market isolation):

- `knowledge-load-test.yml` — gateway correctness, security, and paced load
- `knowledge-failure-under-load.yml` — one authorized replica fault under traffic
- `knowledge-blank-rebuild.yml` — blank volume rebuild from canonical state
- `knowledge-backup.yml` / `knowledge-restore-verify.yml`
- `knowledge-rolling-upgrade.yml` / `knowledge-rollback.yml`

## Market qualification

Market qualification is deliberately separate from production reconciliation.
Do not fold these playbooks into `knowledge-site.yml`. Each market run requires
an explicit confirmation string, WireGuard-only service exposure, and a unique
run ID. CI only syntax-checks these playbooks; it does not execute SIFT1M or
competitor workloads.

Recommended order on a healthy knowledge cell:

1. Convert and stage the public SIFT1M files under
   `/var/tmp/akidb-market-data/…` with
   `scripts/convert_ann_benchmarks_hdf5.py`.
2. Run `knowledge-market-ann.yml` for the absolute AkiDB SIFT1M matrix.
3. Run `knowledge-market-recovery.yml` against that passed, persisted SIFT1M
   run for fsynced acknowledged-mutation, SIGKILL, automatic restart, and exact
   ANN recovery evidence.
4. Run `knowledge-market-competitors.yml` for pinned Milvus then Weaviate on
   the same dataset digests, then the fail-closed parity summary.
5. Run `knowledge-market-graph.yml` for the native G1/G2/G3 matrix on an
   isolated replica.

### AkiDB SIFT1M

```bash
AKIDB_MARKET_RUN_ID=<unique-run-id> \
AKIDB_MARKET_SERVER=akidb-amd64-3 \
AKIDB_MARKET_DRIVER=akidb-amd64-4 \
AKIDB_MARKET_DATASET_DIR=/var/tmp/akidb-market-data/sift1m-fvecs \
AKIDB_MARKET_OUTPUT_DIR=/qualification/evidence/akidb \
AKIDB_MARKET_CONFIRM=yes-isolate-one-qualification-replica \
ansible-playbook playbooks/knowledge-market-ann.yml
```

Isolates one replica, loads the public SIFT1M matrix through `akidb-ann-bench`,
writes point reports, always restores generation readiness, and summarizes with
`scripts/summarize_market_ann.py`.

### Mutable crash recovery

Run this only after the referenced SIFT1M summary has passed for the same
immutable artifact. The playbook reuses that run's isolated data directory;
it never points the crash fault at a knowledge-serving generation.

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

`akidb-recovery-probe` fsyncs an acknowledgement journal after deterministic
insert, update, and delete responses. After Ansible sends SIGKILL to the exact
isolated service main process, the verifier rejects any acknowledged state
regression, bounds in-flight ambiguity to one operation per worker, removes
all probe IDs, and requires the SIFT1M active count and exact Recall@10 before
and after both crash and graceful restarts. The unconditional recovery block
restores exact-generation knowledge-replica readiness.

### Competitor parity (Milvus and Weaviate)

Pinned comparison set reviewed 2026-07-26:

- Milvus server `v2.6.21` with `pymilvus==2.6.17`
- Weaviate server `1.38.6` with `weaviate-client==4.22.0`

Inject `AKIDB_COMPETITOR_MINIO_ACCESS_KEY` and
`AKIDB_COMPETITOR_MINIO_SECRET_KEY` from the CI secret store or an ephemeral
lab credential helper before invoking the playbook. Do not place either value
in the command line or an inventory file.

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

Qualification-only behavior:

- installs a temporary Docker runtime on the isolated server;
- binds database ports only to the WireGuard address;
- runs one engine at a time with `scripts/competitor_ann_bench.py`;
- records resolved container `RepoDigest` values, not tags alone;
- removes containers and data directories after each engine;
- always restores exact-generation AkiDB readiness;
- fails closed through `scripts/summarize_market_parity.py`.

### Native graph matrix

```bash
AKIDB_GRAPH_RUN_ID=<unique-run-id> \
AKIDB_GRAPH_SERVER=akidb-amd64-3 \
AKIDB_GRAPH_OUTPUT_DIR=/qualification/evidence/graph \
AKIDB_GRAPH_CONFIRM=yes-isolate-one-qualification-replica \
ansible-playbook playbooks/knowledge-market-graph.yml
```

Each tier builds once; higher concurrency reopens the same RocksDB graph via
`--skip-build`. Summary: `scripts/summarize_market_graph.py`.

Full gate definitions, absolute and relative thresholds, and the phased
release decision live in
[`docs/quality/market-readiness-qualification.md`](../../docs/quality/market-readiness-qualification.md).

## Qualification phases

### Phase 0 — repeatable infrastructure

- SSH and host inventory
- restricted WireGuard overlay
- OpenTofu design for VPC, VM, disk, firewall, load balancer, and backup target
- Ubuntu 24.04+ AMD64 operating-system baseline; older distributions are
  rejected rather than treated as best-effort targets
- hardware, OS, disk, systemd, and bind-policy preflight
- immutable artifacts, checksums, provenance, rolling activation, rollback

Exit criterion: a second `network.yml` and `site.yml` run is idempotent, and
no AkiDB service port is reachable on a public interface.

### Phase 1 — functional multi-shard cluster (N=2 or N>=4)

- direct health on all shards
- coordinator fan-out paths to every declared shard
- deterministic insert, get, update, delete, and search
- fsynced acknowledged-mutation, SIGKILL, and RocksDB/WAL recovery
- GraphRAG node, edge, traversal, and context-expansion smoke tests

Exit criterion: all API tests pass with no missing shard and no data loss.

### Phase 2 — failure and upgrade behavior

- stop one shard and verify partial-result semantics
- restart and verify convergence
- rolling deploy while issuing reads and writes
- checksum mismatch rejection
- deliberate failed health gate
- rollback to the preceding release
- reboot every host one at a time

Exit criterion: documented recovery time, no silent success, and verified
rollback.

### Phase 3 — performance and capacity

- vector-count steps at 100k, 1m, and the safe disk/RAM limit
- P50/P95/P99 latency, throughput, RSS, disk amplification, and recovery time
- vector-only, hybrid, graph 1-hop, graph 2-hop, and graph+rereank comparison
- cold-start versus warm-cache runs

Exit criterion: reproducible benchmark reports and a supported sizing table.
The current 8-vCPU/32-GB hosts are sufficient to begin; 16 vCPU is useful for
high-concurrency saturation, not a functional requirement.

### Phase 4 — platform matrix

- Linux AMD64 cluster release qualification
- macOS 26 ARM64 regression qualification
- mixed-client compatibility and artifact provenance checks

Exit criterion: only passing combinations are described as supported.

### Phase 5 — architecture decision gate

- Preserve this profile as a measured independent-shard capacity lab.
- Do not add shard replication, placement, rebalancing, and distributed graph
  traversal before one full-replica generation exceeds a measured resource or
  QPS limit.
- Qualify PostgreSQL-led full replicas and generation-aware AX routing in a
  separate topology.
- Keep coordinator bearer/workspace propagation, TLS/mTLS, distributed
  `TextSearch`, backup/restore, deletion propagation, monitoring, and secret
  management as blockers for any production use of this shard profile.

Exit criterion: the private lab exception (`auth.mode=disabled` inside
WireGuard) is never presented as production exposure, and evidence determines
whether a later sharded-replica design is necessary.
