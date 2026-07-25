# AkiDB Linux AMD64 Cluster Deployment

This directory provides the reproducible qualification path for a four-node
Ubuntu 24.04-or-newer AMD64 cluster. The native AkiDB runtime also supports
macOS 26 Apple Silicon and Ubuntu 24.04+ ARM64, but this checksum-pinned
cluster artifact and Ansible profile remain AMD64-specific. They produce
qualification evidence and are not an HA or production-support claim.

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

## Network and host model

SSH uses each host's public IP. AkiDB uses a WireGuard full mesh:

The public endpoints below use the RFC 5737 documentation range; real host
addresses stay only in the gitignored lab inventory.

```text
documentation SSH             private AkiDB service network
192.0.2.11       ────────>     10.77.0.11
192.0.2.12       ────────>     10.77.0.12
192.0.2.13       ────────>     10.77.0.13
192.0.2.14       ────────>     10.77.0.14
```

UFW permits WireGuard UDP only from the other declared peer IPs. Shards bind
`10.77.0.x:50051`; the coordinator binds `10.77.0.11:50050`. The real lab
inventory is stored under `inventories/lab/` and is intentionally gitignored.
WireGuard private keys are generated and retained on their respective hosts.

## Artifact flow

`.github/workflows/linux-amd64-artifact.yml` builds on the Ubuntu 24.04 glibc
baseline and emits:

```text
akidb-linux-amd64-<git-sha>.tar.gz
akidb-linux-amd64-<git-sha>.tar.gz.sha256
```

The archive contains `akidb`, `akidb-server`, `akidb-coordinator`,
`akidb-bench`, and a build manifest. The Linux AMD64 server is compiled with
the optional `generation-s3` control surface; it remains disabled at runtime
unless generation serving is explicitly configured. GitHub attaches a
provenance attestation.

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

Verify local shard health, coordinator-to-shard reachability, coordinator
health, and public bind policy:

```bash
ansible-playbook playbooks/verify.yml
```

The complete, safely rerunnable path is:

```bash
ansible-playbook playbooks/site.yml
```

Rollback requires an already installed release and still rolls one shard at a
time:

```bash
export AKIDB_ROLLBACK_RELEASE_ID=<previous-git-sha>
ansible-playbook playbooks/rollback.yml
```

Configuration is stored per release under `/etc/akidb/releases/`. Binaries are
stored under `/opt/akidb/releases/`; `/opt/akidb/current` is the atomic
activation pointer. Persistent RocksDB, WAL, and snapshots stay under
`/var/lib/akidb` and are never removed by deploy or rollback.

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

### Phase 1 — functional four-shard cluster

- direct health on all shards
- coordinator fan-out paths to all four shards
- deterministic insert, get, update, delete, and search
- restart persistence and RocksDB/WAL recovery
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
- Linux ARM64 cluster artifact and automation qualification
- macOS 26 ARM64 regression qualification
- mixed-client compatibility and artifact provenance checks

Exit criterion: only passing combinations are described as supported.

### Phase 5 — production hardening

- RF2 replication and automatic shard failover
- persisted shard placement and safe rebalancing
- coordinator bearer/workspace propagation
- TLS or mTLS on external data-plane traffic
- coordinator authentication and high availability
- distributed TextSearch
- backup/restore drills and deletion propagation
- node and availability-zone failure drills
- monitoring, alerting, log retention, and SLO dashboards
- Ansible Vault or an external secret manager

Exit criterion: the private lab exception (`auth.mode=disabled` inside
WireGuard) is removed before production exposure.
