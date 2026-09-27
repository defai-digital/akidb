# knowledge_dependencies

Lab-only dependency host for the knowledge-serving cell: PostgreSQL authority
plus the SeaweedFS S3 object store that holds canonical generation artifacts.

Gated by `akidb_knowledge_manage_lab_dependencies`. Production deployments point
the replica and gateway roles at managed PostgreSQL and a durable
S3-compatible object store instead of running this role.

## What this role installs

1. PostgreSQL, its TLS materials, the knowledge-control role and database, and
   a `hostssl` overlay-only client policy.
2. The knowledge-cell CA in the host trust store for the dependency host.
3. SeaweedFS `4.47` as `/usr/local/bin/weed`, from
   `https://github.com/seaweedfs/seaweedfs/releases/download/<tag>/linux_amd64.tar.gz`.
   The archive contains exactly one file, `weed`.
4. A `seaweedfs` system user/group, `/var/lib/seaweedfs/knowledge`, and
   `/etc/seaweedfs` with a `certs/` tree.
5. `/etc/seaweedfs/s3.json` (mode `0640`, `root:seaweedfs`) rendered from
   `templates/s3.json.j2`.
6. The hardened `seaweedfs-knowledge.service` unit.
7. The AWS CLI v2 bundle (checksum-pinned) through the shared
   `tasks/s3_client.yml`, also used by `knowledge-backup.yml`.
8. The immutable-generation bucket, created through `weed shell`.
9. A bounded S3 write probe against that bucket with the publish identity.

## Artifact verification

Upstream publishes only a `.md5` sidecar for release assets — no SHA256, no
GPG/cosign for tarballs. `AKIDB_KNOWLEDGE_SEAWEEDFS_SHA256` is therefore a
mandatory operator input and is passed to `ansible.builtin.get_url` as
`checksum: sha256:…` before anything is extracted. Do not weaken this to the
upstream `.md5`, and do not drop the check.

`AKIDB_KNOWLEDGE_SEAWEEDFS_TAG` defaults to `4.47` and
`AKIDB_KNOWLEDGE_SEAWEEDFS_URL` is derived from it; both are overridable.

## Listener configuration

`weed server -filer -s3` runs master, volume, filer, and the S3 gateway in one
process:

| Component | Port | Scheme | Exposed to the service plane |
| --- | --- | --- | --- |
| S3 gateway | `akidb_knowledge_seaweedfs_port` (8333) | **HTTPS** | yes (overlay only) |
| Master (health, `weed shell`) | `akidb_knowledge_seaweedfs_master_port` (9333) | HTTP | no |
| Filer | 8888 | HTTP | no |
| Volume | 8080 | HTTP | no |
| Prometheus metrics | 9320 | HTTP | no |
| S3 internal gRPC | 18333 (`10000 + s3.port`) | plain gRPC | no |

`roles/service_network` opens exactly one UFW rule for this host: 8333/TCP from
`akidb_overlay_cidr`. Everything else on this host stays closed to the network
under the default `deny incoming` policy, and the replicas only need the S3
port. Bucket setup runs locally on this host through `weed shell`, so the
master port never has to be reachable from another host.

### Why TLS-only needs no `-s3.port.https`

The unit passes `-s3.key.file` and `-s3.cert.file` and deliberately **omits**
`-s3.port.https`. Per `weed/command/s3.go`:

- with a TLS key file set and `portHttps == 0`, the gateway serves TLS on the
  listener it already bound at `-s3.port`, and the plaintext branch
  (`if *s3opt.tlsPrivateKey == "" || *s3opt.portHttps > 0`) is skipped — there
  is no plaintext S3 listener at all;
- with `portHttps > 0`, the gateway serves TLS on `portHttps` **and** starts the
  plaintext listener on `-s3.port`.

So the fail-closed configuration is the one used here: TLS on 8333, no
plaintext listener. Setting `-s3.port=0` does not help — the listener is bound
before the TLS decision, so the HTTPS port would become an ephemeral port while
the S3 gRPC port stayed pinned at 10000.

The clients reach the gateway as `https://<overlay address>:8333` with the
knowledge-cell CA, matching `[storage.seaweedfs]` in the replica, shard, and
market-benchmark templates.

## Credentials

`templates/s3.json.j2` renders three identities. Action literals are
case-sensitive (`Admin`, `Read`, `Write`, `List`, `Tagging`) and are scoped
where the S3 gateway supports it as `Action:<bucket>`:

| Identity | Actions | Used by |
| --- | --- | --- |
| `knowledge-admin` | `Admin`, `Read`, `Write`, `List`, `Tagging` | setup only (bucket creation, `-s3.autoCreateBucket`) |
| `knowledge-replica-read` | `Read:<bucket>`, `List:<bucket>` | knowledge replicas |
| `knowledge-publisher` | `Read:<bucket>`, `Write:<bucket>`, `List:<bucket>`, `Tagging:<bucket>` | AX Fabric publication |

Only the setup identity carries the bare global `Admin` action, which is what
bucket creation and `-s3.autoCreateBucket` require (`isAdmin()` is a literal
membership test on `Admin`). A bucket-scoped admin grant does not satisfy it, so
the long-lived replica and publisher credentials never receive it.

Supplying `-s3.config` at all switches authentication on: with no config file,
and no `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, the gateway permits every
operation anonymously. Never remove `-s3.config`.

`s3.json` changes are applied with a SIGHUP (`ExecReload=/bin/kill -HUP
$MAINPID`), which reloads a file-based `-s3.config` in place. The unit file, the
binary, and the TLS materials still require a restart.

## Volume sizing

The unit runs `-volume.max=0`, which SeaweedFS documents as "the limit will be
auto configured as free disk space divided by volume size"
(`weed/command/server.go`). The divisor is
`akidb_knowledge_seaweedfs_volume_size_limit_mb` (passed as
`-master.volumeSizeLimitMB`, default `1024`). SeaweedFS also assigns each bucket
its own collection, and a collection needs a writable volume.

So the usable rule is:

```text
writable_volumes = round_down(free_disk / volume_size_limit)
require writable_volumes > number_of_buckets + headroom
```

The knowledge cell needs one bucket (`knowledge-generations`); budget for a few
more so a scratch or second collection still fits:

```text
volume_size_limit_mb <= free_disk_mb / (buckets + 3)
```

With the documented admission floor (`akidb_knowledge_min_available_disk_mb`,
51200 MiB) the 1024 default leaves roughly 50 volumes, which is comfortable.
On a small disk it is not: a host with ~5 GiB free gets 5 volumes of 1024 MiB,
which is how the failure below appears. Lower
`akidb_knowledge_seaweedfs_volume_size_limit_mb` (inventory host vars beat the
playbook `group_vars` default) on any host whose free space is small, or the
host must have enough volumes for every bucket it will ever hold.

Lower it only as far as the disk needs: every volume carries its own index and
open file handle, so a very small limit leaves many tiny volumes and more
per-volume overhead. There is no universally correct value — it is a trade
between "enough volumes for every bucket" and "not thousands of them".

## Bucket creation

`weed shell` reports only the status of the last command in a piped run, so the
role pipes exactly one command per invocation.

The role lists the buckets first (`s3.bucket.list`, `changed_when: false`) and
issues `s3.bucket.create -name <bucket>` only when the bucket is absent
(`changed_when: true` on that task only). The reason is empirical: on the pinned
4.47 release, piping `s3.bucket.create -name <existing-bucket>` into
`weed shell` **exits 0 and prints `created bucket <bucket>`** even though the
bucket already existed, so a create-every-run task reported a change on every
run and was not idempotent. The task keeps its tolerance for the
`bucket <name> already exists` failure because
`weed/shell/command_s3_bucket_create.go` still returns that error when the filer
answers `ErrEntryAlreadyExists`, which a different release or filer path can
surface.

## Verification tasks

- master health: `GET http://<overlay>:9333/cluster/healthz` — 200 healthy, 503
  when there is no leader, 423 when locked. Only 200 passes.
- S3 gateway liveness: `GET https://<overlay>:8333/healthz` — 200 with an empty
  body.
- disk-type allowance: one `cluster.check` through `weed shell`.
- bucket presence: one `s3.bucket.list` through `weed shell`.
- writability: a bounded S3 write probe — `aws s3 cp` a tiny temp file to
  `s3://<bucket>/.akidb-write-probe`, then `aws s3 rm` the same key, both over
  `https://<overlay>:8333` with the publish identity (`Read`/`Write`/`List`/
  `Tagging` scoped to the bucket) and the knowledge-cell CA. Both tasks are
  `no_log: true` and report no change.

### What `cluster.check` does and does not prove

`cluster.check` is **not** a writability gate. Measured on SeaweedFS 4.47
against a live single-node cluster whose volumes were fully allocated:

```text
volume.list: Topology volumeSizeLimit:1024 MB hdd(volume:5/5 active:5 free:0 remote:0)
echo "cluster.check" | weed shell -master=...   # exit 0
```

It exited 0 and printed only benign per-hop ping lines, including
`rpc error: code = InvalidArgument desc = unknown ping target localhost:9333 of
type master`, which did not affect the exit status. The source fails only when a
disk type has no allowance at all (`len(DiskInfos) == 0`, or
`MaxVolumeCount == 0` for a disk type). Treat it as a coarse reachability and
allowance check.

Only the write probe shows that a publish will actually land. A bucket whose
volumes are exhausted answers 200 on every health endpoint and accepts no write,
so the probe is the deployment's end-to-end writability verdict.

## Troubleshooting

### `No writable volumes and no free volumes left for {"collection":"..."}`

Every bucket maps to its own collection, and a collection needs a writable
volume. When all volumes are full, writes to a *new* bucket fail with
`No writable volumes and no free volumes left for {"collection":"..."}` even
though the master reports healthy and the S3 gateway answers `/healthz`.

Fastest recognition: run `weed shell` with `volume.list` and read the header.
A fully allocated disk type prints `free:0`:

```text
Topology volumeSizeLimit:1024 MB hdd(volume:5/5 active:5 free:0 remote:0)
```

Recognize it from the same failure in the role's own run: the write probe
(`Require the publish identity to write through the S3 gateway`) fails while
`cluster.check` passed. Fix by lowering
`akidb_knowledge_seaweedfs_volume_size_limit_mb` so the host can hold more
volumes, or by freeing disk space, then rerun the role.

## Health and failure notes

- The service is `Type=simple`: the SeaweedFS process does not send
  `sd_notify` messages, so `Type=notify` would never report readiness.
- `Restart=always` with `RestartSec=5`; `ReadWritePaths=/var/lib/seaweedfs`
  covers the volume store, the master metadata, and the filer LevelDB store.
- The S3 gateway takes a couple of seconds to open its listener after the filer
  is up, so every health check retries.
- A failure between the write probe's `cp` and its `rm` leaves
  `.akidb-write-probe` in the bucket. The next role run removes it, but a
  canonical backup taken in between would include it.
