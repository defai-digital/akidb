# Canonical backup restore verification

Run this drill on an isolated controller with Docker, Python 3.12+, and a local
AkiDB CLI built for that controller. It does not require Docker or an AkiDB binary
on the production dependency host. It never accepts a production database URL or
S3 endpoint. Containers have a dedicated bridge network, random loopback ports,
and fresh credentials; this is not an outbound-network security sandbox.

```bash
python3.12 -m venv /absolute/restore-venv
/absolute/restore-venv/bin/pip install -e ./sdks/python boto3 zstandard
cargo build -p akidb-cli --features akidb-server/generation-postgres
/absolute/restore-venv/bin/python scripts/verify_knowledge_restore.py \
  --archive /absolute/backup.tar.gz \
  --sha256 <recorded-archive-sha256> \
  --server-bin /absolute/target/debug/akidb \
  --output /absolute/new/restore-receipt.json
```

The verifier refuses to overwrite an existing receipt. It checks the archive
checksum before extraction, rejects links and traversal paths, and caps extracted
bytes at 100 GiB. It restores the dump into a fresh PostgreSQL container and
validates every bundle and mutation payload referenced by the backed-up stream's
active and publication generations through their required sequences. Unrelated
extra objects are allowed. Missing objects, sequence gaps, changed manifests, or
size/digest mismatches fail the drill. Backup `s3 sync` does not preserve object
versions: version-specific references are rejected, never silently substituted.

Uncompressed and zstd logical bundles are supported. The independent record
oracle is bounded to 2 GiB decoded input per bundle and holds records in memory;
use an appropriately sized controller. This is a correctness drill, not a bulk
backup benchmark. A publication concurrent with backup or garbage collection can
produce an incomplete archive; closure validation rejects it rather than claiming
recoverability from object counts.

Verified objects are uploaded to a new authenticated SeaweedFS instance. A blank
replica uses a restricted role in the restored database. The verifier waits for
the expected generation, manifest digest and sequence, checks all live records
and retained deletion IDs, and probes up to ten records with vector and lexical
retrieval. Citation fields are checked against canonical records. ANN top-1
identity is not asserted because approximate search and tied vectors need not be
deterministic. Finally both dependencies are stopped and local reads are repeated.

This proves one fixture or supplied backup can restore into one replica on the
controller's platform. It does not qualify SeaweedFS HA, full-cell failover, a
recovery-time objective, or arbitrary data sizes. The final receipt includes the
archive/binary SHA256, generation identities, object and record verification
counts, retrieval probe count, and dependency-outage read result.

`deploy/ansible/playbooks/knowledge-restore-verify.yml` invokes this same verifier
on the controller before its existing audit-record step. Set `AKIDB_QA_PYTHON`,
`AKIDB_RESTORE_SERVER_BIN`, and a new `AKIDB_KNOWLEDGE_RESTORE_EVIDENCE` path in
addition to the backup ID, SHA256, and `AKIDB_KNOWLEDGE_BACKUP_DIR` containing
the archive saved by the backup playbook. The controller must contain this checkout
and its Python SDK. The real-service CI fixture uses:

```bash
/absolute/restore-venv/bin/python scripts/test_knowledge_restore.py \
  --server-bin /absolute/target/debug/akidb \
  --output /absolute/new/fixture-restore.json
```

## Generation fetch and catch-up behavior

S3 HEAD and GET requests have a 30-second overall request budget; the standard
client allows at most three SDK attempts, each capped at ten seconds. Body reads
have a 30-second idle budget. The total fetch budget is 120 seconds plus one
second per declared MiB (the size allowance is capped at 24 hours). Limits cover
successful headers followed by a stalled body. Temporary files are owned before awaiting body I/O and removed on ordinary
failure, timeout and async cancellation. SIGKILL cannot execute destructors;
operators must account for orphan partial files after process or host crashes.

Remote status codes are retained without signed URLs or SDK diagnostic strings.
The worker uses jittered exponential delay after failed reconciliation, capped at
30 seconds (or the configured poll interval if longer). A PostgreSQL connection
alone does not reset failure backoff. Heartbeats use a separate connection so slow
fetches and retry delays do not starve routing freshness. A failed heartbeat
drains the current reconciliation before reconnecting, avoiding overlapping
blocking builds. PostgreSQL connections use keepalive and bounded connect/query
timeouts; heartbeats are freshness reports, not exclusive ownership leases. Active local reads keep
their previous complete revision.

Catch-up verifies the full ordered contract history and both local identity
markers for already-applied mutations, but downloads payloads only for the
unapplied suffix. Revision publication remains atomic; there is no page-by-page
visibility. Historical object bytes are checked during blank rebuild and this
restore drill, rather than downloaded again on every catch-up.

A single tail is limited to 100,000 contracts, 64 MiB of serialized contracts,
64 MiB per payload, and 256 MiB of declared unapplied payload bytes. These bound
input accumulation, not exact allocator RSS. If a tail exceeds a limit, publish a
new base generation; the worker fails closed and does not expose a partial tail.
No throughput or query-latency improvement is claimed without matched measurement.
