# Coordinator entrypoint HA (independent-shard profile)

This runbook covers **active-active coordinator** high availability for the
independent-shard lab/capacity path. It does **not** make shards into
replicas. For agent-facing data HA, use the knowledge-serving cell.

## What HA means here

| Failure | Expected behavior |
| --- | --- |
| One coordinator process dies | Remaining coordinators keep serving; LB stops routing to the dead peer |
| One coordinator host dies | Same, if at least one other coordinator is healthy on another host |
| One **shard** host dies | That partition is unavailable; searches may return partial coverage |
| All coordinators die | Cluster entrypoint is down until a coordinator restarts |

## Topology (recommended)

```text
        clients / SDKs
              │
       VIP or L4 LB (private)
         /            \
   coord-A          coord-B     (active-active, same ordered --shards)
      │                │
      └── fan-out to shard-1..N ──┘
```

Co-locate coordinators on shard hosts for small cells (for example nodes 1
and 2), or run them on dedicated hosts if you want to avoid coupling entrypoint
loss with a data partition.

## Configuration

### Binary / systemd

Each coordinator process gets:

- identical ordered `--shards` / `AKIDB_SHARD_ADDRS`
- `--peers` listing the **other** coordinator advertise addresses
- `--coord-role auto` (default) or explicit `primary` / `secondary`
- `--advertise` set to the service-plane address clients use

Ansible renders these from `groups['akidb_coordinators']` in
`roles/coordinator/templates/akidb-coordinator.service.j2`.

Side-effect leadership (future compaction ownership) uses:

- `auto`: lexicographically smallest advertise address among self+peers
- `primary` / `secondary`: explicit override via `akidb_coord_role`

Search, insert, delete, and update fan-out on **every** healthy coordinator.

### Client entrypoint

Point clients at a **single** VIP or DNS name that load-balances healthy
coordinators. Health check:

```bash
akidb health --server 10.1.0.11:50050 --require-ready
akidb health --server 10.1.0.12:50050 --require-ready
```

Do not hardcode a single coordinator host IP in production clients.

### Soft-state limits

- **Read-your-writes** (`Get` sticky routing after a recent write) is local to
  one coordinator process. Sticky LB sessions reduce cross-instance RYW misses;
  search always fans out to shards.
- **Backpressure** counters are per process.
- Compaction scheduler is not wired in the binary today; when enabled, only
  the side-effect leader should run it.

## Inventory examples

- `deploy/ansible/inventories/example/hosts.yml` — N=4 dual coordinator
- `deploy/ansible/inventories/example/hosts.dual.yml` — N=2 dual coordinator
- `deploy/ansible/inventories/example/hosts.ha-private.yml` — private backplane HA

## Failure drills

1. **Process kill:** `systemctl stop akidb-coordinator` on coord-A; traffic
   through the VIP must continue via coord-B within LB health timeout.
2. **Host isolation:** stop or firewall coord-A; confirm coord-B `health
   --require-ready` stays green.
3. **Shard loss:** stop one shard; coordinator remains up and reports partial
   coverage (product policy decides fail vs degrade).
4. **Rolling upgrade:** deploy shards serially, then each coordinator serially
   (`deploy.yml` already uses `serial: 1` on both groups).

## Alerts (suggested)

- Fewer than 1 healthy coordinator endpoint
- Coordinator gRPC not ready while shards are ready
- Sustained partial coverage below SLO
- Peer coordinator port unreachable from another coordinator host

## Explicit non-goals

- Automatic shard rebalancing or replica promotion
- Shared RYW consistency across coordinators
- Public bind of coordinator ports without a trusted service plane
- Auth bearer propagation to shards (still a production blocker; keep
  `auth.mode=disabled` on the isolated overlay only)
