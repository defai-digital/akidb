# ADR-001: Coordinator Discovery and TUI Architecture

**Status:** Accepted
**Date:** 2026-01-22
**Decision Makers:** AkiDB Engineering Team
**Consulted:** Claude, Gemini, Grok (via ax_discuss)

---

## Context

AkiDB Thor Edition requires enhanced operational capabilities for edge deployments on NVIDIA Jetson Thor clusters. The current coordinator has limitations:

1. No visual monitoring interface for terminal-based edge operations
2. Static shard configuration via CLI flags
3. Single coordinator with no redundancy or discovery
4. No cluster state replication between coordinators

We need to decide on:
- TUI framework selection
- Discovery protocol and implementation
- Leader election mechanism
- State replication approach
- Security model for cluster membership

---

## Decision Drivers

- **Edge-first design**: Must work reliably on isolated LANs without external dependencies
- **Simplicity**: Avoid over-engineering; match existing codebase philosophy
- **Performance**: Sub-50ms query latency SLO must be maintained
- **Security**: Zero-trust edge environment; nodes may be physically accessible
- **Resource efficiency**: Minimal CPU/memory overhead on Jetson Thor (40-130W envelope)

---

## Decisions

### Decision 1: TUI Framework - ratatui

**Status:** Accepted

**Context:** Need a Rust-native TUI framework for terminal dashboard on Jetson Thor.

**Options Considered:**

| Option | Pros | Cons |
|--------|------|------|
| **ratatui** | Active maintenance, async/Tokio compatible, rich widgets | Learning curve |
| tui-rs | Familiar API | Deprecated, no longer maintained |
| cursive | Simpler API | Less flexible, sync-only |
| crossterm-only | Minimal dependencies | No high-level widgets |

**Decision:** Use **ratatui** with crossterm backend.

**Rationale:**
- tui-rs is deprecated; community migrated to ratatui
- Native async support integrates with existing Tokio/gRPC stack
- Built-in widgets (sparklines, gauges, tables) reduce implementation effort
- Active community with regular releases

**Consequences:**
- (+) Rich visualization capabilities out of the box
- (+) Good documentation and examples
- (-) Larger dependency footprint than minimal approach
- (-) Team needs to learn ratatui API

---

### Decision 2: Discovery Protocol - rust-libp2p

**Status:** Accepted

**Context:** Need zero-configuration discovery for coordinators and shards on LAN.

**Options Considered:**

| Option | Pros | Cons |
|--------|------|------|
| **rust-libp2p** | Comprehensive (mDNS, gossip, security), battle-tested | Complexity, larger binary |
| Raw UDP broadcast | Simple, low overhead | No security, no standard |
| mDNS only (mdns-sd) | Standardized, simple | Need separate gossip impl |
| DNS-SD | Works across VLANs | Requires DNS infrastructure |
| etcd/Consul | Proven at scale | External dependency |

**Decision:** Use **rust-libp2p** with mDNS + gossipsub behaviors.

**Rationale:**
- This is what exo-explore/exo actually uses (not raw UDP)
- Provides mDNS discovery, gossip protocol, and Noise encryption in one package
- Pre-shared key support for cluster isolation
- Well-maintained Rust implementation
- Avoids reinventing discovery/gossip protocols

**Consequences:**
- (+) Cohesive networking stack with security built-in
- (+) Namespace isolation via libp2p mechanisms
- (+) Transport encryption without additional code
- (-) Larger binary size (~2-3MB)
- (-) More complex debugging when issues arise

---

### Decision 3: Leader Election - Deterministic with Quorum

**Status:** Accepted

**Context:** Need leader election for coordinator HA without full Raft complexity.

**Options Considered:**

| Option | Pros | Cons |
|--------|------|------|
| **Deterministic (lowest PeerID)** | Simple, predictable, no additional protocol | Less flexible |
| Raft consensus | Industry standard, linearizable | Complex, operational overhead |
| Bully algorithm | Simple | Race conditions possible |
| ZAB (Zookeeper) | Proven | External dependency |

**Decision:** Use **deterministic leader election** based on lowest PeerID among quorum-visible nodes. Reserve Raft for Phase 3 if needed.

**Rationale:**
- Matches codebase philosophy: "Cost-effective edge design"
- Sufficient for 3-10 coordinator clusters
- No additional protocol complexity
- Leader determined by cryptographic PeerID (stable, unique)
- Quorum requirement prevents split-brain

**Algorithm:**
```
leader = min(peer_id for peer in peers if peer.visible_to_quorum)
```

**Consequences:**
- (+) Simple implementation and debugging
- (+) Deterministic behavior (all nodes compute same leader)
- (+) Fast election (no rounds of voting)
- (-) Leader always same node if stable (no load balancing)
- (-) May need Raft later for strong consistency use cases

---

### Decision 4: State Replication - CRDTs via Gossip

**Status:** Accepted

**Context:** Coordinators need consistent view of cluster topology for query routing.

**Options Considered:**

| Option | Pros | Cons |
|--------|------|------|
| **CRDTs via gossipsub** | Partition-tolerant, no leader bottleneck | Eventual consistency |
| Raft log replication | Strong consistency | Leader bottleneck, complexity |
| Central database | Simple query | SPOF, external dependency |
| No replication | Simplest | Inconsistent views |

**Decision:** Use **CRDTs** (Conflict-free Replicated Data Types) disseminated via libp2p gossipsub.

**State Model:**
- `G-Set` (Grow-only Set): Coordinator and shard membership
- `LWW-Register` (Last-Write-Wins): Node health status
- `G-Counter`: Aggregated metrics (QPS, latency sums)

**Rationale:**
- CRDTs guarantee eventual consistency without coordination
- Gossip distributes state to all nodes without leader bottleneck
- Partition-tolerant: both sides of partition maintain local view
- Low overhead: only state changes propagated
- Matches existing "partial results with coverage metrics" pattern

**Consequences:**
- (+) High availability during partitions
- (+) No leader bottleneck for state reads
- (+) Natural fit with gossipsub
- (-) Eventual consistency (stale reads possible)
- (-) G-Set membership requires separate eviction mechanism

---

### Decision 5: Security Model - Pre-Shared Key + Noise

**Status:** Accepted

**Context:** Edge clusters may be physically accessible; need to prevent rogue nodes.

**Options Considered:**

| Option | Pros | Cons |
|--------|------|------|
| **PSK + Noise** | Simple key distribution, strong encryption | Key rotation manual |
| mTLS with CA | Standard, revocation possible | PKI infrastructure needed |
| No security | Simplest | Unacceptable for production |
| Token-based | Flexible | Need token distribution |

**Decision:** Use **pre-shared key (pnet)** for cluster membership and **Noise protocol** for transport encryption.

**Implementation:**
- `cluster_secret` in config (base64-encoded 32-byte key)
- libp2p `pnet` behavior creates private network overlay
- Noise protocol encrypts all coordinator-to-coordinator traffic
- Shards authenticate via same cluster_secret in registration

**Rationale:**
- Simple operational model: one secret per cluster
- libp2p provides pnet and Noise out of the box
- No PKI infrastructure needed for edge deployments
- Sufficient security for isolated LAN clusters

**Consequences:**
- (+) Simple deployment: just configure cluster_secret
- (+) Strong encryption (Noise = modern, audited protocol)
- (-) Key rotation requires cluster restart
- (-) No per-node revocation (need new key if node compromised)

---

### Decision 6: CLI Fallback - Always Authoritative

**Status:** Accepted

**Context:** Auto-discovery may fail; need reliable fallback.

**Decision:** CLI flags (`--shards`, `--coordinators`) always override discovery.

**Rationale:**
- Graceful degradation when discovery fails
- Debugging aid: can test with explicit configuration
- Migration path: existing deployments continue working
- Matches exo approach (can specify peers explicitly)

**Consequences:**
- (+) Reliable fallback for any environment
- (+) Easy debugging and testing
- (-) Two configuration paths to maintain

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                          akidb-tui                               │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐  │
│  │ Topology    │ │ Metrics     │ │ Health      │ │ Controls  │  │
│  │ (tree view) │ │ (gauges)    │ │ (sparklines)│ │ (actions) │  │
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                    subscribes to cluster state
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      akidb-coordinator                           │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Discovery Module                          ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  ││
│  │  │ libp2p      │  │ mDNS        │  │ Gossipsub           │  ││
│  │  │ (transport) │  │ (discovery) │  │ (state propagation) │  ││
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Election Module                           ││
│  │  • Deterministic: lowest PeerID in quorum                   ││
│  │  • Leader lease renewal every 5s                            ││
│  │  • Stepdown on quorum loss                                  ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    State Module (CRDTs)                      ││
│  │  • G-Set: membership                                        ││
│  │  • LWW-Register: health                                     ││
│  │  • G-Counter: metrics                                       ││
│  │  • Persistence: MinIO snapshots                             ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Fanout Module (existing)                  ││
│  │  • Query routing to shards                                  ││
│  │  • Result merging (min-heap)                                ││
│  │  • Partial results with coverage                            ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                         gRPC (50051)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        akidb-server (shards)                     │
│  • GPU-accelerated FAISS                                         │
│  • Registers with coordinator via discovery                      │
│  • Heartbeat every 1s                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure

```
crates/
├── akidb-tui/                    # NEW: TUI dashboard
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── app.rs                # Application state
│       ├── ui/
│       │   ├── mod.rs
│       │   ├── topology.rs       # Coordinator/shard tree
│       │   ├── metrics.rs        # QPS, latency gauges
│       │   ├── health.rs         # Sparklines
│       │   └── controls.rs       # Operator actions
│       └── events.rs             # Input handling
│
├── akidb-coordinator/            # ENHANCED: Add discovery
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── discovery/            # NEW
│       │   ├── mod.rs
│       │   ├── libp2p.rs         # libp2p network setup
│       │   ├── mdns.rs           # mDNS behavior
│       │   └── gossip.rs         # Gossipsub state
│       ├── election/             # NEW
│       │   ├── mod.rs
│       │   └── deterministic.rs  # Lowest PeerID election
│       ├── state/                # NEW
│       │   ├── mod.rs
│       │   ├── crdt.rs           # G-Set, LWW-Register
│       │   └── persistence.rs    # MinIO snapshots
│       └── fanout/               # EXISTING
│           └── ...
```

---

## Related Decisions

- **ADR-002** (future): Raft consensus if deterministic election proves insufficient
- **ADR-003** (future): Web dashboard in addition to TUI

---

## References

- [exo-explore/exo](https://github.com/exo-explore/exo) - Distributed AI inference with libp2p
- [rust-libp2p](https://github.com/libp2p/rust-libp2p) - P2P networking library
- [ratatui](https://github.com/ratatui-org/ratatui) - TUI framework
- [CRDTs paper](https://hal.inria.fr/inria-00555588/document) - Conflict-free Replicated Data Types

---

## Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-22 | 1.0 | Initial decision record |
