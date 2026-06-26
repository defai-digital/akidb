# PRD-001: AkiDB Coordinator TUI Dashboard and Auto-Discovery

**Version:** 1.0
**Status:** Draft
**Author:** AkiDB Team
**Date:** 2026-01-22
**Reviewers:** Engineering Team

---

## 1. Executive Summary

This PRD defines the requirements for enhancing AkiDB's distributed coordinator with:
1. A Terminal User Interface (TUI) dashboard for real-time cluster monitoring
2. An auto-discovery mechanism for zero-configuration coordinator clustering

These features will improve operational visibility and reduce manual configuration overhead for edge deployments on NVIDIA Jetson Thor clusters.

---

## 2. Problem Statement

### Current Pain Points

1. **No Cluster Visibility**: Operators have no real-time view of cluster health, shard status, or query performance metrics on Thor terminals.

2. **Manual Shard Configuration**: Coordinators require explicit `--shards` flags with IP addresses, making deployments brittle and requiring manual updates when nodes change.

3. **No Coordinator Redundancy**: Single coordinator is a SPOF; no mechanism for coordinators to discover each other or elect a leader.

4. **Edge Operation Challenges**: Jetson Thor edge clusters need zero-configuration setups that work reliably in isolated LAN environments.

### Impact

- Increased operational overhead for cluster management
- Slower incident response due to lack of visibility
- Deployment friction when scaling or recovering nodes
- Risk of downtime from coordinator SPOF

---

## 3. Goals and Non-Goals

### Goals

| ID | Goal | Success Metric |
|----|------|----------------|
| G1 | Real-time cluster visibility via TUI | Operators can view cluster state within 500ms refresh |
| G2 | Zero-configuration coordinator discovery | New coordinators join cluster without manual config |
| G3 | Automatic shard discovery | Shards register with coordinators automatically |
| G4 | Leader election for coordinator HA | Cluster continues operating if leader fails |
| G5 | Secure cluster membership | Only authorized nodes can join cluster |

### Non-Goals

| ID | Non-Goal | Rationale |
|----|----------|-----------|
| NG1 | Web-based dashboard | TUI is sufficient for edge terminals; web UI can be added later |
| NG2 | Cross-WAN discovery | Focus on LAN edge clusters; WAN requires different approach |
| NG3 | Full Raft consensus initially | Adds complexity; simple election sufficient for Phase 1-2 |
| NG4 | Data replication between shards | Out of scope; existing MinIO snapshots handle durability |

---

## 4. User Stories

### Operator Stories

| ID | As a... | I want to... | So that... |
|----|---------|--------------|------------|
| US1 | Cluster operator | See all coordinators and shards in a terminal dashboard | I can monitor cluster health without external tools |
| US2 | Cluster operator | See real-time QPS, latency percentiles, and coverage | I can identify performance issues quickly |
| US3 | Cluster operator | See GPU utilization on each Thor node | I can optimize workload distribution |
| US4 | Cluster operator | Evict a failed node from the dashboard | I can recover from failures without SSH |
| US5 | DevOps engineer | Deploy a new coordinator without config changes | New nodes auto-join the cluster |
| US6 | DevOps engineer | Deploy a new shard without coordinator restart | Shards are discovered automatically |
| US7 | Security admin | Restrict cluster membership to authorized nodes | Rogue nodes cannot join the cluster |

### Developer Stories

| ID | As a... | I want to... | So that... |
|----|---------|--------------|------------|
| DS1 | Developer | Run TUI in standalone mode for testing | I can develop without full cluster |
| DS2 | Developer | Configure discovery via config file | I don't need to change code for different environments |

---

## 5. Functional Requirements

### 5.1 TUI Dashboard

| ID | Requirement | Priority | Description |
|----|-------------|----------|-------------|
| TUI-01 | Topology View | P0 | Display tree of coordinators and shards with status indicators |
| TUI-02 | Metrics Panel | P0 | Show QPS, P50/P95/P99 latency, coverage percentage |
| TUI-03 | Health Sparklines | P0 | Real-time health trend visualization for each node |
| TUI-04 | GPU Metrics | P1 | Integration with nvtop for GPU memory/temp on Jetson |
| TUI-05 | Refresh Rate | P0 | Configurable refresh interval (default 500ms) |
| TUI-06 | Keyboard Navigation | P0 | Navigate between panels using keyboard |
| TUI-07 | Node Eviction | P1 | Operator can evict node with confirmation |
| TUI-08 | Force Leader | P2 | Operator can force leader election |
| TUI-09 | Theme Support | P2 | Default, minimal, and high-contrast themes |
| TUI-10 | Backpressure Indicator | P0 | Show when backpressure is being applied |

### 5.2 Auto-Discovery

| ID | Requirement | Priority | Description |
|----|-------------|----------|-------------|
| DIS-01 | mDNS Discovery | P0 | Coordinators discover each other via mDNS on LAN |
| DIS-02 | Namespace Isolation | P0 | Clusters isolated by configurable namespace |
| DIS-03 | Cluster Secret | P0 | Pre-shared key required for cluster membership |
| DIS-04 | Shard Registration | P0 | Shards announce themselves to coordinators |
| DIS-05 | Gossip Protocol | P0 | Cluster state disseminated via gossipsub |
| DIS-06 | Bootstrap Mode | P0 | First node starts in bootstrap mode |
| DIS-07 | CLI Fallback | P0 | --shards flag overrides discovery |
| DIS-08 | Announce Interval | P1 | Configurable announcement interval (default 2.5s) |
| DIS-09 | Heartbeat | P0 | Nodes send heartbeats every 1s |
| DIS-10 | Failure Detection | P0 | Node marked unhealthy after 3 missed heartbeats |

### 5.3 Leader Election

| ID | Requirement | Priority | Description |
|----|-------------|----------|-------------|
| LE-01 | Deterministic Election | P0 | Leader is lowest PeerID among quorum-visible nodes |
| LE-02 | Quorum Requirement | P0 | Leader requires N/2+1 nodes to acknowledge |
| LE-03 | Leader Lease | P1 | Leader must renew lease every 5s |
| LE-04 | Graceful Stepdown | P1 | Leader steps down if loses quorum |
| LE-05 | Election Notification | P0 | All nodes notified of leader changes via gossip |

### 5.4 State Management

| ID | Requirement | Priority | Description |
|----|-------------|----------|-------------|
| SM-01 | CRDT State | P0 | Cluster state stored as CRDTs for consistency |
| SM-02 | Membership G-Set | P0 | Grow-only set for shard/coordinator membership |
| SM-03 | Health LWW-Register | P0 | Last-write-wins register for health status |
| SM-04 | Metrics Counters | P1 | CRDT counters for aggregated metrics |
| SM-05 | State Persistence | P1 | Snapshot state to MinIO periodically |

---

## 6. Non-Functional Requirements

| ID | Category | Requirement | Target |
|----|----------|-------------|--------|
| NFR-01 | Performance | TUI refresh latency | < 100ms |
| NFR-02 | Performance | Discovery time for new node | < 5s |
| NFR-03 | Performance | Leader election time | < 3s |
| NFR-04 | Reliability | Cluster survives single coordinator failure | 100% |
| NFR-05 | Reliability | Query routing continues during election | Yes |
| NFR-06 | Security | All coordinator communication encrypted | TLS/Noise |
| NFR-07 | Security | Cluster membership requires secret | Yes |
| NFR-08 | Resource | TUI memory overhead | < 10MB |
| NFR-09 | Resource | Discovery CPU overhead | < 1% |
| NFR-10 | Compatibility | Works on Jetson Thor (ARM64) | Yes |

---

## 7. Configuration Schema

```toml
[coordinator]
# Coordinator operating mode
# - "auto": Join existing cluster or become leader
# - "bootstrap": Start as initial cluster leader
# - "standalone": No clustering, single coordinator
mode = "auto"

# Consistency vs availability tradeoff
# - "availability": Allow partial results during partitions
# - "consistency": Require quorum for all operations
consistency_bias = "availability"

[discovery]
# Enable auto-discovery
enabled = true

# Discovery method: "libp2p" (recommended) or "static"
method = "libp2p"

# Cluster namespace for isolation (like EXO_LIBP2P_NAMESPACE)
namespace = "akidb-prod"

# Pre-shared key for cluster membership (base64-encoded 32 bytes)
# Generate with: openssl rand -base64 32
cluster_secret = ""

# How often to announce presence (milliseconds)
announce_interval_ms = 2500

# Heartbeat interval (milliseconds)
heartbeat_interval_ms = 1000

# Missed heartbeats before marking node unhealthy
missed_heartbeats_threshold = 3

[discovery.fallback]
# DNS seeds for environments where mDNS doesn't work
dns_seeds = []

# Static shard list (CLI --shards takes precedence)
static_shards = []

[tui]
# Enable TUI dashboard
enabled = true

# Refresh interval (milliseconds)
refresh_interval_ms = 500

# Show GPU metrics via nvtop integration
show_gpu_metrics = true

# Color theme: "default", "minimal", "high-contrast"
theme = "default"

[tui.controls]
# Allow node eviction from TUI
allow_eviction = true

# Require quorum confirmation for destructive actions
require_quorum_confirmation = true

[tui.layout]
# Show topology panel
show_topology = true

# Show metrics panel
show_metrics = true

# Show health sparklines
show_health = true
```

---

## 8. Dependencies

### External Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| rust-libp2p | 0.54+ | mDNS discovery, gossipsub, Noise encryption |
| ratatui | 0.28+ | TUI framework |
| crossterm | 0.28+ | Terminal backend |
| crdts | 7.0+ | CRDT implementations |
| nvtop (optional) | any | GPU metrics on Jetson |

### Internal Dependencies

| Crate | Relationship |
|-------|--------------|
| akidb-common | Types, errors |
| akidb-grpc | gRPC service definitions |
| akidb-storage | MinIO snapshot persistence |

---

## 9. Milestones

| Phase | Milestone | Target | Deliverables |
|-------|-----------|--------|--------------|
| 1 | TUI Dashboard | Week 2 | akidb-tui crate, basic dashboard |
| 2 | Auto-Discovery | Week 4 | libp2p integration, mDNS discovery |
| 3 | Leader Election | Week 5 | Deterministic election, state replication |
| 4 | Production Ready | Week 6 | Security hardening, documentation |

---

## 10. Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| libp2p complexity | Schedule delay | Medium | Use well-documented examples; limit to mDNS+gossip |
| mDNS blocked on some networks | Feature unavailable | Low | Provide DNS-SD fallback and static config |
| Split-brain during network partition | Data inconsistency | Medium | Quorum requirement; availability bias default |
| TUI performance on slow terminals | Poor UX | Low | Configurable refresh rate; minimal theme |

---

## 11. Open Questions

| ID | Question | Status | Answer |
|----|----------|--------|--------|
| Q1 | Should we support web dashboard in addition to TUI? | Deferred | Out of scope for v1; can add later |
| Q2 | What's the maximum cluster size? | Answered | Design for 3-10 coordinators; tested with 5 |
| Q3 | Should Raft be included in initial release? | Answered | No; Phase 3 only if needed |

---

## 12. Appendix

### A. Reference Implementations

- **exo-explore/exo**: UDP broadcast discovery, web dashboard
- **etcd**: Raft consensus, peer discovery
- **Consul**: Service mesh, DNS-SD discovery

### B. Glossary

| Term | Definition |
|------|------------|
| Coordinator | Stateless query router that fans out to shards |
| Shard | AkiDB server instance holding vector data |
| Leader | Coordinator responsible for membership changes |
| Quorum | Majority (N/2+1) of coordinators |
| CRDT | Conflict-free Replicated Data Type |
| PeerID | Unique cryptographic identifier from libp2p |
