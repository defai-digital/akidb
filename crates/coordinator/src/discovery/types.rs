//! Discovery-related types.

use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Peer information discovered via mDNS
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// libp2p PeerId as string
    pub peer_id: String,
    /// Multiaddresses where the peer can be reached
    pub addresses: Vec<String>,
    /// Last time we saw this peer
    pub last_seen: Instant,
    /// Node type (coordinator or shard)
    pub node_type: NodeType,
}

/// Type of node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Coordinator node
    Coordinator,
    /// Shard/storage node
    Shard,
}

/// Events emitted by the discovery system
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// A new peer was discovered
    PeerDiscovered {
        peer_id: String,
        address: String,
        node_type: NodeType,
    },
    /// A peer expired (no longer responding)
    PeerExpired { peer_id: String },
    /// Cluster state update received via gossip
    ClusterStateReceived { state: ClusterStateMessage },
    /// Metrics update received via gossip
    MetricsReceived { metrics: MetricsMessage },
    /// Leader changed
    LeaderChanged {
        old_leader: Option<String>,
        new_leader: String,
    },
}

/// Events from gossip message handling
#[derive(Debug, Clone)]
pub enum GossipEvent {
    /// Cluster state update
    ClusterState(ClusterStateMessage),
    /// Metrics update
    Metrics(MetricsMessage),
}

/// Cluster state message sent via gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateMessage {
    /// PeerId of the sender
    pub sender: String,
    /// Unix timestamp in seconds
    pub timestamp: u64,
    /// Coordinator announcements
    pub coordinators: Vec<CoordinatorAnnouncement>,
    /// Shard announcements
    pub shards: Vec<ShardAnnouncement>,
    /// Current leader PeerId (if known)
    pub leader_id: Option<String>,
}

/// Coordinator announcement in gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorAnnouncement {
    /// libp2p PeerId
    pub peer_id: String,
    /// gRPC address (host:port)
    pub address: String,
    /// Whether this coordinator thinks it's the leader
    pub is_leader: bool,
    /// Health status
    pub healthy: bool,
}

/// Shard announcement in gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardAnnouncement {
    /// Shard identifier
    pub id: String,
    /// gRPC address (host:port)
    pub address: String,
    /// Number of vectors stored
    pub vector_count: u64,
    /// Health score (0.0 - 1.0)
    pub health: f32,
}

/// Metrics message sent via gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsMessage {
    /// PeerId of the sender
    pub sender: String,
    /// Unix timestamp in seconds
    pub timestamp: u64,
    /// QPS
    pub qps: f64,
    /// P50 latency in milliseconds
    pub p50_latency_ms: f64,
    /// P95 latency in milliseconds
    pub p95_latency_ms: f64,
    /// P99 latency in milliseconds
    pub p99_latency_ms: f64,
    /// Coverage percentage (0.0 - 1.0)
    pub coverage: f32,
}

/// Cluster state maintained by each coordinator
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    /// Known coordinators
    pub coordinators: Vec<CoordinatorAnnouncement>,
    /// Known shards
    pub shards: Vec<ShardAnnouncement>,
    /// Current leader PeerId
    pub leader_id: Option<String>,
    /// Last update timestamp
    pub last_update: Option<Instant>,
}

impl ClusterState {
    /// Check if a given peer is the current leader
    pub fn is_leader(&self, peer_id: &str) -> bool {
        self.leader_id
            .as_ref()
            .map(|l| l == peer_id)
            .unwrap_or(false)
    }

    /// Get coordinator count
    pub fn coordinator_count(&self) -> usize {
        self.coordinators.len()
    }

    /// Get shard count
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Merge another state into this one (CRDT-style)
    pub fn merge(&mut self, other: &ClusterStateMessage) {
        // Merge coordinators (add new ones, update existing)
        for coord in &other.coordinators {
            if let Some(existing) = self
                .coordinators
                .iter_mut()
                .find(|c| c.peer_id == coord.peer_id)
            {
                // Update existing
                existing.address = coord.address.clone();
                existing.is_leader = coord.is_leader;
                existing.healthy = coord.healthy;
            } else {
                // Add new
                self.coordinators.push(coord.clone());
            }
        }

        // Merge shards
        for shard in &other.shards {
            if let Some(existing) = self.shards.iter_mut().find(|s| s.id == shard.id) {
                // Update existing
                existing.address = shard.address.clone();
                existing.vector_count = shard.vector_count;
                existing.health = shard.health;
            } else {
                // Add new
                self.shards.push(shard.clone());
            }
        }

        // Update leader if provided
        if other.leader_id.is_some() {
            self.leader_id = other.leader_id.clone();
        }

        self.last_update = Some(Instant::now());
    }
}
