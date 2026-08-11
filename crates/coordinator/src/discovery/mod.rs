//! Static/explicit cluster addressing for the coordinator.
//!
//! **Auto-discovery is not implemented.** Operators must pass explicit
//! coordinator and shard addresses (CLI / config). The previous libp2p-backed
//! discovery implementation was removed due to vulnerable transitive
//! networking crates with no compatible upstream fix.
//!
//! Types such as [`ClusterStateMessage`] remain for offline merge helpers and
//! tests; they are not published on a live gossip bus.

mod config;
mod types;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

pub use config::{CoordinatorMode, DiscoveryConfig};
pub use types::{
    ClusterState, ClusterStateMessage, CoordinatorAnnouncement, DiscoveryEvent, GossipEvent,
    MetricsMessage, NodeType, PeerInfo, ShardAnnouncement,
};

/// Holds an in-process cluster state snapshot.
///
/// This is **not** network auto-discovery. It never scans mDNS/libp2p peers;
/// state is only what callers merge explicitly (or empty defaults).
pub struct DiscoveryService {
    cluster_state: Arc<RwLock<ClusterState>>,
}

impl DiscoveryService {
    /// Create a static discovery holder (explicit configuration only).
    ///
    /// The `config` and `grpc_address` arguments are accepted for API
    /// compatibility; they are not used to advertise or discover peers.
    pub async fn new(_config: DiscoveryConfig, _grpc_address: String) -> Result<Self> {
        tracing::warn!(
            "DiscoveryService is static-only: network auto-discovery is disabled; use explicit coordinator and shard addresses"
        );
        Ok(Self {
            cluster_state: Arc::new(RwLock::new(ClusterState::default())),
        })
    }

    /// Get the current (in-process) cluster state.
    pub fn cluster_state(&self) -> Arc<RwLock<ClusterState>> {
        self.cluster_state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_state_merge() {
        let mut state = ClusterState::default();

        let msg = ClusterStateMessage {
            sender: "peer-1".to_string(),
            timestamp: 12345,
            coordinators: vec![CoordinatorAnnouncement {
                peer_id: "peer-1".to_string(),
                address: "127.0.0.1:50050".to_string(),
                is_leader: true,
                healthy: true,
            }],
            shards: vec![ShardAnnouncement {
                id: "shard-1".to_string(),
                address: "127.0.0.1:50051".to_string(),
                vector_count: 100,
                health: 0.95,
            }],
            leader_id: Some("peer-1".to_string()),
        };

        state.merge(&msg);

        assert_eq!(state.coordinator_count(), 1);
        assert_eq!(state.shard_count(), 1);
        assert!(state.is_leader("peer-1"));
    }
}
