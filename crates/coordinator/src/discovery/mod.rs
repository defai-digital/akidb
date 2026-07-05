//! Discovery configuration and static discovery service placeholder.
//!
//! The previous libp2p-backed discovery implementation depended on vulnerable
//! transitive networking crates that currently have no compatible upstream fix.
//! AkiDB uses explicit coordinator and shard addresses until a safe discovery
//! implementation is available.

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

/// Static discovery placeholder.
pub struct DiscoveryService {
    cluster_state: Arc<RwLock<ClusterState>>,
}

impl DiscoveryService {
    /// Create a discovery service backed by explicit/static configuration.
    pub async fn new(_config: DiscoveryConfig, _grpc_address: String) -> Result<Self> {
        tracing::warn!("Network auto-discovery is disabled; use explicit coordinator addresses");
        Ok(Self {
            cluster_state: Arc::new(RwLock::new(ClusterState::default())),
        })
    }

    /// Get the current cluster state.
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
