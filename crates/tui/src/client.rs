//! gRPC client for fetching cluster state from coordinator.

use std::time::{Duration, Instant};

use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::{GetClusterStateRequest, NodeStatus};
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::app::{
    ClusterState, CoordinatorInfo, MetricsState, NodeStatus as AppNodeStatus, ShardInfo,
};

/// Client for connecting to AkiDB coordinator
pub struct CoordinatorClient {
    client: AkidbClient<Channel>,
    address: String,
}

impl CoordinatorClient {
    /// Connect to a coordinator at the given address
    pub async fn connect(address: &str) -> Result<Self, tonic::transport::Error> {
        let endpoint = if address.starts_with("http://") || address.starts_with("https://") {
            address.to_string()
        } else {
            format!("http://{}", address)
        };

        debug!("Connecting to coordinator at {}", endpoint);
        let client = AkidbClient::connect(endpoint).await?;

        Ok(Self {
            client,
            address: address.to_string(),
        })
    }

    /// Fetch the current cluster state
    pub async fn get_cluster_state(
        &mut self,
    ) -> Result<(ClusterState, MetricsState), tonic::Status> {
        let request = tonic::Request::new(GetClusterStateRequest {});
        let response = self.client.get_cluster_state(request).await?;
        let state = response.into_inner();

        // Convert proto coordinators to app types
        let coordinators: Vec<CoordinatorInfo> = state
            .coordinators
            .into_iter()
            .map(|c| CoordinatorInfo {
                id: c.id,
                peer_id: c.peer_id,
                address: c.address,
                is_leader: c.is_leader,
                is_self: c.is_self,
                last_seen: Instant::now(),
                status: match c.status {
                    x if x == NodeStatus::Healthy as i32 => AppNodeStatus::Healthy,
                    x if x == NodeStatus::Unhealthy as i32 => AppNodeStatus::Unhealthy,
                    _ => AppNodeStatus::Unknown,
                },
            })
            .collect();

        // Convert proto shards to app types
        let shards: Vec<ShardInfo> = state
            .shards
            .into_iter()
            .map(|s| ShardInfo {
                id: s.id,
                address: s.address,
                vector_count: s.vector_count,
                health_score: s.health_score,
                gpu_memory_percent: s.gpu_memory_percent,
                temperature: s.temperature,
                status: match s.status {
                    x if x == NodeStatus::Healthy as i32 => AppNodeStatus::Healthy,
                    x if x == NodeStatus::Unhealthy as i32 => AppNodeStatus::Unhealthy,
                    _ => AppNodeStatus::Unknown,
                },
            })
            .collect();

        let cluster_state = ClusterState {
            coordinators,
            shards,
            leader_id: state.leader_id,
            local_peer_id: Some(state.local_peer_id),
            last_update: Some(Instant::now()),
        };

        // Convert metrics
        let metrics = if let Some(m) = state.metrics {
            MetricsState {
                qps: m.qps,
                p50_latency_ms: m.p50_latency_ms,
                p95_latency_ms: m.p95_latency_ms,
                p99_latency_ms: m.p99_latency_ms,
                coverage: m.coverage,
                backpressure: m.backpressure,
                within_slo: m.within_slo,
                ..Default::default()
            }
        } else {
            MetricsState::default()
        };

        Ok((cluster_state, metrics))
    }

    /// Get the address this client is connected to
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Try to connect to coordinator, returning None if connection fails
pub async fn try_connect(address: &str, timeout: Duration) -> Option<CoordinatorClient> {
    match tokio::time::timeout(timeout, CoordinatorClient::connect(address)).await {
        Ok(Ok(client)) => Some(client),
        Ok(Err(e)) => {
            warn!("Failed to connect to coordinator at {}: {}", address, e);
            None
        }
        Err(_) => {
            warn!("Connection to coordinator at {} timed out", address);
            None
        }
    }
}
