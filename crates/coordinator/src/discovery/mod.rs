//! Auto-discovery module for AkiDB coordinators.
//!
//! This module provides zero-configuration discovery using libp2p:
//! - mDNS for local network peer discovery
//! - Gossipsub for cluster state dissemination
//! - Noise protocol for transport encryption

mod config;
#[cfg(feature = "discovery")]
mod gossip;
#[cfg(feature = "discovery")]
mod mdns;
#[cfg(feature = "discovery")]
mod network;
mod types;

pub use config::{CoordinatorMode, DiscoveryConfig};
pub use types::{
    ClusterState, ClusterStateMessage, CoordinatorAnnouncement, DiscoveryEvent, GossipEvent,
    MetricsMessage, NodeType, PeerInfo, ShardAnnouncement,
};

#[cfg(feature = "discovery")]
use std::sync::Arc;
#[cfg(feature = "discovery")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "discovery")]
use anyhow::Result;
#[cfg(feature = "discovery")]
use libp2p::{
    futures::StreamExt,
    gossipsub,
    swarm::SwarmEvent,
    Swarm,
};
#[cfg(feature = "discovery")]
use tokio::sync::RwLock;
#[cfg(feature = "discovery")]
use tracing::{debug, error, info, warn};

#[cfg(feature = "discovery")]
use self::gossip::GossipHandler;
#[cfg(feature = "discovery")]
use self::mdns::MdnsHandler;
#[cfg(feature = "discovery")]
use self::network::AkiDbBehaviour;
#[cfg(feature = "discovery")]
use self::types::{DiscoveryEvent, GossipEvent};

/// Main discovery service that manages peer discovery and cluster state
#[cfg(feature = "discovery")]
pub struct DiscoveryService {
    /// libp2p swarm
    swarm: Swarm<AkiDbBehaviour>,
    /// mDNS handler
    mdns_handler: MdnsHandler,
    /// Gossip handler
    gossip_handler: GossipHandler,
    /// Discovery configuration
    config: DiscoveryConfig,
    /// Local peer ID
    local_peer_id: String,
    /// Shared cluster state
    cluster_state: Arc<RwLock<ClusterState>>,
    /// gRPC address this coordinator listens on
    grpc_address: String,
}

#[cfg(feature = "discovery")]
impl DiscoveryService {
    /// Create a new discovery service
    pub async fn new(
        config: DiscoveryConfig,
        grpc_address: String,
    ) -> Result<Self> {
        let config = config.normalized();
        let mut swarm = network::create_swarm(&config).await?;
        let local_peer_id = swarm.local_peer_id().to_string();

        let mdns_handler = MdnsHandler::new(config.namespace.clone());
        let gossip_handler = GossipHandler::new(&config.namespace);

        // Subscribe to gossip topics
        gossip_handler.subscribe(&mut swarm.behaviour_mut().gossipsub)?;

        info!(
            "Discovery service created with PeerId: {}",
            local_peer_id
        );

        Ok(Self {
            swarm,
            mdns_handler,
            gossip_handler,
            config,
            local_peer_id,
            cluster_state: Arc::new(RwLock::new(ClusterState::default())),
            grpc_address,
        })
    }

    /// Get the local peer ID
    pub fn local_peer_id(&self) -> &str {
        &self.local_peer_id
    }

    /// Get a reference to the shared cluster state
    pub fn cluster_state(&self) -> Arc<RwLock<ClusterState>> {
        self.cluster_state.clone()
    }

    /// Run the discovery service
    pub async fn run(&mut self) -> Result<()> {
        // Determine listen address
        let listen_addr = if self.config.listen_port == 0 {
            "/ip4/0.0.0.0/tcp/0".parse()?
        } else {
            format!("/ip4/0.0.0.0/tcp/{}", self.config.listen_port).parse()?
        };

        self.swarm.listen_on(listen_addr)?;

        // Set up announce interval
        let mut announce_interval = tokio::time::interval(self.config.announce_interval());

        // Set up cleanup interval for stale peers
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(30));

        info!("Discovery service starting event loop");

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    if let Err(e) = self.handle_swarm_event(event).await {
                        error!("Error handling swarm event: {}", e);
                    }
                }
                _ = announce_interval.tick() => {
                    if let Err(e) = self.announce_self().await {
                        error!("Error announcing self: {}", e);
                    }
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_stale_peers().await;
                }
            }
        }
    }

    /// Handle a swarm event
    async fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<network::AkiDbBehaviourEvent>,
    ) -> Result<()> {
        use network::AkiDbBehaviourEvent;

        match event {
            SwarmEvent::Behaviour(AkiDbBehaviourEvent::Mdns(event)) => {
                let discovery_events = self.mdns_handler.handle_event(event);
                for evt in discovery_events {
                    self.handle_discovery_event(evt).await?;
                }
            }
            SwarmEvent::Behaviour(AkiDbBehaviourEvent::Gossipsub(
                gossipsub::Event::Message { message, .. },
            )) => {
                match self.gossip_handler.handle_message(message) {
                    Ok(evt) => self.handle_gossip_event(evt).await?,
                    Err(e) => warn!("Error handling gossip message: {}", e),
                }
            }
            SwarmEvent::Behaviour(AkiDbBehaviourEvent::Gossipsub(
                gossipsub::Event::Subscribed { peer_id, topic },
            )) => {
                debug!("Peer {} subscribed to {}", peer_id, topic);
            }
            SwarmEvent::Behaviour(AkiDbBehaviourEvent::Identify(event)) => {
                debug!("Identify event: {:?}", event);
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to peer: {}", peer_id);
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer: {}", peer_id);
            }
            _ => {
                debug!("Other swarm event: {:?}", event);
            }
        }

        Ok(())
    }

    /// Handle a discovery event
    async fn handle_discovery_event(&mut self, event: DiscoveryEvent) -> Result<()> {
        match event {
            DiscoveryEvent::PeerDiscovered {
                peer_id, address, ..
            } => {
                info!("Discovered peer: {} at {}", peer_id, address);
                // Dial the peer
                if let Err(e) = self.swarm.dial(address.parse::<libp2p::Multiaddr>()?) {
                    warn!("Failed to dial peer {}: {}", peer_id, e);
                }
            }
            DiscoveryEvent::PeerExpired { peer_id } => {
                info!("Peer expired: {}", peer_id);
                let mut state = self.cluster_state.write().await;
                state.coordinators.retain(|c| c.peer_id != peer_id);
            }
            DiscoveryEvent::ClusterStateReceived { state } => {
                self.cluster_state.write().await.merge(&state);
            }
            DiscoveryEvent::MetricsReceived { .. } => {
                // Handle metrics if needed
            }
            DiscoveryEvent::LeaderChanged {
                old_leader,
                new_leader,
            } => {
                info!(
                    "Leader changed: {:?} -> {}",
                    old_leader, new_leader
                );
            }
        }
        Ok(())
    }

    /// Handle a gossip event
    async fn handle_gossip_event(&mut self, event: GossipEvent) -> Result<()> {
        match event {
            GossipEvent::ClusterState(state) => {
                debug!(
                    "Received cluster state from {} with {} coordinators, {} shards",
                    state.sender,
                    state.coordinators.len(),
                    state.shards.len()
                );
                self.cluster_state.write().await.merge(&state);
            }
            GossipEvent::Metrics(metrics) => {
                debug!(
                    "Received metrics from {}: QPS={:.0}",
                    metrics.sender, metrics.qps
                );
                // Could update metrics dashboard here
            }
        }
        Ok(())
    }

    /// Announce this coordinator to the cluster
    async fn announce_self(&mut self) -> Result<()> {
        let state = self.cluster_state.read().await;

        let message = ClusterStateMessage {
            sender: self.local_peer_id.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs(),
            coordinators: vec![CoordinatorAnnouncement {
                peer_id: self.local_peer_id.clone(),
                address: self.grpc_address.clone(),
                is_leader: state.is_leader(&self.local_peer_id),
                healthy: true,
            }],
            shards: state.shards.clone(),
            leader_id: state.leader_id.clone(),
        };
        drop(state);

        self.gossip_handler.publish_state(
            &mut self.swarm.behaviour_mut().gossipsub,
            &message,
        )?;

        debug!("Announced self to cluster");
        Ok(())
    }

    /// Clean up stale peers
    async fn cleanup_stale_peers(&mut self) {
        let max_age = self.config.stale_peer_max_age();

        let events = self.mdns_handler.cleanup_stale(max_age);
        for event in events {
            if let Err(e) = self.handle_discovery_event(event).await {
                error!("Error handling cleanup event: {}", e);
            }
        }
    }

    /// Register a shard with the cluster
    pub async fn register_shard(&mut self, shard: ShardAnnouncement) -> Result<()> {
        let mut state = self.cluster_state.write().await;

        // Add or update shard
        if let Some(existing) = state.shards.iter_mut().find(|s| s.id == shard.id) {
            *existing = shard;
        } else {
            state.shards.push(shard);
        }

        info!(
            "Registered shard, total shards: {}",
            state.shards.len()
        );
        Ok(())
    }

    /// Get the current leader ID
    pub async fn current_leader(&self) -> Option<String> {
        self.cluster_state.read().await.leader_id.clone()
    }

    /// Check if this coordinator is the leader
    pub async fn is_leader(&self) -> bool {
        self.cluster_state
            .read()
            .await
            .is_leader(&self.local_peer_id)
    }
}

/// Placeholder for when discovery feature is disabled
#[cfg(not(feature = "discovery"))]
pub struct DiscoveryService;

#[cfg(not(feature = "discovery"))]
impl DiscoveryService {
    /// Discovery is disabled
    pub async fn new(
        _config: DiscoveryConfig,
        _grpc_address: String,
    ) -> anyhow::Result<Self> {
        tracing::warn!("Discovery feature is not enabled. Compile with --features discovery");
        Ok(Self)
    }

    /// Get an empty cluster state
    pub fn cluster_state(&self) -> std::sync::Arc<tokio::sync::RwLock<ClusterState>> {
        std::sync::Arc::new(tokio::sync::RwLock::new(ClusterState::default()))
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
