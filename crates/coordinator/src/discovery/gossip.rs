//! Gossipsub handler for cluster state dissemination.

#[cfg(feature = "discovery")]
use anyhow::Result;
#[cfg(feature = "discovery")]
use libp2p::gossipsub::{self, IdentTopic, Message};
#[cfg(feature = "discovery")]
use tracing::{debug, warn};

#[cfg(feature = "discovery")]
use super::types::{ClusterStateMessage, GossipEvent, MetricsMessage};

const CLUSTER_STATE_TOPIC: &str = "cluster-state";
const METRICS_TOPIC: &str = "metrics";

/// Handler for gossipsub messages
#[cfg(feature = "discovery")]
pub struct GossipHandler {
    /// Topic for cluster state updates
    cluster_state_topic: IdentTopic,
    /// Topic for metrics updates
    metrics_topic: IdentTopic,
    /// Cluster namespace
    namespace: String,
}

#[cfg(feature = "discovery")]
impl GossipHandler {
    /// Create a new gossip handler
    pub fn new(namespace: &str) -> Self {
        Self {
            cluster_state_topic: IdentTopic::new(format!(
                "akidb/{}/{}",
                namespace, CLUSTER_STATE_TOPIC
            )),
            metrics_topic: IdentTopic::new(format!("akidb/{}/{}", namespace, METRICS_TOPIC)),
            namespace: namespace.to_string(),
        }
    }

    /// Get the cluster state topic
    pub fn cluster_state_topic(&self) -> &IdentTopic {
        &self.cluster_state_topic
    }

    /// Get the metrics topic
    pub fn metrics_topic(&self) -> &IdentTopic {
        &self.metrics_topic
    }

    /// Subscribe to all topics
    pub fn subscribe(&self, gossipsub: &mut gossipsub::Behaviour) -> Result<()> {
        gossipsub.subscribe(&self.cluster_state_topic)?;
        gossipsub.subscribe(&self.metrics_topic)?;
        debug!(
            "Subscribed to gossip topics for namespace: {}",
            self.namespace
        );
        Ok(())
    }

    /// Publish a cluster state update
    pub fn publish_state(
        &self,
        gossipsub: &mut gossipsub::Behaviour,
        state: &ClusterStateMessage,
    ) -> Result<()> {
        let data = serde_json::to_vec(state)?;
        gossipsub.publish(self.cluster_state_topic.clone(), data)?;
        debug!("Published cluster state update");
        Ok(())
    }

    /// Publish a metrics update
    pub fn publish_metrics(
        &self,
        gossipsub: &mut gossipsub::Behaviour,
        metrics: &MetricsMessage,
    ) -> Result<()> {
        let data = serde_json::to_vec(metrics)?;
        gossipsub.publish(self.metrics_topic.clone(), data)?;
        debug!("Published metrics update");
        Ok(())
    }

    /// Handle an incoming gossip message
    pub fn handle_message(&self, message: Message) -> Result<GossipEvent> {
        let topic = message.topic.as_str();

        if topic.ends_with(CLUSTER_STATE_TOPIC) {
            let state: ClusterStateMessage = serde_json::from_slice(&message.data)?;
            debug!("Received cluster state from {}", state.sender);
            Ok(GossipEvent::ClusterState(state))
        } else if topic.ends_with(METRICS_TOPIC) {
            let metrics: MetricsMessage = serde_json::from_slice(&message.data)?;
            debug!("Received metrics from {}", metrics.sender);
            Ok(GossipEvent::Metrics(metrics))
        } else {
            warn!("Unknown gossip topic: {}", topic);
            Err(anyhow::anyhow!("Unknown gossip topic: {}", topic))
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "discovery")]
    use super::*;

    #[cfg(feature = "discovery")]
    #[test]
    fn test_gossip_handler_topics() {
        let handler = GossipHandler::new("test-ns");
        assert!(handler
            .cluster_state_topic()
            .to_string()
            .contains("test-ns"));
        assert!(handler.metrics_topic().to_string().contains("test-ns"));
    }

    #[cfg(feature = "discovery")]
    #[test]
    fn test_serialize_cluster_state() {
        let state = ClusterStateMessage {
            sender: "test-peer".to_string(),
            timestamp: 1234567890,
            coordinators: vec![],
            shards: vec![],
            leader_id: None,
        };

        let json = serde_json::to_string(&state).unwrap();
        let parsed: ClusterStateMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sender, "test-peer");
        assert_eq!(parsed.timestamp, 1234567890);
    }
}
