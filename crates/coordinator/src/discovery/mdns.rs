//! mDNS discovery handler.

#[cfg(feature = "discovery")]
use std::collections::HashMap;
#[cfg(feature = "discovery")]
use std::time::Instant;

#[cfg(feature = "discovery")]
use libp2p::mdns;
#[cfg(feature = "discovery")]
use tracing::{debug, info};

#[cfg(feature = "discovery")]
use super::types::{DiscoveryEvent, NodeType, PeerInfo};

/// Handler for mDNS discovery events
#[cfg(feature = "discovery")]
pub struct MdnsHandler {
    /// Discovered peers
    discovered_peers: HashMap<String, PeerInfo>,
    /// Cluster namespace
    namespace: String,
}

#[cfg(feature = "discovery")]
impl MdnsHandler {
    /// Create a new mDNS handler
    pub fn new(namespace: String) -> Self {
        Self {
            discovered_peers: HashMap::new(),
            namespace,
        }
    }

    /// Handle an mDNS event and return discovery events
    pub fn handle_event(&mut self, event: mdns::Event) -> Vec<DiscoveryEvent> {
        let mut events = vec![];

        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, addr) in list {
                    let peer_id_str = peer_id.to_string();
                    let addr_str = addr.to_string();

                    debug!("mDNS discovered peer: {} at {}", peer_id_str, addr_str);

                    // Check if this is a new peer
                    if !self.discovered_peers.contains_key(&peer_id_str) {
                        info!("New peer discovered via mDNS: {}", peer_id_str);

                        let peer_info = PeerInfo {
                            peer_id: peer_id_str.clone(),
                            addresses: vec![addr_str.clone()],
                            last_seen: Instant::now(),
                            node_type: NodeType::Coordinator, // Will be updated via identify
                        };

                        self.discovered_peers.insert(peer_id_str.clone(), peer_info);

                        events.push(DiscoveryEvent::PeerDiscovered {
                            peer_id: peer_id_str,
                            address: addr_str,
                            node_type: NodeType::Coordinator,
                        });
                    } else {
                        // Update last seen time
                        if let Some(peer) = self.discovered_peers.get_mut(&peer_id_str) {
                            peer.last_seen = Instant::now();
                            // Add address if not already known
                            if !peer.addresses.contains(&addr_str) {
                                peer.addresses.push(addr_str);
                            }
                        }
                    }
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, addr) in list {
                    let peer_id_str = peer_id.to_string();
                    let addr_str = addr.to_string();

                    debug!("mDNS peer expired: {} at {}", peer_id_str, addr_str);

                    // Remove the peer
                    if self.discovered_peers.remove(&peer_id_str).is_some() {
                        info!("Peer expired via mDNS: {}", peer_id_str);
                        events.push(DiscoveryEvent::PeerExpired {
                            peer_id: peer_id_str,
                        });
                    }
                }
            }
        }

        events
    }

    /// Get all currently discovered peers
    pub fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.discovered_peers.values()
    }

    /// Get peer count
    pub fn peer_count(&self) -> usize {
        self.discovered_peers.len()
    }

    /// Check if a peer is known
    pub fn has_peer(&self, peer_id: &str) -> bool {
        self.discovered_peers.contains_key(peer_id)
    }

    /// Get info for a specific peer
    pub fn get_peer(&self, peer_id: &str) -> Option<&PeerInfo> {
        self.discovered_peers.get(peer_id)
    }

    /// Clean up stale peers (not seen recently)
    pub fn cleanup_stale(&mut self, max_age: std::time::Duration) -> Vec<DiscoveryEvent> {
        let mut events = vec![];
        let now = Instant::now();

        self.discovered_peers.retain(|peer_id, info| {
            if now.duration_since(info.last_seen) > max_age {
                info!("Removing stale peer: {}", peer_id);
                events.push(DiscoveryEvent::PeerExpired {
                    peer_id: peer_id.clone(),
                });
                false
            } else {
                true
            }
        });

        events
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "discovery")]
    use super::*;

    #[cfg(feature = "discovery")]
    #[test]
    fn test_mdns_handler_new() {
        let handler = MdnsHandler::new("test-namespace".to_string());
        assert_eq!(handler.peer_count(), 0);
        assert_eq!(handler.namespace, "test-namespace");
    }
}
