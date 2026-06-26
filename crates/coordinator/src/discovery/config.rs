//! Discovery configuration.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Discovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Enable auto-discovery
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Discovery method: "libp2p" or "static"
    #[serde(default = "default_method")]
    pub method: String,

    /// Cluster namespace for isolation (like EXO_LIBP2P_NAMESPACE)
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Pre-shared key for cluster membership (base64-encoded 32 bytes)
    /// Generate with: openssl rand -base64 32
    #[serde(default)]
    pub cluster_secret: Option<String>,

    /// How often to announce presence (milliseconds)
    #[serde(default = "default_announce_interval")]
    pub announce_interval_ms: u64,

    /// Heartbeat interval (milliseconds)
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    /// Missed heartbeats before marking node unhealthy
    #[serde(default = "default_missed_heartbeats")]
    pub missed_heartbeats_threshold: u32,

    /// Listen port for libp2p (0 for random)
    #[serde(default)]
    pub listen_port: u16,

    /// Address to advertise to other nodes
    #[serde(default)]
    pub advertise_address: Option<String>,

    /// Path to store/load identity keypair
    #[serde(default)]
    pub identity_path: Option<PathBuf>,

    /// Bootstrap peers (for environments where mDNS doesn't work)
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,

    /// Coordinator mode
    #[serde(default)]
    pub mode: CoordinatorMode,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            method: default_method(),
            namespace: default_namespace(),
            cluster_secret: None,
            announce_interval_ms: default_announce_interval(),
            heartbeat_interval_ms: default_heartbeat_interval(),
            missed_heartbeats_threshold: default_missed_heartbeats(),
            listen_port: 0,
            advertise_address: None,
            identity_path: None,
            bootstrap_peers: vec![],
            mode: CoordinatorMode::default(),
        }
    }
}

/// Coordinator operating mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CoordinatorMode {
    /// Join existing cluster or become leader if first
    #[default]
    Auto,
    /// Start as initial cluster leader
    Bootstrap,
    /// No clustering, single coordinator
    Standalone,
}

fn default_enabled() -> bool {
    true
}

fn default_method() -> String {
    "libp2p".to_string()
}

fn default_namespace() -> String {
    "akidb-default".to_string()
}

fn default_announce_interval() -> u64 {
    2500
}

fn default_heartbeat_interval() -> u64 {
    1000
}

fn default_missed_heartbeats() -> u32 {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DiscoveryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.method, "libp2p");
        assert_eq!(config.namespace, "akidb-default");
        assert_eq!(config.announce_interval_ms, 2500);
        assert_eq!(config.heartbeat_interval_ms, 1000);
        assert_eq!(config.missed_heartbeats_threshold, 3);
    }

    #[test]
    fn test_parse_config() {
        let toml = r#"
            enabled = true
            namespace = "akidb-prod"
            cluster_secret = "dGVzdC1zZWNyZXQtYmFzZTY0LWtleQ=="
            announce_interval_ms = 5000
            mode = "bootstrap"
        "#;

        let config: DiscoveryConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.namespace, "akidb-prod");
        assert!(config.cluster_secret.is_some());
        assert_eq!(config.announce_interval_ms, 5000);
        assert_eq!(config.mode, CoordinatorMode::Bootstrap);
    }
}
