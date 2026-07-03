//! Discovery configuration.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub const MIN_ANNOUNCE_INTERVAL_MS: u64 = 1;
pub const MIN_HEARTBEAT_INTERVAL_MS: u64 = 1;
pub const MIN_MISSED_HEARTBEATS: u32 = 1;

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

impl DiscoveryConfig {
    pub fn normalize(&mut self) {
        self.announce_interval_ms = self.announce_interval_ms.max(MIN_ANNOUNCE_INTERVAL_MS);
        self.heartbeat_interval_ms = self.heartbeat_interval_ms.max(MIN_HEARTBEAT_INTERVAL_MS);
        self.missed_heartbeats_threshold =
            self.missed_heartbeats_threshold.max(MIN_MISSED_HEARTBEATS);
    }

    pub fn normalized(mut self) -> Self {
        self.normalize();
        self
    }

    pub fn announce_interval(&self) -> Duration {
        Duration::from_millis(self.announce_interval_ms.max(MIN_ANNOUNCE_INTERVAL_MS))
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms.max(MIN_HEARTBEAT_INTERVAL_MS))
    }

    /// Maximum age before a peer is considered stale.
    pub fn stale_peer_max_age(&self) -> Duration {
        Duration::from_millis(
            self.heartbeat_interval_ms
                .max(MIN_HEARTBEAT_INTERVAL_MS)
                .saturating_mul(self.missed_heartbeats_threshold.max(MIN_MISSED_HEARTBEATS) as u64),
        )
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

    #[test]
    fn test_stale_peer_max_age_does_not_overflow() {
        let config = DiscoveryConfig {
            heartbeat_interval_ms: u64::MAX,
            missed_heartbeats_threshold: 2,
            ..DiscoveryConfig::default()
        };

        let result = std::panic::catch_unwind(|| config.stale_peer_max_age());
        assert!(result.is_ok(), "stale peer age calculation should saturate");
    }

    #[test]
    fn test_zero_intervals_are_sanitized() {
        let mut config = DiscoveryConfig {
            announce_interval_ms: 0,
            heartbeat_interval_ms: 0,
            missed_heartbeats_threshold: 0,
            ..DiscoveryConfig::default()
        };

        config.normalize();

        assert_eq!(config.announce_interval_ms, MIN_ANNOUNCE_INTERVAL_MS);
        assert_eq!(config.heartbeat_interval_ms, MIN_HEARTBEAT_INTERVAL_MS);
        assert_eq!(config.missed_heartbeats_threshold, MIN_MISSED_HEARTBEATS);
        assert_eq!(config.announce_interval(), Duration::from_millis(1));
        assert_eq!(config.heartbeat_interval(), Duration::from_millis(1));
        assert_eq!(config.stale_peer_max_age(), Duration::from_millis(1));
    }

    #[test]
    fn test_stale_peer_max_age_never_returns_zero() {
        let config = DiscoveryConfig {
            heartbeat_interval_ms: 0,
            missed_heartbeats_threshold: 0,
            ..DiscoveryConfig::default()
        };

        assert_eq!(config.stale_peer_max_age(), Duration::from_millis(1));
    }
}
