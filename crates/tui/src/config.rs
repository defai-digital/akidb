//! TUI configuration management.

use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub const MIN_REFRESH_INTERVAL_MS: u64 = 1;

/// TUI dashboard configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuiConfig {
    /// Refresh interval in milliseconds
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval_ms: u64,

    /// Whether to show GPU metrics from legacy/non-macOS deployments
    #[serde(default = "default_show_gpu_metrics")]
    pub show_gpu_metrics: bool,

    /// Color theme
    #[serde(default)]
    pub theme: ThemeConfig,

    /// Coordinator address to connect to
    #[serde(default)]
    pub coordinator_address: Option<String>,

    /// Discovery addresses to try when coordinator_address is not specified
    #[serde(default = "default_discovery_addresses")]
    pub discovery_addresses: Vec<String>,

    /// Whether to use mock data (for testing)
    #[serde(default)]
    pub mock_mode: bool,

    /// Layout configuration
    #[serde(default)]
    pub layout: LayoutConfig,

    /// Controls configuration
    #[serde(default)]
    pub controls: ControlsConfig,
}

impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            refresh_interval_ms: default_refresh_interval(),
            show_gpu_metrics: default_show_gpu_metrics(),
            theme: ThemeConfig::default(),
            coordinator_address: None,
            discovery_addresses: default_discovery_addresses(),
            mock_mode: false,
            layout: LayoutConfig::default(),
            controls: ControlsConfig::default(),
        }
    }
}

impl TuiConfig {
    pub fn normalize(&mut self) {
        self.refresh_interval_ms = self.refresh_interval_ms.max(MIN_REFRESH_INTERVAL_MS);
    }

    /// Load configuration from file (supports both TOML and JSON)
    pub fn load(path: Option<&PathBuf>) -> Result<Self> {
        if let Some(path) = path {
            let content = std::fs::read_to_string(path)?;
            let mut config: TuiConfig = if path.extension().map_or(false, |ext| ext == "json") {
                serde_json::from_str(&content)?
            } else {
                toml::from_str(&content)?
            };
            config.normalize();
            Ok(config)
        } else {
            let mut config = Self::default();
            config.normalize();
            Ok(config)
        }
    }

    /// Load from the default config location
    pub fn load_default() -> Result<Self> {
        // Try to load from common locations (in priority order)
        let mut locations = vec![
            // Production deployment locations
            PathBuf::from("/opt/akidb/config/tui.json"),
            PathBuf::from("/opt/akidb/config/tui.toml"),
            PathBuf::from("/opt/akidb/config/config.json"),
            // Standard system locations
            PathBuf::from("/etc/akidb/tui.toml"),
            PathBuf::from("/etc/akidb/tui.json"),
            // Local development
            PathBuf::from("config/tui.toml"),
            PathBuf::from("config/tui.json"),
        ];

        // Add user config dir if available
        if let Some(config_dir) = dirs::config_dir() {
            locations.push(config_dir.join("akidb").join("tui.toml"));
            locations.push(config_dir.join("akidb").join("tui.json"));
        }

        for location in &locations {
            if location.exists() {
                tracing::info!("Loading TUI config from {:?}", location);
                return Self::load(Some(location));
            }
        }

        tracing::info!("No config file found, using defaults with built-in discovery addresses");
        Ok(Self::default())
    }
}

/// Theme configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ThemeConfig {
    /// Theme name: "default", "minimal", "high-contrast"
    #[serde(default = "default_theme_name")]
    pub name: String,
}

fn default_theme_name() -> String {
    "default".to_string()
}

/// Layout configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutConfig {
    /// Show topology panel
    #[serde(default = "default_true")]
    pub show_topology: bool,

    /// Show metrics panel
    #[serde(default = "default_true")]
    pub show_metrics: bool,

    /// Show health sparklines
    #[serde(default = "default_true")]
    pub show_health: bool,
}

impl Default for LayoutConfig {
    fn default() -> Self {
        Self {
            show_topology: true,
            show_metrics: true,
            show_health: true,
        }
    }
}

/// Controls configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlsConfig {
    /// Allow node eviction from TUI
    #[serde(default = "default_true")]
    pub allow_eviction: bool,

    /// Require quorum confirmation for destructive actions
    #[serde(default = "default_true")]
    pub require_quorum_confirmation: bool,
}

impl Default for ControlsConfig {
    fn default() -> Self {
        Self {
            allow_eviction: true,
            require_quorum_confirmation: true,
        }
    }
}

fn default_refresh_interval() -> u64 {
    500
}

fn default_show_gpu_metrics() -> bool {
    false
}

fn default_true() -> bool {
    true
}

fn default_discovery_addresses() -> Vec<String> {
    vec!["127.0.0.1:50050".to_string()]
}

// Placeholder for dirs crate functionality
mod dirs {
    use std::path::PathBuf;

    pub fn config_dir() -> Option<PathBuf> {
        std::env::var("HOME")
            .ok()
            .map(|h| PathBuf::from(h).join(".config"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TuiConfig::default();
        assert_eq!(config.refresh_interval_ms, 500);
        assert!(!config.show_gpu_metrics);
        assert!(config.layout.show_topology);
        // Default discovery addresses should be set
        assert!(!config.discovery_addresses.is_empty());
        assert!(config.discovery_addresses.contains(&"127.0.0.1:50050".to_string()));
    }

    #[test]
    fn test_parse_config() {
        let toml = r#"
            refresh_interval_ms = 1000
            show_gpu_metrics = false
            coordinator_address = "127.0.0.1:50050"

            [theme]
            name = "minimal"

            [layout]
            show_topology = true
            show_metrics = true
            show_health = false
        "#;

        let config: TuiConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.refresh_interval_ms, 1000);
        assert!(!config.show_gpu_metrics);
        assert_eq!(
            config.coordinator_address,
            Some("127.0.0.1:50050".to_string())
        );
        assert_eq!(config.theme.name, "minimal");
        assert!(!config.layout.show_health);
    }

    #[test]
    fn test_parse_config_with_discovery_addresses() {
        let toml = r#"
            discovery_addresses = ["10.0.0.1:50050", "10.0.0.2:50050"]
        "#;

        let config: TuiConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.discovery_addresses.len(), 2);
        assert_eq!(config.discovery_addresses[0], "10.0.0.1:50050");
        assert_eq!(config.discovery_addresses[1], "10.0.0.2:50050");
    }

    #[test]
    fn test_parse_json_config() {
        let json = r#"{
            "refresh_interval_ms": 1000,
            "discovery_addresses": ["127.0.0.1:50050", "127.0.0.1:50051"]
        }"#;

        let config: TuiConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.refresh_interval_ms, 1000);
        assert_eq!(config.discovery_addresses.len(), 2);
    }

    #[test]
    fn test_normalize_rejects_zero_refresh_interval() {
        let mut config = TuiConfig {
            refresh_interval_ms: 0,
            ..TuiConfig::default()
        };

        config.normalize();

        assert_eq!(config.refresh_interval_ms, MIN_REFRESH_INTERVAL_MS);
    }
}
