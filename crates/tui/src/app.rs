//! Application state management for the TUI dashboard.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::config::TuiConfig;

/// Main application state
pub struct App {
    /// Current cluster state
    pub cluster_state: ClusterState,
    /// Current metrics state
    pub metrics: MetricsState,
    /// Currently selected panel
    pub selected_panel: Panel,
    /// Selected item index within panel
    pub selected_index: usize,
    /// Whether to quit the application
    pub should_quit: bool,
    /// TUI tick rate
    pub tick_rate: Duration,
    /// Configuration
    pub config: TuiConfig,
    /// Show help overlay
    pub show_help: bool,
    /// Status message
    pub status_message: Option<(String, Instant)>,
}

impl App {
    /// Create a new application with the given configuration
    pub fn new(config: TuiConfig) -> Self {
        Self {
            cluster_state: ClusterState::default(),
            metrics: MetricsState::default(),
            selected_panel: Panel::Topology,
            selected_index: 0,
            should_quit: false,
            tick_rate: Duration::from_millis(config.refresh_interval_ms),
            config,
            show_help: false,
            status_message: None,
        }
    }

    /// Create a new application with mock data for testing
    pub fn with_mock_data(config: TuiConfig) -> Self {
        let mut app = Self::new(config);
        app.cluster_state = ClusterState::mock();
        app.metrics = MetricsState::mock();
        app
    }

    /// Set a status message that will be displayed briefly
    pub fn set_status(&mut self, message: impl Into<String>) {
        self.status_message = Some((message.into(), Instant::now()));
    }

    /// Clear expired status messages
    pub fn clear_expired_status(&mut self) {
        if let Some((_, created)) = &self.status_message {
            if created.elapsed() > Duration::from_secs(3) {
                self.status_message = None;
            }
        }
    }

    /// Move selection up
    pub fn select_previous(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    /// Move selection down
    pub fn select_next(&mut self) {
        let max_index = match self.selected_panel {
            Panel::Topology => {
                self.cluster_state.coordinators.len() + self.cluster_state.shards.len()
            }
            Panel::Health => self.cluster_state.shards.len(),
        };
        if self.selected_index < max_index.saturating_sub(1) {
            self.selected_index += 1;
        }
    }

    /// Switch to next panel
    pub fn next_panel(&mut self) {
        self.selected_panel = match self.selected_panel {
            Panel::Topology => Panel::Health,
            Panel::Health => Panel::Topology,
        };
        self.selected_index = 0;
    }

    /// Switch to previous panel
    pub fn previous_panel(&mut self) {
        self.next_panel(); // Only two panels, so same as next
    }
}

/// Panel selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Panel {
    #[default]
    Topology,
    Health,
}

/// Cluster state containing coordinator and shard information
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    /// List of coordinators
    pub coordinators: Vec<CoordinatorInfo>,
    /// List of shards
    pub shards: Vec<ShardInfo>,
    /// Current leader peer ID
    pub leader_id: Option<String>,
    /// Local node peer ID
    pub local_peer_id: Option<String>,
    /// Last update timestamp
    pub last_update: Option<Instant>,
}

impl ClusterState {
    /// Create mock cluster state for testing
    pub fn mock() -> Self {
        Self {
            coordinators: vec![
                CoordinatorInfo {
                    id: "coord-1".to_string(),
                    peer_id: "12D3KooWA1b2c3d4e5f6g7h8i9j0".to_string(),
                    address: "127.0.0.1:50050".to_string(),
                    is_leader: true,
                    is_self: true,
                    last_seen: Instant::now(),
                    status: NodeStatus::Healthy,
                },
                CoordinatorInfo {
                    id: "coord-2".to_string(),
                    peer_id: "12D3KooWB2c3d4e5f6g7h8i9j0k1".to_string(),
                    address: "127.0.0.1:50052".to_string(),
                    is_leader: false,
                    is_self: false,
                    last_seen: Instant::now(),
                    status: NodeStatus::Healthy,
                },
            ],
            shards: vec![
                ShardInfo {
                    id: "shard-1".to_string(),
                    address: "127.0.0.1:50051".to_string(),
                    vector_count: 38,
                    health_score: 0.95,
                    gpu_memory_percent: None,
                    temperature: Some(52.0),
                    status: NodeStatus::Healthy,
                },
                ShardInfo {
                    id: "shard-2".to_string(),
                    address: "127.0.0.1:50053".to_string(),
                    vector_count: 42,
                    health_score: 0.92,
                    gpu_memory_percent: None,
                    temperature: Some(48.0),
                    status: NodeStatus::Healthy,
                },
            ],
            leader_id: Some("12D3KooWA1b2c3d4e5f6g7h8i9j0".to_string()),
            local_peer_id: Some("12D3KooWA1b2c3d4e5f6g7h8i9j0".to_string()),
            last_update: Some(Instant::now()),
        }
    }
}

/// Information about a coordinator node
#[derive(Debug, Clone)]
pub struct CoordinatorInfo {
    /// Coordinator identifier
    pub id: String,
    /// libp2p PeerID
    pub peer_id: String,
    /// Network address
    pub address: String,
    /// Whether this coordinator is the leader
    pub is_leader: bool,
    /// Whether this is the local node
    pub is_self: bool,
    /// Last time we received a heartbeat
    pub last_seen: Instant,
    /// Current health status
    pub status: NodeStatus,
}

impl CoordinatorInfo {
    /// Check if the node is visible (recent heartbeat)
    pub fn is_visible(&self) -> bool {
        self.last_seen.elapsed() < Duration::from_secs(5)
    }
}

/// Information about a shard node
#[derive(Debug, Clone)]
pub struct ShardInfo {
    /// Shard identifier
    pub id: String,
    /// Network address
    pub address: String,
    /// Number of vectors stored
    pub vector_count: u64,
    /// Health score (0.0 - 1.0)
    pub health_score: f32,
    /// GPU memory utilization percentage
    pub gpu_memory_percent: Option<f32>,
    /// GPU temperature in Celsius
    pub temperature: Option<f32>,
    /// Current health status
    pub status: NodeStatus,
}

/// Node health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeStatus {
    /// Node is healthy and responsive
    Healthy,
    /// Node is unhealthy or unresponsive
    Unhealthy,
    /// Node status is unknown
    #[default]
    Unknown,
}

/// Metrics state for the cluster
#[derive(Debug, Clone)]
pub struct MetricsState {
    /// Queries per second
    pub qps: f64,
    /// P50 latency in milliseconds
    pub p50_latency_ms: f64,
    /// P95 latency in milliseconds
    pub p95_latency_ms: f64,
    /// P99 latency in milliseconds
    pub p99_latency_ms: f64,
    /// Coverage percentage (0.0 - 1.0)
    pub coverage: f32,
    /// Current backpressure level (0.0 - 1.0)
    pub backpressure: f32,
    /// Whether we're within SLO
    pub within_slo: bool,
    /// Historical metrics data
    pub history: MetricsHistory,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            qps: 0.0,
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            coverage: 1.0,
            backpressure: 0.0,
            within_slo: true,
            history: MetricsHistory::default(),
        }
    }
}

impl MetricsState {
    /// Create mock metrics for testing
    pub fn mock() -> Self {
        let mut history = MetricsHistory::default();
        // Add some mock history data
        for i in 0..60 {
            let qps = 100.0 + (i as f64 * 0.5).sin() * 20.0;
            let latency = 25.0 + (i as f64 * 0.3).cos() * 5.0;
            history.qps_history.push(qps);
            history.latency_history.push(latency);
        }
        history
            .shard_health
            .insert("shard-1".to_string(), vec![0.95; 60]);
        history
            .shard_health
            .insert("shard-2".to_string(), vec![0.92; 60]);

        Self {
            qps: 125.0,
            p50_latency_ms: 22.5,
            p95_latency_ms: 38.2,
            p99_latency_ms: 45.8,
            coverage: 1.0,
            backpressure: 0.05,
            within_slo: true,
            history,
        }
    }
}

/// Historical metrics data for sparklines
#[derive(Debug, Clone)]
pub struct MetricsHistory {
    /// QPS history (last 60 data points)
    pub qps_history: Vec<f64>,
    /// Latency history (last 60 data points)
    pub latency_history: Vec<f64>,
    /// Per-shard health history
    pub shard_health: HashMap<String, Vec<f32>>,
    /// Maximum history size
    max_size: usize,
}

impl Default for MetricsHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsHistory {
    /// Create new history with default size
    pub fn new() -> Self {
        Self {
            qps_history: Vec::with_capacity(60),
            latency_history: Vec::with_capacity(60),
            shard_health: HashMap::new(),
            max_size: 60,
        }
    }

    /// Add a QPS data point
    pub fn add_qps(&mut self, qps: f64) {
        if self.qps_history.len() >= self.max_size {
            self.qps_history.remove(0);
        }
        self.qps_history.push(qps);
    }

    /// Add a latency data point
    pub fn add_latency(&mut self, latency: f64) {
        if self.latency_history.len() >= self.max_size {
            self.latency_history.remove(0);
        }
        self.latency_history.push(latency);
    }

    /// Add a shard health data point
    pub fn add_shard_health(&mut self, shard_id: &str, health: f32) {
        let history = self
            .shard_health
            .entry(shard_id.to_string())
            .or_insert_with(|| Vec::with_capacity(self.max_size));
        if history.len() >= self.max_size {
            history.remove(0);
        }
        history.push(health);
    }

    /// Get shard health history
    pub fn get_shard_health(&self, shard_id: &str) -> &[f32] {
        self.shard_health
            .get(shard_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}
