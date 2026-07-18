//! Application state management for the TUI dashboard.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::action::Action;
use crate::config::TuiConfig;
use crate::effect::Effect;
use crate::model::{
    AuditPageView, CapabilitiesView, CapabilityView, CollectionView, ConsoleState, ImportPlanInput,
    LoadState, OperationView, SnapshotView,
};

/// Main application state
pub struct App {
    /// Current cluster state
    pub cluster_state: ClusterState,
    /// Current metrics state
    pub metrics: MetricsState,
    /// Currently selected panel
    pub selected_panel: Panel,
    /// Active Operations Console screen
    pub screen: Screen,
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
    /// Read/plan-only management state
    pub console: ConsoleState,
    /// Pending side effects executed by the runtime
    pending_effects: VecDeque<Effect>,
    /// Validation-only import form state
    pub import_form: ImportForm,
    /// Case-insensitive filter applied to the active inventory screen.
    pub filter: String,
    pub filter_editing: bool,
}

impl App {
    /// Create a new application with the given configuration
    pub fn new(mut config: TuiConfig) -> Self {
        config.normalize();
        Self {
            cluster_state: ClusterState::default(),
            metrics: MetricsState::default(),
            selected_panel: Panel::Topology,
            screen: Screen::Overview,
            selected_index: 0,
            should_quit: false,
            tick_rate: Duration::from_millis(config.refresh_interval_ms),
            config,
            show_help: false,
            status_message: None,
            console: ConsoleState::default(),
            pending_effects: VecDeque::new(),
            import_form: ImportForm::default(),
            filter: String::new(),
            filter_editing: false,
        }
    }

    /// Create a new application with mock data for testing
    pub fn with_mock_data(config: TuiConfig) -> Self {
        let mut app = Self::new(config);
        app.cluster_state = ClusterState::mock();
        app.metrics = MetricsState::mock();
        app.console.capabilities = LoadState::Ready {
            value: CapabilitiesView {
                server_version: "mock".to_string(),
                api_version: 1,
                workspace_id: "default".to_string(),
                agent_id: Some("mock-operator".to_string()),
                authenticated: true,
                tls_active: false,
                auth_mode: "loopback_optional".to_string(),
                credential_source: "none".to_string(),
                capabilities: vec![CapabilityView {
                    name: "operations.read".to_string(),
                    supported: true,
                    authorized: true,
                    unavailable_reason: String::new(),
                }],
            },
            observed_at: Instant::now(),
            partial: false,
        };
        app.console.collections = LoadState::Ready {
            value: vec![CollectionView {
                name: "default".to_string(),
                dimensions: 2560,
                metric: "cosine".to_string(),
                embedding_model_id: "mock-embedding".to_string(),
                vector_precision: "f32".to_string(),
                chunk_strategy: "fixed".to_string(),
                vector_count: 80,
            }],
            observed_at: Instant::now(),
            partial: false,
        };
        app.console.operations = LoadState::Ready {
            value: vec![OperationView {
                id: "op-mock-1".to_string(),
                operation_type: "CREATE_SNAPSHOT".to_string(),
                state: "OPERATION_SUCCEEDED".to_string(),
                target: "task:daily".to_string(),
                progress_percent: Some(100.0),
                updated_at_ms: 0,
                items_processed: 80,
                bytes_processed: 4096,
                problem: None,
            }],
            observed_at: Instant::now(),
            partial: false,
        };
        app.console.snapshots = LoadState::Ready {
            value: vec![SnapshotView {
                id: "snapshot-mock-1".to_string(),
                collection: "default".to_string(),
                created_at_ms: 0,
                size_bytes: 4096,
                manifest_present: true,
                verification_state: "VERIFICATION_UNKNOWN".to_string(),
                restore_test_state: "RESTORE_TEST_NEVER".to_string(),
            }],
            observed_at: Instant::now(),
            partial: false,
        };
        app.console.audit = LoadState::Ready {
            value: AuditPageView {
                events: Vec::new(),
                retention_notice: "mock local retention".to_string(),
                integrity_status: "not-tamper-evident".to_string(),
            },
            observed_at: Instant::now(),
            partial: false,
        };
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
        let max_index = match self.screen {
            Screen::Overview => {
                self.cluster_state.coordinators.len() + self.cluster_state.shards.len()
            }
            Screen::Collections => load_len(&self.console.collections),
            Screen::Operations => load_len(&self.console.operations),
            Screen::Snapshots => load_len(&self.console.snapshots),
            Screen::Audit => match &self.console.audit {
                LoadState::Ready { value, .. } | LoadState::Stale { value, .. } => {
                    value.events.len()
                }
                _ => 0,
            },
            Screen::ImportPlan | Screen::Access => 0,
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

    pub fn next_screen(&mut self) {
        self.screen = self.screen.next();
        self.selected_index = 0;
        self.queue_refresh(self.screen);
    }

    pub fn previous_screen(&mut self) {
        self.screen = self.screen.previous();
        self.selected_index = 0;
        self.queue_refresh(self.screen);
    }

    pub fn queue_initial_effects(&mut self) {
        self.queue_effect(Effect::LoadCapabilities);
        self.queue_effect(Effect::LoadCollections);
        self.queue_effect(Effect::LoadOperations);
        self.queue_effect(Effect::LoadSnapshots);
        self.queue_effect(Effect::LoadAudit);
    }

    pub fn queue_refresh(&mut self, screen: Screen) {
        let effect = match screen {
            Screen::Overview | Screen::Access => Effect::LoadCapabilities,
            Screen::Collections => Effect::LoadCollections,
            Screen::Operations => Effect::LoadOperations,
            Screen::Snapshots => Effect::LoadSnapshots,
            Screen::Audit => Effect::LoadAudit,
            Screen::ImportPlan => return,
        };
        self.queue_effect(effect);
    }

    pub fn queue_due_refresh(&mut self) {
        let due = match self.screen {
            Screen::Collections => state_due(&self.console.collections, Duration::from_secs(5)),
            Screen::Operations => state_due(&self.console.operations, Duration::from_secs(1)),
            Screen::Snapshots => state_due(&self.console.snapshots, Duration::from_secs(30)),
            _ => false,
        };
        if due {
            self.queue_refresh(self.screen);
        }
    }

    pub fn queue_effect(&mut self, effect: Effect) {
        if !effect.is_read_or_validate_only() {
            return;
        }
        if self
            .pending_effects
            .iter()
            .any(|pending| pending.kind() == effect.kind())
        {
            return;
        }
        self.pending_effects.push_back(effect);
    }

    pub fn take_effect(&mut self) -> Option<Effect> {
        self.pending_effects.pop_front()
    }

    pub fn request_import_plan(&mut self) {
        match self.import_form.input() {
            Ok(input) => self.queue_effect(Effect::PlanImport(input)),
            Err(error) => self.set_status(error),
        }
    }

    pub fn mark_loading(&mut self, effect: &Effect) {
        match effect {
            Effect::LoadCapabilities => start_loading(&mut self.console.capabilities),
            Effect::LoadCollections => start_loading(&mut self.console.collections),
            Effect::LoadOperations => start_loading(&mut self.console.operations),
            Effect::LoadSnapshots => start_loading(&mut self.console.snapshots),
            Effect::PlanImport(_) => start_loading(&mut self.console.import_plan),
            Effect::LoadAudit => start_loading(&mut self.console.audit),
        }
    }

    pub fn update(&mut self, action: Action) {
        match action {
            Action::CapabilitiesLoaded(result) => {
                finish_loading(&mut self.console.capabilities, result, "diagnostics.read")
            }
            Action::CollectionsLoaded(result) => {
                finish_loading(&mut self.console.collections, result, "collections.read")
            }
            Action::OperationsLoaded(result) => {
                finish_loading(&mut self.console.operations, result, "operations.read")
            }
            Action::SnapshotsLoaded(result) => {
                finish_loading(&mut self.console.snapshots, result, "snapshots.read")
            }
            Action::ImportPlanLoaded(result) => {
                finish_loading(&mut self.console.import_plan, result, "data.import.plan")
            }
            Action::AuditLoaded(result) => {
                finish_loading(&mut self.console.audit, result, "audit.read")
            }
        }
    }
}

fn load_len<T>(state: &LoadState<Vec<T>>) -> usize {
    match state {
        LoadState::Ready { value, .. } | LoadState::Stale { value, .. } => value.len(),
        _ => 0,
    }
}

fn state_due<T>(state: &LoadState<T>, interval: Duration) -> bool {
    !state.is_loading() && state.age().is_none_or(|age| age >= interval)
}

fn start_loading<T>(state: &mut LoadState<T>) {
    let previous = match std::mem::take(state) {
        LoadState::Ready { value, .. } | LoadState::Stale { value, .. } => Some(value),
        LoadState::Loading { previous } => previous,
        _ => None,
    };
    *state = LoadState::Loading { previous };
}

fn finish_loading<T>(state: &mut LoadState<T>, result: Result<T, String>, capability: &str) {
    let previous = match std::mem::take(state) {
        LoadState::Loading { previous } => previous,
        LoadState::Ready { value, .. } | LoadState::Stale { value, .. } => Some(value),
        _ => None,
    };
    *state = match result {
        Ok(value) => LoadState::Ready {
            value,
            observed_at: Instant::now(),
            partial: false,
        },
        Err(error) => match previous {
            Some(value) => LoadState::Stale {
                value,
                observed_at: Instant::now(),
                error,
            },
            None if error.starts_with("PermissionDenied:") => LoadState::Denied {
                capability: capability.to_string(),
            },
            None if error.starts_with("Unimplemented:") => LoadState::Unsupported { reason: error },
            None => LoadState::Failed(error),
        },
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MIN_REFRESH_INTERVAL_MS;

    #[test]
    fn test_app_normalizes_zero_refresh_interval() {
        let app = App::new(TuiConfig {
            refresh_interval_ms: 0,
            ..TuiConfig::default()
        });

        assert_eq!(app.config.refresh_interval_ms, MIN_REFRESH_INTERVAL_MS);
        assert_eq!(
            app.tick_rate,
            Duration::from_millis(MIN_REFRESH_INTERVAL_MS)
        );
    }

    #[test]
    fn import_form_builds_validation_input_only() {
        let mut form = ImportForm {
            staging_id: "stage-1".to_string(),
            object_id: "object-1".to_string(),
            etag: "etag-1".to_string(),
            size_bytes: "1024".to_string(),
            collection: "default".to_string(),
            duplicate_policy: "skip".to_string(),
            ..ImportForm::default()
        };
        let input = form.input().unwrap();
        assert_eq!(input.size_bytes, 1024);
        form.duplicate_policy = "overwrite-everything".to_string();
        assert!(form.input().is_err());
    }

    #[test]
    fn failed_refresh_preserves_stale_data() {
        let mut state = LoadState::Ready {
            value: vec!["existing".to_string()],
            observed_at: Instant::now(),
            partial: false,
        };
        start_loading(&mut state);
        finish_loading(
            &mut state,
            Err("Unavailable: offline".to_string()),
            "test.read",
        );
        match state {
            LoadState::Stale { value, .. } => assert_eq!(value, vec!["existing"]),
            _ => panic!("expected stale state"),
        }
    }
}

/// Panel selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Panel {
    #[default]
    Topology,
    Health,
}

/// Top-level Operations Console navigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Screen {
    #[default]
    Overview,
    Collections,
    Operations,
    Snapshots,
    ImportPlan,
    Access,
    Audit,
}

impl Screen {
    pub const ALL: [Self; 7] = [
        Self::Overview,
        Self::Collections,
        Self::Operations,
        Self::Snapshots,
        Self::ImportPlan,
        Self::Access,
        Self::Audit,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Collections => "Collections",
            Self::Operations => "Operations",
            Self::Snapshots => "Snapshots",
            Self::ImportPlan => "Import Plan",
            Self::Access => "Access",
            Self::Audit => "Audit",
        }
    }

    pub fn next(self) -> Self {
        let index = Self::ALL
            .iter()
            .position(|screen| *screen == self)
            .unwrap_or(0);
        Self::ALL[(index + 1) % Self::ALL.len()]
    }

    pub fn previous(self) -> Self {
        let index = Self::ALL
            .iter()
            .position(|screen| *screen == self)
            .unwrap_or(0);
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImportField {
    #[default]
    StagingId,
    ObjectId,
    Etag,
    SizeBytes,
    Collection,
    DuplicatePolicy,
}

impl ImportField {
    pub fn next(self) -> Self {
        match self {
            Self::StagingId => Self::ObjectId,
            Self::ObjectId => Self::Etag,
            Self::Etag => Self::SizeBytes,
            Self::SizeBytes => Self::Collection,
            Self::Collection => Self::DuplicatePolicy,
            Self::DuplicatePolicy => Self::StagingId,
        }
    }
}

/// Non-secret form for a validation-only staged import request.
#[derive(Debug, Clone)]
pub struct ImportForm {
    pub staging_id: String,
    pub object_id: String,
    pub etag: String,
    pub size_bytes: String,
    pub collection: String,
    pub duplicate_policy: String,
    pub active_field: ImportField,
    pub editing: bool,
}

impl Default for ImportForm {
    fn default() -> Self {
        Self {
            staging_id: String::new(),
            object_id: String::new(),
            etag: String::new(),
            size_bytes: String::new(),
            collection: "default".to_string(),
            duplicate_policy: "skip".to_string(),
            active_field: ImportField::StagingId,
            editing: false,
        }
    }
}

impl ImportForm {
    pub fn active_value_mut(&mut self) -> &mut String {
        match self.active_field {
            ImportField::StagingId => &mut self.staging_id,
            ImportField::ObjectId => &mut self.object_id,
            ImportField::Etag => &mut self.etag,
            ImportField::SizeBytes => &mut self.size_bytes,
            ImportField::Collection => &mut self.collection,
            ImportField::DuplicatePolicy => &mut self.duplicate_policy,
        }
    }

    pub fn input(&self) -> Result<ImportPlanInput, String> {
        if self.staging_id.trim().is_empty()
            || self.object_id.trim().is_empty()
            || self.etag.trim().is_empty()
            || self.collection.trim().is_empty()
        {
            return Err("Import plan fields are incomplete".to_string());
        }
        let size_bytes = self
            .size_bytes
            .trim()
            .parse::<u64>()
            .map_err(|_| "Source size must be a non-negative integer".to_string())?;
        if !matches!(self.duplicate_policy.as_str(), "reject" | "skip" | "update") {
            return Err("Duplicate policy must be reject, skip, or update".to_string());
        }
        Ok(ImportPlanInput {
            staging_id: self.staging_id.trim().to_string(),
            object_id: self.object_id.trim().to_string(),
            etag: self.etag.trim().to_string(),
            size_bytes,
            collection: self.collection.trim().to_string(),
            duplicate_policy: self.duplicate_policy.clone(),
        })
    }
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
