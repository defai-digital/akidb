//! Admin gRPC service for background task management
//!
//! Provides endpoints for:
//! - Monitoring background task status
//! - Viewing task execution history
//! - Triggering snapshots and rebuilds
//! - Managing webhook alerts

use crate::metrics;
use crate::proto::admin_service_server::AdminService;
use crate::proto::{
    BackgroundTaskInfo, CancelTaskRequest, CancelTaskResponse, ConfigureWebhookRequest,
    ConfigureWebhookResponse, GetBackgroundTaskStatusRequest, GetBackgroundTaskStatusResponse,
    GetResourceStatusRequest, GetResourceStatusResponse, GetTaskHistoryRequest,
    GetTaskHistoryResponse, GetWebhookConfigRequest, GetWebhookConfigResponse, RebuildStatus,
    ResourceRequirementsInfo, ResourceStatus, RunningTaskInfo, SnapshotStatus, TaskExecutionInfo,
    TaskExecutionRecord, TaskScheduleInfo, TaskStatus, TriggerRebuildRequest, TriggerRebuildResponse,
    TriggerSnapshotRequest, TriggerSnapshotResponse, WebhookEvent,
};
use akidb_common::scheduler::{
    ResourceGovernor, ResourceRequirements, ResourceSummary, RunningTask, TaskExecution,
    TaskSchedule, TaskState,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

/// Webhook configuration
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub url: String,
    pub events: Vec<WebhookEvent>,
    pub secret: Option<String>,
    pub enabled: bool,
    pub last_delivery: Option<Instant>,
    pub last_delivery_success: Option<bool>,
}

/// Admin service state
pub struct AdminState {
    /// Resource governor reference
    pub governor: Arc<ResourceGovernor>,
    /// Task execution history
    pub task_history: RwLock<Vec<TaskExecution>>,
    /// Maximum history entries
    pub max_history_entries: usize,
    /// Registered tasks (for status queries)
    pub registered_tasks: RwLock<HashMap<String, RegisteredTask>>,
    /// Webhook configuration
    pub webhook_config: RwLock<Option<WebhookConfig>>,
    /// Snapshot trigger callback
    pub snapshot_trigger: Option<Box<dyn Fn(bool, Option<String>) -> Result<String, String> + Send + Sync>>,
    /// Rebuild trigger callback
    pub rebuild_trigger: Option<Box<dyn Fn(bool, Option<String>, bool) -> Result<String, String> + Send + Sync>>,
    /// Task cancel callback
    pub task_canceller: Option<Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>>,
}

/// Registered task info
#[derive(Debug, Clone)]
pub struct RegisteredTask {
    pub task_type: String,
    pub task_id: String,
    pub description: String,
    pub schedule: TaskSchedule,
    pub requirements: ResourceRequirements,
    pub enabled: bool,
    pub current_execution: Option<TaskExecution>,
    pub last_execution: Option<TaskExecution>,
}

impl AdminState {
    /// Create new admin state
    pub fn new(governor: Arc<ResourceGovernor>) -> Self {
        Self {
            governor,
            task_history: RwLock::new(Vec::new()),
            max_history_entries: 1000,
            registered_tasks: RwLock::new(HashMap::new()),
            webhook_config: RwLock::new(None),
            snapshot_trigger: None,
            rebuild_trigger: None,
            task_canceller: None,
        }
    }

    /// Register a task for status tracking
    pub fn register_task(&self, task: RegisteredTask) {
        let key = format!("{}:{}", task.task_type, task.task_id);
        self.registered_tasks.write().insert(key, task);
    }

    /// Update task execution state
    pub fn update_task_execution(&self, task_type: &str, task_id: &str, execution: TaskExecution) {
        let key = format!("{}:{}", task_type, task_id);
        if let Some(task) = self.registered_tasks.write().get_mut(&key) {
            if execution.state == TaskState::Running {
                task.current_execution = Some(execution.clone());
            } else {
                task.current_execution = None;
                task.last_execution = Some(execution.clone());
            }
        }

        // Add to history if completed
        if execution.state != TaskState::Running && execution.state != TaskState::Pending {
            let mut history = self.task_history.write();
            history.push(execution);
            // Trim history if needed
            while history.len() > self.max_history_entries {
                history.remove(0);
            }
        }
    }

    /// Get current snapshot state (placeholder - would integrate with actual snapshot state machine)
    pub fn get_snapshot_state(&self) -> i32 {
        // In a real implementation, this would query the actual snapshot state machine
        0 // SNAPSHOT_IDLE
    }

    /// Get current rebuild state (placeholder - would integrate with actual rebuild state machine)
    pub fn get_rebuild_state(&self) -> i32 {
        // In a real implementation, this would query the actual rebuild state machine
        0 // REBUILD_IDLE
    }

    /// Send webhook notification
    pub async fn send_webhook(&self, event: WebhookEvent, payload: serde_json::Value) {
        let config = self.webhook_config.read().clone();
        if let Some(config) = config {
            if !config.enabled || !config.events.iter().any(|e| *e == event) {
                return;
            }

            let client = reqwest::Client::new();
            let result = client
                .post(&config.url)
                .json(&serde_json::json!({
                    "event": format!("{:?}", event),
                    "timestamp": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                    "payload": payload,
                }))
                .timeout(Duration::from_secs(10))
                .send()
                .await;

            let success = result.is_ok();
            if let Some(config) = self.webhook_config.write().as_mut() {
                config.last_delivery = Some(Instant::now());
                config.last_delivery_success = Some(success);
            }

            if !success {
                warn!(url = %config.url, event = ?event, "Failed to send webhook notification");
            } else {
                debug!(url = %config.url, event = ?event, "Sent webhook notification");
            }
        }
    }
}

/// Admin gRPC service implementation
pub struct AdminServiceImpl {
    state: Arc<AdminState>,
}

impl AdminServiceImpl {
    pub fn new(state: Arc<AdminState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn get_background_task_status(
        &self,
        request: Request<GetBackgroundTaskStatusRequest>,
    ) -> Result<Response<GetBackgroundTaskStatusResponse>, Status> {
        let req = request.into_inner();
        let tasks = self.state.registered_tasks.read();

        let filtered: Vec<BackgroundTaskInfo> = tasks
            .values()
            .filter(|t| {
                if let Some(ref task_type) = req.task_type {
                    if &t.task_type != task_type {
                        return false;
                    }
                }
                if let Some(ref task_id) = req.task_id {
                    if &t.task_id != task_id {
                        return false;
                    }
                }
                true
            })
            .map(|t| registered_task_to_proto(t))
            .collect();

        let summary = self.state.governor.resource_summary();
        let resource_status = resource_summary_to_proto(&summary, &self.state.governor);

        Ok(Response::new(GetBackgroundTaskStatusResponse {
            tasks: filtered,
            resource_status: Some(resource_status),
        }))
    }

    async fn get_task_history(
        &self,
        request: Request<GetTaskHistoryRequest>,
    ) -> Result<Response<GetTaskHistoryResponse>, Status> {
        let req = request.into_inner();
        let history = self.state.task_history.read();
        let limit = if req.limit == 0 { 50 } else { req.limit as usize };
        let status_filter = match req.status_filter {
            Some(status) => Some(task_status_to_state(status)?),
            None => None,
        };

        let filtered: Vec<TaskExecutionRecord> = history
            .iter()
            .rev() // Most recent first
            .filter(|e| {
                if let Some(ref task_type) = req.task_type {
                    if &e.task_type != task_type {
                        return false;
                    }
                }
                if let Some(ref task_id) = req.task_id {
                    if &e.task_id != task_id {
                        return false;
                    }
                }
                if let Some(since) = req.since_timestamp_ms {
                    if (e.started_at as i64) < since {
                        return false;
                    }
                }
                if let Some(expected_state) = &status_filter {
                    if &e.state != expected_state {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .map(|e| task_execution_to_record(e))
            .collect();

        let total_count = filtered.len() as u32;

        Ok(Response::new(GetTaskHistoryResponse {
            executions: filtered,
            total_count,
        }))
    }

    async fn trigger_snapshot(
        &self,
        request: Request<TriggerSnapshotRequest>,
    ) -> Result<Response<TriggerSnapshotResponse>, Status> {
        let req = request.into_inner();

        // Check if snapshot is already in progress
        let current_state = self.state.get_snapshot_state();
        if current_state != 0 && !req.force {
            return Ok(Response::new(TriggerSnapshotResponse {
                accepted: false,
                snapshot_id: String::new(),
                message: "Snapshot already in progress. Use force=true to override.".to_string(),
                current_status: current_state,
            }));
        }

        // Try to trigger snapshot
        if let Some(ref trigger) = self.state.snapshot_trigger {
            match trigger(req.force, req.shard_id.clone()) {
                Ok(snapshot_id) => {
                    info!(snapshot_id = %snapshot_id, "Snapshot triggered via admin API");

                    // Record metric
                    metrics::metrics().task_started("snapshot", &snapshot_id);

                    Ok(Response::new(TriggerSnapshotResponse {
                        accepted: true,
                        snapshot_id,
                        message: "Snapshot started".to_string(),
                        current_status: SnapshotStatus::SnapshotCompressing as i32,
                    }))
                }
                Err(e) => Ok(Response::new(TriggerSnapshotResponse {
                    accepted: false,
                    snapshot_id: String::new(),
                    message: e,
                    current_status: current_state,
                })),
            }
        } else {
            Ok(Response::new(TriggerSnapshotResponse {
                accepted: false,
                snapshot_id: String::new(),
                message: "Snapshot trigger not configured".to_string(),
                current_status: current_state,
            }))
        }
    }

    async fn trigger_rebuild(
        &self,
        request: Request<TriggerRebuildRequest>,
    ) -> Result<Response<TriggerRebuildResponse>, Status> {
        let req = request.into_inner();

        // Check if rebuild is already in progress
        let current_state = self.state.get_rebuild_state();
        if current_state != 0 && !req.force {
            return Ok(Response::new(TriggerRebuildResponse {
                accepted: false,
                rebuild_id: String::new(),
                message: "Rebuild already in progress. Use force=true to override.".to_string(),
                current_status: current_state,
            }));
        }

        // Try to trigger rebuild
        if let Some(ref trigger) = self.state.rebuild_trigger {
            match trigger(req.force, req.shard_id.clone(), req.compact_tombstones) {
                Ok(rebuild_id) => {
                    info!(rebuild_id = %rebuild_id, "Rebuild triggered via admin API");

                    // Record metric
                    metrics::metrics().task_started("rebuild", &rebuild_id);

                    Ok(Response::new(TriggerRebuildResponse {
                        accepted: true,
                        rebuild_id,
                        message: "Rebuild started".to_string(),
                        current_status: RebuildStatus::RebuildPreparing as i32,
                    }))
                }
                Err(e) => Ok(Response::new(TriggerRebuildResponse {
                    accepted: false,
                    rebuild_id: String::new(),
                    message: e,
                    current_status: current_state,
                })),
            }
        } else {
            Ok(Response::new(TriggerRebuildResponse {
                accepted: false,
                rebuild_id: String::new(),
                message: "Rebuild trigger not configured".to_string(),
                current_status: current_state,
            }))
        }
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();
        let mut cancelled = Vec::new();

        if let Some(ref canceller) = self.state.task_canceller {
            // Cancel specific execution
            if !req.execution_id.is_empty() {
                match canceller(&req.execution_id) {
                    Ok(()) => {
                        cancelled.push(req.execution_id.clone());
                        metrics::metrics().task_cancelled("unknown", &req.execution_id);
                    }
                    Err(e) => {
                        return Ok(Response::new(CancelTaskResponse {
                            success: false,
                            message: e,
                            cancelled_execution_ids: vec![],
                        }));
                    }
                }
            }

            // Cancel by type/id
            if req.task_type.is_some() || req.task_id.is_some() {
                let tasks = self.state.registered_tasks.read();
                for task in tasks.values() {
                    if let Some(ref task_type) = req.task_type {
                        if &task.task_type != task_type {
                            continue;
                        }
                    }
                    if let Some(ref task_id) = req.task_id {
                        if &task.task_id != task_id {
                            continue;
                        }
                    }
                    if let Some(ref exec) = task.current_execution {
                        if let Ok(()) = canceller(&exec.execution_id) {
                            cancelled.push(exec.execution_id.clone());
                            metrics::metrics().task_cancelled(&task.task_type, &task.task_id);
                        }
                    }
                }
            }
        }

        let success = !cancelled.is_empty();
        let message = if success {
            format!("Cancelled {} task(s)", cancelled.len())
        } else {
            "No tasks to cancel or canceller not configured".to_string()
        };

        Ok(Response::new(CancelTaskResponse {
            success,
            message,
            cancelled_execution_ids: cancelled,
        }))
    }

    async fn get_resource_status(
        &self,
        _request: Request<GetResourceStatusRequest>,
    ) -> Result<Response<GetResourceStatusResponse>, Status> {
        let summary = self.state.governor.resource_summary();
        let status = resource_summary_to_proto(&summary, &self.state.governor);

        // Update metrics
        metrics::metrics().update_governor_metrics(
            summary.p95_latency_ms,
            summary.cpu_percent,
            summary.memory_mb,
            summary.can_accept_tasks,
        );

        Ok(Response::new(GetResourceStatusResponse {
            status: Some(status),
        }))
    }

    async fn configure_webhook(
        &self,
        request: Request<ConfigureWebhookRequest>,
    ) -> Result<Response<ConfigureWebhookResponse>, Status> {
        let req = request.into_inner();

        // Validate URL
        if req.enabled && req.url.is_empty() {
            return Ok(Response::new(ConfigureWebhookResponse {
                success: false,
                message: "URL is required when enabling webhooks".to_string(),
            }));
        }

        let events = req
            .events
            .into_iter()
            .map(WebhookEvent::try_from)
            .collect::<Result<Vec<_>, _>>();
        let events = match events {
            Ok(events) => events,
            Err(_) => {
                return Ok(Response::new(ConfigureWebhookResponse {
                    success: false,
                    message: "Unknown webhook event".to_string(),
                }));
            }
        };

        let config = WebhookConfig {
            url: req.url,
            events,
            secret: req.secret,
            enabled: req.enabled,
            last_delivery: None,
            last_delivery_success: None,
        };

        *self.state.webhook_config.write() = Some(config);

        info!("Webhook configuration updated via admin API");

        Ok(Response::new(ConfigureWebhookResponse {
            success: true,
            message: "Webhook configuration updated".to_string(),
        }))
    }

    async fn get_webhook_config(
        &self,
        _request: Request<GetWebhookConfigRequest>,
    ) -> Result<Response<GetWebhookConfigResponse>, Status> {
        let config = self.state.webhook_config.read().clone();

        if let Some(config) = config {
            Ok(Response::new(GetWebhookConfigResponse {
                url: Some(config.url),
                events: config.events.into_iter().map(|e| e as i32).collect(),
                enabled: config.enabled,
                last_delivery_timestamp_ms: config.last_delivery.map(|t| {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let elapsed_ms = t.elapsed().as_millis() as i64;
                    // Use saturating subtraction to prevent overflow
                    now_ms.saturating_sub(elapsed_ms)
                }),
                last_delivery_success: config.last_delivery_success,
            }))
        } else {
            Ok(Response::new(GetWebhookConfigResponse {
                url: None,
                events: vec![],
                enabled: false,
                last_delivery_timestamp_ms: None,
                last_delivery_success: None,
            }))
        }
    }
}

// ============================================
// Conversion helpers
// ============================================

fn registered_task_to_proto(task: &RegisteredTask) -> BackgroundTaskInfo {
    BackgroundTaskInfo {
        task_type: task.task_type.clone(),
        task_id: task.task_id.clone(),
        description: task.description.clone(),
        status: task_state_to_status(&task.current_execution, &task.last_execution, task.enabled) as i32,
        schedule: Some(task_schedule_to_proto(&task.schedule)),
        current_execution: task.current_execution.as_ref().map(task_execution_to_info),
        last_execution: task.last_execution.as_ref().map(task_execution_to_info),
        requirements: Some(resource_requirements_to_proto(&task.requirements)),
        enabled: task.enabled,
    }
}

fn task_state_to_status(
    current: &Option<TaskExecution>,
    last: &Option<TaskExecution>,
    enabled: bool,
) -> TaskStatus {
    if !enabled {
        return TaskStatus::Disabled;
    }
    if let Some(exec) = current {
        match exec.state {
            TaskState::Running => TaskStatus::Running,
            TaskState::Pending => TaskStatus::Pending,
            _ => TaskStatus::Idle,
        }
    } else if let Some(exec) = last {
        match exec.state {
            TaskState::Completed => TaskStatus::Completed,
            TaskState::Failed => TaskStatus::Failed,
            TaskState::Cancelled => TaskStatus::Cancelled,
            _ => TaskStatus::Idle,
        }
    } else {
        TaskStatus::Idle
    }
}

fn task_schedule_to_proto(schedule: &TaskSchedule) -> TaskScheduleInfo {
    match schedule {
        TaskSchedule::Cron(expr) => TaskScheduleInfo {
            schedule_type: Some(crate::proto::task_schedule_info::ScheduleType::Cron(
                expr.clone(),
            )),
            next_run_timestamp_ms: None, // Would need cron parser to calculate
        },
        TaskSchedule::Interval(duration) => TaskScheduleInfo {
            schedule_type: Some(crate::proto::task_schedule_info::ScheduleType::IntervalMs(
                duration.as_millis() as u64,
            )),
            next_run_timestamp_ms: None,
        },
        TaskSchedule::Once => TaskScheduleInfo {
            schedule_type: Some(crate::proto::task_schedule_info::ScheduleType::Once(true)),
            next_run_timestamp_ms: None,
        },
        TaskSchedule::Manual => TaskScheduleInfo {
            schedule_type: Some(crate::proto::task_schedule_info::ScheduleType::Manual(true)),
            next_run_timestamp_ms: None,
        },
    }
}

fn task_execution_to_info(exec: &TaskExecution) -> TaskExecutionInfo {
    TaskExecutionInfo {
        execution_id: exec.execution_id.clone(),
        started_at_ms: exec.started_at as i64,
        completed_at_ms: exec.completed_at.map(|t| t as i64),
        status: task_state_to_proto_status(&exec.state) as i32,
        message: exec.result.as_ref().and_then(|r| r.message.clone()),
        error: exec.error.clone(),
        progress: 0.0, // Would need additional tracking
        items_processed: exec.result.as_ref().and_then(|r| r.items_processed),
        bytes_processed: exec.result.as_ref().and_then(|r| r.bytes_processed),
        duration_ms: exec.result.as_ref().map(|r| r.duration_ms).unwrap_or(0),
        retry_attempt: 0, // Would need additional tracking
    }
}

fn task_execution_to_record(exec: &TaskExecution) -> TaskExecutionRecord {
    TaskExecutionRecord {
        execution_id: exec.execution_id.clone(),
        task_type: exec.task_type.clone(),
        task_id: exec.task_id.clone(),
        status: task_state_to_proto_status(&exec.state) as i32,
        started_at_ms: exec.started_at as i64,
        completed_at_ms: exec.completed_at.map(|t| t as i64),
        duration_ms: exec.result.as_ref().map(|r| r.duration_ms).unwrap_or(0),
        message: exec.result.as_ref().and_then(|r| r.message.clone()),
        error: exec.error.clone(),
        items_processed: exec.result.as_ref().and_then(|r| r.items_processed),
        bytes_processed: exec.result.as_ref().and_then(|r| r.bytes_processed),
        retry_attempt: 0,
    }
}

fn task_state_to_proto_status(state: &TaskState) -> TaskStatus {
    match state {
        TaskState::Pending => TaskStatus::Pending,
        TaskState::Running => TaskStatus::Running,
        TaskState::Completed => TaskStatus::Completed,
        TaskState::Failed => TaskStatus::Failed,
        TaskState::Cancelled => TaskStatus::Cancelled,
        TaskState::Disabled => TaskStatus::Disabled,
    }
}

fn task_status_to_state(status: i32) -> Result<TaskState, Status> {
    match TaskStatus::try_from(status) {
        Ok(TaskStatus::Pending) => Ok(TaskState::Pending),
        Ok(TaskStatus::Running) => Ok(TaskState::Running),
        Ok(TaskStatus::Completed) => Ok(TaskState::Completed),
        Ok(TaskStatus::Failed) => Ok(TaskState::Failed),
        Ok(TaskStatus::Cancelled) => Ok(TaskState::Cancelled),
        Ok(TaskStatus::Disabled) => Ok(TaskState::Disabled),
        Ok(TaskStatus::Idle) => Err(Status::invalid_argument(
            "status_filter cannot be TASK_STATUS_IDLE",
        )),
        Err(_) => Err(Status::invalid_argument("unknown status_filter")),
    }
}

fn resource_requirements_to_proto(req: &ResourceRequirements) -> ResourceRequirementsInfo {
    ResourceRequirementsInfo {
        cpu_weight: req.cpu_weight,
        memory_mb: req.memory_mb,
        io_weight: req.io_weight,
        uses_gpu: req.uses_gpu,
    }
}

fn resource_summary_to_proto(summary: &ResourceSummary, governor: &ResourceGovernor) -> ResourceStatus {
    let running = governor.running_tasks();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let running_info: Vec<RunningTaskInfo> = running
        .iter()
        .map(|t| {
            let elapsed_ms = t.started_at.elapsed().as_millis() as i64;
            RunningTaskInfo {
                task_id: t.task_id.clone(),
                task_type: t.task_type.clone(),
                // Use saturating subtraction to prevent overflow
                started_at_ms: now_ms.saturating_sub(elapsed_ms),
                elapsed_ms: t.started_at.elapsed().as_millis() as u64,
                requirements: Some(resource_requirements_to_proto(&t.requirements)),
            }
        })
        .collect();

    ResourceStatus {
        running_tasks: summary.running_tasks as u32,
        max_concurrent_tasks: summary.max_tasks as u32,
        p95_latency_ms: summary.p95_latency_ms,
        latency_threshold_ms: summary.latency_threshold_ms,
        cpu_percent: summary.cpu_percent,
        memory_mb: summary.memory_mb,
        max_memory_mb: 0, // Would need from config
        can_accept_tasks: summary.can_accept_tasks,
        in_cooldown: false, // Would need from governor internal state
        cooldown_remaining_ms: None,
        running: running_info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_common::scheduler::{ResourceGovernorConfig, SimpleMetricsSource};

    fn create_test_state() -> Arc<AdminState> {
        let metrics = Arc::new(SimpleMetricsSource::new());
        let governor = Arc::new(ResourceGovernor::new(
            ResourceGovernorConfig::default(),
            metrics,
        ));
        Arc::new(AdminState::new(governor))
    }

    #[test]
    fn test_register_task() {
        let state = create_test_state();

        let task = RegisteredTask {
            task_type: "snapshot".to_string(),
            task_id: "daily-snapshot".to_string(),
            description: "Daily snapshot task".to_string(),
            schedule: TaskSchedule::Interval(Duration::from_secs(86400)),
            requirements: ResourceRequirements::medium(),
            enabled: true,
            current_execution: None,
            last_execution: None,
        };

        state.register_task(task);

        let tasks = state.registered_tasks.read();
        assert_eq!(tasks.len(), 1);
        assert!(tasks.contains_key("snapshot:daily-snapshot"));
    }

    #[test]
    fn test_task_history() {
        let state = create_test_state();

        let execution = TaskExecution {
            execution_id: "exec-1".to_string(),
            task_type: "snapshot".to_string(),
            task_id: "test".to_string(),
            state: TaskState::Completed,
            started_at: 1000,
            completed_at: Some(2000),
            result: None,
            error: None,
        };

        state.update_task_execution("snapshot", "test", execution);

        let history = state.task_history.read();
        assert_eq!(history.len(), 1);
    }

    #[tokio::test]
    async fn test_task_history_rejects_unknown_status_filter() {
        let state = create_test_state();
        let service = AdminServiceImpl::new(state);

        let result = service
            .get_task_history(Request::new(GetTaskHistoryRequest {
                task_type: None,
                task_id: None,
                limit: 50,
                since_timestamp_ms: None,
                status_filter: Some(999),
            }))
            .await;

        let status = result.expect_err("unknown status filter should be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("status_filter"));
    }

    #[test]
    fn test_webhook_config() {
        let state = create_test_state();

        let config = WebhookConfig {
            url: "https://example.com/webhook".to_string(),
            events: vec![WebhookEvent::WebhookTaskFailed],
            secret: Some("secret".to_string()),
            enabled: true,
            last_delivery: None,
            last_delivery_success: None,
        };

        *state.webhook_config.write() = Some(config);

        let stored = state.webhook_config.read();
        assert!(stored.is_some());
        assert_eq!(stored.as_ref().unwrap().url, "https://example.com/webhook");
    }

    #[tokio::test]
    async fn test_configure_webhook_rejects_unknown_event() {
        let state = create_test_state();
        let service = AdminServiceImpl::new(state.clone());

        let response = service
            .configure_webhook(Request::new(ConfigureWebhookRequest {
                url: "https://example.com/webhook".to_string(),
                events: vec![999],
                secret: None,
                enabled: true,
            }))
            .await
            .expect("configure_webhook should return a response")
            .into_inner();

        assert!(!response.success);
        assert!(response.message.contains("event"));
        assert!(state.webhook_config.read().is_none());
    }
}
