//! Background task trait and types
//!
//! Defines the common interface for all background tasks in AkiDB.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Task schedule definition
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum TaskSchedule {
    /// Run on a cron-like schedule (simplified format: "0 */6 * * *")
    Cron(String),
    /// Run at regular intervals
    Interval(Duration),
    /// Run once and complete
    Once,
    /// Only run when manually triggered
    #[default]
    Manual,
}

/// Resource requirements for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// CPU weight (1-100, relative priority)
    pub cpu_weight: u32,
    /// Expected memory usage in MB
    pub memory_mb: u32,
    /// I/O weight (1-100, relative priority)
    pub io_weight: u32,
    /// Whether this task uses GPU
    pub uses_gpu: bool,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            cpu_weight: 50,
            memory_mb: 100,
            io_weight: 50,
            uses_gpu: false,
        }
    }
}

impl ResourceRequirements {
    /// Create low-priority requirements
    pub fn low() -> Self {
        Self {
            cpu_weight: 20,
            memory_mb: 50,
            io_weight: 20,
            uses_gpu: false,
        }
    }

    /// Create medium-priority requirements
    pub fn medium() -> Self {
        Self::default()
    }

    /// Create high-priority requirements (for critical tasks)
    pub fn high() -> Self {
        Self {
            cpu_weight: 80,
            memory_mb: 500,
            io_weight: 80,
            uses_gpu: false,
        }
    }

    /// Create GPU task requirements
    pub fn gpu() -> Self {
        Self {
            cpu_weight: 60,
            memory_mb: 2000,
            io_weight: 40,
            uses_gpu: true,
        }
    }
}

/// Action to take on task failure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureAction {
    /// Retry with exponential backoff
    Retry { max_attempts: u32, backoff: Vec<Duration> },
    /// Skip this execution, continue schedule
    Skip,
    /// Disable the task
    Disable,
    /// Alert and disable
    AlertAndDisable { webhook_url: Option<String> },
}

impl Default for FailureAction {
    fn default() -> Self {
        FailureAction::Retry {
            max_attempts: 3,
            backoff: vec![
                Duration::from_secs(60),
                Duration::from_secs(300),
                Duration::from_secs(900),
            ],
        }
    }
}

/// Task execution context
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Task execution ID
    pub execution_id: String,
    /// Shard ID (if applicable)
    pub shard_id: Option<String>,
    /// Whether this is a retry
    pub is_retry: bool,
    /// Retry attempt number (0 for first attempt)
    pub retry_attempt: u32,
    /// Cancellation flag
    pub cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl TaskContext {
    /// Create a new task context
    pub fn new(execution_id: String) -> Self {
        Self {
            execution_id,
            shard_id: None,
            is_retry: false,
            retry_attempt: 0,
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Set shard ID
    pub fn with_shard(mut self, shard_id: String) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    /// Mark as retry
    pub fn with_retry(mut self, attempt: u32) -> Self {
        self.is_retry = true;
        self.retry_attempt = attempt;
        self
    }

    /// Check if task was cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Cancel the task
    pub fn cancel(&self) {
        self.cancelled.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// Result of task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Whether the task succeeded
    pub success: bool,
    /// Optional message
    pub message: Option<String>,
    /// Duration of execution
    pub duration_ms: u64,
    /// Items processed (if applicable)
    pub items_processed: Option<u64>,
    /// Bytes processed (if applicable)
    pub bytes_processed: Option<u64>,
}

impl TaskResult {
    /// Create a success result
    pub fn success() -> Self {
        Self {
            success: true,
            message: None,
            duration_ms: 0,
            items_processed: None,
            bytes_processed: None,
        }
    }

    /// Create a success result with message
    pub fn success_with_message(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: Some(message.into()),
            duration_ms: 0,
            items_processed: None,
            bytes_processed: None,
        }
    }

    /// Create a failure result
    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
            duration_ms: 0,
            items_processed: None,
            bytes_processed: None,
        }
    }

    /// Set duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_ms = duration.as_millis() as u64;
        self
    }

    /// Set items processed
    pub fn with_items(mut self, items: u64) -> Self {
        self.items_processed = Some(items);
        self
    }

    /// Set bytes processed
    pub fn with_bytes(mut self, bytes: u64) -> Self {
        self.bytes_processed = Some(bytes);
        self
    }
}

/// Background task trait
#[async_trait]
pub trait BackgroundTask: Send + Sync {
    /// Get the task type identifier
    fn task_type(&self) -> &'static str;

    /// Get the unique task ID
    fn task_id(&self) -> &str;

    /// Get the task schedule
    fn schedule(&self) -> TaskSchedule;

    /// Get resource requirements
    fn resource_requirements(&self) -> ResourceRequirements;

    /// Execute the task
    async fn execute(&self, ctx: &TaskContext) -> TaskResult;

    /// Get failure action
    fn on_failure(&self) -> FailureAction {
        FailureAction::default()
    }

    /// Check if task is enabled
    fn is_enabled(&self) -> bool {
        true
    }

    /// Get human-readable description
    fn description(&self) -> &str {
        self.task_type()
    }
}

/// Task state for tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    /// Task is scheduled but not running
    Pending,
    /// Task is currently running
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed
    Failed,
    /// Task was cancelled
    Cancelled,
    /// Task is disabled
    Disabled,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Pending => write!(f, "pending"),
            TaskState::Running => write!(f, "running"),
            TaskState::Completed => write!(f, "completed"),
            TaskState::Failed => write!(f, "failed"),
            TaskState::Cancelled => write!(f, "cancelled"),
            TaskState::Disabled => write!(f, "disabled"),
        }
    }
}

/// Task execution record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    /// Execution ID
    pub execution_id: String,
    /// Task type
    pub task_type: String,
    /// Task ID
    pub task_id: String,
    /// Current state
    pub state: TaskState,
    /// Started timestamp (Unix seconds)
    pub started_at: u64,
    /// Completed timestamp (Unix seconds)
    pub completed_at: Option<u64>,
    /// Result (if completed)
    pub result: Option<TaskResult>,
    /// Error message (if failed)
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_requirements() {
        let low = ResourceRequirements::low();
        assert_eq!(low.cpu_weight, 20);

        let high = ResourceRequirements::high();
        assert_eq!(high.cpu_weight, 80);

        let gpu = ResourceRequirements::gpu();
        assert!(gpu.uses_gpu);
    }

    #[test]
    fn test_task_result() {
        let result = TaskResult::success()
            .with_duration(Duration::from_secs(10))
            .with_items(1000);

        assert!(result.success);
        assert_eq!(result.duration_ms, 10000);
        assert_eq!(result.items_processed, Some(1000));
    }

    #[test]
    fn test_task_context() {
        let ctx = TaskContext::new("exec-1".to_string())
            .with_shard("shard-0".to_string())
            .with_retry(2);

        assert_eq!(ctx.shard_id, Some("shard-0".to_string()));
        assert!(ctx.is_retry);
        assert_eq!(ctx.retry_attempt, 2);
        assert!(!ctx.is_cancelled());

        ctx.cancel();
        assert!(ctx.is_cancelled());
    }
}
