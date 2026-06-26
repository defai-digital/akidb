//! Background task scheduler for AkiDB
//!
//! Provides unified scheduling for background operations like snapshots,
//! index rebuilds, and cleanup tasks with resource awareness.
//!
//! ## Features
//!
//! - **Resource Governor**: Monitors P95 latency, CPU, and memory to protect search performance
//! - **Task Scheduling**: Cron-like and interval-based scheduling
//! - **Failure Handling**: Configurable retry with exponential backoff
//! - **Observability**: Task state tracking and metrics
//!
//! ## Usage
//!
//! ```ignore
//! use akidb_common::scheduler::{
//!     BackgroundTask, TaskSchedule, ResourceRequirements, TaskContext, TaskResult,
//!     ResourceGovernor, ResourceGovernorConfig, SimpleMetricsSource,
//! };
//!
//! // Define a task
//! struct MyCleanupTask { /* ... */ }
//!
//! #[async_trait]
//! impl BackgroundTask for MyCleanupTask {
//!     fn task_type(&self) -> &'static str { "cleanup" }
//!     fn task_id(&self) -> &str { "cleanup-old-files" }
//!     fn schedule(&self) -> TaskSchedule {
//!         TaskSchedule::Cron("0 3 * * *".to_string()) // Daily at 3 AM
//!     }
//!     fn resource_requirements(&self) -> ResourceRequirements {
//!         ResourceRequirements::low()
//!     }
//!     async fn execute(&self, ctx: &TaskContext) -> TaskResult {
//!         // ... do cleanup
//!         TaskResult::success()
//!     }
//! }
//!
//! // Use resource governor
//! let metrics = Arc::new(SimpleMetricsSource::new());
//! let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), metrics);
//!
//! // Check before starting task
//! if governor.can_start(&task.resource_requirements()) {
//!     let result = task.execute(&ctx).await;
//! }
//! ```

pub mod governor;
pub mod task;

// Re-export main types
pub use governor::{
    MetricsSource, ResourceGovernor, ResourceGovernorConfig, ResourceSummary, RunningTask,
    SimpleMetricsSource,
};

pub use task::{
    BackgroundTask, FailureAction, ResourceRequirements, TaskContext, TaskExecution, TaskResult,
    TaskSchedule, TaskState,
};
