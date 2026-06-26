//! AkiDB Common - Shared types, errors, and utilities
//!
//! This crate provides common functionality used across all AkiDB components.

pub mod config;
pub mod error;
pub mod metrics;
pub mod scheduler;
pub mod types;

pub use error::{AkiDbError, Result};
pub use types::*;

// Re-export scheduler types
pub use scheduler::{
    BackgroundTask, FailureAction, MetricsSource, ResourceGovernor, ResourceGovernorConfig,
    ResourceRequirements, ResourceSummary, RunningTask, SimpleMetricsSource, TaskContext,
    TaskExecution, TaskResult, TaskSchedule, TaskState,
};
