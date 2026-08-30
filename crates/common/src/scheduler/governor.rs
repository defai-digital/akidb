//! Resource governor for background task scheduling
//!
//! Monitors system resources and decides when tasks can run.

use super::task::ResourceRequirements;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Configuration for resource governor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceGovernorConfig {
    /// Maximum CPU percentage for background tasks (0-100)
    pub max_background_cpu_percent: u32,
    /// Maximum memory for background tasks (MB)
    pub max_background_memory_mb: u32,
    /// P95 latency threshold - defer tasks when exceeded (ms)
    pub defer_when_p95_above_ms: u64,
    /// How often to check resource usage (ms)
    pub check_interval_ms: u64,
    /// Maximum concurrent background tasks
    pub max_concurrent_tasks: usize,
    /// Cooldown period after high latency detected (ms)
    pub cooldown_after_high_latency_ms: u64,
}

impl Default for ResourceGovernorConfig {
    fn default() -> Self {
        Self {
            max_background_cpu_percent: 30,
            max_background_memory_mb: 4096,
            defer_when_p95_above_ms: 40,
            check_interval_ms: 1000,
            max_concurrent_tasks: 2,
            cooldown_after_high_latency_ms: 5000,
        }
    }
}

/// Metrics source trait for resource monitoring
pub trait MetricsSource: Send + Sync {
    /// Get current P95 latency in milliseconds
    fn get_p95_latency_ms(&self) -> u64;

    /// Get current CPU usage percentage (0-100)
    fn get_cpu_usage_percent(&self) -> u32;

    /// Get current memory usage in MB
    fn get_memory_usage_mb(&self) -> u32;

    /// Get current active query count
    fn get_active_queries(&self) -> u32;
}

/// Simple metrics source for testing
pub struct SimpleMetricsSource {
    p95_latency_ms: AtomicU64,
    cpu_usage: AtomicU64,
    memory_usage: AtomicU64,
    active_queries: AtomicU64,
}

impl SimpleMetricsSource {
    pub fn new() -> Self {
        Self {
            p95_latency_ms: AtomicU64::new(10),
            cpu_usage: AtomicU64::new(20),
            memory_usage: AtomicU64::new(1000),
            active_queries: AtomicU64::new(0),
        }
    }

    pub fn set_p95_latency(&self, ms: u64) {
        self.p95_latency_ms.store(ms, Ordering::Release);
    }

    pub fn set_cpu_usage(&self, percent: u32) {
        self.cpu_usage.store(percent as u64, Ordering::Release);
    }

    pub fn set_memory_usage(&self, mb: u32) {
        self.memory_usage.store(mb as u64, Ordering::Release);
    }

    pub fn set_active_queries(&self, count: u32) {
        self.active_queries.store(count as u64, Ordering::Release);
    }
}

impl Default for SimpleMetricsSource {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsSource for SimpleMetricsSource {
    fn get_p95_latency_ms(&self) -> u64 {
        self.p95_latency_ms.load(Ordering::Acquire)
    }

    fn get_cpu_usage_percent(&self) -> u32 {
        self.cpu_usage.load(Ordering::Acquire) as u32
    }

    fn get_memory_usage_mb(&self) -> u32 {
        self.memory_usage.load(Ordering::Acquire) as u32
    }

    fn get_active_queries(&self) -> u32 {
        self.active_queries.load(Ordering::Acquire) as u32
    }
}

/// Running task tracking
#[derive(Debug, Clone)]
pub struct RunningTask {
    pub task_id: String,
    pub task_type: String,
    pub started_at: Instant,
    pub requirements: ResourceRequirements,
}

/// Resource governor for managing background task execution
pub struct ResourceGovernor {
    config: ResourceGovernorConfig,
    metrics: Arc<dyn MetricsSource>,
    running_tasks: parking_lot::RwLock<Vec<RunningTask>>,
    last_high_latency: parking_lot::RwLock<Option<Instant>>,
}

impl ResourceGovernor {
    /// Create a new resource governor
    pub fn new(config: ResourceGovernorConfig, metrics: Arc<dyn MetricsSource>) -> Self {
        Self {
            config,
            metrics,
            running_tasks: parking_lot::RwLock::new(Vec::new()),
            last_high_latency: parking_lot::RwLock::new(None),
        }
    }

    /// Check if a task can start given current resources
    pub fn can_start(&self, requirements: &ResourceRequirements) -> bool {
        // Check concurrent task limit
        let running = self.running_tasks.read();
        if running.len() >= self.config.max_concurrent_tasks {
            debug!(
                running = running.len(),
                max = self.config.max_concurrent_tasks,
                "Cannot start task: max concurrent tasks reached"
            );
            return false;
        }

        // Check cooldown
        if let Some(last_high) = *self.last_high_latency.read() {
            let cooldown = Duration::from_millis(self.config.cooldown_after_high_latency_ms);
            if last_high.elapsed() < cooldown {
                debug!(
                    cooldown_remaining_ms = (cooldown - last_high.elapsed()).as_millis(),
                    "Cannot start task: in cooldown period"
                );
                return false;
            }
        }

        // Check P95 latency
        let p95 = self.metrics.get_p95_latency_ms();
        if p95 > self.config.defer_when_p95_above_ms {
            *self.last_high_latency.write() = Some(Instant::now());
            debug!(
                p95_ms = p95,
                threshold_ms = self.config.defer_when_p95_above_ms,
                "Cannot start task: P95 latency too high"
            );
            return false;
        }

        // Check CPU
        let cpu_used = self.metrics.get_cpu_usage_percent();
        let running_cpu = running.iter().fold(0u32, |total, task| {
            total.saturating_add(task.requirements.cpu_weight)
        });
        let total_cpu = cpu_used
            .saturating_add(running_cpu)
            .saturating_add(requirements.cpu_weight);
        let cpu_budget = self.config.max_background_cpu_percent.saturating_add(50);
        if total_cpu > cpu_budget {
            // +50 for headroom
            debug!(
                cpu_used,
                running_cpu,
                requested = requirements.cpu_weight,
                "Cannot start task: CPU budget exceeded"
            );
            return false;
        }

        // Check memory
        let memory_used = self.metrics.get_memory_usage_mb();
        let running_memory = running.iter().fold(0u32, |total, task| {
            total.saturating_add(task.requirements.memory_mb)
        });
        let total_memory = running_memory.saturating_add(requirements.memory_mb);
        if total_memory > self.config.max_background_memory_mb {
            debug!(
                memory_used,
                running_memory,
                requested = requirements.memory_mb,
                max = self.config.max_background_memory_mb,
                "Cannot start task: memory budget exceeded"
            );
            return false;
        }

        true
    }

    /// Check if running tasks should be paused
    pub fn should_pause(&self) -> bool {
        let p95 = self.metrics.get_p95_latency_ms();
        if p95 > self.config.defer_when_p95_above_ms {
            *self.last_high_latency.write() = Some(Instant::now());
            return true;
        }
        false
    }

    /// Register a task as running
    pub fn register_task(&self, task: RunningTask) {
        let mut running = self.running_tasks.write();
        running.push(task);
        info!(running_count = running.len(), "Registered running task");
    }

    /// Unregister a task
    pub fn unregister_task(&self, task_id: &str) {
        let mut running = self.running_tasks.write();
        if let Some(pos) = running.iter().position(|t| t.task_id == task_id) {
            running.remove(pos);
            info!(task_id, running_count = running.len(), "Unregistered task");
        }
    }

    /// Get list of running tasks
    pub fn running_tasks(&self) -> Vec<RunningTask> {
        self.running_tasks.read().clone()
    }

    /// Get current resource usage summary
    pub fn resource_summary(&self) -> ResourceSummary {
        // Read guard must not outlive this statement: can_start() below
        // re-acquires the same lock, and parking_lot::RwLock is not
        // reentrant, so holding both risks a self-deadlock against a
        // queued writer (register_task/unregister_task).
        let running_tasks = self.running_tasks.read().len();
        ResourceSummary {
            running_tasks,
            max_tasks: self.config.max_concurrent_tasks,
            p95_latency_ms: self.metrics.get_p95_latency_ms(),
            latency_threshold_ms: self.config.defer_when_p95_above_ms,
            cpu_percent: self.metrics.get_cpu_usage_percent(),
            memory_mb: self.metrics.get_memory_usage_mb(),
            can_accept_tasks: self.can_start(&ResourceRequirements::low()),
        }
    }

    /// Wait until resources are available (with timeout)
    pub async fn wait_for_resources(
        &self,
        requirements: &ResourceRequirements,
        timeout: Duration,
    ) -> bool {
        let start = Instant::now();
        let check_interval = Duration::from_millis(self.config.check_interval_ms);

        while start.elapsed() < timeout {
            if self.can_start(requirements) {
                return true;
            }
            tokio::time::sleep(check_interval).await;
        }

        warn!(
            timeout_ms = timeout.as_millis(),
            "Timed out waiting for resources"
        );
        false
    }
}

/// Summary of current resource state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSummary {
    pub running_tasks: usize,
    pub max_tasks: usize,
    pub p95_latency_ms: u64,
    pub latency_threshold_ms: u64,
    pub cpu_percent: u32,
    pub memory_mb: u32,
    pub can_accept_tasks: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_governor_basic() {
        let metrics = Arc::new(SimpleMetricsSource::new());
        let governor = ResourceGovernor::new(
            ResourceGovernorConfig {
                max_concurrent_tasks: 2,
                ..Default::default()
            },
            metrics.clone(),
        );

        // Should allow task initially
        assert!(governor.can_start(&ResourceRequirements::low()));

        // Register two tasks
        governor.register_task(RunningTask {
            task_id: "task-1".to_string(),
            task_type: "test".to_string(),
            started_at: Instant::now(),
            requirements: ResourceRequirements::low(),
        });
        governor.register_task(RunningTask {
            task_id: "task-2".to_string(),
            task_type: "test".to_string(),
            started_at: Instant::now(),
            requirements: ResourceRequirements::low(),
        });

        // Should not allow more tasks
        assert!(!governor.can_start(&ResourceRequirements::low()));

        // Unregister one
        governor.unregister_task("task-1");
        assert!(governor.can_start(&ResourceRequirements::low()));
    }

    #[test]
    fn test_governor_latency_check() {
        let metrics = Arc::new(SimpleMetricsSource::new());
        let governor = ResourceGovernor::new(
            ResourceGovernorConfig {
                defer_when_p95_above_ms: 30,
                ..Default::default()
            },
            metrics.clone(),
        );

        // Low latency - should allow
        metrics.set_p95_latency(20);
        assert!(governor.can_start(&ResourceRequirements::low()));

        // High latency - should defer
        metrics.set_p95_latency(50);
        assert!(!governor.can_start(&ResourceRequirements::low()));
    }

    #[test]
    fn test_governor_cpu_overflow_rejects_task() {
        let metrics = Arc::new(SimpleMetricsSource::new());
        metrics.set_cpu_usage(u32::MAX);

        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), metrics);

        assert!(
            !governor.can_start(&ResourceRequirements::low()),
            "saturated CPU usage must reject new tasks instead of overflowing"
        );
    }

    #[test]
    fn test_governor_memory_overflow_rejects_task() {
        let metrics = Arc::new(SimpleMetricsSource::new());
        let governor = ResourceGovernor::new(
            ResourceGovernorConfig {
                max_concurrent_tasks: 2,
                ..Default::default()
            },
            metrics,
        );

        governor.register_task(RunningTask {
            task_id: "large-task".to_string(),
            task_type: "test".to_string(),
            started_at: Instant::now(),
            requirements: ResourceRequirements {
                memory_mb: u32::MAX,
                ..ResourceRequirements::low()
            },
        });

        assert!(
            !governor.can_start(&ResourceRequirements::low()),
            "saturated running memory must reject new tasks instead of overflowing"
        );
    }

    #[test]
    fn test_resource_summary() {
        let metrics = Arc::new(SimpleMetricsSource::new());
        metrics.set_p95_latency(15);
        metrics.set_cpu_usage(25);
        metrics.set_memory_usage(2000);

        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), metrics);
        let summary = governor.resource_summary();

        assert_eq!(summary.running_tasks, 0);
        assert_eq!(summary.p95_latency_ms, 15);
        assert_eq!(summary.cpu_percent, 25);
        assert!(summary.can_accept_tasks);
    }
}
