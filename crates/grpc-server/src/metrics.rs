//! Prometheus metrics for AkiDB
//!
//! Provides comprehensive metrics for:
//! - Request operations (insert, search, delete, etc.)
//! - Vector index state
//! - Background task execution
//! - Resource utilization

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::OnceLock;

/// Global metrics registry
static METRICS: OnceLock<AkiDbMetrics> = OnceLock::new();

/// Get the global metrics instance
pub fn metrics() -> &'static AkiDbMetrics {
    METRICS.get_or_init(AkiDbMetrics::new)
}

/// AkiDB metrics
pub struct AkiDbMetrics {
    // ============================================
    // Request Metrics
    // ============================================
    /// Request counter by operation type
    pub requests_total: CounterVec,
    /// Request latency histogram by operation type
    pub request_latency: HistogramVec,

    // ============================================
    // Vector Index Metrics
    // ============================================
    /// Active vectors gauge
    pub active_vectors: Gauge,
    /// Tombstoned vectors gauge
    pub tombstoned_vectors: Gauge,
    /// GPU memory usage gauge
    pub gpu_memory_bytes: Gauge,
    /// Write buffer size gauge
    pub write_buffer_size: Gauge,
    /// Flush lag histogram
    pub flush_lag_ms: Histogram,
    /// Read-your-writes violations counter
    pub ryw_violations: Counter,
    /// SLO breaches counter by type
    pub slo_breaches: CounterVec,
    /// Rebuild in progress gauge
    pub rebuild_in_progress: Gauge,

    // ============================================
    // Background Task Metrics
    // ============================================
    /// Current state of background tasks (by task_type, task_id)
    /// Labels: task_type, task_id, state (pending/running/completed/failed/cancelled)
    pub background_task_state: IntGaugeVec,
    /// Task execution counter (by task_type, status)
    pub background_task_executions_total: IntCounterVec,
    /// Task execution duration histogram (by task_type)
    pub background_task_duration_seconds: HistogramVec,
    /// Current number of running background tasks
    pub background_tasks_running: IntGauge,
    /// Task progress gauge for long-running tasks (0-100)
    pub background_task_progress: GaugeVec,
    /// Items processed by background tasks
    pub background_task_items_processed: CounterVec,
    /// Bytes processed by background tasks
    pub background_task_bytes_processed: CounterVec,
    /// Task failures by reason
    pub background_task_failures: IntCounterVec,
    /// Task retry attempts
    pub background_task_retries: IntCounterVec,

    // ============================================
    // Snapshot Metrics
    // ============================================
    /// Snapshot state (0=idle, 1=compressing, 2=uploading, 3=verifying, 4=completing, 5=failed, 6=completed)
    pub snapshot_state: IntGauge,
    /// Snapshot upload progress (0-100)
    pub snapshot_upload_progress: Gauge,
    /// Snapshot upload bytes
    pub snapshot_upload_bytes: Counter,
    /// Snapshot total size bytes
    pub snapshot_total_bytes: Gauge,
    /// Snapshot duration histogram
    pub snapshot_duration_seconds: Histogram,
    /// Snapshot success/failure counter
    pub snapshot_operations_total: IntCounterVec,

    // ============================================
    // Index Rebuild Metrics
    // ============================================
    /// Rebuild phase (0=idle, 1=preparing, 2=scanning, 3=building, 4=replaying, 5=validating, 6=swapping, 7=cleaning)
    pub rebuild_phase: IntGauge,
    /// Rebuild progress (0-100)
    pub rebuild_progress: Gauge,
    /// Vectors processed during rebuild
    pub rebuild_vectors_processed: Counter,
    /// Total vectors to process in rebuild
    pub rebuild_vectors_total: Gauge,
    /// Rebuild duration histogram
    pub rebuild_duration_seconds: Histogram,
    /// Rebuild success/failure counter
    pub rebuild_operations_total: IntCounterVec,

    // ============================================
    // Resource Governor Metrics
    // ============================================
    /// Current P95 latency as seen by governor
    pub governor_p95_latency_ms: Gauge,
    /// Governor CPU usage percentage
    pub governor_cpu_percent: Gauge,
    /// Governor memory usage MB
    pub governor_memory_mb: Gauge,
    /// Task deferrals due to resource constraints
    pub governor_deferrals_total: IntCounterVec,
    /// Whether governor can accept new tasks
    pub governor_can_accept_tasks: IntGauge,
}

impl AkiDbMetrics {
    fn new() -> Self {
        Self {
            // ============================================
            // Request Metrics
            // ============================================
            requests_total: CounterVec::new(
                Opts::new("akidb_requests_total", "Total number of requests"),
                &["operation", "status"],
            )
            .unwrap(),

            request_latency: HistogramVec::new(
                HistogramOpts::new("akidb_request_latency_seconds", "Request latency in seconds")
                    .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
                &["operation"],
            )
            .unwrap(),

            // ============================================
            // Vector Index Metrics
            // ============================================
            active_vectors: Gauge::new("akidb_active_vectors", "Number of active vectors").unwrap(),

            tombstoned_vectors: Gauge::new(
                "akidb_tombstoned_vectors",
                "Number of tombstoned vectors",
            )
            .unwrap(),

            gpu_memory_bytes: Gauge::new("akidb_gpu_memory_bytes", "GPU memory usage in bytes")
                .unwrap(),

            write_buffer_size: Gauge::new(
                "akidb_write_buffer_size",
                "Number of vectors pending flush",
            )
            .unwrap(),

            flush_lag_ms: Histogram::with_opts(
                HistogramOpts::new("akidb_flush_lag_ms", "Time from insert to searchable")
                    .buckets(vec![10.0, 25.0, 50.0, 100.0, 200.0, 500.0]),
            )
            .unwrap(),

            ryw_violations: Counter::new(
                "akidb_read_your_writes_violations",
                "Queries within 100ms that missed recently inserted vectors",
            )
            .unwrap(),

            slo_breaches: CounterVec::new(
                Opts::new("akidb_slo_breaches_total", "SLO breach counter"),
                &["breach_type"], // "soft", "hard"
            )
            .unwrap(),

            rebuild_in_progress: Gauge::new(
                "akidb_rebuild_in_progress",
                "1 if rebuild is in progress, 0 otherwise",
            )
            .unwrap(),

            // ============================================
            // Background Task Metrics
            // ============================================
            background_task_state: IntGaugeVec::new(
                Opts::new(
                    "akidb_background_task_state",
                    "Current state of background task (1=active in this state)",
                ),
                &["task_type", "task_id", "state"],
            )
            .unwrap(),

            background_task_executions_total: IntCounterVec::new(
                Opts::new(
                    "akidb_background_task_executions_total",
                    "Total background task executions",
                ),
                &["task_type", "status"], // status: success, failure, cancelled
            )
            .unwrap(),

            background_task_duration_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "akidb_background_task_duration_seconds",
                    "Background task execution duration",
                )
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0]),
                &["task_type"],
            )
            .unwrap(),

            background_tasks_running: IntGauge::new(
                "akidb_background_tasks_running",
                "Number of currently running background tasks",
            )
            .unwrap(),

            background_task_progress: GaugeVec::new(
                Opts::new(
                    "akidb_background_task_progress",
                    "Progress of background task (0-100)",
                ),
                &["task_type", "task_id"],
            )
            .unwrap(),

            background_task_items_processed: CounterVec::new(
                Opts::new(
                    "akidb_background_task_items_processed",
                    "Items processed by background tasks",
                ),
                &["task_type"],
            )
            .unwrap(),

            background_task_bytes_processed: CounterVec::new(
                Opts::new(
                    "akidb_background_task_bytes_processed",
                    "Bytes processed by background tasks",
                ),
                &["task_type"],
            )
            .unwrap(),

            background_task_failures: IntCounterVec::new(
                Opts::new(
                    "akidb_background_task_failures_total",
                    "Background task failures by reason",
                ),
                &["task_type", "reason"],
            )
            .unwrap(),

            background_task_retries: IntCounterVec::new(
                Opts::new(
                    "akidb_background_task_retries_total",
                    "Background task retry attempts",
                ),
                &["task_type"],
            )
            .unwrap(),

            // ============================================
            // Snapshot Metrics
            // ============================================
            snapshot_state: IntGauge::new(
                "akidb_snapshot_state",
                "Current snapshot state (0=idle, 1=compressing, 2=uploading, 3=verifying, 4=completing, 5=failed, 6=completed)",
            )
            .unwrap(),

            snapshot_upload_progress: Gauge::new(
                "akidb_snapshot_upload_progress",
                "Snapshot upload progress percentage (0-100)",
            )
            .unwrap(),

            snapshot_upload_bytes: Counter::new(
                "akidb_snapshot_upload_bytes_total",
                "Total bytes uploaded for snapshots",
            )
            .unwrap(),

            snapshot_total_bytes: Gauge::new(
                "akidb_snapshot_total_bytes",
                "Total size of current snapshot in bytes",
            )
            .unwrap(),

            snapshot_duration_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "akidb_snapshot_duration_seconds",
                    "Snapshot operation duration",
                )
                .buckets(vec![10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 3600.0]),
            )
            .unwrap(),

            snapshot_operations_total: IntCounterVec::new(
                Opts::new(
                    "akidb_snapshot_operations_total",
                    "Total snapshot operations by result",
                ),
                &["result"], // success, failure, resumed
            )
            .unwrap(),

            // ============================================
            // Index Rebuild Metrics
            // ============================================
            rebuild_phase: IntGauge::new(
                "akidb_rebuild_phase",
                "Current rebuild phase (0=idle, 1=preparing, 2=scanning, 3=building, 4=replaying, 5=validating, 6=swapping, 7=cleaning)",
            )
            .unwrap(),

            rebuild_progress: Gauge::new(
                "akidb_rebuild_progress",
                "Rebuild progress percentage (0-100)",
            )
            .unwrap(),

            rebuild_vectors_processed: Counter::new(
                "akidb_rebuild_vectors_processed_total",
                "Vectors processed during rebuild",
            )
            .unwrap(),

            rebuild_vectors_total: Gauge::new(
                "akidb_rebuild_vectors_total",
                "Total vectors to process in current rebuild",
            )
            .unwrap(),

            rebuild_duration_seconds: Histogram::with_opts(
                HistogramOpts::new("akidb_rebuild_duration_seconds", "Rebuild operation duration")
                    .buckets(vec![60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 14400.0]),
            )
            .unwrap(),

            rebuild_operations_total: IntCounterVec::new(
                Opts::new(
                    "akidb_rebuild_operations_total",
                    "Total rebuild operations by result",
                ),
                &["result"], // success, failure, resumed
            )
            .unwrap(),

            // ============================================
            // Resource Governor Metrics
            // ============================================
            governor_p95_latency_ms: Gauge::new(
                "akidb_governor_p95_latency_ms",
                "P95 latency as seen by resource governor",
            )
            .unwrap(),

            governor_cpu_percent: Gauge::new(
                "akidb_governor_cpu_percent",
                "CPU usage percentage tracked by governor",
            )
            .unwrap(),

            governor_memory_mb: Gauge::new(
                "akidb_governor_memory_mb",
                "Memory usage in MB tracked by governor",
            )
            .unwrap(),

            governor_deferrals_total: IntCounterVec::new(
                Opts::new(
                    "akidb_governor_deferrals_total",
                    "Task deferrals due to resource constraints",
                ),
                &["reason"], // latency, cpu, memory, concurrent_limit, cooldown
            )
            .unwrap(),

            governor_can_accept_tasks: IntGauge::new(
                "akidb_governor_can_accept_tasks",
                "Whether governor can accept new tasks (1=yes, 0=no)",
            )
            .unwrap(),
        }
    }

    /// Register all metrics with a Prometheus registry
    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        // Request metrics
        registry.register(Box::new(self.requests_total.clone()))?;
        registry.register(Box::new(self.request_latency.clone()))?;

        // Vector index metrics
        registry.register(Box::new(self.active_vectors.clone()))?;
        registry.register(Box::new(self.tombstoned_vectors.clone()))?;
        registry.register(Box::new(self.gpu_memory_bytes.clone()))?;
        registry.register(Box::new(self.write_buffer_size.clone()))?;
        registry.register(Box::new(self.flush_lag_ms.clone()))?;
        registry.register(Box::new(self.ryw_violations.clone()))?;
        registry.register(Box::new(self.slo_breaches.clone()))?;
        registry.register(Box::new(self.rebuild_in_progress.clone()))?;

        // Background task metrics
        registry.register(Box::new(self.background_task_state.clone()))?;
        registry.register(Box::new(self.background_task_executions_total.clone()))?;
        registry.register(Box::new(self.background_task_duration_seconds.clone()))?;
        registry.register(Box::new(self.background_tasks_running.clone()))?;
        registry.register(Box::new(self.background_task_progress.clone()))?;
        registry.register(Box::new(self.background_task_items_processed.clone()))?;
        registry.register(Box::new(self.background_task_bytes_processed.clone()))?;
        registry.register(Box::new(self.background_task_failures.clone()))?;
        registry.register(Box::new(self.background_task_retries.clone()))?;

        // Snapshot metrics
        registry.register(Box::new(self.snapshot_state.clone()))?;
        registry.register(Box::new(self.snapshot_upload_progress.clone()))?;
        registry.register(Box::new(self.snapshot_upload_bytes.clone()))?;
        registry.register(Box::new(self.snapshot_total_bytes.clone()))?;
        registry.register(Box::new(self.snapshot_duration_seconds.clone()))?;
        registry.register(Box::new(self.snapshot_operations_total.clone()))?;

        // Rebuild metrics
        registry.register(Box::new(self.rebuild_phase.clone()))?;
        registry.register(Box::new(self.rebuild_progress.clone()))?;
        registry.register(Box::new(self.rebuild_vectors_processed.clone()))?;
        registry.register(Box::new(self.rebuild_vectors_total.clone()))?;
        registry.register(Box::new(self.rebuild_duration_seconds.clone()))?;
        registry.register(Box::new(self.rebuild_operations_total.clone()))?;

        // Governor metrics
        registry.register(Box::new(self.governor_p95_latency_ms.clone()))?;
        registry.register(Box::new(self.governor_cpu_percent.clone()))?;
        registry.register(Box::new(self.governor_memory_mb.clone()))?;
        registry.register(Box::new(self.governor_deferrals_total.clone()))?;
        registry.register(Box::new(self.governor_can_accept_tasks.clone()))?;

        Ok(())
    }

    // ============================================
    // Request Metric Helpers
    // ============================================

    /// Record a request
    pub fn record_request(&self, operation: &str, status: &str, latency_secs: f64) {
        self.requests_total
            .with_label_values(&[operation, status])
            .inc();
        self.request_latency
            .with_label_values(&[operation])
            .observe(latency_secs);
    }

    /// Update vector counts
    pub fn update_vector_counts(&self, active: u64, tombstoned: u64) {
        self.active_vectors.set(active as f64);
        self.tombstoned_vectors.set(tombstoned as f64);
    }

    /// Record SLO breach
    pub fn record_slo_breach(&self, breach_type: &str) {
        self.slo_breaches.with_label_values(&[breach_type]).inc();
    }

    // ============================================
    // Background Task Metric Helpers
    // ============================================

    /// Record task state change
    pub fn set_task_state(&self, task_type: &str, task_id: &str, state: &str) {
        // Reset all states for this task to 0
        for s in &["pending", "running", "completed", "failed", "cancelled"] {
            self.background_task_state
                .with_label_values(&[task_type, task_id, s])
                .set(0);
        }
        // Set the current state to 1
        self.background_task_state
            .with_label_values(&[task_type, task_id, state])
            .set(1);
    }

    /// Record task started
    pub fn task_started(&self, task_type: &str, task_id: &str) {
        self.set_task_state(task_type, task_id, "running");
        self.background_tasks_running.inc();
    }

    /// Record task completed
    pub fn task_completed(
        &self,
        task_type: &str,
        task_id: &str,
        duration_secs: f64,
        success: bool,
    ) {
        let status = if success { "success" } else { "failure" };
        self.set_task_state(
            task_type,
            task_id,
            if success { "completed" } else { "failed" },
        );
        self.background_task_executions_total
            .with_label_values(&[task_type, status])
            .inc();
        self.background_task_duration_seconds
            .with_label_values(&[task_type])
            .observe(duration_secs);
        self.background_tasks_running.dec();
        // Clear progress
        self.background_task_progress
            .with_label_values(&[task_type, task_id])
            .set(0.0);
    }

    /// Record task cancelled
    pub fn task_cancelled(&self, task_type: &str, task_id: &str) {
        self.set_task_state(task_type, task_id, "cancelled");
        self.background_task_executions_total
            .with_label_values(&[task_type, "cancelled"])
            .inc();
        self.background_tasks_running.dec();
        self.background_task_progress
            .with_label_values(&[task_type, task_id])
            .set(0.0);
    }

    /// Update task progress
    pub fn set_task_progress(&self, task_type: &str, task_id: &str, progress: f64) {
        self.background_task_progress
            .with_label_values(&[task_type, task_id])
            .set(progress.clamp(0.0, 100.0));
    }

    /// Record task items processed
    pub fn add_task_items_processed(&self, task_type: &str, count: u64) {
        self.background_task_items_processed
            .with_label_values(&[task_type])
            .inc_by(count as f64);
    }

    /// Record task bytes processed
    pub fn add_task_bytes_processed(&self, task_type: &str, bytes: u64) {
        self.background_task_bytes_processed
            .with_label_values(&[task_type])
            .inc_by(bytes as f64);
    }

    /// Record task failure
    pub fn record_task_failure(&self, task_type: &str, reason: &str) {
        self.background_task_failures
            .with_label_values(&[task_type, reason])
            .inc();
    }

    /// Record task retry
    pub fn record_task_retry(&self, task_type: &str) {
        self.background_task_retries
            .with_label_values(&[task_type])
            .inc();
    }

    // ============================================
    // Snapshot Metric Helpers
    // ============================================

    /// Set snapshot state
    pub fn set_snapshot_state(&self, state: i64) {
        self.snapshot_state.set(state);
    }

    /// Update snapshot progress
    pub fn set_snapshot_progress(&self, progress: f64, uploaded_bytes: u64, total_bytes: u64) {
        self.snapshot_upload_progress
            .set(progress.clamp(0.0, 100.0));
        self.snapshot_total_bytes.set(total_bytes as f64);
        self.snapshot_upload_bytes.inc_by(uploaded_bytes as f64);
    }

    /// Record snapshot completed
    pub fn snapshot_completed(&self, success: bool, duration_secs: f64) {
        let result = if success { "success" } else { "failure" };
        self.snapshot_operations_total
            .with_label_values(&[result])
            .inc();
        self.snapshot_duration_seconds.observe(duration_secs);
        self.snapshot_state.set(if success { 6 } else { 5 }); // 6=completed, 5=failed
        self.snapshot_upload_progress.set(0.0);
    }

    /// Record snapshot resumed
    pub fn snapshot_resumed(&self) {
        self.snapshot_operations_total
            .with_label_values(&["resumed"])
            .inc();
    }

    // ============================================
    // Rebuild Metric Helpers
    // ============================================

    /// Set rebuild phase
    pub fn set_rebuild_phase(&self, phase: i64) {
        self.rebuild_phase.set(phase);
        self.rebuild_in_progress
            .set(if phase > 0 && phase < 7 { 1.0 } else { 0.0 });
    }

    /// Update rebuild progress
    pub fn set_rebuild_progress(&self, progress: f64, vectors_processed: u64, total_vectors: u64) {
        self.rebuild_progress.set(progress.clamp(0.0, 100.0));
        self.rebuild_vectors_total.set(total_vectors as f64);
        self.rebuild_vectors_processed
            .inc_by(vectors_processed as f64);
    }

    /// Record rebuild completed
    pub fn rebuild_completed(&self, success: bool, duration_secs: f64) {
        let result = if success { "success" } else { "failure" };
        self.rebuild_operations_total
            .with_label_values(&[result])
            .inc();
        self.rebuild_duration_seconds.observe(duration_secs);
        self.rebuild_phase.set(0);
        self.rebuild_in_progress.set(0.0);
        self.rebuild_progress.set(0.0);
    }

    /// Record rebuild resumed
    pub fn rebuild_resumed(&self) {
        self.rebuild_operations_total
            .with_label_values(&["resumed"])
            .inc();
    }

    // ============================================
    // Resource Governor Metric Helpers
    // ============================================

    /// Update governor resource metrics
    pub fn update_governor_metrics(
        &self,
        p95_ms: u64,
        cpu_percent: u32,
        memory_mb: u32,
        can_accept: bool,
    ) {
        self.governor_p95_latency_ms.set(p95_ms as f64);
        self.governor_cpu_percent.set(cpu_percent as f64);
        self.governor_memory_mb.set(memory_mb as f64);
        self.governor_can_accept_tasks
            .set(if can_accept { 1 } else { 0 });
    }

    /// Record task deferral
    pub fn record_deferral(&self, reason: &str) {
        self.governor_deferrals_total
            .with_label_values(&[reason])
            .inc();
    }
}
