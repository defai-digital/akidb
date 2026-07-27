//! Prometheus metrics for AkiDB
//!
//! Provides comprehensive metrics for:
//! - Request operations (insert, search, delete, etc.)
//! - Vector index state
//! - Background task execution
//! - Resource utilization

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::OnceLock;

/// Global metrics registry
static METRICS: OnceLock<AkiDbMetrics> = OnceLock::new();
static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Get the global metrics instance
pub fn metrics() -> &'static AkiDbMetrics {
    METRICS.get_or_init(AkiDbMetrics::new)
}

pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let registry = Registry::new();
        metrics()
            .register(&registry)
            .expect("AkiDB metric registration must be valid");
        registry
    })
}

pub fn export_metrics() -> String {
    let encoder = TextEncoder::new();
    let families = registry().gather();
    let mut output = Vec::new();
    if encoder.encode(&families, &mut output).is_err() {
        return String::new();
    }
    String::from_utf8_lossy(&output).into_owned()
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

    // ============================================
    // Immutable Generation Replica Metrics
    // ============================================
    pub generation_active_info: IntGaugeVec,
    pub replica_applied_sequence: GaugeVec,
    pub replica_lag: GaugeVec,
    pub generation_build_seconds: HistogramVec,
    pub generation_verify_failures_total: IntCounterVec,
    pub mutation_apply_total: IntCounterVec,
    pub mutation_gap_total: IntCounterVec,
    pub replica_route_ready: IntGaugeVec,
    pub replica_rebuild_seconds: HistogramVec,
    pub generation_disk_available_bytes: GaugeVec,
    pub generation_disk_required_bytes: GaugeVec,
    pub generation_disk_admission_rejections_total: IntCounterVec,
    pub generation_gc_runs_total: IntCounterVec,
    pub generation_gc_candidates: IntGaugeVec,
    pub generation_gc_deleted_bytes_total: IntCounterVec,

    // ============================================
    // Authoritative Memory Metrics
    // ============================================
    /// Canonical memory mutations by outcome and durability.
    pub memory_commit_total: IntCounterVec,
    /// End-to-end canonical memory mutation latency.
    pub memory_commit_latency_seconds: HistogramVec,
    /// Latest canonical sequence applied by each mandatory memory projection.
    pub memory_projection_applied_sequence: GaugeVec,
    /// Canonical-to-projection sequence lag.
    pub memory_projection_lag_sequences: GaugeVec,
    /// Ordered projection gaps detected while applying the memory outbox.
    pub memory_projection_gap_total: IntCounterVec,
    /// End-to-end recall latency for a bounded recipe and outcome.
    pub memory_recall_latency_seconds: HistogramVec,
    /// Recall snapshot persistence attempts by outcome.
    pub memory_recall_snapshot_total: IntCounterVec,
    /// Scoped authorization decisions by known capability and outcome.
    pub memory_authorization_decision_total: IntCounterVec,
    /// Quarantined memory candidates by bounded reason class.
    pub memory_quarantine_total: IntCounterVec,
    /// Recall replay attempts by mode and outcome.
    pub memory_replay_total: IntCounterVec,
    /// Reviewed deletion operations by stage and outcome.
    pub memory_deletion_total: IntCounterVec,
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

            generation_active_info: IntGaugeVec::new(
                Opts::new(
                    "akidb_generation_active_info",
                    "Active immutable generation identity (value is always 1)",
                ),
                &["replica_id", "workspace", "collection", "generation_id"],
            )
            .unwrap(),
            replica_applied_sequence: GaugeVec::new(
                Opts::new(
                    "akidb_replica_applied_sequence",
                    "Latest durable mutation sequence reported by this replica",
                ),
                &["replica_id", "workspace", "collection"],
            )
            .unwrap(),
            replica_lag: GaugeVec::new(
                Opts::new(
                    "akidb_replica_lag",
                    "Authoritative sequence minus replica applied sequence",
                ),
                &["replica_id", "workspace", "collection"],
            )
            .unwrap(),
            generation_build_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "akidb_generation_build_seconds",
                    "Immutable generation build duration",
                )
                .buckets(vec![1.0, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0, 3600.0]),
                &["replica_id", "outcome"],
            )
            .unwrap(),
            generation_verify_failures_total: IntCounterVec::new(
                Opts::new(
                    "akidb_generation_verify_failures_total",
                    "Generation build or verification failures",
                ),
                &["replica_id", "reason"],
            )
            .unwrap(),
            mutation_apply_total: IntCounterVec::new(
                Opts::new(
                    "akidb_mutation_apply_total",
                    "Mutation-tail entries processed by outcome",
                ),
                &["replica_id", "outcome"],
            )
            .unwrap(),
            mutation_gap_total: IntCounterVec::new(
                Opts::new(
                    "akidb_mutation_gap_total",
                    "Detected ordered mutation gaps",
                ),
                &["replica_id"],
            )
            .unwrap(),
            replica_route_ready: IntGaugeVec::new(
                Opts::new(
                    "akidb_replica_route_ready",
                    "Whether the local checkpoint is safe for gateway routing",
                ),
                &["replica_id", "workspace", "collection"],
            )
            .unwrap(),
            replica_rebuild_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "akidb_replica_rebuild_seconds",
                    "Blank-volume or replacement replica rebuild duration",
                )
                .buckets(vec![5.0, 30.0, 60.0, 300.0, 900.0, 3600.0, 14400.0]),
                &["replica_id", "outcome"],
            )
            .unwrap(),
            generation_disk_available_bytes: GaugeVec::new(
                Opts::new(
                    "akidb_generation_disk_available_bytes",
                    "Available bytes observed at generation admission",
                ),
                &["replica_id"],
            )
            .unwrap(),
            generation_disk_required_bytes: GaugeVec::new(
                Opts::new(
                    "akidb_generation_disk_required_bytes",
                    "Estimated shadow-build bytes plus required reserve",
                ),
                &["replica_id"],
            )
            .unwrap(),
            generation_disk_admission_rejections_total: IntCounterVec::new(
                Opts::new(
                    "akidb_generation_disk_admission_rejections_total",
                    "Generation builds rejected for insufficient disk headroom",
                ),
                &["replica_id"],
            )
            .unwrap(),
            generation_gc_runs_total: IntCounterVec::new(
                Opts::new(
                    "akidb_generation_gc_runs_total",
                    "Immutable generation retention scans by outcome",
                ),
                &["replica_id", "mode"],
            )
            .unwrap(),
            generation_gc_candidates: IntGaugeVec::new(
                Opts::new(
                    "akidb_generation_gc_candidates",
                    "Old unreferenced immutable generation directories",
                ),
                &["replica_id"],
            )
            .unwrap(),
            generation_gc_deleted_bytes_total: IntCounterVec::new(
                Opts::new(
                    "akidb_generation_gc_deleted_bytes_total",
                    "Bytes removed by safe immutable generation retention",
                ),
                &["replica_id"],
            )
            .unwrap(),

            memory_commit_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_commit_total",
                    "Canonical memory mutations by outcome and durability",
                ),
                &["result", "durability"],
            )
            .unwrap(),
            memory_commit_latency_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "akidb_memory_commit_latency_seconds",
                    "End-to-end canonical memory mutation latency",
                )
                .buckets(vec![
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
                    5.0,
                ]),
                &["durability"],
            )
            .unwrap(),
            memory_projection_applied_sequence: GaugeVec::new(
                Opts::new(
                    "akidb_memory_projection_applied_sequence",
                    "Latest canonical sequence applied by a mandatory memory projection",
                ),
                &["projection"],
            )
            .unwrap(),
            memory_projection_lag_sequences: GaugeVec::new(
                Opts::new(
                    "akidb_memory_projection_lag_sequences",
                    "Canonical sequence minus memory projection applied sequence",
                ),
                &["projection"],
            )
            .unwrap(),
            memory_projection_gap_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_projection_gap_total",
                    "Ordered memory projection gaps detected while applying the outbox",
                ),
                &["projection"],
            )
            .unwrap(),
            memory_recall_latency_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "akidb_memory_recall_latency_seconds",
                    "End-to-end memory recall latency",
                )
                .buckets(vec![
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
                    5.0,
                ]),
                &["recipe", "result"],
            )
            .unwrap(),
            memory_recall_snapshot_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_recall_snapshot_total",
                    "Memory recall snapshot persistence attempts by outcome",
                ),
                &["result"],
            )
            .unwrap(),
            memory_authorization_decision_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_authorization_decision_total",
                    "Scoped memory authorization decisions by capability and outcome",
                ),
                &["capability", "result"],
            )
            .unwrap(),
            memory_quarantine_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_quarantine_total",
                    "Memory candidates quarantined by bounded reason class",
                ),
                &["reason_class"],
            )
            .unwrap(),
            memory_replay_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_replay_total",
                    "Memory recall replay attempts by mode and outcome",
                ),
                &["mode", "result"],
            )
            .unwrap(),
            memory_deletion_total: IntCounterVec::new(
                Opts::new(
                    "akidb_memory_deletion_total",
                    "Reviewed memory deletion operations by stage and outcome",
                ),
                &["stage", "result"],
            )
            .unwrap(),
        }
    }

    /// Register all metrics with a Prometheus registry
    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        akidb_common::register_prometheus_collectors!(
            registry,
            self.requests_total,
            self.request_latency,
            self.active_vectors,
            self.tombstoned_vectors,
            self.gpu_memory_bytes,
            self.write_buffer_size,
            self.flush_lag_ms,
            self.ryw_violations,
            self.slo_breaches,
            self.rebuild_in_progress,
            self.background_task_state,
            self.background_task_executions_total,
            self.background_task_duration_seconds,
            self.background_tasks_running,
            self.background_task_progress,
            self.background_task_items_processed,
            self.background_task_bytes_processed,
            self.background_task_failures,
            self.background_task_retries,
            self.snapshot_state,
            self.snapshot_upload_progress,
            self.snapshot_upload_bytes,
            self.snapshot_total_bytes,
            self.snapshot_duration_seconds,
            self.snapshot_operations_total,
            self.rebuild_phase,
            self.rebuild_progress,
            self.rebuild_vectors_processed,
            self.rebuild_vectors_total,
            self.rebuild_duration_seconds,
            self.rebuild_operations_total,
            self.governor_p95_latency_ms,
            self.governor_cpu_percent,
            self.governor_memory_mb,
            self.governor_deferrals_total,
            self.governor_can_accept_tasks,
            self.generation_active_info,
            self.replica_applied_sequence,
            self.replica_lag,
            self.generation_build_seconds,
            self.generation_verify_failures_total,
            self.mutation_apply_total,
            self.mutation_gap_total,
            self.replica_route_ready,
            self.replica_rebuild_seconds,
            self.generation_disk_available_bytes,
            self.generation_disk_required_bytes,
            self.generation_disk_admission_rejections_total,
            self.generation_gc_runs_total,
            self.generation_gc_candidates,
            self.generation_gc_deleted_bytes_total,
            self.memory_commit_total,
            self.memory_commit_latency_seconds,
            self.memory_projection_applied_sequence,
            self.memory_projection_lag_sequences,
            self.memory_projection_gap_total,
            self.memory_recall_latency_seconds,
            self.memory_recall_snapshot_total,
            self.memory_authorization_decision_total,
            self.memory_quarantine_total,
            self.memory_replay_total,
            self.memory_deletion_total,
        )
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

    pub fn set_active_generation(
        &self,
        replica_id: &str,
        workspace: &str,
        collection: &str,
        generation_id: &str,
    ) {
        self.generation_active_info.reset();
        self.generation_active_info
            .with_label_values(&[replica_id, workspace, collection, generation_id])
            .set(1);
    }

    pub fn update_replica_checkpoint(
        &self,
        replica_id: &str,
        workspace: &str,
        collection: &str,
        applied_sequence: u64,
        required_sequence: u64,
        route_ready: bool,
    ) {
        self.replica_applied_sequence
            .with_label_values(&[replica_id, workspace, collection])
            .set(applied_sequence as f64);
        self.replica_lag
            .with_label_values(&[replica_id, workspace, collection])
            .set(required_sequence.saturating_sub(applied_sequence) as f64);
        self.replica_route_ready
            .with_label_values(&[replica_id, workspace, collection])
            .set(i64::from(route_ready));
    }

    pub fn observe_generation_build(&self, replica_id: &str, outcome: &str, seconds: f64) {
        self.generation_build_seconds
            .with_label_values(&[replica_id, outcome])
            .observe(seconds);
    }

    pub fn observe_disk_admission(
        &self,
        replica_id: &str,
        available_bytes: u64,
        required_bytes: u64,
    ) {
        self.generation_disk_available_bytes
            .with_label_values(&[replica_id])
            .set(available_bytes as f64);
        self.generation_disk_required_bytes
            .with_label_values(&[replica_id])
            .set(required_bytes as f64);
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

    // ============================================
    // Authoritative Memory Metric Helpers
    // ============================================

    pub fn record_memory_commit(&self, result: &str, durability: &str, latency_secs: f64) {
        let result = bounded_commit_result(result);
        let durability = bounded_durability(durability);
        self.memory_commit_total
            .with_label_values(&[result, durability])
            .inc();
        self.memory_commit_latency_seconds
            .with_label_values(&[durability])
            .observe(latency_secs);
    }

    pub fn set_memory_projection_state(
        &self,
        projection: &str,
        applied_sequence: u64,
        canonical_sequence: u64,
    ) {
        let projection = bounded_projection(projection);
        self.memory_projection_applied_sequence
            .with_label_values(&[projection])
            .set(applied_sequence as f64);
        self.memory_projection_lag_sequences
            .with_label_values(&[projection])
            .set(canonical_sequence.saturating_sub(applied_sequence) as f64);
    }

    pub fn record_memory_projection_gap(&self, projection: &str) {
        self.memory_projection_gap_total
            .with_label_values(&[bounded_projection(projection)])
            .inc();
    }

    pub fn record_memory_recall(&self, recipe: &str, result: &str, latency_secs: f64) {
        self.memory_recall_latency_seconds
            .with_label_values(&[bounded_recall_recipe(recipe), bounded_recall_result(result)])
            .observe(latency_secs);
    }

    pub fn record_memory_recall_snapshot(&self, result: &str) {
        self.memory_recall_snapshot_total
            .with_label_values(&[bounded_snapshot_result(result)])
            .inc();
    }

    pub fn record_memory_authorization(&self, capability: &str, result: &str) {
        self.memory_authorization_decision_total
            .with_label_values(&[
                bounded_memory_capability(capability),
                bounded_authorization_result(result),
            ])
            .inc();
    }

    pub fn record_memory_quarantine(&self, reason_class: &str) {
        self.memory_quarantine_total
            .with_label_values(&[bounded_quarantine_reason(reason_class)])
            .inc();
    }

    pub fn record_memory_replay(&self, mode: &str, result: &str) {
        self.memory_replay_total
            .with_label_values(&[bounded_replay_mode(mode), bounded_replay_result(result)])
            .inc();
    }

    pub fn record_memory_deletion(&self, stage: &str, result: &str) {
        self.memory_deletion_total
            .with_label_values(&[
                bounded_deletion_stage(stage),
                bounded_deletion_result(result),
            ])
            .inc();
    }
}

fn bounded_commit_result(value: &str) -> &'static str {
    match value {
        "committed" => "committed",
        "duplicate" => "duplicate",
        _ => "error",
    }
}

fn bounded_durability(value: &str) -> &'static str {
    match value {
        "synced" | "SYNCED" => "synced",
        _ => "unknown",
    }
}

fn bounded_projection(value: &str) -> &'static str {
    match value {
        "canonical:preview-v2" => "canonical:preview-v2",
        "structured:preview-v2" => "structured:preview-v2",
        "lexical:unicode-alnum-bm25-v2" => "lexical:unicode-alnum-bm25-v2",
        _ => "unknown",
    }
}

fn bounded_recall_recipe(value: &str) -> &'static str {
    match value {
        "preview-bounded-bm25-v1" => "preview-bounded-bm25-v1",
        _ => "unknown",
    }
}

fn bounded_recall_result(value: &str) -> &'static str {
    match value {
        "success" => "success",
        _ => "error",
    }
}

fn bounded_snapshot_result(value: &str) -> &'static str {
    match value {
        "success" => "success",
        _ => "error",
    }
}

fn bounded_memory_capability(value: &str) -> &'static str {
    match value {
        "memory.observe" => "memory.observe",
        "memory.propose" => "memory.propose",
        "memory.remember" => "memory.remember",
        "memory.read" => "memory.read",
        "memory.recall" => "memory.recall",
        "memory.replay" => "memory.replay",
        "memory.correct" => "memory.correct",
        "memory.retract" => "memory.retract",
        "memory.forget" => "memory.forget",
        "memory.history" => "memory.history",
        "memory.export" => "memory.export",
        "memory.delete.plan" => "memory.delete.plan",
        "memory.delete.execute" => "memory.delete.execute",
        _ => "unknown",
    }
}

fn bounded_authorization_result(value: &str) -> &'static str {
    match value {
        "allowed" => "allowed",
        _ => "denied",
    }
}

fn bounded_quarantine_reason(value: &str) -> &'static str {
    match value {
        "context_firewall" => "context_firewall",
        _ => "other",
    }
}

fn bounded_replay_mode(value: &str) -> &'static str {
    match value {
        "exact_retained" => "exact_retained",
        "reexecute" => "reexecute",
        _ => "invalid",
    }
}

fn bounded_replay_result(value: &str) -> &'static str {
    match value {
        "success" => "success",
        "exact_match" => "exact_match",
        "mismatch" => "mismatch",
        "expected_nondeterminism" => "expected_nondeterminism",
        "artifact_expired" => "artifact_expired",
        _ => "error",
    }
}

fn bounded_deletion_stage(value: &str) -> &'static str {
    match value {
        "plan" => "plan",
        "execute" => "execute",
        _ => "unknown",
    }
}

fn bounded_deletion_result(value: &str) -> &'static str {
    match value {
        "success" => "success",
        "duplicate" => "duplicate",
        _ => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_metrics_export_required_content_free_families() {
        let metrics = AkiDbMetrics::new();
        let registry = Registry::new();
        metrics.register(&registry).unwrap();

        metrics.record_memory_commit("committed", "SYNCED", 0.01);
        metrics.set_memory_projection_state("canonical:preview-v2", 4, 7);
        metrics.record_memory_projection_gap("structured:preview-v2");
        metrics.record_memory_recall("preview-bounded-bm25-v1", "success", 0.02);
        metrics.record_memory_recall_snapshot("success");
        metrics.record_memory_authorization("memory.recall", "allowed");
        metrics.record_memory_quarantine("context_firewall");
        metrics.record_memory_replay("reexecute", "exact_match");
        metrics.record_memory_deletion("execute", "success");

        let mut output = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        for family in [
            "akidb_memory_commit_total",
            "akidb_memory_commit_latency_seconds",
            "akidb_memory_projection_applied_sequence",
            "akidb_memory_projection_lag_sequences",
            "akidb_memory_projection_gap_total",
            "akidb_memory_recall_latency_seconds",
            "akidb_memory_recall_snapshot_total",
            "akidb_memory_authorization_decision_total",
            "akidb_memory_quarantine_total",
            "akidb_memory_replay_total",
            "akidb_memory_deletion_total",
        ] {
            assert!(output.contains(family), "missing metric family {family}");
        }
    }

    #[test]
    fn memory_metric_labels_reject_unbounded_values() {
        let metrics = AkiDbMetrics::new();
        let registry = Registry::new();
        metrics.register(&registry).unwrap();
        let secret = "secret-workspace-query-and-principal";

        metrics.record_memory_authorization(secret, secret);
        metrics.record_memory_quarantine(secret);
        metrics.record_memory_replay(secret, secret);
        metrics.record_memory_deletion(secret, secret);
        metrics.record_memory_recall(secret, secret, 0.0);
        metrics.set_memory_projection_state(secret, 0, 0);

        let mut output = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(!output.contains(secret));
        assert!(output.contains("capability=\"unknown\""));
        assert!(output.contains("projection=\"unknown\""));
    }
}
