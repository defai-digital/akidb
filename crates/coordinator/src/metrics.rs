//! Prometheus metrics for AkiDB Coordinator

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use std::sync::OnceLock;

/// Global coordinator metrics registry
static METRICS: OnceLock<CoordinatorMetrics> = OnceLock::new();

/// Global Prometheus registry - metrics are registered once on init
static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Get the global metrics instance
pub fn metrics() -> &'static CoordinatorMetrics {
    METRICS.get_or_init(CoordinatorMetrics::new)
}

/// Get the global registry (initializes metrics if not already done)
fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let registry = Registry::new();
        // Register metrics on first access
        if let Err(e) = metrics().register(&registry) {
            tracing::error!("Failed to register metrics: {}", e);
        }
        registry
    })
}

/// Coordinator-specific metrics
pub struct CoordinatorMetrics {
    /// Search fanout latency histogram
    pub fanout_latency: HistogramVec,
    /// Shard coverage ratio gauge
    pub shard_coverage: Gauge,
    /// Responding shards per search
    pub responding_shards: Histogram,
    /// Requests by operation
    pub requests_total: CounterVec,
    /// Connection pool size per shard
    pub pool_connections: GaugeVec,
    /// Total pools
    pub pool_count: Gauge,
    /// Shard health gauge (1=healthy, 0=unhealthy)
    pub shard_health: GaugeVec,
    /// Partial results counter
    pub partial_results: Counter,
    /// Insert batch latency
    pub batch_insert_latency: Histogram,
    /// Vectors per shard (for monitoring distribution)
    pub shard_vector_count: GaugeVec,
}

impl CoordinatorMetrics {
    fn new() -> Self {
        Self {
            fanout_latency: HistogramVec::new(
                HistogramOpts::new(
                    "akidb_coordinator_fanout_latency_seconds",
                    "Fan-out search latency in seconds",
                )
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
                &["operation"],
            )
            .unwrap(),

            shard_coverage: Gauge::new(
                "akidb_coordinator_shard_coverage",
                "Ratio of responding shards (0.0-1.0)",
            )
            .unwrap(),

            responding_shards: Histogram::with_opts(
                HistogramOpts::new(
                    "akidb_coordinator_responding_shards",
                    "Number of responding shards per search",
                )
                .buckets(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0]),
            )
            .unwrap(),

            requests_total: CounterVec::new(
                Opts::new(
                    "akidb_coordinator_requests_total",
                    "Total coordinator requests",
                ),
                &["operation", "status"],
            )
            .unwrap(),

            pool_connections: GaugeVec::new(
                Opts::new(
                    "akidb_coordinator_pool_connections",
                    "Connection pool size per shard",
                ),
                &["shard"],
            )
            .unwrap(),

            pool_count: Gauge::new(
                "akidb_coordinator_pool_count",
                "Total number of connection pools",
            )
            .unwrap(),

            shard_health: GaugeVec::new(
                Opts::new("akidb_coordinator_shard_health", "Shard health status"),
                &["shard"],
            )
            .unwrap(),

            partial_results: Counter::new(
                "akidb_coordinator_partial_results_total",
                "Number of searches with partial results",
            )
            .unwrap(),

            batch_insert_latency: Histogram::with_opts(
                HistogramOpts::new(
                    "akidb_coordinator_batch_insert_latency_seconds",
                    "Batch insert latency in seconds",
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
            )
            .unwrap(),

            shard_vector_count: GaugeVec::new(
                Opts::new(
                    "akidb_coordinator_shard_vectors",
                    "Estimated vector count per shard",
                ),
                &["shard"],
            )
            .unwrap(),
        }
    }

    /// Register all metrics with a Prometheus registry
    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.fanout_latency.clone()))?;
        registry.register(Box::new(self.shard_coverage.clone()))?;
        registry.register(Box::new(self.responding_shards.clone()))?;
        registry.register(Box::new(self.requests_total.clone()))?;
        registry.register(Box::new(self.pool_connections.clone()))?;
        registry.register(Box::new(self.pool_count.clone()))?;
        registry.register(Box::new(self.shard_health.clone()))?;
        registry.register(Box::new(self.partial_results.clone()))?;
        registry.register(Box::new(self.batch_insert_latency.clone()))?;
        registry.register(Box::new(self.shard_vector_count.clone()))?;
        Ok(())
    }

    /// Record a fanout search
    pub fn record_fanout(
        &self,
        latency_secs: f64,
        coverage: f64,
        responding: usize,
        is_partial: bool,
    ) {
        self.fanout_latency
            .with_label_values(&["search"])
            .observe(latency_secs);
        self.shard_coverage.set(coverage);
        self.responding_shards.observe(responding as f64);
        if is_partial {
            self.partial_results.inc();
        }
    }

    /// Record a request
    pub fn record_request(&self, operation: &str, status: &str) {
        self.requests_total
            .with_label_values(&[operation, status])
            .inc();
    }

    /// Update pool stats
    pub fn update_pool_stats(&self, total_pools: usize, _pool_size: usize) {
        self.pool_count.set(total_pools as f64);
        // Note: per-shard stats would need to be updated individually
    }

    /// Update shard health
    pub fn update_shard_health(&self, shard_id: &str, healthy: bool) {
        self.shard_health
            .with_label_values(&[shard_id])
            .set(if healthy { 1.0 } else { 0.0 });
    }
}

/// Export metrics as Prometheus text format
/// Uses the static registry so metrics are only registered once
pub fn export_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = registry().gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::warn!("Failed to encode metrics: {}", e);
        return String::new();
    }

    String::from_utf8_lossy(&buffer).to_string()
}
