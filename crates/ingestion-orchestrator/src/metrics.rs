//! Prometheus Metrics
//!
//! Metrics for monitoring the ingestion pipeline.

use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
};

const METRICS_NAMESPACE: &str = "akidb";
const INGESTION_SUBSYSTEM: &str = "ingestion";
const SEARCH_SUBSYSTEM: &str = "search";

fn scoped_opts(subsystem: &'static str, name: &'static str, help: &'static str) -> Opts {
    Opts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(subsystem)
}

fn ingestion_opts(name: &'static str, help: &'static str) -> Opts {
    scoped_opts(INGESTION_SUBSYSTEM, name, help)
}

fn ingestion_histogram_opts(
    name: &'static str,
    help: &'static str,
    buckets: Vec<f64>,
) -> HistogramOpts {
    HistogramOpts::new(name, help)
        .namespace(METRICS_NAMESPACE)
        .subsystem(INGESTION_SUBSYSTEM)
        .buckets(buckets)
}

/// Ingestion metrics
pub struct IngestionMetrics {
    /// Documents processed counter
    pub documents_processed: CounterVec,

    /// Documents failed counter
    pub documents_failed: CounterVec,

    /// Chunks created counter
    pub chunks_created: Counter,

    /// Embeddings generated counter
    pub embeddings_generated: Counter,

    /// Vectors inserted counter
    pub vectors_inserted: Counter,

    /// Parse latency histogram
    pub parse_latency: HistogramVec,

    /// Embed latency histogram
    pub embed_latency: Histogram,

    /// Insert latency histogram
    pub insert_latency: Histogram,

    /// Circuit breaker state gauge
    pub circuit_breaker_state: Gauge,

    /// Backpressure active gauge
    pub backpressure_active: Gauge,

    /// Memory usage gauge
    pub memory_usage_pct: Gauge,

    /// Queue depth gauge
    pub queue_depth: Gauge,

    /// Batch size gauge
    pub batch_size: Gauge,

    // === Sync and Lifecycle Metrics ===
    /// Sync runs counter by status
    pub sync_runs: CounterVec,

    /// Sync run duration histogram
    pub sync_duration: Histogram,

    /// Files processed by action type (new, updated, deleted, skipped)
    pub files_processed: CounterVec,

    /// Vectors tombstoned counter
    pub vectors_tombstoned: Counter,

    /// Tag updates counter
    pub tag_updates: Counter,

    /// Tag filter hits in search
    pub tag_filter_hits: Counter,

    /// Current manifest size gauge
    pub manifest_size: Gauge,

    /// Reindex operations counter
    pub reindex_operations: CounterVec,
}

impl IngestionMetrics {
    /// Create new metrics and register with the given registry
    pub fn new(registry: &Registry) -> Self {
        let documents_processed = CounterVec::new(
            ingestion_opts(
                "akidb_ingestion_documents_processed_total",
                "Total documents processed",
            ),
            &["format", "parser"],
        )
        .unwrap();

        let documents_failed = CounterVec::new(
            ingestion_opts(
                "akidb_ingestion_documents_failed_total",
                "Total documents failed",
            ),
            &["format", "stage"],
        )
        .unwrap();

        let chunks_created = Counter::with_opts(ingestion_opts(
            "akidb_ingestion_chunks_created_total",
            "Total chunks created",
        ))
        .unwrap();

        let embeddings_generated = Counter::with_opts(ingestion_opts(
            "akidb_ingestion_embeddings_generated_total",
            "Total embeddings generated",
        ))
        .unwrap();

        let vectors_inserted = Counter::with_opts(ingestion_opts(
            "akidb_ingestion_vectors_inserted_total",
            "Total vectors inserted into AkiDB",
        ))
        .unwrap();

        let parse_latency = HistogramVec::new(
            ingestion_histogram_opts(
                "akidb_ingestion_parse_latency_seconds",
                "Parse latency in seconds",
                vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
            ),
            &["format"],
        )
        .unwrap();

        let embed_latency = Histogram::with_opts(ingestion_histogram_opts(
            "akidb_ingestion_embed_latency_seconds",
            "Embed latency in seconds",
            vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
        ))
        .unwrap();

        let insert_latency = Histogram::with_opts(ingestion_histogram_opts(
            "akidb_ingestion_insert_latency_seconds",
            "Insert latency in seconds",
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
        ))
        .unwrap();

        let circuit_breaker_state = Gauge::with_opts(ingestion_opts(
            "akidb_ingestion_circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=open, 2=half-open)",
        ))
        .unwrap();

        let backpressure_active = Gauge::with_opts(ingestion_opts(
            "akidb_ingestion_backpressure_active",
            "Whether backpressure is active (0/1)",
        ))
        .unwrap();

        let memory_usage_pct = Gauge::with_opts(ingestion_opts(
            "akidb_ingestion_memory_usage_percent",
            "Memory usage percentage",
        ))
        .unwrap();

        let queue_depth = Gauge::with_opts(ingestion_opts(
            "akidb_ingestion_queue_depth",
            "Current queue depth",
        ))
        .unwrap();

        let batch_size = Gauge::with_opts(ingestion_opts(
            "akidb_ingestion_batch_size",
            "Current batch size",
        ))
        .unwrap();

        // Sync and lifecycle metrics
        let sync_runs = CounterVec::new(
            ingestion_opts("akidb_ingestion_sync_runs_total", "Total sync runs"),
            &["status"],
        )
        .unwrap();

        let sync_duration = Histogram::with_opts(ingestion_histogram_opts(
            "akidb_ingestion_sync_duration_seconds",
            "Sync run duration",
            vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0],
        ))
        .unwrap();

        let files_processed = CounterVec::new(
            ingestion_opts(
                "akidb_ingestion_files_processed_total",
                "Files processed by action",
            ),
            &["action"],
        )
        .unwrap();

        let vectors_tombstoned = Counter::with_opts(ingestion_opts(
            "akidb_ingestion_vectors_tombstoned_total",
            "Vectors soft-deleted",
        ))
        .unwrap();

        let tag_updates = Counter::with_opts(ingestion_opts(
            "akidb_ingestion_tag_updates_total",
            "Tag update operations",
        ))
        .unwrap();

        let tag_filter_hits = Counter::with_opts(scoped_opts(
            SEARCH_SUBSYSTEM,
            "akidb_search_tag_filter_hits_total",
            "Tag filter applications in search",
        ))
        .unwrap();

        let manifest_size = Gauge::with_opts(ingestion_opts(
            "akidb_ingestion_manifest_size",
            "Number of entries in manifest",
        ))
        .unwrap();

        let reindex_operations = CounterVec::new(
            ingestion_opts("akidb_ingestion_reindex_total", "Reindex operations"),
            &["status"],
        )
        .unwrap();

        akidb_common::register_prometheus_collectors!(
            registry,
            documents_processed,
            documents_failed,
            chunks_created,
            embeddings_generated,
            vectors_inserted,
            parse_latency,
            embed_latency,
            insert_latency,
            circuit_breaker_state,
            backpressure_active,
            memory_usage_pct,
            queue_depth,
            batch_size,
            sync_runs,
            sync_duration,
            files_processed,
            vectors_tombstoned,
            tag_updates,
            tag_filter_hits,
            manifest_size,
            reindex_operations,
        )
        .unwrap();

        Self {
            documents_processed,
            documents_failed,
            chunks_created,
            embeddings_generated,
            vectors_inserted,
            parse_latency,
            embed_latency,
            insert_latency,
            circuit_breaker_state,
            backpressure_active,
            memory_usage_pct,
            queue_depth,
            batch_size,
            sync_runs,
            sync_duration,
            files_processed,
            vectors_tombstoned,
            tag_updates,
            tag_filter_hits,
            manifest_size,
            reindex_operations,
        }
    }

    /// Create metrics with default registry
    pub fn default_registry() -> (Self, Registry) {
        let registry = Registry::new();
        let metrics = Self::new(&registry);
        (metrics, registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let (metrics, _) = IngestionMetrics::default_registry();

        metrics
            .documents_processed
            .with_label_values(&["pdf", "python"])
            .inc();
        metrics.chunks_created.inc_by(10.0);
        metrics.circuit_breaker_state.set(0.0);
    }

    #[test]
    fn test_sync_metrics() {
        let (metrics, _) = IngestionMetrics::default_registry();

        metrics.sync_runs.with_label_values(&["success"]).inc();
        metrics.sync_duration.observe(60.0);
        metrics
            .files_processed
            .with_label_values(&["new"])
            .inc_by(10.0);
        metrics.vectors_tombstoned.inc_by(5.0);
        metrics.manifest_size.set(1000.0);
    }

    #[test]
    fn test_tag_metrics() {
        let (metrics, _) = IngestionMetrics::default_registry();

        metrics.tag_updates.inc();
        metrics.tag_filter_hits.inc();
        metrics
            .reindex_operations
            .with_label_values(&["success"])
            .inc();
    }
}
