//! Prometheus Metrics
//!
//! Metrics for monitoring the ingestion pipeline.

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    Opts, Registry,
};
use std::sync::Arc;

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
            Opts::new("akidb_ingestion_documents_processed_total", "Total documents processed")
                .namespace("akidb")
                .subsystem("ingestion"),
            &["format", "parser"],
        ).unwrap();

        let documents_failed = CounterVec::new(
            Opts::new("akidb_ingestion_documents_failed_total", "Total documents failed")
                .namespace("akidb")
                .subsystem("ingestion"),
            &["format", "stage"],
        ).unwrap();

        let chunks_created = Counter::with_opts(
            Opts::new("akidb_ingestion_chunks_created_total", "Total chunks created")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let embeddings_generated = Counter::with_opts(
            Opts::new("akidb_ingestion_embeddings_generated_total", "Total embeddings generated")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let vectors_inserted = Counter::with_opts(
            Opts::new("akidb_ingestion_vectors_inserted_total", "Total vectors inserted into AkiDB")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let parse_latency = HistogramVec::new(
            HistogramOpts::new("akidb_ingestion_parse_latency_seconds", "Parse latency in seconds")
                .namespace("akidb")
                .subsystem("ingestion")
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
            &["format"],
        ).unwrap();

        let embed_latency = Histogram::with_opts(
            HistogramOpts::new("akidb_ingestion_embed_latency_seconds", "Embed latency in seconds")
                .namespace("akidb")
                .subsystem("ingestion")
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]),
        ).unwrap();

        let insert_latency = Histogram::with_opts(
            HistogramOpts::new("akidb_ingestion_insert_latency_seconds", "Insert latency in seconds")
                .namespace("akidb")
                .subsystem("ingestion")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
        ).unwrap();

        let circuit_breaker_state = Gauge::with_opts(
            Opts::new("akidb_ingestion_circuit_breaker_state", "Circuit breaker state (0=closed, 1=open, 2=half-open)")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let backpressure_active = Gauge::with_opts(
            Opts::new("akidb_ingestion_backpressure_active", "Whether backpressure is active (0/1)")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let memory_usage_pct = Gauge::with_opts(
            Opts::new("akidb_ingestion_memory_usage_percent", "Memory usage percentage")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let queue_depth = Gauge::with_opts(
            Opts::new("akidb_ingestion_queue_depth", "Current queue depth")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let batch_size = Gauge::with_opts(
            Opts::new("akidb_ingestion_batch_size", "Current batch size")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        // Sync and lifecycle metrics
        let sync_runs = CounterVec::new(
            Opts::new("akidb_ingestion_sync_runs_total", "Total sync runs")
                .namespace("akidb")
                .subsystem("ingestion"),
            &["status"],  // success, failed, skipped
        ).unwrap();

        let sync_duration = Histogram::with_opts(
            HistogramOpts::new("akidb_ingestion_sync_duration_seconds", "Sync run duration")
                .namespace("akidb")
                .subsystem("ingestion")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0]),
        ).unwrap();

        let files_processed = CounterVec::new(
            Opts::new("akidb_ingestion_files_processed_total", "Files processed by action")
                .namespace("akidb")
                .subsystem("ingestion"),
            &["action"],  // new, updated, marked, confirmed, skipped
        ).unwrap();

        let vectors_tombstoned = Counter::with_opts(
            Opts::new("akidb_ingestion_vectors_tombstoned_total", "Vectors soft-deleted")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let tag_updates = Counter::with_opts(
            Opts::new("akidb_ingestion_tag_updates_total", "Tag update operations")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let tag_filter_hits = Counter::with_opts(
            Opts::new("akidb_search_tag_filter_hits_total", "Tag filter applications in search")
                .namespace("akidb")
                .subsystem("search"),
        ).unwrap();

        let manifest_size = Gauge::with_opts(
            Opts::new("akidb_ingestion_manifest_size", "Number of entries in manifest")
                .namespace("akidb")
                .subsystem("ingestion"),
        ).unwrap();

        let reindex_operations = CounterVec::new(
            Opts::new("akidb_ingestion_reindex_total", "Reindex operations")
                .namespace("akidb")
                .subsystem("ingestion"),
            &["status"],  // success, failed
        ).unwrap();

        // Register all metrics
        registry.register(Box::new(documents_processed.clone())).unwrap();
        registry.register(Box::new(documents_failed.clone())).unwrap();
        registry.register(Box::new(chunks_created.clone())).unwrap();
        registry.register(Box::new(embeddings_generated.clone())).unwrap();
        registry.register(Box::new(vectors_inserted.clone())).unwrap();
        registry.register(Box::new(parse_latency.clone())).unwrap();
        registry.register(Box::new(embed_latency.clone())).unwrap();
        registry.register(Box::new(insert_latency.clone())).unwrap();
        registry.register(Box::new(circuit_breaker_state.clone())).unwrap();
        registry.register(Box::new(backpressure_active.clone())).unwrap();
        registry.register(Box::new(memory_usage_pct.clone())).unwrap();
        registry.register(Box::new(queue_depth.clone())).unwrap();
        registry.register(Box::new(batch_size.clone())).unwrap();
        registry.register(Box::new(sync_runs.clone())).unwrap();
        registry.register(Box::new(sync_duration.clone())).unwrap();
        registry.register(Box::new(files_processed.clone())).unwrap();
        registry.register(Box::new(vectors_tombstoned.clone())).unwrap();
        registry.register(Box::new(tag_updates.clone())).unwrap();
        registry.register(Box::new(tag_filter_hits.clone())).unwrap();
        registry.register(Box::new(manifest_size.clone())).unwrap();
        registry.register(Box::new(reindex_operations.clone())).unwrap();

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

        metrics.documents_processed.with_label_values(&["pdf", "python"]).inc();
        metrics.chunks_created.inc_by(10.0);
        metrics.circuit_breaker_state.set(0.0);
    }

    #[test]
    fn test_sync_metrics() {
        let (metrics, _) = IngestionMetrics::default_registry();

        metrics.sync_runs.with_label_values(&["success"]).inc();
        metrics.sync_duration.observe(60.0);
        metrics.files_processed.with_label_values(&["new"]).inc_by(10.0);
        metrics.vectors_tombstoned.inc_by(5.0);
        metrics.manifest_size.set(1000.0);
    }

    #[test]
    fn test_tag_metrics() {
        let (metrics, _) = IngestionMetrics::default_registry();

        metrics.tag_updates.inc();
        metrics.tag_filter_hits.inc();
        metrics.reindex_operations.with_label_values(&["success"]).inc();
    }
}
