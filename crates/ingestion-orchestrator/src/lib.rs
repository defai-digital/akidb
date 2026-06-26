//! AkiDB Ingestion Orchestrator
//!
//! Hybrid document processing pipeline with:
//! - Rust-native parsers for JSON, CSV, HTML, XML, XLSX (60-70%)
//! - Python sidecar for PDF, complex DOCX, ENL (30-40%)
//! - NATS JetStream for event-driven processing
//! - Circuit breaker for fault isolation
//! - Backpressure based on AkiDB latency
//! - Memory coordinator for local host pressure

pub mod config;
pub mod nats;
pub mod parsers;
pub mod python_client;
pub mod chunker;
pub mod batcher;
pub mod circuit_breaker;
pub mod backpressure;
pub mod memory;
pub mod embedding;
pub mod idempotency;
pub mod state;
pub mod metrics;
pub mod storage;
pub mod akidb_client;
pub mod pipeline;
pub mod manifest;
pub mod scheduler;
pub mod lifecycle;
pub mod reindex;

pub use config::IngestionConfig;
pub use pipeline::IngestionPipeline;
pub use manifest::ManifestStore;
pub use scheduler::{ChangeDetector, IngestionScheduler, SchedulerConfig};
pub use lifecycle::{LifecycleManager, LifecycleConfig};
pub use reindex::{Reindexer, ReindexConfig, ReindexResult};

/// Result type for ingestion operations
pub type Result<T> = std::result::Result<T, IngestionError>;

/// Ingestion error types
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("NATS error: {0}")]
    Nats(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Python parser error: {0}")]
    PythonParser(String),

    #[error("Circuit breaker open")]
    CircuitBreakerOpen,

    #[error("Backpressure active")]
    BackpressureActive,

    #[error("Memory pressure: {0}% used")]
    MemoryPressure(f32),

    #[error("Embedding error: {0}")]
    Embedding(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("State tracking error: {0}")]
    State(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Scheduler error: {0}")]
    Scheduler(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Other error: {0}")]
    Other(String),
}

impl From<async_nats::Error> for IngestionError {
    fn from(e: async_nats::Error) -> Self {
        IngestionError::Nats(e.to_string())
    }
}

// Blanket impl for async_nats error types
impl<T> From<async_nats::error::Error<T>> for IngestionError
where
    T: Clone + std::fmt::Debug + std::fmt::Display + PartialEq,
{
    fn from(e: async_nats::error::Error<T>) -> Self {
        IngestionError::Nats(e.to_string())
    }
}
