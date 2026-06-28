//! Ingestion Pipeline
//!
//! Main orchestration logic for the hybrid document processing pipeline.

use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::akidb_client::{AkiDbClient, BatchInsertResult, VectorInsert};
use crate::backpressure::BackpressureController;
use crate::batcher::DynamicBatcher;
use crate::chunker::{Chunk, SemanticChunker};
use crate::circuit_breaker::{CircuitBreaker, CircuitState};
use crate::config::IngestionConfig;
use crate::embedding::EmbeddingClient;
use crate::idempotency::IdempotencyChecker;
use crate::memory::MemoryCoordinator;
use crate::metrics::IngestionMetrics;
use crate::nats::publisher::DlqEntry;
use crate::nats::{DlqPublisher, NatsConsumer, UploadEvent};
use crate::parsers::{route_parser_with_data, DocumentFormat, DocumentMetadata, ParsedDocument};
use crate::python_client::PythonParserClient;
use crate::state::{DocumentState, StateTracker};
use crate::storage::StorageClient;
use crate::Result;

/// Main ingestion pipeline
pub struct IngestionPipeline {
    config: IngestionConfig,
    consumer: NatsConsumer,
    dlq: DlqPublisher,
    storage: StorageClient,
    akidb: AkiDbClient,
    python_client: PythonParserClient,
    chunker: SemanticChunker,
    batcher: DynamicBatcher<String>,
    embedding_client: EmbeddingClient,
    circuit_breaker: Arc<CircuitBreaker>,
    backpressure: Arc<BackpressureController>,
    memory: Arc<MemoryCoordinator>,
    idempotency: IdempotencyChecker,
    state: StateTracker,
    metrics: IngestionMetrics,
}

impl IngestionPipeline {
    /// Create a new ingestion pipeline
    pub async fn new(config: IngestionConfig) -> Result<Self> {
        info!("Initializing ingestion pipeline");

        // Initialize NATS consumer and DLQ publisher
        let consumer = NatsConsumer::new(&config.nats).await?;
        let dlq = DlqPublisher::new(&config.nats).await?;

        // Initialize MinIO/S3 storage client
        info!("Connecting to MinIO/S3 storage");
        let storage = StorageClient::new(&config.storage).await?;

        // Initialize AkiDB client
        info!("Connecting to AkiDB");
        let mut akidb = AkiDbClient::new(&config.akidb);
        akidb.connect().await?;

        // Initialize parser clients
        let python_client = PythonParserClient::new(&config.doc_parser_url);

        // Initialize chunking and batching
        let chunker = SemanticChunker::new(config.chunker.clone());
        let batcher = DynamicBatcher::new(config.batcher.clone());

        // Initialize embedding client
        let embedding_client = EmbeddingClient::with_qwen3(&config.embedding_url);

        // Initialize resilience patterns
        let circuit_breaker = Arc::new(CircuitBreaker::new(config.circuit_breaker.clone()));
        let backpressure = Arc::new(BackpressureController::new(config.backpressure.clone()));
        let memory = Arc::new(MemoryCoordinator::new(config.memory.clone()));

        // Initialize state tracking with persistence
        let idempotency =
            match IdempotencyChecker::new_persistent("/var/lib/akidb/idempotency.db", 100_000) {
                Ok(checker) => checker,
                Err(e) => {
                    warn!(
                        ?e,
                        "Failed to create persistent idempotency checker, falling back to in-memory"
                    );
                    IdempotencyChecker::new(100_000)
                }
            };
        let state = StateTracker::new("/var/lib/akidb/ingestion.db")
            .unwrap_or_else(|_| StateTracker::in_memory().unwrap());

        // Initialize metrics
        let (metrics, _) = IngestionMetrics::default_registry();

        info!("Ingestion pipeline initialized");

        Ok(Self {
            config,
            consumer,
            dlq,
            storage,
            akidb,
            python_client,
            chunker,
            batcher,
            embedding_client,
            circuit_breaker,
            backpressure,
            memory,
            idempotency,
            state,
            metrics,
        })
    }

    /// Run the ingestion pipeline
    pub async fn run(&self) -> Result<()> {
        info!("Starting ingestion pipeline");

        // Start memory monitoring
        let _memory_handle = self.memory.start_monitoring();

        // FIX BUG-H052: Track when memory pressure pause started
        let mut memory_pause_start: Option<std::time::Instant> = None;
        let max_pause_duration =
            std::time::Duration::from_secs(self.config.memory.max_pause_duration_secs);

        loop {
            // Check memory pressure with timeout to prevent indefinite stalls
            if self.memory.is_paused() {
                let pause_start = memory_pause_start.get_or_insert_with(std::time::Instant::now);
                let pause_duration = pause_start.elapsed();

                // FIX BUG-H052: If paused too long, log warning and proceed anyway
                if pause_duration >= max_pause_duration {
                    warn!(
                        pause_secs = pause_duration.as_secs(),
                        max_secs = max_pause_duration.as_secs(),
                        memory_pct = self.memory.usage_percent(),
                        "Memory pressure pause exceeded max duration - proceeding anyway to prevent indefinite stall"
                    );
                    // Reset pause tracking so we re-evaluate after processing
                    memory_pause_start = None;
                } else {
                    debug!(
                        pause_secs = pause_duration.as_secs(),
                        max_secs = max_pause_duration.as_secs(),
                        "Paused due to memory pressure"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
            } else {
                // Memory pressure relieved, reset tracking
                memory_pause_start = None;
            }

            // Check backpressure
            self.backpressure.wait_if_active().await;

            // Fetch batch of messages
            let messages = self.consumer.fetch(10).await?;

            if messages.is_empty() {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            // Process each message
            for msg in messages {
                if let Err(e) = self.process_message(&msg).await {
                    error!(?e, "Failed to process message");

                    // Publish to DLQ for failed documents
                    if let Ok(event) = msg.payload() {
                        let dlq_entry = DlqEntry {
                            original_event: serde_json::to_value(&event).unwrap_or_default(),
                            error: e.to_string(),
                            retry_count: 0,
                            failed_at: chrono::Utc::now().to_rfc3339(),
                            stage: "processing".to_string(),
                        };
                        if let Err(dlq_err) = self.dlq.publish(dlq_entry).await {
                            error!(?dlq_err, "Failed to publish to DLQ");
                        }
                    }

                    // Terminate message (don't redeliver since it's in DLQ)
                    msg.terminate().await?;
                } else {
                    msg.ack().await?;
                }
            }
        }
    }

    /// Process a single upload message
    ///
    /// FIX: Records failed state in state tracker when errors occur after document is recorded.
    async fn process_message(&self, msg: &crate::nats::consumer::NatsMessage) -> Result<()> {
        let event = msg.payload()?;
        let start = Instant::now();

        info!(bucket = %event.bucket, key = %event.key, "Processing upload");

        // Fetch document from MinIO
        debug!(bucket = %event.bucket, key = %event.key, "Fetching document from storage");
        let data = self.storage.fetch(&event.bucket, &event.key).await?;

        // Check idempotency using actual content hash
        let (is_dup, content_hash) = self.idempotency.check_and_mark(&data);
        if is_dup {
            debug!(key = %event.key, hash = %content_hash, "Duplicate document, skipping");
            return Ok(());
        }

        // Record document
        self.state.record_document(&content_hash, &event.key)?;

        // FIX: Call internal processing with error handling for state tracking
        if let Err(e) = self
            .process_document_internal(&event, &content_hash, &data, start)
            .await
        {
            // Record failure in state tracker
            let error_msg = e.to_string();
            if let Err(state_err) =
                self.state
                    .update_state_with_error(&content_hash, DocumentState::Failed, &error_msg)
            {
                error!(?state_err, hash = %content_hash, "Failed to update state to Failed");
            }
            if let Err(idempotency_err) = self.idempotency.unmark_hash(&content_hash) {
                error!(
                    error = %idempotency_err,
                    hash = %content_hash,
                    "Failed to roll back idempotency mark after processing failure"
                );
            }
            return Err(e);
        }

        Ok(())
    }

    /// Internal document processing logic (after recording in state tracker)
    ///
    /// This method is called after the document is recorded, so any errors here
    /// need to be caught by the caller and the state updated to Failed.
    async fn process_document_internal(
        &self,
        event: &UploadEvent,
        content_hash: &str,
        data: &[u8],
        start: Instant,
    ) -> Result<()> {
        // Detect format
        let format = detect_upload_format(event);

        // Parse document
        self.state
            .update_state(&content_hash, DocumentState::Parsing)?;
        let parsed = self.parse_document(&event, format, &data).await?;

        // Chunk document
        self.state
            .update_state(&content_hash, DocumentState::Chunking)?;
        let chunks = self.chunker.chunk(&parsed.text);
        self.state.update_chunk_count(&content_hash, chunks.len())?;
        self.metrics.chunks_created.inc_by(chunks.len() as f64);

        // Embed chunks
        self.state
            .update_state(&content_hash, DocumentState::Embedding)?;
        let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
        let embeddings = self.embedding_client.embed(texts).await?;
        ensure_embedding_alignment(chunks.len(), embeddings.len())?;
        self.metrics
            .embeddings_generated
            .inc_by(embeddings.len() as f64);

        // Insert into AkiDB
        self.state
            .update_state(&content_hash, DocumentState::Inserting)?;

        // Build vectors for insertion
        let vectors: Vec<VectorInsert> = chunks
            .iter()
            .zip(embeddings.iter())
            .enumerate()
            .map(|(i, (chunk, embedding))| {
                let metadata = build_vector_metadata(
                    event,
                    &parsed.metadata,
                    parsed.format,
                    content_hash,
                    chunk,
                    i,
                );

                VectorInsert {
                    id: format!("{}:{}", content_hash, i),
                    embedding: embedding.clone(),
                    metadata,
                    text: chunk.text.clone(),
                }
            })
            .collect();

        // Insert into AkiDB with backpressure awareness
        let insert_start = Instant::now();
        let result = self.akidb.insert_batch(vectors).await?;
        let insert_latency = insert_start.elapsed();

        // Update backpressure based on insert latency (convert to microseconds)
        self.backpressure
            .update_latency(insert_latency.as_micros() as u64);

        if result.failed > 0 {
            warn!(
                failed = result.failed,
                successful = result.successful,
                "Some vectors failed to insert"
            );
        }
        ensure_complete_insert(&result)?;

        self.metrics
            .vectors_inserted
            .inc_by(result.successful as f64);

        // Mark completed
        self.state
            .update_state(&content_hash, DocumentState::Completed)?;

        let duration = start.elapsed();
        info!(
            key = %event.key,
            chunks = chunks.len(),
            vectors = result.successful,
            insert_latency_ms = insert_latency.as_millis(),
            duration_ms = duration.as_millis(),
            "Document processed"
        );

        self.metrics
            .documents_processed
            .with_label_values(&[format_label(format), parser_label_for_data(format, data)])
            .inc();

        Ok(())
    }

    /// Parse document using appropriate parser
    async fn parse_document(
        &self,
        event: &UploadEvent,
        format: DocumentFormat,
        data: &[u8],
    ) -> Result<ParsedDocument> {
        let start = Instant::now();
        let data_size = data.len();

        debug!(
            format = ?format,
            size = data_size,
            key = %event.key,
            "Parsing document"
        );

        let rust_parser = route_parser_with_data(format, data);
        let result = if let Some(parser) = rust_parser {
            // Use Rust parser
            parser.parse(data)
        } else if should_use_python_parser(format, data) {
            // Check circuit breaker
            if self.circuit_breaker.state() == CircuitState::Open {
                warn!("Circuit breaker open, rejecting Python parser request");
                return Err(crate::IngestionError::CircuitBreakerOpen);
            }

            if !self.circuit_breaker.allow_request() {
                warn!("Circuit breaker half-open, request denied");
                return Err(crate::IngestionError::CircuitBreakerOpen);
            }

            // Use Python parser
            match self.python_client.parse(data, &event.key).await {
                Ok(parsed) => {
                    self.circuit_breaker.record_success();
                    debug!(text_len = parsed.text.len(), "Python parser succeeded");
                    Ok(parsed)
                }
                Err(e) => {
                    self.circuit_breaker.record_failure();
                    error!(?e, "Python parser failed");
                    Err(e)
                }
            }
        } else {
            Err(crate::IngestionError::Parse(format!(
                "Unsupported format: {:?}",
                format
            )))
        };

        let duration = start.elapsed();
        self.metrics
            .parse_latency
            .with_label_values(&[format_label(format)])
            .observe(duration.as_secs_f64());

        result
    }
}

fn build_vector_metadata(
    event: &UploadEvent,
    document_metadata: &DocumentMetadata,
    document_format: DocumentFormat,
    content_hash: &str,
    chunk: &Chunk,
    chunk_index: usize,
) -> std::collections::HashMap<String, String> {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("document_key".to_string(), event.key.clone());
    metadata.insert("bucket".to_string(), event.bucket.clone());
    metadata.insert("chunk_index".to_string(), chunk_index.to_string());
    metadata.insert("content_hash".to_string(), content_hash.to_string());
    metadata.insert("start_offset".to_string(), chunk.start_offset.to_string());
    metadata.insert("end_offset".to_string(), chunk.end_offset.to_string());
    metadata.insert("token_count".to_string(), chunk.token_count.to_string());
    metadata.insert(
        "document_format".to_string(),
        format_label(document_format).to_string(),
    );

    if let Some(title) = document_metadata
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        metadata.insert("title".to_string(), title.to_string());
    }
    if let Some(author) = document_metadata
        .author
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        metadata.insert("author".to_string(), author.to_string());
    }
    if let Some(pages) = document_metadata.pages {
        metadata.insert("pages".to_string(), pages.to_string());
    }
    if let Some(word_count) = document_metadata.word_count {
        metadata.insert("word_count".to_string(), word_count.to_string());
    }
    if let Some(extra) = &document_metadata.extra {
        metadata.insert("metadata_extra".to_string(), extra.to_string());
    }

    metadata
}

fn ensure_complete_insert(result: &BatchInsertResult) -> Result<()> {
    if result.failed > 0 {
        return Err(crate::IngestionError::Storage(format!(
            "Partial AkiDB insert: {} succeeded, {} failed out of {}",
            result.successful, result.failed, result.total
        )));
    }
    Ok(())
}

fn should_use_python_parser(format: DocumentFormat, data: &[u8]) -> bool {
    format.requires_python()
        || (matches!(format, DocumentFormat::Docx)
            && route_parser_with_data(format, data).is_none())
}

fn detect_upload_format(event: &UploadEvent) -> DocumentFormat {
    DocumentFormat::from_name_or_content_type(&event.key, event.content_type.as_deref())
}

fn format_label(format: DocumentFormat) -> &'static str {
    match format {
        DocumentFormat::Json => "json",
        DocumentFormat::Csv => "csv",
        DocumentFormat::Tsv => "tsv",
        DocumentFormat::Html => "html",
        DocumentFormat::Xml => "xml",
        DocumentFormat::Xlsx => "xlsx",
        DocumentFormat::Pdf => "pdf",
        DocumentFormat::Docx => "docx",
        DocumentFormat::Txt => "txt",
        DocumentFormat::Unknown => "unknown",
    }
}

fn parser_label_for_data(format: DocumentFormat, data: &[u8]) -> &'static str {
    if route_parser_with_data(format, data).is_some() {
        "rust"
    } else if should_use_python_parser(format, data) {
        "python"
    } else {
        "unsupported"
    }
}

fn ensure_embedding_alignment(chunk_count: usize, embedding_count: usize) -> Result<()> {
    if chunk_count != embedding_count {
        return Err(crate::IngestionError::Embedding(format!(
            "Embedding count mismatch: {} chunks produced {} embeddings",
            chunk_count, embedding_count
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_embedding_alignment_rejects_partial_embedding_response() {
        let result = ensure_embedding_alignment(3, 2);

        assert!(
            matches!(result, Err(crate::IngestionError::Embedding(message)) if message.contains("3 chunks produced 2 embeddings"))
        );
    }

    #[test]
    fn test_ensure_embedding_alignment_allows_exact_match() {
        assert!(ensure_embedding_alignment(2, 2).is_ok());
    }

    #[test]
    fn test_non_simple_docx_routes_to_python_parser() {
        assert!(should_use_python_parser(
            DocumentFormat::Docx,
            b"not a zip file"
        ));
    }

    #[test]
    fn test_rust_native_json_does_not_route_to_python_parser() {
        assert!(!should_use_python_parser(
            DocumentFormat::Json,
            br#"{"ok":true}"#
        ));
    }

    #[test]
    fn test_parser_label_uses_python_for_non_simple_docx() {
        assert_eq!(
            parser_label_for_data(DocumentFormat::Docx, b"not a zip file"),
            "python"
        );
    }

    #[test]
    fn test_parser_label_uses_rust_for_rust_native_json() {
        assert_eq!(
            parser_label_for_data(DocumentFormat::Json, br#"{"ok":true}"#),
            "rust"
        );
    }

    #[test]
    fn test_detect_upload_format_falls_back_to_content_type() {
        let event = UploadEvent {
            bucket: "docs".to_string(),
            key: "opaque-upload".to_string(),
            size: 1024,
            content_type: Some("application/pdf; charset=binary".to_string()),
            timestamp: "2026-06-28T08:00:00Z".to_string(),
        };

        assert_eq!(detect_upload_format(&event), DocumentFormat::Pdf);
    }

    #[test]
    fn test_detect_upload_format_prefers_file_extension() {
        let event = UploadEvent {
            bucket: "docs".to_string(),
            key: "report.csv".to_string(),
            size: 1024,
            content_type: Some("application/pdf".to_string()),
            timestamp: "2026-06-28T08:00:00Z".to_string(),
        };

        assert_eq!(detect_upload_format(&event), DocumentFormat::Csv);
    }

    #[test]
    fn test_build_vector_metadata_preserves_document_metadata() {
        let event = UploadEvent {
            bucket: "docs".to_string(),
            key: "reports/annual.pdf".to_string(),
            size: 1024,
            content_type: Some("application/pdf".to_string()),
            timestamp: "2026-06-28T08:00:00Z".to_string(),
        };
        let document_metadata = DocumentMetadata {
            title: Some("Annual Report".to_string()),
            author: Some("Finance".to_string()),
            pages: Some(7),
            word_count: Some(1200),
            extra: Some(serde_json::json!({
                "parser_format": "pdf",
                "tables": [{"headers": ["customer", "amount"]}],
            })),
            ..Default::default()
        };
        let chunk = Chunk {
            text: "HGC contract amount 1200".to_string(),
            start_offset: 10,
            end_offset: 34,
            token_count: 6,
            index: 0,
        };

        let metadata = build_vector_metadata(
            &event,
            &document_metadata,
            DocumentFormat::Pdf,
            "hash123",
            &chunk,
            0,
        );

        assert_eq!(metadata["document_key"], "reports/annual.pdf");
        assert_eq!(metadata["bucket"], "docs");
        assert_eq!(metadata["content_hash"], "hash123");
        assert_eq!(metadata["document_format"], "pdf");
        assert_eq!(metadata["title"], "Annual Report");
        assert_eq!(metadata["author"], "Finance");
        assert_eq!(metadata["pages"], "7");
        assert_eq!(metadata["word_count"], "1200");
        assert_eq!(metadata["start_offset"], "10");
        assert_eq!(metadata["end_offset"], "34");
        assert_eq!(metadata["token_count"], "6");
        assert!(metadata["metadata_extra"].contains("\"parser_format\":\"pdf\""));
        assert!(metadata["metadata_extra"].contains("\"customer\""));
    }

    #[test]
    fn test_build_vector_metadata_skips_empty_optional_strings() {
        let event = UploadEvent {
            bucket: "docs".to_string(),
            key: "notes.txt".to_string(),
            size: 10,
            content_type: Some("text/plain".to_string()),
            timestamp: "2026-06-28T08:00:00Z".to_string(),
        };
        let document_metadata = DocumentMetadata {
            title: Some("  Release Notes  ".to_string()),
            author: Some("   ".to_string()),
            ..Default::default()
        };
        let chunk = Chunk {
            text: "notes".to_string(),
            start_offset: 0,
            end_offset: 5,
            token_count: 1,
            index: 0,
        };

        let metadata = build_vector_metadata(
            &event,
            &document_metadata,
            DocumentFormat::Txt,
            "hash123",
            &chunk,
            0,
        );

        assert_eq!(metadata["title"], "Release Notes");
        assert!(!metadata.contains_key("author"));
        assert!(!metadata.contains_key("metadata_extra"));
    }

    #[test]
    fn test_ensure_complete_insert_allows_full_success() {
        let result = BatchInsertResult {
            total: 2,
            successful: 2,
            failed: 0,
            results: vec![],
            latency_ms: 10,
        };

        assert!(ensure_complete_insert(&result).is_ok());
    }

    #[test]
    fn test_ensure_complete_insert_rejects_partial_success() {
        let result = BatchInsertResult {
            total: 3,
            successful: 2,
            failed: 1,
            results: vec![],
            latency_ms: 10,
        };

        let err = ensure_complete_insert(&result).unwrap_err();
        assert!(
            matches!(err, crate::IngestionError::Storage(message) if message.contains("Partial AkiDB insert"))
        );
    }
}
