//! Ingestion Pipeline
//!
//! Main orchestration logic for the hybrid document processing pipeline.

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn, error, debug};

use crate::config::IngestionConfig;
use crate::nats::{NatsConsumer, DlqPublisher, UploadEvent};
use crate::parsers::{DocumentFormat, route_parser, ParsedDocument};
use crate::python_client::PythonParserClient;
use crate::chunker::SemanticChunker;
use crate::batcher::DynamicBatcher;
use crate::circuit_breaker::{CircuitBreaker, CircuitState};
use crate::backpressure::BackpressureController;
use crate::memory::MemoryCoordinator;
use crate::embedding::EmbeddingClient;
use crate::idempotency::IdempotencyChecker;
use crate::state::{StateTracker, DocumentState};
use crate::metrics::IngestionMetrics;
use crate::storage::StorageClient;
use crate::akidb_client::{AkiDbClient, VectorInsert};
use crate::Result;
use crate::nats::publisher::DlqEntry;

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
        let idempotency = IdempotencyChecker::new_persistent(
            "/var/lib/akidb/idempotency.db",
            100_000
        ).unwrap_or_else(|e| {
            warn!(?e, "Failed to create persistent idempotency checker, falling back to in-memory");
            IdempotencyChecker::new(100_000)
        });
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
        let max_pause_duration = std::time::Duration::from_secs(self.config.memory.max_pause_duration_secs);

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
        if let Err(e) = self.process_document_internal(&event, &content_hash, &data, start).await {
            // Record failure in state tracker
            let error_msg = e.to_string();
            if let Err(state_err) = self.state.update_state_with_error(&content_hash, DocumentState::Failed, &error_msg) {
                error!(?state_err, hash = %content_hash, "Failed to update state to Failed");
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
        let ext = event.key.rsplit('.').next().unwrap_or("");
        let format = DocumentFormat::from_extension(ext);

        // Parse document
        self.state.update_state(&content_hash, DocumentState::Parsing)?;
        let parsed = self.parse_document(&event, format, &data).await?;

        // Chunk document
        self.state.update_state(&content_hash, DocumentState::Chunking)?;
        let chunks = self.chunker.chunk(&parsed.text);
        self.state.update_chunk_count(&content_hash, chunks.len())?;
        self.metrics.chunks_created.inc_by(chunks.len() as f64);

        // Embed chunks
        self.state.update_state(&content_hash, DocumentState::Embedding)?;
        let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
        let embeddings = self.embedding_client.embed(texts).await?;
        self.metrics.embeddings_generated.inc_by(embeddings.len() as f64);

        // Insert into AkiDB
        self.state.update_state(&content_hash, DocumentState::Inserting)?;

        // Build vectors for insertion
        let vectors: Vec<VectorInsert> = chunks.iter().zip(embeddings.iter()).enumerate().map(|(i, (chunk, embedding))| {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("document_key".to_string(), event.key.clone());
            metadata.insert("bucket".to_string(), event.bucket.clone());
            metadata.insert("chunk_index".to_string(), i.to_string());
            metadata.insert("content_hash".to_string(), content_hash.to_string());
            metadata.insert("start_offset".to_string(), chunk.start_offset.to_string());
            metadata.insert("end_offset".to_string(), chunk.end_offset.to_string());

            VectorInsert {
                id: format!("{}:{}", content_hash, i),
                embedding: embedding.clone(),
                metadata,
                text: chunk.text.clone(),
            }
        }).collect();

        // Insert into AkiDB with backpressure awareness
        let insert_start = Instant::now();
        let result = self.akidb.insert_batch(vectors).await?;
        let insert_latency = insert_start.elapsed();

        // Update backpressure based on insert latency (convert to microseconds)
        self.backpressure.update_latency(insert_latency.as_micros() as u64);

        if result.failed > 0 {
            warn!(
                failed = result.failed,
                successful = result.successful,
                "Some vectors failed to insert"
            );
        }

        self.metrics.vectors_inserted.inc_by(result.successful as f64);

        // Mark completed
        self.state.update_state(&content_hash, DocumentState::Completed)?;

        let duration = start.elapsed();
        info!(
            key = %event.key,
            chunks = chunks.len(),
            vectors = result.successful,
            insert_latency_ms = insert_latency.as_millis(),
            duration_ms = duration.as_millis(),
            "Document processed"
        );

        self.metrics.documents_processed
            .with_label_values(&[format_label(format), parser_label(format)])
            .inc();

        Ok(())
    }

    /// Parse document using appropriate parser
    async fn parse_document(&self, event: &UploadEvent, format: DocumentFormat, data: &[u8]) -> Result<ParsedDocument> {
        let start = Instant::now();
        let data_size = data.len();

        debug!(
            format = ?format,
            size = data_size,
            key = %event.key,
            "Parsing document"
        );

        let result = if format.is_rust_native() {
            // Use Rust parser
            if let Some(parser) = route_parser(format) {
                parser.parse(data)
            } else {
                Err(crate::IngestionError::Parse("No parser for format".to_string()))
            }
        } else if format.requires_python() {
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
                    debug!(
                        text_len = parsed.text.len(),
                        "Python parser succeeded"
                    );
                    Ok(parsed)
                }
                Err(e) => {
                    self.circuit_breaker.record_failure();
                    error!(?e, "Python parser failed");
                    Err(e)
                }
            }
        } else {
            Err(crate::IngestionError::Parse(format!("Unsupported format: {:?}", format)))
        };

        let duration = start.elapsed();
        self.metrics.parse_latency
            .with_label_values(&[format_label(format)])
            .observe(duration.as_secs_f64());

        result
    }
}

fn format_label(format: DocumentFormat) -> &'static str {
    match format {
        DocumentFormat::Json => "json",
        DocumentFormat::Csv => "csv",
        DocumentFormat::Html => "html",
        DocumentFormat::Xml => "xml",
        DocumentFormat::Xlsx => "xlsx",
        DocumentFormat::Pdf => "pdf",
        DocumentFormat::Docx => "docx",
        DocumentFormat::Txt => "txt",
        DocumentFormat::Unknown => "unknown",
    }
}

fn parser_label(format: DocumentFormat) -> &'static str {
    if format.is_rust_native() {
        "rust"
    } else {
        "python"
    }
}
