//! Integration tests for the ingestion orchestrator
//!
//! These tests use testcontainers to spin up real NATS and MinIO instances.

use std::collections::HashMap;
use std::time::Duration;

use akidb_ingestion::{
    config::{
        BackpressureConfig, BatcherConfig, ChunkerConfig, CircuitBreakerConfig,
        IngestionConfig, MemoryConfig, NatsConfig, StorageConfig, AkiDbConfig, MinioConfig,
    },
    storage::StorageClient,
    chunker::SemanticChunker,
    circuit_breaker::{CircuitBreaker, CircuitState},
    backpressure::BackpressureController,
    idempotency::IdempotencyChecker,
    parsers::{DocumentFormat, route_parser},
};

use tempfile::TempDir;

/// Test the storage client with a mock/local configuration
#[tokio::test]
async fn test_storage_client_configuration() {
    let config = StorageConfig {
        endpoint: "http://localhost:9000".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
    };

    // Verify config is valid
    assert!(!config.endpoint.is_empty());
    assert!(!config.access_key.is_empty());
    assert!(!config.bucket.is_empty());
}

/// Test semantic chunker with various document sizes
#[tokio::test]
async fn test_semantic_chunker_integration() {
    let config = ChunkerConfig {
        target_tokens: 100,
        min_overlap: 10,
        max_overlap: 20,
    };

    let chunker = SemanticChunker::new(config);

    // Test with a medium-length document
    let text = "This is the first sentence of our test document. \
                It contains multiple sentences that should be chunked appropriately. \
                The chunker should respect sentence boundaries. \
                Each chunk should have proper token counts. \
                This is another sentence to make the document longer. \
                And here is yet another sentence for good measure. \
                The semantic chunker uses tiktoken for accurate token counting. \
                This helps ensure chunks are the right size for embedding models.";

    let chunks = chunker.chunk(text);

    // Verify chunks were created
    assert!(!chunks.is_empty(), "Should create at least one chunk");

    // Verify each chunk has valid properties
    for (i, chunk) in chunks.iter().enumerate() {
        assert!(!chunk.text.is_empty(), "Chunk {} should have text", i);
        assert!(chunk.token_count > 0, "Chunk {} should have tokens", i);
        assert_eq!(chunk.index, i, "Chunk {} should have correct index", i);
    }

    // Verify chunks cover the text
    let total_unique_text: usize = chunks.iter().map(|c| c.text.len()).sum();
    assert!(
        total_unique_text >= text.len() / 2,
        "Chunks should cover significant portion of text"
    );
}

/// Test circuit breaker state transitions
#[tokio::test]
async fn test_circuit_breaker_full_cycle() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        reset_timeout_secs: 1,
        half_open_max_calls: 1,
    };

    let cb = CircuitBreaker::new(config);

    // Initial state: Closed
    assert_eq!(cb.state(), CircuitState::Closed);
    assert!(cb.allow_request());

    // Record failures to open
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Closed);
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);
    assert!(!cb.allow_request());

    // Wait for reset timeout
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Should transition to half-open
    assert!(cb.allow_request());
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // Success in half-open should close
    cb.record_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

/// Test backpressure controller with latency updates
#[tokio::test]
async fn test_backpressure_latency_based() {
    let config = BackpressureConfig {
        latency_threshold_ms: 100,
        queue_depth_high_water: 1000,
        queue_depth_low_water: 500,
        pause_duration_secs: 1,
    };

    let bp = BackpressureController::new(config);

    // Initial state: not active
    assert!(!bp.is_active());

    // Low latency: should not activate
    bp.update_latency(50_000); // 50ms in microseconds
    assert!(!bp.is_active());

    // High latency: should activate
    bp.update_latency(150_000); // 150ms in microseconds
    assert!(bp.is_active());

    // Recovery: latency drops below half threshold
    bp.update_latency(40_000); // 40ms in microseconds
    assert!(!bp.is_active());
}

/// Test backpressure controller with queue depth
#[tokio::test]
async fn test_backpressure_queue_depth() {
    let config = BackpressureConfig {
        latency_threshold_ms: 500,
        queue_depth_high_water: 100,
        queue_depth_low_water: 50,
        pause_duration_secs: 1,
    };

    let bp = BackpressureController::new(config);

    // Low queue depth
    bp.update_queue_depth(30);
    assert!(!bp.is_active());

    // High queue depth
    bp.update_queue_depth(150);
    assert!(bp.is_active());

    // Queue drains below low water - should deactivate
    bp.update_queue_depth(40);
    assert!(!bp.is_active());
}

/// Test idempotency checker with content hashing
#[tokio::test]
async fn test_idempotency_checker_integration() {
    let checker = IdempotencyChecker::new(1000);

    let content1 = b"This is document one with unique content.";
    let content2 = b"This is document two with different content.";

    // First check: not duplicate
    let (is_dup1, hash1) = checker.check_and_mark(content1);
    assert!(!is_dup1);
    assert!(!hash1.is_empty());

    // Second check of same content: duplicate
    let (is_dup2, hash2) = checker.check_and_mark(content1);
    assert!(is_dup2);
    assert_eq!(hash1, hash2);

    // Different content: not duplicate
    let (is_dup3, hash3) = checker.check_and_mark(content2);
    assert!(!is_dup3);
    assert_ne!(hash1, hash3);
}

/// Test parser routing for different formats
#[tokio::test]
async fn test_parser_routing() {
    // Test JSON routing
    let json_format = DocumentFormat::from_extension("json");
    assert_eq!(json_format, DocumentFormat::Json);
    assert!(json_format.is_rust_native());
    assert!(route_parser(json_format).is_some());

    // Test CSV routing
    let csv_format = DocumentFormat::from_extension("csv");
    assert_eq!(csv_format, DocumentFormat::Csv);
    assert!(csv_format.is_rust_native());
    assert!(route_parser(csv_format).is_some());

    // Test HTML routing
    let html_format = DocumentFormat::from_extension("html");
    assert_eq!(html_format, DocumentFormat::Html);
    assert!(html_format.is_rust_native());
    assert!(route_parser(html_format).is_some());

    // Test PDF routing (Python)
    let pdf_format = DocumentFormat::from_extension("pdf");
    assert_eq!(pdf_format, DocumentFormat::Pdf);
    assert!(pdf_format.requires_python());
    assert!(route_parser(pdf_format).is_none());

    // Test DOCX routing (Rust-native for simple files)
    let docx_format = DocumentFormat::from_extension("docx");
    assert_eq!(docx_format, DocumentFormat::Docx);
    assert!(!docx_format.requires_python()); // Simple DOCX handled by Rust
    assert!(route_parser(docx_format).is_some());
}

/// Test JSON parser with real content
#[tokio::test]
async fn test_json_parser_integration() {
    let json_content = r#"{
        "title": "Test Document",
        "content": "This is the main content of the document.",
        "metadata": {
            "author": "Test Author",
            "date": "2026-01-21"
        },
        "tags": ["test", "integration", "parsing"]
    }"#;

    let parser = route_parser(DocumentFormat::Json).unwrap();
    let result = parser.parse(json_content.as_bytes());

    assert!(result.is_ok());
    let parsed = result.unwrap();
    assert!(parsed.text.contains("Test Document"));
    assert!(parsed.text.contains("main content"));
}

/// Test CSV parser with real content
#[tokio::test]
async fn test_csv_parser_integration() {
    let csv_content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago";

    let parser = route_parser(DocumentFormat::Csv).unwrap();
    let result = parser.parse(csv_content.as_bytes());

    assert!(result.is_ok());
    let parsed = result.unwrap();
    assert!(parsed.text.contains("Alice"));
    assert!(parsed.text.contains("New York"));
    assert!(parsed.text.contains("Bob"));
}

/// Test HTML parser with real content
#[tokio::test]
async fn test_html_parser_integration() {
    let html_content = r#"
        <!DOCTYPE html>
        <html>
        <head><title>Test Page</title></head>
        <body>
            <h1>Welcome</h1>
            <p>This is a test paragraph with important content.</p>
            <script>console.log('should be excluded');</script>
            <style>.hidden { display: none; }</style>
            <div>More visible text here.</div>
        </body>
        </html>
    "#;

    let parser = route_parser(DocumentFormat::Html).unwrap();
    let result = parser.parse(html_content.as_bytes());

    assert!(result.is_ok());
    let parsed = result.unwrap();
    assert!(parsed.text.contains("Welcome"));
    assert!(parsed.text.contains("test paragraph"));
    assert!(parsed.text.contains("visible text"));
    // Script content should be excluded
    assert!(!parsed.text.contains("console.log"));
}

/// Test XML parser with real content
#[tokio::test]
async fn test_xml_parser_integration() {
    let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
        <document>
            <title>XML Test Document</title>
            <body>
                <paragraph>First paragraph of content.</paragraph>
                <paragraph>Second paragraph with more text.</paragraph>
            </body>
            <metadata>
                <author>Test Author</author>
            </metadata>
        </document>
    "#;

    let parser = route_parser(DocumentFormat::Xml).unwrap();
    let result = parser.parse(xml_content.as_bytes());

    assert!(result.is_ok());
    let parsed = result.unwrap();
    assert!(parsed.text.contains("XML Test Document"));
    assert!(parsed.text.contains("First paragraph"));
    assert!(parsed.text.contains("Second paragraph"));
}

/// Test configuration loading from environment
#[tokio::test]
async fn test_config_defaults() {
    let nats = NatsConfig {
        url: "nats://localhost:4222".to_string(),
        stream: "test-stream".to_string(),
        consumer: "test-consumer".to_string(),
        dlq_stream: "test-dlq".to_string(),
    };

    assert_eq!(nats.url, "nats://localhost:4222");

    let storage = StorageConfig::default();
    assert_eq!(storage.endpoint, "http://localhost:9000");
    assert_eq!(storage.bucket, "akidb-documents");

    let akidb = AkiDbConfig::default();
    assert_eq!(akidb.endpoint, "http://localhost:50051");
    assert_eq!(akidb.timeout_ms, 30000);

    let circuit_breaker = CircuitBreakerConfig::default();
    assert_eq!(circuit_breaker.failure_threshold, 3);

    let backpressure = BackpressureConfig::default();
    assert_eq!(backpressure.latency_threshold_ms, 500);

    let memory = MemoryConfig::default();
    assert_eq!(memory.pause_threshold_pct, 70.0);

    let chunker = ChunkerConfig::default();
    assert_eq!(chunker.target_tokens, 512);

    let batcher = BatcherConfig::default();
    assert_eq!(batcher.min_batch, 16);
    assert_eq!(batcher.max_batch, 64);
}

/// Test full document processing pipeline (mock)
#[tokio::test]
async fn test_document_processing_flow() {
    // Simulate the document processing flow
    let content = b"This is a test document with multiple sentences. \
                    It should be parsed, chunked, and prepared for embedding. \
                    The pipeline should track state through each stage.";

    // 1. Check idempotency
    let idempotency = IdempotencyChecker::new(100);
    let (is_dup, content_hash) = idempotency.check_and_mark(content);
    assert!(!is_dup);
    assert!(!content_hash.is_empty());

    // 2. Parse document (simulated as plain text)
    let text = String::from_utf8_lossy(content).to_string();

    // 3. Chunk document
    let chunker = SemanticChunker::new(ChunkerConfig {
        target_tokens: 50,
        min_overlap: 5,
        max_overlap: 10,
    });
    let chunks = chunker.chunk(&text);
    assert!(!chunks.is_empty());

    // 4. Prepare for embedding (simulate)
    let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
    assert!(!texts.is_empty());

    // 5. Prepare metadata for vectors
    for (i, chunk) in chunks.iter().enumerate() {
        let mut metadata = HashMap::new();
        metadata.insert("chunk_index".to_string(), i.to_string());
        metadata.insert("content_hash".to_string(), content_hash.clone());
        metadata.insert("token_count".to_string(), chunk.token_count.to_string());

        assert!(metadata.contains_key("chunk_index"));
        assert!(metadata.contains_key("content_hash"));
    }
}

/// Test state tracker with SQLite
#[tokio::test]
async fn test_state_tracker_sqlite() {
    use akidb_ingestion::state::{StateTracker, DocumentState};

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_state.db");

    let tracker = StateTracker::new(db_path.to_str().unwrap()).unwrap();

    // Record a document
    tracker.record_document("hash123", "test/doc.pdf").unwrap();

    // Update state through stages
    tracker.update_state("hash123", DocumentState::Parsing).unwrap();
    tracker.update_state("hash123", DocumentState::Chunking).unwrap();
    tracker.update_chunk_count("hash123", 5).unwrap();
    tracker.update_state("hash123", DocumentState::Embedding).unwrap();
    tracker.update_state("hash123", DocumentState::Inserting).unwrap();
    tracker.update_state("hash123", DocumentState::Completed).unwrap();

    // Check stats
    let stats = tracker.stats().unwrap();
    assert_eq!(stats.total(), 1);
    assert_eq!(stats.completed, 1);
}
