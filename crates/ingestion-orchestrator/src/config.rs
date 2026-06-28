//! Configuration for the ingestion orchestrator

use std::env::VarError;
use std::fmt::Display;
use std::str::FromStr;

use crate::{IngestionError, Result};
use serde::Deserialize;

/// Main configuration for the ingestion orchestrator
#[derive(Debug, Clone, Deserialize)]
pub struct IngestionConfig {
    /// NATS configuration
    pub nats: NatsConfig,

    /// MinIO configuration (legacy)
    pub minio: MinioConfig,

    /// Storage configuration (S3/MinIO)
    pub storage: StorageConfig,

    /// AkiDB configuration
    pub akidb: AkiDbConfig,

    /// AkiDB coordinator address (legacy)
    pub akidb_coordinator: String,

    /// vLLM/TensorRT embedding service URL
    pub embedding_url: String,

    /// Python parser service URL
    pub doc_parser_url: String,

    /// Circuit breaker configuration
    pub circuit_breaker: CircuitBreakerConfig,

    /// Backpressure configuration
    pub backpressure: BackpressureConfig,

    /// Memory coordinator configuration
    pub memory: MemoryConfig,

    /// Chunker configuration
    pub chunker: ChunkerConfig,

    /// Batcher configuration
    pub batcher: BatcherConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    /// NATS server URL (e.g., "nats://localhost:4222")
    pub url: String,

    /// JetStream stream name
    pub stream: String,

    /// Consumer name
    pub consumer: String,

    /// Dead letter queue stream
    pub dlq_stream: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MinioConfig {
    /// MinIO endpoint
    pub endpoint: String,

    /// Access key
    pub access_key: String,

    /// Secret key
    pub secret_key: String,

    /// Upload bucket name
    pub upload_bucket: String,
}

/// Storage configuration (S3/MinIO)
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// S3/MinIO endpoint URL
    pub endpoint: String,

    /// Access key
    pub access_key: String,

    /// Secret key
    pub secret_key: String,

    /// Default bucket name
    pub bucket: String,

    /// AWS region (default: us-east-1)
    pub region: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            bucket: "akidb-documents".to_string(),
            region: "us-east-1".to_string(),
        }
    }
}

/// AkiDB connection configuration
#[derive(Debug, Clone, Deserialize)]
pub struct AkiDbConfig {
    /// gRPC endpoint (e.g., "http://localhost:50051")
    pub endpoint: String,

    /// Request timeout in milliseconds
    pub timeout_ms: u64,

    /// Max retries on failure
    pub max_retries: usize,

    /// Collection name for vector storage
    #[serde(default = "default_collection")]
    pub collection: String,
}

fn default_collection() -> String {
    "default".to_string()
}

impl Default for AkiDbConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:50051".to_string(),
            timeout_ms: 30000,
            max_retries: 3,
            collection: default_collection(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: usize,

    /// Time to wait before trying half-open (seconds)
    pub reset_timeout_secs: u64,

    /// Max calls in half-open state
    pub half_open_max_calls: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackpressureConfig {
    /// AkiDB insert latency threshold (ms)
    pub latency_threshold_ms: u64,

    /// Queue depth high water mark (activates backpressure)
    pub queue_depth_high_water: usize,

    /// Queue depth low water mark (deactivates backpressure)
    /// FIX: Added low water mark for queue depth deactivation
    #[serde(default = "default_queue_low_water")]
    pub queue_depth_low_water: usize,

    /// Pause duration when backpressure active (seconds)
    pub pause_duration_secs: u64,
}

fn default_queue_low_water() -> usize {
    500 // Default to half of typical high water mark
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    /// Memory usage threshold to pause (percentage)
    pub pause_threshold_pct: f32,

    /// Memory usage threshold to resume (percentage)
    pub resume_threshold_pct: f32,

    /// Memory polling interval (ms)
    pub poll_interval_ms: u64,

    /// FIX BUG-H052: Maximum pause duration before proceeding anyway (seconds)
    /// Prevents indefinite stalls when memory monitoring is stuck or misconfigured.
    /// After this timeout, the pipeline will log a warning and continue processing.
    #[serde(default = "default_max_pause_secs")]
    pub max_pause_duration_secs: u64,
}

fn default_max_pause_secs() -> u64 {
    300 // Default to 5 minutes max pause
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkerConfig {
    /// Target tokens per chunk
    pub target_tokens: usize,

    /// Minimum overlap tokens
    pub min_overlap: usize,

    /// Maximum overlap tokens
    pub max_overlap: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatcherConfig {
    /// Minimum batch size
    pub min_batch: usize,

    /// Maximum batch size
    pub max_batch: usize,

    /// Batch timeout (ms)
    pub timeout_ms: u64,
}

fn env_parse<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|err| IngestionError::Config(format!("invalid {key} value '{value}': {err}"))),
        Err(VarError::NotPresent) => Ok(default),
        Err(VarError::NotUnicode(value)) => Err(IngestionError::Config(format!(
            "invalid {key} value {value:?}: not valid Unicode"
        ))),
    }
}

fn env_parse_percentage(key: &str, default: f32) -> Result<f32> {
    let value = env_parse(key, default)?;
    if !value.is_finite() || !(0.0..=100.0).contains(&value) {
        return Err(IngestionError::Config(format!(
            "{key} must be finite and between 0 and 100"
        )));
    }
    Ok(value)
}

impl IngestionConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Self {
            nats: NatsConfig {
                url: std::env::var("NATS_URL")
                    .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
                stream: std::env::var("NATS_STREAM")
                    .unwrap_or_else(|_| "akidb-uploads".to_string()),
                consumer: std::env::var("NATS_CONSUMER")
                    .unwrap_or_else(|_| "ingestion-orchestrator".to_string()),
                dlq_stream: std::env::var("NATS_DLQ_STREAM")
                    .unwrap_or_else(|_| "akidb-dlq".to_string()),
            },
            minio: MinioConfig {
                endpoint: std::env::var("MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "localhost:9000".to_string()),
                access_key: std::env::var("MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".to_string()),
                secret_key: std::env::var("MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".to_string()),
                upload_bucket: std::env::var("MINIO_UPLOAD_BUCKET")
                    .unwrap_or_else(|_| "uploads".to_string()),
            },
            storage: StorageConfig {
                endpoint: std::env::var("STORAGE_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:9000".to_string()),
                access_key: std::env::var("STORAGE_ACCESS_KEY")
                    .or_else(|_| std::env::var("MINIO_ACCESS_KEY"))
                    .unwrap_or_else(|_| "minioadmin".to_string()),
                secret_key: std::env::var("STORAGE_SECRET_KEY")
                    .or_else(|_| std::env::var("MINIO_SECRET_KEY"))
                    .unwrap_or_else(|_| "minioadmin".to_string()),
                bucket: std::env::var("STORAGE_BUCKET")
                    .unwrap_or_else(|_| "akidb-documents".to_string()),
                region: std::env::var("STORAGE_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
            },
            akidb: AkiDbConfig {
                endpoint: std::env::var("AKIDB_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:50051".to_string()),
                timeout_ms: env_parse("AKIDB_TIMEOUT_MS", 30000)?,
                max_retries: env_parse("AKIDB_MAX_RETRIES", 3)?,
                collection: std::env::var("AKIDB_COLLECTION")
                    .unwrap_or_else(|_| "default".to_string()),
            },
            akidb_coordinator: std::env::var("AKIDB_COORDINATOR")
                .unwrap_or_else(|_| "http://localhost:50050".to_string()),
            embedding_url: std::env::var("EMBEDDING_URL")
                .unwrap_or_else(|_| "http://localhost:8000".to_string()),
            doc_parser_url: std::env::var("DOC_PARSER_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: env_parse("CIRCUIT_BREAKER_THRESHOLD", 3)?,
                reset_timeout_secs: env_parse("CIRCUIT_BREAKER_RESET_SECS", 30)?,
                half_open_max_calls: env_parse("CIRCUIT_BREAKER_HALF_OPEN_CALLS", 1)?,
            },
            backpressure: BackpressureConfig {
                latency_threshold_ms: env_parse("BACKPRESSURE_LATENCY_THRESHOLD_MS", 500)?,
                queue_depth_high_water: env_parse("BACKPRESSURE_QUEUE_DEPTH", 10000)?,
                queue_depth_low_water: env_parse("BACKPRESSURE_QUEUE_LOW_WATER", 5000)?,
                pause_duration_secs: env_parse("BACKPRESSURE_PAUSE_SECS", 5)?,
            },
            memory: MemoryConfig {
                pause_threshold_pct: env_parse_percentage("MEMORY_PAUSE_THRESHOLD_PCT", 70.0)?,
                resume_threshold_pct: env_parse_percentage("MEMORY_RESUME_THRESHOLD_PCT", 60.0)?,
                poll_interval_ms: env_parse("MEMORY_POLL_INTERVAL_MS", 1000)?,
                // FIX BUG-H052: Add max pause duration config
                max_pause_duration_secs: env_parse("MEMORY_MAX_PAUSE_SECS", 300)?,
            },
            chunker: ChunkerConfig {
                target_tokens: env_parse("CHUNKER_TARGET_TOKENS", 512)?,
                min_overlap: env_parse("CHUNKER_MIN_OVERLAP", 20)?,
                max_overlap: env_parse("CHUNKER_MAX_OVERLAP", 50)?,
            },
            batcher: BatcherConfig {
                min_batch: env_parse("BATCHER_MIN_BATCH", 16)?,
                max_batch: env_parse("BATCHER_MAX_BATCH", 64)?,
                timeout_ms: env_parse("BATCHER_TIMEOUT_MS", 100)?,
            },
        })
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            reset_timeout_secs: 30,
            half_open_max_calls: 1,
        }
    }
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            latency_threshold_ms: 500,
            queue_depth_high_water: 10000,
            queue_depth_low_water: 5000, // Half of high water by default
            pause_duration_secs: 5,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            pause_threshold_pct: 70.0,
            resume_threshold_pct: 60.0,
            poll_interval_ms: 1000,
            // FIX BUG-H052: Include max pause duration in default
            max_pause_duration_secs: 300,
        }
    }
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            target_tokens: 512,
            min_overlap: 20,
            max_overlap: 50,
        }
    }
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            min_batch: 16,
            max_batch: 64,
            timeout_ms: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.original {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    #[test]
    fn test_default_configs() {
        let cb = CircuitBreakerConfig::default();
        assert_eq!(cb.failure_threshold, 3);

        let bp = BackpressureConfig::default();
        assert_eq!(bp.latency_threshold_ms, 500);

        let mem = MemoryConfig::default();
        assert_eq!(mem.pause_threshold_pct, 70.0);

        let chunker = ChunkerConfig::default();
        assert_eq!(chunker.target_tokens, 512);

        let batcher = BatcherConfig::default();
        assert_eq!(batcher.min_batch, 16);
    }

    #[test]
    fn test_from_env_rejects_invalid_numeric_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("AKIDB_TIMEOUT_MS", "not-a-number");

        let err = IngestionConfig::from_env()
            .expect_err("invalid numeric env var should reject configuration");

        match err {
            IngestionError::Config(message) => {
                assert!(message.contains("AKIDB_TIMEOUT_MS"), "{message}");
                assert!(message.contains("not-a-number"), "{message}");
            }
            other => panic!("expected config error, got {other:?}"),
        }
    }

    #[test]
    fn test_from_env_rejects_non_finite_memory_thresholds() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvGuard::set("MEMORY_PAUSE_THRESHOLD_PCT", "NaN");

        let err = IngestionConfig::from_env()
            .expect_err("non-finite memory threshold should reject configuration");

        match err {
            IngestionError::Config(message) => {
                assert!(message.contains("MEMORY_PAUSE_THRESHOLD_PCT"), "{message}");
                assert!(message.contains("finite"), "{message}");
            }
            other => panic!("expected config error, got {other:?}"),
        }
    }
}
