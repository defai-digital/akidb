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

    /// Model identifier sent to the OpenAI-compatible embedding endpoint
    pub embedding_model: String,

    /// Python parser service URL
    pub doc_parser_url: String,

    /// Prometheus and health HTTP listen address
    pub metrics_addr: String,

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

    /// Number of JetStream replicas
    pub replicas: usize,
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

fn env_value(key: &str) -> Result<Option<String>> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(value)) => Err(IngestionError::Config(format!(
            "invalid {key} value {value:?}: not valid Unicode"
        ))),
    }
}

fn env_or_file(value_key: &str, file_key: &str) -> Result<Option<String>> {
    if let Some(value) = env_value(value_key)? {
        let value = value.trim().to_string();
        if value.is_empty() {
            return Err(IngestionError::Config(format!(
                "{value_key} contains an empty secret"
            )));
        }
        return Ok(Some(value));
    }
    let Some(path) = env_value(file_key)? else {
        return Ok(None);
    };
    let value = std::fs::read_to_string(&path).map_err(|error| {
        IngestionError::Config(format!("failed to read {file_key} path '{path}': {error}"))
    })?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(IngestionError::Config(format!(
            "{file_key} path '{path}' contains an empty secret"
        )));
    }
    Ok(Some(value))
}

fn credential_from_env(
    primary_value_key: &str,
    primary_file_key: &str,
    fallback_value_key: &str,
    fallback_file_key: &str,
    default: &str,
) -> Result<String> {
    if let Some(value) = env_or_file(primary_value_key, primary_file_key)? {
        return Ok(value);
    }
    Ok(env_or_file(fallback_value_key, fallback_file_key)?.unwrap_or_else(|| default.to_string()))
}

impl IngestionConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        let nats_replicas = env_parse("NATS_REPLICAS", 1)?;
        if !(1..=5).contains(&nats_replicas) {
            return Err(IngestionError::Config(
                "NATS_REPLICAS must be between 1 and 5".to_string(),
            ));
        }

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
                replicas: nats_replicas,
            },
            minio: MinioConfig {
                endpoint: std::env::var("MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "localhost:9000".to_string()),
                access_key: env_or_file("MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY_FILE")?
                    .unwrap_or_else(|| "minioadmin".to_string()),
                secret_key: env_or_file("MINIO_SECRET_KEY", "MINIO_SECRET_KEY_FILE")?
                    .unwrap_or_else(|| "minioadmin".to_string()),
                upload_bucket: std::env::var("MINIO_UPLOAD_BUCKET")
                    .unwrap_or_else(|_| "uploads".to_string()),
            },
            storage: StorageConfig {
                endpoint: std::env::var("STORAGE_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:9000".to_string()),
                access_key: credential_from_env(
                    "STORAGE_ACCESS_KEY",
                    "STORAGE_ACCESS_KEY_FILE",
                    "MINIO_ACCESS_KEY",
                    "MINIO_ACCESS_KEY_FILE",
                    "minioadmin",
                )?,
                secret_key: credential_from_env(
                    "STORAGE_SECRET_KEY",
                    "STORAGE_SECRET_KEY_FILE",
                    "MINIO_SECRET_KEY",
                    "MINIO_SECRET_KEY_FILE",
                    "minioadmin",
                )?,
                bucket: std::env::var("STORAGE_BUCKET")
                    .unwrap_or_else(|_| "akidb-documents".to_string()),
                region: std::env::var("STORAGE_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
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
            embedding_model: std::env::var("EMBEDDING_MODEL")
                .unwrap_or_else(|_| "Qwen/Qwen3-Embedding-4B".to_string()),
            doc_parser_url: std::env::var("DOC_PARSER_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            metrics_addr: std::env::var("INGESTION_METRICS_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:9093".to_string()),
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

        fn unset(key: &'static str) -> Self {
            let original = std::env::var(key).ok();
            std::env::remove_var(key);
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

    #[test]
    fn test_from_env_reads_storage_credentials_from_secret_files() {
        let _lock = ENV_LOCK.lock().unwrap();
        let access_file = tempfile::NamedTempFile::new().unwrap();
        let secret_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(access_file.path(), "access-from-file\n").unwrap();
        std::fs::write(secret_file.path(), "secret-from-file\n").unwrap();
        let _access_value = EnvGuard::unset("STORAGE_ACCESS_KEY");
        let _secret_value = EnvGuard::unset("STORAGE_SECRET_KEY");
        let _access_file = EnvGuard::set(
            "STORAGE_ACCESS_KEY_FILE",
            access_file.path().to_str().unwrap(),
        );
        let _secret_file = EnvGuard::set(
            "STORAGE_SECRET_KEY_FILE",
            secret_file.path().to_str().unwrap(),
        );

        let config = IngestionConfig::from_env().unwrap();

        assert_eq!(config.storage.access_key, "access-from-file");
        assert_eq!(config.storage.secret_key, "secret-from-file");
    }

    #[test]
    fn test_from_env_rejects_empty_secret_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        let secret_file = tempfile::NamedTempFile::new().unwrap();
        let _secret_value = EnvGuard::unset("STORAGE_SECRET_KEY");
        let _secret_file = EnvGuard::set(
            "STORAGE_SECRET_KEY_FILE",
            secret_file.path().to_str().unwrap(),
        );

        let error = IngestionConfig::from_env().unwrap_err();

        assert!(matches!(
            error,
            IngestionError::Config(message) if message.contains("empty secret")
        ));
    }

    #[test]
    fn test_from_env_rejects_empty_direct_secret() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _secret_value = EnvGuard::set("STORAGE_SECRET_KEY", "  ");
        let _secret_file = EnvGuard::unset("STORAGE_SECRET_KEY_FILE");

        let error = IngestionConfig::from_env().unwrap_err();

        assert!(matches!(
            error,
            IngestionError::Config(message)
                if message.contains("STORAGE_SECRET_KEY contains an empty secret")
        ));
    }
}
