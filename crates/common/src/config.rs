//! Configuration types for AkiDB

use serde::{Deserialize, Serialize};

/// Main AkiDB configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AkiDbConfig {
    pub server: ServerConfig,
    pub index: IndexSettings,
    pub storage: StorageConfig,
    #[serde(default)]
    pub sql: SqlMetadataConfig,
    pub observability: ObservabilityConfig,
    pub slo: SloConfig,
    pub embedding: EmbeddingClientConfig,
}

impl Default for AkiDbConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            index: IndexSettings::default(),
            storage: StorageConfig::default(),
            sql: SqlMetadataConfig::default(),
            observability: ObservabilityConfig::default(),
            slo: SloConfig::default(),
            embedding: EmbeddingClientConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub tls_enabled: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            grpc_port: 50051,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSettings {
    /// Index type (HNSW)
    pub index_type: String,
    /// HNSW M parameter (connections per layer)
    pub hnsw_m: u32,
    /// HNSW ef_construction parameter
    pub hnsw_ef_construction: u32,
    /// Default ef_search parameter
    pub hnsw_ef_search: u32,
    /// Rebuild settings
    pub rebuild: RebuildSettings,
    /// Tombstone settings
    pub tombstone: TombstoneSettings,
}

impl Default for IndexSettings {
    fn default() -> Self {
        Self {
            index_type: "HNSW".to_string(),
            hnsw_m: 16,
            hnsw_ef_construction: 128,
            hnsw_ef_search: 64,
            rebuild: RebuildSettings::default(),
            tombstone: TombstoneSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildSettings {
    /// Trigger rebuild when tombstone ratio exceeds this
    pub tombstone_ratio_trigger: f32,
    /// Maximum rebuild duration in seconds
    pub max_duration_seconds: u64,
    /// Schedule rebuilds during these hours (0-23)
    pub preferred_hours: Vec<u8>,
}

impl Default for RebuildSettings {
    fn default() -> Self {
        Self {
            tombstone_ratio_trigger: 0.10,
            max_duration_seconds: 300,
            preferred_hours: vec![2, 3, 4], // 2-5 AM
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneSettings {
    /// Maximum tombstones before forced compaction
    pub max_count: u64,
}

impl Default for TombstoneSettings {
    fn default() -> Self {
        Self { max_count: 100_000 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub rocksdb_path: String,
    pub wal_enabled: bool,
    pub wal_path: String,
    pub minio: MinioConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            rocksdb_path: "./data/rocksdb".to_string(),
            wal_enabled: true,
            wal_path: "./data/wal".to_string(),
            minio: MinioConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlMetadataConfig {
    /// Whether the optional SQL metadata index is enabled.
    pub enabled: bool,
    /// SQL backend name. Use `sqlite` by default; `postgres` requires the server postgres feature.
    pub backend: String,
    /// SQLite database path for standalone metadata filters and audit-ready records.
    pub sqlite_path: String,
    /// PostgreSQL connection URL for enterprise metadata filters and structured RAG.
    pub postgres_url: Option<String>,
}

impl Default for SqlMetadataConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: "sqlite".to_string(),
            sqlite_path: "./data/akidb-metadata.sqlite".to_string(),
            postgres_url: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinioConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub use_ssl: bool,
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            endpoint: "localhost:9000".to_string(),
            bucket: "akidb-snapshots".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            use_ssl: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub tracing_enabled: bool,
    pub otlp_endpoint: Option<String>,
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub log_level: String,
    pub log_format: LogFormat,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: true,
            otlp_endpoint: Some("http://localhost:4317".to_string()),
            metrics_enabled: true,
            metrics_port: 9090,
            log_level: "info".to_string(),
            log_format: LogFormat::Json,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloConfig {
    /// Reference configuration for SLO targets
    pub reference: SloReference,
    /// Backpressure settings
    pub backpressure: BackpressureConfig,
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            reference: SloReference::default(),
            backpressure: BackpressureConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloReference {
    pub dimensions: usize,
    pub vectors_per_shard: usize,
    pub top_k: usize,
    pub nprobe: u32,
    pub batch_size: usize,
    /// Target P95 latency in ms
    pub target_p95_ms: u64,
}

impl Default for SloReference {
    fn default() -> Self {
        Self {
            dimensions: 768,
            vectors_per_shard: 1_000_000,
            top_k: 10,
            nprobe: 32,
            batch_size: 1,
            target_p95_ms: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Soft breach threshold (P95 in ms)
    pub soft_breach_ms: u64,
    /// Hard breach threshold (P95 in ms)
    pub hard_breach_ms: u64,
    /// Enable degraded mode (return partial results)
    pub degraded_mode_enabled: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            soft_breach_ms: 50,
            hard_breach_ms: 75,
            degraded_mode_enabled: true,
        }
    }
}

/// Configuration for the local embedding HTTP client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingClientConfig {
    /// Whether the embedding client is enabled
    pub enabled: bool,
    /// OpenAI-compatible `/v1/embeddings` endpoint URL
    pub url: String,
    /// Model name to request
    pub model: String,
    /// Expected embedding dimensions
    pub dimensions: usize,
    /// HTTP request timeout in milliseconds
    pub timeout_ms: u64,
    /// Maximum batch size per request
    pub max_batch_size: usize,
}

impl Default for EmbeddingClientConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "http://127.0.0.1:8081/v1/embeddings".to_string(),
            model: "Qwen/Qwen3-Embedding-4B".to_string(),
            dimensions: 2560,
            timeout_ms: 10_000,
            max_batch_size: 32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AkiDbConfig::default();
        assert_eq!(config.server.grpc_port, 50051);
        assert_eq!(config.index.hnsw_m, 16);
        assert_eq!(config.slo.reference.dimensions, 768);
        assert!(!config.sql.enabled);
        assert_eq!(config.sql.backend, "sqlite");
        assert!(config.sql.postgres_url.is_none());
    }

    #[test]
    fn test_parse_config_without_sql_uses_default() {
        let config: AkiDbConfig = toml::from_str(
            r#"
            [server]
            host = "127.0.0.1"
            port = 8080
            grpc_port = 50051
            tls_enabled = false

            [index]
            index_type = "HNSW"
            hnsw_m = 16
            hnsw_ef_construction = 128
            hnsw_ef_search = 64

            [index.rebuild]
            tombstone_ratio_trigger = 0.10
            max_duration_seconds = 300
            preferred_hours = [2, 3, 4]

            [index.tombstone]
            max_count = 100000

            [storage]
            rocksdb_path = "./data/rocksdb"
            wal_enabled = true
            wal_path = "./data/wal"

            [storage.minio]
            endpoint = ""
            bucket = ""
            access_key = ""
            secret_key = ""
            use_ssl = false

            [observability]
            tracing_enabled = false
            metrics_enabled = false
            metrics_port = 9090
            log_level = "info"
            log_format = "pretty"

            [slo]
            [slo.reference]
            dimensions = 768
            vectors_per_shard = 1000000
            top_k = 10
            nprobe = 32
            batch_size = 1
            target_p95_ms = 50

            [slo.backpressure]
            soft_breach_ms = 50
            hard_breach_ms = 75
            degraded_mode_enabled = true

            [embedding]
            enabled = false
            url = "http://127.0.0.1:8081/v1/embeddings"
            model = "Qwen/Qwen3-Embedding-4B"
            dimensions = 2560
            timeout_ms = 10000
            max_batch_size = 32
            "#,
        )
        .unwrap();

        assert!(!config.sql.enabled);
        assert_eq!(config.sql.sqlite_path, "./data/akidb-metadata.sqlite");
        assert!(config.sql.postgres_url.is_none());
    }

    #[test]
    fn test_parse_postgres_sql_config() {
        let config: SqlMetadataConfig = toml::from_str(
            r#"
            enabled = true
            backend = "postgres"
            sqlite_path = "./data/akidb-metadata.sqlite"
            postgres_url = "postgres://user:pass@localhost:5432/akidb"
            "#,
        )
        .unwrap();

        assert!(config.enabled);
        assert_eq!(config.backend, "postgres");
        assert_eq!(
            config.postgres_url.as_deref(),
            Some("postgres://user:pass@localhost:5432/akidb")
        );
    }

    #[test]
    fn test_parse_lowercase_log_format() {
        let config: ObservabilityConfig = serde_json::from_str(
            r#"
            {
                "tracing_enabled": false,
                "otlp_endpoint": null,
                "metrics_enabled": false,
                "metrics_port": 9090,
                "log_level": "info",
                "log_format": "pretty"
            }
            "#,
        )
        .unwrap();

        assert_eq!(config.log_format, LogFormat::Pretty);
    }
}
