//! Configuration types for AkiDB

use serde::{Deserialize, Serialize};

/// Main AkiDB configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AkiDbConfig {
    pub server: ServerConfig,
    pub index: IndexSettings,
    pub storage: StorageConfig,
    pub observability: ObservabilityConfig,
    pub slo: SloConfig,
}

impl Default for AkiDbConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            index: IndexSettings::default(),
            storage: StorageConfig::default(),
            observability: ObservabilityConfig::default(),
            slo: SloConfig::default(),
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
    /// Index type (IVF-Flat)
    pub index_type: String,
    /// Number of clusters
    pub nlist: u32,
    /// Default number of probes
    pub nprobe: u32,
    /// Accelerator settings. GPU acceleration is not supported in active Mac-only builds.
    pub gpu: GpuSettings,
    /// Rebuild settings
    pub rebuild: RebuildSettings,
    /// Tombstone settings
    pub tombstone: TombstoneSettings,
}

impl Default for IndexSettings {
    fn default() -> Self {
        Self {
            index_type: "IVF4096,Flat".to_string(),
            nlist: 4096,
            nprobe: 32,
            gpu: GpuSettings::default(),
            rebuild: RebuildSettings::default(),
            tombstone: TombstoneSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuSettings {
    pub enabled: bool,
    pub device_id: i32,
    pub memory_fraction: f32,
    pub fallback_to_cpu: bool,
}

impl Default for GpuSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            device_id: 0,
            memory_fraction: 0.6,
            fallback_to_cpu: true,
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
    /// Use accelerator bitset for tombstone filtering
    pub use_gpu_bitset: bool,
    /// Maximum tombstones before forced compaction
    pub max_count: u64,
}

impl Default for TombstoneSettings {
    fn default() -> Self {
        Self {
            use_gpu_bitset: false,
            max_count: 100_000,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AkiDbConfig::default();
        assert_eq!(config.server.grpc_port, 50051);
        assert_eq!(config.index.nlist, 4096);
        assert_eq!(config.slo.reference.dimensions, 768);
    }
}
