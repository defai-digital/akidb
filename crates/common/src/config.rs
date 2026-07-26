//! Configuration types for AkiDB

use serde::{Deserialize, Serialize};

/// Main AkiDB configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AkiDbConfig {
    pub server: ServerConfig,
    pub index: IndexSettings,
    pub storage: StorageConfig,
    #[serde(default)]
    pub sql: SqlMetadataConfig,
    pub observability: ObservabilityConfig,
    pub slo: SloConfig,
    pub embedding: EmbeddingClientConfig,
    /// Authentication and request authorization (v3.1 trust).
    #[serde(default)]
    pub auth: AuthConfig,
    /// Read/plan-only operations console settings.
    #[serde(default)]
    pub management: ManagementConfig,
    /// Immutable single-node generation serving preview. Disabled by default.
    #[serde(default)]
    pub generation_serving: GenerationServingConfig,
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
            // Secure appliance default: loopback only (ADR-0002.2 / GAP-029).
            host: "127.0.0.1".to_string(),
            port: 8080,
            grpc_port: 50051,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

/// How strictly the data-plane requires a bearer token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// Token optional on loopback; required on non-loopback binds.
    #[default]
    LoopbackOptional,
    /// Token always required.
    Required,
    /// No auth checks (tests only).
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication mode.
    #[serde(default)]
    pub mode: AuthMode,
    /// Path to a token file (mode 0600). Created on first start when missing.
    #[serde(default = "default_token_file")]
    pub token_file: String,
    /// Optional explicit token (overrides file). Prefer env/file in production.
    #[serde(default)]
    pub token: Option<String>,
    /// Workspace ACL settings.
    #[serde(default)]
    pub acl: AclConfig,
}

fn default_token_file() -> String {
    "./data/auth.token".to_string()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::LoopbackOptional,
            token_file: default_token_file(),
            token: None,
            acl: AclConfig::default(),
        }
    }
}

/// Read/plan-only management API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementConfig {
    /// Maximum number of redacted audit events retained in memory.
    #[serde(default = "default_audit_max_entries")]
    pub audit_max_entries: usize,
    /// Import validation-plan limits. Planning never executes ingestion.
    #[serde(default)]
    pub import_plan: ImportPlanConfig,
}

impl Default for ManagementConfig {
    fn default() -> Self {
        Self {
            audit_max_entries: default_audit_max_entries(),
            import_plan: ImportPlanConfig::default(),
        }
    }
}

fn default_audit_max_entries() -> usize {
    1000
}

/// Limits for validation-only import planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportPlanConfig {
    /// Enable planning when a trusted staging resolver is connected.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_import_plan_source_bytes")]
    pub max_source_bytes: u64,
    #[serde(default = "default_import_plan_expanded_bytes")]
    pub max_expanded_bytes: u64,
    #[serde(default = "default_import_plan_ttl_seconds")]
    pub plan_ttl_seconds: u64,
}

impl Default for ImportPlanConfig {
    fn default() -> Self {
        Self {
            // Disabled until the upload gateway can resolve server-issued,
            // immutable staging references in this process.
            enabled: false,
            max_source_bytes: default_import_plan_source_bytes(),
            max_expanded_bytes: default_import_plan_expanded_bytes(),
            plan_ttl_seconds: default_import_plan_ttl_seconds(),
        }
    }
}

fn default_import_plan_source_bytes() -> u64 {
    100 * 1024 * 1024
}

fn default_import_plan_expanded_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_import_plan_ttl_seconds() -> u64 {
    300
}

/// Immutable generation serving with an optional Phase 3 PostgreSQL replica
/// control loop.
///
/// Enabling this replaces the mutable gRPC data path. It does not enable HA,
/// sharding, PostgreSQL control-plane authority, or automatic failover.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationServingConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Stable identity of this local data volume. Required when enabled.
    #[serde(default)]
    pub replica_id: String,
    #[serde(default = "default_generation_root")]
    pub generation_root: String,
    #[serde(default = "default_generation_control_path")]
    pub control_rocksdb_path: String,
    #[serde(default = "default_generation_download_path")]
    pub download_path: String,
    #[serde(default = "default_generation_collection")]
    pub default_collection: String,
    /// Path to the publication-control bearer token. This credential is
    /// intentionally separate from the read data-plane token.
    #[serde(default = "default_generation_control_token_file")]
    pub control_token_file: String,
    /// Optional explicit publication-control token. Prefer env/file in
    /// production.
    #[serde(default)]
    pub control_token: Option<String>,
    /// Empty means only `storage.minio.bucket`.
    #[serde(default)]
    pub allowed_buckets: Vec<String>,
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
    #[serde(default = "default_true")]
    pub require_version_or_digest_key: bool,
    #[serde(default = "default_max_bundle_size")]
    pub max_bundle_size_bytes: u64,
    #[serde(default = "default_generation_max_vectors")]
    pub max_vectors: u64,
    #[serde(default = "default_generation_max_nodes")]
    pub max_graph_nodes: u64,
    #[serde(default = "default_generation_max_edges")]
    pub max_graph_edges: u64,
    /// Free bytes that must remain after the estimated immutable shadow build.
    #[serde(default = "default_generation_minimum_free_bytes_after_build")]
    pub minimum_free_bytes_after_build: u64,
    /// Conservative disk amplification applied to bundle + vector payload.
    #[serde(default = "default_generation_build_overhead_percent")]
    pub estimated_build_overhead_percent: u16,
    #[serde(default)]
    pub replica_control: GenerationReplicaControlConfig,
}

impl Default for GenerationServingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            replica_id: String::new(),
            generation_root: default_generation_root(),
            control_rocksdb_path: default_generation_control_path(),
            download_path: default_generation_download_path(),
            default_collection: default_generation_collection(),
            control_token_file: default_generation_control_token_file(),
            control_token: None,
            allowed_buckets: Vec::new(),
            s3_region: default_s3_region(),
            require_version_or_digest_key: true,
            max_bundle_size_bytes: default_max_bundle_size(),
            max_vectors: default_generation_max_vectors(),
            max_graph_nodes: default_generation_max_nodes(),
            max_graph_edges: default_generation_max_edges(),
            minimum_free_bytes_after_build: default_generation_minimum_free_bytes_after_build(),
            estimated_build_overhead_percent: default_generation_build_overhead_percent(),
            replica_control: GenerationReplicaControlConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaPostgresTlsMode {
    Require,
    Disable,
}

fn default_replica_postgres_tls_mode() -> ReplicaPostgresTlsMode {
    ReplicaPostgresTlsMode::Require
}

/// PostgreSQL authority and replica-admission settings.
///
/// The connection URL is resolved only from the named environment variable so
/// database credentials do not need to be written into the AkiDB TOML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationReplicaControlConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_replica_postgres_url_env")]
    pub postgres_url_env: String,
    #[serde(default = "default_replica_postgres_tls_mode")]
    pub postgres_tls_mode: ReplicaPostgresTlsMode,
    #[serde(default)]
    pub postgres_ca_certificate_path: Option<String>,
    /// Routable private gRPC endpoint advertised to the gateway/control plane.
    #[serde(default)]
    pub endpoint: String,
    /// Stable availability-zone, rack, or host failure domain.
    #[serde(default)]
    pub failure_domain: String,
    #[serde(default = "default_replica_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_replica_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_replica_index_format_version")]
    pub index_format_version: String,
    #[serde(default = "default_supported_graph_schema_versions")]
    pub supported_graph_schema_versions: Vec<String>,
    /// Periodically scan and retain only active/previous/staged generations.
    #[serde(default)]
    pub generation_gc_enabled: bool,
    #[serde(default = "default_generation_gc_interval_ms")]
    pub generation_gc_interval_ms: u64,
    #[serde(default = "default_generation_gc_minimum_age_ms")]
    pub generation_gc_minimum_age_ms: u64,
    /// Report candidates and audit the run without deleting local directories.
    #[serde(default = "default_true")]
    pub generation_gc_dry_run: bool,
}

impl Default for GenerationReplicaControlConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            postgres_url_env: default_replica_postgres_url_env(),
            postgres_tls_mode: default_replica_postgres_tls_mode(),
            postgres_ca_certificate_path: None,
            endpoint: String::new(),
            failure_domain: String::new(),
            poll_interval_ms: default_replica_poll_interval_ms(),
            heartbeat_interval_ms: default_replica_heartbeat_interval_ms(),
            index_format_version: default_replica_index_format_version(),
            supported_graph_schema_versions: default_supported_graph_schema_versions(),
            generation_gc_enabled: false,
            generation_gc_interval_ms: default_generation_gc_interval_ms(),
            generation_gc_minimum_age_ms: default_generation_gc_minimum_age_ms(),
            generation_gc_dry_run: true,
        }
    }
}

fn default_replica_postgres_url_env() -> String {
    "AKIDB_KNOWLEDGE_POSTGRES_URL".to_string()
}

fn default_replica_poll_interval_ms() -> u64 {
    1_000
}

fn default_replica_heartbeat_interval_ms() -> u64 {
    5_000
}

fn default_replica_index_format_version() -> String {
    "akidb-generation-v1".to_string()
}

fn default_supported_graph_schema_versions() -> Vec<String> {
    vec!["ax.knowledge-graph.v1".to_string()]
}

fn default_generation_gc_interval_ms() -> u64 {
    60 * 60 * 1_000
}

fn default_generation_gc_minimum_age_ms() -> u64 {
    24 * 60 * 60 * 1_000
}

fn default_generation_root() -> String {
    "./data/generations".to_string()
}

fn default_generation_control_path() -> String {
    "./data/generation-control".to_string()
}

fn default_generation_download_path() -> String {
    "./data/generation-downloads".to_string()
}

fn default_generation_collection() -> String {
    "default".to_string()
}

fn default_generation_control_token_file() -> String {
    "./data/generation-control.token".to_string()
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

fn default_max_bundle_size() -> u64 {
    50 * 1024 * 1024 * 1024
}

fn default_generation_max_vectors() -> u64 {
    10_000_000
}

fn default_generation_max_nodes() -> u64 {
    20_000_000
}

fn default_generation_max_edges() -> u64 {
    50_000_000
}

fn default_generation_minimum_free_bytes_after_build() -> u64 {
    1024 * 1024 * 1024
}

fn default_generation_build_overhead_percent() -> u16 {
    200
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclConfig {
    /// Default workspace stamped on writes when the client omits one.
    #[serde(default = "default_workspace_id")]
    pub default_workspace: String,
    /// When true, search/memory reads are scoped to the caller workspace.
    #[serde(default = "default_true")]
    pub enforce_workspace: bool,
}

fn default_workspace_id() -> String {
    "default".to_string()
}

fn default_true() -> bool {
    true
}

impl Default for AclConfig {
    fn default() -> Self {
        Self {
            default_workspace: default_workspace_id(),
            enforce_workspace: true,
        }
    }
}

/// Filtered ANN strategy (ADR-0002.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    Pre,
    Post,
    #[default]
    Adaptive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSettings {
    #[serde(default)]
    pub mode: FilterMode,
    /// Over-fetch factor for post-filter candidate generation.
    #[serde(default = "default_overfetch")]
    pub postfilter_overfetch_factor: u32,
    /// Hard bound for the largest adaptive post-filter candidate window.
    #[serde(default = "default_max_postfilter_candidates")]
    pub max_postfilter_candidates: usize,
    /// When estimated selectivity is at or below this, adaptive prefers pre-filter.
    #[serde(default = "default_adaptive_pre_selectivity")]
    pub adaptive_pre_selectivity: f32,
}

fn default_overfetch() -> u32 {
    5
}

fn default_max_postfilter_candidates() -> usize {
    16_384
}

fn default_adaptive_pre_selectivity() -> f32 {
    0.20
}

impl Default for FilterSettings {
    fn default() -> Self {
        Self {
            mode: FilterMode::Adaptive,
            postfilter_overfetch_factor: default_overfetch(),
            max_postfilter_candidates: default_max_postfilter_candidates(),
            adaptive_pre_selectivity: default_adaptive_pre_selectivity(),
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
    /// Vector storage precision: `f32` (default) or `f16` (GAP-010).
    #[serde(default = "default_vector_precision")]
    pub vector_precision: String,
    /// Distance metric: `cosine` (default), `l2`, or `ip`.
    #[serde(default = "default_metric")]
    pub metric: String,
    /// Filtered search settings.
    #[serde(default)]
    pub filter: FilterSettings,
    /// Rebuild settings
    pub rebuild: RebuildSettings,
    /// Tombstone settings
    pub tombstone: TombstoneSettings,
}

fn default_vector_precision() -> String {
    "f32".to_string()
}

fn default_metric() -> String {
    "cosine".to_string()
}

impl Default for IndexSettings {
    fn default() -> Self {
        Self {
            index_type: "HNSW".to_string(),
            hnsw_m: 16,
            hnsw_ef_construction: 128,
            hnsw_ef_search: 64,
            vector_precision: default_vector_precision(),
            metric: default_metric(),
            filter: FilterSettings::default(),
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SloConfig {
    /// Reference configuration for SLO targets
    pub reference: SloReference,
    /// Backpressure settings
    pub backpressure: BackpressureConfig,
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
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.grpc_port, 50051);
        assert_eq!(config.index.hnsw_m, 16);
        assert_eq!(config.index.vector_precision, "f32");
        assert_eq!(config.index.metric, "cosine");
        assert_eq!(config.index.filter.mode, FilterMode::Adaptive);
        assert_eq!(config.index.filter.max_postfilter_candidates, 16_384);
        assert_eq!(config.auth.mode, AuthMode::LoopbackOptional);
        assert!(config.auth.acl.enforce_workspace);
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
