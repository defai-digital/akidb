//! Configuration types for AkiDB

use serde::{Deserialize, Serialize};
use url::Url;

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
    /// Authoritative Memory profile. Disabled by default.
    #[serde(default)]
    pub memory: MemoryServiceConfig,
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
    /// Max gRPC message size (decode and encode) on the data plane.
    ///
    /// tonic's default 4 MiB decode cap silently rejects InsertBatch payloads
    /// above roughly 1,300 768-dim f32 vectors with an opaque transport error.
    /// The default here fits a 10k-vector 768-dim batch with headroom.
    #[serde(default = "default_grpc_max_message_bytes")]
    pub grpc_max_message_bytes: usize,
    /// Serve the standard `grpc.health.v1` protocol alongside the custom
    /// `Akidb/Health` RPC (Kubernetes gRPC probes, grpcurl).
    #[serde(default = "default_grpc_health_enabled")]
    pub grpc_health_enabled: bool,
    /// Serve gRPC server reflection (discloses the service schema). Keep off
    /// on non-loopback binds unless operators need grpcurl-style debugging.
    #[serde(default)]
    pub grpc_reflection_enabled: bool,
}

fn default_grpc_max_message_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_grpc_health_enabled() -> bool {
    true
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
            grpc_max_message_bytes: default_grpc_max_message_bytes(),
            grpc_health_enabled: default_grpc_health_enabled(),
            grpc_reflection_enabled: false,
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
    /// Versioned principal and credential registry for authoritative Memory.
    #[serde(default)]
    pub principals: Vec<PrincipalConfig>,
    /// Authorization epoch. Increment to invalidate cached grant decisions.
    #[serde(default = "default_authorization_epoch")]
    pub authorization_epoch: u64,
    /// Memory-specific fail-closed and one-workspace settings.
    #[serde(default)]
    pub memory: MemoryAuthorizationConfig,
}

fn default_token_file() -> String {
    "./data/auth.token".to_string()
}

fn default_authorization_epoch() -> u64 {
    1
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::LoopbackOptional,
            token_file: default_token_file(),
            token: None,
            acl: AclConfig::default(),
            principals: Vec::new(),
            authorization_epoch: default_authorization_epoch(),
            memory: MemoryAuthorizationConfig::default(),
        }
    }
}

/// Authenticated identity category. It informs audit and default operational
/// policy; it does not grant capabilities by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    #[default]
    Service,
    Agent,
    Administrator,
}

/// One independently rotatable bearer credential for a principal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalCredentialConfig {
    pub credential_id: String,
    /// Inline tokens are supported for tests/bootstrap; prefer file or env.
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub token_file: Option<String>,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default)]
    pub not_before_ms: Option<u64>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
}

/// Versioned principal grants. Client request values can only narrow these
/// maximums.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalConfig {
    pub principal_id: String,
    #[serde(default)]
    pub kind: PrincipalKind,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default = "default_grant_version")]
    pub grant_version: u64,
    #[serde(default)]
    pub credentials: Vec<PrincipalCredentialConfig>,
    #[serde(default)]
    pub workspaces: Vec<String>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub agent_ids: Vec<String>,
    /// Permit delegated agents acting for this principal to access records
    /// without an owner_agent_id. False keeps ownerless/shared memory behind
    /// an explicit administrator-authored grant.
    #[serde(default)]
    pub allow_shared_memory: bool,
    /// Entity keys or administrator-authored `**` wildcard available to this
    /// principal. Request selectors may only narrow this ceiling.
    #[serde(default)]
    pub entity_keys: Vec<String>,
    /// Privacy/data-subject IDs or `**`. Unscoped records remain separately
    /// representable and are not treated as matching a requested subject.
    #[serde(default)]
    pub data_subject_ids: Vec<String>,
    /// Session IDs or `**` available to this principal.
    #[serde(default)]
    pub session_ids: Vec<String>,
    /// Task IDs or `**` available to this principal.
    #[serde(default)]
    pub task_ids: Vec<String>,
    /// Allowed record classifications: public, internal, confidential, and
    /// restricted. Unknown values fail server startup.
    #[serde(default)]
    pub sensitivities: Vec<String>,
    #[serde(default)]
    pub purposes: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

fn default_grant_version() -> u64 {
    1
}

/// Memory authorization is stricter than the legacy vector data-plane ACL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryAuthorizationConfig {
    /// The only authoritative Memory workspace served by this process during
    /// preview. Empty inherits `auth.acl.default_workspace`.
    #[serde(default)]
    pub workspace_id: String,
    /// Explicit migration escape hatch. When false (default), the legacy
    /// global token has no Memory capabilities.
    #[serde(default)]
    pub allow_legacy_principal: bool,
    /// Tests only. An unauthenticated loopback caller otherwise has no Memory
    /// capabilities even when the legacy data plane is loopback-optional.
    #[serde(default)]
    pub allow_unauthenticated_loopback: bool,
}

/// Authoritative Memory service runtime. Disabled by default while the product
/// remains experimental and independently gated from the legacy vector data
/// plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryServiceConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_memory_rocksdb_path")]
    pub rocksdb_path: String,
    #[serde(default = "default_memory_max_recall_items")]
    pub max_recall_items: usize,
    #[serde(default = "default_memory_max_candidates")]
    pub max_candidates: usize,
    #[serde(default = "default_memory_context_token_budget")]
    pub default_context_token_budget: usize,
    #[serde(default = "default_memory_snapshot_max_bytes")]
    pub snapshot_max_bytes: usize,
    /// Declared retention windows. Zero means indefinite retention. Finite
    /// windows fail startup until the corresponding verified GC executor is
    /// available, preventing configuration from promising unenforced erasure.
    #[serde(default)]
    pub retention: MemoryRetentionConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryRetentionConfig {
    #[serde(default)]
    pub raw_event_seconds: u64,
    #[serde(default)]
    pub memory_version_seconds: u64,
    #[serde(default)]
    pub compiler_artifact_seconds: u64,
    #[serde(default)]
    pub index_artifact_seconds: u64,
    #[serde(default)]
    pub audit_seconds: u64,
    #[serde(default)]
    pub snapshot_seconds: u64,
}

impl MemoryRetentionConfig {
    pub fn is_indefinite(&self) -> bool {
        self.raw_event_seconds == 0
            && self.memory_version_seconds == 0
            && self.compiler_artifact_seconds == 0
            && self.index_artifact_seconds == 0
            && self.audit_seconds == 0
            && self.snapshot_seconds == 0
    }
}

fn default_memory_rocksdb_path() -> String {
    "./data/memory-rocksdb".to_string()
}

fn default_memory_max_recall_items() -> usize {
    100
}

fn default_memory_max_candidates() -> usize {
    5_000
}

fn default_memory_context_token_budget() -> usize {
    1_024
}

fn default_memory_snapshot_max_bytes() -> usize {
    4 * 1024 * 1024
}

impl Default for MemoryServiceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rocksdb_path: default_memory_rocksdb_path(),
            max_recall_items: default_memory_max_recall_items(),
            max_candidates: default_memory_max_candidates(),
            default_context_token_budget: default_memory_context_token_budget(),
            snapshot_max_bytes: default_memory_snapshot_max_bytes(),
            retention: MemoryRetentionConfig::default(),
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
    /// Empty means only `storage.seaweedfs.bucket`.
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
///
/// Only `post` and `adaptive` are implemented, and `adaptive` currently behaves
/// exactly like `post` (post-filter over-fetch; true prefilter bitmap
/// integration is tracked as GAP-003). `pre` sizes the candidate window at bare
/// `top_k`, so with a selective predicate it returns fewer results than `top_k`
/// -- it is not a recall-preserving pre-filter today.
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
    ///
    /// RESERVED, not read anywhere in the runtime: adaptive filtering currently
    /// behaves exactly like post-filtering (see [`FilterMode`]). Keeping the
    /// field so shipped configs keep parsing; changing it has no effect.
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
    /// Vector storage precision: `f32` (default), `f16`, or `i8`.
    /// `i8` stores routing codes in the graph and rescores candidates with the original f32 vectors.
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
    /// Tombstone ratio at or above which the shard compacts (physically removes
    /// tombstoned vectors) from its maintenance task.
    pub tombstone_ratio_trigger: f32,
    /// RESERVED, not read anywhere in the runtime: rebuilds are not cancellable
    /// yet, so a duration bound cannot be honored.
    pub max_duration_seconds: u64,
    /// Hours (0-23) during which the shard may compact. Empty means any hour.
    pub preferred_hours: Vec<u8>,
    /// At startup, compare durable vector storage against the derived
    /// projections (HNSW, lexical, graph) and repair any drift. RocksDB is the
    /// source of truth; projections are rebuildable derived data.
    #[serde(default = "default_reconcile_at_startup")]
    pub reconcile_at_startup: bool,
}

fn default_reconcile_at_startup() -> bool {
    true
}

impl Default for RebuildSettings {
    fn default() -> Self {
        Self {
            tombstone_ratio_trigger: 0.10,
            max_duration_seconds: 300,
            preferred_hours: vec![2, 3, 4], // 2-5 AM
            reconcile_at_startup: default_reconcile_at_startup(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneSettings {
    /// Tombstone count at which the shard compacts regardless of the ratio.
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
    pub seaweedfs: SeaweedFsConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            rocksdb_path: "./data/rocksdb".to_string(),
            // Application-level WriteAheadLog is implemented in akidb-storage but
            // not yet wired into the shard mutate path. Default false so config
            // matches runtime behavior; set true only when WAL is integrated.
            wal_enabled: false,
            wal_path: "./data/wal".to_string(),
            seaweedfs: SeaweedFsConfig::default(),
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

#[derive(Clone, Serialize, Deserialize)]
pub struct SeaweedFsConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub use_ssl: bool,
}

impl std::fmt::Debug for SeaweedFsConfig {
    /// Redacts both credentials. A `?config` log field, a panic message, or a
    /// config dump must not print the gateway secret.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SeaweedFsConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &describe_credential(&self.access_key))
            .field("secret_key", &describe_credential(&self.secret_key))
            .field("use_ssl", &self.use_ssl)
            .finish()
    }
}

/// Whether a credential is present, without ever exposing its value.
fn describe_credential(value: &str) -> &'static str {
    if value.trim().is_empty() {
        "<unset>"
    } else {
        "<redacted>"
    }
}

impl Default for SeaweedFsConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8333".to_string(),
            bucket: "akidb-snapshots".to_string(),
            // Credentials come from the deployment (environment variable or
            // secret store). SeaweedFS serves every operation anonymously when
            // it has no identity configuration, so a baked-in default here would
            // be either a published secret or a silent anonymous path.
            access_key: String::new(),
            secret_key: String::new(),
            use_ssl: false,
        }
    }
}

impl SeaweedFsConfig {
    /// S3 credentials for the gateway, which are mandatory.
    ///
    /// SeaweedFS grants anonymous access to every operation when it is started
    /// without an identity configuration, so an empty credential has to fail
    /// here rather than let a request reach an open gateway.
    pub fn credentials(&self) -> Result<(&str, &str), String> {
        if self.access_key.trim().is_empty() || self.secret_key.trim().is_empty() {
            return Err(
                "storage.seaweedfs.access_key and storage.seaweedfs.secret_key are required; supply them from an environment variable or secret store"
                    .to_string(),
            );
        }
        Ok((self.access_key.as_str(), self.secret_key.as_str()))
    }

    /// Gateway origin, with the scheme implied by `use_ssl` when the operator
    /// omitted it.
    ///
    /// Only a credential-free `http`/`https` origin is accepted: userinfo, a
    /// path, a query, or a fragment would either leak a credential or change
    /// what the signed request addresses.
    pub fn normalized_endpoint(&self) -> Result<String, String> {
        let endpoint = self.endpoint.trim();
        if endpoint.is_empty() {
            return Err("storage.seaweedfs.endpoint must not be empty".to_string());
        }
        let required_scheme = if self.use_ssl { "https" } else { "http" };
        let candidate = if endpoint.contains("://") {
            endpoint.to_string()
        } else {
            format!("{required_scheme}://{endpoint}")
        };
        let parsed = Url::parse(&candidate)
            .map_err(|_| "storage.seaweedfs.endpoint is not a valid URL".to_string())?;
        if parsed.scheme() != required_scheme
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || !matches!(parsed.path(), "" | "/")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(format!(
                "storage.seaweedfs.endpoint must be a credential-free {required_scheme} origin"
            ));
        }

        // Hand back the canonical origin rather than the operator's spelling:
        // every request URL is built by concatenating onto this value, so it
        // must already be a usable origin.
        Ok(parsed.origin().ascii_serialization())
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
    /// Emit-only data-plane access records (ADR-0009). Disabled by default.
    #[serde(default)]
    pub access_log: AccessLogConfig,
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
            access_log: AccessLogConfig::default(),
        }
    }
}

/// Emit-only access-record spool (ADR-0009). The spool is a bounded delivery
/// buffer, not an audit store: records are candidate events for a governance
/// platform, never evidence in themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessLogConfig {
    /// Master switch. When false the emitter is a no-op branch.
    pub enabled: bool,
    /// Directory for the JSONL spool files.
    pub directory: String,
    /// Rotate the active spool file once it exceeds this many bytes.
    pub max_file_bytes: u64,
    /// Retain at most this many rotated files; older ones are deleted.
    pub max_files: u32,
    /// Bound on buffered records waiting for the writer; excess is dropped
    /// and counted, never allowed to slow the data plane.
    pub queue_capacity: usize,
}

impl Default for AccessLogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: "./data/access-log".to_string(),
            max_file_bytes: 64 * 1024 * 1024,
            max_files: 8,
            queue_capacity: 4096,
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
    /// Fail closed unless the embedding endpoint is loopback-only. Operators
    /// handling governed data set this so a config mistake can never ship text
    /// to a third-party embedding API.
    #[serde(default)]
    pub require_local_embeddings: bool,
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
            require_local_embeddings: false,
        }
    }
}

impl EmbeddingClientConfig {
    /// Enforce the zero-external-egress policy: when
    /// `require_local_embeddings` is set and the client is enabled, the
    /// endpoint URL must be an `http`/`https` URL whose host is `localhost`
    /// or a loopback IP address. Anything else is a startup error.
    pub fn validate_egress_policy(&self) -> Result<(), String> {
        if !self.require_local_embeddings || !self.enabled {
            return Ok(());
        }
        let parsed = Url::parse(self.url.trim())
            .map_err(|_| format!("embedding.url {:?} is not a valid URL", self.url))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return Err(format!(
                "embedding.url {:?} must use http or https under require_local_embeddings",
                self.url
            ));
        }
        let loopback = match parsed.host() {
            Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            None => {
                return Err(format!("embedding.url {:?} has no host", self.url));
            }
        };
        if !loopback {
            return Err(format!(
                "embedding.url {:?} is not a loopback endpoint; require_local_embeddings forbids external egress",
                self.url
            ));
        }
        Ok(())
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
            wal_enabled = false
            wal_path = "./data/wal"

            [storage.seaweedfs]
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

    #[test]
    fn test_seaweedfs_defaults_carry_no_credentials() {
        let seaweedfs = SeaweedFsConfig::default();

        assert_eq!(seaweedfs.endpoint, "http://localhost:8333");
        assert!(seaweedfs.access_key.is_empty());
        assert!(seaweedfs.secret_key.is_empty());
        // SeaweedFS serves every operation anonymously without an identity
        // configuration, so the shipped default must not resolve to a usable
        // credential.
        assert!(seaweedfs.credentials().is_err());
    }

    #[test]
    fn test_seaweedfs_endpoint_gains_the_scheme_implied_by_tls() {
        let plain = SeaweedFsConfig {
            endpoint: "seaweedfs.internal:8333".to_string(),
            ..Default::default()
        };
        assert_eq!(
            plain.normalized_endpoint().unwrap(),
            "http://seaweedfs.internal:8333"
        );

        let tls = SeaweedFsConfig {
            endpoint: "seaweedfs.internal:8333/".to_string(),
            use_ssl: true,
            ..Default::default()
        };
        assert_eq!(
            tls.normalized_endpoint().unwrap(),
            "https://seaweedfs.internal:8333"
        );
    }

    #[test]
    fn test_seaweedfs_endpoint_rejects_non_origin_values() {
        for endpoint in [
            "",
            "   ",
            "http://",
            "ftp://seaweedfs.internal:8333",
            "http://seaweedfs.internal:8333/prefix",
            "http://seaweedfs.internal:8333?token=secret",
            "http://user:secret@seaweedfs.internal:8333",
            "not a url",
        ] {
            let seaweedfs = SeaweedFsConfig {
                endpoint: endpoint.to_string(),
                ..Default::default()
            };
            assert!(
                seaweedfs.normalized_endpoint().is_err(),
                "{endpoint} should be rejected"
            );
        }
    }

    #[test]
    fn test_seaweedfs_endpoint_is_returned_as_a_canonical_origin() {
        let seaweedfs = SeaweedFsConfig {
            endpoint: "  HTTP://SeaweedFS.Internal:8333/  ".to_string(),
            ..Default::default()
        };

        assert_eq!(
            seaweedfs.normalized_endpoint().unwrap(),
            "http://seaweedfs.internal:8333"
        );
    }

    #[test]
    fn test_seaweedfs_endpoint_scheme_must_match_tls_configuration() {
        let seaweedfs = SeaweedFsConfig {
            endpoint: "http://seaweedfs.internal:8333".to_string(),
            use_ssl: true,
            ..Default::default()
        };

        assert!(seaweedfs.normalized_endpoint().is_err());
    }

    #[test]
    fn test_seaweedfs_credentials_must_be_present() {
        let seaweedfs = SeaweedFsConfig {
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            ..Default::default()
        };
        assert_eq!(seaweedfs.credentials().unwrap(), ("access", "secret"));

        let blank_secret = SeaweedFsConfig {
            access_key: "access".to_string(),
            secret_key: "   ".to_string(),
            ..Default::default()
        };
        assert!(blank_secret.credentials().is_err());
    }

    #[test]
    fn test_seaweedfs_debug_redacts_credentials() {
        let seaweedfs = SeaweedFsConfig {
            access_key: "real-access".to_string(),
            secret_key: "real-secret".to_string(),
            ..Default::default()
        };

        let rendered = format!("{seaweedfs:?}");
        assert!(!rendered.contains("real-access"), "{rendered}");
        assert!(!rendered.contains("real-secret"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    #[test]
    fn test_embedding_egress_policy_rejects_malformed_and_non_http_urls() {
        for url in [
            "not a url",
            "ftp://127.0.0.1:8081/v1/embeddings",
            "unix:///var/run/embedding.sock",
            "https://",
        ] {
            let config = EmbeddingClientConfig {
                enabled: true,
                url: url.to_string(),
                require_local_embeddings: true,
                ..Default::default()
            };
            assert!(
                config.validate_egress_policy().is_err(),
                "{url} should be rejected"
            );
        }
    }

    #[test]
    fn test_embedding_egress_policy_is_inert_until_requested() {
        let external = EmbeddingClientConfig {
            enabled: true,
            url: "https://api.openai.com/v1/embeddings".to_string(),
            ..Default::default()
        };
        assert!(external.validate_egress_policy().is_ok());
    }

    #[test]
    fn test_embedding_egress_policy_rejects_external_endpoints() {
        for url in [
            "https://api.openai.com/v1/embeddings",
            "http://embedding.internal.example:8081/v1/embeddings",
            "http://10.0.0.5:8081/v1/embeddings",
            "http://[fd00::1]:8081/v1/embeddings",
        ] {
            let config = EmbeddingClientConfig {
                enabled: true,
                url: url.to_string(),
                require_local_embeddings: true,
                ..Default::default()
            };
            assert!(
                config.validate_egress_policy().is_err(),
                "{url} should be rejected"
            );
        }
    }

    #[test]
    fn test_embedding_egress_policy_accepts_loopback_endpoints() {
        for url in [
            "http://127.0.0.1:8081/v1/embeddings",
            "http://localhost:8081/v1/embeddings",
            "http://127.0.0.10:8081/v1/embeddings",
            "http://[::1]:8081/v1/embeddings",
        ] {
            let config = EmbeddingClientConfig {
                enabled: true,
                url: url.to_string(),
                require_local_embeddings: true,
                ..Default::default()
            };
            assert!(
                config.validate_egress_policy().is_ok(),
                "{url} should be accepted"
            );
        }
    }

    #[test]
    fn test_embedding_egress_policy_ignores_disabled_client() {
        let config = EmbeddingClientConfig {
            enabled: false,
            url: "https://api.openai.com/v1/embeddings".to_string(),
            require_local_embeddings: true,
            ..Default::default()
        };
        assert!(config.validate_egress_policy().is_ok());
    }
}
