//! AkiDB Server - Main entry point
//!
//! This binary starts the AkiDB gRPC server with vector indexing capabilities.
//! In standalone mode (`--standalone`), it runs with no external dependencies
//! (no MinIO, no NATS) and optionally uses ax-engine for text embeddings.

use akidb_common::config::{AkiDbConfig, AuthMode};
use akidb_common::scheduler::{ResourceGovernor, ResourceGovernorConfig, SimpleMetricsSource};
use akidb_common::VectorId;
#[cfg(feature = "generation-s3")]
use akidb_contracts::KnowledgeScope;
use akidb_embedding::ax_engine::AxEngineEmbedding;
use akidb_faiss::{DistanceMetric, HnswConfig, HnswIndex, VectorIndex, VectorPrecision};
use akidb_graph::NativeGraphIndex;
use akidb_grpc::{
    AdminState, AkiDbService, AuthInterceptor, AuthRuntime, EmbeddingProvider,
    ManagementServiceImpl, ManagementState, StagingRegistry,
};
#[cfg(feature = "generation-s3")]
use akidb_grpc::{
    CollectionRegistry, GenerationController, GenerationDataPlane, GenerationDataPlaneConfig,
    GenerationManagementServiceImpl, GenerationMaterializer, GenerationMaterializerConfig,
    S3GenerationBundleFetcher, S3GenerationBundleFetcherConfig,
};
use akidb_proto::akidb_server::AkidbServer;
#[cfg(feature = "generation-s3")]
use akidb_proto::generation_management_server::GenerationManagementServer;
use akidb_proto::management_service_server::ManagementServiceServer;
#[cfg(feature = "postgres")]
use akidb_sql::PostgresMetadataIndex;
use akidb_sql::{SqliteMetadataIndex, POSTGRES_BACKEND, SQLITE_BACKEND};
#[cfg(feature = "generation-s3")]
use akidb_storage::{GenerationStore, ServingStateStore};
use akidb_storage::{IdMapping, LocalSnapshotBackend, RocksDbBackend, SnapshotManager};
#[cfg(feature = "generation-s3")]
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Adapter from the shared AxEngineEmbedding client to the gRPC EmbeddingProvider trait.
struct AxEngineProvider {
    inner: AxEngineEmbedding,
}

impl AxEngineProvider {
    fn new(inner: AxEngineEmbedding) -> Self {
        Self { inner }
    }
}

impl EmbeddingProvider for AxEngineProvider {
    fn embed_text(&self, text: &str) -> std::result::Result<Vec<f32>, String> {
        use akidb_embedding::EmbeddingService;
        tokio::task::block_in_place(|| self.inner.embed(text)).map_err(|e| e.to_string())
    }

    fn embedding_dimensions(&self) -> usize {
        use akidb_embedding::EmbeddingService;
        self.inner.dimensions()
    }
}

/// AkiDB Server - High-performance vector database
#[derive(clap::Args, Debug)]
pub struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/opt/akidb/config/akidb.toml")]
    pub config: PathBuf,

    /// gRPC listen address (default: config server.host:grpc_port, loopback-first)
    #[arg(short, long, default_value = "")]
    pub listen: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Run in standalone mode (skip MinIO, no external deps)
    #[arg(long, default_value_t = false)]
    pub standalone: bool,
}

pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let log_level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting AkiDB Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Config file: {:?}", args.config);
    if args.standalone {
        info!("Running in standalone mode (no external dependencies)");
    }

    // Load configuration if exists
    let config = if args.config.exists() {
        let config_str = std::fs::read_to_string(&args.config)?;
        toml::from_str::<AkiDbConfig>(&config_str)?
    } else {
        info!("Config file not found, using defaults");
        AkiDbConfig::default()
    };

    // Resolve listen address: CLI override, else config (secure loopback default).
    let listen = if args.listen.trim().is_empty() {
        format!("{}:{}", config.server.host, config.server.grpc_port)
    } else {
        args.listen.clone()
    };
    let addr: SocketAddr = listen.parse()?;
    let bind_host = addr.ip().to_string();
    if !addr.ip().is_loopback() {
        warn!(
            %addr,
            "binding non-loopback address; bearer auth is required unless auth.mode=disabled"
        );
    }

    let auth_runtime = AuthRuntime::bootstrap(config.auth.clone(), &bind_host)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    if config.generation_serving.enabled {
        if args.standalone {
            return Err(
                "generation serving requires configured immutable S3/MinIO publication; it cannot run with --standalone"
                    .into(),
            );
        }
        #[cfg(feature = "generation-s3")]
        {
            return run_generation_server(&config, addr, auth_runtime).await;
        }
        #[cfg(not(feature = "generation-s3"))]
        {
            return Err(
                "generation serving requires an akidb-server build with --features generation-s3"
                    .into(),
            );
        }
    }

    let service = build_service(&config)?;
    let data_interceptor = AuthInterceptor::new(auth_runtime.clone());

    let collections = service.collection_registry();
    let metrics = Arc::new(SimpleMetricsSource::new());
    let governor = Arc::new(ResourceGovernor::new(
        ResourceGovernorConfig::default(),
        metrics,
    ));
    let admin_state = Arc::new(AdminState::new(governor));
    let rocksdb_path = PathBuf::from(&config.storage.rocksdb_path);
    let snapshot_path = rocksdb_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("snapshots");
    let snapshot_manager = Arc::new(SnapshotManager::new(LocalSnapshotBackend::new(
        snapshot_path,
    )));
    let auth_mode = match config.auth.mode {
        AuthMode::LoopbackOptional => "loopback_optional",
        AuthMode::Required => "required",
        AuthMode::Disabled => "disabled",
    };
    let management_state = Arc::new(ManagementState::new(
        admin_state,
        collections,
        Some(snapshot_manager),
        Arc::new(StagingRegistry::default()),
        config.management.import_plan.clone(),
        config.management.audit_max_entries,
        env!("CARGO_PKG_VERSION"),
        auth_mode,
        // Tonic TLS is not wired in this binary yet; report transport truth,
        // not merely the configured intent.
        false,
    ));
    let management = ManagementServiceImpl::new(management_state);
    let management_interceptor = AuthInterceptor::new(auth_runtime);

    info!("Starting gRPC server on {}", addr);

    // Start server with auth interceptor (GAP-001)
    Server::builder()
        .add_service(AkidbServer::with_interceptor(service, data_interceptor))
        .add_service(ManagementServiceServer::with_interceptor(
            management,
            management_interceptor,
        ))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(feature = "generation-s3")]
async fn run_generation_server(
    config: &AkiDbConfig,
    addr: SocketAddr,
    auth_runtime: AuthRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_generation_paths(config)?;
    let generation = &config.generation_serving;
    let generation_store = Arc::new(GenerationStore::open(&generation.generation_root)?);
    let materializer = Arc::new(GenerationMaterializer::new(
        generation_store,
        GenerationMaterializerConfig {
            max_vectors: generation.max_vectors,
            max_graph_nodes: generation.max_graph_nodes,
            max_graph_edges: generation.max_graph_edges,
            hnsw_m: config.index.hnsw_m as usize,
            hnsw_ef_construction: config.index.hnsw_ef_construction as usize,
            hnsw_ef_search: config.index.hnsw_ef_search as usize,
            vector_precision: VectorPrecision::parse(&config.index.vector_precision)?,
            distance_metric: DistanceMetric::parse(&config.index.metric)?,
            ..Default::default()
        },
    ));
    let state_storage = Arc::new(RocksDbBackend::open(&generation.control_rocksdb_path)?);
    let state = Arc::new(ServingStateStore::new(
        state_storage,
        generation.replica_id.clone(),
    )?);
    let controller = Arc::new(GenerationController::new(materializer, state));
    let collections = Arc::new(CollectionRegistry::new());
    let embedding_provider = build_optional_embedding_provider(config);
    let data_plane = GenerationDataPlane::new(
        controller,
        GenerationDataPlaneConfig {
            default_collection: generation.default_collection.clone(),
            slo_threshold_us: config
                .slo
                .reference
                .target_p95_ms
                .saturating_mul(1_000)
                .max(1),
            acl: config.auth.acl.clone(),
            filter_settings: config.index.filter.clone(),
            collections: collections.clone(),
            embedding_provider,
        },
    )?;
    let default_scope = KnowledgeScope::new(
        config.auth.acl.default_workspace.clone(),
        generation.default_collection.clone(),
    );
    data_plane.restore_scope(&default_scope)?;

    let allowed_buckets: HashSet<String> = if generation.allowed_buckets.is_empty() {
        HashSet::from([config.storage.minio.bucket.clone()])
    } else {
        generation.allowed_buckets.iter().cloned().collect()
    };
    let fetcher = Arc::new(S3GenerationBundleFetcher::for_minio(
        &config.storage.minio,
        generation.s3_region.clone(),
        S3GenerationBundleFetcherConfig {
            allowed_buckets,
            download_directory: PathBuf::from(&generation.download_path),
            max_bundle_size_bytes: generation.max_bundle_size_bytes,
            require_version_or_digest_key: generation.require_version_or_digest_key,
        },
    )?);
    let generation_management =
        GenerationManagementServiceImpl::new(Arc::new(data_plane.clone()), fetcher);

    let metrics = Arc::new(SimpleMetricsSource::new());
    let governor = Arc::new(ResourceGovernor::new(
        ResourceGovernorConfig::default(),
        metrics,
    ));
    let admin_state = Arc::new(AdminState::new(governor));
    let auth_mode = auth_mode_name(config.auth.mode);
    let management_state = Arc::new(ManagementState::new(
        admin_state,
        collections,
        // Legacy mutable snapshots are not generation backups.
        None,
        Arc::new(StagingRegistry::default()),
        config.management.import_plan.clone(),
        config.management.audit_max_entries,
        env!("CARGO_PKG_VERSION"),
        auth_mode,
        false,
    ));
    let management = ManagementServiceImpl::new(management_state);

    info!(
        %addr,
        replica_id = %generation.replica_id,
        "Starting immutable single-node generation serving preview"
    );
    Server::builder()
        .add_service(AkidbServer::with_interceptor(
            data_plane,
            AuthInterceptor::new(auth_runtime.clone()),
        ))
        .add_service(GenerationManagementServer::with_interceptor(
            generation_management,
            AuthInterceptor::new(auth_runtime.clone()),
        ))
        .add_service(ManagementServiceServer::with_interceptor(
            management,
            AuthInterceptor::new(auth_runtime),
        ))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(feature = "generation-s3")]
fn auth_mode_name(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::LoopbackOptional => "loopback_optional",
        AuthMode::Required => "required",
        AuthMode::Disabled => "disabled",
    }
}

#[cfg(feature = "generation-s3")]
fn build_optional_embedding_provider(config: &AkiDbConfig) -> Option<Arc<dyn EmbeddingProvider>> {
    if !config.embedding.enabled {
        return None;
    }
    match tokio::task::block_in_place(|| AxEngineEmbedding::new(config.embedding.clone())) {
        Ok(embedding) => {
            info!(
                "Embedding provider enabled: model={}, url={}",
                config.embedding.model, config.embedding.url
            );
            Some(Arc::new(AxEngineProvider::new(embedding)))
        }
        Err(error) => {
            warn!(
                "Failed to create embedding provider: {}. TextSearch will be unavailable.",
                error
            );
            None
        }
    }
}

#[cfg(any(feature = "generation-s3", test))]
fn validate_generation_paths(config: &AkiDbConfig) -> Result<(), Box<dyn std::error::Error>> {
    let generation = &config.generation_serving;
    if generation.replica_id.trim().is_empty() {
        return Err("generation_serving.replica_id is required when enabled".into());
    }
    if generation.default_collection.trim().is_empty() {
        return Err("generation_serving.default_collection must not be empty".into());
    }
    if generation.max_vectors == 0
        || generation.max_graph_nodes == 0
        || generation.max_graph_edges == 0
    {
        return Err("generation serving index limits must be greater than zero".into());
    }

    let configured = [
        PathBuf::from(&generation.generation_root),
        PathBuf::from(&generation.control_rocksdb_path),
        PathBuf::from(&generation.download_path),
    ];
    for path in &configured {
        std::fs::create_dir_all(path)?;
    }
    let canonical: Vec<PathBuf> = configured
        .iter()
        .map(std::fs::canonicalize)
        .collect::<Result<_, _>>()?;
    for (index, path) in canonical.iter().enumerate() {
        for other in canonical.iter().skip(index + 1) {
            if path.starts_with(other) || other.starts_with(path) {
                return Err(format!(
                    "generation data paths must be distinct and non-overlapping: {} and {}",
                    path.display(),
                    other.display()
                )
                .into());
            }
        }
    }
    Ok(())
}

/// Run AkiDB as an MCP server over stdio (newline-delimited JSON-RPC), sharing
/// the same storage, index, and embedding setup as the gRPC server.
pub async fn run_mcp(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // MCP speaks JSON-RPC on stdout, so logs must go to stderr and never stdout.
    let log_level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_writer(std::io::stderr)
        .with_target(true)
        .init();

    let config = if args.config.exists() {
        toml::from_str::<AkiDbConfig>(&std::fs::read_to_string(&args.config)?)?
    } else {
        AkiDbConfig::default()
    };
    if config.generation_serving.enabled {
        return Err(
            "generation serving is currently available only on the authenticated gRPC data plane; MCP startup is refused to prevent a mutable-path bypass"
                .into(),
        );
    }

    let service = Arc::new(build_service(&config)?);
    info!("AkiDB MCP server ready on stdio");
    akidb_grpc::mcp::run_stdio(service).await?;
    Ok(())
}

/// Build a fully-wired AkiDbService (storage, index, vector reload, embedding
/// provider, lexical rebuild) from config. Shared by the gRPC and MCP entry
/// points.
fn build_service(
    config: &AkiDbConfig,
) -> Result<AkiDbService<HnswIndex, RocksDbBackend>, Box<dyn std::error::Error>> {
    // Initialize storage
    let rocksdb_path = config.storage.rocksdb_path.clone();
    info!("Initializing RocksDB at {}", rocksdb_path);
    std::fs::create_dir_all(&rocksdb_path)?;
    let storage = Arc::new(RocksDbBackend::open(&rocksdb_path)?);

    // Initialize ID mapping
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "default"));

    // Initialize vector index
    let precision = VectorPrecision::parse(&config.index.vector_precision)?;
    let metric = DistanceMetric::parse(&config.index.metric)?;
    let index = {
        let hnsw_config = HnswConfig {
            dimensions: config.slo.reference.dimensions,
            capacity: config.slo.reference.vectors_per_shard,
            m: config.index.hnsw_m as usize,
            ef_construction: config.index.hnsw_ef_construction as usize,
            ef_search: config.index.hnsw_ef_search as usize,
            precision,
            metric,
        };
        Arc::new(HnswIndex::new(hnsw_config)?)
    };
    info!(
        precision = ?precision,
        metric = ?metric,
        "Vector index initialized (HNSW mode)"
    );

    let stored_vectors = id_mapping.load_active_vectors()?;
    if !stored_vectors.is_empty() {
        info!(
            "Reloading {} persisted vectors into HNSW index",
            stored_vectors.len()
        );
    }
    let mut reloaded_count = 0usize;
    for stored in stored_vectors {
        let vector_id = VectorId::new(&stored.external_id);
        match index.insert(&vector_id, &stored.vector) {
            Ok(internal_id) => {
                if let Err(e) = id_mapping.upsert_with_vector(
                    &vector_id,
                    internal_id,
                    &stored.vector,
                    &stored.metadata,
                ) {
                    warn!(
                        vector_id = %stored.external_id,
                        error = %e,
                        "Failed to update mapping for reloaded vector"
                    );
                } else {
                    reloaded_count += 1;
                }
            }
            Err(e) => {
                warn!(
                    vector_id = %stored.external_id,
                    error = %e,
                    "Failed to reload persisted vector into index"
                );
            }
        }
    }
    if reloaded_count > 0 {
        info!(
            "Reloaded {} persisted vectors into HNSW index",
            reloaded_count
        );
    }

    // Create gRPC service
    let graph_index = Arc::new(NativeGraphIndex::new(storage));
    let mut service = AkiDbService::new(index, id_mapping, "default")
        .with_graph_index(graph_index)
        .with_acl(config.auth.acl.clone())
        .with_filter_settings(config.index.filter.clone())
        .with_embedding_model_id(config.embedding.model.clone());
    service.seed_collection_schema(
        config.slo.reference.dimensions as u32,
        &config.index.metric,
        &config.index.vector_precision,
        &config.embedding.model,
    );
    info!("Native graph index enabled");

    if config.sql.enabled {
        if config.sql.backend.eq_ignore_ascii_case(SQLITE_BACKEND) {
            let sqlite_path = PathBuf::from(&config.sql.sqlite_path);
            if let Some(parent) = sqlite_path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            let sql_index = Arc::new(SqliteMetadataIndex::open(&sqlite_path)?);
            service = service.with_metadata_sql_index(sql_index);
            let rebuilt = service.rebuild_sql_metadata_index();
            info!(
                path = %sqlite_path.display(),
                records = rebuilt,
                "SQLite metadata adapter enabled"
            );
        } else if config.sql.backend.eq_ignore_ascii_case(POSTGRES_BACKEND) {
            #[cfg(feature = "postgres")]
            {
                let postgres_url = config.sql.postgres_url.as_deref().ok_or_else(|| {
                    "sql.postgres_url is required when sql.backend = 'postgres'".to_string()
                })?;
                let sql_index = Arc::new(PostgresMetadataIndex::connect(postgres_url)?);
                service = service.with_metadata_sql_index(sql_index);
                let rebuilt = service.rebuild_sql_metadata_index();
                info!(records = rebuilt, "PostgreSQL metadata adapter enabled");
            }
            #[cfg(not(feature = "postgres"))]
            {
                return Err(
                    "sql.backend = 'postgres' requires building akidb-server with --features postgres"
                        .into(),
                );
            }
        } else {
            return Err(format!(
                "unsupported SQL metadata backend '{}'; expected '{}' or '{}'",
                config.sql.backend, SQLITE_BACKEND, POSTGRES_BACKEND
            )
            .into());
        }
    }

    // Wire embedding provider if enabled
    if config.embedding.enabled {
        match tokio::task::block_in_place(|| AxEngineEmbedding::new(config.embedding.clone())) {
            Ok(embedding) => {
                let provider = Arc::new(AxEngineProvider::new(embedding));
                service = service.with_embedding_provider(provider);
                info!(
                    "Embedding provider enabled: model={}, url={}",
                    config.embedding.model, config.embedding.url
                );
            }
            Err(e) => {
                warn!(
                    "Failed to create embedding provider: {}. TextSearch will be unavailable.",
                    e
                );
            }
        }
    }

    // Rebuild the lexical index / document store from persisted source text so
    // hybrid retrieval and context packing work after a restart.
    // Bootstrap a newly enabled/empty graph from durable vectors. A full
    // rebuild remains an explicit repair operation because replaying every
    // projection on each start would make large-index startup unbounded.
    let graph_chunks = if service.graph_stats().is_some_and(|stats| stats.nodes == 0) {
        service.rebuild_graph_index()?
    } else {
        0
    };
    if graph_chunks > 0 {
        info!(
            "Rebuilt native graph projections from {} persisted vectors",
            graph_chunks
        );
    }

    let loaded = service.rebuild_lexical_index();
    if loaded > 0 {
        info!("Rebuilt lexical index from {} persisted documents", loaded);
    }

    Ok(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "akidb-server-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn build_service_returns_error_for_invalid_hnsw_config() {
        let rocksdb_path = unique_temp_path("invalid-hnsw");
        let mut config = AkiDbConfig::default();
        config.storage.rocksdb_path = rocksdb_path.display().to_string();
        config.slo.reference.dimensions = 0;

        let result = build_service(&config);

        assert!(result.is_err());
        let message = result.err().unwrap().to_string();
        assert!(message.contains("HNSW dimensions must be > 0"));

        let _ = std::fs::remove_dir_all(rocksdb_path);
    }

    #[test]
    fn generation_mode_requires_stable_replica_identity() {
        let mut config = AkiDbConfig::default();
        config.generation_serving.enabled = true;
        config.generation_serving.replica_id.clear();

        let error = validate_generation_paths(&config).unwrap_err();
        assert!(error.to_string().contains("replica_id is required"));
    }

    #[test]
    fn generation_data_paths_must_not_overlap() {
        let base = unique_temp_path("generation-overlap");
        let mut config = AkiDbConfig::default();
        config.generation_serving.enabled = true;
        config.generation_serving.replica_id = "replica-test".to_string();
        config.generation_serving.generation_root = base.display().to_string();
        config.generation_serving.control_rocksdb_path = base.join("control").display().to_string();
        config.generation_serving.download_path = unique_temp_path("generation-download")
            .display()
            .to_string();

        let error = validate_generation_paths(&config).unwrap_err();
        assert!(error.to_string().contains("non-overlapping"));

        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&config.generation_serving.download_path);
    }
}
