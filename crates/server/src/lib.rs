//! AkiDB Server - Main entry point
//!
//! This binary starts the AkiDB gRPC server with vector indexing capabilities.
//! In standalone mode (`--standalone`), it runs with no external dependencies
//! (no MinIO, no NATS) and optionally uses ax-engine for text embeddings.

use akidb_common::config::AkiDbConfig;
use akidb_common::VectorId;
use akidb_coordinator::AxEngineEmbedding;
use akidb_faiss::{HnswConfig, HnswIndex, VectorIndex};
use akidb_graph::NativeGraphIndex;
use akidb_grpc::proto::akidb_server::AkidbServer;
use akidb_grpc::{AkiDbService, EmbeddingProvider};
use akidb_sql::SqliteMetadataIndex;
use akidb_storage::{IdMapping, RocksDbBackend};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Adapter from coordinator's AxEngineEmbedding to the gRPC EmbeddingProvider trait
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
        use akidb_coordinator::EmbeddingService;
        tokio::task::block_in_place(|| self.inner.embed(text)).map_err(|e| e.to_string())
    }

    fn embedding_dimensions(&self) -> usize {
        use akidb_coordinator::EmbeddingService;
        self.inner.dimensions()
    }
}

/// AkiDB Server - High-performance vector database
#[derive(clap::Args, Debug)]
pub struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/opt/akidb/config/akidb.toml")]
    pub config: PathBuf,

    /// gRPC listen address
    #[arg(short, long, default_value = "0.0.0.0:50051")]
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

    let service = build_service(&config)?;

    // Parse listen address
    let addr: SocketAddr = args.listen.parse()?;
    info!("Starting gRPC server on {}", addr);

    // Start server
    Server::builder()
        .add_service(AkidbServer::new(service))
        .serve(addr)
        .await?;

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
    let index = {
        let hnsw_config = HnswConfig {
            dimensions: config.slo.reference.dimensions,
            capacity: config.slo.reference.vectors_per_shard,
            m: config.index.hnsw_m as usize,
            ef_construction: config.index.hnsw_ef_construction as usize,
            ef_search: config.index.hnsw_ef_search as usize,
        };
        Arc::new(HnswIndex::new(hnsw_config).expect("Failed to create HNSW index"))
    };
    info!("Vector index initialized (HNSW mode)");

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
    let mut service = AkiDbService::new(index, id_mapping, "default").with_graph_index(graph_index);
    info!("Native graph index enabled");

    if config.sql.enabled {
        if !config.sql.backend.eq_ignore_ascii_case("sqlite") {
            return Err(format!(
                "unsupported SQL metadata backend '{}'; expected 'sqlite'",
                config.sql.backend
            )
            .into());
        }
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
    let loaded = service.rebuild_lexical_index();
    if loaded > 0 {
        info!("Rebuilt lexical index from {} persisted documents", loaded);
    }

    Ok(service)
}
