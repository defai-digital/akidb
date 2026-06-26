//! AkiDB Server - Main entry point
//!
//! This binary starts the AkiDB gRPC server with vector indexing capabilities.

use akidb_common::config::AkiDbConfig;
#[cfg(feature = "gpu")]
use akidb_faiss::{GpuIndex, GpuIndexConfig};
#[cfg(not(feature = "gpu"))]
use akidb_faiss::{MockIndex, MockIndexConfig};
use akidb_grpc::proto::akidb_server::AkidbServer;
use akidb_grpc::AkiDbService;
use akidb_storage::{IdMapping, RocksDbBackend};
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// AkiDB Server - High-performance vector database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/opt/akidb/config/akidb.toml")]
    config: PathBuf,

    /// gRPC listen address
    #[arg(short, long, default_value = "0.0.0.0:50051")]
    listen: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'l', long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting AkiDB Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Config file: {:?}", args.config);

    // Load configuration if exists
    let config = if args.config.exists() {
        let config_str = std::fs::read_to_string(&args.config)?;
        toml::from_str::<AkiDbConfig>(&config_str).unwrap_or_default()
    } else {
        info!("Config file not found, using defaults");
        AkiDbConfig::default()
    };

    // Initialize storage
    let rocksdb_path = config.storage.rocksdb_path.clone();

    info!("Initializing RocksDB at {}", rocksdb_path);

    // Create RocksDB directory if it doesn't exist
    std::fs::create_dir_all(&rocksdb_path)?;

    let storage = Arc::new(RocksDbBackend::open(&rocksdb_path)?);

    // Initialize ID mapping
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "default"));

    // Initialize vector index
    #[cfg(feature = "gpu")]
    let index = {
        let index_config = GpuIndexConfig {
            dimension: 768,
            nlist: 4096,
            nprobe: 32,
            device_id: 0,
            memory_fraction: 0.6,
            use_float16: false,
            training_threshold: 100_000,
            rebuild_threshold: 0.10,
            fallback_to_cpu: true,
        };
        info!("Initializing GPU index with FAISS...");
        Arc::new(GpuIndex::new(index_config)?)
    };

    #[cfg(not(feature = "gpu"))]
    let index = {
        let index_config = MockIndexConfig::new(768).with_capacity(1_000_000);
        Arc::new(MockIndex::from_config(index_config))
    };

    #[cfg(feature = "gpu")]
    info!("Vector index initialized (GPU mode with FAISS)");
    #[cfg(not(feature = "gpu"))]
    info!("Vector index initialized (mock mode)");

    // Create gRPC service
    let service = AkiDbService::new(index, id_mapping, "default");

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
