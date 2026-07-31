//! AkiDB Ingestion Orchestrator Binary
//!
//! Runs the hybrid document processing pipeline.

use std::net::SocketAddr;

use akidb_ingestion::metrics::start_metrics_server;
use akidb_ingestion::{IngestionConfig, IngestionError, IngestionPipeline, Result};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "akidb_ingestion=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting AkiDB Ingestion Orchestrator");

    // Load configuration
    let config = IngestionConfig::from_env()?;
    let metrics_addr: SocketAddr = config.metrics_addr.parse().map_err(|error| {
        IngestionError::Config(format!(
            "invalid INGESTION_METRICS_ADDR '{}': {error}",
            config.metrics_addr
        ))
    })?;
    info!(
        nats_url = %config.nats.url,
        nats_stream = %config.nats.stream,
        storage_endpoint = %config.storage.endpoint,
        storage_bucket = %config.storage.bucket,
        akidb_endpoint = %config.akidb.endpoint,
        embedding_url = %config.embedding_url,
        embedding_model = %config.embedding_model,
        doc_parser_url = %config.doc_parser_url,
        %metrics_addr,
        "Configuration loaded"
    );

    // Create and run pipeline
    let pipeline = IngestionPipeline::new(config).await?;
    let _metrics_server = start_metrics_server(pipeline.metrics_registry(), metrics_addr).await?;
    info!(%metrics_addr, "Ingestion metrics endpoint listening");

    // Handle shutdown gracefully
    let shutdown = tokio::signal::ctrl_c();

    tokio::select! {
        result = pipeline.run() => {
            if let Err(e) = result {
                error!(?e, "Pipeline error");
                return Err(e);
            }
        }
        _ = shutdown => {
            info!("Shutdown signal received");
        }
    }

    info!("Ingestion orchestrator stopped");
    Ok(())
}
