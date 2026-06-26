//! AkiDB Ingestion Orchestrator Binary
//!
//! Runs the hybrid document processing pipeline.

use akidb_ingestion::{IngestionConfig, IngestionPipeline, Result};
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "akidb_ingestion=info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting AkiDB Ingestion Orchestrator");

    // Load configuration
    let config = IngestionConfig::from_env()?;
    info!(?config, "Configuration loaded");

    // Create and run pipeline
    let pipeline = IngestionPipeline::new(config).await?;

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
