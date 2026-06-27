//! AkiDB gRPC Server
//!
//! Provides the gRPC API for vector operations and ingestion management.

pub mod admin;
pub mod filter;
pub mod ingestion;
pub mod mcp;
pub mod metrics;
pub mod service;
pub mod tags;
pub mod webhook;

pub use admin::{AdminServiceImpl, AdminState, RegisteredTask};
pub use ingestion::IngestionServiceImpl;
pub use service::{AkiDbService, EmbeddingProvider};
pub use webhook::{WebhookConfig, WebhookEventType, WebhookPayload, WebhookSender, WebhookStats};

/// Generated protobuf types
pub mod proto {
    tonic::include_proto!("akidb.v1");
}
