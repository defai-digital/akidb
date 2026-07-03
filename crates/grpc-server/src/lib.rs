//! AkiDB gRPC Server
//!
//! Provides the gRPC API for vector operations and ingestion management.

#![allow(clippy::result_large_err)]

mod admin;
mod filter;
mod ingestion;
pub mod mcp;
mod metrics;
mod service;
mod tags;
mod webhook;

pub use admin::{AdminServiceImpl, AdminState, RegisteredTask};
pub use ingestion::IngestionServiceImpl;
pub use metrics::{metrics, AkiDbMetrics};
pub use service::{AkiDbService, EmbeddingProvider};
pub use tags::{
    proto_to_rust_tag_value, proto_to_rust_tags, rust_to_proto_tag_value, rust_to_proto_tags,
};
pub use webhook::{WebhookConfig, WebhookEventType, WebhookPayload, WebhookSender, WebhookStats};

/// Generated protobuf types
pub mod proto {
    tonic::include_proto!("akidb.v1");
}
