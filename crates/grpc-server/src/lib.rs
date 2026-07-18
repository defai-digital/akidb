//! AkiDB gRPC Server
//!
//! Provides the gRPC API for vector operations and ingestion management.

#![allow(clippy::result_large_err)]

mod acl;
mod admin;
pub mod auth;
mod collections;
mod filter;
mod ingestion;
mod management;
pub mod mcp;
mod metrics;
mod service;
mod tags;
mod webhook;

pub use admin::{AdminServiceImpl, AdminState, RegisteredTask};
pub use auth::{AuthContext, AuthInterceptor, AuthRuntime};
pub use collections::{CollectionMeta, CollectionRegistry, SharedCollectionRegistry};
pub use ingestion::IngestionServiceImpl;
pub use management::{ManagementServiceImpl, ManagementState, StagedObject, StagingRegistry};
pub use metrics::{metrics, AkiDbMetrics};
pub use service::{AkiDbService, EmbeddingProvider};
pub use tags::{
    proto_to_rust_tag_value, proto_to_rust_tags, rust_to_proto_tag_value, rust_to_proto_tags,
};
pub use webhook::{WebhookConfig, WebhookEventType, WebhookPayload, WebhookSender, WebhookStats};

/// Generated protobuf types
pub use akidb_proto as proto;
