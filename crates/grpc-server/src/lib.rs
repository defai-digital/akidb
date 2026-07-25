//! AkiDB gRPC Server
//!
//! Provides the gRPC API for vector operations and ingestion management.

#![allow(clippy::result_large_err)]

mod acl;
mod admin;
pub mod auth;
mod collections;
mod filter;
pub mod generation;
pub mod generation_control;
pub mod generation_data_plane;
pub mod generation_fetch;
pub mod generation_management;
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
pub use generation::{
    GenerationMaterializer, GenerationMaterializerConfig, GenerationMaterializerError,
    ReadyGenerationRuntime,
};
pub use generation_control::{
    ExpectedActiveGeneration, GenerationControlError, GenerationController, GenerationPublication,
};
pub use generation_data_plane::{GenerationDataPlane, GenerationDataPlaneConfig};
pub use generation_fetch::{
    FetchedGenerationBundle, GenerationBundleFetcher, GenerationFetchError,
};
#[cfg(feature = "generation-s3")]
pub use generation_fetch::{S3GenerationBundleFetcher, S3GenerationBundleFetcherConfig};
pub use generation_management::GenerationManagementServiceImpl;
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
