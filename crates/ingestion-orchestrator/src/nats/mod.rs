//! NATS JetStream integration
//!
//! Provides consumer and publisher for event-driven document processing.

pub mod consumer;
pub mod publisher;

pub use consumer::{NatsConsumer, UploadEvent};
pub use publisher::DlqPublisher;
