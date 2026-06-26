//! NATS JetStream Consumer
//!
//! Consumes document upload events from MinIO bucket notifications.

use async_nats::jetstream::{self, consumer::PullConsumer};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{info, error, debug};

use crate::config::NatsConfig;
use crate::Result;

/// Document upload event from MinIO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadEvent {
    /// Bucket name
    pub bucket: String,

    /// Object key (file path)
    pub key: String,

    /// Object size in bytes
    pub size: u64,

    /// Content type
    pub content_type: Option<String>,

    /// Event timestamp
    pub timestamp: String,
}

/// NATS JetStream consumer for upload events
pub struct NatsConsumer {
    consumer: PullConsumer,
    _stream_name: String,
}

impl NatsConsumer {
    /// Create a new NATS consumer
    pub async fn new(config: &NatsConfig) -> Result<Self> {
        info!(url = %config.url, stream = %config.stream, "Connecting to NATS");

        let client = async_nats::connect(&config.url).await?;
        let jetstream = jetstream::new(client);

        // Get or create the stream
        // Note: MinIO publishes to "minio.uploads", so we need both exact and wildcard subjects
        let stream = jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: config.stream.clone(),
                subjects: vec![
                    "minio.uploads".to_string(),    // Exact match for MinIO notifications
                    "minio.uploads.>".to_string(),  // Wildcard for hierarchical subjects
                ],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                max_messages: 1_000_000,
                max_bytes: 1024 * 1024 * 1024, // 1GB
                ..Default::default()
            })
            .await?;

        // Create durable consumer
        let consumer = stream
            .get_or_create_consumer(
                &config.consumer,
                jetstream::consumer::pull::Config {
                    durable_name: Some(config.consumer.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    max_deliver: 3,
                    ack_wait: std::time::Duration::from_secs(60),
                    ..Default::default()
                },
            )
            .await?;

        info!(
            stream = %config.stream,
            consumer = %config.consumer,
            "NATS consumer created"
        );

        Ok(Self {
            consumer,
            _stream_name: config.stream.clone(),
        })
    }

    /// Fetch a batch of messages
    pub async fn fetch(&self, batch_size: usize) -> Result<Vec<NatsMessage>> {
        let mut messages = self.consumer
            .fetch()
            .max_messages(batch_size)
            .expires(std::time::Duration::from_secs(5))
            .messages()
            .await?;

        let mut result = Vec::new();

        while let Some(msg) = messages.next().await {
            match msg {
                Ok(m) => {
                    result.push(NatsMessage { inner: m });
                }
                Err(e) => {
                    error!(?e, "Error fetching message");
                }
            }
        }

        debug!(count = result.len(), "Fetched messages");
        Ok(result)
    }
}

/// Wrapper around NATS message with ack/nack support
pub struct NatsMessage {
    inner: async_nats::jetstream::message::Message,
}

impl NatsMessage {
    /// Get the message payload as an upload event
    pub fn payload(&self) -> Result<UploadEvent> {
        let data = &self.inner.payload;
        let event: UploadEvent = serde_json::from_slice(data)?;
        Ok(event)
    }

    /// Get raw payload bytes
    pub fn raw_payload(&self) -> &[u8] {
        &self.inner.payload
    }

    /// Acknowledge the message
    pub async fn ack(&self) -> Result<()> {
        self.inner.ack().await.map_err(|e| {
            crate::IngestionError::Nats(e.to_string())
        })
    }

    /// Negative acknowledge (will be redelivered)
    pub async fn nack(&self) -> Result<()> {
        self.inner.ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .map_err(|e| crate::IngestionError::Nats(e.to_string()))
    }

    /// Terminate (won't be redelivered)
    pub async fn terminate(&self) -> Result<()> {
        self.inner.ack_with(async_nats::jetstream::AckKind::Term)
            .await
            .map_err(|e| crate::IngestionError::Nats(e.to_string()))
    }
}
