//! Dead Letter Queue Publisher
//!
//! Publishes failed documents to the DLQ for later retry or investigation.

use async_nats::jetstream::{self, Context};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::NatsConfig;
use crate::nats::ensure_stream;
use crate::Result;

/// Dead letter queue entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// Original upload event
    pub original_event: serde_json::Value,

    /// Error message
    pub error: String,

    /// Number of retry attempts
    pub retry_count: u32,

    /// Timestamp of failure
    pub failed_at: String,

    /// Processing stage where failure occurred
    pub stage: String,
}

/// Publisher for the dead letter queue
pub struct DlqPublisher {
    jetstream: Context,
    stream_name: String,
    subject: String,
}

impl DlqPublisher {
    /// Create a new DLQ publisher
    pub async fn new(config: &NatsConfig) -> Result<Self> {
        let client = async_nats::connect(&config.url).await?;
        let jetstream = jetstream::new(client);

        // Ensure DLQ stream exists
        ensure_stream(
            &jetstream,
            jetstream::stream::Config {
                name: config.dlq_stream.clone(),
                subjects: vec![format!("{}.>", config.dlq_stream)],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 100_000,
                max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
                num_replicas: config.replicas,
                ..Default::default()
            },
        )
        .await?;

        info!(stream = %config.dlq_stream, "DLQ publisher created");

        Ok(Self {
            jetstream,
            stream_name: config.dlq_stream.clone(),
            subject: format!("{}.failed", config.dlq_stream),
        })
    }

    /// Publish a failed document to the DLQ
    pub async fn publish(&self, entry: DlqEntry) -> Result<()> {
        let payload = serde_json::to_vec(&entry)?;

        self.jetstream
            .publish(self.subject.clone(), payload.into())
            .await?
            .await?;

        warn!(
            error = %entry.error,
            stage = %entry.stage,
            retry_count = entry.retry_count,
            "Document sent to DLQ"
        );

        Ok(())
    }

    /// Get DLQ statistics
    pub async fn stats(&self) -> Result<DlqStats> {
        let mut stream = self.jetstream.get_stream(&self.stream_name).await?;
        let info = stream.info().await?;

        Ok(DlqStats {
            message_count: info.state.messages,
            bytes: info.state.bytes,
        })
    }
}

/// DLQ statistics
#[derive(Debug, Clone)]
pub struct DlqStats {
    pub message_count: u64,
    pub bytes: u64,
}
