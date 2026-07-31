//! NATS JetStream Consumer
//!
//! Consumes document upload events from MinIO bucket notifications.

use async_nats::jetstream::{self, consumer::PullConsumer};
use futures::StreamExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::config::NatsConfig;
use crate::nats::ensure_stream;
use crate::{IngestionError, Result};

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

#[derive(Debug, Deserialize)]
struct MinioNotification {
    #[serde(rename = "Records")]
    records: Vec<MinioRecord>,
}

#[derive(Debug, Deserialize)]
struct MinioRecord {
    #[serde(rename = "eventName")]
    event_name: String,
    #[serde(rename = "eventTime")]
    event_time: String,
    s3: MinioS3,
}

#[derive(Debug, Deserialize)]
struct MinioS3 {
    bucket: MinioBucket,
    object: MinioObject,
}

#[derive(Debug, Deserialize)]
struct MinioBucket {
    name: String,
}

#[derive(Debug, Deserialize)]
struct MinioObject {
    key: String,
    size: u64,
    #[serde(rename = "contentType")]
    content_type: Option<String>,
}

fn validate_upload_event(event: UploadEvent) -> Result<UploadEvent> {
    if event.bucket.trim().is_empty() {
        return Err(IngestionError::Nats(
            "upload event bucket must not be empty".to_string(),
        ));
    }
    if event.key.trim().is_empty() {
        return Err(IngestionError::Nats(
            "upload event key must not be empty".to_string(),
        ));
    }
    Ok(event)
}

fn decode_object_key(key: &str) -> Result<String> {
    percent_decode_str(&key.replace('+', " "))
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|error| IngestionError::Nats(format!("invalid object key encoding: {error}")))
}

fn parse_upload_events(data: &[u8]) -> Result<Vec<UploadEvent>> {
    if let Ok(event) = serde_json::from_slice::<UploadEvent>(data) {
        return validate_upload_event(event).map(|event| vec![event]);
    }

    let notification: MinioNotification = serde_json::from_slice(data)?;
    let events = notification
        .records
        .into_iter()
        .filter(|record| record.event_name.starts_with("s3:ObjectCreated:"))
        .map(|record| {
            validate_upload_event(UploadEvent {
                bucket: record.s3.bucket.name,
                key: decode_object_key(&record.s3.object.key)?,
                size: record.s3.object.size,
                content_type: record.s3.object.content_type,
                timestamp: record.event_time,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if events.is_empty() {
        return Err(IngestionError::Nats(
            "notification contains no object-created records".to_string(),
        ));
    }
    Ok(events)
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
        let stream = ensure_stream(
            &jetstream,
            jetstream::stream::Config {
                name: config.stream.clone(),
                subjects: vec![
                    "minio.uploads".to_string(),   // Exact match for MinIO notifications
                    "minio.uploads.>".to_string(), // Wildcard for hierarchical subjects
                ],
                retention: jetstream::stream::RetentionPolicy::WorkQueue,
                max_messages: 1_000_000,
                max_bytes: 1024 * 1024 * 1024, // 1GB
                num_replicas: config.replicas,
                ..Default::default()
            },
        )
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
        let mut messages = self
            .consumer
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

    /// Return messages that are either waiting for delivery or awaiting acknowledgement.
    pub async fn pending_messages(&self) -> Result<u64> {
        let info = self.consumer.get_info().await?;
        Ok(info.num_pending.saturating_add(info.num_ack_pending as u64))
    }
}

/// Wrapper around NATS message with ack/nack support
pub struct NatsMessage {
    inner: async_nats::jetstream::message::Message,
}

impl NatsMessage {
    /// Get canonical upload events from a gateway or MinIO notification.
    pub fn payloads(&self) -> Result<Vec<UploadEvent>> {
        parse_upload_events(&self.inner.payload)
    }

    /// Get raw payload bytes
    pub fn raw_payload(&self) -> &[u8] {
        &self.inner.payload
    }

    /// Acknowledge the message
    pub async fn ack(&self) -> Result<()> {
        self.inner
            .ack()
            .await
            .map_err(|e| crate::IngestionError::Nats(e.to_string()))
    }

    /// Negative acknowledge (will be redelivered)
    pub async fn nack(&self) -> Result<()> {
        self.inner
            .ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .map_err(|e| crate::IngestionError::Nats(e.to_string()))
    }

    /// Terminate (won't be redelivered)
    pub async fn terminate(&self) -> Result<()> {
        self.inner
            .ack_with(async_nats::jetstream::AckKind::Term)
            .await
            .map_err(|e| crate::IngestionError::Nats(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_gateway_event() {
        let events = parse_upload_events(
            br#"{
                "bucket":"akidb-documents",
                "key":"folder/report.pdf",
                "size":42,
                "content_type":"application/pdf",
                "timestamp":"2026-07-28T00:00:00Z",
                "metadata":{"source":"gateway"}
            }"#,
        )
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "folder/report.pdf");
        assert_eq!(events[0].content_type.as_deref(), Some("application/pdf"));
    }

    #[test]
    fn parses_minio_created_records_and_decodes_object_keys() {
        let events = parse_upload_events(
            br#"{
                "Records":[
                    {
                        "eventName":"s3:ObjectCreated:Put",
                        "eventTime":"2026-07-28T00:00:00Z",
                        "s3":{
                            "bucket":{"name":"akidb-documents"},
                            "object":{
                                "key":"folder%2Fquarterly+report.pdf",
                                "size":123,
                                "contentType":"application/pdf"
                            }
                        }
                    },
                    {
                        "eventName":"s3:ObjectRemoved:Delete",
                        "eventTime":"2026-07-28T00:01:00Z",
                        "s3":{
                            "bucket":{"name":"akidb-documents"},
                            "object":{"key":"old.pdf","size":0}
                        }
                    }
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "folder/quarterly report.pdf");
        assert_eq!(events[0].size, 123);
    }

    #[test]
    fn rejects_notifications_without_created_objects() {
        let error = parse_upload_events(
            br#"{
                "Records":[{
                    "eventName":"s3:ObjectRemoved:Delete",
                    "eventTime":"2026-07-28T00:01:00Z",
                    "s3":{
                        "bucket":{"name":"akidb-documents"},
                        "object":{"key":"old.pdf","size":0}
                    }
                }]
            }"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("no object-created records"));
    }
}
