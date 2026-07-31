//! NATS JetStream integration
//!
//! Provides consumer and publisher for event-driven document processing.

use async_nats::jetstream;

use crate::Result;

pub mod consumer;
pub mod publisher;

pub use consumer::{NatsConsumer, UploadEvent};
pub use publisher::DlqPublisher;

fn reconciled_stream_config(
    current: &jetstream::stream::Config,
    desired: &jetstream::stream::Config,
) -> Option<jetstream::stream::Config> {
    let mut updated = current.clone();
    let mut changed = false;

    for subject in &desired.subjects {
        if !updated.subjects.contains(subject) {
            updated.subjects.push(subject.clone());
            changed = true;
        }
    }
    if updated.num_replicas != desired.num_replicas {
        updated.num_replicas = desired.num_replicas;
        changed = true;
    }

    changed.then_some(updated)
}

pub(crate) async fn ensure_stream(
    context: &jetstream::Context,
    desired: jetstream::stream::Config,
) -> Result<jetstream::stream::Stream> {
    let mut stream = context.get_or_create_stream(desired.clone()).await?;
    if let Some(updated) = reconciled_stream_config(&stream.cached_info().config, &desired) {
        context.update_stream(&updated).await?;
        stream = context.get_stream(&desired.name).await?;
    }
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_reconciliation_preserves_subjects_and_updates_replication() {
        let current = jetstream::stream::Config {
            name: "INGESTION".to_string(),
            subjects: vec!["minio.uploads.>".to_string(), "custom.>".to_string()],
            num_replicas: 1,
            ..Default::default()
        };
        let desired = jetstream::stream::Config {
            name: "INGESTION".to_string(),
            subjects: vec!["minio.uploads".to_string(), "minio.uploads.>".to_string()],
            num_replicas: 3,
            ..Default::default()
        };

        let updated = reconciled_stream_config(&current, &desired).unwrap();

        assert_eq!(updated.num_replicas, 3);
        assert_eq!(
            updated.subjects,
            vec!["minio.uploads.>", "custom.>", "minio.uploads"]
        );
    }
}
