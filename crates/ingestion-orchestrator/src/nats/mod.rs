//! NATS JetStream integration
//!
//! Provides consumer and publisher for event-driven document processing.

use async_nats::jetstream;

use crate::Result;

pub mod consumer;
pub mod publisher;

pub use consumer::{NatsConsumer, UploadEvent, MAX_DELIVER};
pub use publisher::DlqPublisher;

/// Connect to NATS using the optional credentials in `NatsConfig`.
///
/// Precedence: `credentials_file` (NKey/JWT) > `token` > `user`+`password` >
/// anonymous. An absent credential section keeps the historical anonymous
/// behavior so existing loopback/compose deployments keep working.
pub(crate) async fn connect(config: &crate::config::NatsConfig) -> Result<async_nats::Client> {
    let options = async_nats::ConnectOptions::new();
    let options = if let Some(path) = &config.credentials_file {
        options
            .credentials_file(path)
            .await
            .map_err(|e| crate::IngestionError::Nats(format!("invalid credentials file: {e}")))?
    } else if let Some(token) = &config.token {
        options.token(token.clone())
    } else if let (Some(user), Some(password)) = (&config.user, &config.password) {
        options.user_and_password(user.clone(), password.clone())
    } else {
        options
    };

    options
        .connect(&config.url)
        .await
        .map_err(crate::IngestionError::from)
}

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
            subjects: vec!["seaweedfs.uploads.>".to_string(), "custom.>".to_string()],
            num_replicas: 1,
            ..Default::default()
        };
        let desired = jetstream::stream::Config {
            name: "INGESTION".to_string(),
            subjects: vec![
                "seaweedfs.uploads".to_string(),
                "seaweedfs.uploads.>".to_string(),
            ],
            num_replicas: 3,
            ..Default::default()
        };

        let updated = reconciled_stream_config(&current, &desired).unwrap();

        assert_eq!(updated.num_replicas, 3);
        assert_eq!(
            updated.subjects,
            vec!["seaweedfs.uploads.>", "custom.>", "seaweedfs.uploads"]
        );
    }
}
