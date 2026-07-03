//! MinIO/S3 Storage Client
//!
//! Fetches documents from MinIO object storage.

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Builder, Credentials, Region};
use tracing::{debug, info, error};

use crate::config::StorageConfig;
use crate::Result;

/// Maximum document size to fetch (100 MB)
/// Documents larger than this will be rejected to prevent OOM
const MAX_DOCUMENT_SIZE: u64 = 100 * 1024 * 1024;

/// MinIO/S3 storage client for fetching documents
pub struct StorageClient {
    client: S3Client,
    default_bucket: String,
    max_size: u64,
}

impl StorageClient {
    /// Create a new storage client
    pub async fn new(config: &StorageConfig) -> Result<Self> {
        info!(endpoint = %config.endpoint, bucket = %config.bucket, "Connecting to MinIO");

        let credentials = Credentials::new(
            &config.access_key,
            &config.secret_key,
            None,
            None,
            "minio",
        );

        let s3_config = Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials)
            .force_path_style(true) // Required for MinIO
            .build();

        let client = S3Client::from_conf(s3_config);

        // Verify connectivity
        match client.list_buckets().send().await {
            Ok(response) => {
                let bucket_names: Vec<_> = response.buckets()
                    .iter()
                    .filter_map(|b| b.name())
                    .collect();
                info!(?bucket_names, "Connected to MinIO");
            }
            Err(e) => {
                error!(?e, "Failed to connect to MinIO");
                return Err(crate::IngestionError::Storage(format!("MinIO connection failed: {}", e)));
            }
        }

        Ok(Self {
            client,
            default_bucket: config.bucket.clone(),
            max_size: MAX_DOCUMENT_SIZE,
        })
    }

    /// Fetch a document from storage
    ///
    /// Checks document size before fetching to prevent OOM on large files.
    pub async fn fetch(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        debug!(bucket, key, "Fetching object");

        // First, check the object size using HEAD request to prevent OOM
        let meta = self.metadata(bucket, key).await?;
        if meta.size > self.max_size {
            error!(
                bucket,
                key,
                size = meta.size,
                max_size = self.max_size,
                "Document too large, rejecting to prevent OOM"
            );
            return Err(crate::IngestionError::Storage(format!(
                "Document too large: {} bytes (max: {} bytes)",
                meta.size, self.max_size
            )));
        }

        let response = self.client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| crate::IngestionError::Storage(format!("Failed to fetch {}/{}: {}", bucket, key, e)))?;

        let body = response.body
            .collect()
            .await
            .map_err(|e| crate::IngestionError::Storage(format!("Failed to read body: {}", e)))?;

        let bytes = body.into_bytes().to_vec();
        debug!(bucket, key, size = bytes.len(), "Object fetched");

        Ok(bytes)
    }

    /// Fetch from the default bucket
    pub async fn fetch_default(&self, key: &str) -> Result<Vec<u8>> {
        self.fetch(&self.default_bucket, key).await
    }

    /// Check if an object exists
    pub async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        match self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.to_string().contains("NoSuchKey") || e.to_string().contains("404") {
                    Ok(false)
                } else {
                    Err(crate::IngestionError::Storage(format!("Failed to check existence: {}", e)))
                }
            }
        }
    }

    /// Get object metadata (size, content type, etc.)
    pub async fn metadata(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        let response = self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| crate::IngestionError::Storage(format!("Failed to get metadata: {}", e)))?;

        Ok(ObjectMetadata {
            size: response.content_length().unwrap_or(0) as u64,
            content_type: response.content_type().map(|s| s.to_string()),
            etag: response.e_tag().map(|s| s.to_string()),
        })
    }
}

/// Object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_metadata() {
        let meta = ObjectMetadata {
            size: 1024,
            content_type: Some("application/pdf".to_string()),
            etag: Some("abc123".to_string()),
        };
        assert_eq!(meta.size, 1024);
    }
}
