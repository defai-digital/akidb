//! Resumable upload support for S3/MinIO snapshot storage
//!
//! Implements multipart uploads with checkpoint persistence for crash recovery.

use super::state_machine::{SnapshotStateMachine, SnapshotStateRecord, UploadCheckpoint};
use crate::{AkiDbError, Result, StorageBackend};
use base64::Engine;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tracing::{debug, info, warn};

/// Default chunk size for multipart uploads (64MB)
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Minimum chunk size required by S3 (5MB)
pub const MIN_CHUNK_SIZE: usize = 5 * 1024 * 1024;

/// Maximum parts in a multipart upload (S3 limit is 10,000)
pub const MAX_PARTS: u32 = 10_000;

/// Configuration for resumable uploads
#[derive(Debug, Clone)]
pub struct ResumableUploadConfig {
    /// Chunk size in bytes
    pub chunk_size: usize,
    /// Maximum retry attempts per chunk
    pub max_chunk_retries: u32,
    /// Timeout per chunk upload (seconds)
    pub chunk_timeout_secs: u64,
    /// Whether to verify checksums
    pub verify_checksums: bool,
}

impl Default for ResumableUploadConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_chunk_retries: 3,
            chunk_timeout_secs: 300, // 5 minutes per chunk
            verify_checksums: true,
        }
    }
}

/// Result of a completed multipart upload part
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPart {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
}

/// Resumable uploader for S3-compatible storage
pub struct ResumableUploader {
    /// HTTP client
    client: Client,
    /// S3 endpoint
    endpoint: String,
    /// Bucket name
    bucket: String,
    /// Access key
    access_key: String,
    /// Secret key
    secret_key: String,
    /// Configuration
    config: ResumableUploadConfig,
}

impl ResumableUploader {
    /// Create a new resumable uploader
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            config: ResumableUploadConfig::default(),
        }
    }

    /// Configure upload settings
    pub fn with_config(mut self, config: ResumableUploadConfig) -> Self {
        self.config = config;
        self
    }

    /// Calculate the number of chunks needed for a file
    pub fn calculate_chunks(&self, file_size: u64) -> u64 {
        if file_size == 0 {
            return 1;
        }
        (file_size + self.config.chunk_size as u64 - 1) / self.config.chunk_size as u64
    }

    /// Initiate a multipart upload
    pub async fn initiate_upload(&self, object_key: &str) -> Result<String> {
        let url = format!(
            "{}/{}/{}?uploads",
            self.endpoint, self.bucket, object_key
        );
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let signature = self.sign_request("POST", &format!("/{}?uploads", object_key), &date);

        let response = self
            .client
            .post(&url)
            .header("Date", &date)
            .header(
                "Authorization",
                format!("AWS {}:{}", self.access_key, signature),
            )
            .send()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to initiate upload: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "Failed to initiate multipart upload: {} - {}",
                status, body
            )));
        }

        let body = response.text().await.map_err(|e| {
            AkiDbError::Internal(format!("Failed to read initiate response: {}", e))
        })?;

        // Parse upload ID from XML response
        let upload_id = Self::parse_upload_id(&body)?;
        info!(upload_id = %upload_id, object_key, "Initiated multipart upload");
        Ok(upload_id)
    }

    /// Upload a single part
    pub async fn upload_part(
        &self,
        object_key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> Result<CompletedPart> {
        let url = format!(
            "{}/{}/{}?partNumber={}&uploadId={}",
            self.endpoint, self.bucket, object_key, part_number, upload_id
        );
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let path = format!(
            "/{}?partNumber={}&uploadId={}",
            object_key, part_number, upload_id
        );
        let signature = self.sign_request("PUT", &path, &date);

        let mut last_error = None;
        for attempt in 0..self.config.max_chunk_retries {
            let result = self
                .client
                .put(&url)
                .header("Date", &date)
                .header(
                    "Authorization",
                    format!("AWS {}:{}", self.access_key, signature),
                )
                .header("Content-Length", data.len())
                .body(data.to_vec())
                .timeout(std::time::Duration::from_secs(self.config.chunk_timeout_secs))
                .send()
                .await;

            match result {
                Ok(response) => {
                    if response.status().is_success() {
                        let etag = response
                            .headers()
                            .get("ETag")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.trim_matches('"').to_string())
                            .unwrap_or_default();

                        debug!(
                            part_number,
                            etag = %etag,
                            size = data.len(),
                            "Uploaded part"
                        );

                        return Ok(CompletedPart {
                            part_number,
                            etag,
                            size: data.len() as u64,
                        });
                    } else {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        last_error = Some(format!("Part upload failed: {} - {}", status, body));
                    }
                }
                Err(e) => {
                    last_error = Some(format!("Part upload request failed: {}", e));
                }
            }

            if attempt < self.config.max_chunk_retries - 1 {
                warn!(
                    part_number,
                    attempt,
                    error = ?last_error,
                    "Part upload failed, retrying"
                );
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
            }
        }

        Err(AkiDbError::Internal(
            last_error.unwrap_or_else(|| "Unknown upload error".to_string()),
        ))
    }

    /// Complete a multipart upload
    pub async fn complete_upload(
        &self,
        object_key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<String> {
        // Build completion XML
        let mut xml = String::from("<CompleteMultipartUpload>");
        for part in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                part.part_number, part.etag
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");

        let url = format!(
            "{}/{}/{}?uploadId={}",
            self.endpoint, self.bucket, object_key, upload_id
        );
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let path = format!("/{}?uploadId={}", object_key, upload_id);
        let signature = self.sign_request("POST", &path, &date);

        let response = self
            .client
            .post(&url)
            .header("Date", &date)
            .header(
                "Authorization",
                format!("AWS {}:{}", self.access_key, signature),
            )
            .header("Content-Type", "application/xml")
            .body(xml)
            .send()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to complete upload: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "Failed to complete multipart upload: {} - {}",
                status, body
            )));
        }

        let body = response.text().await.map_err(|e| {
            AkiDbError::Internal(format!("Failed to read complete response: {}", e))
        })?;

        // Parse ETag from response
        let etag = Self::parse_etag(&body).unwrap_or_default();
        info!(
            upload_id = %upload_id,
            object_key,
            parts_count = parts.len(),
            etag = %etag,
            "Completed multipart upload"
        );
        Ok(etag)
    }

    /// Abort a multipart upload
    pub async fn abort_upload(&self, object_key: &str, upload_id: &str) -> Result<()> {
        let url = format!(
            "{}/{}/{}?uploadId={}",
            self.endpoint, self.bucket, object_key, upload_id
        );
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let path = format!("/{}?uploadId={}", object_key, upload_id);
        let signature = self.sign_request("DELETE", &path, &date);

        let response = self
            .client
            .delete(&url)
            .header("Date", &date)
            .header(
                "Authorization",
                format!("AWS {}:{}", self.access_key, signature),
            )
            .send()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to abort upload: {}", e)))?;

        if !response.status().is_success() && response.status() != reqwest::StatusCode::NOT_FOUND {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "Failed to abort multipart upload: {} - {}",
                status, body
            )));
        }

        info!(upload_id = %upload_id, object_key, "Aborted multipart upload");
        Ok(())
    }

    /// Upload a file with checkpoint support
    pub async fn upload_file_with_checkpoint<S: StorageBackend>(
        &self,
        local_path: &Path,
        object_key: &str,
        state_machine: &SnapshotStateMachine<S>,
        record: &mut SnapshotStateRecord,
    ) -> Result<String> {
        // Get file size
        let metadata = tokio::fs::metadata(local_path).await.map_err(|e| {
            AkiDbError::Internal(format!("Failed to get file metadata: {}", e))
        })?;
        let file_size = metadata.len();
        let total_chunks = self.calculate_chunks(file_size);

        // Check for existing checkpoint
        let (upload_id, mut completed_parts, start_part, resumed_bytes) =
            if let Some(checkpoint) = &record.upload_checkpoint {
                if checkpoint.object_key == object_key && checkpoint.total_bytes == file_size {
                    info!(
                        upload_id = %checkpoint.upload_id,
                        completed_parts = checkpoint.completed_parts.len(),
                        bytes_uploaded = checkpoint.bytes_uploaded,
                        "Resuming upload from checkpoint"
                    );
                    let parts: Vec<CompletedPart> = checkpoint
                        .completed_parts
                        .iter()
                        .map(|(num, etag)| CompletedPart {
                            part_number: *num,
                            etag: etag.clone(),
                            size: 0, // Size not needed for completion
                        })
                        .collect();
                    // Use the checkpoint's bytes_uploaded value when resuming
                    (checkpoint.upload_id.clone(), parts, checkpoint.next_part, checkpoint.bytes_uploaded)
                } else {
                    // Different upload, start fresh
                    let upload_id = self.initiate_upload(object_key).await?;
                    (upload_id, Vec::new(), 1, 0)
                }
            } else {
                let upload_id = self.initiate_upload(object_key).await?;
                (upload_id, Vec::new(), 1, 0)
            };

        // Initialize state - use resumed_bytes from checkpoint when resuming
        let checkpoint = UploadCheckpoint {
            upload_id: upload_id.clone(),
            completed_parts: completed_parts
                .iter()
                .map(|p| (p.part_number, p.etag.clone()))
                .collect(),
            next_part: start_part,
            bytes_uploaded: resumed_bytes,
            total_bytes: file_size,
            object_key: object_key.to_string(),
            local_path: local_path.to_string_lossy().to_string(),
        };
        state_machine.transition_to_uploading(record, total_chunks, file_size, checkpoint)?;

        // Open file and seek to resume position
        let mut file = File::open(local_path).await.map_err(|e| {
            AkiDbError::Internal(format!("Failed to open file: {}", e))
        })?;

        let start_offset = (start_part - 1) as u64 * self.config.chunk_size as u64;
        if start_offset > 0 {
            file.seek(SeekFrom::Start(start_offset)).await.map_err(|e| {
                AkiDbError::Internal(format!("Failed to seek: {}", e))
            })?;
        }

        // Upload remaining parts
        let mut current_part = start_part;
        let mut bytes_uploaded: u64 = resumed_bytes;
        let mut buffer = vec![0u8; self.config.chunk_size];

        loop {
            let bytes_read = file.read(&mut buffer).await.map_err(|e| {
                AkiDbError::Internal(format!("Failed to read file: {}", e))
            })?;

            if bytes_read == 0 {
                break;
            }

            // Upload part
            let part = self
                .upload_part(object_key, &upload_id, current_part, &buffer[..bytes_read])
                .await?;

            completed_parts.push(part.clone());
            bytes_uploaded += part.size;
            current_part += 1;

            // Update checkpoint
            let checkpoint = UploadCheckpoint {
                upload_id: upload_id.clone(),
                completed_parts: completed_parts
                    .iter()
                    .map(|p| (p.part_number, p.etag.clone()))
                    .collect(),
                next_part: current_part,
                bytes_uploaded,
                total_bytes: file_size,
                object_key: object_key.to_string(),
                local_path: local_path.to_string_lossy().to_string(),
            };
            state_machine.update_upload_progress(
                record,
                completed_parts.len() as u64,
                bytes_uploaded,
                checkpoint,
            )?;
        }

        // Complete the upload
        let etag = self
            .complete_upload(object_key, &upload_id, &completed_parts)
            .await?;

        Ok(etag)
    }

    /// Sign an S3 request (AWS Signature Version 2)
    fn sign_request(&self, method: &str, path: &str, date: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha1::Sha1;

        let string_to_sign = format!("{}\n\n\n{}\n/{}{}", method, date, self.bucket, path);
        let mut mac = Hmac::<Sha1>::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC init");
        mac.update(string_to_sign.as_bytes());
        let result = mac.finalize();
        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Parse upload ID from InitiateMultipartUpload response
    fn parse_upload_id(xml: &str) -> Result<String> {
        // Simple parsing - look for <UploadId>...</UploadId>
        if let Some(start) = xml.find("<UploadId>") {
            let start = start + 10;
            if let Some(end) = xml[start..].find("</UploadId>") {
                return Ok(xml[start..start + end].to_string());
            }
        }
        Err(AkiDbError::Internal(
            "Failed to parse upload ID from response".to_string(),
        ))
    }

    /// Parse ETag from CompleteMultipartUpload response
    fn parse_etag(xml: &str) -> Option<String> {
        if let Some(start) = xml.find("<ETag>") {
            let start = start + 6;
            if let Some(end) = xml[start..].find("</ETag>") {
                return Some(xml[start..start + end].trim_matches('"').to_string());
            }
        }
        None
    }
}

/// Upload executor that combines state machine and uploader
pub struct SnapshotUploadExecutor<S: StorageBackend> {
    uploader: ResumableUploader,
    state_machine: Arc<SnapshotStateMachine<S>>,
}

impl<S: StorageBackend> SnapshotUploadExecutor<S> {
    /// Create a new upload executor
    pub fn new(
        uploader: ResumableUploader,
        state_machine: Arc<SnapshotStateMachine<S>>,
    ) -> Self {
        Self {
            uploader,
            state_machine,
        }
    }

    /// Execute an upload with full state machine integration
    pub async fn execute(
        &self,
        local_path: &Path,
        object_key: &str,
        record: &mut SnapshotStateRecord,
    ) -> Result<String> {
        match self
            .uploader
            .upload_file_with_checkpoint(local_path, object_key, &self.state_machine, record)
            .await
        {
            Ok(etag) => {
                self.state_machine.transition_to_verifying(record)?;
                // Verification could go here (e.g., HEAD request to check size/etag)
                self.state_machine.transition_to_completing(record)?;
                self.state_machine.complete_operation(record)?;
                Ok(etag)
            }
            Err(e) => {
                self.state_machine
                    .fail_operation(record, e.to_string())?;
                Err(e)
            }
        }
    }

    /// Resume a failed upload
    pub async fn resume(&self, record: &mut SnapshotStateRecord) -> Result<String> {
        if !record.state.is_resumable() {
            return Err(AkiDbError::InvalidParameter(
                "Operation is not resumable".to_string(),
            ));
        }

        // Reset from failed state if needed
        self.state_machine.reset_for_retry(record)?;

        // Get checkpoint info - clone the paths to avoid borrow issues
        let (local_path, object_key) = {
            let checkpoint = record.upload_checkpoint.as_ref().ok_or_else(|| {
                AkiDbError::InvalidParameter("No checkpoint data for resume".to_string())
            })?;
            (checkpoint.local_path.clone(), checkpoint.object_key.clone())
        };

        self.execute(Path::new(&local_path), &object_key, record).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_chunks() {
        let uploader = ResumableUploader::new(
            "http://localhost:9000",
            "test",
            "access",
            "secret",
        );

        assert_eq!(uploader.calculate_chunks(0), 1);
        assert_eq!(uploader.calculate_chunks(DEFAULT_CHUNK_SIZE as u64), 1);
        assert_eq!(uploader.calculate_chunks(DEFAULT_CHUNK_SIZE as u64 + 1), 2);
        assert_eq!(uploader.calculate_chunks(DEFAULT_CHUNK_SIZE as u64 * 10), 10);
    }

    #[test]
    fn test_parse_upload_id() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult>
                <Bucket>test</Bucket>
                <Key>object</Key>
                <UploadId>abc123</UploadId>
            </InitiateMultipartUploadResult>"#;

        let upload_id = ResumableUploader::parse_upload_id(xml).unwrap();
        assert_eq!(upload_id, "abc123");
    }

    #[test]
    fn test_parse_etag() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUploadResult>
                <Bucket>test</Bucket>
                <Key>object</Key>
                <ETag>"abc123"</ETag>
            </CompleteMultipartUploadResult>"#;

        let etag = ResumableUploader::parse_etag(xml).unwrap();
        assert_eq!(etag, "abc123");
    }

    #[test]
    fn test_config_defaults() {
        let config = ResumableUploadConfig::default();
        assert_eq!(config.chunk_size, DEFAULT_CHUNK_SIZE);
        assert_eq!(config.max_chunk_retries, 3);
        assert!(config.verify_checksums);
    }
}
