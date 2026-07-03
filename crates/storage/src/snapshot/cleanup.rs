//! Snapshot cleanup utilities
//!
//! Handles cleanup of orphaned temporary files and old snapshots.

use super::state_machine::SnapshotStateMachine;
use crate::{AkiDbError, Result, SnapshotBackend, StorageBackend};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, info, warn};

/// Configuration for snapshot cleanup
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Maximum age for temporary files before cleanup (default: 24 hours)
    pub temp_file_max_age: Duration,
    /// Maximum age for completed state records (default: 7 days)
    pub state_record_max_age: Duration,
    /// Maximum snapshots to keep per collection (default: 10)
    pub max_snapshots_per_collection: usize,
    /// Dry run mode (don't actually delete)
    pub dry_run: bool,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            temp_file_max_age: Duration::from_secs(24 * 60 * 60), // 24 hours
            state_record_max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            max_snapshots_per_collection: 10,
            dry_run: false,
        }
    }
}

/// Result of a cleanup operation
#[derive(Debug, Default)]
pub struct CleanupResult {
    /// Number of temporary files deleted
    pub temp_files_deleted: u32,
    /// Number of state records cleaned
    pub state_records_cleaned: u32,
    /// Number of old snapshots deleted
    pub old_snapshots_deleted: u32,
    /// Total bytes freed
    pub bytes_freed: u64,
    /// Errors encountered (non-fatal)
    pub errors: Vec<String>,
}

impl CleanupResult {
    /// Check if any cleanup was performed
    pub fn has_changes(&self) -> bool {
        self.temp_files_deleted > 0
            || self.state_records_cleaned > 0
            || self.old_snapshots_deleted > 0
    }
}

/// Snapshot cleanup service
pub struct SnapshotCleanup<S: StorageBackend> {
    state_machine: Arc<SnapshotStateMachine<S>>,
    config: CleanupConfig,
}

impl<S: StorageBackend> SnapshotCleanup<S> {
    /// Create a new cleanup service
    pub fn new(state_machine: Arc<SnapshotStateMachine<S>>) -> Self {
        Self {
            state_machine,
            config: CleanupConfig::default(),
        }
    }

    /// Configure cleanup settings
    pub fn with_config(mut self, config: CleanupConfig) -> Self {
        self.config = config;
        self
    }

    /// Run full cleanup
    pub async fn run_cleanup<B: SnapshotBackend>(
        &self,
        _backend: &B,
        local_temp_dir: Option<&Path>,
    ) -> Result<CleanupResult> {
        let mut result = CleanupResult::default();

        // Clean up state records
        match self.cleanup_state_records() {
            Ok(count) => result.state_records_cleaned = count,
            Err(e) => {
                result
                    .errors
                    .push(format!("State record cleanup failed: {}", e));
            }
        }

        // Clean up local temp files
        if let Some(temp_dir) = local_temp_dir {
            match self.cleanup_local_temp_files(temp_dir).await {
                Ok((count, bytes)) => {
                    result.temp_files_deleted = count;
                    result.bytes_freed += bytes;
                }
                Err(e) => {
                    result
                        .errors
                        .push(format!("Local temp cleanup failed: {}", e));
                }
            }
        }

        if result.has_changes() || !result.errors.is_empty() {
            info!(
                temp_files = result.temp_files_deleted,
                state_records = result.state_records_cleaned,
                old_snapshots = result.old_snapshots_deleted,
                bytes_freed = result.bytes_freed,
                errors = result.errors.len(),
                "Snapshot cleanup completed"
            );
        }

        Ok(result)
    }

    /// Clean up old state records
    fn cleanup_state_records(&self) -> Result<u32> {
        self.state_machine
            .cleanup_old_records(self.config.state_record_max_age.as_secs())
    }

    /// Clean up local temporary files
    async fn cleanup_local_temp_files(&self, temp_dir: &Path) -> Result<(u32, u64)> {
        if !temp_dir.exists() {
            return Ok((0, 0));
        }

        let mut deleted = 0;
        let mut bytes_freed = 0;
        let max_age = self.config.temp_file_max_age;
        let now = SystemTime::now();

        let mut entries = tokio::fs::read_dir(temp_dir)
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read temp directory: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read directory entry: {}", e)))?
        {
            let path = entry.path();

            // Only clean up files/directories with temp markers
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if !name.starts_with(".tmp") && !name.contains("_tmp_") && !name.ends_with(".tmp") {
                continue;
            }

            let metadata = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(e) => {
                    warn!(path = ?path, error = %e, "Failed to get metadata for temp file");
                    continue;
                }
            };

            let modified = match metadata.modified() {
                Ok(t) => t,
                Err(e) => {
                    warn!(path = ?path, error = %e, "Failed to get modification time");
                    continue;
                }
            };

            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            if age < max_age {
                debug!(path = ?path, age_secs = age.as_secs(), "Temp file too recent, skipping");
                continue;
            }

            let size = if metadata.is_dir() {
                // Calculate directory size recursively
                calculate_dir_size(&path).await.unwrap_or_default()
            } else {
                metadata.len()
            };

            if self.config.dry_run {
                info!(path = ?path, size, age_secs = age.as_secs(), "Would delete temp file (dry run)");
            } else {
                let delete_result = if metadata.is_dir() {
                    tokio::fs::remove_dir_all(&path).await
                } else {
                    tokio::fs::remove_file(&path).await
                };

                match delete_result {
                    Ok(()) => {
                        deleted += 1;
                        bytes_freed += size;
                        debug!(path = ?path, size, "Deleted temp file");
                    }
                    Err(e) => {
                        warn!(path = ?path, error = %e, "Failed to delete temp file");
                    }
                }
            }
        }

        Ok((deleted, bytes_freed))
    }
}

/// Clean up orphaned multipart uploads in S3
pub async fn cleanup_orphaned_uploads(
    client: &reqwest::Client,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    max_age: Duration,
) -> Result<u32> {
    // List multipart uploads
    let url = format!("{}/{}?uploads", endpoint, bucket);
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();

    // Sign request
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    let string_to_sign = format!("GET\n\n\n{}\n/{}?uploads", date, bucket);
    let mut mac = Hmac::<Sha1>::new_from_slice(secret_key.as_bytes()).expect("HMAC init");
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    let response = client
        .get(&url)
        .header("Date", &date)
        .header("Authorization", format!("AWS {}:{}", access_key, signature))
        .send()
        .await
        .map_err(|e| AkiDbError::Internal(format!("Failed to list uploads: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AkiDbError::Internal(format!(
            "Failed to list multipart uploads: {} - {}",
            status, body
        )));
    }

    let body = response
        .text()
        .await
        .map_err(|e| AkiDbError::Internal(format!("Failed to read upload list: {}", e)))?;

    // Parse uploads (simplified - look for Upload blocks)
    let mut aborted = 0;
    let now = chrono::Utc::now();

    // This is a simplified parser - production code should use proper XML parsing
    for upload_block in body.split("<Upload>").skip(1) {
        let upload_id = extract_xml_value(upload_block, "UploadId");
        let key = extract_xml_value(upload_block, "Key");
        let initiated = extract_xml_value(upload_block, "Initiated");

        if let (Some(upload_id), Some(key), Some(initiated)) = (upload_id, key, initiated) {
            // Parse initiated time
            if let Ok(initiated_time) = chrono::DateTime::parse_from_rfc3339(&initiated) {
                let age = now.signed_duration_since(initiated_time.with_timezone(&chrono::Utc));
                if age.num_seconds() > max_age.as_secs() as i64 {
                    // Abort this upload
                    if let Err(e) = abort_multipart_upload(
                        client, endpoint, bucket, access_key, secret_key, &key, &upload_id,
                    )
                    .await
                    {
                        warn!(upload_id = %upload_id, key = %key, error = %e, "Failed to abort orphaned upload");
                    } else {
                        aborted += 1;
                        info!(upload_id = %upload_id, key = %key, age_hours = age.num_hours(), "Aborted orphaned upload");
                    }
                }
            }
        }
    }

    Ok(aborted)
}

/// Abort a multipart upload
async fn abort_multipart_upload(
    client: &reqwest::Client,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    key: &str,
    upload_id: &str,
) -> Result<()> {
    let path = abort_multipart_upload_path(key, upload_id);
    let url = format!("{}/{}{}", endpoint, bucket, path);
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();

    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    let string_to_sign = format!("DELETE\n\n\n{}\n/{}{}", date, bucket, path);
    let mut mac = Hmac::<Sha1>::new_from_slice(secret_key.as_bytes()).expect("HMAC init");
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    let response = client
        .delete(&url)
        .header("Date", &date)
        .header("Authorization", format!("AWS {}:{}", access_key, signature))
        .send()
        .await
        .map_err(|e| AkiDbError::Internal(format!("Failed to abort upload: {}", e)))?;

    if !response.status().is_success() && response.status() != reqwest::StatusCode::NOT_FOUND {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AkiDbError::Internal(format!(
            "Failed to abort upload: {} - {}",
            status, body
        )));
    }

    Ok(())
}

fn abort_multipart_upload_path(key: &str, upload_id: &str) -> String {
    format!(
        "/{}?uploadId={}",
        encode_s3_object_key_path(key),
        encode_s3_query_value(upload_id)
    )
}

fn encode_s3_object_key_path(key: &str) -> String {
    key.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_s3_query_value(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Extract a value from XML
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    if let Some(start) = xml.find(&start_tag) {
        let value_start = start + start_tag.len();
        if let Some(end) = xml[value_start..].find(&end_tag) {
            return Some(decode_xml_entities(&xml[value_start..value_start + end]));
        }
    }
    None
}

/// Decode common XML entities without recursively decoding newly produced text.
fn decode_xml_entities(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut rest = value;

    while let Some(entity_start) = rest.find('&') {
        decoded.push_str(&rest[..entity_start]);
        rest = &rest[entity_start..];

        let Some(entity_end) = rest.find(';') else {
            decoded.push_str(rest);
            return decoded;
        };

        let entity = &rest[..=entity_end];
        match entity {
            "&amp;" => decoded.push('&'),
            "&lt;" => decoded.push('<'),
            "&gt;" => decoded.push('>'),
            "&quot;" => decoded.push('"'),
            "&apos;" => decoded.push('\''),
            _ => decoded.push_str(entity),
        }
        rest = &rest[entity_end + 1..];
    }

    decoded.push_str(rest);
    decoded
}

/// Calculate total size of a directory recursively
async fn calculate_dir_size(path: &Path) -> Result<u64> {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read directory: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read entry: {}", e)))?
        {
            let metadata = entry
                .metadata()
                .await
                .map_err(|e| AkiDbError::Internal(format!("Failed to get metadata: {}", e)))?;

            if metadata.is_dir() {
                stack.push(entry.path());
            } else {
                total += metadata.len();
            }
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_xml_value() {
        let xml = "<Root><UploadId>abc123</UploadId><Key>test/file</Key></Root>";

        assert_eq!(
            extract_xml_value(xml, "UploadId"),
            Some("abc123".to_string())
        );
        assert_eq!(extract_xml_value(xml, "Key"), Some("test/file".to_string()));
        assert_eq!(extract_xml_value(xml, "Missing"), None);
    }

    #[test]
    fn test_extract_xml_value_decodes_common_entities_once() {
        let xml = "<Root><Key>snapshots/a&amp;b/&lt;file&gt;&quot;x&quot;&apos;y&apos;</Key><UploadId>id&amp;1</UploadId><Escaped>&amp;lt;</Escaped></Root>";

        assert_eq!(
            extract_xml_value(xml, "Key"),
            Some("snapshots/a&b/<file>\"x\"'y'".to_string())
        );
        assert_eq!(extract_xml_value(xml, "UploadId"), Some("id&1".to_string()));
        assert_eq!(extract_xml_value(xml, "Escaped"), Some("&lt;".to_string()));
    }

    #[test]
    fn test_abort_multipart_upload_path_encodes_key_segments_and_upload_id() {
        assert_eq!(
            abort_multipart_upload_path("snapshots/snap 1/data/a?b#c&d+e.txt", "id+/= &"),
            "/snapshots/snap%201/data/a%3Fb%23c%26d%2Be.txt?uploadId=id%2B%2F%3D%20%26"
        );
        assert_eq!(
            abort_multipart_upload_path("snapshots/snap-1/nested/file.bin", "upload-1"),
            "/snapshots/snap-1/nested/file.bin?uploadId=upload-1"
        );
    }

    #[test]
    fn test_cleanup_config_defaults() {
        let config = CleanupConfig::default();
        assert_eq!(config.temp_file_max_age, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.max_snapshots_per_collection, 10);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_cleanup_result() {
        let mut result = CleanupResult::default();
        assert!(!result.has_changes());

        result.temp_files_deleted = 1;
        assert!(result.has_changes());

        result.temp_files_deleted = 0;
        result.state_records_cleaned = 1;
        assert!(result.has_changes());
    }
}
