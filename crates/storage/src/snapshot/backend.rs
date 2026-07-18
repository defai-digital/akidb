//! Snapshot storage for AkiDB
//!
//! This module provides snapshot storage backends for persisting index state
//! to various storage systems including local filesystem and S3-compatible
//! object stores (like MinIO).

use akidb_common::{AkiDbError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{debug, info, warn};

const SNAPSHOT_METADATA_FILE: &str = "metadata.json";

fn validate_snapshot_id(snapshot_id: &str) -> Result<()> {
    if snapshot_id.is_empty() {
        return Err(AkiDbError::InvalidParameter(
            "Snapshot id cannot be empty".to_string(),
        ));
    }

    if snapshot_id == "." || snapshot_id == ".." {
        return Err(AkiDbError::InvalidParameter(format!(
            "Invalid snapshot id: {}",
            snapshot_id
        )));
    }

    if !snapshot_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(AkiDbError::InvalidParameter(format!(
            "Invalid snapshot id: {}",
            snapshot_id
        )));
    }

    Ok(())
}

fn validate_snapshot_file_path(relative_path: &str) -> Result<()> {
    if relative_path.is_empty() {
        return Err(AkiDbError::InvalidParameter(
            "Snapshot file path cannot be empty".to_string(),
        ));
    }

    if relative_path.contains('\0') {
        return Err(AkiDbError::InvalidParameter(
            "Path contains null byte".to_string(),
        ));
    }

    let path = std::path::Path::new(relative_path);
    if path.is_absolute() {
        return Err(AkiDbError::InvalidParameter(format!(
            "Absolute snapshot file path rejected: {}",
            relative_path
        )));
    }

    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                return Err(AkiDbError::InvalidParameter(format!(
                    "Path traversal detected: {}",
                    relative_path
                )));
            }
            std::path::Component::Normal(name) => {
                if name == SNAPSHOT_METADATA_FILE {
                    return Err(AkiDbError::InvalidParameter(format!(
                        "Reserved snapshot file path rejected: {}",
                        relative_path
                    )));
                }
            }
            std::path::Component::CurDir => {}
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err(AkiDbError::InvalidParameter(format!(
                    "Absolute snapshot file path rejected: {}",
                    relative_path
                )));
            }
        }
    }

    Ok(())
}

fn snapshot_metadata_id_from_key(key: &str) -> Option<&str> {
    let rest = key.strip_prefix("snapshots/")?;
    let (snapshot_id, path) = rest.split_once('/')?;

    if snapshot_id.is_empty() || path != SNAPSHOT_METADATA_FILE {
        return None;
    }

    Some(snapshot_id)
}

/// Metadata about a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Unique snapshot ID
    pub id: String,
    /// Collection name
    pub collection: String,
    /// Shard ID (if sharded)
    pub shard_id: Option<String>,
    /// Timestamp when snapshot was created
    pub created_at: u64,
    /// Total vectors in the snapshot
    pub total_vectors: u64,
    /// Active (non-deleted) vectors
    pub active_vectors: u64,
    /// Index dimensions
    pub dimensions: usize,
    /// Size in bytes
    pub size_bytes: u64,
    /// Index type (e.g., "IVF-Flat")
    pub index_type: String,
    /// Optional description
    pub description: Option<String>,
}

impl SnapshotMetadata {
    pub fn new(collection: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            collection: collection.to_string(),
            shard_id: None,
            // Use unwrap_or_default to avoid panic if system clock is before epoch
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            total_vectors: 0,
            active_vectors: 0,
            dimensions: 0,
            size_bytes: 0,
            index_type: "IVF-Flat".to_string(),
            description: None,
        }
    }

    /// Integrity checks that must pass before a restored snapshot serves traffic
    /// (GAP-028 / SEC-105).
    pub fn verify_for_restore(&self, files: &[SnapshotFile]) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(AkiDbError::InvalidParameter(
                "snapshot id is empty".to_string(),
            ));
        }
        if self.collection.trim().is_empty() {
            return Err(AkiDbError::InvalidParameter(
                "snapshot collection is empty".to_string(),
            ));
        }
        if self.dimensions == 0 {
            return Err(AkiDbError::InvalidParameter(
                "snapshot dimensions must be > 0 before serving".to_string(),
            ));
        }
        if self.active_vectors > self.total_vectors {
            return Err(AkiDbError::InvalidParameter(format!(
                "snapshot active_vectors ({}) exceeds total_vectors ({})",
                self.active_vectors, self.total_vectors
            )));
        }
        let sum_bytes: u64 = files.iter().map(|f| f.data.len() as u64).sum();
        if self.size_bytes > 0 && sum_bytes > 0 && self.size_bytes != sum_bytes {
            return Err(AkiDbError::InvalidParameter(format!(
                "snapshot size_bytes mismatch: meta={} files={}",
                self.size_bytes, sum_bytes
            )));
        }
        for file in files {
            if file.path.trim().is_empty() {
                return Err(AkiDbError::InvalidParameter(
                    "snapshot file path is empty".to_string(),
                ));
            }
            if file.path.contains("..") {
                return Err(AkiDbError::InvalidParameter(format!(
                    "snapshot file path escapes sandbox: {}",
                    file.path
                )));
            }
        }
        Ok(())
    }
}

/// A snapshot file to be stored
pub struct SnapshotFile {
    /// Relative path within the snapshot
    pub path: String,
    /// File contents
    pub data: Vec<u8>,
}

/// Trait for snapshot storage backends
#[async_trait]
pub trait SnapshotBackend: Send + Sync {
    /// Save a snapshot
    async fn save(&self, metadata: &SnapshotMetadata, files: Vec<SnapshotFile>) -> Result<String>;

    /// Load a snapshot by ID
    async fn load(&self, snapshot_id: &str) -> Result<(SnapshotMetadata, Vec<SnapshotFile>)>;

    /// List available snapshots for a collection
    async fn list(&self, collection: &str) -> Result<Vec<SnapshotMetadata>>;

    /// Delete a snapshot
    async fn delete(&self, snapshot_id: &str) -> Result<()>;

    /// Check if a snapshot exists
    async fn exists(&self, snapshot_id: &str) -> Result<bool>;
}

/// Local filesystem snapshot backend
pub struct LocalSnapshotBackend {
    /// Base directory for snapshots
    base_path: PathBuf,
}

impl LocalSnapshotBackend {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    fn snapshot_dir(&self, snapshot_id: &str) -> PathBuf {
        self.base_path.join(snapshot_id)
    }

    fn metadata_path(&self, snapshot_id: &str) -> PathBuf {
        self.snapshot_dir(snapshot_id).join(SNAPSHOT_METADATA_FILE)
    }

    /// FIX BUG-054: Validate that a file path stays within the snapshot directory
    /// This prevents path traversal attacks like "../../../etc/passwd"
    fn validate_path(
        &self,
        snapshot_dir: &std::path::Path,
        relative_path: &str,
    ) -> Result<PathBuf> {
        validate_snapshot_file_path(relative_path)?;

        // Normalize path components to catch traversal attempts
        let file_path = snapshot_dir.join(relative_path);

        // Use lexical normalization to resolve .. and . without hitting filesystem
        // This catches attempts like "foo/../../../etc/passwd"
        let mut normalized = PathBuf::new();
        for component in file_path.components() {
            match component {
                std::path::Component::ParentDir => {
                    // Only pop if we're still within the snapshot_dir
                    if normalized.starts_with(snapshot_dir) && normalized != snapshot_dir {
                        normalized.pop();
                    } else {
                        // Trying to escape snapshot_dir
                        return Err(AkiDbError::InvalidParameter(format!(
                            "Path traversal detected: {}",
                            relative_path
                        )));
                    }
                }
                std::path::Component::Normal(c) => normalized.push(c),
                std::path::Component::RootDir => normalized.push(std::path::MAIN_SEPARATOR_STR),
                std::path::Component::Prefix(p) => normalized.push(p.as_os_str()),
                std::path::Component::CurDir => {} // Skip "."
            }
        }

        // Final check: ensure the normalized path starts with snapshot_dir
        if !normalized.starts_with(snapshot_dir) {
            return Err(AkiDbError::InvalidParameter(format!(
                "Path traversal detected: {}",
                relative_path
            )));
        }

        Ok(normalized)
    }
}

#[async_trait]
impl SnapshotBackend for LocalSnapshotBackend {
    async fn save(&self, metadata: &SnapshotMetadata, files: Vec<SnapshotFile>) -> Result<String> {
        validate_snapshot_id(&metadata.id)?;
        for file in &files {
            validate_snapshot_file_path(&file.path)?;
        }

        let snapshot_dir = self.snapshot_dir(&metadata.id);

        // Create directory
        tokio::fs::create_dir_all(&snapshot_dir)
            .await
            .map_err(|e| {
                AkiDbError::Internal(format!("Failed to create snapshot directory: {}", e))
            })?;

        // Save metadata
        let metadata_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| AkiDbError::Internal(format!("Failed to serialize metadata: {}", e)))?;
        tokio::fs::write(self.metadata_path(&metadata.id), metadata_json)
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to write metadata: {}", e)))?;

        // Save files
        for file in files {
            // FIX BUG-054: Validate path to prevent traversal attacks
            let file_path = self.validate_path(&snapshot_dir, &file.path)?;
            if let Some(parent) = file_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    AkiDbError::Internal(format!("Failed to create parent directory: {}", e))
                })?;
            }
            tokio::fs::write(&file_path, &file.data)
                .await
                .map_err(|e| {
                    AkiDbError::Internal(format!("Failed to write file {}: {}", file.path, e))
                })?;
        }

        info!(
            "Saved snapshot {} to {}",
            metadata.id,
            snapshot_dir.display()
        );
        Ok(metadata.id.clone())
    }

    async fn load(&self, snapshot_id: &str) -> Result<(SnapshotMetadata, Vec<SnapshotFile>)> {
        validate_snapshot_id(snapshot_id)?;
        let snapshot_dir = self.snapshot_dir(snapshot_id);

        // Use async exists check to avoid blocking the runtime
        if !tokio::fs::try_exists(&snapshot_dir).await.unwrap_or(false) {
            return Err(AkiDbError::VectorNotFound(format!(
                "Snapshot {} not found",
                snapshot_id
            )));
        }

        // Load metadata
        let metadata_content = tokio::fs::read_to_string(self.metadata_path(snapshot_id))
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read metadata: {}", e)))?;
        let metadata: SnapshotMetadata = serde_json::from_str(&metadata_content)
            .map_err(|e| AkiDbError::Internal(format!("Failed to parse metadata: {}", e)))?;

        // Load files (recursively)
        let mut files = Vec::new();
        let mut stack = vec![snapshot_dir.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| AkiDbError::Internal(format!("Failed to read directory: {}", e)))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| AkiDbError::Internal(format!("Failed to read entry: {}", e)))?
            {
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .await
                    .map_err(|e| AkiDbError::Internal(format!("Failed to get file type: {}", e)))?;

                if file_type.is_dir() {
                    stack.push(path);
                } else if file_type.is_file() {
                    // Skip metadata.json
                    if path
                        .file_name()
                        .map(|n| n == SNAPSHOT_METADATA_FILE)
                        .unwrap_or(false)
                    {
                        continue;
                    }

                    // FIX BUG-HUNT-006: Use proper error handling instead of unwrap()
                    // strip_prefix can fail if path doesn't start with snapshot_dir
                    // (e.g., symlinks, mount points, or path canonicalization issues)
                    let relative_path = path
                        .strip_prefix(&snapshot_dir)
                        .map_err(|e| AkiDbError::Internal(format!(
                            "Unexpected path structure during snapshot load: {:?} is not under {:?}: {}",
                            path, snapshot_dir, e
                        )))?
                        .to_string_lossy()
                        .to_string();
                    let data = tokio::fs::read(&path)
                        .await
                        .map_err(|e| AkiDbError::Internal(format!("Failed to read file: {}", e)))?;

                    files.push(SnapshotFile {
                        path: relative_path,
                        data,
                    });
                }
            }
        }

        info!("Loaded snapshot {} with {} files", snapshot_id, files.len());
        Ok((metadata, files))
    }

    async fn list(&self, collection: &str) -> Result<Vec<SnapshotMetadata>> {
        let mut snapshots = Vec::new();

        // Use async exists check to avoid blocking the runtime
        if !tokio::fs::try_exists(&self.base_path)
            .await
            .unwrap_or(false)
        {
            return Ok(snapshots);
        }

        let mut entries = tokio::fs::read_dir(&self.base_path).await.map_err(|e| {
            AkiDbError::Internal(format!("Failed to read snapshot directory: {}", e))
        })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read entry: {}", e)))?
        {
            let path = entry.path();
            // Use async file_type check instead of blocking is_dir()
            let file_type = match entry.file_type().await {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if !file_type.is_dir() {
                continue;
            }

            let metadata_path = path.join(SNAPSHOT_METADATA_FILE);
            // Use async exists check
            if !tokio::fs::try_exists(&metadata_path).await.unwrap_or(false) {
                continue;
            }

            match tokio::fs::read_to_string(&metadata_path).await {
                Ok(content) => match serde_json::from_str::<SnapshotMetadata>(&content) {
                    Ok(metadata) => {
                        if metadata.collection == collection {
                            snapshots.push(metadata);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse metadata in {:?}: {}", path, e);
                    }
                },
                Err(e) => {
                    warn!("Failed to read metadata in {:?}: {}", path, e);
                }
            }
        }

        // Sort by created_at descending (newest first)
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));

        Ok(snapshots)
    }

    async fn delete(&self, snapshot_id: &str) -> Result<()> {
        validate_snapshot_id(snapshot_id)?;
        let snapshot_dir = self.snapshot_dir(snapshot_id);

        // Use async exists check to avoid blocking the runtime
        if !tokio::fs::try_exists(&snapshot_dir).await.unwrap_or(false) {
            return Ok(()); // Idempotent
        }

        tokio::fs::remove_dir_all(&snapshot_dir)
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to delete snapshot: {}", e)))?;

        info!("Deleted snapshot {}", snapshot_id);
        Ok(())
    }

    async fn exists(&self, snapshot_id: &str) -> Result<bool> {
        validate_snapshot_id(snapshot_id)?;
        // Use async exists check to avoid blocking the runtime
        Ok(tokio::fs::try_exists(self.metadata_path(snapshot_id))
            .await
            .unwrap_or(false))
    }
}

/// S3/MinIO compatible snapshot backend
pub struct S3SnapshotBackend {
    /// S3 endpoint URL (e.g., http://minio:9000)
    endpoint: String,
    /// Bucket name
    bucket: String,
    /// Access key
    access_key: String,
    /// Secret key
    secret_key: String,
    /// Region (default: us-east-1)
    region: String,
    /// HTTP client
    client: reqwest::Client,
}

impl S3SnapshotBackend {
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            region: "us-east-1".to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    fn object_key(&self, snapshot_id: &str, file_path: &str) -> String {
        format!("snapshots/{}/{}", snapshot_id, file_path)
    }

    fn object_path(key: &str) -> String {
        format!("/{}", encode_s3_object_key_path(key))
    }

    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/{}{}",
            self.endpoint,
            self.bucket,
            Self::object_path(key)
        )
    }

    fn sign_request(&self, method: &str, path: &str, date: &str) -> String {
        use base64::Engine;
        use hmac::{Hmac, Mac};
        use sha1::Sha1;

        // AWS Signature Version 2 uses HMAC-SHA1 (not SHA256)
        // String to sign format: HTTP-Verb + "\n" + Content-MD5 + "\n" + Content-Type + "\n" + Date + "\n" + CanonicalizedResource
        let string_to_sign = format!("{}\n\n\n{}\n/{}{}", method, date, self.bucket, path);

        let mut mac = Hmac::<Sha1>::new_from_slice(self.secret_key.as_bytes()).expect("HMAC init");
        mac.update(string_to_sign.as_bytes());
        let result = mac.finalize();

        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }

    async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let path = Self::object_path(key);
        let signature = self.sign_request("PUT", &path, &date);

        let url = self.object_url(key);

        let response = self
            .client
            .put(&url)
            .header("Date", &date)
            .header(
                "Authorization",
                format!("AWS {}:{}", self.access_key, signature),
            )
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| AkiDbError::Internal(format!("S3 PUT failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "S3 PUT failed: {} - {}",
                status, body
            )));
        }

        debug!("Uploaded {} to S3", key);
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let path = Self::object_path(key);
        let signature = self.sign_request("GET", &path, &date);

        let url = self.object_url(key);

        let response = self
            .client
            .get(&url)
            .header("Date", &date)
            .header(
                "Authorization",
                format!("AWS {}:{}", self.access_key, signature),
            )
            .send()
            .await
            .map_err(|e| AkiDbError::Internal(format!("S3 GET failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(AkiDbError::VectorNotFound(format!(
                    "Object {} not found",
                    key
                )));
            }
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "S3 GET failed: {} - {}",
                status, body
            )));
        }

        let data = response
            .bytes()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read response: {}", e)))?;

        Ok(data.to_vec())
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let path = Self::object_path(key);
        let signature = self.sign_request("DELETE", &path, &date);

        let url = self.object_url(key);

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
            .map_err(|e| AkiDbError::Internal(format!("S3 DELETE failed: {}", e)))?;

        if !response.status().is_success() && response.status() != reqwest::StatusCode::NOT_FOUND {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "S3 DELETE failed: {} - {}",
                status, body
            )));
        }

        debug!("Deleted {} from S3", key);
        Ok(())
    }

    /// List objects with a given prefix
    ///
    /// FIX BUG-HUNT-201: Fixed S3 signature mismatch for list operations.
    /// Previously signed with path "/" resulting in canonical resource "/{bucket}/"
    /// but URL was "/{bucket}?prefix=..." (no trailing slash), causing signature
    /// verification to fail on strict S3 implementations.
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        // FIX BUG-HUNT-201: Use empty path for bucket-level ListObjects operation
        // Canonical resource becomes "/{bucket}" matching the URL path
        let path = "";
        let signature = self.sign_request("GET", path, &date);

        // S3 ListObjects API - use prefix parameter
        let url = format!(
            "{}/{}?prefix={}&list-type=2",
            self.endpoint,
            self.bucket,
            urlencoding::encode(prefix)
        );

        let response = self
            .client
            .get(&url)
            .header("Date", &date)
            .header(
                "Authorization",
                format!("AWS {}:{}", self.access_key, signature),
            )
            .send()
            .await
            .map_err(|e| AkiDbError::Internal(format!("S3 LIST failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AkiDbError::Internal(format!(
                "S3 LIST failed: {} - {}",
                status, body
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| AkiDbError::Internal(format!("Failed to read LIST response: {}", e)))?;

        // Parse XML response to extract keys
        // Simple parsing - look for <Key>...</Key> tags
        let mut keys = Vec::new();
        for part in body.split("<Key>") {
            if let Some(end_idx) = part.find("</Key>") {
                let key = &part[..end_idx];
                // Decode common XML entities
                let decoded_key = Self::decode_xml_entities(key);
                keys.push(decoded_key);
            }
        }

        debug!("Listed {} objects with prefix {}", keys.len(), prefix);
        Ok(keys)
    }

    /// Decode common XML entities in a string
    fn decode_xml_entities(s: &str) -> String {
        s.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
    }
}

#[async_trait]
impl SnapshotBackend for S3SnapshotBackend {
    async fn save(&self, metadata: &SnapshotMetadata, files: Vec<SnapshotFile>) -> Result<String> {
        validate_snapshot_id(&metadata.id)?;
        for file in &files {
            validate_snapshot_file_path(&file.path)?;
        }

        // Save metadata
        let metadata_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| AkiDbError::Internal(format!("Failed to serialize metadata: {}", e)))?;
        self.put_object(
            &self.object_key(&metadata.id, SNAPSHOT_METADATA_FILE),
            metadata_json.as_bytes(),
        )
        .await?;

        // Save files
        for file in files {
            self.put_object(&self.object_key(&metadata.id, &file.path), &file.data)
                .await?;
        }

        info!(
            "Saved snapshot {} to S3 bucket {}",
            metadata.id, self.bucket
        );
        Ok(metadata.id.clone())
    }

    async fn load(&self, snapshot_id: &str) -> Result<(SnapshotMetadata, Vec<SnapshotFile>)> {
        validate_snapshot_id(snapshot_id)?;
        // Load metadata
        let metadata_bytes = self
            .get_object(&self.object_key(snapshot_id, SNAPSHOT_METADATA_FILE))
            .await?;
        let metadata: SnapshotMetadata = serde_json::from_slice(&metadata_bytes)
            .map_err(|e| AkiDbError::Internal(format!("Failed to parse metadata: {}", e)))?;

        // List all objects in the snapshot prefix
        let prefix = format!("snapshots/{}/", snapshot_id);
        let keys = self.list_objects(&prefix).await?;

        // Load all files (except metadata.json)
        let mut files = Vec::new();
        for key in keys {
            // Extract relative path from key
            let Some(relative_path) = key.strip_prefix(&prefix) else {
                warn!(
                    "Ignoring S3 snapshot object outside prefix {}: {}",
                    prefix, key
                );
                continue;
            };

            if relative_path.is_empty() {
                continue;
            }

            // Skip only the root metadata object, not files such as notmetadata.json.
            if relative_path == SNAPSHOT_METADATA_FILE {
                continue;
            }

            match self.get_object(&key).await {
                Ok(data) => {
                    files.push(SnapshotFile {
                        path: relative_path.to_string(),
                        data,
                    });
                }
                Err(e) => {
                    warn!("Failed to load file {}: {}", key, e);
                }
            }
        }

        info!(
            "Loaded snapshot {} from S3 with {} files",
            snapshot_id,
            files.len()
        );
        Ok((metadata, files))
    }

    async fn list(&self, collection: &str) -> Result<Vec<SnapshotMetadata>> {
        // List all snapshot directories
        let prefix = "snapshots/";
        let keys = self.list_objects(prefix).await?;

        // Find all metadata.json files
        let mut snapshots = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for key in keys {
            let Some(snapshot_id) = snapshot_metadata_id_from_key(&key) else {
                continue;
            };

            // Skip if we've already processed this snapshot
            if seen_ids.contains(snapshot_id) {
                continue;
            }
            seen_ids.insert(snapshot_id.to_string());

            // Load and parse metadata
            match self.get_object(&key).await {
                Ok(data) => {
                    match serde_json::from_slice::<SnapshotMetadata>(&data) {
                        Ok(metadata) => {
                            // Filter by collection
                            if collection.is_empty() || metadata.collection == collection {
                                snapshots.push(metadata);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse metadata {}: {}", key, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to load metadata {}: {}", key, e);
                }
            }
        }

        // Sort by created_at descending (newest first)
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));

        Ok(snapshots)
    }

    async fn delete(&self, snapshot_id: &str) -> Result<()> {
        validate_snapshot_id(snapshot_id)?;
        // FIX BUG-048: Use tombstone approach for atomic deletion
        // 1. Create a deletion marker first (marks intent to delete)
        // 2. Delete all objects
        // 3. Delete the marker last
        // If we crash after step 1 but before completion, cleanup can detect
        // the marker and resume deletion. This prevents partial deletion state.

        let prefix = format!("snapshots/{}/", snapshot_id);
        let tombstone_key = format!("snapshots/{}/.deleting", snapshot_id);

        // Step 1: Create tombstone marker to indicate deletion in progress
        // If this fails, the snapshot is untouched
        self.put_object(&tombstone_key, b"deleting").await?;

        // Step 2: List and delete all objects (excluding tombstone)
        let keys = self.list_objects(&prefix).await?;
        let mut delete_errors = Vec::new();

        for key in &keys {
            // Skip the tombstone marker - we'll delete it last
            if key == &tombstone_key {
                continue;
            }

            if let Err(e) = self.delete_object(key).await {
                warn!("Failed to delete {}: {}", key, e);
                delete_errors.push(key.clone());
            }
        }

        // Step 3: Only delete tombstone if all objects were deleted
        // If there were errors, leave the tombstone so cleanup can retry
        if !delete_errors.is_empty() {
            // Leave tombstone in place for cleanup to detect and retry
            return Err(AkiDbError::Internal(format!(
                "Partial delete failure for snapshot {} ({} objects failed). Tombstone marker left for cleanup retry: {:?}",
                snapshot_id,
                delete_errors.len(),
                delete_errors
            )));
        }

        // All objects deleted, now remove the tombstone marker
        if let Err(e) = self.delete_object(&tombstone_key).await {
            // This is not critical - the snapshot is effectively deleted
            // The orphaned tombstone can be cleaned up later
            warn!(
                "Failed to delete tombstone marker for {}: {}",
                snapshot_id, e
            );
        }

        info!(
            "Deleted snapshot {} from S3 ({} objects)",
            snapshot_id,
            keys.len()
        );
        Ok(())
    }

    async fn exists(&self, snapshot_id: &str) -> Result<bool> {
        validate_snapshot_id(snapshot_id)?;
        match self
            .get_object(&self.object_key(snapshot_id, SNAPSHOT_METADATA_FILE))
            .await
        {
            Ok(_) => Ok(true),
            Err(AkiDbError::VectorNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Snapshot manager that handles snapshot lifecycle
pub struct SnapshotManager {
    backend: Box<dyn SnapshotBackend>,
    /// Maximum snapshots to keep per collection
    max_snapshots: usize,
}

impl SnapshotManager {
    pub fn new(backend: impl SnapshotBackend + 'static) -> Self {
        Self {
            backend: Box::new(backend),
            max_snapshots: 10,
        }
    }

    pub fn with_max_snapshots(mut self, max: usize) -> Self {
        self.max_snapshots = max;
        self
    }

    /// Create a snapshot
    pub async fn create_snapshot(
        &self,
        metadata: SnapshotMetadata,
        files: Vec<SnapshotFile>,
    ) -> Result<String> {
        let collection = metadata.collection.clone();
        let snapshot_id = self.backend.save(&metadata, files).await?;

        // Cleanup old snapshots if we have too many
        self.cleanup_old_snapshots(&collection).await?;

        Ok(snapshot_id)
    }

    /// Restore a snapshot after integrity verification (GAP-028).
    pub async fn restore_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<(SnapshotMetadata, Vec<SnapshotFile>)> {
        let (metadata, files) = self.backend.load(snapshot_id).await?;
        metadata.verify_for_restore(&files)?;
        Ok((metadata, files))
    }

    /// List snapshots for a collection
    pub async fn list_snapshots(&self, collection: &str) -> Result<Vec<SnapshotMetadata>> {
        self.backend.list(collection).await
    }

    /// Delete a snapshot
    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.backend.delete(snapshot_id).await
    }

    /// Cleanup old snapshots, keeping only the most recent ones
    async fn cleanup_old_snapshots(&self, collection: &str) -> Result<()> {
        let snapshots = self.backend.list(collection).await?;

        if snapshots.len() > self.max_snapshots {
            // Delete oldest snapshots
            for snapshot in snapshots.iter().skip(self.max_snapshots) {
                info!("Cleaning up old snapshot {}", snapshot.id);
                self.backend.delete(&snapshot.id).await?;
            }
        }

        Ok(())
    }
}

fn encode_s3_object_key_path(key: &str) -> String {
    key.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_verify_for_restore_rejects_zero_dimensions() {
        let mut meta = SnapshotMetadata::new("default");
        meta.dimensions = 0;
        let err = meta.verify_for_restore(&[]).unwrap_err();
        assert!(err.to_string().contains("dimensions"));
    }

    #[test]
    fn test_snapshot_verify_for_restore_accepts_consistent_meta() {
        let mut meta = SnapshotMetadata::new("default");
        meta.dimensions = 768;
        meta.total_vectors = 10;
        meta.active_vectors = 8;
        meta.size_bytes = 3;
        let files = vec![SnapshotFile {
            path: "index.bin".into(),
            data: vec![1, 2, 3],
        }];
        meta.verify_for_restore(&files).unwrap();
    }

    #[test]
    fn test_s3_object_path_encodes_key_segments_without_encoding_slashes() {
        assert_eq!(
            S3SnapshotBackend::object_path("snapshots/snap 1/data/a?b#c&d+e.txt"),
            "/snapshots/snap%201/data/a%3Fb%23c%26d%2Be.txt"
        );
        assert_eq!(
            S3SnapshotBackend::object_path("snapshots/snap-1/nested/file.bin"),
            "/snapshots/snap-1/nested/file.bin"
        );
    }

    #[test]
    fn test_s3_object_url_uses_encoded_path() {
        let backend = S3SnapshotBackend::new("http://localhost:9000", "bucket", "ak", "sk");

        assert_eq!(
            backend.object_url("snapshots/snap 1/data/a?b#c.txt"),
            "http://localhost:9000/bucket/snapshots/snap%201/data/a%3Fb%23c.txt"
        );
    }

    #[test]
    fn test_snapshot_id_validation_rejects_path_segments() {
        assert!(validate_snapshot_id("snap-2026.06.28_ok").is_ok());

        for snapshot_id in [
            "",
            ".",
            "..",
            "../escape",
            "nested/snap",
            "nested\\snap",
            "snap:1",
        ] {
            assert!(
                matches!(
                    validate_snapshot_id(snapshot_id),
                    Err(AkiDbError::InvalidParameter(_))
                ),
                "snapshot id should be rejected: {snapshot_id:?}"
            );
        }
    }

    #[test]
    fn test_snapshot_file_path_validation_rejects_reserved_metadata() {
        assert!(validate_snapshot_file_path("index.bin").is_ok());
        assert!(validate_snapshot_file_path("data/vectors.bin").is_ok());

        for path in [
            "",
            "metadata.json",
            "data/metadata.json",
            "../outside.bin",
            "data/../outside.bin",
            "/tmp/outside.bin",
        ] {
            assert!(
                matches!(
                    validate_snapshot_file_path(path),
                    Err(AkiDbError::InvalidParameter(_))
                ),
                "snapshot file path should be rejected: {path:?}"
            );
        }
    }

    #[test]
    fn test_snapshot_metadata_key_parser_requires_exact_root_metadata_object() {
        assert_eq!(
            snapshot_metadata_id_from_key("snapshots/snap-1/metadata.json"),
            Some("snap-1")
        );

        for key in [
            "snapshots/snap-1/notmetadata.json",
            "snapshots/snap-1/data/metadata.json",
            "snapshots/snap-1/metadata.json.bak",
            "snapshots//metadata.json",
            "other/snap-1/metadata.json",
        ] {
            assert_eq!(
                snapshot_metadata_id_from_key(key),
                None,
                "key should not be treated as snapshot metadata: {key:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_local_snapshot_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalSnapshotBackend::new(temp_dir.path());

        let mut metadata = SnapshotMetadata::new("test-collection");
        metadata.total_vectors = 1000;
        metadata.dimensions = 768;

        let files = vec![
            SnapshotFile {
                path: "index.bin".to_string(),
                data: vec![1, 2, 3, 4, 5],
            },
            SnapshotFile {
                path: "data/vectors.bin".to_string(),
                data: vec![6, 7, 8, 9, 10],
            },
        ];

        let snapshot_id = backend.save(&metadata, files).await.unwrap();
        assert!(backend.exists(&snapshot_id).await.unwrap());

        let (loaded_metadata, loaded_files) = backend.load(&snapshot_id).await.unwrap();
        assert_eq!(loaded_metadata.collection, "test-collection");
        assert_eq!(loaded_metadata.total_vectors, 1000);
        assert_eq!(loaded_files.len(), 2);

        // List snapshots
        let list = backend.list("test-collection").await.unwrap();
        assert_eq!(list.len(), 1);

        // Delete
        backend.delete(&snapshot_id).await.unwrap();
        assert!(!backend.exists(&snapshot_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_snapshot_rejects_metadata_file_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalSnapshotBackend::new(temp_dir.path());

        let metadata = SnapshotMetadata::new("test-collection");
        let snapshot_dir = temp_dir.path().join(&metadata.id);
        let files = vec![SnapshotFile {
            path: "metadata.json".to_string(),
            data: b"not snapshot metadata".to_vec(),
        }];

        let err = backend.save(&metadata, files).await.unwrap_err();
        assert!(matches!(err, AkiDbError::InvalidParameter(_)));
        assert!(
            !snapshot_dir.exists(),
            "invalid snapshot file should not leave a partial snapshot directory"
        );
    }

    #[tokio::test]
    async fn test_local_snapshot_rejects_traversal_snapshot_id() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalSnapshotBackend::new(temp_dir.path());

        let mut metadata = SnapshotMetadata::new("test-collection");
        metadata.id = "../escape".to_string();

        let err = backend.save(&metadata, vec![]).await.unwrap_err();
        assert!(matches!(err, AkiDbError::InvalidParameter(_)));
    }

    #[tokio::test]
    async fn test_snapshot_manager() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalSnapshotBackend::new(temp_dir.path());
        let manager = SnapshotManager::new(backend).with_max_snapshots(2);

        // Create 3 snapshots
        for i in 0..3 {
            let mut metadata = SnapshotMetadata::new("test-collection");
            metadata.total_vectors = i * 100;
            manager.create_snapshot(metadata, vec![]).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Should only have 2 snapshots (max)
        let list = manager.list_snapshots("test-collection").await.unwrap();
        assert_eq!(list.len(), 2);
    }
}
