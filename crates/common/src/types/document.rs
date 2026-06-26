//! Document identifier types for tracking and lifecycle management.
//!
//! Each document in AkiDB is uniquely identified by a composite key that enables:
//! - Deduplication via content hash
//! - Categorical grouping via category_uid
//! - Source lineage via source_path
//! - Time-ordered indexing via instance_id (UUIDv7)

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::tag::Tags;

/// Unique identifier for a document in the ingestion system.
///
/// # Components
///
/// - `content_hash`: SHA-256 of document content for deduplication
/// - `category_uid`: Optional user-defined category for grouping
/// - `source_path`: MinIO object key for source lineage
/// - `instance_id`: UUIDv7 for time-ordered unique identification
/// - `tags`: Optional key-value metadata for filtering and access control
///
/// # Example
///
/// ```
/// use akidb_common::types::document::DocumentIdentifier;
/// use akidb_common::types::tag::{Tags, TagValue};
///
/// let doc = DocumentIdentifier::new(b"document content", "bucket/path/file.pdf".to_string())
///     .with_category("legal-docs/contracts")
///     .with_tag("access:level", TagValue::Number(3.0));
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentIdentifier {
    /// SHA-256 hash of the document content
    pub content_hash: [u8; 32],

    /// User-provided hierarchical categorization (optional)
    /// Example: "legal-docs/contracts", "hr/policies/2024"
    pub category_uid: Option<String>,

    /// MinIO object key for lineage tracking
    pub source_path: String,

    /// Time-ordered unique ID (UUIDv7)
    /// Enables efficient RocksDB range scans
    pub instance_id: Uuid,

    /// Optional typed key-value tags for filtering and access control
    #[serde(default, skip_serializing_if = "Tags::is_empty")]
    pub tags: Tags,
}

impl DocumentIdentifier {
    /// Create a new document identifier from content and source path.
    ///
    /// Automatically computes the content hash and generates a UUIDv7 instance ID.
    pub fn new(content: &[u8], source_path: String) -> Self {
        let content_hash = Self::compute_hash(content);

        Self {
            content_hash,
            category_uid: None,
            source_path,
            instance_id: Uuid::now_v7(),
            tags: Tags::default(),
        }
    }

    /// Create a document identifier with a pre-computed content hash.
    pub fn with_hash(content_hash: [u8; 32], source_path: String) -> Self {
        Self {
            content_hash,
            category_uid: None,
            source_path,
            instance_id: Uuid::now_v7(),
            tags: Tags::default(),
        }
    }

    /// Set the category UID (builder pattern)
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category_uid = Some(category.into());
        self
    }

    /// Set the tags collection (builder pattern)
    pub fn with_tags(mut self, tags: Tags) -> Self {
        self.tags = tags;
        self
    }

    /// Add a single tag (builder pattern)
    pub fn with_tag(mut self, key: impl Into<String>, value: super::tag::TagValue) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Compute SHA-256 hash of content
    pub fn compute_hash(content: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(content);
        hasher.finalize().into()
    }

    /// Get content hash as hex string
    pub fn content_hash_hex(&self) -> String {
        hex_encode(&self.content_hash)
    }

    /// Check if this document has the same content as another (by hash)
    pub fn is_duplicate_of(&self, other: &DocumentIdentifier) -> bool {
        self.content_hash == other.content_hash
    }

    /// Get the category hierarchy as path components
    pub fn category_path(&self) -> Option<Vec<&str>> {
        self.category_uid.as_ref().map(|c| c.split('/').collect())
    }
}

impl PartialEq for DocumentIdentifier {
    fn eq(&self, other: &Self) -> bool {
        // Two document identifiers are equal if they have the same instance_id
        self.instance_id == other.instance_id
    }
}

impl Eq for DocumentIdentifier {}

impl std::hash::Hash for DocumentIdentifier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.instance_id.hash(state);
    }
}

/// Encode bytes as lowercase hex string
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Metadata associated with a vector in the index.
///
/// This extends DocumentIdentifier with version and tombstone information
/// for lifecycle management.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorMetadata {
    /// Document identifier
    pub doc_id: DocumentIdentifier,

    /// Monotonically increasing version number
    /// Used for version-based reindexing
    pub version: u64,

    /// Tombstone flag for soft delete
    pub tombstone: bool,

    /// When the vector was ingested
    pub ingested_at: chrono::DateTime<chrono::Utc>,
}

impl VectorMetadata {
    /// Create new metadata for a freshly ingested vector
    pub fn new(doc_id: DocumentIdentifier) -> Self {
        Self {
            doc_id,
            version: 1,
            tombstone: false,
            ingested_at: chrono::Utc::now(),
        }
    }

    /// Create metadata with a specific version
    pub fn with_version(doc_id: DocumentIdentifier, version: u64) -> Self {
        Self {
            doc_id,
            version,
            tombstone: false,
            ingested_at: chrono::Utc::now(),
        }
    }

    /// Mark this vector as tombstoned
    pub fn tombstone(&mut self) {
        self.tombstone = true;
    }

    /// Check if this is the latest version (not tombstoned)
    pub fn is_active(&self) -> bool {
        !self.tombstone
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::tag::TagValue;

    #[test]
    fn test_document_identifier_creation() {
        let content = b"Hello, world!";
        let doc = DocumentIdentifier::new(content, "bucket/test.txt".to_string());

        // Verify hash is computed
        assert_eq!(doc.content_hash.len(), 32);
        assert_ne!(doc.content_hash, [0u8; 32]);

        // Verify instance_id is set
        assert!(!doc.instance_id.is_nil());

        // Verify source path
        assert_eq!(doc.source_path, "bucket/test.txt");
    }

    #[test]
    fn test_content_hash_consistency() {
        let content = b"Same content";

        let doc1 = DocumentIdentifier::new(content, "path1".to_string());
        let doc2 = DocumentIdentifier::new(content, "path2".to_string());

        // Same content should produce same hash
        assert_eq!(doc1.content_hash, doc2.content_hash);

        // But different instance_ids
        assert_ne!(doc1.instance_id, doc2.instance_id);

        // Duplicate detection should work
        assert!(doc1.is_duplicate_of(&doc2));
    }

    #[test]
    fn test_builder_pattern() {
        let doc = DocumentIdentifier::new(b"content", "path".to_string())
            .with_category("legal/contracts")
            .with_tag("access:level", TagValue::Number(3.0))
            .with_tag("ml:reviewed", TagValue::Boolean(true));

        assert_eq!(doc.category_uid, Some("legal/contracts".to_string()));
        assert_eq!(doc.tags.len(), 2);
        assert_eq!(doc.tags.get("access:level"), Some(&TagValue::Number(3.0)));
    }

    #[test]
    fn test_category_path() {
        let doc = DocumentIdentifier::new(b"content", "path".to_string())
            .with_category("legal/contracts/2024");

        let path = doc.category_path().unwrap();
        assert_eq!(path, vec!["legal", "contracts", "2024"]);
    }

    #[test]
    fn test_hex_encoding() {
        let doc = DocumentIdentifier::new(b"test", "path".to_string());
        let hex = doc.content_hash_hex();

        // SHA-256 produces 32 bytes = 64 hex chars
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_vector_metadata() {
        let doc = DocumentIdentifier::new(b"content", "path".to_string());
        let mut meta = VectorMetadata::new(doc);

        assert_eq!(meta.version, 1);
        assert!(!meta.tombstone);
        assert!(meta.is_active());

        meta.tombstone();
        assert!(meta.tombstone);
        assert!(!meta.is_active());
    }

    #[test]
    fn test_vector_metadata_versioning() {
        let doc = DocumentIdentifier::new(b"content", "path".to_string());
        let meta = VectorMetadata::with_version(doc, 5);

        assert_eq!(meta.version, 5);
    }
}
