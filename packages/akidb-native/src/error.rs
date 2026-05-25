/// AkiDB unified error type.
///
/// All internal errors are funneled through `AkiDbError` and converted to
/// `napi::Error` at the NAPI boundary via the `From` impl.

#[derive(thiserror::Error, Debug)]
pub enum AkiDbError {
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("manifest not found for collection: {0}")]
    ManifestNotFound(String),

    #[error("segment not found: {0}")]
    SegmentNotFound(String),

    #[error("dimension mismatch: expected {expected}, got {actual} (chunk_id: {chunk_id})")]
    DimensionMismatch {
        expected: usize,
        actual: usize,
        chunk_id: String,
    },

    #[error("storage error: {0}")]
    Storage(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("metadata error: {0}")]
    Metadata(#[from] rusqlite::Error),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(feature = "node-api")]
impl From<AkiDbError> for napi::Error {
    fn from(err: AkiDbError) -> Self {
        napi::Error::from_reason(err.to_string())
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, AkiDbError>;
