//! Error types for AkiDB

use thiserror::Error;

/// Result type alias for AkiDB operations
pub type Result<T> = std::result::Result<T, AkiDbError>;

/// Main error type for AkiDB
#[derive(Error, Debug)]
pub enum AkiDbError {
    // Vector operations
    #[error("Vector not found: {0}")]
    VectorNotFound(String),

    #[error("Vector already exists: {0}")]
    VectorAlreadyExists(String),

    #[error("Vector already deleted: {0}")]
    VectorAlreadyDeleted(String),

    #[error("Invalid vector dimensions: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("Vector ID cannot be reused after deletion: {0}")]
    IdReuseForbidden(String),

    // Index operations
    #[error("Index error: {0}")]
    IndexError(String),

    #[error("Index not ready")]
    IndexNotReady,

    #[error("Index rebuild in progress")]
    RebuildInProgress,

    // Storage operations
    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    // GPU operations
    #[error("GPU error: {0}")]
    GpuError(String),

    #[error("GPU out of memory")]
    GpuOutOfMemory,

    #[error("GPU unavailable, falling back to CPU")]
    GpuUnavailable,

    // Network operations
    #[error("Shard unavailable: {0}")]
    ShardUnavailable(String),

    #[error("Coordinator error: {0}")]
    CoordinatorError(String),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    // Configuration
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    // Internal
    #[error("Internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl AkiDbError {
    /// Returns true if this error indicates a retriable condition
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            AkiDbError::ShardUnavailable(_)
                | AkiDbError::Timeout(_)
                | AkiDbError::GpuUnavailable
                | AkiDbError::RebuildInProgress
        )
    }

    /// Returns true if this is a "not found" style error (success in idempotent ops)
    pub fn is_not_found(&self) -> bool {
        matches!(self, AkiDbError::VectorNotFound(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_retriable() {
        assert!(AkiDbError::ShardUnavailable("shard-1".into()).is_retriable());
        assert!(AkiDbError::Timeout(5000).is_retriable());
        assert!(!AkiDbError::VectorNotFound("vec-1".into()).is_retriable());
    }
}
