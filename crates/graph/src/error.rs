//! Error types for graph retrieval.

use thiserror::Error;

pub type GraphResult<T> = std::result::Result<T, GraphError>;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("graph record not found: {0}")]
    NotFound(String),

    #[error("invalid graph request: {0}")]
    InvalidRequest(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<akidb_common::AkiDbError> for GraphError {
    fn from(value: akidb_common::AkiDbError) -> Self {
        GraphError::Storage(value.to_string())
    }
}

impl From<serde_json::Error> for GraphError {
    fn from(value: serde_json::Error) -> Self {
        GraphError::Serialization(value.to_string())
    }
}
