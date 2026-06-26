//! Text Chunking
//!
//! Semantic chunking with sentence-boundary awareness.

pub mod semantic;

pub use semantic::SemanticChunker;

/// A text chunk with metadata
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Chunk text content
    pub text: String,

    /// Start character offset in original text
    pub start_offset: usize,

    /// End character offset in original text
    pub end_offset: usize,

    /// Approximate token count
    pub token_count: usize,

    /// Chunk index
    pub index: usize,
}
