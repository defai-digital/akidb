//! CPU-based FAISS implementation (placeholder)
//!
//! This module will contain the CPU fallback implementation of the vector index.
//! For now, it re-exports the mock implementation for development.

// CPU implementation will be added when integrating with faiss-rs
// For development, use the mock implementation

pub use crate::mock::MockIndex as CpuIndex;
