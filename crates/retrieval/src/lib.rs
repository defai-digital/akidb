//! AkiDB retrieval layer.
//!
//! This crate owns the retrieval-quality half of AkiDB: the parts that turn a raw
//! vector index into grounded context. It is deliberately independent of the
//! storage and gRPC layers so the retrieval logic can be unit-tested in isolation.
//!
//! Modules:
//! - [`lexical`]: a self-contained in-memory BM25 keyword/identifier index, the
//!   lexical half of hybrid retrieval (the dense half is the `usearch` HNSW index
//!   in `akidb-faiss`).
//! - [`fusion`]: rank fusion (Reciprocal Rank Fusion) and a two-list hybrid
//!   orchestrator that merges dense + lexical ranked results.
//! - [`packer`]: source-grounded context packing — assembles ranked passages
//!   into a token-budget-aware, citation-bearing context block for an LLM.

pub mod fusion;
pub mod lexical;
pub mod packer;
pub mod rerank;

pub use fusion::{Fusion, HybridFuser, RankedInput, Rrf};
pub use lexical::Bm25Index;
pub use packer::{pack, Citation, ContextPack, PackStrategy, PackerConfig, Passage};
pub use rerank::{
    cosine_similarity, mmr, IdentityReranker, LexicalOverlapReranker, MmrItem, RerankItem, Reranker,
};

use akidb_common::VectorId;

/// A document id paired with a retrieval score.
///
/// This is the common currency between retrieval stages (lexical, dense, fusion):
/// each stage produces a ranked list of `ScoredId`s, highest score first.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredId {
    pub id: VectorId,
    pub score: f32,
}

impl ScoredId {
    pub fn new(id: VectorId, score: f32) -> Self {
        Self { id, score }
    }
}
