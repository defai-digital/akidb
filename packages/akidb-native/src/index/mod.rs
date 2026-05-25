//! Index module — shared search result types.

/// Per-result scoring breakdown for query explainability.
#[derive(Debug, Clone)]
pub struct ExplainInfo {
    /// Raw vector similarity score (if vector search was used).
    pub vector_score: Option<f64>,
    /// Raw BM25 score from FTS5 (if keyword search was used).
    pub bm25_score: Option<f64>,
    /// Final RRF score (if hybrid search was used).
    pub rrf_score: Option<f64>,
    /// 1-based rank in vector results (if vector search was used).
    pub vector_rank: Option<usize>,
    /// 1-based rank in keyword results (if keyword search was used).
    pub bm25_rank: Option<usize>,
    /// First 200 chars of chunk text (if stored in FTS5).
    pub chunk_preview: Option<String>,
    /// FTS5 matched terms in the query.
    pub matched_terms: Vec<String>,
}

/// A search result from an index search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub chunk_id: String,
    pub score: f64,
    pub committed: Option<bool>,
    pub explain: Option<ExplainInfo>,
}
