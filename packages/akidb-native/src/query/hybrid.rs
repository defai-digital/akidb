//! Hybrid search module — Reciprocal Rank Fusion (RRF).
//!
//! Combines vector (ANN) search results with BM25 keyword search results
//! using the RRF formula: `score(d) = Σ 1 / (k + rank_i(d))`
//! where `k` is a constant (default 60) and `rank_i(d)` is the 1-based
//! rank of document `d` in result list `i`.

use std::collections::HashMap;

use crate::index::SearchResult;

/// Default RRF constant `k`. Higher values give less weight to top ranks.
const RRF_K: f64 = 60.0;

/// Fuse two ranked result lists using Reciprocal Rank Fusion.
///
/// Both `vector_results` and `keyword_results` should already be sorted
/// by descending score. The output is sorted by descending RRF score
/// with deterministic tie-breaking on chunk_id.
pub fn rrf_fuse(
    vector_results: &[SearchResult],
    keyword_results: &[SearchResult],
    top_k: usize,
    vector_weight: f64,
    keyword_weight: f64,
) -> Vec<SearchResult> {
    let mut scores: HashMap<String, f64> = HashMap::new();

    // Vector results contribution.
    for (rank_0, r) in vector_results.iter().enumerate() {
        let rrf_score = vector_weight / (RRF_K + (rank_0 + 1) as f64);
        *scores.entry(r.chunk_id.clone()).or_insert(0.0) += rrf_score;
    }

    // Keyword results contribution.
    for (rank_0, r) in keyword_results.iter().enumerate() {
        let rrf_score = keyword_weight / (RRF_K + (rank_0 + 1) as f64);
        *scores.entry(r.chunk_id.clone()).or_insert(0.0) += rrf_score;
    }

    // Build committed map from both sources (prefer vector's committed value).
    let mut committed_map: HashMap<String, Option<bool>> = HashMap::new();
    for r in keyword_results {
        committed_map.insert(r.chunk_id.clone(), r.committed);
    }
    for r in vector_results {
        committed_map.insert(r.chunk_id.clone(), r.committed);
    }

    let mut fused: Vec<SearchResult> = scores
        .into_iter()
        .map(|(chunk_id, score)| SearchResult {
            committed: committed_map.get(&chunk_id).copied().flatten(),
            chunk_id,
            score,
            explain: None,
        })
        .collect();

    // Sort: descending score, then ascending chunk_id for determinism.
    fused.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.chunk_id.cmp(&b.chunk_id))
    });

    fused.truncate(top_k);
    fused
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(id: &str, score: f64) -> SearchResult {
        SearchResult {
            chunk_id: id.to_string(),
            score,
            committed: Some(true),
            explain: None,
        }
    }

    #[test]
    fn rrf_fuse_basic() {
        let vector = vec![
            make_result("a", 0.95),
            make_result("b", 0.85),
            make_result("c", 0.75),
        ];
        let keyword = vec![
            make_result("b", 0.90),
            make_result("d", 0.80),
            make_result("a", 0.70),
        ];

        let fused = rrf_fuse(&vector, &keyword, 10, 1.0, 1.0);

        // "a" appears in both lists (rank 1 vector, rank 3 keyword).
        // "b" appears in both lists (rank 2 vector, rank 1 keyword).
        // Both should have higher scores than single-list items.
        let a_score = fused.iter().find(|r| r.chunk_id == "a").unwrap().score;
        let b_score = fused.iter().find(|r| r.chunk_id == "b").unwrap().score;
        let c_score = fused.iter().find(|r| r.chunk_id == "c").unwrap().score;
        let d_score = fused.iter().find(|r| r.chunk_id == "d").unwrap().score;

        // Items in both lists should score higher than single-list items.
        assert!(a_score > c_score);
        assert!(b_score > d_score);
    }

    #[test]
    fn rrf_fuse_respects_top_k() {
        let vector = vec![
            make_result("a", 0.9),
            make_result("b", 0.8),
            make_result("c", 0.7),
        ];
        let keyword = vec![
            make_result("d", 0.9),
            make_result("e", 0.8),
        ];

        let fused = rrf_fuse(&vector, &keyword, 2, 1.0, 1.0);
        assert_eq!(fused.len(), 2);
    }

    #[test]
    fn rrf_fuse_empty_inputs() {
        let empty: Vec<SearchResult> = Vec::new();
        let vector = vec![make_result("a", 0.9)];

        // One empty, one populated.
        let fused = rrf_fuse(&vector, &empty, 10, 1.0, 1.0);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].chunk_id, "a");

        // Both empty.
        let fused2 = rrf_fuse(&empty, &empty, 10, 1.0, 1.0);
        assert!(fused2.is_empty());
    }

    #[test]
    fn rrf_fuse_weights() {
        // When vector_weight is 0, only keyword results matter.
        let vector = vec![make_result("a", 0.9)];
        let keyword = vec![make_result("b", 0.9)];

        let fused = rrf_fuse(&vector, &keyword, 10, 0.0, 1.0);
        let a_score = fused.iter().find(|r| r.chunk_id == "a").map(|r| r.score).unwrap_or(0.0);
        let b_score = fused.iter().find(|r| r.chunk_id == "b").unwrap().score;
        assert!(b_score > a_score);

        // When keyword_weight is 0, only vector results matter.
        let fused2 = rrf_fuse(&vector, &keyword, 10, 1.0, 0.0);
        let a_score2 = fused2.iter().find(|r| r.chunk_id == "a").unwrap().score;
        let b_score2 = fused2.iter().find(|r| r.chunk_id == "b").map(|r| r.score).unwrap_or(0.0);
        assert!(a_score2 > b_score2);
    }

    #[test]
    fn rrf_fuse_deterministic_tie_breaking() {
        // Two items with identical RRF scores should be ordered by chunk_id.
        let vector = vec![make_result("b", 0.9), make_result("a", 0.8)];
        let keyword = vec![make_result("a", 0.9), make_result("b", 0.8)];

        let fused = rrf_fuse(&vector, &keyword, 10, 1.0, 1.0);
        // "a" and "b" each appear at rank 1 in one list and rank 2 in the other.
        // Their RRF scores should be identical, so "a" < "b" in chunk_id order.
        assert_eq!(fused[0].chunk_id, "a");
        assert_eq!(fused[1].chunk_id, "b");
    }
}
