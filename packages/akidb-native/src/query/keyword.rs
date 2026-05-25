//! Keyword search module — BM25 search via FTS5.
//!
//! Wraps MetadataStore::fts_search to provide a consistent SearchResult
//! interface that can be used standalone or combined with vector search
//! in the hybrid module.

use std::collections::HashSet;

use crate::error::Result;
use crate::index::SearchResult;
use crate::metadata::MetadataStore;

/// Run a BM25 keyword search against the FTS5 index.
///
/// Returns `SearchResult` items with scores normalized to [0, 1] range.
/// FTS5 bm25() returns negative values where more-negative = more relevant,
/// so we negate and normalize.
/// Sanitize user input for FTS5 MATCH syntax.
///
/// FTS5 has its own query language (`AND`, `OR`, `NOT`, `NEAR`, `*`, `"`, `-`).
/// Raw user text containing these operators would cause parse errors or alter
/// search semantics. We quote each whitespace-delimited token so FTS5 treats
/// them as literal terms.
fn sanitize_fts5_query(text: &str) -> String {
    text.split_whitespace()
        .filter(|t| !t.is_empty())
        .map(|t| format!("\"{}\"", t.replace('"', "")))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn keyword_search(
    metadata: &MetadataStore,
    collection_id: &str,
    query_text: &str,
    top_k: usize,
    tombstone_set: &HashSet<String>,
) -> Result<Vec<SearchResult>> {
    let safe_query = sanitize_fts5_query(query_text);
    if safe_query.is_empty() {
        return Ok(Vec::new());
    }

    // Over-fetch to account for tombstoned results we'll filter out.
    let fetch_k = if tombstone_set.is_empty() {
        top_k
    } else {
        top_k * 2
    };

    // Try unicode61 BM25 search first (best for space-delimited languages).
    let raw_results = metadata.fts_search(collection_id, &safe_query, fetch_k)?;

    // Fall back to trigram search if unicode61 returned nothing.
    // This handles CJK and other scripts without whitespace word boundaries.
    let raw_results = if raw_results.is_empty() {
        metadata
            .fts_trigram_search(collection_id, query_text, fetch_k)
            .unwrap_or_default()
    } else {
        raw_results
    };

    if raw_results.is_empty() {
        return Ok(Vec::new());
    }

    // Filter tombstones first, then normalize — so the min/max range is computed
    // only from surviving results and scores are not deflated when high-scoring
    // entries are tombstoned.
    let active: Vec<(String, f64)> = raw_results
        .into_iter()
        .filter(|(id, _)| !tombstone_set.contains(id))
        .map(|(id, score)| (id, -score))
        .collect();

    if active.is_empty() {
        return Ok(Vec::new());
    }

    let max_score = active
        .iter()
        .map(|(_, s)| *s)
        .fold(f64::NEG_INFINITY, f64::max);
    let min_score = active
        .iter()
        .map(|(_, s)| *s)
        .fold(f64::INFINITY, f64::min);

    let range = max_score - min_score;

    let mut results: Vec<SearchResult> = active
        .into_iter()
        .map(|(chunk_id, score)| {
            let normalized = if range > 0.0 {
                (score - min_score) / range
            } else {
                1.0
            };
            SearchResult {
                chunk_id,
                score: normalized,
                committed: Some(true),
                explain: None,
            }
        })
        .collect();

    results.truncate(top_k);
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> MetadataStore {
        MetadataStore::open(":memory:").unwrap()
    }

    fn setup_fts(store: &MetadataStore) {
        use crate::metadata::Collection;
        store
            .create_collection(&Collection {
                collection_id: "coll-1".to_string(),
                dimension: 4,
                metric: "cosine".to_string(),
                embedding_model_id: "test".to_string(),
                schema_version: "1".to_string(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                deleted_at: None,
                quantization: "fp16".to_string(),
                hnsw_m: 16,
                hnsw_ef_construction: 200,
                hnsw_ef_search: 100,
            })
            .unwrap();

        store.fts_insert("c-1", "coll-1", "The quick brown fox jumps over the lazy dog").unwrap();
        store.fts_insert("c-2", "coll-1", "Machine learning and artificial intelligence").unwrap();
        store.fts_insert("c-3", "coll-1", "The brown fox was very quick today").unwrap();
        store.fts_insert("c-4", "coll-1", "Deep learning neural networks").unwrap();
    }

    #[test]
    fn keyword_search_basic() {
        let store = test_store();
        setup_fts(&store);

        let results = keyword_search(&store, "coll-1", "quick fox", 10, &HashSet::new()).unwrap();
        assert!(!results.is_empty());

        let ids: Vec<&str> = results.iter().map(|r| r.chunk_id.as_str()).collect();
        assert!(ids.contains(&"c-1"));
        assert!(ids.contains(&"c-3"));
    }

    #[test]
    fn keyword_search_respects_top_k() {
        let store = test_store();
        setup_fts(&store);

        let results = keyword_search(&store, "coll-1", "quick fox", 1, &HashSet::new()).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn keyword_search_filters_tombstones() {
        let store = test_store();
        setup_fts(&store);

        let tombstones: HashSet<String> = ["c-1".to_string()].into_iter().collect();
        let results = keyword_search(&store, "coll-1", "quick fox", 10, &tombstones).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r.chunk_id.as_str()).collect();
        assert!(!ids.contains(&"c-1"));
        assert!(ids.contains(&"c-3"));
    }

    #[test]
    fn keyword_search_empty_query() {
        let store = test_store();
        setup_fts(&store);

        let results = keyword_search(&store, "coll-1", "", 10, &HashSet::new()).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn keyword_search_no_matches() {
        let store = test_store();
        setup_fts(&store);

        let results = keyword_search(&store, "coll-1", "xyzzyplugh", 10, &HashSet::new()).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn keyword_search_scores_normalized() {
        let store = test_store();
        setup_fts(&store);

        let results = keyword_search(&store, "coll-1", "quick fox", 10, &HashSet::new()).unwrap();
        for r in &results {
            assert!(r.score >= 0.0 && r.score <= 1.0, "Score {} out of [0,1] range", r.score);
        }
    }
}
