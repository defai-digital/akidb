//! Reranking and diversity for retrieval results.
//!
//! Two post-fusion quality stages:
//! - [`mmr`]: Maximal Marginal Relevance reselection (RET-006) — trades off
//!   relevance against novelty so near-duplicate chunks don't dominate the top-k.
//! - [`Reranker`]: a hook (RET-005) for re-scoring candidates. A local model can
//!   implement it; [`LexicalOverlapReranker`] is a dependency-free default and
//!   [`IdentityReranker`] is a passthrough.

use akidb_common::VectorId;

use crate::lexical::tokenize;
use crate::ScoredId;

fn finite_score(score: f32) -> f32 {
    if score.is_finite() {
        score
    } else {
        0.0
    }
}

/// Cosine similarity of two equal-length vectors. Returns `0.0` if lengths
/// differ, either vector has zero magnitude, or any value is non-finite.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        if !x.is_finite() || !y.is_finite() {
            return 0.0;
        }
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    finite_score(dot / (na.sqrt() * nb.sqrt()))
}

/// A candidate for MMR reselection: an id, its base relevance, and its embedding.
#[derive(Debug, Clone)]
pub struct MmrItem {
    pub id: VectorId,
    pub relevance: f32,
    pub embedding: Vec<f32>,
}

impl MmrItem {
    pub fn new(id: VectorId, relevance: f32, embedding: Vec<f32>) -> Self {
        Self {
            id,
            relevance,
            embedding,
        }
    }
}

/// Maximal Marginal Relevance reselection.
///
/// `lambda` in `[0, 1]` trades relevance vs. diversity: `1.0` is pure relevance
/// (orders by score), `0.0` maximizes novelty. The first pick is always the most
/// relevant item; each subsequent pick maximizes
/// `lambda * relevance - (1 - lambda) * max_sim_to_already_selected`.
/// Ties break by ascending id for determinism. Returns up to `top_k` items, each
/// scored with its MMR objective value.
pub fn mmr(items: &[MmrItem], lambda: f32, top_k: usize) -> Vec<ScoredId> {
    if items.is_empty() || top_k == 0 {
        return Vec::new();
    }
    let lambda = if lambda.is_finite() {
        lambda.clamp(0.0, 1.0)
    } else {
        0.5
    };

    let mut remaining: Vec<&MmrItem> = items.iter().collect();
    let mut selected: Vec<&MmrItem> = Vec::new();
    let mut out: Vec<ScoredId> = Vec::new();

    // First pick: highest relevance (ties by id).
    let first_idx = (0..remaining.len())
        .max_by(|&i, &j| {
            remaining[i]
                .relevance
                .is_finite()
                .cmp(&remaining[j].relevance.is_finite())
                .then_with(|| {
                    finite_score(remaining[i].relevance)
                        .partial_cmp(&finite_score(remaining[j].relevance))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| remaining[j].id.as_str().cmp(remaining[i].id.as_str()))
        })
        .unwrap();
    let first = remaining.remove(first_idx);
    out.push(ScoredId::new(first.id.clone(), finite_score(first.relevance)));
    selected.push(first);

    while out.len() < top_k && !remaining.is_empty() {
        let mut best_idx = 0usize;
        let mut best_score = f32::NEG_INFINITY;
        for (i, cand) in remaining.iter().enumerate() {
            let max_sim = selected
                .iter()
                .map(|s| cosine_similarity(&cand.embedding, &s.embedding))
                .fold(f32::NEG_INFINITY, f32::max);
            let score = lambda * finite_score(cand.relevance) - (1.0 - lambda) * max_sim;
            let better = score > best_score
                || (score == best_score
                    && cand.id.as_str() < remaining[best_idx].id.as_str());
            if better {
                best_score = score;
                best_idx = i;
            }
        }
        let chosen = remaining.remove(best_idx);
        out.push(ScoredId::new(chosen.id.clone(), best_score));
        selected.push(chosen);
    }

    out
}

/// A candidate passed to a [`Reranker`].
#[derive(Debug, Clone)]
pub struct RerankItem {
    pub id: VectorId,
    pub text: String,
    pub score: f32,
}

impl RerankItem {
    pub fn new(id: VectorId, text: impl Into<String>, score: f32) -> Self {
        Self {
            id,
            text: text.into(),
            score,
        }
    }
}

/// A hook for re-scoring retrieval candidates against the query. Implementations
/// return candidates ordered best-first.
pub trait Reranker {
    fn rerank(&self, query: &str, items: Vec<RerankItem>) -> Vec<ScoredId>;
}

/// Passthrough reranker: preserves input order and scores.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityReranker;

impl Reranker for IdentityReranker {
    fn rerank(&self, _query: &str, items: Vec<RerankItem>) -> Vec<ScoredId> {
        items
            .into_iter()
            .map(|i| ScoredId::new(i.id, finite_score(i.score)))
            .collect()
    }
}

/// Dependency-free reranker scoring each candidate by the fraction of distinct
/// query terms present in its text. A usable local default until a model-based
/// reranker is wired in. Ties break by ascending id.
#[derive(Debug, Clone, Copy, Default)]
pub struct LexicalOverlapReranker;

impl Reranker for LexicalOverlapReranker {
    fn rerank(&self, query: &str, items: Vec<RerankItem>) -> Vec<ScoredId> {
        let query_terms: Vec<String> = {
            let mut t = tokenize(query);
            t.sort_unstable();
            t.dedup();
            t
        };

        let mut scored: Vec<ScoredId> = items
            .into_iter()
            .map(|item| {
                let score = if query_terms.is_empty() {
                    0.0
                } else {
                    let doc_terms = tokenize(&item.text);
                    let hits = query_terms
                        .iter()
                        .filter(|qt| doc_terms.iter().any(|dt| dt == *qt))
                        .count();
                    hits as f32 / query_terms.len() as f32
                };
                ScoredId::new(item.id, score)
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        scored
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> VectorId {
        VectorId::new(s)
    }

    #[test]
    fn test_cosine_similarity() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
        assert!((cosine_similarity(&[1.0, 1.0], &[2.0, 2.0]) - 1.0).abs() < 1e-6);
        // zero vector and length mismatch are 0
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), 0.0);
    }

    #[test]
    fn test_cosine_similarity_rejects_non_finite_values() {
        assert_eq!(cosine_similarity(&[f32::NAN, 1.0], &[1.0, 0.0]), 0.0);
        assert_eq!(cosine_similarity(&[f32::INFINITY], &[f32::INFINITY]), 0.0);
    }

    #[test]
    fn test_mmr_empty_and_zero_topk() {
        assert!(mmr(&[], 0.5, 5).is_empty());
        let items = [MmrItem::new(id("a"), 1.0, vec![1.0, 0.0])];
        assert!(mmr(&items, 0.5, 0).is_empty());
    }

    #[test]
    fn test_mmr_lambda_one_is_pure_relevance() {
        let items = [
            MmrItem::new(id("low"), 0.2, vec![1.0, 0.0]),
            MmrItem::new(id("high"), 0.9, vec![1.0, 0.0]),
            MmrItem::new(id("mid"), 0.5, vec![0.0, 1.0]),
        ];
        let out = mmr(&items, 1.0, 3);
        let order: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order, vec!["high", "mid", "low"]);
    }

    #[test]
    fn test_mmr_does_not_promote_non_finite_relevance() {
        let items = [
            MmrItem::new(id("bad"), f32::NAN, vec![1.0, 0.0]),
            MmrItem::new(id("good"), 0.5, vec![0.0, 1.0]),
        ];

        let out = mmr(&items, 1.0, 2);

        assert_eq!(out[0].id.as_str(), "good");
        assert!(out.iter().all(|item| item.score.is_finite()));
    }

    #[test]
    fn test_mmr_diversity_demotes_near_duplicate() {
        // a and a_dup are near-identical embeddings and both highly relevant;
        // b is slightly less relevant but orthogonal. Pure relevance => a, a_dup,
        // b. With diversity, b should be promoted above the duplicate.
        let items = [
            MmrItem::new(id("a"), 0.90, vec![1.0, 0.0]),
            MmrItem::new(id("a_dup"), 0.85, vec![0.99, 0.01]),
            MmrItem::new(id("b"), 0.80, vec![0.0, 1.0]),
        ];

        let relevance_order: Vec<String> = mmr(&items, 1.0, 3)
            .iter()
            .map(|s| s.id.to_string())
            .collect();
        assert_eq!(relevance_order, vec!["a", "a_dup", "b"]);

        let diverse = mmr(&items, 0.5, 3);
        let order: Vec<&str> = diverse.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order[0], "a", "most relevant still first");
        assert_eq!(order[1], "b", "diverse item promoted over near-duplicate");
        assert_eq!(order[2], "a_dup");
    }

    #[test]
    fn test_mmr_respects_top_k() {
        let items = [
            MmrItem::new(id("a"), 0.9, vec![1.0, 0.0]),
            MmrItem::new(id("b"), 0.8, vec![0.0, 1.0]),
            MmrItem::new(id("c"), 0.7, vec![1.0, 1.0]),
        ];
        assert_eq!(mmr(&items, 0.5, 2).len(), 2);
    }

    #[test]
    fn test_mmr_non_finite_lambda_uses_default() {
        let items = [
            MmrItem::new(id("a"), 0.9, vec![1.0, 0.0]),
            MmrItem::new(id("b"), 0.8, vec![0.0, 1.0]),
        ];

        let out = mmr(&items, f32::NAN, 2);

        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|item| item.score.is_finite()));
    }

    #[test]
    fn test_identity_reranker_preserves_order_and_scores() {
        let items = vec![
            RerankItem::new(id("a"), "x", 0.9),
            RerankItem::new(id("b"), "y", 0.5),
        ];
        let out = IdentityReranker.rerank("q", items);
        assert_eq!(out[0], ScoredId::new(id("a"), 0.9));
        assert_eq!(out[1], ScoredId::new(id("b"), 0.5));
    }

    #[test]
    fn test_identity_reranker_sanitizes_non_finite_scores() {
        let items = vec![RerankItem::new(id("bad"), "x", f32::NAN)];
        let out = IdentityReranker.rerank("q", items);

        assert_eq!(out[0], ScoredId::new(id("bad"), 0.0));
    }

    #[test]
    fn test_lexical_overlap_reranker_ranks_by_query_term_hits() {
        let items = vec![
            RerankItem::new(id("none"), "totally unrelated content", 0.99),
            RerankItem::new(id("both"), "token refresh failure here", 0.10),
            RerankItem::new(id("one"), "token only", 0.50),
        ];
        // Query has two distinct terms: "token", "refresh".
        let out = LexicalOverlapReranker.rerank("token refresh", items);
        let order: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order, vec!["both", "one", "none"]);
        assert!((out[0].score - 1.0).abs() < 1e-6); // both terms
        assert!((out[1].score - 0.5).abs() < 1e-6); // one of two
        assert_eq!(out[2].score, 0.0); // none
    }

    #[test]
    fn test_lexical_overlap_empty_query_scores_zero() {
        let items = vec![RerankItem::new(id("a"), "anything", 0.5)];
        let out = LexicalOverlapReranker.rerank("", items);
        assert_eq!(out[0].score, 0.0);
    }
}
