//! Retrieval-quality evaluation (CHUNK-013, §5.1 release gate).
//!
//! Provides the standard IR metrics (recall@k, nDCG@k) and a *controlled*
//! benchmark that quantifies hybrid (RRF) retrieval against dense-only and
//! lexical-only retrieval. The corpus is constructed so each query has two kinds
//! of relevant documents:
//! - **semantic** matches (embedding near the query, but no shared keyword) —
//!   findable by dense search, missed by lexical;
//! - **lexical** matches (a shared rare keyword, but a far embedding) — findable
//!   by BM25, missed by dense.
//!
//! A purely dense or purely lexical retriever therefore tops out around recall
//! 0.5; fusion recovers both halves. This is a methodology demonstration on
//! synthetic-but-controlled data, not a claim about any particular real corpus —
//! it makes the quality machinery measurable and regression-gated.

use std::collections::HashSet;

use akidb_common::VectorId;

use crate::{cosine_similarity, Bm25Index, HybridFuser, ScoredId};

/// Fraction of the relevant set retrieved within the top `k`.
pub fn recall_at_k(retrieved: &[VectorId], relevant: &HashSet<VectorId>, k: usize) -> f64 {
    if relevant.is_empty() {
        return 0.0;
    }
    let hits = retrieved
        .iter()
        .take(k)
        .filter(|id| relevant.contains(id))
        .count();
    hits as f64 / relevant.len() as f64
}

/// Normalized discounted cumulative gain at `k` for binary relevance.
pub fn ndcg_at_k(retrieved: &[VectorId], relevant: &HashSet<VectorId>, k: usize) -> f64 {
    let mut dcg = 0.0;
    for (i, id) in retrieved.iter().take(k).enumerate() {
        if relevant.contains(id) {
            dcg += 1.0 / (((i + 2) as f64).log2());
        }
    }
    let ideal = relevant.len().min(k);
    let mut idcg = 0.0;
    for i in 0..ideal {
        idcg += 1.0 / (((i + 2) as f64).log2());
    }
    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
}

/// Averaged metrics for one retrieval strategy across all queries.
#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics {
    pub recall: f64,
    pub ndcg: f64,
}

/// Side-by-side results for dense, lexical, and hybrid retrieval.
#[derive(Debug, Clone, Copy)]
pub struct EvalSummary {
    pub queries: usize,
    pub k: usize,
    pub dense: Metrics,
    pub lexical: Metrics,
    pub hybrid: Metrics,
}

/// Tiny deterministic PRNG (no external dep) so results are reproducible.
struct Lcg(u64);
impl Lcg {
    fn next_unit(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // map high bits to [-1, 1)
        ((self.0 >> 40) as f32 / (1u64 << 23) as f32) - 1.0
    }
    fn vector(&mut self, dims: usize) -> Vec<f32> {
        let v: Vec<f32> = (0..dims).map(|_| self.next_unit()).collect();
        normalize(v)
    }
}

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn brute_force_dense(query: &[f32], docs: &[(VectorId, Vec<f32>)], k: usize) -> Vec<ScoredId> {
    let mut scored: Vec<ScoredId> = docs
        .iter()
        .map(|(id, emb)| ScoredId::new(id.clone(), cosine_similarity(query, emb)))
        .collect();
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.as_str().cmp(b.id.as_str()))
    });
    scored.truncate(k);
    scored
}

/// Run the controlled dense/lexical/hybrid comparison and return averaged
/// recall@k and nDCG@k for each strategy.
///
/// - `num_queries`: number of queries (each with `sem_per_q` + `lex_per_q` relevant docs)
/// - `distractors`: irrelevant filler documents shared across queries
/// - `dims`: embedding dimension
/// - `k`: cutoff for metrics
pub fn run_controlled_eval(num_queries: usize, distractors: usize, dims: usize, k: usize) -> EvalSummary {
    const SEM_PER_Q: usize = 5;
    const LEX_PER_Q: usize = 5;
    let mut rng = Lcg(0x9E3779B97F4A7C15);

    let mut docs: Vec<(VectorId, Vec<f32>)> = Vec::new(); // (id, embedding) for dense
    let mut bm25 = Bm25Index::new();

    // Shared distractors: random embeddings + generic filler text (no query terms).
    for d in 0..distractors {
        let id = VectorId::new(format!("distractor-{d}"));
        docs.push((id.clone(), rng.vector(dims)));
        bm25.insert(id, "generic filler content about various unrelated subjects");
    }

    // Per-query relevant docs + the query themselves.
    struct Q {
        embedding: Vec<f32>,
        term: String,
        relevant: HashSet<VectorId>,
    }
    let mut queries: Vec<Q> = Vec::new();
    for q in 0..num_queries {
        let q_emb = rng.vector(dims);
        let term = format!("qterm{q}");
        let mut relevant = HashSet::new();

        // Semantic relevant: embedding near the query, text without the term.
        for s in 0..SEM_PER_Q {
            let id = VectorId::new(format!("q{q}-sem-{s}"));
            // q_emb + small noise, renormalized
            let mut emb = q_emb.clone();
            for x in &mut emb {
                *x += rng.next_unit() * 0.05;
            }
            docs.push((id.clone(), normalize(emb)));
            bm25.insert(id.clone(), "semantically related passage without the keyword");
            relevant.insert(id);
        }
        // Lexical relevant: far embedding, text containing the rare term.
        for l in 0..LEX_PER_Q {
            let id = VectorId::new(format!("q{q}-lex-{l}"));
            docs.push((id.clone(), rng.vector(dims)));
            bm25.insert(id.clone(), &format!("a passage mentioning {term} explicitly"));
            relevant.insert(id);
        }
        queries.push(Q {
            embedding: q_emb,
            term,
            relevant,
        });
    }

    let pool = (k * 4).max(20);
    let fuser = HybridFuser::new();
    let (mut d_rec, mut d_ndcg) = (0.0, 0.0);
    let (mut l_rec, mut l_ndcg) = (0.0, 0.0);
    let (mut h_rec, mut h_ndcg) = (0.0, 0.0);

    for q in &queries {
        let dense = brute_force_dense(&q.embedding, &docs, pool);
        let lexical = bm25.search(&q.term, pool);

        let dense_ids: Vec<VectorId> = dense.iter().take(k).map(|s| s.id.clone()).collect();
        let lex_ids: Vec<VectorId> = lexical.iter().take(k).map(|s| s.id.clone()).collect();
        let hybrid = fuser.fuse(&dense, &lexical, k);
        let hyb_ids: Vec<VectorId> = hybrid.iter().map(|s| s.id.clone()).collect();

        d_rec += recall_at_k(&dense_ids, &q.relevant, k);
        d_ndcg += ndcg_at_k(&dense_ids, &q.relevant, k);
        l_rec += recall_at_k(&lex_ids, &q.relevant, k);
        l_ndcg += ndcg_at_k(&lex_ids, &q.relevant, k);
        h_rec += recall_at_k(&hyb_ids, &q.relevant, k);
        h_ndcg += ndcg_at_k(&hyb_ids, &q.relevant, k);
    }

    let n = queries.len() as f64;
    EvalSummary {
        queries: queries.len(),
        k,
        dense: Metrics { recall: d_rec / n, ndcg: d_ndcg / n },
        lexical: Metrics { recall: l_rec / n, ndcg: l_ndcg / n },
        hybrid: Metrics { recall: h_rec / n, ndcg: h_ndcg / n },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(list: &[&str]) -> Vec<VectorId> {
        list.iter().map(|s| VectorId::new(*s)).collect()
    }

    fn relset(list: &[&str]) -> HashSet<VectorId> {
        list.iter().map(|s| VectorId::new(*s)).collect()
    }

    #[test]
    fn test_recall_at_k() {
        let retrieved = ids(&["a", "x", "b", "y"]);
        let relevant = relset(&["a", "b", "c", "d"]);
        // 2 of 4 relevant in top-4
        assert!((recall_at_k(&retrieved, &relevant, 4) - 0.5).abs() < 1e-9);
        // only "a" in top-1
        assert!((recall_at_k(&retrieved, &relevant, 1) - 0.25).abs() < 1e-9);
        assert_eq!(recall_at_k(&retrieved, &HashSet::new(), 4), 0.0);
    }

    #[test]
    fn test_ndcg_perfect_and_zero() {
        let relevant = relset(&["a", "b"]);
        // perfect ranking
        assert!((ndcg_at_k(&ids(&["a", "b", "x"]), &relevant, 3) - 1.0).abs() < 1e-9);
        // nothing relevant retrieved
        assert_eq!(ndcg_at_k(&ids(&["x", "y"]), &relevant, 2), 0.0);
    }

    #[test]
    fn test_ndcg_rewards_higher_rank() {
        let relevant = relset(&["a"]);
        let high = ndcg_at_k(&ids(&["a", "x", "y"]), &relevant, 3);
        let low = ndcg_at_k(&ids(&["x", "y", "a"]), &relevant, 3);
        assert!(high > low);
        assert!((high - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_hybrid_beats_single_retrievers() {
        // The gate: on the controlled corpus, hybrid recovers both the semantic
        // and lexical halves, so it must outperform either retriever alone.
        let s = run_controlled_eval(/*queries*/ 15, /*distractors*/ 300, /*dims*/ 32, /*k*/ 10);
        assert!(
            s.hybrid.recall > s.dense.recall + 0.15,
            "hybrid recall {:.3} should clearly beat dense {:.3}",
            s.hybrid.recall,
            s.dense.recall
        );
        assert!(
            s.hybrid.recall > s.lexical.recall + 0.15,
            "hybrid recall {:.3} should clearly beat lexical {:.3}",
            s.hybrid.recall,
            s.lexical.recall
        );
        assert!(s.hybrid.ndcg >= s.dense.ndcg && s.hybrid.ndcg >= s.lexical.ndcg);
    }
}
