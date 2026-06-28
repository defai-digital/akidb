//! Rank fusion for hybrid retrieval.
//!
//! Dense (cosine / HNSW) and lexical (BM25) scores live on incompatible scales,
//! so they cannot be added directly. Reciprocal Rank Fusion (RRF) sidesteps this
//! by combining *ranks* rather than raw scores: each result contributes
//! `weight / (k + rank)` from every list it appears in. This is robust, has a
//! single intuitive parameter (`k`), and is the PRD's default fusion ranker.

use akidb_common::VectorId;

use crate::ScoredId;

/// Default RRF rank constant. Larger `k` flattens the contribution of top ranks;
/// 60 is the value from the original Cormack et al. RRF paper and the common
/// default across search engines.
pub const DEFAULT_RRF_K: f32 = 60.0;

fn normalize_rrf_k(k: f32) -> f32 {
    if k.is_finite() && k > 0.0 {
        k
    } else {
        DEFAULT_RRF_K
    }
}

fn normalize_weight(weight: f32) -> Option<f64> {
    if weight.is_finite() && weight > 0.0 {
        Some(weight as f64)
    } else {
        None
    }
}

fn finite_f32(score: f64) -> f32 {
    if !score.is_finite() {
        0.0
    } else if score > f32::MAX as f64 {
        f32::MAX
    } else {
        score as f32
    }
}

/// One ranked input to fusion: a weight and a list of ids ordered best-first.
///
/// The list carries only ids, not scores, because RRF deliberately ignores the
/// original score magnitudes and uses position alone.
#[derive(Debug, Clone, Copy)]
pub struct RankedInput<'a> {
    pub weight: f32,
    pub ids: &'a [VectorId],
}

impl<'a> RankedInput<'a> {
    pub fn new(weight: f32, ids: &'a [VectorId]) -> Self {
        Self { weight, ids }
    }
}

/// A fusion strategy that merges several ranked lists into one.
pub trait Fusion {
    /// Merge `lists` and return the top `top_k` fused results, highest score
    /// first. Implementations must produce a deterministic order.
    fn fuse(&self, lists: &[RankedInput<'_>], top_k: usize) -> Vec<ScoredId>;
}

/// Reciprocal Rank Fusion.
#[derive(Debug, Clone, Copy)]
pub struct Rrf {
    k: f32,
}

impl Default for Rrf {
    fn default() -> Self {
        Self::new()
    }
}

impl Rrf {
    /// RRF with the default rank constant ([`DEFAULT_RRF_K`]).
    pub fn new() -> Self {
        Self { k: DEFAULT_RRF_K }
    }

    /// RRF with an explicit rank constant.
    pub fn with_k(k: f32) -> Self {
        Self {
            k: normalize_rrf_k(k),
        }
    }

    /// The configured rank constant.
    pub fn k(&self) -> f32 {
        self.k
    }
}

impl Fusion for Rrf {
    fn fuse(&self, lists: &[RankedInput<'_>], top_k: usize) -> Vec<ScoredId> {
        if top_k == 0 {
            return Vec::new();
        }

        let k = self.k as f64;
        // Preserve first-seen order so ties break deterministically and
        // independently of HashMap iteration order.
        let mut order: Vec<VectorId> = Vec::new();
        let mut scores: std::collections::HashMap<VectorId, f64> = std::collections::HashMap::new();

        for list in lists {
            let Some(w) = normalize_weight(list.weight) else {
                continue;
            };
            for (pos, id) in list.ids.iter().enumerate() {
                let rank = (pos + 1) as f64; // 1-based
                let contribution = w / (k + rank);
                if !contribution.is_finite() {
                    continue;
                }
                let entry = scores.entry(id.clone());
                if let std::collections::hash_map::Entry::Vacant(_) = entry {
                    order.push(id.clone());
                }
                *entry.or_insert(0.0) += contribution;
            }
        }

        let mut results: Vec<ScoredId> = order
            .into_iter()
            .map(|id| {
                let score = scores[&id];
                ScoredId::new(id, finite_f32(score))
            })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        results.truncate(top_k);
        results
    }
}

/// Convenience orchestrator for the common two-list case: fuse a dense result
/// list with a lexical result list using RRF and per-stage weights.
#[derive(Debug, Clone, Copy)]
pub struct HybridFuser {
    rrf: Rrf,
    dense_weight: f32,
    lexical_weight: f32,
}

impl Default for HybridFuser {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridFuser {
    /// Equal-weighted dense + lexical fusion with default RRF `k`.
    pub fn new() -> Self {
        Self {
            rrf: Rrf::new(),
            dense_weight: 1.0,
            lexical_weight: 1.0,
        }
    }

    /// Set the dense and lexical weights.
    pub fn with_weights(mut self, dense_weight: f32, lexical_weight: f32) -> Self {
        self.dense_weight = dense_weight;
        self.lexical_weight = lexical_weight;
        self
    }

    /// Set the RRF rank constant.
    pub fn with_k(mut self, k: f32) -> Self {
        self.rrf = Rrf::with_k(k);
        self
    }

    /// Fuse a dense ranked list and a lexical ranked list (each best-first) into
    /// the top `top_k` hybrid results. Either list may be empty — fusing with an
    /// empty list degenerates to ranking the non-empty one.
    pub fn fuse(&self, dense: &[ScoredId], lexical: &[ScoredId], top_k: usize) -> Vec<ScoredId> {
        let dense_ids: Vec<VectorId> = dense.iter().map(|s| s.id.clone()).collect();
        let lexical_ids: Vec<VectorId> = lexical.iter().map(|s| s.id.clone()).collect();
        let lists = [
            RankedInput::new(self.dense_weight, &dense_ids),
            RankedInput::new(self.lexical_weight, &lexical_ids),
        ];
        self.rrf.fuse(&lists, top_k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> VectorId {
        VectorId::new(s)
    }

    fn ids(list: &[&str]) -> Vec<VectorId> {
        list.iter().map(|s| id(s)).collect()
    }

    #[test]
    fn test_rrf_item_in_both_lists_ranks_first() {
        let a = ids(&["x", "y"]);
        let b = ids(&["y", "z"]);
        let rrf = Rrf::new();
        let out = rrf.fuse(&[RankedInput::new(1.0, &a), RankedInput::new(1.0, &b)], 10);
        let order: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        // y is in both lists, so it wins; x (rank 1 in A) beats z (rank 2 in B).
        assert_eq!(order, vec!["y", "x", "z"]);
    }

    #[test]
    fn test_rrf_exact_scores() {
        let a = ids(&["x", "y"]);
        let b = ids(&["y", "z"]);
        let rrf = Rrf::with_k(60.0);
        let out = rrf.fuse(&[RankedInput::new(1.0, &a), RankedInput::new(1.0, &b)], 10);
        let score = |needle: &str| out.iter().find(|s| s.id.as_str() == needle).unwrap().score;
        let approx = |got: f32, want: f64| (got as f64 - want).abs() < 1e-6;
        assert!(approx(score("y"), 1.0 / 62.0 + 1.0 / 61.0));
        assert!(approx(score("x"), 1.0 / 61.0));
        assert!(approx(score("z"), 1.0 / 62.0));
    }

    #[test]
    fn test_rrf_empty_inputs_return_empty() {
        let rrf = Rrf::new();
        assert!(rrf.fuse(&[], 10).is_empty());
        let empty: Vec<VectorId> = Vec::new();
        assert!(rrf.fuse(&[RankedInput::new(1.0, &empty)], 10).is_empty());
    }

    #[test]
    fn test_rrf_top_k_zero_returns_empty() {
        let a = ids(&["x", "y"]);
        let rrf = Rrf::new();
        assert!(rrf.fuse(&[RankedInput::new(1.0, &a)], 0).is_empty());
    }

    #[test]
    fn test_rrf_single_list_preserves_order() {
        let a = ids(&["first", "second", "third"]);
        let rrf = Rrf::new();
        let out = rrf.fuse(&[RankedInput::new(1.0, &a)], 10);
        let order: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order, vec!["first", "second", "third"]);
        // Scores strictly decrease with rank.
        assert!(out[0].score > out[1].score);
        assert!(out[1].score > out[2].score);
    }

    #[test]
    fn test_rrf_top_k_truncates() {
        let a = ids(&["a", "b", "c", "d"]);
        let rrf = Rrf::new();
        let out = rrf.fuse(&[RankedInput::new(1.0, &a)], 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id.as_str(), "a");
        assert_eq!(out[1].id.as_str(), "b");
    }

    #[test]
    fn test_rrf_invalid_k_falls_back_to_default() {
        assert_eq!(Rrf::with_k(f32::NAN).k(), DEFAULT_RRF_K);
        assert_eq!(Rrf::with_k(f32::INFINITY).k(), DEFAULT_RRF_K);
        assert_eq!(Rrf::with_k(0.0).k(), DEFAULT_RRF_K);
        assert_eq!(Rrf::with_k(-1.0).k(), DEFAULT_RRF_K);
        assert_eq!(Rrf::with_k(10.0).k(), 10.0);
    }

    #[test]
    fn test_rrf_ignores_invalid_weights() {
        let bad = ids(&["bad"]);
        let good = ids(&["good"]);
        let rrf = Rrf::new();
        let out = rrf.fuse(
            &[
                RankedInput::new(f32::NAN, &bad),
                RankedInput::new(f32::INFINITY, &bad),
                RankedInput::new(-1.0, &bad),
                RankedInput::new(0.0, &bad),
                RankedInput::new(1.0, &good),
            ],
            10,
        );

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id.as_str(), "good");
        assert!(out[0].score.is_finite());
    }

    #[test]
    fn test_rrf_clamps_overflowing_scores() {
        let top = ids(&["top"]);
        let rrf = Rrf::with_k(f32::MIN_POSITIVE);
        let out = rrf.fuse(
            &[
                RankedInput::new(f32::MAX, &top),
                RankedInput::new(f32::MAX, &top),
            ],
            10,
        );

        assert_eq!(out[0].id.as_str(), "top");
        assert_eq!(out[0].score, f32::MAX);
    }

    #[test]
    fn test_rrf_weight_changes_ordering() {
        // Two disjoint single-item lists at rank 1: the higher weight wins.
        let dense = ids(&["dense_top"]);
        let lexical = ids(&["lexical_top"]);
        let rrf = Rrf::new();

        let lexical_heavy = rrf.fuse(
            &[
                RankedInput::new(1.0, &dense),
                RankedInput::new(5.0, &lexical),
            ],
            10,
        );
        assert_eq!(lexical_heavy[0].id.as_str(), "lexical_top");

        let dense_heavy = rrf.fuse(
            &[
                RankedInput::new(5.0, &dense),
                RankedInput::new(1.0, &lexical),
            ],
            10,
        );
        assert_eq!(dense_heavy[0].id.as_str(), "dense_top");
    }

    #[test]
    fn test_rrf_tie_break_is_deterministic_by_id() {
        // Three disjoint lists each with one item at rank 1 and equal weight =>
        // identical scores => deterministic id ordering.
        let la = ids(&["c"]);
        let lb = ids(&["a"]);
        let lc = ids(&["b"]);
        let rrf = Rrf::new();
        let out = rrf.fuse(
            &[
                RankedInput::new(1.0, &la),
                RankedInput::new(1.0, &lb),
                RankedInput::new(1.0, &lc),
            ],
            10,
        );
        let order: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_hybrid_fuser_dense_only_when_lexical_empty() {
        let dense = vec![ScoredId::new(id("d1"), 0.9), ScoredId::new(id("d2"), 0.5)];
        let fuser = HybridFuser::new();
        let out = fuser.fuse(&dense, &[], 10);
        let order: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order, vec!["d1", "d2"]);
    }

    #[test]
    fn test_hybrid_fuser_combines_dense_and_lexical() {
        // d_shared is mid-ranked in dense but top in lexical; fusion should lift
        // it above items that appear in only one list.
        let dense = vec![
            ScoredId::new(id("d_only"), 0.95),
            ScoredId::new(id("d_shared"), 0.40),
        ];
        let lexical = vec![
            ScoredId::new(id("d_shared"), 8.0),
            ScoredId::new(id("l_only"), 2.0),
        ];
        let fuser = HybridFuser::new();
        let out = fuser.fuse(&dense, &lexical, 10);
        assert_eq!(out[0].id.as_str(), "d_shared", "item in both lists wins");
        // All three distinct ids are present exactly once (dedup across lists).
        assert_eq!(out.len(), 3);
        let mut seen: Vec<&str> = out.iter().map(|s| s.id.as_str()).collect();
        seen.sort();
        assert_eq!(seen, vec!["d_only", "d_shared", "l_only"]);
    }
}
