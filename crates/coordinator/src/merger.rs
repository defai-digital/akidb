//! Result merging with min-heap

use akidb_common::SearchResult;
use akidb_invariants::debug_invariant;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

const MAX_INITIAL_ALLOCATION: usize = 100_000;

/// Wrapper for SearchResult that implements Ord for min-heap
struct ScoredResult {
    result: SearchResult,
}

impl PartialEq for ScoredResult {
    fn eq(&self, other: &Self) -> bool {
        // Include both ID and score for proper equality
        self.result.id == other.result.id && self.result.score == other.result.score
    }
}

impl Eq for ScoredResult {}

impl PartialOrd for ScoredResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (we want to keep highest scores)
        // Handle NaN properly to maintain total ordering:
        // - NaN is treated as the smallest value (goes to bottom of heap)
        // - This ensures NaN scores don't corrupt the heap invariants
        match (self.result.score.is_nan(), other.result.score.is_nan()) {
            (true, true) => Ordering::Equal,    // Both NaN: equal
            (true, false) => Ordering::Less,    // self is NaN: self < other (NaN goes to bottom)
            (false, true) => Ordering::Greater, // other is NaN: self > other
            (false, false) => {
                // Neither is NaN, safe to compare (reverse for min-heap)
                let score_cmp = other
                    .result
                    .score
                    .partial_cmp(&self.result.score)
                    .expect("scores are non-NaN per match arm");
                // Tie-break by ID for deterministic ordering
                if score_cmp == Ordering::Equal {
                    self.result.id.as_str().cmp(other.result.id.as_str())
                } else {
                    score_cmp
                }
            }
        }
    }
}

/// Result merger using min-heap for efficient top-k selection
///
/// # Thread Safety
///
/// **WARNING: `ResultMerger` is NOT thread-safe.**
///
/// This struct uses internal mutable state (`best_scores` HashMap and `heap`) that
/// can become temporarily inconsistent during heap compaction (see lines 105-132).
/// Each search request MUST create its own `ResultMerger` instance.
///
/// If concurrent access is needed, wrap the merger in appropriate synchronization
/// (e.g., `Mutex<ResultMerger>`), though this is not recommended for performance
/// reasons. The intended usage pattern is one merger per search request.
pub struct ResultMerger {
    heap: BinaryHeap<ScoredResult>,
    capacity: usize,
    /// FIX BUG-066: Track best score per ID to handle duplicates correctly
    /// Maps ID -> best score seen so far
    best_scores: HashMap<String, f32>,
}

impl ResultMerger {
    /// Create a new merger with given capacity (top_k)
    pub fn new(capacity: usize) -> Self {
        let heap_capacity = capacity.saturating_add(1).min(MAX_INITIAL_ALLOCATION);
        let score_capacity = capacity
            .saturating_mul(2)
            .min(MAX_INITIAL_ALLOCATION.saturating_mul(2));
        Self {
            heap: BinaryHeap::with_capacity(heap_capacity),
            capacity,
            best_scores: HashMap::with_capacity(score_capacity),
        }
    }

    /// Add results from a shard
    pub fn add_results(&mut self, results: Vec<SearchResult>) {
        for result in results {
            self.add(result);
        }
    }

    /// Add a single result
    /// FIX BUG-066: Deduplicates by ID, keeping the HIGHEST score across all shards
    /// When the same ID appears from multiple shards, we keep the best score.
    pub fn add(&mut self, result: SearchResult) {
        let id_str = result.id.to_string();
        let score = result.score;

        // Reject invalid shard scores before they enter the heap or duplicate
        // tracker. Infinity would otherwise outrank every valid result.
        if !score.is_finite() {
            return;
        }

        // FIX BUG-066: Check if we've seen this ID and if the new score is better
        if let Some(&existing_score) = self.best_scores.get(&id_str) {
            // Skip if existing score is better or equal (keep first seen on tie)
            if existing_score >= score {
                return;
            }
            // New score is better - we need to add this result
            // The old entry will be filtered out in finish() via deduplication
            // Update our tracking of the best score
            self.best_scores.insert(id_str.clone(), score);
            // Add to heap (may create temporary duplicate, resolved in finish())
            self.heap.push(ScoredResult { result });

            // FIX BUG-H050, BUG-HUNT-003: Maintain heap size limit to prevent unbounded growth
            // When adding duplicates (better scores for existing IDs), the heap can grow
            // beyond capacity. We use 1.5x capacity as a buffer (reduced from 2x per BUG-HUNT-003)
            // to prevent memory exhaustion with many shards (100 shards × 1000 results × 2x = 200K entries).
            // This bounds memory while avoiding O(n) operations on every add.
            let compaction_threshold = self.capacity.saturating_mul(3) / 2;
            if self.heap.len() > compaction_threshold {
                // Rebuild heap keeping only top capacity entries
                let mut sorted: Vec<_> = self.heap.drain().collect();
                // FIX BUG-HUNT-404: The Ord impl is reversed for min-heap usage, so .sort()
                // would put LOWEST scores first. We need to sort by score descending directly
                // to keep the HIGHEST scores.
                sorted.sort_by(|a, b| {
                    // Sort by score descending (highest first), handle NaN safely
                    match (a.result.score.is_nan(), b.result.score.is_nan()) {
                        (true, true) => std::cmp::Ordering::Equal,
                        (true, false) => std::cmp::Ordering::Greater, // NaN goes to end
                        (false, true) => std::cmp::Ordering::Less,
                        (false, false) => b
                            .result
                            .score
                            .partial_cmp(&a.result.score)
                            .unwrap_or(std::cmp::Ordering::Equal),
                    }
                });

                // FIX BUG-HUNT-202: Rebuild best_scores from remaining heap entries.
                // Without this, evicted entries remain in best_scores, causing valid new
                // results to be incorrectly rejected as "duplicates", returning fewer
                // results than requested even when more valid results exist.
                self.best_scores.clear();
                for scored_result in sorted.iter().take(self.capacity) {
                    self.best_scores.insert(
                        scored_result.result.id.to_string(),
                        scored_result.result.score,
                    );
                }

                self.heap.extend(sorted.into_iter().take(self.capacity));

                // INVARIANT: After compaction, heap should be at most capacity
                debug_invariant!(
                    self.heap.len() <= self.capacity,
                    "Heap size {} exceeds capacity {} after compaction",
                    self.heap.len(),
                    self.capacity
                );
            }
            return;
        }

        // New ID - standard heap insertion logic
        if self.heap.len() < self.capacity {
            self.best_scores.insert(id_str, score);
            self.heap.push(ScoredResult { result });
        } else if let Some(min) = self.heap.peek() {
            // FIX BUG-110: Handle NaN scores in the comparison
            // If min.score is NaN, the comparison `score >= min.score` returns false,
            // which would incorrectly reject valid results. NaN scores should always
            // be evictable since they're invalid results.
            let should_insert = min.result.score.is_nan()
                || score > min.result.score
                || (score == min.result.score && id_str.as_str() < min.result.id.as_str());
            if should_insert {
                // SAFETY: heap is non-empty because peek() returned Some above
                if let Some(evicted) = self.heap.pop() {
                    self.best_scores.remove(&evicted.result.id.to_string());
                    self.best_scores.insert(id_str, score);
                    self.heap.push(ScoredResult { result });
                }
            }
        }
    }

    /// Get the final merged results, sorted by score descending
    /// Non-finite scores are filtered out as they indicate invalid results
    /// FIX BUG-066: Deduplicates by ID, keeping only the best score per ID
    pub fn finish(self) -> Vec<SearchResult> {
        let mut results: Vec<SearchResult> = self
            .heap
            .into_iter()
            .map(|sr| sr.result)
            // Filter out invalid scores as a final defense for stale callers.
            .filter(|r| r.score.is_finite())
            .collect();

        // Sort by score descending with tie-breaking by ID for deterministic ordering
        results.sort_by(|a, b| {
            // SAFETY: non-finite scores filtered out above
            let score_cmp = b
                .score
                .partial_cmp(&a.score)
                .expect("scores are finite per filter above");
            if score_cmp == Ordering::Equal {
                a.id.as_str().cmp(b.id.as_str()) // Tie-break by ID (ascending)
            } else {
                score_cmp
            }
        });

        // FIX BUG-066: Deduplicate by ID, keeping highest score (first occurrence after sort)
        let mut seen = std::collections::HashSet::new();
        results.retain(|r| seen.insert(r.id.to_string()));

        // Truncate to capacity in case duplicates pushed us over
        results.truncate(self.capacity);

        // INVARIANT: Results must be sorted by score descending
        debug_invariant!(
            results.windows(2).all(|w| w[0].score >= w[1].score),
            "Merged results are not sorted by score descending"
        );

        // INVARIANT: Results must not exceed capacity
        debug_invariant!(
            results.len() <= self.capacity,
            "Merged results {} exceed capacity {}",
            results.len(),
            self.capacity
        );

        // INVARIANT: All scores must be valid finite values
        debug_invariant!(
            results.iter().all(|r| r.score.is_finite()),
            "Merged results contain non-finite scores"
        );

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_common::VectorId;

    fn make_result(id: &str, score: f32) -> SearchResult {
        SearchResult::new(VectorId::new(id), score)
    }

    #[test]
    fn test_merger_basic() {
        let mut merger = ResultMerger::new(3);

        merger.add(make_result("a", 0.9));
        merger.add(make_result("b", 0.7));
        merger.add(make_result("c", 0.8));
        merger.add(make_result("d", 0.6));
        merger.add(make_result("e", 0.95));

        let results = merger.finish();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id.as_str(), "e");
        assert_eq!(results[1].id.as_str(), "a");
        assert_eq!(results[2].id.as_str(), "c");
    }

    #[test]
    fn test_merger_multi_shard() {
        let mut merger = ResultMerger::new(5);

        // Results from shard 1
        merger.add_results(vec![
            make_result("s1-a", 0.9),
            make_result("s1-b", 0.7),
            make_result("s1-c", 0.5),
        ]);

        // Results from shard 2
        merger.add_results(vec![
            make_result("s2-a", 0.85),
            make_result("s2-b", 0.75),
            make_result("s2-c", 0.65),
        ]);

        let results = merger.finish();

        assert_eq!(results.len(), 5);
        // Top 5 should be: s1-a(0.9), s2-a(0.85), s2-b(0.75), s1-b(0.7), s2-c(0.65)
        assert_eq!(results[0].id.as_str(), "s1-a");
        assert_eq!(results[1].id.as_str(), "s2-a");
    }

    /// FIX BUG-066: Test that duplicate IDs keep the best score
    #[test]
    fn test_merger_keeps_best_score() {
        let mut merger = ResultMerger::new(5);

        // Same ID from different shards with different scores
        merger.add(make_result("dup-id", 0.5)); // First score
        merger.add(make_result("dup-id", 0.9)); // Better score
        merger.add(make_result("dup-id", 0.3)); // Worse score
        merger.add(make_result("other", 0.8));

        let results = merger.finish();

        // Should have 2 unique IDs
        assert_eq!(results.len(), 2);
        // dup-id should have the best score (0.9)
        let dup_result = results.iter().find(|r| r.id.as_str() == "dup-id").unwrap();
        assert!(
            (dup_result.score - 0.9).abs() < 0.001,
            "Expected 0.9, got {}",
            dup_result.score
        );
    }

    #[test]
    fn test_merger_keeps_lexicographically_smallest_ids_on_score_tie() {
        let mut merger = ResultMerger::new(2);

        merger.add(make_result("a", 0.8));
        merger.add(make_result("b", 0.8));
        merger.add(make_result("c", 0.8));

        let results = merger.finish();
        let ids: Vec<&str> = results.iter().map(|result| result.id.as_str()).collect();

        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn test_merger_rejects_non_finite_scores() {
        let mut merger = ResultMerger::new(3);

        merger.add(make_result("nan", f32::NAN));
        merger.add(make_result("pos_inf", f32::INFINITY));
        merger.add(make_result("neg_inf", f32::NEG_INFINITY));
        merger.add(make_result("valid", 0.7));

        let results = merger.finish();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.as_str(), "valid");
        assert!(results.iter().all(|result| result.score.is_finite()));
    }

    #[test]
    fn test_invalid_duplicate_does_not_block_later_valid_score() {
        let mut merger = ResultMerger::new(3);

        merger.add(make_result("same-id", f32::INFINITY));
        merger.add(make_result("same-id", 0.6));

        let results = merger.finish();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.as_str(), "same-id");
        assert_eq!(results[0].score, 0.6);
    }

    #[test]
    fn test_large_capacity_constructor_does_not_overflow() {
        let result = std::panic::catch_unwind(|| ResultMerger::new(usize::MAX));

        assert!(result.is_ok());
    }
}
