//! Shard routing logic with consistent hashing

use akidb_common::VectorId;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// Number of virtual nodes per shard for consistent hashing
const VIRTUAL_NODES_PER_SHARD: u32 = 150;

/// How long a shard stays out of the fan-out set after a failed request before
/// the coordinator retries it.
///
/// Shards are only ever marked unhealthy by a failed request, so without a
/// retry window a single transient timeout would exclude a shard's data for the
/// lifetime of the process. Each new failure restarts the window, so a shard
/// that stays down costs at most one timed-out probe per window.
pub const UNHEALTHY_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Shard information
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub id: String,
    pub address: String,
    pub healthy: bool,
}

/// Consistent hashing ring entry
#[derive(Debug, Clone)]
struct RingEntry {
    shard_index: usize,
    _virtual_node_id: u32,
}

/// Shard router using consistent hashing for better distribution
pub struct ShardRouter {
    shards: Vec<ShardInfo>,
    /// Consistent hashing ring: hash -> shard index
    ring: BTreeMap<u64, RingEntry>,
    /// When each currently-unhealthy shard was last marked unhealthy, used to
    /// decide whether its retry window has elapsed.
    unhealthy_since: HashMap<String, Instant>,
    /// Shards currently being probed by an in-flight request, and when the
    /// probe was claimed. Limits a recovered-but-still-down shard to one
    /// in-flight probe instead of one probe per concurrent search.
    probes_in_flight: HashMap<String, Instant>,
}

impl ShardRouter {
    /// Create a new router with the given shards
    pub fn new(shards: Vec<ShardInfo>) -> Self {
        let mut router = Self {
            shards,
            ring: BTreeMap::new(),
            unhealthy_since: HashMap::new(),
            probes_in_flight: HashMap::new(),
        };
        router.rebuild_ring();
        router
    }

    /// Rebuild the consistent hashing ring
    fn rebuild_ring(&mut self) {
        self.ring.clear();

        for (shard_idx, shard) in self.shards.iter().enumerate() {
            for vnode in 0..VIRTUAL_NODES_PER_SHARD {
                let hash = Self::hash_key(&format!("{}:{}", shard.id, vnode));
                self.ring.insert(
                    hash,
                    RingEntry {
                        shard_index: shard_idx,
                        _virtual_node_id: vnode,
                    },
                );
            }
        }
    }

    /// FIX BUG-099: Use deterministic hash with good avalanche properties
    /// The previous DefaultHasher (SipHash) uses a random seed per process,
    /// causing inconsistent routing after coordinator restart. This implementation:
    /// - Uses FNV-1a as base hash (deterministic)
    /// - Applies a finalizer for better bit distribution (avalanche effect)
    /// - No external dependencies
    fn hash_key(key: &str) -> u64 {
        // FNV-1a 64-bit constants
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x00000100000001B3;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in key.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        // Finalizer for better avalanche (similar to MurmurHash3/xxHash)
        // This ensures small input changes produce large output changes
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51afd7ed558ccd);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
        hash ^= hash >> 33;

        hash
    }

    /// Get the shard for a vector ID using consistent hashing
    pub fn route(&self, id: &VectorId) -> Option<&ShardInfo> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = Self::hash_key(id.as_str());

        // Find the first entry >= hash (clockwise on the ring)
        let shard_idx = if let Some((_, entry)) = self.ring.range(hash..).next() {
            entry.shard_index
        } else {
            // Wrap around to the first entry
            self.ring.values().next()?.shard_index
        };

        Some(&self.shards[shard_idx])
    }

    /// Route a batch of vector IDs, grouping by shard
    /// Returns a map of shard_id -> list of (vector_id, original_index)
    pub fn route_batch<'a>(
        &'a self,
        ids: &'a [VectorId],
    ) -> HashMap<&'a str, Vec<(&'a VectorId, usize)>> {
        let mut groups: HashMap<&str, Vec<(&VectorId, usize)>> = HashMap::new();

        for (idx, id) in ids.iter().enumerate() {
            if let Some(shard) = self.route(id) {
                groups.entry(&shard.id).or_default().push((id, idx));
            }
        }

        groups
    }

    /// Get all healthy shards for fan-out search
    pub fn healthy_shards(&self) -> Vec<&ShardInfo> {
        self.shards.iter().filter(|s| s.healthy).collect()
    }

    /// Select the shards a fan-out should query, plus the IDs of the shards
    /// deliberately held back.
    ///
    /// Healthy shards are always selected. A shard marked unhealthy by an
    /// earlier failure is selected again once `retry_after` has elapsed since
    /// that failure, so a shard that recovers rejoins without a coordinator
    /// restart; until then it is held back and must be reported as missing so
    /// the response does not claim full coverage.
    ///
    /// Only one probe of a recovering shard is in flight at a time: while a
    /// claimed probe is younger than `retry_after`, further requests keep the
    /// shard held back instead of every one of them paying the shard timeout.
    /// A claim expires with the window, and any health update releases it.
    ///
    /// If every configured shard is inside its retry window, all of them are
    /// probed anyway: holding back the whole cluster would turn a transient
    /// blip into a total search outage, and the resulting failures still land in
    /// the missing list.
    pub fn fanout_selection(&mut self, retry_after: Duration) -> (Vec<&ShardInfo>, Vec<String>) {
        let now = Instant::now();
        let mut selected = Vec::new();
        let mut held_back = Vec::new();

        for shard in &self.shards {
            if shard.healthy {
                selected.push(shard);
                continue;
            }
            let retry_due = self
                .unhealthy_since
                .get(&shard.id)
                .map(|since| now.duration_since(*since) >= retry_after)
                // A shard that was never seen failing has no retry window to
                // wait out.
                .unwrap_or(true);
            let claimed = self
                .probes_in_flight
                .get(&shard.id)
                .map(|claimed| now.duration_since(*claimed) < retry_after)
                .unwrap_or(false);
            if retry_due && !claimed {
                self.probes_in_flight.insert(shard.id.clone(), now);
                selected.push(shard);
            } else {
                held_back.push(shard.id.clone());
            }
        }

        if selected.is_empty() && !self.shards.is_empty() {
            return (self.shards.iter().collect(), Vec::new());
        }

        (selected, held_back)
    }

    /// Release the probe claims of shards a fan-out has finished attempting.
    ///
    /// Every attempted shard needs this, whatever the outcome: the outcome is
    /// already recorded through [`Self::update_health`], and a stale claim would
    /// hold a shard back until it expires.
    pub fn release_probe_claims(&mut self, shard_ids: &[String]) {
        for shard_id in shard_ids {
            self.probes_in_flight.remove(shard_id);
        }
    }

    /// Get all shards
    pub fn all_shards(&self) -> &[ShardInfo] {
        &self.shards
    }

    /// Update shard health status.
    ///
    /// Marking a shard unhealthy records the failure time and restarts the
    /// retry window, so a shard that keeps failing is probed at most once per
    /// `UNHEALTHY_RETRY_AFTER`. Either outcome also releases any in-flight
    /// probe claim, because the probe has now completed.
    pub fn update_health(&mut self, shard_id: &str, healthy: bool) {
        if !self.shards.iter().any(|s| s.id == shard_id) {
            return;
        }
        if let Some(shard) = self.shards.iter_mut().find(|s| s.id == shard_id) {
            shard.healthy = healthy;
        }
        self.probes_in_flight.remove(shard_id);
        if healthy {
            self.unhealthy_since.remove(shard_id);
        } else {
            self.unhealthy_since
                .insert(shard_id.to_string(), Instant::now());
        }
    }

    /// Get shard by ID
    pub fn get_shard(&self, shard_id: &str) -> Option<&ShardInfo> {
        self.shards.iter().find(|s| s.id == shard_id)
    }

    /// Get distribution statistics across shards
    pub fn distribution_stats(&self, sample_ids: &[VectorId]) -> DistributionStats {
        let total = sample_ids.len();

        // Handle empty input to prevent division by zero
        if total == 0 {
            return DistributionStats {
                total_samples: 0,
                shard_percentages: vec![],
            };
        }

        let mut counts: HashMap<String, usize> = HashMap::new();

        for id in sample_ids {
            if let Some(shard) = self.route(id) {
                *counts.entry(shard.id.clone()).or_default() += 1;
            }
        }

        let shard_percentages: Vec<(String, f64)> = counts
            .into_iter()
            .map(|(id, count)| (id, count as f64 / total as f64 * 100.0))
            .collect();

        DistributionStats {
            total_samples: total,
            shard_percentages,
        }
    }
}

/// Statistics about data distribution across shards
#[derive(Debug)]
pub struct DistributionStats {
    pub total_samples: usize,
    pub shard_percentages: Vec<(String, f64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_basic() {
        let shards = vec![
            ShardInfo {
                id: "shard-0".to_string(),
                address: "localhost:50051".to_string(),
                healthy: true,
            },
            ShardInfo {
                id: "shard-1".to_string(),
                address: "localhost:50052".to_string(),
                healthy: true,
            },
        ];

        let router = ShardRouter::new(shards);

        let shard = router.route(&VectorId::new("test-vec")).unwrap();
        assert!(shard.healthy);
    }

    #[test]
    fn test_router_consistent() {
        let shards = vec![
            ShardInfo {
                id: "shard-0".to_string(),
                address: "localhost:50051".to_string(),
                healthy: true,
            },
            ShardInfo {
                id: "shard-1".to_string(),
                address: "localhost:50052".to_string(),
                healthy: true,
            },
        ];

        let router = ShardRouter::new(shards);

        // Same ID should always route to same shard
        let id = VectorId::new("test-vec");
        let shard1 = router.route(&id).unwrap().id.clone();
        let shard2 = router.route(&id).unwrap().id.clone();
        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_distribution_evenness() {
        let shards = vec![
            ShardInfo {
                id: "shard-0".to_string(),
                address: "localhost:50051".to_string(),
                healthy: true,
            },
            ShardInfo {
                id: "shard-1".to_string(),
                address: "localhost:50052".to_string(),
                healthy: true,
            },
        ];

        let router = ShardRouter::new(shards);

        // Generate sample IDs
        let sample_ids: Vec<VectorId> = (0..10000)
            .map(|i| VectorId::new(format!("vec-{}", i)))
            .collect();

        let stats = router.distribution_stats(&sample_ids);

        // With consistent hashing, distribution should be fairly even (within 10% of ideal 50%)
        for (_, pct) in &stats.shard_percentages {
            assert!(*pct > 40.0, "Shard got {}% - too uneven", pct);
            assert!(*pct < 60.0, "Shard got {}% - too uneven", pct);
        }
    }

    fn two_shards() -> Vec<ShardInfo> {
        vec![
            ShardInfo {
                id: "shard-0".to_string(),
                address: "localhost:50051".to_string(),
                healthy: true,
            },
            ShardInfo {
                id: "shard-1".to_string(),
                address: "localhost:50052".to_string(),
                healthy: true,
            },
        ]
    }

    fn selected_ids(router: &mut ShardRouter, retry_after: Duration) -> Vec<String> {
        router
            .fanout_selection(retry_after)
            .0
            .iter()
            .map(|shard| shard.id.clone())
            .collect()
    }

    #[test]
    fn test_unhealthy_shard_is_held_back_then_retried() {
        let mut router = ShardRouter::new(two_shards());

        router.update_health("shard-1", false);

        // A shard that just failed leaves the fan-out set so its timeout is not
        // paid on every search, and it is reported as missing rather than
        // silently dropped.
        let (selected, held_back) = router.fanout_selection(UNHEALTHY_RETRY_AFTER);
        assert_eq!(
            selected.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["shard-0"]
        );
        assert_eq!(held_back, vec!["shard-1".to_string()]);

        // It rejoins on its own once the retry window elapses.
        assert_eq!(
            selected_ids(&mut router, Duration::ZERO),
            vec!["shard-0".to_string(), "shard-1".to_string()]
        );

        // A successful probe clears the failure.
        router.update_health("shard-1", true);
        assert!(router.get_shard("shard-1").unwrap().healthy);
        assert!(router.fanout_selection(UNHEALTHY_RETRY_AFTER).1.is_empty());
    }

    #[test]
    fn test_repeated_failures_restart_the_retry_window() {
        let mut router = ShardRouter::new(two_shards());
        let window = Duration::from_millis(50);

        router.update_health("shard-1", false);
        std::thread::sleep(Duration::from_millis(80));
        assert!(
            selected_ids(&mut router, window).contains(&"shard-1".to_string()),
            "window elapsed"
        );

        // The probe failed as well, so the next attempt waits a full window
        // instead of inheriting the first failure's expired stamp.
        router.update_health("shard-1", false);
        assert_eq!(
            selected_ids(&mut router, window),
            vec!["shard-0".to_string()]
        );
    }

    #[test]
    fn test_a_retry_probe_is_claimed_by_one_request_at_a_time() {
        let mut router = ShardRouter::new(two_shards());
        let window = UNHEALTHY_RETRY_AFTER;

        router.update_health("shard-1", false);
        // Simulate the retry window having elapsed, without sleeping.
        router.unhealthy_since.insert(
            "shard-1".to_string(),
            Instant::now() - window - Duration::from_secs(1),
        );

        // The first request claims the probe and selects the shard.
        let (selected, held_back) = router.fanout_selection(window);
        assert!(selected.iter().any(|s| s.id == "shard-1"));
        assert!(held_back.is_empty());

        // A concurrent request holds it back instead of probing it a second
        // time and paying another full shard timeout.
        let (selected, held_back) = router.fanout_selection(window);
        assert_eq!(held_back, vec!["shard-1".to_string()]);
        assert_eq!(selected.len(), 1);

        // Any health update completes the probe and releases the claim, so the
        // shard is never stuck held back.
        router.update_health("shard-1", false);
        assert!(!router.probes_in_flight.contains_key("shard-1"));
    }

    #[test]
    fn test_all_shards_held_back_are_probed_anyway() {
        let mut router = ShardRouter::new(two_shards());

        router.update_health("shard-0", false);
        router.update_health("shard-1", false);

        // Holding back the entire cluster would turn a blip into a total search
        // outage, so the fan-out falls back to probing every shard.
        let (selected, held_back) = router.fanout_selection(UNHEALTHY_RETRY_AFTER);
        assert_eq!(selected.len(), 2);
        assert!(held_back.is_empty());
    }
}
