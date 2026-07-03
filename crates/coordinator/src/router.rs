//! Shard routing logic with consistent hashing

use akidb_common::VectorId;
use std::collections::{BTreeMap, HashMap};

/// Number of virtual nodes per shard for consistent hashing
const VIRTUAL_NODES_PER_SHARD: u32 = 150;

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
}

impl ShardRouter {
    /// Create a new router with the given shards
    pub fn new(shards: Vec<ShardInfo>) -> Self {
        let mut router = Self {
            shards,
            ring: BTreeMap::new(),
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
                self.ring.insert(hash, RingEntry {
                    shard_index: shard_idx,
                    _virtual_node_id: vnode,
                });
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
    pub fn route_batch<'a>(&'a self, ids: &'a [VectorId]) -> HashMap<&'a str, Vec<(&'a VectorId, usize)>> {
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

    /// Get all shards
    pub fn all_shards(&self) -> &[ShardInfo] {
        &self.shards
    }

    /// Update shard health status
    pub fn update_health(&mut self, shard_id: &str, healthy: bool) {
        if let Some(shard) = self.shards.iter_mut().find(|s| s.id == shard_id) {
            shard.healthy = healthy;
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
}
