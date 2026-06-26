//! Tombstone bitset for tracking deleted vectors
//!
//! This module provides a bitset implementation for efficiently tracking
//! deleted vectors. On GPU builds, this uses device memory for fast filtering.

use crate::{InternalId, Result};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tombstone bitset for tracking deleted vectors
///
/// Uses 1 bit per vector:
/// - 0 = active
/// - 1 = deleted
///
/// Memory usage: N/8 bytes (125KB for 1M vectors)
pub struct TombstoneBitset {
    /// The bitset data
    data: RwLock<Vec<u8>>,
    /// Capacity in number of vectors (use AtomicU64 for thread-safe resize)
    capacity: std::sync::atomic::AtomicU64,
    /// Count of deleted vectors
    deleted_count: AtomicU64,
}

impl TombstoneBitset {
    /// Create a new tombstone bitset with capacity for `n` vectors
    pub fn new(capacity: u64) -> Self {
        let byte_count = ((capacity + 7) / 8) as usize;
        Self {
            data: RwLock::new(vec![0u8; byte_count]),
            capacity: AtomicU64::new(capacity),
            deleted_count: AtomicU64::new(0),
        }
    }

    /// Mark a vector as deleted
    pub fn mark_deleted(&self, id: InternalId) -> Result<()> {
        // FIX BUG-060: Validate ID is non-negative with clear error message
        if !id.is_valid() {
            return Err(crate::AkiDbError::InvalidParameter(format!(
                "Internal ID {} is invalid (negative IDs not allowed)",
                id.0
            )));
        }
        let idx = id.0 as u64;
        let cap = self.capacity.load(Ordering::Acquire);
        if idx >= cap {
            return Err(crate::AkiDbError::InvalidParameter(format!(
                "Internal ID {} exceeds capacity {}",
                idx, cap
            )));
        }

        let byte_idx = (idx / 8) as usize;
        let bit_idx = (idx % 8) as u8;
        let mask = 1u8 << bit_idx;

        let mut data = self.data.write();
        let was_deleted = (data[byte_idx] & mask) != 0;

        if !was_deleted {
            data[byte_idx] |= mask;
            self.deleted_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Check if a vector is deleted
    pub fn is_deleted(&self, id: InternalId) -> bool {
        // FIX BUG-060: Treat negative IDs as not deleted (invalid)
        if !id.is_valid() {
            return false;
        }
        let idx = id.0 as u64;
        if idx >= self.capacity.load(Ordering::Acquire) {
            return false;
        }

        let byte_idx = (idx / 8) as usize;
        let bit_idx = (idx % 8) as u8;
        let mask = 1u8 << bit_idx;

        let data = self.data.read();
        (data[byte_idx] & mask) != 0
    }

    /// Clear a tombstone (un-delete a vector)
    /// FIX BUG-024: Required for upsert operations to resurrect deleted vectors
    pub fn clear_deleted(&self, id: InternalId) -> Result<()> {
        // FIX BUG-060: Validate ID is non-negative with clear error message
        if !id.is_valid() {
            return Err(crate::AkiDbError::InvalidParameter(format!(
                "Internal ID {} is invalid (negative IDs not allowed)",
                id.0
            )));
        }
        let idx = id.0 as u64;
        let cap = self.capacity.load(Ordering::Acquire);
        if idx >= cap {
            return Err(crate::AkiDbError::InvalidParameter(format!(
                "Internal ID {} exceeds capacity {}",
                idx, cap
            )));
        }

        let byte_idx = (idx / 8) as usize;
        let bit_idx = (idx % 8) as u8;
        let mask = 1u8 << bit_idx;

        let mut data = self.data.write();
        let was_deleted = (data[byte_idx] & mask) != 0;

        if was_deleted {
            data[byte_idx] &= !mask;
            // FIX BUG-098: Use compare_exchange loop to prevent underflow
            // The previous fetch_sub approach was flawed because it returns the
            // PREVIOUS value, meaning the decrement already happened before we
            // could check. If prev == 0, the counter had already wrapped to u64::MAX.
            // This loop atomically decrements only if count > 0.
            loop {
                let current = self.deleted_count.load(Ordering::Relaxed);
                if current == 0 {
                    // Already at zero, nothing to decrement (shouldn't happen normally)
                    break;
                }
                if self.deleted_count.compare_exchange_weak(
                    current,
                    current - 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ).is_ok() {
                    break;
                }
                // CAS failed, another thread modified it, retry
            }
        }

        Ok(())
    }

    /// Get the count of deleted vectors
    pub fn deleted_count(&self) -> u64 {
        self.deleted_count.load(Ordering::Relaxed)
    }

    /// Get the tombstone ratio (deleted / capacity)
    pub fn tombstone_ratio(&self) -> f64 {
        let cap = self.capacity.load(Ordering::Acquire);
        if cap == 0 {
            return 0.0;
        }
        self.deleted_count() as f64 / cap as f64
    }

    /// Check if compaction is needed based on threshold
    pub fn needs_compaction(&self, threshold: f64) -> bool {
        self.tombstone_ratio() >= threshold
    }

    /// Reset the bitset (after compaction)
    pub fn reset(&self) {
        let mut data = self.data.write();
        data.fill(0);
        self.deleted_count.store(0, Ordering::Relaxed);
    }

    /// Resize the bitset for new capacity
    pub fn resize(&self, new_capacity: u64) -> Result<()> {
        let new_byte_count = ((new_capacity + 7) / 8) as usize;
        let mut data = self.data.write();
        // FIX BUG-HUNT-603: Resize data FIRST, then update capacity
        // This ensures data is always large enough for any capacity value visible
        // to concurrent readers. While the write lock provides synchronization,
        // resizing data first maintains the invariant that data.len() >= (capacity+7)/8
        // at all times. When shrinking, concurrent is_deleted() calls that still see
        // the old larger capacity will find valid data. When growing, no reader can
        // see the new capacity until we release the lock (after data is resized).
        data.resize(new_byte_count, 0);
        self.capacity.store(new_capacity, Ordering::Release);
        Ok(())
    }

    /// Get the raw bitset data (for FAISS IDSelector integration)
    pub fn as_slice(&self) -> Vec<u8> {
        self.data.read().clone()
    }

    /// Get capacity
    pub fn capacity(&self) -> u64 {
        self.capacity.load(Ordering::Acquire)
    }

    /// Memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        let data = self.data.read();
        data.len()
    }
}

impl std::fmt::Debug for TombstoneBitset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TombstoneBitset")
            .field("capacity", &self.capacity())
            .field("deleted_count", &self.deleted_count())
            .field("tombstone_ratio", &self.tombstone_ratio())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tombstone_basic() {
        let bitset = TombstoneBitset::new(1000);

        assert!(!bitset.is_deleted(InternalId(0)));
        assert!(!bitset.is_deleted(InternalId(100)));

        bitset.mark_deleted(InternalId(100)).unwrap();

        assert!(!bitset.is_deleted(InternalId(0)));
        assert!(bitset.is_deleted(InternalId(100)));
        assert_eq!(bitset.deleted_count(), 1);
    }

    #[test]
    fn test_tombstone_idempotent() {
        let bitset = TombstoneBitset::new(1000);

        bitset.mark_deleted(InternalId(50)).unwrap();
        bitset.mark_deleted(InternalId(50)).unwrap();

        assert_eq!(bitset.deleted_count(), 1);
    }

    #[test]
    fn test_tombstone_ratio() {
        let bitset = TombstoneBitset::new(100);

        for i in 0..10 {
            bitset.mark_deleted(InternalId(i)).unwrap();
        }

        assert!((bitset.tombstone_ratio() - 0.1).abs() < 0.001);
        assert!(bitset.needs_compaction(0.1));
        assert!(!bitset.needs_compaction(0.2));
    }

    #[test]
    fn test_tombstone_reset() {
        let bitset = TombstoneBitset::new(100);

        bitset.mark_deleted(InternalId(10)).unwrap();
        bitset.mark_deleted(InternalId(20)).unwrap();
        assert_eq!(bitset.deleted_count(), 2);

        bitset.reset();

        assert_eq!(bitset.deleted_count(), 0);
        assert!(!bitset.is_deleted(InternalId(10)));
    }

    #[test]
    fn test_tombstone_memory() {
        let bitset = TombstoneBitset::new(1_000_000);
        // 1M vectors = 125KB
        assert_eq!(bitset.memory_bytes(), 125_000);
    }
}
