//! Dynamic Batcher
//!
//! Adjusts batch size based on queue depth and GPU utilization.

use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::debug;

use crate::config::BatcherConfig;

/// Dynamic batcher that adjusts batch size based on load
pub struct DynamicBatcher<T> {
    config: BatcherConfig,
    queue_depth: AtomicUsize,
    gpu_util: AtomicUsize, // Stored as percentage * 100
    sender: mpsc::Sender<T>,
    receiver: mpsc::Receiver<T>,
    /// FIX BUG-H051: Track pending items for accurate queue depth
    pending_items: AtomicUsize,
}

impl<T: Send + 'static> DynamicBatcher<T> {
    /// Create a new dynamic batcher
    pub fn new(config: BatcherConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.max_batch * 4);

        Self {
            config,
            queue_depth: AtomicUsize::new(0),
            gpu_util: AtomicUsize::new(0),
            sender,
            receiver,
            pending_items: AtomicUsize::new(0),
        }
    }

    /// FIX BUG-H051: Increment pending count when items are queued
    /// Callers should use this to track queue depth accurately
    pub fn increment_pending(&self) {
        let new_count = self.pending_items.fetch_add(1, Ordering::SeqCst) + 1;
        self.queue_depth.store(new_count, Ordering::SeqCst);
    }

    /// Get the sender for adding items to the batch queue
    pub fn sender(&self) -> mpsc::Sender<T> {
        self.sender.clone()
    }

    /// Calculate optimal batch size based on current conditions
    pub fn optimal_size(&self) -> usize {
        let queue_depth = self.queue_depth.load(Ordering::SeqCst);
        let gpu_util = self.gpu_util.load(Ordering::SeqCst) as f32 / 100.0;

        // Linear interpolation based on queue depth
        let depth_factor = (queue_depth as f32 / 1000.0).min(1.0);
        let base_size = self.config.min_batch as f32
            + depth_factor * (self.config.max_batch - self.config.min_batch) as f32;

        // Reduce by 50% if GPU utilization is high (gpu_util is 0.0-1.0)
        let adjusted = if gpu_util > 0.8 {
            base_size * 0.5
        } else {
            base_size
        };

        (adjusted as usize)
            .max(self.config.min_batch)
            .min(self.config.max_batch)
    }

    /// Update queue depth metric
    pub fn update_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::SeqCst);
    }

    /// Update GPU utilization metric (0.0-1.0, where 1.0 = 100%)
    pub fn update_gpu_util(&self, util: f32) {
        // Store as integer percentage (0-100) for atomic storage
        self.gpu_util.store((util * 100.0) as usize, Ordering::SeqCst);
    }

    /// Collect a batch of items
    pub async fn collect_batch(&mut self) -> Vec<T> {
        let batch_size = self.optimal_size();
        let mut batch = Vec::with_capacity(batch_size);

        // Try to fill the batch
        let timeout_duration = Duration::from_millis(self.config.timeout_ms);

        loop {
            if batch.len() >= batch_size {
                break;
            }

            match timeout(timeout_duration, self.receiver.recv()).await {
                Ok(Some(item)) => {
                    batch.push(item);
                }
                Ok(None) => {
                    // Channel closed
                    break;
                }
                Err(_) => {
                    // Timeout - return what we have
                    break;
                }
            }
        }

        // FIX BUG-H051: Decrement pending count by items collected, not set to batch.len()
        // Previously, queue_depth was incorrectly set to batch.len() (items collected)
        // instead of the remaining queue items. This caused the adaptive sizing algorithm
        // to make decisions based on wrong queue state.
        let collected = batch.len();
        let remaining = self.pending_items.fetch_sub(collected, Ordering::SeqCst).saturating_sub(collected);
        self.queue_depth.store(remaining, Ordering::SeqCst);

        debug!(
            batch_size = collected,
            optimal = batch_size,
            remaining_queue = remaining,
            "Collected batch"
        );

        batch
    }

    /// Get current queue depth
    pub fn current_queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::SeqCst)
    }
}

/// Batch statistics
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub current_size: usize,
    pub optimal_size: usize,
    pub queue_depth: usize,
    pub gpu_util: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_size_low_load() {
        let batcher: DynamicBatcher<String> = DynamicBatcher::new(BatcherConfig {
            min_batch: 16,
            max_batch: 64,
            timeout_ms: 100,
        });

        batcher.update_queue_depth(0);
        batcher.update_gpu_util(0.0);

        // Low load should give minimum batch size
        assert_eq!(batcher.optimal_size(), 16);
    }

    #[test]
    fn test_optimal_size_high_load() {
        let batcher: DynamicBatcher<String> = DynamicBatcher::new(BatcherConfig {
            min_batch: 16,
            max_batch: 64,
            timeout_ms: 100,
        });

        batcher.update_queue_depth(1000);
        batcher.update_gpu_util(0.5); // 50% GPU utilization

        // High queue depth should give maximum batch size
        assert_eq!(batcher.optimal_size(), 64);
    }

    #[test]
    fn test_optimal_size_high_gpu() {
        let batcher: DynamicBatcher<String> = DynamicBatcher::new(BatcherConfig {
            min_batch: 16,
            max_batch: 64,
            timeout_ms: 100,
        });

        batcher.update_queue_depth(1000);
        batcher.update_gpu_util(0.9); // 90% GPU utilization (above 80% threshold)

        // High GPU util should reduce batch size by 50%
        let size = batcher.optimal_size();
        assert!(size <= 32); // max 64 * 0.5 = 32
    }
}
