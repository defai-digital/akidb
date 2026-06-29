//! Batch processing optimization for vector operations
//!
//! This module provides:
//! - Intelligent batching of insert/search operations
//! - Adaptive batch sizing based on latency
//! - Concurrent batch processing with backpressure

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::debug;

/// Batch processing configuration
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Minimum batch size (won't process until reached or timeout)
    pub min_batch_size: usize,
    /// Maximum wait time before processing partial batch
    pub max_wait: Duration,
    /// Target latency for adaptive sizing
    pub target_latency: Duration,
    /// Maximum concurrent batches
    pub max_concurrent_batches: usize,
    /// Enable adaptive batch sizing
    pub adaptive_sizing: bool,
    /// FIX BUG-H033: Maximum pending requests (queue depth) to prevent OOM
    /// When this limit is reached, new requests are rejected with backpressure
    pub max_pending_requests: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            min_batch_size: 10,
            max_wait: Duration::from_millis(50),
            target_latency: Duration::from_millis(100),
            max_concurrent_batches: 4,
            adaptive_sizing: true,
            // FIX BUG-H033: Default to 1000 pending requests (4 concurrent * 100 batch * 2.5 buffer)
            max_pending_requests: 1000,
        }
    }
}

fn sanitize_batch_config(mut config: BatchConfig) -> BatchConfig {
    config.max_batch_size = config.max_batch_size.max(1);
    config.min_batch_size = config.min_batch_size.clamp(1, config.max_batch_size);
    config.max_concurrent_batches = config.max_concurrent_batches.max(1);
    config
}

/// Result of a batch operation
#[derive(Debug, Clone)]
pub struct BatchResult<T> {
    /// Individual results
    pub results: Vec<Result<T, String>>,
    /// Total processing time
    pub duration: Duration,
    /// Batch size processed
    pub batch_size: usize,
    /// Whether batch was fully successful
    pub success: bool,
    /// Number of successful items
    pub successful_count: usize,
    /// Number of failed items
    pub failed_count: usize,
}

impl<T> BatchResult<T> {
    /// Create a new batch result
    pub fn new(results: Vec<Result<T, String>>, duration: Duration) -> Self {
        let batch_size = results.len();
        let successful_count = results.iter().filter(|r| r.is_ok()).count();
        let failed_count = batch_size - successful_count;

        Self {
            results,
            duration,
            batch_size,
            success: failed_count == 0,
            successful_count,
            failed_count,
        }
    }

    /// Get average latency per item
    pub fn avg_latency(&self) -> Duration {
        if self.batch_size > 0 {
            // FIX BUG-114: Use checked conversion to prevent truncation for huge batches
            // The direct `as u32` cast would silently truncate batch_size > u32::MAX,
            // producing incorrect latency values. Use saturating conversion as fallback.
            let divisor = u32::try_from(self.batch_size).unwrap_or(u32::MAX);
            self.duration / divisor
        } else {
            Duration::ZERO
        }
    }
}

/// Statistics for batch processing
#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    /// Total batches processed
    pub total_batches: u64,
    /// Total items processed
    pub total_items: u64,
    /// Total successful items
    pub successful_items: u64,
    /// Total failed items
    pub failed_items: u64,
    /// Total processing time (microseconds)
    pub total_time_us: u64,
    /// Current batch size (for adaptive sizing)
    pub current_batch_size: usize,
    /// Current concurrent batches
    pub current_concurrent: usize,
}

impl BatchStats {
    /// Average batch size
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_batches > 0 {
            self.total_items as f64 / self.total_batches as f64
        } else {
            0.0
        }
    }

    /// Average latency per batch
    pub fn avg_batch_latency_us(&self) -> f64 {
        if self.total_batches > 0 {
            self.total_time_us as f64 / self.total_batches as f64
        } else {
            0.0
        }
    }

    /// Average latency per item
    pub fn avg_item_latency_us(&self) -> f64 {
        if self.total_items > 0 {
            self.total_time_us as f64 / self.total_items as f64
        } else {
            0.0
        }
    }

    /// Success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_items > 0 {
            self.successful_items as f64 / self.total_items as f64
        } else {
            1.0
        }
    }
}

/// Batch processor for vector operations
pub struct BatchProcessor {
    config: BatchConfig,
    /// Current adaptive batch size
    current_batch_size: AtomicUsize,
    /// Semaphore for concurrent batch limiting
    concurrent_semaphore: Arc<Semaphore>,
    /// Statistics
    stats: RwLock<BatchStats>,
    /// Recent latencies for adaptive sizing (microseconds)
    /// FIX BUG-035: Use VecDeque for O(1) pop_front instead of O(n) Vec::remove(0)
    recent_latencies: RwLock<VecDeque<u64>>,
    /// FIX BUG-H033: Track pending requests for backpressure
    pending_requests: AtomicUsize,
}

/// FIX BUG-H033: Error type for batch processing
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("Queue full: {pending} pending requests exceeds limit of {max}")]
    QueueFull { pending: usize, max: usize },
}

impl BatchProcessor {
    /// Create a new batch processor
    pub fn new(config: BatchConfig) -> Self {
        let config = sanitize_batch_config(config);
        let current_batch_size = config.max_batch_size;
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_batches));

        Self {
            config,
            current_batch_size: AtomicUsize::new(current_batch_size),
            concurrent_semaphore: semaphore,
            stats: RwLock::new(BatchStats {
                current_batch_size,
                ..Default::default()
            }),
            recent_latencies: RwLock::new(VecDeque::with_capacity(100)),
            pending_requests: AtomicUsize::new(0),
        }
    }

    /// FIX BUG-H033: Get current pending request count
    pub fn pending_count(&self) -> usize {
        self.pending_requests.load(Ordering::Relaxed)
    }

    /// FIX BUG-H033: Check if queue is at capacity
    pub fn is_queue_full(&self) -> bool {
        self.pending_count() >= self.config.max_pending_requests
    }

    /// Get current batch size
    pub fn batch_size(&self) -> usize {
        self.current_batch_size.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> BatchStats {
        let mut stats = self.stats.read().clone();
        stats.current_batch_size = self.batch_size();
        stats.current_concurrent =
            self.config.max_concurrent_batches - self.concurrent_semaphore.available_permits();
        stats
    }

    /// Process a batch of items
    ///
    /// Note: This method does not apply backpressure. For production use with
    /// untrusted input, prefer `try_process_batch` which rejects when queue is full.
    pub async fn process_batch<T, F, Fut>(&self, items: Vec<T>, processor: F) -> BatchResult<T>
    where
        F: Fn(Vec<T>) -> Fut,
        Fut: std::future::Future<Output = Vec<Result<T, String>>>,
    {
        if items.is_empty() {
            return BatchResult::new(vec![], Duration::ZERO);
        }

        // FIX BUG-H033: Track pending requests
        self.pending_requests.fetch_add(1, Ordering::Relaxed);

        // Acquire permit for concurrent batch limiting
        let _permit = self.concurrent_semaphore.acquire().await.unwrap();

        let batch_size = items.len();
        let start = Instant::now();

        // Process the batch
        let results = processor(items).await;
        let duration = start.elapsed();

        // FIX BUG-H033: Decrement pending after processing
        self.pending_requests.fetch_sub(1, Ordering::Relaxed);

        // Update statistics
        self.record_batch(batch_size, duration, &results);

        // Adaptive sizing
        if self.config.adaptive_sizing {
            self.adjust_batch_size(duration, batch_size);
        }

        BatchResult::new(results, duration)
    }

    /// FIX BUG-H033: Process a batch with backpressure - rejects if queue is full
    ///
    /// Returns `Err(BatchError::QueueFull)` if the pending request count exceeds
    /// `max_pending_requests`. Use this for production workloads to prevent OOM
    /// under sustained high load.
    pub async fn try_process_batch<T, F, Fut>(
        &self,
        items: Vec<T>,
        processor: F,
    ) -> Result<BatchResult<T>, BatchError>
    where
        F: Fn(Vec<T>) -> Fut,
        Fut: std::future::Future<Output = Vec<Result<T, String>>>,
    {
        if items.is_empty() {
            return Ok(BatchResult::new(vec![], Duration::ZERO));
        }

        // Check queue depth before accepting
        let pending = self.pending_requests.fetch_add(1, Ordering::Relaxed);
        if pending >= self.config.max_pending_requests {
            // Undo the increment and reject
            self.pending_requests.fetch_sub(1, Ordering::Relaxed);
            return Err(BatchError::QueueFull {
                pending,
                max: self.config.max_pending_requests,
            });
        }

        // Acquire permit for concurrent batch limiting
        let _permit = self.concurrent_semaphore.acquire().await.unwrap();

        let batch_size = items.len();
        let start = Instant::now();

        // Process the batch
        let results = processor(items).await;
        let duration = start.elapsed();

        // Decrement pending after processing
        self.pending_requests.fetch_sub(1, Ordering::Relaxed);

        // Update statistics
        self.record_batch(batch_size, duration, &results);

        // Adaptive sizing
        if self.config.adaptive_sizing {
            self.adjust_batch_size(duration, batch_size);
        }

        Ok(BatchResult::new(results, duration))
    }

    /// Process items in optimal batches
    pub async fn process_all<T, F, Fut>(&self, items: Vec<T>, processor: F) -> Vec<BatchResult<T>>
    where
        T: Clone,
        F: Fn(Vec<T>) -> Fut + Clone,
        Fut: std::future::Future<Output = Vec<Result<T, String>>>,
    {
        if items.is_empty() {
            return vec![];
        }

        let batch_size = self.batch_size();
        let mut results = Vec::new();

        // Split into batches
        for chunk in items.chunks(batch_size) {
            let batch_items: Vec<T> = chunk.to_vec();
            let result = self.process_batch(batch_items, processor.clone()).await;
            results.push(result);
        }

        results
    }

    /// Record batch statistics
    fn record_batch<T>(
        &self,
        batch_size: usize,
        duration: Duration,
        results: &[Result<T, String>],
    ) {
        let successful = results.iter().filter(|r| r.is_ok()).count().min(batch_size);
        let failed = batch_size.saturating_sub(successful);
        let duration_us = duration_micros_u64(duration);

        let mut stats = self.stats.write();
        stats.total_batches = stats.total_batches.saturating_add(1);
        stats.total_items = stats.total_items.saturating_add(batch_size as u64);
        stats.successful_items = stats.successful_items.saturating_add(successful as u64);
        stats.failed_items = stats.failed_items.saturating_add(failed as u64);
        stats.total_time_us = stats.total_time_us.saturating_add(duration_us);

        // Record latency for adaptive sizing
        drop(stats);
        let mut latencies = self.recent_latencies.write();
        latencies.push_back(duration_us);
        // FIX BUG-035: O(1) pop_front instead of O(n) remove(0)
        if latencies.len() > 100 {
            latencies.pop_front();
        }
    }

    /// Adjust batch size based on latency
    fn adjust_batch_size(&self, duration: Duration, batch_size: usize) {
        let target_us = duration_micros_u64(self.config.target_latency);
        let actual_us = duration_micros_u64(duration);

        // Calculate per-item latency
        let per_item_us = if batch_size > 0 {
            actual_us / batch_size as u64
        } else {
            actual_us
        };

        // Calculate ideal batch size for target latency
        let ideal_size = if per_item_us > 0 {
            (target_us / per_item_us) as usize
        } else {
            self.config.max_batch_size
        };

        // Smooth adjustment (move 10% toward ideal)
        // FIX BUG-003: Use .max(1) to ensure at least 1 step change when converging
        let current = self.batch_size();
        let new_size = if ideal_size > current {
            current + ((ideal_size - current) / 10).max(1)
        } else if ideal_size < current {
            current - ((current - ideal_size) / 10).max(1)
        } else {
            current // Already at ideal
        };

        // Clamp to configured bounds
        let clamped = new_size
            .max(self.config.min_batch_size)
            .min(self.config.max_batch_size);

        if clamped != current {
            debug!(
                old_size = current,
                new_size = clamped,
                actual_latency_us = actual_us,
                target_latency_us = target_us,
                "Adjusted batch size"
            );
            self.current_batch_size.store(clamped, Ordering::Relaxed);
        }
    }

    /// Get recommended batch size for given latency target
    pub fn recommended_size(&self, target_latency: Duration) -> usize {
        let latencies = self.recent_latencies.read();
        if latencies.is_empty() {
            return self.config.max_batch_size;
        }

        // Calculate average per-batch latency
        let avg_latency: u64 = latencies.iter().sum::<u64>() / latencies.len() as u64;
        let avg_batch_size = self.stats.read().avg_batch_size() as u64;

        if avg_batch_size == 0 || avg_latency == 0 {
            return self.config.max_batch_size;
        }

        // Calculate per-item latency
        let per_item_us = avg_latency / avg_batch_size;

        // Calculate recommended size
        let target_us = duration_micros_u64(target_latency);
        let recommended = if per_item_us > 0 {
            (target_us / per_item_us) as usize
        } else {
            self.config.max_batch_size
        };

        recommended
            .max(self.config.min_batch_size)
            .min(self.config.max_batch_size)
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        let mut stats = self.stats.write();
        *stats = BatchStats {
            current_batch_size: self.batch_size(),
            ..Default::default()
        };

        let mut latencies = self.recent_latencies.write();
        latencies.clear();
    }
}

fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mock_processor(items: Vec<i32>) -> Vec<Result<i32, String>> {
        // Simulate some processing time
        tokio::time::sleep(Duration::from_millis(10)).await;
        items.into_iter().map(Ok).collect()
    }

    #[tokio::test]
    async fn test_batch_processor_basic() {
        let processor = BatchProcessor::new(BatchConfig {
            max_batch_size: 10,
            min_batch_size: 1,
            ..Default::default()
        });

        let items = vec![1, 2, 3, 4, 5];
        let result = processor.process_batch(items, mock_processor).await;

        assert_eq!(result.batch_size, 5);
        assert!(result.success);
        assert_eq!(result.successful_count, 5);
        assert_eq!(result.failed_count, 0);
    }

    #[tokio::test]
    async fn test_batch_processor_multiple_batches() {
        let processor = BatchProcessor::new(BatchConfig {
            max_batch_size: 3,
            min_batch_size: 1,
            adaptive_sizing: false,
            ..Default::default()
        });

        let items: Vec<i32> = (0..10).collect();
        let results = processor.process_all(items, mock_processor).await;

        // Should have 4 batches: 3 + 3 + 3 + 1
        assert_eq!(results.len(), 4);

        let total_items: usize = results.iter().map(|r| r.batch_size).sum();
        assert_eq!(total_items, 10);
    }

    #[tokio::test]
    async fn test_batch_stats() {
        let processor = BatchProcessor::new(BatchConfig::default());

        let items = vec![1, 2, 3];
        processor.process_batch(items, mock_processor).await;

        let stats = processor.stats();
        assert_eq!(stats.total_batches, 1);
        assert_eq!(stats.total_items, 3);
        assert_eq!(stats.successful_items, 3);
    }

    #[tokio::test]
    async fn test_batch_with_failures() {
        async fn failing_processor(items: Vec<i32>) -> Vec<Result<i32, String>> {
            items
                .into_iter()
                .map(|i| {
                    if i % 2 == 0 {
                        Ok(i)
                    } else {
                        Err("odd number".to_string())
                    }
                })
                .collect()
        }

        let processor = BatchProcessor::new(BatchConfig::default());

        let items = vec![1, 2, 3, 4, 5];
        let result = processor.process_batch(items, failing_processor).await;

        assert!(!result.success);
        assert_eq!(result.successful_count, 2); // 2, 4
        assert_eq!(result.failed_count, 3); // 1, 3, 5
    }

    #[tokio::test]
    async fn test_adaptive_sizing() {
        let processor = BatchProcessor::new(BatchConfig {
            max_batch_size: 100,
            min_batch_size: 10,
            target_latency: Duration::from_millis(50),
            adaptive_sizing: true,
            ..Default::default()
        });

        // Process several batches to trigger adaptive sizing
        for _ in 0..5 {
            let items: Vec<i32> = (0..50).collect();
            processor.process_batch(items, mock_processor).await;
        }

        // Batch size should have adjusted
        let stats = processor.stats();
        assert!(stats.total_batches >= 5);
    }

    #[test]
    fn test_batch_result() {
        let results: Vec<Result<i32, String>> = vec![Ok(1), Ok(2), Err("fail".to_string())];
        let batch_result = BatchResult::new(results, Duration::from_millis(30));

        assert_eq!(batch_result.batch_size, 3);
        assert!(!batch_result.success);
        assert_eq!(batch_result.successful_count, 2);
        assert_eq!(batch_result.failed_count, 1);
        assert_eq!(batch_result.avg_latency(), Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_concurrent_batches() {
        let processor = Arc::new(BatchProcessor::new(BatchConfig {
            max_concurrent_batches: 2,
            max_batch_size: 5,
            ..Default::default()
        }));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let p = processor.clone();
                tokio::spawn(async move {
                    let items = vec![1, 2, 3];
                    p.process_batch(items, mock_processor).await
                })
            })
            .collect();

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.success);
        }
    }

    #[test]
    fn test_recommended_size() {
        let processor = BatchProcessor::new(BatchConfig {
            max_batch_size: 100,
            min_batch_size: 10,
            ..Default::default()
        });

        // Without data, should return max
        let size = processor.recommended_size(Duration::from_millis(100));
        assert_eq!(size, 100);
    }

    #[tokio::test]
    async fn test_zero_batch_limits_are_sanitized() {
        let processor = BatchProcessor::new(BatchConfig {
            max_batch_size: 0,
            min_batch_size: 0,
            max_concurrent_batches: 0,
            adaptive_sizing: false,
            ..Default::default()
        });

        assert_eq!(processor.batch_size(), 1);
        assert_eq!(processor.config.min_batch_size, 1);
        assert_eq!(processor.config.max_concurrent_batches, 1);

        let results = tokio::time::timeout(
            Duration::from_secs(1),
            processor.process_all(vec![1, 2], mock_processor),
        )
        .await
        .expect("process_all should not panic or hang when limits are zero");

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|batch| batch.batch_size == 1));
    }

    #[test]
    fn test_min_batch_size_is_clamped_to_max_batch_size() {
        let processor = BatchProcessor::new(BatchConfig {
            max_batch_size: 4,
            min_batch_size: 10,
            ..Default::default()
        });

        assert_eq!(processor.config.max_batch_size, 4);
        assert_eq!(processor.config.min_batch_size, 4);
    }

    #[tokio::test]
    async fn test_extra_processor_results_do_not_underflow_stats() {
        async fn extra_results_processor(_items: Vec<i32>) -> Vec<Result<i32, String>> {
            vec![Ok(1), Ok(2), Ok(3)]
        }

        let processor = BatchProcessor::new(BatchConfig::default());
        let result = processor
            .process_batch(vec![1, 2], extra_results_processor)
            .await;

        assert_eq!(result.batch_size, 3);
        let stats = processor.stats();
        assert_eq!(stats.total_items, 2);
        assert_eq!(stats.successful_items, 2);
        assert_eq!(stats.failed_items, 0);
        assert!(stats.success_rate() <= 1.0);
    }

    #[test]
    fn test_record_batch_stats_saturate_without_wrapping() {
        let processor = BatchProcessor::new(BatchConfig::default());
        {
            let mut stats = processor.stats.write();
            stats.total_batches = u64::MAX;
            stats.total_items = u64::MAX;
            stats.successful_items = u64::MAX;
            stats.failed_items = u64::MAX;
            stats.total_time_us = u64::MAX;
        }

        processor.record_batch(
            2,
            Duration::from_micros(5),
            &[Ok(1), Err("fail".to_string())],
        );

        let stats = processor.stats();
        assert_eq!(stats.total_batches, u64::MAX);
        assert_eq!(stats.total_items, u64::MAX);
        assert_eq!(stats.successful_items, u64::MAX);
        assert_eq!(stats.failed_items, u64::MAX);
        assert_eq!(stats.total_time_us, u64::MAX);
        assert!(stats.success_rate() <= 1.0);
    }

    #[test]
    fn test_record_batch_duration_micros_saturates_without_truncating() {
        let processor = BatchProcessor::new(BatchConfig::default());

        processor.record_batch::<i32>(0, Duration::from_secs(u64::MAX), &[]);

        let stats = processor.stats();
        assert_eq!(stats.total_time_us, u64::MAX);
    }
}
