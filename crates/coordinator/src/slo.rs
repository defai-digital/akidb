//! SLO (Service Level Objective) Estimation API
//!
//! This module provides latency estimation for queries based on current system state,
//! allowing clients to make informed decisions about query timeouts and degraded modes.

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// SLO configuration
#[derive(Debug, Clone)]
pub struct SloConfig {
    /// Target P50 latency
    pub target_p50_ms: u64,
    /// Target P95 latency
    pub target_p95_ms: u64,
    /// Target P99 latency
    pub target_p99_ms: u64,
    /// Window size for latency samples
    pub sample_window_size: usize,
    /// Minimum samples before estimation is reliable
    pub min_samples: usize,
    /// SLO breach threshold (0.0 - 1.0)
    pub breach_threshold: f64,
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            target_p50_ms: 10,
            target_p95_ms: 50,
            target_p99_ms: 100,
            sample_window_size: 1000,
            min_samples: 100,
            breach_threshold: 0.05, // 5% breach allowed
        }
    }
}

/// SLO estimation result
#[derive(Debug, Clone)]
pub struct SloEstimate {
    /// Estimated P50 latency in milliseconds
    pub estimated_p50_ms: u64,
    /// Estimated P95 latency in milliseconds
    pub estimated_p95_ms: u64,
    /// Estimated P99 latency in milliseconds
    pub estimated_p99_ms: u64,
    /// Probability of meeting P50 SLO (0.0 - 1.0)
    pub p50_probability: f64,
    /// Probability of meeting P95 SLO (0.0 - 1.0)
    pub p95_probability: f64,
    /// Probability of meeting P99 SLO (0.0 - 1.0)
    pub p99_probability: f64,
    /// Recommended timeout in milliseconds
    pub recommended_timeout_ms: u64,
    /// Whether degraded mode is recommended
    pub recommend_degraded_mode: bool,
    /// Confidence level of the estimate (0.0 - 1.0)
    pub confidence: f64,
    /// Current system load factor
    pub load_factor: f64,
    /// Number of samples used for estimation
    pub sample_count: usize,
}

impl Default for SloEstimate {
    fn default() -> Self {
        Self {
            estimated_p50_ms: 0,
            estimated_p95_ms: 0,
            estimated_p99_ms: 0,
            p50_probability: 1.0,
            p95_probability: 1.0,
            p99_probability: 1.0,
            recommended_timeout_ms: 100,
            recommend_degraded_mode: false,
            confidence: 0.0,
            load_factor: 0.0,
            sample_count: 0,
        }
    }
}

/// Latency sample with timestamp
#[derive(Debug, Clone)]
struct LatencySample {
    latency_us: u64,
    _timestamp: Instant,
    top_k: usize,
    num_shards: usize,
}

/// SLO estimator for query latency prediction
pub struct SloEstimator {
    config: SloConfig,
    /// Recent latency samples
    samples: RwLock<VecDeque<LatencySample>>,
    /// Total queries processed
    total_queries: AtomicU64,
    /// Queries that breached P95 SLO
    p95_breaches: AtomicU64,
    /// Current concurrent requests (for load estimation)
    concurrent_requests: AtomicU64,
    /// Maximum observed concurrent requests
    max_concurrent: AtomicU64,
}

impl SloEstimator {
    /// Create a new SLO estimator
    pub fn new(config: SloConfig) -> Self {
        Self {
            samples: RwLock::new(VecDeque::with_capacity(config.sample_window_size)),
            total_queries: AtomicU64::new(0),
            p95_breaches: AtomicU64::new(0),
            concurrent_requests: AtomicU64::new(0),
            max_concurrent: AtomicU64::new(0),
            config,
        }
    }

    /// Record a latency sample
    pub fn record_latency(&self, latency_us: u64, top_k: usize, num_shards: usize) {
        let sample = LatencySample {
            latency_us,
            _timestamp: Instant::now(),
            top_k,
            num_shards,
        };

        let mut samples = self.samples.write();
        samples.push_back(sample);

        // Maintain window size
        while samples.len() > self.config.sample_window_size {
            samples.pop_front();
        }

        // Track totals
        saturating_increment_u64(&self.total_queries);

        // Track SLO breaches
        let latency_ms = latency_us / 1000;
        if latency_ms > self.config.target_p95_ms {
            saturating_increment_u64(&self.p95_breaches);
        }
    }

    /// Increment concurrent request count
    pub fn request_started(&self) {
        let current = saturating_increment_u64(&self.concurrent_requests);

        // Update max if needed
        let mut max = self.max_concurrent.load(Ordering::Relaxed);
        while current > max {
            match self.max_concurrent.compare_exchange_weak(
                max,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(m) => max = m,
            }
        }
    }

    /// Decrement concurrent request count
    /// FIX BUG-010: Use saturating_sub to prevent underflow wrapping to u64::MAX
    pub fn request_completed(&self) {
        // Atomically decrement, but don't go below 0
        let mut current = self.concurrent_requests.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                break;
            }
            match self.concurrent_requests.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Get SLO estimate for a query
    pub fn estimate(&self, top_k: usize, num_shards: usize) -> SloEstimate {
        let samples = self.samples.read();

        if samples.len() < self.config.min_samples {
            // Not enough samples, return conservative estimate
            return SloEstimate {
                estimated_p50_ms: self.config.target_p50_ms,
                estimated_p95_ms: self.config.target_p95_ms,
                estimated_p99_ms: self.config.target_p99_ms,
                p50_probability: 0.8,
                p95_probability: 0.8,
                p99_probability: 0.8,
                recommended_timeout_ms: self.config.target_p99_ms * 2,
                recommend_degraded_mode: false,
                confidence: samples.len() as f64 / self.config.min_samples as f64,
                load_factor: self.calculate_load_factor(),
                sample_count: samples.len(),
            };
        }

        // FIX BUG-007: Guard against division by zero
        let top_k_safe = top_k.max(1);
        let num_shards_safe = num_shards.max(1);

        // Filter samples by similar query parameters
        let relevant_samples: Vec<_> = samples
            .iter()
            .filter(|s| {
                // Allow some variance in parameters
                let s_top_k = s.top_k.max(1);
                let s_num_shards = s.num_shards.max(1);
                (s_top_k as f64 / top_k_safe as f64).abs() < 2.0
                    && (s_num_shards as f64 / num_shards_safe as f64).abs() < 2.0
            })
            .map(|s| s.latency_us)
            .collect();

        let sample_count = relevant_samples.len();
        if sample_count < self.config.min_samples / 2 {
            // Not enough relevant samples, use all samples
            let all_latencies: Vec<_> = samples.iter().map(|s| s.latency_us).collect();
            return self.calculate_estimate(&all_latencies, top_k, num_shards, samples.len());
        }

        self.calculate_estimate(&relevant_samples, top_k, num_shards, sample_count)
    }

    /// Calculate SLO estimate from latency samples
    fn calculate_estimate(
        &self,
        latencies: &[u64],
        _top_k: usize,
        _num_shards: usize,
        sample_count: usize,
    ) -> SloEstimate {
        if latencies.is_empty() {
            return SloEstimate::default();
        }

        let mut sorted = latencies.to_vec();
        sorted.sort_unstable();

        let len = sorted.len();
        // FIX BUG-H029: Correct percentile calculation using nearest-rank method
        // Old formula: (len * 0.99) gave index 99 for 100 samples = P100 (max), not P99
        // Correct formula: For P99 of 100 samples, we want the 99th value = index 98
        // Using nearest-rank: index = ceil(p * len) - 1, clamped to valid range
        let p50_idx = Self::percentile_index(len, 0.50);
        let p95_idx = Self::percentile_index(len, 0.95);
        let p99_idx = Self::percentile_index(len, 0.99);

        let p50_us = sorted.get(p50_idx).copied().unwrap_or(0);
        let p95_us = sorted.get(p95_idx).copied().unwrap_or(0);
        let p99_us = sorted.get(p99_idx).copied().unwrap_or(0);

        let p50_ms = p50_us / 1000;
        let p95_ms = p95_us / 1000;
        let p99_ms = p99_us / 1000;

        // Calculate probabilities of meeting SLO
        let p50_probability = self.calculate_probability(&sorted, self.config.target_p50_ms * 1000);
        let p95_probability = self.calculate_probability(&sorted, self.config.target_p95_ms * 1000);
        let p99_probability = self.calculate_probability(&sorted, self.config.target_p99_ms * 1000);

        // Load factor affects estimation
        let load_factor = self.calculate_load_factor();

        // Adjust estimates based on load
        let load_multiplier = 1.0 + (load_factor * 0.5);
        let adjusted_p95_ms = (p95_ms as f64 * load_multiplier) as u64;

        // Recommend degraded mode if SLO is at risk
        let recommend_degraded_mode =
            p95_probability < (1.0 - self.config.breach_threshold) || load_factor > 0.8;

        // Recommended timeout: P99 with load adjustment and safety margin
        let recommended_timeout_ms =
            ((p99_ms as f64 * load_multiplier * 1.5) as u64).max(self.config.target_p99_ms);

        // Confidence based on sample count
        let confidence =
            (sample_count as f64 / self.config.sample_window_size as f64).clamp(0.0, 1.0);

        SloEstimate {
            estimated_p50_ms: p50_ms,
            estimated_p95_ms: adjusted_p95_ms,
            estimated_p99_ms: p99_ms,
            p50_probability,
            p95_probability,
            p99_probability,
            recommended_timeout_ms,
            recommend_degraded_mode,
            confidence,
            load_factor,
            sample_count,
        }
    }

    /// FIX BUG-H029: Calculate correct percentile index using nearest-rank method
    ///
    /// For P99 of 100 samples, returns index 98 (the 99th value).
    /// Formula: ceil(p * n) - 1, clamped to [0, n-1]
    fn percentile_index(len: usize, percentile: f64) -> usize {
        if len == 0 {
            return 0;
        }
        let idx = ((percentile * len as f64).ceil() as usize).saturating_sub(1);
        idx.min(len - 1)
    }

    /// Calculate probability of meeting a latency target
    fn calculate_probability(&self, sorted_latencies: &[u64], target_us: u64) -> f64 {
        if sorted_latencies.is_empty() {
            return 1.0;
        }

        let under_target = sorted_latencies.iter().filter(|&&l| l <= target_us).count();
        under_target as f64 / sorted_latencies.len() as f64
    }

    /// Calculate current load factor (0.0 - 1.0)
    fn calculate_load_factor(&self) -> f64 {
        let current = self.concurrent_requests.load(Ordering::Relaxed);
        let max = self.max_concurrent.load(Ordering::Relaxed).max(1);

        (current as f64 / max as f64).min(1.0)
    }

    /// Get SLO statistics
    pub fn stats(&self) -> SloStats {
        let samples = self.samples.read();
        let total = self.total_queries.load(Ordering::Relaxed);
        let breaches = self.p95_breaches.load(Ordering::Relaxed);

        let breach_rate = if total > 0 {
            breaches as f64 / total as f64
        } else {
            0.0
        };

        // Calculate current percentiles
        let mut latencies: Vec<_> = samples.iter().map(|s| s.latency_us).collect();
        latencies.sort_unstable();

        let len = latencies.len();
        // FIX BUG-H029: Use correct percentile index calculation
        let (p50, p95, p99) = if len > 0 {
            let p50_idx = Self::percentile_index(len, 0.50);
            let p95_idx = Self::percentile_index(len, 0.95);
            let p99_idx = Self::percentile_index(len, 0.99);
            (
                latencies[p50_idx].max(1) / 1000,
                latencies[p95_idx] / 1000,
                latencies[p99_idx] / 1000,
            )
        } else {
            (0, 0, 0)
        };

        SloStats {
            total_queries: total,
            p95_breach_count: breaches,
            p95_breach_rate: breach_rate,
            current_p50_ms: p50,
            current_p95_ms: p95,
            current_p99_ms: p99,
            target_p50_ms: self.config.target_p50_ms,
            target_p95_ms: self.config.target_p95_ms,
            target_p99_ms: self.config.target_p99_ms,
            sample_count: len,
            load_factor: self.calculate_load_factor(),
            slo_compliant: breach_rate <= self.config.breach_threshold,
        }
    }

    /// Check if system is SLO compliant
    pub fn is_slo_compliant(&self) -> bool {
        let total = self.total_queries.load(Ordering::Relaxed);
        if total < self.config.min_samples as u64 {
            return true; // Not enough data
        }

        let breaches = self.p95_breaches.load(Ordering::Relaxed);
        (breaches as f64 / total as f64) <= self.config.breach_threshold
    }

    /// Get configuration
    pub fn config(&self) -> &SloConfig {
        &self.config
    }
}

fn saturating_increment_u64(counter: &AtomicU64) -> u64 {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(1);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

/// SLO statistics
#[derive(Debug, Clone)]
pub struct SloStats {
    /// Total queries processed
    pub total_queries: u64,
    /// Number of P95 SLO breaches
    pub p95_breach_count: u64,
    /// P95 breach rate (0.0 - 1.0)
    pub p95_breach_rate: f64,
    /// Current P50 latency
    pub current_p50_ms: u64,
    /// Current P95 latency
    pub current_p95_ms: u64,
    /// Current P99 latency
    pub current_p99_ms: u64,
    /// Target P50 latency
    pub target_p50_ms: u64,
    /// Target P95 latency
    pub target_p95_ms: u64,
    /// Target P99 latency
    pub target_p99_ms: u64,
    /// Number of samples in window
    pub sample_count: usize,
    /// Current load factor
    pub load_factor: f64,
    /// Whether system is SLO compliant
    pub slo_compliant: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_estimator() -> SloEstimator {
        SloEstimator::new(SloConfig {
            min_samples: 10,
            sample_window_size: 100,
            ..Default::default()
        })
    }

    #[test]
    fn test_record_latency() {
        let estimator = create_estimator();

        // Record some samples
        for i in 0..20 {
            estimator.record_latency(i * 1000, 10, 4);
        }

        let stats = estimator.stats();
        assert_eq!(stats.total_queries, 20);
        assert_eq!(stats.sample_count, 20);
    }

    #[test]
    fn test_estimate_with_few_samples() {
        let estimator = create_estimator();

        // Record fewer than min_samples
        for i in 0..5 {
            estimator.record_latency(i * 1000, 10, 4);
        }

        let estimate = estimator.estimate(10, 4);

        // Should have low confidence
        assert!(estimate.confidence < 1.0);
        assert_eq!(estimate.sample_count, 5);
    }

    #[test]
    fn test_estimate_with_enough_samples() {
        let estimator = create_estimator();

        // Record enough samples with varying latencies
        for i in 0..100 {
            let latency = (i % 50) * 1000; // 0-49ms
            estimator.record_latency(latency, 10, 4);
        }

        let estimate = estimator.estimate(10, 4);

        assert!(estimate.confidence > 0.5);
        assert!(estimate.estimated_p50_ms < 50);
        assert!(estimate.estimated_p95_ms < 100);
    }

    #[test]
    fn test_load_factor() {
        let estimator = create_estimator();

        // Simulate load
        for _ in 0..5 {
            estimator.request_started();
        }

        let load1 = estimator.calculate_load_factor();
        assert!(load1 > 0.0);

        // Complete some requests
        for _ in 0..3 {
            estimator.request_completed();
        }

        let load2 = estimator.calculate_load_factor();
        assert!(load2 < load1);
    }

    #[test]
    fn test_slo_compliance() {
        let estimator = SloEstimator::new(SloConfig {
            min_samples: 10,
            target_p95_ms: 50,
            breach_threshold: 0.10, // 10% allowed
            ..Default::default()
        });

        // Record mostly good latencies
        for _ in 0..90 {
            estimator.record_latency(30_000, 10, 4); // 30ms
        }

        // Record some breaches
        for _ in 0..10 {
            estimator.record_latency(60_000, 10, 4); // 60ms (breach)
        }

        // Should be compliant (10% breach = threshold)
        assert!(estimator.is_slo_compliant());

        // Add more breaches
        for _ in 0..5 {
            estimator.record_latency(60_000, 10, 4);
        }

        // Now should not be compliant
        assert!(!estimator.is_slo_compliant());
    }

    #[test]
    fn test_degraded_mode_recommendation() {
        let estimator = SloEstimator::new(SloConfig {
            min_samples: 10,
            target_p95_ms: 50,
            ..Default::default()
        });

        // Record high latencies
        for _ in 0..20 {
            estimator.record_latency(100_000, 10, 4); // 100ms
        }

        let estimate = estimator.estimate(10, 4);

        // Should recommend degraded mode since P95 is way over target
        assert!(estimate.recommend_degraded_mode);
    }

    #[test]
    fn test_recommended_timeout() {
        let estimator = SloEstimator::new(SloConfig {
            min_samples: 10,
            target_p99_ms: 100,
            ..Default::default()
        });

        // Record consistent latencies
        for _ in 0..50 {
            estimator.record_latency(40_000, 10, 4); // 40ms
        }

        let estimate = estimator.estimate(10, 4);

        // Recommended timeout should be reasonable
        assert!(estimate.recommended_timeout_ms >= 40);
        assert!(estimate.recommended_timeout_ms >= estimator.config().target_p99_ms);
    }

    #[test]
    fn test_latency_counters_saturate_without_wrapping() {
        let estimator = create_estimator();
        estimator.total_queries.store(u64::MAX, Ordering::Relaxed);
        estimator.p95_breaches.store(u64::MAX, Ordering::Relaxed);

        estimator.record_latency(100_000, 10, 4);

        let stats = estimator.stats();
        assert_eq!(stats.total_queries, u64::MAX);
        assert_eq!(stats.p95_breach_count, u64::MAX);
        assert_eq!(stats.sample_count, 1);
    }

    #[test]
    fn test_request_started_saturates_without_wrapping() {
        let estimator = create_estimator();
        estimator
            .concurrent_requests
            .store(u64::MAX, Ordering::Relaxed);
        estimator
            .max_concurrent
            .store(u64::MAX - 1, Ordering::Relaxed);

        estimator.request_started();

        assert_eq!(
            estimator.concurrent_requests.load(Ordering::Relaxed),
            u64::MAX
        );
        assert_eq!(estimator.max_concurrent.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(estimator.calculate_load_factor(), 1.0);

        estimator.request_completed();

        assert_eq!(
            estimator.concurrent_requests.load(Ordering::Relaxed),
            u64::MAX - 1
        );
    }
}
