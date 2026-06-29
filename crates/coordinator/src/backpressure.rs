//! Backpressure handling for the coordinator
//!
//! This module implements:
//! - In-flight request tracking
//! - Rate limiting
//! - Queue depth limits
//! - Graceful degradation under load

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Configuration for backpressure handling
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum concurrent requests
    pub max_concurrent: usize,
    /// Maximum requests per second (0 = unlimited)
    pub rate_limit_rps: u64,
    /// Time window for rate limiting
    pub rate_window: Duration,
    /// Queue depth before rejecting (0 = no queue limit)
    pub max_queue_depth: usize,
    /// Whether to shed load when overloaded
    pub enable_load_shedding: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 1000,
            rate_limit_rps: 0, // Unlimited by default
            rate_window: Duration::from_secs(1),
            max_queue_depth: 5000,
            enable_load_shedding: true,
        }
    }
}

fn sanitize_config(mut config: BackpressureConfig) -> BackpressureConfig {
    config.max_concurrent = config
        .max_concurrent
        .clamp(1, Semaphore::MAX_PERMITS);
    config
}

/// Error returned when request is rejected due to backpressure
#[derive(Debug, Clone)]
pub enum BackpressureError {
    /// Too many concurrent requests
    TooManyConcurrent { current: usize, max: usize },
    /// Rate limit exceeded
    RateLimitExceeded { current_rps: u64, max_rps: u64 },
    /// Queue depth exceeded
    QueueDepthExceeded { current: usize, max: usize },
}

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyConcurrent { current, max } => {
                write!(f, "Too many concurrent requests: {}/{}", current, max)
            }
            Self::RateLimitExceeded { current_rps, max_rps } => {
                write!(f, "Rate limit exceeded: {} RPS (max: {})", current_rps, max_rps)
            }
            Self::QueueDepthExceeded { current, max } => {
                write!(f, "Queue depth exceeded: {}/{}", current, max)
            }
        }
    }
}

impl std::error::Error for BackpressureError {}

/// Guard that tracks an in-flight request
pub struct RequestGuard<'a> {
    controller: &'a BackpressureController,
    #[allow(dead_code)]
    permit: tokio::sync::SemaphorePermit<'a>,
}

impl<'a> Drop for RequestGuard<'a> {
    fn drop(&mut self) {
        self.controller.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Rate limiter using sliding window
struct RateLimiter {
    /// Requests in current window
    count: AtomicU64,
    /// Window start time
    window_start: parking_lot::Mutex<Instant>,
    /// Window duration
    window: Duration,
    /// Max requests per window
    max_requests: u64,
}

impl RateLimiter {
    fn new(max_rps: u64, window: Duration) -> Self {
        // FIX BUG-077: Use milliseconds for precision with sub-second windows
        // Previous code used as_secs() which truncates sub-second durations,
        // causing incorrect max_requests for windows < 1 second
        let window_ms = window.as_millis().max(1) as u64;
        let max_requests = requests_per_window(max_rps, window_ms);
        Self {
            count: AtomicU64::new(0),
            window_start: parking_lot::Mutex::new(Instant::now()),
            window,
            // Calculate max requests for the window using millisecond precision
            // For a 1-second window at 1000 RPS, this is 1000 * 1000 / 1000 = 1000
            // For a 500ms window at 1000 RPS, this is 1000 * 500 / 1000 = 500
            max_requests,
        }
    }

    fn try_acquire(&self) -> Result<(), (u64, u64)> {
        if self.max_requests == 0 {
            return Ok(()); // Unlimited
        }

        let mut window_start = self.window_start.lock();
        let now = Instant::now();

        // Check if we need to reset the window
        if now.duration_since(*window_start) >= self.window {
            *window_start = now;
            self.count.store(1, Ordering::Release);
            return Ok(());
        }

        // Try to increment within the window
        let current = self.count.fetch_add(1, Ordering::AcqRel);
        if current >= self.max_requests {
            self.count.fetch_sub(1, Ordering::AcqRel);
            let elapsed = now.duration_since(*window_start);
            // Use milliseconds for better precision when elapsed is < 1 second
            let elapsed_ms = elapsed.as_millis().max(1) as u64;
            let current_rps = (current * 1000) / elapsed_ms;
            // FIX BUG-077: Use milliseconds for consistent precision when reporting max_rps
            let window_ms = self.window.as_millis().max(1) as u64;
            let max_rps = (self.max_requests * 1000) / window_ms;
            return Err((current_rps, max_rps));
        }

        Ok(())
    }

    fn current_rate(&self) -> u64 {
        let window_start = self.window_start.lock();
        let elapsed = Instant::now().duration_since(*window_start);
        let count = self.count.load(Ordering::Acquire);

        // Use milliseconds for better precision when elapsed is < 1 second
        let elapsed_ms = elapsed.as_millis().max(1) as u64;
        (count * 1000) / elapsed_ms
    }
}

fn requests_per_window(max_rps: u64, window_ms: u64) -> u64 {
    if max_rps == 0 {
        0
    } else {
        max_rps.saturating_mul(window_ms).saturating_div(1000).max(1)
    }
}

/// Controls backpressure for incoming requests
pub struct BackpressureController {
    /// Semaphore for limiting concurrent requests
    semaphore: Semaphore,
    /// Current in-flight requests
    in_flight: AtomicUsize,
    /// Current queue depth (waiting requests)
    queue_depth: AtomicUsize,
    /// Rate limiter
    rate_limiter: RateLimiter,
    /// Configuration
    config: BackpressureConfig,
    /// Total accepted requests
    accepted: AtomicU64,
    /// Total rejected requests
    rejected: AtomicU64,
    /// Total shed requests (when load shedding)
    shed: AtomicU64,
}

impl BackpressureController {
    /// Create a new backpressure controller with default config
    pub fn new() -> Self {
        Self::with_config(BackpressureConfig::default())
    }

    /// Create a new backpressure controller with custom config
    pub fn with_config(config: BackpressureConfig) -> Self {
        let config = sanitize_config(config);
        Self {
            semaphore: Semaphore::new(config.max_concurrent),
            in_flight: AtomicUsize::new(0),
            queue_depth: AtomicUsize::new(0),
            rate_limiter: RateLimiter::new(config.rate_limit_rps, config.rate_window),
            config,
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            shed: AtomicU64::new(0),
        }
    }

    /// Try to acquire a request permit
    ///
    /// Returns a guard that releases the permit when dropped.
    /// Returns an error if backpressure thresholds are exceeded.
    pub async fn try_acquire(&self) -> Result<RequestGuard<'_>, BackpressureError> {
        // Check rate limit first
        if let Err((current_rps, max_rps)) = self.rate_limiter.try_acquire() {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(BackpressureError::RateLimitExceeded { current_rps, max_rps });
        }

        // Check queue depth
        // FIX BUG-063: Note on atomics - fetch_add returns the PREVIOUS value.
        // We check if the previous depth was already at max, meaning our addition
        // would exceed it. This is correct behavior. The queue_depth metric may
        // momentarily show inflated values during the increment-check-rollback window,
        // but this is acceptable for a monitoring metric. Using compare_exchange
        // would be stricter but could cause livelock under extreme load.
        let queue_depth = self.queue_depth.fetch_add(1, Ordering::AcqRel);
        if self.config.max_queue_depth > 0 && queue_depth >= self.config.max_queue_depth {
            self.queue_depth.fetch_sub(1, Ordering::AcqRel);
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(BackpressureError::QueueDepthExceeded {
                current: queue_depth + 1, // FIX BUG-063: Report the actual depth after our add
                max: self.config.max_queue_depth,
            });
        }

        // Try to acquire semaphore permit (non-blocking check for load shedding)
        let permit = if self.config.enable_load_shedding {
            match self.semaphore.try_acquire() {
                Ok(permit) => {
                    self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                    permit
                }
                Err(_) => {
                    // FIX BUG-080: Properly implement load shedding by rejecting
                    // immediately when semaphore is full, and increment shed counter.
                    // Previously this fell through to blocking wait, which defeats
                    // the purpose of load shedding.
                    self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                    self.shed.fetch_add(1, Ordering::Relaxed);
                    self.rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(BackpressureError::TooManyConcurrent {
                        current: self.in_flight.load(Ordering::Acquire),
                        max: self.config.max_concurrent,
                    });
                }
            }
        } else {
            // Blocking wait
            let permit = self.semaphore.acquire().await.map_err(|_| {
                self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                self.rejected.fetch_add(1, Ordering::Relaxed);
                BackpressureError::TooManyConcurrent {
                    current: self.in_flight.load(Ordering::Acquire),
                    max: self.config.max_concurrent,
                }
            })?;
            self.queue_depth.fetch_sub(1, Ordering::AcqRel);
            permit
        };

        self.in_flight.fetch_add(1, Ordering::AcqRel);
        self.accepted.fetch_add(1, Ordering::Relaxed);

        Ok(RequestGuard {
            controller: self,
            permit,
        })
    }

    /// Get current statistics
    pub fn stats(&self) -> BackpressureStats {
        BackpressureStats {
            in_flight: self.in_flight.load(Ordering::Acquire),
            queue_depth: self.queue_depth.load(Ordering::Acquire),
            current_rps: self.rate_limiter.current_rate(),
            total_accepted: self.accepted.load(Ordering::Relaxed),
            total_rejected: self.rejected.load(Ordering::Relaxed),
            total_shed: self.shed.load(Ordering::Relaxed),
            max_concurrent: self.config.max_concurrent,
            max_queue_depth: self.config.max_queue_depth,
            rate_limit_rps: self.config.rate_limit_rps,
        }
    }

    /// Check if the system is under pressure
    pub fn is_under_pressure(&self) -> bool {
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let queue_depth = self.queue_depth.load(Ordering::Acquire);

        // Under pressure if using more than 80% of concurrent capacity
        // or if queue is more than 50% full
        let concurrent_pressure_threshold = self.config.max_concurrent.saturating_mul(8) / 10;
        in_flight > concurrent_pressure_threshold
            || (self.config.max_queue_depth > 0
                && queue_depth > (self.config.max_queue_depth / 2))
    }

    /// Get current load as a percentage (0-100+)
    pub fn load_percentage(&self) -> u32 {
        let in_flight = self.in_flight.load(Ordering::Acquire) as u128;
        let max = self.config.max_concurrent as u128;
        if max == 0 {
            return 0;
        }
        let percentage = in_flight.saturating_mul(100) / max;
        percentage.min(u32::MAX as u128) as u32
    }
}

impl Default for BackpressureController {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about backpressure state
#[derive(Debug, Clone)]
pub struct BackpressureStats {
    /// Current in-flight requests
    pub in_flight: usize,
    /// Current queue depth
    pub queue_depth: usize,
    /// Current requests per second
    pub current_rps: u64,
    /// Total accepted requests
    pub total_accepted: u64,
    /// Total rejected requests
    pub total_rejected: u64,
    /// Total shed requests
    pub total_shed: u64,
    /// Maximum concurrent requests allowed
    pub max_concurrent: usize,
    /// Maximum queue depth allowed
    pub max_queue_depth: usize,
    /// Rate limit in RPS
    pub rate_limit_rps: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_acquire_release() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: 2,
            rate_limit_rps: 0,
            max_queue_depth: 10,
            ..Default::default()
        });

        let _guard1 = controller.try_acquire().await.unwrap();
        assert_eq!(controller.in_flight.load(Ordering::Acquire), 1);

        let _guard2 = controller.try_acquire().await.unwrap();
        assert_eq!(controller.in_flight.load(Ordering::Acquire), 2);

        drop(_guard1);
        assert_eq!(controller.in_flight.load(Ordering::Acquire), 1);

        drop(_guard2);
        assert_eq!(controller.in_flight.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: 100,
            rate_limit_rps: 1000,
            max_queue_depth: 500,
            ..Default::default()
        });

        let _guard = controller.try_acquire().await.unwrap();
        let stats = controller.stats();

        assert_eq!(stats.in_flight, 1);
        assert_eq!(stats.total_accepted, 1);
        assert_eq!(stats.total_rejected, 0);
        assert_eq!(stats.max_concurrent, 100);
    }

    #[tokio::test]
    async fn test_load_percentage() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: 100,
            rate_limit_rps: 0,
            max_queue_depth: 0,
            ..Default::default()
        });

        assert_eq!(controller.load_percentage(), 0);

        let _guard1 = controller.try_acquire().await.unwrap();
        assert_eq!(controller.load_percentage(), 1);

        // Acquire 49 more
        let mut guards = vec![_guard1];
        for _ in 0..49 {
            guards.push(controller.try_acquire().await.unwrap());
        }

        assert_eq!(controller.load_percentage(), 50);
    }

    #[test]
    fn test_rate_limiter_nonzero_rps_small_window_is_not_unlimited() {
        let limiter = RateLimiter::new(1, Duration::from_millis(1));

        assert_eq!(limiter.max_requests, 1);
        assert!(limiter.try_acquire().is_ok());
        assert!(
            limiter.try_acquire().is_err(),
            "nonzero RPS must not be rounded down to unlimited"
        );
    }

    #[test]
    fn test_rate_limiter_zero_rps_remains_unlimited() {
        let limiter = RateLimiter::new(0, Duration::from_millis(1));

        assert_eq!(limiter.max_requests, 0);
        assert!(limiter.try_acquire().is_ok());
        assert!(limiter.try_acquire().is_ok());
    }

    #[tokio::test]
    async fn test_zero_max_concurrent_is_sanitized() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: 0,
            rate_limit_rps: 0,
            max_queue_depth: 0,
            enable_load_shedding: false,
            ..Default::default()
        });

        assert_eq!(controller.stats().max_concurrent, 1);

        let guard = tokio::time::timeout(Duration::from_secs(1), controller.try_acquire())
            .await
            .expect("zero max_concurrent should not hang after normalization")
            .expect("normalized controller should accept one request");

        assert_eq!(controller.stats().in_flight, 1);
        drop(guard);
        assert_eq!(controller.stats().in_flight, 0);
    }

    #[test]
    fn test_is_under_pressure_extreme_max_concurrent_does_not_overflow() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: usize::MAX,
            rate_limit_rps: 0,
            max_queue_depth: 0,
            enable_load_shedding: false,
            ..Default::default()
        });

        assert_eq!(controller.stats().max_concurrent, Semaphore::MAX_PERMITS);
        assert!(!controller.is_under_pressure());
    }

    #[test]
    fn test_load_percentage_extreme_max_concurrent_does_not_overflow() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: usize::MAX,
            rate_limit_rps: 0,
            max_queue_depth: 0,
            enable_load_shedding: false,
            ..Default::default()
        });

        controller
            .in_flight
            .store(Semaphore::MAX_PERMITS, Ordering::Release);

        assert_eq!(controller.load_percentage(), 100);
    }

    #[test]
    fn test_is_under_pressure() {
        let controller = BackpressureController::with_config(BackpressureConfig {
            max_concurrent: 10,
            rate_limit_rps: 0,
            max_queue_depth: 0,
            enable_load_shedding: false,
            ..Default::default()
        });

        assert!(!controller.is_under_pressure());

        // Simulate 9 in-flight requests (90%)
        controller.in_flight.store(9, Ordering::Release);
        assert!(controller.is_under_pressure());
    }
}
