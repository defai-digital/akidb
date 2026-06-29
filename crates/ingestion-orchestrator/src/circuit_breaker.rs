//! Circuit Breaker for Python Parser Fault Isolation
//!
//! Implements the circuit breaker pattern to prevent cascading failures
//! when the Python parser service becomes unavailable or slow.

use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;
use tracing::{info, warn};

use crate::config::CircuitBreakerConfig;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    /// Circuit is closed, requests flow through normally
    Closed = 0,
    /// Circuit is open, requests are rejected
    Open = 1,
    /// Circuit is half-open, limited requests allowed to test recovery
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(v: u8) -> Self {
        match v {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Circuit breaker for fault isolation
pub struct CircuitBreaker {
    state: AtomicU8,
    failure_count: AtomicUsize,
    success_count: AtomicUsize,
    last_failure_time: AtomicU64,
    config: CircuitBreakerConfig,
    start_time: Instant,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        let config = normalize_config(config);
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            last_failure_time: AtomicU64::new(0),
            config,
            start_time: Instant::now(),
        }
    }

    /// Get the current state of the circuit breaker
    pub fn state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::SeqCst))
    }

    /// Check if a request is allowed through
    ///
    /// FIX: Use compare_exchange for atomic state transitions to prevent race conditions
    /// where multiple threads could simultaneously transition states or exceed half-open limits.
    pub fn allow_request(&self) -> bool {
        loop {
            let current_state = self.state.load(Ordering::SeqCst);

            match CircuitState::from(current_state) {
                CircuitState::Closed => return true,
                CircuitState::Open => {
                    // Check if reset timeout has elapsed
                    let last_failure = self.last_failure_time.load(Ordering::SeqCst);
                    let current_time = self.start_time.elapsed().as_secs();

                    // Guard against underflow if last_failure is somehow in the future
                    let elapsed = current_time.saturating_sub(last_failure);

                    if elapsed >= self.config.reset_timeout_secs {
                        // Atomically transition to half-open - only one thread wins
                        match self.state.compare_exchange(
                            CircuitState::Open as u8,
                            CircuitState::HalfOpen as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {
                                // We won the race - set success counter to 1 since we're allowing this request
                                // This ensures record_success() can properly track when enough requests succeed
                                self.success_count.store(1, Ordering::SeqCst);
                                info!("Circuit breaker transitioning to half-open");
                                return true;
                            }
                            Err(_) => {
                                // Another thread changed state - retry the loop
                                continue;
                            }
                        }
                    } else {
                        return false;
                    }
                }
                CircuitState::HalfOpen => loop {
                    let previous = self.success_count.load(Ordering::SeqCst);
                    if previous >= self.config.half_open_max_calls {
                        return false;
                    }

                    match self.success_count.compare_exchange(
                        previous,
                        previous + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => return true,
                        Err(_) => continue,
                    }
                },
            }
        }
    }

    /// Record a successful request
    ///
    /// FIX: Use compare_exchange for atomic state transitions
    pub fn record_success(&self) {
        loop {
            let current_state = self.state.load(Ordering::SeqCst);

            match CircuitState::from(current_state) {
                CircuitState::Closed => {
                    // Reset failure count on success
                    self.failure_count.store(0, Ordering::SeqCst);
                    return;
                }
                CircuitState::HalfOpen => {
                    // Note: success_count was already incremented in allow_request()
                    // We track successful completions here to determine when to close
                    let successes = self.success_count.load(Ordering::SeqCst);

                    if successes >= self.config.half_open_max_calls {
                        // Try to atomically transition back to closed
                        match self.state.compare_exchange(
                            CircuitState::HalfOpen as u8,
                            CircuitState::Closed as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {
                                self.failure_count.store(0, Ordering::SeqCst);
                                info!("Circuit breaker closing after successful test");
                                return;
                            }
                            Err(_) => {
                                // State changed, retry
                                continue;
                            }
                        }
                    }
                    return;
                }
                CircuitState::Open => {
                    // Shouldn't normally happen, but handle gracefully
                    // Try to transition to closed, but don't force it
                    let _ = self.state.compare_exchange(
                        CircuitState::Open as u8,
                        CircuitState::Closed as u8,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                    return;
                }
            }
        }
    }

    /// Record a failed request
    ///
    /// FIX: Use compare_exchange for atomic state transitions
    pub fn record_failure(&self) {
        let previous_failures = self
            .failure_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_add(1))
            })
            .unwrap_or_else(|current| current);
        let failures = previous_failures.saturating_add(1);
        self.last_failure_time
            .store(self.start_time.elapsed().as_secs(), Ordering::SeqCst);

        loop {
            let current_state = self.state.load(Ordering::SeqCst);

            match CircuitState::from(current_state) {
                CircuitState::Closed => {
                    if failures >= self.config.failure_threshold {
                        // Try to atomically transition to Open
                        match self.state.compare_exchange(
                            CircuitState::Closed as u8,
                            CircuitState::Open as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {
                                warn!(
                                    failures,
                                    threshold = self.config.failure_threshold,
                                    "Circuit breaker opening"
                                );
                                return;
                            }
                            Err(_) => {
                                // State changed, but that's ok - someone else opened it
                                // or successes reset the count
                                return;
                            }
                        }
                    }
                    return;
                }
                CircuitState::HalfOpen => {
                    // Any failure in half-open state reopens the circuit
                    match self.state.compare_exchange(
                        CircuitState::HalfOpen as u8,
                        CircuitState::Open as u8,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            warn!("Circuit breaker reopening after failure in half-open state");
                            return;
                        }
                        Err(_) => {
                            // State changed, retry to handle properly
                            continue;
                        }
                    }
                }
                CircuitState::Open => {
                    // Already open, nothing to do
                    return;
                }
            }
        }
    }

    /// Get circuit breaker statistics
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            state: self.state(),
            failure_count: self.failure_count.load(Ordering::SeqCst),
            success_count: self.success_count.load(Ordering::SeqCst),
        }
    }

    /// Reset the circuit breaker to closed state
    pub fn reset(&self) {
        self.state
            .store(CircuitState::Closed as u8, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
        info!("Circuit breaker reset to closed");
    }
}

fn normalize_config(config: CircuitBreakerConfig) -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        failure_threshold: config.failure_threshold.max(1),
        half_open_max_calls: config.half_open_max_calls.max(1),
        ..config
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub failure_count: usize,
    pub success_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout_secs: 1,
            half_open_max_calls: 1,
        }
    }

    #[test]
    fn test_initial_state_is_closed() {
        let cb = CircuitBreaker::new(test_config());
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_opens_after_threshold_failures() {
        let cb = CircuitBreaker::new(test_config());

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let cb = CircuitBreaker::new(test_config());

        cb.record_failure();
        cb.record_failure();
        cb.record_success();

        // Failure count should be reset
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_reset() {
        let cb = CircuitBreaker::new(test_config());

        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_zero_thresholds_are_normalized() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 0,
            reset_timeout_secs: 0,
            half_open_max_calls: 0,
        });

        assert!(cb.allow_request());
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(
            cb.allow_request(),
            "zero half-open max calls should normalize to one"
        );
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_failure_count_saturates_without_wrapping() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: usize::MAX,
            reset_timeout_secs: 1,
            half_open_max_calls: 1,
        });
        cb.failure_count.store(usize::MAX, Ordering::SeqCst);

        cb.record_failure();

        assert_eq!(cb.failure_count.load(Ordering::SeqCst), usize::MAX);
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_half_open_success_count_does_not_wrap() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout_secs: 1,
            half_open_max_calls: usize::MAX,
        });
        cb.state
            .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
        cb.success_count.store(usize::MAX, Ordering::SeqCst);

        assert!(!cb.allow_request());
        assert_eq!(cb.success_count.load(Ordering::SeqCst), usize::MAX);
    }
}
