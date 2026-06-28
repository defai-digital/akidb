//! Backpressure Controller for AkiDB Latency-Aware Throttling
//!
//! Monitors AkiDB insert latency and pauses ingestion when
//! the database is under pressure.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn, debug};

use crate::config::BackpressureConfig;

/// Backpressure controller for AkiDB latency awareness
pub struct BackpressureController {
    /// Whether backpressure is currently active
    active: AtomicBool,

    /// Current observed latency (microseconds)
    current_latency_us: AtomicU64,

    /// Current queue depth
    queue_depth: AtomicU64,

    /// Configuration
    config: BackpressureConfig,
}

impl BackpressureController {
    /// Create a new backpressure controller
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            active: AtomicBool::new(false),
            current_latency_us: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            config,
        }
    }

    /// Check if backpressure is currently active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Update the observed latency and check if backpressure should activate
    pub fn update_latency(&self, latency_us: u64) {
        self.current_latency_us.store(latency_us, Ordering::SeqCst);

        let threshold_us = self.config.latency_threshold_ms.saturating_mul(1000);

        if latency_us > threshold_us {
            if !self.active.swap(true, Ordering::SeqCst) {
                warn!(
                    latency_ms = latency_us / 1000,
                    threshold_ms = self.config.latency_threshold_ms,
                    "Backpressure activated due to high latency"
                );
            }
        } else if self.active.load(Ordering::SeqCst) {
            // Check if we should deactivate
            if latency_us < threshold_us / 2 {
                self.active.store(false, Ordering::SeqCst);
                info!(
                    latency_ms = latency_us / 1000,
                    "Backpressure deactivated, latency recovered"
                );
            }
        }
    }

    /// Update the queue depth and check if backpressure should activate/deactivate
    ///
    /// FIX: Added deactivation logic when queue depth drops below low water mark
    pub fn update_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth as u64, Ordering::SeqCst);

        if depth > self.config.queue_depth_high_water {
            if !self.active.swap(true, Ordering::SeqCst) {
                warn!(
                    depth,
                    high_water = self.config.queue_depth_high_water,
                    "Backpressure activated due to high queue depth"
                );
            }
        } else if self.active.load(Ordering::SeqCst) && depth < self.config.queue_depth_low_water {
            // Deactivate when queue drains below low water mark
            self.active.store(false, Ordering::SeqCst);
            info!(
                depth,
                low_water = self.config.queue_depth_low_water,
                "Backpressure deactivated, queue depth recovered"
            );
        }
    }

    /// Wait if backpressure is active
    pub async fn wait_if_active(&self) {
        if self.is_active() {
            debug!(
                pause_secs = self.config.pause_duration_secs,
                "Waiting due to backpressure"
            );
            sleep(Duration::from_secs(self.config.pause_duration_secs)).await;
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> BackpressureStats {
        BackpressureStats {
            active: self.is_active(),
            current_latency_us: self.current_latency_us.load(Ordering::SeqCst),
            queue_depth: self.queue_depth.load(Ordering::SeqCst),
        }
    }

    /// Force activate backpressure (for testing or manual intervention)
    pub fn activate(&self) {
        self.active.store(true, Ordering::SeqCst);
        info!("Backpressure manually activated");
    }

    /// Force deactivate backpressure
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::SeqCst);
        info!("Backpressure manually deactivated");
    }
}

/// Backpressure statistics
#[derive(Debug, Clone)]
pub struct BackpressureStats {
    pub active: bool,
    pub current_latency_us: u64,
    pub queue_depth: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> BackpressureConfig {
        BackpressureConfig {
            latency_threshold_ms: 500,
            queue_depth_high_water: 1000,
            queue_depth_low_water: 500,
            pause_duration_secs: 1,
        }
    }

    #[test]
    fn test_initial_state() {
        let bp = BackpressureController::new(test_config());
        assert!(!bp.is_active());
    }

    #[test]
    fn test_activates_on_high_latency() {
        let bp = BackpressureController::new(test_config());

        bp.update_latency(100_000); // 100ms - below threshold
        assert!(!bp.is_active());

        bp.update_latency(600_000); // 600ms - above threshold
        assert!(bp.is_active());
    }

    #[test]
    fn test_activates_on_high_queue_depth() {
        let bp = BackpressureController::new(test_config());

        bp.update_queue_depth(500);
        assert!(!bp.is_active());

        bp.update_queue_depth(1500);
        assert!(bp.is_active());
    }

    #[test]
    fn test_deactivates_on_queue_depth_recovery() {
        let bp = BackpressureController::new(test_config());

        // Activate due to high queue depth
        bp.update_queue_depth(1500);
        assert!(bp.is_active());

        // Still active when above low water mark
        bp.update_queue_depth(600);
        assert!(bp.is_active());

        // Deactivates when below low water mark (500)
        bp.update_queue_depth(400);
        assert!(!bp.is_active());
    }

    #[test]
    fn test_deactivates_on_recovery() {
        let bp = BackpressureController::new(test_config());

        bp.update_latency(600_000); // Activate
        assert!(bp.is_active());

        bp.update_latency(200_000); // Below half threshold
        assert!(!bp.is_active());
    }

    #[test]
    fn test_manual_control() {
        let bp = BackpressureController::new(test_config());

        bp.activate();
        assert!(bp.is_active());

        bp.deactivate();
        assert!(!bp.is_active());
    }

    #[test]
    fn test_latency_threshold_conversion_saturates() {
        let bp = BackpressureController::new(BackpressureConfig {
            latency_threshold_ms: u64::MAX,
            queue_depth_high_water: 1000,
            queue_depth_low_water: 500,
            pause_duration_secs: 1,
        });

        bp.update_latency(1);
        assert!(!bp.is_active());
    }
}
