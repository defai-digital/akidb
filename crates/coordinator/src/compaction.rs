//! Compaction scheduler for automatic index rebuilds
//!
//! This module provides a background task that monitors tombstone ratios
//! and triggers index rebuilds when thresholds are exceeded.

use chrono::{Datelike, Timelike};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info};

/// Compaction scheduler configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Tombstone ratio threshold to trigger compaction (default: 0.10 = 10%)
    pub tombstone_threshold: f64,
    /// How often to check if compaction is needed
    pub check_interval: Duration,
    /// Minimum interval between compactions
    pub min_compaction_interval: Duration,
    /// Whether to enable automatic compaction
    pub auto_compaction_enabled: bool,
    /// Maximum concurrent compactions (for multi-shard)
    pub max_concurrent_compactions: usize,
    /// Time window for scheduled compaction (hour of day, 0-23)
    pub scheduled_hour: Option<u8>,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            tombstone_threshold: 0.10,
            check_interval: Duration::from_secs(60), // Check every minute
            min_compaction_interval: Duration::from_secs(3600), // At least 1 hour between compactions
            auto_compaction_enabled: true,
            max_concurrent_compactions: 1,
            scheduled_hour: Some(3), // 3 AM
        }
    }
}

/// Compaction trigger reason
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionTrigger {
    /// Triggered by tombstone threshold exceeded
    TombstoneThreshold { ratio: f64 },
    /// Triggered by scheduled time
    Scheduled,
    /// Triggered manually via API
    Manual,
    /// Triggered by vector count threshold
    VectorCount { count: u64 },
}

impl std::fmt::Display for CompactionTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionTrigger::TombstoneThreshold { ratio } => {
                write!(f, "tombstone_threshold({:.1}%)", ratio * 100.0)
            }
            CompactionTrigger::Scheduled => write!(f, "scheduled"),
            CompactionTrigger::Manual => write!(f, "manual"),
            CompactionTrigger::VectorCount { count } => write!(f, "vector_count({})", count),
        }
    }
}

/// Compaction event sent to the scheduler
#[derive(Debug)]
pub enum CompactionEvent {
    /// Check if compaction is needed
    Check,
    /// Trigger immediate compaction
    TriggerNow(CompactionTrigger),
    /// Update tombstone stats
    UpdateStats {
        shard_id: String,
        tombstone_ratio: f64,
        total_vectors: u64,
    },
    /// Compaction completed
    CompactionCompleted { shard_id: String, success: bool },
    /// Shutdown the scheduler
    Shutdown,
}

/// Compaction statistics
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Total compactions triggered
    pub total_triggered: u64,
    /// Successful compactions
    pub successful: u64,
    /// Failed compactions
    pub failed: u64,
    /// Last compaction time
    pub last_compaction: Option<Instant>,
    /// Last check time
    pub last_check: Option<Instant>,
    /// Currently compacting
    pub in_progress: bool,
}

/// Shard compaction state
#[derive(Debug, Clone)]
struct ShardState {
    shard_id: String,
    tombstone_ratio: f64,
    total_vectors: u64,
    last_compaction: Option<Instant>,
    compacting: bool,
    /// FIX BUG-058: Track whether stats need refresh after compaction
    /// If true, the tombstone_ratio is stale and should be ignored until
    /// fresh stats are received via UpdateStats event
    stats_stale: bool,
}

/// Compaction scheduler handle
///
/// Use this to interact with the background compaction scheduler
pub struct CompactionScheduler {
    /// Channel to send events to the scheduler
    tx: mpsc::Sender<CompactionEvent>,
    /// Configuration
    config: CompactionConfig,
    /// Statistics
    stats: Arc<parking_lot::RwLock<CompactionStats>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl CompactionScheduler {
    /// Create a new compaction scheduler
    ///
    /// Returns the scheduler handle and a receiver for compaction triggers
    pub fn new(config: CompactionConfig) -> (Self, mpsc::Receiver<(String, CompactionTrigger)>) {
        let (tx, event_rx) = mpsc::channel(100);
        let (trigger_tx, trigger_rx) = mpsc::channel(10);
        let stats = Arc::new(parking_lot::RwLock::new(CompactionStats::default()));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn background task
        let config_clone = config.clone();
        let stats_clone = stats.clone();
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move {
            run_scheduler(
                config_clone,
                event_rx,
                trigger_tx,
                stats_clone,
                shutdown_clone,
            )
            .await;
        });

        (
            Self {
                tx,
                config,
                stats,
                shutdown,
            },
            trigger_rx,
        )
    }

    /// Trigger an immediate compaction check
    pub async fn check_now(&self) -> Result<(), mpsc::error::SendError<CompactionEvent>> {
        self.tx.send(CompactionEvent::Check).await
    }

    /// Trigger immediate compaction
    pub async fn trigger_compaction(
        &self,
        trigger: CompactionTrigger,
    ) -> Result<(), mpsc::error::SendError<CompactionEvent>> {
        self.tx.send(CompactionEvent::TriggerNow(trigger)).await
    }

    /// Update shard statistics
    pub async fn update_shard_stats(
        &self,
        shard_id: String,
        tombstone_ratio: f64,
        total_vectors: u64,
    ) -> Result<(), mpsc::error::SendError<CompactionEvent>> {
        self.tx
            .send(CompactionEvent::UpdateStats {
                shard_id,
                tombstone_ratio,
                total_vectors,
            })
            .await
    }

    /// Report compaction completed
    pub async fn compaction_completed(
        &self,
        shard_id: String,
        success: bool,
    ) -> Result<(), mpsc::error::SendError<CompactionEvent>> {
        self.tx
            .send(CompactionEvent::CompactionCompleted { shard_id, success })
            .await
    }

    /// Get current statistics
    pub fn stats(&self) -> CompactionStats {
        self.stats.read().clone()
    }

    /// Get configuration
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    /// Shutdown the scheduler
    pub async fn shutdown(&self) -> Result<(), mpsc::error::SendError<CompactionEvent>> {
        self.shutdown.store(true, Ordering::Release);
        self.tx.send(CompactionEvent::Shutdown).await
    }
}

/// Background scheduler task
async fn run_scheduler(
    config: CompactionConfig,
    mut event_rx: mpsc::Receiver<CompactionEvent>,
    trigger_tx: mpsc::Sender<(String, CompactionTrigger)>,
    stats: Arc<parking_lot::RwLock<CompactionStats>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut check_interval = interval(config.check_interval);
    let mut shard_states: std::collections::HashMap<String, ShardState> =
        std::collections::HashMap::new();

    // FIX BUG-005: Track last scheduled compaction date to ensure once-per-day execution
    // Using Option<(year, ordinal_day)> for date-based tracking
    let mut last_scheduled_date: Option<(i32, u32)> = None;

    info!(
        threshold = config.tombstone_threshold,
        check_interval_secs = config.check_interval.as_secs(),
        "Compaction scheduler started"
    );

    loop {
        tokio::select! {
            // Periodic check
            _ = check_interval.tick() => {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }

                if !config.auto_compaction_enabled {
                    continue;
                }

                // Update last check time
                {
                    let mut stats = stats.write();
                    stats.last_check = Some(Instant::now());
                }

                // Check each shard
                // FIX BUG-058: Also skip shards with stale stats (waiting for fresh data)
                let shards_needing_compaction: Vec<_> = shard_states
                    .values()
                    .filter(|s| {
                        !s.compacting
                            && !s.stats_stale // Don't trigger based on stale data
                            && s.tombstone_ratio >= config.tombstone_threshold
                            && s.last_compaction
                                .map(|t| t.elapsed() >= config.min_compaction_interval)
                                .unwrap_or(true)
                    })
                    .cloned()
                    .collect();

                // FIX BUG-006: Count currently compacting inside the loop
                // to properly enforce the limit as compactions start
                for shard in shards_needing_compaction {
                    // Recount inside loop to enforce limit properly
                    let currently_compacting = shard_states.values().filter(|s| s.compacting).count();
                    if currently_compacting >= config.max_concurrent_compactions {
                        debug!(
                            max = config.max_concurrent_compactions,
                            "Max concurrent compactions reached"
                        );
                        break;
                    }

                    let trigger = CompactionTrigger::TombstoneThreshold {
                        ratio: shard.tombstone_ratio,
                    };

                    info!(
                        shard_id = %shard.shard_id,
                        tombstone_ratio = shard.tombstone_ratio,
                        "Triggering compaction"
                    );

                    if let Err(e) = trigger_tx.send((shard.shard_id.clone(), trigger)).await {
                        error!(error = %e, "Failed to send compaction trigger");
                    } else {
                        // Mark as compacting
                        if let Some(state) = shard_states.get_mut(&shard.shard_id) {
                            state.compacting = true;
                        }

                        let mut stats = stats.write();
                        stats.total_triggered += 1;
                        stats.in_progress = true;
                    }
                }

                // FIX BUG-005: Use date-based tracking for scheduled compaction
                // Track (year, ordinal_day) to ensure we only trigger once per day
                // This is robust against scheduler delays - if we miss the exact hour,
                // we'll still trigger when we first check during that day after the scheduled hour
                if let Some(scheduled_hour) = config.scheduled_hour {
                    let now = chrono::Local::now();
                    let today = (now.year(), now.ordinal());
                    let already_ran_today = last_scheduled_date == Some(today);

                    // Trigger if: past scheduled hour AND haven't run today yet
                    if !already_ran_today && now.hour() as u8 >= scheduled_hour {
                        info!(
                            scheduled_hour = scheduled_hour,
                            current_hour = now.hour(),
                            "Triggering scheduled compaction"
                        );

                        // Trigger scheduled compaction for all shards
                        for (shard_id, state) in &mut shard_states {
                            if !state.compacting {
                                let trigger = CompactionTrigger::Scheduled;
                                if let Err(e) = trigger_tx.send((shard_id.clone(), trigger)).await {
                                    error!(error = %e, "Failed to send scheduled compaction trigger");
                                } else {
                                    state.compacting = true;
                                }
                            }
                        }

                        // Mark that we ran scheduled compaction today
                        last_scheduled_date = Some(today);
                    }
                }
            }

            // Handle events
            Some(event) = event_rx.recv() => {
                match event {
                    CompactionEvent::Check => {
                        debug!("Manual compaction check requested");
                        let mut stats = stats.write();
                        stats.last_check = Some(Instant::now());
                    }

                    CompactionEvent::TriggerNow(trigger) => {
                        info!(trigger = %trigger, "Manual compaction triggered");

                        // Trigger for all shards not currently compacting
                        for (shard_id, state) in &mut shard_states {
                            if !state.compacting {
                                if let Err(e) = trigger_tx.send((shard_id.clone(), trigger.clone())).await {
                                    error!(error = %e, shard = %shard_id, "Failed to send compaction trigger");
                                } else {
                                    state.compacting = true;
                                    let mut stats = stats.write();
                                    stats.total_triggered += 1;
                                    stats.in_progress = true;
                                }
                            }
                        }
                    }

                    CompactionEvent::UpdateStats {
                        shard_id,
                        tombstone_ratio,
                        total_vectors,
                    } => {
                        debug!(
                            shard_id = %shard_id,
                            tombstone_ratio,
                            total_vectors,
                            "Updated shard stats"
                        );

                        // FIX BUG-058: Clear stats_stale flag when fresh stats arrive
                        shard_states
                            .entry(shard_id.clone())
                            .and_modify(|s| {
                                s.tombstone_ratio = tombstone_ratio;
                                s.total_vectors = total_vectors;
                                s.stats_stale = false; // Fresh stats received
                            })
                            .or_insert(ShardState {
                                shard_id,
                                tombstone_ratio,
                                total_vectors,
                                last_compaction: None,
                                compacting: false,
                                stats_stale: false,
                            });
                    }

                    CompactionEvent::CompactionCompleted { shard_id, success } => {
                        info!(
                            shard_id = %shard_id,
                            success,
                            "Compaction completed"
                        );

                        if let Some(state) = shard_states.get_mut(&shard_id) {
                            state.compacting = false;
                            state.last_compaction = Some(Instant::now());
                            // FIX BUG-058: Don't assume tombstone_ratio is 0 after compaction
                            // New tombstones may have been added during compaction, or
                            // compaction may not have removed all tombstones.
                            // Instead, mark stats as stale so we wait for fresh data
                            // before triggering another compaction.
                            if success {
                                state.stats_stale = true; // Wait for fresh stats
                            }
                        }

                        let mut stats = stats.write();
                        if success {
                            stats.successful += 1;
                        } else {
                            stats.failed += 1;
                        }
                        stats.last_compaction = Some(Instant::now());
                        stats.in_progress = shard_states.values().any(|s| s.compacting);
                    }

                    CompactionEvent::Shutdown => {
                        info!("Compaction scheduler shutting down");
                        break;
                    }
                }
            }
        }
    }

    info!("Compaction scheduler stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scheduler_creation() {
        let config = CompactionConfig {
            check_interval: Duration::from_millis(100),
            auto_compaction_enabled: false,
            ..Default::default()
        };

        let (scheduler, _rx) = CompactionScheduler::new(config);

        // Should be able to get stats
        let stats = scheduler.stats();
        assert_eq!(stats.total_triggered, 0);
        assert!(!stats.in_progress);

        // Shutdown
        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_update_shard_stats() {
        let config = CompactionConfig {
            check_interval: Duration::from_millis(100),
            auto_compaction_enabled: false,
            ..Default::default()
        };

        let (scheduler, _rx) = CompactionScheduler::new(config);

        // Update stats
        scheduler
            .update_shard_stats("shard-1".to_string(), 0.15, 10000)
            .await
            .unwrap();

        // Give scheduler time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_trigger() {
        let config = CompactionConfig {
            check_interval: Duration::from_secs(60), // Long interval
            auto_compaction_enabled: true,
            scheduled_hour: None, // Disable scheduled to avoid race with manual trigger
            ..Default::default()
        };

        let (scheduler, mut rx) = CompactionScheduler::new(config);

        // Add a shard
        scheduler
            .update_shard_stats("shard-1".to_string(), 0.05, 10000)
            .await
            .unwrap();

        // Give time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Trigger manual compaction
        scheduler
            .trigger_compaction(CompactionTrigger::Manual)
            .await
            .unwrap();

        // Should receive trigger
        let trigger = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
        assert!(trigger.is_ok());
        if let Ok(Some((shard_id, trigger))) = trigger {
            assert_eq!(shard_id, "shard-1");
            assert_eq!(trigger, CompactionTrigger::Manual);
        }

        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_tombstone_threshold_trigger() {
        let config = CompactionConfig {
            check_interval: Duration::from_millis(50), // Fast checks
            tombstone_threshold: 0.10,
            min_compaction_interval: Duration::from_millis(0),
            auto_compaction_enabled: true,
            ..Default::default()
        };

        let (scheduler, mut rx) = CompactionScheduler::new(config);

        // Add shard with high tombstone ratio
        scheduler
            .update_shard_stats("shard-1".to_string(), 0.15, 10000)
            .await
            .unwrap();

        // Should receive trigger due to threshold
        let trigger = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(trigger.is_ok());
        if let Ok(Some((shard_id, trigger))) = trigger {
            assert_eq!(shard_id, "shard-1");
            assert!(matches!(
                trigger,
                CompactionTrigger::TombstoneThreshold { .. }
            ));
        }

        scheduler.shutdown().await.unwrap();
    }
}
