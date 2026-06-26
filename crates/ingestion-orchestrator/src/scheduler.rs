//! Ingestion Scheduler
//!
//! Hourly scheduled synchronization with MinIO to:
//! - Discover new files missed by event-driven ingestion
//! - Detect modified files (changed ETag)
//! - Track missing files for soft delete
//!
//! Features:
//! - Jitter to prevent thundering herd
//! - Mutex-based overlap prevention
//! - Checkpoint-based resumption
//! - Backpressure integration

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::Mutex;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use akidb_common::types::{ChangeType, DeleteState, DocumentIdentifier, ObjectManifest, SyncResult};
use crate::manifest::ManifestStore;
use crate::{IngestionError, Result};

/// Configuration for the ingestion scheduler
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Interval between sync runs (default: 1 hour)
    pub interval: Duration,
    /// Maximum jitter added to interval (default: 5 minutes)
    pub jitter_max: Duration,
    /// Enable manual trigger via gRPC (default: true)
    pub manual_trigger_enabled: bool,
    /// Deletion threshold (consecutive misses before soft delete)
    pub deletion_threshold: u8,
    /// Hard delete delay in days
    pub hard_delete_delay_days: u32,
    /// Enable the scheduler (can be disabled for testing)
    pub enabled: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600), // 1 hour
            jitter_max: Duration::from_secs(300), // 5 minutes
            manual_trigger_enabled: true,
            deletion_threshold: 3,
            hard_delete_delay_days: 7,
            enabled: true,
        }
    }
}

/// Sync run status
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub run_id: Uuid,
    pub started_at: Instant,
    pub status: SyncState,
    pub result: Option<SyncResult>,
}

/// State of a sync run
#[derive(Debug, Clone, PartialEq)]
pub enum SyncState {
    Running,
    Completed,
    Failed(String),
    Skipped,
}

/// Ingestion scheduler for hourly MinIO sync
pub struct IngestionScheduler {
    config: SchedulerConfig,
    run_lock: Arc<Mutex<()>>,
    manifest: Arc<ManifestStore>,
    last_run: Arc<Mutex<Option<SyncStatus>>>,
}

impl IngestionScheduler {
    /// Create a new scheduler
    pub fn new(config: SchedulerConfig, manifest: Arc<ManifestStore>) -> Self {
        Self {
            config,
            run_lock: Arc::new(Mutex::new(())),
            manifest,
            last_run: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the scheduler loop (runs indefinitely)
    pub async fn run<F, Fut>(&self, sync_fn: F) -> Result<()>
    where
        F: Fn(Arc<ManifestStore>, u64) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<SyncResult>> + Send,
    {
        if !self.config.enabled {
            info!("Scheduler disabled, not starting");
            return Ok(());
        }

        info!(
            interval_secs = self.config.interval.as_secs(),
            jitter_secs = self.config.jitter_max.as_secs(),
            "Ingestion scheduler started"
        );

        // BUG-008 FIX: Add initial random jitter so instances don't start synchronized
        if !self.config.jitter_max.is_zero() {
            let initial_jitter = rand::thread_rng().gen_range(Duration::ZERO..self.config.jitter_max);
            debug!(jitter_ms = initial_jitter.as_millis(), "Initial jitter before first sync");
            tokio::time::sleep(initial_jitter).await;
        }

        loop {
            // Execute sync
            if let Err(e) = self.execute_sync(&sync_fn).await {
                error!(?e, "Sync run failed");
            }

            // BUG-008 FIX: Add jitter to the sleep interval itself
            // This ensures instances naturally drift apart over time
            let jitter = if !self.config.jitter_max.is_zero() {
                rand::thread_rng().gen_range(Duration::ZERO..self.config.jitter_max)
            } else {
                Duration::ZERO
            };

            let sleep_duration = self.config.interval + jitter;
            debug!(
                interval_ms = self.config.interval.as_millis(),
                jitter_ms = jitter.as_millis(),
                total_ms = sleep_duration.as_millis(),
                "Sleeping until next sync"
            );
            tokio::time::sleep(sleep_duration).await;
        }
    }

    /// Manually trigger a sync run
    pub async fn trigger<F, Fut>(&self, sync_fn: F) -> Result<SyncStatus>
    where
        F: Fn(Arc<ManifestStore>, u64) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<SyncResult>> + Send,
    {
        if !self.config.manual_trigger_enabled {
            return Err(IngestionError::Scheduler("Manual trigger disabled".to_string()));
        }

        self.execute_sync(&sync_fn).await
    }

    /// Execute a single sync run
    async fn execute_sync<F, Fut>(&self, sync_fn: &F) -> Result<SyncStatus>
    where
        F: Fn(Arc<ManifestStore>, u64) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<SyncResult>> + Send,
    {
        let run_id = Uuid::now_v7();
        let started_at = Instant::now();

        info!(%run_id, "Starting sync run");

        // Try to acquire lock (non-blocking)
        let guard = match self.run_lock.try_lock() {
            Ok(g) => g,
            Err(_) => {
                warn!(%run_id, "Skipping sync - previous run still active");
                let status = SyncStatus {
                    run_id,
                    started_at,
                    status: SyncState::Skipped,
                    result: None,
                };
                return Ok(status);
            }
        };

        // Update status to running
        {
            let mut last = self.last_run.lock().await;
            *last = Some(SyncStatus {
                run_id,
                started_at,
                status: SyncState::Running,
                result: None,
            });
        }

        // BUG-003 FIX: Get the NEXT epoch value without committing
        // We use current epoch + 1 for the sync, but only commit if sync succeeds
        let current_epoch = self.manifest.current_epoch()?;
        let tentative_epoch = current_epoch + 1;
        info!(%run_id, epoch = tentative_epoch, "Starting sync with tentative epoch");

        // Execute the actual sync with tentative epoch
        let result = sync_fn(Arc::clone(&self.manifest), tentative_epoch).await;

        // Update final status
        let status = match result {
            Ok(sync_result) => {
                // BUG-003 FIX: Only increment epoch AFTER successful sync
                let committed_epoch = self.manifest.increment_epoch()?;
                debug_assert_eq!(committed_epoch, tentative_epoch, "Epoch mismatch after sync");

                info!(
                    %run_id,
                    epoch = committed_epoch,
                    duration_ms = started_at.elapsed().as_millis(),
                    new = sync_result.new_count,
                    updated = sync_result.updated_count,
                    marked = sync_result.marked_count,
                    confirmed = sync_result.confirmed_count,
                    "Sync run completed, epoch committed"
                );

                SyncStatus {
                    run_id,
                    started_at,
                    status: SyncState::Completed,
                    result: Some(sync_result),
                }
            }
            Err(e) => {
                // BUG-003 FIX: Do NOT increment epoch on failure
                error!(%run_id, ?e, epoch = tentative_epoch, "Sync run failed, epoch NOT committed");

                SyncStatus {
                    run_id,
                    started_at,
                    status: SyncState::Failed(e.to_string()),
                    result: None,
                }
            }
        };

        // Store final status
        {
            let mut last = self.last_run.lock().await;
            *last = Some(status.clone());
        }

        drop(guard);
        Ok(status)
    }

    /// Get the status of the last run
    pub async fn last_run_status(&self) -> Option<SyncStatus> {
        self.last_run.lock().await.clone()
    }

    /// Check if a sync is currently running
    pub fn is_running(&self) -> bool {
        self.run_lock.try_lock().is_err()
    }

    /// Get next scheduled run time (approximate)
    pub fn next_run_in(&self) -> Duration {
        self.config.interval
    }
}

/// Change detected during MinIO sync
#[derive(Debug, Clone)]
pub struct MinIOChange {
    pub key: String,
    pub etag: Option<String>,
    pub change_type: ChangeType,
}

/// MinIO change detector using streaming manifest comparison
pub struct ChangeDetector {
    manifest: Arc<ManifestStore>,
}

impl ChangeDetector {
    pub fn new(manifest: Arc<ManifestStore>) -> Self {
        Self { manifest }
    }

    /// Detect changes by comparing MinIO listing with manifest
    ///
    /// Both inputs should be sorted by key for efficient merge-join
    pub fn detect_changes(
        &self,
        minio_objects: Vec<MinIOObject>,
        epoch: u64,
    ) -> Result<Vec<MinIOChange>> {
        let mut changes = Vec::new();

        // Get all manifests as a sorted map
        let manifest_map: std::collections::BTreeMap<String, ObjectManifest> = self
            .manifest
            .list_all()?
            .into_iter()
            .map(|m| (m.key.clone(), m))
            .collect();

        // Track which manifest keys we've seen
        let mut seen_keys = std::collections::HashSet::new();

        // Process MinIO objects
        for obj in minio_objects {
            seen_keys.insert(obj.key.clone());

            match manifest_map.get(&obj.key) {
                None => {
                    // New object
                    changes.push(MinIOChange {
                        key: obj.key,
                        etag: obj.etag,
                        change_type: ChangeType::New,
                    });
                }
                Some(manifest) => {
                    // Check if ETag changed
                    if obj.etag.as_deref() != Some(&manifest.etag) {
                        changes.push(MinIOChange {
                            key: obj.key.clone(),
                            etag: obj.etag,
                            change_type: ChangeType::Updated,
                        });
                    }
                    // Mark as seen (will reset missing count)
                    self.manifest.mark_seen(&obj.key, epoch)?;
                }
            }
        }

        // Find missing objects (in manifest but not in MinIO)
        for (key, manifest) in &manifest_map {
            if !seen_keys.contains(key) && manifest.delete_state.is_active() {
                // Increment missing count
                let count = self.manifest.increment_missing(key)?;

                if count >= akidb_common::types::DELETION_THRESHOLD {
                    changes.push(MinIOChange {
                        key: key.clone(),
                        etag: None,
                        change_type: ChangeType::ConfirmedDelete,
                    });
                } else {
                    changes.push(MinIOChange {
                        key: key.clone(),
                        etag: None,
                        change_type: ChangeType::Missing,
                    });
                }
            }
        }

        Ok(changes)
    }
}

/// MinIO object from listing
#[derive(Debug, Clone)]
pub struct MinIOObject {
    pub key: String,
    pub etag: Option<String>,
    pub size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_manifest_store() -> Arc<ManifestStore> {
        let dir = tempdir().unwrap();
        Arc::new(ManifestStore::open(dir.path()).unwrap())
    }

    #[tokio::test]
    async fn test_scheduler_creation() {
        let manifest = create_test_manifest_store();
        let config = SchedulerConfig::default();
        let scheduler = IngestionScheduler::new(config, manifest);

        assert!(!scheduler.is_running());
        assert!(scheduler.last_run_status().await.is_none());
    }

    #[tokio::test]
    async fn test_scheduler_trigger() {
        let manifest = create_test_manifest_store();
        let config = SchedulerConfig {
            manual_trigger_enabled: true,
            ..Default::default()
        };
        let scheduler = IngestionScheduler::new(config, manifest);

        let status = scheduler
            .trigger(|_manifest, _epoch| async { Ok(SyncResult::default()) })
            .await
            .unwrap();

        assert_eq!(status.status, SyncState::Completed);
    }

    #[tokio::test]
    async fn test_scheduler_overlap_prevention() {
        let manifest = create_test_manifest_store();
        let config = SchedulerConfig::default();
        let scheduler = Arc::new(IngestionScheduler::new(config, manifest));

        // Start a long-running sync
        let scheduler_clone = Arc::clone(&scheduler);
        let handle = tokio::spawn(async move {
            scheduler_clone
                .trigger(|_m, _e| async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(SyncResult::default())
                })
                .await
        });

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Try to trigger another - should be skipped
        let status = scheduler
            .trigger(|_m, _e| async { Ok(SyncResult::default()) })
            .await
            .unwrap();

        assert_eq!(status.status, SyncState::Skipped);

        handle.await.unwrap().unwrap();
    }

    #[test]
    fn test_change_detection() {
        let manifest = create_test_manifest_store();

        // Add some existing entries
        let doc1 = DocumentIdentifier::new(b"content1", "file1.pdf".to_string());
        let m1 = ObjectManifest::new("file1.pdf".to_string(), "etag1".to_string(), doc1);
        manifest.upsert(&m1).unwrap();

        let doc2 = DocumentIdentifier::new(b"content2", "file2.pdf".to_string());
        let m2 = ObjectManifest::new("file2.pdf".to_string(), "etag2".to_string(), doc2);
        manifest.upsert(&m2).unwrap();

        let detector = ChangeDetector::new(manifest);

        // MinIO listing: file1 unchanged, file2 updated, file3 new
        let minio_objects = vec![
            MinIOObject {
                key: "file1.pdf".to_string(),
                etag: Some("etag1".to_string()),
                size: 100,
            },
            MinIOObject {
                key: "file2.pdf".to_string(),
                etag: Some("etag2-new".to_string()), // Changed!
                size: 200,
            },
            MinIOObject {
                key: "file3.pdf".to_string(), // New!
                etag: Some("etag3".to_string()),
                size: 300,
            },
        ];

        let changes = detector.detect_changes(minio_objects, 1).unwrap();

        assert_eq!(changes.len(), 2);
        assert!(changes.iter().any(|c| c.key == "file2.pdf" && c.change_type == ChangeType::Updated));
        assert!(changes.iter().any(|c| c.key == "file3.pdf" && c.change_type == ChangeType::New));
    }

    #[test]
    fn test_change_detection_missing() {
        let manifest = create_test_manifest_store();

        // Add an existing entry
        let doc = DocumentIdentifier::new(b"content", "old_file.pdf".to_string());
        let m = ObjectManifest::new("old_file.pdf".to_string(), "etag".to_string(), doc);
        manifest.upsert(&m).unwrap();

        let detector = ChangeDetector::new(Arc::clone(&manifest));

        // Empty MinIO listing - file is missing
        let minio_objects = vec![];

        let changes = detector.detect_changes(minio_objects, 1).unwrap();

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key, "old_file.pdf");
        assert_eq!(changes[0].change_type, ChangeType::Missing);

        // Missing count should have incremented
        let m = manifest.get("old_file.pdf").unwrap().unwrap();
        assert_eq!(m.missing_count, 1);
    }
}
