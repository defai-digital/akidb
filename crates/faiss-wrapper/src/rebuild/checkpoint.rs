//! Checkpoint management for rebuild operations
//!
//! Provides utilities for creating, storing, and restoring checkpoints
//! during index rebuild operations.

use super::persistent_state::{RebuildCheckpoint, RebuildStateRecord};
use std::path::PathBuf;
use tracing::{debug, info};

/// Configuration for checkpoint management
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Directory for checkpoint files
    pub checkpoint_dir: PathBuf,
    /// Interval for checkpointing (number of vectors)
    pub checkpoint_interval: u64,
    /// Whether to compress checkpoint data
    pub compress: bool,
    /// Maximum checkpoint age before cleanup (seconds)
    pub max_checkpoint_age_secs: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_dir: PathBuf::from("/tmp/akidb/checkpoints"),
            checkpoint_interval: 100_000,
            compress: true,
            max_checkpoint_age_secs: 24 * 60 * 60, // 24 hours
        }
    }
}

/// Checkpoint manager for rebuild operations
pub struct CheckpointManager {
    config: CheckpointConfig,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(config: CheckpointConfig) -> Self {
        Self { config }
    }

    /// Create a checkpoint directory if it doesn't exist
    pub async fn ensure_checkpoint_dir(&self) -> Result<(), String> {
        tokio::fs::create_dir_all(&self.config.checkpoint_dir)
            .await
            .map_err(|e| format!("Failed to create checkpoint directory: {}", e))
    }

    /// Get checkpoint file path for an operation
    pub fn checkpoint_path(&self, operation_id: &str) -> PathBuf {
        self.config
            .checkpoint_dir
            .join(format!("{}.checkpoint", operation_id))
    }

    /// Get exported vectors path for an operation
    pub fn exported_vectors_path(&self, operation_id: &str) -> PathBuf {
        self.config
            .checkpoint_dir
            .join(format!("{}.vectors", operation_id))
    }

    /// Check if a checkpoint should be created
    pub fn should_checkpoint(&self, vectors_processed: u64) -> bool {
        vectors_processed > 0 && vectors_processed.is_multiple_of(self.config.checkpoint_interval)
    }

    /// Create a checkpoint for the current rebuild state
    pub fn create_checkpoint(
        &self,
        record: &RebuildStateRecord,
        last_processed_id: i64,
        vectors_exported: u64,
        total_vectors: u64,
        wal_entries_replayed: u64,
    ) -> RebuildCheckpoint {
        let temp_index_path = match &record.phase {
            super::persistent_state::PersistentRebuildPhase::Building {
                temp_index_path, ..
            } => temp_index_path.clone(),
            _ => String::new(),
        };

        RebuildCheckpoint {
            last_processed_id,
            temp_index_path,
            vectors_exported,
            total_vectors,
            wal_entries_replayed,
            exported_vectors_path: Some(
                self.exported_vectors_path(&record.operation_id)
                    .to_string_lossy()
                    .to_string(),
            ),
        }
    }

    /// Save vector data to checkpoint file
    pub async fn save_vector_checkpoint(
        &self,
        operation_id: &str,
        vectors: &[(i64, Vec<f32>)],
    ) -> Result<String, String> {
        let path = self.exported_vectors_path(operation_id);

        // Serialize vectors
        let data = bincode::serialize(vectors)
            .map_err(|e| format!("Failed to serialize vectors: {}", e))?;

        // Optionally compress
        let final_data = if self.config.compress {
            compress_data(&data)?
        } else {
            data
        };

        // Write atomically (write to temp, then rename)
        let temp_path = path.with_extension("tmp");
        tokio::fs::write(&temp_path, &final_data)
            .await
            .map_err(|e| format!("Failed to write checkpoint: {}", e))?;

        tokio::fs::rename(&temp_path, &path)
            .await
            .map_err(|e| format!("Failed to rename checkpoint: {}", e))?;

        debug!(
            operation_id,
            vectors_count = vectors.len(),
            size_bytes = final_data.len(),
            "Saved vector checkpoint"
        );

        Ok(path.to_string_lossy().to_string())
    }

    /// Load vector data from checkpoint file
    pub async fn load_vector_checkpoint(
        &self,
        operation_id: &str,
    ) -> Result<Vec<(i64, Vec<f32>)>, String> {
        let path = self.exported_vectors_path(operation_id);

        if !path.exists() {
            return Err(format!("Checkpoint file not found: {:?}", path));
        }

        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| format!("Failed to read checkpoint: {}", e))?;

        // Decompress if needed
        let decompressed = if self.config.compress {
            decompress_data(&data)?
        } else {
            data
        };

        let vectors: Vec<(i64, Vec<f32>)> = bincode::deserialize(&decompressed)
            .map_err(|e| format!("Failed to deserialize vectors: {}", e))?;

        info!(
            operation_id,
            vectors_count = vectors.len(),
            "Loaded vector checkpoint"
        );

        Ok(vectors)
    }

    /// Clean up checkpoint files for an operation
    pub async fn cleanup_checkpoint(&self, operation_id: &str) -> Result<(), String> {
        let checkpoint_path = self.checkpoint_path(operation_id);
        let vectors_path = self.exported_vectors_path(operation_id);

        for path in [checkpoint_path, vectors_path] {
            if path.exists() {
                tokio::fs::remove_file(&path)
                    .await
                    .map_err(|e| format!("Failed to remove checkpoint file {:?}: {}", path, e))?;
                debug!(path = ?path, "Removed checkpoint file");
            }
        }

        Ok(())
    }

    /// Clean up old checkpoint files
    pub async fn cleanup_old_checkpoints(&self) -> Result<u32, String> {
        if !self.config.checkpoint_dir.exists() {
            return Ok(0);
        }

        let mut entries = tokio::fs::read_dir(&self.config.checkpoint_dir)
            .await
            .map_err(|e| format!("Failed to read checkpoint directory: {}", e))?;

        let max_age = std::time::Duration::from_secs(self.config.max_checkpoint_age_secs);
        let now = std::time::SystemTime::now();
        let mut cleaned = 0;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| format!("Failed to read entry: {}", e))?
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let metadata = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(_) => continue,
            };

            let modified = match metadata.modified() {
                Ok(t) => t,
                Err(_) => continue,
            };

            let age = now.duration_since(modified).unwrap_or_default();
            if age > max_age && tokio::fs::remove_file(&path).await.is_ok() {
                cleaned += 1;
                debug!(path = ?path, age_secs = age.as_secs(), "Cleaned up old checkpoint");
            }
        }

        if cleaned > 0 {
            info!(cleaned, "Cleaned up old checkpoint files");
        }

        Ok(cleaned)
    }
}

/// Compress data using LZ4 (if available) or just return as-is
fn compress_data(data: &[u8]) -> Result<Vec<u8>, String> {
    // Simple compression marker + data
    // In production, use LZ4 or similar
    let mut result = vec![0x01]; // Marker byte indicating "compressed"
    result.extend_from_slice(data);
    Ok(result)
}

/// Decompress data
fn decompress_data(data: &[u8]) -> Result<Vec<u8>, String> {
    if data.is_empty() {
        return Err("Empty checkpoint data".to_string());
    }

    match data[0] {
        0x01 => Ok(data[1..].to_vec()), // "Compressed" (actually just stripped marker)
        0x00 => Ok(data[1..].to_vec()), // Uncompressed
        _ => Err("Invalid checkpoint format".to_string()),
    }
}

/// Resource-aware rebuild scheduler
pub struct ResourceAwareScheduler {
    /// Maximum P95 latency threshold (ms) - defer rebuild if exceeded
    pub latency_threshold_ms: u64,
    /// Minimum idle time between checks (ms)
    pub check_interval_ms: u64,
    /// Current P95 latency callback
    latency_fn: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl ResourceAwareScheduler {
    /// Create a new resource-aware scheduler
    pub fn new<F>(latency_fn: F) -> Self
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        Self {
            latency_threshold_ms: 40, // Default: defer if P95 > 40ms
            check_interval_ms: 1000,
            latency_fn: Box::new(latency_fn),
        }
    }

    /// Configure latency threshold
    pub fn with_latency_threshold(mut self, threshold_ms: u64) -> Self {
        self.latency_threshold_ms = threshold_ms;
        self
    }

    /// Check if it's safe to proceed with rebuild
    pub fn can_proceed(&self) -> bool {
        let current_latency = (self.latency_fn)();
        current_latency < self.latency_threshold_ms
    }

    /// Wait until it's safe to proceed
    pub async fn wait_until_safe(&self) -> bool {
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 60; // Max 1 minute wait

        while !self.can_proceed() && attempts < MAX_ATTEMPTS {
            debug!(
                current_latency_ms = (self.latency_fn)(),
                threshold_ms = self.latency_threshold_ms,
                "Waiting for system to be less busy"
            );
            tokio::time::sleep(std::time::Duration::from_millis(self.check_interval_ms)).await;
            attempts += 1;
        }

        self.can_proceed()
    }

    /// Check if rebuild should be paused
    pub fn should_pause(&self) -> bool {
        !self.can_proceed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_config_defaults() {
        let config = CheckpointConfig::default();
        assert_eq!(config.checkpoint_interval, 100_000);
        assert!(config.compress);
    }

    #[test]
    fn test_should_checkpoint() {
        let manager = CheckpointManager::new(CheckpointConfig {
            checkpoint_interval: 1000,
            ..Default::default()
        });

        assert!(!manager.should_checkpoint(0));
        assert!(!manager.should_checkpoint(500));
        assert!(manager.should_checkpoint(1000));
        assert!(!manager.should_checkpoint(1500));
        assert!(manager.should_checkpoint(2000));
    }

    #[test]
    fn test_compress_decompress() {
        let original = vec![1, 2, 3, 4, 5];
        let compressed = compress_data(&original).unwrap();
        let decompressed = decompress_data(&compressed).unwrap();
        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_resource_scheduler() {
        let latency = std::sync::atomic::AtomicU64::new(20);
        let scheduler =
            ResourceAwareScheduler::new(move || latency.load(std::sync::atomic::Ordering::Relaxed))
                .with_latency_threshold(40);

        assert!(scheduler.can_proceed());

        // Simulate high load
        let latency = std::sync::atomic::AtomicU64::new(50);
        let scheduler =
            ResourceAwareScheduler::new(move || latency.load(std::sync::atomic::Ordering::Relaxed))
                .with_latency_threshold(40);

        assert!(!scheduler.can_proceed());
    }
}
