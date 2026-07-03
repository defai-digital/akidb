//! Snapshot storage for AkiDB
//!
//! This module provides snapshot storage backends for persisting index state
//! to various storage systems including local filesystem and S3-compatible
//! object stores (like MinIO).
//!
//! ## Features
//!
//! - **Multiple backends**: Local filesystem and S3/MinIO support
//! - **Crash-safe state machine**: Operations persist state to RocksDB
//! - **Resumable uploads**: Multipart uploads with checkpoint recovery
//! - **Cleanup utilities**: Automatic orphan and old snapshot cleanup
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SnapshotManager                          │
//! │  (High-level API for creating, restoring, listing)          │
//! └─────────────────────┬───────────────────────────────────────┘
//!                       │
//!        ┌──────────────┼──────────────┐
//!        │              │              │
//!        ▼              ▼              ▼
//! ┌─────────────┐ ┌──────────┐ ┌─────────────┐
//! │StateMachine │ │ Backend  │ │  Cleanup    │
//! │(RocksDB)    │ │(S3/Local)│ │ (Temp files)│
//! └─────────────┘ └──────────┘ └─────────────┘
//!        │              │
//!        │              ▼
//!        │       ┌──────────────┐
//!        └──────▶│ Resumable    │
//!                │ Uploader     │
//!                └──────────────┘
//! ```
//!
//! ## Usage
//!
//! ### Basic snapshot with local backend
//!
//! ```ignore
//! use akidb_storage::snapshot::{LocalSnapshotBackend, SnapshotManager, SnapshotMetadata};
//!
//! let backend = LocalSnapshotBackend::new("/var/lib/akidb/snapshots");
//! let manager = SnapshotManager::new(backend);
//!
//! // Create a snapshot
//! let metadata = SnapshotMetadata::new("my-collection");
//! let snapshot_id = manager.create_snapshot(metadata, files).await?;
//!
//! // List snapshots
//! let snapshots = manager.list_snapshots("my-collection").await?;
//!
//! // Restore a snapshot
//! let (metadata, files) = manager.restore_snapshot(&snapshot_id).await?;
//! ```
//!
//! ### Resumable upload with state machine
//!
//! ```ignore
//! use akidb_storage::snapshot::{
//!     SnapshotStateMachine, ResumableUploader, SnapshotUploadExecutor
//! };
//!
//! let state_machine = Arc::new(SnapshotStateMachine::new(storage));
//! let uploader = ResumableUploader::new(endpoint, bucket, access_key, secret_key);
//! let executor = SnapshotUploadExecutor::new(uploader, state_machine);
//!
//! // Start a new upload
//! let mut record = state_machine.start_operation(
//!     snapshot_id, collection, shard_id
//! )?;
//!
//! // Execute (automatically checkpoints and can resume)
//! let etag = executor.execute(&local_path, &object_key, &mut record).await?;
//!
//! // On restart, recover and resume
//! let operations = state_machine.recover_operations()?;
//! for mut record in operations {
//!     executor.resume(&mut record).await?;
//! }
//! ```

mod backend;
mod cleanup;
mod resumable_upload;
mod state_machine;

// Re-export main types from backend
pub use backend::{
    LocalSnapshotBackend, S3SnapshotBackend, SnapshotBackend, SnapshotFile, SnapshotManager,
    SnapshotMetadata,
};

// Re-export state machine types
pub use state_machine::{
    SnapshotState, SnapshotStateMachine, SnapshotStateRecord, UploadCheckpoint,
};

// Re-export resumable upload types
pub use resumable_upload::{
    CompletedPart, ResumableUploadConfig, ResumableUploader, SnapshotUploadExecutor,
    DEFAULT_CHUNK_SIZE, MAX_PARTS, MIN_CHUNK_SIZE,
};

// Re-export cleanup types
pub use cleanup::{cleanup_orphaned_uploads, CleanupConfig, CleanupResult, SnapshotCleanup};
