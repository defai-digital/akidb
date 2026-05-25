//! WalWriter — append-only Write-Ahead Log for crash recovery.
//!
//! Each WAL file is a sequence of framed entries:
//!   [4B entry_length (u32 LE)]
//!   [4B crc32 checksum of payload]
//!   [8B sequence_number (u64 LE)]
//!   [nB payload (JSON-encoded record)]
//!
//! After a successful flush, a flush marker is appended:
//!   [4B magic 0x464C5348 ("FLSH")]
//!   [8B flushed_through_sequence (u64 LE)]

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::error::Result;

const ENTRY_HEADER_SIZE: usize = 16;
const FLUSH_MARKER_SIZE: usize = 12;
const FLUSH_MAGIC: u32 = 0x464C5348; // "FLSH"

/// Default maximum WAL file size before rotation (50 MB).
const DEFAULT_MAX_WAL_SIZE_BYTES: u64 = 50 * 1024 * 1024;

pub struct WalWriter {
    fd: Option<File>,
    sequence: u64,
    path: PathBuf,
    current_size_bytes: u64,
    rotated_path: Option<PathBuf>,
    max_size_bytes: u64,
    rotations: u32,
}

impl WalWriter {
    pub fn new(path: impl Into<PathBuf>, max_size_bytes: Option<u64>) -> Self {
        Self {
            fd: None,
            sequence: 0,
            path: path.into(),
            current_size_bytes: 0,
            rotated_path: None,
            max_size_bytes: max_size_bytes.unwrap_or(DEFAULT_MAX_WAL_SIZE_BYTES),
            rotations: 0,
        }
    }

    /// Open (or create) the WAL file for appending.
    pub fn open(&mut self) -> Result<()> {
        if self.fd.is_some() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        // Track existing file size.
        self.current_size_bytes = fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0);
        self.fd = Some(file);
        Ok(())
    }

    /// Append a batch of JSON-serialized records and fsync.
    /// Returns the sequence number of the last written entry.
    pub fn append_batch(&mut self, records: &[serde_json::Value]) -> Result<u64> {
        if self.fd.is_none() {
            self.open()?;
        }

        let mut combined = Vec::new();

        for record in records {
            self.sequence += 1;
            let payload = serde_json::to_vec(record)?;

            let mut header = [0u8; ENTRY_HEADER_SIZE];
            header[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            header[4..8].copy_from_slice(&crc32_compute(&payload).to_le_bytes());
            header[8..16].copy_from_slice(&self.sequence.to_le_bytes());

            combined.extend_from_slice(&header);
            combined.extend_from_slice(&payload);
        }

        let fd = self.fd.as_mut().unwrap();
        fd.write_all(&combined)?;
        fd.flush()?;
        fd.sync_all()?;
        self.current_size_bytes += combined.len() as u64;

        // Rotate if WAL exceeds size limit.
        if self.current_size_bytes >= self.max_size_bytes {
            self.rotate()?;
        }

        Ok(self.sequence)
    }

    /// Write a flush marker indicating all entries up to this sequence
    /// have been successfully flushed to a segment.
    pub fn mark_flushed(&mut self, through_sequence: u64) -> Result<()> {
        if self.fd.is_none() {
            return Ok(());
        }

        let mut marker = [0u8; FLUSH_MARKER_SIZE];
        marker[0..4].copy_from_slice(&FLUSH_MAGIC.to_le_bytes());
        marker[4..12].copy_from_slice(&through_sequence.to_le_bytes());

        let fd = self.fd.as_mut().unwrap();
        fd.write_all(&marker)?;
        fd.flush()?;
        fd.sync_all()?;
        Ok(())
    }

    /// Truncate the WAL file (after successful flush).
    /// Also cleans up any rotated WAL file.
    pub fn truncate(&mut self) -> Result<()> {
        self.close_file();
        let _ = fs::remove_file(&self.path);
        self.cleanup_rotated();
        self.sequence = 0;
        self.current_size_bytes = 0;
        Ok(())
    }

    /// Set the sequence counter (used after recovery).
    pub fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }

    /// Close the WAL file descriptor.
    pub fn close(&mut self) {
        self.close_file();
    }

    /// Rotate the WAL: rename current file to `<path>.old`, create a new one.
    fn rotate(&mut self) -> Result<()> {
        self.close_file();

        let old_path = PathBuf::from(format!("{}.old", self.path.display()));

        // If there's already a rotated file, delete it.
        let _ = fs::remove_file(&old_path);

        if self.path.exists() {
            fs::rename(&self.path, &old_path)?;
            self.rotated_path = Some(old_path);
        }

        // Open a fresh WAL file.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.fd = Some(file);
        self.current_size_bytes = 0;
        self.rotations += 1;
        Ok(())
    }

    fn cleanup_rotated(&mut self) {
        if let Some(path) = self.rotated_path.take() {
            let _ = fs::remove_file(&path);
        }
        let default_old = PathBuf::from(format!("{}.old", self.path.display()));
        let _ = fs::remove_file(&default_old);
    }

    fn close_file(&mut self) {
        self.fd = None; // Drop closes the file.
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        self.close_file();
    }
}

/// CRC32 computation (IEEE polynomial, same as TypeScript implementation).
pub fn crc32_compute(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn append_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let mut writer = WalWriter::new(&wal_path, None);

        let records = vec![
            json!({"chunk_id": "c-1", "vector": [1.0, 0.0]}),
            json!({"chunk_id": "c-2", "vector": [0.0, 1.0]}),
        ];
        let seq = writer.append_batch(&records).unwrap();
        assert_eq!(seq, 2);
        assert!(wal_path.exists());
        assert!(std::fs::metadata(&wal_path).unwrap().len() > 0);
    }

    #[test]
    fn flush_marker() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let mut writer = WalWriter::new(&wal_path, None);

        writer.append_batch(&[json!({"chunk_id": "c-1"})]).unwrap();
        writer.mark_flushed(1).unwrap();
        // No panic = success.
    }

    #[test]
    fn truncate_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let mut writer = WalWriter::new(&wal_path, None);

        writer.append_batch(&[json!({"chunk_id": "c-1"})]).unwrap();
        assert!(wal_path.exists());

        writer.truncate().unwrap();
        assert!(!wal_path.exists());
    }

    #[test]
    fn rotation_at_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let old_path = PathBuf::from(format!("{}.old", wal_path.display()));
        // Very small threshold for testing.
        let mut writer = WalWriter::new(&wal_path, Some(100));

        // Write enough to trigger rotation (old file will appear).
        let mut rotated = false;
        for _ in 0..20 {
            writer.append_batch(&[json!({"chunk_id": "c-1", "data": "some payload data"})]).unwrap();
            if old_path.exists() {
                rotated = true;
                break;
            }
        }

        assert!(rotated, "WAL should have rotated");
    }

    #[test]
    fn truncate_cleans_rotated_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let old_path = PathBuf::from(format!("{}.old", wal_path.display()));
        let mut writer = WalWriter::new(&wal_path, Some(100));

        // Write until rotation occurs.
        for _ in 0..20 {
            writer.append_batch(&[json!({"chunk_id": "c-1", "data": "some payload data"})]).unwrap();
            if old_path.exists() { break; }
        }
        assert!(old_path.exists());

        writer.truncate().unwrap();
        assert!(!old_path.exists());
    }

    #[test]
    fn set_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let mut writer = WalWriter::new(&wal_path, None);

        writer.set_sequence(100);
        let seq = writer.append_batch(&[json!({"chunk_id": "c-1"})]).unwrap();
        assert_eq!(seq, 101);
    }
}
