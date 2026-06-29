//! Write-Ahead Log for durability and rebuild replay

use crate::{AkiDbError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;
use tracing::{debug, info, warn};

/// BUG-HUNT-013: CRC32 checksum for WAL entry integrity verification
/// Uses the IEEE polynomial (same as Ethernet, gzip, PNG)
fn crc32(data: &[u8]) -> u32 {
    // CRC32 lookup table (IEEE polynomial 0xEDB88320)
    static CRC_TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 0;
            while j < 8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };

    let mut crc = 0xFFFFFFFF_u32;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC_TABLE[idx];
    }
    !crc
}

/// BUG-HUNT-013: Magic bytes to help resync after corruption
/// Format: 0xAK (AkiDB marker) + 0x01 (version)
const WAL_ENTRY_MAGIC: [u8; 4] = [0xAA, 0x4B, 0x44, 0x01];

/// FIX BUG-HUNT-002: Maximum vector dimensions to prevent DoS/OOM
/// 16K dimensions supports most embedding models (e.g., OpenAI 3072, BGE 1024)
/// while preventing malicious oversized vectors from exhausting memory.
pub const MAX_VECTOR_DIMENSIONS: usize = 16_384;

/// FIX BUG-HUNT-002: Maximum metadata size in bytes (1MB)
/// Metadata typically contains JSON or key-value pairs, 1MB is generous.
pub const MAX_METADATA_BYTES: usize = 1_048_576;

/// FIX BUG-HUNT-403: Maximum WAL entry size in bytes (100MB)
/// This limit must match the read path validation to ensure entries written
/// can always be read back. 100MB is generous for single entries.
pub const MAX_WAL_ENTRY_BYTES: usize = 100_000_000;

/// WAL entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    /// Insert a vector
    Insert {
        lsn: u64,
        external_id: String,
        internal_id: i64,
        vector: Vec<f32>,
        metadata: Option<Vec<u8>>,
        timestamp: u64,
    },
    /// Delete a vector
    Delete {
        lsn: u64,
        external_id: String,
        internal_id: i64,
        timestamp: u64,
    },
    /// Checkpoint marker
    Checkpoint {
        lsn: u64,
        timestamp: u64,
    },
}

impl WalEntry {
    pub fn lsn(&self) -> u64 {
        match self {
            WalEntry::Insert { lsn, .. } => *lsn,
            WalEntry::Delete { lsn, .. } => *lsn,
            WalEntry::Checkpoint { lsn, .. } => *lsn,
        }
    }
}

/// Write-Ahead Log
pub struct WriteAheadLog {
    /// Path to WAL file
    path: PathBuf,
    /// Current LSN (Log Sequence Number)
    current_lsn: AtomicU64,
    /// Writer (protected by mutex for concurrent access)
    writer: Mutex<Option<BufWriter<File>>>,
    /// Sync mode
    sync_mode: WalSyncMode,
}

#[derive(Debug, Clone, Copy)]
pub enum WalSyncMode {
    /// Sync after every write (safest, slowest)
    Sync,
    /// Sync after batch of writes
    BatchSync,
    /// No sync (fastest, risk of data loss)
    NoSync,
}

impl WriteAheadLog {
    /// Create or open a WAL file
    pub fn open<P: AsRef<Path>>(path: P, sync_mode: WalSyncMode) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        info!("Opening WAL at {:?}", path);

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| AkiDbError::StorageError(format!("Failed to create WAL directory: {}", e)))?;
        }

        // FIX BUG-H014: Clean up orphaned temp files from previous crashes
        // truncate_to() creates *.wal.tmp files that may be left behind on crash
        Self::cleanup_temp_files(&path)?;

        // Find max LSN from existing WAL
        let max_lsn = Self::find_max_lsn(&path)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to open WAL file: {}", e)))?;

        let writer = BufWriter::new(file);

        Ok(Self {
            path,
            current_lsn: AtomicU64::new(max_lsn.saturating_add(1)),
            writer: Mutex::new(Some(writer)),
            sync_mode,
        })
    }

    /// FIX BUG-H014: Clean up orphaned temp files from previous crashes
    ///
    /// truncate_to() creates *.wal.tmp files as part of its atomic rename pattern.
    /// If a crash occurs after creating the temp file but before the rename,
    /// the temp file will be orphaned. This method cleans up such files.
    fn cleanup_temp_files(path: &Path) -> Result<()> {
        let temp_path = path.with_extension("wal.tmp");
        if temp_path.exists() {
            warn!(
                "Found orphaned WAL temp file from previous crash: {:?}. Removing.",
                temp_path
            );
            std::fs::remove_file(&temp_path).map_err(|e| {
                AkiDbError::StorageError(format!(
                    "Failed to remove orphaned WAL temp file {:?}: {}",
                    temp_path, e
                ))
            })?;
            info!("Cleaned up orphaned WAL temp file");
        }
        Ok(())
    }

    /// Find the maximum LSN in an existing WAL file
    fn find_max_lsn(path: &Path) -> Result<u64> {
        if !path.exists() {
            return Ok(0);
        }

        let file = File::open(path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to open WAL for reading: {}", e)))?;

        let reader = BufReader::new(file);
        let mut max_lsn = 0u64;

        // Read entries to find max LSN
        for entry in Self::read_entries_from_reader(reader) {
            match entry {
                Ok(e) => {
                    if e.lsn() > max_lsn {
                        max_lsn = e.lsn();
                    }
                }
                Err(e) => {
                    warn!("Error reading WAL entry: {}", e);
                    break;
                }
            }
        }

        Ok(max_lsn)
    }

    /// Allocate next LSN
    fn next_lsn(&self) -> Result<u64> {
        self.current_lsn
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| current.checked_add(1))
            .map_err(|_| AkiDbError::StorageError("WAL LSN space exhausted".to_string()))
    }

    /// Get current timestamp
    /// FIX BUG-038: Use unwrap_or_default to handle pre-epoch clock gracefully
    fn timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Append an insert entry
    ///
    /// FIX BUG-HUNT-002: Validates vector and metadata sizes to prevent DoS.
    pub fn append_insert(
        &self,
        external_id: &str,
        internal_id: i64,
        vector: &[f32],
        metadata: Option<&[u8]>,
    ) -> Result<u64> {
        // FIX BUG-HUNT-002: Validate vector dimensions
        if vector.len() > MAX_VECTOR_DIMENSIONS {
            return Err(AkiDbError::InvalidParameter(format!(
                "Vector has {} dimensions, maximum allowed is {}",
                vector.len(),
                MAX_VECTOR_DIMENSIONS
            )));
        }

        // FIX BUG-HUNT-002: Validate metadata size
        if let Some(m) = metadata {
            if m.len() > MAX_METADATA_BYTES {
                return Err(AkiDbError::InvalidParameter(format!(
                    "Metadata is {} bytes, maximum allowed is {} bytes",
                    m.len(),
                    MAX_METADATA_BYTES
                )));
            }
        }

        let lsn = self.next_lsn()?;
        let entry = WalEntry::Insert {
            lsn,
            external_id: external_id.to_string(),
            internal_id,
            vector: vector.to_vec(),
            metadata: metadata.map(|m| m.to_vec()),
            timestamp: Self::timestamp(),
        };

        self.append_entry(&entry)?;
        Ok(entry.lsn())
    }

    /// Append a delete entry
    pub fn append_delete(&self, external_id: &str, internal_id: i64) -> Result<u64> {
        let lsn = self.next_lsn()?;
        let entry = WalEntry::Delete {
            lsn,
            external_id: external_id.to_string(),
            internal_id,
            timestamp: Self::timestamp(),
        };

        self.append_entry(&entry)?;
        Ok(entry.lsn())
    }

    /// Append a checkpoint marker
    pub fn append_checkpoint(&self) -> Result<u64> {
        let lsn = self.next_lsn()?;
        let entry = WalEntry::Checkpoint {
            lsn,
            timestamp: Self::timestamp(),
        };

        self.append_entry(&entry)?;
        Ok(entry.lsn())
    }

    /// Append an entry to the WAL
    ///
    /// BUG-HUNT-013: Entry format with checksum for corruption detection:
    /// [magic:4][len:4][data:len][crc32:4]
    ///
    /// The magic bytes help resync after corruption, and the CRC32 checksum
    /// allows detection of corrupted data before deserialization.
    fn append_entry(&self, entry: &WalEntry) -> Result<()> {
        let data = bincode::serialize(entry)
            .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;

        // FIX BUG-HUNT-403: Validate entry size matches read path limit (100MB)
        // Previously, write allowed up to 4GB but read rejected >100MB, causing
        // data loss on recovery. Now we fail fast on write if entry is too large.
        if data.len() > MAX_WAL_ENTRY_BYTES {
            return Err(AkiDbError::InvalidParameter(format!(
                "WAL entry too large: {} bytes exceeds maximum of {} bytes",
                data.len(),
                MAX_WAL_ENTRY_BYTES
            )));
        }

        let mut writer_guard = self.writer.lock();
        let writer = writer_guard.as_mut()
            .ok_or_else(|| AkiDbError::StorageError("WAL writer closed".to_string()))?;

        // BUG-HUNT-013: Write magic bytes for entry start marker
        writer.write_all(&WAL_ENTRY_MAGIC)
            .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;

        // FIX BUG-055: Check for length prefix overflow before casting
        // u32 can only represent up to ~4GB, reject larger entries
        // Note: This is now redundant due to BUG-HUNT-403 fix above, but kept for safety
        let len: u32 = data.len().try_into().map_err(|_| {
            AkiDbError::InvalidParameter(format!(
                "WAL entry too large: {} bytes exceeds maximum of {} bytes",
                data.len(),
                u32::MAX
            ))
        })?;
        writer.write_all(&len.to_le_bytes())
            .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;

        // Write data
        writer.write_all(&data)
            .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;

        // BUG-HUNT-013: Write CRC32 checksum for integrity verification
        let checksum = crc32(&data);
        writer.write_all(&checksum.to_le_bytes())
            .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;

        // Sync based on mode
        match self.sync_mode {
            WalSyncMode::Sync => {
                writer.flush()
                    .map_err(|e| AkiDbError::StorageError(format!("WAL flush error: {}", e)))?;
                // Actually sync to disk - flush() only writes to OS buffer
                writer.get_ref().sync_all()
                    .map_err(|e| AkiDbError::StorageError(format!("WAL sync error: {}", e)))?;
            }
            WalSyncMode::BatchSync | WalSyncMode::NoSync => {}
        }

        debug!("Appended WAL entry with LSN {}", entry.lsn());
        Ok(())
    }

    /// Flush the WAL to disk
    pub fn flush(&self) -> Result<()> {
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            writer.flush()
                .map_err(|e| AkiDbError::StorageError(format!("WAL flush error: {}", e)))?;
            // Actually sync to disk - flush() only writes to OS buffer
            writer.get_ref().sync_all()
                .map_err(|e| AkiDbError::StorageError(format!("WAL sync error: {}", e)))?;
        }
        Ok(())
    }

    /// Read all entries from the WAL
    pub fn read_entries(&self) -> Result<Vec<WalEntry>> {
        let file = File::open(&self.path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to open WAL for reading: {}", e)))?;

        let reader = BufReader::new(file);
        Self::read_entries_from_reader(reader).collect()
    }

    /// Read entries with LSN > start_lsn
    pub fn read_entries_since(&self, start_lsn: u64) -> Result<Vec<WalEntry>> {
        let entries = self.read_entries()?;
        Ok(entries.into_iter().filter(|e| e.lsn() > start_lsn).collect())
    }

    /// Iterator over entries from a reader
    ///
    /// BUG-HUNT-013: Verifies CRC32 checksum before deserializing
    /// BUG-HUNT-014: Continues scanning after corruption to recover subsequent valid entries
    ///
    /// Entry format: [magic:4][len:4][data:len][crc32:4]
    fn read_entries_from_reader(mut reader: BufReader<File>) -> impl Iterator<Item = Result<WalEntry>> {
        let mut corruption_recovery_mode = false;

        std::iter::from_fn(move || {
            loop {
                // BUG-HUNT-014: If in recovery mode, scan for next magic bytes
                if corruption_recovery_mode {
                    match Self::scan_for_magic(&mut reader) {
                        Ok(true) => {
                            corruption_recovery_mode = false;
                            // Magic found, continue with normal read
                        }
                        Ok(false) => {
                            // EOF reached while scanning
                            return None;
                        }
                        Err(e) => {
                            warn!("Error during WAL corruption recovery scan: {}", e);
                            return None;
                        }
                    }
                } else {
                    // Normal read: expect magic bytes
                    let mut magic_buf = [0u8; 4];
                    match reader.read_exact(&mut magic_buf) {
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                        Err(e) => return Some(Err(AkiDbError::StorageError(format!("WAL read error: {}", e)))),
                    }

                    // Check magic bytes
                    if magic_buf != WAL_ENTRY_MAGIC {
                        // BUG-HUNT-014: Magic mismatch - likely legacy format or corruption
                        // Try reading as legacy format (no magic, no checksum)
                        let len = u32::from_le_bytes(magic_buf) as usize;

                        // FIX BUG-HUNT-403: Use constant for max entry size
                        // Sanity check: if len is huge, it's likely corruption not legacy format
                        if len > MAX_WAL_ENTRY_BYTES {
                            warn!("WAL corruption detected: invalid length {}. Scanning for next valid entry.", len);
                            corruption_recovery_mode = true;
                            continue;
                        }

                        // Try legacy read
                        let mut data = vec![0u8; len];
                        if let Err(e) = reader.read_exact(&mut data) {
                            warn!("WAL read error during legacy read: {}. Scanning for next valid entry.", e);
                            corruption_recovery_mode = true;
                            continue;
                        }

                        match bincode::deserialize(&data) {
                            Ok(entry) => return Some(Ok(entry)),
                            Err(e) => {
                                warn!("WAL deserialization error (legacy format): {}. Scanning for next valid entry.", e);
                                corruption_recovery_mode = true;
                                continue;
                            }
                        }
                    }
                }

                // Read length prefix
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
                    Err(e) => {
                        warn!("WAL read error: {}. Scanning for next valid entry.", e);
                        corruption_recovery_mode = true;
                        continue;
                    }
                }

                let len = u32::from_le_bytes(len_buf) as usize;

                // FIX BUG-HUNT-403: Use constant for max entry size
                // Sanity check length
                if len > MAX_WAL_ENTRY_BYTES {
                    warn!("WAL corruption: invalid entry length {}. Scanning for next valid entry.", len);
                    corruption_recovery_mode = true;
                    continue;
                }

                // Read data
                let mut data = vec![0u8; len];
                if let Err(e) = reader.read_exact(&mut data) {
                    warn!("WAL read error: {}. Scanning for next valid entry.", e);
                    corruption_recovery_mode = true;
                    continue;
                }

                // BUG-HUNT-013: Read and verify CRC32 checksum
                let mut checksum_buf = [0u8; 4];
                if let Err(e) = reader.read_exact(&mut checksum_buf) {
                    warn!("WAL checksum read error: {}. Scanning for next valid entry.", e);
                    corruption_recovery_mode = true;
                    continue;
                }

                let stored_checksum = u32::from_le_bytes(checksum_buf);
                let computed_checksum = crc32(&data);

                if stored_checksum != computed_checksum {
                    warn!(
                        "WAL checksum mismatch: stored={:#X}, computed={:#X}. Entry corrupted, scanning for next valid entry.",
                        stored_checksum, computed_checksum
                    );
                    corruption_recovery_mode = true;
                    continue;
                }

                // Deserialize with verified data
                match bincode::deserialize(&data) {
                    Ok(entry) => return Some(Ok(entry)),
                    Err(e) => {
                        warn!("WAL deserialization error despite valid checksum: {}. Scanning for next valid entry.", e);
                        corruption_recovery_mode = true;
                        continue;
                    }
                }
            }
        })
    }

    /// BUG-HUNT-014: Scan forward to find the next magic bytes after corruption
    ///
    /// Returns Ok(true) if magic found, Ok(false) if EOF, Err on read error.
    fn scan_for_magic(reader: &mut BufReader<File>) -> std::io::Result<bool> {
        let mut window = [0u8; 4];
        let mut pos = 0;

        loop {
            let mut byte = [0u8; 1];
            match reader.read_exact(&mut byte) {
                Ok(_) => {
                    // Shift window and add new byte
                    if pos < 4 {
                        window[pos] = byte[0];
                        pos += 1;
                    } else {
                        window.rotate_left(1);
                        window[3] = byte[0];
                    }

                    // Check if window matches magic
                    if pos >= 4 && window == WAL_ENTRY_MAGIC {
                        debug!("WAL recovery: found magic bytes, resuming normal read");
                        return Ok(true);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Truncate WAL up to checkpoint LSN
    ///
    /// FIX BUG-037: Uses atomic rename pattern to ensure crash safety.
    /// The old implementation truncated the WAL file in-place, which could
    /// cause data loss if a crash occurred between truncation and completing
    /// the rewrite. Now we write to a temp file, sync, then atomically rename.
    pub fn truncate_to(&self, checkpoint_lsn: u64) -> Result<()> {
        // Hold the lock throughout the entire operation to prevent races
        let mut writer_guard = self.writer.lock();

        // Read entries to keep (with writer closed to avoid conflicts)
        *writer_guard = None;

        // Read entries from disk
        let file = File::open(&self.path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to open WAL for reading: {}", e)))?;
        let reader = BufReader::new(file);

        // FIX BUG-067: Track and warn about corrupted entries instead of silently discarding
        let mut entries = Vec::new();
        let mut corrupted_count = 0u64;
        let mut last_valid_lsn = 0u64;

        for result in Self::read_entries_from_reader(reader) {
            match result {
                Ok(entry) => {
                    last_valid_lsn = entry.lsn();
                    if entry.lsn() > checkpoint_lsn {
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    corrupted_count += 1;
                    // FIX BUG-067: Log warning with details about the corruption
                    warn!(
                        "WAL corruption detected during truncation: {}. Last valid LSN: {}. \
                         Corrupted entries will be lost. Consider investigating the WAL file.",
                        e, last_valid_lsn
                    );
                    // Continue processing - we still want to preserve valid entries after corruption
                    // Note: Depending on corruption type, subsequent entries may also be unreadable
                }
            }
        }

        if corrupted_count > 0 {
            warn!(
                "WAL truncation completed with {} corrupted entries discarded. \
                 Data loss may have occurred. Review logs for details.",
                corrupted_count
            );
        }

        // FIX BUG-037: Write to temp file first for crash safety
        let temp_path = self.path.with_extension("wal.tmp");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to create temp WAL: {}", e)))?;

        let mut writer: BufWriter<File> = BufWriter::new(file);

        // BUG-HUNT-013: Write entries with new format [magic:4][len:4][data:len][crc32:4]
        for entry in &entries {
            let data = bincode::serialize(entry)
                .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;

            // Write magic bytes
            writer.write_all(&WAL_ENTRY_MAGIC)
                .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;

            // FIX BUG-097: Use try_into() with proper error handling like append_entry()
            // Previously used direct cast which could silently truncate for entries > 4GB
            let len: u32 = data.len().try_into().map_err(|_| {
                AkiDbError::InvalidParameter(format!(
                    "WAL entry too large during truncation: {} bytes exceeds u32::MAX",
                    data.len()
                ))
            })?;
            writer.write_all(&len.to_le_bytes())
                .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;
            writer.write_all(&data)
                .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;

            // Write CRC32 checksum
            let checksum = crc32(&data);
            writer.write_all(&checksum.to_le_bytes())
                .map_err(|e| AkiDbError::StorageError(format!("WAL write error: {}", e)))?;
        }

        Write::flush(&mut writer)
            .map_err(|e| AkiDbError::StorageError(format!("WAL flush error: {}", e)))?;
        // Sync temp file to disk before rename
        writer.get_ref().sync_all()
            .map_err(|e| AkiDbError::StorageError(format!("WAL sync error: {}", e)))?;
        // Drop writer to close file handle before rename
        drop(writer);

        // FIX BUG-037: Atomic rename - if crash happens before this, old WAL is intact
        // If crash happens after this, new WAL is complete and synced
        std::fs::rename(&temp_path, &self.path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to rename temp WAL: {}", e)))?;

        // Reopen for appending (still holding lock)
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to reopen WAL: {}", e)))?;

        *writer_guard = Some(BufWriter::new(file));

        info!("Truncated WAL to LSN {}, {} entries remaining", checkpoint_lsn, entries.len());
        Ok(())
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }
}

/// FIX BUG-H027: Implement Drop to flush buffered writes before dropping
///
/// BufWriter buffers writes in memory. Without this Drop impl, if the process
/// crashes or panics, buffered writes are silently lost. This violates the
/// durability guarantee that WAL entries are persisted before returning success.
impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        // Attempt to flush and sync the writer
        // We use let _ = to ignore errors since we can't propagate them from Drop,
        // but we log them for debugging.
        if let Some(writer) = self.writer.get_mut().as_mut() {
            if let Err(e) = writer.flush() {
                // Log but don't panic - we're in Drop
                tracing::error!("WAL flush failed during drop: {}. Data may have been lost.", e);
            } else if let Err(e) = writer.get_ref().sync_all() {
                tracing::error!("WAL sync failed during drop: {}. Data may have been lost.", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let wal = WriteAheadLog::open(&wal_path, WalSyncMode::Sync).unwrap();

        // Append entries
        let lsn1 = wal.append_insert("vec-1", 0, &[1.0, 2.0, 3.0], None).unwrap();
        let lsn2 = wal.append_insert("vec-2", 1, &[4.0, 5.0, 6.0], None).unwrap();
        let lsn3 = wal.append_delete("vec-1", 0).unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        // Read entries
        let entries = wal.read_entries().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_wal_read_since() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let wal = WriteAheadLog::open(&wal_path, WalSyncMode::Sync).unwrap();

        wal.append_insert("vec-1", 0, &[1.0], None).unwrap();
        wal.append_insert("vec-2", 1, &[2.0], None).unwrap();
        wal.append_checkpoint().unwrap();
        wal.append_insert("vec-3", 2, &[3.0], None).unwrap();

        // Read entries since checkpoint (LSN 3)
        let entries = wal.read_entries_since(3).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write some entries
        {
            let wal = WriteAheadLog::open(&wal_path, WalSyncMode::Sync).unwrap();
            wal.append_insert("vec-1", 0, &[1.0, 2.0], None).unwrap();
            wal.append_insert("vec-2", 1, &[3.0, 4.0], None).unwrap();
            wal.flush().unwrap();
        }

        // Reopen and verify
        {
            let wal = WriteAheadLog::open(&wal_path, WalSyncMode::Sync).unwrap();
            let entries = wal.read_entries().unwrap();
            assert_eq!(entries.len(), 2);

            // New entries should continue from last LSN
            let lsn = wal.append_insert("vec-3", 2, &[5.0, 6.0], None).unwrap();
            assert_eq!(lsn, 3);
        }
    }

    #[test]
    fn test_wal_rejects_lsn_exhaustion_after_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = WriteAheadLog::open(&wal_path, WalSyncMode::Sync).unwrap();
            let entry = WalEntry::Checkpoint {
                lsn: u64::MAX,
                timestamp: 0,
            };
            wal.append_entry(&entry).unwrap();
            wal.flush().unwrap();
        }

        let wal = WriteAheadLog::open(&wal_path, WalSyncMode::Sync).unwrap();

        assert_eq!(wal.current_lsn(), u64::MAX);
        assert!(matches!(
            wal.append_checkpoint(),
            Err(AkiDbError::StorageError(_))
        ));
        assert_eq!(wal.current_lsn(), u64::MAX);
    }
}
