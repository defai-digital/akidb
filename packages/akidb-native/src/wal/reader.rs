//! WalReader — scans a WAL file and replays unflushed entries.
//!
//! Reads the WAL from the start, tracking flush markers. Returns only
//! the entries whose sequence numbers are after the last flush marker.
//! Entries with CRC mismatches or truncated frames are skipped.

use std::fs;
use std::path::Path;

use crate::wal::writer::crc32_compute;

const ENTRY_HEADER_SIZE: usize = 16;
const FLUSH_MARKER_SIZE: usize = 12;
const FLUSH_MAGIC: u32 = 0x464C5348;

/// Result of WAL recovery.
pub struct WalRecoveryResult {
    /// Records that need to be replayed (unflushed).
    pub records: Vec<serde_json::Value>,
    /// The highest sequence number found in the WAL.
    pub max_sequence: u64,
}

pub struct WalReader;

impl WalReader {
    /// Read a WAL file and return all unflushed records.
    /// If the WAL file doesn't exist, returns an empty result.
    pub fn recover(wal_path: &Path) -> WalRecoveryResult {
        if !wal_path.exists() {
            return WalRecoveryResult {
                records: Vec::new(),
                max_sequence: 0,
            };
        }

        let data = match fs::read(wal_path) {
            Ok(d) => d,
            Err(_) => {
                return WalRecoveryResult {
                    records: Vec::new(),
                    max_sequence: 0,
                };
            }
        };

        Self::parse(&data)
    }

    /// Parse a WAL buffer and extract unflushed records.
    pub fn parse(data: &[u8]) -> WalRecoveryResult {
        let mut offset = 0;
        let mut last_flushed_sequence: u64 = 0;
        let mut max_sequence: u64 = 0;

        struct Entry {
            sequence: u64,
            record: serde_json::Value,
        }
        let mut all_entries: Vec<Entry> = Vec::new();

        while offset < data.len() {
            // Check if remaining bytes could be a flush marker.
            if offset + 4 <= data.len() {
                let maybe_magic = u32::from_le_bytes(
                    data[offset..offset + 4].try_into().unwrap(),
                );
                if maybe_magic == FLUSH_MAGIC && offset + FLUSH_MARKER_SIZE <= data.len() {
                    last_flushed_sequence = u64::from_le_bytes(
                        data[offset + 4..offset + 12].try_into().unwrap(),
                    );
                    offset += FLUSH_MARKER_SIZE;
                    continue;
                }
            }

            // Try to read an entry header.
            if offset + ENTRY_HEADER_SIZE > data.len() {
                break;
            }

            let entry_length = u32::from_le_bytes(
                data[offset..offset + 4].try_into().unwrap(),
            ) as usize;
            let expected_crc = u32::from_le_bytes(
                data[offset + 4..offset + 8].try_into().unwrap(),
            );
            let sequence = u64::from_le_bytes(
                data[offset + 8..offset + 16].try_into().unwrap(),
            );

            // Validate payload fits.
            if offset + ENTRY_HEADER_SIZE + entry_length > data.len() {
                break;
            }

            let payload = &data[offset + ENTRY_HEADER_SIZE..offset + ENTRY_HEADER_SIZE + entry_length];

            // Validate CRC.
            let actual_crc = crc32_compute(payload);
            if actual_crc != expected_crc {
                offset += ENTRY_HEADER_SIZE + entry_length;
                continue;
            }

            // Parse JSON payload.
            if let Ok(record) = serde_json::from_slice::<serde_json::Value>(payload) {
                if sequence > max_sequence {
                    max_sequence = sequence;
                }
                all_entries.push(Entry { sequence, record });
            }

            offset += ENTRY_HEADER_SIZE + entry_length;
        }

        // Filter to only entries after the last flush marker.
        let records = all_entries
            .into_iter()
            .filter(|e| e.sequence > last_flushed_sequence)
            .map(|e| e.record)
            .collect();

        WalRecoveryResult {
            records,
            max_sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use serde_json::json;

    #[test]
    fn recover_nonexistent_file() {
        let result = WalReader::recover(Path::new("/tmp/nonexistent_test_wal_12345.wal"));
        assert!(result.records.is_empty());
        assert_eq!(result.max_sequence, 0);
    }

    #[test]
    fn write_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::new(&wal_path, None);
            writer
                .append_batch(&[
                    json!({"chunk_id": "c-1", "vector": [1.0, 0.0]}),
                    json!({"chunk_id": "c-2", "vector": [0.0, 1.0]}),
                ])
                .unwrap();
        }

        let result = WalReader::recover(&wal_path);
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.max_sequence, 2);
        assert_eq!(result.records[0]["chunk_id"], "c-1");
        assert_eq!(result.records[1]["chunk_id"], "c-2");
    }

    #[test]
    fn flush_marker_filters_entries() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::new(&wal_path, None);
            writer
                .append_batch(&[json!({"chunk_id": "c-1"})])
                .unwrap();
            writer
                .append_batch(&[json!({"chunk_id": "c-2"})])
                .unwrap();
            // Mark c-1 as flushed.
            writer.mark_flushed(1).unwrap();
            // Add c-3 after flush marker.
            writer
                .append_batch(&[json!({"chunk_id": "c-3"})])
                .unwrap();
        }

        let result = WalReader::recover(&wal_path);
        // Only c-2 and c-3 should be returned (sequence > 1).
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0]["chunk_id"], "c-2");
        assert_eq!(result.records[1]["chunk_id"], "c-3");
    }

    #[test]
    fn corrupted_entry_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::new(&wal_path, None);
            writer
                .append_batch(&[json!({"chunk_id": "c-1"})])
                .unwrap();
        }

        // Corrupt the payload (flip a byte after the header).
        let mut data = std::fs::read(&wal_path).unwrap();
        if data.len() > ENTRY_HEADER_SIZE + 2 {
            data[ENTRY_HEADER_SIZE + 1] ^= 0xFF;
        }
        std::fs::write(&wal_path, &data).unwrap();

        let result = WalReader::recover(&wal_path);
        assert!(result.records.is_empty());
    }

    #[test]
    fn recovery_with_rotated_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let old_path = dir.path().join("test.wal.old");

        // Write entries to the "old" WAL.
        {
            let mut writer = WalWriter::new(&old_path, None);
            writer
                .append_batch(&[json!({"chunk_id": "c-old"})])
                .unwrap();
        }

        // Write entries to the current WAL.
        {
            let mut writer = WalWriter::new(&wal_path, None);
            writer.set_sequence(1); // Continue from old WAL's sequence.
            writer
                .append_batch(&[json!({"chunk_id": "c-new"})])
                .unwrap();
        }

        // Recover from both — simulating recovery with rotation.
        let old_result = WalReader::recover(&old_path);
        let new_result = WalReader::recover(&wal_path);

        assert_eq!(old_result.records.len(), 1);
        assert_eq!(new_result.records.len(), 1);
    }

    #[test]
    fn sequence_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::new(&wal_path, None);
            for i in 0..5 {
                writer
                    .append_batch(&[json!({"chunk_id": format!("c-{}", i)})])
                    .unwrap();
            }
        }

        let result = WalReader::recover(&wal_path);
        assert_eq!(result.records.len(), 5);
        assert_eq!(result.max_sequence, 5);
    }
}
