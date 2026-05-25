//! WriteBuffer — accumulates records in memory until a flush threshold is reached.
//!
//! Uncommitted records are searchable via brute-force scan in query/mod.rs.
//! There is no in-memory HNSW index for the write buffer by design: HNSW build
//! cost is amortized over segments, not individual writes.

use super::NativeRecord;

const DEFAULT_MAX_RECORDS: usize = 1000;
const DEFAULT_MAX_BYTES: usize = 10 * 1024 * 1024;

pub struct WriteBuffer {
    records: Vec<NativeRecord>,
    estimated_bytes: usize,
    max_records: usize,
    max_bytes: usize,
}

impl WriteBuffer {
    pub fn new(max_records: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            records: Vec::new(),
            estimated_bytes: 0,
            max_records: max_records.unwrap_or(DEFAULT_MAX_RECORDS),
            max_bytes: max_bytes.unwrap_or(DEFAULT_MAX_BYTES),
        }
    }

    pub fn add_batch(&mut self, records: &[NativeRecord]) -> bool {
        for record in records {
            self.estimated_bytes = self.estimated_bytes.saturating_add(estimate_record_bytes(record));
            self.records.push(record.clone());
        }
        self.should_flush()
    }

    /// Drain all records from the buffer.
    pub fn drain(&mut self) -> Vec<NativeRecord> {
        let drained = std::mem::take(&mut self.records);
        self.estimated_bytes = 0;
        drained
    }

    pub fn should_flush(&self) -> bool {
        self.records.len() >= self.max_records || self.estimated_bytes >= self.max_bytes
    }

    pub fn count(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn peek(&self) -> &[NativeRecord] {
        &self.records
    }
}

fn estimate_record_bytes(record: &NativeRecord) -> usize {
    let vector_bytes = record.vector.len() * std::mem::size_of::<f32>();
    let id_bytes = record.chunk_id.len() + record.doc_id.len();
    // Fixed overhead for metadata JSON, chunk_text, and JSON framing.
    vector_bytes + id_bytes + 128
}
