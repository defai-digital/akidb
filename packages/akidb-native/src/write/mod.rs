//! Write module — WriteBuffer and WritePath for the ingestion pipeline.

pub mod buffer;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::{AkiDbError, Result};
use crate::hnsw::HnswGraph;
use crate::metadata::MetadataStore;
use crate::segment::builder::SegmentBuilder;
use crate::segment::checksum::checksum_hex;
use crate::storage::LocalFsBackend;
use crate::wal::reader::WalReader;
use crate::wal::writer::WalWriter;

use self::buffer::WriteBuffer;

// ─── HnswParams ──────────────────────────────────────────────────────────────

/// HNSW graph construction and search parameters, sourced from the collection.
#[derive(Clone, Copy)]
pub struct HnswParams {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

// ─── WritePath ───────────────────────────────────────────────────────────────

pub struct WritePathOptions {
    pub max_records: Option<usize>,
    pub max_bytes: Option<usize>,
    pub wal_path: Option<PathBuf>,
}

pub struct UpsertResult {
    pub segment_ids: Vec<String>,
    pub buffered_count: usize,
}

#[derive(Debug, Clone)]
pub struct NativeRecord {
    pub chunk_id: String,
    pub doc_id: String,
    pub vector: Vec<f32>,
    pub metadata: serde_json::Value,
    pub chunk_text: Option<String>,
}

impl NativeRecord {
    pub fn from_json_value(value: serde_json::Value) -> Result<Self> {
        let chunk_id = value["chunk_id"].as_str().unwrap_or("").to_string();
        let doc_id = value["doc_id"].as_str().unwrap_or("").to_string();
        let vector = value["vector"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect()
            })
            .unwrap_or_default();
        let metadata = value.get("metadata").cloned().unwrap_or(serde_json::json!({}));
        let chunk_text = value
            .get("chunk_text")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Self {
            chunk_id,
            doc_id,
            vector,
            metadata,
            chunk_text,
        })
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "chunk_id": self.chunk_id,
            "doc_id": self.doc_id,
            "vector": self.vector,
            "metadata": self.metadata,
            "chunk_text": self.chunk_text,
        })
    }
}

pub struct WritePath {
    buffer: WriteBuffer,
    wal: Option<WalWriter>,
    last_wal_sequence: u64,
    metadata: Arc<Mutex<MetadataStore>>,
    storage: Arc<LocalFsBackend>,
}

impl WritePath {
    pub fn new(
        metadata: Arc<Mutex<MetadataStore>>,
        storage: Arc<LocalFsBackend>,
        opts: WritePathOptions,
    ) -> Result<Self> {
        let mut buffer = WriteBuffer::new(opts.max_records, opts.max_bytes);
        let mut last_wal_sequence = 0u64;

        let wal = if let Some(wal_path) = &opts.wal_path {
            let mut writer = WalWriter::new(wal_path.clone(), None);
            // Replay rotated WAL (.old) first, then current WAL, to recover all
            // unflushed records after a crash that occurred during WAL rotation.
            let old_path = {
                let mut p = wal_path.as_os_str().to_owned();
                p.push(".old");
                std::path::PathBuf::from(p)
            };
            let old_recovery = WalReader::recover(&old_path);
            if !old_recovery.records.is_empty() {
                let mut dropped = 0usize;
                let recovered: Vec<NativeRecord> = old_recovery
                    .records
                    .into_iter()
                    .filter_map(|record| match NativeRecord::from_json_value(record) {
                        Ok(r) => Some(r),
                        Err(e) => { dropped += 1; eprintln!("[WARN] akidb: WAL(.old) record parse error — record dropped: {e}"); None }
                    })
                    .collect();
                if dropped > 0 {
                    eprintln!("[WARN] akidb: WAL(.old) recovery dropped {dropped} unparseable record(s) — check for WAL corruption");
                }
                buffer.add_batch(&recovered);
            }
            let recovery = WalReader::recover(wal_path);
            if !recovery.records.is_empty() {
                let mut dropped = 0usize;
                let recovered: Vec<NativeRecord> = recovery
                    .records
                    .into_iter()
                    .filter_map(|record| match NativeRecord::from_json_value(record) {
                        Ok(r) => Some(r),
                        Err(e) => { dropped += 1; eprintln!("[WARN] akidb: WAL record parse error — record dropped: {e}"); None }
                    })
                    .collect();
                if dropped > 0 {
                    eprintln!("[WARN] akidb: WAL recovery dropped {dropped} unparseable record(s) — check for WAL corruption");
                }
                buffer.add_batch(&recovered);
            }
            last_wal_sequence = recovery.max_sequence.max(old_recovery.max_sequence);
            writer.set_sequence(last_wal_sequence);
            Some(writer)
        } else {
            None
        };

        Ok(Self {
            buffer,
            wal,
            last_wal_sequence,
            metadata,
            storage,
        })
    }

    /// Upsert records into the write path.
    pub fn upsert_batch(
        &mut self,
        collection_id: &str,
        records: &[NativeRecord],
        dimension: usize,
        metric: &str,
        hnsw: HnswParams,
    ) -> Result<UpsertResult> {
        if records.is_empty() {
            return Ok(UpsertResult {
                segment_ids: Vec::new(),
                buffered_count: self.buffer.count(),
            });
        }

        // WAL: persist records before acknowledging.
        if let Some(wal) = &mut self.wal {
            let wal_records: Vec<serde_json::Value> =
                records.iter().map(NativeRecord::to_json_value).collect();
            self.last_wal_sequence = wal.append_batch(&wal_records)?;
        }

        let should_flush = self.buffer.add_batch(records);
        let mut segment_ids = Vec::new();

        if should_flush {
            let flushed = self.flush(collection_id, dimension, metric, hnsw)?;
            segment_ids.extend(flushed);
        }

        Ok(UpsertResult {
            segment_ids,
            buffered_count: self.buffer.count(),
        })
    }

    /// Force-flush buffered records to a new segment.
    pub fn flush(
        &mut self,
        collection_id: &str,
        dimension: usize,
        metric: &str,
        hnsw: HnswParams,
    ) -> Result<Vec<String>> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        let records = self.buffer.drain();
        let segment_id = self.build_and_store(collection_id, &records, dimension, metric, hnsw)?;

        // WAL: mark flushed and truncate.
        if let Some(wal) = &mut self.wal {
            wal.mark_flushed(self.last_wal_sequence)?;
            wal.truncate()?;
        }

        Ok(vec![segment_id])
    }

    pub fn peek_buffer(&self) -> &[NativeRecord] {
        self.buffer.peek()
    }

    pub fn close(&mut self) {
        if let Some(wal) = &mut self.wal {
            wal.close();
        }
    }

    fn build_and_store(
        &self,
        collection_id: &str,
        records: &[NativeRecord],
        dimension: usize,
        metric: &str,
        hnsw: HnswParams,
    ) -> Result<String> {
        let metadata = self
            .metadata
            .lock()
            .map_err(|_| AkiDbError::InvalidArgument("Metadata lock poisoned".to_string()))?;
        let storage = &*self.storage;

        let mut builder = SegmentBuilder::new();

        // Validate dimensions and collect vectors for HNSW in one pass (one clone per vector).
        let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(records.len());
        for rec in records {
            if rec.vector.len() != dimension {
                return Err(AkiDbError::DimensionMismatch {
                    expected: dimension,
                    actual: rec.vector.len(),
                    chunk_id: rec.chunk_id.clone(),
                });
            }
            vectors.push(rec.vector.clone());
        }

        // Build HNSW index before consuming vectors into the builder.
        let mut graph = HnswGraph::new(metric, dimension, hnsw.m, hnsw.ef_construction, hnsw.ef_search);
        graph.build(&vectors);
        let index_data = graph.serialize();

        // Feed the segment builder by moving vectors from the pre-collected Vec,
        // eliminating the second clone per record that was present in the original loop.
        for (rec, vector) in records.iter().zip(vectors) {
            builder.add_record_with_text(
                rec.chunk_id.clone(),
                vector, // moved — no extra clone
                rec.metadata.clone(),
                rec.chunk_text.clone(),
            )?;
        }

        // Build final segment buffer with embedded index.
        let result = builder.build(Some(&index_data))?;

        // Store in object storage.
        let storage_path = format!("segments/{}/{}.bin", collection_id, result.segment_id);
        storage.put_object(&storage_path, &result.buffer)?;

        // Register in metadata.
        let now = chrono_now();
        metadata.create_segment(&crate::metadata::SegmentMetadata {
            segment_id: result.segment_id.clone(),
            collection_id: collection_id.to_string(),
            record_count: records.len() as i64,
            dimension: dimension as i64,
            size_bytes: result.buffer.len() as i64,
            checksum: checksum_hex(&result.buffer),
            status: "ready".to_string(),
            storage_path,
            created_at: now,
        })?;

        // FTS5: index chunk text for keyword/hybrid search — all in one transaction.
        let fts_owned: Vec<(String, String, String)> = records
            .iter()
            .filter_map(|rec| {
                rec.chunk_text
                    .as_deref()
                    .filter(|t| !t.is_empty())
                    .map(|text| (rec.chunk_id.clone(), collection_id.to_string(), text.to_string()))
            })
            .collect();
        if !fts_owned.is_empty() {
            let fts_refs: Vec<(&str, &str, &str)> = fts_owned
                .iter()
                .map(|(a, b, c)| (a.as_str(), b.as_str(), c.as_str()))
                .collect();
            // The segment is already stored and registered as "ready"; we cannot
            // atomically roll it back here. Surface the failure as a warning so
            // operators can detect partial FTS coverage rather than silently losing
            // keyword/hybrid search results for these chunks.
            if let Err(e) = metadata.fts_insert_batch(&fts_refs) {
                eprintln!(
                    "[WARN] akidb: fts_insert_batch failed for segment {} — keyword search will miss {} chunks: {}",
                    result.segment_id,
                    fts_owned.len(),
                    e,
                );
            }
        }

        Ok(result.segment_id)
    }
}

/// ISO 8601 timestamp without chrono crate.
fn chrono_now() -> String {
    epoch_to_iso8601(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}

/// Convert Unix epoch seconds to an ISO 8601 UTC string (e.g. "2026-02-26T01:23:45Z").
pub fn epoch_to_iso8601(epoch_secs: u64) -> String {
    let secs = epoch_secs;
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Civil date from days since 1970-01-01 (algorithm from Howard Hinnant).
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, hours, minutes, seconds)
}
