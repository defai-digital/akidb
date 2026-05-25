//! SegmentBuilder accumulates records and produces an immutable segment binary.
//!
//! Binary layout v2.1 (backward-compatible with v1 and v2):
//!   [Header 64B][VectorBlock][IDMap][MetadataBlock][BitmapBlock][TextBlock][IndexBlock][Checksum 32B]
//!
//! v1 segments have no BitmapBlock and bitmapOffset=0 in the header.
//! v2 segments include a BitmapBlock with inverted bitmap indexes for metadata fields.
//! v2.1 segments add a TextBlock with per-chunk text content.
//!   Header bytes 62-63 are a flags field: bit 0 (0x0001) = TextBlock present.

use crate::error::{AkiDbError, Result};
use crate::fp16;
use crate::segment::bitmap::BitmapIndex;
use crate::segment::checksum::{compute_checksum, SHA256_BYTES};

/// Magic bytes identifying an AkiDB segment file.
const MAGIC: &[u8; 4] = b"AKDB";

/// Current binary format version.
const FORMAT_VERSION: u16 = 2;

/// Header flags (bytes 62-63).
const FLAG_TEXT_BLOCK: u16 = 0x0001;

/// Fixed header size in bytes.
const HEADER_SIZE: usize = 64;

struct PendingRecord {
    chunk_id: String,
    vector: Vec<f32>,
    metadata: serde_json::Value,
    chunk_text: Option<String>,
}

/// Accumulates records and builds an immutable segment binary buffer.
pub struct SegmentBuilder {
    records: Vec<PendingRecord>,
    dimension: Option<usize>,
}

impl SegmentBuilder {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            dimension: None,
        }
    }

    /// Add a record with optional chunk text to the segment being built.
    pub fn add_record_with_text(
        &mut self,
        chunk_id: String,
        vector: Vec<f32>,
        metadata: serde_json::Value,
        chunk_text: Option<String>,
    ) -> Result<()> {
        match self.dimension {
            None => self.dimension = Some(vector.len()),
            Some(dim) if vector.len() != dim => {
                return Err(AkiDbError::DimensionMismatch {
                    expected: dim,
                    actual: vector.len(),
                    chunk_id: chunk_id.to_string(),
                });
            }
            _ => {}
        }
        self.records.push(PendingRecord {
            chunk_id,
            vector,
            metadata,
            chunk_text,
        });
        Ok(())
    }

    /// Build the complete segment binary.
    /// `index_data` is optional opaque index bytes (e.g. HNSW graph).
    pub fn build(&self, index_data: Option<&[u8]>) -> Result<SegmentBuildResult> {
        if self.records.is_empty() {
            return Err(AkiDbError::InvalidArgument(
                "Cannot build empty segment".into(),
            ));
        }

        let dim = self.dimension.unwrap();
        let vector_block = self.build_vector_block(dim);
        let id_map_block = self.build_id_map_block();
        let metadata_block = self.build_metadata_block();
        let bitmap_block = self.build_bitmap_block();
        let text_block = self.build_text_block();
        let index_block = index_data.unwrap_or(&[]);

        let has_text = !text_block.is_empty();
        let mut flags: u16 = 0;
        if has_text {
            flags |= FLAG_TEXT_BLOCK;
        }

        let offsets = compute_offsets(
            vector_block.len(),
            id_map_block.len(),
            metadata_block.len(),
            bitmap_block.len(),
            text_block.len(),
            index_block.len(),
        );

        let header = build_header(self.records.len(), dim, &offsets, flags);

        // Concatenate body.
        let body_len = HEADER_SIZE
            + vector_block.len()
            + id_map_block.len()
            + metadata_block.len()
            + bitmap_block.len()
            + text_block.len()
            + index_block.len();
        let mut body = Vec::with_capacity(body_len + SHA256_BYTES);
        body.extend_from_slice(&header);
        body.extend_from_slice(&vector_block);
        body.extend_from_slice(&id_map_block);
        body.extend_from_slice(&metadata_block);
        body.extend_from_slice(&bitmap_block);
        body.extend_from_slice(&text_block);
        body.extend_from_slice(index_block);

        let checksum = compute_checksum(&body);
        body.extend_from_slice(&checksum);

        let segment_id = uuid::Uuid::new_v4().to_string();

        Ok(SegmentBuildResult {
            buffer: body,
            segment_id,
        })
    }

    fn build_vector_block(&self, _dim: usize) -> Vec<u8> {
        let mut parts = Vec::new();
        for rec in &self.records {
            parts.extend_from_slice(&fp16::encode_fp16_vector(&rec.vector));
        }
        parts
    }

    fn build_id_map_block(&self) -> Vec<u8> {
        let chunk_ids: Vec<&str> = self.records.iter().map(|r| r.chunk_id.as_str()).collect();
        let json = serde_json::json!({ "chunk_ids": chunk_ids });
        serde_json::to_vec(&json).unwrap()
    }

    fn build_metadata_block(&self) -> Vec<u8> {
        let records: Vec<&serde_json::Value> = self.records.iter().map(|r| &r.metadata).collect();
        let json = serde_json::json!({ "records": records });
        serde_json::to_vec(&json).unwrap()
    }

    fn build_bitmap_block(&self) -> Vec<u8> {
        let metadata_list: Vec<serde_json::Value> =
            self.records.iter().map(|r| r.metadata.clone()).collect();
        let bitmap_index = BitmapIndex::build(&metadata_list);
        bitmap_index.serialize()
    }

    /// Build TextBlock: [4B count][per-chunk: 4B len + UTF-8 bytes]
    /// Returns empty Vec if no records have chunk_text.
    fn build_text_block(&self) -> Vec<u8> {
        let has_any_text = self.records.iter().any(|r| r.chunk_text.is_some());
        if !has_any_text {
            return Vec::new();
        }

        let count = self.records.len() as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&count.to_le_bytes());

        for rec in &self.records {
            let text = rec.chunk_text.as_deref().unwrap_or("");
            let text_bytes = text.as_bytes();
            buf.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(text_bytes);
        }

        buf
    }
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of building a segment.
pub struct SegmentBuildResult {
    pub buffer: Vec<u8>,
    pub segment_id: String,
}

// ── Internal helpers ─────────────────────────────────────────────────────────

struct BlockOffsets {
    vector_block_offset: u64,
    id_map_offset: u64,
    metadata_offset: u64,
    bitmap_offset: u64,
    index_offset: u64,
    checksum_offset: u64,
}

fn compute_offsets(
    vector_len: usize,
    id_map_len: usize,
    metadata_len: usize,
    bitmap_len: usize,
    text_len: usize,
    index_len: usize,
) -> BlockOffsets {
    let vector_block_offset = HEADER_SIZE as u64;
    let id_map_offset = vector_block_offset + vector_len as u64;
    let metadata_offset = id_map_offset + id_map_len as u64;
    let bitmap_offset = metadata_offset + metadata_len as u64;
    let index_offset = bitmap_offset + bitmap_len as u64 + text_len as u64;
    let checksum_offset = index_offset + index_len as u64;

    BlockOffsets {
        vector_block_offset,
        id_map_offset,
        metadata_offset,
        bitmap_offset,
        index_offset,
        checksum_offset,
    }
}

fn build_header(record_count: usize, dimension: usize, offsets: &BlockOffsets, flags: u16) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER_SIZE];

    // magic (4 bytes)
    buf[0..4].copy_from_slice(MAGIC);

    // version (2 bytes, u16 LE)
    buf[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());

    // record_count (4 bytes, u32 LE)
    buf[6..10].copy_from_slice(&(record_count as u32).to_le_bytes());

    // dimension (4 bytes, u32 LE)
    buf[10..14].copy_from_slice(&(dimension as u32).to_le_bytes());

    // offsets (5 x 8 bytes, u64 LE)
    buf[14..22].copy_from_slice(&offsets.vector_block_offset.to_le_bytes());
    buf[22..30].copy_from_slice(&offsets.id_map_offset.to_le_bytes());
    buf[30..38].copy_from_slice(&offsets.metadata_offset.to_le_bytes());
    buf[38..46].copy_from_slice(&offsets.index_offset.to_le_bytes());
    buf[46..54].copy_from_slice(&offsets.checksum_offset.to_le_bytes());

    // v2: bitmap offset at byte 54 (8 bytes)
    buf[54..62].copy_from_slice(&offsets.bitmap_offset.to_le_bytes());

    // v2.1: flags (2 bytes) — bit 0 = TextBlock present
    buf[62..64].copy_from_slice(&flags.to_le_bytes());

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_segment() {
        let mut builder = SegmentBuilder::new();
        builder
            .add_record_with_text("c-1".into(), vec![1.0, 0.0, 0.0, 0.0], json!({"source": "a.pdf"}), None)
            .unwrap();
        builder
            .add_record_with_text("c-2".into(), vec![0.0, 1.0, 0.0, 0.0], json!({"source": "b.pdf"}), None)
            .unwrap();

        let result = builder.build(None).unwrap();
        assert!(!result.buffer.is_empty());
        assert!(!result.segment_id.is_empty());
        assert_eq!(result.segment_id.len(), 36); // UUID v4
    }

    #[test]
    fn dimension_mismatch_error() {
        let mut builder = SegmentBuilder::new();
        builder.add_record_with_text("c-1".into(), vec![1.0, 0.0], json!({}), None).unwrap();
        let result = builder.add_record_with_text("c-2".into(), vec![1.0, 0.0, 0.0], json!({}), None);
        assert!(matches!(result, Err(AkiDbError::DimensionMismatch { .. })));
    }

    #[test]
    fn empty_segment_error() {
        let builder = SegmentBuilder::new();
        let result = builder.build(None);
        assert!(result.is_err());
    }

    #[test]
    fn header_magic_and_version() {
        let mut builder = SegmentBuilder::new();
        builder.add_record_with_text("c-1".into(), vec![1.0, 0.0], json!({}), None).unwrap();
        let result = builder.build(None).unwrap();

        assert_eq!(&result.buffer[0..4], b"AKDB");
        assert_eq!(u16::from_le_bytes([result.buffer[4], result.buffer[5]]), 2);
    }
}
