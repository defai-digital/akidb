//! SegmentReader parses and validates an immutable segment binary buffer.
//!
//! Binary layout v2.1 (backward-compatible with v1 and v2):
//!   [Header 64B][VectorBlock][IDMap][MetadataBlock][BitmapBlock][TextBlock][IndexBlock][Checksum 32B]

use crate::error::{AkiDbError, Result};
use crate::fp16;
use crate::segment::bitmap::BitmapIndex;
use crate::segment::checksum::SHA256_BYTES;

/// Fixed header size in bytes.
const HEADER_SIZE: usize = 64;

/// Header flags (bytes 62-63).
const FLAG_TEXT_BLOCK: u16 = 0x0001;

#[derive(Debug)]
struct ParsedHeader {
    record_count: u32,
    dimension: u32,
    vector_block_offset: u64,
    id_map_offset: u64,
    metadata_offset: u64,
    bitmap_offset: u64,
    index_offset: u64,
    checksum_offset: u64,
    flags: u16,
}

/// Read-only view over a segment binary buffer.
pub struct SegmentReader {
    data: Vec<u8>,
    header: ParsedHeader,
}

impl SegmentReader {
    /// Parse a segment buffer and validate magic bytes.
    /// Does NOT verify the checksum automatically — call `validate_checksum()`.
    pub fn from_buffer(data: Vec<u8>) -> Result<Self> {
        if data.len() < HEADER_SIZE + SHA256_BYTES {
            return Err(AkiDbError::Storage(
                "Buffer too small to be a valid segment".into(),
            ));
        }
        let header = parse_header(&data)?;
        Ok(Self { data, header })
    }

    /// Vector dimension for all records.
    pub fn dimension(&self) -> u32 {
        self.header.dimension
    }

    /// Decode all vectors from the FP16 vector block.
    pub fn get_vectors(&self) -> Result<Vec<Vec<f32>>> {
        let start = self.header.vector_block_offset as usize;
        let end = self.header.id_map_offset as usize;
        let block = &self.data[start..end];

        let bytes_per_vector = self.header.dimension as usize * 2; // FP16 = 2 bytes per element
        let expected_bytes = self.header.record_count as usize * bytes_per_vector;
        if block.len() < expected_bytes {
            return Err(AkiDbError::Storage(format!(
                "Vector block too small: need {expected_bytes} bytes, have {}",
                block.len()
            )));
        }

        let mut results = Vec::with_capacity(self.header.record_count as usize);
        for i in 0..self.header.record_count as usize {
            let offset = i * bytes_per_vector;
            let slice = &block[offset..offset + bytes_per_vector];
            results.push(fp16::decode_fp16_vector(slice));
        }

        Ok(results)
    }

    /// Retrieve the ordered chunk ID list from the ID map block.
    pub fn get_chunk_ids(&self) -> Result<Vec<String>> {
        let start = self.header.id_map_offset as usize;
        let end = self.header.metadata_offset as usize;
        let json_str = std::str::from_utf8(&self.data[start..end])
            .map_err(|e| AkiDbError::Serialization(e.to_string()))?;

        #[derive(serde::Deserialize)]
        struct IdMap {
            chunk_ids: Vec<String>,
        }

        let parsed: IdMap = serde_json::from_str(json_str)?;
        Ok(parsed.chunk_ids)
    }

    /// Retrieve per-record metadata from the metadata block.
    pub fn get_metadata(&self) -> Result<Vec<serde_json::Value>> {
        let start = self.header.metadata_offset as usize;
        // v2 segments: bitmap block sits between metadata and index.
        let end = if self.header.bitmap_offset > 0 {
            self.header.bitmap_offset as usize
        } else {
            self.header.index_offset as usize
        };
        let json_str = std::str::from_utf8(&self.data[start..end])
            .map_err(|e| AkiDbError::Serialization(e.to_string()))?;

        #[derive(serde::Deserialize)]
        struct MetadataBlock {
            records: Vec<serde_json::Value>,
        }

        let parsed: MetadataBlock = serde_json::from_str(json_str)?;
        Ok(parsed.records)
    }

    /// Retrieve the raw index block bytes (opaque to segment layer).
    pub fn get_index_data(&self) -> &[u8] {
        let start = self.header.index_offset as usize;
        let end = self.header.checksum_offset as usize;
        &self.data[start..end]
    }

    /// Whether this segment has a bitmap index (v2+).
    pub fn has_bitmap_index(&self) -> bool {
        self.header.bitmap_offset > 0
    }

    /// Retrieve the deserialized BitmapIndex from the segment.
    pub fn get_bitmap_index(&self) -> Option<BitmapIndex> {
        if !self.has_bitmap_index() {
            return None;
        }
        let start = self.header.bitmap_offset as usize;
        // Bitmap ends where TextBlock or IndexBlock starts.
        let end = if self.has_text_block() {
            self.text_block_start()
        } else {
            self.header.index_offset as usize
        };
        if end <= start {
            return None;
        }
        let block = &self.data[start..end];
        Some(BitmapIndex::deserialize(block))
    }

    /// Whether this segment has a TextBlock (flags bit 0).
    pub fn has_text_block(&self) -> bool {
        self.header.flags & FLAG_TEXT_BLOCK != 0
    }

    /// Retrieve chunk texts from the TextBlock.
    /// Returns None if no TextBlock is present.
    pub fn get_chunk_texts(&self) -> Option<Vec<String>> {
        if !self.has_text_block() {
            return None;
        }
        let start = self.text_block_start();
        let end = self.header.index_offset as usize;
        if end <= start + 4 {
            return None;
        }
        let block = &self.data[start..end];
        let count = u32::from_le_bytes(block[0..4].try_into().unwrap()) as usize;
        let mut texts = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            if offset + 4 > block.len() {
                return None; // Corrupt: header count exceeds block length.
            }
            let len = u32::from_le_bytes(block[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len > block.len() {
                return None; // Corrupt: text entry extends past block boundary.
            }
            let text = String::from_utf8_lossy(&block[offset..offset + len]).to_string();
            offset += len;
            texts.push(text);
        }
        if texts.len() != count {
            return None; // Partial parse — treat as absent rather than silently truncated.
        }
        Some(texts)
    }

    /// Compute the start of the TextBlock.
    /// TextBlock sits between BitmapBlock and IndexBlock.
    /// Uses BitmapIndex::serialized_size to find bitmap end.
    fn text_block_start(&self) -> usize {
        if self.header.bitmap_offset > 0 {
            let bitmap_start = self.header.bitmap_offset as usize;
            let bitmap_region = &self.data[bitmap_start..self.header.index_offset as usize];
            let bitmap_size = BitmapIndex::serialized_size(bitmap_region);
            bitmap_start + bitmap_size
        } else {
            // No bitmap — text block starts right after metadata.
            self.header.index_offset as usize
        }
    }
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn parse_header(data: &[u8]) -> Result<ParsedHeader> {
    if &data[0..4] != b"AKDB" {
        let magic = std::str::from_utf8(&data[0..4]).unwrap_or("????");
        return Err(AkiDbError::Storage(format!(
            "Invalid segment magic bytes: expected \"AKDB\", got \"{magic}\""
        )));
    }

    let bitmap_offset = if data.len() > 61 {
        u64::from_le_bytes(data[54..62].try_into().unwrap())
    } else {
        0
    };

    let flags = if data.len() > 63 {
        u16::from_le_bytes(data[62..64].try_into().unwrap())
    } else {
        0
    };

    Ok(ParsedHeader {
        record_count: u32::from_le_bytes(data[6..10].try_into().unwrap()),
        dimension: u32::from_le_bytes(data[10..14].try_into().unwrap()),
        vector_block_offset: u64::from_le_bytes(data[14..22].try_into().unwrap()),
        id_map_offset: u64::from_le_bytes(data[22..30].try_into().unwrap()),
        metadata_offset: u64::from_le_bytes(data[30..38].try_into().unwrap()),
        bitmap_offset,
        index_offset: u64::from_le_bytes(data[38..46].try_into().unwrap()),
        checksum_offset: u64::from_le_bytes(data[46..54].try_into().unwrap()),
        flags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::builder::SegmentBuilder;
    use serde_json::json;

    fn build_test_segment() -> Vec<u8> {
        let mut builder = SegmentBuilder::new();
        builder
            .add_record_with_text("c-1".into(), vec![1.0, 0.0, 0.0, 0.0], json!({"source": "a.pdf"}), None)
            .unwrap();
        builder
            .add_record_with_text("c-2".into(), vec![0.0, 1.0, 0.0, 0.0], json!({"source": "b.pdf"}), None)
            .unwrap();
        builder
            .add_record_with_text("c-3".into(), vec![0.5, 0.5, 0.0, 0.0], json!({"source": "a.pdf"}), None)
            .unwrap();
        builder.build(None).unwrap().buffer
    }

    #[test]
    fn parse_segment() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        assert_eq!(reader.dimension(), 4);
    }

    #[test]
    fn get_chunk_ids() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        let ids = reader.get_chunk_ids().unwrap();
        assert_eq!(ids, vec!["c-1", "c-2", "c-3"]);
    }

    #[test]
    fn get_vectors() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        let vectors = reader.get_vectors().unwrap();
        assert_eq!(vectors.len(), 3);
        assert_eq!(vectors[0].len(), 4);
        // FP16 precision: check approximate values.
        assert!((vectors[0][0] - 1.0).abs() < 0.01);
        assert!((vectors[1][1] - 1.0).abs() < 0.01);
    }

    #[test]
    fn get_metadata() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        let metadata = reader.get_metadata().unwrap();
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata[0]["source"], "a.pdf");
    }

    #[test]
    fn bitmap_index_present() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        assert!(reader.has_bitmap_index());

        let bitmap = reader.get_bitmap_index().unwrap();
        let result = bitmap.evaluate(&json!({"source": "a.pdf"})).unwrap();
        assert_eq!(result, vec![0, 2]);
    }

    #[test]
    fn get_index_data_empty_when_no_hnsw() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        let index_data = reader.get_index_data();
        assert!(index_data.is_empty());
    }

    #[test]
    fn build_with_index_data() {
        let mut builder = SegmentBuilder::new();
        builder
            .add_record_with_text("c-1".into(), vec![1.0, 0.0], json!({}), None)
            .unwrap();

        let fake_index = vec![1u8, 2, 3, 4, 5];
        let result = builder.build(Some(&fake_index)).unwrap();

        let reader = SegmentReader::from_buffer(result.buffer).unwrap();
        assert_eq!(reader.get_index_data(), &[1, 2, 3, 4, 5]);
    }

    // ── TextBlock tests ─────────────────────────────────────────────────────

    fn build_test_segment_with_text() -> Vec<u8> {
        let mut builder = SegmentBuilder::new();
        builder
            .add_record_with_text(
                "c-1".into(),
                vec![1.0, 0.0, 0.0, 0.0],
                json!({"source": "a.pdf"}),
                Some("Hello world from document A.".into()),
            )
            .unwrap();
        builder
            .add_record_with_text(
                "c-2".into(),
                vec![0.0, 1.0, 0.0, 0.0],
                json!({"source": "b.pdf"}),
                Some("Another chunk of text from B.".into()),
            )
            .unwrap();
        builder
            .add_record_with_text(
                "c-3".into(),
                vec![0.5, 0.5, 0.0, 0.0],
                json!({"source": "a.pdf"}),
                None, // no text for this record
            )
            .unwrap();
        builder.build(None).unwrap().buffer
    }

    #[test]
    fn text_block_present_when_records_have_text() {
        let buf = build_test_segment_with_text();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        assert!(reader.has_text_block());
    }

    #[test]
    fn text_block_absent_when_no_text() {
        let buf = build_test_segment();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        assert!(!reader.has_text_block());
        assert!(reader.get_chunk_texts().is_none());
    }

    #[test]
    fn get_chunk_texts_roundtrip() {
        let buf = build_test_segment_with_text();
        let reader = SegmentReader::from_buffer(buf).unwrap();
        let texts = reader.get_chunk_texts().unwrap();
        assert_eq!(texts.len(), 3);
        assert_eq!(texts[0], "Hello world from document A.");
        assert_eq!(texts[1], "Another chunk of text from B.");
        assert_eq!(texts[2], ""); // None stored as empty string
    }

    #[test]
    fn text_block_with_bitmap_and_index() {
        let mut builder = SegmentBuilder::new();
        builder
            .add_record_with_text(
                "c-1".into(),
                vec![1.0, 0.0],
                json!({"type": "pdf"}),
                Some("PDF content here".into()),
            )
            .unwrap();

        let fake_index = vec![10u8, 20, 30];
        let result = builder.build(Some(&fake_index)).unwrap();

        let reader = SegmentReader::from_buffer(result.buffer).unwrap();

        // All blocks should be readable.
        assert!(reader.has_bitmap_index());
        assert!(reader.has_text_block());
        assert_eq!(reader.get_index_data(), &[10, 20, 30]);

        let bitmap = reader.get_bitmap_index().unwrap();
        let matched = bitmap.evaluate(&json!({"type": "pdf"})).unwrap();
        assert_eq!(matched, vec![0]);

        let texts = reader.get_chunk_texts().unwrap();
        assert_eq!(texts, vec!["PDF content here"]);
    }

    #[test]
    fn text_block_with_unicode() {
        let mut builder = SegmentBuilder::new();
        builder
            .add_record_with_text(
                "c-1".into(),
                vec![1.0, 0.0],
                json!({}),
                Some("日本語テスト 🎉".into()),
            )
            .unwrap();

        let result = builder.build(None).unwrap();
        let reader = SegmentReader::from_buffer(result.buffer).unwrap();
        let texts = reader.get_chunk_texts().unwrap();
        assert_eq!(texts[0], "日本語テスト 🎉");
    }
}
