//! BitmapIndex — inverted bitmap index for metadata filtering.
//!
//! Builds a bitmap per (field, value) pair for fast record filtering.
//! Uses u32-word bitmaps (same as TypeScript Uint32Array implementation).
//!
//! Binary layout (BitmapBlock) — wire-compatible with TypeScript:
//!   [4B  field_count (u32 LE)]
//!   [4B  record_count (u32 LE)]
//!   for each field:
//!     [4B  field_name_length (u32 LE)]
//!     [nB  field_name (UTF-8)]
//!     [4B  value_count (u32 LE)]
//!     for each distinct value:
//!       [4B  value_length (u32 LE)]
//!       [nB  value (UTF-8)]
//!       [4B  bitmap_word_count (u32 LE)]
//!       [nB  bitmap words (u32 LE x word_count)]

use std::collections::BTreeMap;

// ─── Bitmap ─────────────────────────────────────────────────────────────────

/// A compact bitmap backed by a Vec<u32>. Each bit represents a record index.
#[derive(Debug, Clone)]
pub struct Bitmap {
    pub words: Vec<u32>,
    pub size: usize,
}

impl Bitmap {
    pub fn new(size: usize) -> Self {
        let word_count = size.div_ceil(32);
        Self {
            words: vec![0; word_count],
            size,
        }
    }

    pub fn from_words(size: usize, words: Vec<u32>) -> Self {
        Self { words, size }
    }

    /// Set bit at position `i`.
    pub fn set(&mut self, i: usize) {
        self.words[i >> 5] |= 1 << (i & 31);
    }

    /// AND intersection — returns a new bitmap.
    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let len = self.words.len().min(other.words.len());
        let size = self.size.min(other.size);
        let mut result = Bitmap::new(size);
        for i in 0..len {
            result.words[i] = self.words[i] & other.words[i];
        }
        result
    }

    /// OR union — returns a new bitmap.
    pub fn or(&self, other: &Bitmap) -> Bitmap {
        let len = self.words.len().max(other.words.len());
        let size = self.size.max(other.size);
        let words: Vec<u32> = (0..len)
            .map(|i| {
                let a = self.words.get(i).copied().unwrap_or(0);
                let b = other.words.get(i).copied().unwrap_or(0);
                a | b
            })
            .collect();
        Bitmap::from_words(size, words)
    }

    /// Return array of set bit indices.
    pub fn to_array(&self) -> Vec<usize> {
        let mut out = Vec::new();
        for (w, &word) in self.words.iter().enumerate() {
            if word == 0 {
                continue;
            }
            let base = w << 5;
            let mut bits = word;
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                let idx = base + bit;
                if idx < self.size {
                    out.push(idx);
                }
                bits &= bits - 1; // clear lowest set bit
            }
        }
        out
    }

}

// ─── BitmapIndex ────────────────────────────────────────────────────────────

/// Map from field name -> (value -> Bitmap).
/// Using BTreeMap for deterministic serialization order.
type FieldIndex = BTreeMap<String, BTreeMap<String, Bitmap>>;

pub struct BitmapIndex {
    index: FieldIndex,
    pub record_count: usize,
}

impl BitmapIndex {
    /// Build a bitmap index from an array of record metadata (JSON objects).
    pub fn build(metadata_list: &[serde_json::Value]) -> Self {
        let size = metadata_list.len();
        let mut index: FieldIndex = BTreeMap::new();

        for (i, meta) in metadata_list.iter().enumerate() {
            if let Some(obj) = meta.as_object() {
                for (key, value) in obj {
                    if value.is_null() {
                        continue;
                    }
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        _ => continue,
                    };

                    let field_map = index.entry(key.clone()).or_default();
                    let bitmap = field_map
                        .entry(value_str)
                        .or_insert_with(|| Bitmap::new(size));
                    bitmap.set(i);
                }
            }
        }

        Self {
            index,
            record_count: size,
        }
    }

    /// Get the bitmap for a specific (field, value) pair.
    pub fn get(&self, field: &str, value: &str) -> Option<&Bitmap> {
        self.index.get(field)?.get(value)
    }

    /// Evaluate a metadata filter using bitmap intersection/union.
    /// Returns record indices matching the filter, or None if no filters provided.
    pub fn evaluate(&self, filters: &serde_json::Value) -> Option<Vec<usize>> {
        let obj = filters.as_object()?;
        let mut result: Option<Bitmap> = None;

        for (key, expected) in obj {
            let field_bitmap = if let Some(arr) = expected.as_array() {
                // OR: union bitmaps for each value in the array.
                let mut union: Option<Bitmap> = None;
                for val in arr {
                    let val_str = match val {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => continue,
                    };
                    if let Some(bm) = self.get(key, &val_str) {
                        union = Some(match union {
                            Some(u) => u.or(bm),
                            None => bm.clone(),
                        });
                    }
                }
                union
            } else if !expected.is_null() {
                let val_str = match expected {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => continue,
                };
                self.get(key, &val_str).cloned()
            } else {
                continue
            };

            match field_bitmap {
                None => return Some(Vec::new()), // no matching records for this key
                Some(fb) => {
                    result = Some(match result {
                        Some(r) => r.and(&fb),
                        None => fb,
                    });
                }
            }
        }

        result.map(|bm| bm.to_array())
    }

    // ── Serialization (wire-compatible with TypeScript) ─────────────────────

    /// Serialize the bitmap index to a binary buffer.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Field count.
        buf.extend_from_slice(&(self.index.len() as u32).to_le_bytes());

        // Record count.
        buf.extend_from_slice(&(self.record_count as u32).to_le_bytes());

        for (field_name, value_map) in &self.index {
            // Field name.
            let name_bytes = field_name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            // Value count.
            buf.extend_from_slice(&(value_map.len() as u32).to_le_bytes());

            for (value, bitmap) in value_map {
                // Value string.
                let val_bytes = value.as_bytes();
                buf.extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(val_bytes);

                // Bitmap words.
                buf.extend_from_slice(&(bitmap.words.len() as u32).to_le_bytes());
                for &word in &bitmap.words {
                    buf.extend_from_slice(&word.to_le_bytes());
                }
            }
        }

        buf
    }

    /// Compute the serialized size (in bytes) of a bitmap block by scanning
    /// its binary layout without fully deserializing it.
    pub fn serialized_size(data: &[u8]) -> usize {
        if data.len() < 8 {
            return data.len();
        }
        let mut offset = 0;

        let field_count = read_u32(data, offset) as usize;
        offset += 4;
        // record_count
        offset += 4;

        for _ in 0..field_count {
            if offset + 4 > data.len() { return offset; }
            let name_len = read_u32(data, offset) as usize;
            offset += 4 + name_len;

            if offset + 4 > data.len() { return offset; }
            let value_count = read_u32(data, offset) as usize;
            offset += 4;

            for _ in 0..value_count {
                if offset + 4 > data.len() { return offset; }
                let val_len = read_u32(data, offset) as usize;
                offset += 4 + val_len;

                if offset + 4 > data.len() { return offset; }
                let word_count = read_u32(data, offset) as usize;
                offset += 4 + word_count * 4;
            }
        }

        offset
    }

    /// Deserialize a bitmap index from a binary buffer.
    pub fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let field_count = read_u32(data, offset) as usize;
        offset += 4;

        let record_count = read_u32(data, offset) as usize;
        offset += 4;

        let mut index: FieldIndex = BTreeMap::new();

        for _ in 0..field_count {
            // Field name.
            let name_len = read_u32(data, offset) as usize;
            offset += 4;
            let field_name = std::str::from_utf8(&data[offset..offset + name_len])
                .unwrap_or("")
                .to_string();
            offset += name_len;

            // Value count.
            let value_count = read_u32(data, offset) as usize;
            offset += 4;

            let mut value_map = BTreeMap::new();

            for _ in 0..value_count {
                // Value string.
                let val_len = read_u32(data, offset) as usize;
                offset += 4;
                let value = std::str::from_utf8(&data[offset..offset + val_len])
                    .unwrap_or("")
                    .to_string();
                offset += val_len;

                // Bitmap words.
                let word_count = read_u32(data, offset) as usize;
                offset += 4;

                let mut words = vec![0u32; word_count];
                for w in &mut words {
                    *w = read_u32(data, offset);
                    offset += 4;
                }

                value_map.insert(value, Bitmap::from_words(record_count, words));
            }

            index.insert(field_name, value_map);
        }

        Self {
            index,
            record_count,
        }
    }
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bitmap_set_and_query() {
        let mut bm = Bitmap::new(100);
        bm.set(0);
        bm.set(31);
        bm.set(32);
        bm.set(99);

        let set_bits = bm.to_array();
        assert!(set_bits.contains(&0));
        assert!(set_bits.contains(&31));
        assert!(set_bits.contains(&32));
        assert!(set_bits.contains(&99));
        assert!(!set_bits.contains(&1));
        assert!(!set_bits.contains(&50));
    }

    #[test]
    fn bitmap_to_array() {
        let mut bm = Bitmap::new(10);
        bm.set(1);
        bm.set(3);
        bm.set(7);

        assert_eq!(bm.to_array(), vec![1, 3, 7]);
    }

    #[test]
    fn bitmap_and() {
        let mut a = Bitmap::new(10);
        a.set(1);
        a.set(3);
        a.set(5);

        let mut b = Bitmap::new(10);
        b.set(3);
        b.set(5);
        b.set(7);

        let result = a.and(&b);
        assert_eq!(result.to_array(), vec![3, 5]);
    }

    #[test]
    fn bitmap_or() {
        let mut a = Bitmap::new(10);
        a.set(1);
        a.set(3);

        let mut b = Bitmap::new(10);
        b.set(3);
        b.set(5);

        let result = a.or(&b);
        assert_eq!(result.to_array(), vec![1, 3, 5]);
    }

    #[test]
    fn bitmap_all_bits_set() {
        // Build a bitmap with all bits set manually.
        let size = 10usize;
        let word_count = size.div_ceil(32);
        let mut words = vec![0xFFFF_FFFFu32; word_count];
        let remaining = size & 31;
        if remaining > 0 && !words.is_empty() {
            let last = words.len() - 1;
            words[last] = (1u32 << remaining) - 1;
        }
        let full = Bitmap::from_words(size, words);
        assert_eq!(full.to_array(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn bitmap_index_build_and_evaluate() {
        let metadata = vec![
            json!({"source": "a.pdf", "category": "science"}),
            json!({"source": "b.pdf", "category": "math"}),
            json!({"source": "c.pdf", "category": "science"}),
        ];
        let idx = BitmapIndex::build(&metadata);

        // Exact match.
        let result = idx.evaluate(&json!({"category": "science"})).unwrap();
        assert_eq!(result, vec![0, 2]);

        // Array (OR).
        let result = idx
            .evaluate(&json!({"category": ["science", "math"]}))
            .unwrap();
        assert_eq!(result, vec![0, 1, 2]);

        // No match.
        let result = idx.evaluate(&json!({"category": "history"})).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn bitmap_index_serialize_roundtrip() {
        let metadata = vec![
            json!({"source": "a.pdf", "category": "science"}),
            json!({"source": "b.pdf", "category": "math"}),
            json!({"source": "c.pdf", "category": "science"}),
        ];
        let idx = BitmapIndex::build(&metadata);
        let serialized = idx.serialize();
        let restored = BitmapIndex::deserialize(&serialized);

        assert_eq!(restored.record_count, 3);

        let result = restored.evaluate(&json!({"category": "science"})).unwrap();
        assert_eq!(result, vec![0, 2]);
    }
}
