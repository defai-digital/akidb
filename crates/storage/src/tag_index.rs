//! Tag index using RoaringBitmaps for efficient filtering.
//!
//! This module provides a secondary index for tags stored in RocksDB,
//! enabling fast vector filtering by tag key-value pairs.
//!
//! # Storage Format
//!
//! Tags are indexed with keys in the format:
//! - `tag:txt:{key}:{value}` for Text values
//! - `tag:num:{key}:{value}` for Number values
//! - `tag:bool:{key}:{value}` for Boolean values
//! - `tag:lst:{key}:{item}` for TextList values (one entry per item)
//!
//! Values are serialized RoaringBitmaps containing internal vector IDs.

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use roaring::RoaringBitmap;
use rocksdb::{WriteBatch, DB};
use tracing::{debug, warn};

use akidb_common::types::{InternalId, TagValue, Tags};
use akidb_common::{AkiDbError, Result};

/// Prefix for tag index keys in RocksDB
const TAG_INDEX_PREFIX: &[u8] = b"tag:";

/// FIX BUG-HUNT-203: Order-preserving encoding for f64 values
///
/// IEEE 754 floats don't sort correctly as bytes because:
/// 1. Sign bit makes all negatives sort after positives
/// 2. Negative numbers are in reverse order
///
/// This encoding transforms f64 bits to sort correctly:
/// - For positive numbers (sign bit = 0): flip the sign bit
/// - For negative numbers (sign bit = 1): flip all bits
///
/// This ensures lexicographic byte order matches numeric order.
fn encode_f64_sortable(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let encoded = if (bits >> 63) == 0 {
        // Positive: flip sign bit (0 -> 1) to sort after all negatives
        bits ^ (1u64 << 63)
    } else {
        // Negative: flip all bits to reverse the order
        !bits
    };
    encoded.to_be_bytes()
}

/// FIX BUG-HUNT-203: Decode order-preserving f64 encoding back to f64
#[allow(dead_code)]
fn decode_f64_sortable(bytes: &[u8]) -> Option<f64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    let encoded = u64::from_be_bytes(arr);
    let bits = if (encoded >> 63) == 1 {
        // Was positive: flip sign bit back
        encoded ^ (1u64 << 63)
    } else {
        // Was negative: flip all bits back
        !encoded
    };
    Some(f64::from_bits(bits))
}

/// Tag index for efficient filtering by tag key-value pairs.
///
/// Uses RoaringBitmaps to store sets of vector IDs for each tag value,
/// enabling O(1) membership tests and efficient set operations.
///
/// BUG-H001 FIX: Added write_lock to protect read-modify-write operations
/// on RoaringBitmaps from concurrent modification.
pub struct TagIndex {
    db: Arc<DB>,
    /// Write lock for atomic read-modify-write operations
    write_lock: Mutex<()>,
}

impl TagIndex {
    /// Create a new TagIndex backed by the given RocksDB instance
    pub fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            write_lock: Mutex::new(()),
        }
    }

    /// Build the index key for a tag value
    fn index_key(type_prefix: &str, key: &str, value: &str) -> Vec<u8> {
        format!("tag:{}:{}:{}", type_prefix, key, value).into_bytes()
    }

    /// Convert a TagValue to index keys (may return multiple for TextList)
    ///
    /// FIX BUG-HUNT-203: Numbers now use order-preserving hex encoding instead of
    /// formatted strings, so negative numbers sort correctly in range queries.
    fn tag_to_index_keys(key: &str, value: &TagValue) -> Vec<Vec<u8>> {
        match value {
            TagValue::Text(s) => vec![Self::index_key("txt", key, s)],
            TagValue::Number(n) => {
                // FIX BUG-HUNT-203: Use order-preserving encoding for correct sorting
                // Encode as hex string so it can be used as RocksDB key
                let encoded = encode_f64_sortable(*n);
                let hex = encoded.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                vec![Self::index_key("num", key, &hex)]
            },
            TagValue::Boolean(b) => vec![Self::index_key("bool", key, &b.to_string())],
            TagValue::TextList(list) => list
                .iter()
                .map(|s| Self::index_key("lst", key, s))
                .collect(),
        }
    }

    /// BUG-H007 FIX: Validate numeric tags for NaN/Infinity values
    /// Returns an error if any numeric tag value is NaN or infinite
    fn validate_numeric_tags(tags: &Tags) -> Result<()> {
        for (key, value) in tags.iter() {
            if let TagValue::Number(n) = value {
                if n.is_nan() {
                    return Err(AkiDbError::InvalidParameter(format!(
                        "Tag '{}' has NaN value, which is not allowed for numeric tags",
                        key
                    )));
                }
                if n.is_infinite() {
                    return Err(AkiDbError::InvalidParameter(format!(
                        "Tag '{}' has infinite value, which is not allowed for numeric tags",
                        key
                    )));
                }
            }
        }
        Ok(())
    }

    /// Get a bitmap from RocksDB, or empty if not found
    fn get_bitmap(&self, key: &[u8]) -> Result<RoaringBitmap> {
        match self.db.get(key) {
            Ok(Some(data)) => {
                let mut cursor = Cursor::new(data);
                RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
                    AkiDbError::StorageError(format!("Failed to deserialize bitmap: {}", e))
                })
            }
            Ok(None) => Ok(RoaringBitmap::new()),
            Err(e) => Err(AkiDbError::StorageError(format!(
                "RocksDB get error: {}",
                e
            ))),
        }
    }

    /// Serialize a bitmap for storage
    fn serialize_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut buf).map_err(|e| {
            AkiDbError::StorageError(format!("Failed to serialize bitmap: {}", e))
        })?;
        Ok(buf)
    }

    /// Add a vector ID to the tag index
    ///
    /// This creates or updates index entries for all tags associated with the vector.
    ///
    /// BUG-H001 FIX: Acquires write lock for atomic read-modify-write
    /// BUG-H007 FIX: Validates numeric tags for NaN/Infinity
    pub fn add(&self, id: InternalId, tags: &Tags) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }

        // BUG-H007 FIX: Validate numeric tags before processing
        Self::validate_numeric_tags(tags)?;

        // BUG-001 FIX: Validate ID fits in u32 for RoaringBitmap
        let id_u32 = id.as_u32().ok_or_else(|| {
            AkiDbError::InvalidParameter(format!(
                "Internal ID {} exceeds u32 range for tag index (max: {})",
                id.0,
                u32::MAX
            ))
        })?;

        // BUG-H001 FIX: Acquire write lock for atomic read-modify-write
        let _guard = self.write_lock.lock().map_err(|e| {
            AkiDbError::StorageError(format!("TagIndex write lock poisoned: {}", e))
        })?;

        let mut batch = WriteBatch::default();

        for (key, value) in tags.iter() {
            let index_keys = Self::tag_to_index_keys(key, value);

            for idx_key in index_keys {
                let mut bitmap = self.get_bitmap(&idx_key)?;
                bitmap.insert(id_u32);
                let serialized = Self::serialize_bitmap(&bitmap)?;
                batch.put(&idx_key, &serialized);
            }
        }

        self.db
            .write(batch)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB batch write error: {}", e)))?;

        debug!(id = id.0, tag_count = tags.len(), "Added vector to tag index");
        Ok(())
    }

    /// Remove a vector ID from the tag index
    ///
    /// This removes the vector from all index entries for its tags.
    ///
    /// BUG-H001 FIX: Acquires write lock for atomic read-modify-write
    pub fn remove(&self, id: InternalId, tags: &Tags) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }

        // BUG-001 FIX: Validate ID fits in u32 for RoaringBitmap
        let id_u32 = id.as_u32().ok_or_else(|| {
            AkiDbError::InvalidParameter(format!(
                "Internal ID {} exceeds u32 range for tag index (max: {})",
                id.0,
                u32::MAX
            ))
        })?;

        // BUG-H001 FIX: Acquire write lock for atomic read-modify-write
        let _guard = self.write_lock.lock().map_err(|e| {
            AkiDbError::StorageError(format!("TagIndex write lock poisoned: {}", e))
        })?;

        let mut batch = WriteBatch::default();

        for (key, value) in tags.iter() {
            let index_keys = Self::tag_to_index_keys(key, value);

            for idx_key in index_keys {
                let mut bitmap = self.get_bitmap(&idx_key)?;
                bitmap.remove(id_u32);

                if bitmap.is_empty() {
                    // Clean up empty bitmaps
                    batch.delete(&idx_key);
                } else {
                    let serialized = Self::serialize_bitmap(&bitmap)?;
                    batch.put(&idx_key, &serialized);
                }
            }
        }

        self.db
            .write(batch)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB batch write error: {}", e)))?;

        debug!(id = id.0, tag_count = tags.len(), "Removed vector from tag index");
        Ok(())
    }

    /// Update tags for a vector (atomic remove old, add new)
    ///
    /// BUG-H001 FIX: Acquires write lock for atomic read-modify-write
    /// BUG-H007 FIX: Validates new tags for NaN/Infinity
    pub fn update(&self, id: InternalId, old_tags: &Tags, new_tags: &Tags) -> Result<()> {
        // BUG-H007 FIX: Validate new numeric tags before processing
        Self::validate_numeric_tags(new_tags)?;

        // BUG-001 FIX: Validate ID fits in u32 for RoaringBitmap
        let id_u32 = id.as_u32().ok_or_else(|| {
            AkiDbError::InvalidParameter(format!(
                "Internal ID {} exceeds u32 range for tag index (max: {})",
                id.0,
                u32::MAX
            ))
        })?;

        // BUG-H001 FIX: Acquire write lock for atomic read-modify-write
        let _guard = self.write_lock.lock().map_err(|e| {
            AkiDbError::StorageError(format!("TagIndex write lock poisoned: {}", e))
        })?;

        let mut batch = WriteBatch::default();

        // Remove from old tags
        for (key, value) in old_tags.iter() {
            let index_keys = Self::tag_to_index_keys(key, value);
            for idx_key in index_keys {
                let mut bitmap = self.get_bitmap(&idx_key)?;
                bitmap.remove(id_u32);
                if bitmap.is_empty() {
                    batch.delete(&idx_key);
                } else {
                    let serialized = Self::serialize_bitmap(&bitmap)?;
                    batch.put(&idx_key, &serialized);
                }
            }
        }

        // Add to new tags
        for (key, value) in new_tags.iter() {
            let index_keys = Self::tag_to_index_keys(key, value);
            for idx_key in index_keys {
                let mut bitmap = self.get_bitmap(&idx_key)?;
                bitmap.insert(id_u32);
                let serialized = Self::serialize_bitmap(&bitmap)?;
                batch.put(&idx_key, &serialized);
            }
        }

        self.db
            .write(batch)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB batch write error: {}", e)))?;

        debug!(id = id.0, "Updated vector tags in index");
        Ok(())
    }

    /// Query the index with a filter, returning matching vector IDs
    ///
    /// NOTE: This method does not support NOT filters properly because it lacks
    /// knowledge of the universal set. For filters containing NOT, use `query_with_universe`.
    pub fn query(&self, filter: &TagFilter) -> Result<RoaringBitmap> {
        self.evaluate_filter(filter, None)
    }

    /// Query with a universe set for proper NOT filter support
    ///
    /// The universe should contain all valid vector IDs that could match.
    /// NOT filters will compute complement against this universe.
    ///
    /// BUG-004 FIX: This method properly handles NOT filters by computing complement
    pub fn query_with_universe(&self, filter: &TagFilter, universe: &RoaringBitmap) -> Result<RoaringBitmap> {
        self.evaluate_filter(filter, Some(universe))
    }

    /// Query and return a limited number of results as InternalIds
    /// Note: Results are u32 values from RoaringBitmap, converted to i64 for InternalId
    pub fn query_ids(&self, filter: &TagFilter, limit: Option<usize>) -> Result<Vec<InternalId>> {
        let bitmap = self.query(filter)?;
        // BUG-012 FIX: Safe conversion from u32 bitmap values to i64 InternalIds
        let iter = bitmap.iter().map(|id| InternalId::new(i64::from(id)));

        match limit {
            Some(n) => Ok(iter.take(n).collect()),
            None => Ok(iter.collect()),
        }
    }

    /// Query with universe and return limited results as InternalIds
    pub fn query_ids_with_universe(
        &self,
        filter: &TagFilter,
        universe: &RoaringBitmap,
        limit: Option<usize>
    ) -> Result<Vec<InternalId>> {
        let bitmap = self.query_with_universe(filter, universe)?;
        let iter = bitmap.iter().map(|id| InternalId::new(i64::from(id)));

        match limit {
            Some(n) => Ok(iter.take(n).collect()),
            None => Ok(iter.collect()),
        }
    }

    /// Check if a filter contains any NOT clauses
    pub fn filter_contains_not(filter: &TagFilter) -> bool {
        match filter {
            TagFilter::Not(_) => true,
            TagFilter::And(filters) | TagFilter::Or(filters) => {
                filters.iter().any(Self::filter_contains_not)
            }
            TagFilter::Condition(_) => false,
        }
    }

    /// Evaluate a filter recursively
    fn evaluate_filter(&self, filter: &TagFilter, universe: Option<&RoaringBitmap>) -> Result<RoaringBitmap> {
        match filter {
            TagFilter::And(filters) => {
                if filters.is_empty() {
                    // BUG-H005 FIX: Empty AND returns universal set (all IDs match)
                    // If we have a universe, return it; otherwise log warning and return empty
                    if let Some(univ) = universe {
                        return Ok(univ.clone());
                    } else {
                        // Without universe, we can't know what "all" means
                        warn!("Empty AND filter without universe - returning empty set. Use query_with_universe() for correct behavior.");
                        return Ok(RoaringBitmap::new());
                    }
                }

                let mut result = self.evaluate_filter(&filters[0], universe)?;
                for f in filters.iter().skip(1) {
                    if result.is_empty() {
                        // Short-circuit: AND with empty is always empty
                        break;
                    }
                    result &= self.evaluate_filter(f, universe)?;
                }
                Ok(result)
            }

            TagFilter::Or(filters) => {
                let mut result = RoaringBitmap::new();
                for f in filters {
                    result |= self.evaluate_filter(f, universe)?;
                }
                Ok(result)
            }

            TagFilter::Not(inner_filter) => {
                // BUG-004 FIX: Properly compute complement using universe
                let inner_result = self.evaluate_filter(inner_filter, universe)?;

                match universe {
                    Some(univ) => {
                        // Compute complement: universe - inner_result
                        let mut complement = univ.clone();
                        complement -= &inner_result;
                        Ok(complement)
                    }
                    None => {
                        // No universe provided - return error for NOT without universe
                        Err(AkiDbError::InvalidParameter(
                            "NOT filter requires universe set. Use query_with_universe() instead.".to_string()
                        ))
                    }
                }
            }

            TagFilter::Condition(cond) => self.evaluate_condition(cond),
        }
    }

    /// Evaluate a single condition
    fn evaluate_condition(&self, cond: &TagCondition) -> Result<RoaringBitmap> {
        match cond.op {
            TagOperator::Eq => {
                let index_keys = Self::tag_to_index_keys(&cond.key, &cond.value);
                let mut result = RoaringBitmap::new();
                for key in index_keys {
                    result |= self.get_bitmap(&key)?;
                }
                Ok(result)
            }

            TagOperator::Exists => {
                // For EXISTS, we scan all tag entries for this key
                let prefix = format!("tag:txt:{}:", cond.key);
                let mut result = RoaringBitmap::new();

                // Scan all type prefixes
                for type_prefix in ["txt", "num", "bool", "lst"] {
                    let scan_prefix = format!("tag:{}:{}:", type_prefix, cond.key);
                    let iter = self.db.prefix_iterator(scan_prefix.as_bytes());

                    for item in iter {
                        match item {
                            Ok((key, value)) => {
                                if key.starts_with(scan_prefix.as_bytes()) {
                                    let mut cursor = Cursor::new(value.as_ref());
                                    if let Ok(bitmap) =
                                        RoaringBitmap::deserialize_from(&mut cursor)
                                    {
                                        result |= bitmap;
                                    }
                                } else {
                                    break;
                                }
                            }
                            Err(e) => {
                                return Err(AkiDbError::StorageError(format!(
                                    "RocksDB scan error: {}",
                                    e
                                )));
                            }
                        }
                    }
                }
                let _ = prefix; // Suppress unused warning
                Ok(result)
            }

            TagOperator::Contains => {
                // CONTAINS only makes sense for TextList
                if let TagValue::Text(needle) = &cond.value {
                    let key = Self::index_key("lst", &cond.key, needle);
                    self.get_bitmap(&key)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }

            TagOperator::Gt | TagOperator::Lt | TagOperator::Gte | TagOperator::Lte => {
                // Range queries require scanning numeric entries
                if let TagValue::Number(threshold) = cond.value {
                    // BUG-H007 FIX: Validate threshold is not NaN/Infinity
                    if threshold.is_nan() {
                        return Err(AkiDbError::InvalidParameter(
                            "Range query threshold cannot be NaN".to_string()
                        ));
                    }
                    if threshold.is_infinite() {
                        return Err(AkiDbError::InvalidParameter(
                            "Range query threshold cannot be infinite".to_string()
                        ));
                    }
                    self.range_query(&cond.key, threshold, &cond.op)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
        }
    }

    /// Execute a range query on numeric tags
    ///
    /// FIX BUG-HUNT-203: Now properly decodes order-preserving hex encoding
    fn range_query(&self, key: &str, threshold: f64, op: &TagOperator) -> Result<RoaringBitmap> {
        let scan_prefix = format!("tag:num:{}:", key);
        let mut result = RoaringBitmap::new();

        let iter = self.db.prefix_iterator(scan_prefix.as_bytes());

        for item in iter {
            match item {
                Ok((idx_key, value)) => {
                    if !idx_key.starts_with(scan_prefix.as_bytes()) {
                        break;
                    }

                    // FIX BUG-HUNT-203: Extract and decode the hex-encoded numeric value
                    let key_str = String::from_utf8_lossy(&idx_key);
                    if let Some(hex_str) = key_str.strip_prefix(&scan_prefix) {
                        // Decode hex string to bytes, then to f64
                        if let Some(num_val) = Self::decode_hex_f64(hex_str) {
                            let matches = match op {
                                TagOperator::Gt => num_val > threshold,
                                TagOperator::Lt => num_val < threshold,
                                TagOperator::Gte => num_val >= threshold,
                                TagOperator::Lte => num_val <= threshold,
                                _ => false,
                            };

                            if matches {
                                let mut cursor = Cursor::new(value.as_ref());
                                if let Ok(bitmap) = RoaringBitmap::deserialize_from(&mut cursor) {
                                    result |= bitmap;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(AkiDbError::StorageError(format!(
                        "RocksDB scan error: {}",
                        e
                    )));
                }
            }
        }

        Ok(result)
    }

    /// FIX BUG-HUNT-203: Decode hex string to f64 using order-preserving decoding
    fn decode_hex_f64(hex_str: &str) -> Option<f64> {
        if hex_str.len() != 16 {
            return None;
        }
        let mut bytes = [0u8; 8];
        for (i, chunk) in hex_str.as_bytes().chunks(2).enumerate() {
            if i >= 8 {
                return None;
            }
            let hex_byte = std::str::from_utf8(chunk).ok()?;
            bytes[i] = u8::from_str_radix(hex_byte, 16).ok()?;
        }
        decode_f64_sortable(&bytes)
    }

    /// Get statistics about the tag index
    pub fn stats(&self) -> Result<TagIndexStats> {
        let mut total_entries = 0u64;
        let mut total_ids = 0u64;
        let mut unique_keys = std::collections::HashSet::new();

        let iter = self.db.prefix_iterator(TAG_INDEX_PREFIX);

        for item in iter {
            match item {
                Ok((key, value)) => {
                    if !key.starts_with(TAG_INDEX_PREFIX) {
                        break;
                    }

                    total_entries += 1;

                    // Extract key name
                    let key_str = String::from_utf8_lossy(&key);
                    if let Some(rest) = key_str.strip_prefix("tag:") {
                        if let Some((_, rest)) = rest.split_once(':') {
                            if let Some((tag_key, _)) = rest.split_once(':') {
                                unique_keys.insert(tag_key.to_string());
                            }
                        }
                    }

                    let mut cursor = Cursor::new(value.as_ref());
                    if let Ok(bitmap) = RoaringBitmap::deserialize_from(&mut cursor) {
                        total_ids += bitmap.len() as u64;
                    }
                }
                Err(_) => break,
            }
        }

        Ok(TagIndexStats {
            total_entries,
            total_ids,
            unique_keys: unique_keys.len(),
        })
    }
}

/// Tag filter for composable queries
#[derive(Clone, Debug)]
pub enum TagFilter {
    /// All conditions must match (intersection)
    And(Vec<TagFilter>),
    /// Any condition must match (union)
    Or(Vec<TagFilter>),
    /// Negate a condition (complement)
    Not(Box<TagFilter>),
    /// Single condition
    Condition(TagCondition),
}

impl TagFilter {
    /// Create an AND filter
    pub fn and(filters: Vec<TagFilter>) -> Self {
        Self::And(filters)
    }

    /// Create an OR filter
    pub fn or(filters: Vec<TagFilter>) -> Self {
        Self::Or(filters)
    }

    /// Create a NOT filter
    pub fn not(filter: TagFilter) -> Self {
        Self::Not(Box::new(filter))
    }

    /// Create an equality condition
    pub fn eq(key: impl Into<String>, value: TagValue) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value,
            op: TagOperator::Eq,
        })
    }

    /// Create an EXISTS condition
    pub fn exists(key: impl Into<String>) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value: TagValue::Boolean(true), // Placeholder
            op: TagOperator::Exists,
        })
    }

    /// Create a CONTAINS condition (for TextList)
    pub fn contains(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value: TagValue::Text(value.into()),
            op: TagOperator::Contains,
        })
    }

    /// Create a greater-than condition
    pub fn gt(key: impl Into<String>, value: f64) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value: TagValue::Number(value),
            op: TagOperator::Gt,
        })
    }

    /// Create a less-than condition
    pub fn lt(key: impl Into<String>, value: f64) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value: TagValue::Number(value),
            op: TagOperator::Lt,
        })
    }

    /// Create a greater-than-or-equal condition
    pub fn gte(key: impl Into<String>, value: f64) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value: TagValue::Number(value),
            op: TagOperator::Gte,
        })
    }

    /// Create a less-than-or-equal condition
    pub fn lte(key: impl Into<String>, value: f64) -> Self {
        Self::Condition(TagCondition {
            key: key.into(),
            value: TagValue::Number(value),
            op: TagOperator::Lte,
        })
    }
}

/// A single tag condition
#[derive(Clone, Debug)]
pub struct TagCondition {
    /// Tag key
    pub key: String,
    /// Value to compare against
    pub value: TagValue,
    /// Comparison operator
    pub op: TagOperator,
}

/// Tag comparison operators
#[derive(Clone, Debug, PartialEq)]
pub enum TagOperator {
    /// Equality (works for all types)
    Eq,
    /// Greater than (numeric only)
    Gt,
    /// Less than (numeric only)
    Lt,
    /// Greater than or equal (numeric only)
    Gte,
    /// Less than or equal (numeric only)
    Lte,
    /// Contains element (TextList only)
    Contains,
    /// Key exists (any value)
    Exists,
}

/// Statistics about the tag index
#[derive(Clone, Debug, Default)]
pub struct TagIndexStats {
    /// Total number of index entries
    pub total_entries: u64,
    /// Total number of vector IDs across all entries
    pub total_ids: u64,
    /// Number of unique tag keys
    pub unique_keys: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::Options;
    use tempfile::tempdir;

    fn create_test_db() -> Arc<DB> {
        let dir = tempdir().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Arc::new(DB::open(&opts, dir.path()).unwrap())
    }

    #[test]
    fn test_tag_index_add_and_query() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert("color", TagValue::Text("red".to_string()));
        tags.insert("size", TagValue::Number(42.0));

        index.add(InternalId::new(1), &tags).unwrap();
        index.add(InternalId::new(2), &tags).unwrap();

        // Query by color
        let filter = TagFilter::eq("color", TagValue::Text("red".to_string()));
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_tag_index_remove() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert("status", TagValue::Text("active".to_string()));

        index.add(InternalId::new(1), &tags).unwrap();
        index.add(InternalId::new(2), &tags).unwrap();

        // Remove one
        index.remove(InternalId::new(1), &tags).unwrap();

        let filter = TagFilter::eq("status", TagValue::Text("active".to_string()));
        let result = index.query(&filter).unwrap();
        assert!(!result.contains(1));
        assert!(result.contains(2));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_tag_index_and_filter() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        // Vector 1: red, small
        let mut tags1 = Tags::new();
        tags1.insert("color", TagValue::Text("red".to_string()));
        tags1.insert("size", TagValue::Text("small".to_string()));
        index.add(InternalId::new(1), &tags1).unwrap();

        // Vector 2: red, large
        let mut tags2 = Tags::new();
        tags2.insert("color", TagValue::Text("red".to_string()));
        tags2.insert("size", TagValue::Text("large".to_string()));
        index.add(InternalId::new(2), &tags2).unwrap();

        // Vector 3: blue, small
        let mut tags3 = Tags::new();
        tags3.insert("color", TagValue::Text("blue".to_string()));
        tags3.insert("size", TagValue::Text("small".to_string()));
        index.add(InternalId::new(3), &tags3).unwrap();

        // Query: red AND small (should be just vector 1)
        let filter = TagFilter::and(vec![
            TagFilter::eq("color", TagValue::Text("red".to_string())),
            TagFilter::eq("size", TagValue::Text("small".to_string())),
        ]);
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
        assert!(!result.contains(2));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_tag_index_or_filter() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags1 = Tags::new();
        tags1.insert("color", TagValue::Text("red".to_string()));
        index.add(InternalId::new(1), &tags1).unwrap();

        let mut tags2 = Tags::new();
        tags2.insert("color", TagValue::Text("blue".to_string()));
        index.add(InternalId::new(2), &tags2).unwrap();

        let mut tags3 = Tags::new();
        tags3.insert("color", TagValue::Text("green".to_string()));
        index.add(InternalId::new(3), &tags3).unwrap();

        // Query: red OR blue
        let filter = TagFilter::or(vec![
            TagFilter::eq("color", TagValue::Text("red".to_string())),
            TagFilter::eq("color", TagValue::Text("blue".to_string())),
        ]);
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_tag_index_text_list() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert(
            "labels",
            TagValue::TextList(vec!["urgent".to_string(), "bug".to_string()]),
        );
        index.add(InternalId::new(1), &tags).unwrap();

        let mut tags2 = Tags::new();
        tags2.insert(
            "labels",
            TagValue::TextList(vec!["feature".to_string(), "bug".to_string()]),
        );
        index.add(InternalId::new(2), &tags2).unwrap();

        // Query for "bug" label
        let filter = TagFilter::contains("labels", "bug");
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
        assert!(result.contains(2));

        // Query for "urgent" label
        let filter = TagFilter::contains("labels", "urgent");
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_tag_index_numeric_range() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        for i in 1..=10 {
            let mut tags = Tags::new();
            tags.insert("score", TagValue::Number(i as f64 * 10.0));
            index.add(InternalId::new(i), &tags).unwrap();
        }

        // Query: score > 50
        let filter = TagFilter::gt("score", 50.0);
        let result = index.query(&filter).unwrap();
        assert_eq!(result.len(), 5); // 60, 70, 80, 90, 100

        // Query: score <= 30
        let filter = TagFilter::lte("score", 30.0);
        let result = index.query(&filter).unwrap();
        assert_eq!(result.len(), 3); // 10, 20, 30
    }

    #[test]
    fn test_tag_index_exists() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags1 = Tags::new();
        tags1.insert("optional_field", TagValue::Text("value".to_string()));
        index.add(InternalId::new(1), &tags1).unwrap();

        let mut tags2 = Tags::new();
        tags2.insert("other_field", TagValue::Boolean(true));
        index.add(InternalId::new(2), &tags2).unwrap();

        // Query: optional_field exists
        let filter = TagFilter::exists("optional_field");
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_tag_index_update() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut old_tags = Tags::new();
        old_tags.insert("status", TagValue::Text("pending".to_string()));
        index.add(InternalId::new(1), &old_tags).unwrap();

        let mut new_tags = Tags::new();
        new_tags.insert("status", TagValue::Text("complete".to_string()));
        index.update(InternalId::new(1), &old_tags, &new_tags).unwrap();

        // Should not be found with old tag
        let filter = TagFilter::eq("status", TagValue::Text("pending".to_string()));
        let result = index.query(&filter).unwrap();
        assert!(!result.contains(1));

        // Should be found with new tag
        let filter = TagFilter::eq("status", TagValue::Text("complete".to_string()));
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));
    }

    #[test]
    fn test_tag_index_stats() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        for i in 1..=5 {
            let mut tags = Tags::new();
            tags.insert("category", TagValue::Text(format!("cat{}", i % 2)));
            tags.insert("level", TagValue::Number(i as f64));
            index.add(InternalId::new(i), &tags).unwrap();
        }

        let stats = index.stats().unwrap();
        assert!(stats.total_entries > 0);
        assert!(stats.unique_keys > 0);
    }

    #[test]
    fn test_not_filter_with_universe() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        // Add vectors with different colors
        let mut tags1 = Tags::new();
        tags1.insert("color", TagValue::Text("red".to_string()));
        index.add(InternalId::new(1), &tags1).unwrap();

        let mut tags2 = Tags::new();
        tags2.insert("color", TagValue::Text("blue".to_string()));
        index.add(InternalId::new(2), &tags2).unwrap();

        let mut tags3 = Tags::new();
        tags3.insert("color", TagValue::Text("green".to_string()));
        index.add(InternalId::new(3), &tags3).unwrap();

        // Create universe containing all IDs
        let mut universe = RoaringBitmap::new();
        universe.insert(1);
        universe.insert(2);
        universe.insert(3);

        // Query: NOT red (should return 2 and 3)
        let filter = TagFilter::not(
            TagFilter::eq("color", TagValue::Text("red".to_string()))
        );
        let result = index.query_with_universe(&filter, &universe).unwrap();

        assert!(!result.contains(1)); // red - excluded
        assert!(result.contains(2));  // blue - included
        assert!(result.contains(3));  // green - included
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_not_filter_without_universe_fails() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let filter = TagFilter::not(
            TagFilter::eq("color", TagValue::Text("red".to_string()))
        );

        // Should return error when using NOT without universe
        let result = index.query(&filter);
        assert!(result.is_err());
    }

    #[test]
    fn test_id_exceeds_u32_max_fails() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert("key", TagValue::Text("value".to_string()));

        // ID that exceeds u32::MAX
        let large_id = InternalId::new(u32::MAX as i64 + 1);
        let result = index.add(large_id, &tags);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds u32 range"));
    }

    #[test]
    fn test_filter_contains_not_detection() {
        // Simple NOT
        let filter = TagFilter::not(TagFilter::exists("key"));
        assert!(TagIndex::filter_contains_not(&filter));

        // Nested NOT in AND
        let filter = TagFilter::and(vec![
            TagFilter::eq("a", TagValue::Text("b".to_string())),
            TagFilter::not(TagFilter::exists("c")),
        ]);
        assert!(TagIndex::filter_contains_not(&filter));

        // No NOT
        let filter = TagFilter::and(vec![
            TagFilter::eq("a", TagValue::Text("b".to_string())),
            TagFilter::exists("c"),
        ]);
        assert!(!TagIndex::filter_contains_not(&filter));
    }

    #[test]
    fn test_nan_tag_rejected() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert("score", TagValue::Number(f64::NAN));

        let result = index.add(InternalId::new(1), &tags);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN"));
    }

    #[test]
    fn test_infinity_tag_rejected() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert("score", TagValue::Number(f64::INFINITY));

        let result = index.add(InternalId::new(1), &tags);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("infinite"));
    }

    #[test]
    fn test_negative_infinity_tag_rejected() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        let mut tags = Tags::new();
        tags.insert("score", TagValue::Number(f64::NEG_INFINITY));

        let result = index.add(InternalId::new(1), &tags);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("infinite"));
    }

    #[test]
    fn test_nan_range_query_rejected() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        // Add a valid tag first
        let mut tags = Tags::new();
        tags.insert("score", TagValue::Number(50.0));
        index.add(InternalId::new(1), &tags).unwrap();

        // Try range query with NaN threshold
        let filter = TagFilter::gt("score", f64::NAN);
        let result = index.query(&filter);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN"));
    }

    #[test]
    fn test_infinity_range_query_rejected() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        // Add a valid tag first
        let mut tags = Tags::new();
        tags.insert("score", TagValue::Number(50.0));
        index.add(InternalId::new(1), &tags).unwrap();

        // Try range query with infinity threshold
        let filter = TagFilter::lt("score", f64::INFINITY);
        let result = index.query(&filter);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("infinite"));
    }

    /// FIX BUG-HUNT-203: Test that negative numbers sort correctly in range queries
    #[test]
    fn test_negative_number_range_query() {
        let db = create_test_db();
        let index = TagIndex::new(db);

        // Add vectors with negative scores
        // Previously: -9.0 sorted AFTER -1.0 lexicographically (wrong)
        // Now: -9.0 should sort BEFORE -1.0 (correct numeric order)
        let scores = [-9.0, -5.0, -1.0, 0.0, 1.0, 5.0, 9.0];
        for (i, score) in scores.iter().enumerate() {
            let mut tags = Tags::new();
            tags.insert("score", TagValue::Number(*score));
            index.add(InternalId::new((i + 1) as i64), &tags).unwrap();
        }

        // Query: score > -6.0 (should return -5, -1, 0, 1, 5, 9 -> IDs 2,3,4,5,6,7)
        let filter = TagFilter::gt("score", -6.0);
        let result = index.query(&filter).unwrap();
        assert!(!result.contains(1)); // -9.0 is NOT > -6.0
        assert!(result.contains(2));  // -5.0 IS > -6.0
        assert!(result.contains(3));  // -1.0 IS > -6.0
        assert!(result.contains(4));  // 0.0 IS > -6.0
        assert!(result.contains(5));  // 1.0 IS > -6.0
        assert!(result.contains(6));  // 5.0 IS > -6.0
        assert!(result.contains(7));  // 9.0 IS > -6.0
        assert_eq!(result.len(), 6);

        // Query: score < -2.0 (should return -9, -5 -> IDs 1,2)
        let filter = TagFilter::lt("score", -2.0);
        let result = index.query(&filter).unwrap();
        assert!(result.contains(1));  // -9.0 IS < -2.0
        assert!(result.contains(2));  // -5.0 IS < -2.0
        assert!(!result.contains(3)); // -1.0 is NOT < -2.0
        assert_eq!(result.len(), 2);

        // Query: score >= -5.0 AND score <= -1.0 (should return -5, -1 -> IDs 2,3)
        let filter = TagFilter::and(vec![
            TagFilter::gte("score", -5.0),
            TagFilter::lte("score", -1.0),
        ]);
        let result = index.query(&filter).unwrap();
        assert!(!result.contains(1)); // -9.0 is NOT >= -5.0
        assert!(result.contains(2));  // -5.0 IS in range
        assert!(result.contains(3));  // -1.0 IS in range
        assert!(!result.contains(4)); // 0.0 is NOT <= -1.0
        assert_eq!(result.len(), 2);
    }

    /// FIX BUG-HUNT-203: Test order-preserving encoding round-trip
    #[test]
    fn test_f64_sortable_encoding() {
        use super::{encode_f64_sortable, decode_f64_sortable};

        // Test various values
        let test_values = [
            f64::MIN,
            -1e100,
            -1000.0,
            -1.0,
            -0.001,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            0.001,
            1.0,
            1000.0,
            1e100,
            f64::MAX,
        ];

        // Test round-trip
        for &val in &test_values {
            let encoded = encode_f64_sortable(val);
            let decoded = decode_f64_sortable(&encoded).unwrap();
            assert_eq!(val, decoded, "Round-trip failed for {}", val);
        }

        // Test that encoded values sort correctly
        let mut encoded: Vec<_> = test_values.iter()
            .map(|&v| (v, encode_f64_sortable(v)))
            .collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));

        let sorted_values: Vec<f64> = encoded.iter().map(|(v, _)| *v).collect();
        let mut expected = test_values.to_vec();
        expected.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(sorted_values, expected, "Encoded values don't sort correctly");
    }
}
