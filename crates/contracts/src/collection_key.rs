//! Collection key newtype with guaranteed correct encoding
//!
//! This module provides the `CollectionKey` type which guarantees that
//! storage keys are correctly encoded to prevent key collisions.
//!
//! # Problem (BUG-HUNT-004)
//!
//! Previously, keys were encoded as `id:{collection}:{id}` which allowed collisions:
//! ```text
//! collection="foo", id="bar:baz" -> "id:foo:bar:baz"
//! collection="foo:bar", id="baz" -> "id:foo:bar:baz"  // COLLISION!
//! ```
//!
//! # Solution
//!
//! Use length-prefixed encoding that is guaranteed unique:
//! ```text
//! collection="foo", id="bar:baz" -> "id:3:foobar:baz"
//! collection="foo:bar", id="baz" -> "id:7:foo:barbaz"  // No collision
//! ```
//!
//! The `CollectionKey` newtype makes it impossible to create a malformed key.

use serde::{Deserialize, Serialize};

/// Prefix for all ID mapping keys
const ID_MAPPING_PREFIX: &[u8] = b"id:";

/// A storage key for ID mapping with guaranteed correct encoding.
///
/// This type ensures that keys are always properly length-prefixed,
/// preventing the key collision bug (BUG-HUNT-004).
///
/// # Invariants
///
/// - Keys are always in the format: `id:{collection_len}:{collection}{id}`
/// - The encoding is bijective: different (collection, id) pairs always produce different keys
/// - Keys can be safely used as RocksDB keys
///
/// # Example
///
/// ```rust
/// use akidb_contracts::CollectionKey;
///
/// let key = CollectionKey::new("my_collection", "vec-123");
/// assert_eq!(key.as_bytes(), b"id:13:my_collectionvec-123");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionKey(Vec<u8>);

impl CollectionKey {
    /// Create a new collection key with guaranteed correct encoding.
    ///
    /// The key format is: `id:{collection_len}:{collection}{id}`
    ///
    /// This encoding is bijective - different (collection, id) pairs
    /// always produce different keys.
    pub fn new(collection: &str, id: &str) -> Self {
        let collection_len = collection.len();
        let len_str = collection_len.to_string();

        // Pre-calculate capacity for efficiency
        let capacity = ID_MAPPING_PREFIX.len()
            + len_str.len()
            + 1  // colon after length
            + collection_len
            + id.len();

        let mut key = Vec::with_capacity(capacity);
        key.extend_from_slice(ID_MAPPING_PREFIX);
        key.extend_from_slice(len_str.as_bytes());
        key.push(b':');
        key.extend_from_slice(collection.as_bytes());
        key.extend_from_slice(id.as_bytes());

        Self(key)
    }

    /// Get the key as bytes for storage operations
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Convert to owned `Vec<u8>`
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for CollectionKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_key_basic() {
        let key = CollectionKey::new("test", "vec-1");
        // Format: id:{len}:{collection}{id}
        // "test" has length 4, so: id:4:testvec-1
        assert_eq!(key.as_bytes(), b"id:4:testvec-1");
    }

    #[test]
    fn test_collection_key_no_collision() {
        // These would collide with simple colon separator
        let key1 = CollectionKey::new("foo", "bar:baz");
        let key2 = CollectionKey::new("foo:bar", "baz");

        // With length-prefix, they are different
        assert_ne!(key1.as_bytes(), key2.as_bytes());

        // key1: id:3:foobar:baz
        assert_eq!(key1.as_bytes(), b"id:3:foobar:baz");
        // key2: id:7:foo:barbaz
        assert_eq!(key2.as_bytes(), b"id:7:foo:barbaz");
    }

    #[test]
    fn test_collection_key_empty_collection() {
        let key = CollectionKey::new("", "vec-1");
        assert_eq!(key.as_bytes(), b"id:0:vec-1");
    }

    #[test]
    fn test_collection_key_empty_id() {
        let key = CollectionKey::new("test", "");
        assert_eq!(key.as_bytes(), b"id:4:test");
    }

    #[test]
    fn test_collection_key_unicode() {
        // Unicode collection name
        let key = CollectionKey::new("日本語", "vec-1");
        // "日本語" is 9 bytes in UTF-8
        assert!(key.as_bytes().starts_with(b"id:9:"));
    }

    #[test]
    fn test_collection_key_long_collection() {
        let collection = "a".repeat(1000);
        let key = CollectionKey::new(&collection, "vec-1");
        assert!(key.as_bytes().starts_with(b"id:1000:"));
    }

    #[test]
    fn test_collection_key_bijective() {
        // Property: Different inputs always produce different outputs
        let test_cases = [
            ("a", "b"),
            ("ab", ""),
            ("", "ab"),
            ("a:b", "c"),
            ("a", "b:c"),
            ("abc", "def"),
            ("abcdef", ""),
        ];

        let keys: Vec<_> = test_cases
            .iter()
            .map(|(c, i)| CollectionKey::new(c, i))
            .collect();

        // All keys should be unique
        for (i, key1) in keys.iter().enumerate() {
            for (j, key2) in keys.iter().enumerate() {
                if i != j {
                    assert_ne!(
                        key1.as_bytes(),
                        key2.as_bytes(),
                        "Keys for {:?} and {:?} should differ",
                        test_cases[i],
                        test_cases[j]
                    );
                }
            }
        }
    }

    #[test]
    fn test_collection_key_serialization() {
        let key = CollectionKey::new("test", "vec-1");
        let serialized = bincode::serialize(&key).unwrap();
        let deserialized: CollectionKey = bincode::deserialize(&serialized).unwrap();
        assert_eq!(key, deserialized);
    }
}

/// Property-based tests for CollectionKey bijectivity
///
/// These tests verify that the length-prefixed encoding is truly bijective.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Property: Bijectivity - different (collection, id) pairs produce different keys
        ///
        /// This is the core invariant that prevents BUG-HUNT-004.
        #[test]
        fn prop_bijectivity(
            collection1 in ".*",
            id1 in ".*",
            collection2 in ".*",
            id2 in ".*"
        ) {
            let key1 = CollectionKey::new(&collection1, &id1);
            let key2 = CollectionKey::new(&collection2, &id2);

            // If the inputs are different, the outputs must be different
            let inputs_equal = collection1 == collection2 && id1 == id2;
            if !inputs_equal {
                prop_assert_ne!(
                    key1.as_bytes(),
                    key2.as_bytes(),
                    "Keys should differ for ({:?}, {:?}) vs ({:?}, {:?})",
                    collection1, id1, collection2, id2
                );
            } else {
                // If inputs are same, outputs must be same
                prop_assert_eq!(key1.as_bytes(), key2.as_bytes());
            }
        }

        /// Property: Concatenation ambiguity is resolved
        ///
        /// The classic collision case: col="a", id="b:c" vs col="a:b", id="c"
        /// should always produce different keys.
        #[test]
        fn prop_no_concatenation_collision(
            prefix in "[a-z]{1,10}",
            middle in "[a-z]{1,10}",
            suffix in "[a-z]{1,10}"
        ) {
            // Create two inputs that would collide with naive concatenation
            let collection1 = prefix.clone();
            let id1 = format!(":{}{}", middle, suffix);

            let collection2 = format!("{}:{}", prefix, middle);
            let id2 = suffix.clone();

            let key1 = CollectionKey::new(&collection1, &id1);
            let key2 = CollectionKey::new(&collection2, &id2);

            // These must never collide
            prop_assert_ne!(
                key1.as_bytes(),
                key2.as_bytes(),
                "Collision detected! ({:?}, {:?}) == ({:?}, {:?})",
                collection1, id1, collection2, id2
            );
        }

        /// Property: Keys always have the correct prefix
        #[test]
        fn prop_has_correct_prefix(collection in ".*", id in ".*") {
            let key = CollectionKey::new(&collection, &id);
            prop_assert!(key.as_bytes().starts_with(b"id:"));
        }

        /// Property: Serialization round-trip preserves equality
        #[test]
        fn prop_serialization_roundtrip(collection in "[a-zA-Z0-9]{1,50}", id in "[a-zA-Z0-9]{1,50}") {
            let key = CollectionKey::new(&collection, &id);
            let serialized = bincode::serialize(&key).unwrap();
            let deserialized: CollectionKey = bincode::deserialize(&serialized).unwrap();
            prop_assert_eq!(key, deserialized);
        }

        /// Property: Key length is predictable
        #[test]
        fn prop_key_length_predictable(
            collection in "[a-z]{0,100}",
            id in "[a-z]{0,100}"
        ) {
            let key = CollectionKey::new(&collection, &id);

            // Expected length: "id:" + len_digits + ":" + collection + id
            let len_str = collection.len().to_string();
            let expected_len = 3 + len_str.len() + 1 + collection.len() + id.len();

            prop_assert_eq!(key.as_bytes().len(), expected_len);
        }
    }
}
