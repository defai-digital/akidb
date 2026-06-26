//! Tag types for document metadata and filtering.
//!
//! Tags are optional key-value pairs that can be attached to documents for:
//! - Access control (e.g., `access:level = 3`)
//! - ML labeling (e.g., `ml:sentiment = "positive"`)
//! - General metadata filtering

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Maximum number of tags per document
pub const MAX_TAGS: usize = 50;
/// Maximum length for a tag key
pub const MAX_TAG_KEY_LEN: usize = 64;
/// Maximum length for a tag value (text or individual list items)
pub const MAX_TAG_VALUE_LEN: usize = 256;

/// Typed tag values supporting multiple use cases.
///
/// # Examples
///
/// ```
/// use akidb_common::types::tag::TagValue;
///
/// // Access control
/// let level = TagValue::Number(3.0);
///
/// // ML labeling
/// let sentiment = TagValue::Text("positive".to_string());
/// let labels = TagValue::TextList(vec!["spam".to_string(), "urgent".to_string()]);
///
/// // Flags
/// let verified = TagValue::Boolean(true);
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum TagValue {
    /// General text labels
    Text(String),
    /// Numeric values for levels, scores, etc.
    Number(f64),
    /// Boolean flags
    Boolean(bool),
    /// Multi-label support (e.g., ML classification)
    TextList(Vec<String>),
}

impl TagValue {
    /// Returns the type name as a string for indexing
    pub fn type_name(&self) -> &'static str {
        match self {
            TagValue::Text(_) => "txt",
            TagValue::Number(_) => "num",
            TagValue::Boolean(_) => "bool",
            TagValue::TextList(_) => "lst",
        }
    }

    /// Validates the value against length constraints
    pub fn validate(&self, key: &str) -> Result<(), TagValidationError> {
        match self {
            TagValue::Text(s) if s.len() > MAX_TAG_VALUE_LEN => {
                Err(TagValidationError::ValueTooLong(key.to_string()))
            }
            TagValue::TextList(list) => {
                // BUG-005 FIX: Reject empty TextList as it creates no index entries
                if list.is_empty() {
                    return Err(TagValidationError::EmptyList(key.to_string()));
                }
                for item in list {
                    if item.len() > MAX_TAG_VALUE_LEN {
                        return Err(TagValidationError::ValueTooLong(key.to_string()));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Wrapper for document tags with validation support.
///
/// Tags use namespaced keys by convention:
/// - `access:level` - Access control
/// - `ml:label` - Machine learning labels
/// - `review:status` - Review workflow
/// - `project:name` - Custom project tags
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Tags(pub HashMap<String, TagValue>);

impl Tags {
    /// Create an empty Tags collection
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Check if the tags collection is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get the number of tags
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Insert a tag, returning the previous value if any
    pub fn insert(&mut self, key: impl Into<String>, value: TagValue) -> Option<TagValue> {
        self.0.insert(key.into(), value)
    }

    /// Get a tag value by key
    pub fn get(&self, key: &str) -> Option<&TagValue> {
        self.0.get(key)
    }

    /// Remove a tag by key
    pub fn remove(&mut self, key: &str) -> Option<TagValue> {
        self.0.remove(key)
    }

    /// Iterate over key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (&String, &TagValue)> {
        self.0.iter()
    }

    /// Merge another Tags collection, with other taking precedence
    pub fn merge(&mut self, other: Tags) {
        for (k, v) in other.0 {
            self.0.insert(k, v);
        }
    }

    /// Validate all tags against constraints
    pub fn validate(&self) -> Result<(), TagValidationError> {
        if self.0.len() > MAX_TAGS {
            return Err(TagValidationError::TooManyTags(self.0.len()));
        }

        for (key, value) in &self.0 {
            if key.len() > MAX_TAG_KEY_LEN {
                return Err(TagValidationError::KeyTooLong(key.clone()));
            }
            if key.is_empty() {
                return Err(TagValidationError::EmptyKey);
            }
            value.validate(key)?;
        }

        Ok(())
    }
}

impl From<HashMap<String, TagValue>> for Tags {
    fn from(map: HashMap<String, TagValue>) -> Self {
        Self(map)
    }
}

impl IntoIterator for Tags {
    type Item = (String, TagValue);
    type IntoIter = std::collections::hash_map::IntoIter<String, TagValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Tag validation errors
#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub enum TagValidationError {
    #[error("Too many tags: {0} (max: {MAX_TAGS})")]
    TooManyTags(usize),

    #[error("Tag key too long: {0} (max: {MAX_TAG_KEY_LEN} chars)")]
    KeyTooLong(String),

    #[error("Tag key cannot be empty")]
    EmptyKey,

    #[error("Tag value too long for key: {0} (max: {MAX_TAG_VALUE_LEN} chars)")]
    ValueTooLong(String),

    /// BUG-005 FIX: Empty TextList creates no index entries and is likely a bug
    #[error("Empty TextList for key: {0}")]
    EmptyList(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_value_types() {
        assert_eq!(TagValue::Text("test".into()).type_name(), "txt");
        assert_eq!(TagValue::Number(42.0).type_name(), "num");
        assert_eq!(TagValue::Boolean(true).type_name(), "bool");
        assert_eq!(TagValue::TextList(vec!["item".to_string()]).type_name(), "lst");
    }

    #[test]
    fn test_empty_text_list_rejected() {
        let mut tags = Tags::new();
        tags.insert("labels", TagValue::TextList(vec![]));
        assert_eq!(
            tags.validate(),
            Err(TagValidationError::EmptyList("labels".to_string()))
        );
    }

    #[test]
    fn test_tags_validation_success() {
        let mut tags = Tags::new();
        tags.insert("access:level", TagValue::Number(3.0));
        tags.insert("ml:sentiment", TagValue::Text("positive".to_string()));
        assert!(tags.validate().is_ok());
    }

    #[test]
    fn test_tags_too_many() {
        let mut tags = Tags::new();
        for i in 0..=MAX_TAGS {
            tags.insert(format!("key{}", i), TagValue::Boolean(true));
        }
        assert_eq!(
            tags.validate(),
            Err(TagValidationError::TooManyTags(MAX_TAGS + 1))
        );
    }

    #[test]
    fn test_key_too_long() {
        let mut tags = Tags::new();
        let long_key = "k".repeat(MAX_TAG_KEY_LEN + 1);
        tags.insert(long_key.clone(), TagValue::Boolean(true));
        assert_eq!(
            tags.validate(),
            Err(TagValidationError::KeyTooLong(long_key))
        );
    }

    #[test]
    fn test_value_too_long() {
        let mut tags = Tags::new();
        let long_value = "v".repeat(MAX_TAG_VALUE_LEN + 1);
        tags.insert("key", TagValue::Text(long_value));
        assert_eq!(
            tags.validate(),
            Err(TagValidationError::ValueTooLong("key".to_string()))
        );
    }

    #[test]
    fn test_text_list_item_too_long() {
        let mut tags = Tags::new();
        let long_item = "v".repeat(MAX_TAG_VALUE_LEN + 1);
        tags.insert("labels", TagValue::TextList(vec![long_item]));
        assert_eq!(
            tags.validate(),
            Err(TagValidationError::ValueTooLong("labels".to_string()))
        );
    }

    #[test]
    fn test_tags_merge() {
        let mut tags1 = Tags::new();
        tags1.insert("a", TagValue::Number(1.0));
        tags1.insert("b", TagValue::Number(2.0));

        let mut tags2 = Tags::new();
        tags2.insert("b", TagValue::Number(3.0));
        tags2.insert("c", TagValue::Number(4.0));

        tags1.merge(tags2);

        assert_eq!(tags1.get("a"), Some(&TagValue::Number(1.0)));
        assert_eq!(tags1.get("b"), Some(&TagValue::Number(3.0))); // Overwritten
        assert_eq!(tags1.get("c"), Some(&TagValue::Number(4.0)));
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut tags = Tags::new();
        tags.insert("text", TagValue::Text("hello".to_string()));
        tags.insert("num", TagValue::Number(42.5));
        tags.insert("bool", TagValue::Boolean(true));
        tags.insert(
            "list",
            TagValue::TextList(vec!["a".to_string(), "b".to_string()]),
        );

        let json = serde_json::to_string(&tags).unwrap();
        let parsed: Tags = serde_json::from_str(&json).unwrap();
        assert_eq!(tags, parsed);
    }
}
