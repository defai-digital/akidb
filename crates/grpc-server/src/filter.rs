//! Metadata filter evaluation for search.
//!
//! `SearchRequest` carries two optional, previously-unwired filters:
//! - `tag_filter` — a typed, recursive `TagFilter` (AND / OR / NOT / condition)
//!   evaluated against a vector's stored metadata JSON.
//! - `filter` — a legacy JSON object treated as a key/value subset match.
//!
//! This module turns those proto inputs into a single [`MetadataFilter`] whose
//! [`MetadataFilter::matches`] runs against the metadata JSON of a candidate
//! vector. The evaluation is pure (no storage access) so it is unit-testable in
//! isolation; the search handler is responsible for loading each candidate's
//! metadata and calling `matches`.

use std::cmp::Ordering;

use serde_json::Value;

use crate::proto::{
    tag_filter::FilterType, tag_value::Value as TagVal, TagCondition, TagFilter, TagOperator,
};

/// A compiled, evaluable metadata filter built from a `SearchRequest`.
#[derive(Debug, Clone)]
pub struct MetadataFilter {
    /// Legacy JSON-object subset match (every key/value must be present & equal).
    legacy: Option<serde_json::Map<String, Value>>,
    /// Typed recursive tag filter.
    tag: Option<TagFilter>,
}

impl MetadataFilter {
    /// Build a filter from the raw `SearchRequest` inputs.
    ///
    /// Returns `Ok(None)` when neither input is present (no filtering). Returns
    /// `Err` if the legacy `filter` bytes are non-empty but are not a JSON
    /// object, so the caller can reject the request rather than silently
    /// ignoring a malformed filter.
    pub fn build(
        filter_bytes: &[u8],
        tag_filter: Option<TagFilter>,
    ) -> Result<Option<Self>, String> {
        let legacy = if filter_bytes.is_empty() {
            None
        } else {
            let parsed: Value = serde_json::from_slice(filter_bytes)
                .map_err(|e| format!("legacy `filter` is not valid JSON: {e}"))?;
            match parsed {
                Value::Object(map) => Some(map),
                _ => return Err("legacy `filter` must be a JSON object".to_string()),
            }
        };

        // An empty tag filter (no `filter_type`) is treated as "no tag filter".
        let tag = tag_filter.filter(|t| t.filter_type.is_some());

        if legacy.is_none() && tag.is_none() {
            Ok(None)
        } else {
            Ok(Some(Self { legacy, tag }))
        }
    }

    /// Whether `metadata` (a candidate vector's stored metadata JSON) satisfies
    /// the filter. Both the legacy and tag filters must pass when present.
    pub fn matches(&self, metadata: &Value) -> bool {
        if let Some(legacy) = &self.legacy {
            if !legacy.iter().all(|(k, expected)| {
                metadata
                    .get(k)
                    .is_some_and(|actual| value_subset_matches(expected, actual))
            }) {
                return false;
            }
        }
        if let Some(tag) = &self.tag {
            if !tag_filter_matches(metadata, tag) {
                return false;
            }
        }
        true
    }
}

fn value_subset_matches(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            expected.iter().all(|(key, expected)| {
                actual
                    .get(key)
                    .is_some_and(|actual| value_subset_matches(expected, actual))
            })
        }
        _ => actual == expected,
    }
}

/// Recursively evaluate a typed `TagFilter` against metadata JSON.
pub fn tag_filter_matches(metadata: &Value, filter: &TagFilter) -> bool {
    match filter.filter_type.as_ref() {
        // Empty AND is vacuously true; empty OR is false (standard semantics).
        Some(FilterType::And(and)) => and.filters.iter().all(|f| tag_filter_matches(metadata, f)),
        Some(FilterType::Or(or)) => or.filters.iter().any(|f| tag_filter_matches(metadata, f)),
        Some(FilterType::Not(not)) => not
            .filter
            .as_ref()
            .is_none_or(|f| !tag_filter_matches(metadata, f)),
        Some(FilterType::Condition(cond)) => condition_matches(metadata, cond),
        None => true,
    }
}

fn condition_matches(metadata: &Value, cond: &TagCondition) -> bool {
    let op = TagOperator::try_from(cond.op).unwrap_or(TagOperator::TagOpEq);
    let field = metadata_field(metadata, &cond.key);

    // EXISTS only checks presence of a non-null value.
    if op == TagOperator::TagOpExists {
        return field.is_some_and(|v| !v.is_null());
    }

    let Some(field) = field else { return false };
    if field.is_null() {
        return false;
    }
    let Some(value) = cond.value.as_ref().and_then(|v| v.value.as_ref()) else {
        return false;
    };

    match op {
        TagOperator::TagOpEq => value_equals(field, value),
        TagOperator::TagOpGt => compare(field, value) == Some(Ordering::Greater),
        TagOperator::TagOpLt => compare(field, value) == Some(Ordering::Less),
        TagOperator::TagOpGte => {
            matches!(
                compare(field, value),
                Some(Ordering::Greater | Ordering::Equal)
            )
        }
        TagOperator::TagOpLte => {
            matches!(
                compare(field, value),
                Some(Ordering::Less | Ordering::Equal)
            )
        }
        TagOperator::TagOpContains => value_contains(field, value),
        TagOperator::TagOpExists => true, // handled above
    }
}

fn metadata_field<'a>(metadata: &'a Value, key: &str) -> Option<&'a Value> {
    if let Some(value) = metadata.get(key) {
        return Some(value);
    }

    let mut current = metadata;
    for part in key.split('.') {
        if part.is_empty() {
            return None;
        }
        current = current.get(part)?;
    }
    Some(current)
}

/// Equality between a metadata JSON value and a proto tag value.
fn value_equals(field: &Value, value: &TagVal) -> bool {
    match value {
        TagVal::Text(s) => field.as_str() == Some(s.as_str()),
        TagVal::Number(n) => field.as_f64() == Some(*n),
        TagVal::Boolean(b) => field.as_bool() == Some(*b),
        TagVal::TextList(list) => {
            // Set equality between a JSON string array and the text list.
            let Some(arr) = field.as_array() else {
                return false;
            };
            let field_set: Option<Vec<&str>> = arr.iter().map(|v| v.as_str()).collect();
            let Some(mut field_set) = field_set else {
                return false;
            };
            let mut want: Vec<&str> = list.values.iter().map(|s| s.as_str()).collect();
            field_set.sort_unstable();
            field_set.dedup();
            want.sort_unstable();
            want.dedup();
            field_set == want
        }
    }
}

/// Ordering of a metadata JSON value against a proto tag value, when comparable.
/// Numbers compare numerically; strings lexicographically; anything else is
/// incomparable (`None`).
fn compare(field: &Value, value: &TagVal) -> Option<Ordering> {
    match value {
        TagVal::Number(n) => field.as_f64().and_then(|f| f.partial_cmp(n)),
        TagVal::Text(s) => field.as_str().map(|f| f.cmp(s.as_str())),
        _ => None,
    }
}

/// CONTAINS semantics:
/// - string field + text value: substring match.
/// - string field + text list: matches if any listed string is a substring.
/// - array field + scalar value: membership.
/// - array field + text list: non-empty intersection.
fn value_contains(field: &Value, value: &TagVal) -> bool {
    if let Some(s) = field.as_str() {
        return match value {
            TagVal::Text(needle) => s.contains(needle.as_str()),
            TagVal::TextList(list) => list.values.iter().any(|needle| s.contains(needle.as_str())),
            _ => false,
        };
    }
    if let Some(arr) = field.as_array() {
        return match value {
            TagVal::TextList(list) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .any(|e| list.values.iter().any(|s| s == e)),
            scalar => arr.iter().any(|e| value_equals(e, scalar)),
        };
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AndFilter, NotFilter, OrFilter, TagValue, TextList};
    use serde_json::json;

    fn cond(key: &str, op: TagOperator, value: TagVal) -> TagFilter {
        TagFilter {
            filter_type: Some(FilterType::Condition(TagCondition {
                key: key.to_string(),
                value: Some(TagValue { value: Some(value) }),
                op: op as i32,
            })),
        }
    }

    fn text(s: &str) -> TagVal {
        TagVal::Text(s.to_string())
    }

    #[test]
    fn test_eq_text_number_bool() {
        let meta = json!({"category": "docs", "size": 12.0, "public": true});
        assert!(tag_filter_matches(
            &meta,
            &cond("category", TagOperator::TagOpEq, text("docs"))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("category", TagOperator::TagOpEq, text("code"))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("size", TagOperator::TagOpEq, TagVal::Number(12.0))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("public", TagOperator::TagOpEq, TagVal::Boolean(true))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("public", TagOperator::TagOpEq, TagVal::Boolean(false))
        ));
    }

    #[test]
    fn test_eq_missing_key_is_false() {
        let meta = json!({"a": 1});
        assert!(!tag_filter_matches(
            &meta,
            &cond("b", TagOperator::TagOpEq, text("x"))
        ));
    }

    #[test]
    fn test_exists() {
        let meta = json!({"present": "v", "nullish": null});
        assert!(tag_filter_matches(
            &meta,
            &cond("present", TagOperator::TagOpExists, text(""))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("nullish", TagOperator::TagOpExists, text(""))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("absent", TagOperator::TagOpExists, text(""))
        ));
    }

    #[test]
    fn test_tag_filter_matches_nested_dotted_paths() {
        let meta = json!({
            "contract": {
                "customer": "HGC",
                "year": 2025,
                "tags": ["enterprise", "renewal"]
            }
        });

        assert!(tag_filter_matches(
            &meta,
            &cond("contract.customer", TagOperator::TagOpEq, text("HGC"))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond(
                "contract.year",
                TagOperator::TagOpGte,
                TagVal::Number(2025.0)
            )
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("contract.tags", TagOperator::TagOpContains, text("renewal"))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("contract.year", TagOperator::TagOpExists, text(""))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("contract.amount", TagOperator::TagOpExists, text(""))
        ));
    }

    #[test]
    fn test_tag_filter_prefers_literal_dotted_key_over_path() {
        let meta = json!({
            "contract.year": 2026,
            "contract": {
                "year": 2025
            }
        });

        assert!(tag_filter_matches(
            &meta,
            &cond(
                "contract.year",
                TagOperator::TagOpEq,
                TagVal::Number(2026.0)
            )
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond(
                "contract.year",
                TagOperator::TagOpEq,
                TagVal::Number(2025.0)
            )
        ));
    }

    #[test]
    fn test_numeric_ordering() {
        let meta = json!({"score": 50.0});
        assert!(tag_filter_matches(
            &meta,
            &cond("score", TagOperator::TagOpGt, TagVal::Number(40.0))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("score", TagOperator::TagOpGt, TagVal::Number(50.0))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("score", TagOperator::TagOpGte, TagVal::Number(50.0))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("score", TagOperator::TagOpLt, TagVal::Number(60.0))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("score", TagOperator::TagOpLte, TagVal::Number(50.0))
        ));
    }

    #[test]
    fn test_text_ordering() {
        let meta = json!({"name": "m"});
        assert!(tag_filter_matches(
            &meta,
            &cond("name", TagOperator::TagOpGt, text("a"))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("name", TagOperator::TagOpLt, text("z"))
        ));
    }

    #[test]
    fn test_contains_string_and_array() {
        let meta = json!({"path": "src/main.rs", "tags": ["rust", "cli"]});
        assert!(tag_filter_matches(
            &meta,
            &cond("path", TagOperator::TagOpContains, text("main"))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("path", TagOperator::TagOpContains, text("python"))
        ));
        assert!(tag_filter_matches(
            &meta,
            &cond("tags", TagOperator::TagOpContains, text("rust"))
        ));
        assert!(!tag_filter_matches(
            &meta,
            &cond("tags", TagOperator::TagOpContains, text("go"))
        ));
    }

    #[test]
    fn test_contains_text_list_intersection() {
        let meta = json!({"tags": ["rust", "cli"]});
        let any_of = TagVal::TextList(TextList {
            values: vec!["go".into(), "cli".into()],
        });
        assert!(tag_filter_matches(
            &meta,
            &cond("tags", TagOperator::TagOpContains, any_of)
        ));
        let none_of = TagVal::TextList(TextList {
            values: vec!["go".into(), "python".into()],
        });
        assert!(!tag_filter_matches(
            &meta,
            &cond("tags", TagOperator::TagOpContains, none_of)
        ));
    }

    #[test]
    fn test_eq_text_list_set_equality() {
        let meta = json!({"tags": ["b", "a"]});
        let same = TagVal::TextList(TextList {
            values: vec!["a".into(), "b".into()],
        });
        assert!(tag_filter_matches(
            &meta,
            &cond("tags", TagOperator::TagOpEq, same)
        ));
        let diff = TagVal::TextList(TextList {
            values: vec!["a".into()],
        });
        assert!(!tag_filter_matches(
            &meta,
            &cond("tags", TagOperator::TagOpEq, diff)
        ));
    }

    #[test]
    fn test_eq_text_list_ignores_duplicates_as_set_equality() {
        let meta = json!({"tags": ["rust", "rust", "cli"]});
        let same_set = TagVal::TextList(TextList {
            values: vec!["cli".into(), "rust".into(), "cli".into()],
        });

        assert!(
            tag_filter_matches(&meta, &cond("tags", TagOperator::TagOpEq, same_set)),
            "TextList equality is documented as set equality, so duplicates should not matter"
        );
    }

    #[test]
    fn test_and_or_not() {
        let meta = json!({"category": "docs", "score": 80.0});
        let c_docs = cond("category", TagOperator::TagOpEq, text("docs"));
        let c_high = cond("score", TagOperator::TagOpGte, TagVal::Number(70.0));
        let c_code = cond("category", TagOperator::TagOpEq, text("code"));

        let and = TagFilter {
            filter_type: Some(FilterType::And(AndFilter {
                filters: vec![c_docs.clone(), c_high.clone()],
            })),
        };
        assert!(tag_filter_matches(&meta, &and));

        let or = TagFilter {
            filter_type: Some(FilterType::Or(OrFilter {
                filters: vec![c_code.clone(), c_high.clone()],
            })),
        };
        assert!(tag_filter_matches(&meta, &or));

        let not = TagFilter {
            filter_type: Some(FilterType::Not(Box::new(NotFilter {
                filter: Some(Box::new(c_code)),
            }))),
        };
        assert!(tag_filter_matches(&meta, &not));
    }

    #[test]
    fn test_empty_and_is_true_empty_or_is_false() {
        let meta = json!({"a": 1});
        let and = TagFilter {
            filter_type: Some(FilterType::And(AndFilter { filters: vec![] })),
        };
        let or = TagFilter {
            filter_type: Some(FilterType::Or(OrFilter { filters: vec![] })),
        };
        assert!(tag_filter_matches(&meta, &and));
        assert!(!tag_filter_matches(&meta, &or));
    }

    #[test]
    fn test_build_none_when_no_filters() {
        assert!(MetadataFilter::build(&[], None).unwrap().is_none());
        let empty_tag = TagFilter { filter_type: None };
        assert!(MetadataFilter::build(&[], Some(empty_tag))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_build_legacy_subset_match() {
        let bytes = br#"{"category":"docs","lang":"rust"}"#;
        let mf = MetadataFilter::build(bytes, None).unwrap().unwrap();
        assert!(mf.matches(&json!({"category":"docs","lang":"rust","extra":1})));
        assert!(!mf.matches(&json!({"category":"docs"})));
        assert!(!mf.matches(&json!({"category":"code","lang":"rust"})));
    }

    #[test]
    fn test_build_legacy_nested_subset_match() {
        let bytes = br#"{"contract":{"year":2025}}"#;
        let mf = MetadataFilter::build(bytes, None).unwrap().unwrap();

        assert!(mf.matches(&json!({
            "contract": {
                "customer": "HGC",
                "year": 2025,
                "amount": 1200
            },
            "tenant": "defai"
        })));
        assert!(!mf.matches(&json!({
            "contract": {
                "customer": "HGC",
                "year": 2024,
                "amount": 1200
            }
        })));
    }

    #[test]
    fn test_build_malformed_legacy_filter_errors() {
        assert!(MetadataFilter::build(b"not json", None).is_err());
        assert!(MetadataFilter::build(b"[1,2,3]", None).is_err());
    }

    #[test]
    fn test_build_combines_legacy_and_tag() {
        let bytes = br#"{"lang":"rust"}"#;
        let tag = cond("score", TagOperator::TagOpGte, TagVal::Number(70.0));
        let mf = MetadataFilter::build(bytes, Some(tag)).unwrap().unwrap();
        assert!(mf.matches(&json!({"lang":"rust","score":90.0})));
        assert!(!mf.matches(&json!({"lang":"rust","score":50.0})));
        assert!(!mf.matches(&json!({"lang":"go","score":90.0})));
    }

    #[test]
    fn test_matches_against_null_metadata_excludes() {
        let tag = cond("category", TagOperator::TagOpEq, text("docs"));
        let mf = MetadataFilter::build(&[], Some(tag)).unwrap().unwrap();
        assert!(!mf.matches(&Value::Null));
    }
}
