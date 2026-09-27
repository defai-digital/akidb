//! Tag Conversion Utilities
//!
//! Converts between gRPC proto TagValue types and Rust native types.

use std::collections::HashMap;

use akidb_common::types::tag::{TagValue as RustTagValue, Tags};
use tonic::Status;

use crate::proto::{tag_value::Value as ProtoValue, TagValue as ProtoTagValue, TextList};

/// Convert proto TagValue to Rust TagValue
pub fn proto_to_rust_tag_value(proto: &ProtoTagValue) -> Result<RustTagValue, Status> {
    match &proto.value {
        Some(ProtoValue::Text(s)) => Ok(RustTagValue::Text(s.clone())),
        Some(ProtoValue::Number(n)) => Ok(RustTagValue::Number(*n)),
        Some(ProtoValue::Boolean(b)) => Ok(RustTagValue::Boolean(*b)),
        Some(ProtoValue::TextList(list)) => Ok(RustTagValue::TextList(list.values.clone())),
        None => Err(Status::invalid_argument("TagValue is empty")),
    }
}

/// Convert Rust TagValue to proto TagValue
pub fn rust_to_proto_tag_value(rust: &RustTagValue) -> ProtoTagValue {
    let value = match rust {
        RustTagValue::Text(s) => ProtoValue::Text(s.clone()),
        RustTagValue::Number(n) => ProtoValue::Number(*n),
        RustTagValue::Boolean(b) => ProtoValue::Boolean(*b),
        RustTagValue::TextList(list) => ProtoValue::TextList(TextList {
            values: list.clone(),
        }),
    };

    ProtoTagValue { value: Some(value) }
}

/// Convert proto tags map to Rust Tags
pub fn proto_to_rust_tags(proto: HashMap<String, ProtoTagValue>) -> Result<Tags, Status> {
    let mut rust_tags = Tags::default();

    for (key, value) in proto {
        let rust_value = proto_to_rust_tag_value(&value)?;
        rust_tags.insert(key, rust_value);
    }

    // Validate the tags
    rust_tags
        .validate()
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    Ok(rust_tags)
}

/// Convert Rust Tags to proto tags map
pub fn rust_to_proto_tags(rust: &Tags) -> HashMap<String, ProtoTagValue> {
    rust.0
        .iter()
        .map(|(k, v)| (k.clone(), rust_to_proto_tag_value(v)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_tag_conversion() {
        let rust_value = RustTagValue::Text("hello".to_string());
        let proto_value = rust_to_proto_tag_value(&rust_value);
        let back = proto_to_rust_tag_value(&proto_value).unwrap();

        assert_eq!(rust_value, back);
    }

    #[test]
    fn test_number_tag_conversion() {
        let rust_value = RustTagValue::Number(42.5);
        let proto_value = rust_to_proto_tag_value(&rust_value);
        let back = proto_to_rust_tag_value(&proto_value).unwrap();

        assert_eq!(rust_value, back);
    }

    #[test]
    fn test_boolean_tag_conversion() {
        let rust_value = RustTagValue::Boolean(true);
        let proto_value = rust_to_proto_tag_value(&rust_value);
        let back = proto_to_rust_tag_value(&proto_value).unwrap();

        assert_eq!(rust_value, back);
    }

    #[test]
    fn test_text_list_tag_conversion() {
        let rust_value =
            RustTagValue::TextList(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let proto_value = rust_to_proto_tag_value(&rust_value);
        let back = proto_to_rust_tag_value(&proto_value).unwrap();

        assert_eq!(rust_value, back);
    }

    #[test]
    fn test_empty_tag_value_fails() {
        let proto = ProtoTagValue { value: None };
        let result = proto_to_rust_tag_value(&proto);

        assert!(result.is_err());
    }

    #[test]
    fn test_tags_map_conversion() {
        let mut proto_tags = HashMap::new();
        proto_tags.insert(
            "key1".to_string(),
            ProtoTagValue {
                value: Some(ProtoValue::Text("value1".to_string())),
            },
        );
        proto_tags.insert(
            "key2".to_string(),
            ProtoTagValue {
                value: Some(ProtoValue::Number(123.0)),
            },
        );

        let rust_tags = proto_to_rust_tags(proto_tags).unwrap();

        assert_eq!(rust_tags.len(), 2);
        assert_eq!(
            rust_tags.get("key1"),
            Some(&RustTagValue::Text("value1".to_string()))
        );
        assert_eq!(rust_tags.get("key2"), Some(&RustTagValue::Number(123.0)));
    }
}
