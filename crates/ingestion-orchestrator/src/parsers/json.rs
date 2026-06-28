//! JSON Document Parser

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// JSON document parser using serde_json
pub struct JsonParser;

impl JsonParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentParser for JsonParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        let value: serde_json::Value = serde_json::from_slice(data)?;

        // Extract text by flattening JSON values
        let text = extract_text_from_json(&value);
        let word_count = text.split_whitespace().count();

        Ok(ParsedDocument {
            text,
            metadata: DocumentMetadata {
                word_count: Some(word_count),
                extra: Some(serde_json::json!({
                    "type": value_type(&value),
                })),
                ..Default::default()
            },
            format: DocumentFormat::Json,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Json
    }
}

/// Extract text content from JSON value recursively
fn extract_text_from_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(extract_text_from_json)
            .collect::<Vec<_>>()
            .join(" "),
        serde_json::Value::Object(obj) => obj
            .iter()
            .map(|(key, value)| {
                let value_text = extract_text_from_json(value);
                if value_text.is_empty() {
                    key.clone()
                } else {
                    format!("{} {}", key, value_text)
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
        serde_json::Value::Null => String::new(),
    }
}

fn value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Object(_) => "object",
        serde_json::Value::Array(_) => "array",
        _ => "primitive",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_json() {
        let parser = JsonParser::new();
        let data = br#"{"name": "Alice", "age": 30}"#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Alice"));
        assert!(result.text.contains("30"));
    }

    #[test]
    fn test_parse_nested_json() {
        let parser = JsonParser::new();
        let data = br#"{"person": {"name": "Bob"}, "items": ["a", "b"]}"#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Bob"));
        assert!(result.text.contains("a"));
        assert!(result.text.contains("b"));
    }

    #[test]
    fn test_parse_json_preserves_object_keys_for_retrieval() {
        let parser = JsonParser::new();
        let data = br#"{"api_name": "text_search", "contract_amount": 1200}"#;

        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("api_name"));
        assert!(result.text.contains("text_search"));
        assert!(result.text.contains("contract_amount"));
        assert!(result.text.contains("1200"));
    }
}
