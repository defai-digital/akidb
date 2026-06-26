//! XML Document Parser

use quick_xml::events::Event;
use quick_xml::reader::Reader;

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// XML document parser using quick-xml
pub struct XmlParser;

impl XmlParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for XmlParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentParser for XmlParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        let mut reader = Reader::from_reader(data);
        reader.config_mut().trim_text(true);

        let mut texts = Vec::new();
        let mut buf = Vec::new();
        let mut element_count = 0;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Text(e)) => {
                    if let Ok(text) = e.unescape() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            texts.push(trimmed.to_string());
                        }
                    }
                }
                Ok(Event::CData(e)) => {
                    if let Ok(text) = std::str::from_utf8(&e) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            texts.push(trimmed.to_string());
                        }
                    }
                }
                Ok(Event::Start(_)) => {
                    element_count += 1;
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(crate::IngestionError::Parse(format!(
                        "XML parse error: {}",
                        e
                    )));
                }
                _ => {}
            }
            buf.clear();
        }

        let text = texts.join(" ");
        let word_count = text.split_whitespace().count();

        Ok(ParsedDocument {
            text,
            metadata: DocumentMetadata {
                word_count: Some(word_count),
                extra: Some(serde_json::json!({
                    "element_count": element_count,
                })),
                ..Default::default()
            },
            format: DocumentFormat::Xml,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Xml
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_xml() {
        let parser = XmlParser::new();
        let data = br#"
            <?xml version="1.0"?>
            <root>
                <item>First item</item>
                <item>Second item</item>
            </root>
        "#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("First item"));
        assert!(result.text.contains("Second item"));
    }

    #[test]
    fn test_parse_xml_with_cdata() {
        let parser = XmlParser::new();
        let data = br#"
            <root>
                <content><![CDATA[Some CDATA content]]></content>
            </root>
        "#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Some CDATA content"));
    }
}
