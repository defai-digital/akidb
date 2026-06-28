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
        let mut element_stack: Vec<String> = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Text(e)) => {
                    if let Ok(text) = e.unescape() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            texts.push(text_with_current_element(&element_stack, trimmed));
                        }
                    }
                }
                Ok(Event::CData(e)) => {
                    if let Ok(text) = std::str::from_utf8(&e) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            texts.push(text_with_current_element(&element_stack, trimmed));
                        }
                    }
                }
                Ok(Event::Start(e)) => {
                    element_count += 1;
                    let name = element_name(e.name().as_ref());
                    texts.extend(attribute_texts(&name, &e));
                    element_stack.push(name);
                }
                Ok(Event::Empty(e)) => {
                    element_count += 1;
                    let name = element_name(e.name().as_ref());
                    texts.extend(attribute_texts(&name, &e));
                }
                Ok(Event::End(_)) => {
                    element_stack.pop();
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

fn element_name(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).into_owned()
}

fn text_with_current_element(element_stack: &[String], text: &str) -> String {
    match element_stack.last() {
        Some(element) if !element.is_empty() => format!("{} {}", element, text),
        _ => text.to_string(),
    }
}

fn attribute_texts(element: &str, start: &quick_xml::events::BytesStart<'_>) -> Vec<String> {
    start
        .attributes()
        .filter_map(|attr| attr.ok())
        .filter_map(|attr| {
            let key = element_name(attr.key.as_ref());
            let value = attr
                .unescape_value()
                .map(|value| value.into_owned())
                .unwrap_or_else(|_| String::from_utf8_lossy(attr.value.as_ref()).into_owned())
                .trim()
                .to_string();
            if key.is_empty() || value.is_empty() {
                None
            } else {
                Some(format!("{} {} {}", element, key, value))
            }
        })
        .collect()
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

    #[test]
    fn test_parse_xml_preserves_element_value_pairs_for_retrieval() {
        let parser = XmlParser::new();
        let data = br#"
            <contract>
                <customer>HGC</customer>
                <year>2025</year>
                <contract_amount>1200</contract_amount>
            </contract>
        "#;
        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("customer HGC"));
        assert!(result.text.contains("year 2025"));
        assert!(result.text.contains("contract_amount 1200"));
    }

    #[test]
    fn test_parse_xml_preserves_attribute_value_pairs_for_retrieval() {
        let parser = XmlParser::new();
        let data = br#"
            <contracts>
                <contract customer="HGC" year="2025" contract_amount="1200" />
            </contracts>
        "#;
        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("contract customer HGC"));
        assert!(result.text.contains("contract year 2025"));
        assert!(result.text.contains("contract contract_amount 1200"));
    }

    #[test]
    fn test_parse_xml_unescapes_attribute_values_for_retrieval() {
        let parser = XmlParser::new();
        let data = br#"
            <contracts>
                <contract customer="HGC &amp; Co" />
            </contracts>
        "#;
        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("contract customer HGC & Co"));
        assert!(!result.text.contains("HGC &amp; Co"));
    }
}
