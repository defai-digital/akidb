//! HTML Document Parser

use scraper::{ElementRef, Html, Selector};
use std::collections::HashSet;

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// HTML document parser using scraper
pub struct HtmlParser;

impl HtmlParser {
    pub fn new() -> Self {
        Self
    }

    /// Recursively extract text from an element, skipping script and style elements
    fn extract_text_excluding_scripts(
        element: ElementRef,
        text: &mut String,
        excluded_tags: &HashSet<&str>,
    ) {
        for child in element.children() {
            if let Some(el) = ElementRef::wrap(child) {
                let tag_name = el.value().name();
                if excluded_tags.contains(tag_name) {
                    // Skip script, style, and other excluded elements
                    continue;
                }
                Self::push_semantic_attributes(el, text);
                // Recursively process child elements
                Self::extract_text_excluding_scripts(el, text, excluded_tags);
            } else if let Some(text_node) = child.value().as_text() {
                let trimmed = text_node.trim();
                if !trimmed.is_empty() {
                    Self::push_text_piece(text, trimmed);
                }
            }
        }
    }

    fn push_semantic_attributes(element: ElementRef, text: &mut String) {
        for attr in ["alt", "aria-label", "title"] {
            if let Some(value) = element.value().attr(attr).map(str::trim) {
                if !value.is_empty() {
                    Self::push_text_piece(text, value);
                }
            }
        }
    }

    fn push_text_piece(text: &mut String, piece: &str) {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(piece);
    }
}

impl Default for HtmlParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentParser for HtmlParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        let html_str = String::from_utf8_lossy(data);
        let document = Html::parse_document(&html_str);

        // Extract title
        let title = Selector::parse("title")
            .ok()
            .and_then(|sel| document.select(&sel).next())
            .map(|el| el.text().collect::<String>())
            .map(|title| title.trim().to_string())
            .filter(|title| !title.is_empty());

        // Tags to exclude from text extraction
        let excluded_tags: HashSet<&str> = ["script", "style", "noscript", "template"]
            .iter()
            .copied()
            .collect();

        let mut text = String::new();

        // Try to extract from body first
        if let Some(body_sel) = Selector::parse("body").ok() {
            if let Some(body) = document.select(&body_sel).next() {
                Self::extract_text_excluding_scripts(body, &mut text, &excluded_tags);
            }
        }

        // Fallback: if no body, extract from root but still exclude scripts/styles
        if text.is_empty() {
            Self::extract_text_excluding_scripts(
                document.root_element(),
                &mut text,
                &excluded_tags,
            );
        }

        if let Some(title_text) = title.as_deref() {
            if text.is_empty() {
                text.push_str(title_text);
            } else if !text.contains(title_text) {
                text = format!("{} {}", title_text, text);
            }
        }

        let word_count = text.split_whitespace().count();

        Ok(ParsedDocument {
            text,
            metadata: DocumentMetadata {
                title,
                word_count: Some(word_count),
                ..Default::default()
            },
            format: DocumentFormat::Html,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Html
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_html() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <head><title>Test Page</title></head>
                <body>
                    <h1>Hello World</h1>
                    <p>This is a test.</p>
                </body>
            </html>
        "#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Hello World"));
        assert!(result.text.contains("This is a test"));
        assert_eq!(result.metadata.title, Some("Test Page".to_string()));
    }

    #[test]
    fn test_parse_html_includes_title_in_retrieval_text() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <head><title>AkiDB Contract Portal</title></head>
                <body>
                    <p>Welcome.</p>
                </body>
            </html>
        "#;
        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("AkiDB Contract Portal"));
        assert!(result.text.contains("Welcome."));
    }

    #[test]
    fn test_parse_html_trims_title_metadata() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <head><title>
                    AkiDB Contract Portal
                </title></head>
                <body>
                    <p>Welcome.</p>
                </body>
            </html>
        "#;
        let result = parser.parse(data).unwrap();

        assert_eq!(
            result.metadata.title,
            Some("AkiDB Contract Portal".to_string())
        );
        assert!(result.text.starts_with("AkiDB Contract Portal"));
    }

    #[test]
    fn test_excludes_script() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <p>Visible text</p>
                    <script>var x = 1;</script>
                </body>
            </html>
        "#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Visible text"));
        // Script content should be excluded
        assert!(!result.text.contains("var x"));
    }

    #[test]
    fn test_excludes_style() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <p>Visible content</p>
                    <style>.hidden { display: none; }</style>
                </body>
            </html>
        "#;
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Visible content"));
        // Style content should be excluded
        assert!(!result.text.contains("display"));
        assert!(!result.text.contains(".hidden"));
    }

    #[test]
    fn test_parse_html_includes_semantic_attributes_for_retrieval() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <img src="arch.png" alt="AkiDB query planner diagram">
                    <button aria-label="Run ingestion sync"></button>
                    <abbr title="Model Context Protocol">MCP</abbr>
                </body>
            </html>
        "#;
        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("AkiDB query planner diagram"));
        assert!(result.text.contains("Run ingestion sync"));
        assert!(result.text.contains("Model Context Protocol"));
    }
}
