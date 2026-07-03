//! HTML Document Parser

use scraper::{ElementRef, Html, Selector};
use std::collections::HashSet;

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// HTML document parser using scraper
pub struct HtmlParser;

#[derive(Debug, Clone, Copy)]
struct StyleVisibility {
    hidden: bool,
    important: bool,
}

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
                if excluded_tags.contains(tag_name) || Self::is_hidden_element(el) {
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

    fn is_hidden_element(element: ElementRef) -> bool {
        if element.value().attr("hidden").is_some() {
            return true;
        }
        if element
            .value()
            .attr("aria-hidden")
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
        {
            return true;
        }
        let Some(style) = element.value().attr("style") else {
            return false;
        };
        let mut display = None;
        let mut visibility = None;
        for (property, value) in style
            .split(';')
            .filter_map(|declaration| declaration.split_once(':'))
        {
            let property = property.trim();
            let (value, important) = parse_inline_style_value(value);
            if property.eq_ignore_ascii_case("display") {
                update_style_visibility(
                    &mut display,
                    value.eq_ignore_ascii_case("none"),
                    important,
                );
            } else if property.eq_ignore_ascii_case("visibility") {
                update_style_visibility(
                    &mut visibility,
                    value.eq_ignore_ascii_case("hidden") || value.eq_ignore_ascii_case("collapse"),
                    important,
                );
            }
        }

        display.is_some_and(|state| state.hidden) || visibility.is_some_and(|state| state.hidden)
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

fn parse_inline_style_value(value: &str) -> (&str, bool) {
    match value.split_once('!') {
        Some((value, priority)) => (
            value.trim(),
            priority.trim().eq_ignore_ascii_case("important"),
        ),
        None => (value.trim(), false),
    }
}

fn update_style_visibility(state: &mut Option<StyleVisibility>, hidden: bool, important: bool) {
    if state.is_none_or(|current| important || !current.important) {
        *state = Some(StyleVisibility { hidden, important });
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
        if let Ok(body_sel) = Selector::parse("body") {
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
            } else if !contains_phrase_with_boundaries(&text, title_text) {
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

fn contains_phrase_with_boundaries(text: &str, phrase: &str) -> bool {
    let phrase = phrase.trim();
    if phrase.is_empty() {
        return true;
    }

    let text = text.to_lowercase();
    let phrase = phrase.to_lowercase();

    text.match_indices(&phrase).any(|(start, matched)| {
        let end = start + matched.len();
        let before_boundary = start == 0
            || text[..start]
                .chars()
                .next_back()
                .is_some_and(is_phrase_boundary);
        let after_boundary =
            end == text.len() || text[end..].chars().next().is_some_and(is_phrase_boundary);
        before_boundary && after_boundary
    })
}

fn is_phrase_boundary(c: char) -> bool {
    !c.is_alphanumeric() && c != '_' && c != '-'
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
    fn test_parse_html_includes_title_when_only_present_as_body_substring() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <head><title>Contract</title></head>
                <body>
                    <p>Contractor portal status</p>
                </body>
            </html>
        "#;

        let result = parser.parse(data).unwrap();

        assert_eq!(result.metadata.title, Some("Contract".to_string()));
        assert!(
            result.text.starts_with("Contract Contractor portal status"),
            "{}",
            result.text
        );
    }

    #[test]
    fn test_parse_html_does_not_duplicate_title_before_punctuation() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <head><title>AkiDB</title></head>
                <body>
                    <h1>AkiDB.</h1>
                    <p>Retrieval status.</p>
                </body>
            </html>
        "#;

        let result = parser.parse(data).unwrap();

        assert_eq!(result.text.matches("AkiDB").count(), 1, "{}", result.text);
        assert_eq!(result.text, "AkiDB. Retrieval status.");
    }

    #[test]
    fn test_title_phrase_boundary_handles_punctuation_case_and_identifiers() {
        assert!(contains_phrase_with_boundaries("akidb.", "AkiDB"));
        assert!(contains_phrase_with_boundaries("(AkiDB)", "AkiDB"));
        assert!(!contains_phrase_with_boundaries(
            "contract_amount updated",
            "Contract"
        ));
        assert!(!contains_phrase_with_boundaries(
            "contract-amount updated",
            "Contract"
        ));
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

    #[test]
    fn test_parse_html_excludes_hidden_elements() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <p>Visible contract text</p>
                    <div hidden>Hidden draft contract amount</div>
                    <div aria-hidden="true">Screen-reader hidden token</div>
                    <div style="display:none">Inline display none token</div>
                    <div style="visibility: hidden">Inline visibility hidden token</div>
                    <div style="display: none !important">Important display none token</div>
                    <div style="visibility: hidden !important">Important visibility hidden token</div>
                </body>
            </html>
        "#;

        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("Visible contract text"));
        assert!(!result.text.contains("Hidden draft contract amount"));
        assert!(!result.text.contains("Screen-reader hidden token"));
        assert!(!result.text.contains("Inline display none token"));
        assert!(!result.text.contains("Inline visibility hidden token"));
        assert!(!result.text.contains("Important display none token"));
        assert!(!result.text.contains("Important visibility hidden token"));
    }

    #[test]
    fn test_parse_html_excludes_visibility_collapse_elements() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <table>
                        <tr style="visibility: collapse">
                            <td>Collapsed contract amount 9999</td>
                        </tr>
                        <tr>
                            <td>Visible contract amount 1200</td>
                        </tr>
                    </table>
                </body>
            </html>
        "#;

        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("Visible contract amount 1200"));
        assert!(!result.text.contains("Collapsed contract amount 9999"));
    }

    #[test]
    fn test_parse_html_respects_later_inline_style_overrides() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <div style="display:none; display:block">Visible after display override</div>
                    <div style="visibility:hidden; visibility:visible">Visible after visibility override</div>
                </body>
            </html>
        "#;

        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("Visible after display override"));
        assert!(result.text.contains("Visible after visibility override"));
    }

    #[test]
    fn test_parse_html_respects_inline_style_important_priority() {
        let parser = HtmlParser::new();
        let data = br#"
            <html>
                <body>
                    <div style="display:block !important; display:none">Visible important display token</div>
                    <div style="visibility:visible !important; visibility:hidden">Visible important visibility token</div>
                    <div style="display:none !important; display:block">Hidden important display token</div>
                    <div style="visibility:hidden !important; visibility:visible">Hidden important visibility token</div>
                </body>
            </html>
        "#;

        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("Visible important display token"));
        assert!(result.text.contains("Visible important visibility token"));
        assert!(!result.text.contains("Hidden important display token"));
        assert!(!result.text.contains("Hidden important visibility token"));
    }
}
