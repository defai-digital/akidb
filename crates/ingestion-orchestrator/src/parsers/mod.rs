//! Document Parsers
//!
//! Rust-native parsers for common document formats.
//! Delegates to Python sidecar for complex formats (PDF, complex DOCX).

pub mod csv;
pub mod docx;
pub mod html;
pub mod json;
pub mod xlsx;
pub mod xml;

use crate::Result;

/// Parsed document content
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    /// Extracted text content
    pub text: String,

    /// Document metadata
    pub metadata: DocumentMetadata,

    /// Source format
    pub format: DocumentFormat,
}

/// Document metadata
#[derive(Debug, Clone, Default)]
pub struct DocumentMetadata {
    /// Document title if available
    pub title: Option<String>,

    /// Author if available
    pub author: Option<String>,

    /// Creation date if available
    pub created: Option<String>,

    /// Number of pages/sheets
    pub pages: Option<usize>,

    /// Word count
    pub word_count: Option<usize>,

    /// Additional metadata as JSON
    pub extra: Option<serde_json::Value>,
}

/// Supported document formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentFormat {
    Json,
    Csv,
    Tsv,
    Html,
    Xml,
    Xlsx,
    Pdf,
    Docx,
    Txt,
    Unknown,
}

impl DocumentFormat {
    /// Detect format from file extension
    pub fn from_extension(ext: &str) -> Self {
        match normalize_extension(ext).as_str() {
            "json" => DocumentFormat::Json,
            "csv" => DocumentFormat::Csv,
            "tsv" => DocumentFormat::Tsv,
            "html" | "htm" => DocumentFormat::Html,
            "xml" => DocumentFormat::Xml,
            "xlsx" | "xls" | "xlsm" | "xlsb" | "ods" => DocumentFormat::Xlsx,
            "pdf" => DocumentFormat::Pdf,
            "docx" | "doc" => DocumentFormat::Docx,
            "txt" | "text" | "md" => DocumentFormat::Txt,
            _ => DocumentFormat::Unknown,
        }
    }

    /// Check if format can be parsed in Rust
    ///
    /// Note: DOCX can be parsed in Rust for simple documents.
    /// Use `can_parse_in_rust()` with document data for accurate detection.
    pub fn is_rust_native(&self) -> bool {
        matches!(
            self,
            DocumentFormat::Json
                | DocumentFormat::Csv
                | DocumentFormat::Tsv
                | DocumentFormat::Html
                | DocumentFormat::Xml
                | DocumentFormat::Xlsx
                | DocumentFormat::Txt
                | DocumentFormat::Docx
        )
    }

    /// Check if format requires Python parser
    ///
    /// Note: Simple DOCX files can be parsed in Rust.
    /// Use `requires_python_for_data()` with document data for accurate detection.
    pub fn requires_python(&self) -> bool {
        matches!(self, DocumentFormat::Pdf)
    }
}

fn normalize_extension(input: &str) -> String {
    let without_query = input
        .trim()
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .trim_end_matches(['/', '\\']);
    let file_name = without_query
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(without_query);
    let ext = file_name.rsplit_once('.').map_or(file_name, |(_, ext)| ext);
    ext.trim_start_matches('.').to_ascii_lowercase()
}

/// Check if a DOCX file can be parsed in Rust
///
/// Returns true for simple DOCX files without macros, ActiveX, or OLE objects.
pub fn can_parse_docx_in_rust(data: &[u8]) -> bool {
    docx::DocxParser::is_simple(data)
}

/// Parser trait for document formats
pub trait DocumentParser: Send + Sync {
    /// Parse document content
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument>;

    /// Get supported format
    fn format(&self) -> DocumentFormat;
}

/// Route document to appropriate parser based on format
///
/// For DOCX files, use `route_parser_with_data` to check if the file
/// can be parsed in Rust or needs to be routed to Python.
pub fn route_parser(format: DocumentFormat) -> Option<Box<dyn DocumentParser>> {
    match format {
        DocumentFormat::Json => Some(Box::new(json::JsonParser::new())),
        DocumentFormat::Csv => Some(Box::new(csv::CsvParser::new())),
        DocumentFormat::Tsv => Some(Box::new(csv::CsvParser::tsv())),
        DocumentFormat::Html => Some(Box::new(html::HtmlParser::new())),
        DocumentFormat::Xml => Some(Box::new(xml::XmlParser::new())),
        DocumentFormat::Xlsx => Some(Box::new(xlsx::XlsxParser::new())),
        DocumentFormat::Docx => Some(Box::new(docx::DocxParser::new())),
        DocumentFormat::Txt => Some(Box::new(TxtParser)),
        _ => None, // PDF handled by Python
    }
}

/// Route document to appropriate parser, checking DOCX complexity
///
/// For DOCX files, checks if the file is simple enough for Rust parsing.
/// Returns None if the file should be routed to Python.
pub fn route_parser_with_data(
    format: DocumentFormat,
    data: &[u8],
) -> Option<Box<dyn DocumentParser>> {
    match format {
        DocumentFormat::Docx => {
            if can_parse_docx_in_rust(data) {
                Some(Box::new(docx::DocxParser::new()))
            } else {
                None // Route to Python for complex DOCX
            }
        }
        _ => route_parser(format),
    }
}

/// Simple text file parser
struct TxtParser;

impl DocumentParser for TxtParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        let text = String::from_utf8_lossy(data).to_string();
        let word_count = text.split_whitespace().count();

        Ok(ParsedDocument {
            text,
            metadata: DocumentMetadata {
                word_count: Some(word_count),
                ..Default::default()
            },
            format: DocumentFormat::Txt,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Txt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_detection() {
        assert_eq!(DocumentFormat::from_extension("json"), DocumentFormat::Json);
        assert_eq!(DocumentFormat::from_extension("PDF"), DocumentFormat::Pdf);
        assert_eq!(DocumentFormat::from_extension("tsv"), DocumentFormat::Tsv);
        assert_eq!(DocumentFormat::from_extension("xlsx"), DocumentFormat::Xlsx);
        assert_eq!(DocumentFormat::from_extension("xls"), DocumentFormat::Xlsx);
        assert_eq!(DocumentFormat::from_extension("xlsm"), DocumentFormat::Xlsx);
        assert_eq!(DocumentFormat::from_extension("xlsb"), DocumentFormat::Xlsx);
        assert_eq!(DocumentFormat::from_extension("ods"), DocumentFormat::Xlsx);
        assert_eq!(DocumentFormat::from_extension("docx"), DocumentFormat::Docx);
        assert_eq!(
            DocumentFormat::from_extension("foo"),
            DocumentFormat::Unknown
        );
    }

    #[test]
    fn test_format_detection_accepts_filenames_and_paths() {
        assert_eq!(
            DocumentFormat::from_extension("contracts/2025/HGC.CONTRACT.PDF"),
            DocumentFormat::Pdf
        );
        assert_eq!(
            DocumentFormat::from_extension("https://example.test/docs/report.docx?download=1"),
            DocumentFormat::Docx
        );
        assert_eq!(
            DocumentFormat::from_extension("/tmp/export.tsv#sheet"),
            DocumentFormat::Tsv
        );
    }

    #[test]
    fn test_rust_native_check() {
        assert!(DocumentFormat::Json.is_rust_native());
        assert!(DocumentFormat::Csv.is_rust_native());
        assert!(DocumentFormat::Tsv.is_rust_native());
        assert!(DocumentFormat::Docx.is_rust_native());
        assert!(!DocumentFormat::Pdf.is_rust_native());
    }

    #[test]
    fn test_requires_python() {
        assert!(DocumentFormat::Pdf.requires_python());
        assert!(!DocumentFormat::Docx.requires_python());
        assert!(!DocumentFormat::Json.requires_python());
    }

    #[test]
    fn test_route_parser() {
        assert!(route_parser(DocumentFormat::Json).is_some());
        assert!(route_parser(DocumentFormat::Docx).is_some());
        assert!(route_parser(DocumentFormat::Pdf).is_none());
    }

    #[test]
    fn test_spreadsheet_extensions_share_rust_parser_contract() {
        for ext in ["xlsx", "xls", "xlsm", "xlsb", "ods"] {
            let format = DocumentFormat::from_extension(ext);
            assert_eq!(
                format,
                DocumentFormat::Xlsx,
                "{ext} should route as spreadsheet"
            );
            assert!(
                format.is_rust_native(),
                "{ext} should use the Rust spreadsheet parser"
            );
            assert!(
                !format.requires_python(),
                "{ext} should not require the Python parser"
            );

            let parser = route_parser(format)
                .unwrap_or_else(|| panic!("{ext} should have a routed spreadsheet parser"));
            assert_eq!(parser.format(), DocumentFormat::Xlsx);
        }
    }

    #[test]
    fn test_tsv_extension_routes_to_tab_delimited_parser() {
        let format = DocumentFormat::from_extension("tsv");
        let parser = route_parser(format).unwrap();
        let parsed = parser.parse(b"name\tage\nAlice\t30").unwrap();

        assert_eq!(
            parsed
                .metadata
                .extra
                .as_ref()
                .and_then(|extra| extra.get("columns"))
                .and_then(|columns| columns.as_u64()),
            Some(2)
        );
    }
}
