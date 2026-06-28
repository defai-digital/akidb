//! DOCX Parser
//!
//! Rust-native parser for simple DOCX files using docx-rs.
//! Complex DOCX files (macros, ActiveX, OLE objects) are routed to Python.

use std::io::Cursor;

use docx_rs::{
    read_docx, DocumentChild, ParagraphChild, RunChild, TableCellContent, TableChild, TableRowChild,
};
use zip::ZipArchive;

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::{IngestionError, Result};

/// DOCX parser that handles simple documents in Rust
pub struct DocxParser {
    /// Maximum file size to process (in bytes)
    max_size: usize,
}

impl DocxParser {
    /// Create a new DOCX parser
    pub fn new() -> Self {
        Self {
            max_size: 100 * 1024 * 1024, // 100MB
        }
    }

    /// Create with custom max size
    pub fn with_max_size(max_size: usize) -> Self {
        Self { max_size }
    }

    /// Check if a DOCX file is simple enough for Rust parsing
    ///
    /// Returns false if the file contains:
    /// - VBA macros (vbaProject.bin)
    /// - ActiveX controls
    /// - OLE objects
    /// - Complex form elements
    pub fn is_simple(data: &[u8]) -> bool {
        let reader = Cursor::new(data);
        let mut archive = match ZipArchive::new(reader) {
            Ok(a) => a,
            Err(_) => return false,
        };

        // Check for complex elements that require Python
        for i in 0..archive.len() {
            if let Ok(file) = archive.by_index_raw(i) {
                let name = file.name().to_lowercase();

                // VBA macros
                if name.contains("vbaproject") || name.contains("vbaprojectbin") {
                    tracing::debug!("DOCX contains VBA macros, routing to Python");
                    return false;
                }

                // ActiveX controls
                if name.contains("activex") {
                    tracing::debug!("DOCX contains ActiveX controls, routing to Python");
                    return false;
                }

                // OLE objects
                if name.contains("oleobject") || name.contains("embeddings") {
                    tracing::debug!("DOCX contains OLE objects, routing to Python");
                    return false;
                }

                // Complex controls
                if name.contains("controls") {
                    tracing::debug!("DOCX contains form controls, routing to Python");
                    return false;
                }
            }
        }

        true
    }

    /// Extract text from a paragraph
    fn extract_paragraph_text(children: &[ParagraphChild]) -> String {
        let mut text = String::new();

        for child in children {
            match child {
                ParagraphChild::Run(run) => {
                    for run_child in &run.children {
                        match run_child {
                            RunChild::Text(t) => {
                                text.push_str(&t.text);
                            }
                            RunChild::Tab(_) => {
                                text.push('\t');
                            }
                            RunChild::Break(_) => {
                                text.push('\n');
                            }
                            _ => {}
                        }
                    }
                }
                ParagraphChild::Hyperlink(link) => {
                    // Extract text from hyperlink runs
                    for run in &link.children {
                        if let ParagraphChild::Run(r) = run {
                            for run_child in &r.children {
                                if let RunChild::Text(t) = run_child {
                                    text.push_str(&t.text);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        text
    }

    /// Extract text from a table
    fn extract_table_text(rows: &[TableChild]) -> String {
        let mut lines = Vec::new();
        let mut headers: Option<Vec<Option<String>>> = None;
        let table_rows: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|row_child| {
                let TableChild::TableRow(row) = row_child;
                Self::extract_table_row_cells(row)
            })
            .collect();

        let non_empty_rows: Vec<Vec<Option<String>>> = table_rows
            .into_iter()
            .filter(|cells| !row_is_empty(cells))
            .collect();
        let table_cols = non_empty_rows
            .iter()
            .map(|cells| cells.iter().filter(|cell| cell.is_some()).count())
            .max()
            .unwrap_or(0);

        for (idx, cells) in non_empty_rows.iter().enumerate() {
            let next_row = non_empty_rows.get(idx + 1).map(Vec::as_slice);
            let line = match headers.as_deref() {
                Some(headers) => row_text_with_headers(headers, &cells),
                None => {
                    let row_text = row_text_from_cells(&cells);
                    if is_likely_header_row(cells, table_cols, next_row) {
                        headers = Some(cells.clone());
                    }
                    row_text
                }
            };
            if !line.is_empty() {
                lines.push(line);
            }
        }

        lines.join("\n")
    }

    fn extract_table_row_cells(row: &docx_rs::TableRow) -> Vec<Option<String>> {
        let mut cells = Vec::new();

        for cell_child in &row.cells {
            let TableRowChild::TableCell(cell) = cell_child;
            let mut cell_text = String::new();

            for content in &cell.children {
                match content {
                    TableCellContent::Paragraph(p) => {
                        let paragraph_text = Self::extract_paragraph_text(&p.children);
                        if !paragraph_text.trim().is_empty() {
                            if !cell_text.is_empty() {
                                cell_text.push(' ');
                            }
                            cell_text.push_str(&paragraph_text);
                        }
                    }
                    _ => {}
                }
            }

            cells.push(normalize_cell_text(&cell_text));
        }

        cells
    }
}

fn normalize_cell_text(text: &str) -> Option<String> {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn row_is_empty(cells: &[Option<String>]) -> bool {
    cells.iter().all(Option::is_none)
}

fn is_likely_header_row(
    cells: &[Option<String>],
    table_cols: usize,
    next_row: Option<&[Option<String>]>,
) -> bool {
    let non_empty = cells.iter().filter(|cell| cell.is_some()).count();
    let has_multiple_columns = non_empty > 1 || (table_cols <= 1 && non_empty == 1);
    if !has_multiple_columns
        || !cells
            .iter()
            .flatten()
            .all(|cell| is_likely_header_cell(cell))
    {
        return false;
    }

    if cells
        .iter()
        .flatten()
        .any(|cell| has_strong_header_signal(cell))
    {
        return true;
    }

    next_row.is_some_and(|row| {
        row.iter()
            .flatten()
            .any(|cell| !is_likely_header_cell(cell))
    })
}

fn is_likely_header_cell(cell: &str) -> bool {
    let trimmed = cell.trim();
    !trimmed.is_empty()
        && trimmed.chars().any(char::is_alphabetic)
        && trimmed.parse::<f64>().is_err()
        && (!is_short_uppercase_acronym(trimmed) || has_strong_header_signal(trimmed))
}

fn has_strong_header_signal(cell: &str) -> bool {
    let trimmed = cell.trim();
    let lower = trimmed.to_ascii_lowercase();
    if trimmed.contains('_') || trimmed.contains('-') {
        return true;
    }
    if COMMON_HEADER_LABELS.contains(&lower.as_str()) {
        return true;
    }
    lower
        .split_whitespace()
        .all(|part| COMMON_HEADER_LABELS.contains(&part))
}

fn is_short_uppercase_acronym(value: &str) -> bool {
    let letters: Vec<char> = value.chars().filter(|c| c.is_alphabetic()).collect();
    !letters.is_empty()
        && letters.len() <= 8
        && letters.iter().all(|c| c.is_uppercase())
        && !value.chars().any(|c| c.is_lowercase())
}

const COMMON_HEADER_LABELS: &[&str] = &[
    "account",
    "age",
    "amount",
    "author",
    "category",
    "class",
    "code",
    "company",
    "comment",
    "contract",
    "created",
    "customer",
    "date",
    "description",
    "email",
    "file",
    "first",
    "id",
    "key",
    "language",
    "last",
    "name",
    "note",
    "notes",
    "owner",
    "path",
    "price",
    "priority",
    "repo",
    "score",
    "source",
    "status",
    "symbol",
    "tag",
    "tenant",
    "tier",
    "time",
    "title",
    "type",
    "updated",
    "url",
    "user",
    "value",
    "year",
];

fn row_text_from_cells(cells: &[Option<String>]) -> String {
    cells
        .iter()
        .filter_map(|cell| cell.as_deref())
        .collect::<Vec<_>>()
        .join(" ")
}

fn row_text_with_headers(headers: &[Option<String>], cells: &[Option<String>]) -> String {
    cells
        .iter()
        .enumerate()
        .filter_map(|(idx, cell)| {
            let value = cell.as_deref()?;
            let header = headers
                .get(idx)
                .and_then(|header| header.as_deref())
                .unwrap_or_default()
                .trim();
            if header.is_empty() {
                Some(value.to_string())
            } else {
                Some(format!("{} {}", header, value))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

impl Default for DocxParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentParser for DocxParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        // Check file size
        if data.len() > self.max_size {
            return Err(IngestionError::Parse(format!(
                "DOCX file too large: {} bytes (max: {})",
                data.len(),
                self.max_size
            )));
        }

        // Parse the DOCX file
        let docx = read_docx(data)
            .map_err(|e| IngestionError::Parse(format!("Failed to parse DOCX: {}", e)))?;

        let mut text_parts = Vec::new();

        // Extract text from document body
        for child in &docx.document.children {
            match child {
                DocumentChild::Paragraph(p) => {
                    let para_text = Self::extract_paragraph_text(&p.children);
                    if !para_text.trim().is_empty() {
                        text_parts.push(para_text);
                    }
                }
                DocumentChild::Table(table) => {
                    let table_text = Self::extract_table_text(&table.rows);
                    if !table_text.trim().is_empty() {
                        text_parts.push(table_text);
                    }
                }
                _ => {}
            }
        }

        let text = text_parts.join("\n");
        let word_count = text.split_whitespace().count();

        // Try to extract metadata from core properties
        let metadata = DocumentMetadata {
            word_count: Some(word_count),
            ..Default::default()
        };

        Ok(ParsedDocument {
            text,
            metadata,
            format: DocumentFormat::Docx,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Docx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
    use zip::{write::SimpleFileOptions, ZipWriter};

    #[test]
    fn test_is_simple_invalid_zip() {
        let data = b"not a zip file";
        assert!(!DocxParser::is_simple(data));
    }

    #[test]
    fn test_parser_creation() {
        let parser = DocxParser::new();
        assert_eq!(parser.format(), DocumentFormat::Docx);
    }

    #[test]
    fn test_parser_with_max_size() {
        let parser = DocxParser::with_max_size(1024);
        assert_eq!(parser.max_size, 1024);
    }

    #[test]
    fn test_file_too_large() {
        let parser = DocxParser::with_max_size(10);
        let data = vec![0u8; 100];
        let result = parser.parse(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }

    #[test]
    fn test_parse_docx_table_preserves_header_value_pairs_for_retrieval() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_contract_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("customer HGC"), "{}", result.text);
        assert!(result.text.contains("year 2025"), "{}", result.text);
        assert!(
            result.text.contains("contract_amount 1200"),
            "{}",
            result.text
        );
    }

    #[test]
    fn test_parse_docx_table_cell_separates_paragraphs() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_multiline_table_cell();

        let result = parser.parse(&data).unwrap();

        assert!(
            result.text.contains("notes First Second"),
            "{}",
            result.text
        );
        assert!(!result.text.contains("FirstSecond"), "{}", result.text);
    }

    #[test]
    fn test_parse_docx_table_without_header_does_not_promote_first_data_row_to_headers() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_no_header_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("HGC 2025 1200"), "{}", result.text);
        assert!(result.text.contains("DEF 2024 900"), "{}", result.text);
        assert!(!result.text.contains("HGC DEF"), "{}", result.text);
        assert!(!result.text.contains("2025 2024"), "{}", result.text);
    }

    #[test]
    fn test_parse_docx_table_without_header_does_not_promote_all_text_data_row_to_headers() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_all_text_no_header_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("HGC Premium"), "{}", result.text);
        assert!(result.text.contains("DEF Standard"), "{}", result.text);
        assert!(!result.text.contains("HGC DEF"), "{}", result.text);
        assert!(!result.text.contains("Premium Standard"), "{}", result.text);
    }

    #[test]
    fn test_parse_docx_table_preserves_all_text_header_value_pairs() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_all_text_header_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("customer HGC"), "{}", result.text);
        assert!(result.text.contains("tier Premium"), "{}", result.text);
    }

    #[test]
    fn test_parse_docx_table_ignores_empty_wide_rows_for_header_detection() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_empty_wide_row_single_column_table();

        let result = parser.parse(&data).unwrap();

        assert_eq!(result.text, "customer\ncustomer HGC");
    }

    #[test]
    fn test_parse_docx_table_ignores_trailing_empty_cells_for_header_detection() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_trailing_empty_cells_single_column_table();

        let result = parser.parse(&data).unwrap();

        assert_eq!(result.text, "customer\ncustomer HGC");
    }

    #[test]
    fn test_parse_docx_table_preserves_name_header_value_pairs() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_name_header_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("First Name Alice"), "{}", result.text);
        assert!(result.text.contains("Last Name Smith"), "{}", result.text);
    }

    #[test]
    fn test_parse_docx_table_preserves_uppercase_common_header_value_pairs() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_uppercase_common_header_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("ID 123"), "{}", result.text);
        assert!(result.text.contains("Name Alice"), "{}", result.text);
    }

    #[test]
    fn test_parse_docx_table_ignores_title_row_when_selecting_headers() {
        let parser = DocxParser::new();
        let data = minimal_docx_with_title_row_contract_table();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("Contract Export"), "{}", result.text);
        assert!(result.text.contains("customer HGC"), "{}", result.text);
        assert!(result.text.contains("year 2025"), "{}", result.text);
        assert!(
            result.text.contains("contract_amount 1200"),
            "{}",
            result.text
        );
        assert!(
            !result.text.contains("Contract Export HGC"),
            "{}",
            result.text
        );
    }

    fn minimal_docx_with_contract_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>customer</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>year</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>contract_amount</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>2025</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>1200</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_title_row_contract_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>Contract Export</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>customer</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>year</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>contract_amount</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>2025</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>1200</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_no_header_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>2025</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>1200</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>DEF</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>2024</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>900</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_all_text_no_header_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Premium</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>DEF</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Standard</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_all_text_header_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>customer</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>tier</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Premium</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_empty_wide_row_single_column_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>customer</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_trailing_empty_cells_single_column_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>customer</w:t></w:r></w:p></w:tc>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>HGC</w:t></w:r></w:p></w:tc>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
        <w:tc><w:p/></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_name_header_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>First Name</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Last Name</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>Alice</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Smith</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_uppercase_common_header_table() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>ID</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Name</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>123</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Alice</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_multiline_table_cell() -> Vec<u8> {
        minimal_docx_with_document_xml(
            r#"
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>notes</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc>
          <w:p><w:r><w:t>First</w:t></w:r></w:p>
          <w:p><w:r><w:t>Second</w:t></w:r></w:p>
        </w:tc>
      </w:tr>
    </w:tbl>
    <w:sectPr/>
  </w:body>
</w:document>"#,
        )
    }

    fn minimal_docx_with_document_xml(document_xml: &str) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut zip = ZipWriter::new(&mut cursor);
            let options =
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

            add_file(
                &mut zip,
                options,
                "[Content_Types].xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>"#,
            );
            add_file(
                &mut zip,
                options,
                "_rels/.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>"#,
            );
            add_file(
                &mut zip,
                options,
                "word/_rels/document.xml.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
</Relationships>"#,
            );
            add_file(
                &mut zip,
                options,
                "word/document.xml",
                &format!(r#"<?xml version="1.0" encoding="UTF-8"?>{document_xml}"#),
            );
            zip.finish().unwrap();
        }
        cursor.into_inner()
    }

    fn add_file(
        zip: &mut ZipWriter<&mut Cursor<Vec<u8>>>,
        options: SimpleFileOptions,
        path: &str,
        contents: &str,
    ) {
        zip.start_file(path, options).unwrap();
        zip.write_all(contents.as_bytes()).unwrap();
    }
}
