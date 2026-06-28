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

        for row_child in rows {
            let TableChild::TableRow(row) = row_child;
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

            if row_is_empty(&cells) {
                continue;
            }

            let line = match headers.as_deref() {
                Some(headers) => row_text_with_headers(headers, &cells),
                None => {
                    headers = Some(cells.clone());
                    row_text_from_cells(&cells)
                }
            };
            if !line.is_empty() {
                lines.push(line);
            }
        }

        lines.join("\n")
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
