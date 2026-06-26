//! DOCX Parser
//!
//! Rust-native parser for simple DOCX files using docx-rs.
//! Complex DOCX files (macros, ActiveX, OLE objects) are routed to Python.

use std::io::{Cursor, Read};

use docx_rs::{read_docx, DocumentChild, ParagraphChild, RunChild, TableChild, TableCellContent, TableRowChild};
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
        let mut text = String::new();

        for row_child in rows {
            if let TableChild::TableRow(row) = row_child {
                let mut row_texts = Vec::new();

                for cell_child in &row.cells {
                    if let TableRowChild::TableCell(cell) = cell_child {
                        let mut cell_text = String::new();

                        for content in &cell.children {
                            match content {
                                TableCellContent::Paragraph(p) => {
                                    cell_text.push_str(&Self::extract_paragraph_text(&p.children));
                                }
                                _ => {}
                            }
                        }

                        row_texts.push(cell_text.trim().to_string());
                    }
                }

                if !row_texts.is_empty() {
                    text.push_str(&row_texts.join("\t"));
                    text.push('\n');
                }
            }
        }

        text
    }
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
        let docx = read_docx(data).map_err(|e| {
            IngestionError::Parse(format!("Failed to parse DOCX: {}", e))
        })?;

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
}
