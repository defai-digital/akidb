//! XLSX Document Parser

use calamine::{Reader, Xlsx, Data};
use std::io::Cursor;

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// XLSX document parser using calamine
pub struct XlsxParser;

impl XlsxParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for XlsxParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentParser for XlsxParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        let cursor = Cursor::new(data);
        let mut workbook: Xlsx<_> = Xlsx::new(cursor)
            .map_err(|e| crate::IngestionError::Parse(format!("XLSX error: {}", e)))?;

        let mut texts = Vec::new();
        let mut total_rows = 0;
        let mut total_cols = 0;
        let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
        let sheet_count = sheet_names.len();

        for sheet_name in sheet_names {
            if let Ok(range) = workbook.worksheet_range(&sheet_name) {
                let (rows, cols) = range.get_size();
                total_rows += rows;
                total_cols = total_cols.max(cols);

                for row in range.rows() {
                    let row_text: Vec<String> = row
                        .iter()
                        .filter_map(|cell| cell_to_string(cell))
                        .collect();
                    if !row_text.is_empty() {
                        texts.push(row_text.join(" "));
                    }
                }
            }
        }

        let text = texts.join("\n");
        let word_count = text.split_whitespace().count();

        Ok(ParsedDocument {
            text,
            metadata: DocumentMetadata {
                word_count: Some(word_count),
                pages: Some(sheet_count),
                extra: Some(serde_json::json!({
                    "sheets": sheet_count,
                    "rows": total_rows,
                    "columns": total_cols,
                })),
                ..Default::default()
            },
            format: DocumentFormat::Xlsx,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Xlsx
    }
}

/// Convert cell data to string
fn cell_to_string(cell: &Data) -> Option<String> {
    match cell {
        Data::Empty => None,
        Data::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Data::Float(f) => Some(f.to_string()),
        Data::Int(i) => Some(i.to_string()),
        Data::Bool(b) => Some(b.to_string()),
        Data::Error(e) => Some(format!("#ERROR:{:?}", e)),
        Data::DateTime(dt) => Some(dt.to_string()),
        Data::DateTimeIso(s) => Some(s.clone()),
        Data::DurationIso(s) => Some(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Testing XLSX requires actual XLSX file bytes
    // These are placeholder tests

    #[test]
    fn test_xlsx_parser_creation() {
        let parser = XlsxParser::new();
        assert_eq!(parser.format(), DocumentFormat::Xlsx);
    }
}
