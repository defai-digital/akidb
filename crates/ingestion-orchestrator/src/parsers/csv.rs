//! CSV Document Parser

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// CSV document parser
pub struct CsvParser {
    delimiter: u8,
    format: DocumentFormat,
}

impl CsvParser {
    pub fn new() -> Self {
        Self {
            delimiter: b',',
            format: DocumentFormat::Csv,
        }
    }

    pub fn with_delimiter(delimiter: u8) -> Self {
        Self {
            delimiter,
            format: DocumentFormat::Csv,
        }
    }

    pub fn tsv() -> Self {
        Self {
            delimiter: b'\t',
            format: DocumentFormat::Tsv,
        }
    }
}

impl Default for CsvParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentParser for CsvParser {
    fn parse(&self, data: &[u8]) -> Result<ParsedDocument> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .flexible(true)
            .has_headers(false)
            .from_reader(data);

        let mut texts = Vec::new();
        let mut row_count = 0;
        let mut col_count = 0;
        let mut rows = Vec::new();

        for result in reader.records() {
            let record = result.map_err(|e| {
                crate::IngestionError::Parse(format!("CSV record parse error: {}", e))
            })?;
            col_count = col_count.max(record.len());
            rows.push(record.iter().map(cell_to_string).collect::<Vec<_>>());
        }

        let mut headers: Option<Vec<Option<String>>> = None;
        for cells in rows {
            if row_is_empty(&cells) {
                continue;
            }

            let row_text = match headers.as_deref() {
                Some(headers) => {
                    row_count += 1;
                    row_text_with_headers(headers, &cells)
                }
                None => {
                    let row_text = row_text_from_cells(&cells);
                    if is_likely_header_row(&cells, col_count) {
                        headers = Some(cells);
                    }
                    row_text
                }
            };
            if !row_text.is_empty() {
                texts.push(row_text);
            }
        }

        let text = texts.join("\n");
        let word_count = text.split_whitespace().count();

        Ok(ParsedDocument {
            text,
            metadata: DocumentMetadata {
                word_count: Some(word_count),
                extra: Some(serde_json::json!({
                    "rows": row_count,
                    "columns": col_count,
                })),
                ..Default::default()
            },
            format: self.format,
        })
    }

    fn format(&self) -> DocumentFormat {
        self.format
    }
}

fn cell_to_string(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn row_is_empty(cells: &[Option<String>]) -> bool {
    cells.iter().all(Option::is_none)
}

fn is_likely_header_row(cells: &[Option<String>], file_cols: usize) -> bool {
    let non_empty = cells.iter().filter(|cell| cell.is_some()).count();
    non_empty > 1 || file_cols <= 1
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv() {
        let parser = CsvParser::new();
        let data = b"name,age\nAlice,30\nBob,25";
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Alice"));
        assert!(result.text.contains("Bob"));
    }

    #[test]
    fn test_parse_tsv() {
        let parser = CsvParser::tsv();
        let data = b"name\tage\nAlice\t30";
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Alice"));
        assert_eq!(result.format, DocumentFormat::Tsv);
    }

    #[test]
    fn test_parse_csv_preserves_header_value_pairs_for_retrieval() {
        let parser = CsvParser::new();
        let data = b"customer,year,contract_amount\nHGC,2025,1200";
        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("customer HGC"));
        assert!(result.text.contains("year 2025"));
        assert!(result.text.contains("contract_amount 1200"));
    }

    #[test]
    fn test_parse_csv_ignores_title_row_when_selecting_headers() {
        let parser = CsvParser::new();
        let data = b"Contract Export\ncustomer,year,contract_amount\nHGC,2025,1200";

        let result = parser.parse(data).unwrap();

        assert!(result.text.contains("Contract Export"));
        assert!(result.text.contains("customer HGC"));
        assert!(result.text.contains("year 2025"));
        assert!(result.text.contains("contract_amount 1200"));
        assert!(!result.text.contains("Contract Export HGC"));
    }

    #[test]
    fn test_parse_csv_tracks_widest_flexible_row() {
        let parser = CsvParser::new();
        let data = b"name,age\nAlice,30,extra";

        let result = parser.parse(data).unwrap();
        let extra = result.metadata.extra.unwrap();

        assert_eq!(extra["columns"], 3);
        assert!(result.text.contains("extra"));
    }

    #[test]
    fn test_malformed_csv_returns_error() {
        let parser = CsvParser::new();
        let data = b"name,comment\nAlice,\xFF";

        let result = parser.parse(data);

        assert!(result.is_err());
    }
}
