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
            .from_reader(data);

        let mut texts = Vec::new();
        let mut row_count = 0;
        let mut col_count;

        // Get headers
        let headers: Vec<String>;
        match reader.headers() {
            Ok(raw_headers) => {
                headers = raw_headers
                    .iter()
                    .map(|header| header.to_string())
                    .collect();
                col_count = headers.len();
                texts.push(headers.join(" "));
            }
            Err(_) if data.is_empty() => {
                return Ok(ParsedDocument {
                    text: String::new(),
                    metadata: DocumentMetadata {
                        word_count: Some(0),
                        extra: Some(serde_json::json!({
                            "rows": 0,
                            "columns": 0,
                        })),
                        ..Default::default()
                    },
                    format: self.format,
                });
            }
            Err(e) => {
                return Err(crate::IngestionError::Parse(format!(
                    "CSV header parse error: {}",
                    e
                )));
            }
        }

        // Get rows
        for result in reader.records() {
            let record = result.map_err(|e| {
                crate::IngestionError::Parse(format!("CSV record parse error: {}", e))
            })?;
            col_count = col_count.max(record.len());
            let row_text = record
                .iter()
                .enumerate()
                .filter_map(|(idx, value)| {
                    let value = value.trim();
                    if value.is_empty() {
                        return None;
                    }
                    let header = headers.get(idx).map(|s| s.trim()).unwrap_or_default();
                    if header.is_empty() {
                        Some(value.to_string())
                    } else {
                        Some(format!("{} {}", header, value))
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");
            if !row_text.is_empty() {
                texts.push(row_text);
            }
            row_count += 1;
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
