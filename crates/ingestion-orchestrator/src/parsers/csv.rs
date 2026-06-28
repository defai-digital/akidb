//! CSV Document Parser

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// CSV document parser
pub struct CsvParser {
    delimiter: u8,
}

impl CsvParser {
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }

    pub fn with_delimiter(delimiter: u8) -> Self {
        Self { delimiter }
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
        let col_count;

        // Get headers
        match reader.headers() {
            Ok(headers) => {
                col_count = headers.len();
                texts.push(headers.iter().collect::<Vec<_>>().join(" "));
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
                    format: DocumentFormat::Csv,
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
            texts.push(record.iter().collect::<Vec<_>>().join(" "));
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
            format: DocumentFormat::Csv,
        })
    }

    fn format(&self) -> DocumentFormat {
        DocumentFormat::Csv
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
        let parser = CsvParser::with_delimiter(b'\t');
        let data = b"name\tage\nAlice\t30";
        let result = parser.parse(data).unwrap();
        assert!(result.text.contains("Alice"));
    }

    #[test]
    fn test_malformed_csv_returns_error() {
        let parser = CsvParser::new();
        let data = b"name,comment\nAlice,\xFF";

        let result = parser.parse(data);

        assert!(result.is_err());
    }
}
