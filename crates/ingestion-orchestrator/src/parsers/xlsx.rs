//! XLSX Document Parser

use calamine::{Data, Reader, Xlsx};
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

                let mut headers: Option<Vec<Option<String>>> = None;
                for row in range.rows() {
                    let cells: Vec<Option<String>> = row.iter().map(cell_to_string).collect();
                    if row_is_empty(&cells) {
                        continue;
                    }

                    let text = match headers.as_deref() {
                        Some(headers) => row_text_with_headers(headers, &cells),
                        None => {
                            headers = Some(cells.clone());
                            row_text_from_cells(&cells)
                        }
                    };
                    if !text.is_empty() {
                        texts.push(text);
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
    use std::io::{Cursor, Write};
    use zip::{write::SimpleFileOptions, ZipWriter};

    #[test]
    fn test_xlsx_parser_creation() {
        let parser = XlsxParser::new();
        assert_eq!(parser.format(), DocumentFormat::Xlsx);
    }

    #[test]
    fn test_parse_xlsx_preserves_header_value_pairs_for_retrieval() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx();

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("customer HGC"));
        assert!(result.text.contains("year 2025"));
        assert!(result.text.contains("contract_amount 1200"));
    }

    fn minimal_xlsx() -> Vec<u8> {
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
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"#,
            );
            add_file(
                &mut zip,
                options,
                "_rels/.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#,
            );
            add_file(
                &mut zip,
                options,
                "xl/workbook.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Contracts" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"#,
            );
            add_file(
                &mut zip,
                options,
                "xl/_rels/workbook.xml.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"#,
            );
            add_file(
                &mut zip,
                options,
                "xl/worksheets/sheet1.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="inlineStr"><is><t>customer</t></is></c>
      <c r="B1" t="inlineStr"><is><t>year</t></is></c>
      <c r="C1" t="inlineStr"><is><t>contract_amount</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B2"><v>2025</v></c>
      <c r="C2"><v>1200</v></c>
    </row>
  </sheetData>
</worksheet>"#,
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
