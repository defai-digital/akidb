//! Spreadsheet Document Parser

use quick_xml::events::{BytesStart, BytesText, Event};
use quick_xml::reader::Reader;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};
use std::sync::LazyLock;
use zip::ZipArchive;

use crate::parsers::{DocumentFormat, DocumentMetadata, DocumentParser, ParsedDocument};
use crate::Result;

/// Spreadsheet parser for simple XLSX workbooks.
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
        let workbook = parse_workbook(data)?;
        let mut texts = Vec::new();
        let mut total_rows = 0;
        let mut total_cols = 0;
        let sheet_count = workbook.sheets.len();

        for sheet in workbook.sheets {
            total_rows += sheet.rows.len();

            let sheet_rows: Vec<Vec<Option<String>>> = sheet
                .rows
                .into_iter()
                .filter(|cells| !row_is_empty(cells))
                .collect();
            let sheet_cols = sheet_rows
                .iter()
                .map(|cells| cells.iter().filter(|cell| cell.is_some()).count())
                .max()
                .unwrap_or(0);
            total_cols = total_cols.max(sheet_cols);

            let mut headers: Option<Vec<Option<String>>> = None;
            for (idx, cells) in sheet_rows.iter().enumerate() {
                let next_row = sheet_rows.get(idx + 1).map(Vec::as_slice);
                let text = match headers.as_deref() {
                    Some(headers) => row_text_with_headers(headers, cells),
                    None => {
                        let row_text = row_text_from_cells(cells);
                        if is_likely_header_row(cells, sheet_cols, next_row) {
                            headers = Some(cells.to_vec());
                        }
                        row_text
                    }
                };
                if !text.is_empty() {
                    texts.push(text);
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

fn is_likely_header_row(
    cells: &[Option<String>],
    sheet_cols: usize,
    next_row: Option<&[Option<String>]>,
) -> bool {
    let non_empty = cells.iter().filter(|cell| cell.is_some()).count();
    let has_multiple_columns = non_empty > 1 || (sheet_cols <= 1 && non_empty == 1);
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

static HEADER_LABEL_SET: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| COMMON_HEADER_LABELS.iter().copied().collect());

fn has_strong_header_signal(cell: &str) -> bool {
    let trimmed = cell.trim();
    if trimmed.contains('_') || trimmed.contains('-') {
        return true;
    }
    // Use stack buffer for lowercase conversion to avoid allocation on short strings
    let mut buf = [0u8; 128];
    let lower: std::borrow::Cow<'_, str> = if trimmed.len() <= buf.len() {
        let bytes = trimmed.as_bytes();
        buf[..bytes.len()].copy_from_slice(bytes);
        buf[..bytes.len()].make_ascii_lowercase();
        // SAFETY: we only changed ASCII case, preserving UTF-8 validity
        std::borrow::Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(&buf[..bytes.len()]) })
    } else {
        std::borrow::Cow::Owned(trimmed.to_ascii_lowercase())
    };
    if HEADER_LABEL_SET.contains(lower.as_ref()) {
        return true;
    }
    lower
        .split_whitespace()
        .all(|part| HEADER_LABEL_SET.contains(part))
}

fn is_short_uppercase_acronym(value: &str) -> bool {
    let mut letter_count = 0usize;
    let mut has_lowercase = false;
    for c in value.chars() {
        if c.is_alphabetic() {
            letter_count += 1;
            if c.is_lowercase() {
                has_lowercase = true;
            }
        }
    }
    letter_count > 0 && letter_count <= 8 && !has_lowercase
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
    let mut result = String::new();
    let mut first = true;
    for cell in cells {
        if let Some(val) = cell.as_deref() {
            if !first {
                result.push(' ');
            }
            result.push_str(val);
            first = false;
        }
    }
    result
}

fn row_text_with_headers(headers: &[Option<String>], cells: &[Option<String>]) -> String {
    let mut result = String::new();
    let mut first = true;
    for (idx, cell) in cells.iter().enumerate() {
        let Some(value) = cell.as_deref() else {
            continue;
        };
        if !first {
            result.push(' ');
        }
        let header = headers
            .get(idx)
            .and_then(|h| h.as_deref())
            .unwrap_or_default()
            .trim();
        if header.is_empty() {
            result.push_str(value);
        } else {
            result.push_str(header);
            result.push(' ');
            result.push_str(value);
        }
        first = false;
    }
    result
}

struct WorkbookData {
    sheets: Vec<SheetData>,
}

struct SheetData {
    rows: Vec<Vec<Option<String>>>,
}

fn parse_workbook(data: &[u8]) -> Result<WorkbookData> {
    let mut archive = ZipArchive::new(Cursor::new(data))
        .map_err(|e| crate::IngestionError::Parse(format!("Spreadsheet error: {}", e)))?;
    let shared_strings = read_shared_strings(&mut archive)?;
    let relationships = read_workbook_relationships(&mut archive)?;
    let sheet_paths = read_sheet_paths(&mut archive, &relationships)?;

    let mut sheets = Vec::new();
    for path in sheet_paths {
        if let Ok(mut file) = archive.by_name(&path) {
            let mut xml = String::new();
            file.read_to_string(&mut xml).map_err(|e| {
                crate::IngestionError::Parse(format!("Failed to read sheet {}: {}", path, e))
            })?;
            drop(file);
            sheets.push(SheetData {
                rows: parse_sheet_xml(&xml, &shared_strings)?,
            });
        }
    }

    Ok(WorkbookData { sheets })
}

fn read_shared_strings(archive: &mut ZipArchive<Cursor<&[u8]>>) -> Result<Vec<String>> {
    let Ok(mut file) = archive.by_name("xl/sharedStrings.xml") else {
        return Ok(Vec::new());
    };
    let mut xml = String::new();
    file.read_to_string(&mut xml).map_err(|e| {
        crate::IngestionError::Parse(format!("Failed to read shared strings: {}", e))
    })?;

    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(false);
    let mut strings = Vec::new();
    let mut current = String::new();
    let mut in_si = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if local_name(e.name().as_ref()) == b"si" => {
                in_si = true;
                current.clear();
            }
            Ok(Event::Text(e)) if in_si => current.push_str(&decode_text(&e)),
            Ok(Event::CData(e)) if in_si => {
                current.push_str(&String::from_utf8_lossy(e.as_ref()));
            }
            Ok(Event::End(e)) if local_name(e.name().as_ref()) == b"si" => {
                strings.push(std::mem::take(&mut current));
                in_si = false;
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(crate::IngestionError::Parse(format!(
                    "Shared strings parse error: {}",
                    e
                )));
            }
            _ => {}
        }
    }

    Ok(strings)
}

fn read_workbook_relationships(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
) -> Result<HashMap<String, String>> {
    let Ok(mut file) = archive.by_name("xl/_rels/workbook.xml.rels") else {
        return Ok(HashMap::new());
    };
    let mut xml = String::new();
    file.read_to_string(&mut xml).map_err(|e| {
        crate::IngestionError::Parse(format!("Failed to read workbook relationships: {}", e))
    })?;

    let mut reader = Reader::from_str(&xml);
    let mut relationships = HashMap::new();
    loop {
        match reader.read_event() {
            Ok(Event::Empty(e)) | Ok(Event::Start(e))
                if local_name(e.name().as_ref()) == b"Relationship" =>
            {
                if let (Some(id), Some(target)) = (attr_value(&e, b"Id"), attr_value(&e, b"Target"))
                {
                    relationships.insert(id, normalize_sheet_target(&target));
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(crate::IngestionError::Parse(format!(
                    "Workbook relationships parse error: {}",
                    e
                )));
            }
            _ => {}
        }
    }

    Ok(relationships)
}

fn read_sheet_paths(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
    relationships: &HashMap<String, String>,
) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    if let Ok(mut file) = archive.by_name("xl/workbook.xml") {
        let mut xml = String::new();
        file.read_to_string(&mut xml)
            .map_err(|e| crate::IngestionError::Parse(format!("Failed to read workbook: {}", e)))?;
        let mut reader = Reader::from_str(&xml);
        loop {
            match reader.read_event() {
                Ok(Event::Empty(e)) | Ok(Event::Start(e))
                    if local_name(e.name().as_ref()) == b"sheet" =>
                {
                    if let Some(rid) = attr_value(&e, b"id") {
                        if let Some(path) = relationships.get(&rid) {
                            paths.push(path.clone());
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(crate::IngestionError::Parse(format!(
                        "Workbook parse error: {}",
                        e
                    )));
                }
                _ => {}
            }
        }
    }

    if paths.is_empty() {
        for i in 0..archive.len() {
            if let Ok(file) = archive.by_index_raw(i) {
                let name = file.name();
                if name.starts_with("xl/worksheets/") && name.ends_with(".xml") {
                    paths.push(name.to_string());
                }
            }
        }
        paths.sort();
    }

    Ok(paths)
}

fn parse_sheet_xml(xml: &str, shared_strings: &[String]) -> Result<Vec<Vec<Option<String>>>> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);

    let mut rows = Vec::new();
    let mut current_row: Option<Vec<Option<String>>> = None;
    let mut current_cell_type: Option<String> = None;
    let mut current_cell_value = String::new();
    let mut collecting_value = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => match local_name(e.name().as_ref()) {
                b"row" => current_row = Some(Vec::new()),
                b"c" => {
                    current_cell_type = attr_value(&e, b"t");
                    current_cell_value.clear();
                }
                b"v" | b"t" => collecting_value = true,
                _ => {}
            },
            Ok(Event::Empty(e)) if local_name(e.name().as_ref()) == b"c" => {
                if let Some(row) = current_row.as_mut() {
                    row.push(None);
                }
            }
            Ok(Event::Text(e)) if collecting_value => current_cell_value.push_str(&decode_text(&e)),
            Ok(Event::CData(e)) if collecting_value => {
                current_cell_value.push_str(&String::from_utf8_lossy(e.as_ref()));
            }
            Ok(Event::End(e)) => match local_name(e.name().as_ref()) {
                b"v" | b"t" => collecting_value = false,
                b"c" => {
                    if let Some(row) = current_row.as_mut() {
                        row.push(resolve_cell_value(
                            current_cell_type.as_deref(),
                            &current_cell_value,
                            shared_strings,
                        ));
                    }
                    current_cell_type = None;
                    current_cell_value.clear();
                    collecting_value = false;
                }
                b"row" => {
                    if let Some(row) = current_row.take() {
                        rows.push(row);
                    }
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(crate::IngestionError::Parse(format!(
                    "Worksheet parse error: {}",
                    e
                )));
            }
            _ => {}
        }
    }

    Ok(rows)
}

fn resolve_cell_value(
    cell_type: Option<&str>,
    raw_value: &str,
    shared_strings: &[String],
) -> Option<String> {
    let value = match cell_type {
        Some("s") => raw_value
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|idx| shared_strings.get(idx).cloned())
            .unwrap_or_default(),
        Some("b") => match raw_value.trim() {
            "1" => "true".to_string(),
            "0" => "false".to_string(),
            other => other.to_string(),
        },
        _ => raw_value.to_string(),
    };
    normalize_cell_text(&value)
}

fn normalize_cell_text(text: &str) -> Option<String> {
    let mut result = String::with_capacity(text.len());
    let mut first_word = true;
    for word in text.split_whitespace() {
        if !first_word {
            result.push(' ');
        }
        result.push_str(word);
        first_word = false;
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

fn attr_value(start: &BytesStart<'_>, key: &[u8]) -> Option<String> {
    start
        .attributes()
        .filter_map(|attr| attr.ok())
        .find(|attr| local_name(attr.key.as_ref()) == key)
        .map(|attr| String::from_utf8_lossy(attr.value.as_ref()).into_owned())
}

fn normalize_sheet_target(target: &str) -> String {
    let target = target.trim_start_matches('/');
    if target.starts_with("xl/") {
        target.to_string()
    } else {
        format!("xl/{}", target)
    }
}

fn decode_text(text: &BytesText<'_>) -> String {
    text.decode()
        .map(|text| text.into_owned())
        .unwrap_or_else(|_| String::from_utf8_lossy(text.as_ref()).into_owned())
}

fn local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
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
        let data = minimal_xlsx_with_sheet_data(contract_sheet_rows());

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("customer HGC"));
        assert!(result.text.contains("year 2025"));
        assert!(result.text.contains("contract_amount 1200"));
    }

    #[test]
    fn test_parse_xlsx_ignores_title_row_when_selecting_headers() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>Contract Export</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>customer</t></is></c>
      <c r="B2" t="inlineStr"><is><t>year</t></is></c>
      <c r="C2" t="inlineStr"><is><t>contract_amount</t></is></c>
    </row>
    <row r="3">
      <c r="A3" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B3"><v>2025</v></c>
      <c r="C3"><v>1200</v></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("Contract Export"));
        assert!(result.text.contains("customer HGC"));
        assert!(result.text.contains("year 2025"));
        assert!(result.text.contains("contract_amount 1200"));
        assert!(!result.text.contains("Contract Export HGC"));
    }

    #[test]
    fn test_parse_xlsx_single_column_still_uses_first_row_as_header() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>customer</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>HGC</t></is></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("customer HGC"));
    }

    #[test]
    fn test_parse_xlsx_ignores_empty_wide_rows_for_header_detection() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1"/>
      <c r="B1"/>
      <c r="C1"/>
      <c r="D1"/>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>customer</t></is></c>
    </row>
    <row r="3">
      <c r="A3" t="inlineStr"><is><t>HGC</t></is></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert_eq!(result.text, "customer\ncustomer HGC");
        assert_eq!(result.metadata.extra.unwrap()["columns"], 1);
    }

    #[test]
    fn test_parse_xlsx_ignores_trailing_empty_cells_for_header_detection() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>customer</t></is></c>
      <c r="B1"/>
      <c r="C1"/>
      <c r="D1"/>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B2"/>
      <c r="C2"/>
      <c r="D2"/>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert_eq!(result.text, "customer\ncustomer HGC");
        assert_eq!(result.metadata.extra.unwrap()["columns"], 1);
    }

    #[test]
    fn test_parse_xlsx_without_header_does_not_promote_first_data_row_to_headers() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B1"><v>2025</v></c>
      <c r="C1"><v>1200</v></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>DEF</t></is></c>
      <c r="B2"><v>2024</v></c>
      <c r="C2"><v>900</v></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("HGC 2025 1200"), "{}", result.text);
        assert!(result.text.contains("DEF 2024 900"), "{}", result.text);
        assert!(!result.text.contains("HGC DEF"), "{}", result.text);
        assert!(!result.text.contains("2025 2024"), "{}", result.text);
    }

    #[test]
    fn test_parse_xlsx_without_header_does_not_promote_all_text_data_row_to_headers() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B1" t="inlineStr"><is><t>Premium</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>DEF</t></is></c>
      <c r="B2" t="inlineStr"><is><t>Standard</t></is></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("HGC Premium"), "{}", result.text);
        assert!(result.text.contains("DEF Standard"), "{}", result.text);
        assert!(!result.text.contains("HGC DEF"), "{}", result.text);
        assert!(!result.text.contains("Premium Standard"), "{}", result.text);
    }

    #[test]
    fn test_parse_xlsx_preserves_all_text_header_value_pairs() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>customer</t></is></c>
      <c r="B1" t="inlineStr"><is><t>tier</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B2" t="inlineStr"><is><t>Premium</t></is></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("customer HGC"), "{}", result.text);
        assert!(result.text.contains("tier Premium"), "{}", result.text);
    }

    #[test]
    fn test_parse_xlsx_preserves_name_header_value_pairs() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>First Name</t></is></c>
      <c r="B1" t="inlineStr"><is><t>Last Name</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>Alice</t></is></c>
      <c r="B2" t="inlineStr"><is><t>Smith</t></is></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("First Name Alice"), "{}", result.text);
        assert!(result.text.contains("Last Name Smith"), "{}", result.text);
    }

    #[test]
    fn test_parse_xlsx_preserves_uppercase_common_header_value_pairs() {
        let parser = XlsxParser::new();
        let data = minimal_xlsx_with_sheet_data(
            r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>ID</t></is></c>
      <c r="B1" t="inlineStr"><is><t>Name</t></is></c>
    </row>
    <row r="2">
      <c r="A2"><v>123</v></c>
      <c r="B2" t="inlineStr"><is><t>Alice</t></is></c>
    </row>"#,
        );

        let result = parser.parse(&data).unwrap();

        assert!(result.text.contains("ID 123"), "{}", result.text);
        assert!(result.text.contains("Name Alice"), "{}", result.text);
    }

    fn contract_sheet_rows() -> &'static str {
        r#"<row r="1">
      <c r="A1" t="inlineStr"><is><t>customer</t></is></c>
      <c r="B1" t="inlineStr"><is><t>year</t></is></c>
      <c r="C1" t="inlineStr"><is><t>contract_amount</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>HGC</t></is></c>
      <c r="B2"><v>2025</v></c>
      <c r="C2"><v>1200</v></c>
    </row>"#
    }

    fn minimal_xlsx_with_sheet_data(sheet_data: &str) -> Vec<u8> {
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
                &format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    {sheet_data}
  </sheetData>
</worksheet>"#
                ),
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
