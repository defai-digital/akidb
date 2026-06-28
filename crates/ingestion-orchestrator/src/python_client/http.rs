//! HTTP Client for Python Parser Service

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

use crate::parsers::{DocumentFormat, DocumentMetadata, ParsedDocument};
use crate::Result;

/// Request to the Python parser service (matches Python's ParseRequest model)
#[derive(Debug, Serialize)]
pub struct ParseRequest {
    /// Document content as base64
    pub content_base64: String,

    /// File name for format detection
    pub filename: String,

    /// Document format hint (optional)
    pub format: Option<String>,
}

/// Table data from the Python parser
#[derive(Debug, Deserialize, Serialize)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub page: Option<i32>,
}

/// Image reference from the Python parser
#[derive(Debug, Deserialize, Serialize)]
pub struct ImageRef {
    pub index: i32,
    pub page: Option<i32>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub alt_text: Option<String>,
}

/// Response from the Python parser service (matches Python's ParsedDocument model)
#[derive(Debug, Deserialize)]
pub struct ParseResponse {
    /// Extracted text
    pub text: String,

    /// Document format
    pub format: String,

    /// Page count
    pub page_count: i32,

    /// Document metadata
    pub metadata: HashMap<String, serde_json::Value>,

    /// Extracted tables
    pub tables: Vec<TableData>,

    /// Image references
    pub images: Vec<ImageRef>,

    /// Parse time in milliseconds
    pub parse_time_ms: f64,
}

/// Error response from the Python parser
#[derive(Debug, Deserialize)]
pub struct ParseErrorResponse {
    pub error: String,
    pub error_type: String,
    pub details: Option<HashMap<String, serde_json::Value>>,
}

/// Client for the Python document parser service
pub struct PythonParserClient {
    client: Client,
    base_url: String,
}

impl PythonParserClient {
    /// Create a new Python parser client
    pub fn new(base_url: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Parse a document using the Python service
    pub async fn parse(&self, data: &[u8], filename: &str) -> Result<ParsedDocument> {
        let request = ParseRequest {
            content_base64: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                data,
            ),
            filename: filename.to_string(),
            format: None,
        };

        debug!(filename, "Sending parse request to Python service");

        let response = self
            .client
            .post(format!("{}/parse", self.base_url))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            // Try to parse as error response first
            let body = response.text().await.unwrap_or_default();
            if let Ok(error_resp) = serde_json::from_str::<ParseErrorResponse>(&body) {
                return Err(crate::IngestionError::PythonParser(format!(
                    "{}: {}",
                    error_resp.error_type, error_resp.error
                )));
            }
            return Err(crate::IngestionError::PythonParser(format!(
                "HTTP {}: {}",
                status, body
            )));
        }

        let parse_response: ParseResponse = response.json().await?;

        Ok(parsed_document_from_response(parse_response, filename))
    }

    /// Check if the Python parser service is healthy
    pub async fn health_check(&self) -> bool {
        match self
            .client
            .get(format!("{}/health", self.base_url))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(e) => {
                warn!(?e, "Python parser health check failed");
                false
            }
        }
    }
}

fn parsed_document_from_response(parse_response: ParseResponse, filename: &str) -> ParsedDocument {
    let format = document_format_from_response(&parse_response.format, filename);

    // Extract metadata fields if present
    let title = parse_response
        .metadata
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let author = parse_response
        .metadata
        .get("author")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let word_count = parse_response
        .metadata
        .get("word_count")
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok());
    let pages = usize::try_from(parse_response.page_count).ok();
    let extra = Some(serde_json::json!({
        "parser_format": parse_response.format,
        "metadata": parse_response.metadata,
        "tables": parse_response.tables,
        "images": parse_response.images,
        "parse_time_ms": parse_response.parse_time_ms,
    }));

    ParsedDocument {
        text: parse_response.text,
        metadata: DocumentMetadata {
            title,
            author,
            pages,
            word_count,
            extra,
            ..Default::default()
        },
        format,
    }
}

fn document_format_from_response(response_format: &str, filename: &str) -> DocumentFormat {
    let response_format = DocumentFormat::from_extension(response_format.trim());
    if response_format != DocumentFormat::Unknown {
        return response_format;
    }

    let ext = filename.rsplit('.').next().unwrap_or("");
    DocumentFormat::from_extension(ext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = PythonParserClient::new("http://localhost:8080");
        assert_eq!(client.base_url, "http://localhost:8080");
    }

    #[test]
    fn test_client_url_normalization() {
        let client = PythonParserClient::new("http://localhost:8080/");
        assert_eq!(client.base_url, "http://localhost:8080");
    }

    #[test]
    fn test_parse_response_negative_page_count_is_ignored() {
        let response = ParseResponse {
            text: "body".to_string(),
            format: "pdf".to_string(),
            page_count: -1,
            metadata: HashMap::new(),
            tables: vec![],
            images: vec![],
            parse_time_ms: 1.0,
        };

        let parsed = parsed_document_from_response(response, "doc.pdf");

        assert_eq!(parsed.metadata.pages, None);
    }

    #[test]
    fn test_parse_response_format_overrides_filename_extension() {
        let response = ParseResponse {
            text: "body".to_string(),
            format: "pdf".to_string(),
            page_count: 3,
            metadata: HashMap::new(),
            tables: vec![],
            images: vec![],
            parse_time_ms: 1.0,
        };

        let parsed = parsed_document_from_response(response, "upload.bin");

        assert_eq!(parsed.format, DocumentFormat::Pdf);
        assert_eq!(parsed.metadata.pages, Some(3));
    }

    #[test]
    fn test_parse_response_unknown_format_falls_back_to_filename_extension() {
        let response = ParseResponse {
            text: "body".to_string(),
            format: "application/octet-stream".to_string(),
            page_count: 1,
            metadata: HashMap::new(),
            tables: vec![],
            images: vec![],
            parse_time_ms: 1.0,
        };

        let parsed = parsed_document_from_response(response, "report.docx");

        assert_eq!(parsed.format, DocumentFormat::Docx);
    }

    #[test]
    fn test_parse_response_preserves_parser_extra_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("title".to_string(), serde_json::json!("Annual Report"));
        metadata.insert("word_count".to_string(), serde_json::json!(42));
        metadata.insert("producer".to_string(), serde_json::json!("Acrobat"));

        let response = ParseResponse {
            text: "body".to_string(),
            format: "pdf".to_string(),
            page_count: 2,
            metadata,
            tables: vec![TableData {
                headers: vec!["customer".to_string(), "amount".to_string()],
                rows: vec![vec!["HGC".to_string(), "1200".to_string()]],
                page: Some(1),
            }],
            images: vec![ImageRef {
                index: 0,
                page: Some(2),
                width: Some(640),
                height: Some(480),
                alt_text: Some("architecture diagram".to_string()),
            }],
            parse_time_ms: 12.5,
        };

        let parsed = parsed_document_from_response(response, "upload.bin");
        let extra = parsed.metadata.extra.as_ref().unwrap();

        assert_eq!(parsed.metadata.title.as_deref(), Some("Annual Report"));
        assert_eq!(parsed.metadata.word_count, Some(42));
        assert_eq!(extra["parser_format"], "pdf");
        assert_eq!(extra["metadata"]["producer"], "Acrobat");
        assert_eq!(extra["tables"][0]["headers"][0], "customer");
        assert_eq!(extra["images"][0]["alt_text"], "architecture diagram");
        assert_eq!(extra["parse_time_ms"], 12.5);
    }
}
