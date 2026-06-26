//! HTTP Client for Python Parser Service

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

use crate::parsers::{DocumentMetadata, ParsedDocument, DocumentFormat};
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
#[derive(Debug, Deserialize)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub page: Option<i32>,
}

/// Image reference from the Python parser
#[derive(Debug, Deserialize)]
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

        let response = self.client
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

        // Determine format from response or fallback to extension
        let ext = filename.rsplit('.').next().unwrap_or("");
        let format = DocumentFormat::from_extension(ext);

        // Extract metadata fields if present
        let title = parse_response.metadata.get("title")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let author = parse_response.metadata.get("author")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let word_count = parse_response.metadata.get("word_count")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

        Ok(ParsedDocument {
            text: parse_response.text,
            metadata: DocumentMetadata {
                title,
                author,
                pages: Some(parse_response.page_count as usize),
                word_count,
                ..Default::default()
            },
            format,
        })
    }

    /// Check if the Python parser service is healthy
    pub async fn health_check(&self) -> bool {
        match self.client
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

// Use base64 crate properly
use base64::Engine as _;

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
}
