//! Embedding Client
//!
//! Client for vLLM/TensorRT embedding service with retry logic.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, warn};

use crate::Result;

/// Default maximum retry attempts
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff (milliseconds)
const BASE_RETRY_DELAY_MS: u64 = 100;

/// Simple jitter function (returns 0.0 to 0.5)
/// Uses system time nanos for pseudo-randomness (good enough for backoff jitter)
fn rand_jitter() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // Scale to 0.0-0.5 range
    (nanos as f64 % 1000.0) / 2000.0
}

/// Embedding request (OpenAI-compatible)
#[derive(Debug, Serialize)]
pub struct EmbeddingRequest {
    pub model: String,
    pub input: Vec<String>,
}

/// Embedding response
#[derive(Debug, Deserialize)]
pub struct EmbeddingResponse {
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize)]
pub struct EmbeddingData {
    pub embedding: Vec<f32>,
    pub index: usize,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: usize,
    pub total_tokens: usize,
}

/// Client for embedding service with retry logic
pub struct EmbeddingClient {
    client: Client,
    base_url: String,
    model: String,
    max_retries: u32,
}

impl EmbeddingClient {
    /// Create a new embedding client
    pub fn new(base_url: &str, model: &str) -> Self {
        Self::with_retries(base_url, model, DEFAULT_MAX_RETRIES)
    }

    /// Create a new embedding client with custom retry count
    pub fn with_retries(base_url: &str, model: &str, max_retries: u32) -> Self {
        // FIX BUG-H055: Add connect_timeout to prevent multi-minute hangs
        // when embedding server is unreachable. Without this, connections hang
        // for the default TCP timeout (~2 minutes) on each retry attempt.
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            model: model.to_string(),
            max_retries,
        }
    }

    /// Create with default Qwen3 model
    pub fn with_qwen3(base_url: &str) -> Self {
        Self::new(base_url, "Qwen/Qwen3-Embedding-8B")
    }

    /// Check if an error is retryable (transient)
    fn is_retryable_error(status: reqwest::StatusCode) -> bool {
        // Retry on server errors (5xx) and rate limiting (429)
        status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    }

    /// Calculate delay for exponential backoff with jitter
    fn calculate_backoff(attempt: u32) -> Duration {
        let multiplier = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let base_delay = BASE_RETRY_DELAY_MS.saturating_mul(multiplier);
        // Add jitter (0-50% of base delay)
        let jitter = (base_delay as f64 * rand_jitter()) as u64;
        Duration::from_millis(base_delay.saturating_add(jitter))
    }

    /// Embed a batch of texts with retry logic
    pub async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        debug!(count = texts.len(), "Embedding batch");

        let request = EmbeddingRequest {
            model: self.model.clone(),
            input: texts,
        };

        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let delay = Self::calculate_backoff(attempt - 1);
                warn!(
                    attempt = attempt,
                    max_retries = self.max_retries,
                    delay_ms = delay.as_millis(),
                    "Retrying embedding request after delay"
                );
                tokio::time::sleep(delay).await;
            }

            let response = match self
                .client
                .post(format!("{}/v1/embeddings", self.base_url))
                .json(&request)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    // Network errors are retryable
                    last_error = Some(crate::IngestionError::Embedding(format!(
                        "Network error: {}",
                        e
                    )));
                    continue;
                }
            };

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();

                if Self::is_retryable_error(status) && attempt < self.max_retries {
                    last_error = Some(crate::IngestionError::Embedding(format!(
                        "HTTP {}: {}",
                        status, body
                    )));
                    continue;
                }

                return Err(crate::IngestionError::Embedding(format!(
                    "HTTP {}: {}",
                    status, body
                )));
            }

            let embed_response: EmbeddingResponse = response.json().await?;
            return parse_embedding_response(embed_response, request.input.len());
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            crate::IngestionError::Embedding("Max retries exceeded".to_string())
        }))
    }

    /// Embed a single text
    pub async fn embed_single(&self, text: &str) -> Result<Vec<f32>> {
        let embeddings = self.embed(vec![text.to_string()]).await?;
        embeddings
            .into_iter()
            .next()
            .ok_or_else(|| crate::IngestionError::Embedding("No embedding returned".to_string()))
    }

    /// Check if the embedding service is healthy
    pub async fn health_check(&self) -> bool {
        match self
            .client
            .get(format!("{}/health", self.base_url))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Get embedding dimension (by doing a test embedding)
    pub async fn dimension(&self) -> Result<usize> {
        let embedding = self.embed_single("test").await?;
        Ok(embedding.len())
    }
}

fn parse_embedding_response(
    embed_response: EmbeddingResponse,
    expected_count: usize,
) -> Result<Vec<Vec<f32>>> {
    // Sort by index and extract embeddings
    let mut embeddings: Vec<_> = embed_response.data.into_iter().collect();
    embeddings.sort_by_key(|d| d.index);

    // FIX BUG-H048: Validate that response indices are sequential 0..n-1.
    // If backend returns non-sequential indices (e.g., [0, 2, 3] missing index 1),
    // embeddings would be silently mapped to wrong texts causing data corruption.
    if embeddings.len() != expected_count {
        return Err(crate::IngestionError::Embedding(format!(
            "Embedding count mismatch: expected {}, got {}",
            expected_count,
            embeddings.len()
        )));
    }

    let expected_dimension = embeddings
        .first()
        .map(|data| data.embedding.len())
        .unwrap_or(0);
    if expected_dimension == 0 {
        return Err(crate::IngestionError::Embedding(
            "Embedding dimension must not be empty".to_string(),
        ));
    }

    for (expected_idx, data) in embeddings.iter().enumerate() {
        if data.index != expected_idx {
            return Err(crate::IngestionError::Embedding(format!(
                "Embedding index mismatch: expected sequential index {}, got {}. \
                 Backend may have returned non-sequential or duplicate indices.",
                expected_idx, data.index
            )));
        }

        if data.embedding.len() != expected_dimension {
            return Err(crate::IngestionError::Embedding(format!(
                "Embedding dimension mismatch at item {}: expected {}, got {}",
                data.index,
                expected_dimension,
                data.embedding.len()
            )));
        }

        if let Some((dimension, value)) = data
            .embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(crate::IngestionError::Embedding(format!(
                "Embedding value at item {} dimension {} must be finite, got {}",
                data.index, dimension, value
            )));
        }
    }

    Ok(embeddings.into_iter().map(|d| d.embedding).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = EmbeddingClient::with_qwen3("http://localhost:8000");
        assert_eq!(client.model, "Qwen/Qwen3-Embedding-8B");
    }

    #[test]
    fn test_parse_embedding_response_rejects_non_finite_values() {
        let response = EmbeddingResponse {
            data: vec![EmbeddingData {
                embedding: vec![1.0, f32::INFINITY],
                index: 0,
            }],
            model: "test".to_string(),
            usage: None,
        };

        let result = parse_embedding_response(response, 1);

        assert!(
            matches!(result, Err(crate::IngestionError::Embedding(message)) if message.contains("must be finite"))
        );
    }

    #[test]
    fn test_parse_embedding_response_rejects_duplicate_indices() {
        let response = EmbeddingResponse {
            data: vec![
                EmbeddingData {
                    embedding: vec![1.0, 0.0],
                    index: 0,
                },
                EmbeddingData {
                    embedding: vec![0.0, 1.0],
                    index: 0,
                },
            ],
            model: "test".to_string(),
            usage: None,
        };

        let result = parse_embedding_response(response, 2);

        assert!(
            matches!(result, Err(crate::IngestionError::Embedding(message)) if message.contains("Embedding index mismatch"))
        );
    }

    #[test]
    fn test_parse_embedding_response_rejects_empty_embedding() {
        let response = EmbeddingResponse {
            data: vec![EmbeddingData {
                embedding: Vec::new(),
                index: 0,
            }],
            model: "test".to_string(),
            usage: None,
        };

        let result = parse_embedding_response(response, 1);

        assert!(
            matches!(result, Err(crate::IngestionError::Embedding(message)) if message.contains("must not be empty"))
        );
    }

    #[test]
    fn test_parse_embedding_response_rejects_mixed_dimensions() {
        let response = EmbeddingResponse {
            data: vec![
                EmbeddingData {
                    embedding: vec![1.0, 0.0],
                    index: 0,
                },
                EmbeddingData {
                    embedding: vec![0.0, 1.0, 0.5],
                    index: 1,
                },
            ],
            model: "test".to_string(),
            usage: None,
        };

        let result = parse_embedding_response(response, 2);

        assert!(
            matches!(result, Err(crate::IngestionError::Embedding(message)) if message.contains("dimension mismatch"))
        );
    }

    #[test]
    fn test_backoff_saturates_for_large_attempts() {
        let backoff = EmbeddingClient::calculate_backoff(128);
        assert!(backoff >= Duration::from_millis(BASE_RETRY_DELAY_MS));
    }
}
