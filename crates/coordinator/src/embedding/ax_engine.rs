//! OpenAI-compatible embedding HTTP client
//!
//! Calls AkiDB's local ax-engine embedding sidecar at `/v1/embeddings`.
//! The sidecar wraps the current ax-engine native `Session.embed_batch*` API,
//! while this Rust client keeps a stable HTTP contract.

use crate::embedding::{EmbeddingError, EmbeddingResult, EmbeddingService};
use akidb_common::config::EmbeddingClientConfig;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::debug;

/// Request body for `/v1/embeddings`
#[derive(Debug, Serialize)]
struct EmbeddingRequest {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<serde_json::Value>,
    encoding_format: String,
}

/// Single-element request
#[derive(Debug, Serialize)]
struct SingleEmbeddingRequest {
    model: String,
    input: String,
    encoding_format: String,
}

/// OpenAI-compatible embedding response
#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
    model: String,
    usage: Option<UsageInfo>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
    index: usize,
    object: String,
}

#[derive(Debug, Deserialize)]
struct UsageInfo {
    prompt_tokens: u64,
    total_tokens: u64,
}

/// Local embedding HTTP client
pub struct AxEngineEmbedding {
    client: Client,
    config: EmbeddingClientConfig,
}

impl AxEngineEmbedding {
    /// Create a new embedding HTTP client
    pub fn new(config: EmbeddingClientConfig) -> EmbeddingResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| {
                EmbeddingError::BackendError(format!("Failed to build HTTP client: {}", e))
            })?;

        Ok(Self { client, config })
    }

    /// Validate embedding dimensions match expected
    fn validate_dimensions(&self, embedding: &[f32]) -> EmbeddingResult<()> {
        if embedding.len() != self.config.dimensions {
            return Err(EmbeddingError::DimensionMismatch {
                expected: self.config.dimensions,
                actual: embedding.len(),
            });
        }
        if let Some((idx, value)) = embedding
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(EmbeddingError::BackendError(format!(
                "embedding value at dimension {} must be finite, got {}",
                idx, value
            )));
        }
        Ok(())
    }

    /// Parse the OpenAI-compatible response, sorting by `index` to preserve input order
    fn parse_response(
        &self,
        response: EmbeddingResponse,
        expected_count: usize,
    ) -> EmbeddingResult<Vec<Vec<f32>>> {
        if response.data.len() != expected_count {
            return Err(EmbeddingError::BackendError(format!(
                "Expected {} embeddings, got {}",
                expected_count,
                response.data.len()
            )));
        }

        let mut seen_indices = vec![false; expected_count];
        for item in &response.data {
            if item.index >= expected_count {
                return Err(EmbeddingError::BackendError(format!(
                    "embedding response index {} out of range for {} inputs",
                    item.index, expected_count
                )));
            }
            if seen_indices[item.index] {
                return Err(EmbeddingError::BackendError(format!(
                    "duplicate embedding response index {}",
                    item.index
                )));
            }
            seen_indices[item.index] = true;
        }

        // Sort by index to preserve input order.
        let mut data = response.data;
        data.sort_by_key(|d| d.index);

        let mut results = Vec::with_capacity(data.len());
        for item in data {
            self.validate_dimensions(&item.embedding)?;
            results.push(item.embedding);
        }

        Ok(results)
    }
}

impl EmbeddingService for AxEngineEmbedding {
    fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    fn embed(&self, text: &str) -> EmbeddingResult<Vec<f32>> {
        let request = SingleEmbeddingRequest {
            model: self.config.model.clone(),
            input: text.to_string(),
            encoding_format: "float".to_string(),
        };

        let response = self
            .client
            .post(&self.config.url)
            .json(&request)
            .send()
            .map_err(|e| {
                if e.is_timeout() {
                    EmbeddingError::Timeout(Duration::from_millis(self.config.timeout_ms))
                } else if e.is_connect() {
                    EmbeddingError::BackendError(format!(
                        "Cannot connect to embedding sidecar at {}: {}",
                        self.config.url, e
                    ))
                } else {
                    EmbeddingError::BackendError(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(EmbeddingError::BackendError(format!(
                "embedding sidecar returned {}: {}",
                status, body
            )));
        }

        let embedding_response: EmbeddingResponse = response.json().map_err(|e| {
            EmbeddingError::BackendError(format!("Failed to parse response: {}", e))
        })?;

        if let Some(usage) = &embedding_response.usage {
            debug!(
                prompt_tokens = usage.prompt_tokens,
                total_tokens = usage.total_tokens,
                "embedding sidecar usage"
            );
        }

        let mut results = self.parse_response(embedding_response, 1)?;
        results
            .pop()
            .ok_or_else(|| EmbeddingError::BackendError("Empty response".to_string()))
    }

    fn embed_batch(&self, texts: &[&str]) -> EmbeddingResult<Vec<Vec<f32>>> {
        if texts.len() > self.config.max_batch_size {
            return Err(EmbeddingError::BatchTooLarge {
                size: texts.len(),
                max: self.config.max_batch_size,
            });
        }

        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let input_array: Vec<String> = texts.iter().map(|t| t.to_string()).collect();
        let request = EmbeddingRequest {
            model: self.config.model.clone(),
            input: Some(serde_json::Value::Array(
                input_array
                    .into_iter()
                    .map(serde_json::Value::String)
                    .collect(),
            )),
            encoding_format: "float".to_string(),
        };

        let response = self
            .client
            .post(&self.config.url)
            .json(&request)
            .send()
            .map_err(|e| {
                if e.is_timeout() {
                    EmbeddingError::Timeout(Duration::from_millis(self.config.timeout_ms))
                } else if e.is_connect() {
                    EmbeddingError::BackendError(format!(
                        "Cannot connect to embedding sidecar at {}: {}",
                        self.config.url, e
                    ))
                } else {
                    EmbeddingError::BackendError(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(EmbeddingError::BackendError(format!(
                "embedding sidecar returned {}: {}",
                status, body
            )));
        }

        let embedding_response: EmbeddingResponse = response.json().map_err(|e| {
            EmbeddingError::BackendError(format!("Failed to parse response: {}", e))
        })?;

        if let Some(usage) = &embedding_response.usage {
            debug!(
                prompt_tokens = usage.prompt_tokens,
                total_tokens = usage.total_tokens,
                batch_size = texts.len(),
                "embedding sidecar batch usage"
            );
        }

        self.parse_response(embedding_response, texts.len())
    }

    fn is_ready(&self) -> bool {
        // Attempt a lightweight HEAD-like check by sending an empty batch
        // In practice, callers should use the health endpoint; this is a best-effort check.
        // For now, assume ready if the client was constructed successfully.
        // A real health check would GET /health or similar.
        true
    }

    fn name(&self) -> &str {
        "ax-engine"
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> EmbeddingClientConfig {
        EmbeddingClientConfig {
            enabled: true,
            url: "http://127.0.0.1:18080/v1/embeddings".to_string(),
            model: "test-model".to_string(),
            dimensions: 4,
            timeout_ms: 5000,
            max_batch_size: 8,
        }
    }

    /// Start a mock HTTP server that returns valid embeddings
    #[allow(dead_code)]
    fn start_mock_server(dimensions: usize) -> (String, tokio::task::JoinHandle<()>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}/v1/embeddings", addr);
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let handle = tokio::spawn(async move {
            // Simple synchronous mock for tests
            tokio::task::spawn_blocking(move || {
                for _ in 0..10 {
                    if let Ok((mut stream, _)) = listener.accept() {
                        use std::io::{Read, Write};
                        let mut buf = [0u8; 4096];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..n]);

                        // Parse input array size from request body
                        let input_count = request.matches("\"input\"").count();
                        let is_batch = request.contains('[') && input_count > 0;

                        // Build embedding data array
                        let mut data_parts = Vec::new();
                        let actual_count = if is_batch { 2 } else { 1 }; // Default batch of 2 for tests
                        for i in 0..actual_count {
                            let embedding: Vec<f32> = (0..dimensions)
                                .map(|j| (i as f32 + 1.0) * (j as f32 + 1.0) * 0.1)
                                .collect();
                            let emb_str: Vec<String> = embedding.iter().map(|v| v.to_string()).collect();
                            data_parts.push(format!(
                                r#"{{"embedding":[{}],"index":{},"object":"embedding"}}"#,
                                emb_str.join(","),
                                i
                            ));
                        }

                        let body = format!(
                            r#"{{"data":[{}],"model":"test-model","usage":{{"prompt_tokens":10,"total_tokens":10}}}}"#,
                            data_parts.join(",")
                        );

                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        call_count_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
            .await
            .ok();
        });

        (url, handle)
    }

    #[test]
    fn test_ax_engine_client_creation() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.dimensions(), 4);
        assert_eq!(client.name(), "ax-engine");
        assert_eq!(client.max_batch_size(), 8);
        assert!(client.is_ready());
    }

    #[test]
    fn test_batch_too_large() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        let texts: Vec<&str> = (0..20).map(|_| "text").collect();
        let result = client.embed_batch(&texts);
        assert!(matches!(result, Err(EmbeddingError::BatchTooLarge { .. })));
    }

    #[test]
    fn test_empty_batch() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        let result = client.embed_batch(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_dimension_validation() {
        let config = EmbeddingClientConfig {
            dimensions: 4,
            ..test_config()
        };
        let client = AxEngineEmbedding::new(config).unwrap();

        // 3-dim embedding should fail validation
        let bad_embedding = vec![1.0, 2.0, 3.0];
        let result = client.validate_dimensions(&bad_embedding);
        assert!(matches!(
            result,
            Err(EmbeddingError::DimensionMismatch {
                expected: 4,
                actual: 3
            })
        ));

        // 4-dim embedding should pass
        let good_embedding = vec![1.0, 2.0, 3.0, 4.0];
        assert!(client.validate_dimensions(&good_embedding).is_ok());
    }

    #[test]
    fn test_connection_error() {
        // Use a port that's definitely not listening
        let config = EmbeddingClientConfig {
            url: "http://127.0.0.1:19999/v1/embeddings".to_string(),
            timeout_ms: 1000,
            ..test_config()
        };
        let client = AxEngineEmbedding::new(config).unwrap();
        let result = client.embed("hello");
        assert!(result.is_err());
        match result.unwrap_err() {
            EmbeddingError::BackendError(msg) => {
                assert!(msg.contains("Cannot connect") || msg.contains("error"));
            }
            other => panic!("Expected BackendError, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_response_ordering() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        // Response with out-of-order indices
        let response = EmbeddingResponse {
            data: vec![
                EmbeddingData {
                    embedding: vec![4.0, 4.0, 4.0, 4.0],
                    index: 1,
                    object: "embedding".to_string(),
                },
                EmbeddingData {
                    embedding: vec![1.0, 1.0, 1.0, 1.0],
                    index: 0,
                    object: "embedding".to_string(),
                },
            ],
            model: "test-model".to_string(),
            usage: None,
        };

        let results = client.parse_response(response, 2).unwrap();
        assert_eq!(results.len(), 2);
        // First result should be index 0 (sorted)
        assert_eq!(results[0], vec![1.0, 1.0, 1.0, 1.0]);
        // Second result should be index 1
        assert_eq!(results[1], vec![4.0, 4.0, 4.0, 4.0]);
    }

    #[test]
    fn test_parse_response_count_mismatch() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        let response = EmbeddingResponse {
            data: vec![EmbeddingData {
                embedding: vec![1.0, 2.0, 3.0, 4.0],
                index: 0,
                object: "embedding".to_string(),
            }],
            model: "test-model".to_string(),
            usage: None,
        };

        // Expected 2 but got 1
        let result = client.parse_response(response, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_response_rejects_duplicate_indices() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        let response = EmbeddingResponse {
            data: vec![
                EmbeddingData {
                    embedding: vec![1.0, 1.0, 1.0, 1.0],
                    index: 0,
                    object: "embedding".to_string(),
                },
                EmbeddingData {
                    embedding: vec![2.0, 2.0, 2.0, 2.0],
                    index: 0,
                    object: "embedding".to_string(),
                },
            ],
            model: "test-model".to_string(),
            usage: None,
        };

        let result = client.parse_response(response, 2);
        assert!(
            matches!(result, Err(EmbeddingError::BackendError(message)) if message.contains("duplicate embedding response index 0"))
        );
    }

    #[test]
    fn test_parse_response_rejects_out_of_range_index() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        let response = EmbeddingResponse {
            data: vec![
                EmbeddingData {
                    embedding: vec![1.0, 1.0, 1.0, 1.0],
                    index: 0,
                    object: "embedding".to_string(),
                },
                EmbeddingData {
                    embedding: vec![2.0, 2.0, 2.0, 2.0],
                    index: 9,
                    object: "embedding".to_string(),
                },
            ],
            model: "test-model".to_string(),
            usage: None,
        };

        let result = client.parse_response(response, 2);
        assert!(
            matches!(result, Err(EmbeddingError::BackendError(message)) if message.contains("embedding response index 9 out of range"))
        );
    }

    #[test]
    fn test_parse_response_rejects_non_finite_embedding_value() {
        let config = test_config();
        let client = AxEngineEmbedding::new(config).unwrap();

        let response = EmbeddingResponse {
            data: vec![EmbeddingData {
                embedding: vec![1.0, f32::NAN, 3.0, 4.0],
                index: 0,
                object: "embedding".to_string(),
            }],
            model: "test-model".to_string(),
            usage: None,
        };

        let result = client.parse_response(response, 1);
        assert!(
            matches!(result, Err(EmbeddingError::BackendError(message)) if message.contains("must be finite"))
        );
    }
}
