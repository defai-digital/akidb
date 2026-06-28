//! AkiDB gRPC Client
//!
//! Client for inserting vectors into AkiDB.

use std::time::{Duration, Instant};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

use crate::config::AkiDbConfig;
use crate::Result;
use akidb_grpc::proto::{
    akidb_client::AkidbClient as GrpcClient, HealthRequest, InsertBatchRequest,
    InsertBatchResponse, SearchRequest as GrpcSearchRequest, Vector as ProtoVector,
};

/// Vector to insert into AkiDB
#[derive(Debug, Clone)]
pub struct VectorInsert {
    pub id: String,
    pub embedding: Vec<f32>,
    pub metadata: std::collections::HashMap<String, String>,
    /// Source chunk text, indexed for lexical (BM25) hybrid retrieval.
    pub text: String,
}

/// Insert result
#[derive(Debug, Clone)]
pub struct InsertResult {
    pub id: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Batch insert result
#[derive(Debug, Clone)]
pub struct BatchInsertResult {
    pub total: usize,
    pub successful: usize,
    pub failed: usize,
    pub results: Vec<InsertResult>,
    pub latency_ms: u64,
}

/// AkiDB gRPC client
pub struct AkiDbClient {
    endpoint: String,
    channel: Option<Channel>,
    client: Option<GrpcClient<Channel>>,
    timeout: Duration,
    collection: String,
}

impl AkiDbClient {
    /// Create a new AkiDB client
    pub fn new(config: &AkiDbConfig) -> Self {
        Self {
            endpoint: config.endpoint.clone(),
            channel: None,
            client: None,
            timeout: Duration::from_millis(config.timeout_ms),
            collection: config.collection.clone(),
        }
    }

    /// Connect to AkiDB
    pub async fn connect(&mut self) -> Result<()> {
        info!(endpoint = %self.endpoint, "Connecting to AkiDB");

        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| crate::IngestionError::Storage(format!("Invalid endpoint: {}", e)))?
            .timeout(self.timeout)
            .connect_timeout(Duration::from_secs(10));

        let channel = endpoint.connect().await.map_err(|e| {
            crate::IngestionError::Storage(format!("Failed to connect to AkiDB: {}", e))
        })?;

        let client = GrpcClient::new(channel.clone());

        self.channel = Some(channel);
        self.client = Some(client);
        info!("Connected to AkiDB");

        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    /// Insert a single vector
    pub async fn insert(&self, vector: VectorInsert) -> Result<InsertResult> {
        let results = self.insert_batch(vec![vector]).await?;
        results
            .results
            .into_iter()
            .next()
            .ok_or_else(|| crate::IngestionError::Storage("No result returned".to_string()))
    }

    /// Insert a batch of vectors
    pub async fn insert_batch(&self, vectors: Vec<VectorInsert>) -> Result<BatchInsertResult> {
        let start = Instant::now();
        let total = vectors.len();

        if total == 0 {
            return Ok(BatchInsertResult {
                total: 0,
                successful: 0,
                failed: 0,
                results: vec![],
                latency_ms: 0,
            });
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| crate::IngestionError::Storage("Not connected to AkiDB".to_string()))?;

        debug!(count = total, collection = %self.collection, "Inserting vectors into AkiDB");

        // Convert to protobuf vectors
        let proto_vectors: Vec<ProtoVector> = vectors
            .iter()
            .map(|v| {
                // Convert metadata to JSON bytes
                let metadata_bytes = if v.metadata.is_empty() {
                    vec![]
                } else {
                    serde_json::to_vec(&v.metadata).unwrap_or_default()
                };

                ProtoVector {
                    id: v.id.clone(),
                    embedding: v.embedding.clone(),
                    metadata: metadata_bytes,
                    text: v.text.clone(),
                }
            })
            .collect();

        let request = tonic::Request::new(InsertBatchRequest {
            collection: self.collection.clone(),
            vectors: proto_vectors,
        });

        // Make the actual gRPC call
        let mut client = client.clone();
        let response = client.insert_batch(request).await.map_err(|e| {
            error!(error = %e, "gRPC insert_batch failed");
            crate::IngestionError::Storage(format!("gRPC error: {}", e))
        })?;

        let inner = response.into_inner();
        let latency_ms = start.elapsed().as_millis() as u64;
        let batch_result = build_batch_insert_result(&vectors, inner, latency_ms)?;

        debug!(
            total = batch_result.total,
            successful = batch_result.successful,
            failed = batch_result.failed,
            latency_ms = batch_result.latency_ms,
            "Batch insert completed"
        );

        let failed_ids: Vec<_> = batch_result
            .results
            .iter()
            .filter(|result| !result.success)
            .map(|result| result.id.clone())
            .collect();
        if !failed_ids.is_empty() {
            warn!(
                failed_count = failed_ids.len(),
                failed_ids = ?failed_ids,
                "Some vectors failed to insert"
            );
        }

        Ok(batch_result)
    }

    /// Search for similar vectors (for testing/validation)
    pub async fn search(&self, query: Vec<f32>, k: usize) -> Result<Vec<SearchResult>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| crate::IngestionError::Storage("Not connected to AkiDB".to_string()))?;

        debug!(k, dim = query.len(), collection = %self.collection, "Searching AkiDB");
        let top_k = search_top_k(k)?;

        let request = tonic::Request::new(GrpcSearchRequest {
            collection: self.collection.clone(),
            query,
            top_k,
            nprobe: Some(32),
            filter: vec![],
            tag_filter: None,
        });

        let mut client = client.clone();
        let response = client.search(request).await.map_err(|e| {
            error!(error = %e, "gRPC search failed");
            crate::IngestionError::Storage(format!("gRPC search error: {}", e))
        })?;

        let inner = response.into_inner();

        let results: Vec<SearchResult> = inner
            .results
            .into_iter()
            .map(|r| SearchResult {
                id: r.id,
                distance: r.score,
                metadata: if r.metadata.is_empty() {
                    std::collections::HashMap::new()
                } else {
                    parse_search_metadata(&r.metadata)
                },
            })
            .collect();

        debug!(result_count = results.len(), "Search completed");

        Ok(results)
    }

    /// Get current latency for backpressure monitoring
    pub async fn ping(&self) -> Result<Duration> {
        let start = Instant::now();

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| crate::IngestionError::Storage("Not connected to AkiDB".to_string()))?;

        // Use the health check RPC to measure latency
        let request = tonic::Request::new(HealthRequest {});

        let mut client = client.clone();
        let _response = client.health(request).await.map_err(|e| {
            error!(error = %e, "gRPC health check failed");
            crate::IngestionError::Storage(format!("gRPC health error: {}", e))
        })?;

        Ok(start.elapsed())
    }

    /// Get endpoint for metrics
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

fn search_top_k(k: usize) -> Result<u32> {
    u32::try_from(k).map_err(|_| {
        crate::IngestionError::Storage(format!("Search top_k {} exceeds u32 range", k))
    })
}

fn build_batch_insert_result(
    vectors: &[VectorInsert],
    response: InsertBatchResponse,
    latency_ms: u64,
) -> Result<BatchInsertResult> {
    let total = vectors.len();
    let successful = response.inserted_count as usize;
    if successful > total {
        return Err(crate::IngestionError::Storage(format!(
            "Invalid insert response: inserted_count {} exceeds request total {}",
            successful, total
        )));
    }

    let input_ids: std::collections::HashSet<_> =
        vectors.iter().map(|vector| vector.id.as_str()).collect();
    let mut failed_set = std::collections::HashSet::new();
    for id in &response.failed_ids {
        if !input_ids.contains(id.as_str()) {
            return Err(crate::IngestionError::Storage(format!(
                "Invalid insert response: failed id '{}' was not in the request",
                id
            )));
        }
        if !failed_set.insert(id.as_str()) {
            return Err(crate::IngestionError::Storage(format!(
                "Invalid insert response: duplicate failed id '{}'",
                id
            )));
        }
    }

    let failed = failed_set.len();
    if successful + failed != total {
        return Err(crate::IngestionError::Storage(format!(
            "Invalid insert response: inserted_count {} + failed_count {} != request total {}",
            successful, failed, total
        )));
    }

    let results: Vec<InsertResult> = vectors
        .iter()
        .map(|v| {
            let failed = failed_set.contains(v.id.as_str());
            InsertResult {
                id: v.id.clone(),
                success: !failed,
                error: if failed {
                    Some("Insert failed".to_string())
                } else {
                    None
                },
            }
        })
        .collect();

    Ok(BatchInsertResult {
        total,
        successful,
        failed,
        results,
        latency_ms,
    })
}

fn parse_search_metadata(metadata: &str) -> std::collections::HashMap<String, String> {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(metadata)
    else {
        return std::collections::HashMap::new();
    };

    map.into_iter()
        .map(|(key, value)| {
            let value = match value {
                serde_json::Value::String(s) => s,
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => String::new(),
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
            };
            (key, value)
        })
        .collect()
}

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: String,
    pub distance: f32,
    pub metadata: std::collections::HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn vector(id: &str) -> VectorInsert {
        VectorInsert {
            id: id.to_string(),
            embedding: vec![0.1, 0.2],
            metadata: HashMap::new(),
            text: "text".to_string(),
        }
    }

    #[test]
    fn test_vector_insert() {
        let vi = VectorInsert {
            id: "test-1".to_string(),
            embedding: vec![0.1, 0.2, 0.3],
            metadata: std::collections::HashMap::new(),
            text: "hello world".to_string(),
        };
        assert_eq!(vi.id, "test-1");
        assert_eq!(vi.embedding.len(), 3);
        assert_eq!(vi.text, "hello world");
    }

    #[test]
    fn test_batch_result() {
        let result = BatchInsertResult {
            total: 10,
            successful: 8,
            failed: 2,
            results: vec![],
            latency_ms: 100,
        };
        assert_eq!(result.total, 10);
        assert_eq!(result.successful, 8);
    }

    #[test]
    fn test_search_top_k_rejects_u32_overflow() {
        let result = search_top_k((u32::MAX as usize) + 1);

        assert!(
            matches!(result, Err(crate::IngestionError::Storage(message)) if message.contains("exceeds u32 range"))
        );
    }

    #[test]
    fn test_build_batch_insert_result_rejects_inserted_count_overflow() {
        let vectors = vec![vector("a")];
        let response = InsertBatchResponse {
            success: true,
            inserted_count: 2,
            failed_ids: vec![],
        };

        let result = build_batch_insert_result(&vectors, response, 10);

        assert!(
            matches!(result, Err(crate::IngestionError::Storage(message)) if message.contains("exceeds request total"))
        );
    }

    #[test]
    fn test_build_batch_insert_result_rejects_unknown_failed_id() {
        let vectors = vec![vector("a")];
        let response = InsertBatchResponse {
            success: false,
            inserted_count: 0,
            failed_ids: vec!["missing".to_string()],
        };

        let result = build_batch_insert_result(&vectors, response, 10);

        assert!(
            matches!(result, Err(crate::IngestionError::Storage(message)) if message.contains("was not in the request"))
        );
    }

    #[test]
    fn test_build_batch_insert_result_requires_consistent_counts() {
        let vectors = vec![vector("a"), vector("b")];
        let response = InsertBatchResponse {
            success: true,
            inserted_count: 1,
            failed_ids: vec![],
        };

        let result = build_batch_insert_result(&vectors, response, 10);

        assert!(
            matches!(result, Err(crate::IngestionError::Storage(message)) if message.contains("request total"))
        );
    }

    #[test]
    fn test_build_batch_insert_result_maps_failed_ids() {
        let vectors = vec![vector("a"), vector("b")];
        let response = InsertBatchResponse {
            success: false,
            inserted_count: 1,
            failed_ids: vec!["b".to_string()],
        };

        let result = build_batch_insert_result(&vectors, response, 10).unwrap();

        assert_eq!(result.total, 2);
        assert_eq!(result.successful, 1);
        assert_eq!(result.failed, 1);
        assert!(result.results[0].success);
        assert!(!result.results[1].success);
    }

    #[test]
    fn test_parse_search_metadata_preserves_non_string_scalars() {
        let metadata = parse_search_metadata(r#"{"customer":"HGC","year":2025,"active":true}"#);

        assert_eq!(metadata.get("customer").map(String::as_str), Some("HGC"));
        assert_eq!(metadata.get("year").map(String::as_str), Some("2025"));
        assert_eq!(metadata.get("active").map(String::as_str), Some("true"));
    }
}
