//! AkiDB gRPC Client
//!
//! Client for inserting vectors into AkiDB.

use std::time::{Duration, Instant};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, error, warn};

use akidb_grpc::proto::{
    self,
    akidb_client::AkidbClient as GrpcClient,
    InsertBatchRequest, InsertBatchResponse,
    SearchRequest as GrpcSearchRequest,
    HealthRequest,
    Vector as ProtoVector,
};
use crate::config::AkiDbConfig;
use crate::Result;

/// Vector to insert into AkiDB
#[derive(Debug, Clone)]
pub struct VectorInsert {
    pub id: String,
    pub embedding: Vec<f32>,
    pub metadata: std::collections::HashMap<String, String>,
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

        let channel = endpoint.connect().await
            .map_err(|e| crate::IngestionError::Storage(format!("Failed to connect to AkiDB: {}", e)))?;

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
        results.results.into_iter().next().ok_or_else(|| {
            crate::IngestionError::Storage("No result returned".to_string())
        })
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

        let client = self.client.as_ref().ok_or_else(|| {
            crate::IngestionError::Storage("Not connected to AkiDB".to_string())
        })?;

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
                    // TODO(CP5): thread chunk source text through VectorInsert to
                    // populate the lexical index from ingestion.
                    text: String::new(),
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

        // Build results from response
        let failed_set: std::collections::HashSet<_> = inner.failed_ids.iter().collect();
        let results: Vec<InsertResult> = vectors
            .iter()
            .map(|v| {
                let failed = failed_set.contains(&v.id);
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

        let successful = inner.inserted_count as usize;
        let failed = total - successful;

        debug!(
            total,
            successful,
            failed,
            latency_ms,
            "Batch insert completed"
        );

        if !inner.success && !inner.failed_ids.is_empty() {
            warn!(
                failed_count = inner.failed_ids.len(),
                failed_ids = ?inner.failed_ids,
                "Some vectors failed to insert"
            );
        }

        Ok(BatchInsertResult {
            total,
            successful,
            failed,
            results,
            latency_ms,
        })
    }

    /// Search for similar vectors (for testing/validation)
    pub async fn search(&self, query: Vec<f32>, k: usize) -> Result<Vec<SearchResult>> {
        let client = self.client.as_ref().ok_or_else(|| {
            crate::IngestionError::Storage("Not connected to AkiDB".to_string())
        })?;

        debug!(k, dim = query.len(), collection = %self.collection, "Searching AkiDB");

        let request = tonic::Request::new(GrpcSearchRequest {
            collection: self.collection.clone(),
            query,
            top_k: k as u32,
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
                    serde_json::from_str(&r.metadata).unwrap_or_default()
                },
            })
            .collect();

        debug!(result_count = results.len(), "Search completed");

        Ok(results)
    }

    /// Get current latency for backpressure monitoring
    pub async fn ping(&self) -> Result<Duration> {
        let start = Instant::now();

        let client = self.client.as_ref().ok_or_else(|| {
            crate::IngestionError::Storage("Not connected to AkiDB".to_string())
        })?;

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

    #[test]
    fn test_vector_insert() {
        let vi = VectorInsert {
            id: "test-1".to_string(),
            embedding: vec![0.1, 0.2, 0.3],
            metadata: std::collections::HashMap::new(),
        };
        assert_eq!(vi.id, "test-1");
        assert_eq!(vi.embedding.len(), 3);
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
}
