//! AkiDB Coordinator Server
//!
//! This binary runs the coordinator service that fans out searches
//! to all shard nodes and merges results.

use crate::{
    coordinator_metrics, export_metrics, BackpressureConfig, BackpressureController,
    ConsistencyTracker, FanoutExecutor, ShardInfo, ShardRouter,
};
use akidb_proto::akidb_server::{Akidb, AkidbServer};
use akidb_proto::{
    ClusterMetrics, CollectionInfo, CoordinatorNode, CreateCollectionRequest,
    CreateCollectionResponse, DeleteRequest, DeleteResponse, DropCollectionRequest,
    DropCollectionResponse, GetClusterStateRequest, GetClusterStateResponse, GetCollectionRequest,
    GetCollectionResponse, GetRequest, GetResponse, HealthRequest, HealthResponse,
    InsertBatchRequest, InsertBatchResponse, InsertRequest, InsertResponse, ListCollectionsRequest,
    ListCollectionsResponse, NodeStatus, SearchBatchRequest, SearchBatchResponse, SearchRequest,
    SearchResponse, SearchResult as ProtoSearchResult, ShardNode, TextSearchRequest, UpdateRequest,
    UpdateResponse, UpdateStatus, Vector, VisibilityInfo,
};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request as HyperRequest, Response as HyperResponse};
use hyper_util::rt::TokioIo;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// AkiDB Coordinator Server
#[derive(clap::Args, Debug)]
pub struct Args {
    /// gRPC listen address
    #[arg(short, long, default_value = "0.0.0.0:50050")]
    pub listen: String,

    /// Shard addresses (comma-separated)
    #[arg(short, long, default_value = "127.0.0.1:50051")]
    pub shards: String,

    /// Search timeout in milliseconds
    #[arg(short, long, default_value = "5000")]
    pub timeout: u64,

    /// Log level
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Connection pool size per shard
    #[arg(short = 'p', long, default_value = "4")]
    pub pool_size: usize,

    /// Metrics HTTP port (0 to disable)
    #[arg(short = 'm', long, default_value = "9090")]
    pub metrics_port: u16,

    /// Maximum concurrent requests (backpressure)
    #[arg(long, default_value = "1000")]
    pub max_concurrent: usize,

    /// Rate limit (requests per second, 0 = unlimited)
    #[arg(long, default_value = "0")]
    pub rate_limit: u64,

    /// Maximum queue depth for waiting requests
    #[arg(long, default_value = "5000")]
    pub max_queue_depth: usize,
}

/// Coordinator gRPC service
pub struct CoordinatorService {
    fanout: Arc<FanoutExecutor>,
    router: Arc<RwLock<ShardRouter>>,
    /// Tracks recent writes for read-your-writes consistency
    consistency: Arc<ConsistencyTracker>,
    /// Backpressure controller for rate limiting and load shedding
    backpressure: Arc<BackpressureController>,
    /// Local coordinator address
    local_address: String,
    /// Local coordinator ID
    local_id: String,
}

impl CoordinatorService {
    pub fn new(
        shards: Vec<ShardInfo>,
        timeout: Duration,
        pool_size: usize,
        backpressure_config: BackpressureConfig,
        local_address: String,
    ) -> Self {
        let router = Arc::new(RwLock::new(ShardRouter::new(shards)));
        let fanout = Arc::new(FanoutExecutor::with_pool_size(
            router.clone(),
            timeout,
            pool_size,
        ));
        let consistency = Arc::new(ConsistencyTracker::new());
        let backpressure = Arc::new(BackpressureController::with_config(backpressure_config));
        let local_id = format!("coord-{}", local_address.replace([':', '.'], "-"));

        Self {
            fanout,
            router,
            consistency,
            backpressure,
            local_address,
            local_id,
        }
    }

    fn validate_search_controls(top_k: u32, nprobe: Option<u32>) -> Result<(), Status> {
        if top_k == 0 {
            return Err(Status::invalid_argument("top_k must be greater than 0"));
        }
        if top_k > 10000 {
            return Err(Status::invalid_argument("top_k exceeds maximum of 10000"));
        }
        if nprobe == Some(0) {
            return Err(Status::invalid_argument("nprobe must be greater than 0"));
        }
        Ok(())
    }

    fn validate_request_collection(collection: &str) -> Result<(), Status> {
        if collection.is_empty() {
            return Err(Status::invalid_argument("collection cannot be empty"));
        }
        Ok(())
    }

    async fn aggregate_collections(&self) -> Result<Vec<CollectionInfo>, Status> {
        let shards = self.router.read().await.all_shards().to_vec();
        if shards.is_empty() {
            return Err(Status::unavailable("No shards available"));
        }

        let mut merged: BTreeMap<String, CollectionInfo> = BTreeMap::new();
        for shard in shards {
            let mut client = self
                .fanout
                .get_shard_client(&shard.address)
                .await
                .map_err(|_| Status::unavailable("Failed to connect to a shard"))?;
            let response = client
                .list_collections(ListCollectionsRequest {})
                .await
                .map_err(|_| Status::unavailable("A shard collection inventory is unavailable"))?
                .into_inner();
            for collection in response.collections {
                match merged.get_mut(&collection.name) {
                    Some(existing) => {
                        if existing.dimensions != collection.dimensions
                            || existing.metric != collection.metric
                            || existing.embedding_model_id != collection.embedding_model_id
                            || existing.vector_precision != collection.vector_precision
                            || existing.chunk_strategy != collection.chunk_strategy
                        {
                            return Err(Status::failed_precondition(
                                "collection schemas differ across shards",
                            ));
                        }
                        existing.vector_count = existing
                            .vector_count
                            .saturating_add(collection.vector_count);
                    }
                    None => {
                        merged.insert(collection.name.clone(), collection);
                    }
                }
            }
        }
        Ok(merged.into_values().collect())
    }

    fn validate_vector_id(id: &str) -> Result<(), Status> {
        if id.is_empty() {
            return Err(Status::invalid_argument("Vector ID cannot be empty"));
        }
        if id.len() > 1024 {
            return Err(Status::invalid_argument(
                "Vector ID exceeds maximum length of 1024",
            ));
        }
        Ok(())
    }

    fn validate_vector_payload(id: &str, vector: &[f32]) -> Result<(), Status> {
        Self::validate_vector_id(id)?;
        if vector.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(Status::invalid_argument(
                "Vector contains NaN or Infinity values",
            ));
        }
        if vector.is_empty() {
            return Err(Status::invalid_argument("Vector cannot be empty"));
        }
        Ok(())
    }

    fn validate_query_vector(query: &[f32]) -> Result<(), Status> {
        if query.is_empty() {
            return Err(Status::invalid_argument("Query vector cannot be empty"));
        }
        if query.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(Status::invalid_argument(
                "Query vector contains NaN or Infinity values",
            ));
        }
        Ok(())
    }

    fn validate_unique_batch_ids(vectors: &[Vector]) -> Result<(), Status> {
        let mut seen = std::collections::HashSet::with_capacity(vectors.len());
        for vector in vectors {
            if !seen.insert(vector.id.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "insert_batch contains duplicate vector id '{}'",
                    vector.id
                )));
            }
        }
        Ok(())
    }

    fn search_result_metadata(metadata: Option<serde_json::Value>) -> String {
        metadata.map(|value| value.to_string()).unwrap_or_default()
    }

    /// Post-merge score cut + group_by (Phase C knobs survive multi-shard path).
    fn apply_score_and_group(
        results: Vec<ProtoSearchResult>,
        top_k: usize,
        score_threshold: Option<f32>,
        group_by: &str,
        group_size: Option<u32>,
    ) -> Vec<ProtoSearchResult> {
        let mut filtered: Vec<ProtoSearchResult> = match score_threshold {
            Some(min) if min.is_finite() => results
                .into_iter()
                .filter(|r| r.score.is_finite() && r.score >= min)
                .collect(),
            _ => results,
        };
        let key = group_by.trim();
        if key.is_empty() {
            return filtered.into_iter().take(top_k).collect();
        }
        let per_group = group_size.unwrap_or(1).max(1) as usize;
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut out = Vec::new();
        for r in filtered.drain(..) {
            let group_val = {
                if r.metadata.is_empty() {
                    String::new()
                } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(&r.metadata) {
                    match v.get(key) {
                        Some(serde_json::Value::String(s)) => s.clone(),
                        Some(serde_json::Value::Number(n)) => n.to_string(),
                        Some(other) => other.to_string(),
                        None => String::new(),
                    }
                } else {
                    String::new()
                }
            };
            let n = counts.entry(group_val).or_insert(0);
            if *n < per_group {
                *n += 1;
                out.push(r);
            }
            if out.len() >= top_k {
                break;
            }
        }
        out
    }

    fn accumulate_shard_insert_response(
        vector_ids: &[String],
        response: InsertBatchResponse,
        total_inserted: &mut u32,
        all_failed_ids: &mut Vec<String>,
    ) {
        let input_ids: std::collections::HashSet<&str> =
            vector_ids.iter().map(String::as_str).collect();
        let mut failed_seen = std::collections::HashSet::with_capacity(response.failed_ids.len());
        let failed_ids_valid = response
            .failed_ids
            .iter()
            .all(|id| input_ids.contains(id.as_str()) && failed_seen.insert(id.as_str()));
        let counts_valid =
            response.inserted_count as usize + response.failed_ids.len() == vector_ids.len();

        if !failed_ids_valid || !counts_valid {
            warn!(
                inserted_count = response.inserted_count,
                failed_count = response.failed_ids.len(),
                expected_count = vector_ids.len(),
                failed_ids = ?response.failed_ids,
                "Shard batch insert returned inconsistent accounting; marking shard batch failed"
            );
            all_failed_ids.extend(vector_ids.iter().cloned());
            return;
        }

        *total_inserted += response.inserted_count;
        all_failed_ids.extend(response.failed_ids);
    }
}

#[tonic::async_trait]
impl Akidb for CoordinatorService {
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        // Route insert to the appropriate shard based on vector ID
        let req = request.into_inner();
        Self::validate_request_collection(&req.collection)?;
        Self::validate_vector_payload(&req.id, &req.vector)?;
        let router = self.router.read().await;

        let shard = router
            .route(&akidb_common::VectorId::new(&req.id))
            .ok_or_else(|| Status::unavailable("No shards available"))?;
        let shard_id = shard.id.clone();
        let shard_address = shard.address.clone();
        drop(router);

        // FIX BUG-HUNT-401: Use connection pool instead of creating new TCP connection per request
        // Previously created new connections which caused connection exhaustion under load
        let mut client = self
            .fanout
            .get_shard_client(&shard_address)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to get shard client: {}", e)))?;

        let id_clone = req.id.clone();
        let response = client
            .insert(InsertRequest {
                collection: req.collection,
                id: req.id,
                vector: req.vector,
                metadata: req.metadata,
                text: req.text,
            })
            .await
            .map_err(|e| Status::internal(format!("Shard insert failed: {}", e)))?;

        // Record write for consistency tracking AFTER successful write
        // This ensures we don't have stale entries if the write fails
        self.consistency.record_write(&id_clone, &shard_id);
        self.consistency.confirm_write(&id_clone);
        coordinator_metrics().record_request("insert", "success");

        Ok(response)
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

        Self::validate_request_collection(&req.collection)?;
        Self::validate_search_controls(req.top_k, req.nprobe)?;
        Self::validate_query_vector(&req.query)?;

        // Apply backpressure
        let _guard = self.backpressure.try_acquire().await.map_err(|e| {
            coordinator_metrics().record_request("search", "rejected");
            Status::resource_exhausted(format!("Backpressure: {}", e))
        })?;

        // Fan-out search to all shards (forward Phase C knobs).
        let result = self
            .fanout
            .search(
                &req.collection,
                &req.query,
                req.top_k as usize,
                req.nprobe.unwrap_or(32),
                &req.filter,
                req.tag_filter.clone(),
                req.score_threshold,
                req.group_by.clone(),
                req.group_size,
            )
            .await
            .map_err(|e| {
                coordinator_metrics().record_request("search", "error");
                Status::internal(format!("Fan-out search failed: {}", e))
            })?;

        let latency_secs = start.elapsed().as_secs_f64();
        let latency_us = (latency_secs * 1_000_000.0) as u64;

        // Extract values before consuming
        let partial = result.is_partial();
        let coverage = result.coverage();
        let responding_count = result.responding_shards.len();
        let missing_shards = result.missing_shards;

        // Record metrics
        coordinator_metrics().record_fanout(
            latency_secs,
            coverage as f64,
            responding_count,
            partial,
        );
        coordinator_metrics().record_request("search", "success");

        // Convert results to proto format, then re-apply knobs after merge so
        // multi-shard fanout cannot bypass threshold/group semantics.
        let mut results: Vec<ProtoSearchResult> = result
            .results
            .into_iter()
            .map(|r| ProtoSearchResult {
                id: r.id.as_str().to_string(),
                score: r.score,
                metadata: Self::search_result_metadata(r.metadata),
            })
            .collect();
        results = Self::apply_score_and_group(
            results,
            req.top_k as usize,
            req.score_threshold,
            &req.group_by,
            req.group_size,
        );

        Ok(Response::new(SearchResponse {
            results,
            partial,
            missing_shards,
            coverage,
            latency_us,
            within_slo: latency_us < 10_000, // 10ms SLO
            degraded_mode: partial,
            context_pack: String::new(),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

        Self::validate_request_collection(&req.collection)?;
        Self::validate_vector_id(&req.id)?;
        // Broadcast delete to all shards to ensure consistency
        let result = self
            .fanout
            .broadcast_delete(&req.collection, &req.id)
            .await
            .map_err(|e| {
                coordinator_metrics().record_request("delete", "error");
                Status::internal(format!("Broadcast delete failed: {}", e))
            })?;

        // FIX BUG-HUNT-501: Record delete AFTER successful broadcast, not before
        // Previously, record_delete was called before broadcast. If broadcast failed,
        // the consistency tracker already lost the entry, causing read-your-writes
        // violation where deleted vectors could reappear in subsequent reads.
        self.consistency.record_delete(&req.id);

        let latency_secs = start.elapsed().as_secs_f64();
        coordinator_metrics().record_request("delete", "success");

        debug!(
            "Delete {} completed in {:.2}ms: status={:?}, found_on={:?}",
            req.id,
            latency_secs * 1000.0,
            result.status,
            result.found_on_shard
        );

        Ok(Response::new(DeleteResponse {
            success: true,
            id: req.id,
            status: result.status as i32,
            visibility: "immediate".to_string(),
        }))
    }

    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();
        let id_clone = req.id.clone();
        Self::validate_request_collection(&req.collection)?;
        Self::validate_vector_payload(&req.id, &req.vector)?;

        // Broadcast update: delete from all shards, then insert to correct shard
        let result = self
            .fanout
            .broadcast_update(&req.collection, &req.id, req.vector, req.metadata)
            .await
            .map_err(|e| {
                coordinator_metrics().record_request("update", "error");
                Status::internal(format!("Broadcast update failed: {}", e))
            })?;

        // FIX BUG-HUNT-502: Add consistency tracking for update operations
        // Previously, update had no consistency tracking while insert and delete did.
        // This caused read-your-writes failures after updates - clients would see
        // stale data when reading immediately after updating.
        self.consistency
            .record_write(&id_clone, &result.target_shard);
        self.consistency.confirm_write(&id_clone);

        let latency_secs = start.elapsed().as_secs_f64();
        coordinator_metrics().record_request("update", "success");

        debug!(
            "Update {} completed in {:.2}ms: success={}, target_shard={}",
            req.id,
            latency_secs * 1000.0,
            result.update_success,
            result.target_shard
        );

        // Determine update status based on whether vector existed before
        let status = if result.delete_result.was_deleted() {
            UpdateStatus::Updated
        } else {
            UpdateStatus::Created
        };

        Ok(Response::new(UpdateResponse {
            success: result.update_success,
            id: req.id,
            status: status as i32,
            visibility: Some(VisibilityInfo {
                delete_visibility: "immediate".to_string(),
                insert_visibility: "within_100ms".to_string(),
            }),
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        Self::validate_request_collection(&req.collection)?;
        Self::validate_vector_id(&req.id)?;

        // Check if this vector was recently written (read-your-writes consistency)
        let shard_address = if let Some(write_entry) = self.consistency.get_recent_write(&req.id) {
            // Route to the shard where it was written
            let router = self.router.read().await;
            router
                .all_shards()
                .iter()
                .find(|s| s.id == write_entry.shard_id)
                .map(|s| s.address.clone())
                .ok_or_else(|| Status::unavailable("Recently written shard not found"))?
        } else {
            // Use normal routing
            let router = self.router.read().await;
            let shard = router
                .route(&akidb_common::VectorId::new(&req.id))
                .ok_or_else(|| Status::unavailable("No shards available"))?;
            shard.address.clone()
        };

        // FIX BUG-HUNT-401: Use connection pool instead of creating new TCP connection per request
        let mut client = self
            .fanout
            .get_shard_client(&shard_address)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to get shard client: {}", e)))?;

        let response = client.get(req).await?;
        Ok(response)
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let router = self.router.read().await;
        let healthy_count = router.healthy_shards().len();
        let total_count = router.all_shards().len();

        Ok(Response::new(HealthResponse {
            healthy: healthy_count > 0,
            ready: healthy_count == total_count,
            message: format!(
                "Coordinator: {}/{} shards healthy",
                healthy_count, total_count
            ),
            total_vectors: 0, // Would need to aggregate from shards
            active_vectors: 0,
            using_gpu: false,
        }))
    }

    async fn insert_batch(
        &self,
        request: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertBatchResponse>, Status> {
        let req = request.into_inner();
        Self::validate_request_collection(&req.collection)?;
        Self::validate_unique_batch_ids(&req.vectors)?;
        for vector in &req.vectors {
            Self::validate_vector_payload(&vector.id, &vector.embedding)?;
        }
        let router = self.router.read().await;

        // Partition vectors by shard using consistent hashing
        let mut shard_batches: std::collections::HashMap<String, Vec<akidb_proto::Vector>> =
            std::collections::HashMap::new();
        let mut unrouted_ids = Vec::new();

        for vector in req.vectors {
            let id = akidb_common::VectorId::new(&vector.id);
            if let Some(shard) = router.route(&id) {
                shard_batches
                    .entry(shard.id.clone())
                    .or_default()
                    .push(vector);
            } else {
                unrouted_ids.push(vector.id);
            }
        }

        // Get shard addresses for routing
        let shard_addrs: std::collections::HashMap<String, String> = router
            .all_shards()
            .iter()
            .map(|s| (s.id.clone(), s.address.clone()))
            .collect();

        drop(router);

        // Send batches to each shard in parallel
        // FIX BUG-HUNT-402: Track vector IDs per shard to properly report failures
        // FIX BUG-HUNT-504: Use connection pool instead of creating new connections
        let collection = req.collection.clone();
        let mut handles = Vec::new();
        let mut handle_vector_ids = Vec::new();

        for (shard_id, vectors) in shard_batches {
            let addr = match shard_addrs.get(&shard_id) {
                Some(a) => a.clone(),
                None => {
                    unrouted_ids.extend(vectors.iter().map(|v| v.id.clone()));
                    continue;
                }
            };
            let coll = collection.clone();
            // FIX BUG-HUNT-402: Capture vector IDs before moving vectors into the task
            let vector_ids: Vec<String> = vectors.iter().map(|v| v.id.clone()).collect();
            handle_vector_ids.push(vector_ids.clone());
            // FIX BUG-HUNT-504: Clone the fanout Arc to use connection pool in spawned task
            let fanout = self.fanout.clone();

            handles.push(tokio::spawn(async move {
                // FIX BUG-HUNT-504: Use connection pool instead of raw connect()
                // Previously created new TCP connections per shard, bypassing pool's
                // backpressure, retry logic, and causing connection exhaustion under load.
                let client_result = fanout.get_shard_client(&addr).await;

                let result = match client_result {
                    Ok(mut client) => {
                        let batch_req = InsertBatchRequest {
                            collection: coll,
                            vectors,
                        };
                        client.insert_batch(batch_req).await
                    }
                    Err(e) => Err(tonic::Status::unavailable(format!(
                        "Failed to get pooled connection: {}",
                        e
                    ))),
                };
                // FIX BUG-HUNT-402: Return vector_ids alongside result for failure tracking
                (result, vector_ids)
            }));
        }

        // Aggregate results
        let mut total_inserted = 0u32;
        let mut all_failed_ids = unrouted_ids;

        for (idx, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok((Ok(response), vector_ids)) => {
                    let inner = response.into_inner();
                    Self::accumulate_shard_insert_response(
                        &vector_ids,
                        inner,
                        &mut total_inserted,
                        &mut all_failed_ids,
                    );
                }
                Ok((Err(e), vector_ids)) => {
                    // FIX BUG-HUNT-402: Track all vector IDs from failed shard as failed
                    // Previously these were silently lost with only a warning logged
                    warn!(
                        "Shard batch insert failed: {} - marking {} vectors as failed",
                        e,
                        vector_ids.len()
                    );
                    all_failed_ids.extend(vector_ids);
                }
                Err(e) => {
                    let vector_ids = &handle_vector_ids[idx];
                    warn!(
                        "Task join error during batch insert: {} - marking {} vectors as failed",
                        e,
                        vector_ids.len()
                    );
                    all_failed_ids.extend(vector_ids.iter().cloned());
                }
            }
        }

        Ok(Response::new(InsertBatchResponse {
            success: all_failed_ids.is_empty(),
            inserted_count: total_inserted,
            failed_ids: all_failed_ids,
        }))
    }

    async fn search_batch(
        &self,
        request: Request<SearchBatchRequest>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        let req = request.into_inner();

        Self::validate_request_collection(&req.collection)?;
        Self::validate_search_controls(req.top_k, req.nprobe)?;
        for query in &req.queries {
            Self::validate_query_vector(&query.vector)?;
        }

        // FIX BUG-HUNT-503: Process queries in parallel instead of sequentially
        // Previously, each query waited for the previous one to complete, resulting in
        // batch latency = N * single_query_latency. Now we use parallel execution so
        // batch latency ≈ single_query_latency (with some overhead for merging).
        let search_futures: Vec<_> = req
            .queries
            .into_iter()
            .map(|query| {
                let search_req = SearchRequest {
                    collection: req.collection.clone(),
                    query: query.vector,
                    top_k: req.top_k,
                    nprobe: req.nprobe,
                    filter: vec![],
                    tag_filter: None,
            score_threshold: None,
            group_by: String::new(),
            group_size: None,
                };
                self.search(Request::new(search_req))
            })
            .collect();

        // Execute all searches in parallel
        let responses: Vec<Response<SearchResponse>> =
            futures::future::try_join_all(search_futures).await?;
        let results: Vec<SearchResponse> = responses.into_iter().map(|r| r.into_inner()).collect();

        Ok(Response::new(SearchBatchResponse { results }))
    }

    async fn get_cluster_state(
        &self,
        _request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        let router = self.router.read().await;
        let all_shards = router.all_shards();
        let healthy_shards = router.healthy_shards();

        // Build coordinator list (for now just this coordinator, as single-coordinator setup)
        let coordinators = vec![CoordinatorNode {
            id: self.local_id.clone(),
            peer_id: self.local_id.clone(),
            address: self.local_address.clone(),
            is_leader: true, // Single coordinator is always leader
            is_self: true,
            status: NodeStatus::Healthy as i32,
        }];

        // Build shard list from router
        let shards: Vec<ShardNode> = all_shards
            .iter()
            .map(|s| {
                let is_healthy = healthy_shards.iter().any(|hs| hs.id == s.id);
                ShardNode {
                    id: s.id.clone(),
                    address: s.address.clone(),
                    vector_count: 0, // Would need health check to get this
                    health_score: if is_healthy { 1.0 } else { 0.0 },
                    gpu_memory_percent: None,
                    temperature: None,
                    status: if is_healthy {
                        NodeStatus::Healthy as i32
                    } else {
                        NodeStatus::Unhealthy as i32
                    },
                }
            })
            .collect();

        // Build cluster metrics from backpressure stats
        // Note: QPS/latency metrics require histogram tracking which isn't exposed yet
        let bp_stats = self.backpressure.stats();
        let backpressure_ratio = if bp_stats.max_concurrent > 0 {
            bp_stats.in_flight as f32 / bp_stats.max_concurrent as f32
        } else {
            0.0
        };
        let cluster_metrics = Some(ClusterMetrics {
            qps: bp_stats.current_rps as f64,
            p50_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            coverage: healthy_shards.len() as f32 / all_shards.len().max(1) as f32,
            backpressure: backpressure_ratio,
            within_slo: true,
        });

        Ok(Response::new(GetClusterStateResponse {
            coordinators,
            shards,
            leader_id: Some(self.local_id.clone()),
            local_peer_id: self.local_id.clone(),
            metrics: cluster_metrics,
        }))
    }

    async fn create_collection(
        &self,
        _request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        Err(Status::failed_precondition(
            "collection creation is not coordinated yet; use an explicitly selected shard",
        ))
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        let name = request.into_inner().name;
        let collection = self
            .aggregate_collections()
            .await?
            .into_iter()
            .find(|collection| collection.name == name);
        Ok(Response::new(GetCollectionResponse {
            found: collection.is_some(),
            collection,
        }))
    }

    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        Ok(Response::new(ListCollectionsResponse {
            collections: self.aggregate_collections().await?,
        }))
    }

    async fn drop_collection(
        &self,
        _request: Request<DropCollectionRequest>,
    ) -> Result<Response<DropCollectionResponse>, Status> {
        Err(Status::failed_precondition(
            "collection deletion is not coordinated yet; use an explicitly selected shard",
        ))
    }

    async fn text_search(
        &self,
        _request: Request<TextSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Err(Status::unimplemented(
            "TextSearch is only available on shard servers with an embedding provider configured",
        ))
    }
}

pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let log_level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting AkiDB Coordinator");

    // Parse shard addresses
    let shards: Vec<ShardInfo> = args
        .shards
        .split(',')
        .enumerate()
        .map(|(i, addr)| ShardInfo {
            id: format!("shard-{}", i),
            address: addr.trim().to_string(),
            healthy: true,
        })
        .collect();

    info!("Configured {} shards:", shards.len());
    for shard in &shards {
        info!("  {} -> {}", shard.id, shard.address);
    }

    // Create coordinator service with backpressure config
    let timeout = Duration::from_millis(args.timeout);
    let backpressure_config = BackpressureConfig {
        max_concurrent: args.max_concurrent,
        rate_limit_rps: args.rate_limit,
        max_queue_depth: args.max_queue_depth,
        ..Default::default()
    };
    info!(
        "Backpressure config: max_concurrent={}, rate_limit={} RPS, max_queue={}",
        args.max_concurrent,
        if args.rate_limit == 0 {
            "unlimited".to_string()
        } else {
            args.rate_limit.to_string()
        },
        args.max_queue_depth
    );
    let service = CoordinatorService::new(
        shards,
        timeout,
        args.pool_size,
        backpressure_config,
        args.listen.clone(),
    );

    // Initialize connection pools to all shards
    info!("Initializing connection pools...");
    if let Err(e) = service.fanout.init_pools().await {
        warn!("Failed to initialize some connection pools: {}", e);
    }
    let stats = service.fanout.pool_stats();
    info!(
        "Connection pools ready: {} pools, {} total connections",
        stats.total_pools, stats.total_connections
    );

    // Parse listen address
    let addr: SocketAddr = args.listen.parse()?;
    info!("Starting gRPC server on {}", addr);

    // Start metrics HTTP server if enabled
    if args.metrics_port > 0 {
        let metrics_addr: SocketAddr = format!("0.0.0.0:{}", args.metrics_port).parse()?;
        info!("Starting metrics server on {}", metrics_addr);

        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(metrics_addr).await {
                warn!("Metrics server error: {}", e);
            }
        });
    }

    // Start gRPC server
    Server::builder()
        .add_service(AkidbServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

/// Handle metrics HTTP requests
async fn handle_metrics_request(
    req: HyperRequest<hyper::body::Incoming>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let metrics = export_metrics();
            Ok(HyperResponse::builder()
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from(metrics)))
                .unwrap())
        }
        (&Method::GET, "/health") => Ok(HyperResponse::builder()
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(r#"{"status":"ok"}"#)))
            .unwrap()),
        _ => Ok(HyperResponse::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

/// Run the metrics HTTP server
async fn run_metrics_server(
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    info!("Metrics server listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(handle_metrics_request))
                .await
            {
                debug!("Metrics connection error: {:?}", err);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_proto::Query;
    use tonic::Code;

    fn test_service() -> CoordinatorService {
        CoordinatorService::new(
            vec![],
            Duration::from_millis(10),
            1,
            BackpressureConfig::default(),
            "127.0.0.1:0".to_string(),
        )
    }

    fn search_request(top_k: u32, nprobe: Option<u32>) -> SearchRequest {
        SearchRequest {
            collection: "test".to_string(),
            query: vec![0.0; 4],
            top_k,
            nprobe,
            filter: vec![],
            tag_filter: None,
            score_threshold: None,
            group_by: String::new(),
            group_size: None,
        }
    }

    #[test]
    fn test_apply_score_and_group_forwards_threshold_and_parent_group() {
        let results = vec![
            ProtoSearchResult {
                id: "a1".into(),
                score: 0.95,
                metadata: r#"{"parent_id":"p1"}"#.into(),
            },
            ProtoSearchResult {
                id: "a2".into(),
                score: 0.90,
                metadata: r#"{"parent_id":"p1"}"#.into(),
            },
            ProtoSearchResult {
                id: "b1".into(),
                score: 0.40,
                metadata: r#"{"parent_id":"p2"}"#.into(),
            },
            ProtoSearchResult {
                id: "c1".into(),
                score: 0.85,
                metadata: r#"{"parent_id":"p3"}"#.into(),
            },
        ];
        let out = CoordinatorService::apply_score_and_group(
            results,
            10,
            Some(0.5),
            "parent_id",
            Some(1),
        );
        assert!(out.iter().all(|r| r.score >= 0.5));
        assert!(!out.iter().any(|r| r.id == "b1"));
        let parents: std::collections::HashSet<_> = out
            .iter()
            .map(|r| {
                let v: serde_json::Value = serde_json::from_str(&r.metadata).unwrap();
                v["parent_id"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(parents.len(), out.len());
        assert!(out.iter().any(|r| r.id == "a1" || r.id == "a2"));
        assert!(out.iter().any(|r| r.id == "c1"));
    }

    fn search_batch_request(top_k: u32, nprobe: Option<u32>) -> SearchBatchRequest {
        SearchBatchRequest {
            collection: "test".to_string(),
            queries: vec![],
            top_k,
            nprobe,
        }
    }

    fn batch_vector(id: &str) -> Vector {
        Vector {
            id: id.to_string(),
            embedding: vec![0.0; 4],
            metadata: vec![],
            text: String::new(),
        }
    }

    fn insert_request(id: &str, vector: Vec<f32>) -> InsertRequest {
        InsertRequest {
            collection: "test".to_string(),
            id: id.to_string(),
            vector,
            metadata: vec![],
            text: String::new(),
        }
    }

    fn update_request(id: &str, vector: Vec<f32>) -> UpdateRequest {
        UpdateRequest {
            collection: "test".to_string(),
            id: id.to_string(),
            vector,
            metadata: vec![],
        }
    }

    fn get_request(id: &str) -> GetRequest {
        GetRequest {
            collection: "test".to_string(),
            id: id.to_string(),
        }
    }

    #[tokio::test]
    async fn test_insert_rejects_empty_id_before_routing() {
        let service = test_service();
        let result = service
            .insert(Request::new(insert_request("", vec![0.0; 4])))
            .await;

        let status = result.expect_err("empty vector ID should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Vector ID cannot be empty"));
    }

    #[tokio::test]
    async fn test_get_rejects_empty_id_before_routing() {
        let service = test_service();
        let result = service.get(Request::new(get_request(""))).await;

        let status = result.expect_err("empty vector ID should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Vector ID cannot be empty"));
    }

    #[tokio::test]
    async fn test_update_rejects_nan_vector_before_fanout() {
        let service = test_service();
        let result = service
            .update(Request::new(update_request("doc1", vec![0.0, f32::NAN])))
            .await;

        let status = result.expect_err("NaN update vector should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("NaN"));
    }

    #[tokio::test]
    async fn test_search_rejects_zero_top_k_before_fanout() {
        let service = test_service();
        let result = service.search(Request::new(search_request(0, None))).await;

        let status = result.expect_err("zero top_k should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("top_k"));
    }

    #[tokio::test]
    async fn test_search_rejects_top_k_above_limit_before_fanout() {
        let service = test_service();
        let result = service
            .search(Request::new(search_request(10_001, None)))
            .await;

        let status = result.expect_err("oversized top_k should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("top_k"));
    }

    #[tokio::test]
    async fn test_search_rejects_zero_nprobe_before_fanout() {
        let service = test_service();
        let result = service
            .search(Request::new(search_request(1, Some(0))))
            .await;

        let status = result.expect_err("zero nprobe should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("nprobe"));
    }

    #[tokio::test]
    async fn test_search_rejects_nan_query_before_fanout() {
        let service = test_service();
        let mut request = search_request(1, None);
        request.query = vec![0.0, f32::NAN, 1.0];

        let result = service.search(Request::new(request)).await;

        let status = result.expect_err("NaN query should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status
            .message()
            .contains("Query vector contains NaN or Infinity values"));
    }

    #[tokio::test]
    async fn test_search_batch_rejects_zero_top_k_even_when_empty() {
        let service = test_service();
        let result = service
            .search_batch(Request::new(search_batch_request(0, None)))
            .await;

        let status = result.expect_err("zero top_k should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("top_k"));
    }

    #[tokio::test]
    async fn test_search_batch_rejects_zero_nprobe_even_when_empty() {
        let service = test_service();
        let result = service
            .search_batch(Request::new(search_batch_request(1, Some(0))))
            .await;

        let status = result.expect_err("zero nprobe should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("nprobe"));
    }

    #[tokio::test]
    async fn test_search_batch_rejects_empty_query_before_fanout() {
        let service = test_service();
        let result = service
            .search_batch(Request::new(SearchBatchRequest {
                collection: "test".to_string(),
                queries: vec![Query { vector: vec![] }],
                top_k: 1,
                nprobe: None,
            }))
            .await;

        let status = result.expect_err("empty batch query should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Query vector cannot be empty"));
    }

    #[test]
    fn test_search_result_metadata_serializes_json_value() {
        let metadata =
            CoordinatorService::search_result_metadata(Some(serde_json::json!({"tenant": "a"})));

        assert_eq!(metadata, r#"{"tenant":"a"}"#);
    }

    #[test]
    fn test_search_result_metadata_empty_when_missing() {
        assert_eq!(CoordinatorService::search_result_metadata(None), "");
    }

    #[tokio::test]
    async fn test_insert_batch_rejects_duplicate_ids_before_routing() {
        let service = test_service();
        let result = service
            .insert_batch(Request::new(InsertBatchRequest {
                collection: "test".to_string(),
                vectors: vec![batch_vector("dup"), batch_vector("dup")],
            }))
            .await;

        let status = result.expect_err("duplicate batch IDs should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("duplicate"));
    }

    #[tokio::test]
    async fn test_insert_batch_marks_unrouted_vectors_failed() {
        let service = test_service();
        let response = service
            .insert_batch(Request::new(InsertBatchRequest {
                collection: "test".to_string(),
                vectors: vec![batch_vector("doc1")],
            }))
            .await
            .expect("unrouted batch should return a failed batch response")
            .into_inner();

        assert!(!response.success);
        assert_eq!(response.inserted_count, 0);
        assert_eq!(response.failed_ids, vec!["doc1"]);
    }

    #[test]
    fn test_accumulate_shard_insert_response_accepts_consistent_accounting() {
        let vector_ids = vec!["a".to_string(), "b".to_string()];
        let mut total_inserted = 0;
        let mut failed_ids = Vec::new();

        CoordinatorService::accumulate_shard_insert_response(
            &vector_ids,
            InsertBatchResponse {
                success: false,
                inserted_count: 1,
                failed_ids: vec!["b".to_string()],
            },
            &mut total_inserted,
            &mut failed_ids,
        );

        assert_eq!(total_inserted, 1);
        assert_eq!(failed_ids, vec!["b"]);
    }

    #[test]
    fn test_accumulate_shard_insert_response_rejects_unknown_failed_id() {
        let vector_ids = vec!["a".to_string(), "b".to_string()];
        let mut total_inserted = 0;
        let mut failed_ids = Vec::new();

        CoordinatorService::accumulate_shard_insert_response(
            &vector_ids,
            InsertBatchResponse {
                success: false,
                inserted_count: 1,
                failed_ids: vec!["missing".to_string()],
            },
            &mut total_inserted,
            &mut failed_ids,
        );

        assert_eq!(total_inserted, 0);
        assert_eq!(failed_ids, vector_ids);
    }

    #[test]
    fn test_accumulate_shard_insert_response_rejects_inconsistent_counts() {
        let vector_ids = vec!["a".to_string(), "b".to_string()];
        let mut total_inserted = 0;
        let mut failed_ids = Vec::new();

        CoordinatorService::accumulate_shard_insert_response(
            &vector_ids,
            InsertBatchResponse {
                success: true,
                inserted_count: 1,
                failed_ids: vec![],
            },
            &mut total_inserted,
            &mut failed_ids,
        );

        assert_eq!(total_inserted, 0);
        assert_eq!(failed_ids, vector_ids);
    }
}
