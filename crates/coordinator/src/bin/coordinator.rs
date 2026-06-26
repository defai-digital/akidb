//! AkiDB Coordinator Server
//!
//! This binary runs the coordinator service that fans out searches
//! to all shard nodes and merges results.

use akidb_coordinator::{
    coordinator_metrics, export_metrics, BackpressureConfig, BackpressureController,
    ConsistencyTracker, FanoutExecutor, ShardInfo, ShardRouter,
};
use akidb_grpc::proto::akidb_server::{Akidb, AkidbServer};
use akidb_grpc::proto::{
    ClusterMetrics, CoordinatorNode, DeleteRequest, DeleteResponse, DeleteStatus,
    GetClusterStateRequest, GetClusterStateResponse, GetRequest, GetResponse, HealthRequest,
    HealthResponse, InsertBatchRequest, InsertBatchResponse, InsertRequest, InsertResponse,
    NodeStatus, SearchBatchRequest, SearchBatchResponse, SearchRequest, SearchResponse,
    SearchResult as ProtoSearchResult, ShardNode, UpdateRequest, UpdateResponse, UpdateStatus,
    VisibilityInfo,
};
use clap::Parser;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request as HyperRequest, Response as HyperResponse};
use hyper_util::rt::TokioIo;
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
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// gRPC listen address
    #[arg(short, long, default_value = "0.0.0.0:50050")]
    listen: String,

    /// Shard addresses (comma-separated)
    #[arg(short, long, default_value = "192.168.1.61:50051,192.168.1.62:50051")]
    shards: String,

    /// Search timeout in milliseconds
    #[arg(short, long, default_value = "5000")]
    timeout: u64,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Connection pool size per shard
    #[arg(short = 'p', long, default_value = "4")]
    pool_size: usize,

    /// Metrics HTTP port (0 to disable)
    #[arg(short = 'm', long, default_value = "9090")]
    metrics_port: u16,

    /// Maximum concurrent requests (backpressure)
    #[arg(long, default_value = "1000")]
    max_concurrent: usize,

    /// Rate limit (requests per second, 0 = unlimited)
    #[arg(long, default_value = "0")]
    rate_limit: u64,

    /// Maximum queue depth for waiting requests
    #[arg(long, default_value = "5000")]
    max_queue_depth: usize,
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
        let fanout = Arc::new(FanoutExecutor::with_pool_size(router.clone(), timeout, pool_size));
        let consistency = Arc::new(ConsistencyTracker::new());
        let backpressure = Arc::new(BackpressureController::with_config(backpressure_config));
        let local_id = format!("coord-{}", &local_address.replace([':', '.'], "-"));

        Self {
            fanout,
            router,
            consistency,
            backpressure,
            local_address,
            local_id,
        }
    }
}

#[tonic::async_trait]
impl Akidb for CoordinatorService {
    async fn insert(&self, request: Request<InsertRequest>) -> Result<Response<InsertResponse>, Status> {
        // Route insert to the appropriate shard based on vector ID
        let req = request.into_inner();
        let router = self.router.read().await;

        let shard = router
            .route(&akidb_common::VectorId::new(&req.id))
            .ok_or_else(|| Status::unavailable("No shards available"))?;
        let shard_id = shard.id.clone();
        let shard_address = shard.address.clone();
        drop(router);

        // FIX BUG-HUNT-401: Use connection pool instead of creating new TCP connection per request
        // Previously created new connections which caused connection exhaustion under load
        let mut client = self.fanout.get_shard_client(&shard_address)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to get shard client: {}", e)))?;

        let id_clone = req.id.clone();
        let response = client
            .insert(InsertRequest {
                collection: req.collection,
                id: req.id,
                vector: req.vector,
                metadata: req.metadata,
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

    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

        // Apply backpressure
        let _guard = self.backpressure.try_acquire().await.map_err(|e| {
            coordinator_metrics().record_request("search", "rejected");
            Status::resource_exhausted(format!("Backpressure: {}", e))
        })?;

        // Fan-out search to all shards
        let result = self
            .fanout
            .search(&req.query, req.top_k as usize, req.nprobe.unwrap_or(32))
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
        coordinator_metrics().record_fanout(latency_secs, coverage as f64, responding_count, partial);
        coordinator_metrics().record_request("search", "success");

        // Convert results to proto format
        let results: Vec<ProtoSearchResult> = result
            .results
            .into_iter()
            .map(|r| ProtoSearchResult {
                id: r.id.as_str().to_string(),
                score: r.score,
                metadata: String::new(),
            })
            .collect();

        Ok(Response::new(SearchResponse {
            results,
            partial,
            missing_shards,
            coverage,
            latency_us,
            within_slo: latency_us < 10_000, // 10ms SLO
            degraded_mode: partial,
        }))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

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

    async fn update(&self, request: Request<UpdateRequest>) -> Result<Response<UpdateResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();
        let id_clone = req.id.clone();

        // Broadcast update: delete from all shards, then insert to correct shard
        let result = self
            .fanout
            .broadcast_update(&req.collection, &req.id, req.vector)
            .await
            .map_err(|e| {
                coordinator_metrics().record_request("update", "error");
                Status::internal(format!("Broadcast update failed: {}", e))
            })?;

        // FIX BUG-HUNT-502: Add consistency tracking for update operations
        // Previously, update had no consistency tracking while insert and delete did.
        // This caused read-your-writes failures after updates - clients would see
        // stale data when reading immediately after updating.
        self.consistency.record_write(&id_clone, &result.target_shard);
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
        let mut client = self.fanout.get_shard_client(&shard_address)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to get shard client: {}", e)))?;

        let response = client.get(req).await?;
        Ok(response)
    }

    async fn health(&self, _request: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
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
            using_gpu: true,
        }))
    }

    async fn insert_batch(
        &self,
        request: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertBatchResponse>, Status> {
        let req = request.into_inner();
        let router = self.router.read().await;

        // Partition vectors by shard using consistent hashing
        let mut shard_batches: std::collections::HashMap<String, Vec<akidb_grpc::proto::Vector>> =
            std::collections::HashMap::new();

        for vector in req.vectors {
            let id = akidb_common::VectorId::new(&vector.id);
            if let Some(shard) = router.route(&id) {
                shard_batches
                    .entry(shard.id.clone())
                    .or_default()
                    .push(vector);
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

        for (shard_id, vectors) in shard_batches {
            let addr = match shard_addrs.get(&shard_id) {
                Some(a) => a.clone(),
                None => continue,
            };
            let coll = collection.clone();
            // FIX BUG-HUNT-402: Capture vector IDs before moving vectors into the task
            let vector_ids: Vec<String> = vectors.iter().map(|v| v.id.clone()).collect();
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
        let mut all_failed_ids = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((Ok(response), _vector_ids)) => {
                    let inner = response.into_inner();
                    total_inserted += inner.inserted_count;
                    all_failed_ids.extend(inner.failed_ids);
                }
                Ok((Err(e), vector_ids)) => {
                    // FIX BUG-HUNT-402: Track all vector IDs from failed shard as failed
                    // Previously these were silently lost with only a warning logged
                    warn!("Shard batch insert failed: {} - marking {} vectors as failed", e, vector_ids.len());
                    all_failed_ids.extend(vector_ids);
                }
                Err(e) => {
                    warn!("Task join error: {}", e);
                    // Note: We can't recover vector_ids here since the task panicked
                    // This is a rare edge case (task panic) where data loss is unavoidable
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
                };
                self.search(Request::new(search_req))
            })
            .collect();

        // Execute all searches in parallel
        let responses: Vec<Response<SearchResponse>> = futures::future::try_join_all(search_futures).await?;
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

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
        if args.rate_limit == 0 { "unlimited".to_string() } else { args.rate_limit.to_string() },
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
        (&Method::GET, "/health") => {
            Ok(HyperResponse::builder()
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(r#"{"status":"ok"}"#)))
                .unwrap())
        }
        _ => Ok(HyperResponse::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

/// Run the metrics HTTP server
async fn run_metrics_server(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
