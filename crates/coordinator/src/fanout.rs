//! Fan-out search execution with connection pooling

use crate::merger::ResultMerger;
use crate::router::{ShardInfo, ShardRouter};
use akidb_common::{AkiDbError, Result, SearchResult, VectorId};
use akidb_grpc::proto::akidb_client::AkidbClient;
use akidb_grpc::proto::{
    DeleteRequest, DeleteStatus, SearchRequest, SearchResult as ProtoSearchResult, TagFilter,
    UpdateRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

/// Connection pool for a single shard
struct ShardConnectionPool {
    /// Multiple clients for concurrent requests
    clients: Vec<AkidbClient<Channel>>,
    /// Round-robin index
    next_idx: std::sync::atomic::AtomicUsize,
    /// FIX BUG-H018: Track last successful use AND last access separately
    /// last_used: Updated when pool is actually used for an operation
    /// last_accessed: Updated whenever pool is retrieved (even if operation fails)
    last_used: parking_lot::Mutex<Instant>,
    /// Time pool was created or last accessed (for idle cleanup)
    last_accessed: parking_lot::Mutex<Instant>,
}

/// FIX BUG-057: Track failed pool creation attempts for backoff
/// FIX BUG-095: Combined failure tracking into single struct with one mutex
/// to avoid multiple mutex acquisitions and potential inefficiency
struct PoolCreationTracker {
    /// Combined failure state for each address
    state: parking_lot::Mutex<std::collections::HashMap<String, FailureState>>,
}

fn fanout_top_k(top_k: usize) -> Result<u32> {
    u32::try_from(top_k).map_err(|_| {
        AkiDbError::CoordinatorError(format!("Search top_k {} exceeds u32 range", top_k))
    })
}

fn shard_search_request(
    collection: String,
    query: Vec<f32>,
    top_k: u32,
    nprobe: u32,
    filter: Vec<u8>,
    tag_filter: Option<TagFilter>,
) -> SearchRequest {
    SearchRequest {
        collection,
        query,
        top_k,
        nprobe: Some(nprobe),
        filter,
        tag_filter,
    }
}

fn shard_update_request(
    collection: String,
    id: String,
    vector: Vec<f32>,
    metadata: Vec<u8>,
) -> UpdateRequest {
    UpdateRequest {
        collection,
        id,
        vector,
        metadata,
    }
}

fn shard_search_result(result: ProtoSearchResult) -> SearchResult {
    let mut out = SearchResult::new(VectorId::new(result.id), result.score);
    if !result.metadata.is_empty() {
        out.metadata = Some(
            serde_json::from_str(&result.metadata)
                .unwrap_or(serde_json::Value::String(result.metadata)),
        );
    }
    out
}

/// Failure state for a single address
#[derive(Clone)]
struct FailureState {
    /// Last failure time
    last_failure: Instant,
    /// Consecutive failure count
    count: u32,
}

impl PoolCreationTracker {
    fn new() -> Self {
        Self {
            state: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Check if we should retry pool creation (returns backoff duration if too soon)
    fn should_backoff(&self, address: &str) -> Option<Duration> {
        // FIX BUG-095: Single mutex acquisition instead of two
        let state = self.state.lock();

        let failure = state.get(address)?;
        if failure.count == 0 {
            return None;
        }

        let elapsed = failure.last_failure.elapsed();

        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 30s
        let backoff = Duration::from_secs(1 << failure.count.min(4)).min(Duration::from_secs(30));

        if elapsed < backoff {
            Some(backoff - elapsed)
        } else {
            None
        }
    }

    /// Record a pool creation failure
    fn record_failure(&self, address: &str) {
        // FIX BUG-095: Single mutex acquisition instead of two
        let mut state = self.state.lock();

        let entry = state.entry(address.to_string()).or_insert(FailureState {
            last_failure: Instant::now(),
            count: 0,
        });
        entry.count = (entry.count + 1).min(10); // Cap at 10 to prevent overflow
        entry.last_failure = Instant::now();
    }

    /// Record a successful pool creation (reset backoff)
    fn record_success(&self, address: &str) {
        // FIX BUG-095: Single mutex acquisition instead of two
        let mut state = self.state.lock();
        state.remove(address);
    }
}

impl ShardConnectionPool {
    async fn new(address: &str, pool_size: usize) -> Result<Self> {
        let endpoint = format!("http://{}", address);
        let mut clients = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let channel = Endpoint::from_shared(endpoint.clone())
                .map_err(|e| AkiDbError::CoordinatorError(format!("Invalid endpoint: {}", e)))?
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .connect()
                .await
                .map_err(|e| AkiDbError::CoordinatorError(format!("Connection failed: {}", e)))?;

            clients.push(AkidbClient::new(channel));
        }

        let now = Instant::now();
        Ok(Self {
            clients,
            next_idx: std::sync::atomic::AtomicUsize::new(0),
            last_used: parking_lot::Mutex::new(now),
            last_accessed: parking_lot::Mutex::new(now),
        })
    }

    fn get_client(&self) -> AkidbClient<Channel> {
        // FIX BUG-H018: Update last_accessed when pool is retrieved
        *self.last_accessed.lock() = Instant::now();
        let idx = self
            .next_idx
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.clients.len();
        self.clients[idx].clone()
    }

    /// Update the last used timestamp (call on successful operations)
    fn mark_used(&self) {
        let now = Instant::now();
        *self.last_used.lock() = now;
        *self.last_accessed.lock() = now;
    }

    /// FIX BUG-H018: Get the last accessed timestamp (for idle cleanup)
    /// This is updated whenever the pool is retrieved, regardless of operation success
    fn last_accessed_time(&self) -> Instant {
        *self.last_accessed.lock()
    }

    /// Get the last successful use timestamp (for health tracking)
    #[allow(dead_code)]
    fn last_used_time(&self) -> Instant {
        *self.last_used.lock()
    }
}

/// Fan-out search executor with connection pooling
pub struct FanoutExecutor {
    router: Arc<RwLock<ShardRouter>>,
    timeout: Duration,
    /// Connection pools per shard address
    pools: dashmap::DashMap<String, Arc<ShardConnectionPool>>,
    /// Pool size per shard
    pool_size: usize,
    /// FIX BUG-057: Track pool creation failures for backoff
    pool_tracker: PoolCreationTracker,
}

impl FanoutExecutor {
    pub fn new(router: Arc<RwLock<ShardRouter>>, timeout: Duration) -> Self {
        Self::with_pool_size(router, timeout, 4)
    }

    pub fn with_pool_size(
        router: Arc<RwLock<ShardRouter>>,
        timeout: Duration,
        pool_size: usize,
    ) -> Self {
        // Ensure pool_size is at least 1 to prevent division by zero
        let pool_size = pool_size.max(1);
        Self {
            router,
            timeout,
            pools: dashmap::DashMap::new(),
            pool_size,
            pool_tracker: PoolCreationTracker::new(),
        }
    }

    /// Get or create a connection pool for a shard
    async fn get_pool(&self, address: &str) -> Result<Arc<ShardConnectionPool>> {
        // Fast path: check if pool exists
        if let Some(pool) = self.pools.get(address) {
            return Ok(pool.clone());
        }

        // FIX BUG-057: Check backoff before attempting pool creation
        if let Some(remaining) = self.pool_tracker.should_backoff(address) {
            return Err(AkiDbError::CoordinatorError(format!(
                "Pool creation for {} in backoff, retry in {:.1}s",
                address,
                remaining.as_secs_f64()
            )));
        }

        // Slow path: use entry API to avoid race condition where two threads
        // both create pools for the same address
        use dashmap::mapref::entry::Entry;

        // We need to create the pool outside of the entry API since it's async
        // Use a lock-like approach with a placeholder
        let address_str = address.to_string();

        // Check again after acquiring potential entry
        if let Some(pool) = self.pools.get(&address_str) {
            return Ok(pool.clone());
        }

        // Create new pool - this might race, but entry API will ensure only one wins
        info!(
            "Creating connection pool for {} (size={})",
            address, self.pool_size
        );
        let new_pool = match ShardConnectionPool::new(address, self.pool_size).await {
            Ok(pool) => {
                // FIX BUG-057: Reset backoff on success
                self.pool_tracker.record_success(address);
                Arc::new(pool)
            }
            Err(e) => {
                // FIX BUG-057: Record failure for exponential backoff
                self.pool_tracker.record_failure(address);
                return Err(e);
            }
        };

        // Use entry API to ensure atomicity - if another thread created it, use theirs
        match self.pools.entry(address_str) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                entry.insert(new_pool.clone());
                Ok(new_pool)
            }
        }
    }

    /// Initialize connection pools for all shards (call at startup)
    pub async fn init_pools(&self) -> Result<()> {
        let router = self.router.read().await;
        let shards = router.all_shards().to_vec();
        drop(router);

        for shard in shards {
            if let Err(e) = self.get_pool(&shard.address).await {
                warn!("Failed to initialize pool for {}: {}", shard.address, e);
            }
        }
        Ok(())
    }

    /// Execute a fan-out search across all healthy shards
    ///
    /// Returns results from all shards that responded, along with
    /// information about any missing shards.
    pub async fn search(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        nprobe: u32,
        filter: &[u8],
        tag_filter: Option<TagFilter>,
    ) -> Result<FanoutResult> {
        let request_top_k = fanout_top_k(top_k)?;
        let router = self.router.read().await;
        let shards: Vec<ShardInfo> = router
            .healthy_shards()
            .iter()
            .map(|s| (*s).clone())
            .collect();
        drop(router);

        if shards.is_empty() {
            return Err(AkiDbError::CoordinatorError(
                "No healthy shards available".to_string(),
            ));
        }

        let total_shards = shards.len();
        debug!("Fan-out search to {} shards", total_shards);

        // Get connection pools for all shards
        let mut shard_pools = Vec::with_capacity(shards.len());
        let mut missing_shards = Vec::new();
        for shard in &shards {
            match self.get_pool(&shard.address).await {
                Ok(pool) => shard_pools.push((shard.clone(), pool)),
                Err(e) => {
                    warn!("Failed to get pool for {}: {}", shard.address, e);
                    missing_shards.push(shard.id.clone());
                }
            }
        }

        // Launch parallel searches using pooled connections
        let collection = collection.to_string();
        let query_vec: Vec<f32> = query.to_vec();
        let filter = filter.to_vec();
        // FIX BUG-071: Track shard IDs separately to identify failures even on panic
        let mut handles = Vec::with_capacity(shard_pools.len());
        let mut handle_shard_ids = Vec::with_capacity(shard_pools.len());

        for (shard, pool) in shard_pools {
            let collection = collection.clone();
            let query_clone = query_vec.clone();
            let filter = filter.clone();
            let tag_filter = tag_filter.clone();
            let timeout = self.timeout;
            let shard_id = shard.id.clone();
            handle_shard_ids.push(shard_id.clone()); // Track shard ID outside async block

            handles.push(tokio::spawn(async move {
                // Get client from pool (no connection overhead!)
                let mut client = pool.get_client();

                let request = shard_search_request(
                    collection,
                    query_clone,
                    request_top_k,
                    nprobe,
                    filter,
                    tag_filter,
                );

                let search_result = tokio::time::timeout(timeout, client.search(request)).await;

                match search_result {
                    Ok(Ok(response)) => {
                        // Mark pool as healthy on successful response
                        pool.mark_used();
                        let results: Vec<SearchResult> = response
                            .into_inner()
                            .results
                            .into_iter()
                            .map(shard_search_result)
                            .collect();
                        Ok((shard_id, results))
                    }
                    Ok(Err(e)) => Err((shard_id, format!("Search failed: {}", e))),
                    Err(_) => Err((shard_id, "Search timeout".to_string())),
                }
            }));
        }

        // Collect results
        let mut merger = ResultMerger::new(top_k);
        let mut responding_shards = Vec::new();

        // FIX BUG-071: Iterate with index to identify shard even on task panic
        for (idx, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok((shard_id, results))) => {
                    debug!("Shard {} returned {} results", shard_id, results.len());
                    responding_shards.push(shard_id);
                    merger.add_results(results);
                }
                Ok(Err((shard_id, error))) => {
                    warn!("Shard {} failed: {}", shard_id, error);

                    // Update shard health
                    let mut router = self.router.write().await;
                    router.update_health(&shard_id, false);

                    missing_shards.push(shard_id);
                }
                Err(e) => {
                    // FIX BUG-071: Now we can identify which shard's task panicked
                    let shard_id = &handle_shard_ids[idx];
                    warn!(
                        "Task for shard {} panicked or was cancelled: {}",
                        shard_id, e
                    );

                    // Update shard health since it's in an unknown state
                    let mut router = self.router.write().await;
                    router.update_health(shard_id, false);

                    missing_shards.push(shard_id.clone());
                }
            }
        }

        let results = merger.finish();
        info!(
            "Fan-out complete: {} results from {}/{} shards",
            results.len(),
            responding_shards.len(),
            total_shards
        );

        Ok(FanoutResult {
            results,
            responding_shards,
            missing_shards,
            total_shards,
        })
    }

    /// Clean up stale connection pools (call periodically)
    ///
    /// FIX BUG-H018: Uses last_accessed_time instead of last_healthy_time
    /// This ensures idle-but-recently-accessed pools are not prematurely removed.
    /// Pools are considered stale if they haven't been accessed in max_age time.
    pub fn cleanup_stale_pools(&self, max_age: Duration) {
        let now = Instant::now();
        self.pools
            .retain(|_, pool| now.duration_since(pool.last_accessed_time()) < max_age);
    }

    /// Get connection pool statistics
    pub fn pool_stats(&self) -> PoolStats {
        let total_pools = self.pools.len();
        let total_connections = self.pools.iter().map(|p| p.clients.len()).sum();
        PoolStats {
            total_pools,
            total_connections,
            pool_size: self.pool_size,
        }
    }

    /// Broadcast delete to all shards
    ///
    /// This is used when we don't know which shard contains the vector,
    /// or for ensuring consistent deletion across all shards.
    pub async fn broadcast_delete(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<BroadcastDeleteResult> {
        let router = self.router.read().await;
        let shards: Vec<ShardInfo> = router.all_shards().to_vec();
        drop(router);

        if shards.is_empty() {
            return Err(AkiDbError::CoordinatorError(
                "No shards available".to_string(),
            ));
        }

        debug!("Broadcasting delete for {} to {} shards", id, shards.len());

        // FIX BUG-071: Track shard IDs separately to identify failures even on panic
        let mut handles = Vec::with_capacity(shards.len());
        let mut handle_shard_ids = Vec::with_capacity(shards.len());

        for shard in &shards {
            let pool = match self.get_pool(&shard.address).await {
                Ok(p) => p,
                Err(e) => {
                    warn!("Failed to get pool for {}: {}", shard.address, e);
                    continue;
                }
            };

            let shard_id = shard.id.clone();
            handle_shard_ids.push(shard_id.clone()); // Track shard ID outside async block
            let collection = collection.to_string();
            let id = id.to_string();
            let timeout = self.timeout;

            handles.push(tokio::spawn(async move {
                let mut client = pool.get_client();
                let request = DeleteRequest {
                    collection,
                    id: id.clone(),
                };

                let result = tokio::time::timeout(timeout, client.delete(request)).await;

                match result {
                    Ok(Ok(response)) => {
                        let resp = response.into_inner();
                        Ok((shard_id, resp))
                    }
                    Ok(Err(e)) => Err((shard_id, format!("Delete failed: {}", e))),
                    Err(_) => Err((shard_id, "Delete timeout".to_string())),
                }
            }));
        }

        // Collect results
        let mut found_on_shard: Option<String> = None;
        let mut responding_shards = Vec::new();
        let mut failed_shards = Vec::new();
        let mut overall_status = DeleteStatus::NotFound;

        // FIX BUG-071: Iterate with index to identify shard even on task panic
        for (idx, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok((shard_id, response))) => {
                    responding_shards.push(shard_id.clone());

                    // If any shard reports DELETED, the overall result is DELETED
                    if response.status == DeleteStatus::Deleted as i32 {
                        overall_status = DeleteStatus::Deleted;
                        found_on_shard = Some(shard_id);
                    } else if response.status == DeleteStatus::AlreadyDeleted as i32
                        && overall_status != DeleteStatus::Deleted
                    {
                        overall_status = DeleteStatus::AlreadyDeleted;
                    }
                }
                Ok(Err((shard_id, error))) => {
                    warn!("Shard {} delete failed: {}", shard_id, error);
                    failed_shards.push(shard_id);
                }
                Err(e) => {
                    // FIX BUG-071: Now we can identify which shard's task panicked
                    let shard_id = &handle_shard_ids[idx];
                    warn!(
                        "Task for shard {} panicked during broadcast delete: {}",
                        shard_id, e
                    );
                    failed_shards.push(shard_id.clone());
                }
            }
        }

        info!(
            "Broadcast delete complete: status={:?}, found_on={:?}, {}/{} shards responded",
            overall_status,
            found_on_shard,
            responding_shards.len(),
            shards.len()
        );

        Ok(BroadcastDeleteResult {
            status: overall_status,
            found_on_shard,
            responding_shards,
            failed_shards,
        })
    }

    /// Broadcast update (insert to correct shard first, then delete from all shards)
    ///
    /// This uses insert-first-then-delete pattern for safety:
    /// - If insert fails: old data is preserved (no data loss)
    /// - If insert succeeds but delete fails: we have duplicate data (recoverable)
    pub async fn broadcast_update(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        metadata: Vec<u8>,
    ) -> Result<BroadcastUpdateResult> {
        // First, route and insert to correct shard
        let vector_id = VectorId::new(id);
        let router = self.router.read().await;
        let shard = router
            .route(&vector_id)
            .ok_or_else(|| AkiDbError::CoordinatorError("No shards available".to_string()))?;
        let shard_address = shard.address.clone();
        let shard_id = shard.id.clone();
        drop(router);

        let pool = self.get_pool(&shard_address).await?;
        let mut client = pool.get_client();

        let request =
            shard_update_request(collection.to_string(), id.to_string(), vector, metadata);

        let result = tokio::time::timeout(self.timeout, client.update(request)).await;

        let update_success = match result {
            Ok(Ok(response)) => {
                let resp = response.into_inner();
                resp.success
            }
            Ok(Err(e)) => {
                warn!("Update failed on shard {}: {}", shard_id, e);
                false
            }
            Err(_) => {
                warn!("Update timeout on shard {}", shard_id);
                false
            }
        };

        // Only delete from other shards if insert succeeded
        // This ensures we don't lose data on insert failure
        let delete_result = if update_success {
            self.broadcast_delete_except(collection, id, &shard_id)
                .await?
        } else {
            // Return empty delete result on insert failure - preserves old data
            BroadcastDeleteResult {
                status: DeleteStatus::NotFound,
                found_on_shard: None,
                responding_shards: vec![],
                failed_shards: vec![],
            }
        };

        Ok(BroadcastUpdateResult {
            delete_result,
            update_success,
            target_shard: shard_id,
        })
    }

    /// Broadcast delete to all shards except the specified one
    ///
    /// Used during updates to clean up old data from other shards.
    async fn broadcast_delete_except(
        &self,
        collection: &str,
        id: &str,
        except_shard: &str,
    ) -> Result<BroadcastDeleteResult> {
        let router = self.router.read().await;
        let shards: Vec<ShardInfo> = router
            .all_shards()
            .iter()
            .filter(|s| s.id != except_shard)
            .cloned()
            .collect();
        drop(router);

        if shards.is_empty() {
            return Ok(BroadcastDeleteResult {
                status: DeleteStatus::NotFound,
                found_on_shard: None,
                responding_shards: vec![],
                failed_shards: vec![],
            });
        }

        debug!(
            "Broadcasting delete for {} to {} shards (excluding {})",
            id,
            shards.len(),
            except_shard
        );

        // FIX BUG-071: Track shard IDs separately to identify failures even on panic
        let mut handles = Vec::with_capacity(shards.len());
        let mut handle_shard_ids = Vec::with_capacity(shards.len());

        for shard in &shards {
            let pool = match self.get_pool(&shard.address).await {
                Ok(p) => p,
                Err(e) => {
                    warn!("Failed to get pool for {}: {}", shard.address, e);
                    continue;
                }
            };

            let shard_id = shard.id.clone();
            handle_shard_ids.push(shard_id.clone()); // Track shard ID outside async block
            let collection = collection.to_string();
            let id = id.to_string();
            let timeout = self.timeout;

            handles.push(tokio::spawn(async move {
                let mut client = pool.get_client();
                let request = DeleteRequest {
                    collection,
                    id: id.clone(),
                };

                let result = tokio::time::timeout(timeout, client.delete(request)).await;

                match result {
                    Ok(Ok(response)) => {
                        let resp = response.into_inner();
                        Ok((shard_id, resp))
                    }
                    Ok(Err(e)) => Err((shard_id, format!("Delete failed: {}", e))),
                    Err(_) => Err((shard_id, "Delete timeout".to_string())),
                }
            }));
        }

        // Collect results
        let mut found_on_shard: Option<String> = None;
        let mut responding_shards = Vec::new();
        let mut failed_shards = Vec::new();
        let mut overall_status = DeleteStatus::NotFound;

        // FIX BUG-071: Iterate with index to identify shard even on task panic
        for (idx, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok((shard_id, response))) => {
                    responding_shards.push(shard_id.clone());

                    if response.status == DeleteStatus::Deleted as i32 {
                        overall_status = DeleteStatus::Deleted;
                        found_on_shard = Some(shard_id);
                    } else if response.status == DeleteStatus::AlreadyDeleted as i32
                        && overall_status != DeleteStatus::Deleted
                    {
                        overall_status = DeleteStatus::AlreadyDeleted;
                    }
                }
                Ok(Err((shard_id, error))) => {
                    warn!("Shard {} delete failed: {}", shard_id, error);
                    failed_shards.push(shard_id);
                }
                Err(e) => {
                    // FIX BUG-071: Now we can identify which shard's task panicked
                    let shard_id = &handle_shard_ids[idx];
                    warn!(
                        "Task for shard {} panicked during broadcast delete: {}",
                        shard_id, e
                    );
                    failed_shards.push(shard_id.clone());
                }
            }
        }

        Ok(BroadcastDeleteResult {
            status: overall_status,
            found_on_shard,
            responding_shards,
            failed_shards,
        })
    }

    /// Get a pooled client for a specific shard
    ///
    /// This is used for operations that know exactly which shard to target.
    pub async fn get_shard_client(&self, address: &str) -> Result<AkidbClient<Channel>> {
        let pool = self.get_pool(address).await?;
        Ok(pool.get_client())
    }
}

/// Connection pool statistics
#[derive(Debug)]
pub struct PoolStats {
    pub total_pools: usize,
    pub total_connections: usize,
    pub pool_size: usize,
}

/// Result of a fan-out search
#[derive(Debug)]
pub struct FanoutResult {
    /// Merged search results
    pub results: Vec<SearchResult>,
    /// Shards that responded
    pub responding_shards: Vec<String>,
    /// Shards that failed or timed out
    pub missing_shards: Vec<String>,
    /// Total number of shards queried
    pub total_shards: usize,
}

impl FanoutResult {
    /// Calculate coverage ratio
    pub fn coverage(&self) -> f32 {
        if self.total_shards == 0 {
            return 0.0;
        }
        self.responding_shards.len() as f32 / self.total_shards as f32
    }

    /// Check if this is a partial result
    pub fn is_partial(&self) -> bool {
        !self.missing_shards.is_empty() || self.responding_shards.len() < self.total_shards
    }
}

/// Result of a broadcast delete operation
#[derive(Debug)]
pub struct BroadcastDeleteResult {
    /// Overall status (DELETED if found on any shard)
    pub status: DeleteStatus,
    /// Shard where the vector was found (if any)
    pub found_on_shard: Option<String>,
    /// Shards that responded successfully
    pub responding_shards: Vec<String>,
    /// Shards that failed
    pub failed_shards: Vec<String>,
}

impl BroadcastDeleteResult {
    /// Check if the delete was successful (found and deleted)
    pub fn was_deleted(&self) -> bool {
        matches!(self.status, DeleteStatus::Deleted)
    }

    /// Check if all shards responded
    pub fn is_complete(&self) -> bool {
        self.failed_shards.is_empty()
    }
}

/// Result of a broadcast update operation
#[derive(Debug)]
pub struct BroadcastUpdateResult {
    /// Result of the broadcast delete phase
    pub delete_result: BroadcastDeleteResult,
    /// Whether the update insert succeeded
    pub update_success: bool,
    /// The shard where the new vector was inserted
    pub target_shard: String,
}

impl BroadcastUpdateResult {
    /// Check if the overall update was successful
    pub fn is_success(&self) -> bool {
        self.update_success
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fanout_top_k_rejects_u32_overflow() {
        let result = fanout_top_k((u32::MAX as usize) + 1);

        assert!(
            matches!(result, Err(AkiDbError::CoordinatorError(message)) if message.contains("exceeds u32 range"))
        );
    }

    #[test]
    fn test_fanout_top_k_allows_u32_max() {
        assert_eq!(fanout_top_k(u32::MAX as usize).unwrap(), u32::MAX);
    }

    #[test]
    fn test_shard_search_request_uses_requested_collection() {
        let request =
            shard_search_request("tenant-a".to_string(), vec![0.1, 0.2], 5, 32, vec![], None);

        assert_eq!(request.collection, "tenant-a");
        assert_eq!(request.query, vec![0.1, 0.2]);
        assert_eq!(request.top_k, 5);
        assert_eq!(request.nprobe, Some(32));
    }

    #[test]
    fn test_shard_search_request_preserves_metadata_filters() {
        let filter = br#"{"tenant":"defai"}"#.to_vec();
        let tag_filter = TagFilter { filter_type: None };

        let request = shard_search_request(
            "tenant-a".to_string(),
            vec![0.1, 0.2],
            5,
            32,
            filter.clone(),
            Some(tag_filter.clone()),
        );

        assert_eq!(request.filter, filter);
        assert_eq!(request.tag_filter, Some(tag_filter));
    }

    #[test]
    fn test_shard_update_request_preserves_metadata() {
        let metadata = br#"{"tenant":"defai"}"#.to_vec();

        let request = shard_update_request(
            "tenant-a".to_string(),
            "doc1".to_string(),
            vec![0.1, 0.2],
            metadata.clone(),
        );

        assert_eq!(request.collection, "tenant-a");
        assert_eq!(request.id, "doc1");
        assert_eq!(request.vector, vec![0.1, 0.2]);
        assert_eq!(request.metadata, metadata);
    }

    #[test]
    fn test_shard_search_result_preserves_json_metadata() {
        let result = shard_search_result(ProtoSearchResult {
            id: "doc1".to_string(),
            score: 0.9,
            metadata: r#"{"tenant":"a","year":2026}"#.to_string(),
        });

        assert_eq!(result.id.as_str(), "doc1");
        assert_eq!(result.score, 0.9);
        assert_eq!(
            result.metadata,
            Some(serde_json::json!({"tenant": "a", "year": 2026}))
        );
    }

    #[test]
    fn test_shard_search_result_keeps_non_json_metadata_as_string() {
        let result = shard_search_result(ProtoSearchResult {
            id: "doc1".to_string(),
            score: 0.9,
            metadata: "raw metadata".to_string(),
        });

        assert_eq!(
            result.metadata,
            Some(serde_json::Value::String("raw metadata".to_string()))
        );
    }

    #[test]
    fn test_fanout_result_is_partial_when_coverage_incomplete_without_missing_list() {
        let result = FanoutResult {
            results: vec![],
            responding_shards: vec!["shard-a".to_string()],
            missing_shards: vec![],
            total_shards: 2,
        };

        assert_eq!(result.coverage(), 0.5);
        assert!(result.is_partial());
    }
}
