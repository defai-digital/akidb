//! AkiDB gRPC service implementation

use crate::acl::{self, stamp_write_metadata};
use crate::auth::{self, AuthContext};
use crate::collections::{CollectionMeta, CollectionRegistry, SharedCollectionRegistry};
use crate::filter::MetadataFilter;
use crate::proto::{
    akidb_server::Akidb, CollectionInfo, CreateCollectionRequest, CreateCollectionResponse,
    DeleteRequest, DeleteResponse, DeleteStatus, DropCollectionRequest, DropCollectionResponse,
    GetClusterStateRequest, GetClusterStateResponse, GetCollectionRequest, GetCollectionResponse,
    GetRequest, GetResponse, HealthRequest, HealthResponse, InsertBatchRequest,
    InsertBatchResponse, InsertRequest, InsertResponse, ListCollectionsRequest,
    ListCollectionsResponse, SearchBatchRequest, SearchBatchResponse, SearchRequest,
    SearchResponse, SearchResult, TextSearchRequest, UpdateRequest, UpdateResponse, UpdateStatus,
    Vector, VisibilityInfo,
};
use akidb_common::config::{AclConfig, FilterMode, FilterSettings};
use akidb_common::{AkiDbError, VectorId};
use akidb_faiss::{SearchParams, VectorIndex};
use akidb_graph::{
    EdgeKind, GraphEdge, GraphEdgeId, GraphIndex, GraphMutationBatch, GraphNode, GraphNodeId,
    GraphStats, NodeKind, RelatedChunksRequest,
};
use akidb_retrieval::{
    expand_to_parents, mmr, pack, plan_query, Bm25Index, Citation, HybridFuser,
    LexicalOverlapReranker, MatchedChunk, MmrItem, PackerConfig, PlannerInput, RerankItem,
    Reranker, RetrievalMode, ScoredId,
};
use akidb_sql::{MetadataQuery, MetadataSqlIndex, SqlMetadataRecord};
use akidb_storage::{IdMapping, StorageBackend};
use dashmap::DashSet;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};

const MAX_PACK_TOKEN_BUDGET: u32 = 100_000;
const MAX_SQL_POSTFILTER_CANDIDATES: usize = 100_000;
const GRAPH_PROJECTION_EDGE_IDS: &str = "_projection_edge_ids";

const FILE_REFERENCE_SUFFIXES: &[&str] = &[
    ".rs", ".py", ".ts", ".tsx", ".js", ".jsx", ".md", ".json", ".toml", ".yaml", ".yml", ".proto",
    ".sql", ".go", ".java", ".c", ".cc", ".cpp", ".h", ".hpp",
];

fn is_ascii_digits(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_digit())
}

/// Trait for embedding providers (implemented by coordinator's AxEngineEmbedding)
pub trait EmbeddingProvider: Send + Sync {
    /// Generate embedding for text
    fn embed_text(&self, text: &str) -> std::result::Result<Vec<f32>, String>;
    /// Get embedding dimensions
    fn embedding_dimensions(&self) -> usize;
}

/// Per-vector mutation guard.
///
/// Ownership checks and the corresponding write/delete must share this guard
/// so a concurrent request cannot change a vector's workspace between them.
struct MutationLockGuard<'a> {
    locks: &'a DashSet<String>,
    id: String,
}

impl<'a> MutationLockGuard<'a> {
    /// Try to acquire the lock, returns None if already locked
    fn try_acquire(locks: &'a DashSet<String>, id: String) -> Option<Self> {
        if locks.insert(id.clone()) {
            Some(Self { locks, id })
        } else {
            None
        }
    }
}

impl Drop for MutationLockGuard<'_> {
    fn drop(&mut self) {
        self.locks.remove(&self.id);
    }
}

/// AkiDB gRPC service
pub struct AkiDbService<I, S>
where
    I: VectorIndex,
    S: StorageBackend,
{
    index: Arc<I>,
    id_mapping: Arc<IdMapping<S>>,
    collection: String,
    /// Per-key lock set used by every vector mutation.
    mutation_locks: DashSet<String>,
    /// FIX BUG-HUNT-202: Configurable SLO threshold in microseconds
    /// Previously hardcoded to 50_000us, now configurable via constructor
    slo_threshold_us: u64,
    /// Optional embedding provider for text search
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    /// In-memory BM25 lexical index, the lexical half of hybrid retrieval.
    ///
    /// Populated from the optional `text` carried on insert; kept in sync on
    /// delete. NOTE: this index is in-memory only — it is not yet persisted, so
    /// lexical/hybrid retrieval is empty after a restart until documents are
    /// re-ingested. Persisting source text and rebuilding on startup is a
    /// tracked follow-up.
    lexical: Arc<RwLock<Bm25Index>>,
    /// Raw source text per vector id, the document store backing context
    /// packing. Populated alongside `lexical` from insert `text`; same
    /// in-memory-only limitation applies.
    documents: Arc<RwLock<HashMap<VectorId, String>>>,
    /// Optional graph index used to expand packed context with related chunks.
    ///
    /// This is deliberately opt-in: constructing a service without a graph keeps
    /// the existing TextSearch behavior unchanged.
    graph_index: Option<Arc<dyn GraphIndex>>,
    /// Optional SQL metadata mirror for exact structured metadata filters.
    metadata_sql_index: Option<Arc<dyn MetadataSqlIndex>>,
    /// Workspace ACL configuration (v3.1).
    acl: AclConfig,
    /// Filtered search strategy (v3.1).
    filter_settings: FilterSettings,
    /// Collection schema registry (v3.1).
    collections: SharedCollectionRegistry,
    /// Embedding model id bound to this shard for schema/metadata stamping.
    embedding_model_id: Option<String>,
}

impl<I, S> AkiDbService<I, S>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    /// Default SLO threshold in microseconds (50ms)
    const DEFAULT_SLO_THRESHOLD_US: u64 = 50_000;

    /// Create a new service instance with default SLO threshold (50ms)
    pub fn new(
        index: Arc<I>,
        id_mapping: Arc<IdMapping<S>>,
        collection: impl Into<String>,
    ) -> Self {
        Self::with_slo_threshold(
            index,
            id_mapping,
            collection,
            Self::DEFAULT_SLO_THRESHOLD_US,
        )
    }

    /// FIX BUG-HUNT-202: Create a new service instance with configurable SLO threshold
    ///
    /// # Arguments
    /// * `slo_threshold_us` - SLO threshold in microseconds (e.g., 50_000 for 50ms)
    pub fn with_slo_threshold(
        index: Arc<I>,
        id_mapping: Arc<IdMapping<S>>,
        collection: impl Into<String>,
        slo_threshold_us: u64,
    ) -> Self {
        let collection = collection.into();
        let collections = Arc::new(CollectionRegistry::new());
        collections.ensure_default(&collection, 0, "cosine", "f32", "");
        Self {
            index,
            id_mapping,
            collection,
            mutation_locks: DashSet::new(),
            slo_threshold_us,
            embedding_provider: None,
            lexical: Arc::new(RwLock::new(Bm25Index::new())),
            documents: Arc::new(RwLock::new(HashMap::new())),
            graph_index: None,
            metadata_sql_index: None,
            acl: AclConfig::default(),
            filter_settings: FilterSettings::default(),
            collections,
            embedding_model_id: None,
        }
    }

    /// Configure workspace ACL enforcement.
    pub fn with_acl(mut self, acl: AclConfig) -> Self {
        self.acl = acl;
        self
    }

    /// Configure filtered search strategy.
    pub fn with_filter_settings(mut self, filter_settings: FilterSettings) -> Self {
        self.filter_settings = filter_settings;
        self
    }

    /// Attach a shared collection registry (usually pre-seeded at boot).
    pub fn with_collections(mut self, collections: SharedCollectionRegistry) -> Self {
        self.collections = collections;
        self
    }

    /// Bind the embedding model identity used for metadata stamping / schema.
    pub fn with_embedding_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.embedding_model_id = Some(model_id.into());
        self
    }

    /// Seed/update the default collection schema for this shard.
    pub fn seed_collection_schema(
        &self,
        dimensions: u32,
        metric: &str,
        precision: &str,
        embedding_model_id: &str,
    ) {
        self.collections.ensure_default(
            &self.collection,
            dimensions,
            metric,
            precision,
            embedding_model_id,
        );
    }

    /// Return the shared collection schema registry for read-only management
    /// services. Mutations remain governed by the data-plane RPC contract.
    pub fn collection_registry(&self) -> SharedCollectionRegistry {
        self.collections.clone()
    }

    /// Set the embedding provider for text search support
    pub fn with_embedding_provider(mut self, provider: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedding_provider = Some(provider);
        self
    }

    /// Set the graph index used for GraphRAG context expansion.
    pub fn with_graph_index(mut self, graph_index: Arc<dyn GraphIndex>) -> Self {
        self.graph_index = Some(graph_index);
        self
    }

    /// Set the optional SQL metadata index used by structured retrieval mode.
    pub fn with_metadata_sql_index(
        mut self,
        metadata_sql_index: Arc<dyn MetadataSqlIndex>,
    ) -> Self {
        self.metadata_sql_index = Some(metadata_sql_index);
        self
    }

    /// Rebuild the in-memory lexical index and document store from persisted
    /// source text. Call once at startup so hybrid retrieval and context packing
    /// work after a restart. Returns the number of documents loaded.
    pub fn rebuild_lexical_index(&self) -> usize {
        let texts = match self.id_mapping.load_all_texts() {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "failed to load persisted text for lexical rebuild");
                return 0;
            }
        };
        let mut lexical = self.lexical.write();
        let mut documents = self.documents.write();
        *lexical = Bm25Index::new();
        documents.clear();
        let mut count = 0usize;
        for (id, text) in texts {
            match self.id_mapping.get_internal_id(&id) {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(e) => {
                    warn!(vector_id = %id, error = %e, "skipping source text during lexical rebuild");
                    continue;
                }
            }
            lexical.insert(id.clone(), &text);
            documents.insert(id, text);
            count += 1;
        }
        if count > 0 {
            info!(
                documents = count,
                "rebuilt lexical index from persisted text"
            );
        }
        count
    }

    fn sync_source_text(&self, vector_id: &VectorId, text: &str) {
        if text.is_empty() {
            self.lexical.write().remove(vector_id);
            self.documents.write().remove(vector_id);
            if let Err(e) = self.id_mapping.delete_text(vector_id) {
                warn!(vector_id = %vector_id, error = %e, "failed to delete persisted source text");
            }
            return;
        }

        self.lexical.write().insert(vector_id.clone(), text);
        self.documents
            .write()
            .insert(vector_id.clone(), text.to_string());
        if let Err(e) = self.id_mapping.store_text(vector_id, text) {
            warn!(vector_id = %vector_id, error = %e, "failed to persist source text");
        }
    }

    /// Rebuild the optional SQL metadata mirror from durable vector payloads.
    /// Returns the number of records successfully mirrored.
    pub fn rebuild_sql_metadata_index(&self) -> usize {
        let Some(sql_index) = &self.metadata_sql_index else {
            return 0;
        };
        if let Err(e) = sql_index.clear_collection(&self.collection) {
            warn!(collection = %self.collection, error = %e, "failed to clear SQL metadata before rebuild");
            return 0;
        }
        let stored_vectors = match self.id_mapping.load_active_vectors() {
            Ok(vectors) => vectors,
            Err(e) => {
                warn!(error = %e, "failed to load persisted vectors for SQL metadata rebuild");
                return 0;
            }
        };

        let mut count = 0usize;
        for stored in stored_vectors {
            let vector_id = VectorId::new(&stored.external_id);
            if self
                .index_sql_metadata_with_backend(
                    sql_index.as_ref(),
                    &vector_id,
                    stored.internal_id,
                    &stored.metadata,
                    stored.created_at,
                    stored.updated_at,
                )
                .is_ok()
            {
                count += 1;
            }
        }
        if count > 0 {
            info!(
                records = count,
                "rebuilt SQL metadata index from persisted vectors"
            );
        }
        count
    }

    /// Rebuild metadata-derived graph projections from durable active vectors.
    ///
    /// Chunk nodes are removed in a first pass so stale projection edges cannot
    /// survive an interrupted vector/graph write. All active chunks are then
    /// projected again. Domain nodes that are not owned by a chunk are kept.
    pub fn rebuild_graph_index(&self) -> Result<usize, akidb_graph::GraphError> {
        let Some(graph) = &self.graph_index else {
            return Ok(0);
        };
        let stored_vectors = self.id_mapping.load_active_vectors()?;

        for stored in &stored_vectors {
            let vector_id = VectorId::new(&stored.external_id);
            let metadata = String::from_utf8_lossy(&stored.metadata);
            let workspace_id = Self::workspace_id_from_metadata(&metadata);
            graph.delete_node(&Self::chunk_node_id(&workspace_id, &vector_id))?;
        }

        for stored in &stored_vectors {
            self.index_graph_chunk(&VectorId::new(&stored.external_id), &stored.metadata, None)?;
        }
        if !stored_vectors.is_empty() {
            info!(
                chunks = stored_vectors.len(),
                "rebuilt native graph projections from persisted vectors"
            );
        }
        Ok(stored_vectors.len())
    }

    /// Create a new service instance from SloConfig
    ///
    /// FIX BUG-HUNT-202: Convenience constructor that extracts threshold from config
    pub fn with_slo_config(
        index: Arc<I>,
        id_mapping: Arc<IdMapping<S>>,
        collection: impl Into<String>,
        slo_config: &akidb_common::config::SloConfig,
    ) -> Self {
        // Convert from ms to us
        let slo_threshold_us = slo_config.reference.target_p95_ms * 1000;
        Self::with_slo_threshold(index, id_mapping, collection, slo_threshold_us)
    }

    /// Embed text with the configured provider, if any. Used by the MCP layer.
    pub fn embed_text(&self, text: &str) -> std::result::Result<Vec<f32>, String> {
        match &self.embedding_provider {
            Some(p) => p.embed_text(text),
            None => Err("no embedding provider configured".to_string()),
        }
    }

    /// Whether an embedding provider is configured.
    pub fn has_embedding_provider(&self) -> bool {
        self.embedding_provider.is_some()
    }

    fn preview_text(text: &str, max_chars: usize) -> String {
        text.chars().take(max_chars).collect()
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

    fn validate_positive_finite_option(name: &str, value: Option<f32>) -> Result<(), Status> {
        if let Some(value) = value {
            if !value.is_finite() || value <= 0.0 {
                return Err(Status::invalid_argument(format!(
                    "{name} must be finite and greater than 0"
                )));
            }
        }
        Ok(())
    }

    fn validate_text_search_options(req: &TextSearchRequest) -> Result<(), Status> {
        Self::validate_positive_finite_option("dense_weight", req.dense_weight)?;
        Self::validate_positive_finite_option("lexical_weight", req.lexical_weight)?;
        if req.pack {
            if req.pack_token_budget == Some(0) {
                return Err(Status::invalid_argument(
                    "pack_token_budget must be greater than 0 when pack is enabled",
                ));
            }
            if let Some(budget) = req.pack_token_budget {
                if budget > MAX_PACK_TOKEN_BUDGET {
                    return Err(Status::invalid_argument(format!(
                        "pack_token_budget exceeds maximum of {MAX_PACK_TOKEN_BUDGET}"
                    )));
                }
            }
        }
        if let Some(lambda) = req.mmr_lambda {
            if !lambda.is_finite() || !(0.0..=1.0).contains(&lambda) {
                return Err(Status::invalid_argument(
                    "mmr_lambda must be finite and between 0 and 1",
                ));
            }
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

    fn validate_embedding_vector(vector: &[f32]) -> Result<(), Status> {
        if vector.is_empty() {
            return Err(Status::internal(
                "Embedding provider returned invalid query vector: vector cannot be empty",
            ));
        }
        if vector.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(Status::internal(
                "Embedding provider returned invalid query vector: contains NaN or Infinity",
            ));
        }
        Ok(())
    }

    fn validate_request_collection(&self, collection: &str) -> Result<(), Status> {
        if collection.is_empty() {
            return Err(Status::invalid_argument("collection cannot be empty"));
        }
        // Allow registered collection names; the active shard still serves one
        // physical index, so non-active registered collections are rejected
        // until multi-index routing lands.
        if collection != self.collection {
            if self.collections.get(collection).is_some() {
                return Err(Status::failed_precondition(format!(
                    "collection '{collection}' is registered but not the active shard collection '{}'",
                    self.collection
                )));
            }
            return Err(Status::invalid_argument(format!(
                "collection '{}' does not match shard collection '{}'",
                collection, self.collection
            )));
        }
        Ok(())
    }

    fn request_auth_context<T>(&self, request: &Request<T>) -> AuthContext {
        auth::auth_context(request)
    }

    fn metadata_in_auth_workspace(&self, metadata: &[u8], ctx: &AuthContext) -> bool {
        !self.acl.enforce_workspace
            || acl::metadata_in_workspace(
                &Self::metadata_value_from_bytes(metadata),
                &ctx.workspace_id,
            )
    }

    fn existing_vector_metadata(&self, vector_id: &VectorId) -> Result<Option<Vec<u8>>, Status> {
        self.id_mapping
            .get_vector_including_deleted(vector_id)
            .map_err(Self::to_status)
            .map(|entry| entry.map(|entry| entry.metadata))
    }

    fn ensure_existing_vector_access(
        &self,
        vector_id: &VectorId,
        ctx: &AuthContext,
    ) -> Result<Option<Vec<u8>>, Status> {
        let metadata = self.existing_vector_metadata(vector_id)?;
        if metadata
            .as_deref()
            .is_some_and(|metadata| !self.metadata_in_auth_workspace(metadata, ctx))
        {
            // Do not disclose whether an id exists in another workspace.
            return Err(Status::not_found(format!(
                "Vector not found: {}",
                vector_id
            )));
        }
        Ok(metadata)
    }

    fn ensure_insert_does_not_cross_workspace(
        &self,
        vector_id: &VectorId,
        ctx: &AuthContext,
    ) -> Result<Option<Vec<u8>>, Status> {
        let metadata = self.existing_vector_metadata(vector_id)?;
        if metadata
            .as_deref()
            .is_some_and(|metadata| !self.metadata_in_auth_workspace(metadata, ctx))
        {
            return Err(Status::already_exists(format!(
                "Vector id already exists: {}",
                vector_id
            )));
        }
        Ok(metadata)
    }

    /// Build metadata filter from request inputs + workspace ACL scope.
    fn compile_search_filter(
        &self,
        filter_bytes: &[u8],
        tag_filter: Option<crate::proto::TagFilter>,
        ctx: &AuthContext,
    ) -> Result<Option<MetadataFilter>, Status> {
        let user =
            MetadataFilter::build(filter_bytes, tag_filter).map_err(Status::invalid_argument)?;
        acl::apply_workspace_scope(user, ctx, &self.acl).map_err(Status::invalid_argument)
    }

    fn attach_metadata_predicate(
        &self,
        mut params: SearchParams,
        metadata_filter: Option<Arc<MetadataFilter>>,
    ) -> SearchParams {
        if let Some(metadata_filter) = metadata_filter {
            let id_mapping = self.id_mapping.clone();
            params = params.with_filter(Arc::new(move |id: &VectorId| {
                match id_mapping.get_vector(id) {
                    Ok(Some(entry)) => {
                        let meta = if entry.metadata.is_empty() {
                            serde_json::Value::Null
                        } else {
                            match serde_json::from_slice(&entry.metadata) {
                                Ok(meta) => meta,
                                Err(_) => return false,
                            }
                        };
                        metadata_filter.matches(&meta)
                    }
                    _ => false,
                }
            }));
        }
        params
    }

    /// Effective candidate `top_k` under the configured filter strategy.
    fn filtered_search_k(&self, top_k: usize, has_filter: bool) -> usize {
        if !has_filter {
            return top_k;
        }
        let factor = self.filter_settings.postfilter_overfetch_factor.max(1) as usize;
        match self.filter_settings.mode {
            FilterMode::Pre => top_k,
            FilterMode::Post | FilterMode::Adaptive => {
                // Adaptive currently uses post-filter over-fetch; true prefilter
                // bitmap integration is tracked as a follow-up inside GAP-003.
                top_k.saturating_mul(factor).clamp(top_k, top_k.max(500))
            }
        }
    }

    fn collection_info(&self, meta: &CollectionMeta) -> CollectionInfo {
        let vector_count = if meta.name == self.collection {
            self.index.stats().active_vectors
        } else {
            0
        };
        CollectionInfo {
            name: meta.name.clone(),
            dimensions: meta.dimensions,
            metric: meta.metric.clone(),
            embedding_model_id: meta.embedding_model_id.clone(),
            vector_precision: meta.vector_precision.clone(),
            chunk_strategy: meta.chunk_strategy.clone(),
            vector_count,
        }
    }

    /// Apply score_threshold then optional group_by (Phase C knobs).
    fn apply_score_and_group(
        &self,
        results: Vec<SearchResult>,
        top_k: usize,
        score_threshold: Option<f32>,
        group_by: &str,
        group_size: Option<u32>,
    ) -> Vec<SearchResult> {
        let mut filtered: Vec<SearchResult> = match score_threshold {
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
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut out = Vec::new();
        for r in filtered.drain(..) {
            let group_val = Self::metadata_group_key(&r.metadata, key);
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

    fn metadata_group_key(metadata: &str, key: &str) -> String {
        if metadata.is_empty() {
            return String::new();
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata) else {
            return String::new();
        };
        match value.get(key) {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Number(n)) => n.to_string(),
            Some(serde_json::Value::Bool(b)) => b.to_string(),
            Some(other) => other.to_string(),
            None => String::new(),
        }
    }

    fn validate_unique_batch_ids(vectors: &[Vector]) -> Result<(), Status> {
        let mut seen = HashSet::with_capacity(vectors.len());
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

    /// Current index statistics (active/total/tombstoned vectors, dimensions).
    pub fn index_stats(&self) -> akidb_faiss::IndexStats {
        self.index.stats()
    }

    /// Current graph statistics, when graph expansion is configured.
    pub fn graph_stats(&self) -> Option<GraphStats> {
        self.graph_index
            .as_ref()
            .and_then(|graph| match graph.stats() {
                Ok(stats) => Some(stats),
                Err(e) => {
                    warn!(error = %e, "failed to load graph stats");
                    None
                }
            })
    }

    /// Convert AkiDbError to tonic Status
    fn to_status(err: AkiDbError) -> Status {
        match err {
            AkiDbError::VectorNotFound(_) => Status::not_found(err.to_string()),
            AkiDbError::VectorAlreadyExists(_) => Status::already_exists(err.to_string()),
            AkiDbError::DimensionMismatch { .. } => Status::invalid_argument(err.to_string()),
            AkiDbError::InvalidParameter(_) => Status::invalid_argument(err.to_string()),
            AkiDbError::IdReuseForbidden(_) => Status::failed_precondition(err.to_string()),
            AkiDbError::GpuOutOfMemory => Status::resource_exhausted(err.to_string()),
            AkiDbError::RebuildInProgress => Status::unavailable(err.to_string()),
            AkiDbError::Timeout(_) => Status::deadline_exceeded(err.to_string()),
            _ => Status::internal(err.to_string()),
        }
    }

    /// Load a vector's stored metadata as a JSON string, or empty when absent.
    /// Used to populate metadata on fused hybrid results, which come back from
    /// the retrieval layer as id + score only.
    fn load_metadata_string(&self, id: &VectorId) -> String {
        match self.id_mapping.get_vector(id) {
            Ok(Some(entry)) if !entry.metadata.is_empty() => {
                String::from_utf8(entry.metadata).unwrap_or_default()
            }
            _ => String::new(),
        }
    }

    fn load_metadata_value(&self, id: &VectorId) -> Option<serde_json::Value> {
        match self.id_mapping.get_vector(id) {
            Ok(Some(entry)) => {
                if entry.metadata.is_empty() {
                    Some(serde_json::Value::Null)
                } else {
                    serde_json::from_slice(&entry.metadata).ok()
                }
            }
            _ => None,
        }
    }

    fn metadata_matches_filter(&self, id: &VectorId, filter: &MetadataFilter) -> bool {
        self.load_metadata_value(id)
            .is_some_and(|metadata| filter.matches(&metadata))
    }

    fn citation_for_vector(&self, id: &VectorId) -> Citation {
        let Some(metadata) = self.load_metadata_value(id) else {
            return Citation::new(id.as_str());
        };
        let source_uri = ["source_uri", "document_key", "file"]
            .iter()
            .find_map(|key| metadata.get(key).and_then(serde_json::Value::as_str))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| id.as_str());
        let mut citation = Citation::new(source_uri);

        if let Some(version) = ["source_version", "content_hash"]
            .iter()
            .find_map(|key| metadata.get(key).and_then(serde_json::Value::as_str))
            .filter(|value| !value.is_empty())
        {
            citation = citation.with_version(version);
        }

        let offset = |key: &str| {
            metadata.get(key).and_then(|value| match value {
                serde_json::Value::Number(number) => number
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok()),
                serde_json::Value::String(value) => value.parse::<usize>().ok(),
                _ => None,
            })
        };
        if let (Some(start), Some(end)) = (offset("start_offset"), offset("end_offset")) {
            citation = citation.with_span(start, end);
        }
        citation
    }

    fn metadata_value_from_bytes(metadata: &[u8]) -> serde_json::Value {
        if metadata.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(metadata).unwrap_or(serde_json::Value::Null)
        }
    }

    fn validate_metadata_json(metadata: &[u8]) -> Result<(), String> {
        if metadata.is_empty() {
            return Ok(());
        }

        serde_json::from_slice::<serde_json::Value>(metadata)
            .map(|_| ())
            .map_err(|e| format!("Metadata must be valid JSON: {e}"))
    }

    fn validate_vector_id(id: &str) -> Result<(), &'static str> {
        if id.is_empty() {
            return Err("Vector ID cannot be empty");
        }
        if id.len() > 1024 {
            return Err("Vector ID exceeds maximum length of 1024");
        }
        Ok(())
    }

    fn validate_vector_payload(id: &str, vector: &[f32]) -> Result<(), &'static str> {
        Self::validate_vector_id(id)?;
        if vector.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err("Vector contains NaN or Infinity values");
        }
        if vector.is_empty() {
            return Err("Vector cannot be empty");
        }
        Ok(())
    }

    fn current_timestamp_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn index_sql_metadata_with_backend(
        &self,
        sql_index: &dyn MetadataSqlIndex,
        vector_id: &VectorId,
        internal_id: i64,
        metadata: &[u8],
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Result<(), akidb_sql::SqlMetadataError> {
        sql_index.upsert_record(&SqlMetadataRecord::new(
            self.collection.clone(),
            vector_id.as_str().to_string(),
            internal_id,
            Self::metadata_value_from_bytes(metadata),
            created_at_ms,
            updated_at_ms,
        ))
    }

    fn index_sql_metadata(&self, vector_id: &VectorId, internal_id: i64, metadata: &[u8]) {
        let Some(sql_index) = &self.metadata_sql_index else {
            return;
        };
        let now = Self::current_timestamp_ms();
        if let Err(e) = self.index_sql_metadata_with_backend(
            sql_index.as_ref(),
            vector_id,
            internal_id,
            metadata,
            now,
            now,
        ) {
            warn!(vector_id = %vector_id, error = %e, "failed to index SQL metadata");
        }
    }

    fn delete_sql_metadata(&self, vector_id: &VectorId) {
        let Some(sql_index) = &self.metadata_sql_index else {
            return;
        };
        if let Err(e) = sql_index.delete_record(&self.collection, vector_id.as_str()) {
            warn!(vector_id = %vector_id, error = %e, "failed to delete SQL metadata");
        }
    }

    fn sql_query_from_legacy_filter(
        collection: &str,
        filter: &[u8],
        limit: usize,
    ) -> Result<MetadataQuery, Status> {
        let mut query = MetadataQuery::new(collection).with_limit(limit);
        if filter.is_empty() {
            return Ok(query);
        }

        let value: serde_json::Value = serde_json::from_slice(filter)
            .map_err(|e| Status::invalid_argument(format!("invalid SQL metadata filter: {e}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| Status::invalid_argument("SQL metadata filter must be a JSON object"))?;

        for (key, value) in object {
            query = Self::add_sql_legacy_predicate(query, key, value, !key.contains('.'))?;
        }
        Ok(query)
    }

    fn add_sql_legacy_predicate(
        query: MetadataQuery,
        field: &str,
        value: &serde_json::Value,
        can_pushdown_as_path: bool,
    ) -> Result<MetadataQuery, Status> {
        if !can_pushdown_as_path {
            return Ok(query);
        }
        match value {
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    return Ok(query.with_exists(field.to_string()));
                }
                let mut query = query;
                for (key, value) in map {
                    let nested_field = format!("{field}.{key}");
                    query = Self::add_sql_legacy_predicate(
                        query,
                        &nested_field,
                        value,
                        !key.contains('.'),
                    )?;
                }
                Ok(query)
            }
            serde_json::Value::Array(_) => Ok(query.with_exists(field.to_string())),
            _ => Ok(query.with_eq(field.to_string(), value.clone())),
        }
    }

    fn legacy_filter_needs_post_filter(filter: &[u8]) -> Result<bool, Status> {
        if filter.is_empty() {
            return Ok(false);
        }

        let value: serde_json::Value = serde_json::from_slice(filter)
            .map_err(|e| Status::invalid_argument(format!("invalid SQL metadata filter: {e}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| Status::invalid_argument("SQL metadata filter must be a JSON object"))?;
        Ok(object
            .iter()
            .any(|(key, value)| key.contains('.') || Self::value_needs_post_filter(value)))
    }

    fn value_needs_post_filter(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::Object(map) => {
                map.is_empty()
                    || map.iter().any(|(key, value)| {
                        key.contains('.') || Self::value_needs_post_filter(value)
                    })
            }
            serde_json::Value::Array(_) => true,
            _ => false,
        }
    }

    fn build_context_pack(
        &self,
        response_results: &[SearchResult],
        metadata_filter: Option<&MetadataFilter>,
        top_k: usize,
        budget: usize,
    ) -> String {
        let docs = self.documents.read();
        let mut seen = HashSet::new();
        let mut matched: Vec<MatchedChunk> = response_results
            .iter()
            .map(|r| {
                let parent_id = Self::parent_id_from_metadata(&r.metadata);
                let id = VectorId::new(&r.id);
                seen.insert(id.clone());
                let text = docs.get(&id).cloned().unwrap_or_default();
                MatchedChunk::new(id, parent_id, text, r.score)
            })
            .collect();

        if let Some(graph) = &self.graph_index {
            for result in response_results {
                let workspace_id = Self::workspace_id_from_metadata(&result.metadata);
                let seed = Self::chunk_node_id(&workspace_id, &VectorId::new(&result.id));
                match graph.related_chunks(&seed, top_k.saturating_mul(2).max(1)) {
                    Ok(chunks) => {
                        for chunk in chunks {
                            let id = chunk.vector_id;
                            if !seen.insert(id.clone()) {
                                continue;
                            }
                            if let Some(metadata_filter) = metadata_filter {
                                if !self.metadata_matches_filter(&id, metadata_filter) {
                                    continue;
                                }
                            }
                            let Some(text) = docs.get(&id).cloned() else {
                                continue;
                            };
                            if text.is_empty() {
                                continue;
                            }
                            let metadata = self.load_metadata_string(&id);
                            let parent_id = Self::parent_id_from_metadata(&metadata);
                            matched.push(MatchedChunk::new(
                                id,
                                parent_id,
                                text,
                                result.score * 0.85,
                            ));
                        }
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "graph context expansion failed");
                    }
                }
            }
        }

        let mut passages =
            expand_to_parents(&matched, |pid| docs.get(&VectorId::new(pid)).cloned());
        passages.retain(|p| !p.text.is_empty());
        for passage in &mut passages {
            passage.citation = self.citation_for_vector(&passage.id);
        }
        pack(&passages, &PackerConfig::new(budget)).text
    }

    fn sql_metadata_text_search(
        &self,
        req: &TextSearchRequest,
        started_at: Instant,
        scoped_filter: Option<&MetadataFilter>,
        ctx: &AuthContext,
    ) -> Result<Response<SearchResponse>, Status> {
        let Some(sql_index) = &self.metadata_sql_index else {
            return Err(Status::unavailable(
                "Structured SQL retrieval requires the optional SQL metadata adapter",
            ));
        };

        let has_tag_filter = req
            .tag_filter
            .as_ref()
            .and_then(|tag| tag.filter_type.as_ref())
            .is_some();
        let workspace_pushdown = self.acl.enforce_workspace && ctx.workspace_id != "default";
        let needs_post_filter = has_tag_filter
            || Self::legacy_filter_needs_post_filter(&req.filter)?
            || (self.acl.enforce_workspace && !workspace_pushdown);
        let sql_limit = if needs_post_filter {
            (req.top_k as usize)
                .saturating_mul(self.filter_settings.postfilter_overfetch_factor.max(1) as usize)
                .clamp(1_000, MAX_SQL_POSTFILTER_CANDIDATES)
        } else {
            req.top_k as usize
        };
        let mut query =
            Self::sql_query_from_legacy_filter(&self.collection, &req.filter, sql_limit)?;
        if workspace_pushdown {
            query = query.with_eq(acl::WORKSPACE_KEY, serde_json::json!(ctx.workspace_id));
        }
        let ids = sql_index
            .query_ids(&query)
            .map_err(|e| Status::internal(format!("SQL metadata retrieval failed: {e}")))?;
        let candidate_cap_reached = needs_post_filter && ids.len() == sql_limit;

        let results: Vec<SearchResult> = ids
            .into_iter()
            .filter_map(|id| {
                let vector_id = VectorId::new(&id);
                if let Some(filter) = scoped_filter {
                    if !self.metadata_matches_filter(&vector_id, filter) {
                        return None;
                    }
                }
                Some(SearchResult {
                    id,
                    score: 1.0,
                    metadata: self.load_metadata_string(&vector_id),
                })
            })
            .take(req.top_k as usize)
            .collect();
        let context_pack = if req.pack {
            self.build_context_pack(
                &results,
                scoped_filter,
                req.top_k as usize,
                req.pack_token_budget.unwrap_or(1024) as usize,
            )
        } else {
            String::new()
        };
        let latency_us = started_at.elapsed().as_micros() as u64;

        Ok(Response::new(SearchResponse {
            results,
            partial: candidate_cap_reached,
            missing_shards: vec![],
            coverage: if candidate_cap_reached { 0.0 } else { 1.0 },
            latency_us,
            within_slo: latency_us < self.slo_threshold_us,
            degraded_mode: false,
            context_pack,
            serving_generation: None,
        }))
    }

    fn requested_text_retrieval_mode(
        req: &TextSearchRequest,
    ) -> Result<Option<RetrievalMode>, Status> {
        let mode = req.retrieval_mode.trim().to_ascii_lowercase();
        if mode.is_empty() {
            return Ok((!req.hybrid).then_some(RetrievalMode::Vector));
        }

        match mode.as_str() {
            "auto" => Ok(None),
            "vector" | "dense" => Ok(Some(RetrievalMode::Vector)),
            "bm25" | "lexical" | "full_text" | "full-text" => Ok(Some(RetrievalMode::Bm25)),
            "hybrid" => Ok(Some(RetrievalMode::Hybrid)),
            "graph" => Ok(Some(RetrievalMode::Graph)),
            "graph_hybrid" | "graph-hybrid" => Ok(Some(RetrievalMode::GraphHybrid)),
            "sql" | "structured_sql" | "structured-sql" => {
                Ok(Some(RetrievalMode::StructuredSql))
            }
            _ => Err(Status::invalid_argument(format!(
                "invalid retrieval_mode '{}'; expected auto, vector, bm25, hybrid, graph, graph_hybrid, sql, structured_sql, or structured-sql",
                req.retrieval_mode
            ))),
        }
    }

    fn parent_id_from_metadata(metadata: &str) -> Option<String> {
        if metadata.is_empty() {
            return None;
        }
        serde_json::from_str::<serde_json::Value>(metadata)
            .ok()
            .and_then(|m| {
                m.get("parent_id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
    }

    fn scoped_graph_node_id(workspace_id: &str, local_id: String) -> GraphNodeId {
        if workspace_id == "default" {
            GraphNodeId::new(local_id)
        } else {
            GraphNodeId::scoped(workspace_id, &local_id)
        }
    }

    fn chunk_node_id(workspace_id: &str, vector_id: &VectorId) -> GraphNodeId {
        Self::scoped_graph_node_id(workspace_id, format!("chunk:{}", vector_id.as_str()))
    }

    fn graph_seed_nodes_from_query(query: &str, workspace_id: &str) -> Vec<GraphNodeId> {
        let mut seeds = Vec::new();
        let mut seen = HashSet::new();
        for raw in query.split_whitespace() {
            let token = Self::clean_graph_query_token(raw);
            if token.is_empty() {
                continue;
            }
            if Self::looks_like_file_reference(&token) {
                let file = Self::normalize_file_reference(&token);
                Self::push_graph_seed(&mut seeds, &mut seen, workspace_id, NodeKind::File, &file);
            }
            if Self::looks_like_symbol_reference(&token) {
                let symbol = token.trim_end_matches("()");
                Self::push_graph_seed(
                    &mut seeds,
                    &mut seen,
                    workspace_id,
                    NodeKind::Function,
                    symbol,
                );
            }
        }
        seeds
    }

    fn push_graph_seed(
        seeds: &mut Vec<GraphNodeId>,
        seen: &mut HashSet<String>,
        workspace_id: &str,
        kind: NodeKind,
        raw_id: &str,
    ) {
        let node_id = Self::graph_node_id(workspace_id, kind, raw_id);
        if seen.insert(node_id.as_str().to_string()) {
            seeds.push(node_id);
        }
    }

    fn clean_graph_query_token(token: &str) -> String {
        token
            .trim_matches(|c: char| {
                matches!(
                    c,
                    '"' | '\''
                        | '`'
                        | ','
                        | ';'
                        | '?'
                        | '!'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '<'
                        | '>'
                )
            })
            .trim_end_matches([':', '.'])
            .to_string()
    }

    fn looks_like_file_reference(token: &str) -> bool {
        token.contains('/')
            || FILE_REFERENCE_SUFFIXES
                .iter()
                .any(|suffix| Self::file_suffix_end(token, suffix).is_some())
    }

    fn normalize_file_reference(token: &str) -> String {
        for suffix in FILE_REFERENCE_SUFFIXES {
            if let Some(end) = Self::file_suffix_end(token, suffix) {
                return token[..end].to_string();
            }
        }
        token.to_string()
    }

    fn file_suffix_end(token: &str, suffix: &str) -> Option<usize> {
        let start = token.rfind(suffix)?;
        let end = start + suffix.len();
        if end == token.len() {
            return Some(end);
        }
        let rest = token.get(end..)?;
        if rest.starts_with(':')
            && rest[1..]
                .split(':')
                .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
        {
            return Some(end);
        }
        if Self::is_github_line_fragment(rest) {
            Some(end)
        } else {
            None
        }
    }

    fn is_github_line_fragment(rest: &str) -> bool {
        let Some(line) = rest.strip_prefix("#L") else {
            return false;
        };
        if let Some((start, end)) = line.split_once("-L") {
            is_ascii_digits(start) && is_ascii_digits(end)
        } else {
            is_ascii_digits(line)
        }
    }

    fn looks_like_symbol_reference(token: &str) -> bool {
        token.contains("::") || token.ends_with("()")
    }

    fn related_ids_from_metadata(metadata: &str) -> Vec<String> {
        if metadata.is_empty() {
            return Vec::new();
        }
        serde_json::from_str::<serde_json::Value>(metadata)
            .ok()
            .map(|m| Self::metadata_string_values(Some(&m), "related_ids"))
            .unwrap_or_default()
    }

    fn metadata_string_values(metadata: Option<&serde_json::Value>, key: &str) -> Vec<String> {
        let Some(value) = metadata.and_then(|m| m.get(key)) else {
            return Vec::new();
        };
        match value {
            serde_json::Value::String(s) if !s.is_empty() => vec![s.clone()],
            serde_json::Value::Array(values) => values
                .iter()
                .filter_map(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            _ => Vec::new(),
        }
    }

    fn workspace_id_from_metadata_value(metadata: Option<&serde_json::Value>) -> &str {
        metadata
            .and_then(|metadata| metadata.get(acl::WORKSPACE_KEY))
            .and_then(serde_json::Value::as_str)
            .filter(|workspace_id| !workspace_id.is_empty())
            .unwrap_or("default")
    }

    fn workspace_id_from_metadata(metadata: &str) -> String {
        let metadata = serde_json::from_str::<serde_json::Value>(metadata).ok();
        Self::workspace_id_from_metadata_value(metadata.as_ref()).to_string()
    }

    fn graph_node_id(workspace_id: &str, kind: NodeKind, id: &str) -> GraphNodeId {
        Self::scoped_graph_node_id(workspace_id, format!("{}:{}", kind.as_key(), id))
    }

    fn graph_edge_id(kind: EdgeKind, from: &GraphNodeId, to: &GraphNodeId) -> String {
        format!("auto:{}:{}:{}", kind.as_key(), from.as_str(), to.as_str())
    }

    fn scoped_legacy_edge_id(workspace_id: &str, edge_id: String) -> String {
        if workspace_id == "default" {
            edge_id
        } else {
            format!(
                "workspace:{}:{}:{}",
                workspace_id.len(),
                workspace_id,
                edge_id
            )
        }
    }

    fn graph_node(
        workspace_id: &str,
        node_id: GraphNodeId,
        kind: NodeKind,
        raw_id: &str,
    ) -> GraphNode {
        GraphNode::new(node_id, kind)
            .with_property("id", serde_json::json!(raw_id))
            .with_property(acl::WORKSPACE_KEY, serde_json::json!(workspace_id))
    }

    fn graph_edge(
        workspace_id: &str,
        from: &GraphNodeId,
        to: &GraphNodeId,
        kind: EdgeKind,
        evidence_chunk_id: &VectorId,
        metadata: Option<&serde_json::Value>,
    ) -> GraphEdge {
        let mut edge = GraphEdge::new(
            Self::graph_edge_id(kind, from, to),
            from.clone(),
            to.clone(),
            kind,
        )
        .with_property(acl::WORKSPACE_KEY, serde_json::json!(workspace_id))
        .with_property(
            "evidence_chunk_id",
            serde_json::json!(evidence_chunk_id.as_str()),
        );
        for key in [
            "source_uri",
            "source_version",
            "extraction_method",
            "pipeline_version",
        ] {
            if let Some(value) = metadata.and_then(|metadata| metadata.get(key)) {
                edge = edge.with_property(key, value.clone());
            }
        }
        edge
    }

    fn stored_graph_projection_edge_ids(
        graph: &dyn GraphIndex,
        chunk_node_id: &GraphNodeId,
        vector_id: &VectorId,
    ) -> Result<Option<Vec<GraphEdgeId>>, akidb_graph::GraphError> {
        let Some(node) = graph.get_node(chunk_node_id)? else {
            return Ok(None);
        };
        let Some(edge_ids) = node
            .properties
            .get(GRAPH_PROJECTION_EDGE_IDS)
            .and_then(serde_json::Value::as_array)
        else {
            return Ok(None);
        };

        let mut owned_edge_ids = Vec::with_capacity(edge_ids.len());
        for edge_id in edge_ids.iter().filter_map(serde_json::Value::as_str) {
            let edge_id = GraphEdgeId::new(edge_id);
            let Some(edge) = graph.get_edge(&edge_id)? else {
                continue;
            };
            let is_owned_by_projection = edge
                .properties
                .get("evidence_chunk_id")
                .and_then(serde_json::Value::as_str)
                == Some(vector_id.as_str());
            if is_owned_by_projection {
                owned_edge_ids.push(edge_id);
            }
        }
        Ok(Some(owned_edge_ids))
    }

    fn index_graph_chunk(
        &self,
        vector_id: &VectorId,
        metadata: &[u8],
        previous_metadata: Option<&[u8]>,
    ) -> Result<(), akidb_graph::GraphError> {
        let Some(graph) = &self.graph_index else {
            return Ok(());
        };

        let metadata = String::from_utf8_lossy(metadata);
        let metadata_json = serde_json::from_str::<serde_json::Value>(&metadata).ok();
        let workspace_id = Self::workspace_id_from_metadata_value(metadata_json.as_ref());
        let chunk_node_id = Self::chunk_node_id(workspace_id, vector_id);
        let mut chunk_node = Self::graph_node(
            workspace_id,
            chunk_node_id.clone(),
            NodeKind::Chunk,
            vector_id.as_str(),
        )
        .with_property("vector_id", serde_json::json!(vector_id.as_str()));
        for key in [
            "source_uri",
            "source_object_id",
            "source_version",
            "document_id",
            "chunk_index",
            "start_offset",
            "end_offset",
            "extraction_method",
            "pipeline_version",
        ] {
            if let Some(value) = metadata_json
                .as_ref()
                .and_then(|metadata| metadata.get(key))
            {
                chunk_node = chunk_node.with_property(key, value.clone());
            }
        }
        let mut batch = GraphMutationBatch::new().with_replaced_node(chunk_node);
        match Self::stored_graph_projection_edge_ids(graph.as_ref(), &chunk_node_id, vector_id)? {
            Some(edge_ids) => batch.delete_edges.extend(edge_ids),
            None => {
                if let Some(previous_metadata) = previous_metadata {
                    batch
                        .delete_edges
                        .extend(Self::graph_edge_ids_from_metadata(
                            vector_id,
                            previous_metadata,
                        ));
                }
            }
        }

        if let Some(parent_id) = Self::parent_id_from_metadata(&metadata) {
            let parent_vector_id = VectorId::new(parent_id);
            let parent_node_id = Self::chunk_node_id(workspace_id, &parent_vector_id);
            batch.nodes.push(
                Self::graph_node(
                    workspace_id,
                    parent_node_id.clone(),
                    NodeKind::Chunk,
                    parent_vector_id.as_str(),
                )
                .with_property("vector_id", serde_json::json!(parent_vector_id.as_str())),
            );

            let mut parent_edge = Self::graph_edge(
                workspace_id,
                &parent_node_id,
                &chunk_node_id,
                EdgeKind::ParentOf,
                vector_id,
                metadata_json.as_ref(),
            );
            parent_edge.id = GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!(
                    "auto:parent_of:{}:{}",
                    parent_vector_id.as_str(),
                    vector_id.as_str()
                ),
            ));
            batch.edges.push(parent_edge);

            let mut child_edge = Self::graph_edge(
                workspace_id,
                &chunk_node_id,
                &parent_node_id,
                EdgeKind::ChildOf,
                vector_id,
                metadata_json.as_ref(),
            );
            child_edge.id = GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!(
                    "auto:child_of:{}:{}",
                    vector_id.as_str(),
                    parent_vector_id.as_str()
                ),
            ));
            batch.edges.push(child_edge);
        }

        for related_id in Self::related_ids_from_metadata(&metadata) {
            let related_vector_id = VectorId::new(related_id);
            let related_node_id = Self::chunk_node_id(workspace_id, &related_vector_id);
            batch.nodes.push(
                Self::graph_node(
                    workspace_id,
                    related_node_id.clone(),
                    NodeKind::Chunk,
                    related_vector_id.as_str(),
                )
                .with_property("vector_id", serde_json::json!(related_vector_id.as_str())),
            );

            let mut forward_edge = Self::graph_edge(
                workspace_id,
                &chunk_node_id,
                &related_node_id,
                EdgeKind::RelatedTo,
                vector_id,
                metadata_json.as_ref(),
            );
            forward_edge.id = GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!(
                    "auto:related_to:{}:{}",
                    vector_id.as_str(),
                    related_vector_id.as_str()
                ),
            ));
            batch.edges.push(forward_edge);

            let mut reverse_edge = Self::graph_edge(
                workspace_id,
                &related_node_id,
                &chunk_node_id,
                EdgeKind::RelatedTo,
                vector_id,
                metadata_json.as_ref(),
            );
            reverse_edge.id = GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!(
                    "auto:related_to:{}:{}",
                    related_vector_id.as_str(),
                    vector_id.as_str()
                ),
            ));
            batch.edges.push(reverse_edge);
        }

        let chunk_edge_fields = [
            ("imports", EdgeKind::Imports),
            ("calls", EdgeKind::Calls),
            ("depends_on", EdgeKind::DependsOn),
            ("tests", EdgeKind::Tests),
            ("tested_by", EdgeKind::TestedBy),
        ];
        for (field, edge_kind) in chunk_edge_fields {
            for target_id in Self::metadata_string_values(metadata_json.as_ref(), field) {
                let target_vector_id = VectorId::new(target_id);
                let target_node_id = Self::chunk_node_id(workspace_id, &target_vector_id);
                batch.nodes.push(
                    Self::graph_node(
                        workspace_id,
                        target_node_id.clone(),
                        NodeKind::Chunk,
                        target_vector_id.as_str(),
                    )
                    .with_property("vector_id", serde_json::json!(target_vector_id.as_str())),
                );
                batch.edges.push(Self::graph_edge(
                    workspace_id,
                    &chunk_node_id,
                    &target_node_id,
                    edge_kind,
                    vector_id,
                    metadata_json.as_ref(),
                ));
            }
        }

        for owner in Self::metadata_string_values(metadata_json.as_ref(), "owned_by") {
            let owner_node_id = Self::graph_node_id(workspace_id, NodeKind::Person, &owner);
            batch.nodes.push(Self::graph_node(
                workspace_id,
                owner_node_id.clone(),
                NodeKind::Person,
                &owner,
            ));
            batch.edges.push(Self::graph_edge(
                workspace_id,
                &chunk_node_id,
                &owner_node_id,
                EdgeKind::OwnedBy,
                vector_id,
                metadata_json.as_ref(),
            ));
        }

        for commit in Self::metadata_string_values(metadata_json.as_ref(), "changed_by") {
            let commit_node_id = Self::graph_node_id(workspace_id, NodeKind::Commit, &commit);
            batch.nodes.push(Self::graph_node(
                workspace_id,
                commit_node_id.clone(),
                NodeKind::Commit,
                &commit,
            ));
            batch.edges.push(Self::graph_edge(
                workspace_id,
                &chunk_node_id,
                &commit_node_id,
                EdgeKind::ChangedBy,
                vector_id,
                metadata_json.as_ref(),
            ));
        }

        for (field, node_kind) in [
            ("document_id", NodeKind::Document),
            ("section_id", NodeKind::Section),
        ] {
            for structural_id in Self::metadata_string_values(metadata_json.as_ref(), field) {
                let structural_node_id =
                    Self::graph_node_id(workspace_id, node_kind, &structural_id);
                let mut structural_node = Self::graph_node(
                    workspace_id,
                    structural_node_id.clone(),
                    node_kind,
                    &structural_id,
                );
                for key in ["source_uri", "source_version", "source_object_id"] {
                    if let Some(value) = metadata_json
                        .as_ref()
                        .and_then(|metadata| metadata.get(key))
                    {
                        structural_node = structural_node.with_property(key, value.clone());
                    }
                }
                batch.nodes.push(structural_node);
                batch.edges.push(Self::graph_edge(
                    workspace_id,
                    &structural_node_id,
                    &chunk_node_id,
                    EdgeKind::Contains,
                    vector_id,
                    metadata_json.as_ref(),
                ));
            }
        }

        for file in Self::metadata_string_values(metadata_json.as_ref(), "file") {
            let file_node_id = Self::graph_node_id(workspace_id, NodeKind::File, &file);
            batch.nodes.push(Self::graph_node(
                workspace_id,
                file_node_id.clone(),
                NodeKind::File,
                &file,
            ));
            batch.edges.push(Self::graph_edge(
                workspace_id,
                &file_node_id,
                &chunk_node_id,
                EdgeKind::Contains,
                vector_id,
                metadata_json.as_ref(),
            ));
        }

        for symbol in Self::metadata_string_values(metadata_json.as_ref(), "symbol") {
            let symbol_node_id = Self::graph_node_id(workspace_id, NodeKind::Function, &symbol);
            batch.nodes.push(Self::graph_node(
                workspace_id,
                symbol_node_id.clone(),
                NodeKind::Function,
                &symbol,
            ));
            batch.edges.push(Self::graph_edge(
                workspace_id,
                &symbol_node_id,
                &chunk_node_id,
                EdgeKind::Contains,
                vector_id,
                metadata_json.as_ref(),
            ));
        }

        let mut projection_edge_ids: Vec<String> = batch
            .edges
            .iter()
            .map(|edge| edge.id.as_str().to_string())
            .collect();
        projection_edge_ids.sort();
        projection_edge_ids.dedup();
        if let Some(chunk_node) = batch.nodes.iter_mut().find(|node| node.id == chunk_node_id) {
            chunk_node.properties.insert(
                GRAPH_PROJECTION_EDGE_IDS.to_string(),
                serde_json::json!(projection_edge_ids),
            );
        }

        graph.upsert_batch(batch)
    }

    fn graph_edge_ids_from_metadata(vector_id: &VectorId, metadata: &[u8]) -> Vec<GraphEdgeId> {
        let metadata = String::from_utf8_lossy(metadata);
        let metadata_json = serde_json::from_str::<serde_json::Value>(&metadata).ok();
        let workspace_id = Self::workspace_id_from_metadata_value(metadata_json.as_ref());
        let chunk_node_id = Self::chunk_node_id(workspace_id, vector_id);
        let mut edge_ids = Vec::new();

        if let Some(parent_id) = Self::parent_id_from_metadata(&metadata) {
            edge_ids.push(GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!("auto:parent_of:{}:{}", parent_id, vector_id.as_str()),
            )));
            edge_ids.push(GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!("auto:child_of:{}:{}", vector_id.as_str(), parent_id),
            )));
        }

        for related_id in Self::related_ids_from_metadata(&metadata) {
            edge_ids.push(GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!("auto:related_to:{}:{}", vector_id.as_str(), related_id),
            )));
            edge_ids.push(GraphEdgeId::new(Self::scoped_legacy_edge_id(
                workspace_id,
                format!("auto:related_to:{}:{}", related_id, vector_id.as_str()),
            )));
        }

        let chunk_edge_fields = [
            ("imports", EdgeKind::Imports),
            ("calls", EdgeKind::Calls),
            ("depends_on", EdgeKind::DependsOn),
            ("tests", EdgeKind::Tests),
            ("tested_by", EdgeKind::TestedBy),
        ];
        for (field, edge_kind) in chunk_edge_fields {
            for target_id in Self::metadata_string_values(metadata_json.as_ref(), field) {
                let target_node_id = Self::chunk_node_id(workspace_id, &VectorId::new(target_id));
                edge_ids.push(GraphEdgeId::new(Self::graph_edge_id(
                    edge_kind,
                    &chunk_node_id,
                    &target_node_id,
                )));
            }
        }

        for owner in Self::metadata_string_values(metadata_json.as_ref(), "owned_by") {
            let owner_node_id = Self::graph_node_id(workspace_id, NodeKind::Person, &owner);
            edge_ids.push(GraphEdgeId::new(Self::graph_edge_id(
                EdgeKind::OwnedBy,
                &chunk_node_id,
                &owner_node_id,
            )));
        }

        for commit in Self::metadata_string_values(metadata_json.as_ref(), "changed_by") {
            let commit_node_id = Self::graph_node_id(workspace_id, NodeKind::Commit, &commit);
            edge_ids.push(GraphEdgeId::new(Self::graph_edge_id(
                EdgeKind::ChangedBy,
                &chunk_node_id,
                &commit_node_id,
            )));
        }

        for (field, node_kind) in [
            ("document_id", NodeKind::Document),
            ("section_id", NodeKind::Section),
        ] {
            for structural_id in Self::metadata_string_values(metadata_json.as_ref(), field) {
                let structural_node_id =
                    Self::graph_node_id(workspace_id, node_kind, &structural_id);
                edge_ids.push(GraphEdgeId::new(Self::graph_edge_id(
                    EdgeKind::Contains,
                    &structural_node_id,
                    &chunk_node_id,
                )));
            }
        }

        for file in Self::metadata_string_values(metadata_json.as_ref(), "file") {
            let file_node_id = Self::graph_node_id(workspace_id, NodeKind::File, &file);
            edge_ids.push(GraphEdgeId::new(Self::graph_edge_id(
                EdgeKind::Contains,
                &file_node_id,
                &chunk_node_id,
            )));
        }

        for symbol in Self::metadata_string_values(metadata_json.as_ref(), "symbol") {
            let symbol_node_id = Self::graph_node_id(workspace_id, NodeKind::Function, &symbol);
            edge_ids.push(GraphEdgeId::new(Self::graph_edge_id(
                EdgeKind::Contains,
                &symbol_node_id,
                &chunk_node_id,
            )));
        }
        edge_ids
    }

    fn delete_graph_chunk(
        &self,
        vector_id: &VectorId,
        metadata: Option<&[u8]>,
    ) -> Result<(), akidb_graph::GraphError> {
        let Some(graph) = &self.graph_index else {
            return Ok(());
        };
        let metadata = metadata.map(String::from_utf8_lossy).unwrap_or_default();
        let workspace_id = Self::workspace_id_from_metadata(&metadata);
        let node_id = Self::chunk_node_id(&workspace_id, vector_id);
        graph.delete_node(&node_id)?;
        Ok(())
    }

    /// FIX BUG-052: Internal update implementation called while holding the per-key lock
    fn do_update_locked(
        &self,
        id: &str,
        vector_id: &VectorId,
        vector: &[f32],
        metadata: &[u8],
    ) -> Result<Response<UpdateResponse>, Status> {
        let start = Instant::now();

        // Get old internal ID atomically to reduce TOCTOU window
        // The mapping lookup returns None if deleted or never existed
        let old_internal_id = self
            .id_mapping
            .get_internal_id(vector_id)
            .map_err(Self::to_status)?;
        let old_metadata = self
            .id_mapping
            .get_vector(vector_id)
            .map_err(Self::to_status)?
            .map(|entry| entry.metadata);
        let status = if old_internal_id.is_some() {
            UpdateStatus::Updated
        } else {
            UpdateStatus::Created
        };

        // Insert new vector first (to minimize data loss window)
        let new_internal_id = self
            .index
            .insert(vector_id, vector)
            .map_err(Self::to_status)?;

        // Persist mapping and vector payload atomically.
        let mapping_result =
            self.id_mapping
                .upsert_with_vector(vector_id, new_internal_id, vector, metadata);

        if let Err(e) = mapping_result {
            // FIX BUG-HUNT-601: Log rollback failures instead of silently ignoring
            // Previously, rollback failures were discarded with `let _ =`, potentially
            // leaving orphan vectors in the index with no ID mapping.
            if let Err(rollback_err) = self.index.delete(new_internal_id) {
                tracing::error!(
                    vector_id = %vector_id,
                    internal_id = new_internal_id.0,
                    original_error = %e,
                    rollback_error = %rollback_err,
                    "Failed to rollback index insert after mapping failure - orphan vector may exist"
                );
            }
            return Err(Self::to_status(e));
        }

        // Now safe to delete old vector (after mapping updated)
        // FIX BUG-HUNT-601: Log failures when deleting old vector
        if let Some(old_id) = old_internal_id.filter(|old_id| *old_id != new_internal_id) {
            if let Err(delete_err) = self.index.delete(old_id) {
                tracing::warn!(
                    vector_id = %vector_id,
                    old_internal_id = old_id.0,
                    error = %delete_err,
                    "Failed to delete old vector during update - orphan vector may exist"
                );
            }
        }

        self.index_sql_metadata(vector_id, new_internal_id.0, metadata);
        self.index_graph_chunk(vector_id, metadata, old_metadata.as_deref())
            .map_err(|e| Status::internal(format!("graph projection failed: {e}")))?;

        let elapsed = start.elapsed();
        info!(
            "Update {} completed in {:?} with status {:?}",
            id, elapsed, status
        );

        Ok(Response::new(UpdateResponse {
            success: true,
            id: id.to_string(),
            status: status as i32,
            visibility: Some(VisibilityInfo {
                delete_visibility: "immediate".to_string(),
                insert_visibility: "within_100ms".to_string(),
            }),
        }))
    }
}

#[tonic::async_trait]
impl<I, S> Akidb for AkiDbService<I, S>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    #[instrument(skip(self, request))]
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let start = Instant::now();
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();

        debug!("Insert request for ID: {}", req.id);
        self.validate_request_collection(&req.collection)?;

        // Validate input
        if let Err(message) = Self::validate_vector_payload(&req.id, &req.vector) {
            return Err(Status::invalid_argument(message));
        }
        if let Err(message) = Self::validate_metadata_json(&req.metadata) {
            return Err(Status::invalid_argument(message));
        }

        let stamped_metadata = stamp_write_metadata(
            &req.metadata,
            &ctx,
            &self.acl,
            self.embedding_model_id.as_deref(),
        )
        .map_err(Status::invalid_argument)?;

        let vector_id = VectorId::new(&req.id);
        let vector: Vec<f32> = req.vector;
        let _mutation_guard = MutationLockGuard::try_acquire(&self.mutation_locks, req.id.clone())
            .ok_or_else(|| {
                Status::aborted(format!(
                    "Concurrent mutation in progress for ID: {}",
                    req.id
                ))
            })?;

        let old_metadata = self.ensure_insert_does_not_cross_workspace(&vector_id, &ctx)?;
        let old_internal_id = self
            .id_mapping
            .get_internal_id(&vector_id)
            .map_err(Self::to_status)?;

        // Insert into index
        let internal_id = self
            .index
            .insert(&vector_id, &vector)
            .map_err(Self::to_status)?;

        // Persist ID mapping and vector payload atomically. If this fails,
        // rollback the index insert.
        let mapping_result =
            self.id_mapping
                .upsert_with_vector(&vector_id, internal_id, &vector, &stamped_metadata);

        if let Err(e) = mapping_result {
            // FIX BUG-HUNT-601: Log rollback failures instead of silently ignoring
            if let Err(rollback_err) = self.index.delete(internal_id) {
                tracing::error!(
                    vector_id = %vector_id,
                    internal_id = internal_id.0,
                    original_error = %e,
                    rollback_error = %rollback_err,
                    "Failed to rollback index insert after mapping failure - orphan vector may exist"
                );
            }
            return Err(Self::to_status(e));
        }
        if let Some(old_id) = old_internal_id.filter(|old_id| *old_id != internal_id) {
            if let Err(e) = self.index.delete(old_id) {
                warn!(
                    vector_id = %vector_id,
                    old_internal_id = old_id.0,
                    error = %e,
                    "failed to remove replaced vector during insert upsert"
                );
            }
        }

        // Keep BM25, context packing, and persisted source text aligned with
        // upsert semantics. Empty text clears any previous source text.
        self.sync_source_text(&vector_id, &req.text);
        self.index_graph_chunk(&vector_id, &stamped_metadata, old_metadata.as_deref())
            .map_err(|e| Status::internal(format!("graph projection failed: {e}")))?;
        self.index_sql_metadata(&vector_id, internal_id.0, &stamped_metadata);

        let elapsed = start.elapsed();
        info!("Inserted vector {} in {:?}", req.id, elapsed);

        Ok(Response::new(InsertResponse {
            success: true,
            id: req.id,
            internal_id: internal_id.0,
            visibility: Some(VisibilityInfo {
                insert_visibility: "within_100ms".to_string(),
                delete_visibility: String::new(),
            }),
        }))
    }

    #[instrument(skip(self, request))]
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let start = Instant::now();
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();

        debug!("Search request, top_k: {}", req.top_k);

        self.validate_request_collection(&req.collection)?;
        Self::validate_search_controls(req.top_k, req.nprobe)?;
        Self::validate_query_vector(&req.query)?;

        let metadata_filter = self
            .compile_search_filter(&req.filter, req.tag_filter.clone(), &ctx)?
            .map(Arc::new);
        let top_k = req.top_k as usize;
        let search_k = self.filtered_search_k(top_k, metadata_filter.is_some());
        // Over-fetch when grouping so each group still has candidates after cut.
        let fetch_k = if req.group_by.trim().is_empty() {
            search_k
        } else {
            search_k.saturating_mul(4).max(search_k)
        };
        let params = SearchParams::new(fetch_k).with_nprobe(req.nprobe.unwrap_or(32));
        let params = self.attach_metadata_predicate(params, metadata_filter);

        let results = self
            .index
            .search(&req.query, &params)
            .map_err(Self::to_status)?;

        let elapsed = start.elapsed();
        let latency_us = elapsed.as_micros() as u64;

        let mapped: Vec<SearchResult> = results
            .into_iter()
            .map(|r| SearchResult {
                id: r.id.to_string(),
                score: r.score,
                metadata: self.load_metadata_string(&r.id),
            })
            .collect();
        let response_results = self.apply_score_and_group(
            mapped,
            top_k,
            req.score_threshold,
            &req.group_by,
            req.group_size,
        );

        info!(
            "Search returned {} results in {:?}",
            response_results.len(),
            elapsed
        );

        Ok(Response::new(SearchResponse {
            results: response_results,
            partial: false,
            missing_shards: vec![],
            coverage: 1.0,
            latency_us,
            // FIX BUG-HUNT-202: Use configurable SLO threshold instead of hardcoded 50ms
            within_slo: latency_us < self.slo_threshold_us,
            degraded_mode: false,
            context_pack: String::new(),
            serving_generation: None,
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let start = Instant::now();
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();

        debug!("Delete request for ID: {}", req.id);
        self.validate_request_collection(&req.collection)?;
        if let Err(message) = Self::validate_vector_id(&req.id) {
            return Err(Status::invalid_argument(message));
        }

        let vector_id = VectorId::new(&req.id);
        let _mutation_guard = MutationLockGuard::try_acquire(&self.mutation_locks, req.id.clone())
            .ok_or_else(|| {
                Status::aborted(format!(
                    "Concurrent mutation in progress for ID: {}",
                    req.id
                ))
            })?;
        let existing_metadata = self.ensure_existing_vector_access(&vector_id, &ctx)?;
        if existing_metadata.is_some() {
            self.delete_graph_chunk(&vector_id, existing_metadata.as_deref())
                .map_err(|e| Status::internal(format!("graph deletion failed: {e}")))?;
        }

        // Get internal ID and mark deleted
        let status = match self
            .id_mapping
            .mark_deleted(&vector_id)
            .map_err(Self::to_status)?
        {
            Some(internal_id) => {
                // Mark in tombstone
                self.index.delete(internal_id).map_err(Self::to_status)?;
                // Keep the lexical index and document store in sync.
                self.lexical.write().remove(&vector_id);
                self.documents.write().remove(&vector_id);
                if let Err(e) = self.id_mapping.delete_text(&vector_id) {
                    warn!(vector_id = %vector_id, error = %e, "failed to delete persisted text");
                }
                self.delete_sql_metadata(&vector_id);
                DeleteStatus::Deleted
            }
            None => {
                // Check if already deleted
                if self
                    .id_mapping
                    .exists(&vector_id)
                    .map_err(Self::to_status)?
                {
                    DeleteStatus::AlreadyDeleted
                } else {
                    DeleteStatus::NotFound
                }
            }
        };

        let elapsed = start.elapsed();
        info!(
            "Delete {} completed in {:?} with status {:?}",
            req.id, elapsed, status
        );

        Ok(Response::new(DeleteResponse {
            success: true,
            id: req.id,
            status: status as i32,
            visibility: "immediate".to_string(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        let ctx = self.request_auth_context(&request);
        let mut req = request.into_inner();

        debug!("Update request for ID: {}", req.id);
        self.validate_request_collection(&req.collection)?;

        // Validate input
        if req.id.is_empty() {
            return Err(Status::invalid_argument("Vector ID cannot be empty"));
        }
        if req.id.len() > 1024 {
            return Err(Status::invalid_argument(
                "Vector ID exceeds maximum length of 1024",
            ));
        }
        if req.vector.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(Status::invalid_argument(
                "Vector contains NaN or Infinity values",
            ));
        }
        if req.vector.is_empty() {
            return Err(Status::invalid_argument("Vector cannot be empty"));
        }
        if let Err(message) = Self::validate_metadata_json(&req.metadata) {
            return Err(Status::invalid_argument(message));
        }

        // Stamp workspace / embedding model after JSON validation (same as insert).
        req.metadata = stamp_write_metadata(
            &req.metadata,
            &ctx,
            &self.acl,
            self.embedding_model_id.as_deref(),
        )
        .map_err(Status::invalid_argument)?;

        let vector_id = VectorId::new(&req.id);
        let _mutation_guard = MutationLockGuard::try_acquire(&self.mutation_locks, req.id.clone())
            .ok_or_else(|| {
                Status::aborted(format!(
                    "Concurrent mutation in progress for ID: {}",
                    req.id
                ))
            })?;
        self.ensure_existing_vector_access(&vector_id, &ctx)?;

        // Perform the update operation - lock is held by guard
        // Guard will release lock automatically when this function returns (or panics)
        self.do_update_locked(&req.id, &vector_id, &req.vector, &req.metadata)
    }

    #[instrument(skip(self, request))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();
        self.validate_request_collection(&req.collection)?;
        if let Err(message) = Self::validate_vector_id(&req.id) {
            return Err(Status::invalid_argument(message));
        }

        let vector_id = VectorId::new(&req.id);
        self.ensure_existing_vector_access(&vector_id, &ctx)?;

        // Get internal ID
        let internal_id = self
            .id_mapping
            .get_internal_id(&vector_id)
            .map_err(Self::to_status)?
            .ok_or_else(|| Status::not_found(format!("Vector not found: {}", req.id)))?;

        let stored_vector = self
            .id_mapping
            .get_vector(&vector_id)
            .map_err(Self::to_status)?;

        // Get vector from the hot index first, then durable storage. The
        // fallback matters after process restart while the index is rebuilding.
        let vector = match self
            .index
            .get_vector(internal_id)
            .map_err(Self::to_status)?
        {
            Some(vector) => vector,
            None => stored_vector
                .as_ref()
                .map(|entry| entry.vector.clone())
                .ok_or_else(|| Status::not_found(format!("Vector not found: {}", req.id)))?,
        };
        let metadata = stored_vector
            .as_ref()
            .map(|entry| String::from_utf8_lossy(&entry.metadata).into_owned())
            .unwrap_or_default();

        Ok(Response::new(GetResponse {
            id: req.id,
            vector,
            metadata,
            found: true,
            serving_generation: None,
        }))
    }

    #[instrument(skip(self, _request))]
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let stats = self.index.stats();

        Ok(Response::new(HealthResponse {
            healthy: self.index.is_ready(),
            ready: self.index.is_ready() && !self.index.is_rebuilding(),
            message: if self.index.is_rebuilding() {
                "Rebuild in progress".to_string()
            } else if !self.index.is_ready() {
                "Index not ready".to_string()
            } else {
                "OK".to_string()
            },
            total_vectors: stats.total_vectors,
            active_vectors: stats.active_vectors,
            using_gpu: stats.using_gpu,
            serving_generation: None,
        }))
    }

    #[instrument(skip(self, request))]
    async fn insert_batch(
        &self,
        request: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertBatchResponse>, Status> {
        let start = Instant::now();
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();

        debug!("Insert batch request for {} vectors", req.vectors.len());
        self.validate_request_collection(&req.collection)?;
        Self::validate_unique_batch_ids(&req.vectors)?;

        let mut inserted_count = 0u32;
        let mut failed_ids = Vec::new();

        for mut vector in req.vectors {
            if let Err(message) = Self::validate_vector_payload(&vector.id, &vector.embedding) {
                warn!(
                    "Batch insert: ID {} failed validation: {}",
                    vector.id, message
                );
                failed_ids.push(vector.id);
                continue;
            }
            if let Err(message) = Self::validate_metadata_json(&vector.metadata) {
                warn!(
                    "Batch insert: ID {} failed metadata validation: {}",
                    vector.id, message
                );
                failed_ids.push(vector.id);
                continue;
            }

            vector.metadata = match stamp_write_metadata(
                &vector.metadata,
                &ctx,
                &self.acl,
                self.embedding_model_id.as_deref(),
            ) {
                Ok(metadata) => metadata,
                Err(message) => {
                    warn!(
                        "Batch insert: ID {} failed workspace metadata validation: {}",
                        vector.id, message
                    );
                    failed_ids.push(vector.id);
                    continue;
                }
            };

            let vector_id = VectorId::new(&vector.id);
            let Some(_mutation_guard) =
                MutationLockGuard::try_acquire(&self.mutation_locks, vector.id.clone())
            else {
                warn!(
                    "Batch insert: ID {} has a concurrent mutation in progress",
                    vector.id
                );
                failed_ids.push(vector.id);
                continue;
            };
            let old_metadata = match self.ensure_insert_does_not_cross_workspace(&vector_id, &ctx) {
                Ok(metadata) => metadata,
                Err(e) => {
                    warn!(
                        "Batch insert: ID {} failed workspace ownership check: {}",
                        vector.id, e
                    );
                    failed_ids.push(vector.id);
                    continue;
                }
            };
            let old_internal_id = match self.id_mapping.get_internal_id(&vector_id) {
                Ok(internal_id) => internal_id,
                Err(e) => {
                    warn!(
                        "Batch insert: ID {} failed internal id lookup: {}",
                        vector.id, e
                    );
                    failed_ids.push(vector.id);
                    continue;
                }
            };

            match self.index.insert(&vector_id, &vector.embedding) {
                Ok(internal_id) => {
                    match self.id_mapping.upsert_with_vector(
                        &vector_id,
                        internal_id,
                        &vector.embedding,
                        &vector.metadata,
                    ) {
                        Ok(_) => {
                            if let Some(old_id) =
                                old_internal_id.filter(|old_id| *old_id != internal_id)
                            {
                                if let Err(e) = self.index.delete(old_id) {
                                    warn!(
                                        vector_id = %vector.id,
                                        old_internal_id = old_id.0,
                                        error = %e,
                                        "Batch insert: failed to remove replaced vector"
                                    );
                                }
                            }
                            self.sync_source_text(&vector_id, &vector.text);
                            if let Err(e) = self.index_graph_chunk(
                                &vector_id,
                                &vector.metadata,
                                old_metadata.as_deref(),
                            ) {
                                warn!(
                                    vector_id = %vector.id,
                                    error = %e,
                                    "Batch insert: graph projection failed"
                                );
                                failed_ids.push(vector.id);
                                continue;
                            }
                            self.index_sql_metadata(&vector_id, internal_id.0, &vector.metadata);
                            inserted_count += 1;
                        }
                        Err(e) => {
                            // FIX BUG-046, BUG-HUNT-601: Rollback and log failures
                            if let Err(rollback_err) = self.index.delete(internal_id) {
                                tracing::error!(
                                    vector_id = %vector.id,
                                    internal_id = internal_id.0,
                                    original_error = %e,
                                    rollback_error = %rollback_err,
                                    "Batch insert: rollback failed after mapping create failure - orphan vector may exist"
                                );
                            }
                            warn!(
                                "Batch insert: ID {} failed during id_mapping.create: {}",
                                vector.id, e
                            );
                            failed_ids.push(vector.id);
                        }
                    }
                }
                Err(e) => {
                    // FIX BUG-074: Log error details for failed inserts
                    warn!(
                        "Batch insert: ID {} failed during index.insert: {}",
                        vector.id, e
                    );
                    failed_ids.push(vector.id);
                }
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Batch insert: {} succeeded, {} failed in {:?}",
            inserted_count,
            failed_ids.len(),
            elapsed
        );

        Ok(Response::new(InsertBatchResponse {
            success: failed_ids.is_empty(),
            inserted_count,
            failed_ids,
        }))
    }

    #[instrument(skip(self, request))]
    async fn search_batch(
        &self,
        request: Request<SearchBatchRequest>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        let start = Instant::now();
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();

        debug!("Search batch request for {} queries", req.queries.len());

        self.validate_request_collection(&req.collection)?;
        Self::validate_search_controls(req.top_k, req.nprobe)?;

        let metadata_filter = self.compile_search_filter(&[], None, &ctx)?.map(Arc::new);
        let params = SearchParams::new(req.top_k as usize).with_nprobe(req.nprobe.unwrap_or(32));
        let params = self.attach_metadata_predicate(params, metadata_filter);

        let mut results = Vec::with_capacity(req.queries.len());

        for query in req.queries {
            Self::validate_query_vector(&query.vector)?;
            let search_start = Instant::now();

            let search_results = self
                .index
                .search(&query.vector, &params)
                .map_err(Self::to_status)?;

            let latency_us = search_start.elapsed().as_micros() as u64;

            let response_results: Vec<SearchResult> = search_results
                .into_iter()
                .map(|r| SearchResult {
                    id: r.id.to_string(),
                    score: r.score,
                    metadata: self.load_metadata_string(&r.id),
                })
                .collect();

            results.push(SearchResponse {
                results: response_results,
                partial: false,
                missing_shards: vec![],
                coverage: 1.0,
                latency_us,
                // FIX BUG-HUNT-202: Use configurable SLO threshold instead of hardcoded 50ms
                within_slo: latency_us < self.slo_threshold_us,
                degraded_mode: false,
                context_pack: String::new(),
                serving_generation: None,
            });
        }

        let elapsed = start.elapsed();
        info!("Batch search: {} queries in {:?}", results.len(), elapsed);

        Ok(Response::new(SearchBatchResponse { results }))
    }

    async fn get_cluster_state(
        &self,
        _request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        // Shard servers don't provide cluster state - only coordinators do
        Err(Status::unimplemented(
            "GetClusterState is only available on coordinator nodes",
        ))
    }

    #[instrument(skip(self, request))]
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req = request.into_inner();
        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(Status::invalid_argument("collection name cannot be empty"));
        }
        let dimensions = if req.dimensions == 0 {
            self.index.dimensions() as u32
        } else {
            req.dimensions
        };
        if name == self.collection && dimensions as usize != self.index.dimensions() {
            return Err(Status::invalid_argument(format!(
                "dimensions {dimensions} do not match active index dimensions {}",
                self.index.dimensions()
            )));
        }
        let metric = if req.metric.trim().is_empty() {
            "cosine".to_string()
        } else {
            req.metric.trim().to_ascii_lowercase()
        };
        let precision = if req.vector_precision.trim().is_empty() {
            "f32".to_string()
        } else {
            req.vector_precision.trim().to_ascii_lowercase()
        };
        let embedding_model_id = if req.embedding_model_id.trim().is_empty() {
            self.embedding_model_id.clone().unwrap_or_default()
        } else {
            req.embedding_model_id.trim().to_string()
        };
        let chunk_strategy = if req.chunk_strategy.trim().is_empty() {
            "fixed".to_string()
        } else {
            req.chunk_strategy.trim().to_string()
        };

        let meta = CollectionMeta {
            name: name.clone(),
            dimensions,
            metric,
            embedding_model_id,
            vector_precision: precision,
            chunk_strategy,
        };
        match self.collections.create(meta) {
            Ok(_) => Ok(Response::new(CreateCollectionResponse {
                success: true,
                name,
                message: "created".to_string(),
            })),
            Err(e) if e.contains("already exists") => Ok(Response::new(CreateCollectionResponse {
                success: false,
                name,
                message: e,
            })),
            Err(e) => Err(Status::invalid_argument(e)),
        }
    }

    #[instrument(skip(self, request))]
    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        let name = request.into_inner().name;
        match self.collections.get(&name) {
            Some(meta) => Ok(Response::new(GetCollectionResponse {
                found: true,
                collection: Some(self.collection_info(&meta)),
            })),
            None => Ok(Response::new(GetCollectionResponse {
                found: false,
                collection: None,
            })),
        }
    }

    #[instrument(skip(self, _request))]
    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let collections = self
            .collections
            .list()
            .iter()
            .map(|m| self.collection_info(m))
            .collect();
        Ok(Response::new(ListCollectionsResponse { collections }))
    }

    #[instrument(skip(self, request))]
    async fn drop_collection(
        &self,
        request: Request<DropCollectionRequest>,
    ) -> Result<Response<DropCollectionResponse>, Status> {
        let name = request.into_inner().name;
        if name == self.collection {
            return Err(Status::failed_precondition(
                "cannot drop the active shard collection",
            ));
        }
        match self.collections.remove(&name) {
            Ok(()) => Ok(Response::new(DropCollectionResponse {
                success: true,
                message: "dropped".to_string(),
            })),
            Err(e) => Ok(Response::new(DropCollectionResponse {
                success: false,
                message: e,
            })),
        }
    }

    #[instrument(skip(self, request))]
    async fn text_search(
        &self,
        request: Request<TextSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let start = Instant::now();
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();

        if req.text.trim().is_empty() {
            return Err(Status::invalid_argument("Text cannot be empty"));
        }
        self.validate_request_collection(&req.collection)?;
        Self::validate_search_controls(req.top_k, req.nprobe)?;
        Self::validate_text_search_options(&req)?;

        let metadata_filter = self
            .compile_search_filter(&req.filter, req.tag_filter.clone(), &ctx)?
            .map(Arc::new);

        let requested_mode = Self::requested_text_retrieval_mode(&req)?;
        let mut planner_input = PlannerInput::new(req.text.clone())
            .with_pack(req.pack)
            .with_metadata_filter(metadata_filter.is_some());
        if let Some(mode) = requested_mode {
            planner_input = planner_input.with_requested_mode(mode);
        }
        let planner_trace = plan_query(&planner_input);
        if matches!(planner_trace.mode, RetrievalMode::StructuredSql) {
            return self.sql_metadata_text_search(&req, start, metadata_filter.as_deref(), &ctx);
        }
        debug!(
            mode = ?planner_trace.mode,
            graph_enabled = planner_trace.graph_enabled,
            reasons = ?planner_trace.reasons,
            "TextSearch planner trace"
        );

        let top_k = req.top_k as usize;
        // Over-fetch a candidate pool when a later stage (fusion, rerank,
        // diversity) needs room to reorder; otherwise fetch exactly top_k.
        let use_dense = planner_trace.vector_weight > 0.0;
        let use_lexical = planner_trace.lexical_weight > 0.0;
        let needs_pool = (use_dense && use_lexical) || req.rerank || req.diversity;
        let mut search_k = if needs_pool {
            top_k.saturating_mul(4).clamp(top_k, top_k.max(200))
        } else {
            top_k
        };
        search_k = self.filtered_search_k(search_k, metadata_filter.is_some());

        // Dense stage.
        let dense = if use_dense {
            let provider = self.embedding_provider.as_ref().ok_or_else(|| {
                Status::unavailable(
                    "TextSearch vector retrieval requires an embedding provider to be configured",
                )
            })?;
            let query_vector = provider
                .embed_text(&req.text)
                .map_err(|e| Status::internal(format!("Embedding generation failed: {}", e)))?;
            Self::validate_embedding_vector(&query_vector)?;

            debug!(
                text_len = req.text.len(),
                embedding_dim = query_vector.len(),
                "TextSearch embedding generated"
            );

            let params = SearchParams::new(search_k).with_nprobe(req.nprobe.unwrap_or(32));
            let params = self.attach_metadata_predicate(params, metadata_filter.clone());

            // Drop only truly non-finite scores (NaN, inf, -inf) from the
            // raw index output. Zero-score hits are kept here so dense-only
            // and lexical-fusion paths can still surface them.
            let mut dense_hits = self
                .index
                .search(&query_vector, &params)
                .map_err(Self::to_status)?;
            dense_hits.retain(|r| r.score.is_finite());
            dense_hits
        } else {
            Vec::new()
        };

        let lexical = if use_lexical {
            let lexical = self.lexical.read();
            let lexical_k = if metadata_filter.is_some() {
                lexical.len().max(search_k)
            } else {
                search_k
            };
            lexical.search(&req.text, lexical_k)
        } else {
            Vec::new()
        };

        // Base ranked list: hybrid fusion (dense + lexical via RRF) or dense-only.
        // An empty lexical index degrades hybrid cleanly to dense ranking.
        let mut ranked: Vec<ScoredId> = if use_dense && use_lexical && !lexical.is_empty() {
            // Only allow zero/non-positive dense scores into RRF when the same id
            // also appears in the lexical results (proving a real non-dense signal).
            // This prevents orthogonal dense hits with no lexical overlap from
            // gaming fusion by index position alone.
            let lexical_ids: HashSet<_> = lexical.iter().map(|s| s.id.clone()).collect();
            let dense_scored: Vec<ScoredId> = dense
                .iter()
                .filter(|r| r.score > 0.0 || lexical_ids.contains(&r.id))
                .map(|r| ScoredId::new(r.id.clone(), r.score))
                .collect();
            let fuser = HybridFuser::new().with_weights(
                req.dense_weight.unwrap_or(planner_trace.vector_weight),
                req.lexical_weight.unwrap_or(planner_trace.lexical_weight),
            );
            fuser.fuse(&dense_scored, &lexical, search_k)
        } else if use_lexical && !lexical.is_empty() {
            lexical
        } else {
            dense
                .iter()
                .map(|r| ScoredId::new(r.id.clone(), r.score))
                .collect()
        };

        if let Some(metadata_filter) = &metadata_filter {
            ranked.retain(|s| self.metadata_matches_filter(&s.id, metadata_filter));
        }

        if planner_trace.graph_enabled {
            if let Some(graph) = &self.graph_index {
                let mut seen: HashSet<VectorId> = ranked.iter().map(|s| s.id.clone()).collect();
                let graph_limit = top_k.saturating_mul(2).max(1);
                let graph_seed_score = ranked.first().map(|s| s.score + 0.0001).unwrap_or(1.0);

                for seed_node in Self::graph_seed_nodes_from_query(&req.text, &ctx.workspace_id) {
                    let graph_request = RelatedChunksRequest::new(seed_node.clone())
                        .with_max_depth(planner_trace.graph_depth.max(1))
                        .with_per_hop_limit(graph_limit.saturating_mul(8))
                        .with_limit(graph_limit);
                    match graph.related_chunks_with_depth(graph_request) {
                        Ok(chunks) => {
                            for chunk in chunks {
                                let id = chunk.vector_id;
                                if !seen.insert(id.clone()) {
                                    if let Some(existing) = ranked.iter_mut().find(|s| s.id == id) {
                                        existing.score = existing.score.max(graph_seed_score);
                                    }
                                    continue;
                                }
                                if let Some(metadata_filter) = &metadata_filter {
                                    if !self.metadata_matches_filter(&id, metadata_filter) {
                                        continue;
                                    }
                                }
                                ranked.push(ScoredId::new(id, graph_seed_score));
                            }
                        }
                        Err(e) => {
                            warn!(seed = %seed_node, error = %e, "graph query seed expansion failed");
                        }
                    }
                }

                let seeds = ranked.clone();
                for seed in seeds {
                    let seed_node = Self::chunk_node_id(&ctx.workspace_id, &seed.id);
                    let graph_request = RelatedChunksRequest::new(seed_node.clone())
                        .with_max_depth(planner_trace.graph_depth.max(1))
                        .with_per_hop_limit(graph_limit.saturating_mul(8))
                        .with_limit(graph_limit);
                    match graph.related_chunks_with_depth(graph_request) {
                        Ok(chunks) => {
                            for chunk in chunks {
                                let id = chunk.vector_id;
                                if !seen.insert(id.clone()) {
                                    continue;
                                }
                                if let Some(metadata_filter) = &metadata_filter {
                                    if !self.metadata_matches_filter(&id, metadata_filter) {
                                        continue;
                                    }
                                }
                                ranked.push(ScoredId::new(id, seed.score * 0.85));
                            }
                        }
                        Err(e) => {
                            warn!(seed = %seed_node, error = %e, "graph result expansion failed");
                        }
                    }
                }
                ranked.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.id.as_str().cmp(b.id.as_str()))
                });
            }
        }

        // Optional reranking (RET-005): re-score candidates by query-text
        // relevance over their stored source text.
        if req.rerank {
            let docs = self.documents.read();
            let items: Vec<RerankItem> = ranked
                .iter()
                .map(|s| {
                    let text = docs.get(&s.id).cloned().unwrap_or_default();
                    RerankItem::new(s.id.clone(), text, s.score)
                })
                .collect();
            drop(docs);
            ranked = LexicalOverlapReranker.rerank(&req.text, items);
        }

        // Optional diversity (RET-006): MMR reselection over candidate embeddings
        // to suppress near-duplicate results.
        if req.diversity {
            let lambda = req.mmr_lambda.unwrap_or(0.5);
            let items: Vec<MmrItem> = ranked
                .iter()
                .filter_map(|s| {
                    self.id_mapping
                        .get_vector(&s.id)
                        .ok()
                        .flatten()
                        .map(|e| MmrItem::new(s.id.clone(), s.score, e.vector))
                })
                .collect();
            if !items.is_empty() {
                ranked = mmr(&items, lambda, ranked.len());
            }
        }

        // Keep a larger pool before score/group cuts so threshold/group_by can select.
        let pool_cap = top_k.saturating_mul(8).max(top_k);
        ranked.truncate(pool_cap);
        let mapped: Vec<SearchResult> = ranked
            .iter()
            .map(|s| SearchResult {
                metadata: self.load_metadata_string(&s.id),
                id: s.id.to_string(),
                score: s.score,
            })
            .collect();
        let response_results = self.apply_score_and_group(
            mapped,
            top_k,
            req.score_threshold,
            &req.group_by,
            req.group_size,
        );

        // Optionally assemble a source-grounded, citation-bearing context pack
        // (PACK-*). Matched child chunks are expanded to their parent context
        // (CHUNK-003) via the `parent_id` metadata convention, deduped by parent,
        // then assembled within the token budget.
        let context_pack = if req.pack {
            self.build_context_pack(
                &response_results,
                metadata_filter.as_deref(),
                top_k,
                req.pack_token_budget.unwrap_or(1024) as usize,
            )
        } else {
            String::new()
        };

        let elapsed = start.elapsed();
        let latency_us = elapsed.as_micros() as u64;

        info!(
            "TextSearch for '{}' returned {} results in {:?}",
            Self::preview_text(&req.text, 50),
            response_results.len(),
            elapsed
        );

        Ok(Response::new(SearchResponse {
            results: response_results,
            partial: false,
            missing_shards: vec![],
            coverage: 1.0,
            latency_us,
            within_slo: latency_us < self.slo_threshold_us,
            degraded_mode: false,
            context_pack,
            serving_generation: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Query;
    use akidb_faiss::MockIndex;
    use akidb_sql::SqliteMetadataIndex;
    use akidb_storage::RocksDbBackend;
    use tempfile::TempDir;
    use tonic::Code;

    fn test_service() -> (AkiDbService<MockIndex, RocksDbBackend>, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        let id_mapping = Arc::new(IdMapping::new(storage, "test"));
        let index = Arc::new(MockIndex::new(2, 16));
        (AkiDbService::new(index, id_mapping, "test"), dir)
    }

    struct BadEmbedder;

    impl EmbeddingProvider for BadEmbedder {
        fn embed_text(&self, _text: &str) -> std::result::Result<Vec<f32>, String> {
            Ok(vec![f32::NAN, 0.0])
        }

        fn embedding_dimensions(&self) -> usize {
            2
        }
    }

    async fn insert_with_metadata(service: &AkiDbService<MockIndex, RocksDbBackend>) {
        let metadata = br#"{"document_key":"reports/annual.pdf","title":"Annual Report"}"#.to_vec();

        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "doc1:0".to_string(),
                vector: vec![1.0, 0.0],
                metadata,
                text: "annual report text".to_string(),
            }))
            .await
            .unwrap();
    }

    fn bm25_text_search_request(text: &str, top_k: u32) -> TextSearchRequest {
        TextSearchRequest {
            collection: "test".to_string(),
            text: text.to_string(),
            top_k,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: "bm25".to_string(),
            score_threshold: None,
            group_by: String::new(),
            group_size: None,
        }
    }

    #[test]
    fn test_preview_text_does_not_split_utf8_codepoints() {
        let preview = AkiDbService::<MockIndex, RocksDbBackend>::preview_text("測試文本", 3);

        assert_eq!(preview, "測試文");
    }

    async fn insert_text(
        service: &AkiDbService<MockIndex, RocksDbBackend>,
        id: &str,
        vector: Vec<f32>,
        text: &str,
    ) {
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: id.to_string(),
                vector,
                metadata: br#"{"source":"unit-test"}"#.to_vec(),
                text: text.to_string(),
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_dense_search_returns_durable_metadata() {
        let (service, _dir) = test_service();
        insert_with_metadata(&service).await;

        let response = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![1.0, 0.0],
                top_k: 1,
                nprobe: None,
                filter: vec![],
                tag_filter: None,
                score_threshold: None,
                group_by: String::new(),
                group_size: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, "doc1:0");
        assert!(response.results[0].metadata.contains("Annual Report"));
        assert!(response.results[0].metadata.contains("reports/annual.pdf"));
    }

    #[tokio::test]
    async fn test_dense_search_applies_legacy_metadata_filter_before_top_k() {
        let (service, _dir) = test_service();
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "tenant-a".to_string(),
                vector: vec![1.0, 0.0],
                metadata: br#"{"tenant":"a"}"#.to_vec(),
                text: "tenant a".to_string(),
            }))
            .await
            .unwrap();
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "tenant-b".to_string(),
                vector: vec![0.0, 1.0],
                metadata: br#"{"tenant":"b"}"#.to_vec(),
                text: "tenant b".to_string(),
            }))
            .await
            .unwrap();

        let response = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![0.0, 1.0],
                top_k: 1,
                nprobe: None,
                filter: br#"{"tenant":"a"}"#.to_vec(),
                tag_filter: None,
                score_threshold: None,
                group_by: String::new(),
                group_size: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, "tenant-a");
    }

    #[tokio::test]
    async fn test_batch_search_returns_durable_metadata() {
        let (service, _dir) = test_service();
        insert_with_metadata(&service).await;

        let response = service
            .search_batch(Request::new(SearchBatchRequest {
                collection: "test".to_string(),
                queries: vec![Query {
                    vector: vec![1.0, 0.0],
                }],
                top_k: 1,
                nprobe: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].results.len(), 1);
        assert_eq!(response.results[0].results[0].id, "doc1:0");
        assert!(response.results[0].results[0]
            .metadata
            .contains("Annual Report"));
    }

    #[tokio::test]
    async fn test_update_keeps_upserted_vector_searchable() {
        let (service, _dir) = test_service();
        insert_text(&service, "doc1", vec![1.0, 0.0], "original searchable text").await;

        service
            .update(Request::new(UpdateRequest {
                collection: "test".to_string(),
                id: "doc1".to_string(),
                vector: vec![0.0, 1.0],
                metadata: br#"{"title":"Updated"}"#.to_vec(),
            }))
            .await
            .unwrap();

        let response = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![0.0, 1.0],
                top_k: 1,
                nprobe: None,
                filter: vec![],
                tag_filter: None,
                score_threshold: None,
                group_by: String::new(),
                group_size: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, "doc1");
        assert!(response.results[0].metadata.contains("Updated"));

        let stats = service.index_stats();
        assert_eq!(stats.total_vectors, 1);
        assert_eq!(stats.active_vectors, 1);
        assert_eq!(stats.tombstoned_vectors, 0);
    }

    #[tokio::test]
    async fn test_dense_search_rejects_zero_nprobe() {
        let (service, _dir) = test_service();
        let result = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![1.0, 0.0],
                top_k: 1,
                nprobe: Some(0),
                filter: vec![],
                tag_filter: None,
                score_threshold: None,
                group_by: String::new(),
                group_size: None,
            }))
            .await;

        let status = result.expect_err("zero nprobe should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("nprobe"));
    }

    #[tokio::test]
    async fn test_dense_search_rejects_nan_query() {
        let (service, _dir) = test_service();
        let result = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![f32::NAN, 0.0],
                top_k: 1,
                nprobe: None,
                filter: vec![],
                tag_filter: None,
                score_threshold: None,
                group_by: String::new(),
                group_size: None,
            }))
            .await;

        let status = result.expect_err("NaN query should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("NaN"));
    }

    #[tokio::test]
    async fn test_batch_search_rejects_zero_nprobe_before_queries() {
        let (service, _dir) = test_service();
        let result = service
            .search_batch(Request::new(SearchBatchRequest {
                collection: "test".to_string(),
                queries: vec![],
                top_k: 1,
                nprobe: Some(0),
            }))
            .await;

        let status = result.expect_err("zero nprobe should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("nprobe"));
    }

    #[tokio::test]
    async fn test_batch_search_rejects_empty_query() {
        let (service, _dir) = test_service();
        let result = service
            .search_batch(Request::new(SearchBatchRequest {
                collection: "test".to_string(),
                queries: vec![Query { vector: vec![] }],
                top_k: 1,
                nprobe: None,
            }))
            .await;

        let status = result.expect_err("empty query should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Query vector cannot be empty"));
    }

    #[tokio::test]
    async fn test_insert_rejects_empty_id() {
        let (service, _dir) = test_service();
        let result = service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: String::new(),
                vector: vec![1.0, 0.0],
                metadata: vec![],
                text: String::new(),
            }))
            .await;

        let status = result.expect_err("empty vector ID should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Vector ID cannot be empty"));
    }

    #[tokio::test]
    async fn test_insert_rejects_invalid_json_metadata() {
        let (service, _dir) = test_service();
        let result = service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "bad-metadata".to_string(),
                vector: vec![1.0, 0.0],
                metadata: b"not-json".to_vec(),
                text: String::new(),
            }))
            .await;

        let status = result.expect_err("invalid JSON metadata should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Metadata must be valid JSON"));
        assert_eq!(service.index_stats().active_vectors, 0);
    }

    #[tokio::test]
    async fn test_update_rejects_invalid_json_metadata() {
        let (service, _dir) = test_service();
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "doc1".to_string(),
                vector: vec![1.0, 0.0],
                metadata: br#"{"title":"Original"}"#.to_vec(),
                text: String::new(),
            }))
            .await
            .unwrap();

        let result = service
            .update(Request::new(UpdateRequest {
                collection: "test".to_string(),
                id: "doc1".to_string(),
                vector: vec![0.0, 1.0],
                metadata: b"not-json".to_vec(),
            }))
            .await;

        let status = result.expect_err("invalid JSON metadata should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Metadata must be valid JSON"));
    }

    #[tokio::test]
    async fn test_insert_batch_rejects_invalid_json_metadata() {
        let (service, _dir) = test_service();
        let result = service
            .insert_batch(Request::new(InsertBatchRequest {
                collection: "test".to_string(),
                vectors: vec![
                    Vector {
                        id: "valid".to_string(),
                        embedding: vec![1.0, 0.0],
                        metadata: br#"{"ok":true}"#.to_vec(),
                        text: String::new(),
                    },
                    Vector {
                        id: "bad".to_string(),
                        embedding: vec![0.0, 1.0],
                        metadata: b"not-json".to_vec(),
                        text: String::new(),
                    },
                ],
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!result.success);
        assert_eq!(result.inserted_count, 1);
        assert_eq!(result.failed_ids, vec!["bad".to_string()]);
        assert_eq!(service.index_stats().active_vectors, 1);
    }

    #[tokio::test]
    async fn test_get_rejects_empty_id() {
        let (service, _dir) = test_service();
        let result = service
            .get(Request::new(GetRequest {
                collection: "test".to_string(),
                id: String::new(),
            }))
            .await;

        let status = result.expect_err("empty vector ID should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Vector ID cannot be empty"));
    }

    #[tokio::test]
    async fn test_delete_rejects_empty_id() {
        let (service, _dir) = test_service();
        let result = service
            .delete(Request::new(DeleteRequest {
                collection: "test".to_string(),
                id: String::new(),
            }))
            .await;

        let status = result.expect_err("empty vector ID should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("Vector ID cannot be empty"));
    }

    fn assert_collection_mismatch(status: Status) {
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("collection"));
    }

    #[tokio::test]
    async fn test_rpc_entrypoints_reject_wrong_collection() {
        let (service, _dir) = test_service();

        assert_collection_mismatch(
            service
                .insert(Request::new(InsertRequest {
                    collection: "other".to_string(),
                    id: "doc1".to_string(),
                    vector: vec![1.0, 0.0],
                    metadata: vec![],
                    text: "needle".to_string(),
                }))
                .await
                .expect_err("insert should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .insert_batch(Request::new(InsertBatchRequest {
                    collection: "other".to_string(),
                    vectors: vec![],
                }))
                .await
                .expect_err("insert_batch should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .search(Request::new(SearchRequest {
                    collection: "other".to_string(),
                    query: vec![1.0, 0.0],
                    top_k: 1,
                    nprobe: None,
                    filter: vec![],
                    tag_filter: None,
                    score_threshold: None,
                    group_by: String::new(),
                    group_size: None,
                }))
                .await
                .expect_err("search should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .search_batch(Request::new(SearchBatchRequest {
                    collection: "other".to_string(),
                    queries: vec![],
                    top_k: 1,
                    nprobe: None,
                }))
                .await
                .expect_err("search_batch should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .text_search(Request::new(TextSearchRequest {
                    collection: "other".to_string(),
                    text: "needle".to_string(),
                    top_k: 1,
                    nprobe: None,
                    hybrid: false,
                    dense_weight: None,
                    lexical_weight: None,
                    pack: false,
                    pack_token_budget: None,
                    rerank: false,
                    diversity: false,
                    mmr_lambda: None,
                    filter: vec![],
                    tag_filter: None,
                    retrieval_mode: "bm25".to_string(),
                    score_threshold: None,
                    group_by: String::new(),
                    group_size: None,
                }))
                .await
                .expect_err("text_search should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .delete(Request::new(DeleteRequest {
                    collection: "other".to_string(),
                    id: "doc1".to_string(),
                }))
                .await
                .expect_err("delete should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .update(Request::new(UpdateRequest {
                    collection: "other".to_string(),
                    id: "doc1".to_string(),
                    vector: vec![1.0, 0.0],
                    metadata: vec![],
                }))
                .await
                .expect_err("update should reject wrong collection"),
        );

        assert_collection_mismatch(
            service
                .get(Request::new(GetRequest {
                    collection: "other".to_string(),
                    id: "doc1".to_string(),
                }))
                .await
                .expect_err("get should reject wrong collection"),
        );
    }

    #[tokio::test]
    async fn test_insert_batch_rejects_duplicate_ids_before_writes() {
        let (service, _dir) = test_service();

        let result = service
            .insert_batch(Request::new(InsertBatchRequest {
                collection: "test".to_string(),
                vectors: vec![
                    Vector {
                        id: "dup".to_string(),
                        embedding: vec![1.0, 0.0],
                        metadata: vec![],
                        text: "first".to_string(),
                    },
                    Vector {
                        id: "dup".to_string(),
                        embedding: vec![0.0, 1.0],
                        metadata: vec![],
                        text: "second".to_string(),
                    },
                ],
            }))
            .await;

        let status = result.expect_err("duplicate batch IDs should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("duplicate"));
        assert_eq!(service.index_stats().active_vectors, 0);
    }

    #[tokio::test]
    async fn test_bm25_text_search_uses_inserted_source_text_without_embedding_provider() {
        let (service, _dir) = test_service();
        insert_text(
            &service,
            "doc-keyword",
            vec![1.0, 0.0],
            "rare_contract_keyword amount 2025",
        )
        .await;
        insert_text(&service, "doc-other", vec![0.0, 1.0], "unrelated notes").await;

        let response = service
            .text_search(Request::new(bm25_text_search_request(
                "rare_contract_keyword",
                10,
            )))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].id, "doc-keyword");
    }

    #[tokio::test]
    async fn test_text_search_rejects_zero_nprobe() {
        let (service, _dir) = test_service();
        let mut request = bm25_text_search_request("rare_contract_keyword", 10);
        request.nprobe = Some(0);

        let result = service.text_search(Request::new(request)).await;

        let status = result.expect_err("zero nprobe should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("nprobe"));
    }

    #[tokio::test]
    async fn test_text_search_rejects_zero_pack_token_budget() {
        let (service, _dir) = test_service();
        let mut request = bm25_text_search_request("rare_contract_keyword", 10);
        request.pack = true;
        request.pack_token_budget = Some(0);

        let result = service.text_search(Request::new(request)).await;

        let status = result.expect_err("zero pack token budget should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("pack_token_budget"));
    }

    #[tokio::test]
    async fn test_text_search_rejects_excessive_pack_token_budget() {
        let (service, _dir) = test_service();
        let mut request = bm25_text_search_request("rare_contract_keyword", 10);
        request.pack = true;
        request.pack_token_budget = Some(MAX_PACK_TOKEN_BUDGET + 1);

        let result = service.text_search(Request::new(request)).await;

        let status = result.expect_err("excessive pack token budget should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("pack_token_budget"));
    }

    #[tokio::test]
    async fn test_text_search_rejects_invalid_embedding_vector() {
        let (service, _dir) = test_service();
        let service = service.with_embedding_provider(Arc::new(BadEmbedder));
        let mut request = bm25_text_search_request("rare_contract_keyword", 10);
        request.retrieval_mode = "vector".to_string();

        let result = service.text_search(Request::new(request)).await;

        let status = result.expect_err("invalid embedding vector should be rejected");
        assert_eq!(status.code(), Code::Internal);
        assert!(status.message().contains("invalid query vector"));
    }

    #[tokio::test]
    async fn test_insert_empty_text_removes_previous_bm25_document() {
        let (service, _dir) = test_service();
        insert_text(&service, "doc1", vec![1.0, 0.0], "stale_contract_keyword").await;
        insert_text(&service, "doc1", vec![0.0, 1.0], "").await;

        let response = service
            .text_search(Request::new(bm25_text_search_request(
                "stale_contract_keyword",
                10,
            )))
            .await
            .unwrap()
            .into_inner();

        assert!(response.results.is_empty());
    }

    #[tokio::test]
    async fn test_delete_removes_bm25_document() {
        let (service, _dir) = test_service();
        insert_text(&service, "doc1", vec![1.0, 0.0], "deleted_contract_keyword").await;

        service
            .delete(Request::new(DeleteRequest {
                collection: "test".to_string(),
                id: "doc1".to_string(),
            }))
            .await
            .unwrap();

        let response = service
            .text_search(Request::new(bm25_text_search_request(
                "deleted_contract_keyword",
                10,
            )))
            .await
            .unwrap()
            .into_inner();

        assert!(response.results.is_empty());
    }

    #[tokio::test]
    async fn test_delete_returns_already_deleted_on_second_delete() {
        let (service, _dir) = test_service();
        insert_text(&service, "doc1", vec![1.0, 0.0], "deleted_contract_keyword").await;

        let first = service
            .delete(Request::new(DeleteRequest {
                collection: "test".to_string(),
                id: "doc1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.status, DeleteStatus::Deleted as i32);

        let second = service
            .delete(Request::new(DeleteRequest {
                collection: "test".to_string(),
                id: "doc1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(second.status, DeleteStatus::AlreadyDeleted as i32);
    }

    fn with_workspace(workspace: &str) -> crate::auth::AuthContext {
        crate::auth::AuthContext {
            workspace_id: workspace.to_string(),
            agent_id: Some("agent-test".to_string()),
            authenticated: true,
        }
    }

    #[tokio::test]
    async fn test_workspace_acl_isolates_search_results() {
        let (service, _dir) = test_service();

        let mut req_a = Request::new(InsertRequest {
            collection: "test".to_string(),
            id: "a1".to_string(),
            vector: vec![1.0, 0.0],
            metadata: br#"{"title":"alpha"}"#.to_vec(),
            text: "alpha secret".to_string(),
        });
        req_a.extensions_mut().insert(with_workspace("ws-a"));
        service.insert(req_a).await.unwrap();

        let mut req_b = Request::new(InsertRequest {
            collection: "test".to_string(),
            id: "b1".to_string(),
            vector: vec![0.99, 0.01],
            metadata: br#"{"title":"beta"}"#.to_vec(),
            text: "beta secret".to_string(),
        });
        req_b.extensions_mut().insert(with_workspace("ws-b"));
        service.insert(req_b).await.unwrap();

        let mut search_a = Request::new(SearchRequest {
            collection: "test".to_string(),
            query: vec![1.0, 0.0],
            top_k: 10,
            nprobe: None,
            filter: vec![],
            tag_filter: None,
            score_threshold: None,
            group_by: String::new(),
            group_size: None,
        });
        search_a.extensions_mut().insert(with_workspace("ws-a"));
        let hits_a = service.search(search_a).await.unwrap().into_inner().results;
        assert!(hits_a.iter().any(|h| h.id == "a1"));
        assert!(!hits_a.iter().any(|h| h.id == "b1"));

        let mut search_b = Request::new(SearchRequest {
            collection: "test".to_string(),
            query: vec![1.0, 0.0],
            top_k: 10,
            nprobe: None,
            filter: vec![],
            tag_filter: None,
            score_threshold: None,
            group_by: String::new(),
            group_size: None,
        });
        search_b.extensions_mut().insert(with_workspace("ws-b"));
        let hits_b = service.search(search_b).await.unwrap().into_inner().results;
        assert!(hits_b.iter().any(|h| h.id == "b1"));
        assert!(!hits_b.iter().any(|h| h.id == "a1"));
    }

    #[tokio::test]
    async fn test_workspace_acl_hides_and_protects_point_mutations() {
        let (service, _dir) = test_service();
        let mut insert_a = Request::new(InsertRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
            vector: vec![1.0, 0.0],
            metadata: br#"{"title":"workspace a"}"#.to_vec(),
            text: "workspace a evidence".to_string(),
        });
        insert_a.extensions_mut().insert(with_workspace("ws-a"));
        service.insert(insert_a).await.unwrap();

        let mut get_b = Request::new(GetRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
        });
        get_b.extensions_mut().insert(with_workspace("ws-b"));
        assert_eq!(service.get(get_b).await.unwrap_err().code(), Code::NotFound);

        let mut update_b = Request::new(UpdateRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
            vector: vec![0.0, 1.0],
            metadata: br#"{"title":"workspace b overwrite"}"#.to_vec(),
        });
        update_b.extensions_mut().insert(with_workspace("ws-b"));
        assert_eq!(
            service.update(update_b).await.unwrap_err().code(),
            Code::NotFound
        );

        let mut delete_b = Request::new(DeleteRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
        });
        delete_b.extensions_mut().insert(with_workspace("ws-b"));
        assert_eq!(
            service.delete(delete_b).await.unwrap_err().code(),
            Code::NotFound
        );

        let mut insert_b = Request::new(InsertRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
            vector: vec![0.0, 1.0],
            metadata: br#"{"title":"workspace b collision"}"#.to_vec(),
            text: String::new(),
        });
        insert_b.extensions_mut().insert(with_workspace("ws-b"));
        assert_eq!(
            service.insert(insert_b).await.unwrap_err().code(),
            Code::AlreadyExists
        );

        let mut get_a = Request::new(GetRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
        });
        get_a.extensions_mut().insert(with_workspace("ws-a"));
        let stored = service.get(get_a).await.unwrap().into_inner();
        assert_eq!(stored.vector, vec![1.0, 0.0]);
        assert!(stored.metadata.contains("workspace a"));

        let mut delete_a = Request::new(DeleteRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
        });
        delete_a.extensions_mut().insert(with_workspace("ws-a"));
        assert_eq!(
            service.delete(delete_a).await.unwrap().into_inner().status,
            DeleteStatus::Deleted as i32
        );

        let mut tombstone_delete_b = Request::new(DeleteRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
        });
        tombstone_delete_b
            .extensions_mut()
            .insert(with_workspace("ws-b"));
        assert_eq!(
            service.delete(tombstone_delete_b).await.unwrap_err().code(),
            Code::NotFound
        );

        let mut tombstone_insert_b = Request::new(InsertRequest {
            collection: "test".to_string(),
            id: "private-a".to_string(),
            vector: vec![0.0, 1.0],
            metadata: br#"{"title":"workspace b tombstone collision"}"#.to_vec(),
            text: String::new(),
        });
        tombstone_insert_b
            .extensions_mut()
            .insert(with_workspace("ws-b"));
        assert_eq!(
            service.insert(tombstone_insert_b).await.unwrap_err().code(),
            Code::AlreadyExists
        );
    }

    #[tokio::test]
    async fn test_batch_write_and_search_are_workspace_scoped() {
        let (service, _dir) = test_service();
        for (workspace, id, vector) in [
            ("ws-a", "batch-a", vec![1.0, 0.0]),
            ("ws-b", "batch-b", vec![0.99, 0.01]),
        ] {
            let mut request = Request::new(InsertBatchRequest {
                collection: "test".to_string(),
                vectors: vec![Vector {
                    id: id.to_string(),
                    embedding: vector,
                    metadata: br#"{"kind":"batch"}"#.to_vec(),
                    text: format!("{workspace} batch text"),
                }],
            });
            request.extensions_mut().insert(with_workspace(workspace));
            let response = service.insert_batch(request).await.unwrap().into_inner();
            assert!(response.success);
            assert_eq!(response.inserted_count, 1);
        }

        let mut request = Request::new(SearchBatchRequest {
            collection: "test".to_string(),
            queries: vec![Query {
                vector: vec![1.0, 0.0],
            }],
            top_k: 10,
            nprobe: None,
        });
        request.extensions_mut().insert(with_workspace("ws-a"));
        let hits = service
            .search_batch(request)
            .await
            .unwrap()
            .into_inner()
            .results
            .remove(0)
            .results;
        assert!(hits.iter().any(|hit| hit.id == "batch-a"));
        assert!(!hits.iter().any(|hit| hit.id == "batch-b"));
        let metadata: serde_json::Value = serde_json::from_str(&hits[0].metadata).unwrap();
        assert_eq!(metadata[acl::WORKSPACE_KEY], "ws-a");
    }

    #[tokio::test]
    async fn test_structured_sql_results_are_workspace_scoped() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        let id_mapping = Arc::new(IdMapping::new(storage, "test"));
        let index = Arc::new(MockIndex::new(2, 16));
        let sql = Arc::new(SqliteMetadataIndex::in_memory().unwrap());
        let service = AkiDbService::new(index, id_mapping, "test").with_metadata_sql_index(sql);

        for (workspace, id) in [("ws-a", "sql-a"), ("ws-b", "sql-b")] {
            let mut request = Request::new(InsertRequest {
                collection: "test".to_string(),
                id: id.to_string(),
                vector: vec![1.0, 0.0],
                metadata: br#"{"status":"open"}"#.to_vec(),
                text: String::new(),
            });
            request.extensions_mut().insert(with_workspace(workspace));
            service.insert(request).await.unwrap();
        }

        let mut query = bm25_text_search_request("all open records", 10);
        query.retrieval_mode = "structured_sql".to_string();
        query.filter = br#"{"status":"open"}"#.to_vec();
        let mut request = Request::new(query);
        request.extensions_mut().insert(with_workspace("ws-a"));
        let hits = service
            .text_search(request)
            .await
            .unwrap()
            .into_inner()
            .results;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "sql-a");
    }

    #[tokio::test]
    async fn test_context_pack_uses_provenance_citation() {
        let (service, _dir) = test_service();
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "citation-chunk".to_string(),
                vector: vec![1.0, 0.0],
                metadata: br#"{
                    "source_uri":"s3://docs/contracts/acme.pdf",
                    "source_version":"sha256:abc",
                    "start_offset":"10",
                    "end_offset":42
                }"#
                .to_vec(),
                text: "citation_marker_unique contract evidence".to_string(),
            }))
            .await
            .unwrap();

        let mut query = bm25_text_search_request("citation_marker_unique", 1);
        query.pack = true;
        query.pack_token_budget = Some(64);
        let response = service
            .text_search(Request::new(query))
            .await
            .unwrap()
            .into_inner();
        assert!(response
            .context_pack
            .contains("[s3://docs/contracts/acme.pdf@sha256:abc#10:42]"));
    }

    #[tokio::test]
    async fn test_score_threshold_excludes_low_scores() {
        let (service, _dir) = test_service();
        insert_text(&service, "high", vec![1.0, 0.0], "high score doc").await;
        insert_text(&service, "low", vec![0.0, 1.0], "orthogonal doc").await;

        let response = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![1.0, 0.0],
                top_k: 10,
                nprobe: None,
                filter: vec![],
                tag_filter: None,
                score_threshold: Some(0.99),
                group_by: String::new(),
                group_size: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!response.results.is_empty());
        assert!(response.results.iter().all(|r| r.score >= 0.99));
        assert!(response.results.iter().any(|r| r.id == "high"));
        assert!(!response.results.iter().any(|r| r.id == "low"));
    }

    #[tokio::test]
    async fn test_group_by_parent_keeps_one_per_group() {
        let (service, _dir) = test_service();
        for (id, meta) in [
            ("c1", br#"{"parent_id":"p1"}"#.as_slice()),
            ("c2", br#"{"parent_id":"p1"}"#.as_slice()),
            ("c3", br#"{"parent_id":"p2"}"#.as_slice()),
        ] {
            service
                .insert(Request::new(InsertRequest {
                    collection: "test".to_string(),
                    id: id.to_string(),
                    vector: vec![1.0, 0.0],
                    metadata: meta.to_vec(),
                    text: id.to_string(),
                }))
                .await
                .unwrap();
        }

        let response = service
            .search(Request::new(SearchRequest {
                collection: "test".to_string(),
                query: vec![1.0, 0.0],
                top_k: 10,
                nprobe: None,
                filter: vec![],
                tag_filter: None,
                score_threshold: None,
                group_by: "parent_id".to_string(),
                group_size: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();

        let mut parents = std::collections::HashSet::new();
        for r in &response.results {
            let v: serde_json::Value = serde_json::from_str(&r.metadata).unwrap();
            let p = v["parent_id"].as_str().unwrap().to_string();
            assert!(
                parents.insert(p),
                "duplicate parent in results: {:?}",
                response.results
            );
        }
        assert!(response.results.len() <= 2);
        assert!(!response.results.is_empty());
    }

    #[tokio::test]
    async fn test_collection_lifecycle_apis() {
        let (service, _dir) = test_service();
        service.seed_collection_schema(2, "cosine", "f32", "test-model");

        let created = service
            .create_collection(Request::new(crate::proto::CreateCollectionRequest {
                name: "extra".to_string(),
                dimensions: 2,
                metric: "cosine".to_string(),
                embedding_model_id: "m".to_string(),
                vector_precision: "f32".to_string(),
                chunk_strategy: "code".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(created.success);

        let listed = service
            .list_collections(Request::new(crate::proto::ListCollectionsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(listed.collections.iter().any(|c| c.name == "extra"));
        assert!(listed.collections.iter().any(|c| c.name == "test"));

        let got = service
            .get_collection(Request::new(crate::proto::GetCollectionRequest {
                name: "extra".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(got.found);
        assert_eq!(got.collection.as_ref().unwrap().chunk_strategy, "code");

        let dropped = service
            .drop_collection(Request::new(crate::proto::DropCollectionRequest {
                name: "extra".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(dropped.success);
    }

    struct FixedEmbedder;

    impl EmbeddingProvider for FixedEmbedder {
        fn embed_text(&self, _text: &str) -> std::result::Result<Vec<f32>, String> {
            Ok(vec![1.0, 0.0])
        }

        fn embedding_dimensions(&self) -> usize {
            2
        }
    }

    #[tokio::test]
    async fn test_graph_hybrid_expands_related_edges() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
        let index = Arc::new(MockIndex::new(2, 16));
        let graph = Arc::new(akidb_graph::NativeGraphIndex::new(storage));
        let service = AkiDbService::new(index, id_mapping, "test")
            .with_graph_index(graph)
            .with_embedding_provider(Arc::new(FixedEmbedder));

        // Seed BM25+dense match with a calls edge to an orthogonal non-matching target.
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "fn_seed".to_string(),
                vector: vec![1.0, 0.0],
                metadata: br#"{"calls":["fn_hidden"]}"#.to_vec(),
                text: "seed_marker_token_alpha unique_body".to_string(),
            }))
            .await
            .unwrap();
        service
            .insert(Request::new(InsertRequest {
                collection: "test".to_string(),
                id: "fn_hidden".to_string(),
                // Orthogonal dense (cosine 0) + no lexical overlap → not base-retrievable.
                vector: vec![0.0, 1.0],
                metadata: br#"{"symbol":"Hidden"}"#.to_vec(),
                text: "zzzz_unrelated_body_no_overlap_xyz".to_string(),
            }))
            .await
            .unwrap();

        let query = "seed_marker_token_alpha";

        // Control 1: BM25-only misses hidden.
        let mut bm25 = bm25_text_search_request(query, 10);
        bm25.retrieval_mode = "bm25".to_string();
        let bm25_hits = service
            .text_search(Request::new(bm25))
            .await
            .unwrap()
            .into_inner()
            .results;
        assert!(bm25_hits.iter().any(|r| r.id == "fn_seed"));
        assert!(
            !bm25_hits.iter().any(|r| r.id == "fn_hidden"),
            "BM25 alone must miss fn_hidden, got {:?}",
            bm25_hits.iter().map(|r| &r.id).collect::<Vec<_>>()
        );

        // Control 2: hybrid dense+lexical WITHOUT graph must still miss hidden
        // (zero-cosine dense hits are dropped before RRF fusion).
        let mut hybrid = bm25_text_search_request(query, 10);
        hybrid.retrieval_mode = "hybrid".to_string();
        hybrid.hybrid = true;
        let hybrid_hits = service
            .text_search(Request::new(hybrid))
            .await
            .unwrap()
            .into_inner()
            .results;
        assert!(hybrid_hits.iter().any(|r| r.id == "fn_seed"));
        assert!(
            !hybrid_hits.iter().any(|r| r.id == "fn_hidden"),
            "hybrid without graph must miss fn_hidden (proves edge path is required), got {:?}",
            hybrid_hits.iter().map(|r| &r.id).collect::<Vec<_>>()
        );

        // Positive: graph_hybrid expands seed → fn_hidden only via the calls edge.
        let mut graph_req = bm25_text_search_request(query, 10);
        graph_req.retrieval_mode = "graph_hybrid".to_string();
        graph_req.hybrid = true;
        let graph_hits = service
            .text_search(Request::new(graph_req))
            .await
            .unwrap()
            .into_inner()
            .results;
        let ids: Vec<_> = graph_hits.iter().map(|r| r.id.as_str()).collect();
        assert!(
            ids.contains(&"fn_hidden"),
            "graph_hybrid must expand calls edge to fn_hidden, got {ids:?}"
        );

        // Also prove pure graph mode (lexical-only + expand, no dense).
        let mut graph_only = bm25_text_search_request(query, 10);
        graph_only.retrieval_mode = "graph".to_string();
        let graph_only_hits = service
            .text_search(Request::new(graph_only))
            .await
            .unwrap()
            .into_inner()
            .results;
        assert!(
            graph_only_hits.iter().any(|r| r.id == "fn_hidden"),
            "graph mode must expand calls edge to fn_hidden, got {:?}",
            graph_only_hits.iter().map(|r| &r.id).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_graph_projection_is_workspace_scoped_replaced_and_rebuildable() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
        let index = Arc::new(MockIndex::new(2, 16));
        let graph = Arc::new(akidb_graph::NativeGraphIndex::new(storage));
        let service = AkiDbService::new(index, id_mapping, "test").with_graph_index(graph.clone());

        for (workspace, id) in [("ws-a", "chunk-a"), ("ws-b", "chunk-b")] {
            let mut request = Request::new(InsertRequest {
                collection: "test".to_string(),
                id: id.to_string(),
                vector: vec![1.0, 0.0],
                metadata: format!(
                    r#"{{
                        "document_id":"shared-document",
                        "file":"shared.pdf",
                        "source_uri":"s3://docs/{workspace}/shared.pdf",
                        "source_version":"v1",
                        "extraction_method":"deterministic",
                        "pipeline_version":"test"
                    }}"#
                )
                .into_bytes(),
                text: format!("{workspace} evidence"),
            });
            request.extensions_mut().insert(with_workspace(workspace));
            service.insert(request).await.unwrap();
        }

        let document_a = GraphNodeId::scoped("ws-a", "document:shared-document");
        let document_b = GraphNodeId::scoped("ws-b", "document:shared-document");
        let related_a = graph.related_chunks(&document_a, 10).unwrap();
        let related_b = graph.related_chunks(&document_b, 10).unwrap();
        assert_eq!(related_a.len(), 1);
        assert_eq!(related_a[0].vector_id.as_str(), "chunk-a");
        assert_eq!(related_b.len(), 1);
        assert_eq!(related_b[0].vector_id.as_str(), "chunk-b");

        let evidence_edge = graph
            .neighbors(
                akidb_graph::NeighborRequest::new(document_a.clone())
                    .with_direction(akidb_graph::Direction::Out),
            )
            .unwrap()
            .remove(0)
            .edge;
        assert_eq!(evidence_edge.properties["workspace_id"], "ws-a");
        assert_eq!(evidence_edge.properties["evidence_chunk_id"], "chunk-a");
        assert_eq!(
            evidence_edge.properties["extraction_method"],
            "deterministic"
        );

        let retry_document = GraphNodeId::scoped("ws-a", "document:retry-document");
        service
            .index_graph_chunk(
                &VectorId::new("chunk-a"),
                br#"{
                    "workspace_id":"ws-a",
                    "document_id":"retry-document",
                    "file":"shared.pdf",
                    "source_uri":"s3://docs/ws-a/shared.pdf",
                    "source_version":"retry",
                    "extraction_method":"deterministic",
                    "pipeline_version":"test"
                }"#,
                None,
            )
            .unwrap();
        assert!(graph.related_chunks(&document_a, 10).unwrap().is_empty());
        assert_eq!(
            graph.related_chunks(&retry_document, 10).unwrap()[0]
                .vector_id
                .as_str(),
            "chunk-a"
        );

        let mut update = Request::new(UpdateRequest {
            collection: "test".to_string(),
            id: "chunk-a".to_string(),
            vector: vec![0.9, 0.1],
            metadata: br#"{
                "document_id":"replacement-document",
                "file":"shared.pdf",
                "source_uri":"s3://docs/ws-a/shared.pdf",
                "source_version":"v2",
                "extraction_method":"deterministic",
                "pipeline_version":"test"
            }"#
            .to_vec(),
        });
        update.extensions_mut().insert(with_workspace("ws-a"));
        service.update(update).await.unwrap();

        assert!(graph.related_chunks(&document_a, 10).unwrap().is_empty());
        assert!(graph
            .related_chunks(&retry_document, 10)
            .unwrap()
            .is_empty());
        let replacement = GraphNodeId::scoped("ws-a", "document:replacement-document");
        assert_eq!(
            graph.related_chunks(&replacement, 10).unwrap()[0]
                .vector_id
                .as_str(),
            "chunk-a"
        );
        assert_eq!(
            graph.related_chunks(&document_b, 10).unwrap()[0]
                .vector_id
                .as_str(),
            "chunk-b"
        );

        graph
            .delete_node(&GraphNodeId::scoped("ws-a", "chunk:chunk-a"))
            .unwrap();
        assert!(graph.related_chunks(&replacement, 10).unwrap().is_empty());
        assert_eq!(service.rebuild_graph_index().unwrap(), 2);
        assert_eq!(
            graph.related_chunks(&replacement, 10).unwrap()[0]
                .vector_id
                .as_str(),
            "chunk-a"
        );
        assert_eq!(
            graph.related_chunks(&document_b, 10).unwrap()[0]
                .vector_id
                .as_str(),
            "chunk-b"
        );
    }
}
