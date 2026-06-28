//! AkiDB gRPC service implementation

use crate::filter::MetadataFilter;
use crate::proto::{
    akidb_server::Akidb, DeleteRequest, DeleteResponse, DeleteStatus, GetClusterStateRequest,
    GetClusterStateResponse, GetRequest, GetResponse, HealthRequest, HealthResponse,
    InsertBatchRequest, InsertBatchResponse, InsertRequest, InsertResponse, SearchBatchRequest,
    SearchBatchResponse, SearchRequest, SearchResponse, SearchResult, TextSearchRequest,
    UpdateRequest, UpdateResponse, UpdateStatus, VisibilityInfo,
};
use akidb_common::{AkiDbError, VectorId};
use akidb_faiss::{SearchParams, VectorIndex};
use akidb_graph::{
    EdgeKind, GraphEdge, GraphEdgeId, GraphIndex, GraphNode, GraphNodeId, GraphStats, NodeKind,
};
use akidb_retrieval::{
    expand_to_parents, mmr, pack, plan_query, Bm25Index, HybridFuser, LexicalOverlapReranker,
    MatchedChunk, MmrItem, PackerConfig, PlannerInput, RerankItem, Reranker, RetrievalMode,
    ScoredId,
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

/// Trait for embedding providers (implemented by coordinator's AxEngineEmbedding)
pub trait EmbeddingProvider: Send + Sync {
    /// Generate embedding for text
    fn embed_text(&self, text: &str) -> std::result::Result<Vec<f32>, String>;
    /// Get embedding dimensions
    fn embedding_dimensions(&self) -> usize;
}

/// FIX BUG-059: RAII guard for update locks to ensure release on panic
/// This guard automatically removes the lock when dropped, even during panic unwinding
struct UpdateLockGuard<'a> {
    locks: &'a DashSet<String>,
    id: String,
}

impl<'a> UpdateLockGuard<'a> {
    /// Try to acquire the lock, returns None if already locked
    fn try_acquire(locks: &'a DashSet<String>, id: String) -> Option<Self> {
        if locks.insert(id.clone()) {
            Some(Self { locks, id })
        } else {
            None
        }
    }
}

impl Drop for UpdateLockGuard<'_> {
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
    /// FIX BUG-052: Per-key lock set to prevent concurrent update races
    /// When a key is in this set, an update operation is in progress for that ID
    update_locks: DashSet<String>,
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
}

impl<I, S> AkiDbService<I, S>
where
    I: VectorIndex,
    S: StorageBackend,
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
        Self {
            index,
            id_mapping,
            collection: collection.into(),
            update_locks: DashSet::new(),
            slo_threshold_us,
            embedding_provider: None,
            lexical: Arc::new(RwLock::new(Bm25Index::new())),
            documents: Arc::new(RwLock::new(HashMap::new())),
            graph_index: None,
            metadata_sql_index: None,
        }
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
        let count = texts.len();
        for (id, text) in texts {
            lexical.insert(id.clone(), &text);
            documents.insert(id, text);
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
                    Some(serde_json::from_slice(&entry.metadata).unwrap_or(serde_json::Value::Null))
                }
            }
            _ => None,
        }
    }

    fn metadata_matches_filter(&self, id: &VectorId, filter: &MetadataFilter) -> bool {
        self.load_metadata_value(id)
            .is_some_and(|metadata| filter.matches(&metadata))
    }

    fn metadata_value_from_bytes(metadata: &[u8]) -> serde_json::Value {
        if metadata.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(metadata).unwrap_or(serde_json::Value::Null)
        }
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
            if matches!(
                value,
                serde_json::Value::Array(_) | serde_json::Value::Object(_)
            ) {
                return Err(Status::invalid_argument(format!(
                    "SQL metadata filter field '{key}' must be a scalar value"
                )));
            }
            query = query.with_eq(key.clone(), value.clone());
        }
        Ok(query)
    }

    fn sql_metadata_text_search(
        &self,
        req: &TextSearchRequest,
        started_at: Instant,
    ) -> Result<Response<SearchResponse>, Status> {
        let Some(sql_index) = &self.metadata_sql_index else {
            return Err(Status::unavailable(
                "Structured SQL retrieval requires the optional SQL metadata adapter",
            ));
        };
        if req.tag_filter.is_some() {
            return Err(Status::unimplemented(
                "Structured SQL retrieval currently supports the legacy JSON filter; tag_filter pushdown is pending",
            ));
        }

        let query =
            Self::sql_query_from_legacy_filter(&self.collection, &req.filter, req.top_k as usize)?;
        let ids = sql_index
            .query_ids(&query)
            .map_err(|e| Status::internal(format!("SQL metadata retrieval failed: {e}")))?;

        let results: Vec<SearchResult> = ids
            .into_iter()
            .map(|id| {
                let vector_id = VectorId::new(&id);
                SearchResult {
                    id,
                    score: 1.0,
                    metadata: self.load_metadata_string(&vector_id),
                }
            })
            .collect();
        let latency_us = started_at.elapsed().as_micros() as u64;

        Ok(Response::new(SearchResponse {
            results,
            partial: false,
            missing_shards: vec![],
            coverage: 1.0,
            latency_us,
            within_slo: latency_us < self.slo_threshold_us,
            degraded_mode: false,
            context_pack: String::new(),
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
                "invalid retrieval_mode '{}'; expected auto, vector, bm25, hybrid, graph, or graph_hybrid",
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

    fn chunk_node_id(vector_id: &VectorId) -> GraphNodeId {
        GraphNodeId::new(format!("chunk:{}", vector_id.as_str()))
    }

    fn related_ids_from_metadata(metadata: &str) -> Vec<String> {
        if metadata.is_empty() {
            return Vec::new();
        }
        serde_json::from_str::<serde_json::Value>(metadata)
            .ok()
            .and_then(|m| {
                m.get("related_ids").and_then(|v| v.as_array()).map(|ids| {
                    ids.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
            })
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

    fn graph_node_id(kind: NodeKind, id: &str) -> GraphNodeId {
        GraphNodeId::new(format!("{}:{}", kind.as_key(), id))
    }

    fn graph_edge_id(kind: EdgeKind, from: &GraphNodeId, to: &GraphNodeId) -> String {
        format!("auto:{}:{}:{}", kind.as_key(), from.as_str(), to.as_str())
    }

    fn upsert_graph_node(
        graph: &dyn GraphIndex,
        node_id: GraphNodeId,
        kind: NodeKind,
        raw_id: &str,
    ) -> bool {
        graph
            .upsert_node(
                GraphNode::new(node_id, kind).with_property("id", serde_json::json!(raw_id)),
            )
            .is_ok()
    }

    fn upsert_graph_edge(
        graph: &dyn GraphIndex,
        from: &GraphNodeId,
        to: &GraphNodeId,
        kind: EdgeKind,
    ) -> Result<(), akidb_graph::GraphError> {
        graph.upsert_edge(GraphEdge::new(
            Self::graph_edge_id(kind, from, to),
            from.clone(),
            to.clone(),
            kind,
        ))
    }

    fn index_graph_chunk(&self, vector_id: &VectorId, metadata: &[u8]) {
        let Some(graph) = &self.graph_index else {
            return;
        };

        let metadata = String::from_utf8_lossy(metadata);
        let metadata_json = serde_json::from_str::<serde_json::Value>(&metadata).ok();
        let chunk_node_id = Self::chunk_node_id(vector_id);
        let chunk_node = GraphNode::new(chunk_node_id.clone(), NodeKind::Chunk)
            .with_property("vector_id", serde_json::json!(vector_id.as_str()));

        if let Err(e) = graph.upsert_node(chunk_node) {
            warn!(vector_id = %vector_id, error = %e, "failed to index graph chunk node");
            return;
        }

        if let Some(parent_id) = Self::parent_id_from_metadata(&metadata) {
            let parent_vector_id = VectorId::new(parent_id);
            let parent_node_id = Self::chunk_node_id(&parent_vector_id);
            if let Err(e) = graph.upsert_node(
                GraphNode::new(parent_node_id.clone(), NodeKind::Chunk)
                    .with_property("vector_id", serde_json::json!(parent_vector_id.as_str())),
            ) {
                warn!(vector_id = %vector_id, parent = %parent_vector_id, error = %e, "failed to index graph parent node");
            } else {
                let parent_edge_id = format!(
                    "auto:parent_of:{}:{}",
                    parent_vector_id.as_str(),
                    vector_id.as_str()
                );
                if let Err(e) = graph.upsert_edge(GraphEdge::new(
                    parent_edge_id,
                    parent_node_id.clone(),
                    chunk_node_id.clone(),
                    EdgeKind::ParentOf,
                )) {
                    warn!(vector_id = %vector_id, parent = %parent_vector_id, error = %e, "failed to index graph parent edge");
                }

                let child_edge_id = format!(
                    "auto:child_of:{}:{}",
                    vector_id.as_str(),
                    parent_vector_id.as_str()
                );
                if let Err(e) = graph.upsert_edge(GraphEdge::new(
                    child_edge_id,
                    chunk_node_id.clone(),
                    parent_node_id,
                    EdgeKind::ChildOf,
                )) {
                    warn!(vector_id = %vector_id, parent = %parent_vector_id, error = %e, "failed to index graph child edge");
                }
            }
        }

        for related_id in Self::related_ids_from_metadata(&metadata) {
            let related_vector_id = VectorId::new(related_id);
            let related_node_id = Self::chunk_node_id(&related_vector_id);
            if let Err(e) = graph.upsert_node(
                GraphNode::new(related_node_id.clone(), NodeKind::Chunk)
                    .with_property("vector_id", serde_json::json!(related_vector_id.as_str())),
            ) {
                warn!(vector_id = %vector_id, related = %related_vector_id, error = %e, "failed to index graph related node");
                continue;
            }

            let forward_edge_id = format!(
                "auto:related_to:{}:{}",
                vector_id.as_str(),
                related_vector_id.as_str()
            );
            if let Err(e) = graph.upsert_edge(GraphEdge::new(
                forward_edge_id,
                chunk_node_id.clone(),
                related_node_id.clone(),
                EdgeKind::RelatedTo,
            )) {
                warn!(vector_id = %vector_id, related = %related_vector_id, error = %e, "failed to index graph related edge");
            }

            let reverse_edge_id = format!(
                "auto:related_to:{}:{}",
                related_vector_id.as_str(),
                vector_id.as_str()
            );
            if let Err(e) = graph.upsert_edge(GraphEdge::new(
                reverse_edge_id,
                related_node_id,
                chunk_node_id.clone(),
                EdgeKind::RelatedTo,
            )) {
                warn!(vector_id = %vector_id, related = %related_vector_id, error = %e, "failed to index graph reverse related edge");
            }
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
                let target_node_id = Self::chunk_node_id(&target_vector_id);
                if let Err(e) = graph.upsert_node(
                    GraphNode::new(target_node_id.clone(), NodeKind::Chunk)
                        .with_property("vector_id", serde_json::json!(target_vector_id.as_str())),
                ) {
                    warn!(vector_id = %vector_id, target = %target_vector_id, field, error = %e, "failed to index graph code target node");
                    continue;
                }
                if let Err(e) = Self::upsert_graph_edge(
                    graph.as_ref(),
                    &chunk_node_id,
                    &target_node_id,
                    edge_kind,
                ) {
                    warn!(vector_id = %vector_id, target = %target_vector_id, field, error = %e, "failed to index graph code edge");
                }
            }
        }

        for owner in Self::metadata_string_values(metadata_json.as_ref(), "owned_by") {
            let owner_node_id = Self::graph_node_id(NodeKind::Person, &owner);
            if Self::upsert_graph_node(
                graph.as_ref(),
                owner_node_id.clone(),
                NodeKind::Person,
                &owner,
            ) {
                if let Err(e) = Self::upsert_graph_edge(
                    graph.as_ref(),
                    &chunk_node_id,
                    &owner_node_id,
                    EdgeKind::OwnedBy,
                ) {
                    warn!(vector_id = %vector_id, owner, error = %e, "failed to index graph owner edge");
                }
            }
        }

        for commit in Self::metadata_string_values(metadata_json.as_ref(), "changed_by") {
            let commit_node_id = Self::graph_node_id(NodeKind::Commit, &commit);
            if Self::upsert_graph_node(
                graph.as_ref(),
                commit_node_id.clone(),
                NodeKind::Commit,
                &commit,
            ) {
                if let Err(e) = Self::upsert_graph_edge(
                    graph.as_ref(),
                    &chunk_node_id,
                    &commit_node_id,
                    EdgeKind::ChangedBy,
                ) {
                    warn!(vector_id = %vector_id, commit, error = %e, "failed to index graph changed_by edge");
                }
            }
        }

        for file in Self::metadata_string_values(metadata_json.as_ref(), "file") {
            let file_node_id = Self::graph_node_id(NodeKind::File, &file);
            if Self::upsert_graph_node(graph.as_ref(), file_node_id.clone(), NodeKind::File, &file)
            {
                if let Err(e) = Self::upsert_graph_edge(
                    graph.as_ref(),
                    &file_node_id,
                    &chunk_node_id,
                    EdgeKind::Contains,
                ) {
                    warn!(vector_id = %vector_id, file, error = %e, "failed to index graph file edge");
                }
            }
        }

        for symbol in Self::metadata_string_values(metadata_json.as_ref(), "symbol") {
            let symbol_node_id = Self::graph_node_id(NodeKind::Function, &symbol);
            if Self::upsert_graph_node(
                graph.as_ref(),
                symbol_node_id.clone(),
                NodeKind::Function,
                &symbol,
            ) {
                if let Err(e) = Self::upsert_graph_edge(
                    graph.as_ref(),
                    &symbol_node_id,
                    &chunk_node_id,
                    EdgeKind::Contains,
                ) {
                    warn!(vector_id = %vector_id, symbol, error = %e, "failed to index graph symbol edge");
                }
            }
        }
    }

    fn delete_graph_edge_id(&self, edge_id: String) {
        let Some(graph) = &self.graph_index else {
            return;
        };
        if let Err(e) = graph.delete_edge(&GraphEdgeId::new(edge_id.clone())) {
            warn!(edge_id, error = %e, "failed to delete stale graph edge");
        }
    }

    fn delete_graph_edges_from_metadata(&self, vector_id: &VectorId, metadata: &[u8]) {
        if self.graph_index.is_none() {
            return;
        }

        let metadata = String::from_utf8_lossy(metadata);
        let metadata_json = serde_json::from_str::<serde_json::Value>(&metadata).ok();
        let chunk_node_id = Self::chunk_node_id(vector_id);

        if let Some(parent_id) = Self::parent_id_from_metadata(&metadata) {
            self.delete_graph_edge_id(format!(
                "auto:parent_of:{}:{}",
                parent_id,
                vector_id.as_str()
            ));
            self.delete_graph_edge_id(format!(
                "auto:child_of:{}:{}",
                vector_id.as_str(),
                parent_id
            ));
        }

        for related_id in Self::related_ids_from_metadata(&metadata) {
            self.delete_graph_edge_id(format!(
                "auto:related_to:{}:{}",
                vector_id.as_str(),
                related_id
            ));
            self.delete_graph_edge_id(format!(
                "auto:related_to:{}:{}",
                related_id,
                vector_id.as_str()
            ));
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
                let target_node_id = Self::chunk_node_id(&VectorId::new(target_id));
                self.delete_graph_edge_id(Self::graph_edge_id(
                    edge_kind,
                    &chunk_node_id,
                    &target_node_id,
                ));
            }
        }

        for owner in Self::metadata_string_values(metadata_json.as_ref(), "owned_by") {
            let owner_node_id = Self::graph_node_id(NodeKind::Person, &owner);
            self.delete_graph_edge_id(Self::graph_edge_id(
                EdgeKind::OwnedBy,
                &chunk_node_id,
                &owner_node_id,
            ));
        }

        for commit in Self::metadata_string_values(metadata_json.as_ref(), "changed_by") {
            let commit_node_id = Self::graph_node_id(NodeKind::Commit, &commit);
            self.delete_graph_edge_id(Self::graph_edge_id(
                EdgeKind::ChangedBy,
                &chunk_node_id,
                &commit_node_id,
            ));
        }

        for file in Self::metadata_string_values(metadata_json.as_ref(), "file") {
            let file_node_id = Self::graph_node_id(NodeKind::File, &file);
            self.delete_graph_edge_id(Self::graph_edge_id(
                EdgeKind::Contains,
                &file_node_id,
                &chunk_node_id,
            ));
        }

        for symbol in Self::metadata_string_values(metadata_json.as_ref(), "symbol") {
            let symbol_node_id = Self::graph_node_id(NodeKind::Function, &symbol);
            self.delete_graph_edge_id(Self::graph_edge_id(
                EdgeKind::Contains,
                &symbol_node_id,
                &chunk_node_id,
            ));
        }
    }

    fn delete_graph_chunk(&self, vector_id: &VectorId) {
        let Some(graph) = &self.graph_index else {
            return;
        };
        let node_id = Self::chunk_node_id(vector_id);
        if let Err(e) = graph.delete_node(&node_id) {
            warn!(vector_id = %vector_id, error = %e, "failed to delete graph chunk node");
        }
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
        if let Some(old_id) = old_internal_id {
            if let Err(delete_err) = self.index.delete(old_id) {
                tracing::warn!(
                    vector_id = %vector_id,
                    old_internal_id = old_id.0,
                    error = %delete_err,
                    "Failed to delete old vector during update - orphan vector may exist"
                );
            }
        }

        if let Some(metadata) = old_metadata {
            self.delete_graph_edges_from_metadata(vector_id, &metadata);
        }
        self.index_sql_metadata(vector_id, new_internal_id.0, metadata);
        self.index_graph_chunk(vector_id, metadata);

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
        let req = request.into_inner();

        debug!("Insert request for ID: {}", req.id);

        let vector_id = VectorId::new(&req.id);
        let vector: Vec<f32> = req.vector;

        // Validate input
        if req.id.is_empty() {
            return Err(Status::invalid_argument("Vector ID cannot be empty"));
        }
        if req.id.len() > 1024 {
            return Err(Status::invalid_argument(
                "Vector ID exceeds maximum length of 1024",
            ));
        }
        if vector.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(Status::invalid_argument(
                "Vector contains NaN or Infinity values",
            ));
        }
        if vector.is_empty() {
            return Err(Status::invalid_argument("Vector cannot be empty"));
        }

        let old_metadata = self
            .id_mapping
            .get_vector(&vector_id)
            .map_err(Self::to_status)?
            .map(|entry| entry.metadata);

        // Insert into index
        let internal_id = self
            .index
            .insert(&vector_id, &vector)
            .map_err(Self::to_status)?;

        // Persist ID mapping and vector payload atomically. If this fails,
        // rollback the index insert.
        let mapping_result =
            self.id_mapping
                .upsert_with_vector(&vector_id, internal_id, &vector, &req.metadata);

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

        // Keep BM25, context packing, and persisted source text aligned with
        // upsert semantics. Empty text clears any previous source text.
        self.sync_source_text(&vector_id, &req.text);
        if let Some(metadata) = old_metadata {
            self.delete_graph_edges_from_metadata(&vector_id, &metadata);
        }
        self.index_graph_chunk(&vector_id, &req.metadata);
        self.index_sql_metadata(&vector_id, internal_id.0, &req.metadata);

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
        let req = request.into_inner();

        debug!("Search request, top_k: {}", req.top_k);

        // FIX BUG-H013: Validate top_k to prevent panic in SearchParams::new()
        if req.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be greater than 0"));
        }
        if req.top_k > 10000 {
            return Err(Status::invalid_argument("top_k exceeds maximum of 10000"));
        }

        let mut params =
            SearchParams::new(req.top_k as usize).with_nprobe(req.nprobe.unwrap_or(32));

        // Wire optional metadata filtering (RET-003). The predicate loads each
        // candidate's stored metadata and evaluates the request's tag/legacy
        // filter against it; candidates that fail (or whose metadata cannot be
        // read) are excluded.
        match crate::filter::MetadataFilter::build(&req.filter, req.tag_filter.clone()) {
            Ok(Some(metadata_filter)) => {
                let metadata_filter = Arc::new(metadata_filter);
                let id_mapping = self.id_mapping.clone();
                params = params.with_filter(Arc::new(move |id: &VectorId| {
                    match id_mapping.get_vector(id) {
                        Ok(Some(entry)) => {
                            let meta = if entry.metadata.is_empty() {
                                serde_json::Value::Null
                            } else {
                                serde_json::from_slice(&entry.metadata)
                                    .unwrap_or(serde_json::Value::Null)
                            };
                            metadata_filter.matches(&meta)
                        }
                        _ => false,
                    }
                }));
            }
            Ok(None) => {}
            Err(msg) => return Err(Status::invalid_argument(msg)),
        }

        let results = self
            .index
            .search(&req.query, &params)
            .map_err(Self::to_status)?;

        let elapsed = start.elapsed();
        let latency_us = elapsed.as_micros() as u64;

        let response_results: Vec<SearchResult> = results
            .into_iter()
            .map(|r| SearchResult {
                id: r.id.to_string(),
                score: r.score,
                metadata: self.load_metadata_string(&r.id),
            })
            .collect();

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
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        debug!("Delete request for ID: {}", req.id);

        let vector_id = VectorId::new(&req.id);

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
                self.delete_graph_chunk(&vector_id);
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
        let req = request.into_inner();

        debug!("Update request for ID: {}", req.id);

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

        let vector_id = VectorId::new(&req.id);

        // FIX BUG-052 + BUG-059: Use RAII guard for panic-safe per-key locking
        // The guard automatically releases the lock when dropped, even on panic
        let _lock_guard = UpdateLockGuard::try_acquire(&self.update_locks, req.id.clone())
            .ok_or_else(|| {
                Status::aborted(format!("Concurrent update in progress for ID: {}", req.id))
            })?;

        // Perform the update operation - lock is held by guard
        // Guard will release lock automatically when this function returns (or panics)
        self.do_update_locked(&req.id, &vector_id, &req.vector, &req.metadata)
    }

    #[instrument(skip(self, request))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let vector_id = VectorId::new(&req.id);

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
        }))
    }

    #[instrument(skip(self, request))]
    async fn insert_batch(
        &self,
        request: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertBatchResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        debug!("Insert batch request for {} vectors", req.vectors.len());

        let mut inserted_count = 0u32;
        let mut failed_ids = Vec::new();

        for vector in req.vectors {
            let vector_id = VectorId::new(&vector.id);
            let old_metadata = match self.id_mapping.get_vector(&vector_id) {
                Ok(entry) => entry.map(|entry| entry.metadata),
                Err(e) => {
                    warn!(
                        "Batch insert: ID {} failed during existing metadata lookup: {}",
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
                            inserted_count += 1;
                            self.sync_source_text(&vector_id, &vector.text);
                            if let Some(metadata) = old_metadata {
                                self.delete_graph_edges_from_metadata(&vector_id, &metadata);
                            }
                            self.index_graph_chunk(&vector_id, &vector.metadata);
                            self.index_sql_metadata(&vector_id, internal_id.0, &vector.metadata);
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
        let req = request.into_inner();

        debug!("Search batch request for {} queries", req.queries.len());

        // FIX BUG-H013: Validate top_k to prevent panic in SearchParams::new()
        if req.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be greater than 0"));
        }
        if req.top_k > 10000 {
            return Err(Status::invalid_argument("top_k exceeds maximum of 10000"));
        }

        let params = SearchParams::new(req.top_k as usize).with_nprobe(req.nprobe.unwrap_or(32));

        let mut results = Vec::with_capacity(req.queries.len());

        for query in req.queries {
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
    async fn text_search(
        &self,
        request: Request<TextSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        if req.text.is_empty() {
            return Err(Status::invalid_argument("Text cannot be empty"));
        }
        if req.top_k == 0 {
            return Err(Status::invalid_argument("top_k must be greater than 0"));
        }
        if req.top_k > 10000 {
            return Err(Status::invalid_argument("top_k exceeds maximum of 10000"));
        }

        let metadata_filter = match MetadataFilter::build(&req.filter, req.tag_filter.clone()) {
            Ok(Some(filter)) => Some(Arc::new(filter)),
            Ok(None) => None,
            Err(msg) => return Err(Status::invalid_argument(msg)),
        };

        let requested_mode = Self::requested_text_retrieval_mode(&req)?;
        let mut planner_input = PlannerInput::new(req.text.clone())
            .with_pack(req.pack)
            .with_metadata_filter(metadata_filter.is_some());
        if let Some(mode) = requested_mode {
            planner_input = planner_input.with_requested_mode(mode);
        }
        let planner_trace = plan_query(&planner_input);
        if matches!(planner_trace.mode, RetrievalMode::StructuredSql) {
            return self.sql_metadata_text_search(&req, start);
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
        let search_k = if needs_pool {
            top_k.saturating_mul(4).clamp(top_k, 200)
        } else {
            top_k
        };

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

            debug!(
                text_len = req.text.len(),
                embedding_dim = query_vector.len(),
                "TextSearch embedding generated"
            );

            let mut params = SearchParams::new(search_k).with_nprobe(req.nprobe.unwrap_or(32));
            if let Some(metadata_filter) = metadata_filter.clone() {
                let id_mapping = self.id_mapping.clone();
                params = params.with_filter(Arc::new(move |id: &VectorId| {
                    match id_mapping.get_vector(id) {
                        Ok(Some(entry)) => {
                            let meta = if entry.metadata.is_empty() {
                                serde_json::Value::Null
                            } else {
                                serde_json::from_slice(&entry.metadata)
                                    .unwrap_or(serde_json::Value::Null)
                            };
                            metadata_filter.matches(&meta)
                        }
                        _ => false,
                    }
                }));
            }

            self.index
                .search(&query_vector, &params)
                .map_err(Self::to_status)?
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
        let mut ranked: Vec<ScoredId> = if use_dense && use_lexical {
            let dense_scored: Vec<ScoredId> = dense
                .iter()
                .map(|r| ScoredId::new(r.id.clone(), r.score))
                .collect();
            let fuser = HybridFuser::new().with_weights(
                req.dense_weight.unwrap_or(planner_trace.vector_weight),
                req.lexical_weight.unwrap_or(planner_trace.lexical_weight),
            );
            fuser.fuse(&dense_scored, &lexical, search_k)
        } else if use_lexical {
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
                let seeds = ranked.clone();
                let graph_limit = top_k.saturating_mul(2).max(1);
                for seed in seeds {
                    let seed_node = Self::chunk_node_id(&seed.id);
                    match graph.related_chunks(&seed_node, graph_limit) {
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

        ranked.truncate(top_k);
        let response_results: Vec<SearchResult> = ranked
            .iter()
            .map(|s| SearchResult {
                metadata: self.load_metadata_string(&s.id),
                id: s.id.to_string(),
                score: s.score,
            })
            .collect();

        // Optionally assemble a source-grounded, citation-bearing context pack
        // (PACK-*). Matched child chunks are expanded to their parent context
        // (CHUNK-003) via the `parent_id` metadata convention, deduped by parent,
        // then assembled within the token budget.
        let context_pack = if req.pack {
            let budget = req.pack_token_budget.unwrap_or(1024) as usize;
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
                for result in &response_results {
                    let seed = GraphNodeId::new(format!("chunk:{}", result.id));
                    match graph.related_chunks(&seed, top_k.saturating_mul(2).max(1)) {
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
            pack(&passages, &PackerConfig::new(budget)).text
        } else {
            String::new()
        };

        let elapsed = start.elapsed();
        let latency_us = elapsed.as_micros() as u64;

        info!(
            "TextSearch for '{}' returned {} results in {:?}",
            &req.text[..req.text.len().min(50)],
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Query;
    use akidb_faiss::mock::MockIndex;
    use akidb_storage::RocksDbBackend;
    use tempfile::TempDir;

    fn test_service() -> (AkiDbService<MockIndex, RocksDbBackend>, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        let id_mapping = Arc::new(IdMapping::new(storage, "test"));
        let index = Arc::new(MockIndex::new(2, 16));
        (AkiDbService::new(index, id_mapping, "test"), dir)
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
}
