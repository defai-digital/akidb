//! Build one immutable generation from the logical knowledge bundle.
//!
//! The bundle remains architecture-neutral. This materializer writes
//! generation-local RocksDB payload/text/graph projections and builds HNSW and
//! BM25 in memory as verification gates. Runtime activation can reconstruct
//! those in-memory indexes from the immutable local payload store.

use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use akidb_common::{AkiDbError, VectorId};
use akidb_contracts::{
    KnowledgeAssertionState, KnowledgeBundleEdge, KnowledgeBundleEntry, KnowledgeBundleNode,
    KnowledgeBundleRecord, KnowledgeEdgeKind, KnowledgeNodeKind, KnowledgeScope,
};
use akidb_faiss::{DistanceMetric, HnswConfig, HnswIndex, VectorIndex, VectorPrecision};
use akidb_graph::{
    EdgeKind, GraphEdge, GraphEdgeId, GraphIndex, GraphNode, GraphNodeId, NativeGraphIndex,
    NodeKind,
};
use akidb_retrieval::Bm25Index;
use akidb_storage::{
    consume_knowledge_bundle_with_limits, GenerationLayoutError, GenerationPrepareOutcome,
    GenerationStore, IdMapping, KnowledgeBundleReadError, KnowledgeBundleReadLimits,
    PreparedGeneration, ReadyGeneration, RocksDbBackend, StorageBackend,
};
use serde_json::{Map, Value};
use thiserror::Error;
use tracing::warn;

const NODE_MAPPING_PREFIX: &[u8] = b"akidb\0knowledge-node-map\0v1\0";
const MAX_FAILURE_EVIDENCE_CHARS: usize = 4_096;

#[derive(Debug, Clone)]
pub struct GenerationMaterializerConfig {
    pub max_vectors: u64,
    pub max_graph_nodes: u64,
    pub max_graph_edges: u64,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub hnsw_ef_search: usize,
    pub vector_precision: VectorPrecision,
    pub distance_metric: DistanceMetric,
    pub bundle_limits: KnowledgeBundleReadLimits,
}

impl Default for GenerationMaterializerConfig {
    fn default() -> Self {
        Self {
            max_vectors: 10_000_000,
            max_graph_nodes: 20_000_000,
            max_graph_edges: 50_000_000,
            hnsw_m: 16,
            hnsw_ef_construction: 128,
            hnsw_ef_search: 64,
            vector_precision: VectorPrecision::F32,
            distance_metric: DistanceMetric::Cosine,
            bundle_limits: KnowledgeBundleReadLimits::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum GenerationMaterializerError {
    #[error("generation materialization I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Layout(#[from] GenerationLayoutError),

    #[error(transparent)]
    Bundle(#[from] KnowledgeBundleReadError),

    #[error("generation materialization index/storage error: {0}")]
    Data(#[from] AkiDbError),

    #[error("generation materialization graph error: {0}")]
    Graph(#[from] akidb_graph::GraphError),

    #[error("generation materialization JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("generation materialization rejected: {0}")]
    Rejected(String),
}

pub struct GenerationMaterializer {
    store: Arc<GenerationStore>,
    config: GenerationMaterializerConfig,
}

/// Open, verified read runtime for one immutable local generation.
pub struct ReadyGenerationRuntime {
    pub ready: ReadyGeneration,
    pub index: Arc<HnswIndex>,
    pub storage: Arc<RocksDbBackend>,
    pub id_mapping: Arc<IdMapping<RocksDbBackend>>,
    pub graph: Arc<NativeGraphIndex<RocksDbBackend>>,
}

impl GenerationMaterializer {
    pub fn new(store: Arc<GenerationStore>, config: GenerationMaterializerConfig) -> Self {
        Self { store, config }
    }

    pub fn store(&self) -> &Arc<GenerationStore> {
        &self.store
    }

    /// Reopen a READY generation and rebuild its in-memory HNSW index from the
    /// immutable durable payload store. The local bundle checksum, READY seal,
    /// payload/text counts, graph counts, and internal ID order are rechecked.
    pub fn open_ready(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> Result<ReadyGenerationRuntime, GenerationMaterializerError> {
        let ready = self.store.load_ready(scope, generation_id)?;
        self.open_ready_generation(ready)
    }

    pub fn open_ready_generation(
        &self,
        ready: ReadyGeneration,
    ) -> Result<ReadyGenerationRuntime, GenerationMaterializerError> {
        let manifest = &ready.manifest;
        if manifest.expected_vector_count > self.config.max_vectors {
            return Err(GenerationMaterializerError::Rejected(format!(
                "ready vector count {} exceeds configured maximum {}",
                manifest.expected_vector_count, self.config.max_vectors
            )));
        }
        let capacity = usize::try_from(manifest.expected_vector_count)
            .map_err(|_| {
                GenerationMaterializerError::Rejected(
                    "ready vector count cannot fit this platform".to_string(),
                )
            })?
            .max(1);
        let dimensions = usize::try_from(manifest.embedding_dimensions).map_err(|_| {
            GenerationMaterializerError::Rejected(
                "ready embedding dimensions cannot fit this platform".to_string(),
            )
        })?;
        let storage = Arc::new(RocksDbBackend::open(ready.rocksdb_dir())?);
        let id_mapping = Arc::new(IdMapping::new(storage.clone(), manifest.collection.clone()));
        if id_mapping.stored_vector_count()? != manifest.expected_vector_count
            || id_mapping.stored_text_count()? != manifest.expected_vector_count
        {
            return Err(GenerationMaterializerError::Rejected(
                "ready payload/text counts differ from the manifest".to_string(),
            ));
        }

        let index = Arc::new(HnswIndex::new(HnswConfig {
            dimensions,
            capacity,
            m: self.config.hnsw_m,
            ef_construction: self.config.hnsw_ef_construction,
            ef_search: self.config.hnsw_ef_search,
            precision: self.config.vector_precision,
            metric: self.config.distance_metric,
        })?);
        for stored in id_mapping.load_active_vectors()? {
            let internal_id = index.insert(&VectorId::new(&stored.external_id), &stored.vector)?;
            if internal_id.0 != stored.internal_id {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "ready vector {} rebuilt with internal ID {}, expected {}",
                    stored.external_id, internal_id.0, stored.internal_id
                )));
            }
        }
        let stats = index.stats();
        if stats.active_vectors != manifest.expected_vector_count || stats.dimensions != dimensions
        {
            return Err(GenerationMaterializerError::Rejected(
                "rebuilt HNSW statistics differ from the manifest".to_string(),
            ));
        }

        let graph = Arc::new(NativeGraphIndex::new(storage.clone()));
        let graph_stats = graph.stats()?;
        if graph_stats.nodes != ready.marker.node_count
            || graph_stats.edges != manifest.expected_edge_count
        {
            return Err(GenerationMaterializerError::Rejected(
                "ready graph statistics differ from the READY seal".to_string(),
            ));
        }

        Ok(ReadyGenerationRuntime {
            ready,
            index,
            storage,
            id_mapping,
            graph,
        })
    }

    /// Install an already-authorized object stream, build every retrieval
    /// projection, verify it, and atomically publish the local READY directory.
    ///
    /// Phase 2 supports self-contained bundles only. Ordered mutation-tail
    /// replay is introduced with the authoritative Phase 3 control plane.
    pub fn install_and_materialize<R: Read>(
        &self,
        prepared: &PreparedGeneration,
        bundle: R,
        updated_at_ms: u64,
    ) -> Result<ReadyGeneration, GenerationMaterializerError> {
        self.store.install_bundle(prepared, bundle, updated_at_ms)?;
        self.materialize_installed(prepared, updated_at_ms)
    }

    /// Materialize a bundle already installed and checksum-verified by the
    /// generation store.
    pub fn materialize_installed(
        &self,
        prepared: &PreparedGeneration,
        updated_at_ms: u64,
    ) -> Result<ReadyGeneration, GenerationMaterializerError> {
        self.validate_preconditions(prepared)?;
        if prepared.outcome() == GenerationPrepareOutcome::AlreadyReady {
            let ready = self.store.load_ready(
                &prepared.manifest().scope(),
                &prepared.manifest().generation_id,
            )?;
            // A READY marker is necessary but not sufficient for recovery.
            // Reopen every durable projection before treating this retry as
            // successful, then immediately release the verification runtime.
            self.open_ready_generation(ready.clone())?;
            return Ok(ready);
        }
        self.store.mark_materializing(prepared, updated_at_ms)?;
        let result = self.materialize_inner(prepared, updated_at_ms);
        if let Err(error) = &result {
            let failure = bounded_failure_evidence(error);
            if let Err(journal_error) = self.store.fail_build(prepared, failure, updated_at_ms) {
                warn!(
                    generation_id = %prepared.manifest().generation_id,
                    error = %journal_error,
                    "failed to persist generation materialization failure"
                );
            }
        }
        result
    }

    fn validate_preconditions(
        &self,
        prepared: &PreparedGeneration,
    ) -> Result<(), GenerationMaterializerError> {
        let manifest = prepared.manifest();
        if manifest.target_sequence != manifest.base_sequence {
            return Err(GenerationMaterializerError::Rejected(
                "Phase 2 materialization requires target_sequence == base_sequence; mutation-tail replay is not implemented"
                    .to_string(),
            ));
        }
        if manifest.expected_vector_count > self.config.max_vectors {
            return Err(GenerationMaterializerError::Rejected(format!(
                "expected vector count {} exceeds configured maximum {}",
                manifest.expected_vector_count, self.config.max_vectors
            )));
        }
        Ok(())
    }

    fn materialize_inner(
        &self,
        prepared: &PreparedGeneration,
        updated_at_ms: u64,
    ) -> Result<ReadyGeneration, GenerationMaterializerError> {
        let manifest = prepared.manifest();
        let capacity = usize::try_from(manifest.expected_vector_count)
            .map_err(|_| {
                GenerationMaterializerError::Rejected(
                    "expected vector count cannot fit this platform".to_string(),
                )
            })?
            .max(1);
        let dimensions = usize::try_from(manifest.embedding_dimensions).map_err(|_| {
            GenerationMaterializerError::Rejected(
                "embedding dimensions cannot fit this platform".to_string(),
            )
        })?;

        let storage = Arc::new(RocksDbBackend::open(prepared.rocksdb_dir())?);
        let id_mapping = IdMapping::new(storage.clone(), manifest.collection.clone());
        let graph = NativeGraphIndex::new(storage.clone());
        let index = HnswIndex::new(HnswConfig {
            dimensions,
            capacity,
            m: self.config.hnsw_m,
            ef_construction: self.config.hnsw_ef_construction,
            ef_search: self.config.hnsw_ef_search,
            precision: self.config.vector_precision,
            metric: self.config.distance_metric,
        })?;
        let mut lexical = Bm25Index::new();
        let bundle = File::open(prepared.bundle_path())?;
        let mut context = MaterializationContext {
            manifest,
            index: &index,
            id_mapping: &id_mapping,
            graph: &graph,
            storage: storage.as_ref(),
            lexical: &mut lexical,
            config: &self.config,
        };

        let summary = consume_knowledge_bundle_with_limits(
            bundle,
            manifest,
            self.config.bundle_limits,
            |entry| consume_entry(entry, &mut context).map_err(|error| error.to_string()),
        )?;

        validate_materialized_indexes(
            &summary,
            &index,
            &id_mapping,
            &graph,
            &lexical,
            &self.config,
        )?;
        storage.flush()?;

        // All RocksDB handles must close before the immutable directory is
        // fsynced and renamed. This prevents background writes after READY.
        drop(graph);
        drop(id_mapping);
        drop(storage);
        drop(index);
        drop(lexical);

        self.store.record_materialization(
            prepared,
            &summary,
            manifest.target_sequence,
            updated_at_ms,
        )?;
        self.store
            .finalize_ready(prepared, updated_at_ms)
            .map_err(Into::into)
    }
}

struct MaterializationContext<'a> {
    manifest: &'a akidb_contracts::KnowledgeGenerationManifest,
    index: &'a HnswIndex,
    id_mapping: &'a IdMapping<RocksDbBackend>,
    graph: &'a NativeGraphIndex<RocksDbBackend>,
    storage: &'a RocksDbBackend,
    lexical: &'a mut Bm25Index,
    config: &'a GenerationMaterializerConfig,
}

fn consume_entry(
    entry: KnowledgeBundleEntry,
    context: &mut MaterializationContext<'_>,
) -> Result<(), GenerationMaterializerError> {
    match entry {
        KnowledgeBundleEntry::Header { header } => {
            if header.node_count > context.config.max_graph_nodes {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "graph node count {} exceeds configured maximum {}",
                    header.node_count, context.config.max_graph_nodes
                )));
            }
            if header.edge_count > context.config.max_graph_edges {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "graph edge count {} exceeds configured maximum {}",
                    header.edge_count, context.config.max_graph_edges
                )));
            }
            Ok(())
        }
        KnowledgeBundleEntry::Record { record } => materialize_record(
            record,
            context.manifest,
            context.index,
            context.id_mapping,
            context.lexical,
        ),
        KnowledgeBundleEntry::Node { node } => materialize_node(
            node,
            context.manifest,
            context.graph,
            context.id_mapping,
            context.storage,
        ),
        KnowledgeBundleEntry::Edge { edge } => materialize_edge(
            edge,
            context.manifest,
            context.graph,
            context.id_mapping,
            context.storage,
        ),
    }
}

fn materialize_record(
    record: KnowledgeBundleRecord,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
    index: &HnswIndex,
    id_mapping: &IdMapping<RocksDbBackend>,
    lexical: &mut Bm25Index,
) -> Result<(), GenerationMaterializerError> {
    let vector_id = VectorId::new(record.chunk_id.clone());
    let metadata = record_metadata(&record, manifest)?;
    let metadata = serde_json::to_vec(&Value::Object(metadata))?;
    let internal_id = index.insert(&vector_id, &record.vector)?;
    id_mapping.upsert_with_vector(&vector_id, internal_id, &record.vector, &metadata)?;
    id_mapping.store_text(&vector_id, &record.chunk_text)?;
    lexical.insert(vector_id, &record.chunk_text);
    Ok(())
}

fn materialize_node(
    node: KnowledgeBundleNode,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
    graph: &NativeGraphIndex<RocksDbBackend>,
    id_mapping: &IdMapping<RocksDbBackend>,
    storage: &RocksDbBackend,
) -> Result<(), GenerationMaterializerError> {
    let kind = native_node_kind(node.kind);
    if node.kind == KnowledgeNodeKind::Chunk
        && id_mapping
            .get_internal_id(&VectorId::new(&node.node_id))?
            .is_none()
    {
        return Err(GenerationMaterializerError::Rejected(format!(
            "chunk graph node {} has no matching bundle record",
            node.node_id
        )));
    }
    let native_id = native_node_id(&manifest.workspace_id, node.kind, &node.node_id);
    store_node_mapping(storage, &node.node_id, &native_id)?;

    let mut properties = node.properties;
    insert_identity(&mut properties, "workspace_id", &manifest.workspace_id)?;
    insert_identity(&mut properties, "generation_id", &manifest.generation_id)?;
    insert_identity(&mut properties, "id", &node.node_id)?;
    if node.kind == KnowledgeNodeKind::Chunk {
        insert_identity(&mut properties, "vector_id", &node.node_id)?;
    }
    let graph_node = GraphNode {
        id: native_id,
        kind,
        properties,
        created_at_ms: manifest.created_at_ms,
        updated_at_ms: manifest.created_at_ms,
    };
    graph.upsert_node(graph_node)?;
    Ok(())
}

fn materialize_edge(
    edge: KnowledgeBundleEdge,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
    graph: &NativeGraphIndex<RocksDbBackend>,
    id_mapping: &IdMapping<RocksDbBackend>,
    storage: &RocksDbBackend,
) -> Result<(), GenerationMaterializerError> {
    let from = load_node_mapping(storage, &edge.from_node_id)?.ok_or_else(|| {
        GenerationMaterializerError::Rejected(format!(
            "edge {} references missing from node {}",
            edge.edge_id, edge.from_node_id
        ))
    })?;
    let to = load_node_mapping(storage, &edge.to_node_id)?.ok_or_else(|| {
        GenerationMaterializerError::Rejected(format!(
            "edge {} references missing to node {}",
            edge.edge_id, edge.to_node_id
        ))
    })?;
    for chunk_id in &edge.evidence_chunk_ids {
        if id_mapping
            .get_internal_id(&VectorId::new(chunk_id))?
            .is_none()
        {
            return Err(GenerationMaterializerError::Rejected(format!(
                "edge {} references missing evidence chunk {}",
                edge.edge_id, chunk_id
            )));
        }
    }

    let mut properties = edge.properties;
    insert_identity(&mut properties, "workspace_id", &manifest.workspace_id)?;
    insert_identity(&mut properties, "generation_id", &manifest.generation_id)?;
    if let Some(predicate) = &edge.predicate {
        insert_identity(&mut properties, "predicate", predicate)?;
    }
    properties.insert("confidence".to_string(), Value::from(edge.confidence));
    properties.insert(
        "assertion_state".to_string(),
        Value::String(assertion_state_name(edge.assertion_state).to_string()),
    );
    insert_identity(&mut properties, "source_uri", &edge.source_uri)?;
    insert_identity(&mut properties, "source_version", &edge.source_version)?;
    properties.insert(
        "evidence_chunk_ids".to_string(),
        Value::Array(
            edge.evidence_chunk_ids
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    insert_identity(&mut properties, "extractor", &edge.extractor)?;
    if let Some(value) = edge.valid_from_ms {
        properties.insert("valid_from_ms".to_string(), Value::from(value));
    }
    if let Some(value) = edge.valid_to_ms {
        properties.insert("valid_to_ms".to_string(), Value::from(value));
    }
    properties.insert(
        "observed_at_ms".to_string(),
        Value::from(edge.observed_at_ms),
    );

    graph.upsert_edge(GraphEdge {
        id: scoped_edge_id(&manifest.workspace_id, &edge.edge_id),
        from,
        to,
        kind: native_edge_kind(edge.kind),
        weight: edge.weight,
        properties,
        created_at_ms: edge.observed_at_ms,
        updated_at_ms: edge.observed_at_ms,
    })?;
    Ok(())
}

fn validate_materialized_indexes(
    summary: &akidb_storage::KnowledgeBundleSummary,
    index: &HnswIndex,
    id_mapping: &IdMapping<RocksDbBackend>,
    graph: &NativeGraphIndex<RocksDbBackend>,
    lexical: &Bm25Index,
    config: &GenerationMaterializerConfig,
) -> Result<(), GenerationMaterializerError> {
    let vector_stats = index.stats();
    if vector_stats.active_vectors != summary.record_count
        || vector_stats.total_vectors != summary.record_count
    {
        return Err(GenerationMaterializerError::Rejected(format!(
            "HNSW count mismatch: expected {}, active {}, total {}",
            summary.record_count, vector_stats.active_vectors, vector_stats.total_vectors
        )));
    }
    let durable_vectors = id_mapping.stored_vector_count()?;
    if durable_vectors != summary.record_count {
        return Err(GenerationMaterializerError::Rejected(format!(
            "durable vector count mismatch: expected {}, observed {}",
            summary.record_count, durable_vectors
        )));
    }
    let durable_texts = id_mapping.stored_text_count()?;
    if durable_texts != summary.record_count {
        return Err(GenerationMaterializerError::Rejected(format!(
            "durable text count mismatch: expected {}, observed {}",
            summary.record_count, durable_texts
        )));
    }
    if u64::try_from(lexical.len()).unwrap_or(u64::MAX) > summary.record_count {
        return Err(GenerationMaterializerError::Rejected(
            "lexical index contains more documents than the bundle".to_string(),
        ));
    }
    let graph_stats = graph.stats()?;
    if graph_stats.nodes != summary.node_count || graph_stats.edges != summary.edge_count {
        return Err(GenerationMaterializerError::Rejected(format!(
            "graph count mismatch: expected {}/{} nodes/edges, observed {}/{}",
            summary.node_count, summary.edge_count, graph_stats.nodes, graph_stats.edges
        )));
    }
    if summary.record_count > config.max_vectors
        || summary.node_count > config.max_graph_nodes
        || summary.edge_count > config.max_graph_edges
    {
        return Err(GenerationMaterializerError::Rejected(
            "materialized counts exceed configured limits".to_string(),
        ));
    }
    Ok(())
}

fn record_metadata(
    record: &KnowledgeBundleRecord,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
) -> Result<Map<String, Value>, GenerationMaterializerError> {
    let mut metadata = record.metadata.clone();
    for (key, value) in [
        ("workspace_id", manifest.workspace_id.as_str()),
        ("collection", manifest.collection.as_str()),
        ("generation_id", manifest.generation_id.as_str()),
        ("chunk_id", record.chunk_id.as_str()),
        ("doc_id", record.doc_id.as_str()),
        ("document_id", record.doc_id.as_str()),
        ("doc_version", record.doc_version.as_str()),
        ("document_version", record.doc_version.as_str()),
        ("chunk_hash", record.chunk_hash.as_str()),
        ("content_hash", record.chunk_hash.as_str()),
        ("pipeline_signature", record.pipeline_signature.as_str()),
        ("embedding_model_id", record.embedding_model_id.as_str()),
    ] {
        insert_identity(&mut metadata, key, value)?;
    }
    if let Some(context_headings) = &record.context_headings {
        insert_identity(&mut metadata, "context_headings", context_headings)?;
    }
    Ok(metadata)
}

fn insert_identity(
    properties: &mut Map<String, Value>,
    key: &str,
    expected: &str,
) -> Result<(), GenerationMaterializerError> {
    match properties.get(key) {
        None => {
            properties.insert(key.to_string(), Value::String(expected.to_string()));
            Ok(())
        }
        Some(Value::String(actual)) if actual == expected => Ok(()),
        Some(_) => Err(GenerationMaterializerError::Rejected(format!(
            "{key} conflicts with the immutable generation identity"
        ))),
    }
}

fn native_node_kind(kind: KnowledgeNodeKind) -> NodeKind {
    match kind {
        KnowledgeNodeKind::Document => NodeKind::Document,
        KnowledgeNodeKind::Chunk => NodeKind::Chunk,
        KnowledgeNodeKind::Section => NodeKind::Section,
        KnowledgeNodeKind::File => NodeKind::File,
        KnowledgeNodeKind::Function => NodeKind::Function,
        KnowledgeNodeKind::Type => NodeKind::Type,
        KnowledgeNodeKind::Module => NodeKind::Module,
        KnowledgeNodeKind::Commit => NodeKind::Commit,
        KnowledgeNodeKind::Person => NodeKind::Person,
        KnowledgeNodeKind::Entity => NodeKind::Entity,
        KnowledgeNodeKind::Memory => NodeKind::Memory,
    }
}

fn native_edge_kind(kind: KnowledgeEdgeKind) -> EdgeKind {
    match kind {
        KnowledgeEdgeKind::ParentOf => EdgeKind::ParentOf,
        KnowledgeEdgeKind::ChildOf => EdgeKind::ChildOf,
        KnowledgeEdgeKind::Contains => EdgeKind::Contains,
        KnowledgeEdgeKind::Mentions => EdgeKind::Mentions,
        KnowledgeEdgeKind::Imports => EdgeKind::Imports,
        KnowledgeEdgeKind::Calls => EdgeKind::Calls,
        KnowledgeEdgeKind::Implements => EdgeKind::Implements,
        KnowledgeEdgeKind::Tests => EdgeKind::Tests,
        KnowledgeEdgeKind::TestedBy => EdgeKind::TestedBy,
        KnowledgeEdgeKind::DependsOn => EdgeKind::DependsOn,
        KnowledgeEdgeKind::OwnedBy => EdgeKind::OwnedBy,
        KnowledgeEdgeKind::ChangedBy => EdgeKind::ChangedBy,
        KnowledgeEdgeKind::RelatedTo => EdgeKind::RelatedTo,
    }
}

fn native_node_id(workspace_id: &str, kind: KnowledgeNodeKind, raw_id: &str) -> GraphNodeId {
    let local_id = match kind {
        KnowledgeNodeKind::Chunk => format!("chunk:{raw_id}"),
        _ => format!("{}:{raw_id}", native_node_kind(kind).as_key()),
    };
    scoped_node_id(workspace_id, local_id)
}

fn scoped_node_id(workspace_id: &str, local_id: String) -> GraphNodeId {
    if workspace_id == "default" {
        GraphNodeId::new(local_id)
    } else {
        GraphNodeId::scoped(workspace_id, &local_id)
    }
}

fn scoped_edge_id(workspace_id: &str, raw_id: &str) -> GraphEdgeId {
    if workspace_id == "default" {
        GraphEdgeId::new(raw_id)
    } else {
        GraphEdgeId::new(format!(
            "workspace:{}:{}:{}",
            workspace_id.len(),
            workspace_id,
            raw_id
        ))
    }
}

fn node_mapping_key(raw_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_MAPPING_PREFIX.len() + 8 + raw_id.len());
    key.extend_from_slice(NODE_MAPPING_PREFIX);
    key.extend_from_slice(&(raw_id.len() as u64).to_be_bytes());
    key.extend_from_slice(raw_id.as_bytes());
    key
}

fn store_node_mapping(
    storage: &RocksDbBackend,
    raw_id: &str,
    native_id: &GraphNodeId,
) -> Result<(), GenerationMaterializerError> {
    let key = node_mapping_key(raw_id);
    if let Some(existing) = storage.get(&key)? {
        if existing != native_id.as_str().as_bytes() {
            return Err(GenerationMaterializerError::Rejected(format!(
                "graph node ID {raw_id} maps to conflicting native IDs"
            )));
        }
        return Ok(());
    }
    storage.put(&key, native_id.as_str().as_bytes())?;
    Ok(())
}

fn load_node_mapping(
    storage: &RocksDbBackend,
    raw_id: &str,
) -> Result<Option<GraphNodeId>, GenerationMaterializerError> {
    storage
        .get(&node_mapping_key(raw_id))?
        .map(|bytes| {
            String::from_utf8(bytes)
                .map(GraphNodeId::new)
                .map_err(|error| {
                    GenerationMaterializerError::Rejected(format!(
                        "graph node mapping for {raw_id} is invalid UTF-8: {error}"
                    ))
                })
        })
        .transpose()
}

fn assertion_state_name(state: KnowledgeAssertionState) -> &'static str {
    match state {
        KnowledgeAssertionState::Asserted => "asserted",
        KnowledgeAssertionState::Extracted => "extracted",
        KnowledgeAssertionState::Inferred => "inferred",
        KnowledgeAssertionState::HumanVerified => "human_verified",
    }
}

fn bounded_failure_evidence(error: &GenerationMaterializerError) -> String {
    let message = error.to_string();
    let mut bounded: String = message.chars().take(MAX_FAILURE_EVIDENCE_CHARS).collect();
    if bounded.trim().is_empty() {
        bounded = "generation materialization failed".to_string();
    }
    bounded
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_contracts::KnowledgeGenerationManifest;
    use akidb_faiss::{SearchParams, VectorIndex};
    use akidb_storage::{GenerationBuildJournal, GenerationBuildPhase};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    const BUNDLE: &[u8] =
        include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle.ndjson");
    const MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn fixture() -> (Vec<u8>, KnowledgeGenerationManifest, Vec<u8>) {
        let manifest_bytes = MANIFEST.as_bytes().to_vec();
        let manifest = serde_json::from_slice(&manifest_bytes).unwrap();
        (manifest_bytes, manifest, BUNDLE.to_vec())
    }

    fn rewrite_bundle(
        mut manifest: KnowledgeGenerationManifest,
        mutate: impl FnOnce(&mut [Value]),
    ) -> (Vec<u8>, KnowledgeGenerationManifest, Vec<u8>) {
        let mut lines: Vec<Value> = BUNDLE
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        mutate(&mut lines);
        let mut bundle = Vec::new();
        for line in lines {
            bundle.extend(serde_json::to_vec(&line).unwrap());
            bundle.push(b'\n');
        }
        manifest.bundle.sha256 = digest(&bundle);
        manifest.bundle.size_bytes = bundle.len() as u64;
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        (manifest_bytes, manifest, bundle)
    }

    fn prepare(
        manifest_bytes: &[u8],
    ) -> (tempfile::TempDir, Arc<GenerationStore>, PreparedGeneration) {
        let temporary = tempdir().unwrap();
        let store = Arc::new(GenerationStore::open(temporary.path()).unwrap());
        let prepared = store
            .prepare(manifest_bytes, &digest(manifest_bytes), 1)
            .unwrap();
        (temporary, store, prepared)
    }

    fn journal(prepared: &PreparedGeneration) -> GenerationBuildJournal {
        serde_json::from_slice(
            &std::fs::read(prepared.building_dir().join("build-journal.json")).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn materializes_one_bundle_into_rebuildable_ready_indexes() {
        let (_temporary, store, prepared) = prepare(MANIFEST.as_bytes());
        let materializer =
            GenerationMaterializer::new(store, GenerationMaterializerConfig::default());
        let ready = materializer
            .install_and_materialize(&prepared, BUNDLE, 2)
            .unwrap();
        assert_eq!(ready.marker.record_count, 1);
        assert_eq!(ready.marker.node_count, 2);
        assert_eq!(ready.marker.edge_count, 1);

        let runtime = materializer.open_ready_generation(ready.clone()).unwrap();
        let stored = runtime
            .id_mapping
            .get_vector(&VectorId::new("chunk-a"))
            .unwrap()
            .unwrap();
        let metadata: Value = serde_json::from_slice(&stored.metadata).unwrap();
        assert_eq!(metadata["workspace_id"], "workspace-a");
        assert_eq!(metadata["generation_id"], "generation-bundle-fixture");
        assert_eq!(metadata["document_id"], "doc-a");
        assert_eq!(metadata["document_version"], "version-a");
        assert_eq!(
            metadata["content_hash"],
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            runtime.id_mapping.load_all_texts().unwrap(),
            vec![(VectorId::new("chunk-a"), "grounded text".to_string())]
        );

        let results = runtime
            .index
            .search(&[0.1, 0.2, 0.3], &SearchParams::new(1))
            .unwrap();
        assert_eq!(results[0].id, VectorId::new("chunk-a"));

        let chunk = runtime
            .graph
            .get_node(&native_node_id(
                "workspace-a",
                KnowledgeNodeKind::Chunk,
                "chunk-a",
            ))
            .unwrap()
            .unwrap();
        assert_eq!(
            chunk.properties["generation_id"],
            "generation-bundle-fixture"
        );
        let edge = runtime
            .graph
            .get_edge(&scoped_edge_id("workspace-a", "edge-a"))
            .unwrap()
            .unwrap();
        assert_eq!(edge.properties["predicate"], "mentions_product");
        assert_eq!(edge.properties["assertion_state"], "extracted");
        assert_eq!(edge.properties["evidence_chunk_ids"][0], "chunk-a");
    }

    #[test]
    fn ready_generation_retry_revalidates_without_rebuilding() {
        let (_temporary, store, prepared) = prepare(MANIFEST.as_bytes());
        let materializer =
            GenerationMaterializer::new(store.clone(), GenerationMaterializerConfig::default());
        let first = materializer
            .install_and_materialize(&prepared, BUNDLE, 2)
            .unwrap();

        let retried = store
            .prepare(MANIFEST.as_bytes(), &digest(MANIFEST.as_bytes()), 3)
            .unwrap();
        assert_eq!(retried.outcome(), GenerationPrepareOutcome::AlreadyReady);
        let second = materializer
            .install_and_materialize(&retried, std::io::empty(), 4)
            .unwrap();

        assert_eq!(second.marker, first.marker);
        assert!(!retried.building_dir().exists());
        assert!(retried.ready_dir().exists());
    }

    #[test]
    fn conflicting_workspace_metadata_fails_closed() {
        let (_, manifest, _) = fixture();
        let (manifest_bytes, _, bundle) = rewrite_bundle(manifest, |lines| {
            lines[1]["record"]["metadata"]["workspace_id"] = Value::String("other".to_string());
        });
        let (_temporary, store, prepared) = prepare(&manifest_bytes);
        let materializer =
            GenerationMaterializer::new(store, GenerationMaterializerConfig::default());

        let error = materializer
            .install_and_materialize(&prepared, bundle.as_slice(), 2)
            .unwrap_err();
        assert!(error.to_string().contains("workspace_id conflicts"));
        assert_eq!(journal(&prepared).phase, GenerationBuildPhase::Failed);
        assert!(!prepared.ready_dir().exists());
    }

    #[test]
    fn graph_edge_with_missing_endpoint_never_becomes_ready() {
        let (_, manifest, _) = fixture();
        let (manifest_bytes, _, bundle) = rewrite_bundle(manifest, |lines| {
            lines[4]["edge"]["to_node_id"] = Value::String("missing-entity".to_string());
        });
        let (_temporary, store, prepared) = prepare(&manifest_bytes);
        let materializer =
            GenerationMaterializer::new(store, GenerationMaterializerConfig::default());

        let error = materializer
            .install_and_materialize(&prepared, bundle.as_slice(), 2)
            .unwrap_err();
        assert!(error.to_string().contains("missing to node"));
        assert_eq!(journal(&prepared).phase, GenerationBuildPhase::Failed);
        assert!(!prepared.ready_dir().exists());
    }

    #[test]
    fn materializing_journal_resumes_idempotently_after_partial_payload_write() {
        let (_temporary, store, prepared) = prepare(MANIFEST.as_bytes());
        store.install_bundle(&prepared, BUNDLE, 2).unwrap();
        store.mark_materializing(&prepared, 3).unwrap();

        let storage = Arc::new(RocksDbBackend::open(prepared.rocksdb_dir()).unwrap());
        let mapping = IdMapping::new(storage.clone(), prepared.manifest().collection.clone());
        let index = HnswIndex::new(HnswConfig::new(3).with_capacity(1)).unwrap();
        let mut lexical = Bm25Index::new();
        let entry: KnowledgeBundleEntry =
            serde_json::from_slice(BUNDLE.split(|byte| *byte == b'\n').nth(1).unwrap()).unwrap();
        let KnowledgeBundleEntry::Record { record } = entry else {
            panic!("fixture line two must be a record");
        };
        materialize_record(record, prepared.manifest(), &index, &mapping, &mut lexical).unwrap();
        storage.flush().unwrap();
        drop(mapping);
        drop(storage);

        let materializer =
            GenerationMaterializer::new(store, GenerationMaterializerConfig::default());
        let ready = materializer.materialize_installed(&prepared, 4).unwrap();
        let runtime = materializer.open_ready_generation(ready).unwrap();
        assert_eq!(runtime.id_mapping.stored_vector_count().unwrap(), 1);
        assert_eq!(runtime.index.stats().active_vectors, 1);
        assert_eq!(runtime.graph.stats().unwrap().edges, 1);
    }

    #[test]
    fn phase_two_rejects_unapplied_mutation_tail_without_false_failure() {
        let (_, mut manifest, bundle) = fixture();
        manifest.target_sequence = manifest.base_sequence + 1;
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let (_temporary, store, prepared) = prepare(&manifest_bytes);
        let materializer =
            GenerationMaterializer::new(store, GenerationMaterializerConfig::default());

        let error = materializer
            .install_and_materialize(&prepared, bundle.as_slice(), 2)
            .unwrap_err();
        assert!(error.to_string().contains("mutation-tail replay"));
        assert_eq!(
            journal(&prepared).phase,
            GenerationBuildPhase::BundleVerified
        );
        assert!(!prepared.ready_dir().exists());
    }
}
