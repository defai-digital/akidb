//! Build one immutable generation from the logical knowledge bundle.
//!
//! The bundle remains architecture-neutral. This materializer writes
//! generation-local RocksDB payload/text/graph projections and builds HNSW and
//! BM25 in memory as verification gates. Runtime activation can reconstruct
//! those in-memory indexes from the immutable local payload store.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use akidb_common::{AkiDbError, InternalId, VectorId};
use akidb_contracts::{
    KnowledgeAssertionState, KnowledgeBundleEdge, KnowledgeBundleEntry, KnowledgeBundleNode,
    KnowledgeBundleRecord, KnowledgeEdgeKind, KnowledgeGenerationManifest, KnowledgeMutation,
    KnowledgeMutationPayload, KnowledgeNodeKind, KnowledgeOperation, KnowledgeScope,
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
use sha2::{Digest, Sha256};
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
    pub minimum_free_bytes_after_build: u64,
    pub estimated_build_overhead_percent: u16,
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
            minimum_free_bytes_after_build: 1024 * 1024 * 1024,
            estimated_build_overhead_percent: 200,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationDiskAdmission {
    pub available_bytes: u64,
    pub estimated_build_bytes: u64,
    pub required_bytes: u64,
    pub minimum_free_bytes_after_build: u64,
}

/// Open, verified read runtime for one immutable local generation.
pub struct ReadyGenerationRuntime {
    pub ready: ReadyGeneration,
    pub index: Arc<HnswIndex>,
    pub storage: Arc<RocksDbBackend>,
    pub id_mapping: Arc<IdMapping<RocksDbBackend>>,
    pub graph: Arc<NativeGraphIndex<RocksDbBackend>>,
}

#[derive(Debug, Clone)]
pub struct MaterializedKnowledgeMutation {
    pub mutation: KnowledgeMutation,
    pub payload: Option<KnowledgeMutationPayload>,
}

impl GenerationMaterializer {
    pub fn new(store: Arc<GenerationStore>, config: GenerationMaterializerConfig) -> Self {
        Self { store, config }
    }

    pub fn store(&self) -> &Arc<GenerationStore> {
        &self.store
    }

    pub fn config(&self) -> &GenerationMaterializerConfig {
        &self.config
    }

    pub fn prepare(
        &self,
        manifest_bytes: &[u8],
        expected_manifest_sha256: &str,
        updated_at_ms: u64,
    ) -> Result<PreparedGeneration, GenerationMaterializerError> {
        if manifest_bytes.len() > 1024 * 1024 {
            return Err(GenerationMaterializerError::Rejected(
                "generation manifest exceeds the 1 MiB admission limit".to_string(),
            ));
        }
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(manifest_bytes)?;
        manifest.validate().map_err(|error| {
            GenerationMaterializerError::Rejected(format!("invalid generation manifest: {error}"))
        })?;
        self.validate_manifest_preconditions(&manifest)?;
        self.disk_admission(&manifest)?;
        self.store
            .prepare(manifest_bytes, expected_manifest_sha256, updated_at_ms)
            .map_err(Into::into)
    }

    pub fn disk_admission(
        &self,
        manifest: &KnowledgeGenerationManifest,
    ) -> Result<GenerationDiskAdmission, GenerationMaterializerError> {
        let admission = self.disk_admission_evidence(manifest)?;
        if admission.available_bytes < admission.required_bytes {
            return Err(GenerationMaterializerError::Rejected(format!(
                "disk admission rejected generation: available {} bytes, \
                 estimated shadow build {} bytes, required with reserve \
                 {} bytes",
                admission.available_bytes,
                admission.estimated_build_bytes,
                admission.required_bytes
            )));
        }
        Ok(admission)
    }

    /// Calculate current disk evidence without rejecting an already-built
    /// generation. Replica workers use this on every reconciliation so a
    /// process restart cannot make capacity gauges disappear. Call
    /// [`Self::disk_admission`] before starting any shadow build.
    pub fn disk_admission_evidence(
        &self,
        manifest: &KnowledgeGenerationManifest,
    ) -> Result<GenerationDiskAdmission, GenerationMaterializerError> {
        if !(100..=1000).contains(&self.config.estimated_build_overhead_percent) {
            return Err(GenerationMaterializerError::Rejected(
                "estimated build overhead percent must be between 100 and 1000".to_string(),
            ));
        }
        let precision_bytes = match self.config.vector_precision {
            VectorPrecision::F32 => 4_u64,
            VectorPrecision::F16 => 2_u64,
        };
        let vector_bytes = checked_product(&[
            manifest.expected_vector_count,
            u64::from(manifest.embedding_dimensions),
            precision_bytes,
        ])?;
        // Node count is carried in the bundle header rather than the manifest.
        // Vector + edge count is a conservative pre-download proxy.
        let estimated_graph_nodes = manifest
            .expected_vector_count
            .checked_add(manifest.expected_edge_count)
            .ok_or_else(|| {
                GenerationMaterializerError::Rejected(
                    "generation graph-node estimate overflowed".to_string(),
                )
            })?;
        let graph_node_bytes = checked_product(&[estimated_graph_nodes, 256])?;
        let graph_edge_bytes = checked_product(&[manifest.expected_edge_count, 384])?;
        let logical_bytes = manifest
            .bundle
            .size_bytes
            .checked_add(vector_bytes)
            .and_then(|value| value.checked_add(graph_node_bytes))
            .and_then(|value| value.checked_add(graph_edge_bytes))
            .ok_or_else(|| {
                GenerationMaterializerError::Rejected(
                    "generation disk estimate overflowed".to_string(),
                )
            })?;
        let estimated_build_bytes = logical_bytes
            .checked_mul(u64::from(self.config.estimated_build_overhead_percent))
            .and_then(|value| value.checked_add(99))
            .map(|value| value / 100)
            .ok_or_else(|| {
                GenerationMaterializerError::Rejected(
                    "generation disk amplification estimate overflowed".to_string(),
                )
            })?;
        let required_bytes = estimated_build_bytes
            .checked_add(self.config.minimum_free_bytes_after_build)
            .ok_or_else(|| {
                GenerationMaterializerError::Rejected(
                    "generation disk reserve estimate overflowed".to_string(),
                )
            })?;
        let available_bytes = fs2::available_space(self.store.root())?;
        let admission = GenerationDiskAdmission {
            available_bytes,
            estimated_build_bytes,
            required_bytes,
            minimum_free_bytes_after_build: self.config.minimum_free_bytes_after_build,
        };
        Ok(admission)
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
        if ready.marker.record_count > self.config.max_vectors {
            return Err(GenerationMaterializerError::Rejected(format!(
                "ready vector count {} exceeds configured maximum {}",
                ready.marker.record_count, self.config.max_vectors
            )));
        }
        let capacity = usize::try_from(ready.marker.record_count)
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
        let mut active_vectors = id_mapping.load_active_vectors()?;
        active_vectors.sort_by_key(|entry| entry.internal_id);
        if u64::try_from(active_vectors.len()).unwrap_or(u64::MAX) != ready.marker.record_count
            || id_mapping.stored_text_count()? != ready.marker.record_count
        {
            return Err(GenerationMaterializerError::Rejected(
                "ready active payload/text counts differ from the materialization marker"
                    .to_string(),
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
        for (expected_internal_id, stored) in active_vectors.into_iter().enumerate() {
            if stored.internal_id != i64::try_from(expected_internal_id).unwrap_or(i64::MAX) {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "ready vector {} has non-dense internal ID {}",
                    stored.external_id, stored.internal_id
                )));
            }
            let internal_id = index.insert(&VectorId::new(&stored.external_id), &stored.vector)?;
            if internal_id.0 != stored.internal_id {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "ready vector {} rebuilt with internal ID {}, expected {}",
                    stored.external_id, internal_id.0, stored.internal_id
                )));
            }
        }
        let stats = index.stats();
        if stats.active_vectors != ready.marker.record_count || stats.dimensions != dimensions {
            return Err(GenerationMaterializerError::Rejected(
                "rebuilt HNSW statistics differ from the manifest".to_string(),
            ));
        }

        let graph = Arc::new(NativeGraphIndex::new(storage.clone()));
        let graph_stats = graph.stats()?;
        if graph_stats.nodes != ready.marker.node_count
            || graph_stats.edges != ready.marker.edge_count
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

    /// Rebuild a complete mutation-tail revision from the immutable bundle
    /// checkpoint, verify every projection, and atomically seal it.
    ///
    /// Replaying from the base on each checkpoint is intentionally
    /// correctness-first: the live runtime is never modified in place, and a
    /// crash can leave only an ignored shadow directory.
    pub fn materialize_revision(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        mutations: &[MaterializedKnowledgeMutation],
        updated_at_ms: u64,
    ) -> Result<ReadyGenerationRuntime, GenerationMaterializerError> {
        let base = self.store.load_ready(scope, generation_id)?;
        let base_runtime = self.open_ready_generation(base)?;
        self.materialize_revision_from_runtime(&base_runtime, mutations, updated_at_ms)
    }

    /// Build a sealed successor from an already-open immutable runtime.
    ///
    /// A serving RocksDB cannot be opened a second time in the same process.
    /// Taking a checkpoint from the retained runtime also lets later mutation
    /// checkpoints build incrementally while every published revision remains
    /// immutable and independently verified before cutover.
    pub fn materialize_revision_from_runtime(
        &self,
        source: &ReadyGenerationRuntime,
        mutations: &[MaterializedKnowledgeMutation],
        updated_at_ms: u64,
    ) -> Result<ReadyGenerationRuntime, GenerationMaterializerError> {
        let scope = source.ready.manifest.scope();
        let generation_id = source.ready.manifest.generation_id.as_str();
        let source_sequence = source.ready.marker.applied_sequence;
        if mutations.is_empty() {
            return Err(GenerationMaterializerError::Rejected(
                "a post-bundle revision requires at least one mutation".to_string(),
            ));
        }
        for (offset, item) in mutations.iter().enumerate() {
            item.mutation.validate().map_err(|error| {
                GenerationMaterializerError::Rejected(format!("invalid mutation contract: {error}"))
            })?;
            if item.mutation.scope() != scope || item.mutation.generation_id != generation_id {
                return Err(GenerationMaterializerError::Rejected(
                    "mutation scope/generation differs from the source revision".to_string(),
                ));
            }
            let expected_sequence = source_sequence
                .checked_add(u64::try_from(offset).unwrap_or(u64::MAX))
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| {
                    GenerationMaterializerError::Rejected("mutation sequence overflow".to_string())
                })?;
            if item.mutation.sequence != expected_sequence {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "mutation gap: expected {expected_sequence}, observed {}",
                    item.mutation.sequence
                )));
            }
            match (item.mutation.operation, &item.payload) {
                (KnowledgeOperation::Upsert, Some(payload)) => payload
                    .validate_against(&item.mutation, &source.ready.manifest)
                    .map_err(|error| {
                        GenerationMaterializerError::Rejected(format!(
                            "invalid mutation payload: {error}"
                        ))
                    })?,
                (KnowledgeOperation::Delete, None) => {}
                (KnowledgeOperation::Upsert, None) => {
                    return Err(GenerationMaterializerError::Rejected(
                        "upsert mutation payload was not fetched".to_string(),
                    ));
                }
                (KnowledgeOperation::Delete, Some(_)) => {
                    return Err(GenerationMaterializerError::Rejected(
                        "delete mutation unexpectedly contains a payload".to_string(),
                    ));
                }
            }
        }
        let target_sequence = mutations
            .last()
            .map(|item| item.mutation.sequence)
            .unwrap_or(source_sequence);
        let input_digest = mutation_tail_input_digest(&source.ready, mutations)?;
        let Some(prepared) =
            self.store
                .prepare_revision(&scope, generation_id, target_sequence, &input_digest)?
        else {
            let ready = self
                .store
                .load_materialized(&scope, generation_id, target_sequence)?;
            return self.open_ready_generation(ready);
        };

        source.storage.create_checkpoint(prepared.rocksdb_dir())?;

        let storage = Arc::new(RocksDbBackend::open(prepared.rocksdb_dir())?);
        let id_mapping = IdMapping::new(storage.clone(), prepared.manifest().collection.clone());
        let graph = NativeGraphIndex::new(storage.clone());
        for item in mutations {
            apply_materialized_mutation(
                item,
                prepared.manifest(),
                &id_mapping,
                &graph,
                storage.as_ref(),
            )?;
        }
        normalize_internal_ids(&id_mapping)?;
        let (record_count, node_count, edge_count) =
            validate_revision_indexes(&id_mapping, &graph, prepared.manifest(), &self.config)?;
        let materialization_digest =
            logical_materialization_digest(&id_mapping, &graph, prepared.applied_sequence())?;
        storage.flush()?;
        drop(graph);
        drop(id_mapping);
        drop(storage);

        let ready = self.store.finalize_revision(
            &prepared,
            record_count,
            node_count,
            edge_count,
            &materialization_digest,
            updated_at_ms,
        )?;
        self.open_ready_generation(ready)
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
        self.validate_manifest_preconditions(manifest)
    }

    fn validate_manifest_preconditions(
        &self,
        manifest: &KnowledgeGenerationManifest,
    ) -> Result<(), GenerationMaterializerError> {
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

fn checked_product(values: &[u64]) -> Result<u64, GenerationMaterializerError> {
    values.iter().try_fold(1_u64, |product, value| {
        product.checked_mul(*value).ok_or_else(|| {
            GenerationMaterializerError::Rejected(
                "generation disk sizing product overflowed".to_string(),
            )
        })
    })
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

fn apply_materialized_mutation(
    item: &MaterializedKnowledgeMutation,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
    id_mapping: &IdMapping<RocksDbBackend>,
    graph: &NativeGraphIndex<RocksDbBackend>,
    storage: &RocksDbBackend,
) -> Result<(), GenerationMaterializerError> {
    let chunk_id = &item.mutation.chunk_id;
    let evidence_edges: Vec<GraphEdgeId> = graph
        .all_edges()?
        .into_iter()
        .filter(|edge| {
            edge.properties
                .get("evidence_chunk_ids")
                .and_then(Value::as_array)
                .is_some_and(|values| {
                    values
                        .iter()
                        .any(|value| value.as_str() == Some(chunk_id.as_str()))
                })
        })
        .map(|edge| edge.id)
        .collect();
    for edge_id in evidence_edges {
        graph.delete_edge(&edge_id)?;
    }
    graph.delete_node(&native_node_id(
        &manifest.workspace_id,
        KnowledgeNodeKind::Chunk,
        chunk_id,
    ))?;

    let vector_id = VectorId::new(chunk_id.clone());
    match (&item.mutation.operation, &item.payload) {
        (KnowledgeOperation::Delete, None) => {
            id_mapping.mark_deleted(&vector_id)?;
            id_mapping.delete_text(&vector_id)?;
        }
        (KnowledgeOperation::Upsert, Some(payload)) => {
            let internal_id = id_mapping
                .get_internal_id(&vector_id)?
                .unwrap_or(InternalId(0));
            let metadata =
                serde_json::to_vec(&Value::Object(record_metadata(&payload.record, manifest)?))?;
            id_mapping.upsert_with_vector(
                &vector_id,
                internal_id,
                &payload.record.vector,
                &metadata,
            )?;
            id_mapping.store_text(&vector_id, &payload.record.chunk_text)?;
            for node in payload.nodes.iter().cloned() {
                materialize_node(node, manifest, graph, id_mapping, storage)?;
            }
            for edge in payload.edges.iter().cloned() {
                materialize_edge(edge, manifest, graph, id_mapping, storage)?;
            }
        }
        _ => {
            return Err(GenerationMaterializerError::Rejected(
                "mutation operation and fetched payload disagree".to_string(),
            ));
        }
    }
    Ok(())
}

fn normalize_internal_ids(
    id_mapping: &IdMapping<RocksDbBackend>,
) -> Result<(), GenerationMaterializerError> {
    let mut vectors = id_mapping.load_active_vectors()?;
    vectors.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    for (index, vector) in vectors.into_iter().enumerate() {
        let internal_id = i64::try_from(index).map_err(|_| {
            GenerationMaterializerError::Rejected(
                "active vector count cannot fit an internal ID".to_string(),
            )
        })?;
        id_mapping.upsert_with_vector(
            &VectorId::new(vector.external_id),
            InternalId(internal_id),
            &vector.vector,
            &vector.metadata,
        )?;
    }
    Ok(())
}

fn validate_revision_indexes(
    id_mapping: &IdMapping<RocksDbBackend>,
    graph: &NativeGraphIndex<RocksDbBackend>,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
    config: &GenerationMaterializerConfig,
) -> Result<(u64, u64, u64), GenerationMaterializerError> {
    let mut vectors = id_mapping.load_active_vectors()?;
    vectors.sort_by_key(|entry| entry.internal_id);
    let record_count = u64::try_from(vectors.len()).map_err(|_| {
        GenerationMaterializerError::Rejected("revision vector count cannot fit u64".to_string())
    })?;
    if record_count > config.max_vectors || id_mapping.stored_text_count()? != record_count {
        return Err(GenerationMaterializerError::Rejected(
            "revision vector/text counts are inconsistent or exceed limits".to_string(),
        ));
    }
    let dimensions = usize::try_from(manifest.embedding_dimensions).map_err(|_| {
        GenerationMaterializerError::Rejected(
            "revision embedding dimensions cannot fit this platform".to_string(),
        )
    })?;
    let index = HnswIndex::new(HnswConfig {
        dimensions,
        capacity: usize::try_from(record_count).unwrap_or(usize::MAX).max(1),
        m: config.hnsw_m,
        ef_construction: config.hnsw_ef_construction,
        ef_search: config.hnsw_ef_search,
        precision: config.vector_precision,
        metric: config.distance_metric,
    })?;
    for (expected, vector) in vectors.iter().enumerate() {
        let expected = i64::try_from(expected).unwrap_or(i64::MAX);
        if vector.internal_id != expected {
            return Err(GenerationMaterializerError::Rejected(format!(
                "revision vector {} has non-dense internal ID {}",
                vector.external_id, vector.internal_id
            )));
        }
        let inserted = index.insert(&VectorId::new(&vector.external_id), &vector.vector)?;
        if inserted.0 != expected {
            return Err(GenerationMaterializerError::Rejected(
                "revision HNSW internal IDs differ from durable mappings".to_string(),
            ));
        }
    }
    if index.stats().active_vectors != record_count {
        return Err(GenerationMaterializerError::Rejected(
            "revision HNSW count differs from durable vectors".to_string(),
        ));
    }

    let texts = id_mapping.load_all_texts()?;
    let mut lexical = Bm25Index::new();
    for (id, text) in texts {
        lexical.insert(id, &text);
    }
    if u64::try_from(lexical.len()).unwrap_or(u64::MAX) > record_count {
        return Err(GenerationMaterializerError::Rejected(
            "revision lexical index contains more documents than active vectors".to_string(),
        ));
    }

    let stats = graph.stats()?;
    if stats.nodes > config.max_graph_nodes || stats.edges > config.max_graph_edges {
        return Err(GenerationMaterializerError::Rejected(
            "revision graph counts exceed configured limits".to_string(),
        ));
    }
    for node in graph.all_nodes()? {
        if let Some(vector_id) = node.id.as_chunk_vector_id() {
            if id_mapping.get_internal_id(&vector_id)?.is_none() {
                return Err(GenerationMaterializerError::Rejected(format!(
                    "revision graph chunk {} has no active vector",
                    vector_id
                )));
            }
        }
    }
    for edge in graph.all_edges()? {
        if graph.get_node(&edge.from)?.is_none() || graph.get_node(&edge.to)?.is_none() {
            return Err(GenerationMaterializerError::Rejected(format!(
                "revision edge {} references a missing node",
                edge.id
            )));
        }
        if let Some(evidence) = edge
            .properties
            .get("evidence_chunk_ids")
            .and_then(Value::as_array)
        {
            for chunk_id in evidence.iter().filter_map(Value::as_str) {
                if id_mapping
                    .get_internal_id(&VectorId::new(chunk_id))?
                    .is_none()
                {
                    return Err(GenerationMaterializerError::Rejected(format!(
                        "revision edge {} cites deleted evidence chunk {chunk_id}",
                        edge.id
                    )));
                }
            }
        }
    }
    Ok((record_count, stats.nodes, stats.edges))
}

fn mutation_tail_input_digest(
    source: &ReadyGeneration,
    mutations: &[MaterializedKnowledgeMutation],
) -> Result<String, GenerationMaterializerError> {
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"akidb-mutation-tail-input-v2");
    hash_part(&mut digest, source.marker.manifest_sha256.as_bytes());
    hash_part(&mut digest, source.marker.bundle_sha256.as_bytes());
    hash_part(&mut digest, &source.marker.applied_sequence.to_be_bytes());
    hash_part(
        &mut digest,
        source
            .marker
            .materialization_digest
            .as_deref()
            .unwrap_or("immutable-bundle")
            .as_bytes(),
    );
    for item in mutations {
        hash_part(&mut digest, &serde_json::to_vec(&item.mutation)?);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn logical_materialization_digest(
    id_mapping: &IdMapping<RocksDbBackend>,
    graph: &NativeGraphIndex<RocksDbBackend>,
    applied_sequence: u64,
) -> Result<String, GenerationMaterializerError> {
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"akidb-logical-materialization-v1");
    hash_part(&mut digest, &applied_sequence.to_be_bytes());

    let texts: BTreeMap<String, String> = id_mapping
        .load_all_texts()?
        .into_iter()
        .map(|(id, text)| (id.as_str().to_string(), text))
        .collect();
    let mut vectors = id_mapping.load_active_vectors()?;
    vectors.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    for vector in vectors {
        hash_part(&mut digest, vector.external_id.as_bytes());
        for value in vector.vector {
            hash_part(&mut digest, &value.to_bits().to_be_bytes());
        }
        let metadata: Value = serde_json::from_slice(&vector.metadata)?;
        hash_part(&mut digest, &serde_json::to_vec(&metadata)?);
        let text = texts.get(&vector.external_id).ok_or_else(|| {
            GenerationMaterializerError::Rejected(format!(
                "active vector {} lacks source text",
                vector.external_id
            ))
        })?;
        hash_part(&mut digest, text.as_bytes());
    }
    for node in graph.all_nodes()? {
        hash_part(&mut digest, node.id.as_str().as_bytes());
        hash_part(&mut digest, node.kind.as_key().as_bytes());
        hash_part(&mut digest, &serde_json::to_vec(&node.properties)?);
    }
    for edge in graph.all_edges()? {
        hash_part(&mut digest, edge.id.as_str().as_bytes());
        hash_part(&mut digest, edge.from.as_str().as_bytes());
        hash_part(&mut digest, edge.to.as_str().as_bytes());
        hash_part(&mut digest, edge.kind.as_key().as_bytes());
        hash_part(&mut digest, &edge.weight.to_bits().to_be_bytes());
        hash_part(&mut digest, &serde_json::to_vec(&edge.properties)?);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_part(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
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
    fn disk_admission_rejects_before_creating_a_shadow_build() {
        let temporary = tempdir().unwrap();
        let store = Arc::new(GenerationStore::open(temporary.path()).unwrap());
        let config = GenerationMaterializerConfig {
            minimum_free_bytes_after_build: u64::MAX / 2,
            ..GenerationMaterializerConfig::default()
        };
        let materializer = GenerationMaterializer::new(store, config);

        let error = materializer
            .prepare(MANIFEST.as_bytes(), &digest(MANIFEST.as_bytes()), 1)
            .unwrap_err();

        assert!(error.to_string().contains("disk admission rejected"));
        assert!(!temporary.path().join("scopes").exists());
    }

    #[test]
    fn disk_admission_evidence_survives_a_rejected_build() {
        let temporary = tempdir().unwrap();
        let store = Arc::new(GenerationStore::open(temporary.path()).unwrap());
        let config = GenerationMaterializerConfig {
            minimum_free_bytes_after_build: u64::MAX / 2,
            ..GenerationMaterializerConfig::default()
        };
        let materializer = GenerationMaterializer::new(store, config);
        let (_, manifest, _) = fixture();

        let evidence = materializer.disk_admission_evidence(&manifest).unwrap();

        assert!(evidence.available_bytes < evidence.required_bytes);
        assert!(materializer.disk_admission(&manifest).is_err());
    }

    #[test]
    fn disk_admission_reports_estimate_and_post_build_reserve() {
        let temporary = tempdir().unwrap();
        let store = Arc::new(GenerationStore::open(temporary.path()).unwrap());
        let config = GenerationMaterializerConfig {
            minimum_free_bytes_after_build: 0,
            estimated_build_overhead_percent: 100,
            ..GenerationMaterializerConfig::default()
        };
        let materializer = GenerationMaterializer::new(store, config);
        let (_, manifest, _) = fixture();

        let evidence = materializer.disk_admission(&manifest).unwrap();

        assert!(evidence.estimated_build_bytes >= manifest.bundle.size_bytes);
        assert_eq!(evidence.required_bytes, evidence.estimated_build_bytes);
        assert!(evidence.available_bytes >= evidence.required_bytes);
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

    #[test]
    fn mutation_tail_builds_sealed_revisions_and_keeps_base_immutable() {
        let (_temporary, store, prepared) = prepare(MANIFEST.as_bytes());
        let materializer =
            GenerationMaterializer::new(store.clone(), GenerationMaterializerConfig::default());
        materializer
            .install_and_materialize(&prepared, BUNDLE, 2)
            .unwrap();
        let manifest = prepared.manifest().clone();
        let payload_reference = akidb_contracts::ImmutableObjectReference {
            uri: "s3://knowledge/mutations/mutation-11.json".to_string(),
            sha256: "a".repeat(64),
            size_bytes: 1_024,
        };
        let upsert = KnowledgeMutation {
            schema_version: akidb_contracts::KNOWLEDGE_SCHEMA_VERSION,
            mutation_id: "mutation-11".to_string(),
            workspace_id: manifest.workspace_id.clone(),
            collection: manifest.collection.clone(),
            generation_id: manifest.generation_id.clone(),
            sequence: 11,
            operation: KnowledgeOperation::Upsert,
            chunk_id: "chunk-a".to_string(),
            payload: Some(payload_reference),
            created_at_ms: 1_784_995_200_011,
        };
        let payload = KnowledgeMutationPayload {
            schema_version: akidb_contracts::KNOWLEDGE_SCHEMA_VERSION,
            workspace_id: manifest.workspace_id.clone(),
            collection: manifest.collection.clone(),
            generation_id: manifest.generation_id.clone(),
            mutation_id: upsert.mutation_id.clone(),
            sequence: upsert.sequence,
            record: KnowledgeBundleRecord {
                chunk_id: "chunk-a".to_string(),
                doc_id: "doc-a".to_string(),
                doc_version: "version-b".to_string(),
                chunk_hash: "b".repeat(64),
                pipeline_signature: "pipeline-v2".to_string(),
                embedding_model_id: manifest.embedding_model_id.clone(),
                vector: vec![0.9, 0.1, 0.2],
                metadata: Map::from_iter([(
                    "source_uri".to_string(),
                    Value::String("s3://knowledge/documents/doc-a".to_string()),
                )]),
                chunk_text: "revised grounded text".to_string(),
                context_headings: Some("Document > Revised".to_string()),
            },
            nodes: vec![
                KnowledgeBundleNode {
                    node_id: "chunk-a".to_string(),
                    kind: KnowledgeNodeKind::Chunk,
                    properties: Map::from_iter([(
                        "doc_id".to_string(),
                        Value::String("doc-a".to_string()),
                    )]),
                },
                KnowledgeBundleNode {
                    node_id: "entity-a".to_string(),
                    kind: KnowledgeNodeKind::Entity,
                    properties: Map::from_iter([(
                        "entity_type".to_string(),
                        Value::String("product".to_string()),
                    )]),
                },
            ],
            edges: vec![KnowledgeBundleEdge {
                edge_id: "edge-b".to_string(),
                from_node_id: "chunk-a".to_string(),
                to_node_id: "entity-a".to_string(),
                kind: KnowledgeEdgeKind::RelatedTo,
                predicate: Some("mentions_product".to_string()),
                weight: 1.0,
                confidence: 0.99,
                assertion_state: KnowledgeAssertionState::HumanVerified,
                source_uri: "s3://knowledge/documents/doc-a".to_string(),
                source_version: "version-b".to_string(),
                evidence_chunk_ids: vec!["chunk-a".to_string()],
                extractor: "human-review".to_string(),
                valid_from_ms: None,
                valid_to_ms: None,
                observed_at_ms: 1_784_995_200_011,
                properties: Map::new(),
            }],
        };
        let upsert_item = MaterializedKnowledgeMutation {
            mutation: upsert,
            payload: Some(payload),
        };

        let revised = materializer
            .materialize_revision(
                &manifest.scope(),
                &manifest.generation_id,
                std::slice::from_ref(&upsert_item),
                3,
            )
            .unwrap();
        assert_eq!(revised.ready.marker.applied_sequence, 11);
        assert!(revised.ready.marker.materialization_digest.is_some());
        assert_eq!(
            revised.id_mapping.load_all_texts().unwrap(),
            vec![(
                VectorId::new("chunk-a"),
                "revised grounded text".to_string()
            )]
        );
        assert!(revised
            .graph
            .get_edge(&scoped_edge_id("workspace-a", "edge-a"))
            .unwrap()
            .is_none());
        assert!(revised
            .graph
            .get_edge(&scoped_edge_id("workspace-a", "edge-b"))
            .unwrap()
            .is_some());
        drop(revised);

        let delete = MaterializedKnowledgeMutation {
            mutation: KnowledgeMutation {
                schema_version: akidb_contracts::KNOWLEDGE_SCHEMA_VERSION,
                mutation_id: "mutation-12".to_string(),
                workspace_id: manifest.workspace_id.clone(),
                collection: manifest.collection.clone(),
                generation_id: manifest.generation_id.clone(),
                sequence: 12,
                operation: KnowledgeOperation::Delete,
                chunk_id: "chunk-a".to_string(),
                payload: None,
                created_at_ms: 1_784_995_200_012,
            },
            payload: None,
        };
        let deleted = materializer
            .materialize_revision(
                &manifest.scope(),
                &manifest.generation_id,
                &[upsert_item, delete],
                4,
            )
            .unwrap();
        assert_eq!(deleted.ready.marker.applied_sequence, 12);
        assert_eq!(deleted.index.stats().active_vectors, 0);
        assert_eq!(deleted.graph.stats().unwrap().edges, 0);
        drop(deleted);

        let base = materializer
            .open_ready(&manifest.scope(), &manifest.generation_id)
            .unwrap();
        assert_eq!(base.ready.marker.applied_sequence, 10);
        assert_eq!(base.index.stats().active_vectors, 1);
        assert_eq!(
            base.id_mapping.load_all_texts().unwrap()[0].1,
            "grounded text"
        );
    }
}
