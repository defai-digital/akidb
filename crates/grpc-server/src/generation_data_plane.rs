//! Immutable gRPC data plane backed by the controller's active generation.
//!
//! Reads clone one complete generation runtime and delegate to the established
//! vector/BM25/graph service. Point and collection mutations are rejected:
//! generation mode changes data only through verified publication.

use std::collections::HashMap;
use std::sync::Arc;

use akidb_common::config::{AclConfig, FilterSettings};
use akidb_contracts::KnowledgeScope;
use akidb_faiss::{DistanceMetric, HnswIndex, VectorPrecision};
use akidb_storage::{RocksDbBackend, ServingStateRecord};
use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use crate::auth::auth_context;
use crate::collections::{CollectionRegistry, SharedCollectionRegistry};
use crate::proto::akidb_server::Akidb;
use crate::proto::{
    CreateCollectionRequest, CreateCollectionResponse, DeleteRequest, DeleteResponse,
    DropCollectionRequest, DropCollectionResponse, GetClusterStateRequest, GetClusterStateResponse,
    GetCollectionRequest, GetCollectionResponse, GetRequest, GetResponse, HealthRequest,
    HealthResponse, InsertBatchRequest, InsertBatchResponse, InsertRequest, InsertResponse,
    ListCollectionsRequest, ListCollectionsResponse, SearchBatchRequest, SearchBatchResponse,
    SearchRequest, SearchResponse, ServingGenerationEvidence, TextSearchRequest, UpdateRequest,
    UpdateResponse,
};
use crate::{
    AkiDbService, EmbeddingProvider, ExpectedActiveGeneration, GenerationControlError,
    GenerationController, ReadyGenerationRuntime,
};

type ImmutableGenerationService = AkiDbService<HnswIndex, RocksDbBackend>;

#[derive(Clone)]
pub struct GenerationDataPlaneConfig {
    pub default_collection: String,
    pub slo_threshold_us: u64,
    pub acl: AclConfig,
    pub filter_settings: FilterSettings,
    pub collections: SharedCollectionRegistry,
    pub embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
}

impl Default for GenerationDataPlaneConfig {
    fn default() -> Self {
        Self {
            default_collection: "default".to_string(),
            slo_threshold_us: 50_000,
            acl: AclConfig::default(),
            filter_settings: FilterSettings::default(),
            collections: Arc::new(CollectionRegistry::new()),
            embedding_provider: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RuntimeCacheKey {
    scope: KnowledgeScope,
    generation_id: String,
    manifest_sha256: String,
}

/// Generation-aware immutable implementation of the public `Akidb` service.
#[derive(Clone)]
pub struct GenerationDataPlane {
    controller: Arc<GenerationController>,
    config: GenerationDataPlaneConfig,
    services: Arc<Mutex<HashMap<RuntimeCacheKey, Arc<ImmutableGenerationService>>>>,
}

impl GenerationDataPlane {
    pub fn new(
        controller: Arc<GenerationController>,
        config: GenerationDataPlaneConfig,
    ) -> Result<Self, String> {
        if config.default_collection.trim().is_empty() {
            return Err("generation default_collection must not be empty".to_string());
        }
        if config.slo_threshold_us == 0 {
            return Err("generation slo_threshold_us must be greater than zero".to_string());
        }
        Ok(Self {
            controller,
            config,
            services: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn controller(&self) -> &Arc<GenerationController> {
        &self.controller
    }

    /// Build BM25/documents and the collection view before committing a local
    /// active-pointer transition.
    pub fn prepare_generation(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> Result<(), GenerationControlError> {
        let runtime = self
            .controller
            .ready_runtime(scope, generation_id)
            .ok_or_else(|| {
                GenerationControlError::InconsistentState(format!(
                    "READY runtime {generation_id} is not retained for {}/{}",
                    scope.workspace_id, scope.collection
                ))
            })?;
        self.service_for_runtime(&runtime);
        Ok(())
    }

    /// Prewarm the complete data service, then atomically activate it.
    pub fn activate(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        expected_active: &ExpectedActiveGeneration,
        updated_at_ms: u64,
    ) -> Result<ServingStateRecord, GenerationControlError> {
        self.prepare_generation(scope, generation_id)?;
        let record =
            self.controller
                .activate(scope, generation_id, expected_active, updated_at_ms)?;
        self.prune_scope(&record);
        Ok(record)
    }

    /// Prewarm the retained rollback service, then atomically restore it.
    pub fn rollback(
        &self,
        scope: &KnowledgeScope,
        target_generation_id: &str,
        expected_active: &ExpectedActiveGeneration,
        updated_at_ms: u64,
    ) -> Result<ServingStateRecord, GenerationControlError> {
        self.prepare_generation(scope, target_generation_id)?;
        let record = self.controller.rollback(
            scope,
            target_generation_id,
            expected_active,
            updated_at_ms,
        )?;
        self.prune_scope(&record);
        Ok(record)
    }

    /// Restore controller state and prewarm the active service on process boot.
    pub fn restore_scope(
        &self,
        scope: &KnowledgeScope,
    ) -> Result<Option<ServingStateRecord>, GenerationControlError> {
        let record = self.controller.restore_scope(scope)?;
        if let Some(active) = record.as_ref().and_then(|state| state.active.as_ref()) {
            self.prepare_generation(scope, &active.manifest.generation_id)?;
        }
        if let Some(record) = &record {
            self.prune_scope(record);
        }
        Ok(record)
    }

    fn active_service(
        &self,
        scope: &KnowledgeScope,
    ) -> Result<(Arc<ImmutableGenerationService>, ServingGenerationEvidence), Status> {
        let runtime = match self.controller.active_runtime(scope) {
            Some(runtime) => Some(runtime),
            None => self
                .controller
                .restore_scope(scope)
                .map_err(|error| Status::internal(error.to_string()))?
                .and_then(|_| self.controller.active_runtime(scope)),
        }
        .ok_or_else(|| {
            Status::unavailable(format!(
                "no active generation serves {}/{}",
                scope.workspace_id, scope.collection
            ))
        })?;
        let evidence = serving_evidence(&runtime);
        Ok((self.service_for_runtime(&runtime), evidence))
    }

    fn service_for_runtime(
        &self,
        runtime: &ReadyGenerationRuntime,
    ) -> Arc<ImmutableGenerationService> {
        let key = runtime_cache_key(runtime);
        let mut services = self.services.lock();
        if let Some(service) = services.get(&key) {
            return service.clone();
        }

        let manifest = &runtime.ready.manifest;
        let materializer_config = self.controller.materializer().config();
        let mut service = AkiDbService::with_slo_threshold(
            runtime.index.clone(),
            runtime.id_mapping.clone(),
            manifest.collection.clone(),
            self.config.slo_threshold_us,
        )
        .with_acl(self.config.acl.clone())
        .with_filter_settings(self.config.filter_settings.clone())
        .with_collections(self.config.collections.clone())
        .with_embedding_model_id(manifest.embedding_model_id.clone())
        .with_graph_index(runtime.graph.clone());
        if let Some(provider) = &self.config.embedding_provider {
            service = service.with_embedding_provider(provider.clone());
        }
        service.seed_collection_schema(
            manifest.embedding_dimensions,
            metric_name(materializer_config.distance_metric),
            precision_name(materializer_config.vector_precision),
            &manifest.embedding_model_id,
        );
        service.rebuild_lexical_index();
        let service = Arc::new(service);
        services.insert(key, service.clone());
        service
    }

    fn scope_for<T>(&self, request: &Request<T>, collection: &str) -> KnowledgeScope {
        KnowledgeScope::new(auth_context(request).workspace_id, collection.to_string())
    }

    fn default_scope_for<T>(&self, request: &Request<T>) -> KnowledgeScope {
        self.scope_for(request, &self.config.default_collection)
    }

    fn prune_scope(&self, record: &ServingStateRecord) {
        let scope = record.scope();
        let mut retained = Vec::with_capacity(3);
        if let Some(generation) = &record.active {
            retained.push(generation.manifest.generation_id.as_str());
        }
        if let Some(generation) = &record.previous {
            retained.push(generation.manifest.generation_id.as_str());
        }
        if let Some(generation) = &record.staged {
            retained.push(generation.manifest.generation_id.as_str());
        }
        self.services
            .lock()
            .retain(|key, _| key.scope != scope || retained.contains(&key.generation_id.as_str()));
    }

    fn immutable_write_error() -> Status {
        Status::failed_precondition(
            "generation serving mode is immutable; publish a verified generation through the privileged generation-management API",
        )
    }
}

fn runtime_cache_key(runtime: &ReadyGenerationRuntime) -> RuntimeCacheKey {
    RuntimeCacheKey {
        scope: runtime.ready.manifest.scope(),
        generation_id: runtime.ready.manifest.generation_id.clone(),
        manifest_sha256: runtime.ready.marker.manifest_sha256.clone(),
    }
}

fn serving_evidence(runtime: &ReadyGenerationRuntime) -> ServingGenerationEvidence {
    ServingGenerationEvidence {
        workspace_id: runtime.ready.manifest.workspace_id.clone(),
        collection: runtime.ready.manifest.collection.clone(),
        generation_id: runtime.ready.manifest.generation_id.clone(),
        manifest_sha256: runtime.ready.marker.manifest_sha256.clone(),
        applied_sequence: runtime.ready.marker.applied_sequence,
    }
}

fn metric_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::L2 => "l2",
        DistanceMetric::InnerProduct => "ip",
    }
}

fn precision_name(precision: VectorPrecision) -> &'static str {
    match precision {
        VectorPrecision::F32 => "f32",
        VectorPrecision::F16 => "f16",
    }
}

#[tonic::async_trait]
impl Akidb for GenerationDataPlane {
    async fn insert(
        &self,
        _request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        Err(Self::immutable_write_error())
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let scope = self.scope_for(&request, &request.get_ref().collection);
        let (service, evidence) = self.active_service(&scope)?;
        let mut response = Akidb::search(service.as_ref(), request).await?;
        response.get_mut().serving_generation = Some(evidence);
        Ok(response)
    }

    async fn delete(
        &self,
        _request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        Err(Self::immutable_write_error())
    }

    async fn update(
        &self,
        _request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        Err(Self::immutable_write_error())
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let scope = self.scope_for(&request, &request.get_ref().collection);
        let (service, evidence) = self.active_service(&scope)?;
        let mut response = Akidb::get(service.as_ref(), request).await?;
        response.get_mut().serving_generation = Some(evidence);
        Ok(response)
    }

    async fn health(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let scope = self.default_scope_for(&request);
        let Some(runtime) = self.controller.active_runtime(&scope) else {
            return Ok(Response::new(HealthResponse {
                healthy: false,
                ready: false,
                message: format!(
                    "No active generation for {}/{}",
                    scope.workspace_id, scope.collection
                ),
                total_vectors: 0,
                active_vectors: 0,
                using_gpu: false,
                serving_generation: None,
            }));
        };
        let evidence = serving_evidence(&runtime);
        let service = self.service_for_runtime(&runtime);
        let mut response = Akidb::health(service.as_ref(), request).await?;
        response.get_mut().serving_generation = Some(evidence);
        Ok(response)
    }

    async fn insert_batch(
        &self,
        _request: Request<InsertBatchRequest>,
    ) -> Result<Response<InsertBatchResponse>, Status> {
        Err(Self::immutable_write_error())
    }

    async fn search_batch(
        &self,
        request: Request<SearchBatchRequest>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        let scope = self.scope_for(&request, &request.get_ref().collection);
        let (service, evidence) = self.active_service(&scope)?;
        let mut response = Akidb::search_batch(service.as_ref(), request).await?;
        for result in &mut response.get_mut().results {
            result.serving_generation = Some(evidence.clone());
        }
        Ok(response)
    }

    async fn get_cluster_state(
        &self,
        request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        let scope = self.default_scope_for(&request);
        let (service, evidence) = self.active_service(&scope)?;
        let mut response = Akidb::get_cluster_state(service.as_ref(), request).await?;
        response.get_mut().serving_generation = Some(evidence);
        Ok(response)
    }

    async fn text_search(
        &self,
        request: Request<TextSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let scope = self.scope_for(&request, &request.get_ref().collection);
        let (service, evidence) = self.active_service(&scope)?;
        let mut response = Akidb::text_search(service.as_ref(), request).await?;
        response.get_mut().serving_generation = Some(evidence);
        Ok(response)
    }

    async fn create_collection(
        &self,
        _request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        Err(Self::immutable_write_error())
    }

    async fn get_collection(
        &self,
        request: Request<GetCollectionRequest>,
    ) -> Result<Response<GetCollectionResponse>, Status> {
        let scope = self.scope_for(&request, &request.get_ref().name);
        let (service, _) = self.active_service(&scope)?;
        Akidb::get_collection(service.as_ref(), request).await
    }

    async fn list_collections(
        &self,
        request: Request<ListCollectionsRequest>,
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        let scope = self.default_scope_for(&request);
        let (service, _) = self.active_service(&scope)?;
        Akidb::list_collections(service.as_ref(), request).await
    }

    async fn drop_collection(
        &self,
        _request: Request<DropCollectionRequest>,
    ) -> Result<Response<DropCollectionResponse>, Status> {
        Err(Self::immutable_write_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_contracts::KnowledgeGenerationManifest;
    use akidb_storage::{GenerationStore, RocksDbBackend, ServingStateStore};
    use serde_json::Value;
    use sha2::{Digest, Sha256};

    const BUNDLE: &[u8] =
        include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle.ndjson");
    const MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn generation(generation_id: &str) -> (Vec<u8>, KnowledgeGenerationManifest, Vec<u8>) {
        let mut manifest: KnowledgeGenerationManifest = serde_json::from_str(MANIFEST).unwrap();
        manifest.generation_id = generation_id.to_string();
        let mut entries: Vec<Value> = BUNDLE
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        entries[0]["header"]["generation_id"] = Value::String(generation_id.to_string());
        let mut bundle = Vec::new();
        for entry in entries {
            bundle.extend(serde_json::to_vec(&entry).unwrap());
            bundle.push(b'\n');
        }
        manifest.bundle.sha256 = digest(&bundle);
        manifest.bundle.size_bytes = bundle.len() as u64;
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        (manifest_bytes, manifest, bundle)
    }

    fn harness() -> (
        tempfile::TempDir,
        Arc<GenerationController>,
        GenerationDataPlane,
        KnowledgeScope,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let generation_store =
            Arc::new(GenerationStore::open(temporary.path().join("generations")).unwrap());
        let materializer = Arc::new(crate::GenerationMaterializer::new(
            generation_store,
            Default::default(),
        ));
        let control = Arc::new(RocksDbBackend::open(temporary.path().join("control")).unwrap());
        let state = Arc::new(ServingStateStore::new(control, "replica-data-plane").unwrap());
        let controller = Arc::new(GenerationController::new(materializer, state));
        let (manifest_bytes, manifest, bundle) = generation("generation-a");
        controller
            .publish_from_reader(
                &manifest_bytes,
                &digest(&manifest_bytes),
                bundle.as_slice(),
                1,
            )
            .unwrap();
        let scope = manifest.scope();
        let plane = GenerationDataPlane::new(
            controller.clone(),
            GenerationDataPlaneConfig {
                default_collection: scope.collection.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        plane
            .activate(
                &scope,
                &manifest.generation_id,
                &ExpectedActiveGeneration::NoActive,
                2,
            )
            .unwrap();
        (temporary, controller, plane, scope)
    }

    fn authorized<T>(message: T, workspace_id: &str) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(crate::AuthContext {
            workspace_id: workspace_id.to_string(),
            agent_id: Some("test-agent".to_string()),
            authenticated: true,
        });
        request
    }

    #[tokio::test]
    async fn dense_and_lexical_reads_report_exact_served_generation() {
        let (_temporary, _controller, plane, scope) = harness();
        let response = plane
            .search(authorized(
                SearchRequest {
                    collection: scope.collection.clone(),
                    query: vec![0.1, 0.2, 0.3],
                    top_k: 1,
                    ..Default::default()
                },
                &scope.workspace_id,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.results[0].id, "chunk-a");
        assert_eq!(
            response.serving_generation.unwrap().generation_id,
            "generation-a"
        );

        let response = plane
            .text_search(authorized(
                TextSearchRequest {
                    collection: scope.collection.clone(),
                    text: "grounded".to_string(),
                    top_k: 1,
                    retrieval_mode: "bm25".to_string(),
                    ..Default::default()
                },
                &scope.workspace_id,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.results[0].id, "chunk-a");
        assert_eq!(
            response.serving_generation.unwrap().generation_id,
            "generation-a"
        );
    }

    #[tokio::test]
    async fn writes_are_rejected_in_generation_mode() {
        let (_temporary, _controller, plane, scope) = harness();
        let error = plane
            .insert(authorized(
                InsertRequest {
                    collection: scope.collection,
                    id: "new".to_string(),
                    vector: vec![0.1, 0.2, 0.3],
                    ..Default::default()
                },
                &scope.workspace_id,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn workspace_scope_cannot_fall_through_to_another_active_runtime() {
        let (_temporary, _controller, plane, scope) = harness();
        let error = plane
            .search(authorized(
                SearchRequest {
                    collection: scope.collection,
                    query: vec![0.1, 0.2, 0.3],
                    top_k: 1,
                    ..Default::default()
                },
                "other-workspace",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }
}
