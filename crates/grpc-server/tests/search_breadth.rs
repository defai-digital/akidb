//! RPC omission must reach the real HNSW index without inventing an override.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use akidb_common::config::{FilterMode, FilterSettings};
use akidb_faiss::{
    HnswConfig, HnswIndex, IndexStats, InternalId, Result, SearchParams, SearchResult,
    SearchWindow, VectorId, VectorIndex,
};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::{
    InsertRequest, Query, SearchBatchRequest, SearchRequest, TextSearchRequest,
};
use akidb_grpc::{AkiDbService, EmbeddingProvider};
use akidb_storage::{IdMapping, RocksDbBackend};
use tonic::{Code, Request};

struct ObservedIndex {
    inner: HnswIndex,
    breadths: Mutex<Vec<Option<u32>>>,
    predicate_calls: Arc<Mutex<HashMap<String, usize>>>,
}

impl VectorIndex for ObservedIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        self.inner.insert(id, vector)
    }
    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        self.inner.insert_batch(vectors)
    }
    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        self.breadths.lock().unwrap().push(params.nprobe);
        let mut observed = params.clone();
        if let Some(filter) = &params.filter {
            assert!(params.cache_filter_rejections);
            let filter = filter.clone();
            let calls = self.predicate_calls.clone();
            observed.filter = Some(Arc::new(move |id| {
                *calls.lock().unwrap().entry(id.to_string()).or_default() += 1;
                filter(id)
            }));
        }
        self.inner.search(query, &observed)
    }
    fn search_window(&self, query: &[f32], params: &SearchParams) -> Result<SearchWindow> {
        self.breadths.lock().unwrap().push(params.nprobe);
        let mut observed = params.clone();
        if let Some(filter) = &params.filter {
            assert!(params.cache_filter_rejections);
            let filter = filter.clone();
            let calls = self.predicate_calls.clone();
            observed.filter = Some(Arc::new(move |id| {
                *calls.lock().unwrap().entry(id.to_string()).or_default() += 1;
                filter(id)
            }));
        }
        self.inner.search_window(query, &observed)
    }
    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        queries
            .iter()
            .map(|query| self.search(query, params))
            .collect()
    }
    fn delete(&self, id: InternalId) -> Result<()> {
        self.inner.delete(id)
    }
    fn is_deleted(&self, id: InternalId) -> bool {
        self.inner.is_deleted(id)
    }
    fn get_vector(&self, id: InternalId) -> Result<Option<Vec<f32>>> {
        self.inner.get_vector(id)
    }
    fn stats(&self) -> IndexStats {
        self.inner.stats()
    }
    fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }
    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }
    fn train(&self, vectors: &[f32]) -> Result<()> {
        self.inner.train(vectors)
    }
    fn trigger_rebuild(&self) -> Result<()> {
        self.inner.trigger_rebuild()
    }
    fn tombstoned_count(&self) -> u64 {
        self.inner.tombstoned_count()
    }
    fn total_count(&self) -> u64 {
        self.inner.total_count()
    }
    fn compact_tombstones(&self) -> Result<u64> {
        self.inner.compact_tombstones()
    }
    fn is_rebuilding(&self) -> bool {
        self.inner.is_rebuilding()
    }
}

struct Embedder;
impl EmbeddingProvider for Embedder {
    fn embed_text(&self, _: &str) -> std::result::Result<Vec<f32>, String> {
        Ok(vec![1.0, 0.0])
    }
    fn embedding_dimensions(&self) -> usize {
        2
    }
}

#[tokio::test]
async fn every_dense_rpc_preserves_omission_and_explicit_breadth() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
    let index = Arc::new(ObservedIndex {
        inner: HnswIndex::new(HnswConfig::new(2).with_capacity(8).with_ef_search(96)).unwrap(),
        breadths: Mutex::new(Vec::new()),
        predicate_calls: Arc::new(Mutex::new(HashMap::new())),
    });
    let service = AkiDbService::new(
        index.clone(),
        Arc::new(IdMapping::new(storage, "test")),
        "test",
    )
    .with_embedding_provider(Arc::new(Embedder));
    service
        .insert(Request::new(InsertRequest {
            collection: "test".into(),
            id: "row".into(),
            vector: vec![1.0, 0.0],
            text: "row".into(),
            ..Default::default()
        }))
        .await
        .unwrap();

    for nprobe in [None, Some(32), Some(128), Some(0)] {
        index.breadths.lock().unwrap().clear();
        let search = service
            .search(Request::new(SearchRequest {
                collection: "test".into(),
                query: vec![1.0, 0.0],
                top_k: 1,
                nprobe,
                ..Default::default()
            }))
            .await;
        let batch = service
            .search_batch(Request::new(SearchBatchRequest {
                collection: "test".into(),
                queries: vec![Query {
                    vector: vec![1.0, 0.0],
                }],
                top_k: 1,
                nprobe,
                ..Default::default()
            }))
            .await;
        let text = service
            .text_search(Request::new(TextSearchRequest {
                collection: "test".into(),
                text: "row".into(),
                top_k: 1,
                nprobe,
                retrieval_mode: "dense".into(),
                ..Default::default()
            }))
            .await;
        if nprobe == Some(0) {
            assert_eq!(search.unwrap_err().code(), Code::InvalidArgument);
            assert_eq!(batch.unwrap_err().code(), Code::InvalidArgument);
            assert_eq!(text.unwrap_err().code(), Code::InvalidArgument);
            assert!(index.breadths.lock().unwrap().is_empty());
        } else {
            assert_eq!(search.unwrap().into_inner().results.len(), 1);
            assert_eq!(batch.unwrap().into_inner().results[0].results.len(), 1);
            assert_eq!(text.unwrap().into_inner().results.len(), 1);
            assert_eq!(*index.breadths.lock().unwrap(), vec![nprobe; 3]);
        }
    }

    // Exercise the coordinator's actual server and fanout path over a real
    // loopback gRPC connection, not only the request-construction helper.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming =
        tonic::transport::server::TcpIncoming::from_listener(listener, true, None).unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(akidb_grpc::proto::akidb_server::AkidbServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let shards = vec![akidb_coordinator::ShardInfo {
        id: "shard-0".into(),
        address: address.to_string(),
        healthy: true,
    }];
    let coordinator = akidb_coordinator::CoordinatorService::new(
        shards.clone(),
        std::time::Duration::from_secs(5),
        1,
        Default::default(),
        "127.0.0.1:0".into(),
    );
    for nprobe in [None, Some(32), Some(128), Some(0)] {
        index.breadths.lock().unwrap().clear();
        let response = coordinator
            .search(Request::new(SearchRequest {
                collection: "test".into(),
                query: vec![1.0, 0.0],
                top_k: 1,
                nprobe,
                ..Default::default()
            }))
            .await;
        if nprobe == Some(0) {
            assert_eq!(response.unwrap_err().code(), Code::InvalidArgument);
            assert!(index.breadths.lock().unwrap().is_empty());
        } else {
            let response = response.unwrap().into_inner();
            assert!(!response.partial);
            assert_eq!(response.results.len(), 1);
            assert_eq!(*index.breadths.lock().unwrap(), vec![nprobe]);
        }
    }
    let executor = akidb_coordinator::FanoutExecutor::new(
        Arc::new(tokio::sync::RwLock::new(
            akidb_coordinator::ShardRouter::new(shards),
        )),
        std::time::Duration::from_secs(5),
    );
    for nprobe in [None, Some(128)] {
        index.breadths.lock().unwrap().clear();
        let workflow = akidb_coordinator::QueryWorkflow::new(
            vec![1.0, 0.0],
            1,
            std::time::Duration::from_secs(5),
        )
        .with_collection("test");
        let workflow = if let Some(value) = nprobe {
            workflow.with_nprobe(value)
        } else {
            workflow
        };
        let result = workflow.execute(&executor).await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(*index.breadths.lock().unwrap(), vec![nprobe]);
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn selective_rpc_does_not_reread_rejected_metadata_during_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
    let index = Arc::new(ObservedIndex {
        inner: HnswIndex::new(HnswConfig::new(2).with_capacity(100)).unwrap(),
        breadths: Mutex::new(Vec::new()),
        predicate_calls: Arc::new(Mutex::new(HashMap::new())),
    });
    let service = AkiDbService::new(
        index.clone(),
        Arc::new(IdMapping::new(storage, "test")),
        "test",
    );
    for row in 0..100 {
        service
            .insert(Request::new(InsertRequest {
                collection: "test".into(),
                id: row.to_string(),
                vector: vec![row as f32, 1.0],
                metadata: br#"{"bucket":"present"}"#.to_vec(),
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    let response = service
        .search(Request::new(SearchRequest {
            collection: "test".into(),
            query: vec![1.0, 1.0],
            top_k: 1,
            filter: br#"{"bucket":"missing"}"#.to_vec(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.results.is_empty());
    let calls = index.predicate_calls.lock().unwrap();
    assert_eq!(calls.len(), 100);
    assert!(calls.values().all(|count| *count == 1));
}

#[tokio::test]
async fn filtered_candidate_exhaustion_is_visible_without_a_context_pack() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
    let index = Arc::new(HnswIndex::new(HnswConfig::new(2).with_capacity(32)).unwrap());
    let service = AkiDbService::new(index, Arc::new(IdMapping::new(storage, "test")), "test")
        .with_embedding_provider(Arc::new(Embedder))
        .with_filter_settings(FilterSettings {
            mode: FilterMode::Pre,
            postfilter_overfetch_factor: 1,
            max_postfilter_candidates: 4,
            ..FilterSettings::default()
        });
    for row in 0..20 {
        service
            .insert(Request::new(InsertRequest {
                collection: "test".into(),
                id: format!("denied-{row}"),
                vector: vec![1.0, row as f32 * 0.001],
                metadata: br#"{"bucket":"denied"}"#.to_vec(),
                text: "denied".into(),
            }))
            .await
            .unwrap();
    }
    service
        .insert(Request::new(InsertRequest {
            collection: "test".into(),
            id: "allowed".into(),
            vector: vec![-1.0, 0.0],
            metadata: br#"{"bucket":"allowed"}"#.to_vec(),
            text: "allowed".into(),
        }))
        .await
        .unwrap();

    let filter = br#"{"bucket":"allowed"}"#.to_vec();
    let vector = service
        .search(Request::new(SearchRequest {
            collection: "test".into(),
            query: vec![1.0, 0.0],
            top_k: 1,
            filter: filter.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(vector.results.is_empty());
    assert!(vector.candidate_limited);

    let text = service
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "query".into(),
            top_k: 1,
            filter,
            retrieval_mode: "dense".into(),
            pack: false,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(text.results.is_empty());
    assert!(text.context_pack_v1.is_none());
    assert!(text.candidate_limited);
}
