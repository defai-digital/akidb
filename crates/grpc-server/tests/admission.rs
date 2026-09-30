//! Shard-side bounded admission rejects overload with RESOURCE_EXHAUSTED and
//! refuses already-expired client deadlines before expensive work starts.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use akidb_common::config::AdmissionConfig;
use akidb_faiss::{
    IndexStats, InternalId, MockIndex, Result, SearchParams, SearchResult, VectorId, VectorIndex,
};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::SearchRequest;
use akidb_grpc::{AdmissionController, AkiDbService};
use akidb_storage::{IdMapping, RocksDbBackend};
use tonic::{Code, Request};

/// Search gate shared with the test driver: `search` blocks until the driver
/// opens the gate, so a test can hold the single search permit deterministically.
struct Gate {
    opened: bool,
    entered: usize,
}

struct GatedIndex {
    inner: MockIndex,
    state: Arc<(Mutex<Gate>, Condvar)>,
}

impl GatedIndex {
    fn new(dimensions: usize) -> Self {
        Self {
            inner: MockIndex::new(dimensions, 16),
            state: Arc::new((
                Mutex::new(Gate {
                    opened: false,
                    entered: 0,
                }),
                Condvar::new(),
            )),
        }
    }

    fn open(&self) {
        let (lock, cvar) = &*self.state;
        let mut gate = lock.lock().unwrap();
        gate.opened = true;
        cvar.notify_all();
    }

    fn entered(&self) -> usize {
        self.state.0.lock().unwrap().entered
    }

    fn wait_until_open(&self) {
        let (lock, cvar) = &*self.state;
        let mut gate = lock.lock().unwrap();
        gate.entered += 1;
        while !gate.opened {
            gate = cvar.wait(gate).unwrap();
        }
    }
}

impl VectorIndex for GatedIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        self.inner.insert(id, vector)
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        self.inner.insert_batch(vectors)
    }

    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        self.wait_until_open();
        self.inner.search(query, params)
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

fn service_for(
    index: Arc<GatedIndex>,
    storage: Arc<RocksDbBackend>,
) -> AkiDbService<GatedIndex, RocksDbBackend> {
    AkiDbService::new(index, Arc::new(IdMapping::new(storage, "test")), "test").with_admission(
        AdmissionController::new(&AdmissionConfig {
            enabled: true,
            search_permits: 1,
            mutation_permits: 1,
            acquire_timeout_ms: 0,
        }),
    )
}

fn search_request() -> Request<SearchRequest> {
    Request::new(SearchRequest {
        collection: "test".into(),
        query: vec![1.0, 0.0],
        top_k: 1,
        ..Default::default()
    })
}

fn storage() -> (Arc<RocksDbBackend>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
    (storage, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_search_class_rejects_concurrent_search_with_resource_exhausted() {
    let (storage, _dir) = storage();
    let index = Arc::new(GatedIndex::new(2));
    let service = Arc::new(service_for(index.clone(), storage));

    let first = {
        let service = service.clone();
        tokio::spawn(async move { service.search(search_request()).await })
    };
    // Wait until the first search is inside the gated index and therefore
    // holding the only search permit.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while index.entered() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "first search never entered the index"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let err = service
        .search(search_request())
        .await
        .expect_err("saturated search class must reject the concurrent search");
    assert_eq!(
        err.code(),
        Code::ResourceExhausted,
        "unexpected rejection: {err}"
    );

    index.open();
    let response = first.await.unwrap().expect("admitted search must complete");
    assert!(!response.get_ref().partial);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_grpc_deadline_is_rejected_before_index_work() {
    let (storage, _dir) = storage();
    let index = Arc::new(GatedIndex::new(2));
    let service = service_for(index.clone(), storage);

    let mut request = search_request();
    request
        .metadata_mut()
        .insert("grpc-timeout", "0S".parse().unwrap());
    let err = service
        .search(request)
        .await
        .expect_err("an expired client deadline must not start index work");
    assert_eq!(
        err.code(),
        Code::DeadlineExceeded,
        "unexpected rejection: {err}"
    );
    assert_eq!(
        index.entered(),
        0,
        "the index must not be touched for an expired deadline"
    );

    let mut request = search_request();
    request
        .metadata_mut()
        .insert("grpc-timeout", "30S".parse().unwrap());
    index.open();
    service
        .search(request)
        .await
        .expect("a live client deadline must be admitted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_admission_keeps_unlimited_concurrency() {
    static SEARCHED: AtomicBool = AtomicBool::new(false);

    struct UngatedIndex {
        inner: MockIndex,
    }

    impl VectorIndex for UngatedIndex {
        fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
            self.inner.insert(id, vector)
        }
        fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
            self.inner.insert_batch(vectors)
        }
        fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
            SEARCHED.store(true, Ordering::SeqCst);
            self.inner.search(query, params)
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

    let (storage, _dir) = storage();
    let service = Arc::new(AkiDbService::new(
        Arc::new(UngatedIndex {
            inner: MockIndex::new(2, 16),
        }),
        Arc::new(IdMapping::new(storage, "test")),
        "test",
    ));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let service = service.clone();
        handles.push(tokio::spawn(async move {
            service.search(search_request()).await
        }));
    }
    for handle in handles {
        handle
            .await
            .unwrap()
            .expect("disabled admission must never reject");
    }
    assert!(SEARCHED.load(Ordering::SeqCst));
}
