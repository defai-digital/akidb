//! SearchResponse.candidate_limited reports an incomplete page caused by a
//! capped filtered scan: the index flag alone is not enough — the returned
//! page must also be underfilled.

use std::sync::{Arc, Mutex};

use akidb_faiss::{
    IndexStats, InternalId, MockIndex, Result, SearchParams, SearchResult, SearchWindow, VectorId,
    VectorIndex,
};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::{Query, SearchBatchRequest, SearchRequest};
use akidb_grpc::AkiDbService;
use akidb_storage::{IdMapping, RocksDbBackend};
use tonic::Request;

#[derive(Debug, Clone, Copy)]
struct Scenario {
    hits: usize,
    candidate_limited: bool,
}

struct ScriptedIndex {
    inner: MockIndex,
    scenario: Mutex<Scenario>,
}

impl ScriptedIndex {
    fn new(dimensions: usize, scenario: Scenario) -> Self {
        Self {
            inner: MockIndex::new(dimensions, 16),
            scenario: Mutex::new(scenario),
        }
    }

    fn set(&self, scenario: Scenario) {
        *self.scenario.lock().unwrap() = scenario;
    }

    fn scripted_hits(&self) -> Vec<SearchResult> {
        let scenario = *self.scenario.lock().unwrap();
        (0..scenario.hits)
            .map(|i| SearchResult::new(VectorId::new(format!("hit-{i}")), 1.0 - i as f32 * 0.1))
            .collect()
    }
}

impl VectorIndex for ScriptedIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        self.inner.insert(id, vector)
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        self.inner.insert_batch(vectors)
    }

    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        let _ = (query, params);
        Ok(self.scripted_hits())
    }

    fn search_window(&self, query: &[f32], params: &SearchParams) -> Result<SearchWindow> {
        let _ = (query, params);
        let scenario = *self.scenario.lock().unwrap();
        Ok(SearchWindow {
            results: self.scripted_hits(),
            candidate_limited: scenario.candidate_limited,
        })
    }

    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        let _ = params;
        Ok(queries.iter().map(|_| self.scripted_hits()).collect())
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
    index: Arc<ScriptedIndex>,
) -> (
    AkiDbService<ScriptedIndex, RocksDbBackend>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
    let service = AkiDbService::new(index, Arc::new(IdMapping::new(storage, "test")), "test");
    (service, dir)
}

fn search_request(top_k: u32) -> Request<SearchRequest> {
    Request::new(SearchRequest {
        collection: "test".into(),
        query: vec![1.0, 0.0],
        top_k,
        ..Default::default()
    })
}

#[tokio::test]
async fn capped_underfilled_scan_reports_candidate_limited() {
    let index = Arc::new(ScriptedIndex::new(
        2,
        Scenario {
            hits: 1,
            candidate_limited: true,
        },
    ));
    let (service, _dir) = service_for(index);

    let response = service
        .search(search_request(3))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 1);
    assert!(
        response.candidate_limited,
        "a capped scan that underfills the page must be reported"
    );
}

#[tokio::test]
async fn capped_full_page_does_not_report_candidate_limited() {
    let index = Arc::new(ScriptedIndex::new(
        2,
        Scenario {
            hits: 3,
            candidate_limited: true,
        },
    ));
    let (service, _dir) = service_for(index.clone());

    let response = service
        .search(search_request(3))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 3);
    assert!(
        !response.candidate_limited,
        "a full page stays valid even when the underlying scan was capped"
    );

    // The page reports incompleteness only while it is actually short.
    index.set(Scenario {
        hits: 1,
        candidate_limited: true,
    });
    let response = service
        .search(search_request(3))
        .await
        .unwrap()
        .into_inner();
    assert!(response.candidate_limited);
}

#[tokio::test]
async fn uncapped_underfilled_scan_is_not_candidate_limited() {
    let index = Arc::new(ScriptedIndex::new(
        2,
        Scenario {
            hits: 1,
            candidate_limited: false,
        },
    ));
    let (service, _dir) = service_for(index);

    let response = service
        .search(search_request(3))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !response.candidate_limited,
        "an honest short result set is not a capped scan"
    );
}

#[tokio::test]
async fn search_batch_reports_candidate_limited_per_query() {
    let index = Arc::new(ScriptedIndex::new(
        2,
        Scenario {
            hits: 1,
            candidate_limited: true,
        },
    ));
    let (service, _dir) = service_for(index);

    let response = service
        .search_batch(Request::new(SearchBatchRequest {
            collection: "test".into(),
            queries: vec![
                Query {
                    vector: vec![1.0, 0.0],
                },
                Query {
                    vector: vec![0.0, 1.0],
                },
            ],
            top_k: 3,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.results.len(), 2);
    for nested in &response.results {
        assert_eq!(nested.results.len(), 1);
        assert!(
            nested.candidate_limited,
            "every underfilled response in the batch must carry the flag"
        );
    }
}
