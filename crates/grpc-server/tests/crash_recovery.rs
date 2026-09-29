//! Crash-recovery and durability-contract integration tests.
//!
//! These build a real `AkiDbService` over a tempdir RocksDB, simulate a crash
//! by dropping every handle WITHOUT any graceful shutdown (no snapshot save),
//! then reopen the same data directory and verify that:
//! - caller-supplied vectors, text, and deletes survive (durability contract),
//! - the startup reconciliation detects and repairs projection drift,
//! - all of the above works with NO embedding provider configured (the
//!   caller-supplied-vectors contract).
//!
//! The `AKIDB_CRASH_POINT` hook (akidb_common::crash_point) covers the real
//! process-abort variant when driving an actual server binary; these tests
//! cover the equivalent state-machine outcomes in-process.

use std::path::Path;
use std::sync::Arc;

use akidb_faiss::{HnswConfig, HnswIndex};
use akidb_graph::{GraphIndex, NativeGraphIndex, NodeKind};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::{
    DeleteRequest, GetRequest, InsertRequest, SearchRequest, TextSearchRequest,
};
use akidb_grpc::AkiDbService;
use akidb_storage::{IdMapping, RocksDbBackend};
use tonic::{Code, Request};

const DIMS: usize = 3;

/// Handles shared by the service and the test so a single RocksDB instance
/// serves both (RocksDB permits only one writer per directory).
struct Fixture {
    svc: AkiDbService<HnswIndex, RocksDbBackend>,
    id_mapping: Arc<IdMapping<RocksDbBackend>>,
    graph: Option<Arc<NativeGraphIndex<RocksDbBackend>>>,
}

/// Open a service on an existing data directory with no embedding provider.
fn open_service(dir: &Path) -> Fixture {
    let storage = Arc::new(RocksDbBackend::open(dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let svc = AkiDbService::new(index, id_mapping.clone(), "test");
    Fixture {
        svc,
        id_mapping,
        graph: None,
    }
}

fn open_service_with_graph(dir: &Path) -> Fixture {
    let storage = Arc::new(RocksDbBackend::open(dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let graph = Arc::new(NativeGraphIndex::new(storage));
    let svc = AkiDbService::new(index, id_mapping.clone(), "test").with_graph_index(graph.clone());
    Fixture {
        svc,
        id_mapping,
        graph: Some(graph),
    }
}

async fn insert(
    svc: &AkiDbService<HnswIndex, RocksDbBackend>,
    id: &str,
    embedding: Vec<f32>,
    text: &str,
) {
    svc.insert(Request::new(InsertRequest {
        collection: "test".into(),
        id: id.into(),
        vector: embedding,
        metadata: br#"{"k":"v"}"#.to_vec(),
        text: text.into(),
    }))
    .await
    .expect("insert failed");
}

/// D1 contract: every write/query API works with caller-supplied vectors and
/// no embedding endpoint configured; only TextSearch dense mode requires one.
#[tokio::test]
async fn caller_supplied_vectors_are_first_class_without_embedding() {
    let dir = tempfile::tempdir().unwrap();
    let fx = open_service(dir.path());
    let svc = &fx.svc;

    insert(svc, "a", vec![1.0, 0.0, 0.0], "first document").await;
    insert(svc, "b", vec![0.0, 1.0, 0.0], "second document").await;

    let found = svc
        .get(Request::new(GetRequest {
            collection: "test".into(),
            id: "a".into(),
        }))
        .await
        .expect("get failed");
    assert!(found.into_inner().found);

    let results = svc
        .search(Request::new(SearchRequest {
            collection: "test".into(),
            query: vec![1.0, 0.0, 0.0],
            top_k: 2,
            nprobe: None,
            filter: vec![],
            tag_filter: None,
            score_threshold: None,
            group_by: String::new(),
            group_size: None,
        }))
        .await
        .expect("vector search must work without an embedding provider")
        .into_inner();
    assert_eq!(results.results.len(), 2);
    assert_eq!(results.results[0].id, "a");

    // BM25-only text search works without a provider; dense text search fails
    // closed with Unavailable instead of silently degrading.
    let bm25_only = svc
        .text_search(Request::new(text_search_request("first", false, "bm25")))
        .await
        .expect("BM25-only text search must work without an embedding provider");
    assert_eq!(bm25_only.into_inner().results.len(), 1);

    let dense = svc
        .text_search(Request::new(text_search_request("first", false, "")))
        .await;
    assert_eq!(dense.unwrap_err().code(), Code::Unavailable);

    svc.delete(Request::new(DeleteRequest {
        collection: "test".into(),
        id: "b".into(),
    }))
    .await
    .expect("delete failed");

    // A deleted id must not come back: Get either reports found=false or
    // fails closed with NotFound.
    match svc
        .get(Request::new(GetRequest {
            collection: "test".into(),
            id: "b".into(),
        }))
        .await
    {
        Ok(resp) => assert!(!resp.into_inner().found),
        Err(status) => assert_eq!(status.code(), Code::NotFound),
    }
}

fn text_search_request(text: &str, hybrid: bool, retrieval_mode: &str) -> TextSearchRequest {
    TextSearchRequest {
        collection: "test".into(),
        text: text.into(),
        top_k: 5,
        nprobe: None,
        hybrid,
        dense_weight: None,
        lexical_weight: None,
        pack: false,
        pack_token_budget: None,
        rerank: false,
        diversity: false,
        mmr_lambda: None,
        filter: vec![],
        tag_filter: None,
        retrieval_mode: retrieval_mode.into(),
        score_threshold: None,
        group_by: String::new(),
        group_size: None,
        graph_max_depth: None,
        graph_per_seed_fanout: None,
        graph_max_expanded_nodes: None,
        include_diagnostics: false,
    }
}

/// A1 contract: source text is durable in the same synced batch as the vector,
/// so a crash after the acknowledged write loses nothing.
#[tokio::test]
async fn text_and_vector_survive_crash_after_acknowledged_insert() {
    let dir = tempfile::tempdir().unwrap();
    {
        let fx = open_service(dir.path());
        insert(&fx.svc, "doc-1", vec![1.0, 0.0, 0.0], "crash-proof text").await;
        // Simulated crash: drop without snapshot or graceful shutdown.
    }

    let fx = open_service(dir.path());
    let loaded = fx.svc.rebuild_lexical_index();
    assert_eq!(loaded, 1, "persisted source text must survive a crash");

    let report = fx.svc.reconcile_projections(true);
    assert_eq!(report.durable_vectors, 1);
    assert_eq!(report.lexical_documents, 1);
    assert!(!report.graph_repaired);

    // The reopened service answers BM25 search from the recovered text.
    let hits = fx
        .svc
        .text_search(Request::new(text_search_request(
            "crash-proof",
            false,
            "bm25",
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hits.results.len(), 1);
    assert_eq!(hits.results[0].id, "doc-1");
}

/// A1 contract: deletes tombstone the vector and remove its source text in one
/// synced batch, so a crash cannot resurrect either.
#[tokio::test]
async fn delete_is_durable_together_with_text_removal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let fx = open_service(dir.path());
        insert(&fx.svc, "doomed", vec![1.0, 0.0, 0.0], "soon deleted").await;
        fx.svc
            .delete(Request::new(DeleteRequest {
                collection: "test".into(),
                id: "doomed".into(),
            }))
            .await
            .unwrap();
        // Simulated crash right after the acknowledged delete.
    }

    let fx = open_service(dir.path());
    let loaded = fx.svc.rebuild_lexical_index();
    assert_eq!(loaded, 0, "deleted text must not resurrect after a crash");

    let report = fx.svc.reconcile_projections(true);
    assert_eq!(
        report.durable_vectors, 0,
        "tombstoned vectors are not active"
    );
    assert_eq!(report.lexical_documents, 0);
}

/// A2 contract: a stale non-empty graph projection is detected and repaired
/// against durable storage (RocksDB is the source of truth).
#[tokio::test]
async fn graph_drift_is_detected_and_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let fx = open_service_with_graph(dir.path());
    let graph = fx.graph.as_ref().unwrap();
    insert(&fx.svc, "chunk-1", vec![1.0, 0.0, 0.0], "graph doc").await;
    assert_eq!(graph.count_nodes_of_kind(NodeKind::Chunk).unwrap(), 1);

    // Simulate a crash between the durable vector write and the graph
    // projection by writing a second vector directly to storage, bypassing
    // the projection.
    fx.id_mapping
        .upsert_with_vector(
            &akidb_common::VectorId::new("chunk-2"),
            akidb_common::InternalId(99),
            &[0.0, 1.0, 0.0],
            br#"{"k":"v"}"#,
        )
        .unwrap();

    let report = fx.svc.reconcile_projections(false);
    assert_eq!(report.durable_vectors, 2);
    assert_eq!(report.graph_chunk_nodes, 1, "drift must be visible");
    assert!(!report.graph_repaired);

    let report = fx.svc.reconcile_projections(true);
    assert!(report.graph_repaired, "drift must be repaired when asked");
    assert_eq!(graph.count_nodes_of_kind(NodeKind::Chunk).unwrap(), 2);
}

/// A2 detection contract: a legacy partial write (vector durable, text lost —
/// the pre-A1 crash window) is visible in the reconciliation report.
#[tokio::test]
async fn legacy_partial_write_is_visible_in_reconcile_report() {
    let dir = tempfile::tempdir().unwrap();
    let fx = open_service(dir.path());

    // Write only the durable vector payload, as a crash between the synced
    // batch and the (previously unsynced) text write would have left it.
    fx.id_mapping
        .upsert_with_vector(
            &akidb_common::VectorId::new("orphan"),
            akidb_common::InternalId(7),
            &[0.1, 0.2, 0.3],
            &[],
        )
        .unwrap();

    let report = fx.svc.reconcile_projections(true);
    assert_eq!(report.durable_vectors, 1);
    assert_eq!(
        report.lexical_documents, 0,
        "the missing source text must show up as lexical drift"
    );
}

/// Sanity: the crash-point hook is a no-op when the env var is unset.
#[test]
fn crash_point_is_inert_without_env() {
    akidb_common::crash_point::crash_point("insert.after_durable_write");
}
