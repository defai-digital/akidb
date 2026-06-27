//! End-to-end integration tests for hybrid (dense + lexical) search.
//!
//! These build a real `AkiDbService` over a usearch HNSW index and a
//! RocksDB-backed id mapping, with a stub embedding provider, then exercise the
//! gRPC handlers (insert / text_search / delete) to verify that:
//! - inserting `text` populates the lexical index,
//! - `TextSearchRequest.hybrid` fuses dense + lexical results via RRF,
//! - fusion reorders results relative to dense-only,
//! - per-stage weights shift the ranking,
//! - deletes are reflected in lexical results,
//! - an empty lexical index degrades cleanly to dense-only.

use std::sync::Arc;

use akidb_faiss::{HnswConfig, HnswIndex};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::{DeleteRequest, InsertRequest, TextSearchRequest};
use akidb_grpc::{AkiDbService, EmbeddingProvider};
use akidb_storage::{IdMapping, RocksDbBackend};
use tonic::Request;

const DIMS: usize = 3;

/// Stub embedder: every query embeds to the unit vector along axis 0, so dense
/// similarity is fully controlled by the explicit embeddings we insert.
struct StubEmbedder;

impl EmbeddingProvider for StubEmbedder {
    fn embed_text(&self, _text: &str) -> Result<Vec<f32>, String> {
        Ok(vec![1.0, 0.0, 0.0])
    }
    fn embedding_dimensions(&self) -> usize {
        DIMS
    }
}

fn setup() -> AkiDbService<HnswIndex, RocksDbBackend> {
    // keep() keeps the temp dir alive for the test's duration (RocksDB holds it
    // open); cleanup is left to the OS temp reaper.
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    AkiDbService::new(index, id_mapping, "test").with_embedding_provider(Arc::new(StubEmbedder))
}

async fn insert(
    svc: &AkiDbService<HnswIndex, RocksDbBackend>,
    id: &str,
    embedding: Vec<f32>,
    text: &str,
    metadata: &[u8],
) {
    svc.insert(Request::new(InsertRequest {
        collection: "test".into(),
        id: id.into(),
        vector: embedding,
        metadata: metadata.to_vec(),
        text: text.into(),
    }))
    .await
    .expect("insert failed");
}

async fn search(
    svc: &AkiDbService<HnswIndex, RocksDbBackend>,
    text: &str,
    top_k: u32,
    hybrid: bool,
    dense_weight: Option<f32>,
    lexical_weight: Option<f32>,
) -> Vec<(String, String)> {
    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: text.into(),
            top_k,
            nprobe: None,
            hybrid,
            dense_weight,
            lexical_weight,
        }))
        .await
        .expect("text_search failed")
        .into_inner();
    resp.results
        .into_iter()
        .map(|r| (r.id, r.metadata))
        .collect()
}

fn ids(results: &[(String, String)]) -> Vec<String> {
    results.iter().map(|(id, _)| id.clone()).collect()
}

/// Three documents arranged so dense and lexical disagree:
/// - doc_both:    closest embedding AND contains the query term.
/// - doc_dense:   near embedding, no query term.
/// - doc_lexical: orthogonal embedding, strongest query term frequency.
async fn seed_disagreeing(svc: &AkiDbService<HnswIndex, RocksDbBackend>) {
    insert(svc, "doc_both", vec![1.0, 0.0, 0.0], "needle in the document", b"{\"kind\":\"both\"}").await;
    insert(svc, "doc_dense", vec![0.9, 0.1, 0.0], "haystack only filler", b"").await;
    insert(svc, "doc_lexical", vec![0.0, 1.0, 0.0], "needle needle needle", b"").await;
}

#[tokio::test]
async fn test_dense_only_vs_hybrid_reorders_results() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    // Dense-only: ranked purely by cosine to [1,0,0].
    let dense = search(&svc, "needle", 3, false, None, None).await;
    assert_eq!(ids(&dense), vec!["doc_both", "doc_dense", "doc_lexical"]);

    // Hybrid: the lexical signal lifts doc_lexical above doc_dense, while
    // doc_both (strong in both) stays on top.
    let hybrid = search(&svc, "needle", 3, true, None, None).await;
    assert_eq!(ids(&hybrid), vec!["doc_both", "doc_lexical", "doc_dense"]);
}

#[tokio::test]
async fn test_high_lexical_weight_promotes_lexical_match() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    // Heavily weighting the lexical stage pushes the strongest term match to top.
    let hybrid = search(&svc, "needle", 3, true, Some(1.0), Some(5.0)).await;
    assert_eq!(ids(&hybrid)[0], "doc_lexical");
}

#[tokio::test]
async fn test_hybrid_with_empty_lexical_degrades_to_dense() {
    let svc = setup();
    // No text => lexical index stays empty.
    insert(&svc, "a", vec![1.0, 0.0, 0.0], "", b"").await;
    insert(&svc, "b", vec![0.5, 0.5, 0.0], "", b"").await;
    insert(&svc, "c", vec![0.0, 1.0, 0.0], "", b"").await;

    let dense = search(&svc, "anything", 3, false, None, None).await;
    let hybrid = search(&svc, "anything", 3, true, None, None).await;
    assert_eq!(ids(&dense), vec!["a", "b", "c"]);
    assert_eq!(ids(&hybrid), ids(&dense), "empty lexical must not change order");
}

#[tokio::test]
async fn test_delete_removes_from_hybrid_results() {
    let svc = setup();
    insert(&svc, "keep", vec![1.0, 0.0, 0.0], "alpha", b"").await;
    insert(&svc, "drop", vec![0.0, 1.0, 0.0], "alpha beta", b"").await;

    let before = search(&svc, "alpha", 10, true, None, None).await;
    assert!(ids(&before).contains(&"drop".to_string()));

    svc.delete(Request::new(DeleteRequest {
        collection: "test".into(),
        id: "drop".into(),
    }))
    .await
    .expect("delete failed");

    let after = search(&svc, "alpha", 10, true, None, None).await;
    assert!(!ids(&after).contains(&"drop".to_string()), "deleted doc must be gone");
    assert!(ids(&after).contains(&"keep".to_string()));
}

#[tokio::test]
async fn test_hybrid_result_carries_metadata() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    let hybrid = search(&svc, "needle", 3, true, None, None).await;
    let (top_id, top_meta) = &hybrid[0];
    assert_eq!(top_id, "doc_both");
    assert!(top_meta.contains("both"), "fused result should carry stored metadata, got {top_meta:?}");
}
