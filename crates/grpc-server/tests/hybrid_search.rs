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
use akidb_graph::{
    Direction, EdgeKind, GraphEdge, GraphIndex, GraphNode, GraphNodeId, NativeGraphIndex,
    NeighborRequest, NodeKind,
};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::{
    tag_filter::FilterType, tag_value::Value as TagVal, DeleteRequest, InsertBatchRequest,
    InsertRequest, TagCondition, TagFilter, TagOperator, TagValue, TextSearchRequest,
    UpdateRequest, Vector,
};
use akidb_grpc::{AkiDbService, EmbeddingProvider};
use akidb_sql::SqliteMetadataIndex;
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

fn setup_without_embedder() -> AkiDbService<HnswIndex, RocksDbBackend> {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    AkiDbService::new(index, id_mapping, "test")
}

fn setup_with_sql() -> AkiDbService<HnswIndex, RocksDbBackend> {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let sql_index = Arc::new(SqliteMetadataIndex::in_memory().unwrap());
    AkiDbService::new(index, id_mapping, "test").with_metadata_sql_index(sql_index)
}

fn setup_with_graph() -> (
    AkiDbService<HnswIndex, RocksDbBackend>,
    Arc<NativeGraphIndex<RocksDbBackend>>,
) {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let graph = Arc::new(NativeGraphIndex::new(storage));
    let svc = AkiDbService::new(index, id_mapping, "test")
        .with_embedding_provider(Arc::new(StubEmbedder))
        .with_graph_index(graph.clone());
    (svc, graph)
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
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
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
    insert(
        svc,
        "doc_both",
        vec![1.0, 0.0, 0.0],
        "needle in the document",
        b"{\"kind\":\"both\"}",
    )
    .await;
    insert(
        svc,
        "doc_dense",
        vec![0.9, 0.1, 0.0],
        "haystack only filler",
        b"",
    )
    .await;
    insert(
        svc,
        "doc_lexical",
        vec![0.0, 1.0, 0.0],
        "needle needle needle",
        b"",
    )
    .await;
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
async fn test_hybrid_top_k_above_pool_cap_does_not_panic() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 201,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("hybrid top_k above the default pool cap should not panic")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["doc_both", "doc_lexical", "doc_dense"]);
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
async fn test_retrieval_mode_vector_overrides_hybrid_flag() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 3,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: "vector".into(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["doc_both", "doc_dense", "doc_lexical"]);
}

#[tokio::test]
async fn test_retrieval_mode_bm25_does_not_require_embedding_provider() {
    let svc = setup_without_embedder();
    insert(&svc, "dense", vec![1.0, 0.0, 0.0], "haystack only", b"").await;
    insert(&svc, "lexical", vec![0.0, 1.0, 0.0], "needle needle", b"").await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 2,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 text_search should not require an embedder")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["lexical"]);
}

#[tokio::test]
async fn test_bm25_metadata_filter_applies_before_top_k_cutoff() {
    let svc = setup_without_embedder();
    insert(
        &svc,
        "tenant-a-strong",
        vec![1.0, 0.0, 0.0],
        "needle needle needle",
        br#"{"tenant_id":"a"}"#,
    )
    .await;
    insert(
        &svc,
        "tenant-b-match",
        vec![0.0, 1.0, 0.0],
        "needle",
        br#"{"tenant_id":"b"}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
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
            filter: br#"{"tenant_id":"b"}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 filter search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["tenant-b-match"]);
}

#[tokio::test]
async fn test_bm25_tag_filter_applies_before_top_k_cutoff() {
    let svc = setup_without_embedder();
    insert(
        &svc,
        "team-a-strong",
        vec![1.0, 0.0, 0.0],
        "needle needle needle",
        br#"{"team":"a"}"#,
    )
    .await;
    insert(
        &svc,
        "team-b-match",
        vec![0.0, 1.0, 0.0],
        "needle",
        br#"{"team":"b"}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
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
            tag_filter: Some(TagFilter {
                filter_type: Some(FilterType::Condition(TagCondition {
                    key: "team".into(),
                    value: Some(TagValue {
                        value: Some(TagVal::Text("b".into())),
                    }),
                    op: TagOperator::TagOpEq as i32,
                })),
            }),
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 tag filter search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["team-b-match"]);
}

#[tokio::test]
async fn test_bm25_tag_filter_matches_nested_metadata_path() {
    let svc = setup_without_embedder();
    insert(
        &svc,
        "contract-2024-strong",
        vec![1.0, 0.0, 0.0],
        "needle needle needle",
        br#"{"contract":{"customer":"HGC","year":2024}}"#,
    )
    .await;
    insert(
        &svc,
        "contract-2025-match",
        vec![0.0, 1.0, 0.0],
        "needle",
        br#"{"contract":{"customer":"HGC","year":2025}}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
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
            tag_filter: Some(TagFilter {
                filter_type: Some(FilterType::Condition(TagCondition {
                    key: "contract.year".into(),
                    value: Some(TagValue {
                        value: Some(TagVal::Number(2025.0)),
                    }),
                    op: TagOperator::TagOpEq as i32,
                })),
            }),
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 nested tag filter search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["contract-2025-match"]);
}

#[tokio::test]
async fn test_retrieval_mode_sql_uses_metadata_adapter_without_embedder() {
    let svc = setup_with_sql();
    insert(
        &svc,
        "contract-2025",
        vec![1.0, 0.0, 0.0],
        "",
        br#"{"tenant_id":"defai","customer":"HGC","year":2025}"#,
    )
    .await;
    insert(
        &svc,
        "contract-2024",
        vec![0.0, 1.0, 0.0],
        "",
        br#"{"tenant_id":"defai","customer":"HGC","year":2024}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "HGC contract amount".into(),
            top_k: 10,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"tenant_id":"defai","customer":"HGC","year":2025}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "sql".into(),
        }))
        .await
        .expect("sql text_search should not require an embedder")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["contract-2025"]);
    assert!(resp.results[0].metadata.contains("\"year\":2025"));

    svc.delete(Request::new(DeleteRequest {
        collection: "test".into(),
        id: "contract-2025".into(),
    }))
    .await
    .expect("delete failed");

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "HGC contract amount".into(),
            top_k: 10,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"tenant_id":"defai","customer":"HGC","year":2025}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "sql".into(),
        }))
        .await
        .expect("sql text_search should still succeed after delete")
        .into_inner();
    assert!(resp.results.is_empty());
}

#[tokio::test]
async fn test_retrieval_mode_sql_matches_null_metadata_filter() {
    let svc = setup_with_sql();
    insert(
        &svc,
        "active-contract",
        vec![1.0, 0.0, 0.0],
        "",
        br#"{"tenant_id":"defai","deleted_at":null}"#,
    )
    .await;
    insert(
        &svc,
        "missing-deleted-at",
        vec![0.0, 1.0, 0.0],
        "",
        br#"{"tenant_id":"defai"}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "active contract".into(),
            top_k: 10,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"tenant_id":"defai","deleted_at":null}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "sql".into(),
        }))
        .await
        .expect("sql null filter text_search should succeed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["active-contract"]);
}

#[tokio::test]
async fn test_retrieval_mode_sql_applies_nested_legacy_metadata_filter() {
    let svc = setup_with_sql();
    insert(
        &svc,
        "contract-2025",
        vec![1.0, 0.0, 0.0],
        "",
        br#"{"contract":{"customer":"HGC","year":2025,"amount":1200}}"#,
    )
    .await;
    insert(
        &svc,
        "contract-2024",
        vec![0.0, 1.0, 0.0],
        "",
        br#"{"contract":{"customer":"HGC","year":2024,"amount":900}}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "HGC contract amount".into(),
            top_k: 10,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"contract":{"customer":"HGC","year":2025}}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "sql".into(),
        }))
        .await
        .expect("sql nested metadata text_search should succeed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["contract-2025"]);
}

#[tokio::test]
async fn test_retrieval_mode_sql_applies_tag_filter_before_top_k_cutoff() {
    let svc = setup_with_sql();
    insert(
        &svc,
        "b-match",
        vec![1.0, 0.0, 0.0],
        "",
        br#"{"tenant_id":"defai","customer":"HGC"}"#,
    )
    .await;
    insert(
        &svc,
        "a-nonmatch",
        vec![0.0, 1.0, 0.0],
        "",
        br#"{"tenant_id":"other","customer":"HGC"}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "HGC contract amount".into(),
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
            tag_filter: Some(TagFilter {
                filter_type: Some(FilterType::Condition(TagCondition {
                    key: "tenant_id".into(),
                    value: Some(TagValue {
                        value: Some(TagVal::Text("defai".into())),
                    }),
                    op: TagOperator::TagOpEq as i32,
                })),
            }),
            retrieval_mode: "sql".into(),
        }))
        .await
        .expect("sql tag_filter text_search should succeed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["b-match"]);
}

#[tokio::test]
async fn test_rebuild_sql_metadata_index_removes_deleted_vectors() {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let sql_index = Arc::new(SqliteMetadataIndex::in_memory().unwrap());
    let svc = AkiDbService::new(index, id_mapping.clone(), "test")
        .with_metadata_sql_index(sql_index.clone());

    insert(
        &svc,
        "stale-contract",
        vec![1.0, 0.0, 0.0],
        "",
        br#"{"tenant_id":"defai","customer":"HGC","year":2025}"#,
    )
    .await;

    id_mapping
        .mark_deleted(&akidb_common::VectorId::new("stale-contract"))
        .unwrap();

    assert_eq!(svc.rebuild_sql_metadata_index(), 0);

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "HGC contract amount".into(),
            top_k: 10,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"tenant_id":"defai","customer":"HGC","year":2025}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "sql".into(),
        }))
        .await
        .expect("sql text_search after rebuild should succeed")
        .into_inner();

    assert!(
        resp.results.is_empty(),
        "SQL rebuild should clear mirror rows for deleted durable vectors, got {:?}",
        resp.results
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
    );
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
    assert_eq!(
        ids(&hybrid),
        ids(&dense),
        "empty lexical must not change order"
    );
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
    assert!(
        !ids(&after).contains(&"drop".to_string()),
        "deleted doc must be gone"
    );
    assert!(ids(&after).contains(&"keep".to_string()));
}

#[tokio::test]
async fn test_insert_upsert_empty_text_removes_stale_bm25_document() {
    let svc = setup_without_embedder();
    insert(
        &svc,
        "doc",
        vec![1.0, 0.0, 0.0],
        "needle old source text",
        b"",
    )
    .await;

    let before = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search failed")
        .into_inner();
    assert_eq!(before.results[0].id, "doc");

    insert(&svc, "doc", vec![1.0, 0.0, 0.0], "", b"").await;

    let after = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search after upsert failed")
        .into_inner();
    assert!(
        after.results.is_empty(),
        "empty-text upsert should clear stale lexical text, got {:?}",
        after
            .results
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_batch_insert_upsert_empty_text_removes_stale_bm25_document() {
    let svc = setup_without_embedder();
    insert(
        &svc,
        "doc",
        vec![1.0, 0.0, 0.0],
        "needle old source text",
        b"",
    )
    .await;

    svc.insert_batch(Request::new(InsertBatchRequest {
        collection: "test".into(),
        vectors: vec![Vector {
            id: "doc".into(),
            embedding: vec![1.0, 0.0, 0.0],
            metadata: vec![],
            text: String::new(),
        }],
    }))
    .await
    .expect("batch upsert failed");

    let after = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search after batch upsert failed")
        .into_inner();
    assert!(
        after.results.is_empty(),
        "batch empty-text upsert should clear stale lexical text, got {:?}",
        after
            .results
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_batch_insert_rejects_invalid_vectors_without_indexing_text() {
    let svc = setup_without_embedder();

    let resp = svc
        .insert_batch(Request::new(InsertBatchRequest {
            collection: "test".into(),
            vectors: vec![
                Vector {
                    id: "".into(),
                    embedding: vec![1.0, 0.0, 0.0],
                    metadata: b"{}".to_vec(),
                    text: "needle invalid empty id".into(),
                },
                Vector {
                    id: "nan-vector".into(),
                    embedding: vec![f32::NAN, 0.0, 0.0],
                    metadata: b"{}".to_vec(),
                    text: "needle invalid nan".into(),
                },
                Vector {
                    id: "valid-vector".into(),
                    embedding: vec![1.0, 0.0, 0.0],
                    metadata: b"{}".to_vec(),
                    text: "needle valid document".into(),
                },
            ],
        }))
        .await
        .expect("batch insert should return partial failure")
        .into_inner();

    assert!(!resp.success);
    assert_eq!(resp.inserted_count, 1);
    assert_eq!(resp.failed_ids, vec!["", "nan-vector"]);

    let search = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search after partial batch insert failed")
        .into_inner();

    let got: Vec<&str> = search.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["valid-vector"]);
}

#[tokio::test]
async fn test_pack_returns_cited_context() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 3,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: true,
            pack_token_budget: Some(1024),
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    // The pack should contain the top doc's source text and a citation marker.
    assert!(
        !resp.context_pack.is_empty(),
        "expected a non-empty context pack"
    );
    assert!(resp.context_pack.contains("needle in the document"));
    assert!(
        resp.context_pack.contains("[doc_both]"),
        "expected a citation marker, got: {}",
        resp.context_pack
    );
}

#[tokio::test]
async fn test_pack_respects_token_budget() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    // A tiny budget must drop passages: the pack is far shorter than all text.
    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 3,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: true,
            pack_token_budget: Some(3),
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    // At most the first passage fits a 3-token budget.
    let word_count = resp.context_pack.split_whitespace().count();
    assert!(word_count <= 3, "budget exceeded: {} words", word_count);
}

#[tokio::test]
async fn test_no_pack_leaves_context_empty() {
    let svc = setup();
    seed_disagreeing(&svc).await;
    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 3,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();
    assert!(resp.context_pack.is_empty());
}

#[tokio::test]
async fn test_lexical_index_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());

    // "Process A": insert documents with source text (persists text to storage).
    {
        let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
        let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
        let svc_a = AkiDbService::new(index, id_mapping, "test")
            .with_embedding_provider(Arc::new(StubEmbedder));
        insert(&svc_a, "doc1", vec![1.0, 0.0, 0.0], "needle alpha", b"").await;
        insert(
            &svc_a,
            "doc2",
            vec![0.0, 1.0, 0.0],
            "needle beta gamma",
            b"",
        )
        .await;
    }

    // "Process B": a fresh service (empty in-memory indexes) over the same
    // storage, as if the server restarted.
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let svc_b = AkiDbService::new(index, id_mapping, "test")
        .with_embedding_provider(Arc::new(StubEmbedder));

    // Before rebuild, the lexical index is empty.
    let before = search(&svc_b, "needle", 10, true, None, None).await;
    assert!(
        before.is_empty(),
        "expected empty before rebuild, got {:?}",
        ids(&before)
    );

    let loaded = svc_b.rebuild_lexical_index();
    assert_eq!(loaded, 2, "both persisted documents should be rebuilt");

    // After rebuild, lexical retrieval finds the persisted docs (the dense index
    // is empty in this fresh process, so matches come from the rebuilt lexical).
    let after = ids(&search(&svc_b, "needle", 10, true, None, None).await);
    assert!(after.contains(&"doc1".to_string()), "got {after:?}");
    assert!(after.contains(&"doc2".to_string()), "got {after:?}");
}

#[tokio::test]
async fn test_rebuild_lexical_index_removes_stale_in_memory_text() {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let svc = AkiDbService::new(index, id_mapping.clone(), "test");

    id_mapping
        .upsert_with_vector(
            &akidb_common::VectorId::new("stale"),
            akidb_common::InternalId(1),
            &[1.0, 0.0, 0.0],
            b"{}",
        )
        .unwrap();
    id_mapping
        .store_text(&akidb_common::VectorId::new("stale"), "needle old text")
        .unwrap();
    assert_eq!(svc.rebuild_lexical_index(), 1);

    let before = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search failed")
        .into_inner();
    assert_eq!(before.results[0].id, "stale");

    id_mapping
        .delete_text(&akidb_common::VectorId::new("stale"))
        .unwrap();
    assert_eq!(svc.rebuild_lexical_index(), 0);

    let after = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search after rebuild failed")
        .into_inner();
    assert!(
        after.results.is_empty(),
        "rebuild should remove in-memory text missing from durable store, got {:?}",
        after
            .results
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_rebuild_lexical_index_skips_deleted_vectors_with_stale_text() {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let svc = AkiDbService::new(index, id_mapping.clone(), "test");

    let stale_id = akidb_common::VectorId::new("deleted-doc");
    id_mapping
        .upsert_with_vector(
            &stale_id,
            akidb_common::InternalId(1),
            &[1.0, 0.0, 0.0],
            b"{}",
        )
        .unwrap();
    id_mapping
        .store_text(&stale_id, "needle deleted source text")
        .unwrap();
    id_mapping.mark_deleted(&stale_id).unwrap();

    assert_eq!(svc.rebuild_lexical_index(), 0);

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
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
            retrieval_mode: "bm25".into(),
        }))
        .await
        .expect("bm25 search after rebuild failed")
        .into_inner();
    assert!(
        resp.results.is_empty(),
        "rebuild should skip durable text for deleted vectors, got {:?}",
        resp.results
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_rerank_promotes_query_term_match() {
    let svc = setup();
    // doc_dense is closest by embedding but lacks the query term; doc_match is
    // farther but contains it.
    insert(&svc, "doc_dense", vec![1.0, 0.0, 0.0], "haystack only", b"").await;
    insert(&svc, "doc_match", vec![0.9, 0.1, 0.0], "needle needle", b"").await;

    // Dense-only ordering: doc_dense first.
    let plain = ids(&search(&svc, "needle", 10, false, None, None).await);
    assert_eq!(plain.first().map(String::as_str), Some("doc_dense"));

    // With reranking, the query-term match is promoted to the top.
    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
            nprobe: None,
            hybrid: false,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: true,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();
    assert_eq!(
        resp.results.first().map(|r| r.id.as_str()),
        Some("doc_match")
    );
}

#[tokio::test]
async fn test_diversity_demotes_near_duplicate() {
    let svc = setup();
    // a and a_dup are near-identical embeddings; b is orthogonal. All match the
    // query term equally, so relevance ranks a, a_dup, b.
    insert(&svc, "a", vec![1.0, 0.0, 0.0], "needle one", b"").await;
    insert(&svc, "a_dup", vec![0.99, 0.01, 0.0], "needle two", b"").await;
    insert(&svc, "b", vec![0.0, 1.0, 0.0], "needle three", b"").await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 3,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: true,
            mmr_lambda: Some(0.5),
            filter: vec![],
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();
    let order: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(order[0], "a", "most relevant stays first");
    assert_eq!(
        order[1], "b",
        "diverse item promoted above the near-duplicate"
    );
    assert_eq!(order[2], "a_dup");
}

async fn pack_for(
    svc: &AkiDbService<HnswIndex, RocksDbBackend>,
    query: &str,
    top_k: u32,
) -> String {
    svc.text_search(Request::new(TextSearchRequest {
        collection: "test".into(),
        text: query.into(),
        top_k,
        nprobe: None,
        hybrid: true,
        dense_weight: None,
        lexical_weight: None,
        pack: true,
        pack_token_budget: Some(1024),
        rerank: false,
        diversity: false,
        mmr_lambda: None,
        filter: vec![],
        tag_filter: None,
        retrieval_mode: String::new(),
    }))
    .await
    .expect("text_search failed")
    .into_inner()
    .context_pack
}

#[tokio::test]
async fn test_pack_expands_child_to_parent() {
    let svc = setup();
    // Parent: full context, embedding orthogonal to the query and no query term,
    // so it is not itself retrieved. Child: matches query, points at the parent.
    insert(
        &svc,
        "P",
        vec![0.0, 0.0, 1.0],
        "the complete section with lots of detail and context",
        b"",
    )
    .await;
    insert(
        &svc,
        "C",
        vec![1.0, 0.0, 0.0],
        "needle marker",
        br#"{"parent_id":"P"}"#,
    )
    .await;

    let pack = pack_for(&svc, "needle", 5).await;
    assert!(
        pack.contains("complete section"),
        "parent context expected, got: {pack}"
    );
    assert!(
        pack.contains("[P]"),
        "parent citation expected, got: {pack}"
    );
}

#[tokio::test]
async fn test_pack_dedups_siblings_to_single_parent() {
    let svc = setup();
    insert(
        &svc,
        "P",
        vec![0.0, 0.0, 1.0],
        "shared parent context body",
        b"",
    )
    .await;
    insert(
        &svc,
        "C1",
        vec![1.0, 0.0, 0.0],
        "needle alpha",
        br#"{"parent_id":"P"}"#,
    )
    .await;
    insert(
        &svc,
        "C2",
        vec![0.9, 0.1, 0.0],
        "needle beta",
        br#"{"parent_id":"P"}"#,
    )
    .await;

    let pack = pack_for(&svc, "needle", 5).await;
    let occurrences = pack.matches("shared parent context body").count();
    assert_eq!(
        occurrences, 1,
        "parent must appear once for sibling children, got: {pack}"
    );
}

#[tokio::test]
async fn test_pack_includes_graph_related_chunk() {
    let (svc, graph) = setup_with_graph();
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        b"",
    )
    .await;
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "graph expanded implementation context",
        b"",
    )
    .await;

    graph
        .upsert_node(GraphNode::new("chunk:anchor", NodeKind::Chunk))
        .unwrap();
    graph
        .upsert_node(GraphNode::new("chunk:related", NodeKind::Chunk))
        .unwrap();
    graph
        .upsert_edge(GraphEdge::new(
            "anchor-related",
            "chunk:anchor",
            "chunk:related",
            EdgeKind::RelatedTo,
        ))
        .unwrap();

    let pack = pack_for(&svc, "needle", 1).await;
    assert!(
        pack.contains("needle anchor text"),
        "anchor context expected, got: {pack}"
    );
    assert!(
        pack.contains("graph expanded implementation context"),
        "graph-expanded context expected despite top_k=1, got: {pack}"
    );
}

#[tokio::test]
async fn test_graph_mode_returns_graph_expanded_results() {
    let (svc, graph) = setup_with_graph();
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        b"",
    )
    .await;
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "graph result context",
        b"",
    )
    .await;

    graph
        .upsert_node(GraphNode::new("chunk:anchor", NodeKind::Chunk))
        .unwrap();
    graph
        .upsert_node(GraphNode::new("chunk:related", NodeKind::Chunk))
        .unwrap();
    graph
        .upsert_edge(GraphEdge::new(
            "anchor-related-result",
            "chunk:anchor",
            "chunk:related",
            EdgeKind::RelatedTo,
        ))
        .unwrap();

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 2,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: vec![],
            tag_filter: None,
            retrieval_mode: "graph".into(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["anchor", "related"]);
}

#[tokio::test]
async fn test_insert_metadata_indexes_graph_related_ids_for_pack() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "auto indexed graph context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        br#"{"related_ids":["related"]}"#,
    )
    .await;

    let pack = pack_for(&svc, "needle", 1).await;
    assert!(
        pack.contains("auto indexed graph context"),
        "related_ids metadata should create graph context edge, got: {pack}"
    );
}

#[tokio::test]
async fn test_insert_metadata_indexes_single_graph_related_id_for_pack() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "single related graph context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        br#"{"related_ids":"related"}"#,
    )
    .await;

    let pack = pack_for(&svc, "needle", 1).await;
    assert!(
        pack.contains("single related graph context"),
        "related_ids string metadata should create graph context edge, got: {pack}"
    );
}

#[tokio::test]
async fn test_insert_upsert_removes_stale_graph_related_context() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "stale upsert graph context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        br#"{"related_ids":["related"]}"#,
    )
    .await;

    let before = pack_for(&svc, "needle", 1).await;
    assert!(
        before.contains("stale upsert graph context"),
        "precondition failed, got: {before}"
    );

    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        b"{}",
    )
    .await;

    let after = pack_for(&svc, "needle", 1).await;
    assert!(
        after.contains("needle anchor text"),
        "anchor context expected after upsert, got: {after}"
    );
    assert!(
        !after.contains("stale upsert graph context"),
        "upserted metadata should remove stale graph expansion, got: {after}"
    );
}

#[tokio::test]
async fn test_update_removes_stale_graph_related_context() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "stale graph context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        br#"{"related_ids":["related"]}"#,
    )
    .await;

    let before = pack_for(&svc, "needle", 1).await;
    assert!(
        before.contains("stale graph context"),
        "precondition failed, got: {before}"
    );

    svc.update(Request::new(UpdateRequest {
        collection: "test".into(),
        id: "anchor".into(),
        vector: vec![1.0, 0.0, 0.0],
        metadata: b"{}".to_vec(),
    }))
    .await
    .expect("update failed");

    let after = pack_for(&svc, "needle", 1).await;
    assert!(
        after.contains("needle anchor text"),
        "anchor context expected after update, got: {after}"
    );
    assert!(
        !after.contains("stale graph context"),
        "updated metadata should remove stale graph expansion, got: {after}"
    );
}

#[tokio::test]
async fn test_update_preserves_incoming_graph_related_context() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "still related graph context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        br#"{"related_ids":["related"]}"#,
    )
    .await;

    svc.update(Request::new(UpdateRequest {
        collection: "test".into(),
        id: "related".into(),
        vector: vec![0.0, 0.0, 1.0],
        metadata: b"{}".to_vec(),
    }))
    .await
    .expect("update failed");

    let pack = pack_for(&svc, "needle", 1).await;
    assert!(
        pack.contains("still related graph context"),
        "updating related chunk should preserve anchor-owned graph edge, got: {pack}"
    );
}

#[tokio::test]
async fn test_insert_metadata_indexes_code_graph_edges_for_pack() {
    let (svc, graph) = setup_with_graph();
    insert(
        &svc,
        "callee",
        vec![0.0, 0.0, 1.0],
        "called implementation context",
        b"",
    )
    .await;
    insert(
        &svc,
        "dependency",
        vec![0.0, 1.0, 0.0],
        "dependency implementation context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle scheduler context",
        br#"{
            "calls":["callee"],
            "depends_on":["dependency"],
            "owned_by":"Akira",
            "changed_by":"abc123",
            "file":"src/mtp_scheduler.rs",
            "symbol":"run_scheduler"
        }"#,
    )
    .await;

    let neighbors = graph
        .neighbors(
            NeighborRequest::new("chunk:anchor")
                .with_direction(Direction::Out)
                .with_limit(20),
        )
        .unwrap();
    assert!(
        neighbors
            .iter()
            .any(|n| n.edge.kind == EdgeKind::Calls && n.node.id.as_str() == "chunk:callee"),
        "calls edge should point at callee chunk, got {neighbors:?}"
    );
    assert!(
        neighbors
            .iter()
            .any(|n| n.edge.kind == EdgeKind::OwnedBy && n.node.id.as_str() == "person:Akira"),
        "owned_by should create a person edge, got {neighbors:?}"
    );
    assert!(
        neighbors
            .iter()
            .any(|n| n.edge.kind == EdgeKind::ChangedBy && n.node.id.as_str() == "commit:abc123"),
        "changed_by should create a commit edge, got {neighbors:?}"
    );

    let file_chunks = graph
        .related_chunks(&GraphNodeId::new("file:src/mtp_scheduler.rs"), 10)
        .unwrap();
    assert!(
        file_chunks
            .iter()
            .any(|chunk| chunk.vector_id.as_str() == "anchor"),
        "file contains edge should expose anchor as a related chunk, got {file_chunks:?}"
    );

    let pack = pack_for(&svc, "needle", 1).await;
    assert!(
        pack.contains("called implementation context"),
        "calls metadata should create graph-expanded context, got: {pack}"
    );
}

#[tokio::test]
async fn test_delete_removes_auto_indexed_graph_chunk() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "related",
        vec![0.0, 0.0, 1.0],
        "auto indexed graph context",
        b"",
    )
    .await;
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle anchor text",
        br#"{"related_ids":["related"]}"#,
    )
    .await;

    svc.delete(Request::new(DeleteRequest {
        collection: "test".into(),
        id: "anchor".into(),
    }))
    .await
    .expect("delete failed");

    let pack = pack_for(&svc, "auto indexed", 1).await;
    assert!(pack.contains("auto indexed graph context"));
    assert!(
        !pack.contains("needle anchor text"),
        "deleted chunk should not be reintroduced through graph expansion, got: {pack}"
    );
}

#[tokio::test]
async fn test_hybrid_result_carries_metadata() {
    let svc = setup();
    seed_disagreeing(&svc).await;

    let hybrid = search(&svc, "needle", 3, true, None, None).await;
    let (top_id, top_meta) = &hybrid[0];
    assert_eq!(top_id, "doc_both");
    assert!(
        top_meta.contains("both"),
        "fused result should carry stored metadata, got {top_meta:?}"
    );
}

#[tokio::test]
async fn test_text_search_applies_legacy_metadata_filter_to_hybrid_results() {
    let svc = setup();
    insert(
        &svc,
        "tenant_a",
        vec![1.0, 0.0, 0.0],
        "needle shared text",
        br#"{"tenant":"a"}"#,
    )
    .await;
    insert(
        &svc,
        "tenant_b",
        vec![0.9, 0.1, 0.0],
        "needle shared text",
        br#"{"tenant":"b"}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"tenant":"a"}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["tenant_a"]);
}

#[tokio::test]
async fn test_text_search_applies_nested_legacy_metadata_filter() {
    let svc = setup();
    insert(
        &svc,
        "contract-2025",
        vec![1.0, 0.0, 0.0],
        "needle contract text",
        br#"{"contract":{"customer":"HGC","year":2025,"amount":1200}}"#,
    )
    .await;
    insert(
        &svc,
        "contract-2024",
        vec![0.9, 0.1, 0.0],
        "needle contract text",
        br#"{"contract":{"customer":"HGC","year":2024,"amount":900}}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 10,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: false,
            pack_token_budget: None,
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"contract":{"year":2025}}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: String::new(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["contract-2025"]);
}

#[tokio::test]
async fn test_text_search_filter_blocks_graph_expanded_context() {
    let (svc, _graph) = setup_with_graph();
    insert(
        &svc,
        "anchor",
        vec![1.0, 0.0, 0.0],
        "needle allowed context",
        br#"{"tenant":"a","related_ids":["secret"]}"#,
    )
    .await;
    insert(
        &svc,
        "secret",
        vec![0.0, 0.0, 1.0],
        "graph secret context",
        br#"{"tenant":"b"}"#,
    )
    .await;

    let resp = svc
        .text_search(Request::new(TextSearchRequest {
            collection: "test".into(),
            text: "needle".into(),
            top_k: 2,
            nprobe: None,
            hybrid: true,
            dense_weight: None,
            lexical_weight: None,
            pack: true,
            pack_token_budget: Some(1024),
            rerank: false,
            diversity: false,
            mmr_lambda: None,
            filter: br#"{"tenant":"a"}"#.to_vec(),
            tag_filter: None,
            retrieval_mode: "graph_hybrid".into(),
        }))
        .await
        .expect("text_search failed")
        .into_inner();

    let got: Vec<&str> = resp.results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(got, vec!["anchor"]);
    assert!(
        resp.context_pack.contains("needle allowed context"),
        "allowed anchor context expected, got: {}",
        resp.context_pack
    );
    assert!(
        !resp.context_pack.contains("graph secret context"),
        "filtered graph expansion leaked context: {}",
        resp.context_pack
    );
}
