//! Real HNSW/RocksDB/graph/SQL tests of the document commit boundary.

use akidb_common::VectorId;
use akidb_faiss::{HnswConfig, HnswIndex, VectorIndex};
use akidb_graph::{GraphIndex, NativeGraphIndex, NodeKind};
use akidb_grpc::auth::AuthContext;
use akidb_grpc::proto::{akidb_server::Akidb, *};
use akidb_grpc::{AkiDbService, EmbeddingProvider};
use akidb_sql::SqliteMetadataIndex;
use akidb_storage::{IdMapping, RocksDbBackend, StorageBackend};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tonic::{Code, Request};

type Service = AkiDbService<HnswIndex, RocksDbBackend>;
struct Embedding;
impl EmbeddingProvider for Embedding {
    fn embed_text(&self, _: &str) -> Result<Vec<f32>, String> {
        Ok(vec![1.0, 0.0])
    }
    fn embedding_dimensions(&self) -> usize {
        2
    }
}

struct Harness {
    index: Arc<HnswIndex>,
    storage: Arc<RocksDbBackend>,
    service: Arc<Service>,
    mapping: Arc<IdMapping<RocksDbBackend>>,
    graph: Arc<NativeGraphIndex<RocksDbBackend>>,
    _dir: tempfile::TempDir,
}
fn setup() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
    let mapping = Arc::new(IdMapping::new(storage.clone(), "default"));
    let graph = Arc::new(NativeGraphIndex::new(storage.clone()));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(2)).unwrap());
    let service = AkiDbService::new(index.clone(), mapping.clone(), "default")
        .with_embedding_provider(Arc::new(Embedding))
        .with_graph_index(graph.clone())
        .with_metadata_sql_index(Arc::new(SqliteMetadataIndex::in_memory().unwrap()));
    Harness {
        index,
        storage,
        service: Arc::new(service),
        mapping,
        graph,
        _dir: dir,
    }
}
fn scoped<T>(body: T, workspace: &str) -> Request<T> {
    let mut request = Request::new(body);
    request.extensions_mut().insert(AuthContext {
        workspace_id: workspace.into(),
        agent_id: None,
        authenticated: true,
        principal_id: None,
        credential_id: None,
        correlation_hint: None,
    });
    request
}
fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text))
}
fn replacement(version: &str, revision: &str, count: usize) -> ReplaceDocumentRequest {
    ReplaceDocumentRequest {
        collection: "default".into(),
        document_id: digest("source"),
        source_uri: "file:///synthetic/source.md".into(),
        document_version: digest(version),
        settings_fingerprint: digest("settings"),
        expected_revision: revision.into(),
        chunks: (0..count)
            .map(|i| DocumentChunk {
                vector: vec![1.0, i as f32 * 0.001],
                text: format!("document {version} passage {i}"),
                metadata: br#"{"entity_ids":["topic"]}"#.to_vec(),
            })
            .collect(),
        legacy_id_prefix: String::new(),
    }
}
async fn head(service: &Service, workspace: &str) -> GetDocumentResponse {
    service
        .get_document(scoped(
            GetDocumentRequest {
                collection: "default".into(),
                document_id: digest("source"),
            },
            workspace,
        ))
        .await
        .unwrap()
        .into_inner()
}
async fn search(service: &Service, mode: &str) -> SearchResponse {
    service
        .text_search(scoped(
            TextSearchRequest {
                collection: "default".into(),
                text: "document".into(),
                retrieval_mode: mode.into(),
                top_k: 100,
                pack: true,
                pack_token_budget: Some(4096),
                ..Default::default()
            },
            "alpha",
        ))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_is_complete_idempotent_scoped_and_handles_shrink_empty_recreate() {
    let h = setup();
    let body = replacement("first", "", 3);
    let first = h
        .service
        .replace_document(scoped(body.clone(), "alpha"))
        .await
        .unwrap()
        .into_inner();
    let replay = h
        .service
        .replace_document(scoped(body, "alpha"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.unchanged);
    assert_eq!(replay.revision, first.revision);
    assert!(!head(&h.service, "beta").await.found);
    let old_id = search(&h.service, "bm25").await.results[0].id.clone();
    let error = h
        .service
        .delete(scoped(
            DeleteRequest {
                collection: "default".into(),
                id: old_id.clone(),
            },
            "alpha",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    let mut invalid = replacement("bad", &first.revision, 3);
    invalid.chunks[1].vector = vec![1.0];
    assert_eq!(
        h.service
            .replace_document(scoped(invalid, "alpha"))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(head(&h.service, "alpha").await.revision, first.revision);
    assert_eq!(
        h.service
            .replace_document(scoped(replacement("conflict", "", 1), "alpha"))
            .await
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    let mut revision = first.revision;
    for (version, count) in [("second", 1), ("empty", 0), ("recreated", 4)] {
        let response = h
            .service
            .replace_document(scoped(replacement(version, &revision, count), "alpha"))
            .await
            .unwrap()
            .into_inner();
        revision = response.revision;
        let status = head(&h.service, "alpha").await;
        assert!(status.complete);
        assert_eq!(status.chunk_count, count as u32);
        assert_eq!(
            h.mapping.load_vectors_including_deleted().unwrap().len(),
            count,
            "retired managed embeddings must not accumulate on each replacement"
        );
        for mode in ["bm25", "vector", "hybrid", "graph_hybrid"] {
            let result = search(&h.service, mode).await;
            assert_eq!(result.results.len(), count, "mode {mode}");
            assert!(result
                .results
                .iter()
                .all(
                    |r| serde_json::from_str::<serde_json::Value>(&r.metadata).unwrap()
                        ["document_version"]
                        == digest(version)
                ));
            assert!(result
                .context_pack_v1
                .unwrap()
                .items
                .iter()
                .all(|item| item.text.contains(version)));
        }
        assert_eq!(
            h.graph.count_nodes_of_kind(NodeKind::Chunk).unwrap(),
            count as u64
        );
    }
    assert_eq!(
        h.service
            .get(scoped(
                GetRequest {
                    collection: "default".into(),
                    id: old_id
                },
                "alpha"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_never_receive_a_mixture_of_document_versions() {
    let h = setup();
    h.service
        .replace_document(scoped(replacement("initial", "", 12), "alpha"))
        .await
        .unwrap();
    let writer = h.service.clone();
    let update = tokio::spawn(async move {
        for i in 0..12 {
            let status = head(&writer, "alpha").await;
            let body = replacement(&format!("revision{i}"), &status.revision, 12);
            loop {
                match writer.replace_document(scoped(body.clone(), "alpha")).await {
                    Ok(_) => break,
                    Err(e) if e.code() == Code::Aborted => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await
                    }
                    Err(e) => panic!("replacement failed: {e}"),
                }
            }
        }
    });
    for i in 0..60 {
        let result = search(&h.service, ["bm25", "hybrid", "graph_hybrid"][i % 3]).await;
        assert_eq!(result.results.len(), 12);
        let versions: std::collections::HashSet<_> = result
            .results
            .iter()
            .map(|r| {
                serde_json::from_str::<serde_json::Value>(&r.metadata).unwrap()["document_version"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(versions.len(), 1);
    }
    update.await.unwrap();
}

#[tokio::test]
async fn missing_middle_chunk_is_detected_and_repaired_without_tombstone_reuse() {
    let h = setup();
    h.service
        .replace_document(scoped(replacement("first", "", 3), "alpha"))
        .await
        .unwrap();
    let status = head(&h.service, "alpha").await;
    let vectors = h.mapping.load_active_vectors().unwrap();
    let middle = vectors
        .iter()
        .find(|v| v.external_id.ends_with(":1"))
        .unwrap();
    h.mapping
        .mark_deleted(&VectorId::new(&middle.external_id))
        .unwrap();
    assert!(!head(&h.service, "alpha").await.complete);
    h.service
        .replace_document(scoped(replacement("first", &status.revision, 3), "alpha"))
        .await
        .unwrap();
    assert!(head(&h.service, "alpha").await.complete);
    assert_eq!(search(&h.service, "bm25").await.results.len(), 3);
}

#[tokio::test]
async fn legacy_migration_scans_past_missing_middle_chunks() {
    let h = setup();
    for i in [0, 2] {
        h.service
            .insert(scoped(
                InsertRequest {
                    collection: "default".into(),
                    id: format!("0123456789abcdef-{i:05}"),
                    vector: vec![1.0, 0.0],
                    text: "document legacy".into(),
                    metadata:
                        br#"{"ingest":"akidb-index","source_uri":"file:///synthetic/source.md"}"#
                            .to_vec(),
                },
                "alpha",
            ))
            .await
            .unwrap();
    }
    let mut body = replacement("migrated", "", 1);
    body.legacy_id_prefix = "0123456789abcdef-".into();
    h.service
        .replace_document(scoped(body, "alpha"))
        .await
        .unwrap();
    let results = search(&h.service, "bm25").await;
    assert_eq!(results.results.len(), 1);
    assert!(results.context_pack_v1.unwrap().items[0]
        .text
        .contains("migrated"));
}

struct BrokenSql;
impl akidb_sql::MetadataSqlIndex for BrokenSql {
    fn upsert_record(&self, _: &akidb_sql::SqlMetadataRecord) -> akidb_sql::Result<()> {
        Err(akidb_sql::SqlMetadataError::Lock(
            "injected projection failure".into(),
        ))
    }
    fn delete_record(&self, _: &str, _: &str) -> akidb_sql::Result<()> {
        Err(akidb_sql::SqlMetadataError::Lock(
            "injected projection failure".into(),
        ))
    }
    fn clear_collection(&self, _: &str) -> akidb_sql::Result<()> {
        Ok(())
    }
    fn query_ids(&self, _: &akidb_sql::MetadataQuery) -> akidb_sql::Result<Vec<String>> {
        Ok(vec![])
    }
    fn stats(&self) -> akidb_sql::Result<akidb_sql::SqlMetadataStats> {
        Err(akidb_sql::SqlMetadataError::Lock(
            "injected projection failure".into(),
        ))
    }
}

#[tokio::test]
async fn committed_projection_failure_stops_reads_and_keeps_recovery_marker() {
    let h = setup();
    let service = AkiDbService::new(
        Arc::new(HnswIndex::new(HnswConfig::new(2)).unwrap()),
        h.mapping.clone(),
        "default",
    )
    .with_metadata_sql_index(Arc::new(BrokenSql));
    let error = service
        .replace_document(scoped(replacement("committed", "", 2), "alpha"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(h.mapping.load_active_vectors().unwrap().len(), 2);
    assert!(h.mapping.document_projections_dirty().unwrap());
    assert!(
        !service
            .health(Request::new(HealthRequest {}))
            .await
            .unwrap()
            .into_inner()
            .ready
    );
    assert_eq!(
        service
            .get_document(scoped(
                GetDocumentRequest {
                    collection: "default".into(),
                    document_id: digest("source"),
                },
                "alpha"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(
        service
            .search(scoped(
                SearchRequest {
                    collection: "default".into(),
                    query: vec![1.0, 0.0],
                    top_k: 10,
                    ..Default::default()
                },
                "alpha"
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let mut later = replacement("later", "", 1);
    later.document_id = digest("other-source");
    assert_eq!(
        service
            .replace_document(scoped(later, "alpha"))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(h.mapping.load_active_vectors().unwrap().len(), 2);
    assert!(service.recover_document_projections().is_err());
    assert!(h.mapping.document_projections_dirty().unwrap());
}

#[tokio::test]
async fn recovery_removes_retired_graph_nodes_even_when_old_vector_payload_is_missing() {
    let h = setup();
    h.service
        .replace_document(scoped(replacement("old", "", 3), "alpha"))
        .await
        .unwrap();
    let revision = head(&h.service, "alpha").await.revision;
    let middle = h
        .mapping
        .load_active_vectors()
        .unwrap()
        .into_iter()
        .find(|v| v.external_id.ends_with(":1"))
        .unwrap();
    h.storage
        .delete(format!("vec:7:default{}", middle.external_id).as_bytes())
        .unwrap();
    assert!(!head(&h.service, "alpha").await.complete);
    let failed = AkiDbService::new(h.index.clone(), h.mapping.clone(), "default")
        .with_graph_index(h.graph.clone())
        .with_metadata_sql_index(Arc::new(BrokenSql));
    assert_eq!(
        failed
            .replace_document(scoped(replacement("new", &revision, 1), "alpha"))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(h.mapping.document_retired_chunks().unwrap().len(), 3);
    let index = Arc::new(HnswIndex::new(HnswConfig::new(2)).unwrap());
    for stored in h.mapping.load_active_vectors().unwrap() {
        let id = VectorId::new(stored.external_id);
        let internal = index.insert(&id, &stored.vector).unwrap();
        h.mapping
            .upsert_with_vector(&id, internal, &stored.vector, &stored.metadata)
            .unwrap();
    }
    let recovered = AkiDbService::new(index, h.mapping.clone(), "default")
        .with_graph_index(h.graph.clone())
        .with_embedding_provider(Arc::new(Embedding))
        .with_metadata_sql_index(Arc::new(SqliteMetadataIndex::in_memory().unwrap()));
    recovered.rebuild_lexical_index();
    recovered.recover_document_projections().unwrap();
    assert!(!h.mapping.document_projections_dirty().unwrap());
    assert_eq!(h.graph.count_nodes_of_kind(NodeKind::Chunk).unwrap(), 1);
    assert_eq!(search(&recovered, "graph_hybrid").await.results.len(), 1);
    assert!(head(&recovered, "alpha").await.complete);
}
