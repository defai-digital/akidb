//! ADR-0009 access-record emission contract test.
//!
//! Builds a real `AkiDbService` with the spool emitter enabled, drives the
//! data-plane RPCs, and verifies that each emits exactly one content-free
//! access record carrying verified identity, a keyed query digest, hit
//! counts, and latency — and that a failing RPC is recorded as a denial
//! rather than disappearing.

use std::sync::Arc;
use std::time::Duration;

use akidb_common::config::AccessLogConfig;
use akidb_faiss::{HnswConfig, HnswIndex};
use akidb_grpc::proto::akidb_server::Akidb;
use akidb_grpc::proto::{DeleteRequest, GetRequest, InsertRequest, SearchRequest};
use akidb_grpc::{AccessLog, AkiDbService};
use akidb_storage::{IdMapping, RocksDbBackend};
use tonic::Request;

const DIMS: usize = 3;

fn wait_for_records(directory: &std::path::Path, expected: usize) -> Vec<serde_json::Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut records = Vec::new();
        if let Ok(entries) = std::fs::read_dir(directory) {
            for entry in entries.flatten() {
                if let Ok(content) = std::fs::read_to_string(entry.path()) {
                    for line in content.lines() {
                        records.push(serde_json::from_str(line).unwrap());
                    }
                }
            }
        }
        if records.len() >= expected || std::time::Instant::now() > deadline {
            return records;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_plane_rpcs_emit_one_access_record_each() {
    let data_dir = tempfile::tempdir().unwrap();
    let spool_dir = tempfile::tempdir().unwrap();

    let storage = Arc::new(RocksDbBackend::open(data_dir.path()).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "test"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let access_log = AccessLog::spawn(&AccessLogConfig {
        enabled: true,
        directory: spool_dir.path().display().to_string(),
        ..Default::default()
    })
    .unwrap();
    let svc = AkiDbService::new(index, id_mapping, "test").with_access_log(access_log.clone());

    // Insert (ok), Search (ok, 1 hit), Get (ok, found), Delete (ok), and one
    // failing Get (invalid id -> error/denied record).
    svc.insert(Request::new(InsertRequest {
        collection: "test".into(),
        id: "doc-1".into(),
        vector: vec![1.0, 0.0, 0.0],
        metadata: br#"{"k":"v"}"#.to_vec(),
        text: "hello world".into(),
    }))
    .await
    .expect("insert failed");
    svc.search(Request::new(SearchRequest {
        collection: "test".into(),
        query: vec![1.0, 0.0, 0.0],
        top_k: 4,
        ..Default::default()
    }))
    .await
    .expect("search failed");
    svc.get(Request::new(GetRequest {
        collection: "test".into(),
        id: "doc-1".into(),
    }))
    .await
    .expect("get failed");
    svc.delete(Request::new(DeleteRequest {
        collection: "test".into(),
        id: "doc-1".into(),
    }))
    .await
    .expect("delete failed");
    let failed_get = svc
        .get(Request::new(GetRequest {
            collection: "test".into(),
            id: "bad id!".into(),
        }))
        .await;
    assert!(failed_get.is_err());

    let records = wait_for_records(spool_dir.path(), 5);
    assert_eq!(
        records.len(),
        5,
        "expected exactly five access records, got {records:?}"
    );

    let by_op = |op: &str| {
        records
            .iter()
            .find(|record| record["operation"] == op)
            .unwrap_or_else(|| panic!("no access record for {op}: {records:?}"))
    };

    let insert = by_op("Insert");
    assert_eq!(insert["outcome"], "ok");
    assert_eq!(insert["collection"], "test");
    assert!(insert["query_hash"].as_str().unwrap().len() == 64);
    assert!(insert["request_id"].as_str().unwrap().starts_with("rq_"));
    // No principal credential was presented: identity must be honestly null,
    // and workspace falls back to the client/default assertion.
    assert!(insert["principal_id"].is_null());
    assert_eq!(insert["authenticated"], false);

    let search = by_op("Search");
    assert_eq!(search["outcome"], "ok");
    assert_eq!(search["hit_count"], 1);

    let get = by_op("Get");
    assert_eq!(get["outcome"], "ok");
    assert_eq!(get["hit_count"], 1);

    let delete = by_op("Delete");
    assert_eq!(delete["outcome"], "ok");

    let denied: Vec<_> = records
        .iter()
        .filter(|record| record["outcome"] != "ok")
        .collect();
    assert_eq!(
        denied.len(),
        1,
        "the failed Get must be recorded: {records:?}"
    );
    assert_eq!(denied[0]["operation"], "Get");
    assert_eq!(denied[0]["outcome"], "error");
    assert_ne!(denied[0]["status_code"], "OK");

    // No raw content anywhere in the spool: neither the inserted text nor
    // the vector id may appear in any record.
    let raw = std::fs::read_dir(spool_dir.path())
        .unwrap()
        .flatten()
        .map(|entry| std::fs::read_to_string(entry.path()).unwrap())
        .collect::<String>();
    assert!(!raw.contains("hello world"), "spool leaked query text");
    assert!(!raw.contains("doc-1"), "spool leaked a record id");

    assert_eq!(access_log.dropped_count(), 0);
}
