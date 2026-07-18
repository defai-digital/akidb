//! Integration tests for the MCP server surface over a real service.

use std::sync::Arc;

use akidb_faiss::{HnswConfig, HnswIndex};
use akidb_graph::{EdgeKind, GraphEdge, GraphIndex, GraphNode, NativeGraphIndex, NodeKind};
use akidb_grpc::mcp::handle_request;
use akidb_grpc::{AkiDbService, EmbeddingProvider};
use akidb_storage::{IdMapping, RocksDbBackend};
use serde_json::Value;

const DIMS: usize = 3;

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
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage, "default"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    AkiDbService::new(index, id_mapping, "default").with_embedding_provider(Arc::new(StubEmbedder))
}

fn setup_with_graph() -> (
    AkiDbService<HnswIndex, RocksDbBackend>,
    Arc<NativeGraphIndex<RocksDbBackend>>,
) {
    let dir = tempfile::tempdir().unwrap().keep();
    let storage = Arc::new(RocksDbBackend::open(&dir).unwrap());
    let id_mapping = Arc::new(IdMapping::new(storage.clone(), "default"));
    let index = Arc::new(HnswIndex::new(HnswConfig::new(DIMS)).unwrap());
    let graph = Arc::new(NativeGraphIndex::new(storage));
    let service = AkiDbService::new(index, id_mapping, "default")
        .with_embedding_provider(Arc::new(StubEmbedder))
        .with_graph_index(graph.clone());
    (service, graph)
}

async fn call(svc: &AkiDbService<HnswIndex, RocksDbBackend>, raw: &str) -> Value {
    let resp = handle_request(svc, raw).await.expect("expected a response");
    serde_json::from_str(&resp).unwrap()
}

#[tokio::test]
async fn test_initialize_reports_server_info() {
    let svc = setup();
    let v = call(&svc, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#).await;
    assert_eq!(v["id"], 1);
    assert_eq!(v["result"]["serverInfo"]["name"], "akidb");
    assert!(v["result"]["protocolVersion"].is_string());
}

#[tokio::test]
async fn test_tools_list_exposes_the_tool_surface() {
    let svc = setup();
    let v = call(&svc, r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#).await;
    let tools = v["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in ["search", "pack", "memory_write", "memory_read", "status"] {
        assert!(names.contains(&expected), "missing tool {expected}");
    }
    // Workspace ACL must be documented on memory tools (criterion 2).
    for name in ["memory_write", "memory_read", "search", "pack"] {
        let tool = tools.iter().find(|t| t["name"] == name).unwrap();
        let props = &tool["inputSchema"]["properties"];
        assert!(
            props.get("workspace").is_some() || props.get("workspace_id").is_some(),
            "{name} schema must expose workspace"
        );
    }
}

#[tokio::test]
async fn test_mcp_memory_workspace_isolation() {
    let svc = setup();

    // Write into two workspaces via the real MCP tools/call path.
    let write_a = r#"{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"memory_write","arguments":{"id":"mem-a","text":"alpha secret note","workspace":"ws-a"}}}"#;
    let write_b = r#"{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"memory_write","arguments":{"id":"mem-b","text":"beta secret note","workspace":"ws-b"}}}"#;
    let ra = call(&svc, write_a).await;
    assert_eq!(ra["result"]["isError"], false, "{ra}");
    let rb = call(&svc, write_b).await;
    assert_eq!(rb["result"]["isError"], false, "{rb}");

    let read_a = r#"{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"memory_read","arguments":{"query":"secret note","workspace":"ws-a","top_k":10}}}"#;
    let va = call(&svc, read_a).await;
    assert_eq!(va["result"]["isError"], false, "{va}");
    let text_a = va["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(text_a.contains("mem-a"), "ws-a should see mem-a: {text_a}");
    assert!(!text_a.contains("mem-b"), "ws-a must not see mem-b: {text_a}");

    let read_b = r#"{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"memory_read","arguments":{"query":"secret note","workspace":"ws-b","top_k":10}}}"#;
    let vb = call(&svc, read_b).await;
    assert_eq!(vb["result"]["isError"], false, "{vb}");
    let text_b = vb["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(text_b.contains("mem-b"), "ws-b should see mem-b: {text_b}");
    assert!(!text_b.contains("mem-a"), "ws-b must not see mem-a: {text_b}");
}

#[tokio::test]
async fn test_unknown_method_returns_error() {
    let svc = setup();
    let v = call(&svc, r#"{"jsonrpc":"2.0","id":3,"method":"bogus"}"#).await;
    assert_eq!(v["error"]["code"], -32601);
}

#[tokio::test]
async fn test_notification_yields_no_response() {
    let svc = setup();
    let resp = handle_request(
        &svc,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .await;
    assert!(resp.is_none());
}

#[tokio::test]
async fn test_parse_error_response() {
    let svc = setup();
    let v = call(&svc, "not json").await;
    assert_eq!(v["error"]["code"], -32700);
}

#[tokio::test]
async fn test_memory_write_then_read_roundtrip() {
    let svc = setup();

    let write = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"memory_write","arguments":{"id":"m1","kind":"note","text":"remember the api key rotation","conversation_id":"conv-1"}}}"#,
    )
    .await;
    assert_eq!(write["result"]["isError"], false);

    // memory_read scoped to the same conversation should find it.
    let read = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"memory_read","arguments":{"query":"api key","conversation_id":"conv-1"}}}"#,
    )
    .await;
    assert_eq!(read["result"]["isError"], false);
    let text = read["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("m1"), "expected memory m1 in results: {text}");

    // A different conversation must not see it.
    let other = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"memory_read","arguments":{"query":"api key","conversation_id":"conv-2"}}}"#,
    )
    .await;
    let other_text = other["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        !other_text.contains("m1"),
        "conversation scoping leaked: {other_text}"
    );
}

#[tokio::test]
async fn test_memory_write_rejects_unknown_kind() {
    let svc = setup();

    let response = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"memory_write","arguments":{"id":"m-bad","kind":"bogus","text":"should not store"}}}"#,
    )
    .await;

    assert_eq!(response["result"]["isError"], true);
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("kind") && text.contains("unknown"),
        "expected unknown kind error, got: {text}"
    );
}

#[tokio::test]
async fn test_memory_read_rejects_non_string_conversation_scope() {
    let svc = setup();

    let write = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"memory_write","arguments":{"id":"m-scoped","kind":"note","text":"scoped secret memory","conversation_id":"conv-1"}}}"#,
    )
    .await;
    assert_eq!(write["result"]["isError"], false);

    let read = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"memory_read","arguments":{"query":"secret","conversation_id":123}}}"#,
    )
    .await;

    assert_eq!(read["result"]["isError"], true);
    let text = read["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("conversation_id") && text.contains("string"),
        "expected conversation_id type error, got: {text}"
    );
}

#[tokio::test]
async fn test_search_and_pack_tools() {
    let svc = setup();
    call(
        &svc,
        r#"{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"memory_write","arguments":{"id":"d1","kind":"source","text":"needle in the haystack"}}}"#,
    )
    .await;

    let search = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"search","arguments":{"query":"needle","top_k":5}}}"#,
    )
    .await;
    let text = search["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("d1"), "search should find d1: {text}");

    let pack = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{"name":"pack","arguments":{"query":"needle"}}}"#,
    )
    .await;
    let packed = pack["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        packed.contains("needle in the haystack"),
        "pack content: {packed}"
    );
}

#[tokio::test]
async fn test_tool_integer_arguments_reject_overflow() {
    let svc = setup();
    let search = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":23,"method":"tools/call","params":{"name":"search","arguments":{"query":"needle","top_k":18446744073709551615}}}"#,
    )
    .await;

    assert_eq!(search["result"]["isError"], true);
    let text = search["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("top_k") && text.contains("u32"),
        "expected top_k overflow error, got: {text}"
    );
}

#[tokio::test]
async fn test_tool_boolean_arguments_reject_wrong_type() {
    let svc = setup();
    let search = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":24,"method":"tools/call","params":{"name":"search","arguments":{"query":"needle","hybrid":"false"}}}"#,
    )
    .await;

    assert_eq!(search["result"]["isError"], true);
    let text = search["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("hybrid") && text.contains("boolean"),
        "expected hybrid type error, got: {text}"
    );
}

#[tokio::test]
async fn test_status_tool() {
    let svc = setup();
    let v = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":30,"method":"tools/call","params":{"name":"status","arguments":{}}}"#,
    )
    .await;
    let text = v["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("dimensions"), "status: {text}");
}

#[tokio::test]
async fn test_status_tool_reports_graph_stats_when_configured() {
    let (svc, graph) = setup_with_graph();
    graph
        .upsert_node(GraphNode::new("chunk:a", NodeKind::Chunk))
        .unwrap();
    graph
        .upsert_node(GraphNode::new("chunk:b", NodeKind::Chunk))
        .unwrap();
    graph
        .upsert_edge(GraphEdge::new(
            "ab",
            "chunk:a",
            "chunk:b",
            EdgeKind::RelatedTo,
        ))
        .unwrap();

    let v = call(
        &svc,
        r#"{"jsonrpc":"2.0","id":31,"method":"tools/call","params":{"name":"status","arguments":{}}}"#,
    )
    .await;
    let text = v["result"]["content"][0]["text"].as_str().unwrap();
    let status: Value = serde_json::from_str(text).unwrap();
    assert_eq!(status["graph"]["nodes"], 2);
    assert_eq!(status["graph"]["edges"], 1);
    assert_eq!(status["graph"]["chunk_links"], 1);
}
