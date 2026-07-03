//! MCP (Model Context Protocol) server surface (INT-001, ADR-006).
//!
//! Lets any MCP-capable agent use AkiDB as a memory/retrieval tool. Speaks
//! JSON-RPC 2.0 over newline-delimited stdio (the MCP stdio transport) and
//! dispatches `tools/call` into the existing [`AkiDbService`]. The tool surface
//! (the PRD flags this as an open question) is chosen here as the minimal useful
//! set:
//!
//! - `search` — hybrid retrieval, returns ranked ids + scores.
//! - `pack` — hybrid retrieval assembled into a cited context pack.
//! - `memory_write` — store an agent-memory entry (embedded + indexed).
//! - `memory_read` — retrieve memory, optionally scoped to a conversation.
//! - `status` — index statistics.
//!
//! [`handle_request`] is the pure request→response core (testable without I/O);
//! [`run_stdio`] is the thin transport loop.

use std::sync::Arc;

use akidb_faiss::VectorIndex;
use akidb_retrieval::{MemoryEntry, MemoryKind};
use akidb_storage::StorageBackend;
use serde_json::{json, Value};
use tonic::Request;

use crate::proto::akidb_server::Akidb;
use crate::proto::{
    tag_filter::FilterType, tag_value::Value as TagVal, InsertRequest, SearchRequest, TagCondition,
    TagFilter, TagOperator, TagValue, TextSearchRequest,
};
use crate::service::AkiDbService;

const PROTOCOL_VERSION: &str = "2024-11-05";
const COLLECTION: &str = "default";

/// Handle one JSON-RPC message. Returns the response JSON string, or `None` for
/// notifications (which take no response) and unparseable input that lacks an id.
pub async fn handle_request<I, S>(service: &AkiDbService<I, S>, raw: &str) -> Option<String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let req: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return Some(error_response(Value::Null, -32700, "parse error")),
    };

    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    // Notifications (e.g. "notifications/initialized") carry no id and want no reply.
    if method.starts_with("notifications/") || req.get("id").is_none() {
        return None;
    }
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let params = req.get("params").cloned().unwrap_or(Value::Null);

    match method {
        "initialize" => Some(ok_response(id, initialize_result())),
        "ping" => Some(ok_response(id, json!({}))),
        "tools/list" => Some(ok_response(id, json!({ "tools": tool_definitions() }))),
        "tools/call" => {
            let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(Value::Null);
            match call_tool(service, name, &args).await {
                Ok(text) => Some(ok_response(
                    id,
                    json!({ "content": [{ "type": "text", "text": text }], "isError": false }),
                )),
                Err(msg) => Some(ok_response(
                    id,
                    json!({ "content": [{ "type": "text", "text": msg }], "isError": true }),
                )),
            }
        }
        _ => Some(error_response(id, -32601, "method not found")),
    }
}

fn ok_response(id: Value, result: Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string()
}

fn error_response(id: Value, code: i64, message: &str) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }).to_string()
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "serverInfo": { "name": "akidb", "version": env!("CARGO_PKG_VERSION") },
        "capabilities": { "tools": {} },
    })
}

fn tool_definitions() -> Value {
    json!([
        {
            "name": "search",
            "description": "Hybrid (dense + lexical) retrieval; returns ranked ids and scores.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "top_k": { "type": "integer" },
                    "hybrid": { "type": "boolean" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "pack",
            "description": "Retrieve and assemble a source-grounded, cited context pack.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "top_k": { "type": "integer" },
                    "token_budget": { "type": "integer" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "memory_write",
            "description": "Store an agent-memory entry (embedded and indexed for retrieval).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "kind": { "type": "string" },
                    "text": { "type": "string" },
                    "conversation_id": { "type": "string" },
                    "task_id": { "type": "string" },
                    "tool": { "type": "string" },
                    "source_uri": { "type": "string" }
                },
                "required": ["id", "text"]
            }
        },
        {
            "name": "memory_read",
            "description": "Retrieve agent memory, optionally scoped to a conversation_id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "conversation_id": { "type": "string" },
                    "top_k": { "type": "integer" }
                },
                "required": ["query"]
            }
        },
        {
            "name": "status",
            "description": "Index statistics (active/total/tombstoned vectors, dimensions).",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

async fn call_tool<I, S>(
    service: &AkiDbService<I, S>,
    name: &str,
    args: &Value,
) -> Result<String, String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    match name {
        "search" => tool_search(service, args).await,
        "pack" => tool_pack(service, args).await,
        "memory_write" => tool_memory_write(service, args).await,
        "memory_read" => tool_memory_read(service, args).await,
        "status" => Ok(tool_status(service)),
        _ => Err(format!("unknown tool: {name}")),
    }
}

fn arg_str(args: &Value, key: &str) -> Result<Option<String>, String> {
    let Some(value) = args.get(key) else {
        return Ok(None);
    };
    value
        .as_str()
        .map(String::from)
        .map(Some)
        .ok_or_else(|| format!("'{key}' must be a string"))
}

fn required_str(args: &Value, key: &str) -> Result<String, String> {
    arg_str(args, key)?.ok_or_else(|| format!("missing '{key}'"))
}

fn arg_u32(args: &Value, key: &str, default: u32) -> Result<u32, String> {
    let Some(value) = args.get(key) else {
        return Ok(default);
    };
    let Some(n) = value.as_u64() else {
        return Err(format!("'{key}' must be a non-negative integer"));
    };
    u32::try_from(n).map_err(|_| format!("'{key}' exceeds u32 range"))
}

fn arg_bool(args: &Value, key: &str, default: bool) -> Result<bool, String> {
    let Some(value) = args.get(key) else {
        return Ok(default);
    };
    value
        .as_bool()
        .ok_or_else(|| format!("'{key}' must be a boolean"))
}

fn text_search_request(
    text: String,
    top_k: u32,
    hybrid: bool,
    pack: bool,
    budget: Option<u32>,
) -> TextSearchRequest {
    TextSearchRequest {
        collection: COLLECTION.to_string(),
        text,
        top_k,
        nprobe: None,
        hybrid,
        dense_weight: None,
        lexical_weight: None,
        pack,
        pack_token_budget: budget,
        rerank: false,
        diversity: false,
        mmr_lambda: None,
        filter: vec![],
        tag_filter: None,
        retrieval_mode: String::new(),
    }
}

async fn tool_search<I, S>(service: &AkiDbService<I, S>, args: &Value) -> Result<String, String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let query = required_str(args, "query")?;
    let top_k = arg_u32(args, "top_k", 10)?;
    let hybrid = arg_bool(args, "hybrid", true)?;
    let resp = service
        .text_search(Request::new(text_search_request(
            query, top_k, hybrid, false, None,
        )))
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let items: Vec<Value> = resp
        .results
        .iter()
        .map(|r| json!({ "id": r.id, "score": r.score }))
        .collect();
    Ok(serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string()))
}

async fn tool_pack<I, S>(service: &AkiDbService<I, S>, args: &Value) -> Result<String, String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let query = required_str(args, "query")?;
    let top_k = arg_u32(args, "top_k", 10)?;
    let budget = arg_u32(args, "token_budget", 1024)?;
    let resp = service
        .text_search(Request::new(text_search_request(
            query,
            top_k,
            true,
            true,
            Some(budget),
        )))
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    Ok(resp.context_pack)
}

async fn tool_memory_write<I, S>(
    service: &AkiDbService<I, S>,
    args: &Value,
) -> Result<String, String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let id = required_str(args, "id")?;
    let text = required_str(args, "text")?;
    let kind = match arg_str(args, "kind")? {
        Some(kind) => {
            MemoryKind::parse(&kind).ok_or_else(|| format!("unknown memory kind: {kind}"))?
        }
        None => MemoryKind::Note,
    };

    let mut entry = MemoryEntry::new(id.clone(), kind, text.clone());
    if let Some(v) = arg_str(args, "conversation_id")? {
        entry = entry.with_conversation(v);
    }
    if let Some(v) = arg_str(args, "task_id")? {
        entry = entry.with_task(v);
    }
    if let Some(v) = arg_str(args, "tool")? {
        entry = entry.with_tool(v);
    }
    if let Some(v) = arg_str(args, "source_uri")? {
        entry = entry.with_source(v);
    }

    let vector = service.embed_text(&text)?;
    let metadata = serde_json::to_vec(&entry.to_metadata()).map_err(|e| e.to_string())?;
    service
        .insert(Request::new(InsertRequest {
            collection: COLLECTION.to_string(),
            id: id.clone(),
            vector,
            metadata,
            text,
        }))
        .await
        .map_err(|e| e.message().to_string())?;
    Ok(format!("stored memory '{id}'"))
}

async fn tool_memory_read<I, S>(
    service: &AkiDbService<I, S>,
    args: &Value,
) -> Result<String, String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let query = required_str(args, "query")?;
    let top_k = arg_u32(args, "top_k", 10)?;
    let vector = service.embed_text(&query)?;

    // Scope to a conversation when provided, via a typed tag filter.
    let tag_filter = arg_str(args, "conversation_id")?.map(|cid| TagFilter {
        filter_type: Some(FilterType::Condition(TagCondition {
            key: "conversation_id".to_string(),
            value: Some(TagValue {
                value: Some(TagVal::Text(cid)),
            }),
            op: TagOperator::TagOpEq as i32,
        })),
    });

    let resp = service
        .search(Request::new(SearchRequest {
            collection: COLLECTION.to_string(),
            query: vector,
            top_k,
            nprobe: None,
            filter: vec![],
            tag_filter,
        }))
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let items: Vec<Value> = resp
        .results
        .iter()
        .map(|r| json!({ "id": r.id, "score": r.score, "metadata": r.metadata }))
        .collect();
    Ok(serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string()))
}

fn tool_status<I, S>(service: &AkiDbService<I, S>) -> String
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let s = service.index_stats();
    let graph = service.graph_stats();
    json!({
        "active_vectors": s.active_vectors,
        "total_vectors": s.total_vectors,
        "tombstoned_vectors": s.tombstoned_vectors,
        "dimensions": s.dimensions,
        "graph": graph.map(|g| json!({
            "nodes": g.nodes,
            "edges": g.edges,
            "chunk_links": g.chunk_links,
        })),
    })
    .to_string()
}

/// Run the MCP server over newline-delimited JSON-RPC on stdio.
pub async fn run_stdio<I, S>(service: Arc<AkiDbService<I, S>>) -> std::io::Result<()>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if let Some(resp) = handle_request(&service, &line).await {
            stdout.write_all(resp.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}
