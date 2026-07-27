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
//! - `memory_remember` — commit typed authoritative Memory preview data.
//! - `memory_recall` — retrieve typed authoritative Memory preview data.
//! - `status` — index statistics.
//!
//! [`handle_request`] is the pure request→response core (testable without I/O);
//! [`run_stdio`] is the thin transport loop.

use std::sync::Arc;

use akidb_faiss::VectorIndex;
use akidb_retrieval::{MemoryEntry, MemoryKind};
use akidb_storage::StorageBackend;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::Request;

use crate::proto::akidb_server::Akidb;
use crate::proto::memory_content;
use crate::proto::memory_service_server::MemoryService;
use crate::proto::{
    tag_filter::FilterType, tag_value::Value as TagVal, InsertRequest, MemoryContent,
    MemoryEpistemicFormation, MemoryEvidenceInput, MemoryRecallRequest, MemoryRememberRequest,
    MemoryRequestContext, MemoryScopeInput, MemorySensitivity, MemoryTextFact, SearchRequest,
    TagCondition, TagFilter, TagOperator, TagValue, TextSearchRequest,
};
use crate::service::AkiDbService;
use crate::MemoryAuthContext;
use crate::MemoryServiceImpl;

const PROTOCOL_VERSION: &str = "2024-11-05";
const COLLECTION: &str = "default";

/// One startup-authenticated principal session for the authoritative Memory
/// MCP preview. Tool arguments may narrow these defaults but never replace the
/// credential-derived maximum grants held in `auth_context`.
#[derive(Clone)]
pub struct AuthoritativeMemoryMcp<S: StorageBackend> {
    service: Arc<MemoryServiceImpl<S>>,
    auth_context: MemoryAuthContext,
    workspace_id: String,
    namespace: String,
    request_purpose: String,
    delegated_agent_id: Option<String>,
}

impl<S: StorageBackend> AuthoritativeMemoryMcp<S> {
    pub fn new(
        service: Arc<MemoryServiceImpl<S>>,
        auth_context: MemoryAuthContext,
        workspace_id: String,
        namespace: String,
        request_purpose: String,
        delegated_agent_id: Option<String>,
    ) -> Result<Self, String> {
        for (field, value) in [
            ("workspace_id", workspace_id.as_str()),
            ("namespace", namespace.as_str()),
            ("request_purpose", request_purpose.as_str()),
        ] {
            if value.trim().is_empty() || value.trim() != value {
                return Err(format!("{field} must be non-empty and trimmed"));
            }
        }
        if delegated_agent_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty() || value.trim() != value)
        {
            return Err("delegated_agent_id must be non-empty and trimmed".to_string());
        }
        Ok(Self {
            service,
            auth_context,
            workspace_id,
            namespace,
            request_purpose,
            delegated_agent_id,
        })
    }

    fn context(
        &self,
        namespace: String,
        request_purpose: String,
        delegated_agent_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> MemoryRequestContext {
        MemoryRequestContext {
            workspace_id: self.workspace_id.clone(),
            namespace,
            request_purpose,
            delegated_agent_id,
            idempotency_key,
            request_id: Some(format!("mcp_{}", uuid::Uuid::now_v7().simple())),
            scope_narrowing: None,
        }
    }

    fn authenticated_request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request.extensions_mut().insert(self.auth_context.clone());
        request
    }
}

/// Handle one JSON-RPC message. Returns the response JSON string, or `None` for
/// notifications (which take no response) and unparseable input that lacks an id.
pub async fn handle_request<I, S>(service: &AkiDbService<I, S>, raw: &str) -> Option<String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    handle_request_with_memory(service, None, raw).await
}

/// Handle one JSON-RPC message with the optional authoritative Memory preview
/// tools enabled for a startup-authenticated principal session.
pub async fn handle_request_with_memory<I, S>(
    service: &AkiDbService<I, S>,
    memory: Option<&AuthoritativeMemoryMcp<S>>,
    raw: &str,
) -> Option<String>
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
        "tools/list" => Some(ok_response(
            id,
            json!({ "tools": tool_definitions(memory.is_some()) }),
        )),
        "tools/call" => {
            let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(Value::Null);
            match call_tool(service, memory, name, &args).await {
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

fn tool_definitions(authoritative_memory: bool) -> Value {
    let mut tools = vec![
        json!(
        {
            "name": "search",
            "description": "Hybrid (dense + lexical) retrieval; returns ranked ids and scores.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "top_k": { "type": "integer" },
                    "hybrid": { "type": "boolean" },
                    "workspace": { "type": "string", "description": "Workspace ACL scope (workspace_id)." },
                    "workspace_id": { "type": "string" }
                },
                "required": ["query"]
            }
        }),
        json!(
        {
            "name": "pack",
            "description": "Retrieve and assemble a source-grounded, cited context pack.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "top_k": { "type": "integer" },
                    "token_budget": { "type": "integer" },
                    "workspace": { "type": "string" },
                    "workspace_id": { "type": "string" }
                },
                "required": ["query"]
            }
        }),
        json!(
        {
            "name": "memory_write",
            "description": "LEGACY DOCUMENT MEMORY: store a metadata-backed entry in the vector collection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "kind": { "type": "string" },
                    "text": { "type": "string" },
                    "conversation_id": { "type": "string" },
                    "task_id": { "type": "string" },
                    "tool": { "type": "string" },
                    "source_uri": { "type": "string" },
                    "workspace": { "type": "string", "description": "Workspace ACL scope (workspace_id)." },
                    "workspace_id": { "type": "string" }
                },
                "required": ["id", "text"]
            }
        }),
        json!(
        {
            "name": "memory_read",
            "description": "LEGACY DOCUMENT MEMORY: retrieve metadata-backed entries from the vector collection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "conversation_id": { "type": "string" },
                    "top_k": { "type": "integer" },
                    "workspace": { "type": "string", "description": "Workspace ACL scope (workspace_id)." },
                    "workspace_id": { "type": "string" }
                },
                "required": ["query"]
            }
        }),
        json!(
        {
            "name": "status",
            "description": "Index statistics (active/total/tombstoned vectors, dimensions).",
            "inputSchema": { "type": "object", "properties": {} }
        }),
    ];
    if authoritative_memory {
        tools.extend([
            json!({
                "name": "memory_remember",
                "description": "EXPERIMENTAL AUTHORITATIVE MEMORY PREVIEW: commit one typed immutable text fact.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "entity_key": { "type": "string" },
                        "predicate": { "type": "string" },
                        "text": { "type": "string" },
                        "idempotency_key": { "type": "string" },
                        "reason": { "type": "string" },
                        "source_id": {
                            "type": "string",
                            "description": "Optional opaque source event ID; defaults to the idempotency key."
                        },
                        "namespace": { "type": "string" },
                        "request_purpose": { "type": "string" },
                        "delegated_agent_id": { "type": "string" },
                        "data_subject_id": { "type": "string" },
                        "session_id": { "type": "string" },
                        "task_id": { "type": "string" },
                        "sensitivity": {
                            "type": "string",
                            "enum": ["public", "internal", "confidential", "restricted"]
                        }
                    },
                    "required": ["entity_key", "predicate", "text", "idempotency_key", "reason"]
                }
            }),
            json!({
                "name": "memory_recall",
                "description": "EXPERIMENTAL AUTHORITATIVE MEMORY PREVIEW: bounded structured/BM25 recall with a retained snapshot.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string" },
                        "predicate": { "type": "string" },
                        "entity_key": { "type": "string" },
                        "namespace": { "type": "string" },
                        "request_purpose": { "type": "string" },
                        "delegated_agent_id": { "type": "string" },
                        "max_items": { "type": "integer" },
                        "max_context_tokens": { "type": "integer" }
                    }
                }
            }),
        ]);
    }
    Value::Array(tools)
}

async fn call_tool<I, S>(
    service: &AkiDbService<I, S>,
    memory: Option<&AuthoritativeMemoryMcp<S>>,
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
        "memory_remember" => {
            let session =
                memory.ok_or_else(|| "authoritative Memory preview is not enabled".to_string())?;
            tool_memory_remember(session, args).await
        }
        "memory_recall" => {
            let session =
                memory.ok_or_else(|| "authoritative Memory preview is not enabled".to_string())?;
            tool_memory_recall(session, args).await
        }
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
        score_threshold: None,
        group_by: String::new(),
        group_size: None,
        graph_max_depth: None,
        graph_per_seed_fanout: None,
        graph_max_expanded_nodes: None,
        include_diagnostics: false,
    }
}

fn arg_workspace(args: &Value) -> Result<Option<String>, String> {
    Ok(arg_str(args, "workspace")?.or(arg_str(args, "workspace_id")?))
}

async fn tool_search<I, S>(service: &AkiDbService<I, S>, args: &Value) -> Result<String, String>
where
    I: VectorIndex + 'static,
    S: StorageBackend + 'static,
{
    let query = required_str(args, "query")?;
    let top_k = arg_u32(args, "top_k", 10)?;
    let hybrid = arg_bool(args, "hybrid", true)?;
    let workspace = arg_workspace(args)?;
    let resp = service
        .text_search(request_with_workspace(
            text_search_request(query, top_k, hybrid, false, None),
            workspace.as_deref(),
        ))
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
    let workspace = arg_workspace(args)?;
    let resp = service
        .text_search(request_with_workspace(
            text_search_request(query, top_k, true, true, Some(budget)),
            workspace.as_deref(),
        ))
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    Ok(resp.context_pack)
}

fn request_with_workspace<T>(inner: T, workspace: Option<&str>) -> Request<T> {
    use crate::auth::AuthContext;
    let mut req = Request::new(inner);
    if let Some(ws) = workspace {
        if !ws.is_empty() {
            req.extensions_mut().insert(AuthContext {
                workspace_id: ws.to_string(),
                agent_id: None,
                authenticated: true,
            });
        }
    }
    req
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
    let workspace =
        arg_str(args, "workspace")?.or_else(|| arg_str(args, "workspace_id").ok().flatten());

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
    let mut meta_value = entry.to_metadata();
    if let Some(ws) = &workspace {
        if let serde_json::Value::Object(map) = &mut meta_value {
            map.insert("workspace_id".to_string(), json!(ws));
        }
    }
    let metadata = serde_json::to_vec(&meta_value).map_err(|e| e.to_string())?;
    service
        .insert(request_with_workspace(
            InsertRequest {
                collection: COLLECTION.to_string(),
                id: id.clone(),
                vector,
                metadata,
                text,
            },
            workspace.as_deref(),
        ))
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
    let workspace =
        arg_str(args, "workspace")?.or_else(|| arg_str(args, "workspace_id").ok().flatten());

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
        .search(request_with_workspace(
            SearchRequest {
                collection: COLLECTION.to_string(),
                query: vector,
                top_k,
                nprobe: None,
                filter: vec![],
                tag_filter,
                score_threshold: None,
                group_by: String::new(),
                group_size: None,
            },
            workspace.as_deref(),
        ))
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

fn authoritative_memory_selectors<S: StorageBackend>(
    session: &AuthoritativeMemoryMcp<S>,
    args: &Value,
) -> Result<(String, String, Option<String>), String> {
    let namespace = arg_str(args, "namespace")?.unwrap_or_else(|| session.namespace.clone());
    let request_purpose =
        arg_str(args, "request_purpose")?.unwrap_or_else(|| session.request_purpose.clone());
    let delegated_agent_id = match arg_str(args, "delegated_agent_id")? {
        Some(value) => Some(value),
        None => session.delegated_agent_id.clone(),
    };
    Ok((namespace, request_purpose, delegated_agent_id))
}

fn memory_sensitivity(args: &Value) -> Result<MemorySensitivity, String> {
    match arg_str(args, "sensitivity")?.as_deref() {
        None | Some("internal") => Ok(MemorySensitivity::Internal),
        Some("public") => Ok(MemorySensitivity::Public),
        Some("confidential") => Ok(MemorySensitivity::Confidential),
        Some("restricted") => Ok(MemorySensitivity::Restricted),
        Some(value) => Err(format!("unknown sensitivity: {value}")),
    }
}

async fn tool_memory_remember<S>(
    session: &AuthoritativeMemoryMcp<S>,
    args: &Value,
) -> Result<String, String>
where
    S: StorageBackend + 'static,
{
    let entity_key = required_str(args, "entity_key")?;
    let predicate = required_str(args, "predicate")?;
    let text = required_str(args, "text")?;
    let idempotency_key = required_str(args, "idempotency_key")?;
    let reason = required_str(args, "reason")?;
    let content_sha256 = sha256_hex(text.as_bytes());
    let source_id = arg_str(args, "source_id")?.unwrap_or_else(|| idempotency_key.clone());
    let (namespace, request_purpose, delegated_agent_id) =
        authoritative_memory_selectors(session, args)?;
    let request = MemoryRememberRequest {
        context: Some(session.context(
            namespace,
            request_purpose.clone(),
            delegated_agent_id.clone(),
            Some(idempotency_key),
        )),
        scope: Some(MemoryScopeInput {
            entity_key,
            data_subject_id: arg_str(args, "data_subject_id")?,
            owner_agent_id: delegated_agent_id,
            session_id: arg_str(args, "session_id")?,
            task_id: arg_str(args, "task_id")?,
            sensitivity: memory_sensitivity(args)? as i32,
            allowed_purposes: vec![request_purpose],
        }),
        predicate,
        content: Some(MemoryContent {
            value: Some(memory_content::Value::TextFact(MemoryTextFact {
                text,
                language: None,
            })),
        }),
        valid_from_ms: None,
        valid_to_ms: None,
        valid_from_unix_nanos: None,
        valid_to_unix_nanos: None,
        epistemic_formation: MemoryEpistemicFormation::MemoryFormationAgentStatement as i32,
        confidence: None,
        evidence: vec![MemoryEvidenceInput {
            source_plane: "mcp-tool".to_string(),
            source_id,
            source_version: None,
            observed_at_ms: None,
            observed_at_unix_nanos: None,
            content_sha256,
            source_principal_id: None,
        }],
        expected_head_version_ids: Vec::new(),
        reason,
        compiler_artifact_id: None,
        derivation: None,
    };
    let receipt = session
        .service
        .remember(session.authenticated_request(request))
        .await
        .map_err(|error| error.message().to_string())?
        .into_inner();
    Ok(json!({
        "profile": "authoritative_memory_developer_preview",
        "mutation_id": receipt.mutation_id,
        "assertion_id": receipt.assertion_id,
        "version_ids": receipt.version_ids,
        "commit_sequence": receipt.commit_sequence,
        "durability": receipt.durability,
        "projection_status": receipt.projection_status,
        "visible_sequence": receipt.visibility.as_ref().map(|value| value.visible_sequence),
        "duplicate": receipt.duplicate,
    })
    .to_string())
}

async fn tool_memory_recall<S>(
    session: &AuthoritativeMemoryMcp<S>,
    args: &Value,
) -> Result<String, String>
where
    S: StorageBackend + 'static,
{
    let query_text = arg_str(args, "query")?;
    let predicate = arg_str(args, "predicate")?;
    let entity_key = arg_str(args, "entity_key")?;
    if query_text.is_none() && predicate.is_none() && entity_key.is_none() {
        return Err("memory_recall requires 'query', 'predicate', or 'entity_key'".to_string());
    }
    let max_items = arg_u32(args, "max_items", 10)?;
    let max_context_tokens = match args.get("max_context_tokens") {
        Some(_) => Some(arg_u32(args, "max_context_tokens", 1_024)?),
        None => None,
    };
    let (namespace, request_purpose, delegated_agent_id) =
        authoritative_memory_selectors(session, args)?;
    let response = session
        .service
        .recall(session.authenticated_request(MemoryRecallRequest {
            context: Some(session.context(namespace, request_purpose, delegated_agent_id, None)),
            query_text,
            structured_predicates: predicate.into_iter().collect(),
            entity_keys: entity_key.into_iter().collect(),
            max_items,
            max_context_tokens,
            deterministic: true,
            include_explanation_summary: true,
            canonical_at_sequence: None,
            temporal_query: None,
            include_conflicts: false,
            recipe: None,
        }))
        .await
        .map_err(|error| error.message().to_string())?
        .into_inner();
    let items = response
        .items
        .iter()
        .map(|item| {
            json!({
                "assertion_id": item.assertion_id,
                "version_id": item.version_id,
                "namespace": item.namespace,
                "entity_key": item.entity_key,
                "predicate": item.predicate,
                "content": memory_content_json(item.content.as_ref()),
                "state": item.state,
                "source_assurance": item.source_assurance,
                "decision_authority": item.decision_authority,
                "score": item.score,
                "score_signals": item.score_signals,
                "reason": item.reason,
                "committed_sequence": item.committed_sequence,
                "evidence": item.evidence.iter().map(|evidence| json!({
                    "evidence_id": evidence.evidence_id,
                    "source_plane": evidence.source_plane,
                    "source_id": evidence.source_id,
                    "content_sha256": evidence.content_sha256,
                    "source_assurance": evidence.source_assurance,
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "profile": "authoritative_memory_developer_preview",
        "items": items,
        "rendered_context": response.rendered_context,
        "snapshot_id": response.snapshot_id,
        "commit_sequence": response.visibility.as_ref().map(|value| value.commit_sequence),
        "visible_sequence": response.visibility.as_ref().map(|value| value.visible_sequence),
        "partial_status": response.partial_status,
        "policy_decision_id": response.policy_decision_id,
    })
    .to_string())
}

fn memory_content_json(content: Option<&MemoryContent>) -> Value {
    match content.and_then(|content| content.value.as_ref()) {
        Some(memory_content::Value::TextFact(value)) => {
            json!({"kind": "text_fact", "text": value.text, "language": value.language})
        }
        Some(memory_content::Value::StructuredFact(value)) => json!({
            "kind": "structured_fact",
            "schema_id": value.schema_id,
            "canonical_json_utf8": String::from_utf8_lossy(&value.canonical_json),
        }),
        Some(memory_content::Value::Procedure(value)) => json!({
            "kind": "procedure",
            "title": value.title,
            "ordered_steps": value.ordered_steps,
            "preconditions": value.preconditions,
            "failure_recovery": value.failure_recovery,
        }),
        Some(memory_content::Value::Preference(value)) => json!({
            "kind": "preference",
            "value": value.value,
            "context": value.context,
        }),
        Some(memory_content::Value::EpisodeReference(value)) => json!({
            "kind": "episode_reference",
            "event_ids": value.event_ids,
            "summary": value.summary,
        }),
        None => Value::Null,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
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
    run_stdio_with_memory(service, None).await
}

/// Run MCP stdio with optional authoritative Memory preview tools.
pub async fn run_stdio_with_memory<I, S>(
    service: Arc<AkiDbService<I, S>>,
    memory: Option<Arc<AuthoritativeMemoryMcp<S>>>,
) -> std::io::Result<()>
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
        if let Some(resp) = handle_request_with_memory(&service, memory.as_deref(), &line).await {
            stdout.write_all(resp.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}
