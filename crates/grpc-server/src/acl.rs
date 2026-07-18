//! Workspace ACL helpers (ADR-0002.2 / GAP-002).
//!
//! Writes stamp `workspace_id` (and optional `agent_id`) into JSON metadata.
//! Reads AND a workspace equality condition into the request filter when
//! enforcement is enabled.

use serde_json::{json, Map, Value};

use crate::auth::AuthContext;
use crate::filter::MetadataFilter;
use akidb_common::config::AclConfig;

pub const WORKSPACE_KEY: &str = "workspace_id";
pub const AGENT_KEY: &str = "agent_id";
pub const EMBEDDING_MODEL_KEY: &str = "embedding_model_id";

/// Stamp ACL + optional embedding model fields onto metadata bytes.
pub fn stamp_write_metadata(
    metadata: &[u8],
    ctx: &AuthContext,
    acl: &AclConfig,
    embedding_model_id: Option<&str>,
) -> Result<Vec<u8>, String> {
    let mut map = if metadata.is_empty() {
        Map::new()
    } else {
        let value: Value = serde_json::from_slice(metadata)
            .map_err(|e| format!("metadata is not valid JSON: {e}"))?;
        match value {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            _ => return Err("metadata must be a JSON object".to_string()),
        }
    };

    if let Some(existing) = map.get(WORKSPACE_KEY).and_then(|v| v.as_str()) {
        if existing != ctx.workspace_id && acl.enforce_workspace {
            return Err(format!(
                "workspace_id '{existing}' does not match caller workspace '{}'",
                ctx.workspace_id
            ));
        }
    } else {
        map.insert(WORKSPACE_KEY.to_string(), json!(ctx.workspace_id));
    }

    if let Some(agent) = &ctx.agent_id {
        map.entry(AGENT_KEY.to_string())
            .or_insert_with(|| json!(agent));
    }

    if let Some(model) = embedding_model_id {
        map.entry(EMBEDDING_MODEL_KEY.to_string())
            .or_insert_with(|| json!(model));
    }

    serde_json::to_vec(&Value::Object(map)).map_err(|e| e.to_string())
}

/// Merge a workspace scope into an optional metadata filter.
///
/// Missing/`null` `workspace_id` on stored vectors is treated as `"default"` so
/// pre-ACL data remains readable in the default workspace.
pub fn apply_workspace_scope(
    existing: Option<MetadataFilter>,
    ctx: &AuthContext,
    acl: &AclConfig,
) -> Result<Option<MetadataFilter>, String> {
    if !acl.enforce_workspace {
        return Ok(existing);
    }

    let scope = MetadataFilter::workspace_scope(ctx.workspace_id.clone());
    Ok(Some(match existing {
        None => scope,
        Some(filter) => MetadataFilter::and(filter, scope),
    }))
}

/// Whether stored metadata belongs to the caller's workspace.
pub fn metadata_in_workspace(metadata: &Value, workspace_id: &str) -> bool {
    match metadata.get(WORKSPACE_KEY) {
        // Legacy vectors without workspace are treated as default workspace.
        None | Some(Value::Null) => workspace_id == "default",
        Some(Value::String(ws)) => ws == workspace_id,
        Some(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthContext;

    fn ctx(ws: &str) -> AuthContext {
        AuthContext {
            workspace_id: ws.to_string(),
            agent_id: Some("agent-1".to_string()),
            authenticated: true,
        }
    }

    #[test]
    fn stamps_workspace_and_model() {
        let acl = AclConfig::default();
        let out = stamp_write_metadata(b"{}", &ctx("ws-a"), &acl, Some("model-x")).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v[WORKSPACE_KEY], "ws-a");
        assert_eq!(v[AGENT_KEY], "agent-1");
        assert_eq!(v[EMBEDDING_MODEL_KEY], "model-x");
    }

    #[test]
    fn rejects_cross_workspace_stamp() {
        let acl = AclConfig::default();
        let err = stamp_write_metadata(br#"{"workspace_id":"other"}"#, &ctx("ws-a"), &acl, None)
            .unwrap_err();
        assert!(err.contains("does not match"));
    }

    #[test]
    fn legacy_metadata_default_workspace() {
        assert!(metadata_in_workspace(&json!({}), "default"));
        assert!(!metadata_in_workspace(&json!({}), "other"));
        assert!(metadata_in_workspace(
            &json!({"workspace_id": "team"}),
            "team"
        ));
    }
}
