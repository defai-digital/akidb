//! Agent memory schema (§3.6).
//!
//! Durable memory for local agents: conversations, tasks, tool calls, sources,
//! and results. The design decision here (the PRD leaves the canonical schema
//! open) is to model memory as ordinary retrieval documents: a [`MemoryEntry`]
//! carries the text used for embedding/lexical indexing plus structured links
//! (conversation, task, tool, source) that serialize into the same metadata JSON
//! the search path already filters on.
//!
//! Consequences:
//! - **MEM-001**: the schema links conversation → task → tool call → source →
//!   result via metadata fields.
//! - **MEM-002**: memory is retrievable by the *same* hybrid + filter pipeline as
//!   documents — no separate index. Filter by `conversation_id`/`task_id`/`kind`
//!   with the existing tag filter.
//! - **MEM-004**: multiple agents share one store safely because entries are just
//!   namespaced documents; scoping is by metadata, not by separate stores.
//!
//! Timestamps are supplied by the caller (the library takes no clock), keeping it
//! deterministic and testable.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The role an entry plays in the agent's memory graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryKind {
    Conversation,
    Task,
    ToolCall,
    Source,
    Result,
    Note,
}

impl MemoryKind {
    /// Stable string form used in metadata (and filters).
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryKind::Conversation => "conversation",
            MemoryKind::Task => "task",
            MemoryKind::ToolCall => "tool_call",
            MemoryKind::Source => "source",
            MemoryKind::Result => "result",
            MemoryKind::Note => "note",
        }
    }

    /// Parse from the stable string form.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "conversation" => MemoryKind::Conversation,
            "task" => MemoryKind::Task,
            "tool_call" => MemoryKind::ToolCall,
            "source" => MemoryKind::Source,
            "result" => MemoryKind::Result,
            "note" => MemoryKind::Note,
            _ => return None,
        })
    }
}

/// Reserved top-level metadata keys owned by the memory schema. Custom `tags`
/// using these names are dropped on serialization to avoid clobbering the schema.
pub const RESERVED_KEYS: &[&str] = &[
    "memory_kind",
    "conversation_id",
    "task_id",
    "tool",
    "source_uri",
    "timestamp",
];

/// A single agent-memory record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEntry {
    pub id: String,
    pub kind: MemoryKind,
    /// Content used for embedding + lexical retrieval.
    pub text: String,
    pub conversation_id: Option<String>,
    pub task_id: Option<String>,
    pub tool: Option<String>,
    pub source_uri: Option<String>,
    /// Caller-supplied epoch milliseconds (the library has no clock).
    pub timestamp: Option<i64>,
    /// Free-form additional tags (must not use [`RESERVED_KEYS`]).
    pub tags: BTreeMap<String, String>,
}

impl MemoryEntry {
    pub fn new(id: impl Into<String>, kind: MemoryKind, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind,
            text: text.into(),
            conversation_id: None,
            task_id: None,
            tool: None,
            source_uri: None,
            timestamp: None,
            tags: BTreeMap::new(),
        }
    }

    pub fn with_conversation(mut self, id: impl Into<String>) -> Self {
        self.conversation_id = Some(id.into());
        self
    }

    pub fn with_task(mut self, id: impl Into<String>) -> Self {
        self.task_id = Some(id.into());
        self
    }

    pub fn with_tool(mut self, tool: impl Into<String>) -> Self {
        self.tool = Some(tool.into());
        self
    }

    pub fn with_source(mut self, uri: impl Into<String>) -> Self {
        self.source_uri = Some(uri.into());
        self
    }

    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// The text indexed for retrieval (embedding + BM25).
    pub fn retrieval_text(&self) -> &str {
        &self.text
    }

    /// Serialize the entry's links into a metadata JSON object suitable for the
    /// insert path and the tag filter. `None` fields are omitted; reserved keys
    /// take precedence over custom tags.
    pub fn to_metadata(&self) -> Value {
        let mut map = Map::new();
        map.insert("memory_kind".into(), Value::String(self.kind.as_str().into()));
        if let Some(v) = &self.conversation_id {
            map.insert("conversation_id".into(), Value::String(v.clone()));
        }
        if let Some(v) = &self.task_id {
            map.insert("task_id".into(), Value::String(v.clone()));
        }
        if let Some(v) = &self.tool {
            map.insert("tool".into(), Value::String(v.clone()));
        }
        if let Some(v) = &self.source_uri {
            map.insert("source_uri".into(), Value::String(v.clone()));
        }
        if let Some(ts) = self.timestamp {
            map.insert("timestamp".into(), Value::Number(ts.into()));
        }
        for (k, v) in &self.tags {
            if RESERVED_KEYS.contains(&k.as_str()) {
                continue; // never let a tag clobber a schema field
            }
            map.insert(k.clone(), Value::String(v.clone()));
        }
        Value::Object(map)
    }

    /// Reconstruct an entry from a retrieval result: its id, text, and the stored
    /// metadata JSON. Unknown/extra metadata keys become tags. Returns `None` if
    /// the metadata lacks a recognizable `memory_kind`.
    pub fn from_metadata(id: impl Into<String>, text: impl Into<String>, metadata: &Value) -> Option<Self> {
        let obj = metadata.as_object()?;
        let kind = obj
            .get("memory_kind")
            .and_then(|v| v.as_str())
            .and_then(MemoryKind::parse)?;

        let str_field = |key: &str| obj.get(key).and_then(|v| v.as_str()).map(String::from);

        let mut tags = BTreeMap::new();
        for (k, v) in obj {
            if RESERVED_KEYS.contains(&k.as_str()) {
                continue;
            }
            if let Some(s) = v.as_str() {
                tags.insert(k.clone(), s.to_string());
            }
        }

        Some(Self {
            id: id.into(),
            kind,
            text: text.into(),
            conversation_id: str_field("conversation_id"),
            task_id: str_field("task_id"),
            tool: str_field("tool"),
            source_uri: str_field("source_uri"),
            timestamp: obj.get("timestamp").and_then(|v| v.as_i64()),
            tags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_kind_string_roundtrip() {
        for k in [
            MemoryKind::Conversation,
            MemoryKind::Task,
            MemoryKind::ToolCall,
            MemoryKind::Source,
            MemoryKind::Result,
            MemoryKind::Note,
        ] {
            assert_eq!(MemoryKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(MemoryKind::parse("bogus"), None);
    }

    #[test]
    fn test_to_metadata_includes_set_fields_and_omits_none() {
        let entry = MemoryEntry::new("m1", MemoryKind::ToolCall, "ran grep")
            .with_conversation("c1")
            .with_task("t1")
            .with_tool("grep")
            .with_timestamp(1_700_000_000_000);
        let meta = entry.to_metadata();
        assert_eq!(meta["memory_kind"], json!("tool_call"));
        assert_eq!(meta["conversation_id"], json!("c1"));
        assert_eq!(meta["task_id"], json!("t1"));
        assert_eq!(meta["tool"], json!("grep"));
        assert_eq!(meta["timestamp"], json!(1_700_000_000_000i64));
        // source_uri was never set -> omitted
        assert!(meta.get("source_uri").is_none());
    }

    #[test]
    fn test_metadata_roundtrip() {
        let entry = MemoryEntry::new("m2", MemoryKind::Result, "the result text")
            .with_conversation("conv")
            .with_source("file://x")
            .with_tag("importance", "high");
        let meta = entry.to_metadata();
        let restored = MemoryEntry::from_metadata("m2", "the result text", &meta).unwrap();
        assert_eq!(restored, entry);
    }

    #[test]
    fn test_custom_tags_preserved_and_reserved_protected() {
        let entry = MemoryEntry::new("m3", MemoryKind::Note, "note")
            .with_tag("project", "akidb")
            // Attempt to clobber a reserved key via a tag — must be ignored.
            .with_tag("conversation_id", "HACK");
        let meta = entry.to_metadata();
        assert_eq!(meta["project"], json!("akidb"));
        // conversation_id was None on the entry and the tag must not have set it.
        assert!(meta.get("conversation_id").is_none());
    }

    #[test]
    fn test_from_metadata_requires_kind() {
        let meta = json!({"conversation_id": "c1"}); // no memory_kind
        assert!(MemoryEntry::from_metadata("x", "t", &meta).is_none());
    }

    #[test]
    fn test_from_metadata_collects_unknown_keys_as_tags() {
        let meta = json!({"memory_kind": "note", "project": "akidb", "topic": "search"});
        let entry = MemoryEntry::from_metadata("m", "body", &meta).unwrap();
        assert_eq!(entry.kind, MemoryKind::Note);
        assert_eq!(entry.tags.get("project"), Some(&"akidb".to_string()));
        assert_eq!(entry.tags.get("topic"), Some(&"search".to_string()));
    }
}
