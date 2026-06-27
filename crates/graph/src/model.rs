//! Graph data model.

use akidb_common::VectorId;
use serde::{Deserialize, Serialize};

/// Stable node identifier used in graph storage and query APIs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GraphNodeId(String);

impl GraphNodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert a conventional `chunk:<vector_id>` node to a vector id.
    pub fn as_chunk_vector_id(&self) -> Option<VectorId> {
        self.0.strip_prefix("chunk:").map(VectorId::new)
    }
}

impl std::fmt::Display for GraphNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for GraphNodeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for GraphNodeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Stable edge identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GraphEdgeId(String);

impl GraphEdgeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for GraphEdgeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for GraphEdgeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for GraphEdgeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Document,
    Chunk,
    Section,
    File,
    Function,
    Type,
    Module,
    Commit,
    Person,
    Entity,
    Memory,
}

impl NodeKind {
    pub fn as_key(self) -> &'static str {
        match self {
            NodeKind::Document => "document",
            NodeKind::Chunk => "chunk",
            NodeKind::Section => "section",
            NodeKind::File => "file",
            NodeKind::Function => "function",
            NodeKind::Type => "type",
            NodeKind::Module => "module",
            NodeKind::Commit => "commit",
            NodeKind::Person => "person",
            NodeKind::Entity => "entity",
            NodeKind::Memory => "memory",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    ParentOf,
    ChildOf,
    Contains,
    Mentions,
    Imports,
    Calls,
    Implements,
    Tests,
    TestedBy,
    DependsOn,
    OwnedBy,
    ChangedBy,
    RelatedTo,
}

impl EdgeKind {
    pub fn as_key(self) -> &'static str {
        match self {
            EdgeKind::ParentOf => "parent_of",
            EdgeKind::ChildOf => "child_of",
            EdgeKind::Contains => "contains",
            EdgeKind::Mentions => "mentions",
            EdgeKind::Imports => "imports",
            EdgeKind::Calls => "calls",
            EdgeKind::Implements => "implements",
            EdgeKind::Tests => "tests",
            EdgeKind::TestedBy => "tested_by",
            EdgeKind::DependsOn => "depends_on",
            EdgeKind::OwnedBy => "owned_by",
            EdgeKind::ChangedBy => "changed_by",
            EdgeKind::RelatedTo => "related_to",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: GraphNodeId,
    pub kind: NodeKind,
    pub properties: serde_json::Map<String, serde_json::Value>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl GraphNode {
    pub fn new(id: impl Into<GraphNodeId>, kind: NodeKind) -> Self {
        let now = current_timestamp_ms();
        Self {
            id: id.into(),
            kind,
            properties: serde_json::Map::new(),
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: GraphEdgeId,
    pub from: GraphNodeId,
    pub to: GraphNodeId,
    pub kind: EdgeKind,
    pub weight: f32,
    pub properties: serde_json::Map<String, serde_json::Value>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl GraphEdge {
    pub fn new(
        id: impl Into<GraphEdgeId>,
        from: impl Into<GraphNodeId>,
        to: impl Into<GraphNodeId>,
        kind: EdgeKind,
    ) -> Self {
        let now = current_timestamp_ms();
        Self {
            id: id.into(),
            from: from.into(),
            to: to.into(),
            kind,
            weight: 1.0,
            properties: serde_json::Map::new(),
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    pub fn with_weight(mut self, weight: f32) -> Self {
        self.weight = weight;
        self
    }

    pub fn with_property(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNeighbor {
    pub node: GraphNode,
    pub edge: GraphEdge,
    pub direction: DirectionOnEdge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectionOnEdge {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphPath {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelatedChunk {
    pub vector_id: VectorId,
    pub via_node: GraphNodeId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStats {
    pub nodes: u64,
    pub edges: u64,
    pub chunk_links: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteNodeResult {
    pub deleted: bool,
    pub edges_deleted: usize,
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
