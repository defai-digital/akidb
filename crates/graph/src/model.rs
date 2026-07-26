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

    /// Build a workspace-scoped graph id.
    ///
    /// The length-prefixed workspace component avoids delimiter ambiguity while
    /// keeping the local id human-readable for graph inspection.
    pub fn scoped(workspace_id: &str, local_id: &str) -> Self {
        Self(format!(
            "workspace:{}:{}:{}",
            workspace_id.len(),
            workspace_id,
            local_id
        ))
    }

    /// Return the workspace component of a scoped id.
    pub fn workspace_id(&self) -> Option<&str> {
        self.scoped_parts().map(|(workspace_id, _)| workspace_id)
    }

    /// Return the local component of a scoped id, or the complete id when it is
    /// not workspace-scoped.
    pub fn local_id(&self) -> &str {
        self.scoped_parts()
            .map(|(_, local_id)| local_id)
            .unwrap_or(&self.0)
    }

    /// Whether two node ids belong to the same graph workspace namespace.
    ///
    /// Legacy unscoped ids form the default namespace and never match an
    /// explicitly scoped id.
    pub fn is_same_workspace(&self, other: &Self) -> bool {
        self.workspace_id() == other.workspace_id()
    }

    /// Convert a conventional `chunk:<vector_id>` node, scoped or unscoped, to
    /// a vector id.
    pub fn as_chunk_vector_id(&self) -> Option<VectorId> {
        self.local_id().strip_prefix("chunk:").map(VectorId::new)
    }

    fn scoped_parts(&self) -> Option<(&str, &str)> {
        let rest = self.0.strip_prefix("workspace:")?;
        let (workspace_len, rest) = rest.split_once(':')?;
        let workspace_len = workspace_len.parse::<usize>().ok()?;
        if rest.len() <= workspace_len
            || !rest.is_char_boundary(workspace_len)
            || rest.as_bytes().get(workspace_len) != Some(&b':')
        {
            return None;
        }
        Some((&rest[..workspace_len], &rest[workspace_len + 1..]))
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

/// A bounded graph expansion result with the exact edge path that justified it.
///
/// The serving layer uses this trace to expose hop decay and source evidence
/// without turning the graph index into an arbitrary query surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelatedChunkTrace {
    pub vector_id: VectorId,
    pub via_node: GraphNodeId,
    pub hop: u8,
    pub path_edges: Vec<GraphEdge>,
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

/// One atomic graph projection unit.
#[derive(Debug, Clone, Default)]
pub struct GraphMutationBatch {
    pub delete_edges: Vec<GraphEdgeId>,
    pub replace_nodes: Vec<GraphNodeId>,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

impl GraphMutationBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_node(mut self, node: GraphNode) -> Self {
        self.nodes.push(node);
        self
    }

    /// Add an authoritative node value whose properties replace the stored
    /// property map. Other nodes in a batch use patch/merge semantics so
    /// relationship placeholders cannot erase richer node data.
    pub fn with_replaced_node(mut self, node: GraphNode) -> Self {
        self.replace_nodes.push(node.id.clone());
        self.nodes.push(node);
        self
    }

    pub fn with_deleted_edge(mut self, edge_id: impl Into<GraphEdgeId>) -> Self {
        self.delete_edges.push(edge_id.into());
        self
    }

    pub fn with_edge(mut self, edge: GraphEdge) -> Self {
        self.edges.push(edge);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.delete_edges.is_empty()
            && self.replace_nodes.is_empty()
            && self.nodes.is_empty()
            && self.edges.is_empty()
    }
}

pub(crate) fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_node_id_round_trips_workspace_and_chunk() {
        let id = GraphNodeId::scoped("客戶-a", "chunk:vector:1");

        assert_eq!(id.workspace_id(), Some("客戶-a"));
        assert_eq!(id.local_id(), "chunk:vector:1");
        assert_eq!(
            id.as_chunk_vector_id().as_ref().map(VectorId::as_str),
            Some("vector:1")
        );
    }

    #[test]
    fn malformed_scoped_node_id_remains_an_unscoped_id() {
        let id = GraphNodeId::new("workspace:99:short:chunk:v1");

        assert_eq!(id.workspace_id(), None);
        assert_eq!(id.local_id(), id.as_str());
        assert_eq!(id.as_chunk_vector_id(), None);
    }

    #[test]
    fn scoped_node_ids_only_match_the_same_workspace() {
        let a = GraphNodeId::scoped("a", "chunk:1");
        let same = GraphNodeId::scoped("a", "entity:1");
        let b = GraphNodeId::scoped("b", "chunk:1");
        let legacy = GraphNodeId::new("chunk:1");

        assert!(a.is_same_workspace(&same));
        assert!(!a.is_same_workspace(&b));
        assert!(!a.is_same_workspace(&legacy));
        assert!(legacy.is_same_workspace(&GraphNodeId::new("entity:1")));
    }
}
