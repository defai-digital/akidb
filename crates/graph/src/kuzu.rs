//! Experimental Kuzu graph adapter scaffold.
//!
//! Kuzu is intentionally not linked into the default AkiDB build. This module is
//! gated behind the `kuzu` feature so the public adapter boundary can compile
//! and be tested without making the Mac-first hot path depend on Kuzu's C++/FFI
//! packaging. Until the external Kuzu binding is wired, graph operations return
//! [`GraphError::Unavailable`] instead of silently dropping writes.

use crate::error::{GraphError, GraphResult};
use crate::model::{
    DeleteNodeResult, GraphEdge, GraphEdgeId, GraphNeighbor, GraphNode, GraphNodeId, GraphPath,
    GraphStats, RelatedChunk,
};
use crate::query::{GraphIndex, NeighborRequest, PathExistsRequest, TwoHopRequest};
use std::path::{Path, PathBuf};

/// Feature-gated Kuzu adapter descriptor.
#[derive(Debug, Clone)]
pub struct KuzuGraphAdapter {
    path: PathBuf,
}

impl KuzuGraphAdapter {
    /// Create a Kuzu adapter descriptor for a database path.
    ///
    /// This does not open a Kuzu database yet. The real binding will be attached
    /// only after the Kuzu evaluation gates pass.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Kuzu database path configured for the adapter.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Cypher DDL expected by the future adapter implementation.
    pub fn schema_cypher() -> &'static str {
        KUZU_SCHEMA_CYPHER
    }

    fn unavailable(&self) -> GraphError {
        GraphError::Unavailable(format!(
            "Kuzu adapter is experimental; external Kuzu binding is not linked for '{}'",
            self.path.display()
        ))
    }
}

impl GraphIndex for KuzuGraphAdapter {
    fn upsert_node(&self, _node: GraphNode) -> GraphResult<()> {
        Err(self.unavailable())
    }

    fn upsert_edge(&self, _edge: GraphEdge) -> GraphResult<()> {
        Err(self.unavailable())
    }

    fn get_node(&self, _node_id: &GraphNodeId) -> GraphResult<Option<GraphNode>> {
        Err(self.unavailable())
    }

    fn get_edge(&self, _edge_id: &GraphEdgeId) -> GraphResult<Option<GraphEdge>> {
        Err(self.unavailable())
    }

    fn delete_node(&self, _node_id: &GraphNodeId) -> GraphResult<DeleteNodeResult> {
        Err(self.unavailable())
    }

    fn delete_edge(&self, _edge_id: &GraphEdgeId) -> GraphResult<bool> {
        Err(self.unavailable())
    }

    fn neighbors(&self, _request: NeighborRequest) -> GraphResult<Vec<GraphNeighbor>> {
        Err(self.unavailable())
    }

    fn two_hop(&self, _request: TwoHopRequest) -> GraphResult<Vec<GraphPath>> {
        Err(self.unavailable())
    }

    fn related_chunks(
        &self,
        _entity_id: &GraphNodeId,
        _limit: usize,
    ) -> GraphResult<Vec<RelatedChunk>> {
        Err(self.unavailable())
    }

    fn path_exists(&self, _request: PathExistsRequest) -> GraphResult<bool> {
        Err(self.unavailable())
    }

    fn stats(&self) -> GraphResult<GraphStats> {
        Err(self.unavailable())
    }
}

/// Initial Kuzu schema used by the optional adapter evaluation.
pub const KUZU_SCHEMA_CYPHER: &str = r#"
CREATE NODE TABLE IF NOT EXISTS GraphNode(
    id STRING,
    kind STRING,
    properties STRING,
    created_at_ms INT64,
    updated_at_ms INT64,
    PRIMARY KEY(id)
);

CREATE REL TABLE IF NOT EXISTS GraphEdge(
    FROM GraphNode TO GraphNode,
    id STRING,
    kind STRING,
    weight DOUBLE,
    properties STRING,
    created_at_ms INT64,
    updated_at_ms INT64
);
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GraphIndex, GraphNode, GraphNodeId, NodeKind};

    #[test]
    fn test_kuzu_schema_names_expected_tables() {
        let schema = KuzuGraphAdapter::schema_cypher();
        assert!(schema.contains("CREATE NODE TABLE IF NOT EXISTS GraphNode"));
        assert!(schema.contains("CREATE REL TABLE IF NOT EXISTS GraphEdge"));
        assert!(schema.contains("PRIMARY KEY(id)"));
    }

    #[test]
    fn test_kuzu_adapter_reports_unavailable_until_binding_is_linked() {
        let adapter = KuzuGraphAdapter::new("/tmp/akidb-kuzu");
        assert_eq!(adapter.path(), Path::new("/tmp/akidb-kuzu"));

        let err = adapter
            .upsert_node(GraphNode::new(GraphNodeId::new("chunk:a"), NodeKind::Chunk))
            .unwrap_err();
        assert!(matches!(err, GraphError::Unavailable(_)));
    }
}
