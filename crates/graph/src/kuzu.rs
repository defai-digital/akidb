//! Experimental Kuzu graph adapter.
//!
//! Kuzu is intentionally not linked into the default AkiDB build. This module is
//! gated behind the `kuzu` feature so the public adapter boundary can compile
//! and be tested without making the Mac-first hot path depend on Kuzu's C++/FFI
//! packaging.

use crate::error::{GraphError, GraphResult};
use crate::model::{
    DeleteNodeResult, Direction, DirectionOnEdge, EdgeKind, GraphEdge, GraphEdgeId, GraphNeighbor,
    GraphNode, GraphNodeId, GraphPath, GraphStats, NodeKind, RelatedChunk,
};
use crate::query::{GraphIndex, NeighborRequest, PathExistsRequest, TwoHopRequest};
use kuzu_db::{Connection, Database, SystemConfig, Value};
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Feature-gated Kuzu adapter.
#[derive(Debug)]
pub struct KuzuGraphAdapter {
    path: PathBuf,
    database: Database,
    write_lock: Mutex<()>,
}

impl KuzuGraphAdapter {
    /// Open or create a Kuzu database and initialize AkiDB's graph schema.
    pub fn new(path: impl Into<PathBuf>) -> GraphResult<Self> {
        let path = path.into();
        let database = Database::new(&path, SystemConfig::default()).map_err(Self::kuzu_error)?;
        let adapter = Self {
            path,
            database,
            write_lock: Mutex::new(()),
        };
        adapter.initialize_schema()?;
        Ok(adapter)
    }

    /// Open an in-memory Kuzu graph adapter. Intended for tests and benchmark probes.
    pub fn in_memory() -> GraphResult<Self> {
        let database = Database::in_memory(SystemConfig::default()).map_err(Self::kuzu_error)?;
        let adapter = Self {
            path: PathBuf::from(":memory:"),
            database,
            write_lock: Mutex::new(()),
        };
        adapter.initialize_schema()?;
        Ok(adapter)
    }

    /// Kuzu database path configured for the adapter.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Cypher DDL expected by the future adapter implementation.
    pub fn schema_cypher() -> &'static str {
        KUZU_SCHEMA_CYPHER
    }

    fn connection(&self) -> GraphResult<Connection<'_>> {
        Connection::new(&self.database).map_err(Self::kuzu_error)
    }

    fn initialize_schema(&self) -> GraphResult<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| GraphError::Storage("Kuzu graph write lock is poisoned".to_string()))?;
        let conn = self.connection()?;
        conn.query(KUZU_SCHEMA_CYPHER).map_err(Self::kuzu_error)?;
        Ok(())
    }

    fn kuzu_error(error: kuzu_db::Error) -> GraphError {
        GraphError::Storage(format!("Kuzu error: {error}"))
    }

    fn serde_to_string(
        properties: &serde_json::Map<String, serde_json::Value>,
    ) -> GraphResult<String> {
        Ok(serde_json::to_string(properties)?)
    }

    fn serde_from_string(raw: &str) -> GraphResult<serde_json::Map<String, serde_json::Value>> {
        Ok(serde_json::from_str(raw)?)
    }

    fn value_string(row: &[Value], index: usize, field: &str) -> GraphResult<String> {
        match row.get(index) {
            Some(Value::String(value)) => Ok(value.clone()),
            other => Err(GraphError::Serialization(format!(
                "Kuzu field {field} expected string, got {other:?}"
            ))),
        }
    }

    fn value_i64(row: &[Value], index: usize, field: &str) -> GraphResult<i64> {
        match row.get(index) {
            Some(Value::Int64(value)) => Ok(*value),
            Some(Value::Int32(value)) => Ok(i64::from(*value)),
            other => Err(GraphError::Serialization(format!(
                "Kuzu field {field} expected integer, got {other:?}"
            ))),
        }
    }

    fn value_f32(row: &[Value], index: usize, field: &str) -> GraphResult<f32> {
        match row.get(index) {
            Some(Value::Double(value)) => Ok(*value as f32),
            Some(Value::Float(value)) => Ok(*value),
            other => Err(GraphError::Serialization(format!(
                "Kuzu field {field} expected float, got {other:?}"
            ))),
        }
    }

    fn value_count(row: &[Value], index: usize, field: &str) -> GraphResult<u64> {
        match row.get(index) {
            Some(Value::Int64(value)) if *value >= 0 => Ok(*value as u64),
            Some(Value::UInt64(value)) => Ok(*value),
            Some(Value::Int32(value)) if *value >= 0 => Ok(*value as u64),
            other => Err(GraphError::Serialization(format!(
                "Kuzu field {field} expected count, got {other:?}"
            ))),
        }
    }

    fn node_kind_from_key(raw: &str) -> GraphResult<NodeKind> {
        match raw {
            "document" => Ok(NodeKind::Document),
            "chunk" => Ok(NodeKind::Chunk),
            "section" => Ok(NodeKind::Section),
            "file" => Ok(NodeKind::File),
            "function" => Ok(NodeKind::Function),
            "type" => Ok(NodeKind::Type),
            "module" => Ok(NodeKind::Module),
            "commit" => Ok(NodeKind::Commit),
            "person" => Ok(NodeKind::Person),
            "entity" => Ok(NodeKind::Entity),
            "memory" => Ok(NodeKind::Memory),
            _ => Err(GraphError::Serialization(format!(
                "unknown Kuzu node kind: {raw}"
            ))),
        }
    }

    fn edge_kind_from_key(raw: &str) -> GraphResult<EdgeKind> {
        match raw {
            "parent_of" => Ok(EdgeKind::ParentOf),
            "child_of" => Ok(EdgeKind::ChildOf),
            "contains" => Ok(EdgeKind::Contains),
            "mentions" => Ok(EdgeKind::Mentions),
            "imports" => Ok(EdgeKind::Imports),
            "calls" => Ok(EdgeKind::Calls),
            "implements" => Ok(EdgeKind::Implements),
            "tests" => Ok(EdgeKind::Tests),
            "tested_by" => Ok(EdgeKind::TestedBy),
            "depends_on" => Ok(EdgeKind::DependsOn),
            "owned_by" => Ok(EdgeKind::OwnedBy),
            "changed_by" => Ok(EdgeKind::ChangedBy),
            "related_to" => Ok(EdgeKind::RelatedTo),
            _ => Err(GraphError::Serialization(format!(
                "unknown Kuzu edge kind: {raw}"
            ))),
        }
    }

    fn row_to_node(row: &[Value], offset: usize) -> GraphResult<GraphNode> {
        let id = GraphNodeId::new(Self::value_string(row, offset, "node.id")?);
        let kind = Self::node_kind_from_key(&Self::value_string(row, offset + 1, "node.kind")?)?;
        let properties =
            Self::serde_from_string(&Self::value_string(row, offset + 2, "node.properties")?)?;
        let created_at_ms = Self::value_i64(row, offset + 3, "node.created_at_ms")? as u64;
        let updated_at_ms = Self::value_i64(row, offset + 4, "node.updated_at_ms")? as u64;
        Ok(GraphNode {
            id,
            kind,
            properties,
            created_at_ms,
            updated_at_ms,
        })
    }

    fn row_to_edge(row: &[Value], offset: usize) -> GraphResult<GraphEdge> {
        let id = GraphEdgeId::new(Self::value_string(row, offset, "edge.id")?);
        let from = GraphNodeId::new(Self::value_string(row, offset + 1, "edge.from")?);
        let to = GraphNodeId::new(Self::value_string(row, offset + 2, "edge.to")?);
        let kind = Self::edge_kind_from_key(&Self::value_string(row, offset + 3, "edge.kind")?)?;
        let weight = Self::value_f32(row, offset + 4, "edge.weight")?;
        let properties =
            Self::serde_from_string(&Self::value_string(row, offset + 5, "edge.properties")?)?;
        let created_at_ms = Self::value_i64(row, offset + 6, "edge.created_at_ms")? as u64;
        let updated_at_ms = Self::value_i64(row, offset + 7, "edge.updated_at_ms")? as u64;
        Ok(GraphEdge {
            id,
            from,
            to,
            kind,
            weight,
            properties,
            created_at_ms,
            updated_at_ms,
        })
    }

    fn edge_kind_predicate(kinds: &[EdgeKind]) -> String {
        if kinds.is_empty() {
            String::new()
        } else {
            let clauses = kinds
                .iter()
                .map(|kind| format!("r.kind = '{}'", kind.as_key()))
                .collect::<Vec<_>>()
                .join(" OR ");
            format!(" AND ({clauses})")
        }
    }

    fn min_weight_predicate(min_weight: Option<f32>) -> String {
        min_weight
            .map(|value| format!(" AND r.weight >= {}", value as f64))
            .unwrap_or_default()
    }

    fn ensure_placeholder_node(&self, node_id: &GraphNodeId) -> GraphResult<()> {
        if self.get_node(node_id)?.is_none() {
            self.upsert_node(GraphNode::new(node_id.clone(), NodeKind::Entity))?;
        }
        Ok(())
    }

    fn should_link_chunk(edge: &GraphEdge) -> bool {
        edge.to.as_chunk_vector_id().is_some() && Self::is_chunk_expansion_kind(edge.kind)
    }

    fn is_chunk_expansion_kind(kind: EdgeKind) -> bool {
        matches!(
            kind,
            EdgeKind::Calls
                | EdgeKind::Contains
                | EdgeKind::DependsOn
                | EdgeKind::Imports
                | EdgeKind::Mentions
                | EdgeKind::ParentOf
                | EdgeKind::RelatedTo
                | EdgeKind::TestedBy
                | EdgeKind::Tests
        )
    }

    fn query_count(&self, query: &str) -> GraphResult<u64> {
        let conn = self.connection()?;
        let mut result = conn.query(query).map_err(Self::kuzu_error)?;
        let Some(row) = result.next() else {
            return Ok(0);
        };
        Self::value_count(&row, 0, "count")
    }
}

impl GraphIndex for KuzuGraphAdapter {
    fn upsert_node(&self, node: GraphNode) -> GraphResult<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| GraphError::Storage("Kuzu graph write lock is poisoned".to_string()))?;
        let conn = self.connection()?;
        let mut statement = conn
            .prepare(
                "MERGE (n:GraphNode {id: $id}) \
                 SET n.kind = $kind, n.properties = $properties, \
                     n.created_at_ms = $created_at_ms, n.updated_at_ms = $updated_at_ms;",
            )
            .map_err(Self::kuzu_error)?;
        conn.execute(
            &mut statement,
            vec![
                ("id", Value::String(node.id.to_string())),
                ("kind", Value::String(node.kind.as_key().to_string())),
                (
                    "properties",
                    Value::String(Self::serde_to_string(&node.properties)?),
                ),
                ("created_at_ms", Value::Int64(node.created_at_ms as i64)),
                ("updated_at_ms", Value::Int64(node.updated_at_ms as i64)),
            ],
        )
        .map_err(Self::kuzu_error)?;
        Ok(())
    }

    fn upsert_edge(&self, edge: GraphEdge) -> GraphResult<()> {
        if !edge.weight.is_finite() {
            return Err(GraphError::InvalidRequest(format!(
                "edge {} has non-finite weight",
                edge.id
            )));
        }

        self.ensure_placeholder_node(&edge.from)?;
        self.ensure_placeholder_node(&edge.to)?;

        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| GraphError::Storage("Kuzu graph write lock is poisoned".to_string()))?;
        let conn = self.connection()?;

        let mut delete_statement = conn
            .prepare("MATCH ()-[r:GraphEdge]->() WHERE r.id = $id DELETE r;")
            .map_err(Self::kuzu_error)?;
        conn.execute(
            &mut delete_statement,
            vec![("id", Value::String(edge.id.to_string()))],
        )
        .map_err(Self::kuzu_error)?;

        let mut create_statement = conn
            .prepare(
                "MATCH (a:GraphNode), (b:GraphNode) \
                 WHERE a.id = $from AND b.id = $to \
                 CREATE (a)-[:GraphEdge {id: $id, kind: $kind, weight: $weight, \
                     properties: $properties, created_at_ms: $created_at_ms, \
                     updated_at_ms: $updated_at_ms}]->(b);",
            )
            .map_err(Self::kuzu_error)?;
        conn.execute(
            &mut create_statement,
            vec![
                ("from", Value::String(edge.from.to_string())),
                ("to", Value::String(edge.to.to_string())),
                ("id", Value::String(edge.id.to_string())),
                ("kind", Value::String(edge.kind.as_key().to_string())),
                ("weight", Value::Double(f64::from(edge.weight))),
                (
                    "properties",
                    Value::String(Self::serde_to_string(&edge.properties)?),
                ),
                ("created_at_ms", Value::Int64(edge.created_at_ms as i64)),
                ("updated_at_ms", Value::Int64(edge.updated_at_ms as i64)),
            ],
        )
        .map_err(Self::kuzu_error)?;
        Ok(())
    }

    fn get_node(&self, node_id: &GraphNodeId) -> GraphResult<Option<GraphNode>> {
        let conn = self.connection()?;
        let mut statement = conn
            .prepare(
                "MATCH (n:GraphNode) WHERE n.id = $id \
                 RETURN n.id, n.kind, n.properties, n.created_at_ms, n.updated_at_ms \
                 LIMIT 1;",
            )
            .map_err(Self::kuzu_error)?;
        let mut result = conn
            .execute(
                &mut statement,
                vec![("id", Value::String(node_id.to_string()))],
            )
            .map_err(Self::kuzu_error)?;
        result
            .next()
            .map(|row| Self::row_to_node(&row, 0))
            .transpose()
    }

    fn get_edge(&self, edge_id: &GraphEdgeId) -> GraphResult<Option<GraphEdge>> {
        let conn = self.connection()?;
        let mut statement = conn
            .prepare(
                "MATCH (a:GraphNode)-[r:GraphEdge]->(b:GraphNode) WHERE r.id = $id \
                 RETURN r.id, a.id, b.id, r.kind, r.weight, r.properties, \
                        r.created_at_ms, r.updated_at_ms \
                 LIMIT 1;",
            )
            .map_err(Self::kuzu_error)?;
        let mut result = conn
            .execute(
                &mut statement,
                vec![("id", Value::String(edge_id.to_string()))],
            )
            .map_err(Self::kuzu_error)?;
        result
            .next()
            .map(|row| Self::row_to_edge(&row, 0))
            .transpose()
    }

    fn delete_node(&self, node_id: &GraphNodeId) -> GraphResult<DeleteNodeResult> {
        let Some(_) = self.get_node(node_id)? else {
            return Ok(DeleteNodeResult::default());
        };

        let edges_deleted = self
            .neighbors(NeighborRequest::new(node_id.clone()).with_direction(Direction::Both))?
            .into_iter()
            .map(|neighbor| neighbor.edge.id)
            .collect::<HashSet<_>>()
            .len();

        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| GraphError::Storage("Kuzu graph write lock is poisoned".to_string()))?;
        let conn = self.connection()?;
        let mut statement = conn
            .prepare("MATCH (n:GraphNode) WHERE n.id = $id DETACH DELETE n;")
            .map_err(Self::kuzu_error)?;
        conn.execute(
            &mut statement,
            vec![("id", Value::String(node_id.to_string()))],
        )
        .map_err(Self::kuzu_error)?;
        Ok(DeleteNodeResult {
            deleted: true,
            edges_deleted,
        })
    }

    fn delete_edge(&self, edge_id: &GraphEdgeId) -> GraphResult<bool> {
        let existed = self.get_edge(edge_id)?.is_some();
        if !existed {
            return Ok(false);
        }
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| GraphError::Storage("Kuzu graph write lock is poisoned".to_string()))?;
        let conn = self.connection()?;
        let mut statement = conn
            .prepare("MATCH ()-[r:GraphEdge]->() WHERE r.id = $id DELETE r;")
            .map_err(Self::kuzu_error)?;
        conn.execute(
            &mut statement,
            vec![("id", Value::String(edge_id.to_string()))],
        )
        .map_err(Self::kuzu_error)?;
        Ok(true)
    }

    fn neighbors(&self, request: NeighborRequest) -> GraphResult<Vec<GraphNeighbor>> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }

        let mut neighbors = Vec::new();
        let kind_predicate = Self::edge_kind_predicate(&request.edge_kinds);
        let weight_predicate = Self::min_weight_predicate(request.min_weight);
        let conn = self.connection()?;

        if matches!(request.direction, Direction::Out | Direction::Both) {
            let query = format!(
                "MATCH (a:GraphNode)-[r:GraphEdge]->(b:GraphNode) \
                 WHERE a.id = $id{kind_predicate}{weight_predicate} \
                 RETURN b.id AS node_id, b.kind AS node_kind, \
                        b.properties AS node_properties, \
                        b.created_at_ms AS node_created_at_ms, \
                        b.updated_at_ms AS node_updated_at_ms, \
                        r.id AS edge_id, a.id AS edge_from, b.id AS edge_to, \
                        r.kind AS edge_kind, r.weight AS edge_weight, \
                        r.properties AS edge_properties, \
                        r.created_at_ms AS edge_created_at_ms, \
                        r.updated_at_ms AS edge_updated_at_ms;"
            );
            let mut statement = conn.prepare(&query).map_err(Self::kuzu_error)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![("id", Value::String(request.node_id.to_string()))],
                )
                .map_err(Self::kuzu_error)?;
            for row in result {
                neighbors.push(GraphNeighbor {
                    node: Self::row_to_node(&row, 0)?,
                    edge: Self::row_to_edge(&row, 5)?,
                    direction: DirectionOnEdge::Outgoing,
                });
            }
        }

        if matches!(request.direction, Direction::In | Direction::Both) {
            let query = format!(
                "MATCH (a:GraphNode)-[r:GraphEdge]->(b:GraphNode) \
                 WHERE b.id = $id{kind_predicate}{weight_predicate} \
                 RETURN a.id AS node_id, a.kind AS node_kind, \
                        a.properties AS node_properties, \
                        a.created_at_ms AS node_created_at_ms, \
                        a.updated_at_ms AS node_updated_at_ms, \
                        r.id AS edge_id, a.id AS edge_from, b.id AS edge_to, \
                        r.kind AS edge_kind, r.weight AS edge_weight, \
                        r.properties AS edge_properties, \
                        r.created_at_ms AS edge_created_at_ms, \
                        r.updated_at_ms AS edge_updated_at_ms;"
            );
            let mut statement = conn.prepare(&query).map_err(Self::kuzu_error)?;
            let result = conn
                .execute(
                    &mut statement,
                    vec![("id", Value::String(request.node_id.to_string()))],
                )
                .map_err(Self::kuzu_error)?;
            for row in result {
                neighbors.push(GraphNeighbor {
                    node: Self::row_to_node(&row, 0)?,
                    edge: Self::row_to_edge(&row, 5)?,
                    direction: DirectionOnEdge::Incoming,
                });
            }
        }

        let mut seen_edges = HashSet::new();
        neighbors.retain(|neighbor| seen_edges.insert(neighbor.edge.id.clone()));
        neighbors.sort_by(|a, b| {
            b.edge
                .weight
                .partial_cmp(&a.edge.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.node.id.as_str().cmp(b.node.id.as_str()))
                .then_with(|| a.edge.id.as_str().cmp(b.edge.id.as_str()))
        });
        neighbors.truncate(request.limit);
        Ok(neighbors)
    }

    fn two_hop(&self, request: TwoHopRequest) -> GraphResult<Vec<GraphPath>> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }

        let first_hop = self.neighbors(
            NeighborRequest::new(request.node_id.clone())
                .with_direction(Direction::Out)
                .with_edge_kinds(request.edge_kinds.clone())
                .with_limit(request.first_hop_limit),
        )?;

        let mut paths = Vec::new();
        for first in first_hop {
            let second_hop = self.neighbors(
                NeighborRequest::new(first.node.id.clone())
                    .with_direction(Direction::Out)
                    .with_edge_kinds(request.edge_kinds.clone())
                    .with_limit(request.second_hop_limit),
            )?;

            for second in second_hop {
                let score = (first.edge.weight + second.edge.weight) / 2.0;
                paths.push(GraphPath {
                    nodes: vec![first.node.clone(), second.node],
                    edges: vec![first.edge.clone(), second.edge],
                    score,
                });
            }
        }

        paths.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let a_id = a.nodes.last().map(|n| n.id.as_str()).unwrap_or_default();
                    let b_id = b.nodes.last().map(|n| n.id.as_str()).unwrap_or_default();
                    a_id.cmp(b_id)
                })
        });
        paths.truncate(request.limit);
        Ok(paths)
    }

    fn related_chunks(
        &self,
        entity_id: &GraphNodeId,
        limit: usize,
    ) -> GraphResult<Vec<RelatedChunk>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut chunks = Vec::new();
        let mut seen = HashSet::new();
        let one_hop = self.neighbors(
            NeighborRequest::new(entity_id.clone())
                .with_direction(Direction::Both)
                .with_limit(limit.saturating_mul(8)),
        )?;
        for neighbor in one_hop {
            if !Self::is_chunk_expansion_kind(neighbor.edge.kind) {
                continue;
            }
            if let Some(vector_id) = neighbor.node.id.as_chunk_vector_id() {
                if seen.insert(vector_id.clone()) {
                    chunks.push(RelatedChunk {
                        vector_id,
                        via_node: neighbor.node.id,
                    });
                }
            }
            if chunks.len() >= limit {
                break;
            }
        }
        Ok(chunks)
    }

    fn path_exists(&self, request: PathExistsRequest) -> GraphResult<bool> {
        if request.max_depth == 0 {
            return Ok(request.from == request.to);
        }
        if request.max_depth > 3 {
            return Err(GraphError::InvalidRequest(
                "path_exists max_depth is capped at 3".to_string(),
            ));
        }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::from([(request.from.clone(), 0u8)]);
        visited.insert(request.from);

        while let Some((node_id, depth)) = queue.pop_front() {
            if node_id == request.to {
                return Ok(true);
            }
            if depth >= request.max_depth {
                continue;
            }
            let neighbors = self.neighbors(
                NeighborRequest::new(node_id)
                    .with_direction(Direction::Out)
                    .with_edge_kinds(request.edge_kinds.clone())
                    .with_limit(256),
            )?;
            for neighbor in neighbors {
                if visited.insert(neighbor.node.id.clone()) {
                    queue.push_back((neighbor.node.id, depth + 1));
                }
            }
        }

        Ok(false)
    }

    fn stats(&self) -> GraphResult<GraphStats> {
        let nodes = self.query_count("MATCH (n:GraphNode) RETURN count(n);")?;
        let edges = self.query_count("MATCH ()-[r:GraphEdge]->() RETURN count(r);")?;
        let conn = self.connection()?;
        let mut result = conn
            .query(
                "MATCH (a:GraphNode)-[r:GraphEdge]->(b:GraphNode) \
                 RETURN b.id, r.kind;",
            )
            .map_err(Self::kuzu_error)?;
        let mut chunk_links = 0u64;
        for row in &mut result {
            let edge = GraphEdge {
                id: GraphEdgeId::new("_stats"),
                from: GraphNodeId::new("_stats_from"),
                to: GraphNodeId::new(Self::value_string(&row, 0, "stats.to")?),
                kind: Self::edge_kind_from_key(&Self::value_string(&row, 1, "stats.kind")?)?,
                weight: 1.0,
                properties: serde_json::Map::new(),
                created_at_ms: 0,
                updated_at_ms: 0,
            };
            if Self::should_link_chunk(&edge) {
                chunk_links += 1;
            }
        }
        Ok(GraphStats {
            nodes,
            edges,
            chunk_links,
        })
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
    use crate::{EdgeKind, GraphEdge, GraphIndex, GraphNode, GraphNodeId, NodeKind};

    fn edge(id: &str, from: &str, to: &str, kind: EdgeKind, weight: f32) -> GraphEdge {
        GraphEdge::new(id, from, to, kind).with_weight(weight)
    }

    #[test]
    fn test_kuzu_schema_names_expected_tables() {
        let schema = KuzuGraphAdapter::schema_cypher();
        assert!(schema.contains("CREATE NODE TABLE IF NOT EXISTS GraphNode"));
        assert!(schema.contains("CREATE REL TABLE IF NOT EXISTS GraphEdge"));
        assert!(schema.contains("PRIMARY KEY(id)"));
    }

    #[test]
    fn test_kuzu_adapter_upserts_and_reads_node() {
        let adapter = KuzuGraphAdapter::in_memory().unwrap();
        assert_eq!(adapter.path(), Path::new(":memory:"));

        let node = GraphNode::new(GraphNodeId::new("chunk:a"), NodeKind::Chunk)
            .with_property("title", serde_json::json!("A"));
        adapter.upsert_node(node).unwrap();

        let got = adapter
            .get_node(&GraphNodeId::new("chunk:a"))
            .unwrap()
            .unwrap();
        assert_eq!(got.kind, NodeKind::Chunk);
        assert_eq!(got.properties["title"], serde_json::json!("A"));
    }

    #[test]
    fn test_kuzu_adapter_neighbors_and_related_chunks() {
        let adapter = KuzuGraphAdapter::in_memory().unwrap();
        adapter
            .upsert_node(GraphNode::new("entity:mtp", NodeKind::Entity))
            .unwrap();
        adapter
            .upsert_node(GraphNode::new("chunk:vec-1", NodeKind::Chunk))
            .unwrap();
        adapter
            .upsert_edge(edge(
                "mention",
                "entity:mtp",
                "chunk:vec-1",
                EdgeKind::Mentions,
                0.9,
            ))
            .unwrap();

        let neighbors = adapter
            .neighbors(
                NeighborRequest::new("entity:mtp")
                    .with_direction(Direction::Out)
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].node.id, GraphNodeId::new("chunk:vec-1"));

        let chunks = adapter
            .related_chunks(&GraphNodeId::new("entity:mtp"), 10)
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].vector_id.as_str(), "vec-1");
    }

    #[test]
    fn test_kuzu_adapter_related_chunks_includes_incoming_chunk_edges() {
        let adapter = KuzuGraphAdapter::in_memory().unwrap();
        adapter
            .upsert_node(GraphNode::new("entity:mtp", NodeKind::Entity))
            .unwrap();
        adapter
            .upsert_node(GraphNode::new("chunk:incoming", NodeKind::Chunk))
            .unwrap();
        adapter
            .upsert_edge(edge(
                "incoming",
                "chunk:incoming",
                "entity:mtp",
                EdgeKind::Mentions,
                0.9,
            ))
            .unwrap();

        let chunks = adapter
            .related_chunks(&GraphNodeId::new("entity:mtp"), 10)
            .unwrap();
        let vector_ids: Vec<&str> = chunks
            .iter()
            .map(|chunk| chunk.vector_id.as_str())
            .collect();

        assert_eq!(vector_ids, vec!["incoming"]);
    }

    #[test]
    fn test_kuzu_adapter_two_hop_path_delete_and_stats() {
        let adapter = KuzuGraphAdapter::in_memory().unwrap();
        for id in ["a", "b", "c"] {
            adapter
                .upsert_node(GraphNode::new(id, NodeKind::Entity))
                .unwrap();
        }
        adapter
            .upsert_edge(edge("ab", "a", "b", EdgeKind::RelatedTo, 1.0))
            .unwrap();
        adapter
            .upsert_edge(edge("bc", "b", "c", EdgeKind::RelatedTo, 0.5))
            .unwrap();

        assert_eq!(adapter.stats().unwrap().edges, 2);
        assert!(adapter
            .path_exists(PathExistsRequest::new("a", "c", 2))
            .unwrap());
        let paths = adapter.two_hop(TwoHopRequest::new("a")).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes.last().unwrap().id, GraphNodeId::new("c"));

        assert!(adapter.delete_edge(&GraphEdgeId::new("bc")).unwrap());
        assert!(!adapter
            .path_exists(PathExistsRequest::new("a", "c", 2))
            .unwrap());

        let deleted = adapter.delete_node(&GraphNodeId::new("a")).unwrap();
        assert!(deleted.deleted);
        assert_eq!(deleted.edges_deleted, 1);
        assert!(adapter.get_node(&GraphNodeId::new("a")).unwrap().is_none());
    }
}
