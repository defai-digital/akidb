//! Native storage-backed graph index.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use akidb_storage::{BatchOperation, StorageBackend};

use crate::error::{GraphError, GraphResult};
use crate::keys;
use crate::model::{
    DeleteNodeResult, Direction, DirectionOnEdge, EdgeKind, GraphEdge, GraphEdgeId,
    GraphMutationBatch, GraphNeighbor, GraphNode, GraphNodeId, GraphPath, GraphStats, RelatedChunk,
};
use crate::query::{GraphIndex, NeighborRequest, PathExistsRequest, TwoHopRequest};

/// RocksDB-compatible native graph index.
pub struct NativeGraphIndex<S: StorageBackend> {
    storage: Arc<S>,
    mutation_lock: Mutex<()>,
}

impl<S: StorageBackend> NativeGraphIndex<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            mutation_lock: Mutex::new(()),
        }
    }

    fn mutation_guard(&self) -> MutexGuard<'_, ()> {
        self.mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn serialize<T: serde::Serialize>(value: &T) -> GraphResult<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> GraphResult<T> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn edge_refs_for_prefix(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> GraphResult<Vec<GraphEdgeId>> {
        let entries = self.storage.scan_prefix_limited(prefix, limit)?;
        entries
            .into_iter()
            .map(|(_, value)| Self::deserialize::<GraphEdgeId>(&value))
            .collect()
    }

    fn edge_refs_for_node(
        &self,
        node_id: &GraphNodeId,
        direction: Direction,
        kinds: &[EdgeKind],
        limit: Option<usize>,
    ) -> GraphResult<Vec<(GraphEdgeId, DirectionOnEdge)>> {
        let selected_kinds: Vec<Option<EdgeKind>> = if kinds.is_empty() {
            vec![None]
        } else {
            kinds.iter().copied().map(Some).collect()
        };

        let mut refs = Vec::new();
        for kind in selected_kinds {
            if matches!(direction, Direction::Out | Direction::Both) {
                for edge_id in
                    self.edge_refs_for_prefix(&keys::adjacency_prefix_out(node_id, kind), limit)?
                {
                    refs.push((edge_id, DirectionOnEdge::Outgoing));
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for edge_id in
                    self.edge_refs_for_prefix(&keys::adjacency_prefix_in(node_id, kind), limit)?
                {
                    refs.push((edge_id, DirectionOnEdge::Incoming));
                }
            }
        }
        Ok(refs)
    }

    fn graph_neighbor(
        &self,
        source: &GraphNodeId,
        edge_id: &GraphEdgeId,
        direction: DirectionOnEdge,
    ) -> GraphResult<Option<GraphNeighbor>> {
        let Some(edge) = self.get_edge(edge_id)? else {
            return Ok(None);
        };
        let neighbor_id = match direction {
            DirectionOnEdge::Outgoing if edge.from == *source => &edge.to,
            DirectionOnEdge::Incoming if edge.to == *source => &edge.from,
            _ => return Ok(None),
        };
        let Some(node) = self.get_node(neighbor_id)? else {
            return Ok(None);
        };
        if !source.is_same_workspace(&node.id) {
            return Ok(None);
        }
        Ok(Some(GraphNeighbor {
            node,
            edge,
            direction,
        }))
    }

    fn direct_related_chunks(
        &self,
        entity_id: &GraphNodeId,
        limit: usize,
    ) -> GraphResult<Vec<RelatedChunk>> {
        let entries = self
            .storage
            .scan_prefix_limited(&keys::chunk_prefix(entity_id), Some(limit))?;
        let mut chunks = Vec::with_capacity(entries.len());
        for (_, value) in entries {
            let node_id = Self::deserialize::<GraphNodeId>(&value)?;
            if !entity_id.is_same_workspace(&node_id) {
                continue;
            }
            if let Some(vector_id) = node_id.as_chunk_vector_id() {
                chunks.push(RelatedChunk {
                    vector_id,
                    via_node: node_id,
                });
            }
        }
        chunks.sort_by(|a, b| a.vector_id.as_str().cmp(b.vector_id.as_str()));
        chunks.dedup_by(|a, b| a.vector_id == b.vector_id);
        chunks.truncate(limit);
        Ok(chunks)
    }

    fn should_link_chunk(edge: &GraphEdge) -> bool {
        edge.to.as_chunk_vector_id().is_some()
            && matches!(
                edge.kind,
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

    fn node_upsert_operations(
        &self,
        mut node: GraphNode,
        merge_existing_properties: bool,
    ) -> GraphResult<Vec<BatchOperation>> {
        let mut operations = Vec::new();
        if let Some(existing) = self.get_node(&node.id)? {
            if merge_existing_properties {
                let mut properties = existing.properties.clone();
                properties.extend(node.properties);
                node.properties = properties;
            }
            node.created_at_ms = existing.created_at_ms;
            node.updated_at_ms = crate::model::current_timestamp_ms();
            if existing.kind != node.kind {
                operations.push(BatchOperation::Delete {
                    key: keys::kind_key(existing.kind, &node.id),
                });
            }
        }
        operations.extend([
            BatchOperation::Put {
                key: keys::node_key(&node.id),
                value: Self::serialize(&node)?,
            },
            BatchOperation::Put {
                key: keys::kind_key(node.kind, &node.id),
                value: Vec::new(),
            },
        ]);
        Ok(operations)
    }

    fn edge_delete_operations(edge: &GraphEdge) -> Vec<BatchOperation> {
        let mut operations = vec![
            BatchOperation::Delete {
                key: keys::edge_key(&edge.id),
            },
            BatchOperation::Delete {
                key: keys::adjacency_key_out(&edge.from, edge.kind, &edge.id),
            },
            BatchOperation::Delete {
                key: keys::adjacency_key_in(&edge.to, edge.kind, &edge.id),
            },
        ];
        if Self::should_link_chunk(edge) {
            operations.push(BatchOperation::Delete {
                key: keys::chunk_key(&edge.from, &edge.to),
            });
        }
        operations
    }

    fn edge_upsert_operations(&self, mut edge: GraphEdge) -> GraphResult<Vec<BatchOperation>> {
        if !edge.weight.is_finite() {
            return Err(GraphError::InvalidRequest(format!(
                "edge {} has non-finite weight",
                edge.id
            )));
        }
        if !edge.from.is_same_workspace(&edge.to) {
            return Err(GraphError::InvalidRequest(format!(
                "edge {} crosses graph workspace namespaces",
                edge.id
            )));
        }

        let mut operations = Vec::new();
        if let Some(existing) = self.get_edge(&edge.id)? {
            edge.created_at_ms = existing.created_at_ms;
            edge.updated_at_ms = crate::model::current_timestamp_ms();
            operations.extend(Self::edge_delete_operations(&existing));
        }
        operations.extend([
            BatchOperation::Put {
                key: keys::edge_key(&edge.id),
                value: Self::serialize(&edge)?,
            },
            BatchOperation::Put {
                key: keys::adjacency_key_out(&edge.from, edge.kind, &edge.id),
                value: Self::serialize(&edge.id)?,
            },
            BatchOperation::Put {
                key: keys::adjacency_key_in(&edge.to, edge.kind, &edge.id),
                value: Self::serialize(&edge.id)?,
            },
        ]);
        if Self::should_link_chunk(&edge) {
            operations.push(BatchOperation::Put {
                key: keys::chunk_key(&edge.from, &edge.to),
                value: Self::serialize(&edge.to)?,
            });
        }
        Ok(operations)
    }
}

impl<S: StorageBackend> GraphIndex for NativeGraphIndex<S> {
    fn upsert_node(&self, node: GraphNode) -> GraphResult<()> {
        self.upsert_batch(GraphMutationBatch::new().with_replaced_node(node))
    }

    fn upsert_edge(&self, edge: GraphEdge) -> GraphResult<()> {
        self.upsert_batch(GraphMutationBatch::new().with_edge(edge))
    }

    fn upsert_batch(&self, batch: GraphMutationBatch) -> GraphResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let _mutation_guard = self.mutation_guard();

        // Merge duplicate nodes inside one projection unit. Later values win
        // for the same property, but richer properties from an earlier
        // projection are retained.
        let replace_nodes: HashSet<GraphNodeId> = batch.replace_nodes.into_iter().collect();
        let mut nodes = BTreeMap::<GraphNodeId, GraphNode>::new();
        for node in batch.nodes {
            match nodes.entry(node.id.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(node);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let merged = entry.get_mut();
                    merged.kind = node.kind;
                    merged.properties.extend(node.properties);
                    merged.created_at_ms = merged.created_at_ms.min(node.created_at_ms);
                    merged.updated_at_ms = merged.updated_at_ms.max(node.updated_at_ms);
                }
            }
        }
        let edges: BTreeMap<GraphEdgeId, GraphEdge> = batch
            .edges
            .into_iter()
            .map(|edge| (edge.id.clone(), edge))
            .collect();

        let mut operations = Vec::new();
        for edge_id in batch
            .delete_edges
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
        {
            if let Some(edge) = self.get_edge(&edge_id)? {
                operations.extend(Self::edge_delete_operations(&edge));
            }
        }
        for node in nodes.into_values() {
            let merge_existing_properties = !replace_nodes.contains(&node.id);
            operations.extend(self.node_upsert_operations(node, merge_existing_properties)?);
        }
        for edge in edges.into_values() {
            operations.extend(self.edge_upsert_operations(edge)?);
        }
        self.storage.write_batch(operations)?;
        Ok(())
    }

    fn get_node(&self, node_id: &GraphNodeId) -> GraphResult<Option<GraphNode>> {
        self.storage
            .get(&keys::node_key(node_id))?
            .map(|bytes| Self::deserialize(&bytes))
            .transpose()
    }

    fn get_edge(&self, edge_id: &GraphEdgeId) -> GraphResult<Option<GraphEdge>> {
        self.storage
            .get(&keys::edge_key(edge_id))?
            .map(|bytes| Self::deserialize(&bytes))
            .transpose()
    }

    fn delete_node(&self, node_id: &GraphNodeId) -> GraphResult<DeleteNodeResult> {
        let _mutation_guard = self.mutation_guard();
        let Some(node) = self.get_node(node_id)? else {
            return Ok(DeleteNodeResult::default());
        };

        let mut edge_ids: HashSet<GraphEdgeId> = HashSet::new();
        for (edge_id, _) in self.edge_refs_for_node(node_id, Direction::Both, &[], None)? {
            edge_ids.insert(edge_id);
        }

        let mut operations = Vec::new();
        let mut edges_deleted = 0usize;
        for edge_id in &edge_ids {
            if let Some(edge) = self.get_edge(edge_id)? {
                operations.extend(Self::edge_delete_operations(&edge));
                edges_deleted += 1;
            }
        }

        operations.extend([
            BatchOperation::Delete {
                key: keys::node_key(node_id),
            },
            BatchOperation::Delete {
                key: keys::kind_key(node.kind, node_id),
            },
        ]);
        self.storage.write_batch(operations)?;

        Ok(DeleteNodeResult {
            deleted: true,
            edges_deleted,
        })
    }

    fn delete_edge(&self, edge_id: &GraphEdgeId) -> GraphResult<bool> {
        let _mutation_guard = self.mutation_guard();
        let Some(edge) = self.get_edge(edge_id)? else {
            return Ok(false);
        };
        self.storage
            .write_batch(Self::edge_delete_operations(&edge))?;
        Ok(true)
    }

    fn neighbors(&self, request: NeighborRequest) -> GraphResult<Vec<GraphNeighbor>> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }

        let edge_refs = self.edge_refs_for_node(
            &request.node_id,
            request.direction,
            &request.edge_kinds,
            Some(request.limit.saturating_mul(4)),
        )?;
        let mut seen_edges = HashSet::new();
        let mut neighbors = Vec::new();

        for (edge_id, direction) in edge_refs {
            if !seen_edges.insert(edge_id.clone()) {
                continue;
            }
            if let Some(neighbor) = self.graph_neighbor(&request.node_id, &edge_id, direction)? {
                if request
                    .min_weight
                    .is_none_or(|min_weight| neighbor.edge.weight >= min_weight)
                {
                    neighbors.push(neighbor);
                }
            }
        }

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

        if chunks.len() >= limit {
            return Ok(chunks);
        }

        for chunk in self.direct_related_chunks(entity_id, limit)? {
            if seen.insert(chunk.vector_id.clone()) {
                chunks.push(chunk);
            }
            if chunks.len() >= limit {
                break;
            }
        }
        chunks.truncate(limit);
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
        Ok(GraphStats {
            nodes: self
                .storage
                .scan_prefix_limited(keys::node_prefix(), None)?
                .len() as u64,
            edges: self
                .storage
                .scan_prefix_limited(keys::edge_prefix(), None)?
                .len() as u64,
            chunk_links: self
                .storage
                .scan_prefix_limited(keys::chunk_all_prefix(), None)?
                .len() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use akidb_storage::RocksDbBackend;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::model::NodeKind;
    use crate::query::RelatedChunksRequest;

    fn index() -> (tempfile::TempDir, NativeGraphIndex<RocksDbBackend>) {
        let dir = tempdir().unwrap();
        let backend = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        (dir, NativeGraphIndex::new(backend))
    }

    fn node(id: &str, kind: NodeKind) -> GraphNode {
        GraphNode::new(id, kind)
    }

    fn edge(id: &str, from: &str, to: &str, kind: EdgeKind, weight: f32) -> GraphEdge {
        GraphEdge::new(id, from, to, kind).with_weight(weight)
    }

    #[test]
    fn test_upsert_and_get_node() {
        let (_dir, graph) = index();
        let node =
            node("file:src/main.rs", NodeKind::File).with_property("path", json!("src/main.rs"));
        graph.upsert_node(node.clone()).unwrap();

        let got = graph
            .get_node(&GraphNodeId::from("file:src/main.rs"))
            .unwrap();
        assert_eq!(got.unwrap().properties["path"], json!("src/main.rs"));
    }

    #[test]
    fn test_batch_merges_duplicate_node_properties() {
        let (_dir, graph) = index();
        let mut first =
            node("entity:invoice", NodeKind::Entity).with_property("canonical_id", json!("INV-1"));
        first.created_at_ms = 7;
        first.updated_at_ms = 7;
        let second =
            node("entity:invoice", NodeKind::Entity).with_property("tenant_id", json!("tenant-a"));

        graph
            .upsert_batch(GraphMutationBatch::new().with_node(first).with_node(second))
            .unwrap();

        let stored = graph
            .get_node(&GraphNodeId::from("entity:invoice"))
            .unwrap()
            .unwrap();
        assert_eq!(stored.properties["canonical_id"], json!("INV-1"));
        assert_eq!(stored.properties["tenant_id"], json!("tenant-a"));
        assert_eq!(stored.created_at_ms, 7);
    }

    #[test]
    fn test_placeholder_merge_preserves_data_but_authoritative_upsert_replaces_it() {
        let (_dir, graph) = index();
        graph
            .upsert_node(
                node("chunk:evidence", NodeKind::Chunk)
                    .with_property("source_uri", json!("s3://docs/evidence.pdf")),
            )
            .unwrap();
        graph
            .upsert_batch(
                GraphMutationBatch::new().with_node(
                    node("chunk:evidence", NodeKind::Chunk)
                        .with_property("vector_id", json!("evidence")),
                ),
            )
            .unwrap();

        let merged = graph
            .get_node(&GraphNodeId::from("chunk:evidence"))
            .unwrap()
            .unwrap();
        assert_eq!(
            merged.properties["source_uri"],
            json!("s3://docs/evidence.pdf")
        );
        assert_eq!(merged.properties["vector_id"], json!("evidence"));

        graph
            .upsert_node(
                node("chunk:evidence", NodeKind::Chunk)
                    .with_property("source_uri", json!("s3://docs/replacement.pdf")),
            )
            .unwrap();
        let replaced = graph
            .get_node(&GraphNodeId::from("chunk:evidence"))
            .unwrap()
            .unwrap();
        assert_eq!(
            replaced.properties["source_uri"],
            json!("s3://docs/replacement.pdf")
        );
        assert!(!replaced.properties.contains_key("vector_id"));
    }

    #[test]
    fn test_invalid_batch_is_not_partially_committed() {
        let (_dir, graph) = index();
        let batch = GraphMutationBatch::new()
            .with_node(node("entity:a", NodeKind::Entity))
            .with_node(node("chunk:a", NodeKind::Chunk))
            .with_edge(edge(
                "invalid",
                "entity:a",
                "chunk:a",
                EdgeKind::Mentions,
                f32::NAN,
            ));

        assert!(matches!(
            graph.upsert_batch(batch),
            Err(GraphError::InvalidRequest(_))
        ));
        assert_eq!(graph.stats().unwrap(), GraphStats::default());
    }

    #[test]
    fn test_cross_workspace_edge_is_rejected_without_partial_state() {
        let (_dir, graph) = index();
        let a = GraphNodeId::scoped("workspace-a", "entity:customer");
        let b = GraphNodeId::scoped("workspace-b", "chunk:evidence");
        let batch = GraphMutationBatch::new()
            .with_node(GraphNode::new(a.clone(), NodeKind::Entity))
            .with_node(GraphNode::new(b.clone(), NodeKind::Chunk))
            .with_edge(GraphEdge::new("cross-workspace", a, b, EdgeKind::Mentions));

        assert!(matches!(
            graph.upsert_batch(batch),
            Err(GraphError::InvalidRequest(message))
                if message.contains("crosses graph workspace")
        ));
        assert_eq!(graph.stats().unwrap(), GraphStats::default());
    }

    #[test]
    fn test_batch_atomically_replaces_projection_edges() {
        let (_dir, graph) = index();
        graph
            .upsert_batch(
                GraphMutationBatch::new()
                    .with_node(node("entity:invoice", NodeKind::Entity))
                    .with_node(node("chunk:old", NodeKind::Chunk))
                    .with_edge(edge(
                        "old-evidence",
                        "entity:invoice",
                        "chunk:old",
                        EdgeKind::Mentions,
                        1.0,
                    )),
            )
            .unwrap();

        graph
            .upsert_batch(
                GraphMutationBatch::new()
                    .with_deleted_edge("old-evidence")
                    .with_node(node("chunk:new", NodeKind::Chunk))
                    .with_edge(edge(
                        "new-evidence",
                        "entity:invoice",
                        "chunk:new",
                        EdgeKind::Mentions,
                        1.0,
                    )),
            )
            .unwrap();

        let related = graph
            .related_chunks(&GraphNodeId::from("entity:invoice"), 10)
            .unwrap();
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].vector_id.as_str(), "new");
        assert!(graph
            .get_edge(&GraphEdgeId::from("old-evidence"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_related_chunks_with_depth_reaches_two_hop_chunk() {
        let (_dir, graph) = index();
        graph
            .upsert_batch(
                GraphMutationBatch::new()
                    .with_node(node("entity:customer", NodeKind::Entity))
                    .with_node(node("entity:ticket", NodeKind::Entity))
                    .with_node(node("chunk:evidence", NodeKind::Chunk))
                    .with_edge(edge(
                        "customer-ticket",
                        "entity:customer",
                        "entity:ticket",
                        EdgeKind::RelatedTo,
                        1.0,
                    ))
                    .with_edge(edge(
                        "ticket-evidence",
                        "entity:ticket",
                        "chunk:evidence",
                        EdgeKind::Mentions,
                        1.0,
                    )),
            )
            .unwrap();

        assert!(graph
            .related_chunks_with_depth(
                RelatedChunksRequest::new("entity:customer")
                    .with_max_depth(1)
                    .with_limit(10),
            )
            .unwrap()
            .is_empty());
        let related = graph
            .related_chunks_with_depth(
                RelatedChunksRequest::new("entity:customer")
                    .with_max_depth(2)
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].vector_id.as_str(), "evidence");
    }

    #[test]
    fn test_neighbors_by_type_and_direction() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("symbol:a", NodeKind::Function))
            .unwrap();
        graph
            .upsert_node(node("symbol:b", NodeKind::Function))
            .unwrap();
        graph.upsert_node(node("file:x", NodeKind::File)).unwrap();
        graph
            .upsert_edge(edge("e1", "symbol:a", "symbol:b", EdgeKind::Calls, 0.8))
            .unwrap();
        graph
            .upsert_edge(edge("e2", "symbol:a", "file:x", EdgeKind::Contains, 0.9))
            .unwrap();

        let neighbors = graph
            .neighbors(
                NeighborRequest::new("symbol:a")
                    .with_direction(Direction::Out)
                    .with_edge_kinds(vec![EdgeKind::Calls])
                    .with_limit(10),
            )
            .unwrap();

        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].node.id, GraphNodeId::from("symbol:b"));
        assert_eq!(neighbors[0].edge.kind, EdgeKind::Calls);

        let incoming = graph
            .neighbors(
                NeighborRequest::new("symbol:b")
                    .with_direction(Direction::In)
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(incoming[0].node.id, GraphNodeId::from("symbol:a"));
    }

    #[test]
    fn test_neighbors_limit_not_consumed_by_prefixed_node_id() {
        let (_dir, graph) = index();
        graph.upsert_node(node("a", NodeKind::Entity)).unwrap();
        graph.upsert_node(node("target", NodeKind::Entity)).unwrap();
        graph.upsert_node(node("a:b", NodeKind::Entity)).unwrap();
        for index in 0..8 {
            let sibling = format!("sibling:{index}");
            graph.upsert_node(node(&sibling, NodeKind::Entity)).unwrap();
            graph
                .upsert_edge(edge(
                    &format!("sibling-edge:{index}"),
                    "a:b",
                    &sibling,
                    EdgeKind::RelatedTo,
                    1.0,
                ))
                .unwrap();
        }
        graph
            .upsert_edge(edge("real", "a", "target", EdgeKind::RelatedTo, 0.5))
            .unwrap();

        let neighbors = graph
            .neighbors(
                NeighborRequest::new("a")
                    .with_direction(Direction::Out)
                    .with_limit(1),
            )
            .unwrap();

        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].node.id, GraphNodeId::from("target"));
    }

    #[test]
    fn test_two_hop_and_path_exists() {
        let (_dir, graph) = index();
        for id in ["a", "b", "c"] {
            graph.upsert_node(node(id, NodeKind::Entity)).unwrap();
        }
        graph
            .upsert_edge(edge("ab", "a", "b", EdgeKind::RelatedTo, 1.0))
            .unwrap();
        graph
            .upsert_edge(edge("bc", "b", "c", EdgeKind::RelatedTo, 0.5))
            .unwrap();

        let paths = graph.two_hop(TwoHopRequest::new("a")).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes.last().unwrap().id, GraphNodeId::from("c"));
        assert!(graph
            .path_exists(PathExistsRequest::new("a", "c", 2))
            .unwrap());
        assert!(!graph
            .path_exists(PathExistsRequest::new("c", "a", 2))
            .unwrap());
    }

    #[test]
    fn test_related_chunks_direct_mapping_and_delete_edge() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:mtp", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("chunk:vec-1", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_edge(edge(
                "mention",
                "entity:mtp",
                "chunk:vec-1",
                EdgeKind::Mentions,
                1.0,
            ))
            .unwrap();

        let chunks = graph
            .related_chunks(&GraphNodeId::from("entity:mtp"), 10)
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].vector_id.as_str(), "vec-1");

        assert!(graph.delete_edge(&GraphEdgeId::from("mention")).unwrap());
        assert!(graph
            .related_chunks(&GraphNodeId::from("entity:mtp"), 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_related_chunks_does_not_match_prefixed_entity_id() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:a", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("entity:a:b", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("chunk:sibling", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_edge(edge(
                "sibling-chunk",
                "entity:a:b",
                "chunk:sibling",
                EdgeKind::Mentions,
                1.0,
            ))
            .unwrap();

        assert!(graph
            .related_chunks(&GraphNodeId::from("entity:a"), 1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_upsert_edge_replaces_stale_adjacency_and_chunk_link() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:mtp", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("chunk:old", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_node(node("chunk:new", NodeKind::Chunk))
            .unwrap();

        graph
            .upsert_edge(edge(
                "edge:mtp-chunk",
                "entity:mtp",
                "chunk:old",
                EdgeKind::Mentions,
                0.4,
            ))
            .unwrap();
        graph
            .upsert_edge(edge(
                "edge:mtp-chunk",
                "entity:mtp",
                "chunk:new",
                EdgeKind::RelatedTo,
                0.9,
            ))
            .unwrap();

        let stale_kind_neighbors = graph
            .neighbors(
                NeighborRequest::new("entity:mtp")
                    .with_direction(Direction::Out)
                    .with_edge_kinds(vec![EdgeKind::Mentions]),
            )
            .unwrap();
        assert!(
            stale_kind_neighbors.is_empty(),
            "old edge-kind adjacency must be removed on edge replacement"
        );

        let chunks = graph
            .related_chunks(&GraphNodeId::from("entity:mtp"), 10)
            .unwrap();
        assert_eq!(
            chunks,
            vec![RelatedChunk {
                vector_id: akidb_common::VectorId::new("new"),
                via_node: GraphNodeId::from("chunk:new"),
            }],
            "old direct chunk link must be removed on edge replacement"
        );
    }

    #[test]
    fn test_related_chunks_includes_incoming_chunk_edges() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("chunk:vec-1", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_node(node("person:Akira", NodeKind::Person))
            .unwrap();
        graph
            .upsert_edge(edge(
                "owner",
                "chunk:vec-1",
                "person:Akira",
                EdgeKind::OwnedBy,
                1.0,
            ))
            .unwrap();

        let chunks = graph
            .related_chunks(&GraphNodeId::from("person:Akira"), 10)
            .unwrap();

        assert_eq!(
            chunks,
            vec![RelatedChunk {
                vector_id: akidb_common::VectorId::new("vec-1"),
                via_node: GraphNodeId::from("chunk:vec-1"),
            }],
            "entity nodes should resolve chunks connected by incoming metadata edges"
        );
    }

    #[test]
    fn test_related_chunks_combines_direct_and_incoming_chunk_edges() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:mtp", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("chunk:direct", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_node(node("chunk:incoming", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_edge(edge(
                "direct",
                "entity:mtp",
                "chunk:direct",
                EdgeKind::Mentions,
                1.0,
            ))
            .unwrap();
        graph
            .upsert_edge(edge(
                "incoming",
                "chunk:incoming",
                "entity:mtp",
                EdgeKind::OwnedBy,
                1.0,
            ))
            .unwrap();

        let chunks = graph
            .related_chunks(&GraphNodeId::from("entity:mtp"), 10)
            .unwrap();
        let vector_ids: Vec<&str> = chunks
            .iter()
            .map(|chunk| chunk.vector_id.as_str())
            .collect();

        assert_eq!(vector_ids, vec!["direct", "incoming"]);
    }

    #[test]
    fn test_related_chunks_limit_prefers_higher_weight_chunk() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:mtp", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("chunk:a-low", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_node(node("chunk:z-high", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_edge(edge(
                "low",
                "entity:mtp",
                "chunk:a-low",
                EdgeKind::Mentions,
                0.1,
            ))
            .unwrap();
        graph
            .upsert_edge(edge(
                "high",
                "entity:mtp",
                "chunk:z-high",
                EdgeKind::Mentions,
                0.9,
            ))
            .unwrap();

        let chunks = graph
            .related_chunks(&GraphNodeId::from("entity:mtp"), 1)
            .unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].vector_id.as_str(), "z-high");
    }

    #[test]
    fn test_related_chunks_deduplicates_one_hop_chunk_edges_before_limit() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:mtp", NodeKind::Entity))
            .unwrap();
        graph
            .upsert_node(node("chunk:dup", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_node(node("chunk:other", NodeKind::Chunk))
            .unwrap();
        graph
            .upsert_edge(edge(
                "dup-mentions",
                "entity:mtp",
                "chunk:dup",
                EdgeKind::Mentions,
                0.9,
            ))
            .unwrap();
        graph
            .upsert_edge(edge(
                "dup-related",
                "entity:mtp",
                "chunk:dup",
                EdgeKind::RelatedTo,
                0.8,
            ))
            .unwrap();
        graph
            .upsert_edge(edge(
                "other",
                "entity:mtp",
                "chunk:other",
                EdgeKind::Mentions,
                0.7,
            ))
            .unwrap();

        let chunks = graph
            .related_chunks(&GraphNodeId::from("entity:mtp"), 2)
            .unwrap();
        let vector_ids: Vec<&str> = chunks
            .iter()
            .map(|chunk| chunk.vector_id.as_str())
            .collect();

        assert_eq!(vector_ids, vec!["dup", "other"]);
    }

    #[test]
    fn test_delete_node_removes_attached_edges() {
        let (_dir, graph) = index();
        graph.upsert_node(node("a", NodeKind::Entity)).unwrap();
        graph.upsert_node(node("b", NodeKind::Entity)).unwrap();
        graph
            .upsert_edge(edge("ab", "a", "b", EdgeKind::RelatedTo, 1.0))
            .unwrap();

        let result = graph.delete_node(&GraphNodeId::from("a")).unwrap();
        assert!(result.deleted);
        assert_eq!(result.edges_deleted, 1);
        assert!(graph.get_node(&GraphNodeId::from("a")).unwrap().is_none());
        assert!(graph.get_edge(&GraphEdgeId::from("ab")).unwrap().is_none());
        assert!(graph
            .neighbors(NeighborRequest::new("b").with_direction(Direction::In))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_stats() {
        let (_dir, graph) = index();
        graph
            .upsert_node(node("entity:x", NodeKind::Entity))
            .unwrap();
        graph.upsert_node(node("chunk:v", NodeKind::Chunk)).unwrap();
        graph
            .upsert_edge(edge("xv", "entity:x", "chunk:v", EdgeKind::RelatedTo, 1.0))
            .unwrap();

        let stats = graph.stats().unwrap();
        assert_eq!(stats.nodes, 2);
        assert_eq!(stats.edges, 1);
        assert_eq!(stats.chunk_links, 1);
    }
}
