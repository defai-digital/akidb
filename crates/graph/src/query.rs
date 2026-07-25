//! Graph query trait and request types.

use crate::error::GraphResult;
use crate::model::{
    DeleteNodeResult, Direction, EdgeKind, GraphEdge, GraphEdgeId, GraphMutationBatch,
    GraphNeighbor, GraphNode, GraphNodeId, GraphPath, GraphStats, RelatedChunk,
};
use std::collections::{HashSet, VecDeque};

pub trait GraphIndex: Send + Sync {
    fn upsert_node(&self, node: GraphNode) -> GraphResult<()>;
    fn upsert_edge(&self, edge: GraphEdge) -> GraphResult<()>;
    /// Apply one graph projection batch.
    ///
    /// Backends should override this method when they can commit the batch
    /// atomically. The default preserves compatibility for lightweight custom
    /// implementations while keeping the same observable upsert semantics.
    fn upsert_batch(&self, batch: GraphMutationBatch) -> GraphResult<()> {
        for edge_id in batch.delete_edges {
            self.delete_edge(&edge_id)?;
        }
        for node in batch.nodes {
            self.upsert_node(node)?;
        }
        for edge in batch.edges {
            self.upsert_edge(edge)?;
        }
        Ok(())
    }
    fn get_node(&self, node_id: &GraphNodeId) -> GraphResult<Option<GraphNode>>;
    fn get_edge(&self, edge_id: &GraphEdgeId) -> GraphResult<Option<GraphEdge>>;
    fn delete_node(&self, node_id: &GraphNodeId) -> GraphResult<DeleteNodeResult>;
    fn delete_edge(&self, edge_id: &GraphEdgeId) -> GraphResult<bool>;
    fn neighbors(&self, request: NeighborRequest) -> GraphResult<Vec<GraphNeighbor>>;
    fn two_hop(&self, request: TwoHopRequest) -> GraphResult<Vec<GraphPath>>;
    fn related_chunks(
        &self,
        entity_id: &GraphNodeId,
        limit: usize,
    ) -> GraphResult<Vec<RelatedChunk>>;
    /// Traverse a bounded neighborhood and return reachable chunk ids.
    fn related_chunks_with_depth(
        &self,
        request: RelatedChunksRequest,
    ) -> GraphResult<Vec<RelatedChunk>> {
        if request.limit == 0 || request.max_depth == 0 {
            return Ok(Vec::new());
        }
        if request.max_depth > 3 {
            return Err(crate::error::GraphError::InvalidRequest(
                "related_chunks max_depth is capped at 3".to_string(),
            ));
        }

        let mut chunks = Vec::new();
        let mut seen_chunks = HashSet::new();
        let mut visited = HashSet::from([request.node_id.clone()]);
        let mut queue = VecDeque::from([(request.node_id.clone(), 0u8)]);

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= request.max_depth {
                continue;
            }
            let neighbors = self.neighbors(
                NeighborRequest::new(node_id)
                    .with_direction(Direction::Both)
                    .with_edge_kinds(request.edge_kinds.clone())
                    .with_limit(request.per_hop_limit),
            )?;
            for neighbor in neighbors {
                let neighbor_id = neighbor.node.id;
                if !request.node_id.is_same_workspace(&neighbor_id) {
                    continue;
                }
                if let Some(vector_id) = neighbor_id.as_chunk_vector_id() {
                    if neighbor_id != request.node_id && seen_chunks.insert(vector_id.clone()) {
                        chunks.push(RelatedChunk {
                            vector_id,
                            via_node: neighbor_id.clone(),
                        });
                        if chunks.len() >= request.limit {
                            return Ok(chunks);
                        }
                    }
                }
                if depth + 1 < request.max_depth && visited.insert(neighbor_id.clone()) {
                    queue.push_back((neighbor_id, depth + 1));
                }
            }
        }

        Ok(chunks)
    }
    fn path_exists(&self, request: PathExistsRequest) -> GraphResult<bool>;
    fn stats(&self) -> GraphResult<GraphStats>;
}

#[derive(Debug, Clone)]
pub struct NeighborRequest {
    pub node_id: GraphNodeId,
    pub direction: Direction,
    pub edge_kinds: Vec<EdgeKind>,
    pub limit: usize,
    pub min_weight: Option<f32>,
}

impl NeighborRequest {
    pub fn new(node_id: impl Into<GraphNodeId>) -> Self {
        Self {
            node_id: node_id.into(),
            direction: Direction::Both,
            edge_kinds: Vec::new(),
            limit: 100,
            min_weight: None,
        }
    }

    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    pub fn with_edge_kinds(mut self, edge_kinds: Vec<EdgeKind>) -> Self {
        self.edge_kinds = edge_kinds;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_min_weight(mut self, min_weight: f32) -> Self {
        self.min_weight = Some(min_weight);
        self
    }
}

#[derive(Debug, Clone)]
pub struct TwoHopRequest {
    pub node_id: GraphNodeId,
    pub edge_kinds: Vec<EdgeKind>,
    pub first_hop_limit: usize,
    pub second_hop_limit: usize,
    pub limit: usize,
}

impl TwoHopRequest {
    pub fn new(node_id: impl Into<GraphNodeId>) -> Self {
        Self {
            node_id: node_id.into(),
            edge_kinds: Vec::new(),
            first_hop_limit: 50,
            second_hop_limit: 20,
            limit: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelatedChunksRequest {
    pub node_id: GraphNodeId,
    pub edge_kinds: Vec<EdgeKind>,
    pub max_depth: u8,
    pub per_hop_limit: usize,
    pub limit: usize,
}

impl RelatedChunksRequest {
    pub fn new(node_id: impl Into<GraphNodeId>) -> Self {
        Self {
            node_id: node_id.into(),
            edge_kinds: Vec::new(),
            max_depth: 1,
            per_hop_limit: 256,
            limit: 100,
        }
    }

    pub fn with_edge_kinds(mut self, edge_kinds: Vec<EdgeKind>) -> Self {
        self.edge_kinds = edge_kinds;
        self
    }

    pub fn with_max_depth(mut self, max_depth: u8) -> Self {
        self.max_depth = max_depth;
        self
    }

    pub fn with_per_hop_limit(mut self, per_hop_limit: usize) -> Self {
        self.per_hop_limit = per_hop_limit;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Debug, Clone)]
pub struct PathExistsRequest {
    pub from: GraphNodeId,
    pub to: GraphNodeId,
    pub edge_kinds: Vec<EdgeKind>,
    pub max_depth: u8,
}

impl PathExistsRequest {
    pub fn new(from: impl Into<GraphNodeId>, to: impl Into<GraphNodeId>, max_depth: u8) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            edge_kinds: Vec::new(),
            max_depth,
        }
    }
}
