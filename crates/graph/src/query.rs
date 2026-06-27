//! Graph query trait and request types.

use crate::error::GraphResult;
use crate::model::{
    DeleteNodeResult, Direction, EdgeKind, GraphEdge, GraphEdgeId, GraphNeighbor, GraphNode,
    GraphNodeId, GraphPath, GraphStats, RelatedChunk,
};

pub trait GraphIndex: Send + Sync {
    fn upsert_node(&self, node: GraphNode) -> GraphResult<()>;
    fn upsert_edge(&self, edge: GraphEdge) -> GraphResult<()>;
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
