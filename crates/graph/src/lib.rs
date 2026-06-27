//! Graph retrieval primitives for AkiDB.
//!
//! This crate owns AkiDB's bounded GraphRAG graph contract. The default
//! implementation is a lightweight RocksDB-compatible adjacency index built on
//! the existing storage abstraction. External graph engines can implement the
//! same trait later without becoming part of the default hot path.

pub mod error;
mod keys;
#[cfg(feature = "kuzu")]
pub mod kuzu;
pub mod model;
pub mod native;
pub mod query;

pub use error::{GraphError, GraphResult};
pub use model::{
    DeleteNodeResult, Direction, EdgeKind, GraphEdge, GraphEdgeId, GraphNeighbor, GraphNode,
    GraphNodeId, GraphPath, GraphStats, NodeKind, RelatedChunk,
};
pub use native::NativeGraphIndex;
pub use query::{GraphIndex, NeighborRequest, PathExistsRequest, TwoHopRequest};
