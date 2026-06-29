//! Storage key construction.

use crate::model::{EdgeKind, GraphEdgeId, GraphNodeId, NodeKind};

const NODE_PREFIX: &str = "g:node:";
const EDGE_PREFIX: &str = "g:edge:";
const OUT_PREFIX: &str = "g:out:";
const IN_PREFIX: &str = "g:in:";
const CHUNK_PREFIX: &str = "g:chunk:";
const KIND_PREFIX: &str = "g:kind:";

fn component(value: &str) -> String {
    format!("{}:{}:", value.len(), value)
}

pub fn node_key(id: &GraphNodeId) -> Vec<u8> {
    format!("{NODE_PREFIX}{}", id.as_str()).into_bytes()
}

pub fn node_prefix() -> &'static [u8] {
    NODE_PREFIX.as_bytes()
}

pub fn edge_key(id: &GraphEdgeId) -> Vec<u8> {
    format!("{EDGE_PREFIX}{}", id.as_str()).into_bytes()
}

pub fn edge_prefix() -> &'static [u8] {
    EDGE_PREFIX.as_bytes()
}

pub fn adjacency_key_out(from: &GraphNodeId, kind: EdgeKind, edge_id: &GraphEdgeId) -> Vec<u8> {
    format!(
        "{OUT_PREFIX}{}{}:{}",
        component(from.as_str()),
        kind.as_key(),
        edge_id.as_str()
    )
    .into_bytes()
}

pub fn adjacency_key_in(to: &GraphNodeId, kind: EdgeKind, edge_id: &GraphEdgeId) -> Vec<u8> {
    format!(
        "{IN_PREFIX}{}{}:{}",
        component(to.as_str()),
        kind.as_key(),
        edge_id.as_str()
    )
    .into_bytes()
}

pub fn adjacency_prefix_out(node: &GraphNodeId, kind: Option<EdgeKind>) -> Vec<u8> {
    match kind {
        Some(kind) => {
            format!("{OUT_PREFIX}{}{}:", component(node.as_str()), kind.as_key()).into_bytes()
        }
        None => format!("{OUT_PREFIX}{}", component(node.as_str())).into_bytes(),
    }
}

pub fn adjacency_prefix_in(node: &GraphNodeId, kind: Option<EdgeKind>) -> Vec<u8> {
    match kind {
        Some(kind) => {
            format!("{IN_PREFIX}{}{}:", component(node.as_str()), kind.as_key()).into_bytes()
        }
        None => format!("{IN_PREFIX}{}", component(node.as_str())).into_bytes(),
    }
}

pub fn chunk_key(entity: &GraphNodeId, chunk: &GraphNodeId) -> Vec<u8> {
    format!(
        "{CHUNK_PREFIX}{}{}",
        component(entity.as_str()),
        component(chunk.as_str())
    )
    .into_bytes()
}

pub fn chunk_prefix(entity: &GraphNodeId) -> Vec<u8> {
    format!("{CHUNK_PREFIX}{}", component(entity.as_str())).into_bytes()
}

pub fn chunk_all_prefix() -> &'static [u8] {
    CHUNK_PREFIX.as_bytes()
}

pub fn kind_key(kind: NodeKind, node_id: &GraphNodeId) -> Vec<u8> {
    format!("{KIND_PREFIX}{}:{}", kind.as_key(), node_id.as_str()).into_bytes()
}
