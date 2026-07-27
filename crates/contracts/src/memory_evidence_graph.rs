//! Deterministic bounded evidence-graph projection and traversal contract.
//!
//! The graph is a rebuildable, content-free view over already-authorized
//! canonical Memory records. It cannot carry credentials, grants, source
//! assurance, decision authority, or policy mutations. Callers bind one exact
//! scope and immutable record digests before projection; traversal is directed
//! and hard-bounded by depth and node count.

use crate::{MemoryScope, MAX_MEMORY_ID_BYTES, MEMORY_SCHEMA_VERSION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use thiserror::Error;

pub const MEMORY_EVIDENCE_GRAPH_CONTRACT_VERSION: u32 = 1;
pub const REFERENCE_EVIDENCE_GRAPH_ARTIFACT_ID: &str = "evidence-graph:canonical-directed-v1";
pub const MAX_EVIDENCE_GRAPH_INPUT_NODES: usize = 100_000;
pub const MAX_EVIDENCE_GRAPH_INPUT_EDGES: usize = 500_000;
pub const MAX_EVIDENCE_GRAPH_ROOTS: usize = 100;
pub const MAX_EVIDENCE_GRAPH_DEPTH: u32 = 8;
pub const MAX_EVIDENCE_GRAPH_RESULT_NODES: usize = 1_000;

#[derive(Debug, Error)]
pub enum MemoryEvidenceGraphError {
    #[error("invalid evidence graph input: {0}")]
    InvalidInput(String),
    #[error("invalid evidence graph traversal: {0}")]
    InvalidTraversal(String),
    #[error("evidence graph serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type MemoryEvidenceGraphResult<T> = Result<T, MemoryEvidenceGraphError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGraphNodeKind {
    Version,
    Evidence,
    Derivation,
    Relation,
    Reinforcement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGraphEdgeKind {
    SupportedByEvidence,
    DerivedFromVersion,
    DerivedFromEvidence,
    RelatedVersion,
    ReinforcedBy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceGraphNode {
    pub node_id: String,
    pub kind: EvidenceGraphNodeKind,
    /// SHA-256 of the corresponding immutable canonical record.
    pub canonical_record_sha256: String,
    pub committed_sequence: u64,
}

impl EvidenceGraphNode {
    fn validate(&self, visible_sequence: u64) -> MemoryEvidenceGraphResult<()> {
        validate_id("node_id", &self.node_id)?;
        validate_sha256(
            "node.canonical_record_sha256",
            &self.canonical_record_sha256,
        )?;
        if self.committed_sequence == 0 || self.committed_sequence > visible_sequence {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "node committed_sequence must be within the visible barrier".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceGraphEdge {
    pub edge_id: String,
    pub kind: EvidenceGraphEdgeKind,
    pub from_node_id: String,
    pub to_node_id: String,
    pub canonical_record_sha256: String,
    pub committed_sequence: u64,
}

impl EvidenceGraphEdge {
    fn validate(&self, visible_sequence: u64) -> MemoryEvidenceGraphResult<()> {
        validate_id("edge_id", &self.edge_id)?;
        validate_id("from_node_id", &self.from_node_id)?;
        validate_id("to_node_id", &self.to_node_id)?;
        if self.from_node_id == self.to_node_id {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "evidence graph self-edges are prohibited".to_string(),
            ));
        }
        validate_sha256(
            "edge.canonical_record_sha256",
            &self.canonical_record_sha256,
        )?;
        if self.committed_sequence == 0 || self.committed_sequence > visible_sequence {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "edge committed_sequence must be within the visible barrier".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryEvidenceGraphInput {
    pub schema_version: u32,
    pub contract_version: u32,
    pub projection_artifact_id: String,
    pub policy_manifest_id: String,
    pub scope: MemoryScope,
    pub visible_sequence: u64,
    pub root_version_ids: Vec<String>,
    pub nodes: Vec<EvidenceGraphNode>,
    pub edges: Vec<EvidenceGraphEdge>,
    pub input_sha256: String,
}

impl MemoryEvidenceGraphInput {
    pub fn validate(&self) -> MemoryEvidenceGraphResult<()> {
        if self.schema_version != MEMORY_SCHEMA_VERSION
            || self.contract_version != MEMORY_EVIDENCE_GRAPH_CONTRACT_VERSION
        {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "unsupported evidence graph schema or contract version".to_string(),
            ));
        }
        if self.projection_artifact_id != REFERENCE_EVIDENCE_GRAPH_ARTIFACT_ID {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "unknown evidence graph projection artifact".to_string(),
            ));
        }
        validate_id("policy_manifest_id", &self.policy_manifest_id)?;
        self.scope
            .validate()
            .map_err(|error| MemoryEvidenceGraphError::InvalidInput(error.to_string()))?;
        if self.visible_sequence == 0 {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "visible_sequence must be greater than zero".to_string(),
            ));
        }
        if self.root_version_ids.is_empty()
            || self.root_version_ids.len() > MAX_EVIDENCE_GRAPH_ROOTS
            || !is_strictly_sorted_unique(&self.root_version_ids)
        {
            return Err(MemoryEvidenceGraphError::InvalidInput(format!(
                "root_version_ids must contain 1..={MAX_EVIDENCE_GRAPH_ROOTS} sorted unique IDs"
            )));
        }
        if self.nodes.is_empty() || self.nodes.len() > MAX_EVIDENCE_GRAPH_INPUT_NODES {
            return Err(MemoryEvidenceGraphError::InvalidInput(format!(
                "nodes must contain 1..={MAX_EVIDENCE_GRAPH_INPUT_NODES} entries"
            )));
        }
        if self.edges.len() > MAX_EVIDENCE_GRAPH_INPUT_EDGES {
            return Err(MemoryEvidenceGraphError::InvalidInput(format!(
                "edges cannot exceed {MAX_EVIDENCE_GRAPH_INPUT_EDGES} entries"
            )));
        }

        let mut node_ids = BTreeSet::new();
        let mut version_ids = BTreeSet::new();
        for node in &self.nodes {
            node.validate(self.visible_sequence)?;
            if !node_ids.insert(node.node_id.as_str()) {
                return Err(MemoryEvidenceGraphError::InvalidInput(
                    "node IDs must be unique".to_string(),
                ));
            }
            if node.kind == EvidenceGraphNodeKind::Version {
                version_ids.insert(node.node_id.as_str());
            }
        }
        for root in &self.root_version_ids {
            validate_id("root_version_id", root)?;
            if !version_ids.contains(root.as_str()) {
                return Err(MemoryEvidenceGraphError::InvalidInput(
                    "every root must name a version node".to_string(),
                ));
            }
        }

        let mut edge_ids = BTreeSet::new();
        let mut edge_keys = BTreeSet::new();
        for edge in &self.edges {
            edge.validate(self.visible_sequence)?;
            if !node_ids.contains(edge.from_node_id.as_str())
                || !node_ids.contains(edge.to_node_id.as_str())
            {
                return Err(MemoryEvidenceGraphError::InvalidInput(
                    "edge endpoints must name nodes in the exact input set".to_string(),
                ));
            }
            if !edge_ids.insert(edge.edge_id.as_str())
                || !edge_keys.insert((
                    edge.from_node_id.as_str(),
                    edge.to_node_id.as_str(),
                    edge.kind,
                ))
            {
                return Err(MemoryEvidenceGraphError::InvalidInput(
                    "edge IDs and directed typed endpoints must be unique".to_string(),
                ));
            }
        }
        validate_sha256("input_sha256", &self.input_sha256)?;
        if self.input_sha256 != evidence_graph_input_sha256(self)? {
            return Err(MemoryEvidenceGraphError::InvalidInput(
                "input digest differs from canonical immutable fields".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryEvidenceGraphBounds {
    pub max_depth: u32,
    pub max_nodes: usize,
}

impl MemoryEvidenceGraphBounds {
    pub fn validate(self) -> MemoryEvidenceGraphResult<()> {
        if self.max_depth == 0 || self.max_depth > MAX_EVIDENCE_GRAPH_DEPTH {
            return Err(MemoryEvidenceGraphError::InvalidTraversal(format!(
                "max_depth must be between 1 and {MAX_EVIDENCE_GRAPH_DEPTH}"
            )));
        }
        if self.max_nodes == 0 || self.max_nodes > MAX_EVIDENCE_GRAPH_RESULT_NODES {
            return Err(MemoryEvidenceGraphError::InvalidTraversal(format!(
                "max_nodes must be between 1 and {MAX_EVIDENCE_GRAPH_RESULT_NODES}"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryEvidenceGraphTraversal {
    pub contract_version: u32,
    pub projection_artifact_id: String,
    pub policy_manifest_id: String,
    pub scope: MemoryScope,
    pub visible_sequence: u64,
    pub input_sha256: String,
    pub root_version_ids: Vec<String>,
    pub bounds: MemoryEvidenceGraphBounds,
    pub nodes: Vec<EvidenceGraphNode>,
    pub edges: Vec<EvidenceGraphEdge>,
    pub truncated: bool,
    pub traversal_sha256: String,
}

impl MemoryEvidenceGraphTraversal {
    pub fn validate_against(
        &self,
        input: &MemoryEvidenceGraphInput,
    ) -> MemoryEvidenceGraphResult<()> {
        input.validate()?;
        self.bounds.validate()?;
        if self.contract_version != input.contract_version
            || self.projection_artifact_id != input.projection_artifact_id
            || self.policy_manifest_id != input.policy_manifest_id
            || self.scope != input.scope
            || self.visible_sequence != input.visible_sequence
            || self.input_sha256 != input.input_sha256
            || self.root_version_ids != input.root_version_ids
            || self.nodes.len() > self.bounds.max_nodes
        {
            return Err(MemoryEvidenceGraphError::InvalidTraversal(
                "traversal expands or changes its exact input boundary".to_string(),
            ));
        }
        let (expected_nodes, expected_edges, expected_truncated) =
            reference_selection(input, self.bounds)?;
        if self.nodes != expected_nodes
            || self.edges != expected_edges
            || self.truncated != expected_truncated
        {
            return Err(MemoryEvidenceGraphError::InvalidTraversal(
                "traversal differs from the deterministic bounded projection".to_string(),
            ));
        }
        validate_sha256("traversal_sha256", &self.traversal_sha256)?;
        if self.traversal_sha256 != evidence_graph_traversal_sha256(self)? {
            return Err(MemoryEvidenceGraphError::InvalidTraversal(
                "traversal digest differs from canonical fields".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ReferenceEvidenceGraphProjection;

impl ReferenceEvidenceGraphProjection {
    pub fn traverse(
        &self,
        input: &MemoryEvidenceGraphInput,
        bounds: MemoryEvidenceGraphBounds,
    ) -> MemoryEvidenceGraphResult<MemoryEvidenceGraphTraversal> {
        input.validate()?;
        bounds.validate()?;
        let (nodes, edges, truncated) = reference_selection(input, bounds)?;
        let mut traversal = MemoryEvidenceGraphTraversal {
            contract_version: input.contract_version,
            projection_artifact_id: input.projection_artifact_id.clone(),
            policy_manifest_id: input.policy_manifest_id.clone(),
            scope: input.scope.clone(),
            visible_sequence: input.visible_sequence,
            input_sha256: input.input_sha256.clone(),
            root_version_ids: input.root_version_ids.clone(),
            bounds,
            nodes,
            edges,
            truncated,
            traversal_sha256: String::new(),
        };
        traversal.traversal_sha256 = evidence_graph_traversal_sha256(&traversal)?;
        traversal.validate_against(input)?;
        Ok(traversal)
    }
}

pub fn verify_evidence_graph_conformance(
    projection: &ReferenceEvidenceGraphProjection,
    input: &MemoryEvidenceGraphInput,
    bounds: MemoryEvidenceGraphBounds,
) -> MemoryEvidenceGraphResult<MemoryEvidenceGraphTraversal> {
    let first = projection.traverse(input, bounds)?;
    let second = projection.traverse(input, bounds)?;
    if serde_json::to_vec(&first)? != serde_json::to_vec(&second)? {
        return Err(MemoryEvidenceGraphError::InvalidTraversal(
            "evidence graph traversal is nondeterministic".to_string(),
        ));
    }
    Ok(first)
}

pub fn evidence_graph_input_sha256(
    input: &MemoryEvidenceGraphInput,
) -> MemoryEvidenceGraphResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        input.schema_version,
        input.contract_version,
        &input.projection_artifact_id,
        &input.policy_manifest_id,
        &input.scope,
        input.visible_sequence,
        &input.root_version_ids,
        &input.nodes,
        &input.edges,
    ))?))
}

pub fn evidence_graph_traversal_sha256(
    traversal: &MemoryEvidenceGraphTraversal,
) -> MemoryEvidenceGraphResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        traversal.contract_version,
        &traversal.projection_artifact_id,
        &traversal.policy_manifest_id,
        &traversal.scope,
        traversal.visible_sequence,
        &traversal.input_sha256,
        &traversal.root_version_ids,
        traversal.bounds,
        &traversal.nodes,
        &traversal.edges,
        traversal.truncated,
    ))?))
}

fn reference_selection(
    input: &MemoryEvidenceGraphInput,
    bounds: MemoryEvidenceGraphBounds,
) -> MemoryEvidenceGraphResult<(Vec<EvidenceGraphNode>, Vec<EvidenceGraphEdge>, bool)> {
    if input.root_version_ids.len() > bounds.max_nodes {
        return Err(MemoryEvidenceGraphError::InvalidTraversal(
            "max_nodes cannot be smaller than the root set".to_string(),
        ));
    }
    let node_map: BTreeMap<&str, &EvidenceGraphNode> = input
        .nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node))
        .collect();
    let mut outgoing: BTreeMap<&str, Vec<&EvidenceGraphEdge>> = BTreeMap::new();
    for edge in &input.edges {
        outgoing
            .entry(edge.from_node_id.as_str())
            .or_default()
            .push(edge);
    }
    for edges in outgoing.values_mut() {
        edges.sort_by(|left, right| {
            left.to_node_id
                .cmp(&right.to_node_id)
                .then_with(|| left.kind.cmp(&right.kind))
                .then_with(|| left.edge_id.cmp(&right.edge_id))
        });
    }

    let mut selected = BTreeSet::new();
    let mut queue = VecDeque::new();
    for root in &input.root_version_ids {
        selected.insert(root.clone());
        queue.push_back((root.clone(), 0_u32));
    }
    let mut truncated = false;
    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= bounds.max_depth {
            if outgoing.get(node_id.as_str()).is_some_and(|edges| {
                edges
                    .iter()
                    .any(|edge| !selected.contains(edge.to_node_id.as_str()))
            }) {
                truncated = true;
            }
            continue;
        }
        for edge in outgoing.get(node_id.as_str()).into_iter().flatten() {
            if selected.contains(edge.to_node_id.as_str()) {
                continue;
            }
            if selected.len() == bounds.max_nodes {
                truncated = true;
                continue;
            }
            selected.insert(edge.to_node_id.clone());
            queue.push_back((edge.to_node_id.clone(), depth + 1));
        }
    }

    let nodes = selected
        .iter()
        .map(|node_id| {
            (**node_map
                .get(node_id.as_str())
                .expect("validated graph node"))
            .clone()
        })
        .collect::<Vec<_>>();
    let mut edges = input
        .edges
        .iter()
        .filter(|edge| {
            selected.contains(edge.from_node_id.as_str())
                && selected.contains(edge.to_node_id.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    edges.sort_by(|left, right| {
        left.from_node_id
            .cmp(&right.from_node_id)
            .then_with(|| left.to_node_id.cmp(&right.to_node_id))
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.edge_id.cmp(&right.edge_id))
    });
    Ok((nodes, edges, truncated))
}

fn validate_id(field: &str, value: &str) -> MemoryEvidenceGraphResult<()> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_MEMORY_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(MemoryEvidenceGraphError::InvalidInput(format!(
            "{field} must be non-empty, trimmed, bounded text without controls"
        )));
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> MemoryEvidenceGraphResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MemoryEvidenceGraphError::InvalidInput(format!(
            "{field} must be lowercase SHA-256 hex"
        )));
    }
    Ok(())
}

fn is_strictly_sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn sha256_hex(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sensitivity;

    fn scope() -> MemoryScope {
        MemoryScope {
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            entity_key: "service:ingestion".to_string(),
            data_subject_id: None,
            owner_agent_id: Some("agent:codex".to_string()),
            session_id: Some("session-1".to_string()),
            task_id: Some("task-1".to_string()),
            sensitivity: Sensitivity::Internal,
            allowed_purposes: vec!["debugging".to_string()],
        }
    }

    fn node(id: &str, kind: EvidenceGraphNodeKind, sequence: u64) -> EvidenceGraphNode {
        EvidenceGraphNode {
            node_id: id.to_string(),
            kind,
            canonical_record_sha256: sha256_hex(id.as_bytes()),
            committed_sequence: sequence,
        }
    }

    fn edge(id: &str, kind: EvidenceGraphEdgeKind, from: &str, to: &str) -> EvidenceGraphEdge {
        EvidenceGraphEdge {
            edge_id: id.to_string(),
            kind,
            from_node_id: from.to_string(),
            to_node_id: to.to_string(),
            canonical_record_sha256: sha256_hex(id.as_bytes()),
            committed_sequence: 4,
        }
    }

    fn input() -> MemoryEvidenceGraphInput {
        let mut input = MemoryEvidenceGraphInput {
            schema_version: MEMORY_SCHEMA_VERSION,
            contract_version: MEMORY_EVIDENCE_GRAPH_CONTRACT_VERSION,
            projection_artifact_id: REFERENCE_EVIDENCE_GRAPH_ARTIFACT_ID.to_string(),
            policy_manifest_id: "memory-authority-policy-v1".to_string(),
            scope: scope(),
            visible_sequence: 4,
            root_version_ids: vec!["version-a".to_string()],
            nodes: vec![
                node("derivation-a", EvidenceGraphNodeKind::Derivation, 3),
                node("evidence-a", EvidenceGraphNodeKind::Evidence, 1),
                node("evidence-b", EvidenceGraphNodeKind::Evidence, 2),
                node("version-a", EvidenceGraphNodeKind::Version, 4),
                node("version-b", EvidenceGraphNodeKind::Version, 2),
            ],
            edges: vec![
                edge(
                    "edge-a",
                    EvidenceGraphEdgeKind::SupportedByEvidence,
                    "version-a",
                    "evidence-a",
                ),
                edge(
                    "edge-b",
                    EvidenceGraphEdgeKind::DerivedFromVersion,
                    "version-a",
                    "version-b",
                ),
                edge(
                    "edge-c",
                    EvidenceGraphEdgeKind::SupportedByEvidence,
                    "version-b",
                    "evidence-b",
                ),
                edge(
                    "edge-d",
                    EvidenceGraphEdgeKind::DerivedFromEvidence,
                    "version-a",
                    "derivation-a",
                ),
            ],
            input_sha256: String::new(),
        };
        input.input_sha256 = evidence_graph_input_sha256(&input).unwrap();
        input
    }

    #[test]
    fn deterministic_traversal_is_depth_and_node_bounded() {
        let input = input();
        let traversal = verify_evidence_graph_conformance(
            &ReferenceEvidenceGraphProjection,
            &input,
            MemoryEvidenceGraphBounds {
                max_depth: 1,
                max_nodes: 3,
            },
        )
        .unwrap();
        assert_eq!(traversal.nodes.len(), 3);
        assert!(traversal.truncated);
        assert_eq!(traversal.scope, scope());
        assert_eq!(traversal.traversal_sha256.len(), 64);
    }

    #[test]
    fn graph_cannot_cross_scope_or_smuggle_authority() {
        let input = input();
        let traversal = ReferenceEvidenceGraphProjection
            .traverse(
                &input,
                MemoryEvidenceGraphBounds {
                    max_depth: 4,
                    max_nodes: 20,
                },
            )
            .unwrap();
        let encoded = serde_json::to_string(&traversal).unwrap();
        for forbidden in [
            "source_assurance",
            "decision_authority",
            "credential",
            "capability_grant",
        ] {
            assert!(!encoded.contains(forbidden));
        }

        let mut forged = traversal;
        forged.scope.namespace = "other".to_string();
        forged.traversal_sha256 = evidence_graph_traversal_sha256(&forged).unwrap();
        assert!(forged.validate_against(&input).is_err());
    }

    #[test]
    fn dangling_edges_and_unbounded_requests_fail_closed() {
        let mut dangling = input();
        dangling.edges[0].to_node_id = "missing".to_string();
        dangling.input_sha256 = evidence_graph_input_sha256(&dangling).unwrap();
        assert!(dangling.validate().is_err());

        assert!(ReferenceEvidenceGraphProjection
            .traverse(
                &input(),
                MemoryEvidenceGraphBounds {
                    max_depth: MAX_EVIDENCE_GRAPH_DEPTH + 1,
                    max_nodes: 10,
                },
            )
            .is_err());
    }
}
