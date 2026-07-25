//! Logical generation-bundle contract.
//!
//! A bundle is an ordered NDJSON stream whose first entry is a header,
//! followed by records, graph nodes, and graph edges. It contains logical
//! retrieval data only: engine-specific RocksDB, HNSW, and lexical index files
//! are deliberately excluded so replicas can rebuild across versions and CPU
//! architectures.

use crate::error::{ContractResult, ContractViolation, ContractViolationKind};
use crate::knowledge::{
    deserialize_present_value, validate_identifier, validate_nonempty_text,
    validate_safe_json_integer, validate_schema_version, validate_sha256, validate_timestamp,
    violation, KnowledgeGenerationManifest, KnowledgeScope, MAX_EMBEDDING_DIMENSIONS,
    MAX_GRAPH_SCHEMA_BYTES, MAX_ID_BYTES, MAX_MODEL_ID_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use url::Url;

const MAX_PIPELINE_SIGNATURE_BYTES: usize = 1_024;
const MAX_PREDICATE_BYTES: usize = 255;
const MAX_SOURCE_URI_BYTES: usize = 4_096;
const MAX_EXTRACTOR_BYTES: usize = 1_024;
const MAX_CHUNK_TEXT_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONTEXT_HEADINGS_BYTES: usize = 64 * 1024;
const MAX_JSON_PROPERTIES_BYTES: usize = 1024 * 1024;
const MAX_EVIDENCE_CHUNKS: usize = 64;

/// Header that must be the first line of a knowledge bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeBundleHeader {
    pub schema_version: u32,
    pub workspace_id: String,
    pub collection: String,
    pub generation_id: String,
    pub embedding_model_id: String,
    pub embedding_dimensions: u32,
    pub graph_schema_version: String,
    pub base_sequence: u64,
    pub record_count: u64,
    pub node_count: u64,
    pub edge_count: u64,
}

impl KnowledgeBundleHeader {
    pub fn scope(&self) -> KnowledgeScope {
        KnowledgeScope::new(self.workspace_id.clone(), self.collection.clone())
    }

    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        self.scope().validate()?;
        validate_identifier("generation_id", &self.generation_id, MAX_ID_BYTES)?;
        validate_identifier(
            "embedding_model_id",
            &self.embedding_model_id,
            MAX_MODEL_ID_BYTES,
        )?;
        if self.embedding_dimensions == 0 || self.embedding_dimensions > MAX_EMBEDDING_DIMENSIONS {
            return Err(violation(
                "embedding_dimensions",
                format!("embedding_dimensions must be between 1 and {MAX_EMBEDDING_DIMENSIONS}"),
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_identifier(
            "graph_schema_version",
            &self.graph_schema_version,
            MAX_GRAPH_SCHEMA_BYTES,
        )?;
        validate_safe_json_integer("base_sequence", self.base_sequence)?;
        validate_safe_json_integer("record_count", self.record_count)?;
        validate_safe_json_integer("node_count", self.node_count)?;
        validate_safe_json_integer("edge_count", self.edge_count)
    }

    pub fn validate_against(&self, manifest: &KnowledgeGenerationManifest) -> ContractResult<()> {
        self.validate()?;
        manifest.validate()?;
        require_equal("workspace_id", &self.workspace_id, &manifest.workspace_id)?;
        require_equal("collection", &self.collection, &manifest.collection)?;
        require_equal(
            "generation_id",
            &self.generation_id,
            &manifest.generation_id,
        )?;
        require_equal(
            "embedding_model_id",
            &self.embedding_model_id,
            &manifest.embedding_model_id,
        )?;
        if self.embedding_dimensions != manifest.embedding_dimensions {
            return Err(mismatch(
                "embedding_dimensions",
                self.embedding_dimensions,
                manifest.embedding_dimensions,
            ));
        }
        require_equal(
            "graph_schema_version",
            &self.graph_schema_version,
            &manifest.graph_schema_version,
        )?;
        if self.base_sequence != manifest.base_sequence {
            return Err(mismatch(
                "base_sequence",
                self.base_sequence,
                manifest.base_sequence,
            ));
        }
        if self.record_count != manifest.expected_vector_count {
            return Err(mismatch(
                "record_count",
                self.record_count,
                manifest.expected_vector_count,
            ));
        }
        if self.edge_count != manifest.expected_edge_count {
            return Err(mismatch(
                "edge_count",
                self.edge_count,
                manifest.expected_edge_count,
            ));
        }
        Ok(())
    }
}

/// Complete AX Record identity and retrieval payload for one visible chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeBundleRecord {
    pub chunk_id: String,
    pub doc_id: String,
    pub doc_version: String,
    pub chunk_hash: String,
    pub pipeline_signature: String,
    pub embedding_model_id: String,
    pub vector: Vec<f32>,
    pub metadata: Map<String, Value>,
    pub chunk_text: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub context_headings: Option<String>,
}

impl KnowledgeBundleRecord {
    pub fn validate(&self, header: &KnowledgeBundleHeader) -> ContractResult<()> {
        validate_identifier("chunk_id", &self.chunk_id, MAX_ID_BYTES)?;
        validate_identifier("doc_id", &self.doc_id, MAX_ID_BYTES)?;
        validate_identifier("doc_version", &self.doc_version, MAX_ID_BYTES)?;
        validate_sha256("chunk_hash", &self.chunk_hash)?;
        validate_identifier(
            "pipeline_signature",
            &self.pipeline_signature,
            MAX_PIPELINE_SIGNATURE_BYTES,
        )?;
        validate_identifier(
            "embedding_model_id",
            &self.embedding_model_id,
            MAX_MODEL_ID_BYTES,
        )?;
        require_equal(
            "embedding_model_id",
            &self.embedding_model_id,
            &header.embedding_model_id,
        )?;
        if self.vector.len() != header.embedding_dimensions as usize {
            return Err(violation(
                "vector",
                format!(
                    "vector dimensions {} do not match bundle dimensions {}",
                    self.vector.len(),
                    header.embedding_dimensions
                ),
                ContractViolationKind::InvalidFormat,
            ));
        }
        if let Some(index) = self.vector.iter().position(|value| !value.is_finite()) {
            return Err(ContractViolation::invalid_number("vector", index));
        }
        validate_json_size("metadata", &self.metadata, MAX_JSON_PROPERTIES_BYTES)?;
        let source_uri = self
            .metadata
            .get("source_uri")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                violation(
                    "metadata.source_uri",
                    "metadata.source_uri must be a string",
                    ContractViolationKind::Empty,
                )
            })?;
        validate_source_uri(source_uri)?;
        validate_bounded_text("chunk_text", &self.chunk_text, MAX_CHUNK_TEXT_BYTES, false)?;
        if let Some(headings) = &self.context_headings {
            validate_bounded_text(
                "context_headings",
                headings,
                MAX_CONTEXT_HEADINGS_BYTES,
                true,
            )?;
        }
        Ok(())
    }
}

/// Retrieval graph node kind. Domain-specific entity types belong in
/// `properties.entity_type`, keeping the core schema bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeNodeKind {
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

/// Logical graph node included in a generation bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeBundleNode {
    pub node_id: String,
    pub kind: KnowledgeNodeKind,
    pub properties: Map<String, Value>,
}

impl KnowledgeBundleNode {
    pub fn validate(&self) -> ContractResult<()> {
        validate_identifier("node_id", &self.node_id, MAX_ID_BYTES)?;
        validate_json_size("properties", &self.properties, MAX_JSON_PROPERTIES_BYTES)
    }
}

/// Bounded core edge kinds. Domain predicates are carried separately so new
/// business concepts do not require a core enum change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeEdgeKind {
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

/// Provenance class for a graph assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeAssertionState {
    Asserted,
    Extracted,
    Inferred,
    HumanVerified,
}

/// Typed, evidence-bearing graph edge included in a generation bundle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeBundleEdge {
    pub edge_id: String,
    pub from_node_id: String,
    pub to_node_id: String,
    pub kind: KnowledgeEdgeKind,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub predicate: Option<String>,
    pub weight: f32,
    pub confidence: f32,
    pub assertion_state: KnowledgeAssertionState,
    pub source_uri: String,
    pub source_version: String,
    pub evidence_chunk_ids: Vec<String>,
    pub extractor: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub valid_from_ms: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_value"
    )]
    pub valid_to_ms: Option<u64>,
    pub observed_at_ms: u64,
    pub properties: Map<String, Value>,
}

impl KnowledgeBundleEdge {
    pub fn validate(&self) -> ContractResult<()> {
        validate_identifier("edge_id", &self.edge_id, MAX_ID_BYTES)?;
        validate_identifier("from_node_id", &self.from_node_id, MAX_ID_BYTES)?;
        validate_identifier("to_node_id", &self.to_node_id, MAX_ID_BYTES)?;
        if let Some(predicate) = &self.predicate {
            validate_identifier("predicate", predicate, MAX_PREDICATE_BYTES)?;
        } else if self.kind == KnowledgeEdgeKind::RelatedTo {
            return Err(violation(
                "predicate",
                "related_to edges require a domain predicate",
                ContractViolationKind::Empty,
            ));
        }
        validate_unit_interval("weight", self.weight)?;
        validate_unit_interval("confidence", self.confidence)?;
        validate_source_uri(&self.source_uri)?;
        validate_identifier("source_version", &self.source_version, MAX_ID_BYTES)?;
        if self.evidence_chunk_ids.is_empty() {
            return Err(ContractViolation::empty("evidence_chunk_ids"));
        }
        if self.evidence_chunk_ids.len() > MAX_EVIDENCE_CHUNKS {
            return Err(ContractViolation::exceeds_maximum(
                "evidence_chunk_ids",
                self.evidence_chunk_ids.len(),
                MAX_EVIDENCE_CHUNKS,
            ));
        }
        let mut previous: Option<&str> = None;
        for chunk_id in &self.evidence_chunk_ids {
            validate_identifier("evidence_chunk_ids", chunk_id, MAX_ID_BYTES)?;
            if previous.is_some_and(|value| value >= chunk_id.as_str()) {
                return Err(violation(
                    "evidence_chunk_ids",
                    "evidence_chunk_ids must be strictly sorted and unique",
                    ContractViolationKind::InvalidFormat,
                ));
            }
            previous = Some(chunk_id);
        }
        validate_identifier("extractor", &self.extractor, MAX_EXTRACTOR_BYTES)?;
        if let Some(valid_from_ms) = self.valid_from_ms {
            validate_timestamp("valid_from_ms", valid_from_ms)?;
        }
        if let Some(valid_to_ms) = self.valid_to_ms {
            validate_timestamp("valid_to_ms", valid_to_ms)?;
        }
        if let (Some(valid_from_ms), Some(valid_to_ms)) = (self.valid_from_ms, self.valid_to_ms) {
            if valid_to_ms < valid_from_ms {
                return Err(violation(
                    "valid_to_ms",
                    "valid_to_ms must be greater than or equal to valid_from_ms",
                    ContractViolationKind::BelowMinimum,
                ));
            }
        }
        validate_timestamp("observed_at_ms", self.observed_at_ms)?;
        validate_json_size("properties", &self.properties, MAX_JSON_PROPERTIES_BYTES)
    }
}

/// One line in the ordered NDJSON generation stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "entry_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KnowledgeBundleEntry {
    Header { header: KnowledgeBundleHeader },
    Record { record: KnowledgeBundleRecord },
    Node { node: KnowledgeBundleNode },
    Edge { edge: KnowledgeBundleEdge },
}

impl KnowledgeBundleEntry {
    pub fn stable_id(&self) -> Option<&str> {
        match self {
            Self::Header { .. } => None,
            Self::Record { record } => Some(&record.chunk_id),
            Self::Node { node } => Some(&node.node_id),
            Self::Edge { edge } => Some(&edge.edge_id),
        }
    }
}

fn require_equal(field: &'static str, actual: &str, expected: &str) -> ContractResult<()> {
    if actual != expected {
        return Err(violation(
            field,
            format!("{field} does not match the generation manifest"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn mismatch(
    field: &'static str,
    actual: impl std::fmt::Display,
    expected: impl std::fmt::Display,
) -> ContractViolation {
    violation(
        field,
        format!("{field} {actual} does not match expected {expected}"),
        ContractViolationKind::InvalidFormat,
    )
}

fn validate_bounded_text(
    field: &'static str,
    value: &str,
    maximum: usize,
    nonempty: bool,
) -> ContractResult<()> {
    if nonempty {
        validate_nonempty_text(field, value, maximum)?;
    } else if value.len() > maximum {
        return Err(ContractViolation::exceeds_maximum(
            field,
            value.len(),
            maximum,
        ));
    }
    if value.chars().any(|character| character == '\0') {
        return Err(violation(
            field,
            format!("{field} must not contain NUL characters"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_json_size<T: Serialize>(
    field: &'static str,
    value: &T,
    maximum: usize,
) -> ContractResult<()> {
    let size = serde_json::to_vec(value)
        .map_err(|error| {
            violation(
                field,
                format!("{field} cannot be serialized: {error}"),
                ContractViolationKind::InvalidFormat,
            )
        })?
        .len();
    if size > maximum {
        return Err(ContractViolation::exceeds_maximum(field, size, maximum));
    }
    Ok(())
}

fn validate_source_uri(value: &str) -> ContractResult<()> {
    validate_nonempty_text("source_uri", value, MAX_SOURCE_URI_BYTES)?;
    if value.chars().any(char::is_control) {
        return Err(violation(
            "source_uri",
            "source_uri must not contain control characters",
            ContractViolationKind::InvalidFormat,
        ));
    }
    let parsed = Url::parse(value).map_err(|_| {
        violation(
            "source_uri",
            "source_uri must be an absolute canonical-source URI",
            ContractViolationKind::InvalidFormat,
        )
    })?;
    if !matches!(parsed.scheme(), "s3" | "https" | "openwiki") {
        return Err(violation(
            "source_uri",
            "source_uri scheme must be s3, https, or openwiki",
            ContractViolationKind::InvalidFormat,
        ));
    }
    if parsed.host_str().is_none() || parsed.path().is_empty() || parsed.path() == "/" {
        return Err(violation(
            "source_uri",
            "source_uri must include an authority and object path",
            ContractViolationKind::InvalidFormat,
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err(violation(
            "source_uri",
            "source_uri must not contain credentials or a fragment",
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_unit_interval(field: &'static str, value: f32) -> ContractResult<()> {
    if !value.is_finite() {
        return Err(ContractViolation::invalid_number(field, 0));
    }
    if !(0.0..=1.0).contains(&value) {
        return Err(violation(
            field,
            format!("{field} must be between 0 and 1"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KnowledgeBundleCompression, KnowledgeBundleFormat, KNOWLEDGE_SCHEMA_VERSION};

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn manifest() -> KnowledgeGenerationManifest {
        KnowledgeGenerationManifest {
            schema_version: KNOWLEDGE_SCHEMA_VERSION,
            workspace_id: "workspace-a".to_string(),
            collection: "knowledge".to_string(),
            generation_id: "generation-a".to_string(),
            parent_generation_id: None,
            created_at_ms: 1_784_995_200_000,
            embedding_model_id: "model@revision".to_string(),
            embedding_dimensions: 3,
            graph_schema_version: "ax.knowledge-graph.v1".to_string(),
            bundle_format: KnowledgeBundleFormat::NdjsonV1,
            bundle_compression: KnowledgeBundleCompression::None,
            bundle: crate::ImmutableObjectReference {
                uri: "s3://knowledge/generations/generation-a/bundle.ndjson".to_string(),
                sha256: DIGEST.to_string(),
                size_bytes: 1_024,
            },
            base_sequence: 10,
            target_sequence: 10,
            expected_vector_count: 1,
            expected_edge_count: 1,
        }
    }

    fn header() -> KnowledgeBundleHeader {
        KnowledgeBundleHeader {
            schema_version: KNOWLEDGE_SCHEMA_VERSION,
            workspace_id: "workspace-a".to_string(),
            collection: "knowledge".to_string(),
            generation_id: "generation-a".to_string(),
            embedding_model_id: "model@revision".to_string(),
            embedding_dimensions: 3,
            graph_schema_version: "ax.knowledge-graph.v1".to_string(),
            base_sequence: 10,
            record_count: 1,
            node_count: 2,
            edge_count: 1,
        }
    }

    fn record() -> KnowledgeBundleRecord {
        KnowledgeBundleRecord {
            chunk_id: "chunk-a".to_string(),
            doc_id: "doc-a".to_string(),
            doc_version: "version-a".to_string(),
            chunk_hash: DIGEST.to_string(),
            pipeline_signature: "pipeline-v1".to_string(),
            embedding_model_id: "model@revision".to_string(),
            vector: vec![0.1, 0.2, 0.3],
            metadata: Map::from_iter([(
                "source_uri".to_string(),
                Value::String("s3://knowledge/documents/doc-a".to_string()),
            )]),
            chunk_text: "grounded text".to_string(),
            context_headings: Some("Document > Section".to_string()),
        }
    }

    fn edge() -> KnowledgeBundleEdge {
        KnowledgeBundleEdge {
            edge_id: "edge-a".to_string(),
            from_node_id: "chunk-a".to_string(),
            to_node_id: "entity-a".to_string(),
            kind: KnowledgeEdgeKind::RelatedTo,
            predicate: Some("mentions_product".to_string()),
            weight: 0.9,
            confidence: 0.95,
            assertion_state: KnowledgeAssertionState::Extracted,
            source_uri: "s3://knowledge/documents/doc-a".to_string(),
            source_version: "version-a".to_string(),
            evidence_chunk_ids: vec!["chunk-a".to_string()],
            extractor: "ner-v1".to_string(),
            valid_from_ms: None,
            valid_to_ms: None,
            observed_at_ms: 1_784_995_200_000,
            properties: Map::new(),
        }
    }

    #[test]
    fn header_and_record_match_manifest() {
        header().validate_against(&manifest()).unwrap();
        record().validate(&header()).unwrap();
        edge().validate().unwrap();
    }

    #[test]
    fn rejects_dimension_model_and_count_mismatch() {
        let mut bad_record = record();
        bad_record.vector.pop();
        assert_eq!(bad_record.validate(&header()).unwrap_err().field, "vector");

        let mut bad_header = header();
        bad_header.record_count = 2;
        assert_eq!(
            bad_header.validate_against(&manifest()).unwrap_err().field,
            "record_count"
        );
    }

    #[test]
    fn rejects_ambiguous_or_evidence_free_edges() {
        let mut bad_edge = edge();
        bad_edge.predicate = None;
        assert_eq!(bad_edge.validate().unwrap_err().field, "predicate");

        let mut bad_edge = edge();
        bad_edge.evidence_chunk_ids.clear();
        assert_eq!(bad_edge.validate().unwrap_err().field, "evidence_chunk_ids");
    }

    #[test]
    fn rejects_unknown_and_explicit_null_fields() {
        let value =
            serde_json::to_value(KnowledgeBundleEntry::Record { record: record() }).unwrap();
        let mut unknown = value.clone();
        unknown["unexpected"] = Value::Bool(true);
        assert!(serde_json::from_value::<KnowledgeBundleEntry>(unknown).is_err());

        let mut explicit_null = value;
        explicit_null["record"]["context_headings"] = Value::Null;
        assert!(serde_json::from_value::<KnowledgeBundleEntry>(explicit_null).is_err());
    }
}
