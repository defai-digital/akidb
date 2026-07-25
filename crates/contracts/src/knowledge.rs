//! Versioned contracts for rebuildable agentic-knowledge serving.
//!
//! These types are the durable boundary between canonical knowledge producers,
//! the publication control plane, and independently rebuilt AkiDB replicas.
//! Validation is deliberately strict so malformed or ambiguous state cannot
//! enter the generation state machine.

use crate::error::{ContractResult, ContractViolation, ContractViolationKind};
use serde::{Deserialize, Serialize};
use url::Url;

/// Initial agentic-knowledge contract version.
pub const KNOWLEDGE_SCHEMA_VERSION: u32 = 1;

/// Largest integer that all JSON consumers, including JavaScript, preserve
/// without precision loss.
pub const MAX_SAFE_JSON_INTEGER: u64 = 9_007_199_254_740_991;

const MAX_SCOPE_BYTES: usize = 255;
const MAX_ID_BYTES: usize = 1_024;
const MAX_MODEL_ID_BYTES: usize = 512;
const MAX_GRAPH_SCHEMA_BYTES: usize = 512;
const MAX_URI_BYTES: usize = 4_096;
const MAX_FAILURE_BYTES: usize = 16_384;
const MAX_EMBEDDING_DIMENSIONS: u32 = 16_384;

/// A workspace/collection stream is the unit of ordering and activation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeScope {
    pub workspace_id: String,
    pub collection: String,
}

impl KnowledgeScope {
    pub fn new(workspace_id: impl Into<String>, collection: impl Into<String>) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            collection: collection.into(),
        }
    }

    pub fn validate(&self) -> ContractResult<()> {
        validate_identifier("workspace_id", &self.workspace_id, MAX_SCOPE_BYTES)?;
        validate_identifier("collection", &self.collection, MAX_SCOPE_BYTES)
    }
}

/// An immutable, checksum-addressed object stored outside AkiDB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImmutableObjectReference {
    pub uri: String,
    pub sha256: String,
    pub size_bytes: u64,
}

impl ImmutableObjectReference {
    pub fn validate(&self) -> ContractResult<()> {
        validate_uri(&self.uri)?;
        validate_sha256("sha256", &self.sha256)?;
        if self.size_bytes == 0 {
            return Err(violation(
                "size_bytes",
                "size_bytes must be greater than zero",
                ContractViolationKind::BelowMinimum,
            ));
        }
        validate_safe_json_integer("size_bytes", self.size_bytes)?;
        Ok(())
    }
}

/// Immutable description of one rebuildable knowledge generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeGenerationManifest {
    pub schema_version: u32,
    pub workspace_id: String,
    pub collection: String,
    pub generation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_generation_id: Option<String>,
    pub created_at_ms: u64,
    pub embedding_model_id: String,
    pub embedding_dimensions: u32,
    pub graph_schema_version: String,
    pub bundle: ImmutableObjectReference,
    pub base_sequence: u64,
    pub target_sequence: u64,
    pub expected_vector_count: u64,
    pub expected_edge_count: u64,
}

impl KnowledgeGenerationManifest {
    pub fn scope(&self) -> KnowledgeScope {
        KnowledgeScope::new(self.workspace_id.clone(), self.collection.clone())
    }

    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        self.scope().validate()?;
        validate_identifier("generation_id", &self.generation_id, MAX_ID_BYTES)?;
        if let Some(parent) = &self.parent_generation_id {
            validate_identifier("parent_generation_id", parent, MAX_ID_BYTES)?;
            if parent == &self.generation_id {
                return Err(violation(
                    "parent_generation_id",
                    "parent_generation_id must differ from generation_id",
                    ContractViolationKind::InvalidFormat,
                ));
            }
        }
        validate_timestamp("created_at_ms", self.created_at_ms)?;
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
        self.bundle.validate()?;
        validate_safe_json_integer("base_sequence", self.base_sequence)?;
        validate_safe_json_integer("target_sequence", self.target_sequence)?;
        validate_safe_json_integer("expected_vector_count", self.expected_vector_count)?;
        validate_safe_json_integer("expected_edge_count", self.expected_edge_count)?;
        if self.target_sequence < self.base_sequence {
            return Err(violation(
                "target_sequence",
                "target_sequence must be greater than or equal to base_sequence",
                ContractViolationKind::BelowMinimum,
            ));
        }
        Ok(())
    }
}

/// Operation represented by an ordered knowledge mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeOperation {
    Upsert,
    Delete,
}

/// One strictly ordered, idempotent mutation for a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeMutation {
    pub schema_version: u32,
    pub mutation_id: String,
    pub workspace_id: String,
    pub collection: String,
    pub generation_id: String,
    pub sequence: u64,
    pub operation: KnowledgeOperation,
    pub chunk_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<ImmutableObjectReference>,
    pub created_at_ms: u64,
}

impl KnowledgeMutation {
    pub fn scope(&self) -> KnowledgeScope {
        KnowledgeScope::new(self.workspace_id.clone(), self.collection.clone())
    }

    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        self.scope().validate()?;
        validate_identifier("mutation_id", &self.mutation_id, MAX_ID_BYTES)?;
        validate_identifier("generation_id", &self.generation_id, MAX_ID_BYTES)?;
        validate_identifier("chunk_id", &self.chunk_id, MAX_ID_BYTES)?;
        if self.sequence == 0 {
            return Err(violation(
                "sequence",
                "sequence must be greater than zero",
                ContractViolationKind::BelowMinimum,
            ));
        }
        validate_safe_json_integer("sequence", self.sequence)?;
        validate_timestamp("created_at_ms", self.created_at_ms)?;

        match (self.operation, &self.payload) {
            (KnowledgeOperation::Upsert, Some(payload)) => payload.validate()?,
            (KnowledgeOperation::Upsert, None) => {
                return Err(violation(
                    "payload",
                    "upsert mutations require an immutable payload reference",
                    ContractViolationKind::Empty,
                ));
            }
            (KnowledgeOperation::Delete, None) => {}
            (KnowledgeOperation::Delete, Some(_)) => {
                return Err(violation(
                    "payload",
                    "delete mutations must not include a payload",
                    ContractViolationKind::InvalidFormat,
                ));
            }
        }
        Ok(())
    }
}

/// Durable replica checkpoint state reported to the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaState {
    CatchingUp,
    Ready,
    Serving,
    Failed,
}

/// Evidence that one replica has applied a generation through a sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaCheckpoint {
    pub schema_version: u32,
    pub replica_id: String,
    pub workspace_id: String,
    pub collection: String,
    pub generation_id: String,
    pub manifest_sha256: String,
    pub applied_sequence: u64,
    pub state: ReplicaState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

impl ReplicaCheckpoint {
    pub fn scope(&self) -> KnowledgeScope {
        KnowledgeScope::new(self.workspace_id.clone(), self.collection.clone())
    }

    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        self.scope().validate()?;
        validate_identifier("replica_id", &self.replica_id, MAX_ID_BYTES)?;
        validate_identifier("generation_id", &self.generation_id, MAX_ID_BYTES)?;
        validate_sha256("manifest_sha256", &self.manifest_sha256)?;
        validate_safe_json_integer("applied_sequence", self.applied_sequence)?;
        validate_timestamp("updated_at_ms", self.updated_at_ms)?;

        match (&self.state, &self.last_error) {
            (ReplicaState::Failed, Some(error)) => {
                validate_nonempty_text("last_error", error, MAX_FAILURE_BYTES)?;
            }
            (ReplicaState::Failed, None) => {
                return Err(violation(
                    "last_error",
                    "failed checkpoints require non-empty failure evidence",
                    ContractViolationKind::Empty,
                ));
            }
            (_, Some(_)) => {
                return Err(violation(
                    "last_error",
                    "non-failed checkpoints must not include last_error",
                    ContractViolationKind::InvalidFormat,
                ));
            }
            (_, None) => {}
        }
        Ok(())
    }
}

fn validate_schema_version(version: u32) -> ContractResult<()> {
    if version != KNOWLEDGE_SCHEMA_VERSION {
        return Err(violation(
            "schema_version",
            format!("unsupported schema_version {version}; expected {KNOWLEDGE_SCHEMA_VERSION}"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str, maximum: usize) -> ContractResult<()> {
    validate_nonempty_text(field, value, maximum)?;
    if value.chars().any(char::is_control) {
        return Err(violation(
            field,
            format!("{field} must not contain control characters"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_nonempty_text(field: &'static str, value: &str, maximum: usize) -> ContractResult<()> {
    if value.trim().is_empty() {
        return Err(ContractViolation::empty(field));
    }
    if value.trim() != value {
        return Err(violation(
            field,
            format!("{field} must not have leading or trailing whitespace"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    if value.len() > maximum {
        return Err(ContractViolation::exceeds_maximum(
            field,
            value.len(),
            maximum,
        ));
    }
    Ok(())
}

fn validate_sha256(field: &'static str, digest: &str) -> ContractResult<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(violation(
            field,
            format!("{field} must be exactly 64 lowercase hexadecimal characters"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_uri(uri: &str) -> ContractResult<()> {
    validate_nonempty_text("uri", uri, MAX_URI_BYTES)?;
    let parsed = Url::parse(uri).map_err(|_| {
        violation(
            "uri",
            "uri must be an absolute object URI",
            ContractViolationKind::InvalidFormat,
        )
    })?;
    if !matches!(parsed.scheme(), "s3" | "https") {
        return Err(violation(
            "uri",
            "uri scheme must be s3 or https",
            ContractViolationKind::InvalidFormat,
        ));
    }
    if parsed.host_str().is_none() || parsed.path().is_empty() || parsed.path() == "/" {
        return Err(violation(
            "uri",
            "uri must include an object authority and path",
            ContractViolationKind::InvalidFormat,
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(violation(
            "uri",
            "uri must not embed credentials",
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_timestamp(field: &'static str, timestamp_ms: u64) -> ContractResult<()> {
    if timestamp_ms == 0 {
        return Err(violation(
            field,
            format!("{field} must be greater than zero"),
            ContractViolationKind::BelowMinimum,
        ));
    }
    validate_safe_json_integer(field, timestamp_ms)?;
    Ok(())
}

fn validate_safe_json_integer(field: &'static str, value: u64) -> ContractResult<()> {
    if value > MAX_SAFE_JSON_INTEGER {
        return Err(violation(
            field,
            format!("{field} must not exceed the JSON safe integer maximum"),
            ContractViolationKind::ExceedsMaximum,
        ));
    }
    Ok(())
}

fn violation(
    field: &'static str,
    message: impl Into<String>,
    kind: ContractViolationKind,
) -> ContractViolation {
    ContractViolation::new(field, message, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/generation.json");
    const VALID_UPSERT: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/mutation-upsert.json");
    const VALID_DELETE: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/mutation-delete.json");
    const VALID_READY_CHECKPOINT: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/checkpoint-ready.json");
    const VALID_FAILED_CHECKPOINT: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/checkpoint-failed.json");

    #[test]
    fn valid_golden_fixtures_round_trip() {
        let manifest: KnowledgeGenerationManifest =
            serde_json::from_str(VALID_MANIFEST).expect("valid generation fixture");
        manifest.validate().expect("generation must validate");
        assert_json_round_trip(&manifest, VALID_MANIFEST);

        let upsert: KnowledgeMutation =
            serde_json::from_str(VALID_UPSERT).expect("valid upsert fixture");
        upsert.validate().expect("upsert must validate");
        assert_json_round_trip(&upsert, VALID_UPSERT);

        let delete: KnowledgeMutation =
            serde_json::from_str(VALID_DELETE).expect("valid delete fixture");
        delete.validate().expect("delete must validate");
        assert_json_round_trip(&delete, VALID_DELETE);

        let ready: ReplicaCheckpoint =
            serde_json::from_str(VALID_READY_CHECKPOINT).expect("valid ready checkpoint");
        ready.validate().expect("ready checkpoint must validate");
        assert_json_round_trip(&ready, VALID_READY_CHECKPOINT);

        let failed: ReplicaCheckpoint =
            serde_json::from_str(VALID_FAILED_CHECKPOINT).expect("valid failed checkpoint");
        failed.validate().expect("failed checkpoint must validate");
        assert_json_round_trip(&failed, VALID_FAILED_CHECKPOINT);
    }

    #[test]
    fn invalid_golden_fixtures_are_rejected() {
        let bad_digest: KnowledgeGenerationManifest = serde_json::from_str(include_str!(
            "../../../contracts/fixtures/knowledge/v1/invalid/generation-bad-digest.json"
        ))
        .unwrap();
        assert!(bad_digest.validate().is_err());

        let reversed_range: KnowledgeGenerationManifest = serde_json::from_str(include_str!(
            "../../../contracts/fixtures/knowledge/v1/invalid/generation-reversed-sequence.json"
        ))
        .unwrap();
        assert!(reversed_range.validate().is_err());

        let unsafe_integer: KnowledgeGenerationManifest = serde_json::from_str(include_str!(
            "../../../contracts/fixtures/knowledge/v1/invalid/generation-unsafe-integer.json"
        ))
        .unwrap();
        assert!(unsafe_integer.validate().is_err());

        let missing_payload: KnowledgeMutation = serde_json::from_str(include_str!(
            "../../../contracts/fixtures/knowledge/v1/invalid/upsert-missing-payload.json"
        ))
        .unwrap();
        assert!(missing_payload.validate().is_err());

        let delete_payload: KnowledgeMutation = serde_json::from_str(include_str!(
            "../../../contracts/fixtures/knowledge/v1/invalid/delete-with-payload.json"
        ))
        .unwrap();
        assert!(delete_payload.validate().is_err());

        let failed_without_error: ReplicaCheckpoint = serde_json::from_str(include_str!(
            "../../../contracts/fixtures/knowledge/v1/invalid/checkpoint-failed-without-error.json"
        ))
        .unwrap();
        assert!(failed_without_error.validate().is_err());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let mut value: serde_json::Value = serde_json::from_str(VALID_MANIFEST).unwrap();
        value["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<KnowledgeGenerationManifest>(value).is_err());

        let mut nested: serde_json::Value = serde_json::from_str(VALID_MANIFEST).unwrap();
        nested["bundle"]["credentials"] = serde_json::json!("secret");
        assert!(serde_json::from_value::<KnowledgeGenerationManifest>(nested).is_err());
    }

    #[test]
    fn every_documented_manifest_boundary_is_enforced() {
        let valid: KnowledgeGenerationManifest = serde_json::from_str(VALID_MANIFEST).unwrap();

        let mut manifest = valid.clone();
        manifest.schema_version += 1;
        assert_eq!(manifest.validate().unwrap_err().field, "schema_version");

        let mut manifest = valid.clone();
        manifest.workspace_id.clear();
        assert_eq!(manifest.validate().unwrap_err().field, "workspace_id");

        let mut manifest = valid.clone();
        manifest.generation_id = "x".repeat(MAX_ID_BYTES + 1);
        assert_eq!(manifest.validate().unwrap_err().field, "generation_id");

        let mut manifest = valid.clone();
        manifest.parent_generation_id = Some(manifest.generation_id.clone());
        assert_eq!(
            manifest.validate().unwrap_err().field,
            "parent_generation_id"
        );

        let mut manifest = valid.clone();
        manifest.embedding_dimensions = 0;
        assert_eq!(
            manifest.validate().unwrap_err().field,
            "embedding_dimensions"
        );

        let mut manifest = valid.clone();
        manifest.embedding_dimensions = MAX_EMBEDDING_DIMENSIONS + 1;
        assert_eq!(
            manifest.validate().unwrap_err().field,
            "embedding_dimensions"
        );

        let mut manifest = valid.clone();
        manifest.bundle.uri = "s3://bucket".to_string();
        assert_eq!(manifest.validate().unwrap_err().field, "uri");

        let mut manifest = valid.clone();
        manifest.bundle.uri = "https://user:secret@example.com/object".to_string();
        assert_eq!(manifest.validate().unwrap_err().field, "uri");

        let mut manifest = valid.clone();
        manifest.bundle.uri = "http://minio.internal/object".to_string();
        assert_eq!(manifest.validate().unwrap_err().field, "uri");

        let mut manifest = valid;
        manifest.bundle.size_bytes = 0;
        assert_eq!(manifest.validate().unwrap_err().field, "size_bytes");

        let mut manifest: KnowledgeGenerationManifest =
            serde_json::from_str(VALID_MANIFEST).unwrap();
        manifest.target_sequence = MAX_SAFE_JSON_INTEGER + 1;
        assert_eq!(manifest.validate().unwrap_err().field, "target_sequence");
    }

    #[test]
    fn mutation_and_checkpoint_boundary_rules_are_enforced() {
        let mut mutation: KnowledgeMutation = serde_json::from_str(VALID_UPSERT).unwrap();
        mutation.sequence = 0;
        assert_eq!(mutation.validate().unwrap_err().field, "sequence");

        let mut checkpoint: ReplicaCheckpoint =
            serde_json::from_str(VALID_READY_CHECKPOINT).unwrap();
        checkpoint.last_error = Some("unexpected".to_string());
        assert_eq!(checkpoint.validate().unwrap_err().field, "last_error");

        let mut checkpoint: ReplicaCheckpoint =
            serde_json::from_str(VALID_FAILED_CHECKPOINT).unwrap();
        checkpoint.last_error = Some(" ".to_string());
        assert_eq!(checkpoint.validate().unwrap_err().field, "last_error");

        let mut checkpoint: ReplicaCheckpoint =
            serde_json::from_str(VALID_READY_CHECKPOINT).unwrap();
        checkpoint.updated_at_ms = 0;
        assert_eq!(checkpoint.validate().unwrap_err().field, "updated_at_ms");

        let mut checkpoint: ReplicaCheckpoint =
            serde_json::from_str(VALID_READY_CHECKPOINT).unwrap();
        checkpoint.applied_sequence = MAX_SAFE_JSON_INTEGER + 1;
        assert_eq!(checkpoint.validate().unwrap_err().field, "applied_sequence");
    }

    fn assert_json_round_trip<T>(value: &T, source: &str)
    where
        T: Serialize,
    {
        let expected: serde_json::Value = serde_json::from_str(source).unwrap();
        let actual = serde_json::to_value(value).unwrap();
        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    const VALID_MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/generation.json");

    proptest! {
        #[test]
        fn lowercase_sha256_is_accepted(value in "[0-9a-f]{64}") {
            prop_assert!(validate_sha256("sha256", &value).is_ok());
        }

        #[test]
        fn noncanonical_sha256_is_rejected(value in ".{0,80}") {
            let canonical = value.len() == 64
                && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
            prop_assert_eq!(validate_sha256("sha256", &value).is_ok(), canonical);
        }

        #[test]
        fn generation_rejects_reversed_sequence_ranges(
            base in 1u64..u64::MAX,
            delta in 1u64..10_000,
        ) {
            let target = base.saturating_sub(delta).min(base.saturating_sub(1));
            let mut manifest: KnowledgeGenerationManifest =
                serde_json::from_str(VALID_MANIFEST).unwrap();
            manifest.base_sequence = base;
            manifest.target_sequence = target;
            prop_assert!(manifest.validate().is_err());
        }

        #[test]
        fn json_integer_boundaries_are_portable(value in 0u64..=MAX_SAFE_JSON_INTEGER) {
            prop_assert!(validate_safe_json_integer("value", value).is_ok());
        }

        #[test]
        fn nonportable_json_integers_are_rejected(
            value in (MAX_SAFE_JSON_INTEGER + 1)..=u64::MAX,
        ) {
            prop_assert!(validate_safe_json_integer("value", value).is_err());
        }
    }
}
