//! Canonical domain contracts for authoritative AkiDB Memory.
//!
//! These records deliberately separate stable assertion identity, immutable
//! content versions, lifecycle transitions, idempotency, and projection
//! visibility. They contain no transport authentication material and no model
//! is allowed to construct an authorized commit directly.

use crate::error::{ContractResult, ContractViolation, ContractViolationKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use unicode_normalization::UnicodeNormalization;

/// First canonical memory record encoding.
pub const MEMORY_SCHEMA_VERSION: u32 = 1;
/// First canonical assertion-identity normalization and hash contract.
pub const MEMORY_IDENTITY_HASH_VERSION: u32 = 1;

pub const MAX_MEMORY_ID_BYTES: usize = 1_024;
pub const MAX_MEMORY_SCOPE_BYTES: usize = 255;
pub const MAX_MEMORY_NAMESPACE_BYTES: usize = 1_024;
pub const MAX_MEMORY_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_MEMORY_STRUCTURED_BYTES: usize = 256 * 1024;
pub const MAX_MEMORY_PURPOSES: usize = 32;
pub const MAX_MEMORY_EVIDENCE: usize = 128;
pub const MAX_MEMORY_PARENTS: usize = 32;
pub const MAX_MEMORY_ACTIVE_HEADS: usize = 32;
pub const MAX_MEMORY_PROJECTIONS: usize = 32;
pub const MAX_MEMORY_DELETION_TARGETS: usize = 100_000;
const MAX_JSON_DEPTH: usize = 64;

/// Placement, privacy, and disclosure scope for one immutable memory version.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryScope {
    pub workspace_id: String,
    pub namespace: String,
    pub entity_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_subject_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub sensitivity: Sensitivity,
    pub allowed_purposes: Vec<String>,
}

impl MemoryScope {
    pub fn validate(&self) -> ContractResult<()> {
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_namespace(&self.namespace)?;
        validate_identifier("entity_key", &self.entity_key, MAX_MEMORY_ID_BYTES)?;
        validate_optional_identifier(
            "data_subject_id",
            self.data_subject_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier(
            "owner_agent_id",
            self.owner_agent_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier(
            "session_id",
            self.session_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier("task_id", self.task_id.as_deref(), MAX_MEMORY_ID_BYTES)?;
        if self.allowed_purposes.is_empty() {
            return Err(ContractViolation::empty("allowed_purposes"));
        }
        if self.allowed_purposes.len() > MAX_MEMORY_PURPOSES {
            return Err(ContractViolation::exceeds_maximum(
                "allowed_purposes",
                self.allowed_purposes.len(),
                MAX_MEMORY_PURPOSES,
            ));
        }
        let mut purposes = HashSet::with_capacity(self.allowed_purposes.len());
        for purpose in &self.allowed_purposes {
            validate_identifier("allowed_purpose", purpose, MAX_MEMORY_SCOPE_BYTES)?;
            if !purposes.insert(purpose) {
                return Err(violation(
                    "allowed_purposes",
                    "allowed_purposes must not contain duplicates",
                    ContractViolationKind::InvalidFormat,
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    Public,
    Internal,
    Confidential,
    Restricted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    TextFact,
    StructuredFact,
    Procedure,
    Preference,
    EpisodeReference,
}

impl MemoryKind {
    fn canonical_name(self) -> &'static str {
        match self {
            Self::TextFact => "text_fact",
            Self::StructuredFact => "structured_fact",
            Self::Procedure => "procedure",
            Self::Preference => "preference",
            Self::EpisodeReference => "episode_reference",
        }
    }
}

/// Typed immutable content. Large binary source material belongs in evidence,
/// referenced by digest, rather than in this enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryContent {
    TextFact {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
    },
    StructuredFact {
        schema_id: String,
        canonical_json: Value,
    },
    Procedure {
        title: String,
        ordered_steps: Vec<String>,
        preconditions: Vec<String>,
        failure_recovery: Vec<String>,
    },
    Preference {
        value: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<String>,
    },
    EpisodeReference {
        event_ids: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
}

impl MemoryContent {
    pub fn kind(&self) -> MemoryKind {
        match self {
            Self::TextFact { .. } => MemoryKind::TextFact,
            Self::StructuredFact { .. } => MemoryKind::StructuredFact,
            Self::Procedure { .. } => MemoryKind::Procedure,
            Self::Preference { .. } => MemoryKind::Preference,
            Self::EpisodeReference { .. } => MemoryKind::EpisodeReference,
        }
    }

    pub fn validate(&self) -> ContractResult<()> {
        match self {
            Self::TextFact { text, language } => {
                validate_text("content.text", text, MAX_MEMORY_TEXT_BYTES)?;
                validate_optional_identifier(
                    "content.language",
                    language.as_deref(),
                    MAX_MEMORY_SCOPE_BYTES,
                )
            }
            Self::StructuredFact {
                schema_id,
                canonical_json,
            } => {
                validate_identifier("content.schema_id", schema_id, MAX_MEMORY_ID_BYTES)?;
                if canonical_json.is_null() {
                    return Err(violation(
                        "content.canonical_json",
                        "structured memory must not be null",
                        ContractViolationKind::Empty,
                    ));
                }
                if json_depth(canonical_json) > MAX_JSON_DEPTH {
                    return Err(violation(
                        "content.canonical_json",
                        format!("JSON nesting must not exceed {MAX_JSON_DEPTH}"),
                        ContractViolationKind::ExceedsMaximum,
                    ));
                }
                let encoded = serde_json::to_vec(canonical_json).map_err(|error| {
                    violation(
                        "content.canonical_json",
                        format!("structured memory cannot be encoded: {error}"),
                        ContractViolationKind::InvalidFormat,
                    )
                })?;
                if encoded.len() > MAX_MEMORY_STRUCTURED_BYTES {
                    return Err(ContractViolation::exceeds_maximum(
                        "content.canonical_json",
                        encoded.len(),
                        MAX_MEMORY_STRUCTURED_BYTES,
                    ));
                }
                Ok(())
            }
            Self::Procedure {
                title,
                ordered_steps,
                preconditions,
                failure_recovery,
            } => {
                validate_text("content.title", title, MAX_MEMORY_SCOPE_BYTES)?;
                validate_nonempty_list("content.ordered_steps", ordered_steps, 256)?;
                validate_text_list("content.ordered_steps", ordered_steps)?;
                validate_text_list("content.preconditions", preconditions)?;
                validate_text_list("content.failure_recovery", failure_recovery)?;
                let total = title.len()
                    + ordered_steps.iter().map(String::len).sum::<usize>()
                    + preconditions.iter().map(String::len).sum::<usize>()
                    + failure_recovery.iter().map(String::len).sum::<usize>();
                if total > MAX_MEMORY_TEXT_BYTES {
                    return Err(ContractViolation::exceeds_maximum(
                        "content.procedure",
                        total,
                        MAX_MEMORY_TEXT_BYTES,
                    ));
                }
                Ok(())
            }
            Self::Preference { value, context } => {
                validate_text("content.value", value, MAX_MEMORY_TEXT_BYTES)?;
                if let Some(context) = context {
                    validate_text("content.context", context, MAX_MEMORY_TEXT_BYTES)?;
                }
                Ok(())
            }
            Self::EpisodeReference { event_ids, summary } => {
                validate_nonempty_list("content.event_ids", event_ids, MAX_MEMORY_EVIDENCE)?;
                validate_unique_ids("content.event_ids", event_ids)?;
                if let Some(summary) = summary {
                    validate_text("content.summary", summary, MAX_MEMORY_TEXT_BYTES)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpistemicFormation {
    DirectObservation,
    HumanStatement,
    AgentStatement,
    ModelInference,
    DeterministicDerivation,
    ConsolidatedSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAssurance {
    UnverifiedExternal,
    AuthenticatedExternal,
    AuthenticatedAgent,
    AuthenticatedHuman,
    SignedSystem,
    VerifiedCanonicalSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAuthority {
    None,
    Advisory,
    Operational,
    GoverningPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionState {
    Proposed,
    Quarantined,
    Active,
    Superseded,
    Retracted,
    Tombstoned,
}

/// Stable normalized identity for one assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryAssertionIdentity {
    pub workspace_id: String,
    pub namespace: String,
    pub entity_key: String,
    pub predicate: String,
    pub kind: MemoryKind,
}

impl MemoryAssertionIdentity {
    pub fn validate(&self) -> ContractResult<()> {
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_namespace(&self.namespace)?;
        validate_identifier("entity_key", &self.entity_key, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("predicate", &self.predicate, MAX_MEMORY_ID_BYTES)?;
        Ok(())
    }

    pub fn normalized_predicate(&self) -> String {
        normalize_predicate_v1(&self.predicate)
    }

    pub fn identity_hash(&self) -> ContractResult<String> {
        self.validate()?;
        let mut canonical = Vec::new();
        push_component(&mut canonical, b"akidb-memory-assertion");
        canonical.extend_from_slice(&MEMORY_IDENTITY_HASH_VERSION.to_be_bytes());
        push_component(&mut canonical, nfc(&self.workspace_id).as_bytes());
        push_component(&mut canonical, nfc(&self.namespace).as_bytes());
        push_component(&mut canonical, nfc(&self.entity_key).as_bytes());
        push_component(&mut canonical, self.normalized_predicate().as_bytes());
        push_component(&mut canonical, self.kind.canonical_name().as_bytes());
        Ok(sha256_hex(&canonical))
    }

    pub fn assertion_id(&self) -> ContractResult<String> {
        Ok(format!("mem_a1_{}", self.identity_hash()?))
    }
}

/// Stable assertion metadata. Content values live only in immutable versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryAssertion {
    pub schema_version: u32,
    pub assertion_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub entity_key: String,
    pub predicate: String,
    pub kind: MemoryKind,
    pub identity_hash_version: u32,
    pub identity_hash: String,
    pub created_sequence: u64,
    pub created_at_ms: u64,
}

impl MemoryAssertion {
    pub fn identity(&self) -> MemoryAssertionIdentity {
        MemoryAssertionIdentity {
            workspace_id: self.workspace_id.clone(),
            namespace: self.namespace.clone(),
            entity_key: self.entity_key.clone(),
            predicate: self.predicate.clone(),
            kind: self.kind,
        }
    }

    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        if self.identity_hash_version != MEMORY_IDENTITY_HASH_VERSION {
            return Err(violation(
                "identity_hash_version",
                format!(
                    "unsupported identity_hash_version {}; expected {}",
                    self.identity_hash_version, MEMORY_IDENTITY_HASH_VERSION
                ),
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_sha256("identity_hash", &self.identity_hash)?;
        let identity = self.identity();
        let expected_hash = identity.identity_hash()?;
        let expected_id = identity.assertion_id()?;
        if self.identity_hash != expected_hash || self.assertion_id != expected_id {
            return Err(violation(
                "assertion_id",
                "assertion identity does not match its canonical fields",
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_sequence("created_sequence", self.created_sequence)?;
        validate_timestamp("created_at_ms", self.created_at_ms)
    }
}

/// One immutable evidence reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRecord {
    pub schema_version: u32,
    pub evidence_id: String,
    pub workspace_id: String,
    pub source_plane: String,
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_ms: Option<u64>,
    /// Nanosecond-precision source observation time. New APIs use this field;
    /// observed_at_ms remains a compatibility input during preview.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_unix_nanos: Option<i64>,
    pub content_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_principal_id: Option<String>,
    pub source_assurance: SourceAssurance,
    pub created_sequence: u64,
}

impl EvidenceRecord {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("evidence_id", &self.evidence_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("source_plane", &self.source_plane, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("source_id", &self.source_id, MAX_MEMORY_ID_BYTES)?;
        validate_optional_identifier(
            "source_version",
            self.source_version.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        if let Some(observed_at_ms) = self.observed_at_ms {
            validate_timestamp("observed_at_ms", observed_at_ms)?;
            if observed_at_ms > i64::MAX as u64 {
                return Err(violation(
                    "observed_at_ms",
                    "observed_at_ms cannot be represented by the nanosecond compatibility model",
                    ContractViolationKind::InvalidNumber,
                ));
            }
        }
        if self.observed_at_unix_nanos.is_some_and(|value| value <= 0) {
            return Err(violation(
                "observed_at_unix_nanos",
                "observed_at_unix_nanos must be greater than zero",
                ContractViolationKind::BelowMinimum,
            ));
        }
        validate_compatible_instants(
            "observed_at",
            self.observed_at_ms.map(|value| value as i64),
            self.observed_at_unix_nanos,
        )?;
        validate_sha256("content_sha256", &self.content_sha256)?;
        validate_optional_identifier(
            "source_principal_id",
            self.source_principal_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("created_sequence", self.created_sequence)
    }
}

/// Immutable raw source event. Observations supply evidence to compilers but
/// never become active beliefs merely by being persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryObservation {
    pub schema_version: u32,
    pub observation_id: String,
    pub evidence_id: String,
    pub scope: MemoryScope,
    pub source_plane: String,
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_unix_nanos: Option<i64>,
    pub content_sha256: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_payload: Vec<u8>,
    pub source_assurance: SourceAssurance,
    pub policy_decision_id: String,
    pub created_by_principal_id: String,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
}

impl MemoryObservation {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("observation_id", &self.observation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("evidence_id", &self.evidence_id, MAX_MEMORY_ID_BYTES)?;
        self.scope.validate()?;
        validate_identifier("source_plane", &self.source_plane, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("source_id", &self.source_id, MAX_MEMORY_ID_BYTES)?;
        validate_optional_identifier(
            "source_version",
            self.source_version.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        if let Some(observed_at_ms) = self.observed_at_ms {
            validate_timestamp("observed_at_ms", observed_at_ms)?;
            if observed_at_ms > i64::MAX as u64 {
                return Err(violation(
                    "observed_at_ms",
                    "observed_at_ms exceeds the signed time range",
                    ContractViolationKind::InvalidNumber,
                ));
            }
        }
        if self.observed_at_unix_nanos.is_some_and(|value| value <= 0) {
            return Err(violation(
                "observed_at_unix_nanos",
                "observed_at_unix_nanos must be greater than zero",
                ContractViolationKind::BelowMinimum,
            ));
        }
        validate_compatible_instants(
            "observed_at",
            self.observed_at_ms.map(|value| value as i64),
            self.observed_at_unix_nanos,
        )?;
        validate_sha256("content_sha256", &self.content_sha256)?;
        if self.retained_payload.len() > MAX_MEMORY_TEXT_BYTES {
            return Err(ContractViolation::exceeds_maximum(
                "retained_payload",
                self.retained_payload.len(),
                MAX_MEMORY_TEXT_BYTES,
            ));
        }
        if !self.retained_payload.is_empty()
            && sha256_hex(&self.retained_payload) != self.content_sha256
        {
            return Err(violation(
                "retained_payload",
                "retained payload digest differs from content_sha256",
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier(
            "created_by_principal_id",
            &self.created_by_principal_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("committed_sequence", self.committed_sequence)?;
        validate_timestamp("committed_at_ms", self.committed_at_ms)
    }
}

/// One immutable content revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryVersion {
    pub schema_version: u32,
    pub version_id: String,
    pub assertion_id: String,
    pub parent_version_ids: Vec<String>,
    pub scope: MemoryScope,
    pub kind: MemoryKind,
    pub content: MemoryContent,
    pub content_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to_ms: Option<i64>,
    /// Nanosecond-precision valid-time bounds. New APIs use these fields;
    /// millisecond bounds remain readable for preview compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from_unix_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to_unix_nanos: Option<i64>,
    pub epistemic_formation: EpistemicFormation,
    pub source_assurance: SourceAssurance,
    pub decision_authority: DecisionAuthority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    pub evidence_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiler_artifact_id: Option<String>,
    pub policy_decision_id: String,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
    pub created_by_principal_id: String,
}

impl MemoryVersion {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("version_id", &self.version_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        if self.parent_version_ids.len() > MAX_MEMORY_PARENTS {
            return Err(ContractViolation::exceeds_maximum(
                "parent_version_ids",
                self.parent_version_ids.len(),
                MAX_MEMORY_PARENTS,
            ));
        }
        validate_unique_ids("parent_version_ids", &self.parent_version_ids)?;
        if self
            .parent_version_ids
            .iter()
            .any(|id| id == &self.version_id)
        {
            return Err(violation(
                "parent_version_ids",
                "a version cannot be its own parent",
                ContractViolationKind::InvalidFormat,
            ));
        }
        self.scope.validate()?;
        self.content.validate()?;
        if self.kind != self.content.kind() {
            return Err(violation(
                "kind",
                "version kind does not match content kind",
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_sha256("content_sha256", &self.content_sha256)?;
        let actual_content_hash = canonical_content_sha256(&self.content)?;
        if self.content_sha256 != actual_content_hash {
            return Err(violation(
                "content_sha256",
                "content_sha256 does not match canonical content",
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_compatible_instants("valid_from", self.valid_from_ms, self.valid_from_unix_nanos)?;
        validate_compatible_instants("valid_to", self.valid_to_ms, self.valid_to_unix_nanos)?;
        if let (Some(from), Some(to)) = (
            self.effective_valid_from_unix_nanos(),
            self.effective_valid_to_unix_nanos(),
        ) {
            if to <= from {
                return Err(violation(
                    "valid_to",
                    "valid_to must be greater than valid_from",
                    ContractViolationKind::InvalidFormat,
                ));
            }
        }
        if let Some(confidence) = self.confidence {
            if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
                return Err(violation(
                    "confidence",
                    "confidence must be finite and between 0 and 1",
                    ContractViolationKind::InvalidNumber,
                ));
            }
        }
        validate_nonempty_list("evidence_ids", &self.evidence_ids, MAX_MEMORY_EVIDENCE)?;
        validate_unique_ids("evidence_ids", &self.evidence_ids)?;
        validate_optional_identifier(
            "derivation_id",
            self.derivation_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier(
            "compiler_artifact_id",
            self.compiler_artifact_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("committed_sequence", self.committed_sequence)?;
        validate_timestamp("committed_at_ms", self.committed_at_ms)?;
        validate_identifier(
            "created_by_principal_id",
            &self.created_by_principal_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_authority_combination(self)
    }

    pub fn effective_valid_from_unix_nanos(&self) -> Option<i128> {
        effective_unix_nanos(self.valid_from_ms, self.valid_from_unix_nanos)
    }

    pub fn effective_valid_to_unix_nanos(&self) -> Option<i128> {
        effective_unix_nanos(self.valid_to_ms, self.valid_to_unix_nanos)
    }

    pub fn is_valid_at_unix_nanos(&self, instant: i64) -> bool {
        let instant = i128::from(instant);
        self.effective_valid_from_unix_nanos()
            .is_none_or(|from| instant >= from)
            && self
                .effective_valid_to_unix_nanos()
                .is_none_or(|to| instant < to)
    }
}

/// Temporal selection used by canonical reads and retrieval. System order is
/// sequence-based; valid time is normalized to Unix nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryTemporalQuery {
    Current {
        valid_at_unix_nanos: i64,
    },
    ValidAt {
        valid_at_unix_nanos: i64,
    },
    SystemAsOf {
        commit_sequence: u64,
        valid_at_unix_nanos: i64,
    },
    ValidAtAsKnownAt {
        valid_at_unix_nanos: i64,
        commit_sequence: u64,
    },
}

impl MemoryTemporalQuery {
    pub fn validate(&self) -> ContractResult<()> {
        // Sequence zero is the valid genesis view before the first canonical
        // mutation. Storage still rejects any sequence above the current
        // committed sequence.
        Ok(())
    }

    pub fn valid_at_unix_nanos(self) -> i64 {
        match self {
            Self::Current {
                valid_at_unix_nanos,
            }
            | Self::ValidAt {
                valid_at_unix_nanos,
            }
            | Self::SystemAsOf {
                valid_at_unix_nanos,
                ..
            }
            | Self::ValidAtAsKnownAt {
                valid_at_unix_nanos,
                ..
            } => valid_at_unix_nanos,
        }
    }

    pub fn system_sequence(self, current_sequence: u64) -> u64 {
        match self {
            Self::Current { .. } | Self::ValidAt { .. } => current_sequence,
            Self::SystemAsOf {
                commit_sequence, ..
            }
            | Self::ValidAtAsKnownAt {
                commit_sequence, ..
            } => commit_sequence,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryRelationKind {
    Supersedes,
    ConflictsWith,
    DerivedFrom,
    Reinforces,
}

/// Immutable explicit relationship between canonical versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryRelation {
    pub schema_version: u32,
    pub relation_id: String,
    pub workspace_id: String,
    pub kind: MemoryRelationKind,
    pub from_version_id: String,
    pub to_version_id: String,
    pub mutation_id: String,
    pub committed_sequence: u64,
}

impl MemoryRelation {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("relation_id", &self.relation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier(
            "from_version_id",
            &self.from_version_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier("to_version_id", &self.to_version_id, MAX_MEMORY_ID_BYTES)?;
        if self.from_version_id == self.to_version_id {
            return Err(violation(
                "to_version_id",
                "a relation must connect distinct versions",
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_identifier("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_sequence("committed_sequence", self.committed_sequence)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReinforcementOutcome {
    Succeeded,
    Failed,
    Neutral,
}

/// Immutable outcome evidence attached to an existing version. It contains no
/// replacement content, assurance, authority, or scope fields, so
/// reinforcement cannot rewrite or promote the assertion it evaluates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryReinforcement {
    pub schema_version: u32,
    pub reinforcement_id: String,
    pub workspace_id: String,
    pub version_id: String,
    pub evidence_ids: Vec<String>,
    pub outcome: MemoryReinforcementOutcome,
    pub outcome_id: String,
    /// Signed fixed-point utility in millionths, bounded to [-1, 1].
    pub utility_micros: i32,
    pub policy_decision_id: String,
    pub created_by_principal_id: String,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
}

impl MemoryReinforcement {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier(
            "reinforcement_id",
            &self.reinforcement_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("version_id", &self.version_id, MAX_MEMORY_ID_BYTES)?;
        validate_nonempty_list(
            "reinforcement.evidence_ids",
            &self.evidence_ids,
            MAX_MEMORY_EVIDENCE,
        )?;
        validate_unique_ids("reinforcement.evidence_ids", &self.evidence_ids)?;
        validate_identifier("outcome_id", &self.outcome_id, MAX_MEMORY_ID_BYTES)?;
        if !(-1_000_000..=1_000_000).contains(&self.utility_micros) {
            return Err(violation(
                "utility_micros",
                "reinforcement utility must be between -1000000 and 1000000",
                ContractViolationKind::InvalidNumber,
            ));
        }
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier(
            "created_by_principal_id",
            &self.created_by_principal_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("committed_sequence", self.committed_sequence)?;
        validate_timestamp("committed_at_ms", self.committed_at_ms)
    }
}

/// Immutable provenance for a compiler or deterministic transformation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivationRecord {
    pub schema_version: u32,
    pub derivation_id: String,
    pub workspace_id: String,
    pub input_version_ids: Vec<String>,
    pub input_evidence_ids: Vec<String>,
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiler_artifact_id: Option<String>,
    pub deterministic_parameters_sha256: String,
    pub output_version_id: String,
    pub committed_sequence: u64,
}

impl DerivationRecord {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("derivation_id", &self.derivation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_unique_ids("input_version_ids", &self.input_version_ids)?;
        validate_unique_ids("input_evidence_ids", &self.input_evidence_ids)?;
        if self.input_version_ids.is_empty() && self.input_evidence_ids.is_empty() {
            return Err(ContractViolation::empty("derivation inputs"));
        }
        validate_identifier("operation", &self.operation, MAX_MEMORY_SCOPE_BYTES)?;
        validate_optional_identifier(
            "compiler_artifact_id",
            self.compiler_artifact_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sha256(
            "deterministic_parameters_sha256",
            &self.deterministic_parameters_sha256,
        )?;
        validate_identifier(
            "output_version_id",
            &self.output_version_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("committed_sequence", self.committed_sequence)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionOutcome {
    Observed,
    Active,
    Proposed,
    Quarantined,
    Retracted,
    Tombstoned,
    Reinforced,
}

/// Authorized selector for privacy/retention deletion discovery. A selector is
/// intentionally narrower than an arbitrary query so a persisted plan is
/// reviewable and can be reproduced exactly before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MemoryDeletionSelector {
    Source {
        source_plane: String,
        source_id: String,
    },
    DataSubject {
        data_subject_id: String,
    },
}

impl MemoryDeletionSelector {
    pub fn validate(&self) -> ContractResult<()> {
        match self {
            Self::Source {
                source_plane,
                source_id,
            } => {
                validate_identifier(
                    "deletion_selector.source_plane",
                    source_plane,
                    MAX_MEMORY_SCOPE_BYTES,
                )?;
                validate_identifier(
                    "deletion_selector.source_id",
                    source_id,
                    MAX_MEMORY_ID_BYTES,
                )
            }
            Self::DataSubject { data_subject_id } => validate_identifier(
                "deletion_selector.data_subject_id",
                data_subject_id,
                MAX_MEMORY_ID_BYTES,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDeletionTargetKind {
    Selector,
    Assertion,
    Version,
    Evidence,
    Observation,
    Reinforcement,
    RecallSnapshot,
}

/// Immutable dry-run result. Execution is recorded separately so this plan and
/// its digest never change after review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryDeletionPlan {
    pub schema_version: u32,
    pub plan_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub selector: MemoryDeletionSelector,
    pub affected_assertion_ids: Vec<String>,
    pub affected_version_ids: Vec<String>,
    pub affected_evidence_ids: Vec<String>,
    pub affected_observation_ids: Vec<String>,
    pub affected_reinforcement_ids: Vec<String>,
    pub affected_snapshot_ids: Vec<String>,
    pub created_sequence: u64,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub created_by_principal_id: String,
    pub access_scope_sha256: String,
    pub reason: String,
    pub plan_sha256: String,
}

impl MemoryDeletionPlan {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("plan_id", &self.plan_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_namespace(&self.namespace)?;
        self.selector.validate()?;
        validate_deletion_ids("affected_assertion_ids", &self.affected_assertion_ids)?;
        validate_deletion_ids("affected_version_ids", &self.affected_version_ids)?;
        validate_deletion_ids("affected_evidence_ids", &self.affected_evidence_ids)?;
        validate_deletion_ids("affected_observation_ids", &self.affected_observation_ids)?;
        validate_deletion_ids(
            "affected_reinforcement_ids",
            &self.affected_reinforcement_ids,
        )?;
        validate_deletion_ids("affected_snapshot_ids", &self.affected_snapshot_ids)?;
        let total_targets = self
            .affected_assertion_ids
            .len()
            .checked_add(self.affected_version_ids.len())
            .and_then(|value| value.checked_add(self.affected_evidence_ids.len()))
            .and_then(|value| value.checked_add(self.affected_observation_ids.len()))
            .and_then(|value| value.checked_add(self.affected_reinforcement_ids.len()))
            .and_then(|value| value.checked_add(self.affected_snapshot_ids.len()))
            .ok_or_else(|| {
                violation(
                    "affected_targets",
                    "deletion target count overflow",
                    ContractViolationKind::ExceedsMaximum,
                )
            })?;
        if total_targets > MAX_MEMORY_DELETION_TARGETS {
            return Err(ContractViolation::exceeds_maximum(
                "affected_targets",
                total_targets,
                MAX_MEMORY_DELETION_TARGETS,
            ));
        }
        validate_timestamp("created_at_ms", self.created_at_ms)?;
        validate_timestamp("expires_at_ms", self.expires_at_ms)?;
        if self.expires_at_ms <= self.created_at_ms {
            return Err(violation(
                "expires_at_ms",
                "deletion plan expiry must be after creation",
                ContractViolationKind::InvalidFormat,
            ));
        }
        validate_identifier(
            "created_by_principal_id",
            &self.created_by_principal_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sha256("access_scope_sha256", &self.access_scope_sha256)?;
        validate_text("reason", &self.reason, MAX_MEMORY_TEXT_BYTES)?;
        validate_sha256("plan_sha256", &self.plan_sha256)?;
        let expected = memory_deletion_plan_sha256(self)?;
        if self.plan_sha256 != expected {
            return Err(violation(
                "plan_sha256",
                "deletion plan digest does not match its canonical fields",
                ContractViolationKind::InvalidFormat,
            ));
        }
        Ok(())
    }
}

/// Canonical proof that one prohibited representation was covered by an
/// authorized deletion execution. Payload bytes are deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryDeletionTombstone {
    pub schema_version: u32,
    pub tombstone_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub target_kind: MemoryDeletionTargetKind,
    pub target_id: String,
    pub target_sha256: String,
    pub plan_id: String,
    pub execution_id: String,
    pub mutation_id: String,
    pub policy_decision_id: String,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
}

impl MemoryDeletionTombstone {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("tombstone_id", &self.tombstone_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_namespace(&self.namespace)?;
        validate_identifier("target_id", &self.target_id, MAX_MEMORY_ID_BYTES)?;
        validate_sha256("target_sha256", &self.target_sha256)?;
        validate_identifier("plan_id", &self.plan_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("execution_id", &self.execution_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("committed_sequence", self.committed_sequence)?;
        validate_timestamp("committed_at_ms", self.committed_at_ms)
    }
}

/// Separate immutable execution receipt for a deletion plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryDeletionExecution {
    pub schema_version: u32,
    pub execution_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub plan_id: String,
    pub plan_sha256: String,
    pub mutation_id: String,
    pub policy_decision_id: String,
    pub principal_id: String,
    pub affected_tombstone_ids: Vec<String>,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
}

impl MemoryDeletionExecution {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("execution_id", &self.execution_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_namespace(&self.namespace)?;
        validate_identifier("plan_id", &self.plan_id, MAX_MEMORY_ID_BYTES)?;
        validate_sha256("plan_sha256", &self.plan_sha256)?;
        validate_identifier("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier("principal_id", &self.principal_id, MAX_MEMORY_ID_BYTES)?;
        validate_nonempty_list(
            "affected_tombstone_ids",
            &self.affected_tombstone_ids,
            MAX_MEMORY_DELETION_TARGETS + 1,
        )?;
        validate_unique_ids("affected_tombstone_ids", &self.affected_tombstone_ids)?;
        validate_sequence("committed_sequence", self.committed_sequence)?;
        validate_timestamp("committed_at_ms", self.committed_at_ms)
    }
}

/// Auditable policy assignment. The server, never compiler output, assigns
/// source assurance and decision authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDecisionRecord {
    pub schema_version: u32,
    pub policy_decision_id: String,
    pub workspace_id: String,
    pub policy_manifest_id: String,
    pub outcome: PolicyDecisionOutcome,
    pub source_assurance: SourceAssurance,
    pub decision_authority: DecisionAuthority,
    pub reason_codes: Vec<String>,
    pub authorization_decision_id: String,
    pub committed_sequence: u64,
}

impl PolicyDecisionRecord {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier(
            "policy_manifest_id",
            &self.policy_manifest_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_nonempty_list("reason_codes", &self.reason_codes, 32)?;
        validate_unique_ids("reason_codes", &self.reason_codes)?;
        validate_identifier(
            "authorization_decision_id",
            &self.authorization_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("committed_sequence", self.committed_sequence)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOperation {
    Observe,
    Propose,
    Commit,
    Correct,
    Retract,
    Forget,
    Reinforce,
    RetentionDelete,
}

/// Immutable canonical mutation envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryMutation {
    pub schema_version: u32,
    pub mutation_id: String,
    pub operation: MemoryOperation,
    pub workspace_id: String,
    pub assertion_id: String,
    pub input_version_ids: Vec<String>,
    pub output_version_ids: Vec<String>,
    pub expected_head_version_ids: Vec<String>,
    pub idempotency_key_sha256: String,
    pub canonical_request_sha256: String,
    pub principal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegated_agent_id: Option<String>,
    pub authorization_decision_id: String,
    pub policy_decision_id: String,
    pub reason: String,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
}

impl MemoryMutation {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        validate_unique_ids("input_version_ids", &self.input_version_ids)?;
        if matches!(
            self.operation,
            MemoryOperation::Observe
                | MemoryOperation::Propose
                | MemoryOperation::Commit
                | MemoryOperation::Correct
        ) {
            validate_nonempty_list(
                "output_version_ids",
                &self.output_version_ids,
                MAX_MEMORY_PARENTS,
            )?;
        } else if self.output_version_ids.len() > MAX_MEMORY_PARENTS {
            return Err(ContractViolation::exceeds_maximum(
                "output_version_ids",
                self.output_version_ids.len(),
                MAX_MEMORY_PARENTS,
            ));
        }
        validate_unique_ids("output_version_ids", &self.output_version_ids)?;
        validate_unique_ids("expected_head_version_ids", &self.expected_head_version_ids)?;
        validate_sha256("idempotency_key_sha256", &self.idempotency_key_sha256)?;
        validate_sha256("canonical_request_sha256", &self.canonical_request_sha256)?;
        validate_identifier("principal_id", &self.principal_id, MAX_MEMORY_ID_BYTES)?;
        validate_optional_identifier(
            "delegated_agent_id",
            self.delegated_agent_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier(
            "authorization_decision_id",
            &self.authorization_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_text("reason", &self.reason, MAX_MEMORY_TEXT_BYTES)?;
        validate_sequence("committed_sequence", self.committed_sequence)?;
        validate_timestamp("committed_at_ms", self.committed_at_ms)
    }
}

/// Current canonical lifecycle/head projection for an assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryHead {
    pub schema_version: u32,
    pub workspace_id: String,
    pub assertion_id: String,
    pub active_version_ids: Vec<String>,
    pub latest_mutation_id: String,
    pub latest_sequence: u64,
}

impl MemoryHead {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        if self.active_version_ids.len() > MAX_MEMORY_ACTIVE_HEADS {
            return Err(ContractViolation::exceeds_maximum(
                "active_version_ids",
                self.active_version_ids.len(),
                MAX_MEMORY_ACTIVE_HEADS,
            ));
        }
        validate_unique_ids("active_version_ids", &self.active_version_ids)?;
        validate_identifier(
            "latest_mutation_id",
            &self.latest_mutation_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sequence("latest_sequence", self.latest_sequence)
    }
}

/// Lifecycle status is mutable only by appending a new canonical mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionLifecycle {
    pub schema_version: u32,
    pub workspace_id: String,
    pub version_id: String,
    pub state: VersionState,
    pub transition_sequence: u64,
    pub transition_mutation_id: String,
}

impl VersionLifecycle {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("version_id", &self.version_id, MAX_MEMORY_ID_BYTES)?;
        validate_sequence("transition_sequence", self.transition_sequence)?;
        validate_identifier(
            "transition_mutation_id",
            &self.transition_mutation_id,
            MAX_MEMORY_ID_BYTES,
        )
    }
}

/// Stable response persisted for an idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyRecord {
    pub schema_version: u32,
    pub principal_id: String,
    pub workspace_id: String,
    pub operation: MemoryOperation,
    pub idempotency_key_sha256: String,
    pub canonical_request_sha256: String,
    pub policy_decision_id: String,
    pub mutation_id: String,
    pub assertion_id: String,
    pub version_ids: Vec<String>,
    pub commit_sequence: u64,
}

impl IdempotencyRecord {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("principal_id", &self.principal_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_sha256("idempotency_key_sha256", &self.idempotency_key_sha256)?;
        validate_sha256("canonical_request_sha256", &self.canonical_request_sha256)?;
        validate_identifier(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_identifier("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        validate_nonempty_list("version_ids", &self.version_ids, MAX_MEMORY_PARENTS)?;
        validate_unique_ids("version_ids", &self.version_ids)?;
        validate_sequence("commit_sequence", self.commit_sequence)
    }
}

/// Ordered canonical notification consumed by every relevant projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionOutboxEntry {
    pub schema_version: u32,
    pub workspace_id: String,
    pub sequence: u64,
    pub mutation_id: String,
    pub assertion_id: String,
    pub version_ids: Vec<String>,
}

impl ProjectionOutboxEntry {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_sequence("sequence", self.sequence)?;
        validate_identifier("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_identifier("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        validate_nonempty_list("version_ids", &self.version_ids, MAX_MEMORY_PARENTS)?;
        validate_unique_ids("version_ids", &self.version_ids)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionStatus {
    CatchingUp,
    Ready,
    Failed,
}

/// Durable checkpoint for one versioned projection instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionCheckpoint {
    pub schema_version: u32,
    pub workspace_id: String,
    pub projection_id: String,
    pub applied_sequence: u64,
    pub status: ProjectionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

impl ProjectionCheckpoint {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_identifier("projection_id", &self.projection_id, MAX_MEMORY_ID_BYTES)?;
        validate_timestamp("updated_at_ms", self.updated_at_ms)?;
        match (self.status, &self.last_error) {
            (ProjectionStatus::Failed, Some(error)) => {
                validate_text("last_error", error, MAX_MEMORY_TEXT_BYTES)
            }
            (ProjectionStatus::Failed, None) => Err(ContractViolation::empty("last_error")),
            (_, Some(_)) => Err(violation(
                "last_error",
                "non-failed projection checkpoints must not include last_error",
                ContractViolationKind::InvalidFormat,
            )),
            (_, None) => Ok(()),
        }
    }
}

/// Immutable definition of the projection instances required by one recall
/// recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionSetManifest {
    pub schema_version: u32,
    pub projection_set_id: String,
    pub projection_set_version: u32,
    pub projection_ids: Vec<String>,
    /// Immutable artifacts that jointly define candidate generation,
    /// filtering, ranking, and context rendering for this set. Older preview
    /// manifests deserialize with an empty list; newly activated sets are
    /// expected to name every material artifact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_manifest_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer_artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_firewall_artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_build_id: Option<String>,
    pub manifest_sha256: String,
}

impl ProjectionSetManifest {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier(
            "projection_set_id",
            &self.projection_set_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        if self.projection_set_version == 0 {
            return Err(violation(
                "projection_set_version",
                "projection_set_version must be greater than zero",
                ContractViolationKind::BelowMinimum,
            ));
        }
        validate_nonempty_list(
            "projection_ids",
            &self.projection_ids,
            MAX_MEMORY_PROJECTIONS,
        )?;
        validate_unique_ids("projection_ids", &self.projection_ids)?;
        if self.artifact_ids.len() > MAX_MEMORY_PROJECTIONS {
            return Err(ContractViolation::exceeds_maximum(
                "artifact_ids",
                self.artifact_ids.len(),
                MAX_MEMORY_PROJECTIONS,
            ));
        }
        validate_unique_ids("artifact_ids", &self.artifact_ids)?;
        validate_optional_identifier(
            "policy_manifest_id",
            self.policy_manifest_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier(
            "tokenizer_artifact_id",
            self.tokenizer_artifact_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier(
            "context_firewall_artifact_id",
            self.context_firewall_artifact_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_identifier(
            "server_build_id",
            self.server_build_id.as_deref(),
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sha256("manifest_sha256", &self.manifest_sha256)
    }
}

/// Proof that every projection in one immutable set covers a commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityReceipt {
    pub schema_version: u32,
    pub workspace_id: String,
    pub commit_sequence: u64,
    pub projection_set_id: String,
    pub projection_set_version: u32,
    pub visible_sequence: u64,
}

impl VisibilityReceipt {
    pub fn validate(&self) -> ContractResult<()> {
        validate_schema_version(self.schema_version)?;
        validate_identifier("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_sequence("commit_sequence", self.commit_sequence)?;
        validate_identifier(
            "projection_set_id",
            &self.projection_set_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        if self.projection_set_version == 0 {
            return Err(violation(
                "projection_set_version",
                "projection_set_version must be greater than zero",
                ContractViolationKind::BelowMinimum,
            ));
        }
        if self.visible_sequence > self.commit_sequence {
            return Err(violation(
                "visible_sequence",
                "visible_sequence must not exceed commit_sequence",
                ContractViolationKind::InvalidFormat,
            ));
        }
        Ok(())
    }
}

/// Canonical SHA-256 of a typed content value.
pub fn canonical_content_sha256(content: &MemoryContent) -> ContractResult<String> {
    content.validate()?;
    let encoded = serde_json::to_vec(content).map_err(|error| {
        violation(
            "content",
            format!("memory content cannot be encoded: {error}"),
            ContractViolationKind::InvalidFormat,
        )
    })?;
    Ok(sha256_hex(&encoded))
}

/// Digest the immutable deletion plan fields, excluding the digest field
/// itself. Target lists are required to be sorted, so independently generated
/// plans over the same canonical state have one encoding.
pub fn memory_deletion_plan_sha256(plan: &MemoryDeletionPlan) -> ContractResult<String> {
    #[derive(Serialize)]
    struct CanonicalPlan<'a> {
        schema_version: u32,
        plan_id: &'a str,
        workspace_id: &'a str,
        namespace: &'a str,
        selector: &'a MemoryDeletionSelector,
        affected_assertion_ids: &'a [String],
        affected_version_ids: &'a [String],
        affected_evidence_ids: &'a [String],
        affected_observation_ids: &'a [String],
        affected_reinforcement_ids: &'a [String],
        affected_snapshot_ids: &'a [String],
        created_sequence: u64,
        created_at_ms: u64,
        expires_at_ms: u64,
        created_by_principal_id: &'a str,
        access_scope_sha256: &'a str,
        reason: &'a str,
    }

    let encoded = serde_json::to_vec(&CanonicalPlan {
        schema_version: plan.schema_version,
        plan_id: &plan.plan_id,
        workspace_id: &plan.workspace_id,
        namespace: &plan.namespace,
        selector: &plan.selector,
        affected_assertion_ids: &plan.affected_assertion_ids,
        affected_version_ids: &plan.affected_version_ids,
        affected_evidence_ids: &plan.affected_evidence_ids,
        affected_observation_ids: &plan.affected_observation_ids,
        affected_reinforcement_ids: &plan.affected_reinforcement_ids,
        affected_snapshot_ids: &plan.affected_snapshot_ids,
        created_sequence: plan.created_sequence,
        created_at_ms: plan.created_at_ms,
        expires_at_ms: plan.expires_at_ms,
        created_by_principal_id: &plan.created_by_principal_id,
        access_scope_sha256: &plan.access_scope_sha256,
        reason: &plan.reason,
    })
    .map_err(|error| {
        violation(
            "deletion_plan",
            format!("deletion plan cannot be encoded: {error}"),
            ContractViolationKind::InvalidFormat,
        )
    })?;
    Ok(sha256_hex(&encoded))
}

fn validate_authority_combination(version: &MemoryVersion) -> ContractResult<()> {
    if version.decision_authority == DecisionAuthority::GoverningPolicy
        && version.source_assurance != SourceAssurance::VerifiedCanonicalSource
    {
        return Err(violation(
            "decision_authority",
            "governing policy requires a verified canonical source",
            ContractViolationKind::InvalidFormat,
        ));
    }
    if version.decision_authority == DecisionAuthority::Operational
        && version.source_assurance == SourceAssurance::UnverifiedExternal
    {
        return Err(violation(
            "decision_authority",
            "unverified external content cannot have operational authority",
            ContractViolationKind::InvalidFormat,
        ));
    }
    if version.epistemic_formation == EpistemicFormation::ModelInference
        && version.decision_authority == DecisionAuthority::GoverningPolicy
    {
        return Err(violation(
            "decision_authority",
            "model inference cannot create governing policy",
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_schema_version(version: u32) -> ContractResult<()> {
    if version != MEMORY_SCHEMA_VERSION {
        return Err(violation(
            "schema_version",
            format!("unsupported schema_version {version}; expected {MEMORY_SCHEMA_VERSION}"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn effective_unix_nanos(legacy_ms: Option<i64>, nanos: Option<i64>) -> Option<i128> {
    nanos
        .map(i128::from)
        .or_else(|| legacy_ms.map(|value| i128::from(value) * 1_000_000))
}

fn validate_compatible_instants(
    field: &'static str,
    legacy_ms: Option<i64>,
    nanos: Option<i64>,
) -> ContractResult<()> {
    if let (Some(legacy_ms), Some(nanos)) = (legacy_ms, nanos) {
        if i128::from(legacy_ms) * 1_000_000 != i128::from(nanos) {
            return Err(violation(
                field,
                format!("{field} millisecond and nanosecond values disagree"),
                ContractViolationKind::InvalidFormat,
            ));
        }
    }
    Ok(())
}

fn validate_namespace(namespace: &str) -> ContractResult<()> {
    validate_identifier("namespace", namespace, MAX_MEMORY_NAMESPACE_BYTES)?;
    if namespace.starts_with('/')
        || namespace.ends_with('/')
        || namespace.contains("//")
        || namespace.split('/').count() > 16
        || namespace.split('/').any(|component| component == "..")
    {
        return Err(violation(
            "namespace",
            "namespace must be a relative path with at most 16 non-parent segments",
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_optional_identifier(
    field: &'static str,
    value: Option<&str>,
    maximum: usize,
) -> ContractResult<()> {
    if let Some(value) = value {
        validate_identifier(field, value, maximum)?;
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str, maximum: usize) -> ContractResult<()> {
    validate_text(field, value, maximum)?;
    if value.chars().any(char::is_control) {
        return Err(violation(
            field,
            format!("{field} must not contain control characters"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_text(field: &'static str, value: &str, maximum: usize) -> ContractResult<()> {
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
    if value.contains('\0') {
        return Err(violation(
            field,
            format!("{field} must not contain NUL"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_nonempty_list<T>(
    field: &'static str,
    values: &[T],
    maximum: usize,
) -> ContractResult<()> {
    if values.is_empty() {
        return Err(ContractViolation::empty(field));
    }
    if values.len() > maximum {
        return Err(ContractViolation::exceeds_maximum(
            field,
            values.len(),
            maximum,
        ));
    }
    Ok(())
}

fn validate_text_list(field: &'static str, values: &[String]) -> ContractResult<()> {
    if values.len() > 256 {
        return Err(ContractViolation::exceeds_maximum(field, values.len(), 256));
    }
    for value in values {
        validate_text(field, value, MAX_MEMORY_TEXT_BYTES)?;
    }
    Ok(())
}

fn validate_unique_ids(field: &'static str, values: &[String]) -> ContractResult<()> {
    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        validate_identifier(field, value, MAX_MEMORY_ID_BYTES)?;
        if !unique.insert(value) {
            return Err(violation(
                field,
                format!("{field} must not contain duplicates"),
                ContractViolationKind::InvalidFormat,
            ));
        }
    }
    Ok(())
}

fn validate_deletion_ids(field: &'static str, values: &[String]) -> ContractResult<()> {
    if values.len() > MAX_MEMORY_DELETION_TARGETS {
        return Err(ContractViolation::exceeds_maximum(
            field,
            values.len(),
            MAX_MEMORY_DELETION_TARGETS,
        ));
    }
    validate_unique_ids(field, values)?;
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(violation(
            field,
            "deletion target IDs must be strictly sorted",
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn validate_sequence(field: &'static str, sequence: u64) -> ContractResult<()> {
    if sequence == 0 {
        return Err(violation(
            field,
            format!("{field} must be greater than zero"),
            ContractViolationKind::BelowMinimum,
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
            format!("{field} must be a lowercase hexadecimal SHA-256 digest"),
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

fn normalize_predicate_v1(value: &str) -> String {
    value
        .nfc()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn nfc(value: &str) -> String {
    value.nfc().collect()
}

fn push_component(target: &mut Vec<u8>, component: &[u8]) {
    let length = u32::try_from(component.len()).expect("validated component fits in u32");
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(component);
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or_default(),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or_default(),
        _ => 1,
    }
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
    use serde_json::json;

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
            allowed_purposes: vec!["task_execution".to_string(), "debugging".to_string()],
        }
    }

    fn identity() -> MemoryAssertionIdentity {
        MemoryAssertionIdentity {
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            entity_key: "service:ingestion".to_string(),
            predicate: "  uses   retry policy ".trim().to_string(),
            kind: MemoryKind::TextFact,
        }
    }

    #[test]
    fn entity_and_privacy_subject_are_distinct() {
        let mut value = scope();
        value.data_subject_id = Some("user:42".to_string());
        value.validate().unwrap();
        assert_eq!(value.entity_key, "service:ingestion");
        assert_eq!(value.data_subject_id.as_deref(), Some("user:42"));
    }

    #[test]
    fn request_purposes_must_be_nonempty_and_unique() {
        let mut value = scope();
        value.allowed_purposes.clear();
        assert_eq!(
            value.validate().unwrap_err().kind,
            ContractViolationKind::Empty
        );

        value.allowed_purposes = vec!["debugging".to_string(), "debugging".to_string()];
        assert_eq!(
            value.validate().unwrap_err().kind,
            ContractViolationKind::InvalidFormat
        );
    }

    #[test]
    fn assertion_identity_normalizes_unicode_and_whitespace() {
        let first = identity();
        let mut second = first.clone();
        second.predicate = "USES retry   policy".to_string();
        assert_eq!(
            first.identity_hash().unwrap(),
            second.identity_hash().unwrap()
        );
        assert!(first.assertion_id().unwrap().starts_with("mem_a1_"));
    }

    #[test]
    fn assertion_validation_detects_identity_tampering() {
        let identity = identity();
        let mut assertion = MemoryAssertion {
            schema_version: MEMORY_SCHEMA_VERSION,
            assertion_id: identity.assertion_id().unwrap(),
            workspace_id: identity.workspace_id.clone(),
            namespace: identity.namespace.clone(),
            entity_key: identity.entity_key.clone(),
            predicate: identity.predicate.clone(),
            kind: identity.kind,
            identity_hash_version: MEMORY_IDENTITY_HASH_VERSION,
            identity_hash: identity.identity_hash().unwrap(),
            created_sequence: 1,
            created_at_ms: 1,
        };
        assertion.validate().unwrap();
        assertion.entity_key = "service:other".to_string();
        assert!(assertion.validate().is_err());
    }

    #[test]
    fn structured_content_rejects_excessive_depth() {
        let mut value = json!("leaf");
        for _ in 0..MAX_JSON_DEPTH {
            value = json!([value]);
        }
        assert!(MemoryContent::StructuredFact {
            schema_id: "schema:v1".to_string(),
            canonical_json: value,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn authority_axes_are_independent_but_policy_bounded() {
        let content = MemoryContent::TextFact {
            text: "restart only after the queue is drained".to_string(),
            language: Some("en".to_string()),
        };
        let mut version = MemoryVersion {
            schema_version: MEMORY_SCHEMA_VERSION,
            version_id: "version-1".to_string(),
            assertion_id: identity().assertion_id().unwrap(),
            parent_version_ids: vec![],
            scope: scope(),
            kind: content.kind(),
            content_sha256: canonical_content_sha256(&content).unwrap(),
            content,
            valid_from_ms: None,
            valid_to_ms: None,
            valid_from_unix_nanos: None,
            valid_to_unix_nanos: None,
            epistemic_formation: EpistemicFormation::ModelInference,
            source_assurance: SourceAssurance::UnverifiedExternal,
            decision_authority: DecisionAuthority::GoverningPolicy,
            confidence: Some(0.99),
            evidence_ids: vec!["evidence-1".to_string()],
            derivation_id: None,
            compiler_artifact_id: None,
            policy_decision_id: "policy-decision-1".to_string(),
            committed_sequence: 1,
            committed_at_ms: 1,
            created_by_principal_id: "service:compiler".to_string(),
        };
        assert!(version.validate().is_err());

        version.source_assurance = SourceAssurance::VerifiedCanonicalSource;
        version.decision_authority = DecisionAuthority::Advisory;
        version.validate().unwrap();
    }

    #[test]
    fn visibility_is_bound_to_projection_set_and_commit() {
        let receipt = VisibilityReceipt {
            schema_version: MEMORY_SCHEMA_VERSION,
            workspace_id: "workspace-a".to_string(),
            commit_sequence: 12,
            projection_set_id: "typed-recall".to_string(),
            projection_set_version: 1,
            visible_sequence: 11,
        };
        receipt.validate().unwrap();

        let mut invalid = receipt;
        invalid.visible_sequence = 13;
        assert!(invalid.validate().is_err());
    }
}
