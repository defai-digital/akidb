//! Versioned, deterministic compiler boundary for authoritative Memory.
//!
//! Compiler output is proposal data only. These types intentionally contain no
//! credential, grant, source-assurance, decision-authority, policy-mutation, or
//! activation fields. The Memory service must independently validate and
//! authorize every resulting canonical mutation.

use crate::{
    canonical_content_sha256, EpistemicFormation, MemoryContent, MemoryKind, MemoryScope,
    MAX_MEMORY_ACTIVE_HEADS, MAX_MEMORY_EVIDENCE, MAX_MEMORY_ID_BYTES, MAX_MEMORY_TEXT_BYTES,
    MEMORY_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

pub const REFERENCE_TEXT_COMPILER_ARTIFACT_ID: &str = "compiler:reference-text-v1";
pub const MEMORY_COMPILER_CONTRACT_VERSION: u32 = 1;
pub const MAX_MEMORY_COMPILER_JOB_ATTEMPTS: u32 = 100;

#[derive(Debug, Error)]
pub enum MemoryCompilerError {
    #[error("invalid compiler input: {0}")]
    InvalidInput(String),
    #[error("invalid compiler plan: {0}")]
    InvalidPlan(String),
    #[error("compiler serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type MemoryCompilerResult<T> = Result<T, MemoryCompilerError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCompilerJobState {
    Pending,
    Running,
    Succeeded,
    DeadLetter,
}

/// Immutable definition of one scheduled compiler input. Observation payloads
/// remain in the canonical ledger; the job only binds their IDs and the exact
/// compiler/policy artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCompilerJob {
    pub schema_version: u32,
    pub job_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub observation_ids: Vec<String>,
    pub compiler_artifact_id: String,
    pub policy_manifest_id: String,
    pub scheduled_at_ms: u64,
    pub max_attempts: u32,
    pub created_at_ms: u64,
    pub created_by_principal_id: String,
    pub job_sha256: String,
}

impl MemoryCompilerJob {
    pub fn validate(&self) -> MemoryCompilerResult<()> {
        if self.schema_version != MEMORY_SCHEMA_VERSION {
            return Err(MemoryCompilerError::InvalidInput(format!(
                "unsupported job schema version {}",
                self.schema_version
            )));
        }
        validate_id("job_id", &self.job_id)?;
        validate_id("workspace_id", &self.workspace_id)?;
        validate_id("namespace", &self.namespace)?;
        if self.observation_ids.is_empty() || self.observation_ids.len() > MAX_MEMORY_EVIDENCE {
            return Err(MemoryCompilerError::InvalidInput(format!(
                "observation_ids must contain 1..={MAX_MEMORY_EVIDENCE} entries"
            )));
        }
        if !is_strictly_sorted_unique(&self.observation_ids) {
            return Err(MemoryCompilerError::InvalidInput(
                "observation_ids must be strictly sorted and unique".to_string(),
            ));
        }
        for observation_id in &self.observation_ids {
            validate_id("observation_id", observation_id)?;
        }
        validate_id("compiler_artifact_id", &self.compiler_artifact_id)?;
        validate_id("policy_manifest_id", &self.policy_manifest_id)?;
        if self.scheduled_at_ms == 0 || self.created_at_ms == 0 {
            return Err(MemoryCompilerError::InvalidInput(
                "job timestamps must be greater than zero".to_string(),
            ));
        }
        if self.max_attempts == 0 || self.max_attempts > MAX_MEMORY_COMPILER_JOB_ATTEMPTS {
            return Err(MemoryCompilerError::InvalidInput(format!(
                "max_attempts must be between 1 and {MAX_MEMORY_COMPILER_JOB_ATTEMPTS}"
            )));
        }
        validate_id("created_by_principal_id", &self.created_by_principal_id)?;
        validate_sha256("job_sha256", &self.job_sha256)?;
        if self.job_sha256 != memory_compiler_job_sha256(self)? {
            return Err(MemoryCompilerError::InvalidInput(
                "job digest differs from canonical immutable fields".to_string(),
            ));
        }
        Ok(())
    }
}

/// Mutable scheduler state, persisted atomically for each claim/completion or
/// failure transition. It contains no observation content or authority fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCompilerJobStatus {
    pub schema_version: u32,
    pub job_id: String,
    pub workspace_id: String,
    pub state: MemoryCompilerJobState,
    pub attempt_count: u32,
    pub next_attempt_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<String>,
    pub updated_at_ms: u64,
}

impl MemoryCompilerJobStatus {
    pub fn validate(&self, job: &MemoryCompilerJob) -> MemoryCompilerResult<()> {
        if self.schema_version != MEMORY_SCHEMA_VERSION
            || self.job_id != job.job_id
            || self.workspace_id != job.workspace_id
            || self.attempt_count > job.max_attempts
            || self.next_attempt_at_ms == 0
            || self.updated_at_ms == 0
        {
            return Err(MemoryCompilerError::InvalidInput(
                "compiler job status identity, counts, or timestamps are invalid".to_string(),
            ));
        }
        if let Some(owner) = &self.lease_owner_id {
            validate_id("lease_owner_id", owner)?;
        }
        if let Some(digest) = &self.plan_sha256 {
            validate_sha256("plan_sha256", digest)?;
        }
        if let Some(code) = &self.last_error_code {
            validate_id("last_error_code", code)?;
        }
        match self.state {
            MemoryCompilerJobState::Pending => {
                if self.lease_owner_id.is_some()
                    || self.lease_expires_at_ms.is_some()
                    || self.plan_sha256.is_some()
                {
                    return Err(MemoryCompilerError::InvalidInput(
                        "pending job cannot hold a lease or completed plan".to_string(),
                    ));
                }
            }
            MemoryCompilerJobState::Running => {
                if self.attempt_count == 0
                    || self.lease_owner_id.is_none()
                    || self
                        .lease_expires_at_ms
                        .is_none_or(|expiry| expiry <= self.updated_at_ms)
                    || self.plan_sha256.is_some()
                {
                    return Err(MemoryCompilerError::InvalidInput(
                        "running job requires one live lease and no completed plan".to_string(),
                    ));
                }
            }
            MemoryCompilerJobState::Succeeded => {
                if self.attempt_count == 0
                    || self.lease_owner_id.is_some()
                    || self.lease_expires_at_ms.is_some()
                    || self.plan_sha256.is_none()
                {
                    return Err(MemoryCompilerError::InvalidInput(
                        "succeeded job requires a plan and no lease".to_string(),
                    ));
                }
            }
            MemoryCompilerJobState::DeadLetter => {
                if self.attempt_count == 0
                    || self.lease_owner_id.is_some()
                    || self.lease_expires_at_ms.is_some()
                    || self.plan_sha256.is_some()
                    || self.last_error_code.is_none()
                {
                    return Err(MemoryCompilerError::InvalidInput(
                        "dead-letter job requires failure evidence and no lease".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCompilerJobFailure {
    pub schema_version: u32,
    pub failure_id: String,
    pub workspace_id: String,
    pub job_id: String,
    pub attempt: u32,
    pub worker_id: String,
    pub error_code: String,
    pub retryable: bool,
    pub failed_at_ms: u64,
}

impl MemoryCompilerJobFailure {
    pub fn validate(&self, job: &MemoryCompilerJob) -> MemoryCompilerResult<()> {
        if self.schema_version != MEMORY_SCHEMA_VERSION
            || self.workspace_id != job.workspace_id
            || self.job_id != job.job_id
            || self.attempt == 0
            || self.attempt > job.max_attempts
            || self.failed_at_ms == 0
        {
            return Err(MemoryCompilerError::InvalidInput(
                "compiler failure identity, attempt, or time is invalid".to_string(),
            ));
        }
        validate_id("failure_id", &self.failure_id)?;
        validate_id("worker_id", &self.worker_id)?;
        validate_id("error_code", &self.error_code)
    }
}

pub fn memory_compiler_job_sha256(job: &MemoryCompilerJob) -> MemoryCompilerResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        job.schema_version,
        &job.job_id,
        &job.workspace_id,
        &job.namespace,
        &job.observation_ids,
        &job.compiler_artifact_id,
        &job.policy_manifest_id,
        job.scheduled_at_ms,
        job.max_attempts,
        job.created_at_ms,
        &job.created_by_principal_id,
    ))?))
}

/// Authorized observation view passed to a compiler. The retained text is
/// already scope-filtered by the caller. A predicate hint is explicit input,
/// not a model-derived authority claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerObservation {
    pub observation_id: String,
    pub evidence_id: String,
    pub scope: MemoryScope,
    pub retained_text: String,
    pub predicate_hint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from_unix_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to_unix_nanos: Option<i64>,
}

impl CompilerObservation {
    pub fn validate(&self) -> MemoryCompilerResult<()> {
        validate_id("observation_id", &self.observation_id)?;
        validate_id("evidence_id", &self.evidence_id)?;
        self.scope
            .validate()
            .map_err(|error| MemoryCompilerError::InvalidInput(error.to_string()))?;
        validate_text("retained_text", &self.retained_text, MAX_MEMORY_TEXT_BYTES)?;
        validate_id("predicate_hint", &self.predicate_hint)?;
        if self
            .valid_from_unix_nanos
            .is_some_and(|instant| instant <= 0)
            || self.valid_to_unix_nanos.is_some_and(|instant| instant <= 0)
        {
            return Err(MemoryCompilerError::InvalidInput(
                "valid-time instants must be greater than zero".to_string(),
            ));
        }
        if self
            .valid_from_unix_nanos
            .zip(self.valid_to_unix_nanos)
            .is_some_and(|(from, to)| to <= from)
        {
            return Err(MemoryCompilerError::InvalidInput(
                "valid_to_unix_nanos must be greater than valid_from_unix_nanos".to_string(),
            ));
        }
        Ok(())
    }
}

/// Existing authorized head metadata is sufficient for optimistic
/// expectations. Compiler input never includes hidden or out-of-scope heads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerHead {
    pub version_id: String,
    pub scope: MemoryScope,
    pub predicate: String,
    pub kind: MemoryKind,
}

impl CompilerHead {
    pub fn validate(&self) -> MemoryCompilerResult<()> {
        validate_id("head.version_id", &self.version_id)?;
        self.scope
            .validate()
            .map_err(|error| MemoryCompilerError::InvalidInput(error.to_string()))?;
        validate_id("head.predicate", &self.predicate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCompilerInput {
    pub contract_version: u32,
    pub observations: Vec<CompilerObservation>,
    pub existing_authorized_heads: Vec<CompilerHead>,
    pub policy_manifest_id: String,
    pub compiler_artifact_id: String,
}

impl MemoryCompilerInput {
    pub fn validate(&self) -> MemoryCompilerResult<()> {
        if self.contract_version != MEMORY_COMPILER_CONTRACT_VERSION {
            return Err(MemoryCompilerError::InvalidInput(format!(
                "unsupported compiler contract version {}",
                self.contract_version
            )));
        }
        if self.observations.is_empty() || self.observations.len() > MAX_MEMORY_EVIDENCE {
            return Err(MemoryCompilerError::InvalidInput(format!(
                "observations must contain 1..={MAX_MEMORY_EVIDENCE} entries"
            )));
        }
        validate_id("policy_manifest_id", &self.policy_manifest_id)?;
        validate_id("compiler_artifact_id", &self.compiler_artifact_id)?;
        let mut observation_ids = HashSet::new();
        let mut evidence_ids = HashSet::new();
        for observation in &self.observations {
            observation.validate()?;
            if !observation_ids.insert(&observation.observation_id) {
                return Err(MemoryCompilerError::InvalidInput(
                    "observation IDs must be unique".to_string(),
                ));
            }
            if !evidence_ids.insert(&observation.evidence_id) {
                return Err(MemoryCompilerError::InvalidInput(
                    "observation evidence IDs must be unique".to_string(),
                ));
            }
        }
        if self.existing_authorized_heads.len() > MAX_MEMORY_ACTIVE_HEADS * self.observations.len()
        {
            return Err(MemoryCompilerError::InvalidInput(
                "too many existing authorized heads".to_string(),
            ));
        }
        let mut head_ids = HashSet::new();
        for head in &self.existing_authorized_heads {
            head.validate()?;
            if !head_ids.insert(&head.version_id) {
                return Err(MemoryCompilerError::InvalidInput(
                    "existing head IDs must be unique".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// One untrusted proposal. The server derives assurance and authority after
/// validating this candidate; the compiler cannot express either.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerCandidate {
    pub candidate_id: String,
    pub source_observation_ids: Vec<String>,
    pub input_evidence_ids: Vec<String>,
    pub scope: MemoryScope,
    pub predicate: String,
    pub content: MemoryContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from_unix_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to_unix_nanos: Option<i64>,
    pub epistemic_formation: EpistemicFormation,
    pub expected_head_version_ids: Vec<String>,
    pub reason: String,
    pub compiler_artifact_id: String,
    pub deterministic_parameters_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompilerProposalRelationKind {
    PossibleDuplicate,
    Contradicts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerProposalRelation {
    pub relation_id: String,
    pub kind: CompilerProposalRelationKind,
    pub from_candidate_id: String,
    pub to_candidate_id: String,
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCommitPlan {
    pub contract_version: u32,
    pub compiler_artifact_id: String,
    pub policy_manifest_id: String,
    pub candidates: Vec<CompilerCandidate>,
    pub proposed_relations: Vec<CompilerProposalRelation>,
    pub plan_sha256: String,
}

impl MemoryCommitPlan {
    pub fn validate_against(&self, input: &MemoryCompilerInput) -> MemoryCompilerResult<()> {
        input.validate()?;
        if self.contract_version != MEMORY_COMPILER_CONTRACT_VERSION
            || self.compiler_artifact_id != input.compiler_artifact_id
            || self.policy_manifest_id != input.policy_manifest_id
        {
            return Err(MemoryCompilerError::InvalidPlan(
                "plan contract or artifact identity differs from input".to_string(),
            ));
        }
        if self.candidates.is_empty() || self.candidates.len() > input.observations.len() {
            return Err(MemoryCompilerError::InvalidPlan(
                "candidate count must be between one and the observation count".to_string(),
            ));
        }
        let observations: HashMap<&str, &CompilerObservation> = input
            .observations
            .iter()
            .map(|observation| (observation.observation_id.as_str(), observation))
            .collect();
        let authorized_head_ids: HashSet<&str> = input
            .existing_authorized_heads
            .iter()
            .map(|head| head.version_id.as_str())
            .collect();
        let mut candidate_ids = HashSet::new();
        for candidate in &self.candidates {
            validate_id("candidate_id", &candidate.candidate_id)
                .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
            if !candidate_ids.insert(candidate.candidate_id.as_str()) {
                return Err(MemoryCompilerError::InvalidPlan(
                    "candidate IDs must be unique".to_string(),
                ));
            }
            if candidate.source_observation_ids.len() != 1
                || candidate.input_evidence_ids.len() != 1
            {
                return Err(MemoryCompilerError::InvalidPlan(
                    "reference contract candidates must name one observation and evidence"
                        .to_string(),
                ));
            }
            let observation = observations
                .get(candidate.source_observation_ids[0].as_str())
                .ok_or_else(|| {
                    MemoryCompilerError::InvalidPlan(
                        "candidate references an unauthorized observation".to_string(),
                    )
                })?;
            if candidate.input_evidence_ids[0] != observation.evidence_id
                || candidate.scope != observation.scope
                || candidate.predicate != observation.predicate_hint
                || candidate.valid_from_unix_nanos != observation.valid_from_unix_nanos
                || candidate.valid_to_unix_nanos != observation.valid_to_unix_nanos
            {
                return Err(MemoryCompilerError::InvalidPlan(
                    "candidate expands or changes its authorized observation boundary".to_string(),
                ));
            }
            candidate
                .content
                .validate()
                .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
            if candidate.epistemic_formation != EpistemicFormation::DeterministicDerivation {
                return Err(MemoryCompilerError::InvalidPlan(
                    "compiler candidates must be deterministic derivations".to_string(),
                ));
            }
            if candidate.compiler_artifact_id != input.compiler_artifact_id {
                return Err(MemoryCompilerError::InvalidPlan(
                    "candidate compiler artifact differs from input".to_string(),
                ));
            }
            validate_sha256(
                "deterministic_parameters_sha256",
                &candidate.deterministic_parameters_sha256,
            )
            .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
            if candidate
                .expected_head_version_ids
                .iter()
                .any(|id| !authorized_head_ids.contains(id.as_str()))
            {
                return Err(MemoryCompilerError::InvalidPlan(
                    "candidate names an unauthorized expected head".to_string(),
                ));
            }
            let expected_id = candidate_id(candidate)?;
            if candidate.candidate_id != expected_id {
                return Err(MemoryCompilerError::InvalidPlan(
                    "candidate ID differs from canonical candidate content".to_string(),
                ));
            }
        }
        let candidate_ids: HashSet<&str> = self
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect();
        let mut relation_ids = HashSet::new();
        for relation in &self.proposed_relations {
            validate_id("relation_id", &relation.relation_id)
                .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
            if !relation_ids.insert(relation.relation_id.as_str())
                || relation.from_candidate_id == relation.to_candidate_id
                || !candidate_ids.contains(relation.from_candidate_id.as_str())
                || !candidate_ids.contains(relation.to_candidate_id.as_str())
            {
                return Err(MemoryCompilerError::InvalidPlan(
                    "proposal relation endpoints or identity are invalid".to_string(),
                ));
            }
            validate_id("reason_code", &relation.reason_code)
                .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
        }
        validate_sha256("plan_sha256", &self.plan_sha256)
            .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
        if self.plan_sha256 != commit_plan_sha256(self)? {
            return Err(MemoryCompilerError::InvalidPlan(
                "plan digest differs from canonical plan fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub trait MemoryCompiler: Send + Sync {
    fn artifact_id(&self) -> &str;
    fn compile(&self, input: &MemoryCompilerInput) -> MemoryCompilerResult<MemoryCommitPlan>;
}

/// Deterministic non-AX implementation. It performs no semantic inference: an
/// authorized predicate hint plus retained text becomes a typed TextFact
/// proposal, preserving exact scope and evidence.
#[derive(Debug, Default)]
pub struct ReferenceTextCompiler;

impl MemoryCompiler for ReferenceTextCompiler {
    fn artifact_id(&self) -> &str {
        REFERENCE_TEXT_COMPILER_ARTIFACT_ID
    }

    fn compile(&self, input: &MemoryCompilerInput) -> MemoryCompilerResult<MemoryCommitPlan> {
        input.validate()?;
        if input.compiler_artifact_id != self.artifact_id() {
            return Err(MemoryCompilerError::InvalidInput(format!(
                "input requested {}, but this compiler is {}",
                input.compiler_artifact_id,
                self.artifact_id()
            )));
        }
        let parameters_sha256 = sha256_hex(
            format!(
                "akidb-reference-text-compiler-v1\0{}\0{}",
                input.compiler_artifact_id, input.policy_manifest_id
            )
            .as_bytes(),
        );
        let mut observations = input.observations.iter().collect::<Vec<_>>();
        observations.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        let mut candidates = Vec::with_capacity(observations.len());
        for observation in observations {
            let mut expected_head_version_ids = input
                .existing_authorized_heads
                .iter()
                .filter(|head| {
                    head.scope.workspace_id == observation.scope.workspace_id
                        && head.scope.namespace == observation.scope.namespace
                        && head.scope.entity_key == observation.scope.entity_key
                        && head.predicate == observation.predicate_hint
                        && head.kind == MemoryKind::TextFact
                })
                .map(|head| head.version_id.clone())
                .collect::<Vec<_>>();
            expected_head_version_ids.sort();
            let mut candidate = CompilerCandidate {
                candidate_id: String::new(),
                source_observation_ids: vec![observation.observation_id.clone()],
                input_evidence_ids: vec![observation.evidence_id.clone()],
                scope: observation.scope.clone(),
                predicate: observation.predicate_hint.clone(),
                content: MemoryContent::TextFact {
                    text: observation.retained_text.clone(),
                    language: None,
                },
                valid_from_unix_nanos: observation.valid_from_unix_nanos,
                valid_to_unix_nanos: observation.valid_to_unix_nanos,
                epistemic_formation: EpistemicFormation::DeterministicDerivation,
                expected_head_version_ids,
                reason: "deterministic reference compiler proposal".to_string(),
                compiler_artifact_id: input.compiler_artifact_id.clone(),
                deterministic_parameters_sha256: parameters_sha256.clone(),
            };
            candidate.candidate_id = candidate_id(&candidate)?;
            candidates.push(candidate);
        }

        let mut proposed_relations = Vec::new();
        for left_index in 0..candidates.len() {
            for right_index in (left_index + 1)..candidates.len() {
                let left = &candidates[left_index];
                let right = &candidates[right_index];
                if left.scope.workspace_id != right.scope.workspace_id
                    || left.scope.namespace != right.scope.namespace
                    || left.scope.entity_key != right.scope.entity_key
                    || left.predicate != right.predicate
                    || left.content.kind() != right.content.kind()
                {
                    continue;
                }
                let left_digest = canonical_content_sha256(&left.content)
                    .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
                let right_digest = canonical_content_sha256(&right.content)
                    .map_err(|error| MemoryCompilerError::InvalidPlan(error.to_string()))?;
                let (kind, reason_code) = if left_digest == right_digest {
                    (
                        CompilerProposalRelationKind::PossibleDuplicate,
                        "EXPLICIT_POSSIBLE_DUPLICATE",
                    )
                } else {
                    (
                        CompilerProposalRelationKind::Contradicts,
                        "EXPLICIT_CONTRADICTION_REQUIRES_POLICY",
                    )
                };
                let relation_material =
                    format!("{}\0{}\0{:?}", left.candidate_id, right.candidate_id, kind);
                proposed_relations.push(CompilerProposalRelation {
                    relation_id: format!("cmp_r1_{}", sha256_hex(relation_material.as_bytes())),
                    kind,
                    from_candidate_id: left.candidate_id.clone(),
                    to_candidate_id: right.candidate_id.clone(),
                    reason_code: reason_code.to_string(),
                });
            }
        }
        proposed_relations.sort_by(|left, right| left.relation_id.cmp(&right.relation_id));
        let mut plan = MemoryCommitPlan {
            contract_version: MEMORY_COMPILER_CONTRACT_VERSION,
            compiler_artifact_id: input.compiler_artifact_id.clone(),
            policy_manifest_id: input.policy_manifest_id.clone(),
            candidates,
            proposed_relations,
            plan_sha256: String::new(),
        };
        plan.plan_sha256 = commit_plan_sha256(&plan)?;
        plan.validate_against(input)?;
        Ok(plan)
    }
}

/// Reusable conformance check for third-party/non-AX implementations.
pub fn verify_compiler_conformance(
    compiler: &dyn MemoryCompiler,
    input: &MemoryCompilerInput,
) -> MemoryCompilerResult<MemoryCommitPlan> {
    if compiler.artifact_id() != input.compiler_artifact_id {
        return Err(MemoryCompilerError::InvalidInput(
            "compiler artifact identity differs from the input contract".to_string(),
        ));
    }
    let first = compiler.compile(input)?;
    let second = compiler.compile(input)?;
    first.validate_against(input)?;
    second.validate_against(input)?;
    if serde_json::to_vec(&first)? != serde_json::to_vec(&second)? {
        return Err(MemoryCompilerError::InvalidPlan(
            "compiler produced nondeterministic output for identical input".to_string(),
        ));
    }
    Ok(first)
}

fn candidate_id(candidate: &CompilerCandidate) -> MemoryCompilerResult<String> {
    #[derive(Serialize)]
    struct CandidateFingerprint<'a> {
        source_observation_ids: &'a [String],
        input_evidence_ids: &'a [String],
        scope: &'a MemoryScope,
        predicate: &'a str,
        content: &'a MemoryContent,
        valid_from_unix_nanos: Option<i64>,
        valid_to_unix_nanos: Option<i64>,
        epistemic_formation: EpistemicFormation,
        expected_head_version_ids: &'a [String],
        reason: &'a str,
        compiler_artifact_id: &'a str,
        deterministic_parameters_sha256: &'a str,
    }
    let encoded = serde_json::to_vec(&CandidateFingerprint {
        source_observation_ids: &candidate.source_observation_ids,
        input_evidence_ids: &candidate.input_evidence_ids,
        scope: &candidate.scope,
        predicate: &candidate.predicate,
        content: &candidate.content,
        valid_from_unix_nanos: candidate.valid_from_unix_nanos,
        valid_to_unix_nanos: candidate.valid_to_unix_nanos,
        epistemic_formation: candidate.epistemic_formation,
        expected_head_version_ids: &candidate.expected_head_version_ids,
        reason: &candidate.reason,
        compiler_artifact_id: &candidate.compiler_artifact_id,
        deterministic_parameters_sha256: &candidate.deterministic_parameters_sha256,
    })?;
    Ok(format!("cmp_c1_{}", sha256_hex(&encoded)))
}

fn commit_plan_sha256(plan: &MemoryCommitPlan) -> MemoryCompilerResult<String> {
    #[derive(Serialize)]
    struct PlanFingerprint<'a> {
        contract_version: u32,
        compiler_artifact_id: &'a str,
        policy_manifest_id: &'a str,
        candidates: &'a [CompilerCandidate],
        proposed_relations: &'a [CompilerProposalRelation],
    }
    Ok(sha256_hex(&serde_json::to_vec(&PlanFingerprint {
        contract_version: plan.contract_version,
        compiler_artifact_id: &plan.compiler_artifact_id,
        policy_manifest_id: &plan.policy_manifest_id,
        candidates: &plan.candidates,
        proposed_relations: &plan.proposed_relations,
    })?))
}

fn validate_id(field: &str, value: &str) -> MemoryCompilerResult<()> {
    validate_text(field, value, MAX_MEMORY_ID_BYTES)?;
    if value.chars().any(char::is_control) {
        return Err(MemoryCompilerError::InvalidInput(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str, maximum: usize) -> MemoryCompilerResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > maximum
        || value.contains('\0')
    {
        return Err(MemoryCompilerError::InvalidInput(format!(
            "{field} must be non-empty, trimmed, NUL-free, and at most {maximum} bytes"
        )));
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> MemoryCompilerResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MemoryCompilerError::InvalidInput(format!(
            "{field} must be a lowercase hexadecimal SHA-256 digest"
        )));
    }
    Ok(())
}

fn is_strictly_sorted_unique(values: &[String]) -> bool {
    values
        .windows(2)
        .all(|window| window[0].as_bytes() < window[1].as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
            data_subject_id: Some("subject-1".to_string()),
            owner_agent_id: Some("agent:compiler".to_string()),
            session_id: Some("session-1".to_string()),
            task_id: Some("task-1".to_string()),
            sensitivity: Sensitivity::Internal,
            allowed_purposes: vec!["debugging".to_string()],
        }
    }

    fn input() -> MemoryCompilerInput {
        MemoryCompilerInput {
            contract_version: MEMORY_COMPILER_CONTRACT_VERSION,
            observations: vec![
                CompilerObservation {
                    observation_id: "observation-2".to_string(),
                    evidence_id: "evidence-2".to_string(),
                    scope: scope(),
                    retained_text: "Restart immediately without draining.".to_string(),
                    predicate_hint: "uses recovery procedure".to_string(),
                    valid_from_unix_nanos: Some(1_700_000_000_000_000_000),
                    valid_to_unix_nanos: None,
                },
                CompilerObservation {
                    observation_id: "observation-1".to_string(),
                    evidence_id: "evidence-1".to_string(),
                    scope: scope(),
                    retained_text: "Drain the queue before restarting.".to_string(),
                    predicate_hint: "uses recovery procedure".to_string(),
                    valid_from_unix_nanos: Some(1_700_000_000_000_000_000),
                    valid_to_unix_nanos: None,
                },
            ],
            existing_authorized_heads: vec![CompilerHead {
                version_id: "version-head-1".to_string(),
                scope: scope(),
                predicate: "uses recovery procedure".to_string(),
                kind: MemoryKind::TextFact,
            }],
            policy_manifest_id: "memory-authority-policy-v1".to_string(),
            compiler_artifact_id: REFERENCE_TEXT_COMPILER_ARTIFACT_ID.to_string(),
        }
    }

    #[test]
    fn reference_compiler_is_deterministic_and_surfaces_conflicts() {
        let compiler = ReferenceTextCompiler;
        let first = verify_compiler_conformance(&compiler, &input()).unwrap();
        let mut reordered = input();
        reordered.observations.reverse();
        let second = verify_compiler_conformance(&compiler, &reordered).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.candidates.len(), 2);
        assert!(first
            .proposed_relations
            .iter()
            .any(|relation| relation.kind == CompilerProposalRelationKind::Contradicts));
        assert!(first.candidates.iter().all(|candidate| {
            candidate.expected_head_version_ids == vec!["version-head-1".to_string()]
        }));
        let encoded = serde_json::to_string(&first).unwrap();
        assert!(!encoded.contains("decision_authority"));
        assert!(!encoded.contains("source_assurance"));
        assert!(!encoded.contains("credential"));
    }

    #[test]
    fn conformance_rejects_scope_expansion() {
        let compiler = ReferenceTextCompiler;
        let mut plan = compiler.compile(&input()).unwrap();
        plan.candidates[0].scope.workspace_id = "workspace-b".to_string();
        assert!(plan.validate_against(&input()).is_err());
    }
}
