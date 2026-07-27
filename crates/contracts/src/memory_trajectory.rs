//! Deterministic trajectory-to-procedure proposals for authoritative Memory.
//!
//! A trajectory compiler consumes already-authorized, evidence-bound events and
//! emits one untrusted procedure candidate. The candidate is confined to the
//! exact input scope and cannot carry credentials, grants, source assurance,
//! decision authority, or policy changes. The Memory service remains
//! responsible for authorizing and committing any resulting version.

use crate::{
    EpistemicFormation, MemoryContent, MemoryScope, MAX_MEMORY_EVIDENCE, MAX_MEMORY_ID_BYTES,
    MAX_MEMORY_TEXT_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use thiserror::Error;

pub const MEMORY_TRAJECTORY_CONTRACT_VERSION: u32 = 1;
pub const REFERENCE_TRAJECTORY_COMPILER_ARTIFACT_ID: &str = "trajectory:reference-procedure-v1";

#[derive(Debug, Error)]
pub enum MemoryTrajectoryError {
    #[error("invalid trajectory input: {0}")]
    InvalidInput(String),
    #[error("invalid trajectory plan: {0}")]
    InvalidPlan(String),
    #[error("trajectory serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type MemoryTrajectoryResult<T> = Result<T, MemoryTrajectoryError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrajectoryEventOutcome {
    Succeeded,
    Failed,
}

/// One retained event from an authorized trajectory view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryEvent {
    pub event_id: String,
    pub evidence_id: String,
    pub scope: MemoryScope,
    pub ordinal: u32,
    pub action: String,
    pub outcome: TrajectoryEventOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_recovery: Option<String>,
}

impl TrajectoryEvent {
    fn validate(&self) -> MemoryTrajectoryResult<()> {
        validate_id("event_id", &self.event_id)?;
        validate_id("evidence_id", &self.evidence_id)?;
        self.scope
            .validate()
            .map_err(|error| MemoryTrajectoryError::InvalidInput(error.to_string()))?;
        if self.ordinal == 0 {
            return Err(MemoryTrajectoryError::InvalidInput(
                "event ordinal must be greater than zero".to_string(),
            ));
        }
        validate_text("event.action", &self.action, MAX_MEMORY_TEXT_BYTES)?;
        match (self.outcome, self.failure_recovery.as_deref()) {
            (TrajectoryEventOutcome::Succeeded, None) => Ok(()),
            (TrajectoryEventOutcome::Failed, Some(recovery)) => {
                validate_text("event.failure_recovery", recovery, MAX_MEMORY_TEXT_BYTES)
            }
            (TrajectoryEventOutcome::Succeeded, Some(_)) => {
                Err(MemoryTrajectoryError::InvalidInput(
                    "a successful event cannot invent a failure recovery".to_string(),
                ))
            }
            (TrajectoryEventOutcome::Failed, None) => Err(MemoryTrajectoryError::InvalidInput(
                "a failed event requires an explicit retained recovery".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryTrajectoryInput {
    pub contract_version: u32,
    pub trajectory_artifact_id: String,
    pub policy_manifest_id: String,
    pub scope: MemoryScope,
    pub predicate: String,
    pub title: String,
    pub events: Vec<TrajectoryEvent>,
}

impl MemoryTrajectoryInput {
    pub fn validate(&self) -> MemoryTrajectoryResult<()> {
        if self.contract_version != MEMORY_TRAJECTORY_CONTRACT_VERSION {
            return Err(MemoryTrajectoryError::InvalidInput(format!(
                "unsupported trajectory contract version {}",
                self.contract_version
            )));
        }
        validate_id("trajectory_artifact_id", &self.trajectory_artifact_id)?;
        validate_id("policy_manifest_id", &self.policy_manifest_id)?;
        self.scope
            .validate()
            .map_err(|error| MemoryTrajectoryError::InvalidInput(error.to_string()))?;
        validate_id("predicate", &self.predicate)?;
        validate_text("title", &self.title, MAX_MEMORY_ID_BYTES)?;
        if self.events.is_empty() || self.events.len() > MAX_MEMORY_EVIDENCE {
            return Err(MemoryTrajectoryError::InvalidInput(format!(
                "events must contain 1..={MAX_MEMORY_EVIDENCE} entries"
            )));
        }

        let mut event_ids = HashSet::new();
        let mut evidence_ids = HashSet::new();
        let mut expected_ordinal = 1_u32;
        let mut successful_actions = 0_usize;
        for event in &self.events {
            event.validate()?;
            if event.scope != self.scope {
                return Err(MemoryTrajectoryError::InvalidInput(
                    "every trajectory event must have the exact authorized input scope".to_string(),
                ));
            }
            if event.ordinal != expected_ordinal {
                return Err(MemoryTrajectoryError::InvalidInput(
                    "event ordinals must be contiguous and start at one".to_string(),
                ));
            }
            expected_ordinal = expected_ordinal.checked_add(1).ok_or_else(|| {
                MemoryTrajectoryError::InvalidInput("event ordinal overflow".to_string())
            })?;
            if !event_ids.insert(&event.event_id) || !evidence_ids.insert(&event.evidence_id) {
                return Err(MemoryTrajectoryError::InvalidInput(
                    "event and evidence IDs must be unique".to_string(),
                ));
            }
            if event.outcome == TrajectoryEventOutcome::Succeeded {
                successful_actions += 1;
            }
        }
        if successful_actions == 0 {
            return Err(MemoryTrajectoryError::InvalidInput(
                "a procedure requires at least one successful retained action".to_string(),
            ));
        }
        Ok(())
    }
}

/// One untrusted procedure proposal. Its type deliberately has no assurance,
/// authority, credential, grant, activation, or lifecycle fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryProcedureCandidate {
    pub candidate_id: String,
    pub input_event_ids: Vec<String>,
    pub input_evidence_ids: Vec<String>,
    pub scope: MemoryScope,
    pub predicate: String,
    pub content: MemoryContent,
    pub epistemic_formation: EpistemicFormation,
    pub reason_code: String,
    pub trajectory_artifact_id: String,
    pub deterministic_parameters_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryTrajectoryPlan {
    pub contract_version: u32,
    pub trajectory_artifact_id: String,
    pub policy_manifest_id: String,
    pub candidate: TrajectoryProcedureCandidate,
    pub plan_sha256: String,
}

impl MemoryTrajectoryPlan {
    pub fn validate_against(&self, input: &MemoryTrajectoryInput) -> MemoryTrajectoryResult<()> {
        input.validate()?;
        if self.contract_version != MEMORY_TRAJECTORY_CONTRACT_VERSION
            || self.trajectory_artifact_id != input.trajectory_artifact_id
            || self.policy_manifest_id != input.policy_manifest_id
        {
            return Err(MemoryTrajectoryError::InvalidPlan(
                "plan contract, artifact, or policy identity differs from input".to_string(),
            ));
        }

        let expected_event_ids = input
            .events
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        let mut expected_evidence_ids = input
            .events
            .iter()
            .map(|event| event.evidence_id.clone())
            .collect::<Vec<_>>();
        expected_evidence_ids.sort();
        let expected_steps = input
            .events
            .iter()
            .filter(|event| event.outcome == TrajectoryEventOutcome::Succeeded)
            .map(|event| event.action.clone())
            .collect::<Vec<_>>();
        let expected_recovery = input
            .events
            .iter()
            .filter_map(|event| event.failure_recovery.clone())
            .collect::<Vec<_>>();

        if self.candidate.input_event_ids != expected_event_ids
            || self.candidate.input_evidence_ids != expected_evidence_ids
            || self.candidate.scope != input.scope
            || self.candidate.predicate != input.predicate
            || self.candidate.epistemic_formation != EpistemicFormation::DeterministicDerivation
            || self.candidate.reason_code != "retained_successful_trajectory"
            || self.candidate.trajectory_artifact_id != input.trajectory_artifact_id
        {
            return Err(MemoryTrajectoryError::InvalidPlan(
                "candidate changes its input identity, scope, or provenance boundary".to_string(),
            ));
        }
        validate_sha256(
            "deterministic_parameters_sha256",
            &self.candidate.deterministic_parameters_sha256,
        )
        .map_err(|error| MemoryTrajectoryError::InvalidPlan(error.to_string()))?;
        if self.candidate.deterministic_parameters_sha256 != deterministic_parameters_sha256(input)?
        {
            return Err(MemoryTrajectoryError::InvalidPlan(
                "candidate parameter digest differs from the exact input".to_string(),
            ));
        }
        match &self.candidate.content {
            MemoryContent::Procedure {
                title,
                ordered_steps,
                preconditions,
                failure_recovery,
            } if title == &input.title
                && ordered_steps == &expected_steps
                && preconditions.is_empty()
                && failure_recovery == &expected_recovery => {}
            _ => {
                return Err(MemoryTrajectoryError::InvalidPlan(
                    "candidate must be the exact evidence-bound procedure".to_string(),
                ));
            }
        }
        self.candidate
            .content
            .validate()
            .map_err(|error| MemoryTrajectoryError::InvalidPlan(error.to_string()))?;
        if self.candidate.candidate_id != trajectory_candidate_id(&self.candidate)? {
            return Err(MemoryTrajectoryError::InvalidPlan(
                "candidate ID differs from its canonical fingerprint".to_string(),
            ));
        }
        if self.plan_sha256 != trajectory_plan_sha256(self)? {
            return Err(MemoryTrajectoryError::InvalidPlan(
                "plan digest differs from its canonical fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub trait MemoryTrajectoryCompiler: Send + Sync {
    fn artifact_id(&self) -> &str;
    fn compile(
        &self,
        input: &MemoryTrajectoryInput,
    ) -> MemoryTrajectoryResult<MemoryTrajectoryPlan>;
}

#[derive(Debug, Default)]
pub struct ReferenceTrajectoryCompiler;

impl MemoryTrajectoryCompiler for ReferenceTrajectoryCompiler {
    fn artifact_id(&self) -> &str {
        REFERENCE_TRAJECTORY_COMPILER_ARTIFACT_ID
    }

    fn compile(
        &self,
        input: &MemoryTrajectoryInput,
    ) -> MemoryTrajectoryResult<MemoryTrajectoryPlan> {
        input.validate()?;
        if input.trajectory_artifact_id != self.artifact_id() {
            return Err(MemoryTrajectoryError::InvalidInput(
                "requested trajectory artifact is not available".to_string(),
            ));
        }
        let input_event_ids = input
            .events
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        let mut input_evidence_ids = input
            .events
            .iter()
            .map(|event| event.evidence_id.clone())
            .collect::<Vec<_>>();
        input_evidence_ids.sort();
        let ordered_steps = input
            .events
            .iter()
            .filter(|event| event.outcome == TrajectoryEventOutcome::Succeeded)
            .map(|event| event.action.clone())
            .collect::<Vec<_>>();
        let failure_recovery = input
            .events
            .iter()
            .filter_map(|event| event.failure_recovery.clone())
            .collect::<Vec<_>>();
        let mut candidate = TrajectoryProcedureCandidate {
            candidate_id: String::new(),
            input_event_ids,
            input_evidence_ids,
            scope: input.scope.clone(),
            predicate: input.predicate.clone(),
            content: MemoryContent::Procedure {
                title: input.title.clone(),
                ordered_steps,
                preconditions: Vec::new(),
                failure_recovery,
            },
            epistemic_formation: EpistemicFormation::DeterministicDerivation,
            reason_code: "retained_successful_trajectory".to_string(),
            trajectory_artifact_id: input.trajectory_artifact_id.clone(),
            deterministic_parameters_sha256: deterministic_parameters_sha256(input)?,
        };
        candidate.candidate_id = trajectory_candidate_id(&candidate)?;
        let mut plan = MemoryTrajectoryPlan {
            contract_version: MEMORY_TRAJECTORY_CONTRACT_VERSION,
            trajectory_artifact_id: input.trajectory_artifact_id.clone(),
            policy_manifest_id: input.policy_manifest_id.clone(),
            candidate,
            plan_sha256: String::new(),
        };
        plan.plan_sha256 = trajectory_plan_sha256(&plan)?;
        plan.validate_against(input)?;
        Ok(plan)
    }
}

pub fn verify_trajectory_conformance(
    compiler: &dyn MemoryTrajectoryCompiler,
    input: &MemoryTrajectoryInput,
) -> MemoryTrajectoryResult<MemoryTrajectoryPlan> {
    if compiler.artifact_id() != input.trajectory_artifact_id {
        return Err(MemoryTrajectoryError::InvalidInput(
            "compiler artifact identity differs from the input contract".to_string(),
        ));
    }
    let first = compiler.compile(input)?;
    let second = compiler.compile(input)?;
    first.validate_against(input)?;
    second.validate_against(input)?;
    if serde_json::to_vec(&first)? != serde_json::to_vec(&second)? {
        return Err(MemoryTrajectoryError::InvalidPlan(
            "trajectory compiler produced nondeterministic output".to_string(),
        ));
    }
    Ok(first)
}

fn deterministic_parameters_sha256(
    input: &MemoryTrajectoryInput,
) -> MemoryTrajectoryResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        input.contract_version,
        &input.trajectory_artifact_id,
        &input.policy_manifest_id,
        &input.scope,
        &input.predicate,
        &input.title,
        &input.events,
    ))?))
}

fn trajectory_candidate_id(
    candidate: &TrajectoryProcedureCandidate,
) -> MemoryTrajectoryResult<String> {
    Ok(format!(
        "trj_c1_{}",
        sha256_hex(&serde_json::to_vec(&(
            &candidate.input_event_ids,
            &candidate.input_evidence_ids,
            &candidate.scope,
            &candidate.predicate,
            &candidate.content,
            candidate.epistemic_formation,
            &candidate.reason_code,
            &candidate.trajectory_artifact_id,
            &candidate.deterministic_parameters_sha256,
        ))?)
    ))
}

fn trajectory_plan_sha256(plan: &MemoryTrajectoryPlan) -> MemoryTrajectoryResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        plan.contract_version,
        &plan.trajectory_artifact_id,
        &plan.policy_manifest_id,
        &plan.candidate,
    ))?))
}

fn validate_id(field: &str, value: &str) -> MemoryTrajectoryResult<()> {
    validate_text(field, value, MAX_MEMORY_ID_BYTES)?;
    if value.chars().any(char::is_control) {
        return Err(MemoryTrajectoryError::InvalidInput(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str, maximum: usize) -> MemoryTrajectoryResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > maximum
        || value.contains('\0')
    {
        return Err(MemoryTrajectoryError::InvalidInput(format!(
            "{field} must be non-empty, trimmed, NUL-free, and at most {maximum} bytes"
        )));
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> MemoryTrajectoryResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MemoryTrajectoryError::InvalidInput(format!(
            "{field} must be a lowercase hexadecimal SHA-256 digest"
        )));
    }
    Ok(())
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
            data_subject_id: None,
            owner_agent_id: Some("agent:operator".to_string()),
            session_id: Some("session-1".to_string()),
            task_id: Some("incident-7".to_string()),
            sensitivity: Sensitivity::Internal,
            allowed_purposes: vec!["debugging".to_string()],
        }
    }

    fn input() -> MemoryTrajectoryInput {
        MemoryTrajectoryInput {
            contract_version: MEMORY_TRAJECTORY_CONTRACT_VERSION,
            trajectory_artifact_id: REFERENCE_TRAJECTORY_COMPILER_ARTIFACT_ID.to_string(),
            policy_manifest_id: "policy:memory-v1".to_string(),
            scope: scope(),
            predicate: "uses recovery procedure".to_string(),
            title: "Recover ingestion worker".to_string(),
            events: vec![
                TrajectoryEvent {
                    event_id: "event-1".to_string(),
                    evidence_id: "evidence-1".to_string(),
                    scope: scope(),
                    ordinal: 1,
                    action: "Check the projection checkpoint.".to_string(),
                    outcome: TrajectoryEventOutcome::Succeeded,
                    failure_recovery: None,
                },
                TrajectoryEvent {
                    event_id: "event-2".to_string(),
                    evidence_id: "evidence-2".to_string(),
                    scope: scope(),
                    ordinal: 2,
                    action: "Restart without draining the queue.".to_string(),
                    outcome: TrajectoryEventOutcome::Failed,
                    failure_recovery: Some(
                        "Drain the queue before restarting the worker.".to_string(),
                    ),
                },
                TrajectoryEvent {
                    event_id: "event-3".to_string(),
                    evidence_id: "evidence-3".to_string(),
                    scope: scope(),
                    ordinal: 3,
                    action: "Verify the visible sequence catches up.".to_string(),
                    outcome: TrajectoryEventOutcome::Succeeded,
                    failure_recovery: None,
                },
            ],
        }
    }

    #[test]
    fn reference_trajectory_is_deterministic_and_cannot_launder_authority() {
        let plan = verify_trajectory_conformance(&ReferenceTrajectoryCompiler, &input()).unwrap();
        let encoded = serde_json::to_string(&plan).unwrap();
        for forbidden in [
            "source_assurance",
            "decision_authority",
            "credential",
            "capability_grant",
            "authorization",
        ] {
            assert!(!encoded.contains(forbidden));
        }
        assert_eq!(plan.candidate.scope, scope());
        match plan.candidate.content {
            MemoryContent::Procedure {
                ordered_steps,
                failure_recovery,
                ..
            } => {
                assert_eq!(
                    ordered_steps,
                    vec![
                        "Check the projection checkpoint.",
                        "Verify the visible sequence catches up."
                    ]
                );
                assert_eq!(
                    failure_recovery,
                    vec!["Drain the queue before restarting the worker."]
                );
            }
            _ => panic!("reference trajectory must produce a procedure"),
        }
    }

    #[test]
    fn scope_changes_and_missing_recovery_fail_closed() {
        let mut changed_scope = input();
        changed_scope.events[1].scope.namespace = "other".to_string();
        assert!(changed_scope.validate().is_err());

        let mut missing_recovery = input();
        missing_recovery.events[1].failure_recovery = None;
        assert!(missing_recovery.validate().is_err());
    }

    #[test]
    fn forged_output_is_rejected() {
        let input = input();
        let mut plan = ReferenceTrajectoryCompiler.compile(&input).unwrap();
        plan.candidate.scope.namespace = "other".to_string();
        assert!(plan.validate_against(&input).is_err());
    }
}
