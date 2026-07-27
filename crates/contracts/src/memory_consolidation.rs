//! Deterministic, conservative consolidation planning for authoritative Memory.
//!
//! A consolidation plan is advisory. It can identify exact duplicates and
//! explicit conflicts, but it cannot carry replacement content, scope,
//! assurance, authority, credentials, or a commit instruction. The service
//! must separately authorize any reinforcement or lifecycle mutation.

use crate::{
    canonical_content_sha256, MemoryContent, MemoryKind, MemoryScope, MAX_MEMORY_EVIDENCE,
    MAX_MEMORY_ID_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use thiserror::Error;

pub const MEMORY_CONSOLIDATION_CONTRACT_VERSION: u32 = 1;
pub const REFERENCE_CONSOLIDATION_ARTIFACT_ID: &str = "consolidation:reference-exact-v1";

#[derive(Debug, Error)]
pub enum MemoryConsolidationError {
    #[error("invalid consolidation input: {0}")]
    InvalidInput(String),
    #[error("invalid consolidation plan: {0}")]
    InvalidPlan(String),
    #[error("consolidation serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type MemoryConsolidationResult<T> = Result<T, MemoryConsolidationError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsolidationVersion {
    pub version_id: String,
    pub scope: MemoryScope,
    pub predicate: String,
    pub kind: MemoryKind,
    pub content: MemoryContent,
    pub evidence_ids: Vec<String>,
    pub committed_sequence: u64,
}

impl ConsolidationVersion {
    fn validate(&self) -> MemoryConsolidationResult<()> {
        validate_id("version_id", &self.version_id)?;
        self.scope
            .validate()
            .map_err(|error| MemoryConsolidationError::InvalidInput(error.to_string()))?;
        validate_id("predicate", &self.predicate)?;
        self.content
            .validate()
            .map_err(|error| MemoryConsolidationError::InvalidInput(error.to_string()))?;
        if self.kind != self.content.kind() {
            return Err(MemoryConsolidationError::InvalidInput(
                "version kind differs from typed content".to_string(),
            ));
        }
        if self.evidence_ids.is_empty() || self.evidence_ids.len() > MAX_MEMORY_EVIDENCE {
            return Err(MemoryConsolidationError::InvalidInput(format!(
                "evidence_ids must contain 1..={MAX_MEMORY_EVIDENCE} entries"
            )));
        }
        let mut evidence_ids = HashSet::new();
        for evidence_id in &self.evidence_ids {
            validate_id("evidence_id", evidence_id)?;
            if !evidence_ids.insert(evidence_id) {
                return Err(MemoryConsolidationError::InvalidInput(
                    "evidence IDs must be unique per version".to_string(),
                ));
            }
        }
        if self.committed_sequence == 0 {
            return Err(MemoryConsolidationError::InvalidInput(
                "committed_sequence must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryConsolidationInput {
    pub contract_version: u32,
    pub consolidation_artifact_id: String,
    pub policy_manifest_id: String,
    pub versions: Vec<ConsolidationVersion>,
}

impl MemoryConsolidationInput {
    pub fn validate(&self) -> MemoryConsolidationResult<()> {
        if self.contract_version != MEMORY_CONSOLIDATION_CONTRACT_VERSION {
            return Err(MemoryConsolidationError::InvalidInput(format!(
                "unsupported consolidation contract version {}",
                self.contract_version
            )));
        }
        validate_id("consolidation_artifact_id", &self.consolidation_artifact_id)?;
        validate_id("policy_manifest_id", &self.policy_manifest_id)?;
        if self.versions.len() < 2 || self.versions.len() > MAX_MEMORY_EVIDENCE {
            return Err(MemoryConsolidationError::InvalidInput(format!(
                "versions must contain 2..={MAX_MEMORY_EVIDENCE} entries"
            )));
        }
        let mut version_ids = HashSet::new();
        for version in &self.versions {
            version.validate()?;
            if !version_ids.insert(&version.version_id) {
                return Err(MemoryConsolidationError::InvalidInput(
                    "version IDs must be unique".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// A plan only names existing immutable records. It cannot propose replacement
/// content or carry classification fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsolidationAction {
    ExactDuplicate {
        action_id: String,
        retained_version_id: String,
        duplicate_version_ids: Vec<String>,
        evidence_ids: Vec<String>,
        reason_code: String,
    },
    ExplicitConflict {
        action_id: String,
        left_version_id: String,
        right_version_id: String,
        reason_code: String,
    },
}

impl ConsolidationAction {
    fn action_id(&self) -> &str {
        match self {
            Self::ExactDuplicate { action_id, .. } | Self::ExplicitConflict { action_id, .. } => {
                action_id
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryConsolidationPlan {
    pub contract_version: u32,
    pub consolidation_artifact_id: String,
    pub policy_manifest_id: String,
    pub input_version_ids: Vec<String>,
    pub actions: Vec<ConsolidationAction>,
    pub plan_sha256: String,
}

impl MemoryConsolidationPlan {
    pub fn validate_against(
        &self,
        input: &MemoryConsolidationInput,
    ) -> MemoryConsolidationResult<()> {
        input.validate()?;
        if self.contract_version != MEMORY_CONSOLIDATION_CONTRACT_VERSION
            || self.consolidation_artifact_id != input.consolidation_artifact_id
            || self.policy_manifest_id != input.policy_manifest_id
        {
            return Err(MemoryConsolidationError::InvalidPlan(
                "plan contract, artifact, or policy identity differs from input".to_string(),
            ));
        }
        let mut expected_ids = input
            .versions
            .iter()
            .map(|version| version.version_id.clone())
            .collect::<Vec<_>>();
        expected_ids.sort();
        if self.input_version_ids != expected_ids {
            return Err(MemoryConsolidationError::InvalidPlan(
                "plan does not bind the exact sorted input version set".to_string(),
            ));
        }
        let input_ids: HashSet<&str> = expected_ids.iter().map(String::as_str).collect();
        let mut action_ids = HashSet::new();
        let mut duplicate_ids = HashSet::new();
        for action in &self.actions {
            validate_id("action_id", action.action_id())
                .map_err(|error| MemoryConsolidationError::InvalidPlan(error.to_string()))?;
            if !action_ids.insert(action.action_id()) {
                return Err(MemoryConsolidationError::InvalidPlan(
                    "action IDs must be unique".to_string(),
                ));
            }
            match action {
                ConsolidationAction::ExactDuplicate {
                    retained_version_id,
                    duplicate_version_ids,
                    evidence_ids,
                    reason_code,
                    ..
                } => {
                    if !input_ids.contains(retained_version_id.as_str())
                        || duplicate_version_ids.is_empty()
                        || duplicate_version_ids
                            .iter()
                            .any(|id| !input_ids.contains(id.as_str()) || id == retained_version_id)
                    {
                        return Err(MemoryConsolidationError::InvalidPlan(
                            "exact-duplicate action references an invalid input version"
                                .to_string(),
                        ));
                    }
                    if !is_strictly_sorted_unique(duplicate_version_ids)
                        || !is_strictly_sorted_unique(evidence_ids)
                    {
                        return Err(MemoryConsolidationError::InvalidPlan(
                            "duplicate and evidence IDs must be strictly sorted".to_string(),
                        ));
                    }
                    if duplicate_version_ids
                        .iter()
                        .any(|id| !duplicate_ids.insert(id.as_str()))
                    {
                        return Err(MemoryConsolidationError::InvalidPlan(
                            "one version cannot be discarded by multiple actions".to_string(),
                        ));
                    }
                    validate_id("reason_code", reason_code).map_err(|error| {
                        MemoryConsolidationError::InvalidPlan(error.to_string())
                    })?;
                    let retained = input
                        .versions
                        .iter()
                        .find(|version| version.version_id == *retained_version_id)
                        .expect("validated input ID");
                    let retained_digest =
                        canonical_content_sha256(&retained.content).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })?;
                    let mut exact_evidence = BTreeSet::new();
                    exact_evidence.extend(retained.evidence_ids.iter().cloned());
                    for duplicate_id in duplicate_version_ids {
                        let duplicate = input
                            .versions
                            .iter()
                            .find(|version| version.version_id == *duplicate_id)
                            .expect("validated input ID");
                        if grouping_key(duplicate).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })? != grouping_key(retained).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })? || canonical_content_sha256(&duplicate.content).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })? != retained_digest
                        {
                            return Err(MemoryConsolidationError::InvalidPlan(
                                "exact-duplicate action silently merges a conflict".to_string(),
                            ));
                        }
                        exact_evidence.extend(duplicate.evidence_ids.iter().cloned());
                    }
                    if evidence_ids != &exact_evidence.into_iter().collect::<Vec<_>>() {
                        return Err(MemoryConsolidationError::InvalidPlan(
                            "exact-duplicate evidence set is incomplete".to_string(),
                        ));
                    }
                }
                ConsolidationAction::ExplicitConflict {
                    left_version_id,
                    right_version_id,
                    reason_code,
                    ..
                } => {
                    if left_version_id >= right_version_id
                        || !input_ids.contains(left_version_id.as_str())
                        || !input_ids.contains(right_version_id.as_str())
                    {
                        return Err(MemoryConsolidationError::InvalidPlan(
                            "conflict endpoints must be distinct sorted input IDs".to_string(),
                        ));
                    }
                    validate_id("reason_code", reason_code).map_err(|error| {
                        MemoryConsolidationError::InvalidPlan(error.to_string())
                    })?;
                    let left = input
                        .versions
                        .iter()
                        .find(|version| version.version_id == *left_version_id)
                        .expect("validated input ID");
                    let right = input
                        .versions
                        .iter()
                        .find(|version| version.version_id == *right_version_id)
                        .expect("validated input ID");
                    if grouping_key(left)
                        .map_err(|error| MemoryConsolidationError::InvalidPlan(error.to_string()))?
                        != grouping_key(right).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })?
                        || canonical_content_sha256(&left.content).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })? == canonical_content_sha256(&right.content).map_err(|error| {
                            MemoryConsolidationError::InvalidPlan(error.to_string())
                        })?
                    {
                        return Err(MemoryConsolidationError::InvalidPlan(
                            "conflict action does not name differing content in one identity group"
                                .to_string(),
                        ));
                    }
                }
            }
        }
        if self
            .actions
            .windows(2)
            .any(|window| window[0].action_id().as_bytes() >= window[1].action_id().as_bytes())
        {
            return Err(MemoryConsolidationError::InvalidPlan(
                "actions must be strictly sorted by action ID".to_string(),
            ));
        }
        if self.plan_sha256 != consolidation_plan_sha256(self)? {
            return Err(MemoryConsolidationError::InvalidPlan(
                "plan digest differs from canonical fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub trait MemoryConsolidationExecutor: Send + Sync {
    fn artifact_id(&self) -> &str;
    fn plan(
        &self,
        input: &MemoryConsolidationInput,
    ) -> MemoryConsolidationResult<MemoryConsolidationPlan>;
}

#[derive(Debug, Default)]
pub struct ReferenceConsolidationExecutor;

impl MemoryConsolidationExecutor for ReferenceConsolidationExecutor {
    fn artifact_id(&self) -> &str {
        REFERENCE_CONSOLIDATION_ARTIFACT_ID
    }

    fn plan(
        &self,
        input: &MemoryConsolidationInput,
    ) -> MemoryConsolidationResult<MemoryConsolidationPlan> {
        input.validate()?;
        if input.consolidation_artifact_id != self.artifact_id() {
            return Err(MemoryConsolidationError::InvalidInput(
                "requested consolidation artifact is not available".to_string(),
            ));
        }

        let mut groups: BTreeMap<String, Vec<&ConsolidationVersion>> = BTreeMap::new();
        for version in &input.versions {
            groups
                .entry(grouping_key(version)?)
                .or_default()
                .push(version);
        }
        let mut actions = Vec::new();
        for versions in groups.values_mut() {
            versions.sort_by(|left, right| {
                left.committed_sequence
                    .cmp(&right.committed_sequence)
                    .then_with(|| left.version_id.cmp(&right.version_id))
            });
            let mut by_content: BTreeMap<String, Vec<&ConsolidationVersion>> = BTreeMap::new();
            for version in versions.iter().copied() {
                by_content
                    .entry(canonical_content_sha256(&version.content).map_err(|error| {
                        MemoryConsolidationError::InvalidInput(error.to_string())
                    })?)
                    .or_default()
                    .push(version);
            }
            for exact_versions in by_content.values() {
                if exact_versions.len() < 2 {
                    continue;
                }
                let retained = exact_versions[0];
                let mut duplicate_version_ids = exact_versions[1..]
                    .iter()
                    .map(|version| version.version_id.clone())
                    .collect::<Vec<_>>();
                duplicate_version_ids.sort();
                let evidence_ids = exact_versions
                    .iter()
                    .flat_map(|version| version.evidence_ids.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let material = format!(
                    "duplicate\0{}\0{}\0{}",
                    retained.version_id,
                    duplicate_version_ids.join("\0"),
                    evidence_ids.join("\0")
                );
                actions.push(ConsolidationAction::ExactDuplicate {
                    action_id: format!("con_a1_{}", sha256_hex(material.as_bytes())),
                    retained_version_id: retained.version_id.clone(),
                    duplicate_version_ids,
                    evidence_ids,
                    reason_code: "EXACT_CONTENT_DUPLICATE_REINFORCEMENT_ONLY".to_string(),
                });
            }
            let content_representatives = by_content
                .values()
                .map(|exact| exact[0])
                .collect::<Vec<_>>();
            for left_index in 0..content_representatives.len() {
                for right_index in (left_index + 1)..content_representatives.len() {
                    let mut endpoints = [
                        content_representatives[left_index].version_id.clone(),
                        content_representatives[right_index].version_id.clone(),
                    ];
                    endpoints.sort();
                    let material = format!("conflict\0{}\0{}", endpoints[0], endpoints[1]);
                    actions.push(ConsolidationAction::ExplicitConflict {
                        action_id: format!("con_a1_{}", sha256_hex(material.as_bytes())),
                        left_version_id: endpoints[0].clone(),
                        right_version_id: endpoints[1].clone(),
                        reason_code: "CONTENT_CONFLICT_REQUIRES_AUTHORITY_POLICY".to_string(),
                    });
                }
            }
        }
        actions.sort_by(|left, right| left.action_id().cmp(right.action_id()));
        let mut input_version_ids = input
            .versions
            .iter()
            .map(|version| version.version_id.clone())
            .collect::<Vec<_>>();
        input_version_ids.sort();
        let mut plan = MemoryConsolidationPlan {
            contract_version: MEMORY_CONSOLIDATION_CONTRACT_VERSION,
            consolidation_artifact_id: input.consolidation_artifact_id.clone(),
            policy_manifest_id: input.policy_manifest_id.clone(),
            input_version_ids,
            actions,
            plan_sha256: String::new(),
        };
        plan.plan_sha256 = consolidation_plan_sha256(&plan)?;
        plan.validate_against(input)?;
        Ok(plan)
    }
}

pub fn verify_consolidation_conformance(
    executor: &dyn MemoryConsolidationExecutor,
    input: &MemoryConsolidationInput,
) -> MemoryConsolidationResult<MemoryConsolidationPlan> {
    if executor.artifact_id() != input.consolidation_artifact_id {
        return Err(MemoryConsolidationError::InvalidInput(
            "executor artifact identity differs from input".to_string(),
        ));
    }
    let first = executor.plan(input)?;
    let second = executor.plan(input)?;
    first.validate_against(input)?;
    second.validate_against(input)?;
    if serde_json::to_vec(&first)? != serde_json::to_vec(&second)? {
        return Err(MemoryConsolidationError::InvalidPlan(
            "executor produced nondeterministic output".to_string(),
        ));
    }
    Ok(first)
}

fn grouping_key(version: &ConsolidationVersion) -> MemoryConsolidationResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        &version.scope,
        &version.predicate,
        version.kind,
    ))?))
}

fn consolidation_plan_sha256(plan: &MemoryConsolidationPlan) -> MemoryConsolidationResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        plan.contract_version,
        &plan.consolidation_artifact_id,
        &plan.policy_manifest_id,
        &plan.input_version_ids,
        &plan.actions,
    ))?))
}

fn validate_id(field: &str, value: &str) -> MemoryConsolidationResult<()> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_MEMORY_ID_BYTES
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(MemoryConsolidationError::InvalidInput(format!(
            "{field} must be non-empty, trimmed, control-free, and at most {MAX_MEMORY_ID_BYTES} bytes"
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
            data_subject_id: None,
            owner_agent_id: Some("agent:compiler".to_string()),
            session_id: None,
            task_id: None,
            sensitivity: Sensitivity::Internal,
            allowed_purposes: vec!["debugging".to_string()],
        }
    }

    fn version(id: &str, sequence: u64, text: &str, evidence: &str) -> ConsolidationVersion {
        ConsolidationVersion {
            version_id: id.to_string(),
            scope: scope(),
            predicate: "uses recovery procedure".to_string(),
            kind: MemoryKind::TextFact,
            content: MemoryContent::TextFact {
                text: text.to_string(),
                language: None,
            },
            evidence_ids: vec![evidence.to_string()],
            committed_sequence: sequence,
        }
    }

    fn input() -> MemoryConsolidationInput {
        MemoryConsolidationInput {
            contract_version: MEMORY_CONSOLIDATION_CONTRACT_VERSION,
            consolidation_artifact_id: REFERENCE_CONSOLIDATION_ARTIFACT_ID.to_string(),
            policy_manifest_id: "memory-authority-policy-v1".to_string(),
            versions: vec![
                version("version-2", 2, "Drain the queue.", "evidence-2"),
                version("version-3", 3, "Restart immediately.", "evidence-3"),
                version("version-1", 1, "Drain the queue.", "evidence-1"),
            ],
        }
    }

    #[test]
    fn executor_is_deterministic_and_never_silently_merges_conflicts() {
        let executor = ReferenceConsolidationExecutor;
        let first = verify_consolidation_conformance(&executor, &input()).unwrap();
        let mut reordered = input();
        reordered.versions.reverse();
        let second = verify_consolidation_conformance(&executor, &reordered).unwrap();
        assert_eq!(first, second);
        assert!(first
            .actions
            .iter()
            .any(|action| matches!(action, ConsolidationAction::ExactDuplicate { .. })));
        assert!(first
            .actions
            .iter()
            .any(|action| matches!(action, ConsolidationAction::ExplicitConflict { .. })));
        let json = serde_json::to_string(&first).unwrap();
        for forbidden in [
            "content",
            "scope",
            "source_assurance",
            "decision_authority",
            "credential",
        ] {
            assert!(!json.contains(forbidden));
        }
    }

    #[test]
    fn validator_rejects_conflict_laundered_as_duplicate() {
        let executor = ReferenceConsolidationExecutor;
        let mut plan = executor.plan(&input()).unwrap();
        let conflict = plan
            .actions
            .iter()
            .find_map(|action| match action {
                ConsolidationAction::ExplicitConflict {
                    left_version_id,
                    right_version_id,
                    ..
                } => Some((left_version_id.clone(), right_version_id.clone())),
                _ => None,
            })
            .unwrap();
        plan.actions = vec![ConsolidationAction::ExactDuplicate {
            action_id: "forged-action".to_string(),
            retained_version_id: conflict.0,
            duplicate_version_ids: vec![conflict.1],
            evidence_ids: vec!["evidence-1".to_string(), "evidence-3".to_string()],
            reason_code: "FORGED".to_string(),
        }];
        assert!(plan.validate_against(&input()).is_err());
    }
}
