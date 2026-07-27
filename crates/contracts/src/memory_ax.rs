//! Typed integration records for AX Fabric and AX Studio.
//!
//! AX Fabric candidate batches are explicitly untrusted compiler exchange
//! envelopes. AX Studio timeline/debug records expose immutable identifiers and
//! digests, never raw memory content or credentials. Neither contract can grant
//! scope, assurance, authority, or policy permissions.

use crate::{
    MemoryCommitPlan, MemoryCompilerInput, MemoryOperation, MAX_MEMORY_EVIDENCE,
    MAX_MEMORY_ID_BYTES, MAX_MEMORY_SCOPE_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use thiserror::Error;

pub const AX_MEMORY_EXCHANGE_CONTRACT_VERSION: u32 = 1;
pub const MAX_AX_STUDIO_TIMELINE_ENTRIES: usize = 1_000;
pub const MAX_AX_STUDIO_DEBUG_ITEMS: usize = 5_000;

#[derive(Debug, Error)]
pub enum AxMemoryContractError {
    #[error("invalid AX Memory record: {0}")]
    InvalidRecord(String),
    #[error("AX Memory record serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type AxMemoryContractResult<T> = Result<T, AxMemoryContractError>;

/// Reviewable AX Fabric exchange envelope. It binds an exact compiler input to
/// its exact untrusted proposal; committing the proposal remains a separately
/// authenticated MemoryService operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxFabricMemoryCandidateBatch {
    pub contract_version: u32,
    pub batch_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub compiler_input: MemoryCompilerInput,
    pub compiler_input_sha256: String,
    pub commit_plan: MemoryCommitPlan,
    pub created_at_ms: u64,
    pub batch_sha256: String,
}

impl AxFabricMemoryCandidateBatch {
    pub fn validate(&self) -> AxMemoryContractResult<()> {
        validate_contract_version(self.contract_version)?;
        validate_id("batch_id", &self.batch_id, MAX_MEMORY_ID_BYTES)?;
        validate_id("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_id("namespace", &self.namespace, MAX_MEMORY_SCOPE_BYTES)?;
        if self.created_at_ms == 0 {
            return Err(AxMemoryContractError::InvalidRecord(
                "created_at_ms must be greater than zero".to_string(),
            ));
        }
        self.compiler_input
            .validate()
            .map_err(|error| AxMemoryContractError::InvalidRecord(error.to_string()))?;
        self.commit_plan
            .validate_against(&self.compiler_input)
            .map_err(|error| AxMemoryContractError::InvalidRecord(error.to_string()))?;

        let scopes_match = self
            .compiler_input
            .observations
            .iter()
            .map(|observation| &observation.scope)
            .chain(
                self.compiler_input
                    .existing_authorized_heads
                    .iter()
                    .map(|head| &head.scope),
            )
            .chain(
                self.commit_plan
                    .candidates
                    .iter()
                    .map(|candidate| &candidate.scope),
            )
            .all(|scope| {
                scope.workspace_id == self.workspace_id && scope.namespace == self.namespace
            });
        if !scopes_match {
            return Err(AxMemoryContractError::InvalidRecord(
                "candidate batch contains a scope outside its exact exchange boundary".to_string(),
            ));
        }
        validate_sha256("compiler_input_sha256", &self.compiler_input_sha256)?;
        if self.compiler_input_sha256 != compiler_input_sha256(&self.compiler_input)? {
            return Err(AxMemoryContractError::InvalidRecord(
                "compiler input digest differs from the embedded input".to_string(),
            ));
        }
        validate_sha256("batch_sha256", &self.batch_sha256)?;
        if self.batch_sha256 != ax_fabric_candidate_batch_sha256(self)? {
            return Err(AxMemoryContractError::InvalidRecord(
                "candidate batch digest differs from its immutable fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn ax_fabric_candidate_batch_sha256(
    batch: &AxFabricMemoryCandidateBatch,
) -> AxMemoryContractResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        batch.contract_version,
        &batch.batch_id,
        &batch.workspace_id,
        &batch.namespace,
        &batch.compiler_input,
        &batch.compiler_input_sha256,
        &batch.commit_plan,
        batch.created_at_ms,
    ))?))
}

pub fn compiler_input_sha256(input: &MemoryCompilerInput) -> AxMemoryContractResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(input)?))
}

/// Content-free immutable timeline entry for AX Studio. A timeline reader can
/// correlate the canonical mutation, policy decision, projection set, and
/// artifact set without receiving memory text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxStudioMemoryTimelineEntry {
    pub contract_version: u32,
    pub entry_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub committed_sequence: u64,
    pub committed_at_ms: u64,
    pub mutation_id: String,
    pub operation: MemoryOperation,
    pub assertion_id: String,
    pub input_version_ids: Vec<String>,
    pub output_version_ids: Vec<String>,
    pub policy_decision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_set_id: Option<String>,
    pub artifact_ids: Vec<String>,
    pub entry_sha256: String,
}

impl AxStudioMemoryTimelineEntry {
    pub fn validate(&self) -> AxMemoryContractResult<()> {
        validate_contract_version(self.contract_version)?;
        validate_id("entry_id", &self.entry_id, MAX_MEMORY_ID_BYTES)?;
        validate_id("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_id("namespace", &self.namespace, MAX_MEMORY_SCOPE_BYTES)?;
        if self.committed_sequence == 0 || self.committed_at_ms == 0 {
            return Err(AxMemoryContractError::InvalidRecord(
                "timeline sequence and timestamp must be greater than zero".to_string(),
            ));
        }
        validate_id("mutation_id", &self.mutation_id, MAX_MEMORY_ID_BYTES)?;
        validate_id("assertion_id", &self.assertion_id, MAX_MEMORY_ID_BYTES)?;
        validate_unique_ids(
            "input_version_ids",
            &self.input_version_ids,
            MAX_MEMORY_EVIDENCE,
        )?;
        validate_unique_ids(
            "output_version_ids",
            &self.output_version_ids,
            MAX_MEMORY_EVIDENCE,
        )?;
        validate_id(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_optional_id("snapshot_id", self.snapshot_id.as_deref())?;
        validate_optional_id("projection_set_id", self.projection_set_id.as_deref())?;
        validate_sorted_unique_ids("artifact_ids", &self.artifact_ids, MAX_MEMORY_EVIDENCE)?;
        validate_sha256("entry_sha256", &self.entry_sha256)?;
        if self.entry_sha256 != ax_studio_timeline_entry_sha256(self)? {
            return Err(AxMemoryContractError::InvalidRecord(
                "timeline entry digest differs from its immutable fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn ax_studio_timeline_entry_sha256(
    entry: &AxStudioMemoryTimelineEntry,
) -> AxMemoryContractResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        entry.contract_version,
        &entry.entry_id,
        &entry.workspace_id,
        &entry.namespace,
        entry.committed_sequence,
        entry.committed_at_ms,
        &entry.mutation_id,
        entry.operation,
        &entry.assertion_id,
        &entry.input_version_ids,
        &entry.output_version_ids,
        &entry.policy_decision_id,
        &entry.snapshot_id,
        &entry.projection_set_id,
        &entry.artifact_ids,
    ))?))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxStudioMemoryTimelinePage {
    pub contract_version: u32,
    pub workspace_id: String,
    pub namespace: String,
    pub start_after_sequence: u64,
    pub entries: Vec<AxStudioMemoryTimelineEntry>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_sequence: Option<u64>,
    pub page_sha256: String,
}

impl AxStudioMemoryTimelinePage {
    pub fn validate(&self) -> AxMemoryContractResult<()> {
        validate_contract_version(self.contract_version)?;
        validate_id("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_id("namespace", &self.namespace, MAX_MEMORY_SCOPE_BYTES)?;
        if self.entries.len() > MAX_AX_STUDIO_TIMELINE_ENTRIES {
            return Err(AxMemoryContractError::InvalidRecord(format!(
                "timeline page exceeds {MAX_AX_STUDIO_TIMELINE_ENTRIES} entries"
            )));
        }
        let mut prior_sequence = self.start_after_sequence;
        let mut entry_ids = HashSet::new();
        for entry in &self.entries {
            entry.validate()?;
            if entry.workspace_id != self.workspace_id
                || entry.namespace != self.namespace
                || entry.committed_sequence <= prior_sequence
                || !entry_ids.insert(&entry.entry_id)
            {
                return Err(AxMemoryContractError::InvalidRecord(
                    "timeline entries must be unique, scope-bound, and strictly ordered"
                        .to_string(),
                ));
            }
            prior_sequence = entry.committed_sequence;
        }
        match (self.has_more, self.next_after_sequence) {
            (true, Some(next)) if !self.entries.is_empty() && next == prior_sequence => {}
            (false, None) => {}
            _ => {
                return Err(AxMemoryContractError::InvalidRecord(
                    "timeline continuation token does not match page state".to_string(),
                ));
            }
        }
        validate_sha256("page_sha256", &self.page_sha256)?;
        if self.page_sha256 != ax_studio_timeline_page_sha256(self)? {
            return Err(AxMemoryContractError::InvalidRecord(
                "timeline page digest differs from its immutable fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn ax_studio_timeline_page_sha256(
    page: &AxStudioMemoryTimelinePage,
) -> AxMemoryContractResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        page.contract_version,
        &page.workspace_id,
        &page.namespace,
        page.start_after_sequence,
        &page.entries,
        page.has_more,
        page.next_after_sequence,
    ))?))
}

/// Content-free debugger binding for one retained recall snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxStudioMemoryDebugRecord {
    pub contract_version: u32,
    pub debug_record_id: String,
    pub workspace_id: String,
    pub namespace: String,
    pub snapshot_id: String,
    pub canonical_sequence: u64,
    pub visible_sequence: u64,
    pub projection_set_id: String,
    pub policy_decision_id: String,
    pub artifact_ids: Vec<String>,
    pub item_version_ids: Vec<String>,
    pub rendered_context_sha256: String,
    pub recorded_at_ms: u64,
    pub record_sha256: String,
}

impl AxStudioMemoryDebugRecord {
    pub fn validate(&self) -> AxMemoryContractResult<()> {
        validate_contract_version(self.contract_version)?;
        validate_id(
            "debug_record_id",
            &self.debug_record_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_id("workspace_id", &self.workspace_id, MAX_MEMORY_SCOPE_BYTES)?;
        validate_id("namespace", &self.namespace, MAX_MEMORY_SCOPE_BYTES)?;
        validate_id("snapshot_id", &self.snapshot_id, MAX_MEMORY_ID_BYTES)?;
        if self.canonical_sequence == 0
            || self.visible_sequence == 0
            || self.visible_sequence > self.canonical_sequence
            || self.recorded_at_ms == 0
        {
            return Err(AxMemoryContractError::InvalidRecord(
                "debugger sequences or timestamp are invalid".to_string(),
            ));
        }
        validate_id(
            "projection_set_id",
            &self.projection_set_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_id(
            "policy_decision_id",
            &self.policy_decision_id,
            MAX_MEMORY_ID_BYTES,
        )?;
        validate_sorted_unique_ids("artifact_ids", &self.artifact_ids, MAX_MEMORY_EVIDENCE)?;
        validate_unique_ids(
            "item_version_ids",
            &self.item_version_ids,
            MAX_AX_STUDIO_DEBUG_ITEMS,
        )?;
        validate_sha256("rendered_context_sha256", &self.rendered_context_sha256)?;
        validate_sha256("record_sha256", &self.record_sha256)?;
        if self.record_sha256 != ax_studio_debug_record_sha256(self)? {
            return Err(AxMemoryContractError::InvalidRecord(
                "debug record digest differs from its immutable fields".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn ax_studio_debug_record_sha256(
    record: &AxStudioMemoryDebugRecord,
) -> AxMemoryContractResult<String> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        record.contract_version,
        &record.debug_record_id,
        &record.workspace_id,
        &record.namespace,
        &record.snapshot_id,
        record.canonical_sequence,
        record.visible_sequence,
        &record.projection_set_id,
        &record.policy_decision_id,
        &record.artifact_ids,
        &record.item_version_ids,
        &record.rendered_context_sha256,
        record.recorded_at_ms,
    ))?))
}

fn validate_contract_version(version: u32) -> AxMemoryContractResult<()> {
    if version != AX_MEMORY_EXCHANGE_CONTRACT_VERSION {
        return Err(AxMemoryContractError::InvalidRecord(format!(
            "unsupported AX Memory contract version {version}"
        )));
    }
    Ok(())
}

fn validate_optional_id(field: &str, value: Option<&str>) -> AxMemoryContractResult<()> {
    if let Some(value) = value {
        validate_id(field, value, MAX_MEMORY_ID_BYTES)?;
    }
    Ok(())
}

fn validate_unique_ids(
    field: &str,
    values: &[String],
    maximum: usize,
) -> AxMemoryContractResult<()> {
    if values.len() > maximum {
        return Err(AxMemoryContractError::InvalidRecord(format!(
            "{field} exceeds {maximum} entries"
        )));
    }
    let mut unique = HashSet::new();
    for value in values {
        validate_id(field, value, MAX_MEMORY_ID_BYTES)?;
        if !unique.insert(value) {
            return Err(AxMemoryContractError::InvalidRecord(format!(
                "{field} must contain unique IDs"
            )));
        }
    }
    Ok(())
}

fn validate_sorted_unique_ids(
    field: &str,
    values: &[String],
    maximum: usize,
) -> AxMemoryContractResult<()> {
    validate_unique_ids(field, values, maximum)?;
    if values
        .windows(2)
        .any(|window| window[0].as_bytes() >= window[1].as_bytes())
    {
        return Err(AxMemoryContractError::InvalidRecord(format!(
            "{field} must be strictly sorted"
        )));
    }
    Ok(())
}

fn validate_id(field: &str, value: &str, maximum: usize) -> AxMemoryContractResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > maximum
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(AxMemoryContractError::InvalidRecord(format!(
            "{field} must be non-empty, trimmed, control-free, and at most {maximum} bytes"
        )));
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> AxMemoryContractResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AxMemoryContractError::InvalidRecord(format!(
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
    use crate::{
        CompilerHead, CompilerObservation, MemoryCompiler, ReferenceTextCompiler, Sensitivity,
        MEMORY_COMPILER_CONTRACT_VERSION, REFERENCE_TEXT_COMPILER_ARTIFACT_ID,
    };
    use serde_json::Value;

    fn scope() -> crate::MemoryScope {
        crate::MemoryScope {
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            entity_key: "service:memory".to_string(),
            data_subject_id: None,
            owner_agent_id: Some("agent:fabric".to_string()),
            session_id: None,
            task_id: None,
            sensitivity: Sensitivity::Internal,
            allowed_purposes: vec!["debugging".to_string()],
        }
    }

    fn candidate_batch() -> AxFabricMemoryCandidateBatch {
        let input = MemoryCompilerInput {
            contract_version: MEMORY_COMPILER_CONTRACT_VERSION,
            observations: vec![CompilerObservation {
                observation_id: "observation-1".to_string(),
                evidence_id: "evidence-1".to_string(),
                scope: scope(),
                retained_text: "Drain the queue before restarting.".to_string(),
                predicate_hint: "uses recovery procedure".to_string(),
                valid_from_unix_nanos: None,
                valid_to_unix_nanos: None,
            }],
            existing_authorized_heads: Vec::<CompilerHead>::new(),
            policy_manifest_id: "policy:memory-v1".to_string(),
            compiler_artifact_id: REFERENCE_TEXT_COMPILER_ARTIFACT_ID.to_string(),
        };
        let commit_plan = ReferenceTextCompiler.compile(&input).unwrap();
        let compiler_input_sha256 = compiler_input_sha256(&input).unwrap();
        let mut batch = AxFabricMemoryCandidateBatch {
            contract_version: AX_MEMORY_EXCHANGE_CONTRACT_VERSION,
            batch_id: "batch-1".to_string(),
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            compiler_input: input,
            compiler_input_sha256,
            commit_plan,
            created_at_ms: 1_700_000_000_000,
            batch_sha256: String::new(),
        };
        batch.batch_sha256 = ax_fabric_candidate_batch_sha256(&batch).unwrap();
        batch
    }

    fn collect_keys(value: &Value, keys: &mut HashSet<String>) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    keys.insert(key.clone());
                    collect_keys(value, keys);
                }
            }
            Value::Array(values) => {
                for value in values {
                    collect_keys(value, keys);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn fabric_batch_is_exact_scope_bound_and_contains_no_authority_fields() {
        let batch = candidate_batch();
        batch.validate().unwrap();
        let mut keys = HashSet::new();
        collect_keys(&serde_json::to_value(&batch).unwrap(), &mut keys);
        for forbidden in [
            "credential",
            "capability_grant",
            "source_assurance",
            "decision_authority",
            "authorization",
        ] {
            assert!(!keys.contains(forbidden));
        }

        let mut forged = batch;
        forged.workspace_id = "workspace-b".to_string();
        forged.batch_sha256 = ax_fabric_candidate_batch_sha256(&forged).unwrap();
        assert!(forged.validate().is_err());
    }

    fn timeline_entry(sequence: u64) -> AxStudioMemoryTimelineEntry {
        let mut entry = AxStudioMemoryTimelineEntry {
            contract_version: AX_MEMORY_EXCHANGE_CONTRACT_VERSION,
            entry_id: format!("entry-{sequence}"),
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            committed_sequence: sequence,
            committed_at_ms: 1_700_000_000_000 + sequence,
            mutation_id: format!("mutation-{sequence}"),
            operation: MemoryOperation::Commit,
            assertion_id: "assertion-1".to_string(),
            input_version_ids: Vec::new(),
            output_version_ids: vec![format!("version-{sequence}")],
            policy_decision_id: format!("policy-decision-{sequence}"),
            snapshot_id: None,
            projection_set_id: Some("projection-set-v2".to_string()),
            artifact_ids: vec!["lexical-v2".to_string(), "tokenizer-v2".to_string()],
            entry_sha256: String::new(),
        };
        entry.entry_sha256 = ax_studio_timeline_entry_sha256(&entry).unwrap();
        entry
    }

    #[test]
    fn studio_timeline_and_debugger_are_content_free_and_ordered() {
        let entries = vec![timeline_entry(11), timeline_entry(12)];
        let mut page = AxStudioMemoryTimelinePage {
            contract_version: AX_MEMORY_EXCHANGE_CONTRACT_VERSION,
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            start_after_sequence: 10,
            entries,
            has_more: true,
            next_after_sequence: Some(12),
            page_sha256: String::new(),
        };
        page.page_sha256 = ax_studio_timeline_page_sha256(&page).unwrap();
        page.validate().unwrap();

        let rendered_context_sha256 = sha256_hex(b"quoted retained context");
        let mut debug = AxStudioMemoryDebugRecord {
            contract_version: AX_MEMORY_EXCHANGE_CONTRACT_VERSION,
            debug_record_id: "debug-1".to_string(),
            workspace_id: "workspace-a".to_string(),
            namespace: "repo/akidb".to_string(),
            snapshot_id: "snapshot-1".to_string(),
            canonical_sequence: 12,
            visible_sequence: 12,
            projection_set_id: "projection-set-v2".to_string(),
            policy_decision_id: "policy-decision-recall-1".to_string(),
            artifact_ids: vec!["lexical-v2".to_string(), "tokenizer-v2".to_string()],
            item_version_ids: vec!["version-11".to_string(), "version-12".to_string()],
            rendered_context_sha256,
            recorded_at_ms: 1_700_000_000_100,
            record_sha256: String::new(),
        };
        debug.record_sha256 = ax_studio_debug_record_sha256(&debug).unwrap();
        debug.validate().unwrap();

        let encoded = serde_json::to_string(&(page, debug)).unwrap();
        assert!(!encoded.contains("quoted retained context"));
    }
}
