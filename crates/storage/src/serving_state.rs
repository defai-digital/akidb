//! Crash-persistent state for versioned, rebuildable knowledge replicas.
//!
//! This module deliberately does not change the live server or gRPC data path.
//! It makes the local generation, ordered-replay, activation, and rollback
//! invariants executable before remote publication is introduced.

use crate::{AkiDbError, BatchOperation, StorageBackend};
use akidb_contracts::{
    ContractViolation, KnowledgeGenerationManifest, KnowledgeMutation, KnowledgeScope,
    ReplicaCheckpoint, ReplicaState, KNOWLEDGE_SCHEMA_VERSION,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;

/// Version of the RocksDB serving-state encoding.
pub const SERVING_STATE_SCHEMA_VERSION: u32 = 1;

const KEY_NAMESPACE: &[u8] = b"akidb\0knowledge-serving\0v1\0";
const MAX_REPLICA_ID_BYTES: usize = 1_024;
const MAX_FAILURE_BYTES: usize = 16_384;

/// Local lifecycle states. `Staged` is intentionally not reported as a
/// [`ReplicaState`] because a staged manifest has not loaded its bundle yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalGenerationState {
    Staged,
    CatchingUp,
    Ready,
    Serving,
    Failed,
}

/// Durable state for one immutable generation on this replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationServingState {
    pub manifest: KnowledgeGenerationManifest,
    pub manifest_sha256: String,
    pub applied_sequence: u64,
    pub state: LocalGenerationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Durable active/staged/rollback view for one workspace and collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServingStateRecord {
    pub schema_version: u32,
    pub replica_id: String,
    pub workspace_id: String,
    pub collection: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<GenerationServingState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<GenerationServingState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged: Option<GenerationServingState>,
    pub updated_at_ms: u64,
}

impl ServingStateRecord {
    pub fn scope(&self) -> KnowledgeScope {
        KnowledgeScope::new(self.workspace_id.clone(), self.collection.clone())
    }
}

/// Result of staging an immutable generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageGenerationOutcome {
    Staged,
    AlreadyStaged,
}

/// Result of recording an ordered mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyMutationOutcome {
    Applied,
    Duplicate,
}

/// Errors preserve the distinction between retriable gaps and permanent
/// identity conflicts so a future materializer can react safely.
#[derive(Debug, Error)]
pub enum ServingStateError {
    #[error(transparent)]
    Contract(#[from] ContractViolation),

    #[error(transparent)]
    Storage(#[from] AkiDbError),

    #[error("serving-state serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid replica_id: {0}")]
    InvalidReplicaId(String),

    #[error("{field} must be greater than zero")]
    InvalidTimestamp { field: &'static str },

    #[error("no serving state exists for {workspace_id}/{collection}")]
    StateNotFound {
        workspace_id: String,
        collection: String,
    },

    #[error("no staged generation exists for {workspace_id}/{collection}")]
    StagedGenerationNotFound {
        workspace_id: String,
        collection: String,
    },

    #[error("generation mismatch: expected {expected}, received {actual}")]
    GenerationMismatch { expected: String, actual: String },

    #[error("scope mismatch: expected {expected}, received {actual}")]
    ScopeMismatch { expected: String, actual: String },

    #[error("generation conflict: {0}")]
    GenerationConflict(String),

    #[error("invalid serving-state transition: {0}")]
    InvalidTransition(String),

    #[error("mutation sequence gap/out-of-order: expected {expected}, received {actual}")]
    SequenceGap { expected: u64, actual: u64 },

    #[error(
        "sequence {sequence} is already owned by mutation {existing_mutation_id}, not {received_mutation_id}"
    )]
    SequenceConflict {
        sequence: u64,
        existing_mutation_id: String,
        received_mutation_id: String,
    },

    #[error(
        "mutation {mutation_id} is already bound to sequence {existing_sequence}, not {received_sequence}"
    )]
    MutationIdConflict {
        mutation_id: String,
        existing_sequence: u64,
        received_sequence: u64,
    },

    #[error("mutation {mutation_id} was redelivered with different content")]
    MutationContentConflict { mutation_id: String },

    #[error("corrupt serving state: {0}")]
    CorruptState(String),
}

type ServingStateResult<T> = std::result::Result<T, ServingStateError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationMarker {
    mutation_id: String,
    sequence: u64,
    mutation_sha256: String,
}

/// Process-local owner of a replica's durable serving state.
///
/// All read-modify-write transitions are serialized through one mutex. The
/// state record, checkpoint, and mutation markers are committed through one
/// backend batch, which maps to one atomic RocksDB `WriteBatch`.
pub struct ServingStateStore<S: StorageBackend> {
    storage: Arc<S>,
    replica_id: String,
    transition_lock: Mutex<()>,
}

impl<S: StorageBackend> ServingStateStore<S> {
    pub fn new(storage: Arc<S>, replica_id: impl Into<String>) -> ServingStateResult<Self> {
        let replica_id = replica_id.into();
        validate_local_text(
            "replica_id",
            &replica_id,
            MAX_REPLICA_ID_BYTES,
            ServingStateError::InvalidReplicaId,
        )?;
        Ok(Self {
            storage,
            replica_id,
            transition_lock: Mutex::new(()),
        })
    }

    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }

    /// Loads and validates a scope record. Corruption is never converted to an
    /// empty/default state.
    pub fn load(&self, scope: &KnowledgeScope) -> ServingStateResult<Option<ServingStateRecord>> {
        let _guard = self.transition_lock.lock();
        self.load_unlocked(scope)
    }

    /// Loads the durable checkpoint for one generation.
    pub fn load_checkpoint(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> ServingStateResult<Option<ReplicaCheckpoint>> {
        let _guard = self.transition_lock.lock();
        scope.validate()?;
        let key = checkpoint_key(scope, generation_id);
        let Some(bytes) = self.storage.get(&key)? else {
            return Ok(None);
        };
        let checkpoint: ReplicaCheckpoint = serde_json::from_slice(&bytes)?;
        checkpoint.validate().map_err(|error| {
            ServingStateError::CorruptState(format!(
                "invalid checkpoint for generation {generation_id}: {error}"
            ))
        })?;
        if checkpoint.replica_id != self.replica_id {
            return Err(ServingStateError::CorruptState(format!(
                "checkpoint replica_id {} does not match local replica_id {}",
                checkpoint.replica_id, self.replica_id
            )));
        }
        if checkpoint.scope() != *scope || checkpoint.generation_id != generation_id {
            return Err(ServingStateError::CorruptState(
                "checkpoint key and payload identity differ".to_string(),
            ));
        }
        Ok(Some(checkpoint))
    }

    /// Persists a generation as staged without touching the active pointer.
    pub fn stage_generation(
        &self,
        manifest: KnowledgeGenerationManifest,
        manifest_sha256: impl Into<String>,
        updated_at_ms: u64,
    ) -> ServingStateResult<StageGenerationOutcome> {
        let _guard = self.transition_lock.lock();
        manifest.validate()?;
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let manifest_sha256 = manifest_sha256.into();
        validate_sha256("manifest_sha256", &manifest_sha256)?;
        let scope = manifest.scope();

        let mut record = self
            .load_unlocked(&scope)?
            .unwrap_or_else(|| ServingStateRecord {
                schema_version: SERVING_STATE_SCHEMA_VERSION,
                replica_id: self.replica_id.clone(),
                workspace_id: scope.workspace_id.clone(),
                collection: scope.collection.clone(),
                active: None,
                previous: None,
                staged: None,
                updated_at_ms,
            });

        if generation_is_present(&record.active, &manifest.generation_id)
            || generation_is_present(&record.previous, &manifest.generation_id)
        {
            return Err(ServingStateError::GenerationConflict(format!(
                "generation {} is already active or retained for rollback",
                manifest.generation_id
            )));
        }

        if let Some(staged) = &record.staged {
            if staged.manifest.generation_id != manifest.generation_id {
                return Err(ServingStateError::GenerationConflict(format!(
                    "generation {} is already staged",
                    staged.manifest.generation_id
                )));
            }
            if staged.manifest == manifest && staged.manifest_sha256 == manifest_sha256 {
                return Ok(StageGenerationOutcome::AlreadyStaged);
            }
            return Err(ServingStateError::GenerationConflict(format!(
                "generation {} was staged with different immutable content",
                manifest.generation_id
            )));
        }

        record.staged = Some(GenerationServingState {
            applied_sequence: manifest.base_sequence,
            manifest,
            manifest_sha256,
            state: LocalGenerationState::Staged,
            last_error: None,
        });
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;
        self.persist_record(&record, Vec::new())?;
        Ok(StageGenerationOutcome::Staged)
    }

    /// Marks the immutable bundle as locally loaded and starts tail replay.
    pub fn mark_bundle_loaded(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        updated_at_ms: u64,
    ) -> ServingStateResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let mut record = self.load_required_unlocked(scope)?;
        let checkpoint = {
            let staged = staged_mut(&mut record, generation_id)?;
            match staged.state {
                LocalGenerationState::Staged => {
                    staged.state = LocalGenerationState::CatchingUp;
                }
                LocalGenerationState::CatchingUp => return Ok(()),
                other => {
                    return Err(ServingStateError::InvalidTransition(format!(
                        "bundle-loaded requires staged; generation is {other:?}"
                    )));
                }
            }
            checkpoint_for(&self.replica_id, staged, updated_at_ms)?
        };
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;
        self.persist_record(&record, vec![checkpoint])
    }

    /// Records exactly the next mutation and its identity markers atomically.
    ///
    /// Applying the vector/BM25/graph payload is a Phase 2 materializer concern.
    /// That materializer must add its index operations to the same commit
    /// boundary before this method is connected to a runtime data path.
    pub fn apply_mutation(
        &self,
        mutation: &KnowledgeMutation,
        updated_at_ms: u64,
    ) -> ServingStateResult<ApplyMutationOutcome> {
        let _guard = self.transition_lock.lock();
        mutation.validate()?;
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let scope = mutation.scope();
        let sequence_key =
            mutation_sequence_key(&scope, &mutation.generation_id, mutation.sequence);
        let identity_key =
            mutation_identity_key(&scope, &mutation.generation_id, &mutation.mutation_id);
        let mutation_sha256 = mutation_fingerprint(mutation)?;

        if let Some(bytes) = self.storage.get(&sequence_key)? {
            let marker = decode_marker(&bytes)?;
            let existing_identity_key =
                mutation_identity_key(&scope, &mutation.generation_id, &marker.mutation_id);
            let identity_bytes = self.storage.get(&existing_identity_key)?.ok_or_else(|| {
                ServingStateError::CorruptState(format!(
                    "sequence marker exists without mutation identity marker for {}",
                    marker.mutation_id
                ))
            })?;
            let identity_marker = decode_marker(&identity_bytes)?;
            if identity_marker != marker {
                return Err(ServingStateError::CorruptState(format!(
                    "sequence and identity markers differ for {}",
                    marker.mutation_id
                )));
            }
            if marker.mutation_id != mutation.mutation_id {
                return Err(ServingStateError::SequenceConflict {
                    sequence: mutation.sequence,
                    existing_mutation_id: marker.mutation_id,
                    received_mutation_id: mutation.mutation_id.clone(),
                });
            }
            if marker.mutation_sha256 != mutation_sha256 {
                return Err(ServingStateError::MutationContentConflict {
                    mutation_id: mutation.mutation_id.clone(),
                });
            }
            return Ok(ApplyMutationOutcome::Duplicate);
        }

        if let Some(bytes) = self.storage.get(&identity_key)? {
            let marker = decode_marker(&bytes)?;
            if marker.sequence != mutation.sequence {
                return Err(ServingStateError::MutationIdConflict {
                    mutation_id: mutation.mutation_id.clone(),
                    existing_sequence: marker.sequence,
                    received_sequence: mutation.sequence,
                });
            }
            return Err(ServingStateError::CorruptState(format!(
                "mutation identity marker exists without sequence marker for {}",
                mutation.mutation_id
            )));
        }

        let mut record = self.load_required_unlocked(&scope)?;
        let checkpoint = {
            let staged = staged_mut(&mut record, &mutation.generation_id)?;
            if staged.state != LocalGenerationState::CatchingUp {
                return Err(ServingStateError::InvalidTransition(format!(
                    "mutation replay requires catching_up; generation is {:?}",
                    staged.state
                )));
            }
            let expected = staged.applied_sequence.checked_add(1).ok_or_else(|| {
                ServingStateError::CorruptState("applied_sequence overflow".to_string())
            })?;
            if mutation.sequence != expected {
                return Err(ServingStateError::SequenceGap {
                    expected,
                    actual: mutation.sequence,
                });
            }
            if mutation.sequence > staged.manifest.target_sequence {
                return Err(ServingStateError::InvalidTransition(format!(
                    "mutation sequence {} exceeds target sequence {}",
                    mutation.sequence, staged.manifest.target_sequence
                )));
            }
            staged.applied_sequence = mutation.sequence;
            checkpoint_for(&self.replica_id, staged, updated_at_ms)?
        };
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;

        let marker = MutationMarker {
            mutation_id: mutation.mutation_id.clone(),
            sequence: mutation.sequence,
            mutation_sha256,
        };
        let marker_bytes = serde_json::to_vec(&marker)?;
        let mut operations = self.record_operations(&record, vec![checkpoint])?;
        operations.push(BatchOperation::Put {
            key: sequence_key,
            value: marker_bytes.clone(),
        });
        operations.push(BatchOperation::Put {
            key: identity_key,
            value: marker_bytes,
        });
        self.storage.write_batch(operations)?;
        Ok(ApplyMutationOutcome::Applied)
    }

    /// Promotes a caught-up staged generation to ready.
    pub fn mark_ready(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        updated_at_ms: u64,
    ) -> ServingStateResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let mut record = self.load_required_unlocked(scope)?;
        let checkpoint = {
            let staged = staged_mut(&mut record, generation_id)?;
            match staged.state {
                LocalGenerationState::Ready => return Ok(()),
                LocalGenerationState::CatchingUp => {}
                other => {
                    return Err(ServingStateError::InvalidTransition(format!(
                        "ready requires catching_up; generation is {other:?}"
                    )));
                }
            }
            if staged.applied_sequence != staged.manifest.target_sequence {
                return Err(ServingStateError::InvalidTransition(format!(
                    "generation is at sequence {}, target is {}",
                    staged.applied_sequence, staged.manifest.target_sequence
                )));
            }
            staged.state = LocalGenerationState::Ready;
            checkpoint_for(&self.replica_id, staged, updated_at_ms)?
        };
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;
        self.persist_record(&record, vec![checkpoint])
    }

    /// Atomically activates a ready generation and retains the former active
    /// generation as the single rollback target.
    pub fn activate(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        updated_at_ms: u64,
    ) -> ServingStateResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let mut record = self.load_required_unlocked(scope)?;
        if generation_is_present(&record.active, generation_id) {
            return Ok(());
        }
        let mut next = record.staged.take().ok_or_else(|| missing_staged(scope))?;
        ensure_generation(&next, generation_id)?;
        if next.state != LocalGenerationState::Ready {
            record.staged = Some(next);
            return Err(ServingStateError::InvalidTransition(
                "only a ready generation may be activated".to_string(),
            ));
        }

        let mut checkpoints = Vec::with_capacity(2);
        if let Some(mut active) = record.active.take() {
            active.state = LocalGenerationState::Ready;
            active.last_error = None;
            checkpoints.push(checkpoint_for(&self.replica_id, &active, updated_at_ms)?);
            record.previous = Some(active);
        }
        next.state = LocalGenerationState::Serving;
        next.last_error = None;
        checkpoints.push(checkpoint_for(&self.replica_id, &next, updated_at_ms)?);
        record.active = Some(next);
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;
        self.persist_record(&record, checkpoints)
    }

    /// Atomically swaps the active generation with the retained rollback target.
    pub fn rollback(
        &self,
        scope: &KnowledgeScope,
        target_generation_id: &str,
        updated_at_ms: u64,
    ) -> ServingStateResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let mut record = self.load_required_unlocked(scope)?;
        if generation_is_present(&record.active, target_generation_id) {
            return Ok(());
        }
        let mut target = record.previous.take().ok_or_else(|| {
            ServingStateError::InvalidTransition("no rollback generation is retained".to_string())
        })?;
        ensure_generation(&target, target_generation_id)?;
        let mut former_active = record.active.take().ok_or_else(|| {
            ServingStateError::CorruptState(
                "rollback target exists but active generation is absent".to_string(),
            )
        })?;

        target.state = LocalGenerationState::Serving;
        target.last_error = None;
        former_active.state = LocalGenerationState::Ready;
        former_active.last_error = None;
        let checkpoints = vec![
            checkpoint_for(&self.replica_id, &target, updated_at_ms)?,
            checkpoint_for(&self.replica_id, &former_active, updated_at_ms)?,
        ];
        record.active = Some(target);
        record.previous = Some(former_active);
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;
        self.persist_record(&record, checkpoints)
    }

    /// Records non-empty failure evidence without changing the active pointer.
    pub fn fail_staged(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        failure: impl Into<String>,
        updated_at_ms: u64,
    ) -> ServingStateResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp("updated_at_ms", updated_at_ms)?;
        let failure = failure.into();
        validate_local_text("last_error", &failure, MAX_FAILURE_BYTES, |message| {
            ServingStateError::InvalidTransition(message)
        })?;
        let mut record = self.load_required_unlocked(scope)?;
        let checkpoint = {
            let staged = staged_mut(&mut record, generation_id)?;
            if staged.state == LocalGenerationState::Serving {
                return Err(ServingStateError::InvalidTransition(
                    "active serving generation cannot be failed through fail_staged".to_string(),
                ));
            }
            if staged.state == LocalGenerationState::Failed
                && staged.last_error.as_deref() == Some(failure.as_str())
            {
                return Ok(());
            }
            staged.state = LocalGenerationState::Failed;
            staged.last_error = Some(failure);
            checkpoint_for(&self.replica_id, staged, updated_at_ms)?
        };
        record.updated_at_ms = updated_at_ms;
        validate_record(&record, &self.replica_id)?;
        self.persist_record(&record, vec![checkpoint])
    }

    fn load_required_unlocked(
        &self,
        scope: &KnowledgeScope,
    ) -> ServingStateResult<ServingStateRecord> {
        self.load_unlocked(scope)?
            .ok_or_else(|| ServingStateError::StateNotFound {
                workspace_id: scope.workspace_id.clone(),
                collection: scope.collection.clone(),
            })
    }

    fn load_unlocked(
        &self,
        scope: &KnowledgeScope,
    ) -> ServingStateResult<Option<ServingStateRecord>> {
        scope.validate()?;
        let Some(bytes) = self.storage.get(&state_key(scope))? else {
            return Ok(None);
        };
        let record: ServingStateRecord = serde_json::from_slice(&bytes)?;
        validate_record(&record, &self.replica_id)?;
        if record.scope() != *scope {
            return Err(ServingStateError::CorruptState(
                "state key and payload scope differ".to_string(),
            ));
        }
        Ok(Some(record))
    }

    fn persist_record(
        &self,
        record: &ServingStateRecord,
        checkpoints: Vec<ReplicaCheckpoint>,
    ) -> ServingStateResult<()> {
        let operations = self.record_operations(record, checkpoints)?;
        self.storage.write_batch(operations)?;
        Ok(())
    }

    fn record_operations(
        &self,
        record: &ServingStateRecord,
        checkpoints: Vec<ReplicaCheckpoint>,
    ) -> ServingStateResult<Vec<BatchOperation>> {
        let scope = record.scope();
        let mut operations = Vec::with_capacity(1 + checkpoints.len());
        operations.push(BatchOperation::Put {
            key: state_key(&scope),
            value: serde_json::to_vec(record)?,
        });
        for checkpoint in checkpoints {
            checkpoint.validate()?;
            operations.push(BatchOperation::Put {
                key: checkpoint_key(&scope, &checkpoint.generation_id),
                value: serde_json::to_vec(&checkpoint)?,
            });
        }
        Ok(operations)
    }
}

fn staged_mut<'a>(
    record: &'a mut ServingStateRecord,
    generation_id: &str,
) -> ServingStateResult<&'a mut GenerationServingState> {
    let scope = record.scope();
    let staged = record
        .staged
        .as_mut()
        .ok_or_else(|| missing_staged(&scope))?;
    ensure_generation(staged, generation_id)?;
    Ok(staged)
}

fn missing_staged(scope: &KnowledgeScope) -> ServingStateError {
    ServingStateError::StagedGenerationNotFound {
        workspace_id: scope.workspace_id.clone(),
        collection: scope.collection.clone(),
    }
}

fn ensure_generation(
    generation: &GenerationServingState,
    generation_id: &str,
) -> ServingStateResult<()> {
    if generation.manifest.generation_id != generation_id {
        return Err(ServingStateError::GenerationMismatch {
            expected: generation.manifest.generation_id.clone(),
            actual: generation_id.to_string(),
        });
    }
    Ok(())
}

fn generation_is_present(generation: &Option<GenerationServingState>, generation_id: &str) -> bool {
    generation
        .as_ref()
        .is_some_and(|state| state.manifest.generation_id == generation_id)
}

fn checkpoint_for(
    replica_id: &str,
    generation: &GenerationServingState,
    updated_at_ms: u64,
) -> ServingStateResult<ReplicaCheckpoint> {
    let state = match generation.state {
        LocalGenerationState::CatchingUp => ReplicaState::CatchingUp,
        LocalGenerationState::Ready => ReplicaState::Ready,
        LocalGenerationState::Serving => ReplicaState::Serving,
        LocalGenerationState::Failed => ReplicaState::Failed,
        LocalGenerationState::Staged => {
            return Err(ServingStateError::InvalidTransition(
                "a staged manifest has no reportable replica checkpoint".to_string(),
            ));
        }
    };
    let checkpoint = ReplicaCheckpoint {
        schema_version: KNOWLEDGE_SCHEMA_VERSION,
        replica_id: replica_id.to_string(),
        workspace_id: generation.manifest.workspace_id.clone(),
        collection: generation.manifest.collection.clone(),
        generation_id: generation.manifest.generation_id.clone(),
        manifest_sha256: generation.manifest_sha256.clone(),
        applied_sequence: generation.applied_sequence,
        state,
        last_error: generation.last_error.clone(),
        updated_at_ms,
    };
    checkpoint.validate()?;
    Ok(checkpoint)
}

fn validate_record(record: &ServingStateRecord, replica_id: &str) -> ServingStateResult<()> {
    if record.schema_version != SERVING_STATE_SCHEMA_VERSION {
        return Err(ServingStateError::CorruptState(format!(
            "unsupported state schema version {}",
            record.schema_version
        )));
    }
    record.scope().validate().map_err(|error| {
        ServingStateError::CorruptState(format!("invalid persisted scope: {error}"))
    })?;
    validate_timestamp("updated_at_ms", record.updated_at_ms)
        .map_err(|error| ServingStateError::CorruptState(error.to_string()))?;
    if record.replica_id != replica_id {
        return Err(ServingStateError::CorruptState(format!(
            "record replica_id {} does not match local replica_id {replica_id}",
            record.replica_id
        )));
    }

    let mut generation_ids = HashSet::new();
    for (role, generation) in [
        ("active", record.active.as_ref()),
        ("previous", record.previous.as_ref()),
        ("staged", record.staged.as_ref()),
    ] {
        let Some(generation) = generation else {
            continue;
        };
        validate_generation_state(role, generation, &record.scope())?;
        if !generation_ids.insert(generation.manifest.generation_id.as_str()) {
            return Err(ServingStateError::CorruptState(format!(
                "generation {} appears in more than one role",
                generation.manifest.generation_id
            )));
        }
    }
    if record.active.is_none() && record.previous.is_some() {
        return Err(ServingStateError::CorruptState(
            "previous generation exists without an active generation".to_string(),
        ));
    }
    Ok(())
}

fn validate_generation_state(
    role: &str,
    generation: &GenerationServingState,
    scope: &KnowledgeScope,
) -> ServingStateResult<()> {
    generation.manifest.validate().map_err(|error| {
        ServingStateError::CorruptState(format!(
            "invalid {role} generation {}: {error}",
            generation.manifest.generation_id
        ))
    })?;
    validate_sha256("manifest_sha256", &generation.manifest_sha256)
        .map_err(|error| ServingStateError::CorruptState(error.to_string()))?;
    if generation.manifest.scope() != *scope {
        return Err(ServingStateError::CorruptState(format!(
            "{role} generation has a different scope"
        )));
    }
    if generation.applied_sequence < generation.manifest.base_sequence
        || generation.applied_sequence > generation.manifest.target_sequence
    {
        return Err(ServingStateError::CorruptState(format!(
            "{role} applied sequence {} is outside {}..={}",
            generation.applied_sequence,
            generation.manifest.base_sequence,
            generation.manifest.target_sequence
        )));
    }
    if matches!(
        generation.state,
        LocalGenerationState::Ready | LocalGenerationState::Serving
    ) && generation.applied_sequence != generation.manifest.target_sequence
    {
        return Err(ServingStateError::CorruptState(format!(
            "{role} generation is {:?} before reaching target sequence",
            generation.state
        )));
    }
    if generation.state == LocalGenerationState::Staged
        && generation.applied_sequence != generation.manifest.base_sequence
    {
        return Err(ServingStateError::CorruptState(format!(
            "{role} staged generation has advanced beyond its bundle base sequence"
        )));
    }
    match generation.state {
        LocalGenerationState::Failed => {
            let failure = generation.last_error.as_deref().ok_or_else(|| {
                ServingStateError::CorruptState(format!(
                    "{role} failed generation lacks failure evidence"
                ))
            })?;
            validate_local_text(
                "last_error",
                failure,
                MAX_FAILURE_BYTES,
                ServingStateError::CorruptState,
            )?;
        }
        _ if generation.last_error.is_some() => {
            return Err(ServingStateError::CorruptState(format!(
                "{role} non-failed generation contains failure evidence"
            )));
        }
        _ => {}
    }
    match role {
        "active" if generation.state != LocalGenerationState::Serving => {
            return Err(ServingStateError::CorruptState(
                "active generation is not serving".to_string(),
            ));
        }
        "previous" if generation.state != LocalGenerationState::Ready => {
            return Err(ServingStateError::CorruptState(
                "previous generation is not ready".to_string(),
            ));
        }
        "staged" if generation.state == LocalGenerationState::Serving => {
            return Err(ServingStateError::CorruptState(
                "staged generation cannot be serving".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn validate_local_text<F>(
    field: &str,
    value: &str,
    maximum: usize,
    error: F,
) -> ServingStateResult<()>
where
    F: FnOnce(String) -> ServingStateError,
{
    if value.trim().is_empty() {
        return Err(error(format!("{field} cannot be empty")));
    }
    if value.trim() != value {
        return Err(error(format!(
            "{field} must not have leading or trailing whitespace"
        )));
    }
    if value.len() > maximum {
        return Err(error(format!(
            "{field} length {} exceeds maximum {maximum}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(error(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_timestamp(field: &'static str, value: u64) -> ServingStateResult<()> {
    if value == 0 {
        return Err(ServingStateError::InvalidTimestamp { field });
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> ServingStateResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ServingStateError::CorruptState(format!(
            "{field} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn mutation_fingerprint(mutation: &KnowledgeMutation) -> ServingStateResult<String> {
    let bytes = serde_json::to_vec(mutation)?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn decode_marker(bytes: &[u8]) -> ServingStateResult<MutationMarker> {
    let marker: MutationMarker = serde_json::from_slice(bytes)?;
    validate_local_text(
        "mutation_id",
        &marker.mutation_id,
        1_024,
        ServingStateError::CorruptState,
    )?;
    validate_sha256("mutation_sha256", &marker.mutation_sha256)?;
    if marker.sequence == 0 {
        return Err(ServingStateError::CorruptState(
            "mutation marker sequence must be greater than zero".to_string(),
        ));
    }
    Ok(marker)
}

fn state_key(scope: &KnowledgeScope) -> Vec<u8> {
    compound_key(b"state", scope, &[])
}

fn checkpoint_key(scope: &KnowledgeScope, generation_id: &str) -> Vec<u8> {
    compound_key(b"checkpoint", scope, &[generation_id.as_bytes()])
}

fn mutation_sequence_key(scope: &KnowledgeScope, generation_id: &str, sequence: u64) -> Vec<u8> {
    let sequence_bytes = sequence.to_be_bytes();
    compound_key(
        b"mutation-sequence",
        scope,
        &[generation_id.as_bytes(), &sequence_bytes],
    )
}

fn mutation_identity_key(
    scope: &KnowledgeScope,
    generation_id: &str,
    mutation_id: &str,
) -> Vec<u8> {
    compound_key(
        b"mutation-identity",
        scope,
        &[generation_id.as_bytes(), mutation_id.as_bytes()],
    )
}

fn compound_key(kind: &[u8], scope: &KnowledgeScope, components: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        KEY_NAMESPACE.len()
            + kind.len()
            + scope.workspace_id.len()
            + scope.collection.len()
            + components.iter().map(|value| value.len()).sum::<usize>()
            + 32,
    );
    key.extend_from_slice(KEY_NAMESPACE);
    push_component(&mut key, kind);
    push_component(&mut key, scope.workspace_id.as_bytes());
    push_component(&mut key, scope.collection.as_bytes());
    for component in components {
        push_component(&mut key, component);
    }
    key
}

fn push_component(key: &mut Vec<u8>, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("validated key component fits in u32");
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Result as StorageResult, RocksDbBackend};
    use akidb_contracts::{ImmutableObjectReference, KnowledgeOperation};
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use tempfile::tempdir;

    const DIGEST_A: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const DIGEST_B: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    #[derive(Default)]
    struct MemoryBackend {
        values: StdMutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        batch_count: AtomicUsize,
        fail_next_batch: AtomicBool,
    }

    impl MemoryBackend {
        fn batch_count(&self) -> usize {
            self.batch_count.load(Ordering::SeqCst)
        }

        fn fail_next_batch(&self) {
            self.fail_next_batch.store(true, Ordering::SeqCst);
        }
    }

    impl StorageBackend for MemoryBackend {
        fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
            self.values
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &[u8]) -> StorageResult<()> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }

        fn exists(&self, key: &[u8]) -> StorageResult<bool> {
            Ok(self.values.lock().unwrap().contains_key(key))
        }

        fn write_batch(&self, operations: Vec<BatchOperation>) -> StorageResult<()> {
            self.batch_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_next_batch.swap(false, Ordering::SeqCst) {
                return Err(AkiDbError::StorageError(
                    "injected atomic batch failure".to_string(),
                ));
            }
            let mut values = self.values.lock().unwrap();
            for operation in operations {
                match operation {
                    BatchOperation::Put { key, value } => {
                        values.insert(key, value);
                    }
                    BatchOperation::Delete { key } => {
                        values.remove(&key);
                    }
                }
            }
            Ok(())
        }

        fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        fn flush(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    fn scope() -> KnowledgeScope {
        KnowledgeScope::new("workspace-a", "knowledge")
    }

    fn manifest(generation_id: &str, parent: Option<&str>) -> KnowledgeGenerationManifest {
        KnowledgeGenerationManifest {
            schema_version: KNOWLEDGE_SCHEMA_VERSION,
            workspace_id: "workspace-a".to_string(),
            collection: "knowledge".to_string(),
            generation_id: generation_id.to_string(),
            parent_generation_id: parent.map(str::to_string),
            created_at_ms: 1_784_995_200_000,
            embedding_model_id: "model@revision".to_string(),
            embedding_dimensions: 768,
            graph_schema_version: "ax.knowledge-graph.v1".to_string(),
            bundle: ImmutableObjectReference {
                uri: format!("s3://knowledge/generations/{generation_id}/bundle.tar.zst"),
                sha256: DIGEST_A.to_string(),
                size_bytes: 4_096,
            },
            base_sequence: 100,
            target_sequence: 102,
            expected_vector_count: 10,
            expected_edge_count: 20,
        }
    }

    fn mutation(generation_id: &str, sequence: u64, mutation_id: &str) -> KnowledgeMutation {
        KnowledgeMutation {
            schema_version: KNOWLEDGE_SCHEMA_VERSION,
            mutation_id: mutation_id.to_string(),
            workspace_id: "workspace-a".to_string(),
            collection: "knowledge".to_string(),
            generation_id: generation_id.to_string(),
            sequence,
            operation: KnowledgeOperation::Upsert,
            chunk_id: format!("chunk-{sequence}"),
            payload: Some(ImmutableObjectReference {
                uri: format!("s3://knowledge/mutations/{mutation_id}.json"),
                sha256: DIGEST_B.to_string(),
                size_bytes: 512,
            }),
            created_at_ms: 1_784_995_200_000 + sequence,
        }
    }

    fn ready_generation(
        store: &ServingStateStore<MemoryBackend>,
        generation_id: &str,
        parent: Option<&str>,
        start_time: u64,
    ) {
        store
            .stage_generation(manifest(generation_id, parent), DIGEST_A, start_time)
            .unwrap();
        store
            .mark_bundle_loaded(&scope(), generation_id, start_time + 1)
            .unwrap();
        store
            .apply_mutation(
                &mutation(generation_id, 101, &format!("{generation_id}-m101")),
                start_time + 2,
            )
            .unwrap();
        store
            .apply_mutation(
                &mutation(generation_id, 102, &format!("{generation_id}-m102")),
                start_time + 3,
            )
            .unwrap();
        store
            .mark_ready(&scope(), generation_id, start_time + 4)
            .unwrap();
    }

    #[test]
    fn staging_never_changes_active_generation() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        ready_generation(&store, "generation-a", None, 1);
        store.activate(&scope(), "generation-a", 10).unwrap();

        store
            .stage_generation(manifest("generation-b", Some("generation-a")), DIGEST_B, 11)
            .unwrap();
        let state = store.load(&scope()).unwrap().unwrap();
        assert_eq!(state.active.unwrap().manifest.generation_id, "generation-a");
        assert_eq!(state.staged.unwrap().manifest.generation_id, "generation-b");
    }

    #[test]
    fn duplicate_delivery_is_a_noop_and_conflicts_are_rejected() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend.clone(), "replica-1").unwrap();
        store
            .stage_generation(manifest("generation-a", None), DIGEST_A, 1)
            .unwrap();
        store
            .mark_bundle_loaded(&scope(), "generation-a", 2)
            .unwrap();
        let first = mutation("generation-a", 101, "mutation-a");
        assert_eq!(
            store.apply_mutation(&first, 3).unwrap(),
            ApplyMutationOutcome::Applied
        );
        let writes_after_first = backend.batch_count();
        assert_eq!(
            store.apply_mutation(&first, 4).unwrap(),
            ApplyMutationOutcome::Duplicate
        );
        assert_eq!(backend.batch_count(), writes_after_first);

        let same_sequence = mutation("generation-a", 101, "mutation-b");
        assert!(matches!(
            store.apply_mutation(&same_sequence, 5),
            Err(ServingStateError::SequenceConflict { .. })
        ));

        let reused_id = mutation("generation-a", 102, "mutation-a");
        assert!(matches!(
            store.apply_mutation(&reused_id, 6),
            Err(ServingStateError::MutationIdConflict { .. })
        ));
    }

    #[test]
    fn repeated_stage_is_a_noop_but_changed_content_conflicts() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend.clone(), "replica-1").unwrap();
        let generation = manifest("generation-a", None);
        assert_eq!(
            store
                .stage_generation(generation.clone(), DIGEST_A, 1)
                .unwrap(),
            StageGenerationOutcome::Staged
        );
        let writes_after_first = backend.batch_count();
        assert_eq!(
            store
                .stage_generation(generation.clone(), DIGEST_A, 2)
                .unwrap(),
            StageGenerationOutcome::AlreadyStaged
        );
        assert_eq!(backend.batch_count(), writes_after_first);

        assert!(matches!(
            store.stage_generation(generation, DIGEST_B, 3),
            Err(ServingStateError::GenerationConflict(_))
        ));
    }

    #[test]
    fn redelivery_with_changed_content_is_rejected() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        store
            .stage_generation(manifest("generation-a", None), DIGEST_A, 1)
            .unwrap();
        store
            .mark_bundle_loaded(&scope(), "generation-a", 2)
            .unwrap();
        let first = mutation("generation-a", 101, "mutation-a");
        store.apply_mutation(&first, 3).unwrap();
        let mut changed = first;
        changed.chunk_id = "different-chunk".to_string();
        assert!(matches!(
            store.apply_mutation(&changed, 4),
            Err(ServingStateError::MutationContentConflict { .. })
        ));
    }

    #[test]
    fn a_gap_blocks_readiness_without_advancing_checkpoint() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        store
            .stage_generation(manifest("generation-a", None), DIGEST_A, 1)
            .unwrap();
        store
            .mark_bundle_loaded(&scope(), "generation-a", 2)
            .unwrap();
        assert!(matches!(
            store.apply_mutation(&mutation("generation-a", 102, "mutation-102"), 3),
            Err(ServingStateError::SequenceGap {
                expected: 101,
                actual: 102
            })
        ));
        assert!(matches!(
            store.mark_ready(&scope(), "generation-a", 4),
            Err(ServingStateError::InvalidTransition(_))
        ));
        let checkpoint = store
            .load_checkpoint(&scope(), "generation-a")
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.applied_sequence, 100);
        assert_eq!(checkpoint.state, ReplicaState::CatchingUp);
    }

    #[test]
    fn mutation_for_another_scope_or_generation_is_rejected() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        store
            .stage_generation(manifest("generation-a", None), DIGEST_A, 1)
            .unwrap();
        store
            .mark_bundle_loaded(&scope(), "generation-a", 2)
            .unwrap();

        let other_generation = mutation("generation-b", 101, "mutation-b");
        assert!(matches!(
            store.apply_mutation(&other_generation, 3),
            Err(ServingStateError::GenerationMismatch { .. })
        ));

        let mut other_scope = mutation("generation-a", 101, "mutation-c");
        other_scope.workspace_id = "workspace-b".to_string();
        assert!(matches!(
            store.apply_mutation(&other_scope, 4),
            Err(ServingStateError::StateNotFound { .. })
        ));
        assert_eq!(
            store
                .load_checkpoint(&scope(), "generation-a")
                .unwrap()
                .unwrap()
                .applied_sequence,
            100
        );
    }

    #[test]
    fn activation_and_rollback_update_both_checkpoints_atomically() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        ready_generation(&store, "generation-a", None, 1);
        store.activate(&scope(), "generation-a", 10).unwrap();
        ready_generation(&store, "generation-b", Some("generation-a"), 20);
        store.activate(&scope(), "generation-b", 30).unwrap();

        let state = store.load(&scope()).unwrap().unwrap();
        assert_eq!(
            state.active.as_ref().unwrap().manifest.generation_id,
            "generation-b"
        );
        assert_eq!(
            state.previous.as_ref().unwrap().manifest.generation_id,
            "generation-a"
        );
        assert_eq!(
            store
                .load_checkpoint(&scope(), "generation-b")
                .unwrap()
                .unwrap()
                .state,
            ReplicaState::Serving
        );
        assert_eq!(
            store
                .load_checkpoint(&scope(), "generation-a")
                .unwrap()
                .unwrap()
                .state,
            ReplicaState::Ready
        );

        store.rollback(&scope(), "generation-a", 31).unwrap();
        let rolled_back = store.load(&scope()).unwrap().unwrap();
        assert_eq!(
            rolled_back.active.unwrap().manifest.generation_id,
            "generation-a"
        );
        assert_eq!(
            rolled_back.previous.unwrap().manifest.generation_id,
            "generation-b"
        );
    }

    #[test]
    fn failed_staged_generation_keeps_active_serving_and_requires_evidence() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        ready_generation(&store, "generation-a", None, 1);
        store.activate(&scope(), "generation-a", 10).unwrap();
        store
            .stage_generation(manifest("generation-b", Some("generation-a")), DIGEST_B, 11)
            .unwrap();

        assert!(matches!(
            store.fail_staged(&scope(), "generation-b", "   ", 12),
            Err(ServingStateError::InvalidTransition(_))
        ));
        store
            .fail_staged(&scope(), "generation-b", "bundle checksum mismatch", 13)
            .unwrap();

        let state = store.load(&scope()).unwrap().unwrap();
        assert_eq!(state.active.unwrap().manifest.generation_id, "generation-a");
        assert_eq!(state.staged.unwrap().state, LocalGenerationState::Failed);
        let checkpoint = store
            .load_checkpoint(&scope(), "generation-b")
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.state, ReplicaState::Failed);
        assert_eq!(
            checkpoint.last_error.as_deref(),
            Some("bundle checksum mismatch")
        );
    }

    #[test]
    fn active_state_survives_rocksdb_restart() {
        let directory = tempdir().unwrap();
        {
            let backend = Arc::new(RocksDbBackend::open(directory.path()).unwrap());
            let store = ServingStateStore::new(backend, "replica-1").unwrap();
            let mut no_tail = manifest("generation-a", None);
            no_tail.target_sequence = no_tail.base_sequence;
            store.stage_generation(no_tail, DIGEST_A, 1).unwrap();
            store
                .mark_bundle_loaded(&scope(), "generation-a", 2)
                .unwrap();
            store.mark_ready(&scope(), "generation-a", 3).unwrap();
            store.activate(&scope(), "generation-a", 4).unwrap();
        }
        {
            let backend = Arc::new(RocksDbBackend::open(directory.path()).unwrap());
            let store = ServingStateStore::new(backend, "replica-1").unwrap();
            let recovered = store.load(&scope()).unwrap().unwrap();
            assert_eq!(
                recovered.active.unwrap().manifest.generation_id,
                "generation-a"
            );
            assert_eq!(
                store
                    .load_checkpoint(&scope(), "generation-a")
                    .unwrap()
                    .unwrap()
                    .state,
                ReplicaState::Serving
            );
        }
    }

    #[test]
    fn failed_atomic_batch_leaves_no_partial_state_or_markers() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend.clone(), "replica-1").unwrap();
        store
            .stage_generation(manifest("generation-a", None), DIGEST_A, 1)
            .unwrap();
        store
            .mark_bundle_loaded(&scope(), "generation-a", 2)
            .unwrap();
        backend.fail_next_batch();
        let next = mutation("generation-a", 101, "mutation-a");
        assert!(matches!(
            store.apply_mutation(&next, 3),
            Err(ServingStateError::Storage(_))
        ));
        assert_eq!(
            store
                .load_checkpoint(&scope(), "generation-a")
                .unwrap()
                .unwrap()
                .applied_sequence,
            100
        );
        assert_eq!(
            store.apply_mutation(&next, 4).unwrap(),
            ApplyMutationOutcome::Applied
        );
    }

    #[test]
    fn length_prefixed_scope_keys_do_not_alias() {
        let backend = Arc::new(MemoryBackend::default());
        let store = ServingStateStore::new(backend, "replica-1").unwrap();
        let mut first = manifest("generation-a", None);
        first.workspace_id = "a".to_string();
        first.collection = "bc".to_string();
        let mut second = manifest("generation-b", None);
        second.workspace_id = "ab".to_string();
        second.collection = "c".to_string();
        store.stage_generation(first, DIGEST_A, 1).unwrap();
        store.stage_generation(second, DIGEST_B, 2).unwrap();
        assert!(store
            .load(&KnowledgeScope::new("a", "bc"))
            .unwrap()
            .is_some());
        assert!(store
            .load(&KnowledgeScope::new("ab", "c"))
            .unwrap()
            .is_some());
    }

    proptest! {
        #[test]
        fn any_out_of_order_sequence_is_rejected_without_advancing(
            actual in 102u64..1_000,
        ) {
            let backend = Arc::new(MemoryBackend::default());
            let store = ServingStateStore::new(backend, "replica-1").unwrap();
            let mut generation = manifest("generation-a", None);
            generation.target_sequence = 1_000;
            store.stage_generation(generation, DIGEST_A, 1).unwrap();
            store.mark_bundle_loaded(&scope(), "generation-a", 2).unwrap();
            let result = store.apply_mutation(
                &mutation("generation-a", actual, &format!("mutation-{actual}")),
                3,
            );
            let (expected, received) = match result {
                Err(ServingStateError::SequenceGap { expected, actual }) => (expected, actual),
                other => {
                    return Err(TestCaseError::fail(format!(
                        "expected sequence gap, received {other:?}"
                    )));
                }
            };
            prop_assert_eq!(expected, 101);
            prop_assert_eq!(received, actual);
            let state = store.load(&scope()).unwrap().unwrap();
            prop_assert_eq!(state.staged.unwrap().applied_sequence, 100);
        }
    }
}
