//! Single-node generation publication and atomic read-runtime cutover.
//!
//! This controller is deliberately local. PostgreSQL remains the future
//! multi-replica authority; this module coordinates one replica's durable
//! serving state, immutable generation directories, and in-process read
//! runtime without claiming distributed HA.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use akidb_contracts::KnowledgeScope;
use akidb_storage::{
    GenerationLayoutError, GenerationServingState, LocalGenerationState, ReadyGeneration,
    RocksDbBackend, ServingStateError, ServingStateRecord, ServingStateStore,
};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tracing::warn;

use crate::{GenerationMaterializer, GenerationMaterializerError, ReadyGenerationRuntime};

const MAX_CONTROL_FAILURE_CHARS: usize = 4_096;

/// Compare-and-swap condition for activation and rollback.
///
/// `Any` is useful for local recovery. Management APIs should normally send
/// either `NoActive` for first publication or `Generation` for an explicit
/// compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedActiveGeneration {
    Any,
    NoActive,
    Generation(String),
}

impl ExpectedActiveGeneration {
    fn matches(&self, actual: Option<&str>) -> bool {
        match self {
            Self::Any => true,
            Self::NoActive => actual.is_none(),
            Self::Generation(expected) => actual == Some(expected.as_str()),
        }
    }

    fn description(&self) -> String {
        match self {
            Self::Any => "<any>".to_string(),
            Self::NoActive => "<none>".to_string(),
            Self::Generation(generation_id) => generation_id.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum GenerationControlError {
    #[error(transparent)]
    Materializer(#[from] GenerationMaterializerError),

    #[error(transparent)]
    Layout(#[from] GenerationLayoutError),

    #[error(transparent)]
    State(#[from] ServingStateError),

    #[error("active generation compare-and-swap failed: expected {expected}, observed {actual}")]
    ActiveGenerationConflict { expected: String, actual: String },

    #[error("generation control state is inconsistent: {0}")]
    InconsistentState(String),
}

/// Result of a verified local publication. Publication does not activate it.
#[derive(Debug, Clone)]
pub struct GenerationPublication {
    pub ready: ReadyGeneration,
    pub state: ServingStateRecord,
}

#[derive(Default)]
struct ScopeRuntimeSet {
    active: Option<Arc<ReadyGenerationRuntime>>,
    previous: Option<Arc<ReadyGenerationRuntime>>,
    staged: Option<Arc<ReadyGenerationRuntime>>,
}

/// Coordinates crash-safe local generation transitions.
///
/// The outer transition mutex serializes publication, activation, rollback,
/// and restart recovery. The runtime write lock spans each durable active
/// pointer transition, so a request obtains either the complete old runtime or
/// the complete new runtime, never a mixture.
pub struct GenerationController {
    materializer: Arc<GenerationMaterializer>,
    state: Arc<ServingStateStore<RocksDbBackend>>,
    transition_lock: Mutex<()>,
    runtimes: RwLock<HashMap<KnowledgeScope, ScopeRuntimeSet>>,
}

impl GenerationController {
    pub fn new(
        materializer: Arc<GenerationMaterializer>,
        state: Arc<ServingStateStore<RocksDbBackend>>,
    ) -> Self {
        Self {
            materializer,
            state,
            transition_lock: Mutex::new(()),
            runtimes: RwLock::new(HashMap::new()),
        }
    }

    pub fn replica_id(&self) -> &str {
        self.state.replica_id()
    }

    pub fn materializer(&self) -> &Arc<GenerationMaterializer> {
        &self.materializer
    }

    /// Stage, checksum, materialize, seal, and mark one generation READY.
    ///
    /// The active runtime remains untouched throughout. Retrying exact content
    /// after the physical READY rename is safe and repairs any missing durable
    /// serving-state transition.
    pub fn publish_from_reader<R: Read>(
        &self,
        manifest_bytes: &[u8],
        manifest_sha256: &str,
        bundle: R,
        updated_at_ms: u64,
    ) -> Result<GenerationPublication, GenerationControlError> {
        let _transition = self.transition_lock.lock();
        let prepared =
            self.materializer
                .store()
                .prepare(manifest_bytes, manifest_sha256, updated_at_ms)?;
        let manifest = prepared.manifest().clone();
        let scope = manifest.scope();

        if let Some(record) = self.state.load(&scope)? {
            if let Some(existing) = generation_by_id(&record, &manifest.generation_id) {
                ensure_same_generation(existing, &manifest, manifest_sha256)?;
                if matches!(
                    existing.state,
                    LocalGenerationState::Serving | LocalGenerationState::Ready
                ) {
                    let ready = self
                        .materializer
                        .store()
                        .load_ready(&scope, &manifest.generation_id)?;
                    self.ensure_runtime_cache(&record)?;
                    return Ok(GenerationPublication {
                        ready,
                        state: record,
                    });
                }
            }
        }

        self.state.stage_generation(
            manifest.clone(),
            manifest_sha256.to_string(),
            updated_at_ms,
        )?;

        let ready =
            match self
                .materializer
                .install_and_materialize(&prepared, bundle, updated_at_ms)
            {
                Ok(ready) => ready,
                Err(error) => {
                    let failure = bounded_control_failure(&error);
                    if let Err(state_error) = self.state.fail_staged(
                        &scope,
                        &manifest.generation_id,
                        failure,
                        updated_at_ms,
                    ) {
                        warn!(
                            workspace_id = %scope.workspace_id,
                            collection = %scope.collection,
                            generation_id = %manifest.generation_id,
                            error = %state_error,
                            "failed to persist generation-control failure evidence"
                        );
                    }
                    return Err(error.into());
                }
            };

        let record = self.required_state(&scope)?;
        let staged = record.staged.as_ref().ok_or_else(|| {
            GenerationControlError::InconsistentState(format!(
                "generation {} became physically ready without staged serving state",
                manifest.generation_id
            ))
        })?;
        ensure_same_generation(staged, &manifest, manifest_sha256)?;
        match staged.state {
            LocalGenerationState::Staged => {
                self.state
                    .mark_bundle_loaded(&scope, &manifest.generation_id, updated_at_ms)?;
                self.state
                    .mark_ready(&scope, &manifest.generation_id, updated_at_ms)?;
            }
            LocalGenerationState::CatchingUp => {
                self.state
                    .mark_ready(&scope, &manifest.generation_id, updated_at_ms)?;
            }
            LocalGenerationState::Ready => {}
            LocalGenerationState::Failed => {
                return Err(GenerationControlError::InconsistentState(format!(
                    "generation {} is physically ready but serving state is failed",
                    manifest.generation_id
                )));
            }
            LocalGenerationState::Serving => {
                return Err(GenerationControlError::InconsistentState(format!(
                    "staged generation {} is already marked serving",
                    manifest.generation_id
                )));
            }
        }

        let state = self.required_state(&scope)?;
        self.ensure_runtime_cache(&state)?;
        Ok(GenerationPublication { ready, state })
    }

    /// Activate a READY staged generation and atomically switch local reads.
    pub fn activate(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
        expected_active: &ExpectedActiveGeneration,
        updated_at_ms: u64,
    ) -> Result<ServingStateRecord, GenerationControlError> {
        let _transition = self.transition_lock.lock();
        let before = self.required_state(scope)?;
        let actual_active = active_generation_id(&before);

        if actual_active == Some(generation_id) {
            self.ensure_runtime_cache(&before)?;
            return Ok(before);
        }
        ensure_expected_active(expected_active, actual_active)?;

        let staged = before.staged.as_ref().ok_or_else(|| {
            GenerationControlError::InconsistentState(format!(
                "generation {generation_id} cannot activate without staged state"
            ))
        })?;
        if staged.manifest.generation_id != generation_id
            || staged.state != LocalGenerationState::Ready
        {
            return Err(GenerationControlError::InconsistentState(format!(
                "generation {generation_id} is not the READY staged generation"
            )));
        }

        self.ensure_runtime_cache(&before)?;

        // Holding this write lock across the durable state commit creates the
        // local read barrier. Existing requests retain a complete Arc to the
        // old runtime; new requests cannot clone one until the swap completes.
        {
            let mut runtimes = self.runtimes.write();
            let runtime_set = runtimes.get_mut(scope).ok_or_else(|| {
                GenerationControlError::InconsistentState(
                    "runtime cache disappeared during activation".to_string(),
                )
            })?;
            let next = runtime_set.staged.clone().ok_or_else(|| {
                GenerationControlError::InconsistentState(
                    "READY staged runtime was not restored".to_string(),
                )
            })?;
            if next.ready.manifest.generation_id != generation_id {
                return Err(GenerationControlError::InconsistentState(
                    "staged runtime identity differs from durable state".to_string(),
                ));
            }
            self.state.activate(scope, generation_id, updated_at_ms)?;
            let former_active = runtime_set.active.replace(next);
            runtime_set.previous = former_active;
            runtime_set.staged = None;
        }

        let after = self.required_state(scope)?;
        self.reconcile_pointer_cache(&after);
        Ok(after)
    }

    /// Roll back to the single retained previous generation.
    pub fn rollback(
        &self,
        scope: &KnowledgeScope,
        target_generation_id: &str,
        expected_active: &ExpectedActiveGeneration,
        updated_at_ms: u64,
    ) -> Result<ServingStateRecord, GenerationControlError> {
        let _transition = self.transition_lock.lock();
        let before = self.required_state(scope)?;
        let actual_active = active_generation_id(&before);

        if actual_active == Some(target_generation_id) {
            self.ensure_runtime_cache(&before)?;
            return Ok(before);
        }
        ensure_expected_active(expected_active, actual_active)?;

        let previous = before.previous.as_ref().ok_or_else(|| {
            GenerationControlError::InconsistentState(
                "rollback requested without a retained previous generation".to_string(),
            )
        })?;
        if previous.manifest.generation_id != target_generation_id
            || previous.state != LocalGenerationState::Ready
        {
            return Err(GenerationControlError::InconsistentState(format!(
                "generation {target_generation_id} is not the retained READY rollback target"
            )));
        }

        self.ensure_runtime_cache(&before)?;
        {
            let mut runtimes = self.runtimes.write();
            let runtime_set = runtimes.get_mut(scope).ok_or_else(|| {
                GenerationControlError::InconsistentState(
                    "runtime cache disappeared during rollback".to_string(),
                )
            })?;
            let target = runtime_set.previous.clone().ok_or_else(|| {
                GenerationControlError::InconsistentState(
                    "rollback runtime was not restored".to_string(),
                )
            })?;
            if target.ready.manifest.generation_id != target_generation_id {
                return Err(GenerationControlError::InconsistentState(
                    "rollback runtime identity differs from durable state".to_string(),
                ));
            }

            self.state
                .rollback(scope, target_generation_id, updated_at_ms)?;
            let former_active = runtime_set.active.replace(target);
            runtime_set.previous = former_active;
        }

        let after = self.required_state(scope)?;
        self.reconcile_pointer_cache(&after);
        Ok(after)
    }

    /// Restore active and rollback runtimes from durable serving state after a
    /// process restart. No state transition is performed.
    pub fn restore_scope(
        &self,
        scope: &KnowledgeScope,
    ) -> Result<Option<ServingStateRecord>, GenerationControlError> {
        let _transition = self.transition_lock.lock();
        let Some(record) = self.state.load(scope)? else {
            self.runtimes.write().remove(scope);
            return Ok(None);
        };
        self.ensure_runtime_cache(&record)?;
        self.reconcile_pointer_cache(&record);
        Ok(Some(record))
    }

    pub fn status(
        &self,
        scope: &KnowledgeScope,
    ) -> Result<Option<ServingStateRecord>, GenerationControlError> {
        self.state.load(scope).map_err(Into::into)
    }

    /// Clone the active immutable runtime for one complete read operation.
    pub fn active_runtime(&self, scope: &KnowledgeScope) -> Option<Arc<ReadyGenerationRuntime>> {
        self.runtimes
            .read()
            .get(scope)
            .and_then(|runtime_set| runtime_set.active.clone())
    }

    /// Clone any locally retained active, previous, or READY staged runtime.
    pub fn ready_runtime(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> Option<Arc<ReadyGenerationRuntime>> {
        self.cached_runtime(scope, generation_id)
    }

    fn required_state(
        &self,
        scope: &KnowledgeScope,
    ) -> Result<ServingStateRecord, GenerationControlError> {
        self.state.load(scope)?.ok_or_else(|| {
            GenerationControlError::InconsistentState(format!(
                "no serving state exists for {}/{}",
                scope.workspace_id, scope.collection
            ))
        })
    }

    fn ensure_runtime_cache(
        &self,
        record: &ServingStateRecord,
    ) -> Result<(), GenerationControlError> {
        let scope = record.scope();
        let active = self.runtime_for_state(&scope, record.active.as_ref())?;
        let previous = self.runtime_for_state(&scope, record.previous.as_ref())?;
        let staged = match record.staged.as_ref() {
            Some(generation) if generation.state == LocalGenerationState::Ready => {
                self.runtime_for_state(&scope, Some(generation))?
            }
            _ => None,
        };
        self.runtimes.write().insert(
            scope,
            ScopeRuntimeSet {
                active,
                previous,
                staged,
            },
        );
        Ok(())
    }

    fn runtime_for_state(
        &self,
        scope: &KnowledgeScope,
        generation: Option<&GenerationServingState>,
    ) -> Result<Option<Arc<ReadyGenerationRuntime>>, GenerationControlError> {
        let Some(generation) = generation else {
            return Ok(None);
        };
        if let Some(existing) = self.cached_runtime(scope, &generation.manifest.generation_id) {
            return Ok(Some(existing));
        }
        Ok(Some(Arc::new(
            self.materializer
                .open_ready(scope, &generation.manifest.generation_id)?,
        )))
    }

    fn cached_runtime(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> Option<Arc<ReadyGenerationRuntime>> {
        let runtimes = self.runtimes.read();
        let runtime_set = runtimes.get(scope)?;
        runtime_set
            .active
            .iter()
            .chain(runtime_set.previous.iter())
            .chain(runtime_set.staged.iter())
            .find(|runtime| runtime.ready.manifest.generation_id == generation_id)
            .cloned()
    }

    fn reconcile_pointer_cache(&self, record: &ServingStateRecord) {
        if let Err(error) = self.materializer.store().reconcile_pointer_set(record) {
            // The RocksDB state and in-process runtime are authoritative. This
            // filesystem pointer set is explicitly a reconstructible cache.
            warn!(
                workspace_id = %record.workspace_id,
                collection = %record.collection,
                error = %error,
                "failed to reconcile recoverable generation pointer cache"
            );
        }
    }
}

fn generation_by_id<'a>(
    record: &'a ServingStateRecord,
    generation_id: &str,
) -> Option<&'a GenerationServingState> {
    record
        .active
        .iter()
        .chain(record.previous.iter())
        .chain(record.staged.iter())
        .find(|generation| generation.manifest.generation_id == generation_id)
}

fn ensure_same_generation(
    existing: &GenerationServingState,
    manifest: &akidb_contracts::KnowledgeGenerationManifest,
    manifest_sha256: &str,
) -> Result<(), GenerationControlError> {
    if existing.manifest == *manifest && existing.manifest_sha256 == manifest_sha256 {
        return Ok(());
    }
    Err(GenerationControlError::InconsistentState(format!(
        "generation {} already exists with different immutable content",
        manifest.generation_id
    )))
}

fn active_generation_id(record: &ServingStateRecord) -> Option<&str> {
    record
        .active
        .as_ref()
        .map(|generation| generation.manifest.generation_id.as_str())
}

fn ensure_expected_active(
    expected: &ExpectedActiveGeneration,
    actual: Option<&str>,
) -> Result<(), GenerationControlError> {
    if expected.matches(actual) {
        return Ok(());
    }
    Err(GenerationControlError::ActiveGenerationConflict {
        expected: expected.description(),
        actual: actual.unwrap_or("<none>").to_string(),
    })
}

fn bounded_control_failure(error: &GenerationMaterializerError) -> String {
    let mut failure: String = error
        .to_string()
        .chars()
        .take(MAX_CONTROL_FAILURE_CHARS)
        .collect();
    if failure.trim().is_empty() {
        failure = "generation publication failed".to_string();
    }
    failure
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_contracts::KnowledgeGenerationManifest;
    use akidb_faiss::{SearchParams, VectorIndex};
    use akidb_storage::GenerationStore;
    use serde_json::Value;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    const BUNDLE: &[u8] =
        include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle.ndjson");
    const MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");

    struct Harness {
        _temporary: TempDir,
        materializer: Arc<GenerationMaterializer>,
        state: Arc<ServingStateStore<RocksDbBackend>>,
        controller: GenerationController,
    }

    impl Harness {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let generation_store =
                Arc::new(GenerationStore::open(temporary.path().join("generations")).unwrap());
            let materializer = Arc::new(GenerationMaterializer::new(
                generation_store,
                Default::default(),
            ));
            let state_storage =
                Arc::new(RocksDbBackend::open(temporary.path().join("control")).unwrap());
            let state = Arc::new(ServingStateStore::new(state_storage, "replica-test").unwrap());
            let controller = GenerationController::new(materializer.clone(), state.clone());
            Self {
                _temporary: temporary,
                materializer,
                state,
                controller,
            }
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn generation(
        generation_id: &str,
        parent_generation_id: Option<&str>,
        vector: [f64; 3],
    ) -> (Vec<u8>, KnowledgeGenerationManifest, Vec<u8>) {
        let mut manifest: KnowledgeGenerationManifest = serde_json::from_str(MANIFEST).unwrap();
        manifest.generation_id = generation_id.to_string();
        manifest.parent_generation_id = parent_generation_id.map(str::to_string);

        let mut entries: Vec<Value> = BUNDLE
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        entries[0]["header"]["generation_id"] = Value::String(generation_id.to_string());
        entries[1]["record"]["vector"] =
            Value::Array(vector.into_iter().map(Value::from).collect());
        let mut bundle = Vec::new();
        for entry in entries {
            bundle.extend(serde_json::to_vec(&entry).unwrap());
            bundle.push(b'\n');
        }
        manifest.bundle.sha256 = digest(&bundle);
        manifest.bundle.size_bytes = bundle.len() as u64;
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        (manifest_bytes, manifest, bundle)
    }

    fn publish(
        controller: &GenerationController,
        generation_id: &str,
        parent_generation_id: Option<&str>,
        vector: [f64; 3],
        timestamp: u64,
    ) -> KnowledgeGenerationManifest {
        let (manifest_bytes, manifest, bundle) =
            generation(generation_id, parent_generation_id, vector);
        controller
            .publish_from_reader(
                &manifest_bytes,
                &digest(&manifest_bytes),
                bundle.as_slice(),
                timestamp,
            )
            .unwrap();
        manifest
    }

    #[test]
    fn publish_is_shadowed_until_atomic_activation_and_rollback() {
        let harness = Harness::new();
        let first = publish(
            &harness.controller,
            "generation-a",
            None,
            [1.0, 0.0, 0.0],
            1,
        );
        let scope = first.scope();
        assert!(harness.controller.active_runtime(&scope).is_none());

        harness
            .controller
            .activate(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::NoActive,
                2,
            )
            .unwrap();
        assert_eq!(
            harness
                .controller
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-a"
        );

        publish(
            &harness.controller,
            "generation-b",
            Some("generation-a"),
            [0.0, 1.0, 0.0],
            3,
        );
        assert_eq!(
            harness
                .controller
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-a"
        );

        let activated = harness
            .controller
            .activate(
                &scope,
                "generation-b",
                &ExpectedActiveGeneration::Generation("generation-a".to_string()),
                4,
            )
            .unwrap();
        assert_eq!(
            activated.previous.as_ref().unwrap().manifest.generation_id,
            "generation-a"
        );
        let runtime = harness.controller.active_runtime(&scope).unwrap();
        assert_eq!(
            runtime.ready.manifest.generation_id,
            "generation-b".to_string()
        );
        let results = runtime
            .index
            .search(&[0.0, 1.0, 0.0], &SearchParams::new(1))
            .unwrap();
        assert_eq!(results[0].id.as_str(), "chunk-a");
        drop(runtime);

        let rolled_back = harness
            .controller
            .rollback(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::Generation("generation-b".to_string()),
                5,
            )
            .unwrap();
        assert_eq!(
            rolled_back.active.as_ref().unwrap().manifest.generation_id,
            "generation-a"
        );
        assert_eq!(
            harness
                .controller
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-a"
        );
    }

    #[test]
    fn activation_cas_mismatch_leaves_state_and_runtime_unchanged() {
        let harness = Harness::new();
        let first = publish(
            &harness.controller,
            "generation-a",
            None,
            [1.0, 0.0, 0.0],
            1,
        );
        let scope = first.scope();
        harness
            .controller
            .activate(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::NoActive,
                2,
            )
            .unwrap();
        publish(
            &harness.controller,
            "generation-b",
            Some("generation-a"),
            [0.0, 1.0, 0.0],
            3,
        );

        let error = harness
            .controller
            .activate(
                &scope,
                "generation-b",
                &ExpectedActiveGeneration::Generation("unexpected".to_string()),
                4,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            GenerationControlError::ActiveGenerationConflict { .. }
        ));
        assert_eq!(
            harness
                .controller
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-a"
        );
        assert_eq!(
            harness
                .state
                .load(&scope)
                .unwrap()
                .unwrap()
                .active
                .unwrap()
                .manifest
                .generation_id,
            "generation-a"
        );
    }

    #[test]
    fn failed_shadow_build_never_changes_active_runtime() {
        let harness = Harness::new();
        let first = publish(
            &harness.controller,
            "generation-a",
            None,
            [1.0, 0.0, 0.0],
            1,
        );
        let scope = first.scope();
        harness
            .controller
            .activate(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::NoActive,
                2,
            )
            .unwrap();

        let (manifest_bytes, _, mut bundle) =
            generation("generation-b", Some("generation-a"), [0.0, 1.0, 0.0]);
        bundle[0] ^= 0xff;
        let error = harness
            .controller
            .publish_from_reader(
                &manifest_bytes,
                &digest(&manifest_bytes),
                bundle.as_slice(),
                3,
            )
            .unwrap_err();
        assert!(error.to_string().contains("bundle digest mismatch"));
        assert_eq!(
            harness
                .controller
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-a"
        );
        let state = harness.state.load(&scope).unwrap().unwrap();
        assert_eq!(state.active.unwrap().manifest.generation_id, "generation-a");
        assert_eq!(state.staged.unwrap().state, LocalGenerationState::Failed);
    }

    #[test]
    fn restart_restores_active_and_previous_runtime_from_durable_state() {
        let harness = Harness::new();
        let first = publish(
            &harness.controller,
            "generation-a",
            None,
            [1.0, 0.0, 0.0],
            1,
        );
        let scope = first.scope();
        harness
            .controller
            .activate(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::NoActive,
                2,
            )
            .unwrap();
        publish(
            &harness.controller,
            "generation-b",
            Some("generation-a"),
            [0.0, 1.0, 0.0],
            3,
        );
        harness
            .controller
            .activate(
                &scope,
                "generation-b",
                &ExpectedActiveGeneration::Generation("generation-a".to_string()),
                4,
            )
            .unwrap();

        let Harness {
            _temporary,
            materializer,
            state,
            controller,
        } = harness;
        drop(controller);
        let restored = GenerationController::new(materializer, state);
        let state = restored.restore_scope(&scope).unwrap().unwrap();
        assert_eq!(state.active.unwrap().manifest.generation_id, "generation-b");
        assert_eq!(
            restored
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-b"
        );
        restored
            .rollback(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::Generation("generation-b".to_string()),
                5,
            )
            .unwrap();
        assert_eq!(
            restored
                .active_runtime(&scope)
                .unwrap()
                .ready
                .manifest
                .generation_id,
            "generation-a"
        );
        drop(_temporary);
    }

    #[test]
    fn exact_publish_retry_after_ready_repairs_state_idempotently() {
        let harness = Harness::new();
        let (manifest_bytes, manifest, bundle) = generation("generation-a", None, [1.0, 0.0, 0.0]);
        let manifest_sha256 = digest(&manifest_bytes);
        let first = harness
            .controller
            .publish_from_reader(&manifest_bytes, &manifest_sha256, bundle.as_slice(), 1)
            .unwrap();
        let second = harness
            .controller
            .publish_from_reader(&manifest_bytes, &manifest_sha256, std::io::empty(), 2)
            .unwrap();

        assert_eq!(second.ready.marker, first.ready.marker);
        let staged = second.state.staged.unwrap();
        assert_eq!(staged.manifest, manifest);
        assert_eq!(staged.state, LocalGenerationState::Ready);
    }

    #[test]
    fn pointer_cache_tracks_atomic_activation_and_rollback() {
        let harness = Harness::new();
        let first = publish(
            &harness.controller,
            "generation-a",
            None,
            [1.0, 0.0, 0.0],
            1,
        );
        let scope = first.scope();
        harness
            .controller
            .activate(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::NoActive,
                2,
            )
            .unwrap();
        publish(
            &harness.controller,
            "generation-b",
            Some("generation-a"),
            [0.0, 1.0, 0.0],
            3,
        );
        harness
            .controller
            .activate(
                &scope,
                "generation-b",
                &ExpectedActiveGeneration::Generation("generation-a".to_string()),
                4,
            )
            .unwrap();

        let pointers = harness
            .materializer
            .store()
            .load_pointer_set(&scope)
            .unwrap()
            .unwrap();
        assert_eq!(pointers.active.unwrap().generation_id, "generation-b");
        assert_eq!(pointers.previous.unwrap().generation_id, "generation-a");

        harness
            .controller
            .rollback(
                &scope,
                "generation-a",
                &ExpectedActiveGeneration::Generation("generation-b".to_string()),
                5,
            )
            .unwrap();
        let pointers = harness
            .materializer
            .store()
            .load_pointer_set(&scope)
            .unwrap()
            .unwrap();
        assert_eq!(pointers.active.unwrap().generation_id, "generation-a");
        assert_eq!(pointers.previous.unwrap().generation_id, "generation-b");
    }
}
