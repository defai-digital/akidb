//! Crash-recoverable physical layout for immutable knowledge generations.
//!
//! A generation is built under a hashed `.building` directory and becomes
//! visible only after validation evidence and a READY marker are durable, then
//! the whole directory is renamed atomically. The RocksDB serving-state record
//! remains authoritative; the filesystem pointer set is a recoverable cache.

use crate::{
    GenerationServingState, KnowledgeBundleSummary, LocalGenerationState, ServingStateRecord,
    SERVING_STATE_SCHEMA_VERSION,
};
use akidb_contracts::{
    ContractViolation, ImmutableObjectReference, KnowledgeGenerationManifest, KnowledgeScope,
};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

/// Version of local generation journals, markers, and pointer caches.
pub const GENERATION_LAYOUT_SCHEMA_VERSION: u32 = 1;

const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_FAILURE_BYTES: usize = 16 * 1024;
const MANIFEST_FILE: &str = "manifest.json";
const MANIFEST_DIGEST_FILE: &str = "manifest.sha256";
const BUNDLE_FILE: &str = "bundle.object";
const JOURNAL_FILE: &str = "build-journal.json";
const MATERIALIZATION_FILE: &str = "materialization.json";
const READY_FILE: &str = "READY.json";
const POINTER_SET_FILE: &str = "pointers.json";

type LayoutResult<T> = std::result::Result<T, GenerationLayoutError>;

#[derive(Debug, Error)]
pub enum GenerationLayoutError {
    #[error(transparent)]
    Contract(#[from] ContractViolation),

    #[error("generation layout I/O error during {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("generation layout JSON error in {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("manifest exceeds the {maximum}-byte limit")]
    ManifestTooLarge { maximum: usize },

    #[error("invalid lowercase SHA-256 digest: {0}")]
    InvalidDigest(String),

    #[error("manifest digest mismatch: expected {expected}, calculated {actual}")]
    ManifestDigestMismatch { expected: String, actual: String },

    #[error("generation layout conflict: {0}")]
    Conflict(String),

    #[error("generation layout rejected symbolic link at {0}")]
    SymbolicLink(PathBuf),

    #[error("generation layout expected a directory at {0}")]
    NotDirectory(PathBuf),

    #[error("generation layout expected a regular file at {0}")]
    NotRegularFile(PathBuf),

    #[error("generation build is not ready for transition: {0}")]
    InvalidTransition(String),

    #[error("bundle size mismatch: expected {expected}, observed {actual}")]
    BundleSizeMismatch { expected: u64, actual: u64 },

    #[error("bundle digest mismatch: expected {expected}, calculated {actual}")]
    BundleDigestMismatch { expected: String, actual: String },

    #[error("generation is not ready: {0}")]
    NotReady(String),

    #[error("invalid local generation state: {0}")]
    CorruptState(String),
}

/// Durable physical build phases. `Ready` means the immutable directory has a
/// complete seal; it does not by itself make the generation active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationBuildPhase {
    Staged,
    BundleVerified,
    Materializing,
    Verified,
    Ready,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationBuildJournal {
    pub schema_version: u32,
    pub workspace_id: String,
    pub collection: String,
    pub generation_id: String,
    pub manifest_sha256: String,
    pub phase: GenerationBuildPhase,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationEvidence {
    pub schema_version: u32,
    pub generation_id: String,
    pub manifest_sha256: String,
    pub record_count: u64,
    pub node_count: u64,
    pub edge_count: u64,
    pub decoded_bytes: u64,
    pub applied_sequence: u64,
    pub verified_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyGenerationMarker {
    pub schema_version: u32,
    pub workspace_id: String,
    pub collection: String,
    pub generation_id: String,
    pub manifest_sha256: String,
    pub bundle_sha256: String,
    pub bundle_size_bytes: u64,
    pub applied_sequence: u64,
    pub record_count: u64,
    pub node_count: u64,
    pub edge_count: u64,
    pub ready_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationPointer {
    pub generation_id: String,
    pub manifest_sha256: String,
    pub applied_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationPointerSet {
    pub schema_version: u32,
    pub workspace_id: String,
    pub collection: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<GenerationPointer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<GenerationPointer>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationPrepareOutcome {
    Started,
    Resumed,
    AlreadyReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleInstallOutcome {
    Installed,
    AlreadyVerified,
}

#[derive(Debug, Clone)]
struct GenerationPaths {
    scope_dir: PathBuf,
    generations_dir: PathBuf,
    building_dir: PathBuf,
    ready_dir: PathBuf,
}

/// Validated handle returned by [`GenerationStore::prepare`].
#[derive(Debug, Clone)]
pub struct PreparedGeneration {
    manifest: KnowledgeGenerationManifest,
    manifest_sha256: String,
    paths: GenerationPaths,
    outcome: GenerationPrepareOutcome,
}

impl PreparedGeneration {
    pub fn manifest(&self) -> &KnowledgeGenerationManifest {
        &self.manifest
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub fn outcome(&self) -> GenerationPrepareOutcome {
        self.outcome
    }

    pub fn building_dir(&self) -> &Path {
        &self.paths.building_dir
    }

    pub fn ready_dir(&self) -> &Path {
        &self.paths.ready_dir
    }
}

#[derive(Debug, Clone)]
pub struct ReadyGeneration {
    pub manifest: KnowledgeGenerationManifest,
    pub marker: ReadyGenerationMarker,
    pub directory: PathBuf,
}

/// Serializes physical transitions within one process. Multi-replica ordering
/// remains a later control-plane responsibility.
pub struct GenerationStore {
    root: PathBuf,
    transition_lock: Mutex<()>,
}

impl GenerationStore {
    /// Open or create a generation root. The configured root itself may not be
    /// a symlink; all generated descendants use SHA-256 path components.
    pub fn open(root: impl AsRef<Path>) -> LayoutResult<Self> {
        let root = root.as_ref();
        if let Ok(metadata) = fs::symlink_metadata(root) {
            if metadata.file_type().is_symlink() {
                return Err(GenerationLayoutError::SymbolicLink(root.to_path_buf()));
            }
            if !metadata.is_dir() {
                return Err(GenerationLayoutError::NotDirectory(root.to_path_buf()));
            }
        } else {
            create_dir_all(root)?;
        }
        let root = fs::canonicalize(root).map_err(|source| GenerationLayoutError::Io {
            operation: "canonicalize generation root",
            path: root.to_path_buf(),
            source,
        })?;
        reject_symlink_or_non_directory(&root)?;
        Ok(Self {
            root,
            transition_lock: Mutex::new(()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Validate exact manifest bytes and create or resume one shadow directory.
    pub fn prepare(
        &self,
        manifest_bytes: &[u8],
        expected_manifest_sha256: &str,
        updated_at_ms: u64,
    ) -> LayoutResult<PreparedGeneration> {
        let _guard = self.transition_lock.lock();
        validate_timestamp(updated_at_ms)?;
        validate_digest(expected_manifest_sha256)?;
        if manifest_bytes.len() > MAX_MANIFEST_BYTES {
            return Err(GenerationLayoutError::ManifestTooLarge {
                maximum: MAX_MANIFEST_BYTES,
            });
        }
        let actual_manifest_sha256 = digest_bytes(manifest_bytes);
        if actual_manifest_sha256 != expected_manifest_sha256 {
            return Err(GenerationLayoutError::ManifestDigestMismatch {
                expected: expected_manifest_sha256.to_string(),
                actual: actual_manifest_sha256,
            });
        }
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(manifest_bytes)
            .map_err(|source| GenerationLayoutError::Json {
                path: PathBuf::from("<manifest request>"),
                source,
            })?;
        manifest.validate()?;
        let paths = self.ensure_scope_paths(&manifest)?;

        if paths.ready_dir.exists() {
            let ready =
                self.load_ready_at(&paths.ready_dir, &manifest.scope(), &manifest.generation_id)?;
            if ready.manifest != manifest
                || ready.marker.manifest_sha256 != expected_manifest_sha256
            {
                return Err(GenerationLayoutError::Conflict(format!(
                    "ready generation {} has different immutable manifest content",
                    manifest.generation_id
                )));
            }
            return Ok(PreparedGeneration {
                manifest,
                manifest_sha256: expected_manifest_sha256.to_string(),
                paths,
                outcome: GenerationPrepareOutcome::AlreadyReady,
            });
        }

        if paths.building_dir.exists() {
            self.verify_building_identity(
                &paths.building_dir,
                manifest_bytes,
                &manifest,
                expected_manifest_sha256,
            )?;
            return Ok(PreparedGeneration {
                manifest,
                manifest_sha256: expected_manifest_sha256.to_string(),
                paths,
                outcome: GenerationPrepareOutcome::Resumed,
            });
        }

        create_dir(&paths.building_dir)?;
        for child in ["rocksdb", "vector", "lexical", "graph"] {
            create_dir(&paths.building_dir.join(child))?;
        }
        atomic_write(&paths.building_dir, MANIFEST_FILE, manifest_bytes, false)?;
        atomic_write(
            &paths.building_dir,
            MANIFEST_DIGEST_FILE,
            format!("{expected_manifest_sha256}\n").as_bytes(),
            false,
        )?;
        let journal = journal_for(
            &manifest,
            expected_manifest_sha256,
            GenerationBuildPhase::Staged,
            updated_at_ms,
            None,
        );
        atomic_write_json(&paths.building_dir, JOURNAL_FILE, &journal)?;
        sync_dir(&paths.building_dir)?;
        sync_dir(&paths.generations_dir)?;

        Ok(PreparedGeneration {
            manifest,
            manifest_sha256: expected_manifest_sha256.to_string(),
            paths,
            outcome: GenerationPrepareOutcome::Started,
        })
    }

    /// Stream an already-authorized object body into the shadow directory and
    /// verify exact compressed byte length and SHA-256 before installation.
    pub fn install_bundle<R: Read>(
        &self,
        prepared: &PreparedGeneration,
        mut reader: R,
        updated_at_ms: u64,
    ) -> LayoutResult<BundleInstallOutcome> {
        let _guard = self.transition_lock.lock();
        validate_timestamp(updated_at_ms)?;
        self.ensure_prepared_identity(prepared)?;
        let directory = if prepared.paths.ready_dir.exists() {
            &prepared.paths.ready_dir
        } else {
            &prepared.paths.building_dir
        };
        let bundle_path = directory.join(BUNDLE_FILE);
        if bundle_path.exists() {
            verify_bundle_file(&bundle_path, &prepared.manifest.bundle)?;
            if directory == &prepared.paths.building_dir {
                let journal = self.load_journal(&prepared.paths.building_dir)?;
                if journal.phase == GenerationBuildPhase::Staged {
                    self.write_journal(
                        prepared,
                        GenerationBuildPhase::BundleVerified,
                        updated_at_ms,
                        None,
                    )?;
                }
            }
            return Ok(BundleInstallOutcome::AlreadyVerified);
        }
        if directory == &prepared.paths.ready_dir {
            return Err(GenerationLayoutError::CorruptState(format!(
                "ready generation {} has no bundle object",
                prepared.manifest.generation_id
            )));
        }

        let journal = self.load_journal(&prepared.paths.building_dir)?;
        if !matches!(
            journal.phase,
            GenerationBuildPhase::Staged | GenerationBuildPhase::BundleVerified
        ) {
            return Err(GenerationLayoutError::InvalidTransition(format!(
                "bundle installation requires staged; build is {:?}",
                journal.phase
            )));
        }

        let temporary_path = prepared
            .paths
            .building_dir
            .join(format!(".bundle.{}.partial", Uuid::new_v4()));
        let result =
            write_and_verify_bundle(&temporary_path, &mut reader, &prepared.manifest.bundle);
        if let Err(error) = result {
            remove_file_if_exists(&temporary_path)?;
            return Err(error);
        }
        reject_symlink_if_present(&bundle_path)?;
        fs::rename(&temporary_path, &bundle_path).map_err(|source| GenerationLayoutError::Io {
            operation: "install verified bundle",
            path: bundle_path.clone(),
            source,
        })?;
        sync_dir(&prepared.paths.building_dir)?;
        self.write_journal(
            prepared,
            GenerationBuildPhase::BundleVerified,
            updated_at_ms,
            None,
        )?;
        Ok(BundleInstallOutcome::Installed)
    }

    pub fn mark_materializing(
        &self,
        prepared: &PreparedGeneration,
        updated_at_ms: u64,
    ) -> LayoutResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp(updated_at_ms)?;
        self.ensure_building(prepared)?;
        verify_bundle_file(
            &prepared.paths.building_dir.join(BUNDLE_FILE),
            &prepared.manifest.bundle,
        )?;
        let journal = self.load_journal(&prepared.paths.building_dir)?;
        match journal.phase {
            GenerationBuildPhase::BundleVerified => self.write_journal(
                prepared,
                GenerationBuildPhase::Materializing,
                updated_at_ms,
                None,
            ),
            GenerationBuildPhase::Materializing => Ok(()),
            other => Err(GenerationLayoutError::InvalidTransition(format!(
                "materialization requires bundle_verified; build is {other:?}"
            ))),
        }
    }

    /// Persist successful vector/lexical/payload/graph build evidence.
    pub fn record_materialization(
        &self,
        prepared: &PreparedGeneration,
        summary: &KnowledgeBundleSummary,
        applied_sequence: u64,
        updated_at_ms: u64,
    ) -> LayoutResult<MaterializationEvidence> {
        let _guard = self.transition_lock.lock();
        validate_timestamp(updated_at_ms)?;
        self.ensure_building(prepared)?;
        summary.header.validate_against(&prepared.manifest)?;
        if applied_sequence != prepared.manifest.target_sequence {
            return Err(GenerationLayoutError::InvalidTransition(format!(
                "materialization checkpoint {applied_sequence} does not match target {}",
                prepared.manifest.target_sequence
            )));
        }
        let journal = self.load_journal(&prepared.paths.building_dir)?;
        if matches!(
            journal.phase,
            GenerationBuildPhase::Verified | GenerationBuildPhase::Ready
        ) {
            let existing: MaterializationEvidence =
                read_json(&prepared.paths.building_dir.join(MATERIALIZATION_FILE))?;
            validate_evidence(&existing, prepared)?;
            if evidence_matches_summary(&existing, summary, applied_sequence) {
                return Ok(existing);
            }
            return Err(GenerationLayoutError::Conflict(
                "verified materialization evidence differs on retry".to_string(),
            ));
        }
        if journal.phase != GenerationBuildPhase::Materializing {
            return Err(GenerationLayoutError::InvalidTransition(format!(
                "build evidence requires materializing; build is {:?}",
                journal.phase
            )));
        }
        let evidence = MaterializationEvidence {
            schema_version: GENERATION_LAYOUT_SCHEMA_VERSION,
            generation_id: prepared.manifest.generation_id.clone(),
            manifest_sha256: prepared.manifest_sha256.clone(),
            record_count: summary.record_count,
            node_count: summary.node_count,
            edge_count: summary.edge_count,
            decoded_bytes: summary.decoded_bytes,
            applied_sequence,
            verified_at_ms: updated_at_ms,
        };
        validate_evidence(&evidence, prepared)?;
        atomic_write_json(
            &prepared.paths.building_dir,
            MATERIALIZATION_FILE,
            &evidence,
        )?;
        self.write_journal(
            prepared,
            GenerationBuildPhase::Verified,
            updated_at_ms,
            None,
        )?;
        Ok(evidence)
    }

    /// Seal and atomically rename a verified shadow directory. This does not
    /// change the authoritative serving-state active pointer.
    pub fn finalize_ready(
        &self,
        prepared: &PreparedGeneration,
        ready_at_ms: u64,
    ) -> LayoutResult<ReadyGeneration> {
        let _guard = self.transition_lock.lock();
        validate_timestamp(ready_at_ms)?;
        if prepared.paths.ready_dir.exists() {
            return self.load_ready_at(
                &prepared.paths.ready_dir,
                &prepared.manifest.scope(),
                &prepared.manifest.generation_id,
            );
        }
        self.ensure_building(prepared)?;
        let journal = self.load_journal(&prepared.paths.building_dir)?;
        if !matches!(
            journal.phase,
            GenerationBuildPhase::Verified | GenerationBuildPhase::Ready
        ) {
            return Err(GenerationLayoutError::InvalidTransition(format!(
                "ready seal requires verified; build is {:?}",
                journal.phase
            )));
        }
        verify_bundle_file(
            &prepared.paths.building_dir.join(BUNDLE_FILE),
            &prepared.manifest.bundle,
        )?;
        let evidence: MaterializationEvidence =
            read_json(&prepared.paths.building_dir.join(MATERIALIZATION_FILE))?;
        validate_evidence(&evidence, prepared)?;
        match journal.phase {
            GenerationBuildPhase::Verified => {
                let marker = ready_marker(prepared, &evidence, ready_at_ms);
                validate_ready_marker(&marker, &prepared.manifest, &prepared.manifest_sha256)?;
                validate_marker_evidence(&marker, &evidence)?;
                atomic_write_json(&prepared.paths.building_dir, READY_FILE, &marker)?;
                self.write_journal(prepared, GenerationBuildPhase::Ready, ready_at_ms, None)?;
            }
            GenerationBuildPhase::Ready => {
                let marker: ReadyGenerationMarker =
                    read_json(&prepared.paths.building_dir.join(READY_FILE))?;
                validate_ready_marker(&marker, &prepared.manifest, &prepared.manifest_sha256)?;
                validate_marker_evidence(&marker, &evidence)?;
            }
            _ => unreachable!("phase gate above permits only verified or ready"),
        }
        sync_tree(&prepared.paths.building_dir)?;
        fs::rename(&prepared.paths.building_dir, &prepared.paths.ready_dir).map_err(|source| {
            GenerationLayoutError::Io {
                operation: "publish immutable ready directory",
                path: prepared.paths.ready_dir.clone(),
                source,
            }
        })?;
        sync_dir(&prepared.paths.generations_dir)?;
        self.load_ready_at(
            &prepared.paths.ready_dir,
            &prepared.manifest.scope(),
            &prepared.manifest.generation_id,
        )
    }

    /// Persist failure evidence in the shadow journal. The active generation
    /// and cached pointer set are untouched.
    pub fn fail_build(
        &self,
        prepared: &PreparedGeneration,
        failure: impl Into<String>,
        updated_at_ms: u64,
    ) -> LayoutResult<()> {
        let _guard = self.transition_lock.lock();
        validate_timestamp(updated_at_ms)?;
        self.ensure_building(prepared)?;
        let failure = failure.into();
        if failure.trim().is_empty()
            || failure.trim() != failure
            || failure.len() > MAX_FAILURE_BYTES
        {
            return Err(GenerationLayoutError::InvalidTransition(
                "failure evidence must be non-empty, trimmed, and bounded".to_string(),
            ));
        }
        self.write_journal(
            prepared,
            GenerationBuildPhase::Failed,
            updated_at_ms,
            Some(failure),
        )
    }

    pub fn load_ready(
        &self,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> LayoutResult<ReadyGeneration> {
        let _guard = self.transition_lock.lock();
        scope.validate()?;
        let paths = self.paths(scope, generation_id);
        self.load_ready_at(&paths.ready_dir, scope, generation_id)
    }

    /// Atomically refresh the whole active/previous filesystem cache from one
    /// authoritative RocksDB record, avoiding mixed pointer generations.
    pub fn reconcile_pointer_set(
        &self,
        record: &ServingStateRecord,
    ) -> LayoutResult<GenerationPointerSet> {
        let _guard = self.transition_lock.lock();
        if record.schema_version != SERVING_STATE_SCHEMA_VERSION {
            return Err(GenerationLayoutError::CorruptState(format!(
                "unsupported serving-state schema {}",
                record.schema_version
            )));
        }
        let scope = record.scope();
        scope.validate()?;
        let paths = self.ensure_scope_paths_for_scope(&scope)?;
        let active = match &record.active {
            Some(generation) => {
                if generation.state != LocalGenerationState::Serving {
                    return Err(GenerationLayoutError::CorruptState(
                        "active generation is not serving".to_string(),
                    ));
                }
                Some(self.pointer_for(&scope, generation)?)
            }
            None => None,
        };
        let previous = match &record.previous {
            Some(generation) => {
                if generation.state != LocalGenerationState::Ready {
                    return Err(GenerationLayoutError::CorruptState(
                        "previous generation is not ready".to_string(),
                    ));
                }
                Some(self.pointer_for(&scope, generation)?)
            }
            None => None,
        };
        if active.is_none() && previous.is_some() {
            return Err(GenerationLayoutError::CorruptState(
                "previous generation exists without active generation".to_string(),
            ));
        }
        let pointers = GenerationPointerSet {
            schema_version: GENERATION_LAYOUT_SCHEMA_VERSION,
            workspace_id: scope.workspace_id.clone(),
            collection: scope.collection.clone(),
            active,
            previous,
            updated_at_ms: record.updated_at_ms,
        };
        validate_pointer_set(&pointers, &scope)?;
        atomic_write_json(&paths.scope_dir, POINTER_SET_FILE, &pointers)?;
        Ok(pointers)
    }

    pub fn load_pointer_set(
        &self,
        scope: &KnowledgeScope,
    ) -> LayoutResult<Option<GenerationPointerSet>> {
        let _guard = self.transition_lock.lock();
        scope.validate()?;
        let paths = self.paths(scope, "placeholder");
        let pointer_path = paths.scope_dir.join(POINTER_SET_FILE);
        if !pointer_path.exists() {
            return Ok(None);
        }
        let pointers: GenerationPointerSet = read_json(&pointer_path)?;
        validate_pointer_set(&pointers, scope)?;
        if let Some(pointer) = &pointers.active {
            self.validate_pointer_target(scope, pointer)?;
        }
        if let Some(pointer) = &pointers.previous {
            self.validate_pointer_target(scope, pointer)?;
        }
        Ok(Some(pointers))
    }

    fn ensure_scope_paths(
        &self,
        manifest: &KnowledgeGenerationManifest,
    ) -> LayoutResult<GenerationPaths> {
        self.ensure_scope_paths_for_scope(&manifest.scope())?;
        Ok(self.paths(&manifest.scope(), &manifest.generation_id))
    }

    fn ensure_scope_paths_for_scope(
        &self,
        scope: &KnowledgeScope,
    ) -> LayoutResult<GenerationPaths> {
        let paths = self.paths(scope, "placeholder");
        let scopes_dir = self.root.join("scopes");
        ensure_child_dir(&self.root, &scopes_dir)?;
        let workspace_dir = paths
            .scope_dir
            .parent()
            .ok_or_else(|| {
                GenerationLayoutError::CorruptState("scope path has no parent".to_string())
            })?
            .to_path_buf();
        ensure_child_dir(&scopes_dir, &workspace_dir)?;
        ensure_child_dir(&workspace_dir, &paths.scope_dir)?;
        ensure_child_dir(&paths.scope_dir, &paths.generations_dir)?;
        Ok(paths)
    }

    fn paths(&self, scope: &KnowledgeScope, generation_id: &str) -> GenerationPaths {
        let workspace_dir = self
            .root
            .join("scopes")
            .join(format!("w-{}", digest_bytes(scope.workspace_id.as_bytes())));
        let scope_dir =
            workspace_dir.join(format!("c-{}", digest_bytes(scope.collection.as_bytes())));
        let generations_dir = scope_dir.join("generations");
        let key = format!("g-{}", digest_bytes(generation_id.as_bytes()));
        GenerationPaths {
            scope_dir,
            generations_dir: generations_dir.clone(),
            building_dir: generations_dir.join(format!("{key}.building")),
            ready_dir: generations_dir.join(key),
        }
    }

    fn verify_building_identity(
        &self,
        directory: &Path,
        manifest_bytes: &[u8],
        manifest: &KnowledgeGenerationManifest,
        expected_manifest_sha256: &str,
    ) -> LayoutResult<()> {
        reject_symlink_or_non_directory(directory)?;
        let stored_bytes = read_bounded_file(&directory.join(MANIFEST_FILE), MAX_MANIFEST_BYTES)?;
        if stored_bytes != manifest_bytes {
            return Err(GenerationLayoutError::Conflict(format!(
                "generation {} is already building from different manifest bytes",
                manifest.generation_id
            )));
        }
        let stored_digest = read_text_file(&directory.join(MANIFEST_DIGEST_FILE))?;
        if stored_digest.trim_end() != expected_manifest_sha256 {
            return Err(GenerationLayoutError::Conflict(format!(
                "generation {} building digest differs",
                manifest.generation_id
            )));
        }
        let stored_manifest: KnowledgeGenerationManifest = serde_json::from_slice(&stored_bytes)
            .map_err(|source| GenerationLayoutError::Json {
                path: directory.join(MANIFEST_FILE),
                source,
            })?;
        if &stored_manifest != manifest {
            return Err(GenerationLayoutError::Conflict(format!(
                "generation {} parsed manifest differs",
                manifest.generation_id
            )));
        }
        let journal = self.load_journal(directory)?;
        validate_journal(&journal, manifest, expected_manifest_sha256)
    }

    fn ensure_prepared_identity(&self, prepared: &PreparedGeneration) -> LayoutResult<()> {
        if prepared.paths.ready_dir.exists() {
            let ready = self.load_ready_at(
                &prepared.paths.ready_dir,
                &prepared.manifest.scope(),
                &prepared.manifest.generation_id,
            )?;
            if ready.marker.manifest_sha256 != prepared.manifest_sha256 {
                return Err(GenerationLayoutError::Conflict(
                    "prepared handle digest differs from ready generation".to_string(),
                ));
            }
            return Ok(());
        }
        self.ensure_building(prepared)?;
        let journal = self.load_journal(&prepared.paths.building_dir)?;
        validate_journal(&journal, &prepared.manifest, &prepared.manifest_sha256)
    }

    fn ensure_building(&self, prepared: &PreparedGeneration) -> LayoutResult<()> {
        if !prepared.paths.building_dir.exists() {
            return Err(GenerationLayoutError::InvalidTransition(format!(
                "generation {} has no shadow build directory",
                prepared.manifest.generation_id
            )));
        }
        reject_symlink_or_non_directory(&prepared.paths.building_dir)
    }

    fn load_journal(&self, directory: &Path) -> LayoutResult<GenerationBuildJournal> {
        read_json(&directory.join(JOURNAL_FILE))
    }

    fn write_journal(
        &self,
        prepared: &PreparedGeneration,
        phase: GenerationBuildPhase,
        updated_at_ms: u64,
        last_error: Option<String>,
    ) -> LayoutResult<()> {
        let journal = journal_for(
            &prepared.manifest,
            &prepared.manifest_sha256,
            phase,
            updated_at_ms,
            last_error,
        );
        validate_journal(&journal, &prepared.manifest, &prepared.manifest_sha256)?;
        atomic_write_json(&prepared.paths.building_dir, JOURNAL_FILE, &journal)
    }

    fn load_ready_at(
        &self,
        directory: &Path,
        scope: &KnowledgeScope,
        generation_id: &str,
    ) -> LayoutResult<ReadyGeneration> {
        if !directory.exists() {
            return Err(GenerationLayoutError::NotReady(generation_id.to_string()));
        }
        reject_symlink_or_non_directory(directory)?;
        let manifest_bytes = read_bounded_file(&directory.join(MANIFEST_FILE), MAX_MANIFEST_BYTES)?;
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|source| GenerationLayoutError::Json {
                path: directory.join(MANIFEST_FILE),
                source,
            })?;
        manifest.validate()?;
        if manifest.scope() != *scope || manifest.generation_id != generation_id {
            return Err(GenerationLayoutError::CorruptState(
                "ready directory identity differs from requested generation".to_string(),
            ));
        }
        let manifest_sha256 = digest_bytes(&manifest_bytes);
        let stored_digest = read_text_file(&directory.join(MANIFEST_DIGEST_FILE))?;
        if stored_digest.trim_end() != manifest_sha256 {
            return Err(GenerationLayoutError::CorruptState(
                "ready manifest digest file does not match manifest bytes".to_string(),
            ));
        }
        let marker: ReadyGenerationMarker = read_json(&directory.join(READY_FILE))?;
        validate_ready_marker(&marker, &manifest, &manifest_sha256)?;
        verify_bundle_file(&directory.join(BUNDLE_FILE), &manifest.bundle)?;
        let evidence: MaterializationEvidence = read_json(&directory.join(MATERIALIZATION_FILE))?;
        validate_materialization_evidence(&evidence, &manifest, &manifest_sha256)?;
        validate_marker_evidence(&marker, &evidence)?;
        let journal: GenerationBuildJournal = read_json(&directory.join(JOURNAL_FILE))?;
        validate_journal(&journal, &manifest, &manifest_sha256)?;
        if journal.phase != GenerationBuildPhase::Ready {
            return Err(GenerationLayoutError::CorruptState(
                "ready directory journal is not ready".to_string(),
            ));
        }
        Ok(ReadyGeneration {
            manifest,
            marker,
            directory: directory.to_path_buf(),
        })
    }

    fn pointer_for(
        &self,
        scope: &KnowledgeScope,
        generation: &GenerationServingState,
    ) -> LayoutResult<GenerationPointer> {
        let ready = self.load_ready_at(
            &self
                .paths(scope, &generation.manifest.generation_id)
                .ready_dir,
            scope,
            &generation.manifest.generation_id,
        )?;
        if ready.marker.manifest_sha256 != generation.manifest_sha256
            || ready.marker.applied_sequence != generation.applied_sequence
        {
            return Err(GenerationLayoutError::Conflict(format!(
                "ready marker for {} does not match serving state",
                generation.manifest.generation_id
            )));
        }
        Ok(GenerationPointer {
            generation_id: generation.manifest.generation_id.clone(),
            manifest_sha256: generation.manifest_sha256.clone(),
            applied_sequence: generation.applied_sequence,
        })
    }

    fn validate_pointer_target(
        &self,
        scope: &KnowledgeScope,
        pointer: &GenerationPointer,
    ) -> LayoutResult<()> {
        validate_digest(&pointer.manifest_sha256)?;
        let ready = self.load_ready_at(
            &self.paths(scope, &pointer.generation_id).ready_dir,
            scope,
            &pointer.generation_id,
        )?;
        if ready.marker.manifest_sha256 != pointer.manifest_sha256
            || ready.marker.applied_sequence != pointer.applied_sequence
        {
            return Err(GenerationLayoutError::CorruptState(format!(
                "cached pointer for {} differs from ready marker",
                pointer.generation_id
            )));
        }
        Ok(())
    }
}

fn journal_for(
    manifest: &KnowledgeGenerationManifest,
    manifest_sha256: &str,
    phase: GenerationBuildPhase,
    updated_at_ms: u64,
    last_error: Option<String>,
) -> GenerationBuildJournal {
    GenerationBuildJournal {
        schema_version: GENERATION_LAYOUT_SCHEMA_VERSION,
        workspace_id: manifest.workspace_id.clone(),
        collection: manifest.collection.clone(),
        generation_id: manifest.generation_id.clone(),
        manifest_sha256: manifest_sha256.to_string(),
        phase,
        updated_at_ms,
        last_error,
    }
}

fn ready_marker(
    prepared: &PreparedGeneration,
    evidence: &MaterializationEvidence,
    ready_at_ms: u64,
) -> ReadyGenerationMarker {
    ReadyGenerationMarker {
        schema_version: GENERATION_LAYOUT_SCHEMA_VERSION,
        workspace_id: prepared.manifest.workspace_id.clone(),
        collection: prepared.manifest.collection.clone(),
        generation_id: prepared.manifest.generation_id.clone(),
        manifest_sha256: prepared.manifest_sha256.clone(),
        bundle_sha256: prepared.manifest.bundle.sha256.clone(),
        bundle_size_bytes: prepared.manifest.bundle.size_bytes,
        applied_sequence: evidence.applied_sequence,
        record_count: evidence.record_count,
        node_count: evidence.node_count,
        edge_count: evidence.edge_count,
        ready_at_ms,
    }
}

fn validate_journal(
    journal: &GenerationBuildJournal,
    manifest: &KnowledgeGenerationManifest,
    manifest_sha256: &str,
) -> LayoutResult<()> {
    if journal.schema_version != GENERATION_LAYOUT_SCHEMA_VERSION
        || journal.workspace_id != manifest.workspace_id
        || journal.collection != manifest.collection
        || journal.generation_id != manifest.generation_id
        || journal.manifest_sha256 != manifest_sha256
    {
        return Err(GenerationLayoutError::CorruptState(
            "build journal identity differs from generation manifest".to_string(),
        ));
    }
    validate_digest(&journal.manifest_sha256)?;
    validate_timestamp(journal.updated_at_ms)?;
    match (&journal.phase, &journal.last_error) {
        (GenerationBuildPhase::Failed, Some(error))
            if !error.trim().is_empty()
                && error.trim() == error
                && error.len() <= MAX_FAILURE_BYTES => {}
        (GenerationBuildPhase::Failed, _) => {
            return Err(GenerationLayoutError::CorruptState(
                "failed build journal lacks valid failure evidence".to_string(),
            ));
        }
        (_, None) => {}
        (_, Some(_)) => {
            return Err(GenerationLayoutError::CorruptState(
                "non-failed build journal contains failure evidence".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_evidence(
    evidence: &MaterializationEvidence,
    prepared: &PreparedGeneration,
) -> LayoutResult<()> {
    validate_materialization_evidence(evidence, &prepared.manifest, &prepared.manifest_sha256)
}

fn validate_materialization_evidence(
    evidence: &MaterializationEvidence,
    manifest: &KnowledgeGenerationManifest,
    manifest_sha256: &str,
) -> LayoutResult<()> {
    if evidence.schema_version != GENERATION_LAYOUT_SCHEMA_VERSION
        || evidence.generation_id != manifest.generation_id
        || evidence.manifest_sha256 != manifest_sha256
        || evidence.record_count != manifest.expected_vector_count
        || evidence.edge_count != manifest.expected_edge_count
        || evidence.applied_sequence != manifest.target_sequence
    {
        return Err(GenerationLayoutError::CorruptState(
            "materialization evidence differs from manifest or target checkpoint".to_string(),
        ));
    }
    validate_timestamp(evidence.verified_at_ms)?;
    Ok(())
}

fn evidence_matches_summary(
    evidence: &MaterializationEvidence,
    summary: &KnowledgeBundleSummary,
    applied_sequence: u64,
) -> bool {
    evidence.record_count == summary.record_count
        && evidence.node_count == summary.node_count
        && evidence.edge_count == summary.edge_count
        && evidence.decoded_bytes == summary.decoded_bytes
        && evidence.applied_sequence == applied_sequence
}

fn validate_ready_marker(
    marker: &ReadyGenerationMarker,
    manifest: &KnowledgeGenerationManifest,
    manifest_sha256: &str,
) -> LayoutResult<()> {
    if marker.schema_version != GENERATION_LAYOUT_SCHEMA_VERSION
        || marker.workspace_id != manifest.workspace_id
        || marker.collection != manifest.collection
        || marker.generation_id != manifest.generation_id
        || marker.manifest_sha256 != manifest_sha256
        || marker.bundle_sha256 != manifest.bundle.sha256
        || marker.bundle_size_bytes != manifest.bundle.size_bytes
        || marker.applied_sequence != manifest.target_sequence
        || marker.record_count != manifest.expected_vector_count
        || marker.edge_count != manifest.expected_edge_count
    {
        return Err(GenerationLayoutError::CorruptState(
            "ready marker differs from generation manifest".to_string(),
        ));
    }
    validate_timestamp(marker.ready_at_ms)?;
    Ok(())
}

fn validate_marker_evidence(
    marker: &ReadyGenerationMarker,
    evidence: &MaterializationEvidence,
) -> LayoutResult<()> {
    if marker.generation_id != evidence.generation_id
        || marker.manifest_sha256 != evidence.manifest_sha256
        || marker.applied_sequence != evidence.applied_sequence
        || marker.record_count != evidence.record_count
        || marker.node_count != evidence.node_count
        || marker.edge_count != evidence.edge_count
    {
        return Err(GenerationLayoutError::CorruptState(
            "ready marker differs from materialization evidence".to_string(),
        ));
    }
    Ok(())
}

fn validate_pointer_set(
    pointers: &GenerationPointerSet,
    scope: &KnowledgeScope,
) -> LayoutResult<()> {
    if pointers.schema_version != GENERATION_LAYOUT_SCHEMA_VERSION
        || pointers.workspace_id != scope.workspace_id
        || pointers.collection != scope.collection
    {
        return Err(GenerationLayoutError::CorruptState(
            "cached pointer-set identity differs from scope".to_string(),
        ));
    }
    if pointers.active.is_none() && pointers.previous.is_some() {
        return Err(GenerationLayoutError::CorruptState(
            "cached previous pointer exists without active pointer".to_string(),
        ));
    }
    if let (Some(active), Some(previous)) = (&pointers.active, &pointers.previous) {
        if active.generation_id == previous.generation_id {
            return Err(GenerationLayoutError::CorruptState(
                "cached active and previous pointers reference the same generation".to_string(),
            ));
        }
    }
    validate_timestamp(pointers.updated_at_ms)
}

fn validate_timestamp(value: u64) -> LayoutResult<()> {
    if value == 0 || value > akidb_contracts::MAX_SAFE_JSON_INTEGER {
        return Err(GenerationLayoutError::CorruptState(
            "timestamp must be a positive JSON-safe integer".to_string(),
        ));
    }
    Ok(())
}

fn validate_digest(value: &str) -> LayoutResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(GenerationLayoutError::InvalidDigest(value.to_string()));
    }
    Ok(())
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn verify_bundle_file(path: &Path, reference: &ImmutableObjectReference) -> LayoutResult<()> {
    reject_symlink_or_regular_file(path)?;
    let mut file = File::open(path).map_err(|source| GenerationLayoutError::Io {
        operation: "open bundle for verification",
        path: path.to_path_buf(),
        source,
    })?;
    verify_reader(&mut file, reference)
}

fn write_and_verify_bundle<R: Read>(
    path: &Path,
    reader: &mut R,
    reference: &ImmutableObjectReference,
) -> LayoutResult<()> {
    reject_symlink_if_present(path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| GenerationLayoutError::Io {
            operation: "create bundle partial",
            path: path.to_path_buf(),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| GenerationLayoutError::Io {
                operation: "read bundle stream",
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        observed = observed.saturating_add(read as u64);
        if observed > reference.size_bytes {
            return Err(GenerationLayoutError::BundleSizeMismatch {
                expected: reference.size_bytes,
                actual: observed,
            });
        }
        hasher.update(&buffer[..read]);
        file.write_all(&buffer[..read])
            .map_err(|source| GenerationLayoutError::Io {
                operation: "write bundle partial",
                path: path.to_path_buf(),
                source,
            })?;
    }
    if observed != reference.size_bytes {
        return Err(GenerationLayoutError::BundleSizeMismatch {
            expected: reference.size_bytes,
            actual: observed,
        });
    }
    let actual = format!("{:x}", hasher.finalize());
    if actual != reference.sha256 {
        return Err(GenerationLayoutError::BundleDigestMismatch {
            expected: reference.sha256.clone(),
            actual,
        });
    }
    file.sync_all().map_err(|source| GenerationLayoutError::Io {
        operation: "sync bundle partial",
        path: path.to_path_buf(),
        source,
    })
}

fn verify_reader<R: Read>(
    reader: &mut R,
    reference: &ImmutableObjectReference,
) -> LayoutResult<()> {
    let mut hasher = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| GenerationLayoutError::Io {
                operation: "read bundle for verification",
                path: PathBuf::from(BUNDLE_FILE),
                source,
            })?;
        if read == 0 {
            break;
        }
        observed = observed.saturating_add(read as u64);
        if observed > reference.size_bytes {
            return Err(GenerationLayoutError::BundleSizeMismatch {
                expected: reference.size_bytes,
                actual: observed,
            });
        }
        hasher.update(&buffer[..read]);
    }
    if observed != reference.size_bytes {
        return Err(GenerationLayoutError::BundleSizeMismatch {
            expected: reference.size_bytes,
            actual: observed,
        });
    }
    let actual = format!("{:x}", hasher.finalize());
    if actual != reference.sha256 {
        return Err(GenerationLayoutError::BundleDigestMismatch {
            expected: reference.sha256.clone(),
            actual,
        });
    }
    Ok(())
}

fn atomic_write_json<T: Serialize>(directory: &Path, name: &str, value: &T) -> LayoutResult<()> {
    let mut bytes = serde_json::to_vec(value).map_err(|source| GenerationLayoutError::Json {
        path: directory.join(name),
        source,
    })?;
    bytes.push(b'\n');
    atomic_write(directory, name, &bytes, true)
}

fn atomic_write(directory: &Path, name: &str, bytes: &[u8], replace: bool) -> LayoutResult<()> {
    reject_symlink_or_non_directory(directory)?;
    let destination = directory.join(name);
    reject_symlink_if_present(&destination)?;
    if !replace && destination.exists() {
        return Err(GenerationLayoutError::Conflict(format!(
            "{} already exists",
            destination.display()
        )));
    }
    let temporary = directory.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| GenerationLayoutError::Io {
            operation: "create atomic temporary file",
            path: temporary.clone(),
            source,
        })?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|source| GenerationLayoutError::Io {
            operation: "write atomic temporary file",
            path: temporary.clone(),
            source,
        })?;
    fs::rename(&temporary, &destination).map_err(|source| GenerationLayoutError::Io {
        operation: "replace atomic destination",
        path: destination,
        source,
    })?;
    sync_dir(directory)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> LayoutResult<T> {
    reject_symlink_or_regular_file(path)?;
    let bytes = read_bounded_file(path, MAX_MANIFEST_BYTES)?;
    serde_json::from_slice(&bytes).map_err(|source| GenerationLayoutError::Json {
        path: path.to_path_buf(),
        source,
    })
}

fn read_text_file(path: &Path) -> LayoutResult<String> {
    let bytes = read_bounded_file(path, 4096)?;
    String::from_utf8(bytes).map_err(|source| {
        GenerationLayoutError::CorruptState(format!(
            "{} contains invalid UTF-8: {source}",
            path.display()
        ))
    })
}

fn read_bounded_file(path: &Path, maximum: usize) -> LayoutResult<Vec<u8>> {
    reject_symlink_or_regular_file(path)?;
    let metadata = fs::metadata(path).map_err(|source| GenerationLayoutError::Io {
        operation: "read file metadata",
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.len() > maximum as u64 {
        return Err(GenerationLayoutError::CorruptState(format!(
            "{} exceeds the {maximum}-byte limit",
            path.display()
        )));
    }
    fs::read(path).map_err(|source| GenerationLayoutError::Io {
        operation: "read file",
        path: path.to_path_buf(),
        source,
    })
}

fn create_dir_all(path: &Path) -> LayoutResult<()> {
    fs::create_dir_all(path).map_err(|source| GenerationLayoutError::Io {
        operation: "create directory tree",
        path: path.to_path_buf(),
        source,
    })
}

fn create_dir(path: &Path) -> LayoutResult<()> {
    fs::create_dir(path).map_err(|source| GenerationLayoutError::Io {
        operation: "create directory",
        path: path.to_path_buf(),
        source,
    })
}

fn ensure_child_dir(parent: &Path, child: &Path) -> LayoutResult<()> {
    reject_symlink_or_non_directory(parent)?;
    if child.exists() {
        return reject_symlink_or_non_directory(child);
    }
    create_dir(child)?;
    reject_symlink_or_non_directory(child)?;
    sync_dir(parent)
}

fn reject_symlink_if_present(path: &Path) -> LayoutResult<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(GenerationLayoutError::SymbolicLink(path.to_path_buf()));
        }
    }
    Ok(())
}

fn reject_symlink_or_non_directory(path: &Path) -> LayoutResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| GenerationLayoutError::Io {
        operation: "inspect directory",
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(GenerationLayoutError::SymbolicLink(path.to_path_buf()));
    }
    if !metadata.is_dir() {
        return Err(GenerationLayoutError::NotDirectory(path.to_path_buf()));
    }
    Ok(())
}

fn reject_symlink_or_regular_file(path: &Path) -> LayoutResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| GenerationLayoutError::Io {
        operation: "inspect regular file",
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(GenerationLayoutError::SymbolicLink(path.to_path_buf()));
    }
    if !metadata.is_file() {
        return Err(GenerationLayoutError::NotRegularFile(path.to_path_buf()));
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> LayoutResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(GenerationLayoutError::SymbolicLink(path.to_path_buf()))
        }
        Ok(metadata) if metadata.is_file() => {
            fs::remove_file(path).map_err(|source| GenerationLayoutError::Io {
                operation: "remove failed partial file",
                path: path.to_path_buf(),
                source,
            })
        }
        Ok(_) => Err(GenerationLayoutError::NotRegularFile(path.to_path_buf())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(GenerationLayoutError::Io {
            operation: "inspect failed partial file",
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn sync_dir(path: &Path) -> LayoutResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| GenerationLayoutError::Io {
            operation: "sync directory",
            path: path.to_path_buf(),
            source,
        })
}

fn sync_tree(root: &Path) -> LayoutResult<()> {
    reject_symlink_or_non_directory(root)?;
    let mut pending = vec![root.to_path_buf()];
    let mut directories = Vec::new();
    while let Some(directory) = pending.pop() {
        reject_symlink_or_non_directory(&directory)?;
        directories.push(directory.clone());
        let entries = fs::read_dir(&directory).map_err(|source| GenerationLayoutError::Io {
            operation: "read generation directory",
            path: directory.clone(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| GenerationLayoutError::Io {
                operation: "read generation directory entry",
                path: directory.clone(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|source| GenerationLayoutError::Io {
                    operation: "inspect generation entry",
                    path: path.clone(),
                    source,
                })?;
            if file_type.is_symlink() {
                return Err(GenerationLayoutError::SymbolicLink(path));
            }
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file() {
                File::open(&path)
                    .and_then(|file| file.sync_all())
                    .map_err(|source| GenerationLayoutError::Io {
                        operation: "sync generation file",
                        path,
                        source,
                    })?;
            } else {
                return Err(GenerationLayoutError::NotRegularFile(path));
            }
        }
    }
    for directory in directories.into_iter().rev() {
        sync_dir(&directory)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{consume_knowledge_bundle, GenerationServingState};
    use akidb_contracts::KnowledgeGenerationManifest;
    use tempfile::tempdir;

    const BUNDLE: &[u8] =
        include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle.ndjson");
    const MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");

    fn fixture() -> (Vec<u8>, KnowledgeGenerationManifest, String) {
        let bytes = MANIFEST.as_bytes().to_vec();
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(&bytes).unwrap();
        let digest = digest_bytes(&bytes);
        (bytes, manifest, digest)
    }

    fn prepare_store() -> (
        tempfile::TempDir,
        GenerationStore,
        PreparedGeneration,
        KnowledgeGenerationManifest,
    ) {
        let temporary = tempdir().unwrap();
        let store = GenerationStore::open(temporary.path()).unwrap();
        let (bytes, manifest, digest) = fixture();
        let prepared = store.prepare(&bytes, &digest, 1).unwrap();
        (temporary, store, prepared, manifest)
    }

    fn complete(
        store: &GenerationStore,
        prepared: &PreparedGeneration,
        manifest: &KnowledgeGenerationManifest,
    ) -> ReadyGeneration {
        store.install_bundle(prepared, BUNDLE, 2).unwrap();
        store.mark_materializing(prepared, 3).unwrap();
        let summary = consume_knowledge_bundle(BUNDLE, manifest, |_| Ok(())).unwrap();
        store
            .record_materialization(prepared, &summary, manifest.target_sequence, 4)
            .unwrap();
        store.finalize_ready(prepared, 5).unwrap()
    }

    #[test]
    fn prepares_hashed_shadow_paths_without_exposing_ids() {
        let (_temporary, store, prepared, manifest) = prepare_store();
        assert_eq!(prepared.outcome(), GenerationPrepareOutcome::Started);
        let path = prepared.building_dir().to_string_lossy();
        assert!(!path.contains(&manifest.workspace_id));
        assert!(!path.contains(&manifest.collection));
        assert!(!path.contains(&manifest.generation_id));
        assert!(path.ends_with(".building"));
        assert!(!prepared.ready_dir().exists());
        assert!(store.root().is_absolute());
    }

    #[test]
    fn exact_build_resumes_but_changed_manifest_conflicts() {
        let (temporary, store, prepared, mut manifest) = prepare_store();
        store.install_bundle(&prepared, BUNDLE, 2).unwrap();
        drop(store);

        let reopened = GenerationStore::open(temporary.path()).unwrap();
        let (bytes, _, digest) = fixture();
        let resumed = reopened.prepare(&bytes, &digest, 3).unwrap();
        assert_eq!(resumed.outcome(), GenerationPrepareOutcome::Resumed);
        assert_eq!(
            reopened.load_journal(resumed.building_dir()).unwrap().phase,
            GenerationBuildPhase::BundleVerified
        );

        manifest.created_at_ms += 1;
        let changed = serde_json::to_vec(&manifest).unwrap();
        let changed_digest = digest_bytes(&changed);
        assert!(matches!(
            reopened.prepare(&changed, &changed_digest, 4),
            Err(GenerationLayoutError::Conflict(_))
        ));
    }

    #[test]
    fn corrupt_or_truncated_bundle_never_installs_or_becomes_ready() {
        let (_temporary, store, prepared, _) = prepare_store();
        assert!(matches!(
            store.install_bundle(&prepared, &BUNDLE[..BUNDLE.len() - 1], 2),
            Err(GenerationLayoutError::BundleSizeMismatch { .. })
        ));
        assert!(!prepared.building_dir().join(BUNDLE_FILE).exists());
        assert!(matches!(
            store.finalize_ready(&prepared, 3),
            Err(GenerationLayoutError::InvalidTransition(_))
        ));

        let mut corrupt = BUNDLE.to_vec();
        corrupt[0] ^= 1;
        assert!(matches!(
            store.install_bundle(&prepared, corrupt.as_slice(), 4),
            Err(GenerationLayoutError::BundleDigestMismatch { .. })
        ));
        assert!(!prepared.ready_dir().exists());
    }

    #[test]
    fn verified_build_is_atomically_sealed_and_survives_restart() {
        let (temporary, store, prepared, manifest) = prepare_store();
        let ready = complete(&store, &prepared, &manifest);
        assert!(!prepared.building_dir().exists());
        assert_eq!(ready.marker.applied_sequence, manifest.target_sequence);
        assert_eq!(ready.marker.record_count, manifest.expected_vector_count);

        let (bytes, _, digest) = fixture();
        let same = store.prepare(&bytes, &digest, 6).unwrap();
        assert_eq!(same.outcome(), GenerationPrepareOutcome::AlreadyReady);
        assert_eq!(
            store.install_bundle(&same, BUNDLE, 7).unwrap(),
            BundleInstallOutcome::AlreadyVerified
        );

        drop(store);
        let reopened = GenerationStore::open(temporary.path()).unwrap();
        let recovered = reopened
            .load_ready(&manifest.scope(), &manifest.generation_id)
            .unwrap();
        assert_eq!(recovered.marker, ready.marker);
    }

    #[test]
    fn verified_materialization_retry_preserves_original_evidence() {
        let (_temporary, store, prepared, manifest) = prepare_store();
        store.install_bundle(&prepared, BUNDLE, 2).unwrap();
        store.mark_materializing(&prepared, 3).unwrap();
        let summary = consume_knowledge_bundle(BUNDLE, &manifest, |_| Ok(())).unwrap();
        let original = store
            .record_materialization(&prepared, &summary, manifest.target_sequence, 4)
            .unwrap();
        let retried = store
            .record_materialization(&prepared, &summary, manifest.target_sequence, 99)
            .unwrap();
        assert_eq!(retried, original);
        assert_eq!(retried.verified_at_ms, 4);
    }

    #[test]
    fn ready_journal_before_directory_rename_is_recovered() {
        let (temporary, store, prepared, manifest) = prepare_store();
        store.install_bundle(&prepared, BUNDLE, 2).unwrap();
        store.mark_materializing(&prepared, 3).unwrap();
        let summary = consume_knowledge_bundle(BUNDLE, &manifest, |_| Ok(())).unwrap();
        let evidence = store
            .record_materialization(&prepared, &summary, manifest.target_sequence, 4)
            .unwrap();
        let marker = ready_marker(&prepared, &evidence, 5);
        atomic_write_json(prepared.building_dir(), READY_FILE, &marker).unwrap();
        store
            .write_journal(&prepared, GenerationBuildPhase::Ready, 5, None)
            .unwrap();
        drop(store);

        let reopened = GenerationStore::open(temporary.path()).unwrap();
        let recovered = reopened.finalize_ready(&prepared, 6).unwrap();
        assert_eq!(recovered.marker, marker);
        assert!(!prepared.building_dir().exists());
        assert!(prepared.ready_dir().exists());
    }

    #[test]
    fn ready_load_rejects_bundle_corruption() {
        let (_temporary, store, prepared, manifest) = prepare_store();
        let ready = complete(&store, &prepared, &manifest);
        let mut corrupt = BUNDLE.to_vec();
        corrupt[0] ^= 1;
        fs::write(ready.directory.join(BUNDLE_FILE), corrupt).unwrap();

        assert!(matches!(
            store.load_ready(&manifest.scope(), &manifest.generation_id),
            Err(GenerationLayoutError::BundleDigestMismatch { .. })
        ));
    }

    #[test]
    fn failed_build_keeps_evidence_and_cannot_be_sealed() {
        let (_temporary, store, prepared, _) = prepare_store();
        store
            .fail_build(&prepared, "injected vector build failure", 2)
            .unwrap();
        let journal = store.load_journal(prepared.building_dir()).unwrap();
        assert_eq!(journal.phase, GenerationBuildPhase::Failed);
        assert_eq!(
            journal.last_error.as_deref(),
            Some("injected vector build failure")
        );
        assert!(matches!(
            store.install_bundle(&prepared, BUNDLE, 3),
            Err(GenerationLayoutError::InvalidTransition(_))
        ));
        assert!(!prepared.ready_dir().exists());
    }

    #[test]
    fn reinstall_does_not_clear_a_failed_build_journal() {
        let (_temporary, store, prepared, _) = prepare_store();
        store.install_bundle(&prepared, BUNDLE, 2).unwrap();
        store
            .fail_build(&prepared, "injected index failure", 3)
            .unwrap();
        assert_eq!(
            store.install_bundle(&prepared, BUNDLE, 4).unwrap(),
            BundleInstallOutcome::AlreadyVerified
        );
        let journal = store.load_journal(prepared.building_dir()).unwrap();
        assert_eq!(journal.phase, GenerationBuildPhase::Failed);
        assert_eq!(
            journal.last_error.as_deref(),
            Some("injected index failure")
        );
    }

    #[test]
    fn pointer_set_is_atomic_recoverable_cache_of_ready_state() {
        let (temporary, store, prepared, manifest) = prepare_store();
        complete(&store, &prepared, &manifest);
        let generation = GenerationServingState {
            manifest: manifest.clone(),
            manifest_sha256: prepared.manifest_sha256().to_string(),
            applied_sequence: manifest.target_sequence,
            state: LocalGenerationState::Serving,
            last_error: None,
        };
        let record = ServingStateRecord {
            schema_version: SERVING_STATE_SCHEMA_VERSION,
            replica_id: "replica-a".to_string(),
            workspace_id: manifest.workspace_id.clone(),
            collection: manifest.collection.clone(),
            active: Some(generation),
            previous: None,
            staged: None,
            updated_at_ms: 10,
        };
        let written = store.reconcile_pointer_set(&record).unwrap();
        assert_eq!(
            written.active.as_ref().unwrap().generation_id,
            manifest.generation_id
        );

        drop(store);
        let reopened = GenerationStore::open(temporary.path()).unwrap();
        let loaded = reopened
            .load_pointer_set(&manifest.scope())
            .unwrap()
            .unwrap();
        assert_eq!(loaded, written);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_scope_component_is_rejected() {
        use std::os::unix::fs::symlink;

        let temporary = tempdir().unwrap();
        let store = GenerationStore::open(temporary.path()).unwrap();
        let outside = tempdir().unwrap();
        symlink(outside.path(), temporary.path().join("scopes")).unwrap();
        let (bytes, _, digest) = fixture();
        assert!(matches!(
            store.prepare(&bytes, &digest, 1),
            Err(GenerationLayoutError::SymbolicLink(_))
        ));
    }
}
