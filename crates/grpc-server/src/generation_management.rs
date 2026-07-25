//! Authenticated control API for one-node immutable generation publication.
//!
//! This service is deliberately separate from the read/plan-only Operations
//! Console API. Phase 2 waits for local materialization synchronously and does
//! not claim multi-replica authority or HA.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use akidb_contracts::{KnowledgeGenerationManifest, KnowledgeScope};
use akidb_storage::{
    GenerationServingState, LocalGenerationState, ServingStateError, ServingStateRecord,
    SERVING_STATE_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::auth::{auth_context, AuthContext};
use crate::generation_fetch::{GenerationBundleFetcher, GenerationFetchError};
use crate::proto::active_generation_precondition::Condition;
use crate::proto::generation_management_server::GenerationManagement;
use crate::proto::{
    ActivateGenerationRequest, ActiveGenerationPrecondition, GenerationReplicaStatus,
    GetGenerationStatusRequest, LocalGenerationInfo, RollbackGenerationRequest,
    StageGenerationRequest,
};
use crate::{
    ExpectedActiveGeneration, GenerationControlError, GenerationDataPlane,
    GenerationMaterializerError,
};

const MAX_MANIFEST_REQUEST_BYTES: usize = 1024 * 1024;

pub struct GenerationManagementServiceImpl {
    data_plane: Arc<GenerationDataPlane>,
    fetcher: Arc<dyn GenerationBundleFetcher>,
}

impl GenerationManagementServiceImpl {
    pub fn new(
        data_plane: Arc<GenerationDataPlane>,
        fetcher: Arc<dyn GenerationBundleFetcher>,
    ) -> Self {
        Self {
            data_plane,
            fetcher,
        }
    }

    fn controller(&self) -> &Arc<crate::GenerationController> {
        self.data_plane.controller()
    }
}

#[tonic::async_trait]
impl GenerationManagement for GenerationManagementServiceImpl {
    async fn stage_generation(
        &self,
        request: Request<StageGenerationRequest>,
    ) -> Result<Response<GenerationReplicaStatus>, Status> {
        let ctx = require_authenticated(&request)?;
        let req = request.into_inner();
        if req.manifest_json.is_empty() {
            return Err(Status::invalid_argument("manifest_json is required"));
        }
        if req.manifest_json.len() > MAX_MANIFEST_REQUEST_BYTES {
            return Err(Status::invalid_argument(
                "manifest_json exceeds the one-megabyte request limit",
            ));
        }
        validate_manifest_digest(&req.manifest_json, &req.manifest_sha256)?;
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(&req.manifest_json)
            .map_err(|_| Status::invalid_argument("manifest_json is not valid contract JSON"))?;
        manifest
            .validate()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        authorize_scope(&ctx, &manifest.scope())?;

        let fetched = self
            .fetcher
            .fetch(&manifest.bundle)
            .await
            .map_err(fetch_status)?;
        let controller = self.controller().clone();
        let manifest_bytes = req.manifest_json;
        let manifest_sha256 = req.manifest_sha256;
        let generation_id = manifest.generation_id.clone();
        let scope = manifest.scope();
        let updated_at_ms = now_ms()?;
        let publication = tokio::task::spawn_blocking(move || {
            let file = fetched.open()?;
            controller
                .publish_from_reader(&manifest_bytes, &manifest_sha256, file, updated_at_ms)
                .map_err(PublicationTaskError::Control)
        })
        .await
        .map_err(|error| {
            warn!(%error, "generation publication task failed to join");
            Status::internal("generation publication task failed")
        })?
        .map_err(publication_task_status)?;

        self.data_plane
            .prepare_generation(&scope, &generation_id)
            .map_err(control_status)?;
        info!(
            workspace_id = %scope.workspace_id,
            collection = %scope.collection,
            generation_id = %generation_id,
            actor_id = ?ctx.agent_id,
            "generation staged and locally ready"
        );
        Ok(Response::new(status_to_proto(publication.state)))
    }

    async fn get_generation_status(
        &self,
        request: Request<GetGenerationStatusRequest>,
    ) -> Result<Response<GenerationReplicaStatus>, Status> {
        let ctx = require_authenticated(&request)?;
        let req = request.into_inner();
        let scope = KnowledgeScope::new(req.workspace_id, req.collection);
        authorize_scope(&ctx, &scope)?;
        let record = self
            .controller()
            .status(&scope)
            .map_err(control_status)?
            .ok_or_else(|| Status::not_found("generation serving state not found"))?;
        Ok(Response::new(status_to_proto(record)))
    }

    async fn activate_generation(
        &self,
        request: Request<ActivateGenerationRequest>,
    ) -> Result<Response<GenerationReplicaStatus>, Status> {
        let ctx = require_authenticated(&request)?;
        let req = request.into_inner();
        let scope = KnowledgeScope::new(req.workspace_id, req.collection);
        authorize_scope(&ctx, &scope)?;
        validate_generation_id(&req.generation_id)?;
        let expected = parse_precondition(req.expected_active)?;
        let record = self
            .data_plane
            .activate(&scope, &req.generation_id, &expected, now_ms()?)
            .map_err(control_status)?;
        info!(
            workspace_id = %scope.workspace_id,
            collection = %scope.collection,
            generation_id = %req.generation_id,
            actor_id = ?ctx.agent_id,
            "generation activated locally"
        );
        Ok(Response::new(status_to_proto(record)))
    }

    async fn rollback_generation(
        &self,
        request: Request<RollbackGenerationRequest>,
    ) -> Result<Response<GenerationReplicaStatus>, Status> {
        let ctx = require_authenticated(&request)?;
        let req = request.into_inner();
        let scope = KnowledgeScope::new(req.workspace_id, req.collection);
        authorize_scope(&ctx, &scope)?;
        validate_generation_id(&req.target_generation_id)?;
        let expected = parse_precondition(req.expected_active)?;
        let record = self
            .data_plane
            .rollback(&scope, &req.target_generation_id, &expected, now_ms()?)
            .map_err(control_status)?;
        info!(
            workspace_id = %scope.workspace_id,
            collection = %scope.collection,
            generation_id = %req.target_generation_id,
            actor_id = ?ctx.agent_id,
            "generation rolled back locally"
        );
        Ok(Response::new(status_to_proto(record)))
    }
}

#[derive(Debug, thiserror::Error)]
enum PublicationTaskError {
    #[error(transparent)]
    Fetch(#[from] GenerationFetchError),
    #[error(transparent)]
    Control(GenerationControlError),
}

fn require_authenticated<T>(request: &Request<T>) -> Result<AuthContext, Status> {
    let ctx = auth_context(request);
    if !ctx.authenticated {
        return Err(Status::unauthenticated(
            "authenticated control-plane identity is required",
        ));
    }
    Ok(ctx)
}

fn authorize_scope(ctx: &AuthContext, scope: &KnowledgeScope) -> Result<(), Status> {
    scope
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if scope.workspace_id != ctx.workspace_id {
        return Err(Status::permission_denied("workspace is not authorized"));
    }
    Ok(())
}

fn validate_generation_id(generation_id: &str) -> Result<(), Status> {
    if generation_id.trim().is_empty()
        || generation_id.trim() != generation_id
        || generation_id.len() > 1024
        || generation_id.chars().any(char::is_control)
    {
        return Err(Status::invalid_argument("invalid generation_id"));
    }
    Ok(())
}

fn validate_manifest_digest(manifest_json: &[u8], expected: &str) -> Result<(), Status> {
    if expected.len() != 64
        || !expected
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Status::invalid_argument(
            "manifest_sha256 must be 64 lowercase hexadecimal characters",
        ));
    }
    let actual = format!("{:x}", Sha256::digest(manifest_json));
    if actual != expected {
        return Err(Status::invalid_argument(
            "manifest_sha256 does not match manifest_json",
        ));
    }
    Ok(())
}

fn parse_precondition(
    precondition: Option<ActiveGenerationPrecondition>,
) -> Result<ExpectedActiveGeneration, Status> {
    match precondition.and_then(|value| value.condition) {
        Some(Condition::NoActive(true)) => Ok(ExpectedActiveGeneration::NoActive),
        Some(Condition::NoActive(false)) => Err(Status::invalid_argument(
            "expected_active.no_active must be true",
        )),
        Some(Condition::GenerationId(generation_id)) => {
            validate_generation_id(&generation_id)?;
            Ok(ExpectedActiveGeneration::Generation(generation_id))
        }
        None => Err(Status::invalid_argument(
            "expected_active compare-and-swap condition is required",
        )),
    }
}

fn status_to_proto(record: ServingStateRecord) -> GenerationReplicaStatus {
    GenerationReplicaStatus {
        schema_version: SERVING_STATE_SCHEMA_VERSION,
        replica_id: record.replica_id,
        workspace_id: record.workspace_id,
        collection: record.collection,
        active: record.active.map(generation_to_proto),
        previous: record.previous.map(generation_to_proto),
        staged: record.staged.map(generation_to_proto),
        updated_at_ms: record.updated_at_ms,
    }
}

fn generation_to_proto(generation: GenerationServingState) -> LocalGenerationInfo {
    LocalGenerationInfo {
        generation_id: generation.manifest.generation_id,
        manifest_sha256: generation.manifest_sha256,
        bundle_sha256: generation.manifest.bundle.sha256,
        base_sequence: generation.manifest.base_sequence,
        target_sequence: generation.manifest.target_sequence,
        applied_sequence: generation.applied_sequence,
        state: local_state_name(generation.state).to_string(),
        last_error: generation.last_error,
    }
}

fn local_state_name(state: LocalGenerationState) -> &'static str {
    match state {
        LocalGenerationState::Staged => "staged",
        LocalGenerationState::CatchingUp => "catching_up",
        LocalGenerationState::Ready => "ready",
        LocalGenerationState::Serving => "serving",
        LocalGenerationState::Failed => "failed",
    }
}

fn fetch_status(error: GenerationFetchError) -> Status {
    match error {
        GenerationFetchError::Unauthorized(message) => Status::permission_denied(message),
        GenerationFetchError::Unavailable(message) => Status::unavailable(message),
        GenerationFetchError::Transport(message) => {
            warn!(error = %message, "generation object fetch failed");
            Status::unavailable("generation object fetch failed")
        }
        GenerationFetchError::Io(error) => {
            warn!(%error, "generation object temporary-file operation failed");
            Status::unavailable("generation object fetch failed")
        }
        GenerationFetchError::Rejected(message) => Status::failed_precondition(message),
    }
}

fn publication_task_status(error: PublicationTaskError) -> Status {
    match error {
        PublicationTaskError::Fetch(error) => fetch_status(error),
        PublicationTaskError::Control(error) => control_status(error),
    }
}

fn control_status(error: GenerationControlError) -> Status {
    match error {
        GenerationControlError::ActiveGenerationConflict { .. } => {
            Status::aborted(error.to_string())
        }
        GenerationControlError::Layout(
            akidb_storage::GenerationLayoutError::ManifestDigestMismatch { .. }
            | akidb_storage::GenerationLayoutError::InvalidDigest(_)
            | akidb_storage::GenerationLayoutError::ManifestTooLarge { .. }
            | akidb_storage::GenerationLayoutError::Contract(_),
        ) => Status::invalid_argument(error.to_string()),
        GenerationControlError::State(ServingStateError::StateNotFound { .. }) => {
            Status::not_found(error.to_string())
        }
        GenerationControlError::Materializer(
            GenerationMaterializerError::Bundle(_) | GenerationMaterializerError::Rejected(_),
        ) => Status::failed_precondition(error.to_string()),
        GenerationControlError::Materializer(GenerationMaterializerError::Layout(
            akidb_storage::GenerationLayoutError::BundleDigestMismatch { .. }
            | akidb_storage::GenerationLayoutError::BundleSizeMismatch { .. },
        )) => Status::data_loss(error.to_string()),
        GenerationControlError::InconsistentState(_) => {
            Status::failed_precondition(error.to_string())
        }
        other => {
            warn!(error = %other, "generation control operation failed");
            Status::internal("generation control operation failed")
        }
    }
}

fn now_ms() -> Result<u64, Status> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before the Unix epoch"))?
        .as_millis();
    u64::try_from(milliseconds)
        .map_err(|_| Status::internal("system clock cannot be represented in milliseconds"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_storage::{GenerationStore, RocksDbBackend, ServingStateStore};
    use async_trait::async_trait;
    use tempfile::TempDir;

    const BUNDLE: &[u8] =
        include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle.ndjson");
    const MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");

    struct FixtureFetcher {
        directory: TempDir,
        bundle: Vec<u8>,
    }

    #[async_trait]
    impl GenerationBundleFetcher for FixtureFetcher {
        async fn fetch(
            &self,
            _reference: &akidb_contracts::ImmutableObjectReference,
        ) -> Result<crate::FetchedGenerationBundle, GenerationFetchError> {
            let path = self
                .directory
                .path()
                .join(format!("{}.bundle", uuid::Uuid::new_v4()));
            std::fs::write(&path, &self.bundle)?;
            crate::FetchedGenerationBundle::temporary(path)
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn service_with_bundle(
        bundle: Vec<u8>,
    ) -> (
        tempfile::TempDir,
        GenerationManagementServiceImpl,
        KnowledgeGenerationManifest,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let generation_store =
            Arc::new(GenerationStore::open(temporary.path().join("generations")).unwrap());
        let materializer = Arc::new(crate::GenerationMaterializer::new(
            generation_store,
            Default::default(),
        ));
        let control = Arc::new(RocksDbBackend::open(temporary.path().join("control")).unwrap());
        let state = Arc::new(ServingStateStore::new(control, "replica-management").unwrap());
        let controller = Arc::new(crate::GenerationController::new(materializer, state));
        let data_plane = Arc::new(
            GenerationDataPlane::new(
                controller,
                crate::GenerationDataPlaneConfig {
                    default_collection: "knowledge".to_string(),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let fetcher = Arc::new(FixtureFetcher {
            directory: tempfile::tempdir().unwrap(),
            bundle,
        });
        let manifest = serde_json::from_str(MANIFEST).unwrap();
        (
            temporary,
            GenerationManagementServiceImpl::new(data_plane, fetcher),
            manifest,
        )
    }

    fn authenticated<T>(message: T, workspace_id: &str) -> Request<T> {
        let mut request = Request::new(message);
        request.extensions_mut().insert(AuthContext {
            workspace_id: workspace_id.to_string(),
            agent_id: Some("publisher".to_string()),
            authenticated: true,
        });
        request
    }

    #[tokio::test]
    async fn stage_activate_and_status_require_explicit_cas() {
        let (_temporary, service, manifest) = service_with_bundle(BUNDLE.to_vec());
        let manifest_bytes = MANIFEST.as_bytes().to_vec();
        let staged = service
            .stage_generation(authenticated(
                StageGenerationRequest {
                    manifest_json: manifest_bytes.clone(),
                    manifest_sha256: digest(&manifest_bytes),
                },
                &manifest.workspace_id,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(staged.staged.unwrap().state, "ready");
        assert!(staged.active.is_none());

        let error = service
            .activate_generation(authenticated(
                ActivateGenerationRequest {
                    workspace_id: manifest.workspace_id.clone(),
                    collection: manifest.collection.clone(),
                    generation_id: manifest.generation_id.clone(),
                    expected_active: None,
                },
                &manifest.workspace_id,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        let active = service
            .activate_generation(authenticated(
                ActivateGenerationRequest {
                    workspace_id: manifest.workspace_id.clone(),
                    collection: manifest.collection.clone(),
                    generation_id: manifest.generation_id.clone(),
                    expected_active: Some(ActiveGenerationPrecondition {
                        condition: Some(Condition::NoActive(true)),
                    }),
                },
                &manifest.workspace_id,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(active.active.unwrap().state, "serving");
        assert!(active.staged.is_none());

        let status = service
            .get_generation_status(authenticated(
                GetGenerationStatusRequest {
                    workspace_id: manifest.workspace_id.clone(),
                    collection: manifest.collection.clone(),
                },
                &manifest.workspace_id,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.active.unwrap().generation_id, manifest.generation_id);
    }

    #[tokio::test]
    async fn unauthenticated_and_cross_workspace_publication_are_denied() {
        let (_temporary, service, _manifest) = service_with_bundle(BUNDLE.to_vec());
        let manifest_bytes = MANIFEST.as_bytes().to_vec();
        let error = service
            .stage_generation(Request::new(StageGenerationRequest {
                manifest_json: manifest_bytes.clone(),
                manifest_sha256: digest(&manifest_bytes),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        let error = service
            .stage_generation(authenticated(
                StageGenerationRequest {
                    manifest_json: manifest_bytes.clone(),
                    manifest_sha256: digest(&manifest_bytes),
                },
                "other-workspace",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn corrupt_fetched_object_never_becomes_ready() {
        let (_temporary, service, manifest) = service_with_bundle(b"corrupt".to_vec());
        let manifest_bytes = MANIFEST.as_bytes().to_vec();
        let error = service
            .stage_generation(authenticated(
                StageGenerationRequest {
                    manifest_json: manifest_bytes.clone(),
                    manifest_sha256: digest(&manifest_bytes),
                },
                &manifest.workspace_id,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }
}
