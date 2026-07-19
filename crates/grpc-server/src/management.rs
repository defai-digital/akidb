//! Read/plan-only management API used by the Operations Console.
//!
//! This module intentionally contains no import execution, export, identity
//! mutation, snapshot mutation, restore, collection deletion, task cancel, or
//! arbitrary storage access methods. Ratatui and CLI clients are consumers of
//! this server-enforced boundary, not authorization boundaries themselves.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use akidb_common::config::ImportPlanConfig;
use akidb_common::scheduler::{TaskExecution, TaskState};
use akidb_storage::{SnapshotManager, SnapshotMetadata};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};
use tracing::warn;
use uuid::Uuid;

use crate::admin::AdminState;
use crate::auth::{auth_context, AuthContext};
use crate::collections::SharedCollectionRegistry;
use crate::proto::management_service_server::ManagementService;
use crate::proto::{
    AuditEvent, GetManagementCapabilitiesRequest, GetManagementCapabilitiesResponse,
    GetManagementOperationRequest, GetSnapshotRequest, ListAuditEventsRequest,
    ListAuditEventsResponse, ListManagementOperationsRequest, ListManagementOperationsResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ManagementCapability, ManagementOperation,
    ManagementOperationState, ManagementOperationType, ManagementPlan, OperationProblem,
    PlanFinding, PlanImportRequest, PlanSeverity, RestoreTestState, SnapshotOperationalInfo,
    StagedObjectRef, VerificationState,
};

const MANAGEMENT_API_VERSION: u32 = 1;
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 200;
const MAX_SAFE_TEXT: usize = 256;

/// Trusted metadata for an immutable object registered by a staging service.
///
/// The public management RPC cannot create these records. A future upload
/// gateway adapter must register them after its own authentication, size, type,
/// and immutability checks.
#[derive(Debug, Clone)]
pub struct StagedObject {
    pub reference: StagedObjectRef,
    pub workspace_id: String,
    pub format: String,
    pub estimated_expanded_bytes: Option<u64>,
    pub estimated_documents: Option<u64>,
    pub estimated_chunks: Option<u64>,
    pub estimated_vectors: Option<u64>,
    pub vector_dimensions: Option<u32>,
}

/// In-process adapter point for trusted staging metadata.
#[derive(Debug, Default)]
pub struct StagingRegistry {
    objects: RwLock<HashMap<String, StagedObject>>,
}

impl StagingRegistry {
    pub fn register(&self, object: StagedObject) {
        self.objects
            .write()
            .insert(object.reference.staging_id.clone(), object);
    }

    fn resolve(&self, staging_id: &str) -> Option<StagedObject> {
        self.objects.read().get(staging_id).cloned()
    }
}

/// Shared state for the safe management surface.
pub struct ManagementState {
    admin: Arc<AdminState>,
    collections: SharedCollectionRegistry,
    snapshots: Option<Arc<SnapshotManager>>,
    staging: Arc<StagingRegistry>,
    import_plan: ImportPlanConfig,
    audit_events: RwLock<VecDeque<AuditEvent>>,
    audit_max_entries: usize,
    server_version: String,
    auth_mode: String,
    tls_active: bool,
}

impl ManagementState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        admin: Arc<AdminState>,
        collections: SharedCollectionRegistry,
        snapshots: Option<Arc<SnapshotManager>>,
        staging: Arc<StagingRegistry>,
        import_plan: ImportPlanConfig,
        audit_max_entries: usize,
        server_version: impl Into<String>,
        auth_mode: impl Into<String>,
        tls_active: bool,
    ) -> Self {
        Self {
            admin,
            collections,
            snapshots,
            staging,
            import_plan,
            audit_events: RwLock::new(VecDeque::new()),
            audit_max_entries: audit_max_entries.max(1),
            server_version: server_version.into(),
            auth_mode: auth_mode.into(),
            tls_active,
        }
    }

    pub fn staging_registry(&self) -> Arc<StagingRegistry> {
        self.staging.clone()
    }

    fn audit(
        &self,
        ctx: &AuthContext,
        action: &str,
        target_type: &str,
        target_id: &str,
        outcome: &str,
        reason_code: &str,
    ) {
        let event = AuditEvent {
            event_id: Uuid::new_v4().to_string(),
            occurred_at_ms: now_ms(),
            actor_id: ctx
                .agent_id
                .as_deref()
                .map(sanitize_text)
                .unwrap_or_else(|| "local-operator".to_string()),
            workspace_id: sanitize_text(&ctx.workspace_id),
            action: sanitize_text(action),
            target_type: sanitize_text(target_type),
            target_id: sanitize_identifier(target_id),
            outcome: sanitize_text(outcome),
            reason_code: sanitize_text(reason_code),
            request_id: Uuid::new_v4().to_string(),
            source: "grpc".to_string(),
        };

        let mut events = self.audit_events.write();
        events.push_front(event);
        while events.len() > self.audit_max_entries {
            events.pop_back();
        }
    }
}

/// gRPC implementation containing only v1 read and validation operations.
pub struct ManagementServiceImpl {
    state: Arc<ManagementState>,
}

impl ManagementServiceImpl {
    pub fn new(state: Arc<ManagementState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl ManagementService for ManagementServiceImpl {
    async fn get_management_capabilities(
        &self,
        request: Request<GetManagementCapabilitiesRequest>,
    ) -> Result<Response<GetManagementCapabilitiesResponse>, Status> {
        let ctx = auth_context(&request);
        self.state.audit(
            &ctx,
            "management.capabilities.read",
            "server",
            "self",
            "succeeded",
            "OK",
        );

        let import_supported = self.state.import_plan.enabled;
        let snapshot_supported = self.state.snapshots.is_some();
        let capabilities = vec![
            capability("cluster.read", true, ""),
            capability("collections.read", true, ""),
            capability("operations.read", true, ""),
            capability(
                "snapshots.read",
                snapshot_supported,
                "snapshot backend is not configured",
            ),
            capability(
                "data.import.plan",
                import_supported,
                "trusted staging resolver is not configured",
            ),
            capability("audit.read", true, ""),
            capability(
                "principals.read",
                false,
                "identity inventory API is not available",
            ),
            capability("diagnostics.read", true, ""),
        ];

        Ok(Response::new(GetManagementCapabilitiesResponse {
            server_version: self.state.server_version.clone(),
            management_api_version: MANAGEMENT_API_VERSION,
            capabilities,
            workspace_id: ctx.workspace_id,
            agent_id: ctx.agent_id,
            authenticated: ctx.authenticated,
            tls_active: self.state.tls_active,
            auth_mode: self.state.auth_mode.clone(),
        }))
    }

    async fn list_management_operations(
        &self,
        request: Request<ListManagementOperationsRequest>,
    ) -> Result<Response<ListManagementOperationsResponse>, Status> {
        let ctx = auth_context(&request);
        let req = request.into_inner();
        validate_operation_filters(&req)?;
        let mut operations = collect_operations(&self.state.admin, &ctx.workspace_id);
        operations.retain(|operation| operation_matches(operation, &req));
        operations.sort_by_key(|op| Reverse(op.updated_at_ms));
        let total_count = operations.len() as u32;
        let (operations, next_cursor) = paginate(operations, req.limit, &req.cursor)?;

        self.state.audit(
            &ctx,
            "operations.read",
            "operation",
            "list",
            "succeeded",
            "OK",
        );

        Ok(Response::new(ListManagementOperationsResponse {
            operations,
            next_cursor,
            total_count,
        }))
    }

    async fn get_management_operation(
        &self,
        request: Request<GetManagementOperationRequest>,
    ) -> Result<Response<ManagementOperation>, Status> {
        let ctx = auth_context(&request);
        let operation_id = request.get_ref().operation_id.clone();
        let operation = collect_operations(&self.state.admin, &ctx.workspace_id)
            .into_iter()
            .find(|operation| operation.operation_id == operation_id)
            .ok_or_else(|| Status::not_found("operation not found"))?;

        self.state.audit(
            &ctx,
            "operations.read",
            "operation",
            &operation_id,
            "succeeded",
            "OK",
        );
        Ok(Response::new(operation))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let ctx = auth_context(&request);
        let req = request.into_inner();
        let manager = self
            .state
            .snapshots
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("snapshot inventory is unavailable"))?;

        let collections = match req.collection.as_deref().map(str::trim) {
            Some(collection) if !collection.is_empty() => vec![collection.to_string()],
            _ => self
                .state
                .collections
                .list()
                .into_iter()
                .map(|collection| collection.name)
                .collect(),
        };

        let mut snapshots = Vec::new();
        for collection in collections {
            match manager.list_snapshots(&collection).await {
                Ok(items) => snapshots.extend(items.into_iter().map(snapshot_to_proto)),
                Err(error) => {
                    warn!(collection = %collection, error = %error, "snapshot inventory read failed");
                    return Err(Status::internal("snapshot inventory read failed"));
                }
            }
        }
        snapshots.sort_by_key(|s| Reverse(s.created_at_ms));
        let total_count = snapshots.len() as u32;
        let (snapshots, next_cursor) = paginate(snapshots, req.limit, &req.cursor)?;

        self.state.audit(
            &ctx,
            "snapshots.read",
            "snapshot",
            "list",
            "succeeded",
            "OK",
        );
        Ok(Response::new(ListSnapshotsResponse {
            snapshots,
            next_cursor,
            total_count,
        }))
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<SnapshotOperationalInfo>, Status> {
        let ctx = auth_context(&request);
        let snapshot_id = request.get_ref().snapshot_id.clone();
        let manager = self
            .state
            .snapshots
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("snapshot inventory is unavailable"))?;

        for collection in self.state.collections.list() {
            let snapshots = manager.list_snapshots(&collection.name).await.map_err(|error| {
                warn!(collection = %collection.name, error = %error, "snapshot inventory read failed");
                Status::internal("snapshot inventory read failed")
            })?;
            if let Some(snapshot) = snapshots
                .into_iter()
                .find(|snapshot| snapshot.id == snapshot_id)
            {
                self.state.audit(
                    &ctx,
                    "snapshots.read",
                    "snapshot",
                    &snapshot_id,
                    "succeeded",
                    "OK",
                );
                return Ok(Response::new(snapshot_to_proto(snapshot)));
            }
        }
        Err(Status::not_found("snapshot not found"))
    }

    async fn plan_import(
        &self,
        request: Request<PlanImportRequest>,
    ) -> Result<Response<ManagementPlan>, Status> {
        let ctx = auth_context(&request);
        let req = request.into_inner();
        let source = req
            .source
            .ok_or_else(|| Status::invalid_argument("source is required"))?;
        validate_staged_reference(&source)?;

        if !self.state.import_plan.enabled {
            self.state.audit(
                &ctx,
                "data.import.plan",
                "staged_object",
                &source.staging_id,
                "denied",
                "STAGING_UNAVAILABLE",
            );
            return Err(Status::failed_precondition(
                "import planning is unavailable until trusted staging is configured",
            ));
        }

        if !req.workspace_id.is_empty() && req.workspace_id != ctx.workspace_id {
            self.state.audit(
                &ctx,
                "data.import.plan",
                "staged_object",
                &source.staging_id,
                "denied",
                "WORKSPACE_MISMATCH",
            );
            return Err(Status::permission_denied("workspace is not authorized"));
        }

        let staged = self
            .state
            .staging
            .resolve(&source.staging_id)
            .filter(|staged| staged.workspace_id == ctx.workspace_id)
            .ok_or_else(|| Status::not_found("staged object not found"))?;

        if staged.reference.object_id != source.object_id
            || staged.reference.etag != source.etag
            || staged.reference.size_bytes != source.size_bytes
        {
            self.state.audit(
                &ctx,
                "data.import.plan",
                "staged_object",
                &source.staging_id,
                "failed",
                "SOURCE_FINGERPRINT_CHANGED",
            );
            return Err(Status::failed_precondition(
                "staged object fingerprint changed",
            ));
        }

        let collection = self
            .state
            .collections
            .get(&req.collection)
            .ok_or_else(|| Status::not_found("collection not found"))?;
        if !matches!(req.duplicate_policy.as_str(), "reject" | "skip" | "update") {
            return Err(Status::invalid_argument(
                "duplicate_policy must be reject, skip, or update",
            ));
        }

        let mut findings = Vec::new();
        if source.size_bytes > self.state.import_plan.max_source_bytes {
            findings.push(finding(
                "SOURCE_TOO_LARGE",
                PlanSeverity::PlanError,
                "source exceeds the configured validation limit",
            ));
        }
        if staged
            .estimated_expanded_bytes
            .is_some_and(|size| size > self.state.import_plan.max_expanded_bytes)
        {
            findings.push(finding(
                "EXPANDED_SIZE_TOO_LARGE",
                PlanSeverity::PlanError,
                "estimated expanded size exceeds the configured limit",
            ));
        }
        if !is_supported_format(&staged.format) {
            findings.push(finding(
                "UNSUPPORTED_FORMAT",
                PlanSeverity::PlanError,
                "staged object format is not supported",
            ));
        }
        if staged
            .vector_dimensions
            .is_some_and(|dimensions| dimensions != collection.dimensions)
        {
            findings.push(finding(
                "DIMENSION_MISMATCH",
                PlanSeverity::PlanError,
                "staged vector dimensions do not match the collection",
            ));
        }
        findings.push(finding(
            "EXECUTION_NOT_AVAILABLE",
            PlanSeverity::PlanInfo,
            "Operations Console v1 validates plans but cannot execute imports",
        ));

        let created_at_ms = now_ms();
        let expires_at_ms = created_at_ms.saturating_add(
            i64::try_from(self.state.import_plan.plan_ttl_seconds)
                .unwrap_or(i64::MAX / 1000)
                .saturating_mul(1000),
        );
        let source_fingerprint = sha256_hex(&format!(
            "{}\0{}\0{}\0{}",
            source.staging_id, source.object_id, source.etag, source.size_bytes
        ));
        let plan_id = Uuid::new_v4().to_string();
        let plan_hash = sha256_hex(&format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            plan_id,
            source_fingerprint,
            ctx.workspace_id,
            collection.name,
            req.duplicate_policy,
            expires_at_ms
        ));

        let plan = ManagementPlan {
            plan_id,
            plan_hash,
            kind: "import".to_string(),
            target_type: "collection".to_string(),
            target_id: collection.name,
            workspace_id: ctx.workspace_id.clone(),
            source_fingerprint,
            created_at_ms,
            expires_at_ms,
            // There is deliberately no execution RPC in management API v1.
            executable: false,
            required_execute_permission: "data.import.execute".to_string(),
            source_bytes: source.size_bytes,
            estimated_expanded_bytes: staged.estimated_expanded_bytes,
            estimated_documents: staged.estimated_documents,
            estimated_chunks: staged.estimated_chunks,
            estimated_vectors: staged.estimated_vectors,
            findings,
        };

        self.state.audit(
            &ctx,
            "data.import.plan",
            "collection",
            &plan.target_id,
            "succeeded",
            "VALIDATION_ONLY",
        );
        Ok(Response::new(plan))
    }

    async fn list_audit_events(
        &self,
        request: Request<ListAuditEventsRequest>,
    ) -> Result<Response<ListAuditEventsResponse>, Status> {
        let ctx = auth_context(&request);
        let req = request.into_inner();
        self.state
            .audit(&ctx, "audit.read", "audit_event", "list", "succeeded", "OK");

        let mut events: Vec<_> = self
            .state
            .audit_events
            .read()
            .iter()
            .filter(|event| event.workspace_id == ctx.workspace_id)
            .filter(|event| {
                req.action
                    .as_ref()
                    .is_none_or(|action| event.action == *action)
            })
            .filter(|event| {
                req.outcome
                    .as_ref()
                    .is_none_or(|outcome| event.outcome == *outcome)
            })
            .filter(|event| {
                req.since_timestamp_ms
                    .is_none_or(|since| event.occurred_at_ms >= since)
            })
            .cloned()
            .collect();
        events.sort_by_key(|e| Reverse(e.occurred_at_ms));
        let total_count = events.len() as u32;
        let (events, next_cursor) = paginate(events, req.limit, &req.cursor)?;

        Ok(Response::new(ListAuditEventsResponse {
            events,
            next_cursor,
            total_count,
            retention_notice: format!(
                "volatile local retention; at most {} redacted events",
                self.state.audit_max_entries
            ),
            integrity_status: "not-tamper-evident".to_string(),
        }))
    }
}

fn capability(name: &str, supported: bool, reason: &str) -> ManagementCapability {
    ManagementCapability {
        name: name.to_string(),
        supported,
        authorized: supported,
        unavailable_reason: if supported {
            String::new()
        } else {
            reason.to_string()
        },
    }
}

fn collect_operations(admin: &AdminState, workspace_id: &str) -> Vec<ManagementOperation> {
    let mut executions: Vec<TaskExecution> = admin.task_history.read().clone();
    let mut seen: HashSet<String> = executions
        .iter()
        .map(|execution| execution.execution_id.clone())
        .collect();
    for task in admin.registered_tasks.read().values() {
        if let Some(execution) = &task.current_execution {
            if seen.insert(execution.execution_id.clone()) {
                executions.push(execution.clone());
            }
        }
    }
    executions
        .into_iter()
        .map(|execution| task_to_operation(execution, workspace_id))
        .collect()
}

fn task_to_operation(execution: TaskExecution, workspace_id: &str) -> ManagementOperation {
    let operation_type = match execution.task_type.as_str() {
        "ingestion" | "sync" => ManagementOperationType::IngestDocuments,
        "import" => ManagementOperationType::ImportRecords,
        "export" => ManagementOperationType::ExportRecords,
        "snapshot" => ManagementOperationType::CreateSnapshot,
        "snapshot_verify" | "verify_snapshot" => ManagementOperationType::VerifySnapshot,
        "restore" => ManagementOperationType::RestoreSnapshot,
        "rebuild" | "index_rebuild" => ManagementOperationType::RebuildIndex,
        _ => ManagementOperationType::ManagementOperationUnspecified,
    };
    let state = match execution.state {
        TaskState::Pending => ManagementOperationState::OperationQueued,
        TaskState::Running => ManagementOperationState::OperationRunning,
        TaskState::Completed => ManagementOperationState::OperationSucceeded,
        TaskState::Failed | TaskState::Disabled => ManagementOperationState::OperationFailed,
        TaskState::Cancelled => ManagementOperationState::OperationCancelled,
    };
    let started_at_ms = i64::try_from(execution.started_at)
        .unwrap_or(i64::MAX / 1000)
        .saturating_mul(1000);
    let completed_at_ms = execution.completed_at.map(|completed| {
        i64::try_from(completed)
            .unwrap_or(i64::MAX / 1000)
            .saturating_mul(1000)
    });
    let updated_at_ms = completed_at_ms.unwrap_or(started_at_ms);
    let (items_processed, bytes_processed) = execution
        .result
        .as_ref()
        .map(|result| {
            (
                result.items_processed.unwrap_or_default(),
                result.bytes_processed.unwrap_or_default(),
            )
        })
        .unwrap_or_default();
    let problems: Vec<_> = execution
        .error
        .as_deref()
        .map(|error| OperationProblem {
            reason_code: "TASK_FAILED".to_string(),
            message: sanitize_text(error),
            item_ref: None,
            retryable: false,
        })
        .into_iter()
        .collect();

    ManagementOperation {
        operation_id: execution.execution_id,
        r#type: operation_type as i32,
        state: state as i32,
        target_type: "task".to_string(),
        target_id: sanitize_identifier(&execution.task_id),
        workspace_id: workspace_id.to_string(),
        actor_id: "scheduler".to_string(),
        created_at_ms: started_at_ms,
        started_at_ms: Some(started_at_ms),
        updated_at_ms,
        completed_at_ms,
        progress_percent: match state {
            ManagementOperationState::OperationSucceeded => Some(100.0),
            _ => None,
        },
        items_discovered: items_processed,
        items_processed,
        items_skipped: 0,
        items_failed: u64::from(!problems.is_empty()),
        bytes_processed,
        cancellable: false,
        retryable: false,
        plan_id: None,
        plan_hash: None,
        total_problem_count: problems.len() as u32,
        problems,
    }
}

fn validate_operation_filters(req: &ListManagementOperationsRequest) -> Result<(), Status> {
    if let Some(value) = req.type_filter {
        ManagementOperationType::try_from(value)
            .map_err(|_| Status::invalid_argument("unknown operation type filter"))?;
    }
    if let Some(value) = req.state_filter {
        ManagementOperationState::try_from(value)
            .map_err(|_| Status::invalid_argument("unknown operation state filter"))?;
    }
    Ok(())
}

fn operation_matches(
    operation: &ManagementOperation,
    req: &ListManagementOperationsRequest,
) -> bool {
    req.type_filter
        .is_none_or(|expected| operation.r#type == expected)
        && req
            .state_filter
            .is_none_or(|expected| operation.state == expected)
        && req
            .target_type
            .as_ref()
            .is_none_or(|expected| operation.target_type == *expected)
        && req
            .target_id
            .as_ref()
            .is_none_or(|expected| operation.target_id == *expected)
        && req
            .since_timestamp_ms
            .is_none_or(|since| operation.updated_at_ms >= since)
}

fn snapshot_to_proto(snapshot: SnapshotMetadata) -> SnapshotOperationalInfo {
    SnapshotOperationalInfo {
        snapshot_id: snapshot.id,
        collection: snapshot.collection,
        shard_id: snapshot.shard_id,
        created_at_ms: i64::try_from(snapshot.created_at)
            .unwrap_or(i64::MAX / 1000)
            .saturating_mul(1000),
        size_bytes: snapshot.size_bytes,
        format_version: "snapshot-metadata-v1".to_string(),
        akidb_version: env!("CARGO_PKG_VERSION").to_string(),
        manifest_present: true,
        verification_state: VerificationState::VerificationUnknown as i32,
        verified_at_ms: None,
        restore_test_state: RestoreTestState::RestoreTestNever as i32,
        restore_tested_at_ms: None,
        restore_test_target: None,
        expires_at_ms: None,
    }
}

fn finding(code: &str, severity: PlanSeverity, message: &str) -> PlanFinding {
    PlanFinding {
        code: code.to_string(),
        severity: severity as i32,
        message: message.to_string(),
        item_ref: None,
    }
}

fn is_supported_format(format: &str) -> bool {
    matches!(
        format.to_ascii_lowercase().as_str(),
        "pdf" | "docx" | "xlsx" | "csv" | "html" | "markdown" | "text"
    )
}

fn validate_staged_reference(source: &StagedObjectRef) -> Result<(), Status> {
    let staging_id_is_opaque = !source.staging_id.is_empty()
        && source.staging_id.len() <= 128
        && source
            .staging_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !staging_id_is_opaque {
        return Err(Status::invalid_argument(
            "staging_id must be an opaque server-issued identifier",
        ));
    }
    if source.object_id.is_empty()
        || source.object_id.len() > MAX_SAFE_TEXT
        || source.etag.is_empty()
        || source.etag.len() > MAX_SAFE_TEXT
        || source.object_id.chars().any(char::is_control)
        || source.etag.chars().any(char::is_control)
    {
        return Err(Status::invalid_argument(
            "staged object fingerprint fields are invalid",
        ));
    }
    Ok(())
}

fn paginate<T>(
    items: Vec<T>,
    requested_limit: u32,
    cursor: &str,
) -> Result<(Vec<T>, String), Status> {
    let limit = if requested_limit == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        (requested_limit as usize).min(MAX_PAGE_SIZE)
    };
    let offset = if cursor.is_empty() {
        0
    } else {
        cursor
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid pagination cursor"))?
    };
    if offset > items.len() {
        return Err(Status::invalid_argument(
            "pagination cursor is out of range",
        ));
    }
    let total = items.len();
    let page: Vec<T> = items.into_iter().skip(offset).take(limit).collect();
    let next_offset = offset.saturating_add(page.len());
    let next_cursor = if next_offset < total {
        next_offset.to_string()
    } else {
        String::new()
    };
    Ok((page, next_cursor))
}

fn sanitize_identifier(value: &str) -> String {
    let safe: String = value
        .chars()
        .take(MAX_SAFE_TEXT)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if safe.is_empty() {
        "redacted".to_string()
    } else {
        safe
    }
}

fn sanitize_text(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_SAFE_TEXT)
        .collect()
}

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::{CollectionMeta, CollectionRegistry};
    use crate::proto::management_service_server::ManagementService;
    use akidb_common::config::ImportPlanConfig;
    use akidb_common::scheduler::{ResourceGovernor, ResourceGovernorConfig, SimpleMetricsSource};

    fn service(import_enabled: bool) -> (ManagementServiceImpl, Arc<ManagementState>) {
        let governor = Arc::new(ResourceGovernor::new(
            ResourceGovernorConfig::default(),
            Arc::new(SimpleMetricsSource::new()),
        ));
        let admin = Arc::new(AdminState::new(governor));
        let collections = Arc::new(CollectionRegistry::new());
        collections
            .create(CollectionMeta {
                name: "default".to_string(),
                dimensions: 3,
                metric: "cosine".to_string(),
                embedding_model_id: "test-model".to_string(),
                vector_precision: "f32".to_string(),
                chunk_strategy: "fixed".to_string(),
            })
            .unwrap();
        let state = Arc::new(ManagementState::new(
            admin,
            collections,
            None,
            Arc::new(StagingRegistry::default()),
            ImportPlanConfig {
                enabled: import_enabled,
                ..ImportPlanConfig::default()
            },
            100,
            "test",
            "loopback_optional",
            false,
        ));
        (ManagementServiceImpl::new(state.clone()), state)
    }

    fn request<T>(value: T) -> Request<T> {
        let mut request = Request::new(value);
        request.extensions_mut().insert(AuthContext {
            workspace_id: "default".to_string(),
            agent_id: Some("test-agent".to_string()),
            authenticated: true,
        });
        request
    }

    #[tokio::test]
    async fn capabilities_expose_no_execution_permissions() {
        let (service, _) = service(false);
        let response = service
            .get_management_capabilities(request(GetManagementCapabilitiesRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.management_api_version, 1);
        assert!(response.capabilities.iter().all(|capability| {
            !capability.name.contains("execute")
                && !capability.name.contains("delete")
                && !capability.name.contains("restore")
        }));
        let import = response
            .capabilities
            .iter()
            .find(|capability| capability.name == "data.import.plan")
            .unwrap();
        assert!(!import.supported);
    }

    #[tokio::test]
    async fn import_plan_requires_registered_immutable_source() {
        let (service, _) = service(true);
        let status = service
            .plan_import(request(PlanImportRequest {
                source: Some(StagedObjectRef {
                    staging_id: "unknown".to_string(),
                    object_id: "object".to_string(),
                    etag: "etag".to_string(),
                    size_bytes: 10,
                }),
                collection: "default".to_string(),
                workspace_id: "default".to_string(),
                duplicate_policy: "skip".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn import_plan_rejects_urls_and_filesystem_paths() {
        let (service, _) = service(true);
        for staging_id in [
            "https://example.test/file",
            "/tmp/private.xlsx",
            "../secret",
        ] {
            let status = service
                .plan_import(request(PlanImportRequest {
                    source: Some(StagedObjectRef {
                        staging_id: staging_id.to_string(),
                        object_id: "object".to_string(),
                        etag: "etag".to_string(),
                        size_bytes: 10,
                    }),
                    collection: "default".to_string(),
                    workspace_id: "default".to_string(),
                    duplicate_policy: "skip".to_string(),
                }))
                .await
                .unwrap_err();
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn valid_import_plan_is_validation_only_and_audited() {
        let (service, state) = service(true);
        let source = StagedObjectRef {
            staging_id: "stage-1".to_string(),
            object_id: "object-1".to_string(),
            etag: "etag-1".to_string(),
            size_bytes: 1024,
        };
        state.staging_registry().register(StagedObject {
            reference: source.clone(),
            workspace_id: "default".to_string(),
            format: "xlsx".to_string(),
            estimated_expanded_bytes: Some(4096),
            estimated_documents: Some(2),
            estimated_chunks: Some(5),
            estimated_vectors: Some(5),
            vector_dimensions: Some(3),
        });

        let plan = service
            .plan_import(request(PlanImportRequest {
                source: Some(source),
                collection: "default".to_string(),
                workspace_id: "default".to_string(),
                duplicate_policy: "skip".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!plan.executable);
        assert_eq!(plan.required_execute_permission, "data.import.execute");
        assert_eq!(plan.estimated_vectors, Some(5));
        assert!(state
            .audit_events
            .read()
            .iter()
            .any(|event| event.action == "data.import.plan"));
    }

    #[test]
    fn sanitizer_does_not_preserve_paths_or_controls() {
        assert_eq!(sanitize_identifier("/tmp/secret\nfile"), "_tmp_secret_file");
        assert_eq!(sanitize_text("safe\ntext"), "safetext");
    }
}
