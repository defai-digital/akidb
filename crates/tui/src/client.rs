//! gRPC client for fetching cluster state from coordinator.

use std::time::{Duration, Instant};
use std::{fs, path::PathBuf};

use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::management_service_client::ManagementServiceClient;
use akidb_proto::{
    GetClusterStateRequest, GetManagementCapabilitiesRequest, ListAuditEventsRequest,
    ListCollectionsRequest, ListManagementOperationsRequest, ListSnapshotsRequest,
    ManagementOperationState, ManagementOperationType, NodeStatus, PlanImportRequest, PlanSeverity,
    RestoreTestState, StagedObjectRef, VerificationState,
};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, warn};

use crate::app::{
    ClusterState, CoordinatorInfo, MetricsState, NodeStatus as AppNodeStatus, ShardInfo,
};
use crate::model::{
    AuditEventView, AuditPageView, CapabilitiesView, CapabilityView, CollectionView,
    ImportPlanInput, ImportPlanView, OperationView, PlanFindingView, SnapshotView,
};

/// Client for connecting to AkiDB coordinator
pub struct CoordinatorClient {
    client: AkidbClient<Channel>,
    address: String,
}

/// Sanitized credential metadata. Token values never enter application state,
/// logs, rendered models, or Debug output.
struct CredentialMetadata {
    authorization: Option<MetadataValue<Ascii>>,
    workspace: Option<MetadataValue<Ascii>>,
    agent: Option<MetadataValue<Ascii>>,
    workspace_id: String,
    source_kind: &'static str,
}

impl CredentialMetadata {
    fn from_environment() -> Self {
        let environment_token = std::env::var("AKIDB_AUTH_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty());
        let file_token = if environment_token.is_none() {
            let path = std::env::var("AKIDB_AUTH_TOKEN_FILE")
                .ok()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./data/auth.token"));
            read_secure_token_file(&path)
        } else {
            None
        };
        let source_kind = if environment_token.is_some() {
            "environment"
        } else if file_token.is_some() {
            "file"
        } else {
            "none"
        };
        let authorization = environment_token
            .or(file_token)
            .and_then(|token| format!("Bearer {}", token.trim()).parse().ok());
        let workspace_id = std::env::var("AKIDB_WORKSPACE")
            .ok()
            .filter(|workspace| !workspace.trim().is_empty())
            .unwrap_or_else(|| "default".to_string());
        let workspace = workspace_id.parse().ok();
        let agent = std::env::var("AKIDB_AGENT")
            .ok()
            .filter(|agent| !agent.trim().is_empty())
            .and_then(|agent| agent.parse().ok());
        Self {
            authorization,
            workspace,
            agent,
            workspace_id,
            source_kind,
        }
    }

    fn request<T>(&self, value: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(value);
        if let Some(value) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        if let Some(value) = &self.workspace {
            request
                .metadata_mut()
                .insert("x-akidb-workspace", value.clone());
        }
        if let Some(value) = &self.agent {
            request
                .metadata_mut()
                .insert("x-akidb-agent", value.clone());
        }
        request
    }
}

/// Read/plan-only client facade shared by Operations Console components.
pub struct OperationsClient {
    data: AkidbClient<Channel>,
    management: ManagementServiceClient<Channel>,
    credentials: CredentialMetadata,
    address: String,
}

impl OperationsClient {
    pub async fn connect(address: &str) -> Result<Self, tonic::transport::Error> {
        let endpoint = if address.starts_with("http://") || address.starts_with("https://") {
            address.to_string()
        } else {
            format!("http://{address}")
        };
        let channel = Endpoint::from_shared(endpoint)?.connect().await?;
        Ok(Self {
            data: AkidbClient::new(channel.clone()),
            management: ManagementServiceClient::new(channel),
            credentials: CredentialMetadata::from_environment(),
            address: address.to_string(),
        })
    }

    pub async fn capabilities(&mut self) -> Result<CapabilitiesView, tonic::Status> {
        let response = self
            .management
            .get_management_capabilities(
                self.credentials
                    .request(GetManagementCapabilitiesRequest {}),
            )
            .await?
            .into_inner();
        Ok(CapabilitiesView {
            server_version: response.server_version,
            api_version: response.management_api_version,
            workspace_id: response.workspace_id,
            agent_id: response.agent_id,
            authenticated: response.authenticated,
            tls_active: response.tls_active,
            auth_mode: response.auth_mode,
            credential_source: self.credentials.source_kind.to_string(),
            capabilities: response
                .capabilities
                .into_iter()
                .map(|capability| CapabilityView {
                    name: capability.name,
                    supported: capability.supported,
                    authorized: capability.authorized,
                    unavailable_reason: capability.unavailable_reason,
                })
                .collect(),
        })
    }

    pub async fn list_collections(&mut self) -> Result<Vec<CollectionView>, tonic::Status> {
        let response = self
            .data
            .list_collections(self.credentials.request(ListCollectionsRequest {}))
            .await?
            .into_inner();
        Ok(response
            .collections
            .into_iter()
            .map(|collection| CollectionView {
                name: collection.name,
                dimensions: collection.dimensions,
                metric: collection.metric,
                embedding_model_id: collection.embedding_model_id,
                vector_precision: collection.vector_precision,
                chunk_strategy: collection.chunk_strategy,
                vector_count: collection.vector_count,
            })
            .collect())
    }

    pub async fn list_operations(&mut self) -> Result<Vec<OperationView>, tonic::Status> {
        let response = self
            .management
            .list_management_operations(self.credentials.request(ListManagementOperationsRequest {
                type_filter: None,
                state_filter: None,
                target_type: None,
                target_id: None,
                since_timestamp_ms: None,
                limit: 100,
                cursor: String::new(),
            }))
            .await?
            .into_inner();
        Ok(response
            .operations
            .into_iter()
            .map(|operation| {
                let operation_type = ManagementOperationType::try_from(operation.r#type)
                    .map(|value| value.as_str_name().to_string())
                    .unwrap_or_else(|_| "MANAGEMENT_OPERATION_UNSPECIFIED".to_string());
                let state = ManagementOperationState::try_from(operation.state)
                    .map(|value| value.as_str_name().to_string())
                    .unwrap_or_else(|_| "OPERATION_STATE_UNSPECIFIED".to_string());
                let problem = operation
                    .problems
                    .first()
                    .map(|problem| format!("{}: {}", problem.reason_code, problem.message));
                OperationView {
                    id: operation.operation_id,
                    operation_type,
                    state,
                    target: format!("{}:{}", operation.target_type, operation.target_id),
                    progress_percent: operation.progress_percent,
                    updated_at_ms: operation.updated_at_ms,
                    items_processed: operation.items_processed,
                    bytes_processed: operation.bytes_processed,
                    problem,
                }
            })
            .collect())
    }

    pub async fn list_snapshots(&mut self) -> Result<Vec<SnapshotView>, tonic::Status> {
        let response = self
            .management
            .list_snapshots(self.credentials.request(ListSnapshotsRequest {
                collection: None,
                limit: 100,
                cursor: String::new(),
            }))
            .await?
            .into_inner();
        Ok(response
            .snapshots
            .into_iter()
            .map(|snapshot| SnapshotView {
                id: snapshot.snapshot_id,
                collection: snapshot.collection,
                created_at_ms: snapshot.created_at_ms,
                size_bytes: snapshot.size_bytes,
                manifest_present: snapshot.manifest_present,
                verification_state: VerificationState::try_from(snapshot.verification_state)
                    .map(|value| value.as_str_name().to_string())
                    .unwrap_or_else(|_| "VERIFICATION_UNKNOWN".to_string()),
                restore_test_state: RestoreTestState::try_from(snapshot.restore_test_state)
                    .map(|value| value.as_str_name().to_string())
                    .unwrap_or_else(|_| "RESTORE_TEST_NEVER".to_string()),
            })
            .collect())
    }

    pub async fn plan_import(
        &mut self,
        input: ImportPlanInput,
    ) -> Result<ImportPlanView, tonic::Status> {
        let response = self
            .management
            .plan_import(self.credentials.request(PlanImportRequest {
                source: Some(StagedObjectRef {
                    staging_id: input.staging_id,
                    object_id: input.object_id,
                    etag: input.etag,
                    size_bytes: input.size_bytes,
                }),
                collection: input.collection,
                workspace_id: self.credentials.workspace_id.clone(),
                duplicate_policy: input.duplicate_policy,
            }))
            .await?
            .into_inner();
        Ok(ImportPlanView {
            plan_id: response.plan_id,
            plan_hash: response.plan_hash,
            target_id: response.target_id,
            workspace_id: response.workspace_id,
            source_fingerprint: response.source_fingerprint,
            source_bytes: response.source_bytes,
            estimated_expanded_bytes: response.estimated_expanded_bytes,
            estimated_documents: response.estimated_documents,
            estimated_chunks: response.estimated_chunks,
            estimated_vectors: response.estimated_vectors,
            expires_at_ms: response.expires_at_ms,
            executable: response.executable,
            findings: response
                .findings
                .into_iter()
                .map(|finding| PlanFindingView {
                    severity: PlanSeverity::try_from(finding.severity)
                        .map(|value| value.as_str_name().to_string())
                        .unwrap_or_else(|_| "PLAN_INFO".to_string()),
                    code: finding.code,
                    message: finding.message,
                })
                .collect(),
        })
    }

    pub async fn list_audit(&mut self) -> Result<AuditPageView, tonic::Status> {
        let response = self
            .management
            .list_audit_events(self.credentials.request(ListAuditEventsRequest {
                action: None,
                outcome: None,
                since_timestamp_ms: None,
                limit: 100,
                cursor: String::new(),
            }))
            .await?
            .into_inner();
        Ok(AuditPageView {
            events: response
                .events
                .into_iter()
                .map(|event| AuditEventView {
                    occurred_at_ms: event.occurred_at_ms,
                    actor_id: event.actor_id,
                    action: event.action,
                    target: format!("{}:{}", event.target_type, event.target_id),
                    outcome: event.outcome,
                    reason_code: event.reason_code,
                    request_id: event.request_id,
                })
                .collect(),
            retention_notice: response.retention_notice,
            integrity_status: response.integrity_status,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }
}

fn read_secure_token_file(path: &std::path::Path) -> Option<String> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_file() || metadata.len() > 4096 {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return None;
        }
    }
    let token = fs::read_to_string(path).ok()?;
    let token = token.trim();
    if token.is_empty() || token.contains('\r') || token.contains('\n') {
        None
    } else {
        Some(token.to_string())
    }
}

impl CoordinatorClient {
    /// Connect to a coordinator at the given address
    pub async fn connect(address: &str) -> Result<Self, tonic::transport::Error> {
        let endpoint = if address.starts_with("http://") || address.starts_with("https://") {
            address.to_string()
        } else {
            format!("http://{}", address)
        };

        debug!("Connecting to coordinator at {}", endpoint);
        let client = AkidbClient::connect(endpoint).await?;

        Ok(Self {
            client,
            address: address.to_string(),
        })
    }

    /// Fetch the current cluster state
    pub async fn get_cluster_state(
        &mut self,
    ) -> Result<(ClusterState, MetricsState), tonic::Status> {
        let request = tonic::Request::new(GetClusterStateRequest {});
        let response = self.client.get_cluster_state(request).await?;
        let state = response.into_inner();

        // Convert proto coordinators to app types
        let coordinators: Vec<CoordinatorInfo> = state
            .coordinators
            .into_iter()
            .map(|c| CoordinatorInfo {
                id: c.id,
                peer_id: c.peer_id,
                address: c.address,
                is_leader: c.is_leader,
                is_self: c.is_self,
                last_seen: Instant::now(),
                status: match c.status {
                    x if x == NodeStatus::Healthy as i32 => AppNodeStatus::Healthy,
                    x if x == NodeStatus::Unhealthy as i32 => AppNodeStatus::Unhealthy,
                    _ => AppNodeStatus::Unknown,
                },
            })
            .collect();

        // Convert proto shards to app types
        let shards: Vec<ShardInfo> = state
            .shards
            .into_iter()
            .map(|s| ShardInfo {
                id: s.id,
                address: s.address,
                vector_count: s.vector_count,
                health_score: s.health_score,
                gpu_memory_percent: s.gpu_memory_percent,
                temperature: s.temperature,
                status: match s.status {
                    x if x == NodeStatus::Healthy as i32 => AppNodeStatus::Healthy,
                    x if x == NodeStatus::Unhealthy as i32 => AppNodeStatus::Unhealthy,
                    _ => AppNodeStatus::Unknown,
                },
            })
            .collect();

        let cluster_state = ClusterState {
            coordinators,
            shards,
            leader_id: state.leader_id,
            local_peer_id: Some(state.local_peer_id),
            last_update: Some(Instant::now()),
        };

        // Convert metrics
        let metrics = if let Some(m) = state.metrics {
            MetricsState {
                qps: m.qps,
                p50_latency_ms: m.p50_latency_ms,
                p95_latency_ms: m.p95_latency_ms,
                p99_latency_ms: m.p99_latency_ms,
                coverage: m.coverage,
                backpressure: m.backpressure,
                within_slo: m.within_slo,
                ..Default::default()
            }
        } else {
            MetricsState::default()
        };

        Ok((cluster_state, metrics))
    }

    /// Get the address this client is connected to
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Try to connect to coordinator, returning None if connection fails
pub async fn try_connect(address: &str, timeout: Duration) -> Option<CoordinatorClient> {
    match tokio::time::timeout(timeout, CoordinatorClient::connect(address)).await {
        Ok(Ok(client)) => Some(client),
        Ok(Err(e)) => {
            warn!("Failed to connect to coordinator at {}: {}", address, e);
            None
        }
        Err(_) => {
            warn!("Connection to coordinator at {} timed out", address);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn token_file_requires_regular_mode_0600_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.token");
        fs::write(&path, "secret-token\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            read_secure_token_file(&path).as_deref(),
            Some("secret-token")
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_secure_token_file(&path).is_none());
    }
}
