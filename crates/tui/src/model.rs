//! Sanitized Operations Console view models.
//!
//! Generated protobuf messages and credential material do not enter component
//! state. These types contain only bounded operational metadata suitable for
//! rendering and diagnostic summaries.

use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub enum LoadState<T> {
    #[default]
    NotLoaded,
    Loading {
        previous: Option<T>,
    },
    Ready {
        value: T,
        observed_at: Instant,
        partial: bool,
    },
    Stale {
        value: T,
        observed_at: Instant,
        error: String,
    },
    Denied {
        capability: String,
    },
    Unsupported {
        reason: String,
    },
    Failed(String),
}

impl<T> LoadState<T> {
    pub fn age(&self) -> Option<std::time::Duration> {
        match self {
            Self::Ready { observed_at, .. } | Self::Stale { observed_at, .. } => {
                Some(observed_at.elapsed())
            }
            _ => None,
        }
    }

    pub fn is_loading(&self) -> bool {
        matches!(self, Self::Loading { .. })
    }
}

#[derive(Debug, Clone, Default)]
pub struct CapabilitiesView {
    pub server_version: String,
    pub api_version: u32,
    pub workspace_id: String,
    pub agent_id: Option<String>,
    pub authenticated: bool,
    pub tls_active: bool,
    pub auth_mode: String,
    pub credential_source: String,
    pub capabilities: Vec<CapabilityView>,
}

#[derive(Debug, Clone)]
pub struct CapabilityView {
    pub name: String,
    pub supported: bool,
    pub authorized: bool,
    pub unavailable_reason: String,
}

#[derive(Debug, Clone)]
pub struct CollectionView {
    pub name: String,
    pub dimensions: u32,
    pub metric: String,
    pub embedding_model_id: String,
    pub vector_precision: String,
    pub chunk_strategy: String,
    pub vector_count: u64,
}

#[derive(Debug, Clone)]
pub struct OperationView {
    pub id: String,
    pub operation_type: String,
    pub state: String,
    pub target: String,
    pub progress_percent: Option<f32>,
    pub updated_at_ms: i64,
    pub items_processed: u64,
    pub bytes_processed: u64,
    pub problem: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnapshotView {
    pub id: String,
    pub collection: String,
    pub created_at_ms: i64,
    pub size_bytes: u64,
    pub manifest_present: bool,
    pub verification_state: String,
    pub restore_test_state: String,
}

#[derive(Debug, Clone)]
pub struct AuditEventView {
    pub occurred_at_ms: i64,
    pub actor_id: String,
    pub action: String,
    pub target: String,
    pub outcome: String,
    pub reason_code: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct AuditPageView {
    pub events: Vec<AuditEventView>,
    pub retention_notice: String,
    pub integrity_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportPlanInput {
    pub staging_id: String,
    pub object_id: String,
    pub etag: String,
    pub size_bytes: u64,
    pub collection: String,
    pub duplicate_policy: String,
}

#[derive(Debug, Clone)]
pub struct PlanFindingView {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Default)]
pub struct ImportPlanView {
    pub plan_id: String,
    pub plan_hash: String,
    pub target_id: String,
    pub workspace_id: String,
    pub source_fingerprint: String,
    pub source_bytes: u64,
    pub estimated_expanded_bytes: Option<u64>,
    pub estimated_documents: Option<u64>,
    pub estimated_chunks: Option<u64>,
    pub estimated_vectors: Option<u64>,
    pub expires_at_ms: i64,
    pub executable: bool,
    pub findings: Vec<PlanFindingView>,
}

#[derive(Debug, Default)]
pub struct ConsoleState {
    pub capabilities: LoadState<CapabilitiesView>,
    pub collections: LoadState<Vec<CollectionView>>,
    pub operations: LoadState<Vec<OperationView>>,
    pub snapshots: LoadState<Vec<SnapshotView>>,
    pub audit: LoadState<AuditPageView>,
    pub import_plan: LoadState<ImportPlanView>,
}
