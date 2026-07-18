//! Typed async results returned to application state.

use crate::model::{
    AuditPageView, CapabilitiesView, CollectionView, ImportPlanView, OperationView, SnapshotView,
};

#[derive(Debug)]
pub enum Action {
    CapabilitiesLoaded(Result<CapabilitiesView, String>),
    CollectionsLoaded(Result<Vec<CollectionView>, String>),
    OperationsLoaded(Result<Vec<OperationView>, String>),
    SnapshotsLoaded(Result<Vec<SnapshotView>, String>),
    ImportPlanLoaded(Result<ImportPlanView, String>),
    AuditLoaded(Result<AuditPageView, String>),
}
