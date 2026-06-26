//! Ingestion Service gRPC Implementation
//!
//! Provides gRPC handlers for:
//! - Triggering and monitoring sync runs
//! - Updating tags on existing documents
//! - Reindexing categories
//! - Category management

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::proto::{
    ingestion_service_server::IngestionService,
    CategoryInfo, DeleteCategoryRequest, DeleteCategoryResponse,
    GetSyncStatusRequest, ListCategoriesRequest, ListCategoriesResponse,
    ReindexCategoryRequest, ReindexCategoryResponse,
    SyncRunStatus, SyncStats, SyncStatusResponse,
    TriggerSyncRequest, TriggerSyncResponse,
    UpdateTagsRequest, UpdateTagsResponse,
};

/// Ingestion service implementation
pub struct IngestionServiceImpl {
    // In a full implementation, these would be actual service dependencies
    // scheduler: Arc<IngestionScheduler>,
    // manifest: Arc<ManifestStore>,
    // reindexer: Arc<Reindexer>,
}

impl IngestionServiceImpl {
    /// Create a new ingestion service
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for IngestionServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl IngestionService for IngestionServiceImpl {
    async fn trigger_sync(
        &self,
        request: Request<TriggerSyncRequest>,
    ) -> Result<Response<TriggerSyncResponse>, Status> {
        let req = request.into_inner();
        info!(force = req.force, "Trigger sync requested");

        // In a full implementation, this would trigger the scheduler
        // For now, return a placeholder response
        Ok(Response::new(TriggerSyncResponse {
            run_id: uuid::Uuid::now_v7().to_string(),
            status: SyncRunStatus::SyncStarted.into(),
            message: "Sync triggered successfully".to_string(),
        }))
    }

    async fn get_sync_status(
        &self,
        _request: Request<GetSyncStatusRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        // In a full implementation, this would query the scheduler
        Ok(Response::new(SyncStatusResponse {
            last_run_id: String::new(),
            last_run_status: "idle".to_string(),
            last_run_timestamp_ms: 0,
            next_run_timestamp_ms: 0,
            is_running: false,
            last_run_stats: Some(SyncStats {
                new_count: 0,
                updated_count: 0,
                marked_count: 0,
                confirmed_count: 0,
                skipped_count: 0,
            }),
        }))
    }

    async fn update_tags(
        &self,
        request: Request<UpdateTagsRequest>,
    ) -> Result<Response<UpdateTagsResponse>, Status> {
        let req = request.into_inner();

        // Validate document_id
        if req.document_id.is_empty() {
            return Err(Status::invalid_argument("document_id is required"));
        }

        // Validate tags
        if req.tags.is_empty() {
            return Err(Status::invalid_argument("tags cannot be empty"));
        }

        info!(
            document_id = %req.document_id,
            tag_count = req.tags.len(),
            merge = req.merge,
            "Update tags requested"
        );

        // In a full implementation, this would:
        // 1. Convert proto TagValue to akidb_common::types::TagValue
        // 2. Validate tags using Tags::validate()
        // 3. Update metadata in RocksDB
        // 4. Update tag index
        // 5. Return count of updated vectors

        Ok(Response::new(UpdateTagsResponse {
            success: true,
            error: String::new(),
            vectors_updated: 0,
        }))
    }

    async fn reindex_category(
        &self,
        request: Request<ReindexCategoryRequest>,
    ) -> Result<Response<ReindexCategoryResponse>, Status> {
        let req = request.into_inner();

        if req.category_uid.is_empty() {
            return Err(Status::invalid_argument("category_uid is required"));
        }

        info!(
            category = %req.category_uid,
            dry_run = req.dry_run,
            "Reindex category requested"
        );

        // In a full implementation, this would:
        // 1. Plan the reindex operation
        // 2. If dry_run, return the plan without executing
        // 3. Execute the reindex with progress tracking
        // 4. Tombstone old versions

        Ok(Response::new(ReindexCategoryResponse {
            success: true,
            run_id: uuid::Uuid::now_v7().to_string(),
            documents: 0,
            vectors_inserted: 0,
            vectors_tombstoned: 0,
            old_version: 0,
            new_version: 1,
            error: String::new(),
        }))
    }

    async fn delete_category(
        &self,
        request: Request<DeleteCategoryRequest>,
    ) -> Result<Response<DeleteCategoryResponse>, Status> {
        let req = request.into_inner();

        if req.category_uid.is_empty() {
            return Err(Status::invalid_argument("category_uid is required"));
        }

        info!(
            category = %req.category_uid,
            hard_delete = req.hard_delete,
            "Delete category requested"
        );

        // In a full implementation, this would:
        // 1. Query all vectors with the category
        // 2. If hard_delete, permanently remove them
        // 3. Otherwise, set tombstone flag

        Ok(Response::new(DeleteCategoryResponse {
            success: true,
            vectors_affected: 0,
        }))
    }

    async fn list_categories(
        &self,
        request: Request<ListCategoriesRequest>,
    ) -> Result<Response<ListCategoriesResponse>, Status> {
        let req = request.into_inner();

        let limit = if req.limit == 0 { 100 } else { req.limit };
        let offset = req.offset;

        info!(limit = limit, offset = offset, "List categories requested");

        // In a full implementation, this would:
        // 1. Scan manifest for unique category_uids
        // 2. Aggregate counts per category
        // 3. Return paginated results

        Ok(Response::new(ListCategoriesResponse {
            categories: vec![],
            total_count: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trigger_sync() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(TriggerSyncRequest { force: false });
        let response = service.trigger_sync(request).await.unwrap();

        assert!(!response.get_ref().run_id.is_empty());
        assert_eq!(response.get_ref().status, SyncRunStatus::SyncStarted as i32);
    }

    #[tokio::test]
    async fn test_get_sync_status() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(GetSyncStatusRequest {});
        let response = service.get_sync_status(request).await.unwrap();

        assert!(!response.get_ref().is_running);
    }

    #[tokio::test]
    async fn test_update_tags_validation() {
        let service = IngestionServiceImpl::new();

        // Empty document_id should fail
        let request = Request::new(UpdateTagsRequest {
            collection: "test".to_string(),
            document_id: String::new(),
            tags: std::collections::HashMap::new(),
            merge: false,
        });
        let result = service.update_tags(request).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("document_id"));
    }

    #[tokio::test]
    async fn test_reindex_category_validation() {
        let service = IngestionServiceImpl::new();

        // Empty category should fail
        let request = Request::new(ReindexCategoryRequest {
            category_uid: String::new(),
            dry_run: false,
        });
        let result = service.reindex_category(request).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("category_uid"));
    }

    #[tokio::test]
    async fn test_delete_category_validation() {
        let service = IngestionServiceImpl::new();

        // Empty category should fail
        let request = Request::new(DeleteCategoryRequest {
            category_uid: String::new(),
            hard_delete: false,
        });
        let result = service.delete_category(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_categories() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(ListCategoriesRequest {
            limit: 10,
            offset: 0,
        });
        let response = service.list_categories(request).await.unwrap();

        assert_eq!(response.get_ref().categories.len(), 0);
    }
}
