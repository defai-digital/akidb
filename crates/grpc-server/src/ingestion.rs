//! Ingestion Service gRPC Implementation
//!
//! Provides gRPC handlers for:
//! - Triggering and monitoring sync runs
//! - Updating tags on existing documents
//! - Reindexing categories
//! - Category management

use tonic::{Request, Response, Status};
use tracing::info;

use crate::proto::{
    ingestion_service_server::IngestionService, DeleteCategoryRequest, DeleteCategoryResponse,
    GetSyncStatusRequest, ListCategoriesRequest, ListCategoriesResponse, ReindexCategoryRequest,
    ReindexCategoryResponse, SyncStatusResponse, TriggerSyncRequest, TriggerSyncResponse,
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

    fn unconfigured(operation: &str) -> Status {
        Status::failed_precondition(format!(
            "Ingestion {operation} is not configured on this server"
        ))
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

        Err(Self::unconfigured("sync trigger"))
    }

    async fn get_sync_status(
        &self,
        _request: Request<GetSyncStatusRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        Err(Self::unconfigured("sync status"))
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

        Err(Self::unconfigured("tag update"))
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

        Err(Self::unconfigured("category reindex"))
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

        Err(Self::unconfigured("category deletion"))
    }

    async fn list_categories(
        &self,
        request: Request<ListCategoriesRequest>,
    ) -> Result<Response<ListCategoriesResponse>, Status> {
        let req = request.into_inner();

        let limit = if req.limit == 0 { 100 } else { req.limit };
        let offset = req.offset;

        info!(limit = limit, offset = offset, "List categories requested");

        Err(Self::unconfigured("category listing"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{tag_value, TagValue};

    fn assert_unconfigured(err: Status) {
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("not configured"));
    }

    #[tokio::test]
    async fn test_trigger_sync() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(TriggerSyncRequest { force: false });
        let err = service.trigger_sync(request).await.unwrap_err();

        assert_unconfigured(err);
    }

    #[tokio::test]
    async fn test_get_sync_status() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(GetSyncStatusRequest {});
        let err = service.get_sync_status(request).await.unwrap_err();

        assert_unconfigured(err);
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
    async fn test_update_tags_requires_configured_backend() {
        let service = IngestionServiceImpl::new();
        let mut tags = std::collections::HashMap::new();
        tags.insert(
            "status".to_string(),
            TagValue {
                value: Some(tag_value::Value::Text("active".to_string())),
            },
        );

        let request = Request::new(UpdateTagsRequest {
            collection: "test".to_string(),
            document_id: "doc-1".to_string(),
            tags,
            merge: true,
        });
        let err = service.update_tags(request).await.unwrap_err();

        assert_unconfigured(err);
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
    async fn test_reindex_category_requires_configured_backend() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(ReindexCategoryRequest {
            category_uid: "cat-1".to_string(),
            dry_run: false,
        });
        let err = service.reindex_category(request).await.unwrap_err();

        assert_unconfigured(err);
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
    async fn test_delete_category_requires_configured_backend() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(DeleteCategoryRequest {
            category_uid: "cat-1".to_string(),
            hard_delete: false,
        });
        let err = service.delete_category(request).await.unwrap_err();

        assert_unconfigured(err);
    }

    #[tokio::test]
    async fn test_list_categories() {
        let service = IngestionServiceImpl::new();

        let request = Request::new(ListCategoriesRequest {
            limit: 10,
            offset: 0,
        });
        let err = service.list_categories(request).await.unwrap_err();

        assert_unconfigured(err);
    }
}
