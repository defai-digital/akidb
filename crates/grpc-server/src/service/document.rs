//! Atomic document visibility on one mutable shard, with a durable commit point.

use super::*;
use crate::proto::{
    GetDocumentRequest, GetDocumentResponse, ReplaceDocumentRequest, ReplaceDocumentResponse,
};
use akidb_contracts::document::{
    validate_document_bounds, validate_hash, DOCUMENT_CHUNK_PREFIX, MAX_DOCUMENT_CHUNKS,
};
use akidb_storage::{DocumentHead, DocumentVector, DocumentWriteError};
use prost::Message;
use sha2::{Digest, Sha256};

fn scope_key(workspace: &str, document: &str) -> String {
    let mut hash = Sha256::new();
    hash.update((workspace.len() as u64).to_le_bytes());
    hash.update(workspace.as_bytes());
    hash.update(document.as_bytes());
    format!("{:x}", hash.finalize())
}

/// Marks the shard's projections as requiring restart recovery unless disarmed.
///
/// A document replacement stages vectors and then projects them outside the
/// commit batch. If the task is cancelled (client disconnect) or unwinds
/// mid-way, the projections may be half-applied, so the guard arms before
/// staging and only a path that leaves the shard consistent disarms it. Arming
/// the flag eagerly, as the code used to, made health report "restart
/// required" for the whole duration of every normal replacement.
struct ProjectionFaultGuard<'a> {
    flag: &'a std::sync::atomic::AtomicBool,
    armed: bool,
}

impl<'a> ProjectionFaultGuard<'a> {
    fn new(flag: &'a std::sync::atomic::AtomicBool) -> Self {
        Self { flag, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProjectionFaultGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.flag
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

impl<I: VectorIndex + 'static, S: StorageBackend + 'static> AkiDbService<I, S> {
    pub(super) fn get_document_inner(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<GetDocumentResponse>, Status> {
        let ctx = self.request_auth_context(&request);
        let req = request.into_inner();
        self.validate_request_collection(&req.collection)?;
        validate_hash("document_id", &req.document_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let head = self
            .id_mapping
            .get_document(&scope_key(&ctx.workspace_id, &req.document_id))
            .map_err(Self::to_status)?;
        let Some(head) = head else {
            return Ok(Response::new(GetDocumentResponse::default()));
        };
        let complete = self
            .id_mapping
            .document_is_complete(&head)
            .map_err(Self::to_status)?;
        Ok(Response::new(GetDocumentResponse {
            found: true,
            revision: head.revision,
            document_version: head.document_version,
            settings_fingerprint: head.settings_fingerprint,
            chunk_count: head.chunk_ids.len() as u32,
            complete,
        }))
    }

    pub(super) fn replace_document_inner(
        &self,
        request: Request<ReplaceDocumentRequest>,
    ) -> Result<Response<ReplaceDocumentResponse>, Status> {
        let ctx = self.request_auth_context(&request);
        let mut req = request.into_inner();
        self.validate_request_collection(&req.collection)?;
        validate_document_bounds(req.chunks.len(), req.encoded_len())
            .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        for (field, value) in [
            ("document_id", &req.document_id),
            ("document_version", &req.document_version),
            ("settings_fingerprint", &req.settings_fingerprint),
        ] {
            validate_hash(field, value).map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        if req.source_uri.is_empty()
            || req.source_uri.len() > 8192
            || url::Url::parse(&req.source_uri).is_err()
        {
            return Err(Status::invalid_argument(
                "source_uri must be an absolute URI of at most 8192 bytes",
            ));
        }
        if req.expected_revision.len() > 64 {
            return Err(Status::invalid_argument("invalid expected revision"));
        }
        // Validate/stamp every chunk before touching any projection.
        for chunk in &mut req.chunks {
            if chunk.vector.len() != self.index.dimensions()
                || chunk.vector.iter().any(|v| !v.is_finite())
                || chunk.text.trim().is_empty()
            {
                return Err(Status::invalid_argument("each chunk needs nonempty text and a finite vector matching the collection dimensions"));
            }
            let mut metadata: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(&chunk.metadata).map_err(|_| {
                    Status::invalid_argument("chunk metadata must be a JSON object")
                })?;
            metadata.insert("document_id".into(), req.document_id.clone().into());
            metadata.insert(
                "document_version".into(),
                req.document_version.clone().into(),
            );
            metadata.insert("source_uri".into(), req.source_uri.clone().into());
            metadata.insert(
                "content_hash".into(),
                format!("{:x}", Sha256::digest(chunk.text.as_bytes())).into(),
            );
            chunk.metadata = stamp_write_metadata(
                &serde_json::to_vec(&metadata).expect("JSON map"),
                &ctx,
                &self.acl,
                self.embedding_model_id.as_deref(),
            )
            .map_err(Status::invalid_argument)?;
        }
        validate_document_bounds(req.chunks.len(), req.encoded_len())
            .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        let key = scope_key(&ctx.workspace_id, &req.document_id);
        let old = self
            .id_mapping
            .get_document(&key)
            .map_err(Self::to_status)?;
        let expected = std::mem::take(&mut req.expected_revision);
        let request_hash = format!("{:x}", Sha256::digest(req.encode_to_vec()));
        if let Some(head) = &old {
            if head.request_hash == request_hash
                && self
                    .id_mapping
                    .document_is_complete(head)
                    .map_err(Self::to_status)?
            {
                return Ok(Response::new(ReplaceDocumentResponse {
                    revision: head.revision.clone(),
                    chunk_count: head.chunk_ids.len() as u32,
                    unchanged: true,
                }));
            }
            if !req.legacy_id_prefix.is_empty() {
                return Err(Status::invalid_argument(
                    "legacy migration is allowed only on the first replacement",
                ));
            }
            if head.source_uri != req.source_uri {
                return Err(Status::failed_precondition(
                    "document identity is already bound to another source URI",
                ));
            }
        }
        if old
            .as_ref()
            .map(|h| h.revision.as_str())
            .unwrap_or_default()
            != expected
        {
            return Err(Status::aborted(
                "document revision changed; refresh GetDocument before retrying",
            ));
        }
        let mut retired = old
            .as_ref()
            .map(|h| h.chunk_ids.clone())
            .unwrap_or_default();
        if old.is_none() && !req.legacy_id_prefix.is_empty() {
            let prefix = &req.legacy_id_prefix;
            if prefix.len() != 17
                || !prefix.ends_with('-')
                || !prefix[..16].bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(Status::invalid_argument("invalid legacy ID prefix"));
            }
            let legacy: Vec<_> = self
                .id_mapping
                .vectors_with_id_prefix(prefix, MAX_DOCUMENT_CHUNKS + 1)
                .map_err(Self::to_status)?
                .into_iter()
                // Earlier runs leave tombstones behind. They are already
                // invisible, need no retirement, and must not consume the
                // atomic bound: a file indexed once with small chunks could
                // otherwise never be migrated again.
                .filter(|vector| !vector.deleted)
                .collect();
            if legacy.len() > MAX_DOCUMENT_CHUNKS {
                return Err(Status::resource_exhausted(
                    "legacy document exceeds the atomic replacement bound",
                ));
            }
            let uri = url::Url::parse(&req.source_uri).expect("validated URI");
            let legacy_uri = uri
                .to_file_path()
                .ok()
                .map(|p| format!("file://{}", p.display()));
            for vector in legacy {
                let meta = Self::metadata_value_from_bytes(&vector.metadata);
                if !self.metadata_in_auth_workspace(&vector.metadata, &ctx)
                    || meta["ingest"] != "akidb-index"
                    || !(meta["source_uri"] == req.source_uri
                        || legacy_uri
                            .as_ref()
                            .is_some_and(|u| meta["source_uri"] == *u))
                {
                    return Err(Status::failed_precondition(
                        "legacy ID prefix does not belong to this source and workspace",
                    ));
                }
                retired.push(vector.external_id);
            }
        }
        // Capture old projection metadata before the durable batch tombstones it.
        let mut prior = HashMap::new();
        for id in &retired {
            if let Some(vector) = self
                .id_mapping
                .get_vector_including_deleted(&VectorId::new(id))
                .map_err(Self::to_status)?
            {
                prior.insert(id.clone(), vector);
            }
        }
        let revision = uuid::Uuid::new_v4().to_string();
        // Fail closed only if this task is cancelled or unwinds while staging
        // or projecting: the guard sets the flag on drop unless a path below
        // leaves the shard consistent.
        let mut projection_fault = ProjectionFaultGuard::new(&self.serving_fault);
        let mut staged = Vec::new();
        for (position, chunk) in req.chunks.into_iter().enumerate() {
            let id = VectorId::new(format!("{DOCUMENT_CHUNK_PREFIX}{revision}:{position}"));
            match self.index.insert(&id, &chunk.vector) {
                Ok(internal_id) => staged.push(DocumentVector {
                    id,
                    internal_id,
                    vector: chunk.vector,
                    metadata: chunk.metadata,
                    text: chunk.text,
                }),
                Err(error) => {
                    if self.rollback_document_stage(&staged) {
                        projection_fault.disarm();
                    }
                    return Err(Self::to_status(error));
                }
            }
        }
        crash_point("document.before_commit");
        let head = DocumentHead {
            workspace_id: ctx.workspace_id.clone(),
            revision: revision.clone(),
            document_version: req.document_version,
            settings_fingerprint: req.settings_fingerprint,
            source_uri: req.source_uri,
            request_hash,
            chunk_ids: staged.iter().map(|v| v.id.to_string()).collect(),
        };
        if let Err(error) = self
            .id_mapping
            .replace_document(&key, &expected, &head, &staged, &retired)
        {
            let rolled_back = self.rollback_document_stage(&staged);
            match error {
                DocumentWriteError::BeforeCommit(error) => {
                    if rolled_back {
                        projection_fault.disarm();
                    }
                    return Err(Self::to_status(error));
                }
                DocumentWriteError::CommitUncertain(error) => {
                    // The commit may or may not have landed; stay fail-closed
                    // and let the guard keep the fault flag set.
                    return Err(Self::to_status(error));
                }
            }
        }
        crash_point("document.after_commit");
        // Until this completes, no data-plane request can observe projections.
        let projections = (|| -> Result<(), Status> {
            let fallback =
                serde_json::to_vec(&serde_json::json!({acl::WORKSPACE_KEY: ctx.workspace_id}))
                    .expect("workspace JSON");
            for retired_id in &retired {
                let id = VectorId::new(retired_id);
                let metadata = prior
                    .get(retired_id)
                    .map(|v| v.metadata.as_slice())
                    .unwrap_or(&fallback);
                // Rebuilds can renumber internal IDs. Resolve by external ID;
                // a historical tombstone's internal number may now be reused.
                if let Some(internal_id) = self.index.internal_id_of(&id) {
                    self.index.delete(internal_id).map_err(Self::to_status)?;
                }
                self.sync_source_text(&id, "");
                self.delete_graph_chunk(&id, Some(metadata))
                    .map_err(|e| Status::internal(e.to_string()))?;
                if let Some(sql) = &self.metadata_sql_index {
                    sql.delete_record(&self.collection, id.as_str())
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
            }
            for vector in &staged {
                self.sync_source_text(&vector.id, &vector.text);
                self.index_graph_chunk(&vector.id, &vector.metadata, None)
                    .map_err(|e| Status::internal(e.to_string()))?;
                if let Some(sql) = &self.metadata_sql_index {
                    let now = Self::current_timestamp_ms();
                    self.index_sql_metadata_with_backend(
                        sql.as_ref(),
                        &vector.id,
                        vector.internal_id.0,
                        &vector.metadata,
                        now,
                        now,
                    )
                    .map_err(|e| Status::internal(e.to_string()))?;
                }
            }
            self.id_mapping
                .finish_document_projections()
                .map_err(Self::to_status)?;
            Ok(())
        })();
        if let Err(error) = projections {
            // Keep the cause in the log: the client status is deliberately
            // generic, but without this the only recovery signal is a restart
            // whose failure message names no operation or chunk.
            warn!(
                document = %req.document_id,
                error = %error,
                "document projections failed after commit; restart required"
            );
            // The guard keeps the shard fail-closed (`serving_fault`).
            return Err(Status::unavailable(
                "document committed; restart required to rebuild projections before retrying",
            ));
        }
        projection_fault.disarm();
        self.serving_fault
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(Response::new(ReplaceDocumentResponse {
            revision,
            chunk_count: staged.len() as u32,
            unchanged: false,
        }))
    }

    /// Drop the staged vectors from the index. Returns true when every staged
    /// vector was tombstoned, i.e. no projection was left behind.
    fn rollback_document_stage(&self, staged: &[DocumentVector]) -> bool {
        let mut clean = true;
        for vector in staged {
            if self.index.delete(vector.internal_id).is_err() {
                clean = false;
            }
        }
        clean
    }

    /// Must run before opening network listeners, even when optional general
    /// reconciliation is disabled. The dirty marker is part of the commit.
    pub fn recover_document_projections(&self) -> Result<(), akidb_common::AkiDbError> {
        if !self.id_mapping.document_projections_dirty()? {
            return Ok(());
        }
        let active = self.id_mapping.load_active_vectors()?.len();
        if self.index.stats().active_vectors as usize != active
            || self.id_mapping.load_all_texts()?.len() != self.documents.read().len()
        {
            return Err(AkiDbError::StorageError(
                "document index/text recovery incomplete".into(),
            ));
        }
        for (workspace, retired_id) in self.id_mapping.document_retired_chunks()? {
            let metadata = serde_json::to_vec(&serde_json::json!({acl::WORKSPACE_KEY: workspace}))
                .expect("workspace JSON");
            self.delete_graph_chunk(&VectorId::new(retired_id), Some(&metadata))
                .map_err(|e| AkiDbError::StorageError(e.to_string()))?;
        }
        self.rebuild_graph_index()
            .map_err(|e| AkiDbError::StorageError(e.to_string()))?;
        if self.metadata_sql_index.is_some() {
            // A failed rebuild is incomplete recovery even when the active set
            // is empty: `active == 0` must not let an error pass for "zero
            // records" and clear the marker that forces this retry.
            match self.rebuild_sql_metadata_index() {
                Ok(rebuilt) if rebuilt == active => {}
                Ok(rebuilt) => {
                    return Err(AkiDbError::StorageError(format!(
                        "document SQL recovery incomplete: rebuilt {rebuilt} of {active} records"
                    )));
                }
                Err(error) => {
                    return Err(AkiDbError::StorageError(format!(
                        "document SQL recovery incomplete: {error}"
                    )));
                }
            }
        }
        self.id_mapping.finish_document_projections()
    }
}
