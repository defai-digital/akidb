//! PostgreSQL-authoritative immutable generation replica worker.
//!
//! PostgreSQL owns publication and activation. Each worker independently
//! downloads the logical bundle, rebuilds local indexes, reports a durable
//! checkpoint, and only switches its local active pointer after observing the
//! committed global pointer. Control-plane outages never tear down the gRPC
//! data plane or its last known-good local generation.

use std::collections::BTreeSet;
use std::io::Read;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use akidb_common::config::ReplicaPostgresTlsMode;
use akidb_contracts::{
    KnowledgeGenerationManifest, KnowledgeMutation, KnowledgeMutationPayload, KnowledgeOperation,
    KnowledgeScope, KNOWLEDGE_SCHEMA_VERSION,
};
use akidb_storage::{
    GenerationServingState, LocalGenerationState, ReadyGenerationMarker, ReplicaVolumeClaimOutcome,
    ServingStateRecord,
};
use parking_lot::Mutex;
use rustls_tokio_postgres::{config_from_ca_cert, config_platform_verifier, MakeRustlsConnect};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::{Client, Config as PostgresConfig, NoTls, Row};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    metrics, ExpectedActiveGeneration, GenerationBundleFetcher, GenerationControlError,
    GenerationDataPlane, GenerationFetchError, MaterializedKnowledgeMutation,
};

const REQUIRED_CONTROL_MIGRATION_VERSION: i32 = 1;
const REQUIRED_CONTROL_MIGRATION_NAME: &str = "authoritative_knowledge_control_plane";
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
const MAX_CHECKPOINT_ERROR_CHARS: usize = 16_000;
const MAX_MUTATION_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const MUTATION_PAGE_SIZE: i64 = 1_000;

#[derive(Debug, Clone)]
pub struct ReplicaWorkerConfig {
    pub replica_id: String,
    pub endpoint: String,
    pub failure_domain: String,
    pub workspace_id: String,
    pub collection: String,
    pub postgres_url: String,
    pub postgres_tls_mode: ReplicaPostgresTlsMode,
    pub postgres_ca_certificate_path: Option<PathBuf>,
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub index_format_version: String,
    pub supported_graph_schema_versions: Vec<String>,
    pub generation_gc_enabled: bool,
    pub generation_gc_interval: Duration,
    pub generation_gc_minimum_age: Duration,
    pub generation_gc_dry_run: bool,
    pub software_version: String,
}

impl ReplicaWorkerConfig {
    pub fn validate(&self) -> Result<(), ReplicaWorkerError> {
        for (field, value) in [
            ("replica_id", self.replica_id.as_str()),
            ("endpoint", self.endpoint.as_str()),
            ("failure_domain", self.failure_domain.as_str()),
            ("workspace_id", self.workspace_id.as_str()),
            ("collection", self.collection.as_str()),
            ("postgres_url", self.postgres_url.as_str()),
            ("index_format_version", self.index_format_version.as_str()),
            ("software_version", self.software_version.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.poll_interval.is_zero() || self.heartbeat_interval.is_zero() {
            return Err(ReplicaWorkerError::Configuration(
                "poll and heartbeat intervals must be greater than zero".to_string(),
            ));
        }
        if self.supported_graph_schema_versions.is_empty() {
            return Err(ReplicaWorkerError::Configuration(
                "at least one supported graph schema version is required".to_string(),
            ));
        }
        for version in &self.supported_graph_schema_versions {
            validate_text("supported_graph_schema_versions", version)?;
        }
        if self.generation_gc_enabled
            && (self.generation_gc_interval.is_zero() || self.generation_gc_minimum_age.is_zero())
        {
            return Err(ReplicaWorkerError::Configuration(
                "enabled generation GC requires a positive interval and minimum age".to_string(),
            ));
        }
        Ok(())
    }

    pub fn scope(&self) -> KnowledgeScope {
        KnowledgeScope::new(self.workspace_id.clone(), self.collection.clone())
    }
}

#[derive(Debug, Error)]
pub enum ReplicaWorkerError {
    #[error("replica worker configuration error: {0}")]
    Configuration(String),

    #[error("replica PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("replica generation fetch error: {0}")]
    Fetch(#[from] GenerationFetchError),

    #[error("replica generation control error: {0}")]
    Control(#[from] GenerationControlError),

    #[error("replica manifest JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("replica blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("replica control-plane contract error: {0}")]
    Contract(String),

    #[error("replica materialization diverged: {0}")]
    Divergence(String),

    #[error("replica mutation tail is not yet converged: {0}")]
    MutationTail(String),
}

#[derive(Debug, Clone)]
struct ReplicaDirective {
    active_generation_id: Option<String>,
    active_target_sequence: u64,
    publication_generation_id: Option<String>,
    drained: bool,
}

#[derive(Debug, Clone)]
struct AuthoritativeGeneration {
    manifest_bytes: Vec<u8>,
    manifest_sha256: String,
    manifest: KnowledgeGenerationManifest,
    required_sequence: u64,
    status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportedState {
    CatchingUp,
    Ready,
    Serving,
    Failed,
}

impl ReportedState {
    fn as_str(self) -> &'static str {
        match self {
            Self::CatchingUp => "catching_up",
            Self::Ready => "ready",
            Self::Serving => "serving",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
struct CheckpointReport {
    generation_id: String,
    manifest_sha256: String,
    applied_sequence: u64,
    state: ReportedState,
    last_error: Option<String>,
    vector_count: u64,
    edge_count: u64,
    generation_digest: String,
    index_ready: bool,
}

pub struct PostgresReplicaWorker {
    config: ReplicaWorkerConfig,
    data_plane: Arc<GenerationDataPlane>,
    fetcher: Arc<dyn GenerationBundleFetcher>,
    blank_volume: AtomicBool,
    last_generation_gc: Mutex<Option<Instant>>,
}

impl PostgresReplicaWorker {
    pub fn new(
        config: ReplicaWorkerConfig,
        data_plane: Arc<GenerationDataPlane>,
        fetcher: Arc<dyn GenerationBundleFetcher>,
    ) -> Result<Self, ReplicaWorkerError> {
        config.validate()?;
        let claimed_at_ms = now_ms()?;
        let blank_volume = match data_plane
            .controller()
            .materializer()
            .store()
            .claim_replica_volume(&config.replica_id, claimed_at_ms)
            .map_err(|error| ReplicaWorkerError::Configuration(error.to_string()))?
        {
            ReplicaVolumeClaimOutcome::Claimed => {
                info!(
                    replica_id = %config.replica_id,
                    "claimed blank generation volume for replica"
                );
                true
            }
            ReplicaVolumeClaimOutcome::AlreadyOwned => false,
        };
        metrics().update_replica_checkpoint(
            &config.replica_id,
            &config.workspace_id,
            &config.collection,
            0,
            0,
            false,
        );
        Ok(Self {
            config,
            data_plane,
            fetcher,
            blank_volume: AtomicBool::new(blank_volume),
            last_generation_gc: Mutex::new(None),
        })
    }

    /// Run forever with bounded reconnect backoff. Callers should spawn this
    /// alongside the gRPC server and abort it only during process shutdown.
    pub async fn run(self: Arc<Self>) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match connect_postgres(&self.config).await {
                Ok((mut client, connection_task)) => {
                    backoff = Duration::from_secs(1);
                    let result = self.run_connected(&mut client).await;
                    connection_task.abort();
                    if let Err(error) = result {
                        warn!(
                            replica_id = %self.config.replica_id,
                            error = %error,
                            "replica control loop disconnected; local active reads remain available"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        replica_id = %self.config.replica_id,
                        error = %error,
                        "replica could not connect to PostgreSQL authority; local active reads remain available"
                    );
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
        }
    }

    async fn run_connected(&self, client: &mut Client) -> Result<(), ReplicaWorkerError> {
        verify_control_schema(client).await?;
        self.heartbeat(client).await?;
        self.reconcile_once(client).await?;

        let mut poll = tokio::time::interval(self.config.poll_interval);
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The initial reconciliation above replaces each interval's immediate
        // first tick.
        poll.tick().await;
        heartbeat.tick().await;

        loop {
            tokio::select! {
                _ = poll.tick() => self.reconcile_once(client).await?,
                _ = heartbeat.tick() => {
                    self.heartbeat(client).await?;
                    self.report_local_states(client).await?;
                }
            }
        }
    }

    pub async fn reconcile_once(&self, client: &mut Client) -> Result<(), ReplicaWorkerError> {
        self.heartbeat(client).await?;
        let Some(directive) = self.load_directive(client).await? else {
            return Ok(());
        };
        if directive.drained {
            debug!(
                replica_id = %self.config.replica_id,
                "replica is drained from request routing; authoritative reconciliation continues"
            );
        }

        if let Some(active_id) = &directive.active_generation_id {
            let active = self.load_generation(client, active_id).await?;
            if let Err(error) = self.ensure_built(client, &active).await {
                self.report_generation_failure(client, &active, &error)
                    .await?;
                return Ok(());
            }
            if active.required_sequence != directive.active_target_sequence {
                return Err(ReplicaWorkerError::Contract(format!(
                    "stream active target {} differs from generation required sequence {}",
                    directive.active_target_sequence, active.required_sequence
                )));
            }
            self.align_local_active(&active).await?;
        }

        if let Some(publication_id) = &directive.publication_generation_id {
            if directive.active_generation_id.as_deref() != Some(publication_id.as_str()) {
                let publication = self.load_generation(client, publication_id).await?;
                if matches!(publication.status.as_str(), "staged" | "ready") {
                    if let Err(error) = self.ensure_built(client, &publication).await {
                        self.report_generation_failure(client, &publication, &error)
                            .await?;
                        return Ok(());
                    }
                }
            }
        }

        self.report_local_states(client).await?;
        self.maybe_garbage_collect(client, &directive).await
    }

    async fn maybe_garbage_collect(
        &self,
        client: &Client,
        directive: &ReplicaDirective,
    ) -> Result<(), ReplicaWorkerError> {
        if !self.config.generation_gc_enabled || directive.active_generation_id.is_none() {
            return Ok(());
        }
        {
            let mut last = self.last_generation_gc.lock();
            if last.is_some_and(|value| value.elapsed() < self.config.generation_gc_interval) {
                return Ok(());
            }
            *last = Some(Instant::now());
        }

        let scope = self.config.scope();
        let mut retained = BTreeSet::new();
        if let Some(active) = &directive.active_generation_id {
            retained.insert(active.clone());
        }
        if let Some(publication) = &directive.publication_generation_id {
            retained.insert(publication.clone());
        }
        if let Some(record) = self.data_plane.controller().status(&scope)? {
            for generation in [
                record.active.as_ref(),
                record.previous.as_ref(),
                record.staged.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                retained.insert(generation.manifest.generation_id.clone());
            }
        }
        if retained.is_empty() {
            return Ok(());
        }

        let store = self.data_plane.controller().materializer().store().clone();
        let minimum_age_ms = u64::try_from(self.config.generation_gc_minimum_age.as_millis())
            .map_err(|_| {
                ReplicaWorkerError::Configuration(
                    "generation GC minimum age exceeds u64 milliseconds".to_string(),
                )
            })?;
        let completed_at_ms = now_ms()?;
        let dry_run = self.config.generation_gc_dry_run;
        let evidence = tokio::task::spawn_blocking(move || {
            store.garbage_collect_scope(&scope, &retained, minimum_age_ms, completed_at_ms, dry_run)
        })
        .await?
        .map_err(|error| ReplicaWorkerError::Control(error.into()))?;

        let details = serde_json::to_value(&evidence)?;
        let action = if evidence.dry_run {
            "generation.gc.dry_run"
        } else {
            "generation.gc.applied"
        };
        client
            .execute(
                r#"
                insert into knowledge_audit(
                  workspace_id, collection, action, actor_id, request_id,
                  replica_id, details
                )
                values ($1, $2, $3, $4, $5, $6, $7)
                on conflict (actor_id, request_id, action) do nothing
                "#,
                &[
                    &self.config.workspace_id,
                    &self.config.collection,
                    &action,
                    &"akidb-replica-worker",
                    &Uuid::new_v4().to_string(),
                    &self.config.replica_id,
                    &details,
                ],
            )
            .await?;
        metrics()
            .generation_gc_runs_total
            .with_label_values(&[
                self.config.replica_id.as_str(),
                if evidence.dry_run {
                    "dry_run"
                } else {
                    "applied"
                },
            ])
            .inc();
        metrics()
            .generation_gc_candidates
            .with_label_values(&[self.config.replica_id.as_str()])
            .set(evidence.candidates.len() as i64);
        metrics()
            .generation_gc_deleted_bytes_total
            .with_label_values(&[self.config.replica_id.as_str()])
            .inc_by(evidence.deleted_bytes);
        info!(
            replica_id = %self.config.replica_id,
            dry_run = evidence.dry_run,
            candidates = evidence.candidates.len(),
            deleted = evidence.deleted.len(),
            deleted_bytes = evidence.deleted_bytes,
            "completed safe immutable generation retention scan"
        );
        Ok(())
    }

    async fn heartbeat(&self, client: &Client) -> Result<(), ReplicaWorkerError> {
        let knowledge_versions = Value::Array(vec![Value::from(KNOWLEDGE_SCHEMA_VERSION)]);
        let graph_versions = Value::Array(
            self.config
                .supported_graph_schema_versions
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        );
        client
            .execute(
                r#"
                insert into knowledge_replicas(
                  replica_id, endpoint, failure_domain, software_version,
                  index_format_version, supported_knowledge_schema_versions,
                  supported_graph_schema_versions, process_ready, drained,
                  heartbeat_at, updated_at
                )
                values ($1, $2, $3, $4, $5, $6, $7, true, false,
                        clock_timestamp(), clock_timestamp())
                on conflict (replica_id) do update
                set endpoint = excluded.endpoint,
                    failure_domain = excluded.failure_domain,
                    software_version = excluded.software_version,
                    index_format_version = excluded.index_format_version,
                    supported_knowledge_schema_versions =
                      excluded.supported_knowledge_schema_versions,
                    supported_graph_schema_versions =
                      excluded.supported_graph_schema_versions,
                    process_ready = true,
                    drained = knowledge_replicas.drained,
                    heartbeat_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                "#,
                &[
                    &self.config.replica_id,
                    &self.config.endpoint,
                    &self.config.failure_domain,
                    &self.config.software_version,
                    &self.config.index_format_version,
                    &knowledge_versions,
                    &graph_versions,
                ],
            )
            .await?;
        Ok(())
    }

    async fn load_directive(
        &self,
        client: &Client,
    ) -> Result<Option<ReplicaDirective>, ReplicaWorkerError> {
        let row = client
            .query_opt(
                r#"
                select s.active_generation_id, s.active_target_sequence,
                       s.publication_generation_id, r.drained
                from knowledge_streams s
                join knowledge_replicas r on r.replica_id = $3
                where s.workspace_id = $1 and s.collection = $2
                "#,
                &[
                    &self.config.workspace_id,
                    &self.config.collection,
                    &self.config.replica_id,
                ],
            )
            .await?;
        row.map(directive_from_row).transpose()
    }

    async fn load_generation(
        &self,
        client: &Client,
        generation_id: &str,
    ) -> Result<AuthoritativeGeneration, ReplicaWorkerError> {
        let row = client
            .query_opt(
                r#"
                select manifest_bytes, manifest_sha256, required_sequence, status
                from knowledge_generations
                where generation_id = $1
                  and workspace_id = $2
                  and collection = $3
                "#,
                &[
                    &generation_id,
                    &self.config.workspace_id,
                    &self.config.collection,
                ],
            )
            .await?
            .ok_or_else(|| {
                ReplicaWorkerError::Contract(format!(
                    "authority points to missing generation {generation_id}"
                ))
            })?;
        generation_from_row(row, &self.config.scope())
    }

    async fn ensure_built(
        &self,
        client: &mut Client,
        generation: &AuthoritativeGeneration,
    ) -> Result<(), ReplicaWorkerError> {
        if generation.required_sequence < generation.manifest.target_sequence {
            return Err(ReplicaWorkerError::Contract(format!(
                "generation {} required sequence {} precedes immutable target {}",
                generation.manifest.generation_id,
                generation.required_sequence,
                generation.manifest.target_sequence
            )));
        }
        let mut local = self
            .data_plane
            .controller()
            .status(&self.config.scope())?
            .and_then(|record| {
                local_generation(&record, &generation.manifest.generation_id).cloned()
            });
        if let Some(existing) = &local {
            ensure_local_identity(existing, generation)?;
            if existing.applied_sequence > generation.required_sequence {
                return Err(ReplicaWorkerError::Divergence(format!(
                    "local generation {} checkpoint {} exceeds authority {}",
                    generation.manifest.generation_id,
                    existing.applied_sequence,
                    generation.required_sequence
                )));
            }
            if existing.state == LocalGenerationState::Failed {
                return Err(ReplicaWorkerError::Divergence(format!(
                    "local generation {} is failed: {}",
                    generation.manifest.generation_id,
                    existing
                        .last_error
                        .as_deref()
                        .unwrap_or("no failure evidence")
                )));
            }
        }

        let materializer = self.data_plane.controller().materializer();
        let disk_evidence = materializer
            .disk_admission_evidence(&generation.manifest)
            .map_err(GenerationControlError::from)?;
        metrics().observe_disk_admission(
            &self.config.replica_id,
            disk_evidence.available_bytes,
            disk_evidence.required_bytes,
        );

        if !local.as_ref().is_some_and(|existing| {
            matches!(
                existing.state,
                LocalGenerationState::Ready | LocalGenerationState::Serving
            )
        }) {
            if let Err(error) = materializer.disk_admission(&generation.manifest) {
                metrics()
                    .generation_disk_admission_rejections_total
                    .with_label_values(&[&self.config.replica_id])
                    .inc();
                return Err(ReplicaWorkerError::Control(error.into()));
            }
            let build_started = Instant::now();
            let build_result: Result<(), ReplicaWorkerError> = async {
                let fetched = self.fetcher.fetch(&generation.manifest.bundle).await?;
                let controller = self.data_plane.controller().clone();
                let manifest_bytes = generation.manifest_bytes.clone();
                let manifest_sha256 = generation.manifest_sha256.clone();
                let updated_at_ms = now_ms()?;
                tokio::task::spawn_blocking(move || {
                    let file = fetched.open()?;
                    controller
                        .publish_from_reader(&manifest_bytes, &manifest_sha256, file, updated_at_ms)
                        .map_err(ReplicaWorkerError::Control)
                })
                .await??;
                Ok(())
            }
            .await;
            let build_seconds = build_started.elapsed().as_secs_f64();
            metrics().observe_generation_build(
                &self.config.replica_id,
                if build_result.is_ok() {
                    "success"
                } else {
                    "failure"
                },
                build_seconds,
            );
            if let Err(error) = build_result {
                if self.blank_volume.load(Ordering::Acquire) {
                    metrics()
                        .replica_rebuild_seconds
                        .with_label_values(&[&self.config.replica_id, "failure"])
                        .observe(build_seconds);
                }
                metrics()
                    .generation_verify_failures_total
                    .with_label_values(&[&self.config.replica_id, metric_failure_reason(&error)])
                    .inc();
                return Err(error);
            }
            if self.blank_volume.swap(false, Ordering::AcqRel) {
                metrics()
                    .replica_rebuild_seconds
                    .with_label_values(&[&self.config.replica_id, "success"])
                    .observe(build_seconds);
            }
            self.data_plane.prepare_generation(
                &generation.manifest.scope(),
                &generation.manifest.generation_id,
            )?;
            local = self
                .data_plane
                .controller()
                .status(&self.config.scope())?
                .and_then(|record| {
                    local_generation(&record, &generation.manifest.generation_id).cloned()
                });
        }

        let mut local = local.ok_or_else(|| {
            ReplicaWorkerError::Contract(
                "materialized generation has no local serving state".to_string(),
            )
        })?;
        ensure_local_identity(&local, generation)?;
        if local.applied_sequence < generation.required_sequence {
            let mutations = match self.load_materialized_mutations(client, generation).await {
                Ok(mutations) => mutations,
                Err(error) => {
                    if matches!(&error, ReplicaWorkerError::MutationTail(_)) {
                        metrics()
                            .mutation_gap_total
                            .with_label_values(&[&self.config.replica_id])
                            .inc();
                    }
                    return Err(error);
                }
            };
            let contracts: Vec<KnowledgeMutation> =
                mutations.iter().map(|item| item.mutation.clone()).collect();
            let already_applied = local
                .applied_sequence
                .checked_sub(generation.manifest.target_sequence)
                .ok_or_else(|| {
                    ReplicaWorkerError::Divergence(format!(
                        "local checkpoint {} precedes immutable target {}",
                        local.applied_sequence, generation.manifest.target_sequence
                    ))
                })?;
            let suffix_start = usize::try_from(already_applied).map_err(|_| {
                ReplicaWorkerError::Contract(
                    "local mutation checkpoint cannot fit this platform".to_string(),
                )
            })?;
            let suffix = mutations.get(suffix_start..).ok_or_else(|| {
                ReplicaWorkerError::MutationTail(format!(
                    "local checkpoint {} exceeds fetched mutation tail",
                    local.applied_sequence
                ))
            })?;
            if suffix.is_empty() {
                return Err(ReplicaWorkerError::MutationTail(format!(
                    "generation {} requires sequence {} but has no unapplied mutation",
                    generation.manifest.generation_id, generation.required_sequence
                )));
            }
            let source = self
                .data_plane
                .controller()
                .ready_runtime(&self.config.scope(), &generation.manifest.generation_id)
                .ok_or_else(|| {
                    ReplicaWorkerError::Contract(
                        "local revision source runtime is not retained".to_string(),
                    )
                })?;
            let materializer = self.data_plane.controller().materializer().clone();
            let suffix = suffix.to_vec();
            let applied_mutation_count = u64::try_from(suffix.len()).unwrap_or(u64::MAX);
            let updated_at_ms = now_ms()?;
            let runtime = tokio::task::spawn_blocking(move || {
                materializer
                    .materialize_revision_from_runtime(&source, &suffix, updated_at_ms)
                    .map_err(GenerationControlError::from)
                    .map_err(ReplicaWorkerError::Control)
            })
            .await??;
            let state = self
                .data_plane
                .install_revision(runtime, &contracts, updated_at_ms)?;
            metrics()
                .mutation_apply_total
                .with_label_values(&[&self.config.replica_id, "applied"])
                .inc_by(applied_mutation_count);
            local = local_generation(&state, &generation.manifest.generation_id)
                .cloned()
                .ok_or_else(|| {
                    ReplicaWorkerError::Contract(
                        "installed revision disappeared from local serving state".to_string(),
                    )
                })?;
        }
        if local.applied_sequence != generation.required_sequence {
            return Err(ReplicaWorkerError::MutationTail(format!(
                "generation {} reached {}, authority requires {}",
                generation.manifest.generation_id,
                local.applied_sequence,
                generation.required_sequence
            )));
        }
        self.report_generation_state(client, &local, generation, None)
            .await
    }

    async fn load_materialized_mutations(
        &self,
        client: &Client,
        generation: &AuthoritativeGeneration,
    ) -> Result<Vec<MaterializedKnowledgeMutation>, ReplicaWorkerError> {
        let mut after = generation.manifest.target_sequence;
        let mut mutations = Vec::new();
        while after < generation.required_sequence {
            let rows = client
                .query(
                    r#"
                    select contract
                    from knowledge_mutations
                    where workspace_id = $1
                      and collection = $2
                      and generation_id = $3
                      and sequence > $4
                      and sequence <= $5
                    order by sequence
                    limit $6
                    "#,
                    &[
                        &self.config.workspace_id,
                        &self.config.collection,
                        &generation.manifest.generation_id,
                        &u64_to_i64(after, "mutation after_sequence")?,
                        &u64_to_i64(generation.required_sequence, "mutation required_sequence")?,
                        &MUTATION_PAGE_SIZE,
                    ],
                )
                .await?;
            if rows.is_empty() {
                return Err(ReplicaWorkerError::MutationTail(format!(
                    "mutation sequence gap after {after} for generation {}",
                    generation.manifest.generation_id
                )));
            }
            for row in rows {
                let contract: Value = row.try_get("contract")?;
                let mutation: KnowledgeMutation = serde_json::from_value(contract)?;
                mutation
                    .validate()
                    .map_err(|error| ReplicaWorkerError::Contract(error.to_string()))?;
                let expected = after.checked_add(1).ok_or_else(|| {
                    ReplicaWorkerError::Contract("mutation sequence overflow".to_string())
                })?;
                if mutation.sequence != expected
                    || mutation.scope() != self.config.scope()
                    || mutation.generation_id != generation.manifest.generation_id
                {
                    return Err(ReplicaWorkerError::MutationTail(format!(
                        "expected mutation {} for generation {}, observed {} for {}",
                        expected,
                        generation.manifest.generation_id,
                        mutation.sequence,
                        mutation.generation_id
                    )));
                }
                let payload = match (&mutation.operation, &mutation.payload) {
                    (KnowledgeOperation::Upsert, Some(reference)) => {
                        if reference.size_bytes > MAX_MUTATION_PAYLOAD_BYTES {
                            return Err(ReplicaWorkerError::Contract(format!(
                                "mutation {} payload exceeds {} bytes",
                                mutation.mutation_id, MAX_MUTATION_PAYLOAD_BYTES
                            )));
                        }
                        let fetched = self.fetcher.fetch(reference).await?;
                        let mut bytes =
                            Vec::with_capacity(usize::try_from(reference.size_bytes).unwrap_or(0));
                        fetched
                            .open()?
                            .take(MAX_MUTATION_PAYLOAD_BYTES.saturating_add(1))
                            .read_to_end(&mut bytes)
                            .map_err(GenerationFetchError::from)?;
                        Some(parse_verified_mutation_payload(
                            reference,
                            &mutation,
                            &generation.manifest,
                            &bytes,
                        )?)
                    }
                    (KnowledgeOperation::Delete, None) => None,
                    _ => {
                        return Err(ReplicaWorkerError::Contract(format!(
                            "mutation {} operation/payload contract is inconsistent",
                            mutation.mutation_id
                        )));
                    }
                };
                after = mutation.sequence;
                mutations.push(MaterializedKnowledgeMutation { mutation, payload });
            }
        }
        Ok(mutations)
    }

    async fn align_local_active(
        &self,
        generation: &AuthoritativeGeneration,
    ) -> Result<(), ReplicaWorkerError> {
        let scope = self.config.scope();
        let record = self
            .data_plane
            .controller()
            .status(&scope)?
            .ok_or_else(|| {
                ReplicaWorkerError::Contract(
                    "locally built generation has no serving state".to_string(),
                )
            })?;
        let current = record
            .active
            .as_ref()
            .map(|active| active.manifest.generation_id.as_str());
        if current == Some(generation.manifest.generation_id.as_str()) {
            metrics().set_active_generation(
                &self.config.replica_id,
                &self.config.workspace_id,
                &self.config.collection,
                &generation.manifest.generation_id,
            );
            return Ok(());
        }
        let expected = current
            .map(|value| ExpectedActiveGeneration::Generation(value.to_string()))
            .unwrap_or(ExpectedActiveGeneration::NoActive);
        let updated_at_ms = now_ms()?;
        if record.staged.as_ref().is_some_and(|staged| {
            staged.manifest.generation_id == generation.manifest.generation_id
        }) {
            self.data_plane.activate(
                &scope,
                &generation.manifest.generation_id,
                &expected,
                updated_at_ms,
            )?;
        } else if record.previous.as_ref().is_some_and(|previous| {
            previous.manifest.generation_id == generation.manifest.generation_id
        }) {
            self.data_plane.rollback(
                &scope,
                &generation.manifest.generation_id,
                &expected,
                updated_at_ms,
            )?;
        } else {
            return Err(ReplicaWorkerError::Contract(format!(
                "global active generation {} is neither local staged nor previous",
                generation.manifest.generation_id
            )));
        }
        metrics().set_active_generation(
            &self.config.replica_id,
            &self.config.workspace_id,
            &self.config.collection,
            &generation.manifest.generation_id,
        );
        Ok(())
    }

    async fn report_local_states(&self, client: &mut Client) -> Result<(), ReplicaWorkerError> {
        let Some(record) = self.data_plane.controller().status(&self.config.scope())? else {
            return Ok(());
        };
        for generation in [
            record.active.as_ref(),
            record.previous.as_ref(),
            record.staged.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            let authority = self
                .load_generation(client, &generation.manifest.generation_id)
                .await?;
            self.report_generation_state(client, generation, &authority, None)
                .await?;
        }
        Ok(())
    }

    async fn report_generation_failure(
        &self,
        client: &mut Client,
        generation: &AuthoritativeGeneration,
        error: &ReplicaWorkerError,
    ) -> Result<(), ReplicaWorkerError> {
        let failure = bounded_error(error);
        let local = self
            .data_plane
            .controller()
            .status(&self.config.scope())?
            .and_then(|record| {
                local_generation(&record, &generation.manifest.generation_id).cloned()
            })
            .filter(|state| ensure_local_identity(state, generation).is_ok());
        let marker = local.as_ref().and_then(|state| {
            self.data_plane
                .controller()
                .materializer()
                .store()
                .load_materialized(
                    &generation.manifest.scope(),
                    &generation.manifest.generation_id,
                    state.applied_sequence,
                )
                .ok()
        });
        let (applied_sequence, vector_count, edge_count, generation_digest) = match (local, marker)
        {
            (Some(state), Some(ready)) => (
                state.applied_sequence,
                ready.marker.record_count,
                ready.marker.edge_count,
                digest_for_marker(&ready.marker, &self.config.index_format_version),
            ),
            _ => (
                generation.manifest.base_sequence,
                0,
                0,
                materialization_digest(
                    &generation.manifest_sha256,
                    &generation.manifest.bundle.sha256,
                    generation.manifest.base_sequence,
                    0,
                    0,
                    &self.config.index_format_version,
                ),
            ),
        };
        let report = CheckpointReport {
            generation_id: generation.manifest.generation_id.clone(),
            manifest_sha256: generation.manifest_sha256.clone(),
            applied_sequence,
            state: ReportedState::Failed,
            last_error: Some(failure),
            vector_count,
            edge_count,
            generation_digest,
            index_ready: false,
        };
        self.persist_checkpoint(client, &report).await
    }

    async fn report_generation_state(
        &self,
        client: &mut Client,
        local: &GenerationServingState,
        authority: &AuthoritativeGeneration,
        failure_override: Option<String>,
    ) -> Result<(), ReplicaWorkerError> {
        ensure_local_identity(local, authority)?;
        let marker = self
            .data_plane
            .controller()
            .materializer()
            .store()
            .load_materialized(
                &authority.manifest.scope(),
                &authority.manifest.generation_id,
                local.applied_sequence,
            )
            .ok();
        let (state, last_error, index_ready) = match local.state {
            LocalGenerationState::Ready => (ReportedState::Ready, None, true),
            LocalGenerationState::Serving => (ReportedState::Serving, None, true),
            LocalGenerationState::Staged | LocalGenerationState::CatchingUp => {
                (ReportedState::CatchingUp, None, false)
            }
            LocalGenerationState::Failed => (
                ReportedState::Failed,
                failure_override
                    .or_else(|| local.last_error.clone())
                    .or_else(|| Some("local generation failed".to_string())),
                false,
            ),
        };
        let (vector_count, edge_count, applied_sequence, digest) = if let Some(ready) = marker {
            (
                ready.marker.record_count,
                ready.marker.edge_count,
                local.applied_sequence,
                digest_for_marker(&ready.marker, &self.config.index_format_version),
            )
        } else {
            (
                0,
                0,
                local.applied_sequence,
                materialization_digest(
                    &authority.manifest_sha256,
                    &authority.manifest.bundle.sha256,
                    local.applied_sequence,
                    0,
                    0,
                    &self.config.index_format_version,
                ),
            )
        };
        let effective_state = if index_ready && applied_sequence < authority.required_sequence {
            ReportedState::CatchingUp
        } else {
            state
        };
        let report = CheckpointReport {
            generation_id: authority.manifest.generation_id.clone(),
            manifest_sha256: authority.manifest_sha256.clone(),
            applied_sequence,
            state: effective_state,
            last_error: if effective_state == ReportedState::Failed {
                last_error.map(|error| bounded_text(&error, MAX_CHECKPOINT_ERROR_CHARS))
            } else {
                None
            },
            vector_count,
            edge_count,
            generation_digest: digest,
            index_ready,
        };
        self.persist_checkpoint(client, &report).await
    }

    async fn persist_checkpoint(
        &self,
        client: &mut Client,
        report: &CheckpointReport,
    ) -> Result<(), ReplicaWorkerError> {
        let transaction = client.transaction().await?;
        let generation = transaction
            .query_one(
                r#"
                select required_sequence, materialization_digest,
                       materialized_vector_count, materialized_edge_count
                from knowledge_generations
                where generation_id = $1
                  and workspace_id = $2
                  and collection = $3
                for update
                "#,
                &[
                    &report.generation_id,
                    &self.config.workspace_id,
                    &self.config.collection,
                ],
            )
            .await?;
        let required_sequence = nonnegative_i64(
            generation.get::<_, i64>("required_sequence"),
            "required_sequence",
        )?;
        if report.applied_sequence > required_sequence {
            return Err(ReplicaWorkerError::Contract(format!(
                "local checkpoint {} exceeds authority {}",
                report.applied_sequence, required_sequence
            )));
        }

        if matches!(report.state, ReportedState::Ready | ReportedState::Serving) {
            if report.applied_sequence != required_sequence {
                return Err(ReplicaWorkerError::MutationTail(format!(
                    "ready checkpoint {} does not reach required {}",
                    report.applied_sequence, required_sequence
                )));
            }
            let updated = transaction
                .execute(
                    r#"
                    update knowledge_generations
                    set materialization_digest =
                          coalesce(materialization_digest, $2),
                        materialized_vector_count =
                          coalesce(materialized_vector_count, $3),
                        materialized_edge_count =
                          coalesce(materialized_edge_count, $4)
                    where generation_id = $1
                      and required_sequence = $5
                      and (
                        materialization_digest is null
                        or (
                          materialization_digest = $2
                          and materialized_vector_count = $3
                          and materialized_edge_count = $4
                        )
                      )
                    "#,
                    &[
                        &report.generation_id,
                        &report.generation_digest,
                        &u64_to_i64(report.vector_count, "vector_count")?,
                        &u64_to_i64(report.edge_count, "edge_count")?,
                        &u64_to_i64(report.applied_sequence, "applied_sequence")?,
                    ],
                )
                .await?;
            if updated != 1 {
                return Err(ReplicaWorkerError::Divergence(format!(
                    "generation {} digest/counts differ from a ready peer",
                    report.generation_id
                )));
            }
        }

        transaction
            .execute(
                r#"
                insert into knowledge_replica_checkpoints(
                  replica_id, workspace_id, collection, generation_id,
                  manifest_sha256, applied_sequence, state, last_error,
                  vector_count, edge_count, generation_digest, index_ready,
                  updated_at
                )
                values (
                  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                  clock_timestamp()
                )
                on conflict (
                  replica_id, workspace_id, collection, generation_id
                ) do update
                set manifest_sha256 = excluded.manifest_sha256,
                    applied_sequence = excluded.applied_sequence,
                    state = excluded.state,
                    last_error = excluded.last_error,
                    vector_count = excluded.vector_count,
                    edge_count = excluded.edge_count,
                    generation_digest = excluded.generation_digest,
                    index_ready = excluded.index_ready,
                    updated_at = excluded.updated_at
                "#,
                &[
                    &self.config.replica_id,
                    &self.config.workspace_id,
                    &self.config.collection,
                    &report.generation_id,
                    &report.manifest_sha256,
                    &u64_to_i64(report.applied_sequence, "applied_sequence")?,
                    &report.state.as_str(),
                    &report.last_error,
                    &u64_to_i64(report.vector_count, "vector_count")?,
                    &u64_to_i64(report.edge_count, "edge_count")?,
                    &report.generation_digest,
                    &report.index_ready,
                ],
            )
            .await?;
        transaction
            .query_one(
                "select knowledge_reconcile_generation_ready($1)",
                &[&report.generation_id],
            )
            .await?;
        transaction.commit().await?;
        metrics().update_replica_checkpoint(
            &self.config.replica_id,
            &self.config.workspace_id,
            &self.config.collection,
            report.applied_sequence,
            required_sequence,
            report.index_ready
                && matches!(report.state, ReportedState::Ready | ReportedState::Serving)
                && report.applied_sequence == required_sequence,
        );
        Ok(())
    }
}

fn metric_failure_reason(error: &ReplicaWorkerError) -> &'static str {
    match error {
        ReplicaWorkerError::MutationTail(_) => "mutation_tail",
        ReplicaWorkerError::Divergence(_) => "divergence",
        ReplicaWorkerError::Fetch(GenerationFetchError::Rejected(_)) => "bundle_rejected",
        ReplicaWorkerError::Control(_) => "materialization",
        ReplicaWorkerError::Contract(_) => "contract",
        ReplicaWorkerError::Json(_) => "manifest_json",
        ReplicaWorkerError::Postgres(_) => "postgres",
        ReplicaWorkerError::Configuration(_) => "configuration",
        ReplicaWorkerError::Join(_) => "worker_join",
        ReplicaWorkerError::Fetch(_) => "bundle_fetch",
    }
}

async fn verify_control_schema(client: &Client) -> Result<(), ReplicaWorkerError> {
    let row = client
        .query_opt(
            r#"
            select name
            from knowledge_schema_migrations
            where version = $1
            "#,
            &[&REQUIRED_CONTROL_MIGRATION_VERSION],
        )
        .await?
        .ok_or_else(|| {
            ReplicaWorkerError::Contract(format!(
                "control migration {REQUIRED_CONTROL_MIGRATION_VERSION} is missing"
            ))
        })?;
    let name: String = row.get("name");
    if name != REQUIRED_CONTROL_MIGRATION_NAME {
        return Err(ReplicaWorkerError::Contract(format!(
            "control migration name {name} does not match {REQUIRED_CONTROL_MIGRATION_NAME}"
        )));
    }
    Ok(())
}

async fn connect_postgres(
    config: &ReplicaWorkerConfig,
) -> Result<(Client, JoinHandle<()>), ReplicaWorkerError> {
    let mut postgres = PostgresConfig::from_str(&config.postgres_url)
        .map_err(|error| ReplicaWorkerError::Configuration(error.to_string()))?;
    match config.postgres_tls_mode {
        ReplicaPostgresTlsMode::Disable => {
            ensure_loopback_postgres(&postgres)?;
            postgres.ssl_mode(SslMode::Disable);
            let (client, connection) = postgres.connect(NoTls).await?;
            let task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    warn!(%error, "replica PostgreSQL loopback connection closed");
                }
            });
            Ok((client, task))
        }
        ReplicaPostgresTlsMode::Require => {
            postgres.ssl_mode(SslMode::Require);
            let tls_config = match &config.postgres_ca_certificate_path {
                Some(path) => config_from_ca_cert(path).map_err(|error| {
                    ReplicaWorkerError::Configuration(format!(
                        "failed to load PostgreSQL CA certificate {}: {error}",
                        path.display()
                    ))
                })?,
                None => config_platform_verifier().map_err(|error| {
                    ReplicaWorkerError::Configuration(format!(
                        "failed to configure PostgreSQL platform verifier: {error}"
                    ))
                })?,
            };
            let (client, connection) = postgres.connect(MakeRustlsConnect::new(tls_config)).await?;
            let task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    warn!(%error, "replica PostgreSQL TLS connection closed");
                }
            });
            Ok((client, task))
        }
    }
}

fn ensure_loopback_postgres(config: &PostgresConfig) -> Result<(), ReplicaWorkerError> {
    if config.get_hosts().is_empty() {
        return Err(ReplicaWorkerError::Configuration(
            "plaintext PostgreSQL requires an explicit loopback or Unix-socket host".to_string(),
        ));
    }
    for host in config.get_hosts() {
        match host {
            Host::Unix(_) => {}
            Host::Tcp(value) if value == "localhost" => {}
            Host::Tcp(value) => {
                let loopback = value
                    .parse::<IpAddr>()
                    .map(|address| address.is_loopback())
                    .unwrap_or(false);
                if !loopback {
                    return Err(ReplicaWorkerError::Configuration(
                        "postgres_tls_mode=disable is permitted only for loopback or Unix sockets"
                            .to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn directive_from_row(row: Row) -> Result<ReplicaDirective, ReplicaWorkerError> {
    Ok(ReplicaDirective {
        active_generation_id: row.try_get("active_generation_id")?,
        active_target_sequence: nonnegative_i64(
            row.try_get("active_target_sequence")?,
            "active_target_sequence",
        )?,
        publication_generation_id: row.try_get("publication_generation_id")?,
        drained: row.try_get("drained")?,
    })
}

fn generation_from_row(
    row: Row,
    expected_scope: &KnowledgeScope,
) -> Result<AuthoritativeGeneration, ReplicaWorkerError> {
    let manifest_bytes: Vec<u8> = row.try_get("manifest_bytes")?;
    let manifest_sha256: String = row.try_get("manifest_sha256")?;
    let actual = format!("{:x}", Sha256::digest(&manifest_bytes));
    if actual != manifest_sha256 {
        return Err(ReplicaWorkerError::Contract(format!(
            "manifest bytes digest {actual} differs from authority {manifest_sha256}"
        )));
    }
    let manifest: KnowledgeGenerationManifest = serde_json::from_slice(&manifest_bytes)?;
    manifest
        .validate()
        .map_err(|error| ReplicaWorkerError::Contract(error.to_string()))?;
    if manifest.scope() != *expected_scope {
        return Err(ReplicaWorkerError::Contract(
            "manifest scope differs from the selected stream".to_string(),
        ));
    }
    if !matches!(
        row.get::<_, String>("status").as_str(),
        "staged" | "ready" | "active" | "superseded"
    ) {
        return Err(ReplicaWorkerError::Contract(
            "authority points to a failed or abandoned generation".to_string(),
        ));
    }
    Ok(AuthoritativeGeneration {
        manifest_bytes,
        manifest_sha256,
        manifest,
        required_sequence: nonnegative_i64(row.try_get("required_sequence")?, "required_sequence")?,
        status: row.try_get("status")?,
    })
}

fn local_generation<'a>(
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

fn ensure_local_identity(
    local: &GenerationServingState,
    authority: &AuthoritativeGeneration,
) -> Result<(), ReplicaWorkerError> {
    if local.manifest != authority.manifest || local.manifest_sha256 != authority.manifest_sha256 {
        return Err(ReplicaWorkerError::Divergence(format!(
            "local generation {} immutable identity differs from PostgreSQL",
            authority.manifest.generation_id
        )));
    }
    Ok(())
}

fn digest_for_marker(marker: &ReadyGenerationMarker, index_format_version: &str) -> String {
    if let Some(logical_digest) = &marker.materialization_digest {
        let mut digest = Sha256::new();
        for value in [
            "akidb-materialization-v2",
            logical_digest.as_str(),
            index_format_version,
        ] {
            digest.update(value.as_bytes());
            digest.update([0]);
        }
        return format!("{:x}", digest.finalize());
    }
    materialization_digest(
        &marker.manifest_sha256,
        &marker.bundle_sha256,
        marker.applied_sequence,
        marker.record_count,
        marker.edge_count,
        index_format_version,
    )
}

fn parse_verified_mutation_payload(
    reference: &akidb_contracts::ImmutableObjectReference,
    mutation: &KnowledgeMutation,
    manifest: &KnowledgeGenerationManifest,
    bytes: &[u8],
) -> Result<KnowledgeMutationPayload, ReplicaWorkerError> {
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != reference.size_bytes {
        return Err(ReplicaWorkerError::Contract(format!(
            "mutation {} payload length changed after verified fetch",
            mutation.mutation_id
        )));
    }
    let payload_sha256 = format!("{:x}", Sha256::digest(bytes));
    if payload_sha256 != reference.sha256 {
        return Err(ReplicaWorkerError::Contract(format!(
            "mutation {} payload digest {} differs from authority {}",
            mutation.mutation_id, payload_sha256, reference.sha256
        )));
    }
    let payload: KnowledgeMutationPayload = serde_json::from_slice(bytes)?;
    payload
        .validate_against(mutation, manifest)
        .map_err(|error| ReplicaWorkerError::Contract(error.to_string()))?;
    Ok(payload)
}

fn materialization_digest(
    manifest_sha256: &str,
    bundle_sha256: &str,
    applied_sequence: u64,
    vector_count: u64,
    edge_count: u64,
    index_format_version: &str,
) -> String {
    let mut digest = Sha256::new();
    for value in [
        "akidb-materialization-v1".to_string(),
        manifest_sha256.to_string(),
        bundle_sha256.to_string(),
        applied_sequence.to_string(),
        vector_count.to_string(),
        edge_count.to_string(),
        index_format_version.to_string(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

fn validate_text(field: &str, value: &str) -> Result<(), ReplicaWorkerError> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > 16_384
        || value.chars().any(char::is_control)
    {
        return Err(ReplicaWorkerError::Configuration(format!(
            "{field} must be non-empty canonical text"
        )));
    }
    Ok(())
}

fn nonnegative_i64(value: i64, field: &str) -> Result<u64, ReplicaWorkerError> {
    u64::try_from(value)
        .map_err(|_| ReplicaWorkerError::Contract(format!("{field} is negative or too large")))
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, ReplicaWorkerError> {
    i64::try_from(value)
        .map_err(|_| ReplicaWorkerError::Contract(format!("{field} exceeds PostgreSQL bigint")))
}

fn now_ms() -> Result<u64, ReplicaWorkerError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ReplicaWorkerError::Configuration(error.to_string()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| ReplicaWorkerError::Configuration("system time overflow".to_string()))
}

fn bounded_error(error: &ReplicaWorkerError) -> String {
    bounded_text(&error.to_string(), MAX_CHECKPOINT_ERROR_CHARS)
}

fn bounded_text(value: &str, maximum_chars: usize) -> String {
    let bounded: String = value.chars().take(maximum_chars).collect();
    if bounded.trim().is_empty() {
        "replica generation failure".to_string()
    } else {
        bounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    use akidb_faiss::{SearchParams, VectorIndex};
    use akidb_graph::GraphIndex;
    use akidb_storage::{GenerationStore, RocksDbBackend, ServingStateStore};
    use async_trait::async_trait;
    use tempfile::TempDir;

    use crate::{
        GenerationController, GenerationDataPlaneConfig, GenerationMaterializer,
        GenerationMaterializerConfig,
    };

    #[test]
    fn plaintext_postgres_is_restricted_to_loopback() {
        let loopback = PostgresConfig::from_str("host=127.0.0.1 port=5432 user=postgres").unwrap();
        ensure_loopback_postgres(&loopback).unwrap();

        let remote = PostgresConfig::from_str("host=10.77.0.10 port=5432 user=postgres").unwrap();
        assert!(ensure_loopback_postgres(&remote).is_err());
    }

    #[test]
    fn materialization_digest_is_stable_and_version_sensitive() {
        let first = materialization_digest(&"a".repeat(64), &"b".repeat(64), 7, 10, 5, "format-v1");
        assert_eq!(first.len(), 64);
        assert_eq!(
            first,
            materialization_digest(&"a".repeat(64), &"b".repeat(64), 7, 10, 5, "format-v1",)
        );
        assert_ne!(
            first,
            materialization_digest(&"a".repeat(64), &"b".repeat(64), 7, 10, 5, "format-v2",)
        );
    }

    #[test]
    fn mutation_payload_requires_exact_authoritative_bytes() {
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(include_bytes!(
            "../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json"
        ))
        .unwrap();
        let mutation: KnowledgeMutation = serde_json::from_slice(include_bytes!(
            "../../../contracts/fixtures/knowledge/v1/valid/mutation-upsert-bundle.json"
        ))
        .unwrap();
        let reference = mutation.payload.as_ref().unwrap();
        let bytes = include_bytes!(
            "../../../contracts/fixtures/knowledge/v1/valid/mutation-payload-upsert.json"
        );
        parse_verified_mutation_payload(reference, &mutation, &manifest, bytes).unwrap();

        let mut corrupted = bytes.to_vec();
        corrupted[0] ^= 1;
        assert!(
            parse_verified_mutation_payload(reference, &mutation, &manifest, &corrupted)
                .unwrap_err()
                .to_string()
                .contains("payload digest")
        );
    }

    #[derive(Clone)]
    struct RetainedFixtureFetcher {
        objects: Arc<HashMap<String, PathBuf>>,
        unavailable: Arc<AtomicBool>,
    }

    #[async_trait]
    impl GenerationBundleFetcher for RetainedFixtureFetcher {
        async fn fetch(
            &self,
            reference: &akidb_contracts::ImmutableObjectReference,
        ) -> Result<crate::FetchedGenerationBundle, GenerationFetchError> {
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(GenerationFetchError::Unavailable(
                    "fixture object store is unavailable".to_string(),
                ));
            }
            let path = self.objects.get(&reference.uri).ok_or_else(|| {
                GenerationFetchError::Unavailable(format!(
                    "fixture object {} does not exist",
                    reference.uri
                ))
            })?;
            crate::FetchedGenerationBundle::retained(path)
        }
    }

    struct ReplicaHarness {
        _volume: TempDir,
        worker: Arc<PostgresReplicaWorker>,
        data_plane: Arc<GenerationDataPlane>,
    }

    impl ReplicaHarness {
        fn new(
            replica_id: &str,
            failure_domain: &str,
            postgres_url: &str,
            fetcher: Arc<dyn GenerationBundleFetcher>,
        ) -> Self {
            let volume = tempfile::tempdir().unwrap();
            let generation_store =
                Arc::new(GenerationStore::open(volume.path().join("generations")).unwrap());
            let materializer = Arc::new(GenerationMaterializer::new(
                generation_store,
                GenerationMaterializerConfig::default(),
            ));
            let control = Arc::new(RocksDbBackend::open(volume.path().join("control")).unwrap());
            let state = Arc::new(ServingStateStore::new(control, replica_id.to_string()).unwrap());
            let controller = Arc::new(GenerationController::new(materializer, state));
            let data_plane = Arc::new(
                GenerationDataPlane::new(
                    controller,
                    GenerationDataPlaneConfig {
                        default_collection: "knowledge".to_string(),
                        ..GenerationDataPlaneConfig::default()
                    },
                )
                .unwrap(),
            );
            let worker = Arc::new(
                PostgresReplicaWorker::new(
                    ReplicaWorkerConfig {
                        replica_id: replica_id.to_string(),
                        endpoint: format!("http://{replica_id}.test:50051"),
                        failure_domain: failure_domain.to_string(),
                        workspace_id: "workspace-a".to_string(),
                        collection: "knowledge".to_string(),
                        postgres_url: postgres_url.to_string(),
                        postgres_tls_mode: ReplicaPostgresTlsMode::Disable,
                        postgres_ca_certificate_path: None,
                        poll_interval: Duration::from_millis(10),
                        heartbeat_interval: Duration::from_millis(10),
                        index_format_version: "test-index-v1".to_string(),
                        supported_graph_schema_versions: vec!["ax.knowledge-graph.v1".to_string()],
                        generation_gc_enabled: false,
                        generation_gc_interval: Duration::from_secs(60),
                        generation_gc_minimum_age: Duration::from_secs(60),
                        generation_gc_dry_run: true,
                        software_version: env!("CARGO_PKG_VERSION").to_string(),
                    },
                    data_plane.clone(),
                    fetcher,
                )
                .unwrap(),
            );
            Self {
                _volume: volume,
                worker,
                data_plane,
            }
        }

        fn assert_golden_projection(&self) -> String {
            let scope = KnowledgeScope::new("workspace-a", "knowledge");
            let runtime = self
                .data_plane
                .controller()
                .active_runtime(&scope)
                .expect("replica should retain the active runtime");
            assert_eq!(runtime.ready.marker.applied_sequence, 11);
            assert_eq!(runtime.graph.stats().unwrap().edges, 1);
            assert_eq!(
                runtime.id_mapping.load_all_texts().unwrap(),
                vec![(
                    akidb_common::VectorId::new("chunk-a"),
                    "grounded text".to_string()
                )]
            );
            let result = runtime
                .index
                .search(&[0.1, 0.2, 0.3], &SearchParams::new(1))
                .unwrap();
            assert_eq!(result[0].id.as_str(), "chunk-a");
            runtime
                .ready
                .marker
                .materialization_digest
                .clone()
                .expect("mutation revision should have a logical digest")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires AKIDB_KNOWLEDGE_POSTGRES_URL pointing at disposable PostgreSQL"]
    async fn three_blank_replicas_converge_rebuild_and_isolate_a_gap() {
        let postgres_url = std::env::var("AKIDB_KNOWLEDGE_POSTGRES_URL")
            .expect("AKIDB_KNOWLEDGE_POSTGRES_URL is required");
        let (admin, admin_task) = connect_test_postgres(&postgres_url).await;
        let schema = format!("akidb_replica_{}", uuid::Uuid::new_v4().simple());
        admin
            .batch_execute(&format!(
                "create schema \"{schema}\"; set search_path to \"{schema}\";"
            ))
            .await
            .unwrap();
        admin.batch_execute(TEST_CONTROL_SCHEMA).await.unwrap();
        verify_control_schema(&admin).await.unwrap();
        seed_generation_and_mutation(&admin).await;

        let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../contracts/fixtures/knowledge/v1/valid");
        let unavailable = Arc::new(AtomicBool::new(false));
        let fetcher = Arc::new(RetainedFixtureFetcher {
            objects: Arc::new(HashMap::from([
                (
                    "s3://knowledge/generations/generation-bundle-fixture/bundle.ndjson"
                        .to_string(),
                    fixture_root.join("bundle.ndjson"),
                ),
                (
                    "s3://knowledge/mutations/mutation-bundle-11.json".to_string(),
                    fixture_root.join("mutation-payload-upsert.json"),
                ),
            ])),
            unavailable: unavailable.clone(),
        });

        let mut replicas = vec![
            ReplicaHarness::new("replica-a", "zone-a", &postgres_url, fetcher.clone()),
            ReplicaHarness::new("replica-b", "zone-b", &postgres_url, fetcher.clone()),
            ReplicaHarness::new("replica-c", "zone-c", &postgres_url, fetcher.clone()),
        ];
        let mut connections = Vec::new();
        for _ in 0..replicas.len() {
            connections.push(connect_test_postgres_in_schema(&postgres_url, &schema).await);
        }

        for (replica, (client, _)) in replicas.iter().zip(connections.iter_mut()) {
            replica.worker.reconcile_once(client).await.unwrap();
        }
        let logical_digests: HashSet<String> = replicas
            .iter()
            .map(ReplicaHarness::assert_golden_projection)
            .collect();
        assert_eq!(logical_digests.len(), 1);
        assert_ready_checkpoint_parity(&admin, 3).await;

        // Repeated polling is an idempotent redelivery, not a second apply.
        for (replica, (client, _)) in replicas.iter().zip(connections.iter_mut()) {
            replica.worker.reconcile_once(client).await.unwrap();
        }
        assert_ready_checkpoint_parity(&admin, 3).await;

        // A drained replica is excluded from request routing, but must still
        // reconcile. Otherwise blank rebuilds and rolling upgrades deadlock.
        admin
            .execute(
                "update knowledge_replicas set drained = true where replica_id = 'replica-c'",
                &[],
            )
            .await
            .unwrap();

        // Delete one local volume and rebuild the same stable replica identity
        // while it remains drained.
        drop(replicas.pop().unwrap());
        let replacement =
            ReplicaHarness::new("replica-c", "zone-c", &postgres_url, fetcher.clone());
        replacement
            .worker
            .reconcile_once(&mut connections[2].0)
            .await
            .unwrap();
        assert!(logical_digests.contains(&replacement.assert_golden_projection()));
        replicas.push(replacement);
        assert_ready_checkpoint_parity(&admin, 3).await;
        let drained: bool = admin
            .query_one(
                "select drained from knowledge_replicas where replica_id = 'replica-c'",
                &[],
            )
            .await
            .unwrap()
            .get("drained");
        assert!(drained, "reconciliation must not clear the routing drain");
        admin
            .execute(
                "update knowledge_replicas set drained = false where replica_id = 'replica-c'",
                &[],
            )
            .await
            .unwrap();

        // A gap marks only the polling replica failed and preserves its last
        // known-good local runtime.
        seed_sequence_gap(&admin).await;
        replicas[2]
            .worker
            .reconcile_once(&mut connections[2].0)
            .await
            .unwrap();
        let states = admin
            .query(
                "select replica_id, state, applied_sequence \
                 from knowledge_replica_checkpoints order by replica_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(states[0].get::<_, String>("state"), "serving");
        assert_eq!(states[1].get::<_, String>("state"), "serving");
        assert_eq!(states[2].get::<_, String>("state"), "failed");
        assert_eq!(states[2].get::<_, i64>("applied_sequence"), 11);
        replicas[2].assert_golden_projection();

        // MinIO and control connections are not on the active read path.
        unavailable.store(true, Ordering::SeqCst);
        for (_, task) in connections.drain(..) {
            task.abort();
        }
        for replica in &replicas {
            replica.assert_golden_projection();
        }

        drop(replicas);
        admin
            .batch_execute("set search_path to public")
            .await
            .unwrap();
        admin
            .batch_execute(&format!("drop schema \"{schema}\" cascade"))
            .await
            .unwrap();
        admin_task.abort();
    }

    async fn connect_test_postgres(postgres_url: &str) -> (Client, JoinHandle<()>) {
        let config = PostgresConfig::from_str(postgres_url).unwrap();
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        (client, task)
    }

    async fn connect_test_postgres_in_schema(
        postgres_url: &str,
        schema: &str,
    ) -> (Client, JoinHandle<()>) {
        let (client, task) = connect_test_postgres(postgres_url).await;
        client
            .batch_execute(&format!("set search_path to \"{schema}\""))
            .await
            .unwrap();
        (client, task)
    }

    async fn seed_generation_and_mutation(client: &Client) {
        let manifest_bytes =
            include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");
        let manifest_sha256 = format!("{:x}", Sha256::digest(manifest_bytes));
        let manifest: KnowledgeGenerationManifest = serde_json::from_slice(manifest_bytes).unwrap();
        let mutation: KnowledgeMutation = serde_json::from_slice(include_bytes!(
            "../../../contracts/fixtures/knowledge/v1/valid/mutation-upsert-bundle.json"
        ))
        .unwrap();
        client
            .execute(
                "insert into knowledge_streams(\
                   workspace_id, collection, next_sequence, active_generation_id, \
                   active_manifest_sha256, active_target_sequence, \
                   publication_generation_id, stream_version, \
                   minimum_ready_replicas, minimum_failure_domains, heartbeat_ttl_ms\
                 ) values ($1, $2, 12, $3, $4, 11, null, 1, 2, 2, 60000)",
                &[
                    &manifest.workspace_id,
                    &manifest.collection,
                    &manifest.generation_id,
                    &manifest_sha256,
                ],
            )
            .await
            .unwrap();
        client
            .execute(
                "insert into knowledge_generations(\
                   generation_id, workspace_id, collection, status, manifest, \
                   manifest_bytes, manifest_sha256, bundle_uri, bundle_sha256, \
                   required_sequence, materialization_digest, \
                   materialized_vector_count, materialized_edge_count\
                 ) values ($1, $2, $3, 'active', $4, $5, $6, $7, $8, 11, null, null, null)",
                &[
                    &manifest.generation_id,
                    &manifest.workspace_id,
                    &manifest.collection,
                    &serde_json::to_value(&manifest).unwrap(),
                    &manifest_bytes.as_slice(),
                    &manifest_sha256,
                    &manifest.bundle.uri,
                    &manifest.bundle.sha256,
                ],
            )
            .await
            .unwrap();
        client
            .execute(
                "insert into knowledge_mutations(\
                   workspace_id, collection, sequence, mutation_id, \
                   generation_id, contract\
                 ) values ($1, $2, $3, $4, $5, $6)",
                &[
                    &mutation.workspace_id,
                    &mutation.collection,
                    &(mutation.sequence as i64),
                    &mutation.mutation_id,
                    &mutation.generation_id,
                    &serde_json::to_value(&mutation).unwrap(),
                ],
            )
            .await
            .unwrap();
    }

    async fn seed_sequence_gap(client: &Client) {
        let mutation = KnowledgeMutation {
            schema_version: KNOWLEDGE_SCHEMA_VERSION,
            mutation_id: "mutation-gap-13".to_string(),
            workspace_id: "workspace-a".to_string(),
            collection: "knowledge".to_string(),
            generation_id: "generation-bundle-fixture".to_string(),
            sequence: 13,
            operation: KnowledgeOperation::Delete,
            chunk_id: "chunk-a".to_string(),
            payload: None,
            created_at_ms: 1_784_995_200_013,
        };
        client
            .execute(
                "insert into knowledge_mutations(\
                   workspace_id, collection, sequence, mutation_id, \
                   generation_id, contract\
                 ) values ($1, $2, 13, $3, $4, $5)",
                &[
                    &mutation.workspace_id,
                    &mutation.collection,
                    &mutation.mutation_id,
                    &mutation.generation_id,
                    &serde_json::to_value(&mutation).unwrap(),
                ],
            )
            .await
            .unwrap();
        client
            .batch_execute(
                "update knowledge_streams set active_target_sequence = 13; \
                 update knowledge_generations \
                 set required_sequence = 13, materialization_digest = null, \
                     materialized_vector_count = null, \
                     materialized_edge_count = null;",
            )
            .await
            .unwrap();
    }

    async fn assert_ready_checkpoint_parity(client: &Client, expected: usize) {
        let rows = client
            .query(
                "select applied_sequence, state, vector_count, edge_count, \
                        generation_digest, index_ready \
                 from knowledge_replica_checkpoints order by replica_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), expected);
        let digests: HashSet<String> = rows
            .iter()
            .map(|row| row.get("generation_digest"))
            .collect();
        assert_eq!(digests.len(), 1);
        for row in rows {
            assert_eq!(row.get::<_, i64>("applied_sequence"), 11);
            assert_eq!(row.get::<_, String>("state"), "serving");
            assert_eq!(row.get::<_, i64>("vector_count"), 1);
            assert_eq!(row.get::<_, i64>("edge_count"), 1);
            assert!(row.get::<_, bool>("index_ready"));
        }
    }

    const TEST_CONTROL_SCHEMA: &str = r#"
create table knowledge_schema_migrations (
  version integer primary key,
  name text not null
);
insert into knowledge_schema_migrations(version, name)
values (1, 'authoritative_knowledge_control_plane');

create table knowledge_streams (
  workspace_id text not null,
  collection text not null,
  next_sequence bigint not null,
  active_generation_id text,
  active_manifest_sha256 char(64),
  active_target_sequence bigint not null,
  publication_generation_id text,
  stream_version bigint not null,
  minimum_ready_replicas smallint not null,
  minimum_failure_domains smallint not null,
  heartbeat_ttl_ms integer not null,
  updated_at timestamptz not null default clock_timestamp(),
  primary key (workspace_id, collection)
);

create table knowledge_generations (
  generation_id text primary key,
  workspace_id text not null,
  collection text not null,
  status text not null,
  manifest jsonb not null,
  manifest_bytes bytea not null,
  manifest_sha256 char(64) not null,
  bundle_uri text not null,
  bundle_sha256 char(64) not null,
  required_sequence bigint not null,
  materialization_digest char(64),
  materialized_vector_count bigint,
  materialized_edge_count bigint
);

create table knowledge_mutations (
  workspace_id text not null,
  collection text not null,
  sequence bigint not null,
  mutation_id text not null unique,
  generation_id text not null,
  contract jsonb not null,
  primary key (workspace_id, collection, sequence)
);

create table knowledge_replicas (
  replica_id text primary key,
  endpoint text not null,
  failure_domain text not null,
  software_version text not null,
  index_format_version text not null,
  supported_knowledge_schema_versions jsonb not null,
  supported_graph_schema_versions jsonb not null,
  process_ready boolean not null,
  drained boolean not null,
  heartbeat_at timestamptz not null,
  registered_at timestamptz not null default clock_timestamp(),
  updated_at timestamptz not null
);

create table knowledge_replica_checkpoints (
  replica_id text not null,
  workspace_id text not null,
  collection text not null,
  generation_id text not null,
  manifest_sha256 char(64) not null,
  applied_sequence bigint not null,
  state text not null,
  last_error text,
  vector_count bigint not null,
  edge_count bigint not null,
  generation_digest char(64) not null,
  index_ready boolean not null,
  updated_at timestamptz not null,
  primary key (replica_id, workspace_id, collection, generation_id)
);

create function knowledge_reconcile_generation_ready(text)
returns boolean language sql as $$ select true $$;
"#;
}
