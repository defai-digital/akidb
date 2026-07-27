use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use akidb_graph::{
    Direction, EdgeKind, GraphIndex, GraphNodeId, NativeGraphIndex, NeighborRequest,
};
use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::memory_content;
use akidb_proto::memory_service_client::MemoryServiceClient;
use akidb_proto::{
    GetMemoryCapabilitiesRequest, HealthRequest, MemoryContent, MemoryDeletionSelector,
    MemoryEpistemicFormation, MemoryEvidenceInput, MemoryExecuteDeletionRequest,
    MemoryExportRequest, MemoryListHistoryRequest, MemoryPlanDeletionRequest, MemoryRecallRequest,
    MemoryReinforceRequest, MemoryReinforcementOutcome, MemoryRememberRequest, MemoryReplayMode,
    MemoryReplayRecallRequest, MemoryRequestContext, MemoryScopeInput, MemorySensitivity,
    MemorySourceDeletionSelector, MemoryTemporalMode, MemoryTemporalQuery, MemoryTextFact,
};
use akidb_storage::RocksDbBackend;
use serde_json::json;
use sha2::{Digest, Sha256};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::Request;

/// AkiDB command line interface.
#[derive(Parser, Debug)]
#[command(name = "akidb")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run an AkiDB shard server.
    Server(akidb_server::Args),

    /// Run an AkiDB MCP server over stdio (for MCP-capable agents).
    Mcp(akidb_server::Args),

    /// Run an AkiDB coordinator.
    Coordinator(akidb_coordinator::ServerArgs),

    /// Open the AkiDB terminal dashboard.
    Tui(akidb_tui::Args),

    /// Query the read/plan-only management API without a TUI.
    Ops(OpsArgs),

    /// Check gRPC health and exit non-zero when the service is unhealthy.
    Health(HealthArgs),

    /// Inspect the local native graph index stored in RocksDB.
    Graph(GraphArgs),

    /// Use the separately authenticated authoritative Memory API.
    Memory(Box<MemoryArgs>),
}

#[derive(Parser, Debug)]
struct HealthArgs {
    /// Shard or coordinator gRPC endpoint.
    #[arg(long, default_value = "127.0.0.1:50051")]
    server: String,

    /// Require both healthy=true and ready=true.
    #[arg(long, default_value_t = false)]
    require_ready: bool,

    /// Connection and RPC timeout.
    #[arg(long, default_value_t = 5)]
    timeout_seconds: u64,

    /// PEM CA used to verify a TLS-enabled AkiDB server.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Certificate DNS name override (defaults to the endpoint host).
    #[arg(long)]
    tls_domain: Option<String>,
}

#[derive(Parser, Debug)]
struct MemoryArgs {
    /// AkiDB gRPC endpoint.
    #[arg(long, default_value = "127.0.0.1:50051")]
    server: String,

    /// Authorized workspace and namespace. These values only narrow the
    /// principal grant bound to the bearer credential.
    #[arg(long)]
    workspace: String,
    #[arg(long)]
    namespace: String,
    #[arg(long, default_value = "agent-memory")]
    purpose: String,
    #[arg(long)]
    delegated_agent: Option<String>,

    /// Principal token file. AKIDB_MEMORY_PRINCIPAL_TOKEN takes precedence.
    #[arg(long, default_value = "./data/memory-preview/principal.token")]
    token_file: PathBuf,

    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,

    #[command(subcommand)]
    command: MemoryCommand,
}

#[derive(Subcommand, Debug)]
enum MemoryCommand {
    /// Show the server's honest Memory profile and artifact set.
    Capabilities,
    /// Commit a typed text fact with synced durability.
    RememberText {
        #[arg(long)]
        entity_key: String,
        #[arg(long)]
        predicate: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        source_plane: String,
        #[arg(long)]
        source_id: String,
        #[arg(long)]
        idempotency_key: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value = "internal")]
        sensitivity: String,
        #[arg(long = "expected-head")]
        expected_head_version_ids: Vec<String>,
        #[arg(long)]
        valid_from_unix_nanos: Option<i64>,
        #[arg(long)]
        valid_to_unix_nanos: Option<i64>,
    },
    /// Run bounded deterministic recall and retain a replay snapshot.
    Recall {
        #[arg(long)]
        query: String,
        #[arg(long = "predicate")]
        structured_predicates: Vec<String>,
        #[arg(long = "entity")]
        entity_keys: Vec<String>,
        #[arg(long, default_value_t = 10)]
        max_items: u32,
        #[arg(long, default_value_t = 1024)]
        max_context_tokens: u32,
        #[arg(long, default_value = "current")]
        temporal_mode: String,
        #[arg(long)]
        valid_at_unix_nanos: Option<i64>,
        #[arg(long)]
        commit_sequence: Option<u64>,
    },
    /// Return retained bytes or re-execute against the retained artifact set.
    Replay {
        snapshot_id: String,
        #[arg(long, default_value_t = false)]
        reexecute: bool,
    },
    /// Return immutable lineage for one assertion.
    History {
        assertion_id: String,
        #[arg(long)]
        from_sequence: Option<u64>,
        #[arg(long)]
        to_sequence: Option<u64>,
        #[arg(long, default_value_t = 1000)]
        limit: u32,
    },
    /// Stream scoped canonical JSON records and their SHA-256 digests.
    Export {
        #[arg(long, default_value_t = 10_000)]
        limit: u32,
    },
    /// Attach success/failure evidence without rewriting a Memory version.
    Reinforce {
        version_id: String,
        #[arg(long)]
        outcome: String,
        #[arg(long)]
        outcome_id: String,
        #[arg(long)]
        utility_micros: i32,
        #[arg(long)]
        source_plane: String,
        #[arg(long)]
        source_id: String,
        #[arg(long)]
        evidence_sha256: String,
        #[arg(long)]
        idempotency_key: String,
        #[arg(long)]
        reason: String,
    },
    /// Produce an immutable dry-run source or data-subject deletion plan.
    PlanDeletion {
        #[arg(long)]
        source_plane: Option<String>,
        #[arg(long)]
        source_id: Option<String>,
        #[arg(long)]
        data_subject_id: Option<String>,
        #[arg(long, default_value_t = 900)]
        expires_in_seconds: u64,
        #[arg(long)]
        reason: String,
    },
    /// Execute one fresh, checksum-bound deletion plan and propagate tombstones.
    ExecuteDeletion {
        plan_id: String,
        #[arg(long)]
        plan_sha256: String,
        #[arg(long)]
        idempotency_key: String,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Parser, Debug)]
struct GraphArgs {
    /// RocksDB path used by the AkiDB server.
    #[arg(long, default_value = "/opt/akidb/data/rocksdb")]
    rocksdb: PathBuf,

    #[command(subcommand)]
    command: GraphCommand,
}

#[derive(Parser, Debug)]
struct OpsArgs {
    /// Shard management endpoint.
    #[arg(long, default_value = "127.0.0.1:50051")]
    management: String,

    #[command(subcommand)]
    command: OpsCommand,
}

#[derive(Subcommand, Debug)]
enum OpsCommand {
    /// Show API version, authentication state, and authorized capabilities.
    Capabilities,
    /// List collection schemas and vector counts.
    Collections,
    /// List canonical background operations.
    Operations,
    /// List snapshot integrity and restore-test evidence.
    Snapshots,
    /// List server-redacted management audit events.
    Audit,
    /// Validate an immutable server-issued staged object without importing it.
    PlanImport {
        #[arg(long)]
        staging_id: String,
        #[arg(long)]
        object_id: String,
        #[arg(long)]
        etag: String,
        #[arg(long)]
        size_bytes: u64,
        #[arg(long, default_value = "default")]
        collection: String,
        #[arg(long, default_value = "skip")]
        duplicate_policy: String,
    },
}

#[derive(Subcommand, Debug)]
enum GraphCommand {
    /// Print graph node/edge/chunk-link counts.
    Stats,

    /// List neighbors for a graph node id such as chunk:doc1 or file:src/lib.rs.
    Neighbors {
        /// Graph node id.
        node_id: String,

        /// Direction: out, in, or both.
        #[arg(long, default_value = "both")]
        direction: String,

        /// Optional edge kind filter, repeatable. Example: --edge-kind calls.
        #[arg(long = "edge-kind")]
        edge_kinds: Vec<String>,

        /// Maximum neighbors to return.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },

    /// List chunk vector ids directly related to a graph entity.
    RelatedChunks {
        /// Graph entity node id.
        entity_id: String,

        /// Maximum chunks to return.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Server(args) => akidb_server::run(args)
            .await
            .map_err(|e| anyhow::anyhow!("{e}")),
        Command::Mcp(args) => akidb_server::run_mcp(args)
            .await
            .map_err(|e| anyhow::anyhow!("{e}")),
        Command::Coordinator(args) => akidb_coordinator::run_server(args)
            .await
            .map_err(|e| anyhow::anyhow!("{e}")),
        Command::Tui(args) => akidb_tui::run(args).await,
        Command::Ops(args) => run_ops(args).await,
        Command::Health(args) => run_health(args).await,
        Command::Graph(args) => run_graph(args),
        Command::Memory(args) => run_memory(*args).await,
    }
}

async fn run_health(args: HealthArgs) -> anyhow::Result<()> {
    let tls_enabled = args.server.starts_with("https://") || args.tls_ca.is_some();
    let endpoint = if args.server.starts_with("http://") || args.server.starts_with("https://") {
        args.server.clone()
    } else if tls_enabled {
        format!("https://{}", args.server)
    } else {
        format!("http://{}", args.server)
    };
    let timeout = Duration::from_secs(args.timeout_seconds);
    let mut endpoint = Endpoint::from_shared(endpoint)?
        .connect_timeout(timeout)
        .timeout(timeout);
    if tls_enabled {
        let domain = args
            .tls_domain
            .clone()
            .or_else(|| endpoint.uri().host().map(str::to_string))
            .ok_or_else(|| anyhow::anyhow!("TLS endpoint has no certificate domain"))?;
        let mut tls = ClientTlsConfig::new().domain_name(domain);
        if let Some(ca_path) = args.tls_ca {
            tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(ca_path)?));
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    let channel = endpoint.connect().await?;
    let mut client = AkidbClient::new(channel);
    let mut request = Request::new(HealthRequest {});
    attach_health_credentials(&mut request)?;

    let health = client.health(request).await?.into_inner();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "healthy": health.healthy,
            "ready": health.ready,
            "message": health.message,
            "total_vectors": health.total_vectors,
            "active_vectors": health.active_vectors,
            "using_gpu": health.using_gpu,
        }))?
    );

    if !health.healthy {
        anyhow::bail!("AkiDB service reported healthy=false");
    }
    if args.require_ready && !health.ready {
        anyhow::bail!("AkiDB service reported ready=false");
    }
    Ok(())
}

fn attach_health_credentials(request: &mut Request<HealthRequest>) -> anyhow::Result<()> {
    if let Some(token) = health_token() {
        let value: MetadataValue<Ascii> = format!("Bearer {token}").parse()?;
        request.metadata_mut().insert("authorization", value);
    }
    if let Ok(workspace) = std::env::var("AKIDB_WORKSPACE") {
        if !workspace.trim().is_empty() {
            request
                .metadata_mut()
                .insert("x-akidb-workspace", workspace.trim().parse()?);
        }
    }
    if let Ok(agent) = std::env::var("AKIDB_AGENT") {
        if !agent.trim().is_empty() {
            request
                .metadata_mut()
                .insert("x-akidb-agent", agent.trim().parse()?);
        }
    }
    Ok(())
}

fn health_token() -> Option<String> {
    if let Ok(token) = std::env::var("AKIDB_AUTH_TOKEN") {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Some(token);
        }
    }
    let path = std::env::var("AKIDB_AUTH_TOKEN_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data/auth.token"));
    std::fs::read_to_string(path)
        .ok()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
}

async fn run_memory(args: MemoryArgs) -> anyhow::Result<()> {
    let MemoryArgs {
        server,
        workspace,
        namespace,
        purpose,
        delegated_agent,
        token_file,
        timeout_seconds,
        command,
    } = args;
    let token = memory_token(&token_file)?;
    let endpoint = if server.starts_with("http://") || server.starts_with("https://") {
        server
    } else {
        format!("http://{server}")
    };
    let timeout = Duration::from_secs(timeout_seconds);
    let channel = Endpoint::from_shared(endpoint)?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await?;
    let mut client = MemoryServiceClient::new(channel);

    match command {
        MemoryCommand::Capabilities => {
            let request = memory_request(GetMemoryCapabilitiesRequest {}, &token)?;
            let capabilities = client
                .get_memory_capabilities(request)
                .await?
                .into_inner()
                .capabilities
                .ok_or_else(|| anyhow::anyhow!("server omitted Memory capabilities"))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "profile_status": capabilities.profile_status,
                    "supported_rpcs": capabilities.supported_rpcs,
                    "supported_temporal_modes": capabilities.supported_temporal_modes,
                    "durability_modes": capabilities.durability_modes,
                    "active_projection_recipes": capabilities.active_projection_recipes,
                    "workspace_topology": capabilities.workspace_topology,
                    "dense_retrieval_available": capabilities.dense_retrieval_available,
                    "active_projection_manifest_sha256":
                        capabilities.active_projection_manifest_sha256,
                    "policy_manifest_id": capabilities.policy_manifest_id,
                    "tokenizer_artifact_id": capabilities.tokenizer_artifact_id,
                    "context_firewall_artifact_id":
                        capabilities.context_firewall_artifact_id,
                    "server_build_id": capabilities.server_build_id,
                    "retention_policy": capabilities.retention_policy.map(|policy| json!({
                        "raw_event_seconds": policy.raw_event_seconds,
                        "memory_version_seconds": policy.memory_version_seconds,
                        "compiler_artifact_seconds": policy.compiler_artifact_seconds,
                        "index_artifact_seconds": policy.index_artifact_seconds,
                        "audit_seconds": policy.audit_seconds,
                        "snapshot_seconds": policy.snapshot_seconds,
                        "zero_means_indefinite": policy.zero_means_indefinite,
                        "finite_windows_enforced": policy.finite_windows_enforced,
                    })),
                }))?
            );
        }
        MemoryCommand::RememberText {
            entity_key,
            predicate,
            text,
            source_plane,
            source_id,
            idempotency_key,
            reason,
            sensitivity,
            expected_head_version_ids,
            valid_from_unix_nanos,
            valid_to_unix_nanos,
        } => {
            let content_sha256 = sha256_hex(text.as_bytes());
            let body = MemoryRememberRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    Some(idempotency_key),
                )),
                scope: Some(MemoryScopeInput {
                    entity_key,
                    data_subject_id: None,
                    owner_agent_id: delegated_agent,
                    session_id: None,
                    task_id: None,
                    sensitivity: parse_memory_sensitivity(&sensitivity)? as i32,
                    allowed_purposes: vec![purpose],
                }),
                predicate,
                content: Some(MemoryContent {
                    value: Some(memory_content::Value::TextFact(MemoryTextFact {
                        text,
                        language: None,
                    })),
                }),
                valid_from_ms: None,
                valid_to_ms: None,
                epistemic_formation: MemoryEpistemicFormation::MemoryFormationHumanStatement as i32,
                confidence: None,
                evidence: vec![MemoryEvidenceInput {
                    source_plane,
                    source_id,
                    source_version: None,
                    observed_at_ms: None,
                    content_sha256,
                    source_principal_id: None,
                    observed_at_unix_nanos: None,
                }],
                expected_head_version_ids,
                reason,
                valid_from_unix_nanos,
                valid_to_unix_nanos,
                compiler_artifact_id: None,
                derivation: None,
            };
            let receipt = client
                .remember(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&mutation_receipt_json(&receipt))?
            );
        }
        MemoryCommand::Recall {
            query,
            structured_predicates,
            entity_keys,
            max_items,
            max_context_tokens,
            temporal_mode,
            valid_at_unix_nanos,
            commit_sequence,
        } => {
            let body = MemoryRecallRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    None,
                )),
                query_text: Some(query),
                structured_predicates,
                entity_keys,
                max_items,
                max_context_tokens: Some(max_context_tokens),
                deterministic: true,
                include_explanation_summary: true,
                canonical_at_sequence: None,
                temporal_query: Some(MemoryTemporalQuery {
                    mode: parse_memory_temporal_mode(&temporal_mode)? as i32,
                    valid_at_unix_nanos,
                    commit_sequence,
                }),
                include_conflicts: false,
                recipe: Some("preview-bounded-bm25-v1".to_string()),
            };
            let response = client
                .recall(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "snapshot_id": response.snapshot_id,
                    "policy_decision_id": response.policy_decision_id,
                    "visibility": response.visibility.map(|value| json!({
                        "commit_sequence": value.commit_sequence,
                        "visible_sequence": value.visible_sequence,
                        "projection_set_id": value.projection_set_id,
                        "projection_set_version": value.projection_set_version,
                    })),
                    "items": response.items.into_iter().map(|item| json!({
                        "assertion_id": item.assertion_id,
                        "version_id": item.version_id,
                        "predicate": item.predicate,
                        "entity_key": item.entity_key,
                        "score": item.score,
                        "score_signals": item.score_signals,
                        "evidence_ids": item.evidence.into_iter()
                            .map(|evidence| evidence.evidence_id)
                            .collect::<Vec<_>>(),
                    })).collect::<Vec<_>>(),
                    "rendered_context": response.rendered_context,
                    "partial_status": response.partial_status,
                }))?
            );
        }
        MemoryCommand::Replay {
            snapshot_id,
            reexecute,
        } => {
            let body = MemoryReplayRecallRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    None,
                )),
                snapshot_id,
                mode: if reexecute {
                    MemoryReplayMode::Reexecute as i32
                } else {
                    MemoryReplayMode::ExactRetained as i32
                },
            };
            let response = client
                .replay_recall(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "replay_mode": response.replay_mode,
                    "exact_match": response.exact_match,
                    "comparison_status": response.comparison_status,
                    "mismatch_fields": response.mismatch_fields,
                    "expected_response_sha256": response.expected_response_sha256,
                    "actual_response_sha256": response.actual_response_sha256,
                    "artifacts_retained": response.artifacts_retained,
                    "snapshot_id": response.recall.as_ref().map(|recall| &recall.snapshot_id),
                    "rendered_context":
                        response.recall.as_ref().map(|recall| &recall.rendered_context),
                }))?
            );
        }
        MemoryCommand::History {
            assertion_id,
            from_sequence,
            to_sequence,
            limit,
        } => {
            let body = MemoryListHistoryRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    None,
                )),
                assertion_id,
                from_sequence,
                to_sequence,
                limit,
            };
            let response = client
                .list_history(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "found": response.found,
                    "assertion": response.assertion.map(|assertion| json!({
                        "assertion_id": assertion.assertion_id,
                        "entity_key": assertion.entity_key,
                        "predicate": assertion.predicate,
                        "kind": assertion.kind,
                    })),
                    "versions": response.versions.into_iter().map(|item| json!({
                        "version_id": item.version_id,
                        "state": item.state,
                        "committed_sequence": item.committed_sequence,
                        "evidence_ids": item.evidence.into_iter()
                            .map(|evidence| evidence.evidence_id)
                            .collect::<Vec<_>>(),
                    })).collect::<Vec<_>>(),
                    "lifecycle_transitions": response.lifecycle_transitions.into_iter()
                        .map(|transition| json!({
                            "version_id": transition.version_id,
                            "state": transition.state,
                            "sequence": transition.transition_sequence,
                        })).collect::<Vec<_>>(),
                    "mutations": response.mutations.into_iter().map(|mutation| json!({
                        "mutation_id": mutation.mutation_id,
                        "operation": mutation.operation,
                        "committed_sequence": mutation.committed_sequence,
                    })).collect::<Vec<_>>(),
                    "relations": response.relations.into_iter().map(|relation| json!({
                        "relation_id": relation.relation_id,
                        "kind": relation.kind,
                        "from_version_id": relation.from_version_id,
                        "to_version_id": relation.to_version_id,
                    })).collect::<Vec<_>>(),
                }))?
            );
        }
        MemoryCommand::Export { limit } => {
            let body = MemoryExportRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    None,
                )),
                limit,
            };
            let mut stream = client
                .export(memory_request(body, &token)?)
                .await?
                .into_inner();
            let mut records = Vec::new();
            while let Some(record) = stream.message().await? {
                let canonical_json =
                    serde_json::from_slice::<serde_json::Value>(&record.canonical_json)
                        .unwrap_or_else(|_| json!({"base16": hex_bytes(&record.canonical_json)}));
                records.push(json!({
                    "record_type": record.record_type,
                    "record_id": record.record_id,
                    "sha256": record.sha256,
                    "canonical_json": canonical_json,
                }));
            }
            println!("{}", serde_json::to_string_pretty(&records)?);
        }
        MemoryCommand::Reinforce {
            version_id,
            outcome,
            outcome_id,
            utility_micros,
            source_plane,
            source_id,
            evidence_sha256,
            idempotency_key,
            reason,
        } => {
            validate_sha256_argument("evidence-sha256", &evidence_sha256)?;
            let body = MemoryReinforceRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    Some(idempotency_key),
                )),
                version_id,
                evidence: vec![MemoryEvidenceInput {
                    source_plane,
                    source_id,
                    source_version: None,
                    observed_at_ms: None,
                    content_sha256: evidence_sha256,
                    source_principal_id: None,
                    observed_at_unix_nanos: None,
                }],
                outcome: parse_memory_reinforcement_outcome(&outcome)? as i32,
                outcome_id,
                utility_micros,
                reason,
            };
            let receipt = client
                .reinforce(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&mutation_receipt_json(&receipt))?
            );
        }
        MemoryCommand::PlanDeletion {
            source_plane,
            source_id,
            data_subject_id,
            expires_in_seconds,
            reason,
        } => {
            let selector = match (source_plane, source_id, data_subject_id) {
                (Some(source_plane), Some(source_id), None) => {
                    akidb_proto::memory_deletion_selector::Selector::Source(
                        MemorySourceDeletionSelector {
                            source_plane,
                            source_id,
                        },
                    )
                }
                (None, None, Some(data_subject_id)) => {
                    akidb_proto::memory_deletion_selector::Selector::DataSubjectId(data_subject_id)
                }
                _ => anyhow::bail!(
                    "select exactly one deletion mode: both --source-plane/--source-id, or --data-subject-id"
                ),
            };
            let body = MemoryPlanDeletionRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    None,
                )),
                selector: Some(MemoryDeletionSelector {
                    selector: Some(selector),
                }),
                reason,
                expires_in_seconds: Some(expires_in_seconds),
            };
            let plan = client
                .plan_deletion(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "plan_id": plan.plan_id,
                    "plan_sha256": plan.plan_sha256,
                    "created_sequence": plan.created_sequence,
                    "created_at_ms": plan.created_at_ms,
                    "expires_at_ms": plan.expires_at_ms,
                    "selector_type": plan.selector_type,
                    "total_affected_records": plan.total_affected_records,
                    "affected_assertion_ids": plan.affected_assertion_ids,
                    "affected_version_ids": plan.affected_version_ids,
                    "affected_evidence_ids": plan.affected_evidence_ids,
                    "affected_observation_ids": plan.affected_observation_ids,
                    "affected_reinforcement_ids": plan.affected_reinforcement_ids,
                    "affected_snapshot_ids": plan.affected_snapshot_ids,
                }))?
            );
        }
        MemoryCommand::ExecuteDeletion {
            plan_id,
            plan_sha256,
            idempotency_key,
            reason,
        } => {
            validate_sha256_argument("plan-sha256", &plan_sha256)?;
            let body = MemoryExecuteDeletionRequest {
                context: Some(memory_context(
                    &workspace,
                    &namespace,
                    &purpose,
                    delegated_agent.as_deref(),
                    Some(idempotency_key),
                )),
                plan_id,
                plan_sha256,
                reason,
            };
            let receipt = client
                .execute_deletion(memory_request(body, &token)?)
                .await?
                .into_inner();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "execution_id": receipt.execution_id,
                    "plan_id": receipt.plan_id,
                    "plan_sha256": receipt.plan_sha256,
                    "mutation_id": receipt.mutation_id,
                    "commit_sequence": receipt.commit_sequence,
                    "durability": receipt.durability,
                    "projection_status": receipt.projection_status,
                    "policy_decision_id": receipt.policy_decision_id,
                    "duplicate": receipt.duplicate,
                    "affected_assertion_ids": receipt.affected_assertion_ids,
                    "affected_version_ids": receipt.affected_version_ids,
                    "affected_evidence_ids": receipt.affected_evidence_ids,
                    "affected_observation_ids": receipt.affected_observation_ids,
                    "affected_reinforcement_ids": receipt.affected_reinforcement_ids,
                    "affected_snapshot_ids": receipt.affected_snapshot_ids,
                    "tombstone_ids": receipt.tombstone_ids,
                }))?
            );
        }
    }
    Ok(())
}

fn memory_token(path: &PathBuf) -> anyhow::Result<String> {
    let token = std::env::var("AKIDB_MEMORY_PRINCIPAL_TOKEN")
        .ok()
        .or_else(|| std::fs::read_to_string(path).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Memory principal token is unavailable; set AKIDB_MEMORY_PRINCIPAL_TOKEN or --token-file"
            )
        })?;
    Ok(token)
}

fn memory_context(
    workspace_id: &str,
    namespace: &str,
    request_purpose: &str,
    delegated_agent_id: Option<&str>,
    idempotency_key: Option<String>,
) -> MemoryRequestContext {
    MemoryRequestContext {
        workspace_id: workspace_id.to_string(),
        namespace: namespace.to_string(),
        request_purpose: request_purpose.to_string(),
        delegated_agent_id: delegated_agent_id.map(str::to_string),
        idempotency_key,
        request_id: None,
        scope_narrowing: None,
    }
}

fn memory_request<T>(body: T, token: &str) -> anyhow::Result<Request<T>> {
    let mut request = Request::new(body);
    let value: MetadataValue<Ascii> = format!("Bearer {token}").parse()?;
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

fn parse_memory_sensitivity(value: &str) -> anyhow::Result<MemorySensitivity> {
    match value.to_ascii_lowercase().as_str() {
        "public" => Ok(MemorySensitivity::Public),
        "internal" => Ok(MemorySensitivity::Internal),
        "confidential" => Ok(MemorySensitivity::Confidential),
        "restricted" => Ok(MemorySensitivity::Restricted),
        _ => Err(anyhow::anyhow!(
            "invalid sensitivity '{value}'; expected public, internal, confidential, or restricted"
        )),
    }
}

fn parse_memory_temporal_mode(value: &str) -> anyhow::Result<MemoryTemporalMode> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "current" => Ok(MemoryTemporalMode::Current),
        "valid_at" => Ok(MemoryTemporalMode::ValidAt),
        "system_as_of" => Ok(MemoryTemporalMode::SystemAsOf),
        "valid_at_as_known_at" => Ok(MemoryTemporalMode::ValidAtAsKnownAt),
        _ => Err(anyhow::anyhow!(
            "invalid temporal mode '{value}'; expected current, valid-at, system-as-of, or valid-at-as-known-at"
        )),
    }
}

fn parse_memory_reinforcement_outcome(value: &str) -> anyhow::Result<MemoryReinforcementOutcome> {
    match value.to_ascii_lowercase().as_str() {
        "succeeded" | "success" => Ok(MemoryReinforcementOutcome::Succeeded),
        "failed" | "failure" => Ok(MemoryReinforcementOutcome::Failed),
        "neutral" => Ok(MemoryReinforcementOutcome::Neutral),
        _ => Err(anyhow::anyhow!(
            "invalid reinforcement outcome '{value}'; expected succeeded, failed, or neutral"
        )),
    }
}

fn validate_sha256_argument(name: &str, value: &str) -> anyhow::Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        anyhow::bail!("--{name} must be a lowercase hexadecimal SHA-256 digest");
    }
    Ok(())
}

fn mutation_receipt_json(receipt: &akidb_proto::MemoryMutationReceipt) -> serde_json::Value {
    json!({
        "mutation_id": receipt.mutation_id,
        "assertion_id": receipt.assertion_id,
        "version_ids": receipt.version_ids,
        "commit_sequence": receipt.commit_sequence,
        "durability": receipt.durability,
        "projection_status": receipt.projection_status,
        "visible_sequence": receipt.visibility.as_ref().map(|value| value.visible_sequence),
        "policy_decision_id": receipt.policy_decision_id,
        "duplicate": receipt.duplicate,
        "version_state": receipt.version_state,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

async fn run_ops(args: OpsArgs) -> anyhow::Result<()> {
    use akidb_tui::client::OperationsClient;
    use akidb_tui::model::ImportPlanInput;

    let mut client = OperationsClient::connect(&args.management).await?;
    let output = match args.command {
        OpsCommand::Capabilities => {
            let value = client.capabilities().await?;
            json!({
                "server_version": value.server_version,
                "management_api_version": value.api_version,
                "workspace_id": value.workspace_id,
                "agent_id": value.agent_id,
                "authenticated": value.authenticated,
                "tls_active": value.tls_active,
                "auth_mode": value.auth_mode,
                "credential_source": value.credential_source,
                "capabilities": value.capabilities.into_iter().map(|capability| json!({
                    "name": capability.name,
                    "supported": capability.supported,
                    "authorized": capability.authorized,
                    "unavailable_reason": capability.unavailable_reason,
                })).collect::<Vec<_>>(),
            })
        }
        OpsCommand::Collections => {
            let values = client.list_collections().await?;
            json!(values
                .into_iter()
                .map(|value| json!({
                    "name": value.name,
                    "dimensions": value.dimensions,
                    "metric": value.metric,
                    "embedding_model_id": value.embedding_model_id,
                    "vector_precision": value.vector_precision,
                    "chunk_strategy": value.chunk_strategy,
                    "vector_count": value.vector_count,
                }))
                .collect::<Vec<_>>())
        }
        OpsCommand::Operations => {
            let values = client.list_operations().await?;
            json!(values
                .into_iter()
                .map(|value| json!({
                    "operation_id": value.id,
                    "type": value.operation_type,
                    "state": value.state,
                    "target": value.target,
                    "progress_percent": value.progress_percent,
                    "updated_at_ms": value.updated_at_ms,
                    "items_processed": value.items_processed,
                    "bytes_processed": value.bytes_processed,
                    "problem": value.problem,
                }))
                .collect::<Vec<_>>())
        }
        OpsCommand::Snapshots => {
            let values = client.list_snapshots().await?;
            json!(values
                .into_iter()
                .map(|value| json!({
                    "snapshot_id": value.id,
                    "collection": value.collection,
                    "created_at_ms": value.created_at_ms,
                    "size_bytes": value.size_bytes,
                    "manifest_present": value.manifest_present,
                    "verification_state": value.verification_state,
                    "restore_test_state": value.restore_test_state,
                }))
                .collect::<Vec<_>>())
        }
        OpsCommand::Audit => {
            let value = client.list_audit().await?;
            json!({
                "retention_notice": value.retention_notice,
                "integrity_status": value.integrity_status,
                "events": value.events.into_iter().map(|event| json!({
                    "occurred_at_ms": event.occurred_at_ms,
                    "actor_id": event.actor_id,
                    "action": event.action,
                    "target": event.target,
                    "outcome": event.outcome,
                    "reason_code": event.reason_code,
                    "request_id": event.request_id,
                })).collect::<Vec<_>>(),
            })
        }
        OpsCommand::PlanImport {
            staging_id,
            object_id,
            etag,
            size_bytes,
            collection,
            duplicate_policy,
        } => {
            let value = client
                .plan_import(ImportPlanInput {
                    staging_id,
                    object_id,
                    etag,
                    size_bytes,
                    collection,
                    duplicate_policy,
                })
                .await?;
            json!({
                "plan_id": value.plan_id,
                "plan_hash": value.plan_hash,
                "target_id": value.target_id,
                "workspace_id": value.workspace_id,
                "source_fingerprint": value.source_fingerprint,
                "source_bytes": value.source_bytes,
                "estimated_expanded_bytes": value.estimated_expanded_bytes,
                "estimated_documents": value.estimated_documents,
                "estimated_chunks": value.estimated_chunks,
                "estimated_vectors": value.estimated_vectors,
                "expires_at_ms": value.expires_at_ms,
                "executable": value.executable,
                "findings": value.findings.into_iter().map(|finding| json!({
                    "severity": finding.severity,
                    "code": finding.code,
                    "message": finding.message,
                })).collect::<Vec<_>>(),
            })
        }
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn run_graph(args: GraphArgs) -> anyhow::Result<()> {
    let storage = Arc::new(RocksDbBackend::open(&args.rocksdb)?);
    let graph = NativeGraphIndex::new(storage);

    match args.command {
        GraphCommand::Stats => {
            let stats = graph.stats()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "nodes": stats.nodes,
                    "edges": stats.edges,
                    "chunk_links": stats.chunk_links,
                }))?
            );
        }
        GraphCommand::Neighbors {
            node_id,
            direction,
            edge_kinds,
            limit,
        } => {
            let edge_kinds = parse_edge_kinds(&edge_kinds)?;
            let neighbors = graph.neighbors(
                NeighborRequest::new(GraphNodeId::new(node_id))
                    .with_direction(parse_direction(&direction)?)
                    .with_edge_kinds(edge_kinds)
                    .with_limit(limit),
            )?;
            let rows: Vec<_> = neighbors
                .into_iter()
                .map(|n| {
                    json!({
                        "node_id": n.node.id.as_str(),
                        "node_kind": n.node.kind.as_key(),
                        "edge_id": n.edge.id.as_str(),
                        "edge_kind": n.edge.kind.as_key(),
                        "from": n.edge.from.as_str(),
                        "to": n.edge.to.as_str(),
                        "weight": n.edge.weight,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        GraphCommand::RelatedChunks { entity_id, limit } => {
            let chunks = graph.related_chunks(&GraphNodeId::new(entity_id), limit)?;
            let rows: Vec<_> = chunks
                .into_iter()
                .map(|c| {
                    json!({
                        "vector_id": c.vector_id.as_str(),
                        "via_node": c.via_node.as_str(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
    }

    Ok(())
}

fn parse_direction(direction: &str) -> anyhow::Result<Direction> {
    match direction.to_ascii_lowercase().as_str() {
        "out" | "outgoing" => Ok(Direction::Out),
        "in" | "incoming" => Ok(Direction::In),
        "both" => Ok(Direction::Both),
        _ => Err(anyhow::anyhow!(
            "invalid direction '{direction}'; expected out, in, or both"
        )),
    }
}

fn parse_edge_kinds(values: &[String]) -> anyhow::Result<Vec<EdgeKind>> {
    values.iter().map(|v| parse_edge_kind(v)).collect()
}

fn parse_edge_kind(value: &str) -> anyhow::Result<EdgeKind> {
    match value.to_ascii_lowercase().as_str() {
        "parent_of" | "parent-of" => Ok(EdgeKind::ParentOf),
        "child_of" | "child-of" => Ok(EdgeKind::ChildOf),
        "contains" => Ok(EdgeKind::Contains),
        "mentions" => Ok(EdgeKind::Mentions),
        "imports" => Ok(EdgeKind::Imports),
        "calls" => Ok(EdgeKind::Calls),
        "implements" => Ok(EdgeKind::Implements),
        "tests" => Ok(EdgeKind::Tests),
        "tested_by" | "tested-by" => Ok(EdgeKind::TestedBy),
        "depends_on" | "depends-on" => Ok(EdgeKind::DependsOn),
        "owned_by" | "owned-by" => Ok(EdgeKind::OwnedBy),
        "changed_by" | "changed-by" => Ok(EdgeKind::ChangedBy),
        "related_to" | "related-to" => Ok(EdgeKind::RelatedTo),
        _ => Err(anyhow::anyhow!("invalid edge kind '{value}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_direction() {
        assert_eq!(parse_direction("out").unwrap(), Direction::Out);
        assert_eq!(parse_direction("incoming").unwrap(), Direction::In);
        assert_eq!(parse_direction("both").unwrap(), Direction::Both);
        assert!(parse_direction("sideways").is_err());
    }

    #[test]
    fn test_parse_edge_kind_aliases() {
        assert_eq!(parse_edge_kind("calls").unwrap(), EdgeKind::Calls);
        assert_eq!(parse_edge_kind("depends-on").unwrap(), EdgeKind::DependsOn);
        assert_eq!(parse_edge_kind("related_to").unwrap(), EdgeKind::RelatedTo);
        assert!(parse_edge_kind("unknown").is_err());
    }

    #[test]
    fn test_parse_memory_temporal_modes_and_sensitivity() {
        assert_eq!(
            parse_memory_temporal_mode("current").unwrap(),
            MemoryTemporalMode::Current
        );
        assert_eq!(
            parse_memory_temporal_mode("valid-at-as-known-at").unwrap(),
            MemoryTemporalMode::ValidAtAsKnownAt
        );
        assert!(parse_memory_temporal_mode("latest-ish").is_err());

        assert_eq!(
            parse_memory_sensitivity("CONFIDENTIAL").unwrap(),
            MemorySensitivity::Confidential
        );
        assert!(parse_memory_sensitivity("secret").is_err());
    }

    #[test]
    fn test_memory_cli_parses_reexecute_and_temporal_recall() {
        let cli = Cli::try_parse_from([
            "akidb",
            "memory",
            "--workspace",
            "workspace-a",
            "--namespace",
            "agents",
            "recall",
            "--query",
            "shipping address",
            "--temporal-mode",
            "valid-at-as-known-at",
            "--valid-at-unix-nanos",
            "1710000000000000000",
            "--commit-sequence",
            "42",
        ])
        .unwrap();
        let Command::Memory(memory) = cli.command else {
            panic!("expected Memory command");
        };
        let MemoryCommand::Recall {
            temporal_mode,
            valid_at_unix_nanos,
            commit_sequence,
            ..
        } = memory.command
        else {
            panic!("expected recall command");
        };
        assert_eq!(temporal_mode, "valid-at-as-known-at");
        assert_eq!(valid_at_unix_nanos, Some(1_710_000_000_000_000_000));
        assert_eq!(commit_sequence, Some(42));

        let replay = Cli::try_parse_from([
            "akidb",
            "memory",
            "--workspace",
            "workspace-a",
            "--namespace",
            "agents",
            "replay",
            "snapshot-1",
            "--reexecute",
        ])
        .unwrap();
        let Command::Memory(memory) = replay.command else {
            panic!("expected Memory command");
        };
        assert!(matches!(
            memory.command,
            MemoryCommand::Replay {
                snapshot_id,
                reexecute: true,
            } if snapshot_id == "snapshot-1"
        ));
    }

    #[test]
    fn test_memory_evidence_digest_is_stable() {
        assert_eq!(
            sha256_hex(b"authoritative memory"),
            "1b8b141445ef027a7e77105eb7b25dd6c3f92976b4ee828d891fde013c1da440"
        );
    }

    #[test]
    fn test_memory_cli_parses_reinforcement_and_deletion_commands() {
        let reinforce = Cli::try_parse_from([
            "akidb",
            "memory",
            "--workspace",
            "workspace-a",
            "--namespace",
            "agents",
            "reinforce",
            "version-1",
            "--outcome",
            "succeeded",
            "--outcome-id",
            "run-1",
            "--utility-micros",
            "750000",
            "--source-plane",
            "task-run",
            "--source-id",
            "run-1",
            "--evidence-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--idempotency-key",
            "reinforce-1",
            "--reason",
            "task succeeded",
        ])
        .unwrap();
        let Command::Memory(memory) = reinforce.command else {
            panic!("expected Memory command");
        };
        assert!(matches!(
            memory.command,
            MemoryCommand::Reinforce {
                version_id,
                utility_micros: 750_000,
                ..
            } if version_id == "version-1"
        ));

        let plan = Cli::try_parse_from([
            "akidb",
            "memory",
            "--workspace",
            "workspace-a",
            "--namespace",
            "agents",
            "plan-deletion",
            "--data-subject-id",
            "subject-1",
            "--reason",
            "privacy request",
        ])
        .unwrap();
        let Command::Memory(memory) = plan.command else {
            panic!("expected Memory command");
        };
        assert!(matches!(
            memory.command,
            MemoryCommand::PlanDeletion {
                data_subject_id: Some(subject),
                ..
            } if subject == "subject-1"
        ));

        let execute = Cli::try_parse_from([
            "akidb",
            "memory",
            "--workspace",
            "workspace-a",
            "--namespace",
            "agents",
            "execute-deletion",
            "plan-1",
            "--plan-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--idempotency-key",
            "delete-1",
            "--reason",
            "reviewed",
        ])
        .unwrap();
        let Command::Memory(memory) = execute.command else {
            panic!("expected Memory command");
        };
        assert!(matches!(
            memory.command,
            MemoryCommand::ExecuteDeletion { plan_id, .. } if plan_id == "plan-1"
        ));
    }
}
