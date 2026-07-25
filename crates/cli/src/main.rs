use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use akidb_graph::{
    Direction, EdgeKind, GraphIndex, GraphNodeId, NativeGraphIndex, NeighborRequest,
};
use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::HealthRequest;
use akidb_storage::RocksDbBackend;
use serde_json::json;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Endpoint;
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
    }
}

async fn run_health(args: HealthArgs) -> anyhow::Result<()> {
    let endpoint = if args.server.starts_with("http://") || args.server.starts_with("https://") {
        args.server.clone()
    } else {
        format!("http://{}", args.server)
    };
    let timeout = Duration::from_secs(args.timeout_seconds);
    let channel = Endpoint::from_shared(endpoint)?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await?;
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
}
