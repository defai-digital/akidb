use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

use akidb_graph::{
    Direction, EdgeKind, GraphIndex, GraphNodeId, NativeGraphIndex, NeighborRequest,
};
use akidb_storage::RocksDbBackend;
use serde_json::json;

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

    /// Inspect the local native graph index stored in RocksDB.
    Graph(GraphArgs),
}

#[derive(Parser, Debug)]
struct GraphArgs {
    /// RocksDB path used by the AkiDB server.
    #[arg(long, default_value = "/opt/akidb/data/rocksdb")]
    rocksdb: PathBuf,

    #[command(subcommand)]
    command: GraphCommand,
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
        Command::Graph(args) => run_graph(args),
    }
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
