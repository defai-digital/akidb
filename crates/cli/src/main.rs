use clap::{Parser, Subcommand};

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
    }
}
