use clap::Parser;

/// AkiDB Coordinator Server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    args: akidb_coordinator::ServerArgs,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    akidb_coordinator::run_server(cli.args).await
}
