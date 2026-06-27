use clap::Parser;

/// AkiDB TUI Dashboard - Monitor your AkiDB deployment
#[derive(Parser, Debug)]
#[command(name = "akidb-tui")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    args: akidb_tui::Args,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    akidb_tui::run(cli.args).await
}
