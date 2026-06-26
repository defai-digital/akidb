//! AkiDB TUI Dashboard
//!
//! Terminal User Interface for monitoring AkiDB deployments.

use std::io;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use std::time::Duration;

use akidb_tui::{
    client::CoordinatorClient,
    config::TuiConfig,
    events::{handle_key_event, Event, EventHandler},
    ui, App,
};

/// AkiDB TUI Dashboard - Monitor your AkiDB deployment
#[derive(Parser, Debug)]
#[command(name = "akidb-tui")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Coordinator address to connect to (e.g., 192.168.1.61:50050)
    #[arg(long)]
    coordinator: Option<String>,

    /// Use mock data for testing
    #[arg(long)]
    mock: bool,

    /// Refresh interval in milliseconds
    #[arg(long, default_value = "500")]
    refresh: u64,

    /// Theme: default, minimal, high-contrast
    #[arg(long, default_value = "default")]
    theme: String,

    /// Log level: trace, debug, info, warn, error
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Test connection to coordinator and print cluster state (no TUI)
    #[arg(long)]
    test_connection: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging (to file, not stdout since we're using the terminal)
    let log_level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // Disable ANSI colors in logs to avoid terminal escape code issues
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting AkiDB TUI Dashboard");

    // Load configuration
    let mut config = if let Some(path) = &args.config {
        TuiConfig::load(Some(path))?
    } else {
        TuiConfig::load_default()?
    };

    // Override with CLI arguments
    if let Some(coordinator) = args.coordinator.clone() {
        config.coordinator_address = Some(coordinator);
    }
    if args.mock {
        config.mock_mode = true;
    }
    config.refresh_interval_ms = args.refresh;
    config.theme.name = args.theme;

    // If test_connection mode, just test and print results
    if args.test_connection {
        return test_connection(args.coordinator).await;
    }

    // Run the TUI
    let result = run_tui(config).await;

    // Ensure terminal is restored even on error
    if let Err(e) = &result {
        eprintln!("Error: {}", e);
    }

    result
}

/// Run the TUI application
async fn run_tui(config: TuiConfig) -> Result<()> {
    // Check if we have a terminal
    if !atty::is(atty::Stream::Stdout) {
        return Err(anyhow::anyhow!(
            "TUI requires an interactive terminal.\n\
             Please run directly via SSH: ssh devop@192.168.1.61\n\
             Then run: akidb-tui --coordinator 127.0.0.1:50050\n\
             Or use --test-connection to test without a terminal."
        ));
    }

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create application state
    let mut app = if config.mock_mode {
        App::with_mock_data(config)
    } else {
        App::new(config)
    };

    // Create event handler
    let mut events = EventHandler::new(app.tick_rate);

    // Try to connect to coordinator
    let mut coordinator_client: Option<CoordinatorClient> = None;
    if !app.config.mock_mode {
        // Build list of addresses to try
        let addresses_to_try: Vec<String> = if let Some(addr) = app.config.coordinator_address.clone() {
            vec![addr]
        } else {
            // Auto-discovery: use addresses from config
            app.config.discovery_addresses.clone()
        };

        app.set_status("Discovering coordinator...");

        for addr in &addresses_to_try {
            info!("Trying coordinator at {}", addr);

            match tokio::time::timeout(
                Duration::from_secs(2),
                CoordinatorClient::connect(addr),
            )
            .await
            {
                Ok(Ok(client)) => {
                    info!("Connected to coordinator at {}", addr);
                    app.set_status(format!("Connected to {}", addr));
                    coordinator_client = Some(client);
                    break;
                }
                Ok(Err(e)) => {
                    info!("Failed to connect to {}: {}", addr, e);
                }
                Err(_) => {
                    info!("Connection to {} timed out", addr);
                }
            }
        }

        if coordinator_client.is_none() {
            app.set_status("No coordinator found - running in offline mode");
        }
    }

    // Main event loop
    loop {
        // Clear expired status messages
        app.clear_expired_status();

        // Draw the UI
        terminal.draw(|frame| ui::draw(frame, &app))?;

        // Handle events
        match events.next().await {
            Some(Event::Tick) => {
                if app.config.mock_mode {
                    // Update mock data for testing
                    update_mock_data(&mut app);
                } else if let Some(ref mut client) = coordinator_client {
                    // Fetch cluster state from coordinator
                    match client.get_cluster_state().await {
                        Ok((cluster_state, metrics)) => {
                            app.cluster_state = cluster_state;
                            // Preserve history but update current metrics
                            app.metrics.qps = metrics.qps;
                            app.metrics.p50_latency_ms = metrics.p50_latency_ms;
                            app.metrics.p95_latency_ms = metrics.p95_latency_ms;
                            app.metrics.p99_latency_ms = metrics.p99_latency_ms;
                            app.metrics.coverage = metrics.coverage;
                            app.metrics.backpressure = metrics.backpressure;
                            app.metrics.within_slo = metrics.within_slo;
                            // Update history
                            app.metrics.history.add_qps(metrics.qps);
                            app.metrics.history.add_latency(metrics.p50_latency_ms);
                        }
                        Err(e) => {
                            // Log error but don't spam status bar
                            tracing::debug!("Failed to fetch cluster state: {}", e);
                        }
                    }
                }
            }
            Some(Event::Key(key)) => {
                if handle_key_event(&mut app, key) {
                    break;
                }
            }
            Some(Event::ClusterUpdate(state)) => {
                app.cluster_state = state;
            }
            Some(Event::MetricsUpdate(metrics)) => {
                app.metrics = metrics;
            }
            Some(Event::Resize(_, _)) => {
                // Terminal will handle resize automatically
            }
            None => {
                break;
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    info!("AkiDB TUI Dashboard shutdown complete");

    Ok(())
}

/// Update mock data to simulate live updates
fn update_mock_data(app: &mut App) {
    use std::time::Instant;

    // Slightly vary QPS
    let jitter = (rand_simple() - 0.5) * 10.0;
    app.metrics.qps = (app.metrics.qps + jitter).max(50.0).min(200.0);

    // Add to history
    app.metrics.history.add_qps(app.metrics.qps);
    app.metrics.history.add_latency(app.metrics.p50_latency_ms);

    // Update shard health slightly
    for shard in &mut app.cluster_state.shards {
        let health_jitter = (rand_simple() - 0.5) * 0.02;
        shard.health_score = (shard.health_score + health_jitter as f32)
            .max(0.8)
            .min(1.0);
        app.metrics
            .history
            .add_shard_health(&shard.id, shard.health_score);
    }

    // Update coordinator timestamps
    for coord in &mut app.cluster_state.coordinators {
        coord.last_seen = Instant::now();
    }

    app.cluster_state.last_update = Some(Instant::now());
}

/// Simple pseudo-random number generator (0.0 to 1.0)
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    ((seed as f64 * 1.1) % 1000.0) / 1000.0
}

/// Test connection to coordinator and print cluster state
async fn test_connection(coordinator: Option<String>) -> Result<()> {
    let addr = coordinator.ok_or_else(|| anyhow::anyhow!("--coordinator address required for --test-connection"))?;

    println!("Testing connection to coordinator at {}...", addr);

    match CoordinatorClient::connect(&addr).await {
        Ok(mut client) => {
            println!("✓ Connected successfully!\n");

            match client.get_cluster_state().await {
                Ok((cluster_state, metrics)) => {
                    println!("=== Cluster State ===");
                    println!("Leader ID: {:?}", cluster_state.leader_id);
                    println!("Local Peer ID: {:?}", cluster_state.local_peer_id);

                    println!("\nCoordinators ({}):", cluster_state.coordinators.len());
                    for coord in &cluster_state.coordinators {
                        println!("  • {} at {} (leader={}, self={}, status={:?})",
                            coord.id, coord.address, coord.is_leader, coord.is_self, coord.status);
                    }

                    println!("\nShards ({}):", cluster_state.shards.len());
                    for shard in &cluster_state.shards {
                        println!("  • {} at {} (vectors={}, health={:.0}%, status={:?})",
                            shard.id, shard.address, shard.vector_count,
                            shard.health_score * 100.0, shard.status);
                    }

                    println!("\nMetrics:");
                    println!("  QPS: {:.1}", metrics.qps);
                    println!("  Coverage: {:.1}%", metrics.coverage * 100.0);
                    println!("  Backpressure: {:.1}%", metrics.backpressure * 100.0);
                    println!("  Within SLO: {}", metrics.within_slo);

                    println!("\n✓ GetClusterState RPC working correctly!");
                }
                Err(e) => {
                    println!("✗ GetClusterState RPC failed: {}", e);
                    return Err(anyhow::anyhow!("GetClusterState failed: {}", e));
                }
            }
        }
        Err(e) => {
            println!("✗ Connection failed: {}", e);
            return Err(anyhow::anyhow!("Connection failed: {}", e));
        }
    }

    Ok(())
}
