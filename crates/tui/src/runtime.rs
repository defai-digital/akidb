//! AkiDB TUI Dashboard
//!
//! Terminal User Interface for monitoring AkiDB deployments.

use std::io::{self, IsTerminal};
use std::path::PathBuf;

use anyhow::Result;
use crossterm::{
    cursor,
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use std::time::Duration;

use crate::{
    action::Action,
    client::{CoordinatorClient, OperationsClient},
    config::TuiConfig,
    effect::Effect,
    events::{handle_key_event, Event, EventHandler},
    ui, App,
};

/// Best-effort terminal restoration on normal return, error, or unwinding.
struct TerminalRestoreGuard;

impl Drop for TerminalRestoreGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(
            stdout,
            LeaveAlternateScreen,
            DisableMouseCapture,
            cursor::Show
        );
    }
}

/// AkiDB TUI Dashboard - Monitor your AkiDB deployment
#[derive(clap::Args, Debug)]
pub struct Args {
    /// Configuration file path
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Coordinator address to connect to (e.g., 127.0.0.1:50050)
    #[arg(long)]
    pub coordinator: Option<String>,

    /// Shard management address (defaults to 127.0.0.1:50051)
    #[arg(long)]
    pub management: Option<String>,

    /// Use mock data for testing
    #[arg(long)]
    pub mock: bool,

    /// Refresh interval in milliseconds
    #[arg(long, default_value = "500")]
    pub refresh: u64,

    /// Theme: default, minimal, high-contrast
    #[arg(long, default_value = "default")]
    pub theme: String,

    /// Log level: trace, debug, info, warn, error
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Test connection to coordinator and print cluster state (no TUI)
    #[arg(long)]
    pub test_connection: bool,
}

pub async fn run(args: Args) -> Result<()> {
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
    if let Some(management) = args.management.clone() {
        config.management_address = Some(management);
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
    if !io::stdout().is_terminal() {
        return Err(anyhow::anyhow!(
            "TUI requires an interactive terminal.\n\
             Run locally on the Mac host: akidb tui --coordinator 127.0.0.1:50050\n\
             Or use --test-connection to test without a terminal."
        ));
    }

    // Setup terminal
    enable_raw_mode()?;
    let _restore_guard = TerminalRestoreGuard;
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
        let addresses_to_try: Vec<String> =
            if let Some(addr) = app.config.coordinator_address.clone() {
                vec![addr]
            } else {
                // Auto-discovery: use addresses from config
                app.config.discovery_addresses.clone()
            };

        app.set_status("Discovering coordinator...");

        for addr in &addresses_to_try {
            info!("Trying coordinator at {}", addr);

            match tokio::time::timeout(Duration::from_secs(2), CoordinatorClient::connect(addr))
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

    let cluster_refresh = coordinator_client.take().map(|mut client| {
        let (refresh_tx, mut refresh_rx) = mpsc::channel(1);
        let event_tx = events.sender();
        tokio::spawn(async move {
            while refresh_rx.recv().await.is_some() {
                match client.get_cluster_state().await {
                    Ok((cluster_state, metrics)) => {
                        if event_tx.send(Event::ClusterUpdate(cluster_state)).is_err()
                            || event_tx.send(Event::MetricsUpdate(metrics)).is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%error, "failed to fetch cluster state");
                    }
                }
            }
        });
        refresh_tx
    });

    // The coordinator does not yet aggregate management metadata, so the
    // Operations Console connects to the shard's authenticated read/plan API.
    // One bounded worker owns the client and serializes effects; rendering and
    // keyboard input never perform management RPCs.
    let mut management_effects: Option<mpsc::Sender<Effect>> = None;
    if !app.config.mock_mode {
        let management_address = app
            .config
            .management_address
            .clone()
            .unwrap_or_else(|| "127.0.0.1:50051".to_string());
        match tokio::time::timeout(
            Duration::from_secs(2),
            OperationsClient::connect(&management_address),
        )
        .await
        {
            Ok(Ok(client)) => {
                let (effect_tx, effect_rx) = mpsc::channel(8);
                let event_tx = events.sender();
                tokio::spawn(run_management_worker(client, effect_rx, event_tx));
                management_effects = Some(effect_tx);
                app.queue_initial_effects();
                app.set_status(format!("Management connected: {management_address}"));
            }
            Ok(Err(error)) => {
                let message = format!("management endpoint unavailable: {error}");
                app.update(Action::CapabilitiesLoaded(Err(message.clone())));
                app.update(Action::CollectionsLoaded(Err(message.clone())));
                app.update(Action::OperationsLoaded(Err(message.clone())));
                app.update(Action::SnapshotsLoaded(Err(message.clone())));
                app.update(Action::AuditLoaded(Err(message)));
            }
            Err(_) => {
                app.update(Action::CapabilitiesLoaded(Err(
                    "management connection timed out".to_string(),
                )));
            }
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
                } else if let Some(refresh) = &cluster_refresh {
                    // Capacity one guarantees at most one queued refresh while
                    // the worker owns the only in-flight cluster request.
                    let _ = refresh.try_send(());
                }
                app.queue_due_refresh();
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
                app.metrics.qps = metrics.qps;
                app.metrics.p50_latency_ms = metrics.p50_latency_ms;
                app.metrics.p95_latency_ms = metrics.p95_latency_ms;
                app.metrics.p99_latency_ms = metrics.p99_latency_ms;
                app.metrics.coverage = metrics.coverage;
                app.metrics.backpressure = metrics.backpressure;
                app.metrics.within_slo = metrics.within_slo;
                app.metrics.history.add_qps(metrics.qps);
                app.metrics.history.add_latency(metrics.p50_latency_ms);
            }
            Some(Event::ConsoleAction(action)) => {
                app.update(action);
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

        let mut management_closed = false;
        if let Some(effect_tx) = &management_effects {
            while let Some(effect) = app.take_effect() {
                match effect_tx.try_send(effect.clone()) {
                    Ok(()) => app.mark_loading(&effect),
                    Err(mpsc::error::TrySendError::Full(effect)) => {
                        app.queue_effect(effect);
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        app.set_status("Management connection closed");
                        management_closed = true;
                        break;
                    }
                }
            }
        }
        if management_closed {
            management_effects = None;
        }
    }

    // The restoration guard handles raw mode and alternate-screen cleanup even
    // when an earlier draw or RPC path returns an error.
    terminal.show_cursor()?;

    info!("AkiDB TUI Dashboard shutdown complete");

    Ok(())
}

async fn run_management_worker(
    mut client: OperationsClient,
    mut effects: mpsc::Receiver<Effect>,
    events: mpsc::UnboundedSender<Event>,
) {
    while let Some(effect) = effects.recv().await {
        if !effect.is_read_or_validate_only() {
            continue;
        }
        let action = match effect {
            Effect::LoadCapabilities => {
                Action::CapabilitiesLoaded(client.capabilities().await.map_err(status_message))
            }
            Effect::LoadCollections => {
                Action::CollectionsLoaded(client.list_collections().await.map_err(status_message))
            }
            Effect::LoadOperations => {
                Action::OperationsLoaded(client.list_operations().await.map_err(status_message))
            }
            Effect::LoadSnapshots => {
                Action::SnapshotsLoaded(client.list_snapshots().await.map_err(status_message))
            }
            Effect::PlanImport(input) => {
                Action::ImportPlanLoaded(client.plan_import(input).await.map_err(status_message))
            }
            Effect::LoadAudit => {
                Action::AuditLoaded(client.list_audit().await.map_err(status_message))
            }
        };
        if events.send(Event::ConsoleAction(action)).is_err() {
            break;
        }
    }
}

fn status_message(status: tonic::Status) -> String {
    format!("{:?}: {}", status.code(), status.message())
}

/// Update mock data to simulate live updates
fn update_mock_data(app: &mut App) {
    use std::time::Instant;

    // Slightly vary QPS
    let jitter = (rand_simple() - 0.5) * 10.0;
    app.metrics.qps = (app.metrics.qps + jitter).clamp(50.0, 200.0);

    // Add to history
    app.metrics.history.add_qps(app.metrics.qps);
    app.metrics.history.add_latency(app.metrics.p50_latency_ms);

    // Update shard health slightly
    for shard in &mut app.cluster_state.shards {
        let health_jitter = (rand_simple() - 0.5) * 0.02;
        shard.health_score = (shard.health_score + health_jitter as f32).clamp(0.8, 1.0);
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
    let addr = coordinator
        .ok_or_else(|| anyhow::anyhow!("--coordinator address required for --test-connection"))?;

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
                        println!(
                            "  • {} at {} (leader={}, self={}, status={:?})",
                            coord.id, coord.address, coord.is_leader, coord.is_self, coord.status
                        );
                    }

                    println!("\nShards ({}):", cluster_state.shards.len());
                    for shard in &cluster_state.shards {
                        println!(
                            "  • {} at {} (vectors={}, health={:.0}%, status={:?})",
                            shard.id,
                            shard.address,
                            shard.vector_count,
                            shard.health_score * 100.0,
                            shard.status
                        );
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
