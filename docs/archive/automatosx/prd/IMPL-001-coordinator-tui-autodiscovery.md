# Implementation Plan: AkiDB Coordinator TUI and Auto-Discovery

**Version:** 1.0
**Date:** 2026-01-22
**Related PRD:** PRD-001
**Related ADR:** ADR-001

---

## Executive Summary

This document provides a detailed implementation plan for the AkiDB Coordinator TUI Dashboard and Auto-Discovery features. The implementation is divided into 4 phases over 6 weeks.

---

## Phase 1: TUI Dashboard Foundation (Week 1-2)

### Objective
Create a new `akidb-tui` crate with basic dashboard displaying cluster information.

### Tasks

#### 1.1 Create akidb-tui Crate Structure
**Priority:** P0 | **Estimate:** 2 hours

```bash
crates/akidb-tui/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── main.rs           # Standalone binary
│   ├── app.rs            # Application state
│   ├── config.rs         # TUI configuration
│   ├── ui/
│   │   ├── mod.rs
│   │   ├── layout.rs     # Main layout
│   │   ├── topology.rs   # Topology panel
│   │   ├── metrics.rs    # Metrics panel
│   │   ├── health.rs     # Health sparklines
│   │   └── controls.rs   # Control bar
│   ├── events.rs         # Keyboard/tick events
│   └── theme.rs          # Color themes
```

**Cargo.toml dependencies:**
```toml
[dependencies]
ratatui = "0.28"
crossterm = "0.28"
tokio = { version = "1", features = ["full"] }
tokio-util = "0.7"
akidb-common = { path = "../akidb-common" }
akidb-grpc = { path = "../grpc-server" }
serde = { version = "1", features = ["derive"] }
toml = "0.8"
tracing = "0.1"
```

#### 1.2 Implement Application State
**Priority:** P0 | **Estimate:** 3 hours

```rust
// src/app.rs
pub struct App {
    pub cluster_state: ClusterState,
    pub metrics: MetricsState,
    pub selected_panel: Panel,
    pub should_quit: bool,
    pub tick_rate: Duration,
}

pub struct ClusterState {
    pub coordinators: Vec<CoordinatorInfo>,
    pub shards: Vec<ShardInfo>,
    pub leader_id: Option<String>,
}

pub struct CoordinatorInfo {
    pub id: String,
    pub peer_id: String,
    pub address: String,
    pub is_leader: bool,
    pub is_self: bool,
    pub last_seen: Instant,
    pub status: NodeStatus,
}

pub struct ShardInfo {
    pub id: String,
    pub address: String,
    pub vector_count: u64,
    pub health_score: f32,
    pub gpu_memory_percent: Option<f32>,
    pub temperature: Option<f32>,
    pub status: NodeStatus,
}

pub struct MetricsState {
    pub qps: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub coverage: f32,
    pub backpressure: f32,
    pub within_slo: bool,
    pub history: MetricsHistory,
}
```

#### 1.3 Implement Main Layout
**Priority:** P0 | **Estimate:** 4 hours

```rust
// src/ui/layout.rs
pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),   // Header
            Constraint::Min(10),     // Main content
            Constraint::Length(3),   // Metrics bar
            Constraint::Length(1),   // Controls
        ])
        .split(frame.size());

    draw_header(frame, chunks[0], app);
    draw_main_content(frame, chunks[1], app);
    draw_metrics_bar(frame, chunks[2], app);
    draw_controls(frame, chunks[3], app);
}

fn draw_main_content(frame: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(40),  // Topology
            Constraint::Percentage(60),  // Health
        ])
        .split(area);

    topology::draw(frame, chunks[0], app);
    health::draw(frame, chunks[1], app);
}
```

#### 1.4 Implement Topology Panel
**Priority:** P0 | **Estimate:** 3 hours

```rust
// src/ui/topology.rs
pub fn draw(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .title(" Cluster Topology ")
        .borders(Borders::ALL);

    let items: Vec<ListItem> = build_topology_items(app);

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));

    frame.render_widget(list, area);
}

fn build_topology_items(app: &App) -> Vec<ListItem> {
    let mut items = vec![];

    // Coordinators section
    items.push(ListItem::new("─ Coordinators"));
    for coord in &app.cluster_state.coordinators {
        let status_icon = match coord.status {
            NodeStatus::Healthy => "●",
            NodeStatus::Unhealthy => "○",
            NodeStatus::Unknown => "◌",
        };
        let leader_marker = if coord.is_leader { " (leader)" } else { "" };
        items.push(ListItem::new(format!(
            "  {} {}{}", status_icon, coord.id, leader_marker
        )));
    }

    // Shards section
    items.push(ListItem::new("─ Shards"));
    for shard in &app.cluster_state.shards {
        let status_icon = match shard.status {
            NodeStatus::Healthy => "●",
            NodeStatus::Unhealthy => "○",
            NodeStatus::Unknown => "◌",
        };
        items.push(ListItem::new(format!(
            "  {} {} [{}]",
            status_icon,
            shard.address,
            format_vector_count(shard.vector_count)
        )));
    }

    items
}
```

#### 1.5 Implement Metrics Bar
**Priority:** P0 | **Estimate:** 2 hours

```rust
// src/ui/metrics.rs
pub fn draw(frame: &mut Frame, area: Rect, app: &App) {
    let metrics = &app.metrics;

    let text = format!(
        "QPS: {:.0} │ P50: {:.1}ms │ P95: {:.1}ms │ P99: {:.1}ms │ Coverage: {:.0}% │ SLO: {}",
        metrics.qps,
        metrics.p50_latency_ms,
        metrics.p95_latency_ms,
        metrics.p99_latency_ms,
        metrics.coverage * 100.0,
        if metrics.within_slo { "✓" } else { "✗" }
    );

    let style = if metrics.within_slo {
        Style::default().fg(Color::Green)
    } else {
        Style::default().fg(Color::Red)
    };

    let paragraph = Paragraph::new(text)
        .style(style)
        .block(Block::default().borders(Borders::ALL));

    frame.render_widget(paragraph, area);
}
```

#### 1.6 Implement Health Sparklines
**Priority:** P0 | **Estimate:** 3 hours

```rust
// src/ui/health.rs
pub fn draw(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .title(" Health Trends ")
        .borders(Borders::ALL);

    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Divide into rows for each shard
    let shard_count = app.cluster_state.shards.len().max(1);
    let row_height = (inner.height as usize / shard_count).max(2);

    for (i, shard) in app.cluster_state.shards.iter().enumerate() {
        let y = inner.y + (i * row_height) as u16;
        if y >= inner.y + inner.height {
            break;
        }

        let row_area = Rect::new(inner.x, y, inner.width, row_height as u16);
        draw_shard_health(frame, row_area, shard, &app.metrics.history);
    }
}

fn draw_shard_health(frame: &mut Frame, area: Rect, shard: &ShardInfo, history: &MetricsHistory) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(25),  // Shard info
            Constraint::Min(20),     // Sparkline
        ])
        .split(area);

    // Shard info
    let info = format!("{}\nHealth: {:.0}%", shard.address, shard.health_score * 100.0);
    let info_widget = Paragraph::new(info);
    frame.render_widget(info_widget, chunks[0]);

    // Health sparkline
    let data: Vec<u64> = history.get_shard_health(&shard.id)
        .iter()
        .map(|h| (h * 100.0) as u64)
        .collect();

    let sparkline = Sparkline::default()
        .data(&data)
        .max(100)
        .style(Style::default().fg(Color::Green));

    frame.render_widget(sparkline, chunks[1]);
}
```

#### 1.7 Implement Event Loop
**Priority:** P0 | **Estimate:** 3 hours

```rust
// src/events.rs
pub enum Event {
    Tick,
    Key(KeyEvent),
    ClusterUpdate(ClusterState),
    MetricsUpdate(MetricsState),
}

pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<Event>,
    _tx: mpsc::UnboundedSender<Event>,
}

impl EventHandler {
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let tx_clone = tx.clone();

        // Tick timer
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_rate);
            loop {
                interval.tick().await;
                if tx_clone.send(Event::Tick).is_err() {
                    break;
                }
            }
        });

        // Keyboard events
        let tx_clone = tx.clone();
        std::thread::spawn(move || {
            loop {
                if crossterm::event::poll(Duration::from_millis(100)).unwrap() {
                    if let crossterm::event::Event::Key(key) = crossterm::event::read().unwrap() {
                        if tx_clone.send(Event::Key(key)).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        Self { rx, _tx: tx }
    }

    pub async fn next(&mut self) -> Option<Event> {
        self.rx.recv().await
    }
}
```

#### 1.8 Implement Main Binary
**Priority:** P0 | **Estimate:** 2 hours

```rust
// src/main.rs
#[tokio::main]
async fn main() -> Result<()> {
    // Parse args and load config
    let config = TuiConfig::load()?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app and event handler
    let mut app = App::new(config);
    let mut events = EventHandler::new(app.tick_rate);

    // Connect to coordinator for data (if available)
    let coordinator_client = connect_to_coordinator(&config).await.ok();

    // Main loop
    loop {
        // Draw UI
        terminal.draw(|frame| ui::layout::draw(frame, &app))?;

        // Handle events
        match events.next().await {
            Some(Event::Tick) => {
                if let Some(client) = &coordinator_client {
                    app.update_from_coordinator(client).await;
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
            None => break,
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

    Ok(())
}
```

#### 1.9 Add gRPC Data Fetching
**Priority:** P1 | **Estimate:** 3 hours

```rust
// src/app.rs
impl App {
    pub async fn update_from_coordinator(&mut self, client: &CoordinatorClient) {
        // Fetch health info
        if let Ok(health) = client.health(HealthRequest {}).await {
            // Update metrics from health response
            // This requires adding metrics to Health response or new RPC
        }

        // For Phase 1, we'll poll existing endpoints
        // Phase 2 will add proper cluster state RPC
    }
}
```

### Phase 1 Deliverables

- [ ] `akidb-tui` crate with basic structure
- [ ] Topology panel showing coordinators and shards
- [ ] Metrics bar with QPS, latency, coverage
- [ ] Health sparklines for each shard
- [ ] Keyboard navigation (quit, refresh)
- [ ] Connection to coordinator via gRPC
- [ ] Basic themes (default, minimal)

### Phase 1 Testing

```bash
# Build TUI
cargo build -p akidb-tui

# Run standalone (mock data)
cargo run -p akidb-tui -- --mock

# Run connected to coordinator
cargo run -p akidb-tui -- --coordinator 192.168.1.61:50050
```

---

## Phase 2: Auto-Discovery (Week 3-4)

### Objective
Add libp2p-based discovery to the coordinator for zero-config clustering.

### Tasks

#### 2.1 Add libp2p Dependencies
**Priority:** P0 | **Estimate:** 1 hour

```toml
# crates/akidb-coordinator/Cargo.toml
[dependencies]
libp2p = { version = "0.54", features = [
    "mdns",
    "gossipsub",
    "noise",
    "tcp",
    "tokio",
    "macros",
    "identify",
] }
libp2p-identity = "0.2"
```

#### 2.2 Create Discovery Module Structure
**Priority:** P0 | **Estimate:** 2 hours

```bash
crates/akidb-coordinator/src/discovery/
├── mod.rs
├── config.rs       # Discovery configuration
├── network.rs      # libp2p network setup
├── mdns.rs         # mDNS behavior
├── gossip.rs       # Gossipsub for state
└── types.rs        # Discovery-related types
```

#### 2.3 Implement Network Setup
**Priority:** P0 | **Estimate:** 4 hours

```rust
// src/discovery/network.rs
use libp2p::{
    core::transport::upgrade,
    gossipsub, identify, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, Swarm,
};

#[derive(NetworkBehaviour)]
pub struct AkiDbBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
}

pub async fn create_swarm(config: &DiscoveryConfig) -> Result<Swarm<AkiDbBehaviour>> {
    // Generate or load identity
    let local_key = if let Some(key_path) = &config.identity_path {
        load_identity(key_path)?
    } else {
        libp2p_identity::Keypair::generate_ed25519()
    };
    let local_peer_id = PeerId::from(local_key.public());

    info!("Local PeerID: {}", local_peer_id);

    // Create transport with Noise encryption
    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&local_key)?)
        .multiplex(yamux::Config::default())
        .boxed();

    // Configure mDNS
    let mdns = mdns::tokio::Behaviour::new(
        mdns::Config::default(),
        local_peer_id,
    )?;

    // Configure gossipsub
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .build()
        .map_err(|e| anyhow::anyhow!("Gossipsub config error: {}", e))?;

    let gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(local_key.clone()),
        gossipsub_config,
    )?;

    // Configure identify
    let identify = identify::Behaviour::new(
        identify::Config::new("/akidb/1.0.0".to_string(), local_key.public())
    );

    // Create behaviour
    let behaviour = AkiDbBehaviour {
        mdns,
        gossipsub,
        identify,
    };

    // Create swarm
    let swarm = Swarm::new(
        transport,
        behaviour,
        local_peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );

    Ok(swarm)
}
```

#### 2.4 Implement mDNS Discovery
**Priority:** P0 | **Estimate:** 3 hours

```rust
// src/discovery/mdns.rs
use libp2p::mdns;

pub struct MdnsHandler {
    discovered_peers: HashMap<PeerId, PeerInfo>,
    namespace: String,
}

impl MdnsHandler {
    pub fn new(namespace: String) -> Self {
        Self {
            discovered_peers: HashMap::new(),
            namespace,
        }
    }

    pub fn handle_event(&mut self, event: mdns::Event) -> Vec<DiscoveryEvent> {
        let mut events = vec![];

        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, addr) in list {
                    info!("mDNS discovered peer: {} at {}", peer_id, addr);

                    if !self.discovered_peers.contains_key(&peer_id) {
                        self.discovered_peers.insert(peer_id, PeerInfo {
                            peer_id,
                            addresses: vec![addr.clone()],
                            last_seen: Instant::now(),
                        });
                        events.push(DiscoveryEvent::PeerDiscovered { peer_id, addr });
                    }
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, addr) in list {
                    info!("mDNS peer expired: {} at {}", peer_id, addr);
                    self.discovered_peers.remove(&peer_id);
                    events.push(DiscoveryEvent::PeerExpired { peer_id });
                }
            }
        }

        events
    }
}
```

#### 2.5 Implement Gossipsub State
**Priority:** P0 | **Estimate:** 4 hours

```rust
// src/discovery/gossip.rs
use libp2p::gossipsub::{self, TopicHash};

const CLUSTER_STATE_TOPIC: &str = "akidb/cluster-state/1.0.0";
const METRICS_TOPIC: &str = "akidb/metrics/1.0.0";

pub struct GossipHandler {
    cluster_state_topic: gossipsub::IdentTopic,
    metrics_topic: gossipsub::IdentTopic,
}

impl GossipHandler {
    pub fn new(namespace: &str) -> Self {
        Self {
            cluster_state_topic: gossipsub::IdentTopic::new(
                format!("{}/{}", namespace, CLUSTER_STATE_TOPIC)
            ),
            metrics_topic: gossipsub::IdentTopic::new(
                format!("{}/{}", namespace, METRICS_TOPIC)
            ),
        }
    }

    pub fn subscribe(&self, gossipsub: &mut gossipsub::Behaviour) -> Result<()> {
        gossipsub.subscribe(&self.cluster_state_topic)?;
        gossipsub.subscribe(&self.metrics_topic)?;
        Ok(())
    }

    pub fn publish_state(
        &self,
        gossipsub: &mut gossipsub::Behaviour,
        state: &ClusterStateMessage,
    ) -> Result<()> {
        let data = serde_json::to_vec(state)?;
        gossipsub.publish(self.cluster_state_topic.clone(), data)?;
        Ok(())
    }

    pub fn handle_message(
        &self,
        message: gossipsub::Message,
    ) -> Result<GossipEvent> {
        let topic = message.topic.as_str();

        if topic.ends_with(CLUSTER_STATE_TOPIC) {
            let state: ClusterStateMessage = serde_json::from_slice(&message.data)?;
            Ok(GossipEvent::ClusterState(state))
        } else if topic.ends_with(METRICS_TOPIC) {
            let metrics: MetricsMessage = serde_json::from_slice(&message.data)?;
            Ok(GossipEvent::Metrics(metrics))
        } else {
            Err(anyhow::anyhow!("Unknown topic: {}", topic))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateMessage {
    pub sender: String,
    pub timestamp: u64,
    pub coordinators: Vec<CoordinatorAnnouncement>,
    pub shards: Vec<ShardAnnouncement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorAnnouncement {
    pub peer_id: String,
    pub address: String,
    pub is_leader: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardAnnouncement {
    pub id: String,
    pub address: String,
    pub vector_count: u64,
    pub health: f32,
}
```

#### 2.6 Implement Discovery Service
**Priority:** P0 | **Estimate:** 4 hours

```rust
// src/discovery/mod.rs
pub struct DiscoveryService {
    swarm: Swarm<AkiDbBehaviour>,
    mdns_handler: MdnsHandler,
    gossip_handler: GossipHandler,
    config: DiscoveryConfig,
    local_peer_id: PeerId,
    cluster_state: Arc<RwLock<ClusterState>>,
}

impl DiscoveryService {
    pub async fn new(config: DiscoveryConfig) -> Result<Self> {
        let swarm = network::create_swarm(&config).await?;
        let local_peer_id = *swarm.local_peer_id();

        let mdns_handler = MdnsHandler::new(config.namespace.clone());
        let mut gossip_handler = GossipHandler::new(&config.namespace);

        // Subscribe to topics
        gossip_handler.subscribe(swarm.behaviour_mut().gossipsub)?;

        Ok(Self {
            swarm,
            mdns_handler,
            gossip_handler,
            config,
            local_peer_id,
            cluster_state: Arc::new(RwLock::new(ClusterState::default())),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        // Listen on all interfaces
        self.swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        // Announce interval
        let mut announce_interval = tokio::time::interval(
            Duration::from_millis(self.config.announce_interval_ms)
        );

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await?;
                }
                _ = announce_interval.tick() => {
                    self.announce_self().await?;
                }
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<AkiDbBehaviourEvent>) -> Result<()> {
        match event {
            SwarmEvent::Behaviour(AkiDbBehaviourEvent::Mdns(event)) => {
                let discovery_events = self.mdns_handler.handle_event(event);
                for event in discovery_events {
                    self.handle_discovery_event(event).await?;
                }
            }
            SwarmEvent::Behaviour(AkiDbBehaviourEvent::Gossipsub(
                gossipsub::Event::Message { message, .. }
            )) => {
                if let Ok(event) = self.gossip_handler.handle_message(message) {
                    self.handle_gossip_event(event).await?;
                }
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            }
            _ => {}
        }
        Ok(())
    }

    async fn announce_self(&mut self) -> Result<()> {
        let state = self.cluster_state.read().await;
        let message = ClusterStateMessage {
            sender: self.local_peer_id.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs(),
            coordinators: vec![CoordinatorAnnouncement {
                peer_id: self.local_peer_id.to_string(),
                address: self.config.advertise_address.clone(),
                is_leader: state.is_leader(&self.local_peer_id.to_string()),
            }],
            shards: state.shards.clone(),
        };

        self.gossip_handler.publish_state(
            &mut self.swarm.behaviour_mut().gossipsub,
            &message,
        )?;

        Ok(())
    }

    pub fn cluster_state(&self) -> Arc<RwLock<ClusterState>> {
        self.cluster_state.clone()
    }
}
```

### Phase 2 Deliverables

- [ ] libp2p network setup with mDNS + gossipsub
- [ ] Peer discovery via mDNS
- [ ] Cluster state dissemination via gossip
- [ ] Configuration for namespace and announce interval
- [ ] Integration with existing coordinator

### Phase 2 Testing

```bash
# Start first coordinator in bootstrap mode
AKIDB_DISCOVERY_ENABLED=true \
AKIDB_DISCOVERY_NAMESPACE=test-cluster \
cargo run -p akidb-coordinator -- --bootstrap

# Start second coordinator (auto-discovers first)
AKIDB_DISCOVERY_ENABLED=true \
AKIDB_DISCOVERY_NAMESPACE=test-cluster \
cargo run -p akidb-coordinator
```

---

## Phase 3: Leader Election (Week 5)

### Objective
Implement deterministic leader election among coordinators.

### Tasks

#### 3.1 Create Election Module
**Priority:** P0 | **Estimate:** 4 hours

```rust
// src/election/mod.rs
pub struct ElectionService {
    local_peer_id: String,
    cluster_state: Arc<RwLock<ClusterState>>,
    leader_lease: Arc<RwLock<Option<LeaderLease>>>,
}

#[derive(Debug, Clone)]
pub struct LeaderLease {
    pub leader_id: String,
    pub acquired_at: Instant,
    pub expires_at: Instant,
}

impl ElectionService {
    pub fn new(local_peer_id: String, cluster_state: Arc<RwLock<ClusterState>>) -> Self {
        Self {
            local_peer_id,
            cluster_state,
            leader_lease: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;
            self.evaluate_leadership().await?;
        }
    }

    async fn evaluate_leadership(&self) -> Result<()> {
        let state = self.cluster_state.read().await;

        // Get visible coordinators (those with recent heartbeats)
        let visible_coordinators: Vec<_> = state.coordinators
            .iter()
            .filter(|c| c.is_visible())
            .collect();

        // Check quorum
        let total = state.coordinators.len();
        let quorum = (total / 2) + 1;

        if visible_coordinators.len() < quorum {
            warn!("No quorum: {} visible out of {} (need {})",
                  visible_coordinators.len(), total, quorum);

            // If we're current leader, step down
            let mut lease = self.leader_lease.write().await;
            if let Some(l) = lease.as_ref() {
                if l.leader_id == self.local_peer_id {
                    info!("Stepping down as leader due to quorum loss");
                    *lease = None;
                }
            }
            return Ok(());
        }

        // Determine leader: lowest PeerID among visible nodes
        let leader_id = visible_coordinators
            .iter()
            .map(|c| &c.peer_id)
            .min()
            .unwrap()
            .clone();

        // Update lease
        let mut lease = self.leader_lease.write().await;
        let is_new_leader = lease.as_ref().map(|l| &l.leader_id) != Some(&leader_id);

        if is_new_leader {
            info!("New leader elected: {}", leader_id);
        }

        *lease = Some(LeaderLease {
            leader_id: leader_id.clone(),
            acquired_at: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(5),
        });

        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        let lease = self.leader_lease.read().await;
        lease.as_ref()
            .map(|l| l.leader_id == self.local_peer_id && l.expires_at > Instant::now())
            .unwrap_or(false)
    }

    pub async fn current_leader(&self) -> Option<String> {
        let lease = self.leader_lease.read().await;
        lease.as_ref()
            .filter(|l| l.expires_at > Instant::now())
            .map(|l| l.leader_id.clone())
    }
}
```

### Phase 3 Deliverables

- [ ] Deterministic leader election (lowest PeerID)
- [ ] Quorum checking
- [ ] Leader lease with expiry
- [ ] Graceful stepdown on quorum loss

---

## Phase 4: Integration and Hardening (Week 6)

### Objective
Integrate all components, add security, and prepare for production.

### Tasks

#### 4.1 Security Hardening
- [ ] Implement cluster_secret validation
- [ ] Add Noise encryption verification
- [ ] Rate limiting on discovery

#### 4.2 Integration Testing
- [ ] Multi-node cluster tests
- [ ] Network partition simulation
- [ ] Leader failover tests

#### 4.3 Documentation
- [ ] Configuration guide
- [ ] Operational runbook
- [ ] TUI user guide

#### 4.4 Performance Testing
- [ ] Measure discovery overhead
- [ ] Verify query latency SLO maintained
- [ ] TUI refresh performance

---

## File Summary

### New Files to Create

| Path | Purpose |
|------|---------|
| `crates/akidb-tui/Cargo.toml` | TUI crate manifest |
| `crates/akidb-tui/src/lib.rs` | Library root |
| `crates/akidb-tui/src/main.rs` | TUI binary entry |
| `crates/akidb-tui/src/app.rs` | Application state |
| `crates/akidb-tui/src/config.rs` | Configuration |
| `crates/akidb-tui/src/ui/mod.rs` | UI module |
| `crates/akidb-tui/src/ui/layout.rs` | Main layout |
| `crates/akidb-tui/src/ui/topology.rs` | Topology panel |
| `crates/akidb-tui/src/ui/metrics.rs` | Metrics panel |
| `crates/akidb-tui/src/ui/health.rs` | Health sparklines |
| `crates/akidb-tui/src/ui/controls.rs` | Control bar |
| `crates/akidb-tui/src/events.rs` | Event handling |
| `crates/akidb-tui/src/theme.rs` | Color themes |
| `crates/akidb-coordinator/src/discovery/mod.rs` | Discovery module |
| `crates/akidb-coordinator/src/discovery/config.rs` | Discovery config |
| `crates/akidb-coordinator/src/discovery/network.rs` | libp2p setup |
| `crates/akidb-coordinator/src/discovery/mdns.rs` | mDNS handler |
| `crates/akidb-coordinator/src/discovery/gossip.rs` | Gossipsub handler |
| `crates/akidb-coordinator/src/discovery/types.rs` | Discovery types |
| `crates/akidb-coordinator/src/election/mod.rs` | Election module |
| `crates/akidb-coordinator/src/election/deterministic.rs` | Leader election |
| `crates/akidb-coordinator/src/state/mod.rs` | State module |
| `crates/akidb-coordinator/src/state/crdt.rs` | CRDT implementations |

### Files to Modify

| Path | Changes |
|------|---------|
| `crates/akidb-coordinator/Cargo.toml` | Add libp2p dependencies |
| `crates/akidb-coordinator/src/main.rs` | Initialize discovery service |
| `config/default.toml` | Add discovery and TUI config sections |
| `Cargo.toml` (workspace) | Add akidb-tui member |

---

## Success Criteria

### Phase 1 Complete When:
- [ ] TUI displays mock cluster data
- [ ] TUI connects to coordinator and shows live data
- [ ] Keyboard navigation works
- [ ] Refresh rate is configurable

### Phase 2 Complete When:
- [ ] Two coordinators auto-discover each other
- [ ] Shard announcements propagate via gossip
- [ ] CLI fallback works when discovery disabled

### Phase 3 Complete When:
- [ ] Leader is elected deterministically
- [ ] Leader failover works within 5s
- [ ] Split-brain prevented by quorum

### Phase 4 Complete When:
- [ ] All tests pass
- [ ] Documentation complete
- [ ] Deployed to Thor cluster successfully
