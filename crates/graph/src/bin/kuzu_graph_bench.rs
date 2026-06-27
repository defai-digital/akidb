#[cfg(not(feature = "kuzu"))]
fn main() {
    eprintln!("kuzu-graph-bench requires `cargo run -p akidb-graph --features kuzu --bin kuzu-graph-bench`");
    std::process::exit(2);
}

#[cfg(feature = "kuzu")]
fn main() {
    if let Err(error) = enabled::run() {
        eprintln!("ERROR: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "kuzu")]
mod enabled {
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use akidb_graph::kuzu::KuzuGraphAdapter;
    use akidb_graph::{
        Direction, EdgeKind, GraphEdge, GraphIndex, GraphNode, GraphNodeId, NativeGraphIndex,
        NeighborRequest, NodeKind, PathExistsRequest, TwoHopRequest,
    };
    use akidb_storage::{RocksDbBackend, StorageBackend};
    use serde::Serialize;
    use serde_json::json;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[derive(Debug, Clone)]
    struct Args {
        nodes: usize,
        edges: usize,
        queries_per_kind: usize,
        output: PathBuf,
        work_dir: PathBuf,
        shape: String,
    }

    impl Args {
        fn parse() -> Result<Self> {
            let mut values = HashMap::new();
            let mut iter = env::args().skip(1);
            while let Some(arg) = iter.next() {
                match arg.as_str() {
                    "--nodes" | "--edges" | "--queries-per-kind" | "--output" | "--work-dir"
                    | "--shape" => {
                        let value = iter
                            .next()
                            .ok_or_else(|| format!("{arg} requires a value"))?;
                        values.insert(arg.trim_start_matches("--").to_string(), value);
                    }
                    "--help" | "-h" => {
                        print_help();
                        std::process::exit(0);
                    }
                    other => return Err(format!("unknown argument: {other}").into()),
                }
            }

            let output = values
                .remove("output")
                .map(PathBuf::from)
                .unwrap_or_else(default_output_path);
            let work_dir = values
                .remove("work-dir")
                .map(PathBuf::from)
                .unwrap_or_else(default_work_dir);

            Ok(Self {
                nodes: parse_usize(values.remove("nodes"), 1_000, "nodes")?,
                edges: parse_usize(values.remove("edges"), 5_000, "edges")?,
                queries_per_kind: parse_usize(
                    values.remove("queries-per-kind"),
                    250,
                    "queries-per-kind",
                )?,
                output,
                work_dir,
                shape: values
                    .remove("shape")
                    .unwrap_or_else(|| "synthetic_code_graph".to_string()),
            })
        }
    }

    fn parse_usize(value: Option<String>, default: usize, name: &str) -> Result<usize> {
        let Some(raw) = value else {
            return Ok(default);
        };
        let parsed = raw
            .parse::<usize>()
            .map_err(|_| format!("{name} must be a positive integer"))?;
        if parsed == 0 {
            return Err(format!("{name} must be > 0").into());
        }
        Ok(parsed)
    }

    fn print_help() {
        println!(
            "Usage: kuzu-graph-bench [--nodes N] [--edges N] [--queries-per-kind N] \\
             [--output PATH] [--work-dir PATH] [--shape NAME]"
        );
    }

    fn default_output_path() -> PathBuf {
        PathBuf::from(format!(
            "docs/reports/kuzu-decision-{}.json",
            command_output("date", &["-u", "+%Y%m%dT%H%M%SZ"]).unwrap_or_else(|| "unknown".into())
        ))
    }

    fn default_work_dir() -> PathBuf {
        env::temp_dir().join(format!(
            "akidb-kuzu-bench-{}-{}",
            std::process::id(),
            unix_millis()
        ))
    }

    pub fn run() -> Result<()> {
        let args = Args::parse()?;
        if args.nodes < 2 {
            return Err("--nodes must be at least 2".into());
        }
        fs::create_dir_all(&args.work_dir)?;
        if let Some(parent) = args.output.parent() {
            fs::create_dir_all(parent)?;
        }

        let dataset = Dataset::generate(args.nodes, args.edges);
        let query_plan = QueryPlan::generate(dataset.entity_count, args.queries_per_kind);

        let native_dir = args.work_dir.join("native-rocksdb");
        let native_report = {
            let storage = Arc::new(RocksDbBackend::open(&native_dir)?);
            let graph = NativeGraphIndex::new(Arc::clone(&storage));
            let ingest = ingest_graph(&graph, &dataset)?;
            storage.flush()?;
            let (queries, signatures) = run_queries(&graph, &query_plan)?;
            let stats = graph.stats()?;
            let peak_rss_bytes = current_rss_bytes().max(1);
            drop(graph);
            storage.flush()?;
            drop(storage);
            BackendRun {
                report: BackendReport {
                    available: true,
                    implementation: "akidb-native-graph".to_string(),
                    ingest,
                    storage: StorageReport {
                        bytes: dir_size(&native_dir).max(1),
                    },
                    memory: MemoryReport { peak_rss_bytes },
                    queries,
                },
                signatures,
                node_count: stats.nodes,
                edge_count: stats.edges,
            }
        };

        let kuzu_dir = args.work_dir.join("kuzu");
        let kuzu_report = {
            let (ingest, queries, signatures, stats, peak_rss_bytes) = {
                let graph = KuzuGraphAdapter::new(&kuzu_dir)?;
                let ingest = ingest_graph(&graph, &dataset)?;
                let (queries, signatures) = run_queries(&graph, &query_plan)?;
                let stats = graph.stats()?;
                let peak_rss_bytes = current_rss_bytes().max(1);
                (ingest, queries, signatures, stats, peak_rss_bytes)
            };
            BackendRun {
                report: BackendReport {
                    available: true,
                    implementation: "kuzu-rust".to_string(),
                    ingest,
                    storage: StorageReport {
                        bytes: dir_size(&kuzu_dir).max(1),
                    },
                    memory: MemoryReport { peak_rss_bytes },
                    queries,
                },
                signatures,
                node_count: stats.nodes,
                edge_count: stats.edges,
            }
        };

        let parity = result_parity_percent(&native_report.signatures, &kuzu_report.signatures);
        let recommendation = recommendation(&native_report.report, &kuzu_report.report, parity);
        let report = DecisionArtifact {
            schema_version: 1,
            generated_at: command_output("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"])
                .unwrap_or_else(|| unix_millis().to_string()),
            hardware: HardwareReport {
                os: command_output("sw_vers", &["-productVersion"])
                    .map(|version| format!("macOS {version}"))
                    .unwrap_or_else(|| env::consts::OS.to_string()),
                arch: env::consts::ARCH.to_string(),
                mac_model: command_output("sysctl", &["-n", "hw.model"])
                    .unwrap_or_else(|| "unknown".to_string()),
                memory_bytes: command_output("sysctl", &["-n", "hw.memsize"])
                    .and_then(|raw| raw.parse::<u64>().ok()),
            },
            software: SoftwareReport {
                akidb_commit: command_output("git", &["rev-parse", "--short", "HEAD"])
                    .unwrap_or_else(|| "unknown".to_string()),
                rustc: command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".into()),
                kuzu_version: kuzu_version(),
            },
            dataset: DatasetReport {
                shape: args.shape,
                nodes: dataset.nodes.len() as u64,
                edges: dataset.edges.len() as u64,
                related_chunk_edges: dataset.related_chunk_edges as u64,
            },
            query_mix: query_plan.mix_report(),
            backends: BackendsReport {
                native: native_report.report,
                kuzu: kuzu_report.report,
            },
            correctness: CorrectnessReport {
                result_parity_percent: parity,
                native_node_count: native_report.node_count,
                kuzu_node_count: kuzu_report.node_count,
                native_edge_count: native_report.edge_count,
                kuzu_edge_count: kuzu_report.edge_count,
            },
            decision: DecisionReport {
                recommendation: recommendation.clone(),
                rationale: rationale(&recommendation, parity),
                rollback_plan: "Keep the native graph index as the default hot path and disable the akidb-graph/kuzu feature.".to_string(),
                packaging_source: "Homebrew shared Kuzu library plus kuzu Rust crate".to_string(),
                upstream_status: "Homebrew currently marks Kuzu deprecated because the upstream repository is archived.".to_string(),
                maintenance_owner: "AkiDB maintainers".to_string(),
            },
        };

        fs::write(&args.output, serde_json::to_string_pretty(&report)? + "\n")?;
        println!("{}", args.output.display());
        Ok(())
    }

    #[derive(Debug)]
    struct Dataset {
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
        entity_count: usize,
        related_chunk_edges: usize,
    }

    impl Dataset {
        fn generate(node_count: usize, edge_count: usize) -> Self {
            let chunk_count = (node_count / 5).max(1).min(node_count - 1);
            let entity_count = node_count - chunk_count;
            let mut nodes = Vec::with_capacity(node_count);
            for index in 0..entity_count {
                nodes.push(
                    GraphNode::new(format!("entity:{index}"), NodeKind::Entity)
                        .with_property("ordinal", json!(index)),
                );
            }
            for index in 0..chunk_count {
                nodes.push(
                    GraphNode::new(format!("chunk:{index}"), NodeKind::Chunk)
                        .with_property("ordinal", json!(index)),
                );
            }

            let edge_kinds = [
                EdgeKind::RelatedTo,
                EdgeKind::Calls,
                EdgeKind::DependsOn,
                EdgeKind::Imports,
            ];
            let mut edges = Vec::with_capacity(edge_count);
            let mut related_chunk_edges = 0usize;
            for index in 0..edge_count {
                if index % 5 == 0 {
                    related_chunk_edges += 1;
                    edges.push(
                        GraphEdge::new(
                            format!("edge:{index}"),
                            format!("entity:{}", index % entity_count),
                            format!("chunk:{}", index % chunk_count),
                            EdgeKind::Mentions,
                        )
                        .with_weight(weight(index)),
                    );
                } else {
                    let from = index % entity_count;
                    let mut to = (index.wrapping_mul(31).wrapping_add(7)) % entity_count;
                    if to == from {
                        to = (to + 1) % entity_count;
                    }
                    edges.push(
                        GraphEdge::new(
                            format!("edge:{index}"),
                            format!("entity:{from}"),
                            format!("entity:{to}"),
                            edge_kinds[index % edge_kinds.len()],
                        )
                        .with_weight(weight(index)),
                    );
                }
            }
            Self {
                nodes,
                edges,
                entity_count,
                related_chunk_edges,
            }
        }
    }

    fn weight(index: usize) -> f32 {
        0.5 + ((index % 500) as f32 / 1_000.0)
    }

    #[derive(Debug, Clone)]
    enum QueryCase {
        Neighbors(GraphNodeId),
        TwoHop(GraphNodeId),
        PathExists(GraphNodeId, GraphNodeId),
        RelatedChunks(GraphNodeId),
    }

    #[derive(Debug)]
    struct QueryPlan {
        cases: Vec<QueryCase>,
        counts: HashMap<&'static str, usize>,
    }

    impl QueryPlan {
        fn generate(entity_count: usize, queries_per_kind: usize) -> Self {
            let mut cases = Vec::with_capacity(queries_per_kind * 4);
            let mut counts = HashMap::new();
            counts.insert("neighbors", queries_per_kind);
            counts.insert("two_hop", queries_per_kind);
            counts.insert("path_exists", queries_per_kind);
            counts.insert("related_chunks", queries_per_kind);
            for index in 0..queries_per_kind {
                let node = GraphNodeId::new(format!("entity:{}", index % entity_count));
                let to = GraphNodeId::new(format!("entity:{}", (index + 2) % entity_count));
                cases.push(QueryCase::Neighbors(node.clone()));
                cases.push(QueryCase::TwoHop(node.clone()));
                cases.push(QueryCase::PathExists(node.clone(), to));
                cases.push(QueryCase::RelatedChunks(node));
            }
            Self { cases, counts }
        }

        fn mix_report(&self) -> Vec<QueryMixItem> {
            ["neighbors", "two_hop", "path_exists", "related_chunks"]
                .into_iter()
                .map(|kind| QueryMixItem {
                    kind: kind.to_string(),
                    count: *self.counts.get(kind).unwrap_or(&0) as u64,
                })
                .collect()
        }
    }

    struct BackendRun {
        report: BackendReport,
        signatures: Vec<String>,
        node_count: u64,
        edge_count: u64,
    }

    fn ingest_graph<G: GraphIndex>(graph: &G, dataset: &Dataset) -> Result<IngestReport> {
        let started = Instant::now();
        for node in &dataset.nodes {
            graph.upsert_node(node.clone())?;
        }
        for edge in &dataset.edges {
            graph.upsert_edge(edge.clone())?;
        }
        let elapsed = elapsed_ms(started.elapsed()).max(0.000001);
        Ok(IngestReport {
            wall_time_ms: elapsed,
            nodes_per_sec: dataset.nodes.len() as f64 / (elapsed / 1_000.0),
            edges_per_sec: dataset.edges.len() as f64 / (elapsed / 1_000.0),
        })
    }

    fn run_queries<G: GraphIndex>(
        graph: &G,
        query_plan: &QueryPlan,
    ) -> Result<(QueryReport, Vec<String>)> {
        let mut latencies = Vec::with_capacity(query_plan.cases.len());
        let mut signatures = Vec::with_capacity(query_plan.cases.len());
        let started = Instant::now();
        let mut errors = 0u64;

        for case in &query_plan.cases {
            let query_started = Instant::now();
            let signature = query_signature(graph, case);
            latencies.push(elapsed_ms(query_started.elapsed()));
            match signature {
                Ok(value) => signatures.push(value),
                Err(error) => {
                    errors += 1;
                    signatures.push(format!("ERROR:{error}"));
                }
            }
        }

        if errors > 0 {
            return Err(format!("{errors} graph queries failed").into());
        }

        let wall_ms = elapsed_ms(started.elapsed()).max(0.000001);
        Ok((
            QueryReport {
                qps: query_plan.cases.len() as f64 / (wall_ms / 1_000.0),
                errors,
                latency: LatencyReport::from_samples(&mut latencies),
            },
            signatures,
        ))
    }

    fn query_signature<G: GraphIndex>(graph: &G, case: &QueryCase) -> Result<String> {
        match case {
            QueryCase::Neighbors(node) => {
                let neighbors = graph.neighbors(
                    NeighborRequest::new(node.clone())
                        .with_direction(Direction::Out)
                        .with_limit(16),
                )?;
                Ok(format!(
                    "neighbors:{}",
                    neighbors
                        .into_iter()
                        .map(|neighbor| format!(
                            "{}:{}:{}",
                            neighbor.node.id,
                            neighbor.edge.id,
                            neighbor.edge.kind.as_key()
                        ))
                        .collect::<Vec<_>>()
                        .join("|")
                ))
            }
            QueryCase::TwoHop(node) => {
                let mut request = TwoHopRequest::new(node.clone());
                request.first_hop_limit = 16;
                request.second_hop_limit = 8;
                request.limit = 16;
                let paths = graph.two_hop(request)?;
                Ok(format!(
                    "two_hop:{}",
                    paths
                        .into_iter()
                        .map(|path| {
                            let edge_ids = path
                                .edges
                                .into_iter()
                                .map(|edge| edge.id.to_string())
                                .collect::<Vec<_>>()
                                .join(">");
                            let node_ids = path
                                .nodes
                                .into_iter()
                                .map(|node| node.id.to_string())
                                .collect::<Vec<_>>()
                                .join(">");
                            format!("{node_ids}:{edge_ids}")
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                ))
            }
            QueryCase::PathExists(from, to) => {
                let exists =
                    graph.path_exists(PathExistsRequest::new(from.clone(), to.clone(), 2))?;
                Ok(format!("path_exists:{exists}"))
            }
            QueryCase::RelatedChunks(node) => {
                let chunks = graph.related_chunks(node, 16)?;
                Ok(format!(
                    "related_chunks:{}",
                    chunks
                        .into_iter()
                        .map(|chunk| format!("{}:{}", chunk.vector_id, chunk.via_node))
                        .collect::<Vec<_>>()
                        .join("|")
                ))
            }
        }
    }

    fn result_parity_percent(native: &[String], kuzu: &[String]) -> f64 {
        if native.is_empty() || native.len() != kuzu.len() {
            return 0.0;
        }
        let matches = native
            .iter()
            .zip(kuzu.iter())
            .filter(|(left, right)| left == right)
            .count();
        matches as f64 * 100.0 / native.len() as f64
    }

    fn recommendation(native: &BackendReport, kuzu: &BackendReport, parity: f64) -> String {
        let p95_ratio = kuzu.queries.latency.p95_ms / native.queries.latency.p95_ms.max(0.000001);
        let p99_ratio = kuzu.queries.latency.p99_ms / native.queries.latency.p99_ms.max(0.000001);
        let ingest_ratio = kuzu.ingest.wall_time_ms / native.ingest.wall_time_ms.max(0.000001);
        let storage_ratio = kuzu.storage.bytes as f64 / native.storage.bytes.max(1) as f64;
        let rss_ratio =
            kuzu.memory.peak_rss_bytes as f64 / native.memory.peak_rss_bytes.max(1) as f64;
        let qps_ratio = kuzu.queries.qps / native.queries.qps.max(0.000001);

        if parity >= 99.5
            && p95_ratio <= 3.0
            && p99_ratio <= 3.0
            && ingest_ratio <= 4.0
            && storage_ratio <= 5.0
            && rss_ratio <= 4.0
            && qps_ratio >= 0.25
        {
            "ship_optional_kuzu".to_string()
        } else {
            "reject_kuzu".to_string()
        }
    }

    fn rationale(recommendation: &str, parity: f64) -> String {
        if recommendation == "ship_optional_kuzu" {
            format!(
                "Kuzu matches native result parity at {parity:.2}% and passes optional-adapter benchmark gates; native remains the default hot path."
            )
        } else {
            format!(
                "Kuzu does not currently clear optional-adapter benchmark gates at {parity:.2}% parity, so native remains the only supported hot path."
            )
        }
    }

    #[derive(Debug, Serialize)]
    struct DecisionArtifact {
        schema_version: u64,
        generated_at: String,
        hardware: HardwareReport,
        software: SoftwareReport,
        dataset: DatasetReport,
        query_mix: Vec<QueryMixItem>,
        backends: BackendsReport,
        correctness: CorrectnessReport,
        decision: DecisionReport,
    }

    #[derive(Debug, Serialize)]
    struct HardwareReport {
        os: String,
        arch: String,
        mac_model: String,
        memory_bytes: Option<u64>,
    }

    #[derive(Debug, Serialize)]
    struct SoftwareReport {
        akidb_commit: String,
        rustc: String,
        kuzu_version: String,
    }

    #[derive(Debug, Serialize)]
    struct DatasetReport {
        shape: String,
        nodes: u64,
        edges: u64,
        related_chunk_edges: u64,
    }

    #[derive(Debug, Serialize)]
    struct QueryMixItem {
        kind: String,
        count: u64,
    }

    #[derive(Debug, Serialize)]
    struct BackendsReport {
        native: BackendReport,
        kuzu: BackendReport,
    }

    #[derive(Debug, Serialize)]
    struct BackendReport {
        available: bool,
        implementation: String,
        ingest: IngestReport,
        storage: StorageReport,
        memory: MemoryReport,
        queries: QueryReport,
    }

    #[derive(Debug, Serialize)]
    struct IngestReport {
        wall_time_ms: f64,
        nodes_per_sec: f64,
        edges_per_sec: f64,
    }

    #[derive(Debug, Serialize)]
    struct StorageReport {
        bytes: u64,
    }

    #[derive(Debug, Serialize)]
    struct MemoryReport {
        peak_rss_bytes: u64,
    }

    #[derive(Debug, Serialize)]
    struct QueryReport {
        qps: f64,
        errors: u64,
        latency: LatencyReport,
    }

    #[derive(Debug, Serialize)]
    struct LatencyReport {
        count: u64,
        min_ms: f64,
        p50_ms: f64,
        p95_ms: f64,
        p99_ms: f64,
        max_ms: f64,
    }

    impl LatencyReport {
        fn from_samples(samples: &mut [f64]) -> Self {
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            if samples.is_empty() {
                return Self {
                    count: 0,
                    min_ms: 0.0,
                    p50_ms: 0.0,
                    p95_ms: 0.0,
                    p99_ms: 0.0,
                    max_ms: 0.0,
                };
            }

            Self {
                count: samples.len() as u64,
                min_ms: samples[0],
                p50_ms: percentile(samples, 0.50),
                p95_ms: percentile(samples, 0.95),
                p99_ms: percentile(samples, 0.99),
                max_ms: samples[samples.len() - 1],
            }
        }
    }

    #[derive(Debug, Serialize)]
    struct CorrectnessReport {
        result_parity_percent: f64,
        native_node_count: u64,
        kuzu_node_count: u64,
        native_edge_count: u64,
        kuzu_edge_count: u64,
    }

    #[derive(Debug, Serialize)]
    struct DecisionReport {
        recommendation: String,
        rationale: String,
        rollback_plan: String,
        packaging_source: String,
        upstream_status: String,
        maintenance_owner: String,
    }

    fn percentile(sorted_samples: &[f64], percentile: f64) -> f64 {
        if sorted_samples.is_empty() {
            return 0.0;
        }
        let index = ((sorted_samples.len() - 1) as f64 * percentile).ceil() as usize;
        sorted_samples[index.min(sorted_samples.len() - 1)]
    }

    fn elapsed_ms(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1_000.0
    }

    fn unix_millis() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }

    fn command_output(command: &str, args: &[&str]) -> Option<String> {
        let output = Command::new(command).args(args).output().ok()?;
        if !output.status.success() {
            return None;
        }
        let value = String::from_utf8(output.stdout).ok()?.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    }

    fn kuzu_version() -> String {
        command_output("kuzu", &["--version"])
            .or_else(|| command_output("brew", &["list", "--versions", "kuzu"]))
            .unwrap_or_else(|| "kuzu 0.11.3".to_string())
    }

    fn current_rss_bytes() -> u64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if status != 0 {
            return 1;
        }
        let usage = unsafe { usage.assume_init() };
        let rss = usage.ru_maxrss.max(1) as u64;
        if cfg!(target_os = "macos") {
            rss
        } else {
            rss.saturating_mul(1024)
        }
    }

    fn dir_size(path: &Path) -> u64 {
        let Ok(metadata) = fs::metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }

        let mut total = 0u64;
        let Ok(entries) = fs::read_dir(path) else {
            return total;
        };
        for entry in entries.flatten() {
            total = total.saturating_add(dir_size(&entry.path()));
        }
        total
    }
}
