//! Bounded property-graph and GraphRAG kernel qualification.
//!
//! This is deliberately not labelled an LDBC implementation: AkiDB exposes a
//! bounded retrieval graph rather than a general Cypher/GQL database.  The
//! workload applies LDBC-style principles—known-answer queries, concurrent
//! traversal, persistent reload, mutation integrity, and percentile latency—
//! to the graph contract AkiDB actually ships.

use akidb_graph::{
    Direction, EdgeKind, GraphEdge, GraphEdgeId, GraphIndex, GraphMutationBatch, GraphNode,
    GraphNodeId, NativeGraphIndex, NeighborRequest, NodeKind, PathExistsRequest,
    RelatedChunksRequest, TwoHopRequest,
};
use akidb_storage::RocksDbBackend;
use clap::Parser;
use serde::Serialize;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// New, empty directory for the persistent graph index.
    #[arg(long)]
    data_dir: PathBuf,

    /// Stable graph workspace.
    #[arg(long, default_value = "qualification")]
    workspace: String,

    /// Number of deterministic document nodes.
    #[arg(long, default_value = "10000")]
    documents: usize,

    /// Chunk nodes and contains edges per document.
    #[arg(long, default_value = "4")]
    chunks_per_document: usize,

    /// Shared entity nodes used by two-hop and related-evidence queries.
    #[arg(long, default_value = "1000")]
    entities: usize,

    /// Documents committed in each atomic graph batch.
    #[arg(long, default_value = "100")]
    batch_documents: usize,

    /// Known-answer traversal operations.
    #[arg(long, default_value = "10000")]
    queries: usize,

    /// Fixed concurrent traversal workers.
    #[arg(long, default_value = "8")]
    concurrency: usize,

    /// Required exact known-answer accuracy.
    #[arg(long, default_value = "1")]
    min_accuracy: f64,

    /// Required successful operations per second.
    #[arg(long, default_value = "0")]
    min_qps: f64,

    /// Maximum p99 traversal latency in milliseconds (zero disables).
    #[arg(long, default_value = "0")]
    max_p99_ms: f64,

    /// Machine-readable report path.
    #[arg(long)]
    output_json: PathBuf,
}

#[derive(Debug, Serialize)]
struct BuildReport {
    nodes: u64,
    edges: u64,
    duration_ms: u128,
    nodes_per_second: f64,
    edges_per_second: f64,
    persisted_bytes: u64,
}

#[derive(Debug, Serialize)]
struct LatencyReport {
    count: usize,
    min_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Serialize)]
struct QueryReport {
    requested: usize,
    succeeded: usize,
    incorrect: usize,
    errors: usize,
    concurrency: usize,
    duration_ms: u128,
    qps: f64,
    known_answer_accuracy: f64,
    operations: std::collections::BTreeMap<&'static str, usize>,
    latency: LatencyReport,
}

#[derive(Debug, Serialize)]
struct IntegrityReport {
    persistent_reopen_ms: u128,
    stats_match: bool,
    cross_workspace_rejected_atomically: bool,
    excessive_depth_rejected: bool,
    incident_edges_deleted: bool,
}

#[derive(Debug, Serialize)]
struct GraphReport {
    schema_version: u32,
    report_type: &'static str,
    generated_at_unix_ms: u128,
    workload: WorkloadReport,
    build: BuildReport,
    integrity: IntegrityReport,
    query: QueryReport,
    verdict: Verdict,
}

#[derive(Debug, Serialize)]
struct WorkloadReport {
    workspace: String,
    documents: usize,
    chunks_per_document: usize,
    entities: usize,
    batch_documents: usize,
    topology: &'static str,
}

#[derive(Debug, Serialize)]
struct Verdict {
    status: &'static str,
    failures: Vec<String>,
}

#[derive(Default)]
struct Measurements {
    latencies: Vec<Duration>,
    succeeded: usize,
    incorrect: usize,
    errors: usize,
    operations: std::collections::BTreeMap<&'static str, usize>,
}

fn validate_args(args: &Args) -> Result<(), String> {
    for (name, value) in [
        ("documents", args.documents),
        ("chunks-per-document", args.chunks_per_document),
        ("entities", args.entities),
        ("batch-documents", args.batch_documents),
        ("queries", args.queries),
        ("concurrency", args.concurrency),
    ] {
        if value == 0 {
            return Err(format!("--{name} must be positive"));
        }
    }
    if args.entities > args.documents {
        return Err("--entities cannot exceed --documents".to_string());
    }
    if args.chunks_per_document > 256 {
        return Err("--chunks-per-document cannot exceed 256".to_string());
    }
    if args.concurrency > 4_096 {
        return Err("--concurrency cannot exceed 4096".to_string());
    }
    if !is_canonical(&args.workspace) {
        return Err("--workspace must be canonical text".to_string());
    }
    for (name, value) in [
        ("min-accuracy", args.min_accuracy),
        ("min-qps", args.min_qps),
        ("max-p99-ms", args.max_p99_ms),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(format!("--{name} must be finite and non-negative"));
        }
    }
    if args.min_accuracy > 1.0 {
        return Err("--min-accuracy cannot exceed 1".to_string());
    }
    if args.data_dir.exists()
        && args
            .data_dir
            .read_dir()
            .map_err(|error| error.to_string())?
            .next()
            .is_some()
    {
        return Err("--data-dir must not exist or must be empty".to_string());
    }
    Ok(())
}

fn is_canonical(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= 256
        && !value.contains(['\n', '\r', '\0'])
}

fn document_id(workspace: &str, document: usize) -> GraphNodeId {
    GraphNodeId::scoped(workspace, &format!("document:{document:010}"))
}

fn chunk_id(workspace: &str, document: usize, chunk: usize) -> GraphNodeId {
    GraphNodeId::scoped(workspace, &format!("chunk:{document:010}:{chunk:03}"))
}

fn entity_id(workspace: &str, entity: usize) -> GraphNodeId {
    GraphNodeId::scoped(workspace, &format!("entity:{entity:010}"))
}

fn contains_edge_id(workspace: &str, document: usize, chunk: usize) -> GraphEdgeId {
    GraphEdgeId::new(format!("{workspace}:contains:{document:010}:{chunk:03}"))
}

fn mentions_edge_id(workspace: &str, document: usize, chunk: usize) -> GraphEdgeId {
    GraphEdgeId::new(format!("{workspace}:mentions:{document:010}:{chunk:03}"))
}

fn expected_counts(args: &Args) -> (u64, u64) {
    let chunks = args.documents * args.chunks_per_document;
    (
        (args.entities + args.documents + chunks) as u64,
        (chunks * 2) as u64,
    )
}

fn build_graph(
    graph: &NativeGraphIndex<RocksDbBackend>,
    args: &Args,
) -> Result<BuildReport, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut entity_batch = GraphMutationBatch::new();
    for entity in 0..args.entities {
        entity_batch = entity_batch.with_replaced_node(GraphNode::new(
            entity_id(&args.workspace, entity),
            NodeKind::Entity,
        ));
    }
    graph.upsert_batch(entity_batch)?;

    for batch_start in (0..args.documents).step_by(args.batch_documents) {
        let batch_end = (batch_start + args.batch_documents).min(args.documents);
        let mut batch = GraphMutationBatch::new();
        for document in batch_start..batch_end {
            let document_node = document_id(&args.workspace, document);
            batch =
                batch.with_replaced_node(GraphNode::new(document_node.clone(), NodeKind::Document));
            let entity_node = entity_id(&args.workspace, document % args.entities);
            for chunk in 0..args.chunks_per_document {
                let chunk_node = chunk_id(&args.workspace, document, chunk);
                batch = batch
                    .with_replaced_node(GraphNode::new(chunk_node.clone(), NodeKind::Chunk))
                    .with_edge(GraphEdge::new(
                        contains_edge_id(&args.workspace, document, chunk),
                        document_node.clone(),
                        chunk_node.clone(),
                        EdgeKind::Contains,
                    ))
                    .with_edge(GraphEdge::new(
                        mentions_edge_id(&args.workspace, document, chunk),
                        chunk_node,
                        entity_node.clone(),
                        EdgeKind::Mentions,
                    ));
            }
        }
        graph.upsert_batch(batch)?;
    }
    let duration = started.elapsed();
    let stats = graph.stats()?;
    Ok(BuildReport {
        nodes: stats.nodes,
        edges: stats.edges,
        duration_ms: duration.as_millis(),
        nodes_per_second: stats.nodes as f64 / duration.as_secs_f64(),
        edges_per_second: stats.edges as f64 / duration.as_secs_f64(),
        persisted_bytes: directory_size(&args.data_dir)?,
    })
}

fn integrity_checks(
    graph: &NativeGraphIndex<RocksDbBackend>,
    args: &Args,
    reopen_ms: u128,
) -> Result<IntegrityReport, Box<dyn std::error::Error>> {
    let (expected_nodes, expected_edges) = expected_counts(args);
    let stats_match =
        graph.stats()?.nodes == expected_nodes && graph.stats()?.edges == expected_edges;

    let before = graph.stats()?;
    let cross_workspace = GraphMutationBatch::new()
        .with_node(GraphNode::new(
            GraphNodeId::scoped(&args.workspace, "entity:cross-source"),
            NodeKind::Entity,
        ))
        .with_node(GraphNode::new(
            GraphNodeId::scoped("forbidden-workspace", "chunk:cross-target"),
            NodeKind::Chunk,
        ))
        .with_edge(GraphEdge::new(
            "cross-workspace-integrity-probe",
            GraphNodeId::scoped(&args.workspace, "entity:cross-source"),
            GraphNodeId::scoped("forbidden-workspace", "chunk:cross-target"),
            EdgeKind::RelatedTo,
        ));
    let cross_workspace_rejected_atomically =
        graph.upsert_batch(cross_workspace).is_err() && graph.stats()? == before;
    let excessive_depth_rejected = graph
        .path_exists(PathExistsRequest::new(
            document_id(&args.workspace, 0),
            entity_id(&args.workspace, 0),
            4,
        ))
        .is_err();

    let scratch_a = GraphNodeId::scoped(&args.workspace, "entity:delete-a");
    let scratch_b = GraphNodeId::scoped(&args.workspace, "entity:delete-b");
    let scratch_edge = GraphEdgeId::new("delete-integrity-edge");
    graph.upsert_batch(
        GraphMutationBatch::new()
            .with_node(GraphNode::new(scratch_a.clone(), NodeKind::Entity))
            .with_node(GraphNode::new(scratch_b.clone(), NodeKind::Entity))
            .with_edge(GraphEdge::new(
                scratch_edge.clone(),
                scratch_a.clone(),
                scratch_b.clone(),
                EdgeKind::RelatedTo,
            )),
    )?;
    let deleted = graph.delete_node(&scratch_a)?;
    let incident_edges_deleted =
        deleted.deleted && deleted.edges_deleted == 1 && graph.get_edge(&scratch_edge)?.is_none();
    graph.delete_node(&scratch_b)?;

    Ok(IntegrityReport {
        persistent_reopen_ms: reopen_ms,
        stats_match,
        cross_workspace_rejected_atomically,
        excessive_depth_rejected,
        incident_edges_deleted,
    })
}

fn measure_queries(graph: Arc<NativeGraphIndex<RocksDbBackend>>, args: &Args) -> QueryReport {
    let next = Arc::new(AtomicUsize::new(0));
    let measurements = Arc::new(Mutex::new(Measurements::default()));
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..args.concurrency {
            let graph = Arc::clone(&graph);
            let next = Arc::clone(&next);
            let measurements = Arc::clone(&measurements);
            scope.spawn(move || loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= args.queries {
                    break;
                }
                let operation = index % 5;
                let query_started = Instant::now();
                let result = execute_known_answer(&graph, args, index, operation);
                let elapsed = query_started.elapsed();
                let mut measured = measurements
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                measured.latencies.push(elapsed);
                let operation_name = operation_name(operation);
                *measured.operations.entry(operation_name).or_default() += 1;
                match result {
                    Ok(true) => measured.succeeded += 1,
                    Ok(false) => measured.incorrect += 1,
                    Err(()) => measured.errors += 1,
                }
            });
        }
    });
    let duration = started.elapsed();
    let measured = measurements
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let answered = measured.succeeded + measured.incorrect;
    QueryReport {
        requested: args.queries,
        succeeded: measured.succeeded,
        incorrect: measured.incorrect,
        errors: measured.errors,
        concurrency: args.concurrency,
        duration_ms: duration.as_millis(),
        qps: measured.succeeded as f64 / duration.as_secs_f64(),
        known_answer_accuracy: if answered == 0 {
            0.0
        } else {
            measured.succeeded as f64 / answered as f64
        },
        operations: measured.operations.clone(),
        latency: latency_report(&measured.latencies),
    }
}

fn operation_name(operation: usize) -> &'static str {
    match operation {
        0 => "one_hop_neighbors",
        1 => "two_hop_paths",
        2 => "bounded_path_exists",
        3 => "related_chunks",
        _ => "negative_path",
    }
}

fn execute_known_answer(
    graph: &NativeGraphIndex<RocksDbBackend>,
    args: &Args,
    query_index: usize,
    operation: usize,
) -> Result<bool, ()> {
    let document = (query_index / 5) % args.documents;
    let entity = document % args.entities;
    match operation {
        0 => {
            let expected = (0..args.chunks_per_document)
                .map(|chunk| chunk_id(&args.workspace, document, chunk))
                .collect::<HashSet<_>>();
            let observed = graph
                .neighbors(
                    NeighborRequest::new(document_id(&args.workspace, document))
                        .with_direction(Direction::Out)
                        .with_edge_kinds(vec![EdgeKind::Contains])
                        .with_limit(args.chunks_per_document + 1),
                )
                .map_err(|_| ())?
                .into_iter()
                .map(|neighbor| neighbor.node.id)
                .collect::<HashSet<_>>();
            Ok(observed == expected)
        }
        1 => {
            let paths = graph
                .two_hop(TwoHopRequest {
                    node_id: document_id(&args.workspace, document),
                    edge_kinds: Vec::new(),
                    first_hop_limit: args.chunks_per_document + 1,
                    second_hop_limit: 2,
                    limit: args.chunks_per_document + 1,
                })
                .map_err(|_| ())?;
            Ok(paths.len() == args.chunks_per_document
                && paths.iter().all(|path| {
                    path.nodes.last().map(|node| &node.id)
                        == Some(&entity_id(&args.workspace, entity))
                }))
        }
        2 => graph
            .path_exists(PathExistsRequest::new(
                document_id(&args.workspace, document),
                entity_id(&args.workspace, entity),
                2,
            ))
            .map_err(|_| ()),
        3 => {
            let expected = expected_entity_chunks(args, entity);
            let observed = graph
                .related_chunks_with_depth(
                    RelatedChunksRequest::new(entity_id(&args.workspace, entity))
                        .with_max_depth(1)
                        .with_per_hop_limit(expected.len() + 1)
                        .with_limit(expected.len() + 1),
                )
                .map_err(|_| ())?
                .into_iter()
                .map(|chunk| chunk.via_node)
                .collect::<HashSet<_>>();
            Ok(observed == expected)
        }
        _ => graph
            .path_exists(PathExistsRequest::new(
                document_id(&args.workspace, document),
                document_id(&args.workspace, (document + 1) % args.documents),
                3,
            ))
            .map(|exists| !exists)
            .map_err(|_| ()),
    }
}

fn expected_entity_chunks(args: &Args, entity: usize) -> HashSet<GraphNodeId> {
    (entity..args.documents)
        .step_by(args.entities)
        .flat_map(|document| {
            (0..args.chunks_per_document)
                .map(move |chunk| chunk_id(&args.workspace, document, chunk))
        })
        .collect()
}

fn latency_report(values: &[Duration]) -> LatencyReport {
    if values.is_empty() {
        return LatencyReport {
            count: 0,
            min_ms: 0.0,
            mean_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let as_ms = |value: Duration| value.as_secs_f64() * 1_000.0;
    LatencyReport {
        count: sorted.len(),
        min_ms: as_ms(sorted[0]),
        mean_ms: sorted.iter().map(|value| as_ms(*value)).sum::<f64>() / sorted.len() as f64,
        p50_ms: as_ms(percentile(&sorted, 0.50)),
        p95_ms: as_ms(percentile(&sorted, 0.95)),
        p99_ms: as_ms(percentile(&sorted, 0.99)),
        max_ms: as_ms(*sorted.last().unwrap()),
    }
}

fn percentile(values: &[Duration], quantile: f64) -> Duration {
    let index = ((values.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn directory_size(path: &Path) -> io::Result<u64> {
    let mut size = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        size += if metadata.is_dir() {
            directory_size(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(size)
}

fn generated_at_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn write_report(path: &Path, report: &GraphReport) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_file_name(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    fs::write(&temporary, serde_json::to_vec_pretty(report)?)?;
    fs::rename(temporary, path)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args).map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    fs::create_dir_all(&args.data_dir)?;
    let backend = Arc::new(RocksDbBackend::open(&args.data_dir)?);
    let graph = NativeGraphIndex::new(Arc::clone(&backend));
    let build = build_graph(&graph, &args)?;
    let (expected_nodes, expected_edges) = expected_counts(&args);
    drop(graph);
    drop(backend);

    let reopen_started = Instant::now();
    let backend = Arc::new(RocksDbBackend::open(&args.data_dir)?);
    let graph = Arc::new(NativeGraphIndex::new(backend));
    let reopen_ms = reopen_started.elapsed().as_millis();
    let integrity = integrity_checks(&graph, &args, reopen_ms)?;
    let query = measure_queries(Arc::clone(&graph), &args);

    let mut failures = Vec::new();
    if build.nodes != expected_nodes || build.edges != expected_edges {
        failures.push(format!(
            "materialized {}/{} nodes and {}/{} edges",
            build.nodes, expected_nodes, build.edges, expected_edges
        ));
    }
    if !integrity.stats_match {
        failures.push("persistent stats do not match the generated graph".to_string());
    }
    if !integrity.cross_workspace_rejected_atomically {
        failures.push("cross-workspace batch was not rejected atomically".to_string());
    }
    if !integrity.excessive_depth_rejected {
        failures.push("depth greater than three was not rejected".to_string());
    }
    if !integrity.incident_edges_deleted {
        failures.push("node deletion did not remove incident edges".to_string());
    }
    if query.incorrect != 0 || query.errors != 0 || query.succeeded != query.requested {
        failures.push(format!(
            "{} incorrect and {} errored queries out of {}",
            query.incorrect, query.errors, query.requested
        ));
    }
    if query.known_answer_accuracy < args.min_accuracy {
        failures.push(format!(
            "known-answer accuracy {:.6} is below {:.6}",
            query.known_answer_accuracy, args.min_accuracy
        ));
    }
    if query.qps < args.min_qps {
        failures.push(format!("QPS {:.3} is below {:.3}", query.qps, args.min_qps));
    }
    if args.max_p99_ms > 0.0 && query.latency.p99_ms > args.max_p99_ms {
        failures.push(format!(
            "p99 {:.3}ms exceeds {:.3}ms",
            query.latency.p99_ms, args.max_p99_ms
        ));
    }
    let report = GraphReport {
        schema_version: 1,
        report_type: "akidb.bounded-graph-benchmark.v1",
        generated_at_unix_ms: generated_at_unix_ms(),
        workload: WorkloadReport {
            workspace: args.workspace.clone(),
            documents: args.documents,
            chunks_per_document: args.chunks_per_document,
            entities: args.entities,
            batch_documents: args.batch_documents,
            topology: "document-contains-chunk-mentions-entity",
        },
        build,
        integrity,
        query,
        verdict: Verdict {
            status: if failures.is_empty() { "pass" } else { "fail" },
            failures,
        },
    };
    write_report(&args.output_json, &report)?;
    println!(
        "{}",
        serde_json::json!({
            "output": args.output_json,
            "verdict": report.verdict.status,
            "nodes": report.build.nodes,
            "edges": report.build.edges,
            "accuracy": report.query.known_answer_accuracy,
            "qps": report.query.qps,
            "p99_ms": report.query.latency.p99_ms,
            "failures": report.verdict.failures,
        })
    );
    if report.verdict.status == "pass" {
        Ok(())
    } else {
        Err("graph qualification gates failed".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_args() -> Args {
        Args {
            data_dir: PathBuf::from("unused"),
            workspace: "workspace-a".to_string(),
            documents: 10,
            chunks_per_document: 4,
            entities: 2,
            batch_documents: 2,
            queries: 10,
            concurrency: 2,
            min_accuracy: 1.0,
            min_qps: 0.0,
            max_p99_ms: 0.0,
            output_json: PathBuf::from("unused.json"),
        }
    }

    #[test]
    fn deterministic_topology_counts_are_exact() {
        assert_eq!(expected_counts(&test_args()), (52, 80));
    }

    #[test]
    fn entity_expected_chunks_cover_assigned_documents() {
        let args = test_args();
        let chunks = expected_entity_chunks(&args, 0);
        assert_eq!(chunks.len(), 20);
        assert!(chunks.contains(&chunk_id("workspace-a", 0, 0)));
        assert!(chunks.contains(&chunk_id("workspace-a", 8, 3)));
        assert!(!chunks.contains(&chunk_id("workspace-a", 1, 0)));
    }

    #[test]
    fn node_ids_are_workspace_scoped() {
        let id = chunk_id("workspace-a", 7, 2);
        assert_eq!(id.workspace_id(), Some("workspace-a"));
        assert_eq!(id.local_id(), "chunk:0000000007:002");
    }
}
