//! Ground-truth ANN benchmark compatible with standard fvecs/ivecs datasets.
//!
//! Unlike the synthetic capacity benchmark, this binary measures Recall@K
//! against exact neighbors and uses a fixed async worker pool.  This matches
//! the core methodology used by ANN-Benchmarks, VectorDBBench, Milvus, and
//! Weaviate without coupling the AkiDB release artifact to their runtimes.

use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::{
    DeleteRequest, DeleteStatus, HealthRequest, InsertBatchRequest, InsertRequest, SearchRequest,
    UpdateRequest, UpdateStatus, Vector,
};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

const MAX_DIMENSIONS: usize = 16_384;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// AkiDB gRPC origin.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    server: String,

    /// Dataset label retained in the report.
    #[arg(long)]
    dataset_name: String,

    /// Base vectors in fvecs format.
    #[arg(long)]
    train_fvecs: PathBuf,

    /// Query vectors in fvecs format.
    #[arg(long)]
    query_fvecs: PathBuf,

    /// Exact neighbor IDs in ivecs format.
    #[arg(long)]
    neighbors_ivecs: PathBuf,

    /// Distance metric configured on the isolated server under test.
    #[arg(long, value_parser = ["cosine", "l2", "ip"])]
    metric: String,

    /// AkiDB collection.
    #[arg(long, default_value = "default")]
    collection: String,

    /// Stable prefix used to map dataset row IDs to AkiDB IDs.
    #[arg(long, default_value = "ann")]
    id_prefix: String,

    /// Do not load train vectors; query an already loaded corpus.
    #[arg(long, default_value_t = false)]
    skip_load: bool,

    /// Quiescence window after a load, excluded from import and query timing.
    #[arg(long, default_value = "0")]
    post_load_settle_seconds: u64,

    /// Maximum train vectors to load (all when omitted).
    #[arg(long)]
    train_limit: Option<usize>,

    /// Maximum query vectors to measure (all when omitted).
    #[arg(long)]
    query_limit: Option<usize>,

    /// Insert batch size.
    #[arg(long, default_value = "256")]
    batch_size: usize,

    /// Neighbors returned and scored for recall.
    #[arg(long, default_value = "10")]
    top_k: usize,

    /// HNSW/IVF search breadth exposed by the portable API.
    #[arg(long, default_value = "64")]
    nprobe: u32,

    /// Fixed concurrent query workers.
    #[arg(long, default_value = "1")]
    concurrency: usize,

    /// Apply a deterministic metadata filter with selectivity 1/modulus.
    ///
    /// Train rows are labeled for moduli 2, 20, and 100 during load. The
    /// target label for each query is derived from its exact nearest neighbor,
    /// allowing the official ground-truth ordering to remain authoritative.
    #[arg(long)]
    filter_modulus: Option<u32>,

    /// Warm-up queries excluded from measurements.
    #[arg(long, default_value = "1000")]
    warmup_queries: usize,

    /// Full deterministic query-set repetitions included in measurements.
    #[arg(long, default_value = "1")]
    measurement_rounds: usize,

    /// Optional bearer credential environment variable.
    #[arg(long, default_value = "AKIDB_AUTH_TOKEN")]
    token_env: String,

    /// Authorized workspace metadata.
    #[arg(long, default_value = "default")]
    workspace: String,

    /// Optional PEM CA for an HTTPS gRPC origin.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Optional TLS server identity override.
    #[arg(long)]
    tls_domain: Option<String>,

    /// Connect and per-request timeout.
    #[arg(long, default_value = "30")]
    timeout_seconds: u64,

    /// Required average Recall@K gate.
    #[arg(long, default_value = "0")]
    min_recall: f64,

    /// Required successful measured QPS gate.
    #[arg(long, default_value = "0")]
    min_qps: f64,

    /// Maximum p99 request latency gate in milliseconds (zero disables).
    #[arg(long, default_value = "0")]
    max_p99_ms: f64,

    /// Concurrent insert-update-delete cycles per second (zero disables).
    #[arg(long, default_value = "0")]
    mixed_cycle_qps: f64,

    /// Duration of the concurrent mutation/search phase.
    #[arg(long, default_value = "0")]
    mixed_duration_seconds: u64,

    /// Concurrent mutation workers used to reach the requested cycle rate.
    #[arg(long, default_value = "8")]
    mixed_writers: usize,

    /// Unique ID prefix for transient mixed-workload rows.
    #[arg(long, default_value = "ann-mixed")]
    mixed_id_prefix: String,

    /// Machine-readable report path.
    #[arg(long)]
    output_json: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct FileIdentity {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct DatasetReport {
    name: String,
    dimensions: usize,
    train_vectors: usize,
    query_vectors: usize,
    ground_truth_width: usize,
    metric: String,
    train: FileIdentity,
    queries: FileIdentity,
    neighbors: FileIdentity,
}

#[derive(Debug, Clone, Serialize)]
struct LoadReport {
    skipped: bool,
    requested: usize,
    inserted: usize,
    failed: usize,
    duration_ms: u128,
    vectors_per_second: f64,
}

#[derive(Debug, Clone, Serialize)]
struct LatencyReport {
    count: usize,
    min_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct QueryReport {
    requested: usize,
    unique_queries: usize,
    measurement_rounds: usize,
    succeeded: usize,
    failed: usize,
    concurrency: usize,
    warmup_queries: usize,
    top_k: usize,
    nprobe: u32,
    duration_ms: u128,
    qps: f64,
    recall_at_k: f64,
    filter_violations: usize,
    result_count_violations: usize,
    duplicate_results: usize,
    unparseable_results: usize,
    invalid_scores: usize,
    latency: LatencyReport,
}

#[derive(Debug, Clone, Serialize)]
struct FilterReport {
    enabled: bool,
    metadata_key: Option<String>,
    modulus: Option<u32>,
    expected_selectivity: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct HealthReport {
    healthy: bool,
    ready: bool,
    total_vectors: u64,
    active_vectors: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AnnReport {
    schema_version: u32,
    report_type: &'static str,
    generated_at_unix_ms: u128,
    server: String,
    collection: String,
    dataset: DatasetReport,
    health_before: HealthReport,
    health_after: HealthReport,
    load: LoadReport,
    post_load_settle_seconds: u64,
    filter: FilterReport,
    query: QueryReport,
    mixed: Option<MixedReport>,
    verdict: Verdict,
}

#[derive(Debug, Clone, Serialize)]
struct Verdict {
    status: &'static str,
    failures: Vec<String>,
}

#[derive(Debug)]
struct QueryMeasurements {
    latencies: Vec<Duration>,
    recall_sum: f64,
    succeeded: usize,
    failed: usize,
    filter_violations: usize,
    result_count_violations: usize,
    duplicate_results: usize,
    unparseable_results: usize,
    invalid_scores: usize,
}

#[derive(Debug, Clone, Serialize)]
struct MixedReport {
    duration_seconds: u64,
    requested_cycle_qps: f64,
    mutation_writers: usize,
    mutation: MutationReport,
    search: MixedSearchReport,
    health_before: HealthReport,
    health_after: HealthReport,
}

#[derive(Debug, Clone, Serialize)]
struct MutationReport {
    requested_cycles: usize,
    completed_cycles: usize,
    failed_cycles: usize,
    insert_failures: usize,
    update_failures: usize,
    delete_failures: usize,
    duration_ms: u128,
    cycles_per_second: f64,
    insert_latency: LatencyReport,
    update_latency: LatencyReport,
    delete_latency: LatencyReport,
}

#[derive(Debug, Clone, Serialize)]
struct MixedSearchReport {
    requested: usize,
    succeeded: usize,
    failed: usize,
    concurrency: usize,
    top_k: usize,
    nprobe: u32,
    duration_ms: u128,
    qps: f64,
    recall_at_k: f64,
    result_count_violations: usize,
    duplicate_results: usize,
    unparseable_results: usize,
    invalid_scores: usize,
    latency: LatencyReport,
}

#[derive(Debug)]
struct MutationMeasurements {
    completed_cycles: usize,
    failed_cycles: usize,
    insert_failures: usize,
    update_failures: usize,
    delete_failures: usize,
    insert_latencies: Vec<Duration>,
    update_latencies: Vec<Duration>,
    delete_latencies: Vec<Duration>,
}

struct QueryDataset {
    queries: Vec<Vec<f32>>,
    neighbors: Vec<Vec<u32>>,
    dimensions: usize,
    ground_truth_width: usize,
}

struct FvecReader {
    reader: BufReader<File>,
    dimension: Option<usize>,
    path: PathBuf,
}

impl FvecReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
            dimension: None,
            path: path.to_path_buf(),
        })
    }

    fn next(&mut self) -> io::Result<Option<Vec<f32>>> {
        let Some(dimension) = read_dimension(&mut self.reader)? else {
            return Ok(None);
        };
        validate_dimension(&self.path, self.dimension, dimension)?;
        self.dimension = Some(dimension);
        let mut bytes = vec![0_u8; dimension * size_of::<f32>()];
        self.reader.read_exact(&mut bytes)?;
        Ok(Some(
            bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        ))
    }
}

struct IvecReader {
    reader: BufReader<File>,
    width: Option<usize>,
    path: PathBuf,
}

impl IvecReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
            width: None,
            path: path.to_path_buf(),
        })
    }

    fn next(&mut self) -> io::Result<Option<Vec<u32>>> {
        let Some(width) = read_dimension(&mut self.reader)? else {
            return Ok(None);
        };
        validate_dimension(&self.path, self.width, width)?;
        self.width = Some(width);
        let mut bytes = vec![0_u8; width * size_of::<i32>()];
        self.reader.read_exact(&mut bytes)?;
        let values = bytes
            .chunks_exact(4)
            .map(|chunk| {
                let value = i32::from_le_bytes(chunk.try_into().unwrap());
                u32::try_from(value).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} contains a negative neighbor ID", self.path.display()),
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Some(values))
    }
}

fn read_dimension(reader: &mut BufReader<File>) -> io::Result<Option<usize>> {
    let mut bytes = [0_u8; 4];
    let mut observed = 0;
    while observed < bytes.len() {
        match reader.read(&mut bytes[observed..])? {
            0 if observed == 0 => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated vector dimension",
                ))
            }
            count => observed += count,
        }
    }
    let dimension = i32::from_le_bytes(bytes);
    usize::try_from(dimension)
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_DIMENSIONS)
        .map(Some)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("vector dimension must be in 1..={MAX_DIMENSIONS}"),
            )
        })
}

fn validate_dimension(path: &Path, expected: Option<usize>, observed: usize) -> io::Result<()> {
    if expected.is_some_and(|value| value != observed) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} contains inconsistent vector dimensions", path.display()),
        ));
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    for (name, value) in [
        ("batch-size", args.batch_size),
        ("top-k", args.top_k),
        ("concurrency", args.concurrency),
        ("measurement-rounds", args.measurement_rounds),
        ("mixed-writers", args.mixed_writers),
    ] {
        if value == 0 {
            return Err(format!("--{name} must be positive"));
        }
    }
    if args.top_k > 1_000 {
        return Err("--top-k cannot exceed 1000".to_string());
    }
    if args.concurrency > 4_096 {
        return Err("--concurrency cannot exceed 4096".to_string());
    }
    if args.measurement_rounds > 10 {
        return Err("--measurement-rounds cannot exceed 10".to_string());
    }
    if args.mixed_writers > 256 {
        return Err("--mixed-writers cannot exceed 256".to_string());
    }
    if args.timeout_seconds == 0 || args.timeout_seconds > 300 {
        return Err("--timeout-seconds must be in 1..=300".to_string());
    }
    if args.post_load_settle_seconds > 600 {
        return Err("--post-load-settle-seconds cannot exceed 600".to_string());
    }
    if args.mixed_duration_seconds > 3_600 {
        return Err("--mixed-duration-seconds cannot exceed 3600".to_string());
    }
    if (args.mixed_cycle_qps > 0.0) != (args.mixed_duration_seconds > 0) {
        return Err(
            "--mixed-cycle-qps and --mixed-duration-seconds must both be set or both be zero"
                .to_string(),
        );
    }
    if args.train_limit == Some(0) || args.query_limit == Some(0) {
        return Err("--train-limit and --query-limit must be positive when set".to_string());
    }
    if args
        .filter_modulus
        .is_some_and(|modulus| !matches!(modulus, 2 | 20 | 100))
    {
        return Err("--filter-modulus must be one of 2, 20, or 100".to_string());
    }
    for (name, value) in [
        ("min-recall", args.min_recall),
        ("min-qps", args.min_qps),
        ("max-p99-ms", args.max_p99_ms),
        ("mixed-cycle-qps", args.mixed_cycle_qps),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(format!("--{name} must be finite and non-negative"));
        }
    }
    if args.min_recall > 1.0 {
        return Err("--min-recall cannot exceed 1".to_string());
    }
    if args.mixed_cycle_qps > 100_000.0 {
        return Err("--mixed-cycle-qps cannot exceed 100000".to_string());
    }
    if args.mixed_cycle_qps > 0.0
        && args.mixed_cycle_qps * (args.mixed_duration_seconds as f64) < 1.0
    {
        return Err("mixed workload must schedule at least one cycle".to_string());
    }
    if !is_canonical_origin(&args.server)
        || !is_canonical(&args.dataset_name)
        || !is_canonical(&args.collection)
        || !is_canonical(&args.workspace)
        || !is_canonical(&args.id_prefix)
        || !is_canonical(&args.mixed_id_prefix)
        || !is_environment_name(&args.token_env)
    {
        return Err(
            "collection, workspace, ID prefix, or token environment is not canonical".to_string(),
        );
    }
    if args.mixed_id_prefix == args.id_prefix {
        return Err("--mixed-id-prefix must differ from --id-prefix".to_string());
    }
    for path in [&args.train_fvecs, &args.query_fvecs, &args.neighbors_ivecs] {
        if !path.is_file() {
            return Err(format!("dataset file does not exist: {}", path.display()));
        }
    }
    Ok(())
}

fn is_canonical_origin(value: &str) -> bool {
    let authority = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"));
    authority.is_some_and(|authority| {
        !authority.is_empty() && !authority.contains(['/', '?', '#', '@', '\n', '\r', '\0', ' '])
    })
}

fn is_canonical(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= 256
        && !value.contains(['\n', '\r', '\0'])
}

fn is_environment_name(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(|first| first.is_ascii_uppercase())
        && chars.all(|value| value.is_ascii_uppercase() || value.is_ascii_digit() || value == '_')
}

async fn connect(args: &Args) -> Result<AkidbClient<Channel>, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(args.timeout_seconds);
    let mut endpoint = Endpoint::from_shared(args.server.clone())?
        .connect_timeout(timeout)
        .timeout(timeout);
    if args.server.starts_with("https://") {
        let mut tls = ClientTlsConfig::new();
        if let Some(domain) = &args.tls_domain {
            tls = tls.domain_name(domain);
        }
        if let Some(path) = &args.tls_ca {
            tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(path)?));
        }
        endpoint = endpoint.tls_config(tls)?;
    } else if args.tls_ca.is_some() || args.tls_domain.is_some() {
        return Err("TLS options require an https:// server".into());
    }
    Ok(AkidbClient::new(endpoint.connect().await?))
}

fn authenticated<T>(message: T, args: &Args) -> Result<Request<T>, Box<dyn std::error::Error>> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-akidb-workspace",
        MetadataValue::try_from(args.workspace.as_str())?,
    );
    request
        .metadata_mut()
        .insert("x-akidb-agent", MetadataValue::from_static("ann-benchmark"));
    if let Ok(token) = std::env::var(&args.token_env) {
        if token.is_empty() || token.trim() != token || token.contains(['\n', '\r']) {
            return Err(format!("{} contains a non-canonical token", args.token_env).into());
        }
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}"))?,
        );
    }
    Ok(request)
}

async fn health(
    client: &mut AkidbClient<Channel>,
    args: &Args,
) -> Result<HealthReport, Box<dyn std::error::Error>> {
    let value = client
        .health(authenticated(HealthRequest {}, args)?)
        .await?
        .into_inner();
    Ok(HealthReport {
        healthy: value.healthy,
        ready: value.ready,
        total_vectors: value.total_vectors,
        active_vectors: value.active_vectors,
    })
}

async fn load_train(
    client: &mut AkidbClient<Channel>,
    args: &Args,
) -> Result<(LoadReport, usize, usize), Box<dyn std::error::Error>> {
    let mut source = FvecReader::open(&args.train_fvecs)?;
    let mut inserted = 0;
    let mut failed = 0;
    let mut batch: Vec<Vector> = Vec::with_capacity(args.batch_size);
    let started = Instant::now();
    let limit = args.train_limit.unwrap_or(usize::MAX);

    while inserted + failed + batch.len() < limit {
        let Some(vector) = source.next()? else {
            break;
        };
        let row = inserted + failed + batch.len();
        batch.push(Vector {
            id: dataset_id(&args.id_prefix, row),
            embedding: vector,
            metadata: serde_json::to_vec(&serde_json::json!({
                "ann_row_id": row,
                "ann_label_100": row % 100,
                "ann_label_20": row % 20,
                "ann_label_2": row % 2,
            }))?,
            text: String::new(),
        });
        if batch.len() == args.batch_size {
            let (ok, not_ok) = insert_batch(client, args, std::mem::take(&mut batch)).await?;
            inserted += ok;
            failed += not_ok;
        }
    }
    if !batch.is_empty() {
        let (ok, not_ok) = insert_batch(client, args, batch).await?;
        inserted += ok;
        failed += not_ok;
    }
    let elapsed = started.elapsed();
    let requested = inserted + failed;
    Ok((
        LoadReport {
            skipped: false,
            requested,
            inserted,
            failed,
            duration_ms: elapsed.as_millis(),
            vectors_per_second: inserted as f64 / elapsed.as_secs_f64(),
        },
        source.dimension.unwrap_or(0),
        requested,
    ))
}

async fn insert_batch(
    client: &mut AkidbClient<Channel>,
    args: &Args,
    vectors: Vec<Vector>,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let requested = vectors.len();
    let response = client
        .insert_batch(authenticated(
            InsertBatchRequest {
                collection: args.collection.clone(),
                vectors,
            },
            args,
        )?)
        .await?
        .into_inner();
    let inserted = response.inserted_count as usize;
    let failed = requested.saturating_sub(inserted);
    if !response.success || failed != response.failed_ids.len() {
        return Err("insert batch returned inconsistent success evidence".into());
    }
    Ok((inserted, failed))
}

fn load_queries(args: &Args) -> Result<QueryDataset, Box<dyn std::error::Error>> {
    let mut query_source = FvecReader::open(&args.query_fvecs)?;
    let mut neighbor_source = IvecReader::open(&args.neighbors_ivecs)?;
    let mut queries = Vec::new();
    let mut neighbors = Vec::new();
    let limit = args.query_limit.unwrap_or(usize::MAX);
    while queries.len() < limit {
        match (query_source.next()?, neighbor_source.next()?) {
            (Some(query), Some(exact)) => {
                if exact.len() < args.top_k {
                    return Err("ground-truth width is smaller than top-k".into());
                }
                queries.push(query);
                neighbors.push(exact);
            }
            (None, None) => break,
            _ => return Err("query and ground-truth record counts differ".into()),
        }
    }
    if queries.is_empty() {
        return Err("query dataset is empty".into());
    }
    Ok(QueryDataset {
        queries,
        neighbors,
        dimensions: query_source.dimension.unwrap_or(0),
        ground_truth_width: neighbor_source.width.unwrap_or(0),
    })
}

async fn warmup(
    client: &mut AkidbClient<Channel>,
    args: &Args,
    dataset: &QueryDataset,
) -> Result<usize, Box<dyn std::error::Error>> {
    let count = args.warmup_queries.min(dataset.queries.len());
    for (query, expected) in dataset.queries.iter().zip(&dataset.neighbors).take(count) {
        client
            .search(authenticated(
                search_request(args, query.clone(), expected),
                args,
            )?)
            .await?;
    }
    Ok(count)
}

async fn measure_queries(
    client: AkidbClient<Channel>,
    args: &Args,
    queries: Vec<Vec<f32>>,
    neighbors: Vec<Vec<u32>>,
    warmup_count: usize,
) -> QueryReport {
    let queries = Arc::new(queries);
    let neighbors = Arc::new(neighbors);
    let total_requests = queries.len().saturating_mul(args.measurement_rounds);
    let next = Arc::new(AtomicUsize::new(0));
    let measurements = Arc::new(Mutex::new(QueryMeasurements {
        latencies: Vec::with_capacity(total_requests),
        recall_sum: 0.0,
        succeeded: 0,
        failed: 0,
        filter_violations: 0,
        result_count_violations: 0,
        duplicate_results: 0,
        unparseable_results: 0,
        invalid_scores: 0,
    }));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let mut worker_client = client.clone();
        let worker_queries = Arc::clone(&queries);
        let worker_neighbors = Arc::clone(&neighbors);
        let worker_next = Arc::clone(&next);
        let worker_measurements = Arc::clone(&measurements);
        let worker_args = WorkerArgs {
            collection: args.collection.clone(),
            id_prefix: args.id_prefix.clone(),
            workspace: args.workspace.clone(),
            token: std::env::var(&args.token_env).ok(),
            top_k: args.top_k,
            nprobe: args.nprobe,
            filter_modulus: args.filter_modulus,
        };
        workers.push(tokio::spawn(async move {
            loop {
                let task = worker_next.fetch_add(1, Ordering::Relaxed);
                if task >= total_requests {
                    break;
                }
                let index = task % worker_queries.len();
                let request = worker_request(
                    search_request_parts(
                        &worker_args.collection,
                        worker_args.top_k,
                        worker_args.nprobe,
                        worker_queries[index].clone(),
                        filter_bytes(worker_args.filter_modulus, &worker_neighbors[index]),
                    ),
                    &worker_args,
                );
                let query_started = Instant::now();
                let response = match request {
                    Ok(request) => worker_client.search(request).await,
                    Err(_) => {
                        let mut result = worker_measurements.lock().await;
                        result.failed += 1;
                        continue;
                    }
                };
                let latency = query_started.elapsed();
                let mut result = worker_measurements.lock().await;
                match response {
                    Ok(response) => {
                        let response_results = response.into_inner().results;
                        if response_results.len() != worker_args.top_k {
                            result.result_count_violations += 1;
                        }
                        result.invalid_scores += response_results
                            .iter()
                            .filter(|value| !value.score.is_finite())
                            .count();
                        let returned_rows = response_results
                            .iter()
                            .filter_map(|value| parse_dataset_id(&worker_args.id_prefix, &value.id))
                            .collect::<Vec<_>>();
                        let parsed_result_count = returned_rows.len();
                        result.unparseable_results +=
                            response_results.len().saturating_sub(returned_rows.len());
                        if let Some(modulus) = worker_args.filter_modulus {
                            let target = filter_target(modulus, &worker_neighbors[index]);
                            result.filter_violations += returned_rows
                                .iter()
                                .filter(|row| **row % modulus != target)
                                .count();
                        }
                        let returned = returned_rows.into_iter().collect::<HashSet<_>>();
                        result.duplicate_results +=
                            parsed_result_count.saturating_sub(returned.len());
                        let expected = filtered_ground_truth(
                            &worker_neighbors[index],
                            worker_args.top_k,
                            worker_args.filter_modulus,
                        );
                        result.recall_sum += recall_at_k(&returned, &expected, worker_args.top_k);
                        result.succeeded += 1;
                        result.latencies.push(latency);
                    }
                    Err(_) => result.failed += 1,
                }
            }
        }));
    }
    for worker in workers {
        if worker.await.is_err() {
            let mut result = measurements.lock().await;
            result.failed += 1;
        }
    }
    let elapsed = started.elapsed();
    let result = measurements.lock().await;
    QueryReport {
        requested: total_requests,
        unique_queries: queries.len(),
        measurement_rounds: args.measurement_rounds,
        succeeded: result.succeeded,
        failed: result.failed,
        concurrency: args.concurrency,
        warmup_queries: warmup_count,
        top_k: args.top_k,
        nprobe: args.nprobe,
        duration_ms: elapsed.as_millis(),
        qps: result.succeeded as f64 / elapsed.as_secs_f64(),
        recall_at_k: if result.succeeded == 0 {
            0.0
        } else {
            result.recall_sum / result.succeeded as f64
        },
        filter_violations: result.filter_violations,
        result_count_violations: result.result_count_violations,
        duplicate_results: result.duplicate_results,
        unparseable_results: result.unparseable_results,
        invalid_scores: result.invalid_scores,
        latency: latency_report(&result.latencies),
    }
}

async fn measure_mixed(
    client: AkidbClient<Channel>,
    args: &Args,
    queries: Vec<Vec<f32>>,
    neighbors: Vec<Vec<u32>>,
    health_before: HealthReport,
) -> Result<MixedReport, Box<dyn std::error::Error>> {
    let queries = Arc::new(queries);
    let neighbors = Arc::new(neighbors);
    let requested_cycles =
        (args.mixed_cycle_qps * args.mixed_duration_seconds as f64).floor() as usize;
    let next_cycle = Arc::new(AtomicUsize::new(0));
    let next_query = Arc::new(AtomicUsize::new(0));
    let writers_done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let schedule_started = tokio::time::Instant::now();
    let deadline = schedule_started + Duration::from_secs(args.mixed_duration_seconds);
    let worker_args = WorkerArgs {
        collection: args.collection.clone(),
        id_prefix: args.id_prefix.clone(),
        workspace: args.workspace.clone(),
        token: std::env::var(&args.token_env).ok(),
        top_k: args.top_k,
        nprobe: args.nprobe,
        filter_modulus: None,
    };

    let mut search_workers = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let mut worker_client = client.clone();
        let worker_queries = Arc::clone(&queries);
        let worker_neighbors = Arc::clone(&neighbors);
        let worker_next = Arc::clone(&next_query);
        let worker_done = Arc::clone(&writers_done);
        let worker_args = worker_args.clone();
        search_workers.push(tokio::spawn(async move {
            let mut result = QueryMeasurements {
                latencies: Vec::new(),
                recall_sum: 0.0,
                succeeded: 0,
                failed: 0,
                filter_violations: 0,
                result_count_violations: 0,
                duplicate_results: 0,
                unparseable_results: 0,
                invalid_scores: 0,
            };
            while tokio::time::Instant::now() < deadline || !worker_done.load(Ordering::Acquire) {
                let task = worker_next.fetch_add(1, Ordering::Relaxed);
                let index = task % worker_queries.len();
                let request = worker_request(
                    search_request_parts(
                        &worker_args.collection,
                        worker_args.top_k,
                        worker_args.nprobe,
                        worker_queries[index].clone(),
                        Vec::new(),
                    ),
                    &worker_args,
                );
                let query_started = Instant::now();
                let response = match request {
                    Ok(request) => worker_client.search(request).await,
                    Err(_) => {
                        result.failed += 1;
                        continue;
                    }
                };
                let latency = query_started.elapsed();
                match response {
                    Ok(response) => {
                        let response_results = response.into_inner().results;
                        if response_results.len() != worker_args.top_k {
                            result.result_count_violations += 1;
                        }
                        result.invalid_scores += response_results
                            .iter()
                            .filter(|value| !value.score.is_finite())
                            .count();
                        let returned_rows = response_results
                            .iter()
                            .filter_map(|value| parse_dataset_id(&worker_args.id_prefix, &value.id))
                            .collect::<Vec<_>>();
                        let parsed_result_count = returned_rows.len();
                        result.unparseable_results +=
                            response_results.len().saturating_sub(returned_rows.len());
                        let returned = returned_rows.into_iter().collect::<HashSet<_>>();
                        result.duplicate_results +=
                            parsed_result_count.saturating_sub(returned.len());
                        result.recall_sum +=
                            recall_at_k(&returned, &worker_neighbors[index], worker_args.top_k);
                        result.succeeded += 1;
                        result.latencies.push(latency);
                    }
                    Err(_) => result.failed += 1,
                }
            }
            result
        }));
    }

    let mut mutation_workers = Vec::with_capacity(args.mixed_writers);
    for _ in 0..args.mixed_writers {
        let mut worker_client = client.clone();
        let worker_queries = Arc::clone(&queries);
        let worker_next = Arc::clone(&next_cycle);
        let worker_args = worker_args.clone();
        let mixed_id_prefix = args.mixed_id_prefix.clone();
        let cycle_qps = args.mixed_cycle_qps;
        mutation_workers.push(tokio::spawn(async move {
            let mut result = MutationMeasurements {
                completed_cycles: 0,
                failed_cycles: 0,
                insert_failures: 0,
                update_failures: 0,
                delete_failures: 0,
                insert_latencies: Vec::new(),
                update_latencies: Vec::new(),
                delete_latencies: Vec::new(),
            };
            loop {
                let cycle = worker_next.fetch_add(1, Ordering::Relaxed);
                if cycle >= requested_cycles {
                    break;
                }
                let scheduled =
                    schedule_started + Duration::from_secs_f64(cycle as f64 / cycle_qps);
                tokio::time::sleep_until(scheduled).await;
                let id = dataset_id(&mixed_id_prefix, cycle);
                let shift = 1_000_000.0_f32 + (cycle % 1_000) as f32;
                let vector = worker_queries[cycle % worker_queries.len()]
                    .iter()
                    .map(|value| value + shift)
                    .collect::<Vec<_>>();
                let metadata =
                    format!(r#"{{"ann_mixed":true,"cycle":{cycle},"revision":1}}"#).into_bytes();

                let insert_started = Instant::now();
                let inserted = match worker_request(
                    InsertRequest {
                        collection: worker_args.collection.clone(),
                        id: id.clone(),
                        vector: vector.clone(),
                        metadata,
                        text: String::new(),
                    },
                    &worker_args,
                ) {
                    Ok(request) => worker_client
                        .insert(request)
                        .await
                        .map(|response| response.into_inner().success)
                        .unwrap_or(false),
                    Err(_) => false,
                };
                result.insert_latencies.push(insert_started.elapsed());
                if !inserted {
                    result.insert_failures += 1;
                    result.failed_cycles += 1;
                    continue;
                }

                let update_started = Instant::now();
                let updated = match worker_request(
                    UpdateRequest {
                        collection: worker_args.collection.clone(),
                        id: id.clone(),
                        vector: vector.iter().map(|value| value + 1.0).collect(),
                        metadata: format!(r#"{{"ann_mixed":true,"cycle":{cycle},"revision":2}}"#)
                            .into_bytes(),
                    },
                    &worker_args,
                ) {
                    Ok(request) => worker_client
                        .update(request)
                        .await
                        .map(|response| {
                            let value = response.into_inner();
                            value.success && value.status == UpdateStatus::Updated as i32
                        })
                        .unwrap_or(false),
                    Err(_) => false,
                };
                result.update_latencies.push(update_started.elapsed());
                if !updated {
                    result.update_failures += 1;
                }

                let delete_started = Instant::now();
                let deleted = match worker_request(
                    DeleteRequest {
                        collection: worker_args.collection.clone(),
                        id,
                    },
                    &worker_args,
                ) {
                    Ok(request) => worker_client
                        .delete(request)
                        .await
                        .map(|response| {
                            let value = response.into_inner();
                            value.success && value.status == DeleteStatus::Deleted as i32
                        })
                        .unwrap_or(false),
                    Err(_) => false,
                };
                result.delete_latencies.push(delete_started.elapsed());
                if !deleted {
                    result.delete_failures += 1;
                }
                if updated && deleted {
                    result.completed_cycles += 1;
                } else {
                    result.failed_cycles += 1;
                }
            }
            result
        }));
    }

    let mut mutation = MutationMeasurements {
        completed_cycles: 0,
        failed_cycles: 0,
        insert_failures: 0,
        update_failures: 0,
        delete_failures: 0,
        insert_latencies: Vec::new(),
        update_latencies: Vec::new(),
        delete_latencies: Vec::new(),
    };
    for worker in mutation_workers {
        match worker.await {
            Ok(value) => {
                mutation.completed_cycles += value.completed_cycles;
                mutation.failed_cycles += value.failed_cycles;
                mutation.insert_failures += value.insert_failures;
                mutation.update_failures += value.update_failures;
                mutation.delete_failures += value.delete_failures;
                mutation.insert_latencies.extend(value.insert_latencies);
                mutation.update_latencies.extend(value.update_latencies);
                mutation.delete_latencies.extend(value.delete_latencies);
            }
            Err(_) => mutation.failed_cycles += 1,
        }
    }
    writers_done.store(true, Ordering::Release);

    let mut search = QueryMeasurements {
        latencies: Vec::new(),
        recall_sum: 0.0,
        succeeded: 0,
        failed: 0,
        filter_violations: 0,
        result_count_violations: 0,
        duplicate_results: 0,
        unparseable_results: 0,
        invalid_scores: 0,
    };
    for worker in search_workers {
        match worker.await {
            Ok(value) => {
                search.latencies.extend(value.latencies);
                search.recall_sum += value.recall_sum;
                search.succeeded += value.succeeded;
                search.failed += value.failed;
                search.result_count_violations += value.result_count_violations;
                search.duplicate_results += value.duplicate_results;
                search.unparseable_results += value.unparseable_results;
                search.invalid_scores += value.invalid_scores;
            }
            Err(_) => search.failed += 1,
        }
    }
    let elapsed = started.elapsed();
    let mut health_client = client;
    let health_after = health(&mut health_client, args).await?;
    let search_requested = search.succeeded + search.failed;
    Ok(MixedReport {
        duration_seconds: args.mixed_duration_seconds,
        requested_cycle_qps: args.mixed_cycle_qps,
        mutation_writers: args.mixed_writers,
        mutation: MutationReport {
            requested_cycles,
            completed_cycles: mutation.completed_cycles,
            failed_cycles: mutation.failed_cycles,
            insert_failures: mutation.insert_failures,
            update_failures: mutation.update_failures,
            delete_failures: mutation.delete_failures,
            duration_ms: elapsed.as_millis(),
            cycles_per_second: mutation.completed_cycles as f64 / elapsed.as_secs_f64(),
            insert_latency: latency_report(&mutation.insert_latencies),
            update_latency: latency_report(&mutation.update_latencies),
            delete_latency: latency_report(&mutation.delete_latencies),
        },
        search: MixedSearchReport {
            requested: search_requested,
            succeeded: search.succeeded,
            failed: search.failed,
            concurrency: args.concurrency,
            top_k: args.top_k,
            nprobe: args.nprobe,
            duration_ms: elapsed.as_millis(),
            qps: search.succeeded as f64 / elapsed.as_secs_f64(),
            recall_at_k: if search.succeeded == 0 {
                0.0
            } else {
                search.recall_sum / search.succeeded as f64
            },
            result_count_violations: search.result_count_violations,
            duplicate_results: search.duplicate_results,
            unparseable_results: search.unparseable_results,
            invalid_scores: search.invalid_scores,
            latency: latency_report(&search.latencies),
        },
        health_before,
        health_after,
    })
}

#[derive(Clone)]
struct WorkerArgs {
    collection: String,
    id_prefix: String,
    workspace: String,
    token: Option<String>,
    top_k: usize,
    nprobe: u32,
    filter_modulus: Option<u32>,
}

fn worker_request<T>(
    message: T,
    args: &WorkerArgs,
) -> Result<Request<T>, tonic::metadata::errors::InvalidMetadataValue> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-akidb-workspace",
        MetadataValue::try_from(args.workspace.as_str())?,
    );
    request
        .metadata_mut()
        .insert("x-akidb-agent", MetadataValue::from_static("ann-benchmark"));
    if let Some(token) = &args.token {
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}"))?,
        );
    }
    Ok(request)
}

fn search_request(args: &Args, query: Vec<f32>, expected: &[u32]) -> SearchRequest {
    search_request_parts(
        &args.collection,
        args.top_k,
        args.nprobe,
        query,
        filter_bytes(args.filter_modulus, expected),
    )
}

fn search_request_parts(
    collection: &str,
    top_k: usize,
    nprobe: u32,
    query: Vec<f32>,
    filter: Vec<u8>,
) -> SearchRequest {
    SearchRequest {
        collection: collection.to_string(),
        query,
        top_k: top_k as u32,
        nprobe: Some(nprobe),
        filter,
        tag_filter: None,
        score_threshold: None,
        group_by: String::new(),
        group_size: None,
    }
}

fn filter_target(modulus: u32, expected: &[u32]) -> u32 {
    expected[0] % modulus
}

fn filter_bytes(modulus: Option<u32>, expected: &[u32]) -> Vec<u8> {
    modulus.map_or_else(Vec::new, |modulus| {
        format!(
            r#"{{"ann_label_{modulus}":{}}}"#,
            filter_target(modulus, expected)
        )
        .into_bytes()
    })
}

fn filtered_ground_truth(expected: &[u32], top_k: usize, modulus: Option<u32>) -> Vec<u32> {
    match modulus {
        Some(modulus) => {
            let target = filter_target(modulus, expected);
            expected
                .iter()
                .copied()
                .filter(|row| row % modulus == target)
                .take(top_k)
                .collect()
        }
        None => expected.iter().copied().take(top_k).collect(),
    }
}

fn recall_at_k(returned: &HashSet<u32>, expected: &[u32], top_k: usize) -> f64 {
    let expected = expected.iter().take(top_k).copied().collect::<HashSet<_>>();
    returned.intersection(&expected).count() as f64 / top_k as f64
}

fn validate_ground_truth(neighbors: &[Vec<u32>], train_vectors: usize) -> Result<(), String> {
    for (query_index, expected) in neighbors.iter().enumerate() {
        let unique = expected.iter().copied().collect::<HashSet<_>>();
        if unique.len() != expected.len() {
            return Err(format!(
                "ground truth query {query_index} contains duplicate IDs"
            ));
        }
        if let Some(value) = expected
            .iter()
            .find(|value| **value as usize >= train_vectors)
        {
            return Err(format!(
                "ground truth query {query_index} references row {value} outside {train_vectors} train vectors"
            ));
        }
    }
    Ok(())
}

fn validate_filtered_ground_truth(
    neighbors: &[Vec<u32>],
    top_k: usize,
    modulus: Option<u32>,
) -> Result<(), String> {
    let Some(modulus) = modulus else {
        return Ok(());
    };
    for (query_index, expected) in neighbors.iter().enumerate() {
        let filtered = filtered_ground_truth(expected, top_k, Some(modulus));
        if filtered.len() != top_k {
            return Err(format!(
                "ground truth query {query_index} has only {} exact neighbors for modulus {modulus}; top-k requires {top_k}",
                filtered.len()
            ));
        }
    }
    Ok(())
}

fn dataset_id(prefix: &str, row: usize) -> String {
    format!("{prefix}-{row:010}")
}

fn parse_dataset_id(prefix: &str, value: &str) -> Option<u32> {
    value
        .strip_prefix(prefix)?
        .strip_prefix('-')?
        .parse::<u32>()
        .ok()
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

fn file_identity(path: &Path) -> io::Result<FileIdentity> {
    let mut source = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(FileIdentity {
        path: path.display().to_string(),
        bytes: path.metadata()?.len(),
        sha256: format!("{:x}", digest.finalize()),
    })
}

fn generated_at_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn write_report(path: &Path, report: &AnnReport) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_file_name(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&temporary, serde_json::to_vec_pretty(report)?)?;
    std::fs::rename(temporary, path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args).map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    let train_identity = file_identity(&args.train_fvecs)?;
    let query_identity = file_identity(&args.query_fvecs)?;
    let neighbor_identity = file_identity(&args.neighbors_ivecs)?;
    let query_dataset = load_queries(&args)?;
    let dataset_query_vectors = query_dataset.queries.len();
    let mut client = connect(&args).await?;
    let health_before = health(&mut client, &args).await?;
    if !health_before.healthy || !health_before.ready {
        return Err("AkiDB is not healthy and ready".into());
    }
    if !args.skip_load && health_before.active_vectors != 0 {
        return Err(format!(
            "market benchmark requires an empty isolated server, found {} active vectors",
            health_before.active_vectors
        )
        .into());
    }

    let (load, train_dimensions, train_vectors) = if args.skip_load {
        if health_before.active_vectors == 0 {
            return Err("--skip-load requires a non-empty AkiDB corpus".into());
        }
        let mut source = FvecReader::open(&args.train_fvecs)?;
        let first = source.next()?.ok_or("train dataset is empty")?;
        (
            LoadReport {
                skipped: true,
                requested: 0,
                inserted: 0,
                failed: 0,
                duration_ms: 0,
                vectors_per_second: 0.0,
            },
            first.len(),
            args.train_limit
                .unwrap_or(health_before.active_vectors as usize),
        )
    } else {
        load_train(&mut client, &args).await?
    };
    if train_dimensions == 0 || train_dimensions != query_dataset.dimensions {
        return Err(format!(
            "train dimension {train_dimensions} differs from query dimension {}",
            query_dataset.dimensions
        )
        .into());
    }
    validate_ground_truth(&query_dataset.neighbors, train_vectors)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
    validate_filtered_ground_truth(&query_dataset.neighbors, args.top_k, args.filter_modulus)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
    if !load.skipped && args.post_load_settle_seconds > 0 {
        tokio::time::sleep(Duration::from_secs(args.post_load_settle_seconds)).await;
    }
    let health_after = health(&mut client, &args).await?;
    let warmup_count = warmup(&mut client, &args, &query_dataset).await?;
    let mixed_inputs = (args.mixed_cycle_qps > 0.0).then(|| {
        (
            client.clone(),
            query_dataset.queries.clone(),
            query_dataset.neighbors.clone(),
            health_after.clone(),
        )
    });
    let query = measure_queries(
        client,
        &args,
        query_dataset.queries,
        query_dataset.neighbors,
        warmup_count,
    )
    .await;
    let mixed = match mixed_inputs {
        Some((mixed_client, queries, neighbors, mixed_health_before)) => {
            Some(measure_mixed(mixed_client, &args, queries, neighbors, mixed_health_before).await?)
        }
        None => None,
    };

    let mut failures = Vec::new();
    if load.failed != 0 {
        failures.push(format!("{} vectors failed to load", load.failed));
    }
    if !load.skipped && health_after.active_vectors != load.inserted as u64 {
        failures.push(format!(
            "health reports {} active vectors after inserting {} into an empty server",
            health_after.active_vectors, load.inserted
        ));
    }
    if query.failed != 0 || query.succeeded != query.requested {
        failures.push(format!(
            "{} of {} measured queries failed",
            query.failed, query.requested
        ));
    }
    if query.filter_violations != 0 {
        failures.push(format!(
            "{} returned vectors violated the requested metadata filter",
            query.filter_violations
        ));
    }
    if query.result_count_violations != 0
        || query.duplicate_results != 0
        || query.unparseable_results != 0
        || query.invalid_scores != 0
    {
        failures.push(format!(
            "{} result-count violations, {} duplicate IDs, {} unparseable IDs, and {} invalid scores",
            query.result_count_violations,
            query.duplicate_results,
            query.unparseable_results,
            query.invalid_scores
        ));
    }
    if query.recall_at_k < args.min_recall {
        failures.push(format!(
            "Recall@{} {:.6} is below {:.6}",
            args.top_k, query.recall_at_k, args.min_recall
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
    if let Some(mixed) = &mixed {
        if mixed.mutation.completed_cycles != mixed.mutation.requested_cycles
            || mixed.mutation.failed_cycles != 0
            || mixed.mutation.insert_failures != 0
            || mixed.mutation.update_failures != 0
            || mixed.mutation.delete_failures != 0
        {
            failures.push(format!(
                "mixed mutations completed {} of {} cycles with {} failed cycles, {} insert failures, {} update failures, and {} delete failures",
                mixed.mutation.completed_cycles,
                mixed.mutation.requested_cycles,
                mixed.mutation.failed_cycles,
                mixed.mutation.insert_failures,
                mixed.mutation.update_failures,
                mixed.mutation.delete_failures,
            ));
        }
        if mixed.mutation.cycles_per_second < args.mixed_cycle_qps * 0.90 {
            failures.push(format!(
                "mixed mutation rate {:.3} cycles/s is below 90% of requested {:.3}",
                mixed.mutation.cycles_per_second, args.mixed_cycle_qps
            ));
        }
        if mixed.search.requested == 0
            || mixed.search.failed != 0
            || mixed.search.succeeded != mixed.search.requested
        {
            failures.push(format!(
                "{} of {} mixed-workload searches failed",
                mixed.search.failed, mixed.search.requested
            ));
        }
        if mixed.search.result_count_violations != 0
            || mixed.search.duplicate_results != 0
            || mixed.search.unparseable_results != 0
            || mixed.search.invalid_scores != 0
        {
            failures.push(format!(
                "mixed search observed {} result-count violations, {} duplicate IDs, {} unparseable IDs, and {} invalid scores",
                mixed.search.result_count_violations,
                mixed.search.duplicate_results,
                mixed.search.unparseable_results,
                mixed.search.invalid_scores,
            ));
        }
        if mixed.search.recall_at_k < args.min_recall {
            failures.push(format!(
                "mixed Recall@{} {:.6} is below {:.6}",
                args.top_k, mixed.search.recall_at_k, args.min_recall
            ));
        }
        if !mixed.health_after.healthy
            || !mixed.health_after.ready
            || mixed.health_after.active_vectors != mixed.health_before.active_vectors
        {
            failures.push(format!(
                "mixed workload did not reconcile active vectors: before={}, after={}",
                mixed.health_before.active_vectors, mixed.health_after.active_vectors
            ));
        }
    }
    let report = AnnReport {
        schema_version: 2,
        report_type: "akidb.market-ann-benchmark.v2",
        generated_at_unix_ms: generated_at_unix_ms(),
        server: args.server.clone(),
        collection: args.collection.clone(),
        dataset: DatasetReport {
            name: args.dataset_name.clone(),
            dimensions: train_dimensions,
            train_vectors,
            query_vectors: dataset_query_vectors,
            ground_truth_width: query_dataset.ground_truth_width,
            metric: args.metric.clone(),
            train: train_identity,
            queries: query_identity,
            neighbors: neighbor_identity,
        },
        health_before,
        health_after,
        load,
        post_load_settle_seconds: if args.skip_load {
            0
        } else {
            args.post_load_settle_seconds
        },
        filter: FilterReport {
            enabled: args.filter_modulus.is_some(),
            metadata_key: args
                .filter_modulus
                .map(|modulus| format!("ann_label_{modulus}")),
            modulus: args.filter_modulus,
            expected_selectivity: args.filter_modulus.map(|modulus| 1.0 / f64::from(modulus)),
        },
        query,
        mixed,
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
            "recall_at_k": report.query.recall_at_k,
            "qps": report.query.qps,
            "p99_ms": report.query.latency.p99_ms,
            "filter_violations": report.query.filter_violations,
            "result_count_violations": report.query.result_count_violations,
            "duplicate_results": report.query.duplicate_results,
            "unparseable_results": report.query.unparseable_results,
            "invalid_scores": report.query.invalid_scores,
            "mixed_cycles": report.mixed.as_ref().map(|value| value.mutation.completed_cycles),
            "mixed_search_recall": report.mixed.as_ref().map(|value| value.search.recall_at_k),
            "failures": report.verdict.failures,
        })
    );
    if report.verdict.status == "pass" {
        Ok(())
    } else {
        Err("ANN qualification gates failed".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_fvec(file: &mut NamedTempFile, values: &[f32]) {
        file.write_all(&(values.len() as i32).to_le_bytes())
            .unwrap();
        for value in values {
            file.write_all(&value.to_le_bytes()).unwrap();
        }
    }

    fn write_ivec(file: &mut NamedTempFile, values: &[i32]) {
        file.write_all(&(values.len() as i32).to_le_bytes())
            .unwrap();
        for value in values {
            file.write_all(&value.to_le_bytes()).unwrap();
        }
    }

    #[test]
    fn reads_fvecs_and_rejects_mixed_dimensions() {
        let mut file = NamedTempFile::new().unwrap();
        write_fvec(&mut file, &[1.0, 2.0]);
        write_fvec(&mut file, &[3.0, 4.0]);
        let mut reader = FvecReader::open(file.path()).unwrap();
        assert_eq!(reader.next().unwrap(), Some(vec![1.0, 2.0]));
        assert_eq!(reader.next().unwrap(), Some(vec![3.0, 4.0]));
        assert_eq!(reader.next().unwrap(), None);

        let mut invalid = NamedTempFile::new().unwrap();
        write_fvec(&mut invalid, &[1.0, 2.0]);
        write_fvec(&mut invalid, &[3.0]);
        let mut reader = FvecReader::open(invalid.path()).unwrap();
        reader.next().unwrap();
        assert_eq!(
            reader.next().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn reads_ivecs_and_rejects_negative_ids() {
        let mut file = NamedTempFile::new().unwrap();
        write_ivec(&mut file, &[1, 2, 3]);
        let mut reader = IvecReader::open(file.path()).unwrap();
        assert_eq!(reader.next().unwrap(), Some(vec![1, 2, 3]));

        let mut invalid = NamedTempFile::new().unwrap();
        write_ivec(&mut invalid, &[1, -2]);
        let mut reader = IvecReader::open(invalid.path()).unwrap();
        assert_eq!(
            reader.next().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn computes_standard_recall_at_k() {
        let returned = HashSet::from([2, 3, 9]);
        assert_eq!(recall_at_k(&returned, &[1, 2, 3, 4], 3), 2.0 / 3.0);
    }

    #[test]
    fn maps_dataset_ids_round_trip() {
        let value = dataset_id("sift1m", 42);
        assert_eq!(value, "sift1m-0000000042");
        assert_eq!(parse_dataset_id("sift1m", &value), Some(42));
        assert_eq!(parse_dataset_id("other", &value), None);
    }

    #[test]
    fn validates_ground_truth_bounds_and_uniqueness() {
        assert!(validate_ground_truth(&[vec![0, 2, 1]], 3).is_ok());
        assert!(validate_ground_truth(&[vec![0, 0, 1]], 3)
            .unwrap_err()
            .contains("duplicate"));
        assert!(validate_ground_truth(&[vec![0, 3, 1]], 3)
            .unwrap_err()
            .contains("outside"));
    }

    #[test]
    fn derives_exact_filtered_ground_truth_from_ordered_neighbors() {
        let expected = vec![41, 2, 4, 7, 6, 8];
        assert_eq!(filtered_ground_truth(&expected, 3, Some(2)), vec![41, 7]);
        assert!(
            validate_filtered_ground_truth(std::slice::from_ref(&expected), 2, Some(2)).is_ok()
        );
        assert!(validate_filtered_ground_truth(&[expected], 3, Some(2))
            .unwrap_err()
            .contains("only 2"));
        assert_eq!(
            filter_bytes(Some(20), &[41]),
            br#"{"ann_label_20":1}"#.to_vec()
        );
    }
}
