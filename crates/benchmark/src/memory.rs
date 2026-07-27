//! Authoritative AkiDB Memory systems benchmark.
//!
//! The benchmark uses the public gRPC MemoryService, commits deterministic
//! active versions with synced durability, verifies projection visibility and
//! known-answer recall, and writes a complete machine-readable run report.

use akidb_proto::memory_content;
use akidb_proto::memory_service_client::MemoryServiceClient;
use akidb_proto::{
    GetMemoryCapabilitiesRequest, MemoryContent, MemoryEpistemicFormation, MemoryEvidenceInput,
    MemoryRecallRequest, MemoryRememberRequest, MemoryRequestContext, MemoryScopeInput,
    MemorySensitivity, MemoryTextFact,
};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Endpoint;
use tonic::Request;

const REPORT_SCHEMA_VERSION: u32 = 1;
const RSS_SAMPLE_INTERVAL_MS: u64 = 100;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Args {
    /// AkiDB gRPC origin.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    server: String,

    /// Process-pinned authorized workspace.
    #[arg(long, default_value = "memory-benchmark")]
    workspace: String,

    /// Unique empty namespace for this run.
    #[arg(long)]
    namespace: String,

    #[arg(long, default_value = "memory-benchmark")]
    purpose: String,

    /// Stable run ID embedded in generated source and entity IDs.
    #[arg(long)]
    run_id: String,

    /// Stable operator-supplied host label for multi-host qualification.
    #[arg(long)]
    host_label: Option<String>,

    /// Bearer credential environment variable.
    #[arg(long, default_value = "AKIDB_MEMORY_PRINCIPAL_TOKEN")]
    token_env: String,

    /// Fallback bearer credential file.
    #[arg(long)]
    token_file: Option<PathBuf>,

    /// Number of active versions to create.
    #[arg(long)]
    versions: usize,

    /// Simultaneous unary Remember RPCs.
    #[arg(long, default_value_t = 8)]
    commit_concurrency: usize,

    /// Measured known-answer recalls.
    #[arg(long, default_value_t = 1000)]
    queries: usize,

    /// Unmeasured recalls before the measured sample.
    #[arg(long, default_value_t = 20)]
    warmup_queries: usize,

    /// Simultaneous Recall RPCs.
    #[arg(long, default_value_t = 8)]
    query_concurrency: usize,

    /// Maximum items per recall.
    #[arg(long, default_value_t = 10)]
    top_k: u32,

    /// Context budget used for every recall.
    #[arg(long, default_value_t = 256)]
    context_tokens: u32,

    /// Connect and per-RPC deadline.
    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,

    /// Server RocksDB directory, when benchmark and server share a host.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Server PID for resident-set sampling.
    #[arg(long)]
    server_pid: Option<u32>,

    /// Immutable JSON report path.
    #[arg(long)]
    output_json: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct LatencyStats {
    count: usize,
    min_us: u128,
    mean_us: u128,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
    samples_us: Vec<u128>,
}

#[derive(Debug, Serialize)]
struct Hardware {
    hostname: String,
    machine_id_sha256: Option<String>,
    os: String,
    kernel: String,
    architecture: String,
    cpu: String,
    logical_cores: usize,
    memory_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Software {
    akidb_version: &'static str,
    git_commit: String,
    rustc: String,
    git_status_available: bool,
    dirty_worktree: bool,
}

#[derive(Debug, Serialize)]
struct RunConfiguration {
    server: String,
    workspace: String,
    namespace: String,
    purpose: String,
    run_id: String,
    host_label: Option<String>,
    versions: usize,
    commit_concurrency: usize,
    queries: usize,
    warmup_queries: usize,
    query_concurrency: usize,
    top_k: u32,
    context_tokens: u32,
    timeout_seconds: u64,
    token_source: &'static str,
    data_dir: Option<String>,
    server_pid: Option<u32>,
}

#[derive(Debug, Serialize)]
struct CapabilitySnapshot {
    profile_status: String,
    supported_rpcs: Vec<String>,
    durability_modes: Vec<String>,
    active_projection_recipes: Vec<String>,
    workspace_topology: String,
    active_projection_manifest_sha256: String,
    tokenizer_artifact_id: String,
    server_build_id: String,
    retention_policy: Option<RetentionSnapshot>,
}

#[derive(Debug, Serialize)]
struct RetentionSnapshot {
    raw_event_seconds: u64,
    memory_version_seconds: u64,
    compiler_artifact_seconds: u64,
    index_artifact_seconds: u64,
    audit_seconds: u64,
    snapshot_seconds: u64,
    zero_means_indefinite: bool,
    finite_windows_enforced: bool,
}

#[derive(Debug, Serialize)]
struct CommitReport {
    requested: usize,
    succeeded: usize,
    failed: usize,
    wall_time_ms: u128,
    throughput_per_second: f64,
    first_commit_sequence: Option<u64>,
    last_commit_sequence: Option<u64>,
    maximum_visibility_lag_sequences: u64,
    acknowledgement_through_visible_latency: LatencyStats,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RecallReport {
    requested: usize,
    succeeded: usize,
    incorrect: usize,
    failed: usize,
    wall_time_ms: u128,
    throughput_per_second: f64,
    latency: LatencyStats,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ResourceReport {
    disk_bytes_before: Option<u64>,
    disk_bytes_after_commits: Option<u64>,
    disk_bytes_after_queries: Option<u64>,
    disk_bytes_delta: Option<u64>,
    server_rss_bytes_before: Option<u64>,
    server_rss_bytes_after_commits: Option<u64>,
    server_rss_bytes_after_queries: Option<u64>,
    peak_observed_server_rss_bytes: Option<u64>,
    server_rss_sample_interval_ms: Option<u64>,
    server_rss_sample_count: u64,
}

#[derive(Debug, Serialize)]
struct Verdict {
    status: &'static str,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    schema_version: u32,
    report_type: &'static str,
    generated_at_unix_ms: u128,
    dataset_sha256: String,
    hardware: Hardware,
    software: Software,
    configuration: RunConfiguration,
    capabilities: CapabilitySnapshot,
    commit: CommitReport,
    recall: RecallReport,
    resources: ResourceReport,
    verdict: Verdict,
}

#[derive(Debug)]
struct CommitSample {
    index: usize,
    version_id: String,
    commit_sequence: u64,
    visibility_lag: u64,
    latency: Duration,
}

#[derive(Debug)]
struct QuerySample {
    correct: bool,
    latency: Duration,
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
    handle: tokio::task::JoinHandle<()>,
}

fn validate_args(args: &Args) -> Result<(), String> {
    for (name, value, maximum) in [
        ("versions", args.versions, 10_000_000),
        ("commit-concurrency", args.commit_concurrency, 4096),
        ("queries", args.queries, 1_000_000),
        ("query-concurrency", args.query_concurrency, 4096),
        ("top-k", args.top_k as usize, 100),
        ("context-tokens", args.context_tokens as usize, 1_000_000),
    ] {
        if value == 0 || value > maximum {
            return Err(format!("--{name} must be between 1 and {maximum}"));
        }
    }
    if args.warmup_queries > 1_000_000 {
        return Err("--warmup-queries cannot exceed 1000000".to_string());
    }
    if args.timeout_seconds == 0 || args.timeout_seconds > 3600 {
        return Err("--timeout-seconds must be between 1 and 3600".to_string());
    }
    for (name, value) in [
        ("workspace", args.workspace.as_str()),
        ("namespace", args.namespace.as_str()),
        ("purpose", args.purpose.as_str()),
        ("run-id", args.run_id.as_str()),
    ] {
        if value.is_empty()
            || value.trim() != value
            || value.len() > 1024
            || value.contains(['\0', '\n', '\r'])
        {
            return Err(format!("--{name} is not canonical bounded text"));
        }
    }
    if let Some(host_label) = &args.host_label {
        if host_label.is_empty()
            || host_label.trim() != host_label
            || host_label.len() > 128
            || !host_label
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || "._-".contains(value))
        {
            return Err("--host-label is not a canonical portable label".to_string());
        }
    }
    if args.output_json.exists() {
        return Err(format!(
            "--output-json already exists: {}",
            args.output_json.display()
        ));
    }
    Ok(())
}

fn bearer_token(args: &Args) -> Result<(String, &'static str), String> {
    if let Ok(value) = std::env::var(&args.token_env) {
        let value = value.trim().to_string();
        if !value.is_empty() {
            return Ok((value, "environment"));
        }
    }
    if let Some(path) = &args.token_file {
        let value = fs::read_to_string(path).map_err(|error| {
            format!(
                "cannot read token file {}: {error}",
                path.as_path().display()
            )
        })?;
        let value = value.trim().to_string();
        if !value.is_empty() {
            return Ok((value, "file"));
        }
    }
    Err(format!(
        "bearer token unavailable through {} or --token-file",
        args.token_env
    ))
}

fn authenticated<T>(body: T, token: &str) -> Result<Request<T>, String> {
    let mut request = Request::new(body);
    let value: MetadataValue<Ascii> = format!("Bearer {token}")
        .parse()
        .map_err(|error| format!("invalid bearer metadata: {error}"))?;
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

fn context(args: &Args, idempotency_key: Option<String>) -> MemoryRequestContext {
    MemoryRequestContext {
        workspace_id: args.workspace.clone(),
        namespace: args.namespace.clone(),
        request_purpose: args.purpose.clone(),
        delegated_agent_id: None,
        idempotency_key,
        request_id: None,
        scope_narrowing: None,
    }
}

fn entity_key(args: &Args, index: usize) -> String {
    format!("benchmark:{}:{index:010}", args.run_id)
}

fn unique_term(index: usize) -> String {
    format!("benchterm{index:010}")
}

fn remember_request(args: &Args, index: usize) -> MemoryRememberRequest {
    let text = format!(
        "Deterministic authoritative memory {} in bucket{:04}.",
        unique_term(index),
        index % 1000
    );
    let content_sha256 = format!("{:x}", Sha256::digest(text.as_bytes()));
    MemoryRememberRequest {
        context: Some(context(
            args,
            Some(format!("memory-bench:{}:{index:010}", args.run_id)),
        )),
        scope: Some(MemoryScopeInput {
            entity_key: entity_key(args, index),
            data_subject_id: None,
            owner_agent_id: None,
            session_id: None,
            task_id: None,
            sensitivity: MemorySensitivity::Internal as i32,
            allowed_purposes: vec![args.purpose.clone()],
        }),
        predicate: "has benchmark fact".to_string(),
        content: Some(MemoryContent {
            value: Some(memory_content::Value::TextFact(MemoryTextFact {
                text,
                language: Some("en".to_string()),
            })),
        }),
        valid_from_ms: None,
        valid_to_ms: None,
        epistemic_formation: MemoryEpistemicFormation::MemoryFormationDirectObservation as i32,
        confidence: None,
        evidence: vec![MemoryEvidenceInput {
            source_plane: "memory-benchmark".to_string(),
            source_id: format!("{}:{index:010}", args.run_id),
            source_version: Some(REPORT_SCHEMA_VERSION.to_string()),
            observed_at_ms: None,
            content_sha256,
            source_principal_id: None,
            observed_at_unix_nanos: None,
        }],
        expected_head_version_ids: Vec::new(),
        reason: "deterministic systems benchmark".to_string(),
        valid_from_unix_nanos: None,
        valid_to_unix_nanos: None,
        compiler_artifact_id: None,
        derivation: None,
    }
}

fn recall_request(args: &Args, index: usize) -> MemoryRecallRequest {
    MemoryRecallRequest {
        context: Some(context(args, None)),
        query_text: Some(unique_term(index)),
        structured_predicates: Vec::new(),
        entity_keys: vec![entity_key(args, index)],
        max_items: args.top_k,
        max_context_tokens: Some(args.context_tokens),
        deterministic: true,
        include_explanation_summary: false,
        canonical_at_sequence: None,
        temporal_query: None,
        include_conflicts: false,
        recipe: Some("preview-bounded-bm25-v1".to_string()),
    }
}

async fn commit_one(
    mut client: MemoryServiceClient<tonic::transport::Channel>,
    args: Arc<Args>,
    token: Arc<String>,
    index: usize,
) -> Result<CommitSample, String> {
    let started = Instant::now();
    let receipt = client
        .remember(authenticated(remember_request(&args, index), &token)?)
        .await
        .map_err(|error| format!("remember index {index}: {error}"))?
        .into_inner();
    let latency = started.elapsed();
    if receipt.version_ids.len() != 1 || receipt.commit_sequence == 0 {
        return Err(format!(
            "remember index {index}: malformed mutation receipt"
        ));
    }
    if receipt.durability != "SYNCED" || receipt.projection_status != "VISIBLE" {
        return Err(format!(
            "remember index {index}: durability={} projection_status={}",
            receipt.durability, receipt.projection_status
        ));
    }
    let visibility = receipt
        .visibility
        .ok_or_else(|| format!("remember index {index}: missing visibility receipt"))?;
    if visibility.commit_sequence != receipt.commit_sequence
        || visibility.visible_sequence < receipt.commit_sequence
    {
        return Err(format!(
            "remember index {index}: inconsistent visibility receipt"
        ));
    }
    Ok(CommitSample {
        index,
        version_id: receipt.version_ids[0].clone(),
        commit_sequence: receipt.commit_sequence,
        visibility_lag: receipt
            .commit_sequence
            .saturating_sub(visibility.visible_sequence),
        latency,
    })
}

async fn run_commits(
    client: MemoryServiceClient<tonic::transport::Channel>,
    args: Arc<Args>,
    token: Arc<String>,
) -> (CommitReport, Vec<Option<String>>) {
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut next_index = 0usize;
    let mut samples = Vec::with_capacity(args.versions);
    let mut errors = Vec::new();
    while next_index < args.versions || !tasks.is_empty() {
        while next_index < args.versions && tasks.len() < args.commit_concurrency {
            tasks.spawn(commit_one(
                client.clone(),
                args.clone(),
                token.clone(),
                next_index,
            ));
            next_index += 1;
        }
        if let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(sample)) => samples.push(sample),
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(format!("commit task failed: {error}")),
            }
        }
    }
    let wall = started.elapsed();
    let mut version_ids = vec![None; args.versions];
    let mut sequences = Vec::with_capacity(samples.len());
    let mut visibility_lag = 0;
    let mut latencies = Vec::with_capacity(samples.len());
    for sample in samples {
        version_ids[sample.index] = Some(sample.version_id);
        sequences.push(sample.commit_sequence);
        visibility_lag = visibility_lag.max(sample.visibility_lag);
        latencies.push(sample.latency);
    }
    sequences.sort_unstable();
    let succeeded = latencies.len();
    (
        CommitReport {
            requested: args.versions,
            succeeded,
            failed: errors.len(),
            wall_time_ms: wall.as_millis(),
            throughput_per_second: rate(succeeded, wall),
            first_commit_sequence: sequences.first().copied(),
            last_commit_sequence: sequences.last().copied(),
            maximum_visibility_lag_sequences: visibility_lag,
            acknowledgement_through_visible_latency: latency_stats(latencies),
            errors: bounded_errors(errors),
        },
        version_ids,
    )
}

async fn query_one(
    mut client: MemoryServiceClient<tonic::transport::Channel>,
    args: Arc<Args>,
    token: Arc<String>,
    index: usize,
    expected_version_id: String,
) -> Result<QuerySample, String> {
    let started = Instant::now();
    let response = client
        .recall(authenticated(recall_request(&args, index), &token)?)
        .await
        .map_err(|error| format!("recall index {index}: {error}"))?
        .into_inner();
    let latency = started.elapsed();
    if response.snapshot_id.is_empty() || response.visibility.is_none() {
        return Err(format!(
            "recall index {index}: missing snapshot or visibility"
        ));
    }
    Ok(QuerySample {
        correct: response
            .items
            .iter()
            .any(|item| item.version_id == expected_version_id),
        latency,
    })
}

async fn run_queries(
    client: MemoryServiceClient<tonic::transport::Channel>,
    args: Arc<Args>,
    token: Arc<String>,
    version_ids: &[Option<String>],
) -> RecallReport {
    let mut warmup_errors = Vec::new();
    for query in 0..args.warmup_queries {
        let index = deterministic_index(query, args.versions);
        let Some(expected) = version_ids[index].clone() else {
            warmup_errors.push(format!("warmup index {index}: commit was unavailable"));
            continue;
        };
        if let Err(error) =
            query_one(client.clone(), args.clone(), token.clone(), index, expected).await
        {
            warmup_errors.push(error);
        }
    }

    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut next_query = 0usize;
    let mut samples = Vec::with_capacity(args.queries);
    let mut errors = warmup_errors;
    while next_query < args.queries || !tasks.is_empty() {
        while next_query < args.queries && tasks.len() < args.query_concurrency {
            let index = deterministic_index(next_query + args.warmup_queries, args.versions);
            if let Some(expected) = version_ids[index].clone() {
                tasks.spawn(query_one(
                    client.clone(),
                    args.clone(),
                    token.clone(),
                    index,
                    expected,
                ));
            } else {
                errors.push(format!("query index {index}: commit was unavailable"));
            }
            next_query += 1;
        }
        if let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(sample)) => samples.push(sample),
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(format!("query task failed: {error}")),
            }
        }
    }
    let wall = started.elapsed();
    let incorrect = samples.iter().filter(|sample| !sample.correct).count();
    let succeeded = samples.len().saturating_sub(incorrect);
    let latencies = samples.into_iter().map(|sample| sample.latency).collect();
    RecallReport {
        requested: args.queries,
        succeeded,
        incorrect,
        failed: errors.len(),
        wall_time_ms: wall.as_millis(),
        throughput_per_second: rate(succeeded, wall),
        latency: latency_stats(latencies),
        errors: bounded_errors(errors),
    }
}

fn deterministic_index(query: usize, versions: usize) -> usize {
    query.wrapping_mul(1_103_515_245).wrapping_add(12_345) % versions
}

fn rate(count: usize, duration: Duration) -> f64 {
    if duration.is_zero() {
        count as f64
    } else {
        count as f64 / duration.as_secs_f64()
    }
}

fn latency_stats(values: Vec<Duration>) -> LatencyStats {
    let mut samples_us = values
        .into_iter()
        .map(|value| value.as_micros())
        .collect::<Vec<_>>();
    samples_us.sort_unstable();
    let count = samples_us.len();
    LatencyStats {
        count,
        min_us: samples_us.first().copied().unwrap_or(0),
        mean_us: if count == 0 {
            0
        } else {
            samples_us.iter().sum::<u128>() / count as u128
        },
        p50_us: nearest_rank(&samples_us, 50, 100),
        p95_us: nearest_rank(&samples_us, 95, 100),
        p99_us: nearest_rank(&samples_us, 99, 100),
        max_us: samples_us.last().copied().unwrap_or(0),
        samples_us,
    }
}

fn nearest_rank(values: &[u128], numerator: usize, denominator: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let rank = values
        .len()
        .saturating_mul(numerator)
        .saturating_add(denominator - 1)
        / denominator;
    values[rank.max(1).min(values.len()) - 1]
}

fn bounded_errors(mut errors: Vec<String>) -> Vec<String> {
    errors.truncate(100);
    errors
}

fn command_output(command: &str, arguments: &[&str]) -> String {
    Command::new(command)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn hardware() -> Hardware {
    let os = fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("PRETTY_NAME=")
                    .map(|value| value.trim_matches('"').to_string())
            })
        })
        .unwrap_or_else(|| command_output("sw_vers", &["-productVersion"]));
    let cpu = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                (name.trim() == "model name").then(|| value.trim().to_string())
            })
        })
        .unwrap_or_else(|| command_output("sysctl", &["-n", "machdep.cpu.brand_string"]));
    let memory_bytes = fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let value = line.strip_prefix("MemTotal:")?;
                value
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()?
                    .checked_mul(1024)
            })
        })
        .or_else(|| command_output("sysctl", &["-n", "hw.memsize"]).parse().ok());
    Hardware {
        hostname: command_output("hostname", &[]),
        machine_id_sha256: fs::read_to_string("/etc/machine-id")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(|value| format!("{:x}", Sha256::digest(value.as_bytes()))),
        os,
        kernel: command_output("uname", &["-sr"]),
        architecture: std::env::consts::ARCH.to_string(),
        cpu,
        logical_cores: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        memory_bytes,
    }
}

fn software() -> Software {
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string());
    let dirty_worktree = match &status {
        Some(value) => !value.is_empty(),
        None => true,
    };
    Software {
        akidb_version: env!("CARGO_PKG_VERSION"),
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        rustc: command_output("rustc", &["--version"]),
        git_status_available: status.is_some(),
        dirty_worktree,
    }
}

fn directory_bytes(path: Option<&Path>) -> Option<u64> {
    fn walk(path: &Path) -> std::io::Result<u64> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            return Ok(0);
        }
        if metadata.is_file() {
            return Ok(metadata.len());
        }
        let mut total = 0u64;
        if metadata.is_dir() {
            for entry in fs::read_dir(path)? {
                total = total.saturating_add(walk(&entry?.path())?);
            }
        }
        Ok(total)
    }
    path.and_then(|value| walk(value).ok())
}

fn rss_bytes(pid: Option<u32>) -> Option<u64> {
    let pid = pid?;
    let status_path = PathBuf::from(format!("/proc/{pid}/status"));
    if let Ok(status) = fs::read_to_string(status_path) {
        if let Some(value) = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        }) {
            return value.checked_mul(1024);
        }
    }
    command_output("ps", &["-o", "rss=", "-p", &pid.to_string()])
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

fn start_rss_sampler(pid: Option<u32>) -> Option<RssSampler> {
    let pid = pid?;
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(0));
    let count = Arc::new(AtomicU64::new(0));
    let task_stop = stop.clone();
    let task_peak = peak.clone();
    let task_count = count.clone();
    let handle = tokio::spawn(async move {
        loop {
            if let Some(value) = rss_bytes(Some(pid)) {
                task_peak.fetch_max(value, Ordering::Relaxed);
                task_count.fetch_add(1, Ordering::Relaxed);
            }
            if task_stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(RSS_SAMPLE_INTERVAL_MS)).await;
        }
    });
    Some(RssSampler {
        stop,
        peak,
        count,
        handle,
    })
}

fn dataset_sha256(args: &Args) -> String {
    format!(
        "{:x}",
        Sha256::digest(
            format!(
                "akidb-memory-bench-v1\0{}\0{}\0{}\0{}\0{}",
                args.workspace, args.namespace, args.purpose, args.run_id, args.versions
            )
            .as_bytes()
        )
    )
}

fn write_report(path: &Path, report: &BenchmarkReport) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("cannot create report {}: {error}", path.display()))?;
    serde_json::to_writer_pretty(&mut file, report).map_err(|error| error.to_string())?;
    file.write_all(b"\n").map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    let (token, token_source) = bearer_token(&args)?;
    let endpoint = if args.server.starts_with("http://") || args.server.starts_with("https://") {
        args.server.clone()
    } else {
        format!("http://{}", args.server)
    };
    let timeout = Duration::from_secs(args.timeout_seconds);
    let channel = Endpoint::from_shared(endpoint)?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await?;
    let mut client = MemoryServiceClient::new(channel);
    let capabilities = client
        .get_memory_capabilities(authenticated(GetMemoryCapabilitiesRequest {}, &token)?)
        .await?
        .into_inner()
        .capabilities
        .ok_or("server omitted Memory capabilities")?;
    let required_rpcs = ["Remember", "Recall"];
    for rpc in required_rpcs {
        if !capabilities.supported_rpcs.iter().any(|value| value == rpc) {
            return Err(format!("server does not advertise required RPC {rpc}").into());
        }
    }
    if !capabilities
        .durability_modes
        .iter()
        .any(|mode| mode == "SYNCED")
    {
        return Err("server does not advertise SYNCED Memory durability".into());
    }

    let disk_before = directory_bytes(args.data_dir.as_deref());
    let rss_before = rss_bytes(args.server_pid);
    let rss_sampler = start_rss_sampler(args.server_pid);
    let args = Arc::new(args);
    let token = Arc::new(token);
    let (commit, version_ids) = run_commits(client.clone(), args.clone(), token.clone()).await;
    let disk_after_commits = directory_bytes(args.data_dir.as_deref());
    let rss_after_commits = rss_bytes(args.server_pid);
    let recall = if commit.failed == 0 && commit.succeeded == args.versions {
        run_queries(client.clone(), args.clone(), token.clone(), &version_ids).await
    } else {
        RecallReport {
            requested: args.queries,
            succeeded: 0,
            incorrect: 0,
            failed: args.queries,
            wall_time_ms: 0,
            throughput_per_second: 0.0,
            latency: latency_stats(Vec::new()),
            errors: vec!["recall skipped because commit qualification failed".to_string()],
        }
    };
    let disk_after_queries = directory_bytes(args.data_dir.as_deref());
    let rss_after_queries = rss_bytes(args.server_pid);
    let (sampled_peak_rss, rss_sample_count) = match rss_sampler {
        Some(sampler) => {
            sampler.stop.store(true, Ordering::Relaxed);
            let _ = sampler.handle.await;
            (
                Some(sampler.peak.load(Ordering::Relaxed)),
                sampler.count.load(Ordering::Relaxed),
            )
        }
        None => (None, 0),
    };
    let peak_rss = [
        rss_before,
        rss_after_commits,
        rss_after_queries,
        sampled_peak_rss,
    ]
    .into_iter()
    .flatten()
    .max();
    let mut failures = Vec::new();
    if commit.succeeded != args.versions || commit.failed != 0 {
        failures.push(format!(
            "commit qualification failed: {}/{} succeeded, {} failed",
            commit.succeeded, args.versions, commit.failed
        ));
    }
    if commit.maximum_visibility_lag_sequences != 0 {
        failures.push(format!(
            "visible receipt lagged by {} sequences",
            commit.maximum_visibility_lag_sequences
        ));
    }
    if recall.succeeded != args.queries || recall.incorrect != 0 || recall.failed != 0 {
        failures.push(format!(
            "recall qualification failed: {}/{} correct, {} incorrect, {} failed",
            recall.succeeded, args.queries, recall.incorrect, recall.failed
        ));
    }
    let software = software();
    let report = BenchmarkReport {
        schema_version: REPORT_SCHEMA_VERSION,
        report_type: "akidb_authoritative_memory_systems_benchmark",
        generated_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        dataset_sha256: dataset_sha256(&args),
        hardware: hardware(),
        software,
        configuration: RunConfiguration {
            server: args.server.clone(),
            workspace: args.workspace.clone(),
            namespace: args.namespace.clone(),
            purpose: args.purpose.clone(),
            run_id: args.run_id.clone(),
            host_label: args.host_label.clone(),
            versions: args.versions,
            commit_concurrency: args.commit_concurrency,
            queries: args.queries,
            warmup_queries: args.warmup_queries,
            query_concurrency: args.query_concurrency,
            top_k: args.top_k,
            context_tokens: args.context_tokens,
            timeout_seconds: args.timeout_seconds,
            token_source,
            data_dir: args
                .data_dir
                .as_ref()
                .map(|path| path.display().to_string()),
            server_pid: args.server_pid,
        },
        capabilities: CapabilitySnapshot {
            profile_status: capabilities.profile_status,
            supported_rpcs: capabilities.supported_rpcs,
            durability_modes: capabilities.durability_modes,
            active_projection_recipes: capabilities.active_projection_recipes,
            workspace_topology: capabilities.workspace_topology,
            active_projection_manifest_sha256: capabilities.active_projection_manifest_sha256,
            tokenizer_artifact_id: capabilities.tokenizer_artifact_id,
            server_build_id: capabilities.server_build_id,
            retention_policy: capabilities
                .retention_policy
                .map(|policy| RetentionSnapshot {
                    raw_event_seconds: policy.raw_event_seconds,
                    memory_version_seconds: policy.memory_version_seconds,
                    compiler_artifact_seconds: policy.compiler_artifact_seconds,
                    index_artifact_seconds: policy.index_artifact_seconds,
                    audit_seconds: policy.audit_seconds,
                    snapshot_seconds: policy.snapshot_seconds,
                    zero_means_indefinite: policy.zero_means_indefinite,
                    finite_windows_enforced: policy.finite_windows_enforced,
                }),
        },
        commit,
        recall,
        resources: ResourceReport {
            disk_bytes_before: disk_before,
            disk_bytes_after_commits: disk_after_commits,
            disk_bytes_after_queries: disk_after_queries,
            disk_bytes_delta: disk_before
                .zip(disk_after_queries)
                .map(|(before, after)| after.saturating_sub(before)),
            server_rss_bytes_before: rss_before,
            server_rss_bytes_after_commits: rss_after_commits,
            server_rss_bytes_after_queries: rss_after_queries,
            peak_observed_server_rss_bytes: peak_rss,
            server_rss_sample_interval_ms: args.server_pid.map(|_| RSS_SAMPLE_INTERVAL_MS),
            server_rss_sample_count: rss_sample_count,
        },
        verdict: Verdict {
            status: if failures.is_empty() { "PASS" } else { "FAIL" },
            failures,
        },
    };
    write_report(&args.output_json, &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.verdict.status != "PASS" {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_and_dataset_are_deterministic() {
        let values = vec![
            Duration::from_micros(4),
            Duration::from_micros(1),
            Duration::from_micros(3),
            Duration::from_micros(2),
        ];
        let stats = latency_stats(values);
        assert_eq!(stats.p50_us, 2);
        assert_eq!(stats.p95_us, 4);
        assert_eq!(stats.p99_us, 4);
        assert_eq!(deterministic_index(7, 100), deterministic_index(7, 100));
    }

    #[test]
    fn generated_ids_are_bounded_and_stable() {
        let args = Args {
            server: "http://127.0.0.1:50051".to_string(),
            workspace: "memory-benchmark".to_string(),
            namespace: "benchmark/run".to_string(),
            purpose: "memory-benchmark".to_string(),
            run_id: "run-1".to_string(),
            host_label: Some("akidb-amd64-1".to_string()),
            token_env: "AKIDB_MEMORY_PRINCIPAL_TOKEN".to_string(),
            token_file: None,
            versions: 1000,
            commit_concurrency: 8,
            queries: 100,
            warmup_queries: 10,
            query_concurrency: 8,
            top_k: 10,
            context_tokens: 256,
            timeout_seconds: 60,
            data_dir: None,
            server_pid: None,
            output_json: PathBuf::from("report.json"),
        };
        assert_eq!(entity_key(&args, 42), "benchmark:run-1:0000000042");
        assert_eq!(unique_term(42), "benchterm0000000042");
        assert_eq!(
            dataset_sha256(&args),
            "8acc90d67319d0d798b981905a96eaa5b5e986378d288d59a31fbcf75f47e2ea"
        );
    }
}
