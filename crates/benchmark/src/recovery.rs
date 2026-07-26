//! Crash-recovery qualification probe for the mutable AkiDB data plane.
//!
//! The mutator fsyncs a small append-only acknowledgement journal before it
//! advances an ID to its next state.  A separate verifier can therefore
//! distinguish an acknowledged durability regression from the one operation
//! that may have committed while its response was in flight at crash time.

use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::{
    DeleteRequest, DeleteStatus, GetRequest, HealthRequest, InsertRequest, UpdateRequest,
    UpdateStatus,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

const JOURNAL_SCHEMA_VERSION: u32 = 1;
const REPORT_SCHEMA_VERSION: u32 = 1;
const MAX_DIMENSIONS: usize = 16_384;
const MAX_CYCLES: usize = 1_000_000;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// AkiDB gRPC origin.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    server: String,

    /// AkiDB collection.
    #[arg(long, default_value = "default")]
    collection: String,

    /// Authorized workspace metadata.
    #[arg(long, default_value = "default")]
    workspace: String,

    /// Optional bearer credential environment variable.
    #[arg(long, default_value = "AKIDB_AUTH_TOKEN")]
    token_env: String,

    /// Optional PEM CA for an HTTPS gRPC origin.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Optional TLS server identity override.
    #[arg(long)]
    tls_domain: Option<String>,

    /// Connect and per-request timeout.
    #[arg(long, default_value = "30")]
    timeout_seconds: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Generate deterministic insert/update/delete traffic and fsync every ack.
    Mutate {
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        id_prefix: String,
        #[arg(long, default_value = "128")]
        dimensions: usize,
        #[arg(long, default_value = "8")]
        workers: usize,
        #[arg(long, default_value = "100")]
        cycle_qps: f64,
        #[arg(long, default_value = "60")]
        duration_seconds: u64,
        #[arg(long, default_value = "100000")]
        max_cycles: usize,
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        output_json: PathBuf,
    },

    /// Exit successfully once a live journal contains the requested ack floor.
    Inspect {
        #[arg(long)]
        journal: PathBuf,
        #[arg(long, default_value = "1")]
        min_insert_acks: usize,
        #[arg(long, default_value = "1")]
        min_update_acks: usize,
        #[arg(long, default_value = "1")]
        min_delete_acks: usize,
    },

    /// Verify acknowledged state after restart, then optionally remove probe IDs.
    Verify {
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        expected_baseline_active_vectors: u64,
        #[arg(long, default_value = "1")]
        min_insert_acks: usize,
        #[arg(long, default_value = "1")]
        min_update_acks: usize,
        #[arg(long, default_value = "1")]
        min_delete_acks: usize,
        #[arg(long, default_value_t = false)]
        cleanup: bool,
        #[arg(long, default_value = "60")]
        cleanup_timeout_seconds: u64,
        #[arg(long)]
        output_json: PathBuf,
    },
}

#[derive(Debug, Clone)]
struct ClientContext {
    collection: String,
    workspace: String,
    token: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TargetState {
    Inserted,
    Updated,
    Deleted,
}

impl TargetState {
    fn for_cycle(cycle: usize) -> Self {
        match cycle % 3 {
            0 => Self::Inserted,
            1 => Self::Updated,
            _ => Self::Deleted,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
enum JournalRecord {
    Header {
        schema_version: u32,
        sequence: u64,
        run_id: String,
        id_prefix: String,
        dimensions: usize,
        baseline_active_vectors: u64,
        started_at_unix_ms: u64,
    },
    Allocate {
        sequence: u64,
        cycle: usize,
        target: TargetState,
    },
    Ack {
        sequence: u64,
        cycle: usize,
        operation: Operation,
        revision: u8,
        acknowledged_at_unix_ms: u64,
    },
}

impl JournalRecord {
    fn sequence(&self) -> u64 {
        match self {
            Self::Header { sequence, .. }
            | Self::Allocate { sequence, .. }
            | Self::Ack { sequence, .. } => *sequence,
        }
    }
}

#[derive(Debug)]
struct JournalWriter {
    file: File,
    next_sequence: u64,
}

impl JournalWriter {
    fn create(
        path: &Path,
        run_id: &str,
        id_prefix: &str,
        dimensions: usize,
        baseline_active_vectors: u64,
    ) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)?;
        let mut writer = Self {
            file,
            next_sequence: 0,
        };
        writer.append(JournalRecord::Header {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence: 0,
            run_id: run_id.to_string(),
            id_prefix: id_prefix.to_string(),
            dimensions,
            baseline_active_vectors,
            started_at_unix_ms: generated_at_unix_ms(),
        })?;
        Ok(writer)
    }

    fn append(&mut self, record: JournalRecord) -> io::Result<()> {
        if record.sequence() != self.next_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal sequence is not contiguous",
            ));
        }
        let mut encoded = serde_json::to_vec(&record)?;
        encoded.push(b'\n');
        self.file.write_all(&encoded)?;
        self.file.sync_data()?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("journal sequence exhausted"))?;
        Ok(())
    }

    fn allocate(&mut self, cycle: usize, target: TargetState) -> io::Result<()> {
        self.append(JournalRecord::Allocate {
            sequence: self.next_sequence,
            cycle,
            target,
        })
    }

    fn ack(&mut self, cycle: usize, operation: Operation, revision: u8) -> io::Result<()> {
        self.append(JournalRecord::Ack {
            sequence: self.next_sequence,
            cycle,
            operation,
            revision,
            acknowledged_at_unix_ms: generated_at_unix_ms(),
        })
    }
}

#[derive(Debug, Clone)]
struct JournalHeader {
    run_id: String,
    id_prefix: String,
    dimensions: usize,
    baseline_active_vectors: u64,
}

#[derive(Debug, Clone)]
struct CycleState {
    target: TargetState,
    last_ack: Option<Operation>,
}

#[derive(Debug, Clone)]
struct JournalSnapshot {
    header: JournalHeader,
    cycles: BTreeMap<usize, CycleState>,
    insert_acks: usize,
    update_acks: usize,
    delete_acks: usize,
    complete_records: usize,
    ignored_partial_tail: bool,
}

#[derive(Debug, Default)]
struct WorkerSummary {
    allocations: usize,
    insert_acks: usize,
    update_acks: usize,
    delete_acks: usize,
    completed_targets: usize,
    rpc_failures: usize,
    journal_failures: usize,
}

impl WorkerSummary {
    fn merge(&mut self, other: Self) {
        self.allocations += other.allocations;
        self.insert_acks += other.insert_acks;
        self.update_acks += other.update_acks;
        self.delete_acks += other.delete_acks;
        self.completed_targets += other.completed_targets;
        self.rpc_failures += other.rpc_failures;
        self.journal_failures += other.journal_failures;
    }
}

#[derive(Debug, Serialize)]
struct HealthReport {
    healthy: bool,
    ready: bool,
    total_vectors: u64,
    active_vectors: u64,
}

#[derive(Debug, Serialize)]
struct MutateReport {
    schema_version: u32,
    report_type: &'static str,
    generated_at_unix_ms: u64,
    run_id: String,
    server: String,
    collection: String,
    dimensions: usize,
    workers: usize,
    requested_cycle_qps: f64,
    duration_seconds: u64,
    max_cycles: usize,
    elapsed_ms: u128,
    baseline: HealthReport,
    allocations: usize,
    insert_acks: usize,
    update_acks: usize,
    delete_acks: usize,
    completed_targets: usize,
    rpc_failures: usize,
    journal_failures: usize,
    termination_reason: &'static str,
}

#[derive(Debug, Serialize)]
struct VerificationReport {
    schema_version: u32,
    report_type: &'static str,
    generated_at_unix_ms: u64,
    run_id: String,
    server: String,
    collection: String,
    journal_sha256: String,
    complete_journal_records: usize,
    ignored_partial_tail: bool,
    allocated_cycles: usize,
    insert_acks: usize,
    update_acks: usize,
    delete_acks: usize,
    acknowledged_states_verified: usize,
    accepted_unacknowledged_advances: usize,
    cleanup_requested: bool,
    cleanup_deleted: usize,
    health_before_cleanup: HealthReport,
    health_after_cleanup: Option<HealthReport>,
    verdict: Verdict,
}

#[derive(Debug, Serialize)]
struct Verdict {
    status: &'static str,
    failures: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ProbeMetadata {
    recovery_probe: bool,
    run_id: String,
    cycle: usize,
    revision: u8,
}

fn generated_at_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn validate_identifier(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || value.trim() != value
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(format!(
            "{label} must be 1-128 characters from A-Z, a-z, 0-9, dot, underscore, or dash"
        ));
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

fn is_environment_name(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(|first| first.is_ascii_uppercase())
        && chars.all(|value| value.is_ascii_uppercase() || value.is_ascii_digit() || value == '_')
}

fn validate_args(args: &Args) -> Result<(), String> {
    if !is_canonical_origin(&args.server) {
        return Err("--server must be a canonical http(s) origin".to_string());
    }
    validate_identifier("collection", &args.collection)?;
    validate_identifier("workspace", &args.workspace)?;
    if !is_environment_name(&args.token_env) {
        return Err("--token-env must be a canonical environment name".to_string());
    }
    if args.timeout_seconds == 0 || args.timeout_seconds > 300 {
        return Err("--timeout-seconds must be between 1 and 300".to_string());
    }
    if args.server.starts_with("http://") && (args.tls_ca.is_some() || args.tls_domain.is_some()) {
        return Err("TLS options require an https:// server".to_string());
    }

    match &args.command {
        Command::Mutate {
            run_id,
            id_prefix,
            dimensions,
            workers,
            cycle_qps,
            duration_seconds,
            max_cycles,
            journal,
            output_json,
        } => {
            validate_identifier("run-id", run_id)?;
            validate_identifier("id-prefix", id_prefix)?;
            if *dimensions == 0 || *dimensions > MAX_DIMENSIONS {
                return Err(format!(
                    "--dimensions must be between 1 and {MAX_DIMENSIONS}"
                ));
            }
            if *workers == 0 || *workers > 256 {
                return Err("--workers must be between 1 and 256".to_string());
            }
            if !cycle_qps.is_finite() || *cycle_qps <= 0.0 || *cycle_qps > 100_000.0 {
                return Err("--cycle-qps must be finite and in (0, 100000]".to_string());
            }
            if *duration_seconds == 0 || *duration_seconds > 3_600 {
                return Err("--duration-seconds must be between 1 and 3600".to_string());
            }
            if *max_cycles == 0 || *max_cycles > MAX_CYCLES {
                return Err(format!("--max-cycles must be between 1 and {MAX_CYCLES}"));
            }
            if journal == output_json {
                return Err("--journal and --output-json must differ".to_string());
            }
        }
        Command::Inspect { .. } => {}
        Command::Verify {
            cleanup_timeout_seconds,
            journal,
            output_json,
            ..
        } => {
            if *cleanup_timeout_seconds == 0 || *cleanup_timeout_seconds > 900 {
                return Err("--cleanup-timeout-seconds must be between 1 and 900".to_string());
            }
            if journal == output_json {
                return Err("--journal and --output-json must differ".to_string());
            }
        }
    }
    Ok(())
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
    }
    Ok(AkidbClient::new(endpoint.connect().await?))
}

fn request<T>(
    message: T,
    context: &ClientContext,
) -> Result<Request<T>, tonic::metadata::errors::InvalidMetadataValue> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-akidb-workspace",
        MetadataValue::try_from(context.workspace.as_str())?,
    );
    request.metadata_mut().insert(
        "x-akidb-agent",
        MetadataValue::from_static("recovery-benchmark"),
    );
    if let Some(token) = &context.token {
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}"))?,
        );
    }
    Ok(request)
}

async fn health(
    client: &mut AkidbClient<Channel>,
    context: &ClientContext,
) -> Result<HealthReport, Box<dyn std::error::Error>> {
    let value = client
        .health(request(HealthRequest {}, context)?)
        .await?
        .into_inner();
    Ok(HealthReport {
        healthy: value.healthy,
        ready: value.ready,
        total_vectors: value.total_vectors,
        active_vectors: value.active_vectors,
    })
}

fn probe_id(prefix: &str, cycle: usize) -> String {
    format!("{prefix}-{cycle:012}")
}

fn probe_vector(dimensions: usize, cycle: usize, revision: u8) -> Vec<f32> {
    (0..dimensions)
        .map(|index| {
            let value = cycle
                .wrapping_mul(31)
                .wrapping_add(index.wrapping_mul(17))
                .wrapping_add(usize::from(revision) * 13)
                % 10_000;
            value as f32 / 10_000.0
        })
        .collect()
}

fn probe_metadata(run_id: &str, cycle: usize, revision: u8) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "recovery_probe": true,
        "run_id": run_id,
        "cycle": cycle,
        "revision": revision,
    }))
    .expect("probe metadata is serializable")
}

fn append_allocate(
    journal: &Arc<Mutex<JournalWriter>>,
    cycle: usize,
    target: TargetState,
) -> io::Result<()> {
    journal
        .lock()
        .map_err(|_| io::Error::other("journal lock poisoned"))?
        .allocate(cycle, target)
}

fn append_ack(
    journal: &Arc<Mutex<JournalWriter>>,
    cycle: usize,
    operation: Operation,
    revision: u8,
) -> io::Result<()> {
    journal
        .lock()
        .map_err(|_| io::Error::other("journal lock poisoned"))?
        .ack(cycle, operation, revision)
}

struct MutateOptions<'a> {
    run_id: &'a str,
    id_prefix: &'a str,
    dimensions: usize,
    workers: usize,
    cycle_qps: f64,
    duration_seconds: u64,
    max_cycles: usize,
    journal_path: &'a Path,
    output_json: &'a Path,
}

async fn run_mutate(
    args: &Args,
    options: MutateOptions<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let MutateOptions {
        run_id,
        id_prefix,
        dimensions,
        workers,
        cycle_qps,
        duration_seconds,
        max_cycles,
        journal_path,
        output_json,
    } = options;
    let context = client_context(args)?;
    let mut client = connect(args).await?;
    let baseline = health(&mut client, &context).await?;
    if !baseline.healthy || !baseline.ready {
        return Err("AkiDB must be healthy and ready before the mutation probe".into());
    }

    let journal = Arc::new(Mutex::new(JournalWriter::create(
        journal_path,
        run_id,
        id_prefix,
        dimensions,
        baseline.active_vectors,
    )?));
    let next_cycle = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let schedule_started = tokio::time::Instant::now();
    let deadline = schedule_started + Duration::from_secs(duration_seconds);
    let mut tasks = Vec::with_capacity(workers);

    for _ in 0..workers {
        let mut worker_client = client.clone();
        let worker_context = context.clone();
        let worker_journal = Arc::clone(&journal);
        let worker_next = Arc::clone(&next_cycle);
        let worker_stop = Arc::clone(&stop);
        let worker_run_id = run_id.to_string();
        let worker_id_prefix = id_prefix.to_string();
        tasks.push(tokio::spawn(async move {
            let mut summary = WorkerSummary::default();
            while !worker_stop.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline {
                let cycle = worker_next.fetch_add(1, Ordering::Relaxed);
                if cycle >= max_cycles {
                    break;
                }
                let scheduled =
                    schedule_started + Duration::from_secs_f64(cycle as f64 / cycle_qps);
                if scheduled >= deadline {
                    break;
                }
                tokio::time::sleep_until(scheduled).await;
                if worker_stop.load(Ordering::Acquire) {
                    break;
                }

                let target = TargetState::for_cycle(cycle);
                if append_allocate(&worker_journal, cycle, target).is_err() {
                    summary.journal_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                summary.allocations += 1;
                let id = probe_id(&worker_id_prefix, cycle);
                let inserted = match request(
                    InsertRequest {
                        collection: worker_context.collection.clone(),
                        id: id.clone(),
                        vector: probe_vector(dimensions, cycle, 1),
                        metadata: probe_metadata(&worker_run_id, cycle, 1),
                        text: String::new(),
                    },
                    &worker_context,
                ) {
                    Ok(request) => worker_client
                        .insert(request)
                        .await
                        .map(|response| response.into_inner().success)
                        .unwrap_or(false),
                    Err(_) => false,
                };
                if !inserted {
                    summary.rpc_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                if append_ack(&worker_journal, cycle, Operation::Insert, 1).is_err() {
                    summary.journal_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                summary.insert_acks += 1;
                if target == TargetState::Inserted {
                    summary.completed_targets += 1;
                    continue;
                }

                let updated = match request(
                    UpdateRequest {
                        collection: worker_context.collection.clone(),
                        id: id.clone(),
                        vector: probe_vector(dimensions, cycle, 2),
                        metadata: probe_metadata(&worker_run_id, cycle, 2),
                    },
                    &worker_context,
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
                if !updated {
                    summary.rpc_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                if append_ack(&worker_journal, cycle, Operation::Update, 2).is_err() {
                    summary.journal_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                summary.update_acks += 1;
                if target == TargetState::Updated {
                    summary.completed_targets += 1;
                    continue;
                }

                let deleted = match request(
                    DeleteRequest {
                        collection: worker_context.collection.clone(),
                        id,
                    },
                    &worker_context,
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
                if !deleted {
                    summary.rpc_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                if append_ack(&worker_journal, cycle, Operation::Delete, 3).is_err() {
                    summary.journal_failures += 1;
                    worker_stop.store(true, Ordering::Release);
                    break;
                }
                summary.delete_acks += 1;
                summary.completed_targets += 1;
            }
            summary
        }));
    }

    let mut summary = WorkerSummary::default();
    for task in tasks {
        match task.await {
            Ok(worker) => summary.merge(worker),
            Err(_) => summary.rpc_failures += 1,
        }
    }
    let termination_reason = if summary.journal_failures > 0 {
        "journal_failure"
    } else if summary.rpc_failures > 0 {
        "rpc_interruption"
    } else if next_cycle.load(Ordering::Relaxed) >= max_cycles {
        "max_cycles"
    } else {
        "duration"
    };
    let report = MutateReport {
        schema_version: REPORT_SCHEMA_VERSION,
        report_type: "akidb.market-recovery-mutate.v1",
        generated_at_unix_ms: generated_at_unix_ms(),
        run_id: run_id.to_string(),
        server: args.server.clone(),
        collection: args.collection.clone(),
        dimensions,
        workers,
        requested_cycle_qps: cycle_qps,
        duration_seconds,
        max_cycles,
        elapsed_ms: started.elapsed().as_millis(),
        baseline,
        allocations: summary.allocations,
        insert_acks: summary.insert_acks,
        update_acks: summary.update_acks,
        delete_acks: summary.delete_acks,
        completed_targets: summary.completed_targets,
        rpc_failures: summary.rpc_failures,
        journal_failures: summary.journal_failures,
        termination_reason,
    };
    write_json_atomic(output_json, &report)?;
    println!("{}", serde_json::to_string(&report)?);
    if summary.journal_failures == 0 {
        Ok(())
    } else {
        Err("recovery mutation journal failed".into())
    }
}

fn parse_journal(path: &Path) -> Result<JournalSnapshot, String> {
    let source = File::open(path).map_err(|error| format!("open journal: {error}"))?;
    let mut reader = BufReader::new(source);
    let mut records = Vec::new();
    let mut ignored_partial_tail = false;
    loop {
        let mut line = Vec::new();
        let count = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("read journal: {error}"))?;
        if count == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            ignored_partial_tail = true;
            break;
        }
        line.pop();
        if line.is_empty() {
            return Err("journal contains an empty complete line".to_string());
        }
        let record = serde_json::from_slice::<JournalRecord>(&line)
            .map_err(|error| format!("invalid complete journal record: {error}"))?;
        records.push(record);
    }
    if records.is_empty() {
        return Err("journal contains no complete records".to_string());
    }
    for (expected, record) in records.iter().enumerate() {
        if record.sequence() != expected as u64 {
            return Err(format!(
                "journal sequence {} differs from expected {expected}",
                record.sequence()
            ));
        }
    }

    let header = match &records[0] {
        JournalRecord::Header {
            schema_version,
            run_id,
            id_prefix,
            dimensions,
            baseline_active_vectors,
            ..
        } => {
            if *schema_version != JOURNAL_SCHEMA_VERSION {
                return Err(format!("journal schema {schema_version} is not supported"));
            }
            JournalHeader {
                run_id: run_id.clone(),
                id_prefix: id_prefix.clone(),
                dimensions: *dimensions,
                baseline_active_vectors: *baseline_active_vectors,
            }
        }
        _ => return Err("first journal record must be a header".to_string()),
    };
    let mut cycles = BTreeMap::<usize, CycleState>::new();
    let mut insert_acks = 0;
    let mut update_acks = 0;
    let mut delete_acks = 0;
    for record in records.iter().skip(1) {
        match record {
            JournalRecord::Header { .. } => {
                return Err("journal contains more than one header".to_string());
            }
            JournalRecord::Allocate { cycle, target, .. } => {
                if *target != TargetState::for_cycle(*cycle) {
                    return Err(format!("cycle {cycle} has an invalid target"));
                }
                if cycles
                    .insert(
                        *cycle,
                        CycleState {
                            target: *target,
                            last_ack: None,
                        },
                    )
                    .is_some()
                {
                    return Err(format!("cycle {cycle} was allocated more than once"));
                }
            }
            JournalRecord::Ack {
                cycle,
                operation,
                revision,
                ..
            } => {
                let state = cycles
                    .get_mut(cycle)
                    .ok_or_else(|| format!("cycle {cycle} was acknowledged before allocation"))?;
                let expected = match state.last_ack {
                    None => Operation::Insert,
                    Some(Operation::Insert) if state.target != TargetState::Inserted => {
                        Operation::Update
                    }
                    Some(Operation::Update) if state.target == TargetState::Deleted => {
                        Operation::Delete
                    }
                    _ => return Err(format!("cycle {cycle} has an impossible ack transition")),
                };
                if *operation != expected {
                    return Err(format!(
                        "cycle {cycle} acknowledged {operation:?}, expected {expected:?}"
                    ));
                }
                let expected_revision = match operation {
                    Operation::Insert => 1,
                    Operation::Update => 2,
                    Operation::Delete => 3,
                };
                if *revision != expected_revision {
                    return Err(format!("cycle {cycle} has an invalid revision"));
                }
                state.last_ack = Some(*operation);
                match operation {
                    Operation::Insert => insert_acks += 1,
                    Operation::Update => update_acks += 1,
                    Operation::Delete => delete_acks += 1,
                }
            }
        }
    }
    Ok(JournalSnapshot {
        header,
        cycles,
        insert_acks,
        update_acks,
        delete_acks,
        complete_records: records.len(),
        ignored_partial_tail,
    })
}

fn inspect_journal(
    journal: &Path,
    min_insert_acks: usize,
    min_update_acks: usize,
    min_delete_acks: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = parse_journal(journal).map_err(io::Error::other)?;
    let passed = snapshot.insert_acks >= min_insert_acks
        && snapshot.update_acks >= min_update_acks
        && snapshot.delete_acks >= min_delete_acks;
    println!(
        "{}",
        serde_json::json!({
            "allocated_cycles": snapshot.cycles.len(),
            "insert_acks": snapshot.insert_acks,
            "update_acks": snapshot.update_acks,
            "delete_acks": snapshot.delete_acks,
            "ignored_partial_tail": snapshot.ignored_partial_tail,
            "ready_for_crash": passed,
        })
    );
    if passed {
        Ok(())
    } else {
        Err("journal acknowledgement floor has not been reached".into())
    }
}

fn metadata_revision(metadata: &str, run_id: &str, cycle: usize) -> Result<u8, String> {
    let value = serde_json::from_str::<ProbeMetadata>(metadata)
        .map_err(|error| format!("invalid probe metadata: {error}"))?;
    if !value.recovery_probe || value.run_id != run_id || value.cycle != cycle {
        return Err("probe metadata identity mismatch".to_string());
    }
    if !matches!(value.revision, 1 | 2) {
        return Err("probe metadata revision is invalid".to_string());
    }
    Ok(value.revision)
}

fn vector_matches(actual: &[f32], expected: &[f32]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(left, right)| (*left - *right).abs() <= 1.0e-6)
}

fn state_is_accepted(state: &CycleState, actual_revision: Option<u8>) -> (bool, bool) {
    match (state.last_ack, state.target, actual_revision) {
        (None, _, None) => (true, false),
        (None, _, Some(1)) => (true, true),
        (Some(Operation::Insert), TargetState::Inserted, Some(1)) => (true, false),
        (Some(Operation::Insert), TargetState::Updated | TargetState::Deleted, Some(1)) => {
            (true, false)
        }
        (Some(Operation::Insert), TargetState::Updated | TargetState::Deleted, Some(2)) => {
            (true, true)
        }
        (Some(Operation::Update), TargetState::Updated, Some(2)) => (true, false),
        (Some(Operation::Update), TargetState::Deleted, Some(2)) => (true, false),
        (Some(Operation::Update), TargetState::Deleted, None) => (true, true),
        (Some(Operation::Delete), TargetState::Deleted, None) => (true, false),
        _ => (false, false),
    }
}

async fn run_verify(
    args: &Args,
    journal: &Path,
    expected_baseline_active_vectors: u64,
    minimum_acks: (usize, usize, usize),
    cleanup: bool,
    cleanup_timeout_seconds: u64,
    output_json: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = parse_journal(journal).map_err(io::Error::other)?;
    let context = client_context(args)?;
    let mut client = connect(args).await?;
    let health_before_cleanup = health(&mut client, &context).await?;
    let mut failures = Vec::new();
    if !health_before_cleanup.healthy || !health_before_cleanup.ready {
        failures.push("AkiDB was not healthy and ready after recovery".to_string());
    }
    if snapshot.header.baseline_active_vectors != expected_baseline_active_vectors {
        failures.push(format!(
            "journal baseline {} differs from expected {}",
            snapshot.header.baseline_active_vectors, expected_baseline_active_vectors
        ));
    }
    if snapshot.insert_acks < minimum_acks.0
        || snapshot.update_acks < minimum_acks.1
        || snapshot.delete_acks < minimum_acks.2
    {
        failures.push(format!(
            "ack floors not met: insert={}/{}, update={}/{}, delete={}/{}",
            snapshot.insert_acks,
            minimum_acks.0,
            snapshot.update_acks,
            minimum_acks.1,
            snapshot.delete_acks,
            minimum_acks.2
        ));
    }

    let mut verified = 0;
    let mut unacknowledged_advances = 0;
    let mut found_cycles = HashSet::new();
    for (cycle, state) in &snapshot.cycles {
        let id = probe_id(&snapshot.header.id_prefix, *cycle);
        let response = client
            .get(request(
                GetRequest {
                    collection: args.collection.clone(),
                    id,
                },
                &context,
            )?)
            .await;
        let value = match response {
            Ok(response) => response.into_inner(),
            Err(error) => {
                failures.push(format!("get cycle {cycle} failed: {}", error.code()));
                continue;
            }
        };
        let actual_revision = if value.found {
            found_cycles.insert(*cycle);
            match metadata_revision(&value.metadata, &snapshot.header.run_id, *cycle) {
                Ok(revision) => {
                    if !vector_matches(
                        &value.vector,
                        &probe_vector(snapshot.header.dimensions, *cycle, revision),
                    ) {
                        failures.push(format!("cycle {cycle} vector does not match revision"));
                    }
                    Some(revision)
                }
                Err(error) => {
                    failures.push(format!("cycle {cycle}: {error}"));
                    None
                }
            }
        } else {
            None
        };
        let (accepted, advanced) = state_is_accepted(state, actual_revision);
        if accepted {
            verified += 1;
            if advanced {
                unacknowledged_advances += 1;
            }
        } else {
            failures.push(format!(
                "cycle {cycle} regressed or advanced beyond its crash boundary: ack={:?}, target={:?}, actual_revision={actual_revision:?}",
                state.last_ack, state.target
            ));
        }
    }

    let mut cleanup_deleted = 0;
    let mut health_after_cleanup = None;
    if cleanup {
        for cycle in snapshot.cycles.keys() {
            let value = client
                .delete(request(
                    DeleteRequest {
                        collection: args.collection.clone(),
                        id: probe_id(&snapshot.header.id_prefix, *cycle),
                    },
                    &context,
                )?)
                .await;
            match value {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success
                        || !matches!(
                            response.status,
                            value if value == DeleteStatus::Deleted as i32
                                || value == DeleteStatus::NotFound as i32
                                || value == DeleteStatus::AlreadyDeleted as i32
                        )
                    {
                        failures.push(format!("cleanup delete for cycle {cycle} was inconsistent"));
                    } else if response.status == DeleteStatus::Deleted as i32 {
                        cleanup_deleted += 1;
                    }
                }
                Err(error) => failures.push(format!(
                    "cleanup delete for cycle {cycle} failed: {}",
                    error.code()
                )),
            }
        }
        let deadline = Instant::now() + Duration::from_secs(cleanup_timeout_seconds);
        loop {
            match health(&mut client, &context).await {
                Ok(value)
                    if value.healthy
                        && value.ready
                        && value.active_vectors == expected_baseline_active_vectors =>
                {
                    health_after_cleanup = Some(value);
                    break;
                }
                Ok(value) if Instant::now() >= deadline => {
                    health_after_cleanup = Some(value);
                    failures
                        .push("active-vector count did not reconcile after cleanup".to_string());
                    break;
                }
                Err(error) if Instant::now() >= deadline => {
                    failures.push(format!("health after cleanup failed: {error}"));
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }

    let report = VerificationReport {
        schema_version: REPORT_SCHEMA_VERSION,
        report_type: "akidb.market-recovery-verification.v1",
        generated_at_unix_ms: generated_at_unix_ms(),
        run_id: snapshot.header.run_id,
        server: args.server.clone(),
        collection: args.collection.clone(),
        journal_sha256: sha256_file(journal)?,
        complete_journal_records: snapshot.complete_records,
        ignored_partial_tail: snapshot.ignored_partial_tail,
        allocated_cycles: snapshot.cycles.len(),
        insert_acks: snapshot.insert_acks,
        update_acks: snapshot.update_acks,
        delete_acks: snapshot.delete_acks,
        acknowledged_states_verified: verified,
        accepted_unacknowledged_advances: unacknowledged_advances,
        cleanup_requested: cleanup,
        cleanup_deleted,
        health_before_cleanup,
        health_after_cleanup,
        verdict: Verdict {
            status: if failures.is_empty() { "pass" } else { "fail" },
            failures,
        },
    };
    write_json_atomic(output_json, &report)?;
    println!("{}", serde_json::to_string(&report)?);
    if report.verdict.status == "pass" {
        Ok(())
    } else {
        Err("recovery verification failed".into())
    }
}

fn client_context(args: &Args) -> Result<ClientContext, Box<dyn std::error::Error>> {
    let token = std::env::var(&args.token_env).ok();
    if token.as_deref().is_some_and(|value| {
        value.is_empty() || value.trim() != value || value.contains(['\n', '\r'])
    }) {
        return Err(format!("{} contains a non-canonical token", args.token_env).into());
    }
    Ok(ClientContext {
        collection: args.collection.clone(),
        workspace: args.workspace.clone(),
        token,
    })
}

fn sha256_file(path: &Path) -> io::Result<String> {
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
    Ok(format!("{:x}", digest.finalize()))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_file_name(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args).map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    match &args.command {
        Command::Mutate {
            run_id,
            id_prefix,
            dimensions,
            workers,
            cycle_qps,
            duration_seconds,
            max_cycles,
            journal,
            output_json,
        } => {
            run_mutate(
                &args,
                MutateOptions {
                    run_id,
                    id_prefix,
                    dimensions: *dimensions,
                    workers: *workers,
                    cycle_qps: *cycle_qps,
                    duration_seconds: *duration_seconds,
                    max_cycles: *max_cycles,
                    journal_path: journal,
                    output_json,
                },
            )
            .await
        }
        Command::Inspect {
            journal,
            min_insert_acks,
            min_update_acks,
            min_delete_acks,
        } => inspect_journal(
            journal,
            *min_insert_acks,
            *min_update_acks,
            *min_delete_acks,
        ),
        Command::Verify {
            journal,
            expected_baseline_active_vectors,
            min_insert_acks,
            min_update_acks,
            min_delete_acks,
            cleanup,
            cleanup_timeout_seconds,
            output_json,
        } => {
            run_verify(
                &args,
                journal,
                *expected_baseline_active_vectors,
                (*min_insert_acks, *min_update_acks, *min_delete_acks),
                *cleanup,
                *cleanup_timeout_seconds,
                output_json,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn journal_fixture() -> (TempDir, PathBuf) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("probe.ndjson");
        let mut writer = JournalWriter::create(&path, "run-1234567", "probe", 4, 100).unwrap();
        writer.allocate(0, TargetState::Inserted).unwrap();
        writer.ack(0, Operation::Insert, 1).unwrap();
        writer.allocate(1, TargetState::Updated).unwrap();
        writer.ack(1, Operation::Insert, 1).unwrap();
        writer.ack(1, Operation::Update, 2).unwrap();
        writer.allocate(2, TargetState::Deleted).unwrap();
        writer.ack(2, Operation::Insert, 1).unwrap();
        writer.ack(2, Operation::Update, 2).unwrap();
        writer.ack(2, Operation::Delete, 3).unwrap();
        (directory, path)
    }

    #[test]
    fn journal_round_trip_tracks_acknowledged_states() {
        let (_directory, path) = journal_fixture();
        let snapshot = parse_journal(&path).unwrap();
        assert_eq!(snapshot.header.baseline_active_vectors, 100);
        assert_eq!(snapshot.cycles.len(), 3);
        assert_eq!(snapshot.insert_acks, 3);
        assert_eq!(snapshot.update_acks, 2);
        assert_eq!(snapshot.delete_acks, 1);
        assert!(!snapshot.ignored_partial_tail);
    }

    #[test]
    fn journal_ignores_only_an_unterminated_tail() {
        let (_directory, path) = journal_fixture();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(br#"{"record_type":"ack""#)
            .unwrap();
        let snapshot = parse_journal(&path).unwrap();
        assert!(snapshot.ignored_partial_tail);
        assert_eq!(snapshot.delete_acks, 1);
    }

    #[test]
    fn journal_rejects_an_invalid_complete_record() {
        let (_directory, path) = journal_fixture();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"not-json\n").unwrap();
        let error = parse_journal(&path).unwrap_err();
        assert!(error.contains("invalid complete journal record"));
    }

    #[test]
    fn crash_boundary_accepts_only_one_unacknowledged_advance() {
        let inserted = CycleState {
            target: TargetState::Updated,
            last_ack: Some(Operation::Insert),
        };
        assert_eq!(state_is_accepted(&inserted, Some(1)), (true, false));
        assert_eq!(state_is_accepted(&inserted, Some(2)), (true, true));
        assert_eq!(state_is_accepted(&inserted, None), (false, false));

        let updated = CycleState {
            target: TargetState::Deleted,
            last_ack: Some(Operation::Update),
        };
        assert_eq!(state_is_accepted(&updated, Some(2)), (true, false));
        assert_eq!(state_is_accepted(&updated, None), (true, true));
        assert_eq!(state_is_accepted(&updated, Some(1)), (false, false));
    }

    #[test]
    fn probe_vectors_are_revision_specific_and_repeatable() {
        assert_eq!(probe_vector(4, 7, 1), probe_vector(4, 7, 1));
        assert_ne!(probe_vector(4, 7, 1), probe_vector(4, 7, 2));
        assert_eq!(probe_vector(4, 7, 1).len(), 4);
    }
}
