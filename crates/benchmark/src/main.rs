//! AkiDB Performance Benchmark
//!
//! This tool benchmarks the AkiDB vector database on the supported Mac-only path.

use akidb_grpc::proto::akidb_client::AkidbClient;
use akidb_grpc::proto::{HealthRequest, InsertBatchRequest, InsertRequest, SearchRequest, Vector};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Normal};
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// AkiDB Performance Benchmark
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    server: String,

    /// Vector dimension
    #[arg(short, long, default_value = "768")]
    dimension: usize,

    /// Number of vectors to insert for training
    #[arg(short = 'n', long, default_value = "10000")]
    num_vectors: usize,

    /// Batch size for inserts
    #[arg(short, long, default_value = "100")]
    batch_size: usize,

    /// Number of search queries to run
    #[arg(short = 'q', long, default_value = "1000")]
    num_queries: usize,

    /// Top-k for search
    #[arg(short = 'k', long, default_value = "10")]
    top_k: u32,

    /// Number of IVF probes
    #[arg(long, default_value = "32")]
    nprobe: u32,

    /// Concurrency level for search benchmark
    #[arg(short, long, default_value = "1")]
    concurrency: usize,

    /// SLO target for search latency, in milliseconds
    #[arg(long, default_value = "50")]
    slo_ms: u64,

    /// Seed for deterministic synthetic vectors
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Prefix for generated vector IDs
    #[arg(long)]
    id_prefix: Option<String>,

    /// Optional path for machine-readable JSON benchmark results
    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
struct LatencyStats {
    count: usize,
    min_us: u128,
    avg_us: u128,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
}

#[derive(Debug, Clone, Serialize)]
struct InsertSummary {
    vectors_requested: usize,
    vectors_inserted: usize,
    duration_ms: u128,
    throughput_vectors_per_sec: f64,
}

#[derive(Debug, Clone, Serialize)]
struct SearchSummary {
    queries_requested: usize,
    queries_succeeded: usize,
    concurrency: usize,
    top_k: u32,
    nprobe: u32,
    wall_time_ms: u128,
    throughput_queries_per_sec: f64,
    avg_results_per_query: f64,
    slo_ms: u64,
    slo_compliance_percent: f64,
    latency: LatencyStats,
}

#[derive(Debug, Clone, Serialize)]
struct SingleInsertSummary {
    count: usize,
    latency: LatencyStats,
}

#[derive(Debug, Clone, Serialize)]
struct HealthSnapshot {
    healthy: bool,
    ready: bool,
    using_gpu: bool,
    total_vectors: u64,
    active_vectors: u64,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct HardwareMetadata {
    os: String,
    kernel: String,
    arch: String,
    mac_model: String,
    cpu_brand: String,
    memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct SoftwareMetadata {
    akidb_version: String,
    git_commit: String,
    rustc: String,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkReport {
    benchmark_version: u32,
    generated_at_unix_ms: u128,
    server: String,
    dataset: DatasetMetadata,
    hardware: HardwareMetadata,
    software: SoftwareMetadata,
    health_before: HealthSnapshot,
    health_after_insert: HealthSnapshot,
    insert: InsertSummary,
    single_insert: SingleInsertSummary,
    search: SearchSummary,
}

#[derive(Debug, Clone, Serialize)]
struct DatasetMetadata {
    dimension: usize,
    vectors: usize,
    batch_size: usize,
    seed: u64,
    id_prefix: String,
}

fn generated_at_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn command_output(command: &str, args: &[&str]) -> String {
    Command::new(command)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn hardware_metadata() -> HardwareMetadata {
    HardwareMetadata {
        os: command_output("sw_vers", &["-productVersion"]),
        kernel: command_output("uname", &["-sr"]),
        arch: command_output("uname", &["-m"]),
        mac_model: command_output("sysctl", &["-n", "hw.model"]),
        cpu_brand: command_output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        memory_bytes: command_output("sysctl", &["-n", "hw.memsize"]).parse().ok(),
    }
}

fn software_metadata() -> SoftwareMetadata {
    SoftwareMetadata {
        akidb_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        rustc: command_output("rustc", &["--version"]),
    }
}

fn percentile_us(sorted_latencies: &[Duration], percentile: f64) -> u128 {
    if sorted_latencies.is_empty() {
        return 0;
    }

    let clamped = percentile.clamp(0.0, 1.0);
    let rank = ((sorted_latencies.len() - 1) as f64 * clamped).round() as usize;
    sorted_latencies[rank].as_micros()
}

fn latency_stats(latencies: &[Duration]) -> LatencyStats {
    if latencies.is_empty() {
        return LatencyStats {
            count: 0,
            min_us: 0,
            avg_us: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
        };
    }

    let mut sorted = latencies.to_vec();
    sorted.sort();
    let avg = sorted.iter().map(|d| d.as_micros()).sum::<u128>() / sorted.len() as u128;

    LatencyStats {
        count: sorted.len(),
        min_us: sorted.first().map(Duration::as_micros).unwrap_or(0),
        avg_us: avg,
        p50_us: percentile_us(&sorted, 0.50),
        p95_us: percentile_us(&sorted, 0.95),
        p99_us: percentile_us(&sorted, 0.99),
        max_us: sorted.last().map(Duration::as_micros).unwrap_or(0),
    }
}

fn generate_random_vector(dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    let normal = Normal::new(0.0, 1.0).unwrap();
    let mut vec: Vec<f32> = (0..dim).map(|_| normal.sample(&mut rng) as f32).collect();

    // L2 normalize for better similarity search
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        vec.iter_mut().for_each(|x| *x /= norm);
    }
    vec
}

async fn check_health(
    client: &mut AkidbClient<Channel>,
) -> Result<HealthSnapshot, Box<dyn std::error::Error>> {
    let response = client.health(HealthRequest {}).await?;
    let health = response.into_inner();

    println!("\n=== Server Health ===");
    println!("Healthy: {}", health.healthy);
    println!("Ready: {}", health.ready);
    println!(
        "Using GPU: {} (unsupported in Mac-only builds)",
        health.using_gpu
    );
    println!("Total vectors: {}", health.total_vectors);
    println!("Active vectors: {}", health.active_vectors);
    println!("Message: {}", health.message);

    Ok(HealthSnapshot {
        healthy: health.healthy,
        ready: health.ready,
        using_gpu: health.using_gpu,
        total_vectors: health.total_vectors,
        active_vectors: health.active_vectors,
        message: health.message,
    })
}

async fn benchmark_inserts(
    client: &mut AkidbClient<Channel>,
    args: &Args,
    id_prefix: &str,
) -> Result<InsertSummary, Box<dyn std::error::Error>> {
    println!("\n=== Insert Benchmark ===");
    println!(
        "Inserting {} vectors (dim={}) in batches of {}...",
        args.num_vectors, args.dimension, args.batch_size
    );

    let pb = ProgressBar::new(args.num_vectors as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    let start = Instant::now();
    let mut total_inserted = 0;

    for batch_start in (0..args.num_vectors).step_by(args.batch_size) {
        let batch_end = (batch_start + args.batch_size).min(args.num_vectors);
        let batch_size = batch_end - batch_start;

        let vectors: Vec<Vector> = (batch_start..batch_end)
            .map(|i| Vector {
                id: format!("{}-vec-{:08}", id_prefix, i),
                embedding: generate_random_vector(args.dimension, args.seed.wrapping_add(i as u64)),
                metadata: vec![],
            })
            .collect();

        let request = InsertBatchRequest {
            collection: "default".to_string(),
            vectors,
        };

        let response = client.insert_batch(request).await?;
        let result = response.into_inner();

        if result.success {
            total_inserted += batch_size;
        } else {
            println!(
                "Warning: Batch insert failed, {} failed IDs",
                result.failed_ids.len()
            );
        }

        pb.set_position(batch_end as u64);
    }

    pb.finish_with_message("Insert complete");
    let duration = start.elapsed();

    let rate = total_inserted as f64 / duration.as_secs_f64();
    println!("Inserted {} vectors in {:.2?}", total_inserted, duration);
    println!("Insert rate: {:.0} vectors/sec", rate);

    Ok(InsertSummary {
        vectors_requested: args.num_vectors,
        vectors_inserted: total_inserted,
        duration_ms: duration.as_millis(),
        throughput_vectors_per_sec: rate,
    })
}

async fn benchmark_single_inserts(
    client: &mut AkidbClient<Channel>,
    args: &Args,
    id_prefix: &str,
    count: usize,
) -> Result<SingleInsertSummary, Box<dyn std::error::Error>> {
    println!("\n=== Single Insert Latency Test ({} inserts) ===", count);

    let mut latencies = Vec::with_capacity(count);

    for i in 0..count {
        let vector = generate_random_vector(
            args.dimension,
            args.seed
                .wrapping_add(args.num_vectors as u64)
                .wrapping_add(i as u64),
        );
        let request = InsertRequest {
            collection: "default".to_string(),
            id: format!("{}-single-{:08}", id_prefix, i),
            vector,
            metadata: vec![],
        };

        let start = Instant::now();
        let _response = client.insert(request).await?;
        latencies.push(start.elapsed());
    }

    let stats = latency_stats(&latencies);

    println!("Single insert latency:");
    println!("  Avg: {} us", stats.avg_us);
    println!("  P50: {} us", stats.p50_us);
    println!("  P95: {} us", stats.p95_us);
    println!("  P99: {} us", stats.p99_us);

    Ok(SingleInsertSummary {
        count,
        latency: stats,
    })
}

async fn benchmark_searches(
    client: &mut AkidbClient<Channel>,
    args: &Args,
) -> Result<SearchSummary, Box<dyn std::error::Error>> {
    if args.concurrency > 1 {
        return benchmark_searches_concurrent(&args.server, args).await;
    }

    println!("\n=== Search Benchmark ===");
    println!(
        "Running {} search queries (top_k={}, nprobe={})...",
        args.num_queries, args.top_k, args.nprobe
    );

    let pb = ProgressBar::new(args.num_queries as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut latencies = Vec::with_capacity(args.num_queries);
    let mut total_results = 0;
    let wall_start = Instant::now();

    for i in 0..args.num_queries {
        let query = generate_random_vector(
            args.dimension,
            args.seed.wrapping_add(10_000_000).wrapping_add(i as u64),
        );
        let request = SearchRequest {
            collection: "default".to_string(),
            query,
            top_k: args.top_k,
            nprobe: Some(args.nprobe),
            filter: vec![],
            tag_filter: None,
        };

        let start = Instant::now();
        let response = client.search(request).await?;
        let duration = start.elapsed();

        latencies.push(duration);
        total_results += response.into_inner().results.len();

        pb.set_position(i as u64 + 1);
    }

    let wall_time = wall_start.elapsed();
    pb.finish_with_message("Search complete");

    let stats = latency_stats(&latencies);
    let qps = latencies.len() as f64 / wall_time.as_secs_f64();
    let avg_results = total_results as f64 / args.num_queries as f64;
    let slo_threshold_us = u128::from(args.slo_ms) * 1000;
    let within_slo = latencies
        .iter()
        .filter(|d| d.as_micros() < slo_threshold_us)
        .count();
    let slo_percentage = within_slo as f64 / latencies.len() as f64 * 100.0;

    println!("\nSearch Performance:");
    println!("  Total wall-clock time: {:.2?}", wall_time);
    println!("  QPS: {:.0} queries/sec", qps);
    println!("  Avg results per query: {:.1}", avg_results);
    println!("\nLatency (microseconds):");
    println!("  Min: {} us", stats.min_us);
    println!("  Avg: {} us", stats.avg_us);
    println!("  P50: {} us", stats.p50_us);
    println!("  P95: {} us", stats.p95_us);
    println!("  P99: {} us", stats.p99_us);
    println!("  Max: {} us", stats.max_us);

    println!(
        "\nSLO Compliance (< {}ms): {:.1}%",
        args.slo_ms, slo_percentage
    );

    Ok(SearchSummary {
        queries_requested: args.num_queries,
        queries_succeeded: latencies.len(),
        concurrency: args.concurrency,
        top_k: args.top_k,
        nprobe: args.nprobe,
        wall_time_ms: wall_time.as_millis(),
        throughput_queries_per_sec: qps,
        avg_results_per_query: avg_results,
        slo_ms: args.slo_ms,
        slo_compliance_percent: slo_percentage,
        latency: stats,
    })
}

async fn benchmark_searches_concurrent(
    server: &str,
    args: &Args,
) -> Result<SearchSummary, Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    println!("\n=== Concurrent Search Benchmark ===");
    println!(
        "Running {} search queries with {} concurrent workers (top_k={}, nprobe={})...",
        args.num_queries, args.concurrency, args.top_k, args.nprobe
    );

    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let completed = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        args.num_queries,
    )));

    let pb = ProgressBar::new(args.num_queries as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    let overall_start = Instant::now();
    let mut handles = Vec::with_capacity(args.num_queries);

    for query_idx in 0..args.num_queries {
        let permit = semaphore.clone().acquire_owned().await?;
        let server = server.to_string();
        let dim = args.dimension;
        let top_k = args.top_k;
        let nprobe = args.nprobe;
        let seed = args
            .seed
            .wrapping_add(10_000_000)
            .wrapping_add(query_idx as u64);
        let latencies = latencies.clone();
        let completed = completed.clone();
        let pb = pb.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;

            // Each task creates its own connection (simulating real load)
            let mut client = match AkidbClient::connect(server).await {
                Ok(c) => c,
                Err(_) => return,
            };

            let query = generate_random_vector(dim, seed);
            let request = SearchRequest {
                collection: "default".to_string(),
                query,
                top_k,
                nprobe: Some(nprobe),
                filter: vec![],
                tag_filter: None,
            };

            let start = Instant::now();
            if client.search(request).await.is_ok() {
                let duration = start.elapsed();
                latencies.lock().await.push(duration);
            }

            let count = completed.fetch_add(1, Ordering::Relaxed) + 1;
            pb.set_position(count as u64);
        }));
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    let overall_time = overall_start.elapsed();
    pb.finish_with_message("Search complete");

    // Calculate stats
    let latencies = Arc::try_unwrap(latencies)
        .unwrap_or_else(|_| panic!("Failed to unwrap latencies"))
        .into_inner();

    if latencies.is_empty() {
        println!("No successful queries!");
        return Ok(SearchSummary {
            queries_requested: args.num_queries,
            queries_succeeded: 0,
            concurrency: args.concurrency,
            top_k: args.top_k,
            nprobe: args.nprobe,
            wall_time_ms: overall_time.as_millis(),
            throughput_queries_per_sec: 0.0,
            avg_results_per_query: 0.0,
            slo_ms: args.slo_ms,
            slo_compliance_percent: 0.0,
            latency: latency_stats(&[]),
        });
    }

    let stats = latency_stats(&latencies);

    let qps = latencies.len() as f64 / overall_time.as_secs_f64();
    let slo_threshold_us = u128::from(args.slo_ms) * 1000;
    let within_slo = latencies
        .iter()
        .filter(|d| d.as_micros() < slo_threshold_us)
        .count();
    let slo_percentage = within_slo as f64 / latencies.len() as f64 * 100.0;

    println!("\nConcurrent Search Performance:");
    println!("  Concurrency: {} workers", args.concurrency);
    println!("  Total wall-clock time: {:.2?}", overall_time);
    println!("  Throughput: {:.0} queries/sec", qps);
    println!(
        "  Successful queries: {}/{}",
        latencies.len(),
        args.num_queries
    );
    println!("\nLatency (microseconds):");
    println!("  Min: {} us", stats.min_us);
    println!("  Avg: {} us", stats.avg_us);
    println!("  P50: {} us", stats.p50_us);
    println!("  P95: {} us", stats.p95_us);
    println!("  P99: {} us", stats.p99_us);
    println!("  Max: {} us", stats.max_us);

    println!(
        "\nSLO Compliance (< {}ms): {:.1}%",
        args.slo_ms, slo_percentage
    );

    Ok(SearchSummary {
        queries_requested: args.num_queries,
        queries_succeeded: latencies.len(),
        concurrency: args.concurrency,
        top_k: args.top_k,
        nprobe: args.nprobe,
        wall_time_ms: overall_time.as_millis(),
        throughput_queries_per_sec: qps,
        avg_results_per_query: args.top_k as f64,
        slo_ms: args.slo_ms,
        slo_compliance_percent: slo_percentage,
        latency: stats,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    FmtSubscriber::builder().with_max_level(Level::INFO).init();
    let id_prefix = args
        .id_prefix
        .clone()
        .unwrap_or_else(|| format!("bench-{}", generated_at_unix_ms()));

    println!("========================================");
    println!("  AkiDB Performance Benchmark");
    println!("========================================");
    println!("Server: {}", args.server);
    println!("Dimension: {}", args.dimension);
    println!("Seed: {}", args.seed);
    println!("ID prefix: {}", id_prefix);

    // Connect to server
    println!("\nConnecting to server...");
    let mut client = AkidbClient::connect(args.server.clone()).await?;
    println!("Connected!");

    // Check health
    let health_before = check_health(&mut client).await?;

    // Run insert benchmark
    let insert = benchmark_inserts(&mut client, &args, &id_prefix).await?;

    // Check health again (should show vectors in index)
    let health_after_insert = check_health(&mut client).await?;

    // Run single insert latency test
    let single_insert = benchmark_single_inserts(&mut client, &args, &id_prefix, 100).await?;

    // Run search benchmark
    let search = benchmark_searches(&mut client, &args).await?;

    // Final summary
    println!("\n========================================");
    println!("  BENCHMARK SUMMARY");
    println!("========================================");
    println!("Vectors indexed: {}", args.num_vectors + 100);
    println!(
        "Insert throughput: {:.0} vec/sec",
        insert.throughput_vectors_per_sec
    );

    println!(
        "Search avg latency: {} us ({:.2} ms)",
        search.latency.avg_us,
        search.latency.avg_us as f64 / 1000.0
    );
    println!(
        "Search P95 latency: {} us ({:.2} ms)",
        search.latency.p95_us,
        search.latency.p95_us as f64 / 1000.0
    );
    println!("Search QPS: {:.0}", search.throughput_queries_per_sec);

    let report = BenchmarkReport {
        benchmark_version: 1,
        generated_at_unix_ms: generated_at_unix_ms(),
        server: args.server.clone(),
        dataset: DatasetMetadata {
            dimension: args.dimension,
            vectors: args.num_vectors,
            batch_size: args.batch_size,
            seed: args.seed,
            id_prefix,
        },
        hardware: hardware_metadata(),
        software: software_metadata(),
        health_before,
        health_after_insert,
        insert,
        single_insert,
        search,
    };

    if let Some(path) = &args.output_json {
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(path, json)?;
        println!("Benchmark JSON written to {}", path.display());
    }

    Ok(())
}
