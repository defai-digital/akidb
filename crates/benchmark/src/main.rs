//! AkiDB GPU Performance Benchmark
//!
//! This tool benchmarks the AkiDB vector database to verify GPU acceleration.

use akidb_grpc::proto::akidb_client::AkidbClient;
use akidb_grpc::proto::{
    HealthRequest, InsertBatchRequest, InsertRequest, SearchRequest, Vector,
};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use rand_distr::{Distribution, Normal};
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// AkiDB GPU Performance Benchmark
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "http://192.168.1.61:50051")]
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
}

fn generate_random_vector(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    let normal = Normal::new(0.0, 1.0).unwrap();
    let mut vec: Vec<f32> = (0..dim).map(|_| normal.sample(&mut rng) as f32).collect();

    // L2 normalize for better similarity search
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        vec.iter_mut().for_each(|x| *x /= norm);
    }
    vec
}

async fn check_health(client: &mut AkidbClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.health(HealthRequest {}).await?;
    let health = response.into_inner();

    println!("\n=== Server Health ===");
    println!("Healthy: {}", health.healthy);
    println!("Ready: {}", health.ready);
    println!("Using GPU: {}", health.using_gpu);
    println!("Total vectors: {}", health.total_vectors);
    println!("Active vectors: {}", health.active_vectors);
    println!("Message: {}", health.message);

    Ok(())
}

async fn benchmark_inserts(
    client: &mut AkidbClient<Channel>,
    args: &Args,
) -> Result<Duration, Box<dyn std::error::Error>> {
    println!("\n=== Insert Benchmark ===");
    println!("Inserting {} vectors (dim={}) in batches of {}...",
             args.num_vectors, args.dimension, args.batch_size);

    let pb = ProgressBar::new(args.num_vectors as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let start = Instant::now();
    let mut total_inserted = 0;

    for batch_start in (0..args.num_vectors).step_by(args.batch_size) {
        let batch_end = (batch_start + args.batch_size).min(args.num_vectors);
        let batch_size = batch_end - batch_start;

        let vectors: Vec<Vector> = (batch_start..batch_end)
            .map(|i| Vector {
                id: format!("vec_{:08}", i),
                embedding: generate_random_vector(args.dimension),
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
            println!("Warning: Batch insert failed, {} failed IDs", result.failed_ids.len());
        }

        pb.set_position(batch_end as u64);
    }

    pb.finish_with_message("Insert complete");
    let duration = start.elapsed();

    let rate = total_inserted as f64 / duration.as_secs_f64();
    println!("Inserted {} vectors in {:.2?}", total_inserted, duration);
    println!("Insert rate: {:.0} vectors/sec", rate);

    Ok(duration)
}

async fn benchmark_single_inserts(
    client: &mut AkidbClient<Channel>,
    args: &Args,
    count: usize,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    println!("\n=== Single Insert Latency Test ({} inserts) ===", count);

    let mut latencies = Vec::with_capacity(count);

    for i in 0..count {
        let vector = generate_random_vector(args.dimension);
        let request = InsertRequest {
            collection: "default".to_string(),
            id: format!("single_vec_{:08}", i),
            vector,
            metadata: vec![],
        };

        let start = Instant::now();
        let _response = client.insert(request).await?;
        latencies.push(start.elapsed());
    }

    // Calculate stats
    latencies.sort();
    let avg = latencies.iter().map(|d| d.as_micros()).sum::<u128>() / latencies.len() as u128;
    let p50 = latencies[latencies.len() / 2].as_micros();
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize].as_micros();
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize].as_micros();

    println!("Single insert latency:");
    println!("  Avg: {} us", avg);
    println!("  P50: {} us", p50);
    println!("  P95: {} us", p95);
    println!("  P99: {} us", p99);

    Ok(latencies)
}

async fn benchmark_searches(
    client: &mut AkidbClient<Channel>,
    args: &Args,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    if args.concurrency > 1 {
        return benchmark_searches_concurrent(&args.server, args).await;
    }

    println!("\n=== Search Benchmark ===");
    println!("Running {} search queries (top_k={}, nprobe={})...",
             args.num_queries, args.top_k, args.nprobe);

    let pb = ProgressBar::new(args.num_queries as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let mut latencies = Vec::with_capacity(args.num_queries);
    let mut total_results = 0;

    for i in 0..args.num_queries {
        let query = generate_random_vector(args.dimension);
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

    pb.finish_with_message("Search complete");

    // Calculate stats
    latencies.sort();
    let total_time: Duration = latencies.iter().sum();
    let avg = latencies.iter().map(|d| d.as_micros()).sum::<u128>() / latencies.len() as u128;
    let p50 = latencies[latencies.len() / 2].as_micros();
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize].as_micros();
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize].as_micros();
    let min = latencies.first().unwrap().as_micros();
    let max = latencies.last().unwrap().as_micros();

    let qps = args.num_queries as f64 / total_time.as_secs_f64();
    let avg_results = total_results as f64 / args.num_queries as f64;

    println!("\nSearch Performance:");
    println!("  Total time: {:.2?}", total_time);
    println!("  QPS: {:.0} queries/sec", qps);
    println!("  Avg results per query: {:.1}", avg_results);
    println!("\nLatency (microseconds):");
    println!("  Min: {} us", min);
    println!("  Avg: {} us", avg);
    println!("  P50: {} us", p50);
    println!("  P95: {} us", p95);
    println!("  P99: {} us", p99);
    println!("  Max: {} us", max);

    // Check SLO (10ms = 10,000 us)
    let within_slo = latencies.iter().filter(|d| d.as_micros() < 10_000).count();
    let slo_percentage = within_slo as f64 / latencies.len() as f64 * 100.0;
    println!("\nSLO Compliance (< 10ms): {:.1}%", slo_percentage);

    Ok(latencies)
}

async fn benchmark_searches_concurrent(
    server: &str,
    args: &Args,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    println!("\n=== Concurrent Search Benchmark ===");
    println!("Running {} search queries with {} concurrent workers (top_k={}, nprobe={})...",
             args.num_queries, args.concurrency, args.top_k, args.nprobe);

    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let completed = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(args.num_queries)));

    let pb = ProgressBar::new(args.num_queries as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let overall_start = Instant::now();
    let mut handles = Vec::with_capacity(args.num_queries);

    for _ in 0..args.num_queries {
        let permit = semaphore.clone().acquire_owned().await?;
        let server = server.to_string();
        let dim = args.dimension;
        let top_k = args.top_k;
        let nprobe = args.nprobe;
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

            let query = generate_random_vector(dim);
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
    let mut latencies = Arc::try_unwrap(latencies)
        .unwrap_or_else(|_| panic!("Failed to unwrap latencies"))
        .into_inner();

    if latencies.is_empty() {
        println!("No successful queries!");
        return Ok(vec![]);
    }

    latencies.sort();
    let avg = latencies.iter().map(|d| d.as_micros()).sum::<u128>() / latencies.len() as u128;
    let p50 = latencies[latencies.len() / 2].as_micros();
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize].as_micros();
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize].as_micros();
    let min = latencies.first().unwrap().as_micros();
    let max = latencies.last().unwrap().as_micros();

    let qps = latencies.len() as f64 / overall_time.as_secs_f64();

    println!("\nConcurrent Search Performance:");
    println!("  Concurrency: {} workers", args.concurrency);
    println!("  Total wall-clock time: {:.2?}", overall_time);
    println!("  Throughput: {:.0} queries/sec", qps);
    println!("  Successful queries: {}/{}", latencies.len(), args.num_queries);
    println!("\nLatency (microseconds):");
    println!("  Min: {} us", min);
    println!("  Avg: {} us", avg);
    println!("  P50: {} us", p50);
    println!("  P95: {} us", p95);
    println!("  P99: {} us", p99);
    println!("  Max: {} us", max);

    // Check SLO (10ms = 10,000 us)
    let within_slo = latencies.iter().filter(|d| d.as_micros() < 10_000).count();
    let slo_percentage = within_slo as f64 / latencies.len() as f64 * 100.0;
    println!("\nSLO Compliance (< 10ms): {:.1}%", slo_percentage);

    Ok(latencies)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .init();

    println!("========================================");
    println!("  AkiDB GPU Performance Benchmark");
    println!("========================================");
    println!("Server: {}", args.server);
    println!("Dimension: {}", args.dimension);

    // Connect to server
    println!("\nConnecting to server...");
    let mut client = AkidbClient::connect(args.server.clone()).await?;
    println!("Connected!");

    // Check health
    check_health(&mut client).await?;

    // Run insert benchmark
    let insert_duration = benchmark_inserts(&mut client, &args).await?;

    // Check health again (should show vectors in index)
    check_health(&mut client).await?;

    // Run single insert latency test
    benchmark_single_inserts(&mut client, &args, 100).await?;

    // Run search benchmark
    let search_latencies = benchmark_searches(&mut client, &args).await?;

    // Final summary
    println!("\n========================================");
    println!("  BENCHMARK SUMMARY");
    println!("========================================");
    println!("Vectors indexed: {}", args.num_vectors + 100);
    println!("Insert throughput: {:.0} vec/sec",
             (args.num_vectors as f64) / insert_duration.as_secs_f64());

    let search_latencies_us: Vec<u128> = search_latencies.iter().map(|d| d.as_micros()).collect();
    let avg_search = search_latencies_us.iter().sum::<u128>() / search_latencies_us.len() as u128;
    println!("Search avg latency: {} us ({:.2} ms)", avg_search, avg_search as f64 / 1000.0);

    let total_search_time: Duration = search_latencies.iter().sum();
    println!("Search QPS: {:.0}", args.num_queries as f64 / total_search_time.as_secs_f64());

    Ok(())
}
