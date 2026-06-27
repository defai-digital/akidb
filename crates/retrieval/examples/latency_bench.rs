//! Hybrid retrieval latency benchmark (§5.2 SLO gate: P95 < 50 ms).
//!
//! Measures per-query latency of the real hybrid path — dense ANN (usearch HNSW)
//! + lexical (BM25) + RRF fusion — over N indexed documents. Reports P50/P95/P99.
//!
//! Run: `cargo run -p akidb-retrieval --release --example latency_bench`
//! For the full SLO scale: set `AKIDB_BENCH_N=1000000 AKIDB_BENCH_DIM=768`.
//!
//! Defaults to 100k x 256-dim for a quick real measurement.

use std::time::Instant;

use akidb_common::VectorId;
use akidb_faiss::{HnswConfig, HnswIndex, SearchParams, VectorIndex};
use akidb_retrieval::{Bm25Index, HybridFuser, ScoredId};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn unit(&mut self) -> f32 {
        ((self.next() >> 40) as f32 / (1u64 << 23) as f32) - 1.0
    }
    fn vector(&mut self, dims: usize) -> Vec<f32> {
        let mut v: Vec<f32> = (0..dims).map(|_| self.unit()).collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        v
    }
    fn word(&mut self, vocab: usize) -> String {
        format!("w{}", self.next() as usize % vocab)
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let n = env_usize("AKIDB_BENCH_N", 100_000);
    let dims = env_usize("AKIDB_BENCH_DIM", 256);
    let queries = env_usize("AKIDB_BENCH_QUERIES", 1_000);
    let top_k = env_usize("AKIDB_BENCH_TOPK", 10);
    let vocab = 5_000;
    let mut rng = Lcg(0xDEADBEEFCAFEF00D);

    println!("Hybrid latency benchmark: N={n} dims={dims} queries={queries} top_k={top_k}");

    // Build the dense index + lexical index.
    let index = HnswIndex::new(HnswConfig::new(dims)).expect("hnsw");
    let mut bm25 = Bm25Index::new();

    let build_start = Instant::now();
    for i in 0..n {
        let id = VectorId::new(format!("v{i}"));
        index.insert(&id, &rng.vector(dims)).expect("insert");
        let text = format!("{} {} {} {}", rng.word(vocab), rng.word(vocab), rng.word(vocab), rng.word(vocab));
        bm25.insert(id, &text);
    }
    println!("  build: {} docs in {:.1}s", n, build_start.elapsed().as_secs_f64());

    // Pre-generate queries.
    let qs: Vec<(Vec<f32>, String)> = (0..queries)
        .map(|_| (rng.vector(dims), format!("{} {}", rng.word(vocab), rng.word(vocab))))
        .collect();

    let fuser = HybridFuser::new();
    let pool = (top_k * 4).max(40);

    // Warm-up.
    for (qv, qt) in qs.iter().take(20) {
        let dense = index.search(qv, &SearchParams::new(pool)).unwrap();
        let lexical = bm25.search(qt, pool);
        let dense_scored: Vec<ScoredId> = dense.iter().map(|r| ScoredId::new(r.id.clone(), r.score)).collect();
        let _ = fuser.fuse(&dense_scored, &lexical, top_k);
    }

    // Measured runs.
    let mut dense_us: Vec<f64> = Vec::with_capacity(queries);
    let mut hybrid_us: Vec<f64> = Vec::with_capacity(queries);
    for (qv, qt) in &qs {
        let t0 = Instant::now();
        let dense = index.search(qv, &SearchParams::new(pool)).unwrap();
        let dense_elapsed = t0.elapsed().as_secs_f64() * 1e6;

        let t1 = Instant::now();
        let lexical = bm25.search(qt, pool);
        let dense_scored: Vec<ScoredId> = dense.iter().map(|r| ScoredId::new(r.id.clone(), r.score)).collect();
        let _ = fuser.fuse(&dense_scored, &lexical, top_k);
        let hybrid_elapsed = dense_elapsed + t1.elapsed().as_secs_f64() * 1e6;

        dense_us.push(dense_elapsed);
        hybrid_us.push(hybrid_elapsed);
    }

    dense_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    hybrid_us.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let report = |label: &str, v: &[f64]| {
        println!(
            "  {label:<8} P50={:.3}ms  P95={:.3}ms  P99={:.3}ms  max={:.3}ms",
            percentile(v, 50.0) / 1000.0,
            percentile(v, 95.0) / 1000.0,
            percentile(v, 99.0) / 1000.0,
            v.last().copied().unwrap_or(0.0) / 1000.0,
        );
    };
    println!("\n  latency (per query):");
    report("dense", &dense_us);
    report("hybrid", &hybrid_us);

    let p95_ms = percentile(&hybrid_us, 95.0) / 1000.0;
    println!(
        "\n  hybrid P95 = {:.3}ms  (SLO target: < 50ms @ 1M) -> {}",
        p95_ms,
        if p95_ms < 50.0 { "PASS" } else { "OVER SLO" }
    );
}
