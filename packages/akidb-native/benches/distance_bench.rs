/// Criterion benchmarks for distance kernels and HNSW search.
///
/// Run: `cargo bench --bench distance_bench --no-default-features`
/// Quick dry run: `cargo bench --bench distance_bench --no-default-features -- --quick`
use akidb_native::bench_support::{
    cosine_distance, dot_product, l2_distance, normalize, HnswGraph,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn gen_vector(dim: usize, seed: usize) -> Vec<f32> {
    (0..dim)
        .map(|i| ((seed * dim + i) as f32 * 0.0017).sin())
        .collect()
}

fn gen_normalized(dim: usize, seed: usize) -> Vec<f32> {
    normalize(&gen_vector(dim, seed))
}

fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum::<f32>()
        .sqrt()
}

fn cosine_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    1.0 - dot_product_scalar(a, b)
}

fn bench_dot_product(c: &mut Criterion) {
    let mut group = c.benchmark_group("dot_product");
    for dim in [128, 384, 1536] {
        let a = gen_vector(dim, 1);
        let b = gen_vector(dim, 2);
        group.bench_with_input(BenchmarkId::new("simd", dim), &dim, |bench, _| {
            bench.iter(|| black_box(dot_product(black_box(&a), black_box(&b))))
        });
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| black_box(dot_product_scalar(black_box(&a), black_box(&b))))
        });
    }
    group.finish();
}

fn bench_l2_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("l2_distance");
    for dim in [128, 384, 1536] {
        let a = gen_vector(dim, 3);
        let b = gen_vector(dim, 4);
        group.bench_with_input(BenchmarkId::new("simd", dim), &dim, |bench, _| {
            bench.iter(|| black_box(l2_distance(black_box(&a), black_box(&b))))
        });
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| black_box(l2_distance_scalar(black_box(&a), black_box(&b))))
        });
    }
    group.finish();
}

fn bench_cosine_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("cosine_distance");
    for dim in [128, 384, 1536] {
        let a = gen_normalized(dim, 5);
        let b = gen_normalized(dim, 6);
        group.bench_with_input(BenchmarkId::new("simd", dim), &dim, |bench, _| {
            bench.iter(|| black_box(cosine_distance(black_box(&a), black_box(&b))))
        });
        group.bench_with_input(BenchmarkId::new("scalar", dim), &dim, |bench, _| {
            bench.iter(|| black_box(cosine_distance_scalar(black_box(&a), black_box(&b))))
        });
    }
    group.finish();
}

fn bench_hnsw_search(c: &mut Criterion) {
    let dim = 128;
    let n = 10_000;

    let vectors: Vec<Vec<f32>> = (0..n).map(|i| gen_normalized(dim, i)).collect();

    let mut graph = HnswGraph::new("cosine", dim, 16, 200, 100);
    graph.build(&vectors);

    let query = gen_normalized(dim, n + 1);

    let mut group = c.benchmark_group("hnsw_search");
    group.bench_function("10k_128dim_top10", |bench| {
        bench.iter(|| black_box(graph.search(black_box(&query), 10)))
    });
    group.bench_function("10k_128dim_top100", |bench| {
        bench.iter(|| black_box(graph.search(black_box(&query), 100)))
    });
    group.finish();
}

fn bench_hnsw_search_filtered(c: &mut Criterion) {
    let dim = 128;
    let n = 10_000;

    let vectors: Vec<Vec<f32>> = (0..n).map(|i| gen_normalized(dim, i)).collect();

    let mut graph = HnswGraph::new("cosine", dim, 16, 200, 100);
    graph.build(&vectors);

    let query = gen_normalized(dim, n + 1);

    let mut group = c.benchmark_group("hnsw_search_filtered");
    group.bench_function("10k_128dim_50pct", |bench| {
        bench.iter(|| black_box(graph.search_filtered(black_box(&query), 10, |id| id % 2 == 0)))
    });
    group.bench_function("10k_128dim_1pct", |bench| {
        bench.iter(|| black_box(graph.search_filtered(black_box(&query), 10, |id| id % 100 == 0)))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_dot_product,
    bench_l2_distance,
    bench_cosine_distance,
    bench_hnsw_search,
    bench_hnsw_search_filtered,
);
criterion_main!(benches);
