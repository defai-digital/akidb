/// Criterion benchmarks for end-to-end engine operations.
///
/// Run: `cargo bench --bench engine_bench --no-default-features`
/// Quick dry run: `cargo bench --bench engine_bench --no-default-features -- --quick`
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use akidb_native::bench_support::NativeRecord;
use akidb_native::{EngineInner, EngineOptions, SearchMode, SearchOptions};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use serde_json::json;
use tempfile::TempDir;

fn gen_vector(dim: usize, seed: usize) -> Vec<f32> {
    let mut vector: Vec<f32> = (0..dim)
        .map(|i| ((seed * dim + i) as f32 * 0.0013).sin())
        .collect();
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

fn make_record(dim: usize, index: usize) -> NativeRecord {
    NativeRecord {
        chunk_id: format!("chunk-{index:06}"),
        doc_id: format!("doc-{index:06}"),
        vector: gen_vector(dim, index),
        metadata: json!({
            "source_uri": format!("file://doc-{index:06}.txt"),
            "kind": if index.is_multiple_of(2) { "even" } else { "odd" },
            "offset": index,
        }),
        chunk_text: Some(format!(
            "document {index} discusses vectors filters and ranking"
        )),
    }
}

fn search_opts(collection_id: &str, dim: usize, seed: usize) -> SearchOptions {
    SearchOptions {
        collection_id: collection_id.to_string(),
        query_vector: gen_vector(dim, seed),
        top_k: 10,
        filters: None,
        manifest_version: None,
        include_uncommitted: true,
        mode: SearchMode::Vector,
        query_text: None,
        vector_weight: 1.0,
        keyword_weight: 1.0,
        explain: false,
        ef_search: None,
    }
}

fn hybrid_search_opts(collection_id: &str, dim: usize, seed: usize) -> SearchOptions {
    SearchOptions {
        mode: SearchMode::Hybrid,
        query_text: Some("vectors ranking".to_string()),
        ..search_opts(collection_id, dim, seed)
    }
}

fn setup_published_engine(
    collection_id: &str,
    dim: usize,
    record_count: usize,
) -> (TempDir, EngineInner, String) {
    let tempdir = tempfile::tempdir().unwrap();
    let engine = EngineInner::open(EngineOptions {
        storage_path: tempdir.path().join("storage"),
        disable_wal: true,
    })
    .unwrap();
    engine
        .create_collection(collection_id, dim as i64, "cosine", "bench-model", "fp16", 16, 200, 100)
        .unwrap();
    let records: Vec<NativeRecord> = (0..record_count).map(|i| make_record(dim, i)).collect();
    engine.upsert_batch(collection_id, &records).unwrap();
    let manifest = engine
        .auto_publish(collection_id, "bench-model", "bench-pipeline")
        .unwrap();
    (tempdir, engine, manifest.manifest_id)
}

fn bench_vector_search(c: &mut Criterion) {
    let (_tempdir, engine, _manifest_id) = setup_published_engine("vector-bench", 64, 2_000);
    let opts = search_opts("vector-bench", 64, 9_999);

    let mut group = c.benchmark_group("engine_vector_search");
    group.measurement_time(Duration::from_secs(6));
    group.bench_function("2k_64d_top10", |bench| {
        bench.iter(|| black_box(engine.search(black_box(opts.clone())).unwrap()))
    });
    group.finish();
}

fn bench_hybrid_search(c: &mut Criterion) {
    let (_tempdir, engine, _manifest_id) = setup_published_engine("hybrid-bench", 64, 2_000);
    let opts = hybrid_search_opts("hybrid-bench", 64, 8_888);

    let mut group = c.benchmark_group("engine_hybrid_search");
    group.measurement_time(Duration::from_secs(6));
    group.bench_function("2k_64d_top10", |bench| {
        bench.iter(|| black_box(engine.search(black_box(opts.clone())).unwrap()))
    });
    group.finish();
}

fn bench_publish_with_pending_tombstones(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_publish");
    group.sample_size(10);
    group.bench_function("1k_64d_delete50", |bench| {
        bench.iter_batched(
            || {
                let (tempdir, engine, _manifest_id) =
                    setup_published_engine("publish-bench", 64, 1_000);
                let tombstones: Vec<String> =
                    (0..50).map(|i| format!("chunk-{i:06}")).collect();
                engine
                    .delete_chunks("publish-bench", &tombstones, "manual_revoke")
                    .unwrap();
                (tempdir, engine)
            },
            |(_tempdir, engine)| {
                black_box(
                    engine
                        .auto_publish("publish-bench", "bench-model", "bench-pipeline-v2")
                        .unwrap(),
                )
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_rollback_after_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_rollback");
    group.sample_size(10);
    group.bench_function("1k_64d_delete50", |bench| {
        bench.iter_batched(
            || {
                let (tempdir, engine, manifest_id) =
                    setup_published_engine("rollback-bench", 64, 1_000);
                let tombstones: Vec<String> =
                    (0..50).map(|i| format!("chunk-{i:06}")).collect();
                engine
                    .delete_chunks("rollback-bench", &tombstones, "manual_revoke")
                    .unwrap();
                engine
                    .auto_publish("rollback-bench", "bench-model", "bench-pipeline-v2")
                    .unwrap();
                (tempdir, engine, manifest_id)
            },
            |(_tempdir, engine, manifest_id)| {
                black_box(engine.rollback("rollback-bench", &manifest_id).unwrap())
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn run_contention_round() {
    let (_tempdir, engine, _manifest_id) = setup_published_engine("contention-bench", 64, 1_500);
    let engine = Arc::new(engine);
    let barrier = Arc::new(Barrier::new(3));

    let search_engine = Arc::clone(&engine);
    let search_barrier = Arc::clone(&barrier);
    let search_thread = thread::spawn(move || {
        search_barrier.wait();
        for seed in 0..6 {
            let opts = search_opts("contention-bench", 64, 20_000 + seed);
            black_box(search_engine.search(opts).unwrap());
        }
    });

    let write_engine = Arc::clone(&engine);
    let write_barrier = Arc::clone(&barrier);
    let write_thread = thread::spawn(move || {
        write_barrier.wait();
        for round in 0..3 {
            let base = 50_000 + round * 32;
            let records: Vec<NativeRecord> =
                (0..32).map(|offset| make_record(64, base + offset)).collect();
            write_engine
                .upsert_batch("contention-bench", &records)
                .unwrap();
            black_box(
                write_engine
                    .auto_publish("contention-bench", "bench-model", "bench-pipeline-v2")
                    .unwrap(),
            );
        }
    });

    barrier.wait();
    search_thread.join().unwrap();
    write_thread.join().unwrap();
}

fn bench_search_publish_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_contention");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));
    group.bench_function("search_vs_publish_round", |bench| {
        bench.iter(run_contention_round)
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_vector_search,
    bench_hybrid_search,
    bench_publish_with_pending_tombstones,
    bench_rollback_after_publish,
    bench_search_publish_contention,
);
criterion_main!(benches);
