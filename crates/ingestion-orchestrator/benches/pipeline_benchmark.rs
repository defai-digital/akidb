//! Performance benchmarks for the ingestion pipeline
//!
//! Run with: cargo bench -p akidb-ingestion

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;

use akidb_ingestion::{
    backpressure::BackpressureController,
    chunker::SemanticChunker,
    circuit_breaker::CircuitBreaker,
    config::BackpressureConfig,
    config::ChunkerConfig,
    config::CircuitBreakerConfig,
    idempotency::IdempotencyChecker,
    parsers::{route_parser, DocumentFormat},
};

/// Generate test documents of various sizes
fn generate_document(sentences: usize) -> String {
    let base_sentences = [
        "This is a test sentence for benchmarking purposes.",
        "The quick brown fox jumps over the lazy dog.",
        "Machine learning models require careful tuning and validation.",
        "Vector databases enable efficient similarity search at scale.",
        "Document processing pipelines must handle diverse content types.",
        "Natural language processing has advanced significantly in recent years.",
        "Embedding models convert text into dense vector representations.",
        "Efficient chunking is crucial for retrieval-augmented generation.",
    ];

    let mut doc = String::new();
    for i in 0..sentences {
        doc.push_str(base_sentences[i % base_sentences.len()]);
        doc.push(' ');
    }
    doc
}

/// Benchmark semantic chunking with tiktoken
fn benchmark_chunking(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunking");
    group.measurement_time(Duration::from_secs(10));

    let config = ChunkerConfig {
        target_tokens: 512,
        min_overlap: 20,
        max_overlap: 50,
    };
    let chunker = SemanticChunker::new(config);

    for size in [10, 50, 100, 500, 1000].iter() {
        let doc = generate_document(*size);
        group.throughput(Throughput::Bytes(doc.len() as u64));

        group.bench_with_input(BenchmarkId::new("sentences", size), &doc, |b, doc| {
            b.iter(|| {
                let chunks = chunker.chunk(black_box(doc));
                black_box(chunks)
            })
        });
    }

    group.finish();
}

/// Benchmark idempotency checking (SHA-256 hashing)
fn benchmark_idempotency(c: &mut Criterion) {
    let mut group = c.benchmark_group("idempotency");
    group.measurement_time(Duration::from_secs(5));

    let checker = IdempotencyChecker::new(10_000);

    for size in [1024, 10240, 102400, 1024000].iter() {
        let data: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("bytes", size), &data, |b, data| {
            b.iter(|| {
                let (is_dup, hash) = checker.check_and_mark(black_box(data));
                black_box((is_dup, hash))
            })
        });
    }

    group.finish();
}

/// Benchmark JSON parsing
fn benchmark_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");
    group.measurement_time(Duration::from_secs(5));

    let parser = route_parser(DocumentFormat::Json).unwrap();

    // Small JSON
    let small_json = r#"{"title": "Test", "content": "Hello world"}"#;

    // Medium JSON with nested structure
    let medium_json = r#"{
        "document": {
            "title": "Test Document",
            "sections": [
                {"heading": "Introduction", "content": "This is the intro."},
                {"heading": "Body", "content": "This is the main content."},
                {"heading": "Conclusion", "content": "This is the conclusion."}
            ]
        },
        "metadata": {"author": "Test", "date": "2026-01-21"}
    }"#;

    // Large JSON with array
    let mut large_json = String::from(r#"{"items": ["#);
    for i in 0..100 {
        if i > 0 {
            large_json.push(',');
        }
        large_json.push_str(&format!(
            r#"{{"id": {}, "text": "Item {} with some longer text content for testing."}}"#,
            i, i
        ));
    }
    large_json.push_str("]}");

    group.bench_function("small", |b| {
        b.iter(|| {
            let result = parser.parse(black_box(small_json.as_bytes()));
            black_box(result)
        })
    });

    group.bench_function("medium", |b| {
        b.iter(|| {
            let result = parser.parse(black_box(medium_json.as_bytes()));
            black_box(result)
        })
    });

    group.bench_function("large", |b| {
        b.iter(|| {
            let result = parser.parse(black_box(large_json.as_bytes()));
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark CSV parsing
fn benchmark_csv_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parsing");
    group.measurement_time(Duration::from_secs(5));

    let parser = route_parser(DocumentFormat::Csv).unwrap();

    // Generate CSVs of different sizes
    fn generate_csv(rows: usize) -> String {
        let mut csv = String::from("id,name,email,description\n");
        for i in 0..rows {
            csv.push_str(&format!(
                "{},User{},user{}@example.com,Description for user {} with extra text\n",
                i, i, i, i
            ));
        }
        csv
    }

    for rows in [10, 100, 1000].iter() {
        let csv = generate_csv(*rows);
        group.throughput(Throughput::Bytes(csv.len() as u64));

        group.bench_with_input(BenchmarkId::new("rows", rows), &csv, |b, csv| {
            b.iter(|| {
                let result = parser.parse(black_box(csv.as_bytes()));
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark HTML parsing
fn benchmark_html_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("html_parsing");
    group.measurement_time(Duration::from_secs(5));

    let parser = route_parser(DocumentFormat::Html).unwrap();

    let html = r#"
        <!DOCTYPE html>
        <html>
        <head><title>Test Page</title></head>
        <body>
            <header><nav>Navigation content</nav></header>
            <main>
                <h1>Main Heading</h1>
                <p>First paragraph with important content.</p>
                <p>Second paragraph with more text.</p>
                <ul>
                    <li>List item one</li>
                    <li>List item two</li>
                    <li>List item three</li>
                </ul>
                <script>console.log('script content');</script>
                <style>.hidden { display: none; }</style>
            </main>
            <footer>Footer content</footer>
        </body>
        </html>
    "#;

    group.bench_function("page", |b| {
        b.iter(|| {
            let result = parser.parse(black_box(html.as_bytes()));
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark circuit breaker operations
fn benchmark_circuit_breaker(c: &mut Criterion) {
    let mut group = c.benchmark_group("circuit_breaker");

    let config = CircuitBreakerConfig {
        failure_threshold: 5,
        reset_timeout_secs: 30,
        half_open_max_calls: 3,
    };

    group.bench_function("allow_request", |b| {
        let cb = CircuitBreaker::new(config.clone());
        b.iter(|| black_box(cb.allow_request()))
    });

    group.bench_function("record_success", |b| {
        let cb = CircuitBreaker::new(config.clone());
        b.iter(|| {
            cb.record_success();
            black_box(())
        })
    });

    group.bench_function("record_failure", |b| {
        let cb = CircuitBreaker::new(config.clone());
        b.iter(|| {
            cb.record_failure();
            cb.reset(); // Reset to prevent opening
            black_box(())
        })
    });

    group.finish();
}

/// Benchmark backpressure controller
fn benchmark_backpressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure");

    let config = BackpressureConfig {
        latency_threshold_ms: 500,
        queue_depth_high_water: 10000,
        queue_depth_low_water: 5000,
        pause_duration_secs: 5,
    };

    group.bench_function("update_latency", |b| {
        let bp = BackpressureController::new(config.clone());
        let mut latency = 0u64;
        b.iter(|| {
            bp.update_latency(black_box(latency));
            latency = (latency + 10000) % 1000000;
            black_box(())
        })
    });

    group.bench_function("is_active", |b| {
        let bp = BackpressureController::new(config.clone());
        b.iter(|| black_box(bp.is_active()))
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_chunking,
    benchmark_idempotency,
    benchmark_json_parsing,
    benchmark_csv_parsing,
    benchmark_html_parsing,
    benchmark_circuit_breaker,
    benchmark_backpressure,
);

criterion_main!(benches);
