# Vector Quality Gates

AkiDB tracks retrieval quality separately from raw throughput. Throughput
benchmarks answer "how fast is search"; these gates answer "did search return
the expected neighbors." Treat these QA gates as release evidence, not as ad hoc
smoke tests.

The gate design follows common practice from ANN-Benchmarks, BEIR/MTEB-style
retrieval evaluation, and VectorDBBench: compare against ground truth, report
ranking metrics, and keep latency visible.

References:

- ANN-Benchmarks: https://github.com/erikbern/ann-benchmarks
- BEIR: https://github.com/beir-cellar/beir
- MTEB: https://github.com/embeddings-benchmark/mteb
- VectorDBBench: https://github.com/zilliztech/VectorDBBench

## Local Quality Suite

Run every local quality gate that can run with the current environment:

```bash
./scripts/qa_all.sh --build
```

Always-on gates: vector quality, correctness KPI table, feature matrix
(CRUD/filters/BM25), and live filtered search. The semantic text retrieval gate
runs only when `AX_ENGINE_MODEL_DIR` is set, unless `--require-text` is passed.
See [feature-qa-matrix.md](feature-qa-matrix.md) for the full feature map.

For release validation where local embedding artifacts are available:

```bash
AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-0.6B-4bit-DWQ \
AX_ENGINE_MODEL=mlx-community/Qwen3-Embedding-0.6B-4bit-DWQ \
EMBEDDING_DIMENSIONS=1024 \
./scripts/qa_all.sh --build --require-text
```

## Vector Index Quality

```bash
python3 scripts/qa_vector_quality.py --build
```

This script:

- Starts a clean standalone AkiDB server unless `--external-server` is used.
- Generates deterministic clustered vectors.
- Computes exact brute-force cosine ground truth.
- Compares AkiDB topK results to that ground truth.
- Writes a JSON artifact under `qa-results/`.

Default gates:

| Metric | Default Gate |
| --- | --- |
| Mean recall@k | `>= 0.98` |
| Mean nDCG@k | `>= 0.98` |
| Hit rate@k | `>= 0.99` |
| Server P95 latency | `<= 50 ms` |
| Wall-clock P95 latency | `<= 2000 ms` |

The server latency gate is the primary latency signal. Wall-clock latency is
kept as a loose guard because this harness shells out to `grpcurl` for each
request, so process startup and local scheduler noise can dominate small runs.

Example larger run:

```bash
python3 scripts/qa_vector_quality.py \
  --build \
  --vectors 10000 \
  --queries 1000 \
  --dimensions 768 \
  --top-k 10 \
  --output qa-results/vector-quality-10k-768d.json
```

## Semantic Text Retrieval

```bash
AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-0.6B-4bit-DWQ \
AX_ENGINE_MODEL=mlx-community/Qwen3-Embedding-0.6B-4bit-DWQ \
EMBEDDING_DIMENSIONS=1024 \
python3 scripts/qa_text_retrieval.py --build
```

This script starts both the local embedding sidecar and AkiDB, inserts a small
curated corpus, queries with natural-language prompts, and scores returned
documents using recall, nDCG, MRR, and hit rate.

Default gates:

| Metric | Default Gate |
| --- | --- |
| Mean recall@k | `>= 0.85` |
| Mean nDCG@k | `>= 0.75` |
| Mean MRR@k | `>= 0.75` |
| Hit rate@k | `>= 0.85` |
| Wall-clock P95 latency | `<= 2000 ms` |

Use this gate to catch regressions in the complete text path: tokenizer,
embedding sidecar, embedding dimensions, AkiDB insert, `TextSearch`, and ranking.
It is intentionally small and deterministic. It does not replace a full
BEIR/MTEB benchmark run for model selection.

## Result Artifacts

Both scripts write JSON under `qa-results/`, which is ignored by Git. Keep those
files for local diagnosis. Attach selected artifacts to release notes only when
they are intended as evidence for a specific build.

Each artifact includes:

- dataset shape and model/dimension metadata
- configured pass/fail thresholds
- aggregate metrics
- latency percentiles
- low-recall or per-query details

## Publishing Rule

Do not publish a recall or semantic-relevance claim from `akidb-bench` alone.
Use `akidb-bench` for latency and throughput, and use these QA scripts for
quality claims. For a full **ingest + Get + retrieval KPI table** (missing data,
wrong payload, ghost IDs), use
[correctness-kpi.md](correctness-kpi.md) / `scripts/qa_correctness_kpi.py`.
A release quality statement should include:

- the `qa_vector_quality.py` JSON artifact
- the `qa_correctness_kpi.py` JSON/Markdown table
- the `qa_text_retrieval.py` JSON artifact when text search is in scope
- the benchmark artifact for throughput/latency
- the exact embedding model ID and dimensions
