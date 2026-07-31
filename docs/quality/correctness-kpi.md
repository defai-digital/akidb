# Correctness KPI Gate

Status: local + deployed-cluster correctness release gate

This gate answers product questions that latency-only tools cannot:

| User concern | KPI evidence |
| --- | --- |
| Missing data after ingest | `ingest_ack_rate`, `get_found_rate` |
| Missing index membership | `index_active_delta_match`, `self_hit_rate` |
| Wrong ingestion / corrupted payload | `get_embedding_match_rate` |
| Wrong retrieval / bad ranking | `mean_recall_at_k`, `mean_ndcg_at_k`, `mean_mrr_at_k` |
| Garbage / ghost results | `unreadable_result_id_rate`, `duplicate_result_rate`, `short_result_rate` |
| Partial multi-shard coverage | `partial_response_rate`, `query_failure_rate` |

It is intentionally separate from `akidb-bench` (throughput) and is **not** a
substitute for the public SIFT1M market matrix in
[market-readiness-qualification.md](market-readiness-qualification.md).

## Market alignment

Methodology follows the same principles as:

- [ANN-Benchmarks](https://github.com/erikbern/ann-benchmarks) — exact neighbors + Recall@K
- [VectorDBBench](https://github.com/zilliztech/VectorDBBench) — load + query quality together
- IR evaluation — nDCG@K, MRR@K, hit-rate

Differences from full market Lane A:

- Uses a deterministic synthetic cosine corpus (fast, no SIFT download).
- Suitable for CI, smoke, and post-deploy verification.
- Full public-dataset release claims still require SIFT1M/GIST evidence.

## Run locally (starts a temporary server)

```bash
python3 scripts/qa_correctness_kpi.py --build \
  --vectors 500 --queries 100 --dimensions 128
```

## Run against a deployed endpoint

```bash
# Example: SSH local forward to a private coordinator
ssh -N -L 15050:10.1.0.132:50050 c3-8-cluster-1 &

python3 scripts/qa_correctness_kpi.py \
  --external-server \
  --server 127.0.0.1:15050 \
  --dimensions 768 \
  --vectors 500 \
  --queries 100 \
  --top-k 10 \
  --nprobe 64 \
  --shard-health 10.1.0.132:50051,10.1.1.121:50051,10.1.3.4:50051,10.1.1.87:50051
```

When the entrypoint is a coordinator, pass `--shard-health` so
`index_active_delta_match` can sum real shard counters (coordinator health may
report `total_vectors=0` even when shards hold data).

## Artifacts

Writes under `qa-results/` (gitignored):

- `correctness-kpi-*.json` — machine-readable KPI rows + details
- `correctness-kpi-*.md` — human-readable benchmarking table

## Default gates

| KPI | Default gate |
| --- | --- |
| `ingest_ack_rate` | `== 1.0` |
| `get_found_rate` | `== 1.0` |
| `get_embedding_match_rate` | `== 1.0` when embeddings returned |
| `mean_recall_at_k` | `>= 0.98` |
| `min_recall_at_k` | `>= 0.80` |
| `mean_ndcg_at_k` | `>= 0.98` |
| `mean_mrr_at_k` | `>= 0.90` |
| `self_hit_rate` | `>= 0.99` |
| `unreadable_result_id_rate` | `== 0` (IDs that Search returns but Get cannot load) |
| `batch_only_result_rate` | `== 1.0` on a **clean** corpus only |
| `duplicate_result_rate` | `== 0` |
| `short_result_rate` | `== 0` |
| `query_failure_rate` | `== 0` |
| `partial_response_rate` | `== 0` |
| `search_p95_server_ms` | `<= 100` |

## Relationship to other QA

| Tool | Proves |
| --- | --- |
| `akidb-bench` | Insert/search speed only |
| `qa_vector_quality.py` | Recall/nDCG on synthetic vectors |
| `qa_correctness_kpi.py` | **Full correctness KPI table** (ingest + Get + recall + integrity) |
| `qa_text_retrieval.py` | Semantic text path with embeddings |
| `akidb-ann-bench` + market playbooks | Public-dataset market Lane A |

For a release claim about “no missing data / correct retrieval,” attach the
`correctness-kpi-*.json` artifact (and market SIFT evidence when claiming public
dataset parity).
