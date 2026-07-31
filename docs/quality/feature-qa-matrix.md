# Feature QA Matrix (AkiDB v0.10+)

This document maps **current product features** to automated quality gates.
It is the operator-facing answer to: “which features are tested, and what
KPI proves they work?”

## Product surface (latest)

| Area | Features | Primary QA |
| --- | --- | --- |
| Dense retrieval | HNSW insert/search, f32 cosine | `qa_vector_quality.py`, `qa_correctness_kpi.py` |
| CRUD durability | Insert, Get, Update, Delete | `qa_feature_matrix.py` |
| Metadata filters | Legacy JSON filter + typed `TagFilter` | `qa_feature_matrix.py`, `qa_filtered_search.py` |
| Hybrid / BM25 | Text field + `TextSearch retrieval_mode=bm25` (no embedder) | `qa_feature_matrix.py` |
| Semantic text | Embedding sidecar + TextSearch | `qa_text_retrieval.py` (needs `AX_ENGINE_MODEL_DIR`) |
| Quality knobs | score_threshold, group_by, ACL, graph_hybrid | `qa_quality_knobs.py` (cargo tests) |
| Code retrieval | Language chunking fixtures | `qa_code_retrieval.py` |
| Correctness KPI table | Missing data / wrong ingest / wrong retrieval | `qa_correctness_kpi.py` |
| Generation serving | Stage/activate/rollback generations | `qa_generation_serving.py` + compose/MinIO |
| Authoritative Memory | Observe/Remember/Recall/history | `qualify-agentic-memory-amd64.sh`, memory scripts |
| Market ANN | SIFT1M Recall@K, competitors, recovery | Ansible market playbooks + summarizers |
| Multi-shard / HA entry | Coordinator fan-out, active-active leadership | Deploy `verify.yml` + cluster correctness KPI |

## Local suite

```bash
# Core always-on gates (vector + correctness KPI + feature matrix + filtered search)
./scripts/qa_all.sh --build

# Also require embedding-backed text retrieval
AX_ENGINE_MODEL_DIR=/path/to/model \
AX_ENGINE_MODEL=... \
EMBEDDING_DIMENSIONS=1024 \
./scripts/qa_all.sh --build --require-text
```

### What `qa_all.sh` runs

| Gate | Script | Proves |
| --- | --- | --- |
| 1 | `qa_vector_quality.py` | Dense Recall@K / nDCG vs exact GT |
| 1b | `qa_correctness_kpi.py` | Ingest ack, Get integrity, retrieval KPI table |
| 1c | `qa_feature_matrix.py` | CRUD, filters, BM25, score threshold, SearchBatch |
| 1d | `qa_filtered_search.py` | Filter purity + filtered recall (live) |
| 2 | `qa_text_retrieval.py` | Semantic TextSearch (optional) |

Optional heavier gates (not always in `qa_all.sh` because they need cargo time
or external deps):

```bash
python3 scripts/qa_quality_knobs.py
python3 scripts/qa_code_retrieval.py
python3 scripts/entry_smoke_live.py   # needs grpcio / .venv-smoke
```

## Feature matrix checks

`scripts/qa_feature_matrix.py` emits Markdown + JSON:

| Check | Feature |
| --- | --- |
| `health_ready` | Health API |
| `insert_success_rate` / `get_found_rate` | Durable write/read |
| `update_roundtrip` | Update + Get embedding match |
| `delete_hides_get` / `delete_hides_search` | Tombstone delete |
| `dense_self_hit` | Dense search membership |
| `search_batch_count` | SearchBatch |
| `legacy_filter_bucket` | Legacy metadata filter |
| `tag_filter_eq` | Typed TagFilter EQ |
| `score_threshold_restricts` | Score threshold |
| `bm25_textsearch_no_embedding` | BM25 path without embedder |

## Against a deployed cluster

```bash
# Tunnel private coordinator if needed
ssh -N -L 15050:10.1.0.132:50050 c3-8-cluster-1 &

python3 scripts/qa_feature_matrix.py \
  --external-server --server 127.0.0.1:15050 --dimensions 768

python3 scripts/qa_correctness_kpi.py \
  --external-server --server 127.0.0.1:15050 --dimensions 768 \
  --shard-health 127.0.0.1:15051,...
```

## Not claimed by this suite

- Public SIFT1M / GIST market ANN parity (see `market-readiness-qualification.md`)
- Full knowledge-cell generation soak (see knowledge-cell qualification)
- Authoritative Memory production HA claims (developer preview + AMD64 systems profile)
- Multi-coordinator leadership election under partition (unit/integration coverage; ops verify separately)

## Publishing rule

For a release statement about feature correctness on standalone or cluster:

1. Attach `feature-matrix-*.json` + `correctness-kpi-*.md`
2. Attach vector-quality JSON
3. Only claim embedding TextSearch if `qa_text_retrieval.py` ran
4. Only claim market ANN parity with SIFT evidence + summarizer PASS
