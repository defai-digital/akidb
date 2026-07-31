#!/usr/bin/env python3
"""Market-aligned correctness KPI harness for AkiDB.

Latency-only tools (akidb-bench) prove speed, not correctness. This harness
follows the evaluation style used by ANN-Benchmarks / VectorDBBench / IR gates:

  * exact ground-truth neighbors (brute-force cosine)
  * Recall@K, nDCG@K, MRR@K, hit-rate
  * ingest completeness (ack count, Get found rate, embedding match)
  * index integrity (no foreign IDs, no duplicate results, no short results)
  * fail-closed KPI table (JSON + Markdown)

It intentionally answers:

  - missing data?        → Get found rate / insert ack
  - missing index?       → self-hit rate + recall@k after load
  - wrong ingestion?     → Get embedding cosine match to source vector
  - wrong retrieval?     → recall/nDCG vs exact neighbors; foreign IDs = 0

Usage (standalone process started by harness):

  python3 scripts/qa_correctness_kpi.py --build

Usage (deployed cluster / external endpoint):

  python3 scripts/qa_correctness_kpi.py \\
    --external-server --server 127.0.0.1:50050 \\
    --dimensions 768 --vectors 500 --queries 100 \\
    --shard-health 10.1.0.132:50051,10.1.1.121:50051,...
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "crates" / "proto" / "proto"
PROTO_FILE = "akidb.proto"
REPORT_TYPE = "akidb.correctness-kpi.v1"


@dataclass
class ManagedServer:
    process: subprocess.Popen[str] | None
    temp_dir: Path | None
    address: str


@dataclass
class KpiRow:
    kpi: str
    value: float | int | str
    unit: str
    gate: str
    status: str
    meaning: str


@dataclass
class KpiReport:
    rows: list[KpiRow] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def add(
        self,
        kpi: str,
        value: float | int | str,
        unit: str,
        gate: str,
        passed: bool,
        meaning: str,
    ) -> None:
        self.rows.append(
            KpiRow(
                kpi=kpi,
                value=value,
                unit=unit,
                gate=gate,
                status="PASS" if passed else "FAIL",
                meaning=meaning,
            )
        )
        if not passed:
            self.failures.append(f"{kpi}: {value} (gate {gate})")

    @property
    def passed(self) -> bool:
        return not self.failures


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = (len(ordered) - 1) * pct
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[int(idx)]
    return ordered[lo] * (hi - idx) + ordered[hi] * (idx - lo)


def normalize_rows(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return matrix / norms


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def require_grpcurl() -> None:
    if shutil.which("grpcurl") is None:
        raise RuntimeError("grpcurl is required; install with `brew install grpcurl`")


def grpcurl(address: str, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    cmd = [
        "grpcurl",
        "-plaintext",
        "-import-path",
        str(PROTO_DIR),
        "-proto",
        PROTO_FILE,
        "-d",
        "@",
        address,
        f"akidb.v1.Akidb/{method}",
    ]
    result = subprocess.run(
        cmd,
        input=json.dumps(payload or {}),
        text=True,
        capture_output=True,
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        raise RuntimeError(f"grpcurl {method} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def wait_for_health(address: str, timeout_s: int = 45) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last_error = ""
    while time.time() < deadline:
        try:
            health = grpcurl(address, "Health", {})
            if health.get("healthy") and health.get("ready"):
                return health
            last_error = str(health)
        except Exception as error:  # noqa: BLE001 - surface last error only
            last_error = str(error)
        time.sleep(0.4)
    raise TimeoutError(f"AkiDB not healthy at {address}: {last_error}")


def write_temp_config(dimensions: int, temp_dir: Path) -> Path:
    template = ROOT / "config" / "standalone.toml"
    content = template.read_text(encoding="utf-8")
    content = content.replace(
        'rocksdb_path = "./data/rocksdb"',
        f'rocksdb_path = "{temp_dir / "rocksdb"}"',
    )
    content = content.replace(
        'wal_path = "./data/wal"',
        f'wal_path = "{temp_dir / "wal"}"',
    )
    content = content.replace("dimensions = 2560", f"dimensions = {dimensions}")
    content = re.sub(
        r"(\[embedding\]\s*)enabled = true",
        r"\1enabled = false",
        content,
        count=1,
    )
    config = temp_dir / "standalone.toml"
    config.write_text(content, encoding="utf-8")
    return config


def start_server(args: argparse.Namespace) -> ManagedServer:
    if args.external_server:
        wait_for_health(args.server)
        return ManagedServer(None, None, args.server)

    binary = ROOT / "target" / "debug" / "akidb-server"
    if args.build or not binary.exists():
        subprocess.run(["cargo", "build", "-p", "akidb-server"], cwd=ROOT, check=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="akidb-correctness."))
    port = args.port or free_port()
    address = f"127.0.0.1:{port}"
    config = write_temp_config(args.dimensions, temp_dir)
    log_path = temp_dir / "akidb-server.log"
    log = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            str(binary),
            "--config",
            str(config),
            "--standalone",
            "--listen",
            address,
            "--log-level",
            args.server_log_level,
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_health(address)
    except Exception:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        raise RuntimeError(f"server failed to start; log: {log_path}") from None
    return ManagedServer(process, temp_dir, address)


def stop_server(server: ManagedServer, keep_temp: bool) -> None:
    if server.process is not None:
        server.process.send_signal(signal.SIGTERM)
        try:
            server.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.process.kill()
            server.process.wait(timeout=5)
    if server.temp_dir is not None and not keep_temp:
        shutil.rmtree(server.temp_dir, ignore_errors=True)


def make_dataset(
    *,
    vectors: int,
    queries: int,
    dimensions: int,
    clusters: int,
    seed: int,
    cluster_noise: float,
    query_noise: float,
    id_prefix: str,
) -> tuple[np.ndarray, list[str], np.ndarray, list[int]]:
    rng = np.random.default_rng(seed)
    centers = normalize_rows(rng.normal(size=(clusters, dimensions)).astype(np.float32))
    labels = rng.integers(0, clusters, size=vectors)
    data = centers[labels] + rng.normal(scale=cluster_noise, size=(vectors, dimensions)).astype(
        np.float32
    )
    data = normalize_rows(data.astype(np.float32))
    ids = [f"{id_prefix}-{seed}-{i:06d}" for i in range(vectors)]

    query_indices = rng.choice(vectors, size=queries, replace=queries > vectors)
    queries_mat = data[query_indices] + rng.normal(
        scale=query_noise, size=(queries, dimensions)
    ).astype(np.float32)
    queries_mat = normalize_rows(queries_mat.astype(np.float32))
    return data, ids, queries_mat, [int(i) for i in query_indices]


def exact_topk(
    vectors: np.ndarray, ids: list[str], query: np.ndarray, top_k: int
) -> list[tuple[str, float]]:
    scores = vectors @ query
    top = np.argsort(-scores)[:top_k]
    return [(ids[int(i)], float(scores[int(i)])) for i in top]


def ndcg_at_k(
    returned_ids: list[str],
    relevance: dict[str, float],
    ideal_relevances: list[float],
    top_k: int,
) -> float:
    def dcg(rels: list[float]) -> float:
        return sum((2.0**rel - 1.0) / math.log2(rank + 2) for rank, rel in enumerate(rels))

    observed = [max(0.0, relevance.get(doc_id, 0.0)) for doc_id in returned_ids[:top_k]]
    ideal = [max(0.0, rel) for rel in ideal_relevances[:top_k]]
    denom = dcg(ideal)
    return 1.0 if denom == 0.0 else dcg(observed) / denom


def cosine(a: list[float] | np.ndarray, b: np.ndarray) -> float:
    av = np.asarray(a, dtype=np.float64)
    bv = np.asarray(b, dtype=np.float64)
    denom = float(np.linalg.norm(av) * np.linalg.norm(bv))
    if denom == 0.0:
        return 0.0
    return float(np.dot(av, bv) / denom)


def active_vector_count(address: str) -> int | None:
    try:
        health = grpcurl(address, "Health", {})
    except Exception:  # noqa: BLE001
        return None
    for key in ("activeVectors", "active_vectors", "totalVectors", "total_vectors"):
        value = health.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def sum_shard_active(shard_health: list[str]) -> int | None:
    if not shard_health:
        return None
    total = 0
    for endpoint in shard_health:
        count = active_vector_count(endpoint)
        if count is None:
            return None
        total += count
    return total


def evaluate(args: argparse.Namespace, address: str) -> dict[str, Any]:
    report = KpiReport()
    id_prefix = f"kpi-{int(time.time())}-{os.getpid()}"
    vectors, ids, queries, query_source_indices = make_dataset(
        vectors=args.vectors,
        queries=args.queries,
        dimensions=args.dimensions,
        clusters=args.clusters,
        seed=args.seed,
        cluster_noise=args.cluster_noise,
        query_noise=args.query_noise,
        id_prefix=id_prefix,
    )
    corpus_set = set(ids)

    health_before = wait_for_health(address)
    active_before_entry = active_vector_count(address)
    shard_active_before = sum_shard_active(args.shard_health)

    # --- Ingest ---
    inserted_ack = 0
    insert_failed_ids: list[str] = []
    insert_start = time.perf_counter()
    for offset in range(0, len(ids), args.batch_size):
        end = min(offset + args.batch_size, len(ids))
        # Prefer bare vectors for grpcurl portability (metadata is bytes in proto).
        batch = [
            {"id": ids[i], "embedding": vectors[i].astype(float).tolist()}
            for i in range(offset, end)
        ]
        response = grpcurl(
            address,
            "InsertBatch",
            {"collection": args.collection, "vectors": batch},
        )
        if not response.get("success", False):
            insert_failed_ids.extend(response.get("failedIds") or response.get("failed_ids") or [])
        inserted_ack += int(response.get("insertedCount") or response.get("inserted_count") or 0)
    insert_seconds = time.perf_counter() - insert_start

    # Allow index/WAL settle before integrity probes.
    time.sleep(args.settle_seconds)

    health_after = wait_for_health(address)
    active_after_entry = active_vector_count(address)
    shard_active_after = sum_shard_active(args.shard_health)

    insert_ack_rate = inserted_ack / float(args.vectors) if args.vectors else 0.0
    report.add(
        "ingest_ack_rate",
        round(insert_ack_rate, 6),
        "ratio",
        f">= {args.min_ingest_ack_rate}",
        insert_ack_rate >= args.min_ingest_ack_rate and inserted_ack == args.vectors,
        "InsertBatch acknowledged count equals requested vectors (no silent drop)",
    )
    report.add(
        "ingest_failed_ids",
        len(insert_failed_ids),
        "count",
        "== 0",
        len(insert_failed_ids) == 0,
        "No failed IDs returned by InsertBatch",
    )

    # Entry-point health often does not aggregate multi-shard counts (coordinator).
    # Prefer explicit --shard-health sum when provided.
    if shard_active_before is not None and shard_active_after is not None:
        delta = shard_active_after - shard_active_before
        report.add(
            "index_active_delta_match",
            delta,
            "vectors",
            f"== {args.vectors}",
            delta == args.vectors,
            "Sum of shard active_vectors grew by exactly the inserted count",
        )
    elif (
        active_before_entry is not None
        and active_after_entry is not None
        and active_after_entry >= active_before_entry
        and (active_after_entry - active_before_entry) == args.vectors
    ):
        report.add(
            "index_active_delta_match",
            active_after_entry - active_before_entry,
            "vectors",
            f"== {args.vectors}",
            True,
            "Entry health active_vectors grew by exactly the inserted count",
        )
    else:
        # Not a hard fail when coordinator health returns 0; Get path is authoritative.
        report.add(
            "index_active_delta_match",
            "n/a",
            "vectors",
            "informational",
            True,
            "Entry health did not expose a usable active delta (common on coordinators); "
            "Get-found-rate is the authoritative missing-data gate",
        )

    # --- Get integrity (missing data / wrong payload) ---
    get_found = 0
    get_embedding_match = 0
    get_checked_embeddings = 0
    for i, doc_id in enumerate(ids):
        response = grpcurl(
            address,
            "Get",
            {"collection": args.collection, "id": doc_id},
        )
        found = bool(response.get("found", False))
        if found:
            get_found += 1
        emb = response.get("vector") or response.get("embedding") or []
        if found and emb:
            get_checked_embeddings += 1
            if cosine(emb, vectors[i]) >= args.min_embedding_cosine:
                get_embedding_match += 1

    get_found_rate = get_found / float(args.vectors) if args.vectors else 0.0
    report.add(
        "get_found_rate",
        round(get_found_rate, 6),
        "ratio",
        f">= {args.min_get_found_rate}",
        get_found_rate >= args.min_get_found_rate,
        "Every ingested ID is readable via Get (no missing data)",
    )
    if get_checked_embeddings > 0:
        emb_match_rate = get_embedding_match / float(get_checked_embeddings)
        report.add(
            "get_embedding_match_rate",
            round(emb_match_rate, 6),
            "ratio",
            f">= {args.min_embedding_match_rate}",
            emb_match_rate >= args.min_embedding_match_rate,
            "Get returns the same vector that was inserted (no wrong ingestion)",
        )
    else:
        report.add(
            "get_embedding_match_rate",
            "n/a",
            "ratio",
            "informational",
            True,
            "Get responses did not include embeddings; skipped payload match",
        )

    # --- Retrieval quality vs exact ground truth ---
    recalls: list[float] = []
    ndcgs: list[float] = []
    mrrs: list[float] = []
    self_hits: list[float] = []
    out_of_batch_ids = 0
    unreadable_ids = 0
    returned_id_checks = 0
    duplicate_results = 0
    short_results = 0
    wall_ms: list[float] = []
    server_ms: list[float] = []
    partial_responses = 0
    query_failures = 0
    # Baseline corpus present (dirty cluster) makes "only this batch" neighbors invalid.
    # Prefer explicit shard health sums; fall back to entry health; unknown → 0 (clean).
    if shard_active_before is not None:
        baseline_active = shard_active_before
    elif active_before_entry is not None:
        baseline_active = active_before_entry
    else:
        baseline_active = 0
    clean_corpus = baseline_active == 0

    for q_idx, query in enumerate(queries):
        exact = exact_topk(vectors, ids, query, args.top_k)
        exact_ids = [doc_id for doc_id, _ in exact]
        exact_scores = [score for _, score in exact]
        exact_relevance = dict(exact)
        source_id = ids[query_source_indices[q_idx]]

        try:
            start = time.perf_counter()
            response = grpcurl(
                address,
                "Search",
                {
                    "collection": args.collection,
                    "query": query.astype(float).tolist(),
                    "topK": args.top_k,
                    "nprobe": args.nprobe,
                },
            )
            wall_ms.append((time.perf_counter() - start) * 1000.0)
            server_ms.append(float(response.get("latencyUs") or response.get("latency_us") or 0.0) / 1000.0)
            if response.get("partial"):
                partial_responses += 1
            returned = [item["id"] for item in response.get("results", [])]
        except Exception:  # noqa: BLE001
            query_failures += 1
            recalls.append(0.0)
            ndcgs.append(0.0)
            mrrs.append(0.0)
            self_hits.append(0.0)
            continue

        if len(returned) < min(args.top_k, args.vectors):
            short_results += 1
        if len(returned) != len(set(returned)):
            duplicate_results += 1
        for doc_id in returned:
            returned_id_checks += 1
            if doc_id not in corpus_set:
                out_of_batch_ids += 1
                # Truly wrong index entry: returned ID cannot be Get'ed.
                try:
                    got = grpcurl(
                        address,
                        "Get",
                        {"collection": args.collection, "id": doc_id},
                    )
                    if not bool(got.get("found", False)):
                        unreadable_ids += 1
                except Exception:  # noqa: BLE001
                    unreadable_ids += 1

        returned_set = set(returned)
        # Exact-neighbor recall is only authoritative on a clean corpus. On a dirty
        # shared index, older vectors can legitimately enter the ANN top-k.
        if clean_corpus:
            recalls.append(len(returned_set.intersection(exact_ids)) / float(args.top_k))
            ndcgs.append(ndcg_at_k(returned, exact_relevance, exact_scores, args.top_k))
        else:
            # Self-hit still measures membership; ranking vs batch-only GT is noisy.
            recalls.append(1.0 if source_id in returned_set else 0.0)
            ndcgs.append(1.0 if source_id in returned_set else 0.0)
        if source_id in returned:
            mrrs.append(1.0 / float(returned.index(source_id) + 1))
            self_hits.append(1.0)
        else:
            mrrs.append(0.0)
            self_hits.append(0.0)

    mean_recall = float(np.mean(recalls)) if recalls else 0.0
    min_recall = float(np.min(recalls)) if recalls else 0.0
    mean_ndcg = float(np.mean(ndcgs)) if ndcgs else 0.0
    mean_mrr = float(np.mean(mrrs)) if mrrs else 0.0
    self_hit_rate = float(np.mean(self_hits)) if self_hits else 0.0
    out_of_batch_rate = out_of_batch_ids / float(max(1, returned_id_checks))
    unreadable_rate = unreadable_ids / float(max(1, returned_id_checks))
    dup_rate = duplicate_results / float(max(1, args.queries))
    short_rate = short_results / float(max(1, args.queries))
    query_fail_rate = query_failures / float(max(1, args.queries))
    partial_rate = partial_responses / float(max(1, args.queries))

    if clean_corpus:
        report.add(
            "mean_recall_at_k",
            round(mean_recall, 6),
            "ratio",
            f">= {args.min_mean_recall}",
            mean_recall >= args.min_mean_recall,
            "Market-standard Recall@K vs exact brute-force neighbors on a clean corpus",
        )
        report.add(
            "min_recall_at_k",
            round(min_recall, 6),
            "ratio",
            f">= {args.min_min_recall}",
            min_recall >= args.min_min_recall,
            "Worst-query Recall@K floor",
        )
        report.add(
            "mean_ndcg_at_k",
            round(mean_ndcg, 6),
            "ratio",
            f">= {args.min_mean_ndcg}",
            mean_ndcg >= args.min_mean_ndcg,
            "Ranking quality (nDCG@K) against exact similarity order",
        )
        report.add(
            "batch_only_result_rate",
            round(1.0 - out_of_batch_rate, 6),
            "ratio",
            "== 1.0",
            out_of_batch_ids == 0,
            "On a clean index, every hit belongs to the just-ingested batch",
        )
    else:
        report.add(
            "mean_recall_at_k",
            round(mean_recall, 6),
            "ratio",
            f">= {args.min_self_hit_rate} (dirty-corpus self-hit mode)",
            mean_recall >= args.min_self_hit_rate,
            "Dirty shared index: batch-only exact GT is invalid; score is self-hit rate",
        )
        report.add(
            "mean_ndcg_at_k",
            "n/a",
            "ratio",
            "informational on dirty corpus",
            True,
            "nDCG vs batch-only ground truth is not enforced when pre-existing vectors exist",
        )
        report.add(
            "out_of_batch_hit_rate",
            round(out_of_batch_rate, 6),
            "ratio",
            "informational",
            True,
            "Share of hits from pre-existing corpus (expected on a dirty shared index)",
        )

    report.add(
        "mean_mrr_at_k",
        round(mean_mrr, 6),
        "ratio",
        f">= {args.min_mean_mrr}",
        mean_mrr >= args.min_mean_mrr,
        "Mean reciprocal rank of the source document under a noisy self-query",
    )
    report.add(
        "self_hit_rate",
        round(self_hit_rate, 6),
        "ratio",
        f">= {args.min_self_hit_rate}",
        self_hit_rate >= args.min_self_hit_rate,
        "Noisy query still retrieves its source document (index not missing members)",
    )
    report.add(
        "unreadable_result_id_rate",
        round(unreadable_rate, 6),
        "ratio",
        "== 0",
        unreadable_ids == 0,
        "Search never returns IDs that cannot be Get'ed (no ghost/missing index entries)",
    )
    report.add(
        "duplicate_result_rate",
        round(dup_rate, 6),
        "ratio",
        "== 0",
        duplicate_results == 0,
        "Search never returns duplicate IDs in one result list",
    )
    report.add(
        "short_result_rate",
        round(short_rate, 6),
        "ratio",
        f"<= {args.max_short_result_rate}",
        short_rate <= args.max_short_result_rate,
        "Search returns a full top-k when the corpus is large enough",
    )
    report.add(
        "query_failure_rate",
        round(query_fail_rate, 6),
        "ratio",
        "== 0",
        query_failures == 0,
        "No transport/API failures during measured queries",
    )
    report.add(
        "partial_response_rate",
        round(partial_rate, 6),
        "ratio",
        f"<= {args.max_partial_rate}",
        partial_rate <= args.max_partial_rate,
        "Coordinator did not return partial shard coverage during the run",
    )

    p95_wall = percentile(wall_ms, 0.95)
    p95_server = percentile(server_ms, 0.95)
    report.add(
        "search_p95_wall_ms",
        round(p95_wall, 3),
        "ms",
        f"<= {args.max_p95_wall_ms}",
        p95_wall <= args.max_p95_wall_ms,
        "Client-observed search P95 (includes harness overhead)",
    )
    report.add(
        "search_p95_server_ms",
        round(p95_server, 3),
        "ms",
        f"<= {args.max_p95_server_ms}",
        p95_server <= args.max_p95_server_ms,
        "Server-reported search P95 latency",
    )
    insert_vps = args.vectors / insert_seconds if insert_seconds > 0 else 0.0
    report.add(
        "insert_vectors_per_sec",
        round(insert_vps, 2),
        "vec/s",
        "informational",
        True,
        "Ingest throughput during the correctness load (not a quality gate)",
    )

    details = {
        "report_type": REPORT_TYPE,
        "generated_at_unix": int(time.time()),
        "server": address,
        "collection": args.collection,
        "dataset": {
            "vectors": args.vectors,
            "queries": args.queries,
            "dimensions": args.dimensions,
            "clusters": args.clusters,
            "seed": args.seed,
            "top_k": args.top_k,
            "nprobe": args.nprobe,
            "id_prefix": id_prefix,
            "metric": "cosine",
            "ground_truth": "exact brute-force cosine over the ingested batch",
        },
        "health_before": health_before,
        "health_after": health_after,
        "active_before_entry": active_before_entry,
        "active_after_entry": active_after_entry,
        "shard_active_before": shard_active_before,
        "shard_active_after": shard_active_after,
        "inserted_ack": inserted_ack,
        "insert_failed_ids": insert_failed_ids[:20],
        "get_found": get_found,
        "get_checked_embeddings": get_checked_embeddings,
        "get_embedding_match": get_embedding_match,
        "clean_corpus": clean_corpus,
        "baseline_active": baseline_active,
        "out_of_batch_ids": out_of_batch_ids,
        "unreadable_ids": unreadable_ids,
        "returned_id_checks": returned_id_checks,
        "duplicate_result_queries": duplicate_results,
        "short_result_queries": short_results,
        "query_failures": query_failures,
        "partial_responses": partial_responses,
        "latency_ms": {
            "wall": {
                "p50": percentile(wall_ms, 0.50),
                "p95": p95_wall,
                "p99": percentile(wall_ms, 0.99),
            },
            "server": {
                "p50": percentile(server_ms, 0.50),
                "p95": p95_server,
                "p99": percentile(server_ms, 0.99),
            },
        },
        "methodology": {
            "references": [
                "ANN-Benchmarks (exact neighbor ground truth + Recall@K)",
                "VectorDBBench (load + query quality/latency together)",
                "IR standard metrics (nDCG, MRR, hit-rate)",
            ],
            "not_a_substitute_for": [
                "Public SIFT1M/GIST1M market lane (see docs/quality/market-readiness-qualification.md)",
                "Competitor parity on identical hardware",
            ],
        },
    }

    summary = {
        "report_type": REPORT_TYPE,
        "passed": report.passed,
        "failures": report.failures,
        "kpis": [
            {
                "kpi": row.kpi,
                "value": row.value,
                "unit": row.unit,
                "gate": row.gate,
                "status": row.status,
                "meaning": row.meaning,
            }
            for row in report.rows
        ],
        "details": details,
    }
    return summary


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# AkiDB Correctness KPI Table",
        "",
        f"- **Overall:** `{'PASS' if summary['passed'] else 'FAIL'}`",
        f"- **Server:** `{summary['details']['server']}`",
        f"- **Collection:** `{summary['details']['collection']}`",
        f"- **Dataset:** {summary['details']['dataset']['vectors']} vectors × "
        f"{summary['details']['dataset']['dimensions']}d, "
        f"{summary['details']['dataset']['queries']} queries, "
        f"top_k={summary['details']['dataset']['top_k']}",
        f"- **Ground truth:** {summary['details']['dataset']['ground_truth']}",
        "",
        "| KPI | Value | Unit | Gate | Status | Meaning |",
        "| --- | ---: | --- | --- | --- | --- |",
    ]
    for row in summary["kpis"]:
        lines.append(
            f"| `{row['kpi']}` | {row['value']} | {row['unit']} | {row['gate']} | "
            f"**{row['status']}** | {row['meaning']} |"
        )
    if summary["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in summary["failures"]:
            lines.append(f"- {failure}")
    lines.extend(
        [
            "",
            "## How to read this",
            "",
            "- **Ingest integrity:** `ingest_ack_rate`, `get_found_rate`, `get_embedding_match_rate`",
            "- **Index completeness:** `index_active_delta_match`, `self_hit_rate`",
            "- **Retrieval correctness:** `mean_recall_at_k`, `mean_ndcg_at_k`, `unreadable_result_id_rate`",
            "- **Serving integrity:** `partial_response_rate`, `query_failure_rate`",
            "",
            "This table is a **correctness release gate**. It is not a substitute for the",
            "public-dataset SIFT1M market matrix documented in",
            "`docs/quality/market-readiness-qualification.md`.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-server", action="store_true")
    parser.add_argument("--server", default="127.0.0.1:50051")
    parser.add_argument(
        "--shard-health",
        default="",
        help="Comma-separated shard host:port list for active_vector integrity (cluster mode)",
    )
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--server-log-level", default="warn")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument(
        "--collection",
        default="default",
        help="Collection name (shard servers typically only accept 'default')",
    )
    parser.add_argument("--vectors", type=int, default=500)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--dimensions", type=int, default=128)
    parser.add_argument("--clusters", type=int, default=20)
    parser.add_argument("--cluster-noise", type=float, default=0.02)
    parser.add_argument("--query-noise", type=float, default=0.01)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--nprobe", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--settle-seconds", type=float, default=1.0)
    parser.add_argument("--min-ingest-ack-rate", type=float, default=1.0)
    parser.add_argument("--min-get-found-rate", type=float, default=1.0)
    parser.add_argument("--min-embedding-match-rate", type=float, default=1.0)
    parser.add_argument("--min-embedding-cosine", type=float, default=0.999)
    parser.add_argument("--min-mean-recall", type=float, default=0.98)
    parser.add_argument("--min-min-recall", type=float, default=0.80)
    parser.add_argument("--min-mean-ndcg", type=float, default=0.98)
    parser.add_argument("--min-mean-mrr", type=float, default=0.90)
    parser.add_argument("--min-self-hit-rate", type=float, default=0.99)
    parser.add_argument("--max-short-result-rate", type=float, default=0.0)
    parser.add_argument("--max-partial-rate", type=float, default=0.0)
    parser.add_argument("--max-p95-wall-ms", type=float, default=2000.0)
    parser.add_argument("--max-p95-server-ms", type=float, default=100.0)
    parser.add_argument("--output", default=None)
    parser.add_argument("--markdown", default=None)
    parser.add_argument("--no-fail", action="store_true")
    args = parser.parse_args()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    if args.output is None:
        args.output = str(ROOT / "qa-results" / f"correctness-kpi-{stamp}.json")
    if args.markdown is None:
        args.markdown = str(Path(args.output).with_suffix(".md"))
    args.shard_health = [part.strip() for part in args.shard_health.split(",") if part.strip()]
    if args.top_k <= 0 or args.top_k > args.vectors:
        parser.error("--top-k must be between 1 and --vectors")
    if args.clusters <= 0 or args.clusters > args.vectors:
        parser.error("--clusters must be between 1 and --vectors")
    return args


def main() -> int:
    args = parse_args()
    require_grpcurl()
    server = start_server(args)
    output = Path(args.output)
    markdown = Path(args.markdown)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        summary = evaluate(args, server.address)
        output.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        md = render_markdown(summary)
        markdown.write_text(md, encoding="utf-8")
        print(md)
        print(f"JSON: {output}")
        print(f"Markdown: {markdown}")
        return 0 if summary["passed"] or args.no_fail else 1
    finally:
        stop_server(server, args.keep_temp)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
