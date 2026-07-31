#!/usr/bin/env python3
"""AkiDB vector quality QA harness.

This script measures retrieval quality against exact brute-force cosine
ground truth. It is intentionally separate from throughput benchmarks: the
primary pass/fail signal is whether AkiDB returns the right neighbors.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "crates" / "proto" / "proto"
PROTO_FILE = "akidb.proto"


@dataclass
class ManagedServer:
    process: subprocess.Popen[str] | None
    temp_dir: Path | None
    address: str


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


def grpcurl(address: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
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
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        raise RuntimeError(f"grpcurl {method} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def wait_for_health(address: str, timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            health = grpcurl(address, "Health", {})
            if health.get("healthy") and health.get("ready"):
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"AkiDB did not become healthy at {address}")


def write_temp_config(dimensions: int, temp_dir: Path) -> Path:
    template = ROOT / "config" / "standalone.toml"
    content = template.read_text()
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
    config.write_text(content)
    return config


def start_server(args: argparse.Namespace) -> ManagedServer:
    if args.external_server:
        return ManagedServer(None, None, args.server)

    binary = ROOT / "target" / "debug" / "akidb-server"
    if args.build or not binary.exists():
        subprocess.run(["cargo", "build", "-p", "akidb-server"], cwd=ROOT, check=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="akidb-quality."))
    port = args.port or free_port()
    address = f"127.0.0.1:{port}"
    config = write_temp_config(args.dimensions, temp_dir)
    log_path = temp_dir / "akidb-server.log"
    log = log_path.open("w")
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
        raise RuntimeError(f"server failed to start; log: {log_path}")

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


def make_dataset(args: argparse.Namespace) -> tuple[np.ndarray, list[str], np.ndarray, list[str]]:
    rng = np.random.default_rng(args.seed)
    centers = normalize_rows(rng.normal(size=(args.clusters, args.dimensions)).astype(np.float32))
    labels = rng.integers(0, args.clusters, size=args.vectors)
    vectors = centers[labels] + rng.normal(
        scale=args.cluster_noise,
        size=(args.vectors, args.dimensions),
    ).astype(np.float32)
    vectors = normalize_rows(vectors.astype(np.float32))
    ids = [f"qa-{args.seed}-{i:06d}" for i in range(args.vectors)]

    query_indices = rng.choice(args.vectors, size=args.queries, replace=args.queries > args.vectors)
    queries = vectors[query_indices] + rng.normal(
        scale=args.query_noise,
        size=(args.queries, args.dimensions),
    ).astype(np.float32)
    queries = normalize_rows(queries.astype(np.float32))
    expected_ids = [ids[int(i)] for i in query_indices]
    return vectors, ids, queries, expected_ids


def insert_vectors(address: str, collection: str, vectors: np.ndarray, ids: list[str], batch_size: int) -> float:
    start = time.perf_counter()
    for offset in range(0, len(ids), batch_size):
        batch = [
            {"id": ids[i], "embedding": vectors[i].astype(float).tolist()}
            for i in range(offset, min(offset + batch_size, len(ids)))
        ]
        response = grpcurl(address, "InsertBatch", {"collection": collection, "vectors": batch})
        if not response.get("success", False):
            raise RuntimeError(f"InsertBatch failed: {response}")
    return time.perf_counter() - start


def exact_topk(vectors: np.ndarray, ids: list[str], query: np.ndarray, top_k: int) -> list[tuple[str, float]]:
    scores = vectors @ query
    top = np.argsort(-scores)[:top_k]
    return [(ids[int(i)], float(scores[int(i)])) for i in top]


def ndcg_at_k(returned_ids: list[str], relevance: dict[str, float], ideal_relevances: list[float], top_k: int) -> float:
    def dcg(rels: list[float]) -> float:
        return sum((2.0**rel - 1.0) / math.log2(rank + 2) for rank, rel in enumerate(rels))

    observed = [max(0.0, relevance.get(doc_id, 0.0)) for doc_id in returned_ids[:top_k]]
    ideal = [max(0.0, rel) for rel in ideal_relevances[:top_k]]
    denom = dcg(ideal)
    return 1.0 if denom == 0.0 else dcg(observed) / denom


def evaluate(args: argparse.Namespace, address: str) -> dict[str, Any]:
    vectors, ids, queries, expected_ids = make_dataset(args)
    insert_seconds = insert_vectors(address, args.collection, vectors, ids, args.batch_size)

    recalls: list[float] = []
    hit_rates: list[float] = []
    mrrs: list[float] = []
    ndcgs: list[float] = []
    wall_latencies_ms: list[float] = []
    server_latencies_ms: list[float] = []
    failures: list[dict[str, Any]] = []

    for idx, query in enumerate(queries):
        exact = exact_topk(vectors, ids, query, args.top_k)
        exact_ids = [doc_id for doc_id, _score in exact]
        exact_scores = [score for _doc_id, score in exact]
        exact_relevance = dict(exact)

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
        wall_latencies_ms.append((time.perf_counter() - start) * 1000.0)
        server_latencies_ms.append(float(response.get("latencyUs", 0.0)) / 1000.0)

        returned_ids = [item["id"] for item in response.get("results", [])]
        returned_set = set(returned_ids)
        recall = len(returned_set.intersection(exact_ids)) / float(args.top_k)
        recalls.append(recall)

        expected_id = expected_ids[idx]
        hit_rates.append(1.0 if expected_id in returned_set else 0.0)
        if expected_id in returned_ids:
            mrrs.append(1.0 / float(returned_ids.index(expected_id) + 1))
        else:
            mrrs.append(0.0)
        ndcgs.append(ndcg_at_k(returned_ids, exact_relevance, exact_scores, args.top_k))

        if recall < args.per_query_min_recall:
            failures.append(
                {
                    "query_index": idx,
                    "recall": recall,
                    "expected_top": exact_ids[: args.top_k],
                    "returned": returned_ids,
                }
            )

    summary = {
        "dataset": {
            "vectors": args.vectors,
            "queries": args.queries,
            "dimensions": args.dimensions,
            "clusters": args.clusters,
            "seed": args.seed,
            "top_k": args.top_k,
        },
        "thresholds": {
            "min_mean_recall_at_k": args.min_mean_recall,
            "min_mean_ndcg_at_k": args.min_mean_ndcg,
            "min_hit_rate_at_k": args.min_hit_rate,
            "max_p95_wall_latency_ms": args.max_p95_latency_ms,
            "max_p95_server_latency_ms": args.max_server_p95_latency_ms,
        },
        "results": {
            "mean_recall_at_k": float(np.mean(recalls)),
            "min_recall_at_k": float(np.min(recalls)),
            "mean_ndcg_at_k": float(np.mean(ndcgs)),
            "mean_mrr_at_k": float(np.mean(mrrs)),
            "hit_rate_at_k": float(np.mean(hit_rates)),
            "insert_seconds": insert_seconds,
            "insert_vectors_per_second": args.vectors / insert_seconds if insert_seconds > 0 else 0.0,
            "wall_latency_ms": {
                "p50": percentile(wall_latencies_ms, 0.50),
                "p95": percentile(wall_latencies_ms, 0.95),
                "p99": percentile(wall_latencies_ms, 0.99),
            },
            "server_latency_ms": {
                "p50": percentile(server_latencies_ms, 0.50),
                "p95": percentile(server_latencies_ms, 0.95),
                "p99": percentile(server_latencies_ms, 0.99),
            },
            "low_recall_queries": failures[:10],
            "low_recall_query_count": len(failures),
        },
    }

    passed = (
        summary["results"]["mean_recall_at_k"] >= args.min_mean_recall
        and summary["results"]["mean_ndcg_at_k"] >= args.min_mean_ndcg
        and summary["results"]["hit_rate_at_k"] >= args.min_hit_rate
        and summary["results"]["wall_latency_ms"]["p95"] <= args.max_p95_latency_ms
        and summary["results"]["server_latency_ms"]["p95"] <= args.max_server_p95_latency_ms
    )
    summary["passed"] = bool(passed)
    return summary


def print_summary(summary: dict[str, Any], output: Path) -> None:
    results = summary["results"]
    print("=== AkiDB Vector Quality QA ===")
    print(f"passed: {summary['passed']}")
    print(f"mean recall@k: {results['mean_recall_at_k']:.4f}")
    print(f"min recall@k:  {results['min_recall_at_k']:.4f}")
    print(f"mean nDCG@k:   {results['mean_ndcg_at_k']:.4f}")
    print(f"mean MRR@k:    {results['mean_mrr_at_k']:.4f}")
    print(f"hit rate@k:    {results['hit_rate_at_k']:.4f}")
    print(f"insert rate:   {results['insert_vectors_per_second']:.1f} vectors/sec")
    print(f"wall p95:      {results['wall_latency_ms']['p95']:.2f} ms")
    print(f"server p95:    {results['server_latency_ms']['p95']:.2f} ms")
    print(f"result file:   {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-server", action="store_true", help="Use --server instead of starting standalone")
    parser.add_argument("--server", default="127.0.0.1:50051")
    parser.add_argument("--port", type=int, default=0, help="Standalone server port; 0 chooses a free port")
    parser.add_argument("--server-log-level", default="warn")
    parser.add_argument("--build", action="store_true", help="Build akidb-server before running")
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
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-mean-recall", type=float, default=0.98)
    parser.add_argument("--per-query-min-recall", type=float, default=0.80)
    parser.add_argument("--min-mean-ndcg", type=float, default=0.98)
    parser.add_argument("--min-hit-rate", type=float, default=0.99)
    parser.add_argument("--max-p95-latency-ms", type=float, default=2000.0)
    parser.add_argument("--max-server-p95-latency-ms", type=float, default=50.0)
    parser.add_argument("--output", default=None)
    parser.add_argument("--no-fail", action="store_true", help="Always exit 0 after writing results")
    args = parser.parse_args()
    if args.output is None:
        args.output = str(ROOT / "qa-results" / f"vector-quality-{int(time.time())}.json")
    if args.top_k <= 0 or args.top_k > args.vectors:
        parser.error("--top-k must be between 1 and --vectors")
    if args.clusters <= 0 or args.clusters > args.vectors:
        parser.error("--clusters must be between 1 and --vectors")
    return args


def main() -> int:
    args = parse_args()
    if shutil.which("grpcurl") is None:
        raise RuntimeError("grpcurl is required; install with `brew install grpcurl`")

    server = start_server(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        summary = evaluate(args, server.address)
        output.write_text(json.dumps(summary, indent=2, sort_keys=True))
        print_summary(summary, output)
        return 0 if summary["passed"] or args.no_fail else 1
    finally:
        stop_server(server, args.keep_temp)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
