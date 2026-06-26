#!/usr/bin/env python3
"""End-to-end semantic retrieval QA for AkiDB.

This BEIR/MTEB-style smoke gate uses a small labeled corpus. It embeds documents
with the configured local embedding endpoint, inserts those vectors into AkiDB,
then evaluates TextSearch results against qrels.
"""

from __future__ import annotations

import argparse
import base64
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
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "crates" / "grpc-server" / "proto"
PROTO_FILE = "akidb.proto"


CORPUS: list[dict[str, str]] = [
    {
        "id": "doc-rust-memory",
        "text": "Rust prevents memory bugs with ownership, borrowing, lifetimes, and compile-time safety checks.",
    },
    {
        "id": "doc-postgres-sql",
        "text": "PostgreSQL is a relational SQL database with transactions, indexes, schemas, and durable tables.",
    },
    {
        "id": "doc-vector-search",
        "text": "Vector databases store embeddings and retrieve nearest neighbors with cosine or dot-product similarity.",
    },
    {
        "id": "doc-apple-metal",
        "text": "Apple Silicon machine learning uses Metal and MLX to accelerate tensor operations on Mac GPUs.",
    },
    {
        "id": "doc-qwen-embedding",
        "text": "Qwen embedding models convert natural language text into dense vectors for semantic search.",
    },
    {
        "id": "doc-kubernetes",
        "text": "Kubernetes schedules containers across a cluster and manages deployments, services, and rolling updates.",
    },
    {
        "id": "doc-backup-restore",
        "text": "Backup and restore plans protect data after disk failure by using snapshots, replication, and recovery tests.",
    },
    {
        "id": "doc-observability",
        "text": "Observability tracks production health with metrics, traces, logs, latency percentiles, and error rates.",
    },
    {
        "id": "doc-security",
        "text": "Security hardening includes secret management, TLS, access control, auditing, and least privilege.",
    },
    {
        "id": "doc-basketball",
        "text": "Basketball teams score by shooting, defending, rebounding, passing, and running fast breaks.",
    },
    {
        "id": "doc-cooking",
        "text": "Cooking pasta requires boiling salted water, timing the noodles, and finishing sauce in a pan.",
    },
    {
        "id": "doc-gardening",
        "text": "Garden plants need soil, sunlight, pruning, compost, and careful watering through the growing season.",
    },
]


QUERIES: list[dict[str, Any]] = [
    {
        "id": "q-rust",
        "text": "How does a systems language avoid memory safety bugs?",
        "relevant": {"doc-rust-memory": 3},
    },
    {
        "id": "q-sql",
        "text": "Which database stores relational tables and supports SQL transactions?",
        "relevant": {"doc-postgres-sql": 3},
    },
    {
        "id": "q-vector",
        "text": "How do vector databases find semantically similar items?",
        "relevant": {"doc-vector-search": 3, "doc-qwen-embedding": 1},
    },
    {
        "id": "q-metal",
        "text": "What accelerates machine learning on Apple Silicon Macs?",
        "relevant": {"doc-apple-metal": 3},
    },
    {
        "id": "q-embedding",
        "text": "Which model turns text into dense vectors for retrieval?",
        "relevant": {"doc-qwen-embedding": 3, "doc-vector-search": 1},
    },
    {
        "id": "q-containers",
        "text": "How can I run and roll out containers across a cluster?",
        "relevant": {"doc-kubernetes": 3},
    },
    {
        "id": "q-backup",
        "text": "How should a service recover data after disk failure?",
        "relevant": {"doc-backup-restore": 3},
    },
    {
        "id": "q-monitoring",
        "text": "How do I monitor production latency and error rates?",
        "relevant": {"doc-observability": 3},
    },
    {
        "id": "q-secrets",
        "text": "What protects secrets and limits user permissions?",
        "relevant": {"doc-security": 3},
    },
]


@dataclass
class ProcessHandle:
    process: subprocess.Popen[str] | None
    name: str


@dataclass
class Stack:
    server: ProcessHandle
    sidecar: ProcessHandle
    temp_dir: Path | None
    grpc_address: str
    embedding_url: str


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
        cwd=ROOT,
    )
    if result.returncode != 0:
        raise RuntimeError(f"grpcurl {method} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def http_json(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if payload is None:
        request = urllib.request.Request(url)
    else:
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_http(url: str, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            http_json(url)
            return
        except Exception:
            time.sleep(0.5)
    raise TimeoutError(f"HTTP service did not become ready: {url}")


def wait_for_grpc(address: str, timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            health = grpcurl(address, "Health", {})
            if health.get("healthy") and health.get("ready"):
                return
        except Exception:
            time.sleep(0.5)
    raise TimeoutError(f"AkiDB did not become ready: {address}")


def write_config(temp_dir: Path, dimensions: int, embedding_url: str, model: str) -> Path:
    content = (ROOT / "config" / "standalone.toml").read_text()
    content = content.replace(
        'rocksdb_path = "./data/rocksdb"',
        f'rocksdb_path = "{temp_dir / "rocksdb"}"',
    )
    content = content.replace(
        'wal_path = "./data/wal"',
        f'wal_path = "{temp_dir / "wal"}"',
    )
    content = content.replace('url = "http://127.0.0.1:8081/v1/embeddings"', f'url = "{embedding_url}"')
    content = content.replace('model = "Qwen/Qwen3-Embedding-4B"', f'model = "{model}"')
    content = content.replace("dimensions = 2560", f"dimensions = {dimensions}")
    content = re.sub(r"(\[embedding\]\s*)enabled = false", r"\1enabled = true", content, count=1)
    path = temp_dir / "standalone.toml"
    path.write_text(content)
    return path


def start_stack(args: argparse.Namespace) -> Stack:
    if args.external_stack:
        return Stack(
            server=ProcessHandle(None, "akidb-server"),
            sidecar=ProcessHandle(None, "embedding-sidecar"),
            temp_dir=None,
            grpc_address=args.server,
            embedding_url=args.embedding_url,
        )

    if not args.model_dir:
        raise RuntimeError("--model-dir or AX_ENGINE_MODEL_DIR is required unless --external-stack is used")
    model_dir = Path(args.model_dir).expanduser().resolve()
    if not (model_dir / "model-manifest.json").is_file():
        raise RuntimeError(f"{model_dir} does not contain model-manifest.json")

    binary = ROOT / "target" / "debug" / "akidb-server"
    if args.build or not binary.exists():
        subprocess.run(["cargo", "build", "-p", "akidb-server"], cwd=ROOT, check=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="akidb-text-qa."))
    embedding_port = args.embedding_port or free_port()
    grpc_port = args.port or free_port()
    embedding_url = f"http://127.0.0.1:{embedding_port}/v1/embeddings"
    grpc_address = f"127.0.0.1:{grpc_port}"

    sidecar_log = (temp_dir / "embedding-sidecar.log").open("w")
    sidecar = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts" / "ax_engine_embedding_server.py"),
            "--model-dir",
            str(model_dir),
            "--model-id",
            args.model,
            "--port",
            str(embedding_port),
        ],
        cwd=ROOT,
        stdout=sidecar_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_http(f"http://127.0.0.1:{embedding_port}/health")
    except Exception:
        sidecar.terminate()
        raise RuntimeError(f"embedding sidecar failed to start; log: {temp_dir / 'embedding-sidecar.log'}")

    config = write_config(temp_dir, args.dimensions, embedding_url, args.model)
    server_log = (temp_dir / "akidb-server.log").open("w")
    server = subprocess.Popen(
        [
            str(binary),
            "--config",
            str(config),
            "--standalone",
            "--listen",
            grpc_address,
            "--log-level",
            args.server_log_level,
        ],
        cwd=ROOT,
        stdout=server_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_grpc(grpc_address)
    except Exception:
        server.terminate()
        sidecar.terminate()
        raise RuntimeError(f"akidb-server failed to start; log: {temp_dir / 'akidb-server.log'}")

    return Stack(
        server=ProcessHandle(server, "akidb-server"),
        sidecar=ProcessHandle(sidecar, "embedding-sidecar"),
        temp_dir=temp_dir,
        grpc_address=grpc_address,
        embedding_url=embedding_url,
    )


def stop_stack(stack: Stack, keep_temp: bool) -> None:
    for handle in [stack.server, stack.sidecar]:
        if handle.process is None:
            continue
        handle.process.send_signal(signal.SIGTERM)
        try:
            handle.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            handle.process.kill()
            handle.process.wait(timeout=5)
    if stack.temp_dir is not None and not keep_temp:
        shutil.rmtree(stack.temp_dir, ignore_errors=True)


def embed_texts(embedding_url: str, model: str, texts: list[str]) -> list[list[float]]:
    response = http_json(embedding_url, {"model": model, "input": texts})
    data = sorted(response["data"], key=lambda item: item["index"])
    return [item["embedding"] for item in data]


def insert_corpus(stack: Stack, args: argparse.Namespace) -> float:
    start = time.perf_counter()
    texts = [doc["text"] for doc in CORPUS]
    embeddings = embed_texts(stack.embedding_url, args.model, texts)
    vectors = []
    for doc, embedding in zip(CORPUS, embeddings, strict=True):
        metadata = json.dumps({"text": doc["text"]}).encode("utf-8")
        vectors.append({
            "id": doc["id"],
            "embedding": embedding,
            "metadata": base64.b64encode(metadata).decode("ascii"),
        })
    response = grpcurl(stack.grpc_address, "InsertBatch", {"collection": args.collection, "vectors": vectors})
    if not response.get("success", False):
        raise RuntimeError(f"InsertBatch failed: {response}")
    return time.perf_counter() - start


def dcg(rels: list[float]) -> float:
    return sum((2.0**rel - 1.0) / math.log2(rank + 2) for rank, rel in enumerate(rels))


def score_query(returned_ids: list[str], relevant: dict[str, int], top_k: int) -> dict[str, float]:
    top = returned_ids[:top_k]
    hits = [doc_id for doc_id in top if doc_id in relevant]
    recall = len(hits) / float(len(relevant))
    mrr = 0.0
    for rank, doc_id in enumerate(top, start=1):
        if doc_id in relevant:
            mrr = 1.0 / float(rank)
            break
    observed = [float(relevant.get(doc_id, 0)) for doc_id in top]
    ideal = sorted((float(score) for score in relevant.values()), reverse=True)[:top_k]
    ndcg = 1.0 if not ideal else dcg(observed) / dcg(ideal)
    return {"recall": recall, "mrr": mrr, "ndcg": ndcg}


def evaluate(stack: Stack, args: argparse.Namespace) -> dict[str, Any]:
    insert_seconds = insert_corpus(stack, args)
    per_query: list[dict[str, Any]] = []
    latencies_ms: list[float] = []
    server_latencies_ms: list[float] = []

    for query in QUERIES:
        start = time.perf_counter()
        response = grpcurl(
            stack.grpc_address,
            "TextSearch",
            {"collection": args.collection, "text": query["text"], "topK": args.top_k},
        )
        latencies_ms.append((time.perf_counter() - start) * 1000.0)
        server_latencies_ms.append(float(response.get("latencyUs", 0.0)) / 1000.0)
        returned_ids = [item["id"] for item in response.get("results", [])]
        metrics = score_query(returned_ids, query["relevant"], args.top_k)
        per_query.append(
            {
                "id": query["id"],
                "text": query["text"],
                "returned": returned_ids,
                "relevant": query["relevant"],
                **metrics,
            }
        )

    mean_recall = sum(item["recall"] for item in per_query) / len(per_query)
    mean_mrr = sum(item["mrr"] for item in per_query) / len(per_query)
    mean_ndcg = sum(item["ndcg"] for item in per_query) / len(per_query)
    hit_rate = sum(1.0 if item["mrr"] > 0.0 else 0.0 for item in per_query) / len(per_query)
    summary = {
        "dataset": {
            "corpus_documents": len(CORPUS),
            "queries": len(QUERIES),
            "top_k": args.top_k,
            "model": args.model,
            "dimensions": args.dimensions,
        },
        "thresholds": {
            "min_mean_recall": args.min_mean_recall,
            "min_mean_ndcg": args.min_mean_ndcg,
            "min_mean_mrr": args.min_mean_mrr,
            "min_hit_rate": args.min_hit_rate,
            "max_p95_latency_ms": args.max_p95_latency_ms,
        },
        "results": {
            "mean_recall_at_k": mean_recall,
            "mean_ndcg_at_k": mean_ndcg,
            "mean_mrr_at_k": mean_mrr,
            "hit_rate_at_k": hit_rate,
            "insert_seconds": insert_seconds,
            "wall_latency_ms": {
                "p50": percentile(latencies_ms, 0.50),
                "p95": percentile(latencies_ms, 0.95),
                "p99": percentile(latencies_ms, 0.99),
            },
            "server_latency_ms": {
                "p50": percentile(server_latencies_ms, 0.50),
                "p95": percentile(server_latencies_ms, 0.95),
                "p99": percentile(server_latencies_ms, 0.99),
            },
            "per_query": per_query,
        },
    }
    summary["passed"] = bool(
        mean_recall >= args.min_mean_recall
        and mean_ndcg >= args.min_mean_ndcg
        and mean_mrr >= args.min_mean_mrr
        and hit_rate >= args.min_hit_rate
        and summary["results"]["wall_latency_ms"]["p95"] <= args.max_p95_latency_ms
    )
    return summary


def print_summary(summary: dict[str, Any], output: Path) -> None:
    results = summary["results"]
    print("=== AkiDB Text Retrieval QA ===")
    print(f"passed: {summary['passed']}")
    print(f"mean recall@k: {results['mean_recall_at_k']:.4f}")
    print(f"mean nDCG@k:   {results['mean_ndcg_at_k']:.4f}")
    print(f"mean MRR@k:    {results['mean_mrr_at_k']:.4f}")
    print(f"hit rate@k:    {results['hit_rate_at_k']:.4f}")
    print(f"wall p95:      {results['wall_latency_ms']['p95']:.2f} ms")
    print(f"server p95:    {results['server_latency_ms']['p95']:.2f} ms")
    print(f"result file:   {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-stack", action="store_true", help="Use existing AkiDB and embedding endpoint")
    parser.add_argument("--server", default="127.0.0.1:50051")
    parser.add_argument("--embedding-url", default="http://127.0.0.1:8081/v1/embeddings")
    parser.add_argument("--model-dir", default=os.environ.get("AX_ENGINE_MODEL_DIR", ""))
    parser.add_argument("--model", default=os.environ.get("AX_ENGINE_MODEL", "Qwen/Qwen3-Embedding-4B"))
    parser.add_argument("--dimensions", type=int, default=int(os.environ.get("EMBEDDING_DIMENSIONS", "2560")))
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--embedding-port", type=int, default=0)
    parser.add_argument("--server-log-level", default="warn")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--collection", default=None)
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--min-mean-recall", type=float, default=0.85)
    parser.add_argument("--min-mean-ndcg", type=float, default=0.75)
    parser.add_argument("--min-mean-mrr", type=float, default=0.75)
    parser.add_argument("--min-hit-rate", type=float, default=0.85)
    parser.add_argument("--max-p95-latency-ms", type=float, default=2000.0)
    parser.add_argument("--output", default=None)
    parser.add_argument("--no-fail", action="store_true")
    args = parser.parse_args()
    if args.collection is None:
        args.collection = f"text-qa-{int(time.time())}-{os.getpid()}"
    if args.output is None:
        args.output = str(ROOT / "qa-results" / f"text-retrieval-{int(time.time())}.json")
    return args


def main() -> int:
    args = parse_args()
    if shutil.which("grpcurl") is None:
        raise RuntimeError("grpcurl is required; install with `brew install grpcurl`")
    stack = start_stack(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        summary = evaluate(stack, args)
        output.write_text(json.dumps(summary, indent=2, sort_keys=True))
        print_summary(summary, output)
        return 0 if summary["passed"] or args.no_fail else 1
    finally:
        stop_stack(stack, args.keep_temp)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
