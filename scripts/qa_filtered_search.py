#!/usr/bin/env python3
"""Filtered search quality gate (GAP-004 / RET-104).

Generates clustered vectors with a categorical `bucket` tag, runs exact
brute-force cosine ground truth restricted to the filter, and validates
either:

  * offline harness construction (--dry-run), or
  * live AkiDB Search with legacy JSON filter and/or typed TagFilter.

Usage:
  python3 scripts/qa_filtered_search.py --dry-run
  python3 scripts/qa_filtered_search.py --external-server --server 127.0.0.1:50051
  python3 scripts/qa_filtered_search.py --build
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "crates" / "proto" / "proto"
PROTO_FILE = "akidb.proto"


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def gen_clustered(
    n: int, dim: int, buckets: int, seed: int
) -> list[tuple[str, list[float], dict[str, Any]]]:
    rng = random.Random(seed)
    centers = [[rng.gauss(0, 1) for _ in range(dim)] for _ in range(buckets)]
    out: list[tuple[str, list[float], dict[str, Any]]] = []
    for i in range(n):
        b = i % buckets
        vec = [centers[b][d] + rng.gauss(0, 0.05) for d in range(dim)]
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        vec = [v / norm for v in vec]
        meta = {"bucket": str(b), "workspace_id": "default"}
        out.append((f"filt-{seed}-{i:04d}", vec, meta))
    return out


def brute_force_topk(
    query: list[float],
    corpus: list[tuple[str, list[float], dict[str, Any]]],
    top_k: int,
    bucket: str | None,
) -> list[str]:
    scored = []
    for vid, vec, meta in corpus:
        if bucket is not None and meta.get("bucket") != bucket:
            continue
        scored.append((cosine(query, vec), vid))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [vid for _, vid in scored[:top_k]]


def recall_at_k(expected: list[str], got: list[str]) -> float:
    if not expected:
        return 1.0
    exp = set(expected)
    hit = sum(1 for g in got if g in exp)
    return hit / len(expected)


def b64_json(obj: dict[str, Any]) -> str:
    return base64.b64encode(
        json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
        raise RuntimeError(f"{method} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def wait_health(address: str, timeout_s: float = 45.0) -> None:
    deadline = time.time() + timeout_s
    last = ""
    while time.time() < deadline:
        try:
            health = grpcurl(address, "Health", {})
            if health.get("healthy") and health.get("ready"):
                return
            last = str(health)
        except Exception as error:  # noqa: BLE001
            last = str(error)
        time.sleep(0.3)
    raise TimeoutError(f"not healthy: {last}")


def start_server(args: argparse.Namespace) -> tuple[subprocess.Popen[str] | None, Path | None, str]:
    if args.external_server:
        wait_health(args.server)
        return None, None, args.server

    if shutil.which("grpcurl") is None:
        raise RuntimeError("grpcurl is required for live mode")

    binary = ROOT / "target" / "debug" / "akidb-server"
    if args.build or not binary.exists():
        subprocess.run(["cargo", "build", "-p", "akidb-server"], cwd=ROOT, check=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="akidb-filtered-qa."))
    port = args.port or free_port()
    address = f"127.0.0.1:{port}"
    template = (ROOT / "config" / "standalone.toml").read_text(encoding="utf-8")
    content = template.replace('rocksdb_path = "./data/rocksdb"', f'rocksdb_path = "{temp_dir / "rocksdb"}"')
    content = content.replace('wal_path = "./data/wal"', f'wal_path = "{temp_dir / "wal"}"')
    content = content.replace("dimensions = 2560", f"dimensions = {args.dimensions}")
    content = re.sub(
        r"(\[embedding\]\s*)enabled = true",
        r"\1enabled = false",
        content,
        count=1,
    )
    config = temp_dir / "standalone.toml"
    config.write_text(content, encoding="utf-8")
    log = (temp_dir / "server.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            str(binary),
            "--config",
            str(config),
            "--standalone",
            "--listen",
            address,
            "--log-level",
            "warn",
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_health(address)
    except Exception:
        process.terminate()
        process.wait(timeout=5)
        raise
    return process, temp_dir, address


def stop_server(process: subprocess.Popen[str] | None, temp_dir: Path | None) -> None:
    if process is not None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    if temp_dir is not None:
        shutil.rmtree(temp_dir, ignore_errors=True)


def live_search(
    address: str,
    collection: str,
    query: list[float],
    top_k: int,
    bucket: str,
    mode: str,
    nprobe: int,
) -> list[str]:
    payload: dict[str, Any] = {
        "collection": collection,
        "query": query,
        "topK": top_k,
        "nprobe": nprobe,
    }
    if mode == "legacy":
        payload["filter"] = b64_json({"bucket": bucket})
    elif mode == "tag":
        payload["tagFilter"] = {
            "condition": {
                "key": "bucket",
                "value": {"text": bucket},
                "op": "TAG_OP_EQ",
            }
        }
    else:
        raise ValueError(mode)
    response = grpcurl(address, "Search", payload)
    return [item["id"] for item in response.get("results", []) if item.get("id")]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vectors", type=int, default=200)
    parser.add_argument("--dimensions", type=int, default=32)
    parser.add_argument("--buckets", type=int, default=10)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--nprobe", type=int, default=32)
    parser.add_argument("--collection", default="default")
    parser.add_argument("--min-recall", type=float, default=0.85)
    parser.add_argument("--max-filter-violation-rate", type=float, default=0.0)
    parser.add_argument("--output", type=Path, default=Path("qa-results/filtered-search-quality.json"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--external-server", action="store_true")
    parser.add_argument("--server", default="127.0.0.1:50051")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument(
        "--filter-mode",
        choices=("legacy", "tag", "both"),
        default="both",
        help="Which live filter encoding to exercise",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.external_server and not args.build:
        # Allow running against existing debug binary without --build
        if not (ROOT / "target" / "debug" / "akidb-server").exists():
            parser.error("provide --dry-run, --external-server, or --build")

    corpus = gen_clustered(args.vectors, args.dimensions, args.buckets, args.seed)
    selectivity = 1.0 / args.buckets
    rng = random.Random(args.seed + 1)

    process = None
    temp_dir = None
    address = ""
    modes: list[str]
    if args.dry_run:
        modes = ["dry_run"]
    else:
        if shutil.which("grpcurl") is None:
            print("grpcurl is required for live filtered search", file=sys.stderr)
            return 2
        process, temp_dir, address = start_server(args)
        # Load corpus
        for vid, vec, meta in corpus:
            grpcurl(
                address,
                "Insert",
                {
                    "collection": args.collection,
                    "id": vid,
                    "vector": vec,
                    "metadata": b64_json(meta),
                },
            )
        time.sleep(0.5)
        modes = ["legacy", "tag"] if args.filter_mode == "both" else [args.filter_mode]

    try:
        mode_reports: dict[str, Any] = {}
        overall_pass = True
        for mode in modes:
            recalls: list[float] = []
            violations = 0
            total_hits = 0
            for qi in range(args.queries):
                target_bucket = str(qi % args.buckets)
                candidates = [c for c in corpus if c[2]["bucket"] == target_bucket]
                _qid, qvec, _ = rng.choice(candidates)
                expected = brute_force_topk(qvec, corpus, args.top_k, target_bucket)
                if mode == "dry_run":
                    got = expected
                else:
                    got = live_search(
                        address,
                        args.collection,
                        qvec,
                        args.top_k,
                        target_bucket,
                        mode,
                        args.nprobe,
                    )
                recalls.append(recall_at_k(expected, got))
                allowed = {c[0] for c in corpus if c[2]["bucket"] == target_bucket}
                for doc in got:
                    total_hits += 1
                    if doc not in allowed:
                        violations += 1
            mean_recall = sum(recalls) / len(recalls) if recalls else 0.0
            violation_rate = violations / float(total_hits) if total_hits else 0.0
            passed = (
                mean_recall >= args.min_recall
                and violation_rate <= args.max_filter_violation_rate
            )
            overall_pass = overall_pass and passed
            mode_reports[mode] = {
                "mean_recall_at_k": mean_recall,
                "filter_violation_rate": violation_rate,
                "violations": violations,
                "total_hits": total_hits,
                "passed": passed,
            }

        artifact = {
            "gate": "filtered_search",
            "vectors": args.vectors,
            "dimensions": args.dimensions,
            "buckets": args.buckets,
            "selectivity": selectivity,
            "queries": args.queries,
            "top_k": args.top_k,
            "min_recall_gate": args.min_recall,
            "modes": mode_reports,
            "passed": overall_pass,
            "server": address or "dry-run",
            "note": (
                "Offline mode validates ground-truth construction only. "
                "Live mode inserts tagged vectors and asserts filter purity + recall."
            ),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(artifact, indent=2))
        return 0 if overall_pass else 1
    finally:
        stop_server(process, temp_dir)


if __name__ == "__main__":
    sys.exit(main())
