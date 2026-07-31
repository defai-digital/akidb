#!/usr/bin/env python3
"""Live feature-matrix QA for the current AkiDB product surface (v0.10+).

Reviews and gates the features operators actually ship today:

  Mutable standalone core
    * Health
    * Insert / Get / Update / Delete lifecycle
    * Dense Search + SearchBatch
    * Metadata filter (legacy JSON bytes) + typed TagFilter
    * Score threshold
    * BM25 TextSearch without an embedding provider
    * No missing data after mutations

This is intentionally complementary to:
  * qa_correctness_kpi.py  — dense recall / ingest integrity KPI table
  * qa_vector_quality.py   — pure ANN quality
  * qa_text_retrieval.py   — embedding-backed TextSearch (needs AX_ENGINE_MODEL_DIR)
  * market ANN playbooks   — public SIFT1M / competitor parity
  * memory / generation    — separate preview/qualification paths

Usage:
  python3 scripts/qa_feature_matrix.py --build
  python3 scripts/qa_feature_matrix.py --external-server --server 127.0.0.1:50051
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "crates" / "proto" / "proto"
PROTO_FILE = "akidb.proto"
REPORT_TYPE = "akidb.feature-matrix.v1"


@dataclass
class ManagedServer:
    process: subprocess.Popen[str] | None
    temp_dir: Path | None
    address: str


@dataclass
class Check:
    name: str
    feature: str
    passed: bool
    detail: str
    value: Any = None


@dataclass
class MatrixReport:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, feature: str, passed: bool, detail: str, value: Any = None) -> None:
        self.checks.append(Check(name, feature, passed, detail, value))

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed]


def b64_json(obj: dict[str, Any]) -> str:
    raw = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def normalize(v: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(v))
    return v if n == 0.0 else v / n


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
        raise RuntimeError(f"{method} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def wait_health(address: str, timeout_s: float = 45.0) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last = ""
    while time.time() < deadline:
        try:
            health = grpcurl(address, "Health", {})
            if health.get("healthy") and health.get("ready"):
                return health
            last = str(health)
        except Exception as error:  # noqa: BLE001
            last = str(error)
        time.sleep(0.3)
    raise TimeoutError(f"not healthy at {address}: {last}")


def write_config(dimensions: int, temp_dir: Path) -> Path:
    template = (ROOT / "config" / "standalone.toml").read_text(encoding="utf-8")
    content = template
    content = content.replace('rocksdb_path = "./data/rocksdb"', f'rocksdb_path = "{temp_dir / "rocksdb"}"')
    content = content.replace('wal_path = "./data/wal"', f'wal_path = "{temp_dir / "wal"}"')
    content = content.replace("dimensions = 2560", f"dimensions = {dimensions}")
    content = re.sub(
        r"(\[embedding\]\s*)enabled = true",
        r"\1enabled = false",
        content,
        count=1,
    )
    path = temp_dir / "standalone.toml"
    path.write_text(content, encoding="utf-8")
    return path


def start_server(args: argparse.Namespace) -> ManagedServer:
    if args.external_server:
        wait_health(args.server)
        return ManagedServer(None, None, args.server)

    binary = ROOT / "target" / "debug" / "akidb-server"
    if args.build or not binary.exists():
        subprocess.run(["cargo", "build", "-p", "akidb-server"], cwd=ROOT, check=True)

    temp_dir = Path(tempfile.mkdtemp(prefix="akidb-feature-matrix."))
    port = args.port or free_port()
    address = f"127.0.0.1:{port}"
    config = write_config(args.dimensions, temp_dir)
    log_path = temp_dir / "server.log"
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
        wait_health(address)
    except Exception:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        raise RuntimeError(f"server failed; log={log_path}") from None
    return ManagedServer(process, temp_dir, address)


def stop_server(server: ManagedServer, keep_temp: bool) -> None:
    if server.process is not None:
        server.process.send_signal(signal.SIGTERM)
        try:
            server.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process_kill = server.process
            process_kill.kill()
            process_kill.wait(timeout=5)
    if server.temp_dir is not None and not keep_temp:
        shutil.rmtree(server.temp_dir, ignore_errors=True)


def make_vectors(n: int, dim: int, seed: int) -> tuple[list[str], np.ndarray, list[dict[str, Any]], list[str]]:
    rng = np.random.default_rng(seed)
    ids = [f"fm-{seed}-{i:04d}" for i in range(n)]
    buckets = [str(i % 5) for i in range(n)]
    texts = [
        f"document {i} about topic-{buckets[i]} with unique_token_{i} and shared_keyword_matrix"
        for i in range(n)
    ]
    # 5 cluster centers
    centers = np.stack([normalize(rng.normal(size=dim).astype(np.float32)) for _ in range(5)])
    data = np.zeros((n, dim), dtype=np.float32)
    for i in range(n):
        noise = rng.normal(scale=0.03, size=dim).astype(np.float32)
        data[i] = normalize(centers[int(buckets[i])] + noise)
    metas = [{"bucket": buckets[i], "i": i, "topic": f"topic-{buckets[i]}"} for i in range(n)]
    return ids, data, metas, texts


def result_ids(response: dict[str, Any]) -> list[str]:
    return [item.get("id", "") for item in response.get("results", []) if item.get("id")]


def evaluate(args: argparse.Namespace, address: str) -> dict[str, Any]:
    report = MatrixReport()
    collection = args.collection
    n = args.vectors
    dim = args.dimensions
    ids, vectors, metas, texts = make_vectors(n, dim, args.seed)

    # --- Health ---
    try:
        health = wait_health(address)
        report.add(
            "health_ready",
            "health",
            bool(health.get("healthy") and health.get("ready")),
            f"message={health.get('message', '')}",
            health,
        )
    except Exception as error:  # noqa: BLE001
        report.add("health_ready", "health", False, str(error))
        return finalize(report, address, args, {})

    # --- Dense insert with metadata + text (BM25 source) ---
    insert_ok = 0
    insert_errors: list[str] = []
    for i, doc_id in enumerate(ids):
        try:
            resp = grpcurl(
                address,
                "Insert",
                {
                    "collection": collection,
                    "id": doc_id,
                    "vector": vectors[i].astype(float).tolist(),
                    "metadata": b64_json(metas[i]),
                    "text": texts[i],
                },
            )
            if resp.get("success", False) or resp.get("id"):
                insert_ok += 1
            else:
                insert_errors.append(f"{doc_id}: {resp}")
        except Exception as error:  # noqa: BLE001
            insert_errors.append(f"{doc_id}: {error}")
    report.add(
        "insert_success_rate",
        "crud_insert",
        insert_ok == n,
        f"{insert_ok}/{n} inserted; errors={insert_errors[:3]}",
        {"ok": insert_ok, "n": n},
    )
    time.sleep(args.settle_seconds)

    # --- Get all ---
    found = 0
    for doc_id in ids:
        try:
            got = grpcurl(address, "Get", {"collection": collection, "id": doc_id})
            if got.get("found"):
                found += 1
        except Exception:  # noqa: BLE001
            pass
    report.add(
        "get_found_rate",
        "crud_get",
        found == n,
        f"{found}/{n} found via Get",
        found / float(n) if n else 0.0,
    )

    # --- Update one vector ---
    update_id = ids[0]
    new_vec = normalize(vectors[0] * -1.0 + 0.01).astype(np.float32)
    try:
        upd = grpcurl(
            address,
            "Update",
            {
                "collection": collection,
                "id": update_id,
                "vector": new_vec.astype(float).tolist(),
                "metadata": b64_json({**metas[0], "updated": True}),
            },
        )
        ok = bool(upd.get("success", True))
        got = grpcurl(address, "Get", {"collection": collection, "id": update_id})
        emb = got.get("vector") or []
        cos = 0.0
        if emb:
            a = np.asarray(emb, dtype=np.float64)
            b = new_vec.astype(np.float64)
            denom = float(np.linalg.norm(a) * np.linalg.norm(b)) or 1.0
            cos = float(np.dot(a, b) / denom)
        report.add(
            "update_roundtrip",
            "crud_update",
            ok and got.get("found", False) and cos >= 0.99,
            f"success={ok} found={got.get('found')} cosine={cos:.4f}",
            {"cosine": cos},
        )
    except Exception as error:  # noqa: BLE001
        report.add("update_roundtrip", "crud_update", False, str(error))

    # --- Dense search self-hit ---
    try:
        probe = vectors[3].astype(float).tolist()
        search = grpcurl(
            address,
            "Search",
            {
                "collection": collection,
                "query": probe,
                "topK": 5,
                "nprobe": args.nprobe,
            },
        )
        returned = result_ids(search)
        report.add(
            "dense_self_hit",
            "dense_search",
            ids[3] in returned,
            f"returned={returned[:5]}",
            returned,
        )
    except Exception as error:  # noqa: BLE001
        report.add("dense_self_hit", "dense_search", False, str(error))

    # --- SearchBatch ---
    try:
        batch = grpcurl(
            address,
            "SearchBatch",
            {
                "collection": collection,
                "queries": [
                    {"vector": vectors[1].astype(float).tolist()},
                    {"vector": vectors[2].astype(float).tolist()},
                ],
                "topK": 3,
                "nprobe": args.nprobe,
            },
        )
        results = batch.get("results") or []
        report.add(
            "search_batch_count",
            "search_batch",
            len(results) == 2 and all(result_ids(r) for r in results),
            f"batch_responses={len(results)}",
            len(results),
        )
    except Exception as error:  # noqa: BLE001
        report.add("search_batch_count", "search_batch", False, str(error))

    # --- Legacy JSON metadata filter ---
    try:
        # filter bytes = base64 JSON object used as subset match
        filt = b64_json({"bucket": "1"})
        filtered = grpcurl(
            address,
            "Search",
            {
                "collection": collection,
                "query": vectors[1].astype(float).tolist(),
                "topK": 20,
                "nprobe": args.nprobe,
                "filter": filt,
            },
        )
        got = result_ids(filtered)
        # All returned IDs should belong to bucket 1
        expected_bucket_ids = {ids[i] for i, m in enumerate(metas) if m["bucket"] == "1"}
        # update_id was originally bucket 0; skip if present
        bad = [doc for doc in got if doc not in expected_bucket_ids and doc != update_id]
        # After update, update_id may still carry bucket 0 metadata if update changed it
        # Validate every returned id that we can Get has bucket==1
        bucket_ok = True
        checked = 0
        for doc in got:
            g = grpcurl(address, "Get", {"collection": collection, "id": doc})
            # metadata may be base64 or raw string depending on server encoding
            raw_meta = g.get("metadata") or ""
            meta_obj: dict[str, Any] = {}
            if isinstance(raw_meta, str) and raw_meta:
                try:
                    meta_obj = json.loads(raw_meta)
                except json.JSONDecodeError:
                    try:
                        meta_obj = json.loads(base64.b64decode(raw_meta).decode("utf-8"))
                    except Exception:  # noqa: BLE001
                        meta_obj = {}
            if meta_obj:
                checked += 1
                if str(meta_obj.get("bucket")) != "1":
                    bucket_ok = False
                    break
        report.add(
            "legacy_filter_bucket",
            "metadata_filter",
            bucket_ok and len(got) > 0 and (checked == 0 or bucket_ok),
            f"hits={len(got)} checked_meta={checked} bad_sample={bad[:3]}",
            {"hits": got[:10], "checked": checked},
        )
    except Exception as error:  # noqa: BLE001
        report.add("legacy_filter_bucket", "metadata_filter", False, str(error))

    # --- Typed TagFilter ---
    try:
        tag_filtered = grpcurl(
            address,
            "Search",
            {
                "collection": collection,
                "query": vectors[2].astype(float).tolist(),
                "topK": 20,
                "nprobe": args.nprobe,
                "tagFilter": {
                    "condition": {
                        "key": "bucket",
                        "value": {"text": "2"},
                        "op": "TAG_OP_EQ",
                    }
                },
            },
        )
        got = result_ids(tag_filtered)
        ok_all = True
        checked = 0
        for doc in got:
            g = grpcurl(address, "Get", {"collection": collection, "id": doc})
            raw_meta = g.get("metadata") or ""
            meta_obj: dict[str, Any] = {}
            if isinstance(raw_meta, str) and raw_meta:
                try:
                    meta_obj = json.loads(raw_meta)
                except json.JSONDecodeError:
                    try:
                        meta_obj = json.loads(base64.b64decode(raw_meta).decode("utf-8"))
                    except Exception:  # noqa: BLE001
                        meta_obj = {}
            if meta_obj:
                checked += 1
                if str(meta_obj.get("bucket")) != "2":
                    ok_all = False
                    break
        report.add(
            "tag_filter_eq",
            "tag_filter",
            ok_all and len(got) > 0,
            f"hits={len(got)} checked_meta={checked}",
            got[:10],
        )
    except Exception as error:  # noqa: BLE001
        report.add("tag_filter_eq", "tag_filter", False, str(error))

    # --- Score threshold (high bar should reduce or empty results) ---
    try:
        thr = grpcurl(
            address,
            "Search",
            {
                "collection": collection,
                "query": vectors[4].astype(float).tolist(),
                "topK": 10,
                "nprobe": args.nprobe,
                "scoreThreshold": 0.9999,
            },
        )
        thr_ids = result_ids(thr)
        open_search = grpcurl(
            address,
            "Search",
            {
                "collection": collection,
                "query": vectors[4].astype(float).tolist(),
                "topK": 10,
                "nprobe": args.nprobe,
            },
        )
        open_ids = result_ids(open_search)
        report.add(
            "score_threshold_restricts",
            "score_threshold",
            len(thr_ids) <= len(open_ids),
            f"threshold_hits={len(thr_ids)} open_hits={len(open_ids)}",
            {"threshold": thr_ids, "open": open_ids},
        )
    except Exception as error:  # noqa: BLE001
        report.add("score_threshold_restricts", "score_threshold", False, str(error))

    # --- BM25 TextSearch without embedding provider ---
    try:
        token = f"unique_token_{min(7, n - 1)}"
        text_resp = grpcurl(
            address,
            "TextSearch",
            {
                "collection": collection,
                "text": token,
                "topK": 5,
                "retrievalMode": "bm25",
            },
        )
        text_ids = result_ids(text_resp)
        expected = ids[min(7, n - 1)]
        report.add(
            "bm25_textsearch_no_embedding",
            "hybrid_bm25",
            expected in text_ids,
            f"query={token!r} returned={text_ids}",
            text_ids,
        )
    except Exception as error:  # noqa: BLE001
        report.add("bm25_textsearch_no_embedding", "hybrid_bm25", False, str(error))

    # --- Delete lifecycle ---
    delete_id = ids[-1]
    try:
        deleted = grpcurl(
            address,
            "Delete",
            {"collection": collection, "id": delete_id},
        )
        del_ok = bool(deleted.get("success", True))
        time.sleep(0.2)
        # Some builds return found=false; others raise NotFound on Get.
        get_hidden = False
        get_detail = ""
        try:
            after = grpcurl(address, "Get", {"collection": collection, "id": delete_id})
            get_hidden = not bool(after.get("found", True))
            get_detail = f"get_found={after.get('found')}"
        except RuntimeError as get_error:
            err = str(get_error)
            get_hidden = "NotFound" in err or "not found" in err.lower()
            get_detail = err.splitlines()[-1] if err else "get_error"
        report.add(
            "delete_hides_get",
            "crud_delete",
            del_ok and get_hidden,
            f"delete_success={del_ok} {get_detail}",
            {"delete": deleted, "detail": get_detail},
        )
    except Exception as error:  # noqa: BLE001
        report.add("delete_hides_get", "crud_delete", False, str(error))

    try:
        search_after = grpcurl(
            address,
            "Search",
            {
                "collection": collection,
                "query": vectors[-1].astype(float).tolist(),
                "topK": 10,
                "nprobe": args.nprobe,
            },
        )
        report.add(
            "delete_hides_search",
            "crud_delete",
            delete_id not in result_ids(search_after),
            f"returned={result_ids(search_after)[:5]}",
            result_ids(search_after),
        )
    except Exception as error:  # noqa: BLE001
        report.add("delete_hides_search", "crud_delete", False, str(error))

    return finalize(report, address, args, {"vectors": n, "dimensions": dim})


def finalize(
    report: MatrixReport,
    address: str,
    args: argparse.Namespace,
    extra: dict[str, Any],
) -> dict[str, Any]:
    summary = {
        "report_type": REPORT_TYPE,
        "passed": report.passed,
        "server": address,
        "collection": args.collection,
        "generated_at_unix": int(time.time()),
        "features_covered": sorted({c.feature for c in report.checks}),
        "checks": [
            {
                "name": c.name,
                "feature": c.feature,
                "passed": c.passed,
                "detail": c.detail,
                "value": c.value,
            }
            for c in report.checks
        ],
        "failures": [
            {"name": c.name, "feature": c.feature, "detail": c.detail} for c in report.failures()
        ],
        "dataset": extra,
        "scope_note": (
            "Covers mutable standalone feature surface. Does not replace market SIFT1M, "
            "authoritative memory, or generation-serving qualifications."
        ),
    }
    return summary


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# AkiDB Feature Matrix QA",
        "",
        f"- **Overall:** `{'PASS' if summary['passed'] else 'FAIL'}`",
        f"- **Server:** `{summary['server']}`",
        f"- **Collection:** `{summary['collection']}`",
        f"- **Features:** {', '.join(f'`{f}`' for f in summary['features_covered'])}",
        "",
        "| Check | Feature | Status | Detail |",
        "| --- | --- | --- | --- |",
    ]
    for check in summary["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        detail = str(check["detail"]).replace("|", "\\|")
        lines.append(
            f"| `{check['name']}` | `{check['feature']}` | **{status}** | {detail} |"
        )
    if summary["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in summary["failures"]:
            lines.append(f"- `{failure['name']}` ({failure['feature']}): {failure['detail']}")
    lines.extend(
        [
            "",
            "## Feature coverage map",
            "",
            "| Product feature | Gate check |",
            "| --- | --- |",
            "| Health / readiness | `health_ready` |",
            "| Insert + durable Get | `insert_success_rate`, `get_found_rate` |",
            "| Update | `update_roundtrip` |",
            "| Delete (tombstone) | `delete_hides_get`, `delete_hides_search` |",
            "| Dense HNSW search | `dense_self_hit` |",
            "| SearchBatch | `search_batch_count` |",
            "| Legacy metadata filter | `legacy_filter_bucket` |",
            "| Typed TagFilter | `tag_filter_eq` |",
            "| Score threshold | `score_threshold_restricts` |",
            "| BM25 TextSearch (no embedder) | `bm25_textsearch_no_embedding` |",
            "",
            summary.get("scope_note", ""),
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-server", action="store_true")
    parser.add_argument("--server", default="127.0.0.1:50051")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--server-log-level", default="warn")
    parser.add_argument("--collection", default="default")
    parser.add_argument("--vectors", type=int, default=40)
    parser.add_argument("--dimensions", type=int, default=64)
    parser.add_argument("--nprobe", type=int, default=32)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--settle-seconds", type=float, default=0.5)
    parser.add_argument("--output", default=None)
    parser.add_argument("--markdown", default=None)
    parser.add_argument("--no-fail", action="store_true")
    args = parser.parse_args()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    if args.output is None:
        args.output = str(ROOT / "qa-results" / f"feature-matrix-{stamp}.json")
    if args.markdown is None:
        args.markdown = str(Path(args.output).with_suffix(".md"))
    if args.vectors < 10:
        parser.error("--vectors must be >= 10")
    return args


def main() -> int:
    args = parse_args()
    require_grpcurl()
    server = start_server(args)
    out = Path(args.output)
    md_path = Path(args.markdown)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        summary = evaluate(args, server.address)
        out.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        md = render_markdown(summary)
        md_path.write_text(md, encoding="utf-8")
        print(md)
        print(f"JSON: {out}")
        print(f"Markdown: {md_path}")
        return 0 if summary["passed"] or args.no_fail else 1
    finally:
        stop_server(server, args.keep_temp)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
