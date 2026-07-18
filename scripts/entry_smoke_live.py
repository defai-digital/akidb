#!/usr/bin/env python3
"""Docker-free live entry smoke against a real akidb server process.

Starts `akidb-cli server --standalone` on loopback, inserts two workspace-scoped
vectors via the gRPC stub, asserts isolation, and exercises BM25 TextSearch
(graph_hybrid mode when edges present). Does not require embedding sidecar.

Usage:
  python3 scripts/entry_smoke_live.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = Path(
    os.environ.get(
        "AKIDB_SMOKE_SCRATCH",
        ROOT / "qa-results",
    )
)
# Prefer project-local smoke venv when system Python lacks grpcio.
try:
    import grpc  # noqa: F401
except ImportError:
    _venv_py = ROOT / ".venv-smoke" / "bin" / "python"
    if _venv_py.exists():
        os.execv(str(_venv_py), [str(_venv_py), str(Path(__file__).resolve()), *sys.argv[1:]])


def wait_port(host: str, port: int, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def main() -> int:
    # Import generated stubs from the in-repo Python SDK.
    sys.path.insert(0, str(ROOT / "sdks" / "python"))
    try:
        import grpc
        from akidb import akidb_pb2 as pb
        from akidb import akidb_pb2_grpc as pb_grpc
    except Exception as e:
        print(f"FATAL: cannot import python grpc stubs: {e}", file=sys.stderr)
        return 2

    data = SCRATCH / "entry-smoke-data"
    data.mkdir(parents=True, exist_ok=True)
    config = data / "smoke.toml"
    port = 50151
    config.write_text(
        f"""
[server]
host = "127.0.0.1"
port = 18080
grpc_port = {port}
tls_enabled = false

[auth]
mode = "loopback_optional"
token_file = "{data}/auth.token"

[auth.acl]
default_workspace = "default"
enforce_workspace = true

[index]
index_type = "HNSW"
hnsw_m = 16
hnsw_ef_construction = 64
hnsw_ef_search = 32
vector_precision = "f32"
metric = "cosine"

[index.filter]
mode = "adaptive"
postfilter_overfetch_factor = 5
adaptive_pre_selectivity = 0.20

[index.rebuild]
tombstone_ratio_trigger = 0.10
max_duration_seconds = 300
preferred_hours = [2, 3, 4]

[index.tombstone]
max_count = 100000

[storage]
rocksdb_path = "{data}/rocksdb"
wal_enabled = true
wal_path = "{data}/wal"

[storage.minio]
endpoint = "localhost:9000"
bucket = "akidb-snapshots"
access_key = "x"
secret_key = "y"
use_ssl = false

[sql]
enabled = false
backend = "sqlite"
sqlite_path = "{data}/meta.sqlite"

[observability]
tracing_enabled = false
metrics_enabled = false
metrics_port = 19090
log_level = "info"
log_format = "pretty"

[slo.reference]
dimensions = 4
vectors_per_shard = 10000
top_k = 10
nprobe = 32
batch_size = 1
target_p95_ms = 50

[slo.backpressure]
soft_breach_ms = 50
hard_breach_ms = 75
degraded_mode_enabled = true

[embedding]
enabled = false
url = "http://127.0.0.1:8081/v1/embeddings"
model = "none"
dimensions = 4
timeout_ms = 10000
max_batch_size = 32
"""
    )

    # Build CLI if needed
    subprocess.run(
        ["cargo", "build", "-p", "akidb-cli", "-q"],
        cwd=ROOT,
        check=True,
    )
    bin_path = ROOT / "target" / "debug" / "akidb"
    if not bin_path.exists():
        # package may emit as akidb from akidb-cli
        candidates = list((ROOT / "target" / "debug").glob("akidb*"))
        print("bin candidates", candidates)
        bin_path = next((c for c in candidates if c.is_file() and os.access(c, os.X_OK)), bin_path)

    log_path = SCRATCH / "entry-smoke-server.log"
    server_log = open(log_path, "w")
    proc = subprocess.Popen(
        [
            str(bin_path),
            "server",
            "--standalone",
            "--config",
            str(config),
            "--listen",
            f"127.0.0.1:{port}",
        ],
        cwd=ROOT,
        stdout=server_log,
        stderr=subprocess.STDOUT,
    )
    try:
        if not wait_port("127.0.0.1", port, 90.0):
            server_log.flush()
            print(f"server failed to listen; log={log_path.read_text()[-2000:]}", file=sys.stderr)
            return 1

        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = pb_grpc.AkidbStub(channel)

        def insert(vid: str, text: str, workspace: str, vec=None):
            if vec is None:
                vec = [1.0, 0.0, 0.0, 0.0] if workspace == "ws-a" else [0.0, 1.0, 0.0, 0.0]
            meta = json.dumps({"title": text, "workspace_id": workspace}).encode()
            md = (("x-akidb-workspace", workspace),)
            return stub.Insert(
                pb.InsertRequest(
                    collection="default",
                    id=vid,
                    vector=vec,
                    metadata=meta,
                    text=text,
                ),
                metadata=md,
                timeout=10,
            )

        insert("doc-a", "workspace alpha secret note", "ws-a")
        insert("doc-b", "workspace beta secret note", "ws-b")

        def search(workspace: str):
            md = (("x-akidb-workspace", workspace),)
            return stub.Search(
                pb.SearchRequest(
                    collection="default",
                    query=[1.0, 0.0, 0.0, 0.0] if workspace == "ws-a" else [0.0, 1.0, 0.0, 0.0],
                    top_k=10,
                ),
                metadata=md,
                timeout=10,
            )

        ra = search("ws-a")
        ids_a = [r.id for r in ra.results]
        rb = search("ws-b")
        ids_b = [r.id for r in rb.results]

        # BM25 text search (no embedding required)
        text = stub.TextSearch(
            pb.TextSearchRequest(
                collection="default",
                text="alpha secret",
                top_k=5,
                hybrid=False,
                retrieval_mode="bm25",
            ),
            metadata=(("x-akidb-workspace", "ws-a"),),
            timeout=10,
        )
        text_ids = [r.id for r in text.results]

        # Graph mode (lexical + graph expand) works without embedding provider.
        # graph_hybrid needs dense embeddings; unit tests cover that path.
        graph = stub.TextSearch(
            pb.TextSearchRequest(
                collection="default",
                text="alpha secret",
                top_k=5,
                hybrid=False,
                retrieval_mode="graph",
            ),
            metadata=(("x-akidb-workspace", "ws-a"),),
            timeout=10,
        )

        artifact = {
            "gate": "entry_smoke_live",
            "listen": f"127.0.0.1:{port}",
            "ids_ws_a": ids_a,
            "ids_ws_b": ids_b,
            "bm25_ids": text_ids,
            "graph_mode_ids": [r.id for r in graph.results],
            "isolation_ok": ("doc-a" in ids_a and "doc-b" not in ids_a and "doc-b" in ids_b and "doc-a" not in ids_b),
            "bm25_ok": "doc-a" in text_ids and "doc-b" not in text_ids,
            "graph_ok": "doc-a" in [r.id for r in graph.results],
            "server_log": str(log_path),
        }
        artifact["passed"] = (
            artifact["isolation_ok"] and artifact["bm25_ok"] and artifact["graph_ok"]
        )
        out = SCRATCH / "entry-smoke.log"
        out.write_text(json.dumps(artifact, indent=2) + "\n")
        print(json.dumps(artifact, indent=2))
        return 0 if artifact["passed"] else 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        server_log.close()


if __name__ == "__main__":
    sys.exit(main())
