#!/usr/bin/env python3
"""Exercise the public onboarding path against a real isolated AkiDB process.

The deterministic local embedding stub verifies plumbing, not retrieval quality.
No user corpus, credentials, default data directory, or external service is used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Embeddings(BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:
        pass

    def do_POST(self) -> None:
        size = int(self.headers.get("Content-Length", "0"))
        if self.path != "/v1/embeddings" or size > 2_000_000:
            self.send_error(400)
            return
        request = json.loads(self.rfile.read(size))
        texts = request["input"]
        if isinstance(texts, str):
            texts = [texts]
        data = []
        for index, text in enumerate(texts):
            digest = hashlib.sha256(text.encode()).digest()
            vector = [(byte + 1) / 256 for byte in digest[:8]]
            data.append({"object": "embedding", "index": index, "embedding": vector})
        body = json.dumps({"object": "list", "model": "adoption-stub", "data": data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/akidb")
    parser.add_argument("--embedding-url", help="Optional real loopback embeddings endpoint")
    parser.add_argument("--embedding-model", default="nomic-embed-text")
    parser.add_argument("--dimensions", type=int, default=768)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    # Crash qualification is intentional; never leave large core files behind.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    # Do not inherit operator overrides, tokens, or embedding credentials.
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AKIDB_", "AX_ENGINE_", "SEAWEEDFS_"))}
    for key in ("OPENAI_API_KEY", "OPENAI_BASE_URL"):
        env.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="akidb-adoption-") as tmp:
        work = Path(tmp).resolve()
        corpus = work / "corpus"
        shutil.copytree(ROOT / "contracts/fixtures/adoption/documents", corpus)
        (corpus / "secret.json").write_text('{"synthetic": "not for indexing"}\n')
        (corpus / ".hidden.md").write_text("not for indexing\n")
        (corpus / "excluded.md").write_text("not for indexing\n")
        (corpus / ".akidbignore").write_text("excluded.md\n")
        (corpus / "linked.md").symlink_to(corpus / "runbook.md")
        stub = ThreadingHTTPServer(("127.0.0.1", 0), Embeddings)
        worker = threading.Thread(target=stub.serve_forever, daemon=True)
        worker.start()
        embedding = ["--embedding-url", args.embedding_url or f"http://127.0.0.1:{stub.server_port}/v1/embeddings",
                     "--embedding-model", args.embedding_model if args.embedding_url else "adoption-stub",
                     "--dimensions", str(args.dimensions if args.embedding_url else 8)]
        port = free_port()
        connect = ["--server", f"127.0.0.1:{port}", "--timeout-seconds", "5"]
        server: subprocess.Popen | None = None
        log = (work / "server.log").open("wb")

        def run(*command: str, success: bool = True) -> subprocess.CompletedProcess:
            result = subprocess.run([str(binary), *command], cwd=work, env=env,
                                    capture_output=True, text=True, timeout=45)
            if (result.returncode == 0) != success:
                raise AssertionError(f"{command[0]} unexpected exit {result.returncode}\n"
                                     f"{result.stdout}\n{result.stderr}")
            return result

        def stop() -> None:
            nonlocal server
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
                server = None

        def start(crash_stage: str | None = None) -> None:
            nonlocal server
            server_env = dict(env)
            if crash_stage:
                server_env["AKIDB_CRASH_POINT"] = crash_stage
            server = subprocess.Popen([str(binary), "start", "--data-dir", str(work / "data"),
                                       "--grpc-port", str(port), *embedding, "--log-level", "warn"],
                                      cwd=work, env=server_env, stdout=log, stderr=log)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise AssertionError("server exited during startup")
                probe = subprocess.run([str(binary), "health", "--server", f"127.0.0.1:{port}",
                                        "--require-ready"], env=env, cwd=work,
                                       capture_output=True, timeout=6)
                if probe.returncode == 0:
                    return
                time.sleep(0.2)
            raise AssertionError("server did not become ready")

        def index(*extra: str, success: bool = True) -> subprocess.CompletedProcess:
            return run("index", str(corpus), *connect, *embedding, "--state-dir", str(work / "state"),
                       "--chunk-chars", "80", *extra, success=success)

        def pack(*extra: str, success: bool = True) -> dict:
            result = run("pack", "Cedar health port", *connect, "--retrieval-mode", "bm25",
                         "--json", "--token-budget", "512", *extra, success=success)
            return json.loads(result.stdout)

        try:
            start()
            report = json.loads(run("doctor", *connect, *embedding, "--json").stdout)
            assert report["ok"] is True
            serialized = json.dumps(report)
            assert str(work) not in serialized and "127.0.0.1" not in serialized
            negative = embedding[:-1] + ["7"]
            report = json.loads(run("doctor", *connect, *negative, "--json", success=False).stdout)
            assert report["ok"] is False
            dry = index("--dry-run").stdout
            assert "secret.json\texcluded:" in dry and "linked.md\texcluded:" in dry
            assert "excluded.md\texcluded:" in dry and ".hidden.md\texcluded:" in dry
            index()
            assert "2 unchanged" in index().stdout
            result = pack("--verify-sources-under", str(corpus))
            assert result["items"] and "7410" in result["text"]
            assert result["used_tokens"] <= result["token_budget"]
            assert result["token_count_is_estimate"] is True
            assert all(item["source_verification"] == "verified" for item in result["items"])
            if args.embedding_url:
                for query, expected in [("Cedar service health endpoint port", "runbook.md"),
                                        ("Who owns canonical knowledge sources and publication decisions?", "releases.md")]:
                    ranked = json.loads(run("pack", query, *connect, "--retrieval-mode", "vector",
                                            "--top-k", "1", "--json", "--verify-sources-under", str(corpus)).stdout)
                    assert ranked["items"] and ranked["items"][0]["citation"]["source_uri"].endswith(expected)
            first_ids = [item["chunk_id"] for item in result["items"]]
            outside = work / "unrelated"
            outside.mkdir()
            wrong_scope = pack("--verify-sources-under", str(outside), success=False)
            assert all(item["source_verification"] == "outside_allowed_root" for item in wrong_scope["items"])
            source = corpus / "runbook.md"
            original = source.read_text()
            source.write_text(original + "Additional recovery notes.\n")
            stale = pack("--verify-sources-under", str(corpus), success=False)
            assert any(item["source_verification"] == "document_version_mismatch_or_unsupported" for item in stale["items"])
            source.write_text(original)
            stop()
            start()
            recovered = pack("--verify-sources-under", str(corpus))
            assert [item["chunk_id"] for item in recovered["items"]] == first_ids
            source.write_text("Cedar health port is 7530.\n")
            index()
            shrunk = pack("--verify-sources-under", str(corpus))
            assert len(shrunk["items"]) == 1 and "7530" in shrunk["text"]
            source.write_text("")
            index()
            assert not pack()["items"]
            source.write_text(original)
            index()
            assert "7410" in pack("--verify-sources-under", str(corpus))["text"]
            for stage, new_port, expected_port in [
                ("document.before_commit", "7540", "7410"),
                ("document.after_commit", "7550", "7550"),
            ]:
                stop()
                start(stage)
                source.write_text(original.replace("7410", new_port))
                index(success=False)
                assert server is not None
                server.wait(timeout=10)
                assert server.returncode != 0
                stop()
                start()
                result = pack()
                assert expected_port in result["text"]
                versions = {item["citation"]["document_version"] for item in result["items"]}
                assert len(versions) == 1
            pack("--verify-sources-under", str(corpus))
            # Resolve an ambiguous committed reply and refresh the local receipt.
            index()
            source.unlink()
            assert "1 removed" in index().stdout
            assert not pack()["items"]
            print(json.dumps({"schema": "akidb.adoption-smoke.v1", "ok": True,
                              "embedding": "real-model-two-known-sources" if args.embedding_url else "deterministic-stub-not-quality-evidence",
                              "checks": ["doctor", "negative-dimensions", "exclusions", "index",
                                         "unchanged", "citations", "scope", "changed-source",
                                         "restart", "shrink", "empty", "recreate",
                                         "crash-before-commit", "crash-after-commit", "delete"]}, indent=2))
        finally:
            stop()
            log.close()
            stub.shutdown()
            stub.server_close()
            worker.join(timeout=5)


if __name__ == "__main__":
    main()
