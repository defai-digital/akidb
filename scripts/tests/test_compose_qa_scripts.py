from __future__ import annotations

import json
import os
import subprocess
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOAD_TEST = ROOT / "deploy" / "compose" / "scripts" / "load-test.sh"
E2E_TEST = ROOT / "deploy" / "compose" / "scripts" / "e2e-test.sh"
COMPOSE_FILE = ROOT / "deploy" / "compose" / "docker-compose.yml"
PROD_COMPOSE_FILE = ROOT / "deploy" / "compose" / "docker-compose.prod.yml"
STANDALONE_COMPOSE_FILE = ROOT / "deploy" / "seaweedfs" / "docker-compose.yml"
SEAWEEDFS_DIR = ROOT / "deploy" / "compose" / "seaweedfs"
S3_CONFIG_GENERATOR = SEAWEEDFS_DIR / "gen-s3-config.sh"
BUCKET_SETUP = SEAWEEDFS_DIR / "create-buckets.sh"
GENERATION_GATE = ROOT / "scripts" / "test-generation-serving-seaweedfs.sh"


def write_executable(path: Path, contents: str) -> None:
    path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")
    path.chmod(0o755)


class ComposeQaScriptTests(unittest.TestCase):
    def run_script(
        self,
        source: Path,
        *,
        env: dict[str, str],
        interpreter: str,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [interpreter, str(source)],
            cwd=ROOT,
            env={**os.environ, **env},
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def test_seaweedfs_stack_always_loads_generated_s3_credentials(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        generator = S3_CONFIG_GENERATOR.read_text(encoding="utf-8")
        bucket_setup = BUCKET_SETUP.read_text(encoding="utf-8")
        gate = GENERATION_GATE.read_text(encoding="utf-8")

        # SeaweedFS allows anonymous access to everything when -s3.config is
        # absent, so the config file is mounted and its generator is a gate.
        self.assertIn("image: chrislusf/seaweedfs:4.47", compose)
        self.assertNotIn("seaweedfs:latest", compose)
        self.assertIn("-s3.config=/etc/seaweedfs/s3.json", compose)
        self.assertIn("seaweedfs-config:/etc/seaweedfs:ro", compose)
        self.assertIn("SEAWEEDFS_ACCESS_KEY_FILE: /run/secrets/seaweedfs_access_key", compose)
        self.assertIn(
            "SEAWEEDFS_SECRET_KEY_FILE: /run/secrets/seaweedfs_secret_key", compose
        )
        self.assertIn("file: ./secrets/seaweedfs_access_key.txt", compose)
        self.assertIn("file: ./secrets/seaweedfs_secret_key.txt", compose)
        self.assertIn("condition: service_completed_successfully", compose)
        self.assertIn(
            '"actions": ["Admin", "Read", "Write", "List", "Tagging"]', generator
        )
        self.assertIn("set -eu", generator)

        # Clients reach the gateway at the SeaweedFS S3 port.
        self.assertIn(
            "UPLOAD_GATEWAY_SEAWEEDFS_ENDPOINT: http://seaweedfs:8333", compose
        )
        self.assertIn("STORAGE_ENDPOINT: http://seaweedfs:8333", compose)
        self.assertIn('"127.0.0.1:${SEAWEEDFS_API_PORT:-8333}:8333"', compose)
        self.assertIn('"127.0.0.1:${SEAWEEDFS_MASTER_PORT:-9333}:9333"', compose)

        # Buckets come from one `weed shell` invocation each.
        self.assertIn(
            'echo "s3.bucket.create -name ${bucket}" | weed shell', bucket_setup
        )
        self.assertIn("akidb-documents", compose)
        self.assertIn("akidb-documents akidb-snapshots", compose)

        # The generation gate keeps the same shape against the SeaweedFS image.
        self.assertIn(
            'AKIDB_SEAWEEDFS_IMAGE:-chrislusf/seaweedfs:4.47', gate
        )
        self.assertIn("--aws-sigv4", gate)
        self.assertIn("curl --fail --silent \"$SEAWEEDFS_ENDPOINT/healthz\"", gate)
        self.assertNotIn("9000", gate)

    def test_standalone_seaweedfs_compose_is_pinned_and_authenticated(self) -> None:
        standalone = STANDALONE_COMPOSE_FILE.read_text(encoding="utf-8")
        prod = PROD_COMPOSE_FILE.read_text(encoding="utf-8")

        self.assertIn("image: chrislusf/seaweedfs:4.47", standalone)
        self.assertIn("./s3.json:/etc/seaweedfs/s3.json:ro", standalone)
        self.assertIn("-s3.config=/etc/seaweedfs/s3.json", standalone)
        self.assertIn("seaweedfs-data:", standalone)
        self.assertIn("s3.bucket.create -name $$bucket", standalone)
        self.assertIn("akidb-snapshots", standalone)
        self.assertIn("akidb-wal", standalone)
        self.assertIn("seaweedfs:", prod)

    def test_seaweedfs_stack_drops_the_bucket_notification_path(self) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        standalone = STANDALONE_COMPOSE_FILE.read_text(encoding="utf-8")
        prod = PROD_COMPOSE_FILE.read_text(encoding="utf-8")
        e2e = E2E_TEST.read_text(encoding="utf-8")
        load = LOAD_TEST.read_text(encoding="utf-8")

        # SeaweedFS does not implement S3 bucket notifications. The upload
        # gateway's own NATS publish is the event path and the ingestion
        # scheduler's periodic sync is the recovery path.
        self.assertNotIn("MINIO_NOTIFY", compose)
        self.assertIn("upload gateway's own NATS publish", compose)
        self.assertIn("periodic", compose)
        for name, text in (
            ("docker-compose.yml", compose),
            ("docker-compose.prod.yml", prod),
            ("deploy/seaweedfs/docker-compose.yml", standalone),
            ("e2e-test.sh", e2e),
            ("load-test.sh", load),
        ):
            self.assertNotIn("minio", text.lower(), name)
        self.assertIn("seaweedfs.uploads", e2e)
        self.assertIn("seaweedfs.uploads.>", e2e)

    def test_s3_credentials_generator_fails_closed_on_bad_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            # The generator chowns the volume for uid/gid 1000; the test
            # runner is not root, so the ownership call is stubbed out.
            shim = fake_bin / "chown"
            shim.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            shim.chmod(0o755)
            env = {
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "SEAWEEDFS_ACCESS_KEY_FILE": str(root / "access"),
                "SEAWEEDFS_SECRET_KEY_FILE": str(root / "secret"),
                "SEAWEEDFS_S3_CONFIG": str(root / "out" / "s3.json"),
            }

            missing = self.run_script(S3_CONFIG_GENERATOR, env=env, interpreter="sh")
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn(
                "cannot read the SeaweedFS access key secret", missing.stderr
            )

            (root / "access").write_text("akidb-admin\n", encoding="utf-8")
            (root / "secret").write_text("\n", encoding="utf-8")
            empty = self.run_script(S3_CONFIG_GENERATOR, env=env, interpreter="sh")
            self.assertNotEqual(empty.returncode, 0)
            self.assertIn("is empty", empty.stderr)

            for label, value in (
                ("double quote", 'sec"ret\n'),
                ("backslash", "sec\\ret\n"),
                ("newline", "sec\nret\n"),
            ):
                with self.subTest(label):
                    (root / "secret").write_text(value, encoding="utf-8")
                    rejected = self.run_script(
                        S3_CONFIG_GENERATOR, env=env, interpreter="sh"
                    )
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertNotEqual(rejected.stderr.strip(), "")

            (root / "secret").write_text("akidb-secret-key\n", encoding="utf-8")
            rendered = self.run_script(S3_CONFIG_GENERATOR, env=env, interpreter="sh")
            self.assertEqual(rendered.returncode, 0, rendered.stderr)
            identity = json.loads(
                (root / "out" / "s3.json").read_text(encoding="utf-8")
            )["identities"][0]
            self.assertEqual(identity["name"], "akidb-admin")
            self.assertEqual(
                identity["actions"], ["Admin", "Read", "Write", "List", "Tagging"]
            )
            self.assertEqual(
                identity["credentials"],
                [{"accessKey": "akidb-admin", "secretKey": "akidb-secret-key"}],
            )

    def test_e2e_requires_every_service_check(self) -> None:
        script = E2E_TEST.read_text(encoding="utf-8")

        self.assertIn('log_error "Doc-parser service failed to start"', script)
        self.assertIn(
            'if [ "$TESTS_PASSED" -eq "$TESTS_TOTAL" ]; then',
            script,
        )
        self.assertNotIn(
            "native formats remain testable",
            script,
        )

    def run_bash(
        self,
        source: Path,
        body: str,
        *,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = f'source "$1"\n{body}'
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)
        return subprocess.run(
            ["bash", "-c", command, "bash", str(source)],
            cwd=ROOT,
            env=merged_env,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def test_load_configuration_rejects_zero_and_missing_values(self) -> None:
        result = self.run_bash(
            LOAD_TEST,
            """
            TOTAL_DOCS=0
            if validate_configuration; then
                exit 9
            fi
            if parse_args --docs; then
                exit 10
            fi
            """,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("TOTAL_DOCS must be a positive integer", result.stdout)
        self.assertIn("--docs requires a value", result.stdout)

    def test_uploads_run_concurrently_and_preserve_success_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            results = root / "results"
            documents = results / "test-docs"
            fake_bin.mkdir()
            documents.mkdir(parents=True)
            for index in range(6):
                (documents / f"test_doc_{index}.txt").write_text(
                    f"document {index}\n", encoding="utf-8"
                )

            state_file = root / "curl-state"
            write_executable(
                fake_bin / "curl",
                """
                #!/usr/bin/env python3
                import fcntl
                import os
                from pathlib import Path
                import sys
                import time

                state_path = Path(os.environ["FAKE_CURL_STATE"])

                def update(delta: int) -> None:
                    state_path.touch()
                    with state_path.open("r+", encoding="utf-8") as state:
                        fcntl.flock(state, fcntl.LOCK_EX)
                        values = state.read().split()
                        active, maximum = map(int, values) if values else (0, 0)
                        active += delta
                        maximum = max(maximum, active)
                        state.seek(0)
                        state.truncate()
                        state.write(f"{active} {maximum}\\n")
                        state.flush()
                        fcntl.flock(state, fcntl.LOCK_UN)

                update(1)
                time.sleep(0.15)
                update(-1)
                print("500" if "test_doc_2.txt" in " ".join(sys.argv) else "201", end="")
                """,
            )

            env = {
                "CONCURRENT_UPLOADS": "3",
                "FAKE_CURL_STATE": str(state_file),
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "RESULTS_DIR": str(results),
                "TIMESTAMP": "regression",
                "TOTAL_DOCS": "6",
            }
            result = self.run_bash(LOAD_TEST, "upload_documents", env=env)

            self.assertEqual(
                result.returncode,
                0,
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
            )
            summary = json.loads(
                (results / "upload_summary_regression.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(summary["success_count"], 5)
            self.assertEqual(summary["fail_count"], 1)
            _, maximum = map(
                int, state_file.read_text(encoding="utf-8").split()
            )
            self.assertGreaterEqual(maximum, 3)

    def test_search_uses_akidb_text_search_and_emits_valid_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            results = root / "results"
            grpcurl_log = root / "grpcurl.log"
            fake_bin.mkdir()
            results.mkdir()

            write_executable(
                fake_bin / "jq",
                """
                #!/bin/sh
                printf '%s\\n' \
                  '{"collection":"qa","text":"query","topK":10,"retrievalMode":"bm25"}'
                """,
            )
            write_executable(
                fake_bin / "grpcurl",
                """
                #!/bin/sh
                printf '%s\\n' "$*" >> "$FAKE_GRPCURL_LOG"
                """,
            )

            env = {
                "AKIDB_COLLECTION": "qa",
                "AKIDB_SERVER": "qa-shard.internal:50051",
                "FAKE_GRPCURL_LOG": str(grpcurl_log),
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "RESULTS_DIR": str(results),
                "SEARCH_DURATION": "1",
                "SEARCH_MAX_IN_FLIGHT": "2",
                "SEARCH_QPS": "2",
                "TIMESTAMP": "regression",
            }
            result = self.run_bash(LOAD_TEST, "run_search_load_test", env=env)

            self.assertEqual(result.returncode, 0, result.stderr)
            summary = json.loads(
                (results / "search_summary_regression.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(summary["total_requests"], 2)
            self.assertEqual(summary["success_count"], 2)
            invocations = grpcurl_log.read_text(encoding="utf-8")
            self.assertEqual(invocations.count("akidb.v1.Akidb/TextSearch"), 2)
            self.assertIn("qa-shard.internal:50051", invocations)
            self.assertNotIn("http://localhost:8080/search", invocations)

    def test_ingestion_wait_fails_when_metrics_are_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_bin = Path(temporary)
            write_executable(
                fake_bin / "curl",
                """
                #!/bin/sh
                exit 7
                """,
            )
            env = {
                "INGESTION_POLL_SECONDS": "1",
                "INGESTION_WAIT_SECONDS": "1",
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
            }
            result = self.run_bash(
                LOAD_TEST,
                """
                if wait_for_ingestion; then
                    exit 9
                fi
                """,
                env=env,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(
                "Ingestion did not complete within 1s", result.stdout
            )

    def test_ingestion_wait_rejects_stale_zero_queue_metric(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_bin = Path(temporary) / "bin"
            fake_bin.mkdir()
            write_executable(
                fake_bin / "curl",
                """
                #!/bin/sh
                printf '%s\n' \
                  '{"status":"success","data":{"result":[{"value":[0,"0"]}]}}'
                """,
            )
            result = self.run_bash(
                LOAD_TEST,
                """
                INGESTION_WAIT_SECONDS=1
                INGESTION_POLL_SECONDS=1
                EXPECTED_INGESTED_DOCS=2
                INGESTION_BASELINE_PROCESSED=0
                if wait_for_ingestion; then
                    exit 9
                fi
                """,
                env={
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                },
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("processed: 0/2", result.stdout)

    def test_ingestion_wait_writes_end_to_end_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            results = root / "results"
            fake_bin.mkdir()
            results.mkdir()
            write_executable(
                fake_bin / "curl",
                """
                #!/bin/sh
                case "$*" in
                    *documents_processed*) value=2 ;;
                    *) value=0 ;;
                esac
                printf \
                  '{"status":"success","data":{"result":[{"value":[0,"%s"]}]}}\n' \
                  "$value"
                """,
            )
            write_executable(
                fake_bin / "sleep",
                """
                #!/bin/sh
                exit 0
                """,
            )
            result = self.run_bash(
                LOAD_TEST,
                """
                EXPECTED_INGESTED_DOCS=2
                INGESTION_BASELINE_PROCESSED=0
                INGESTION_START_TIME="$(date +%s.%N)"
                wait_for_ingestion
                """,
                env={
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "RESULTS_DIR": str(results),
                    "TIMESTAMP": "regression",
                },
            )

            self.assertEqual(
                result.returncode,
                0,
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
            )
            summary = json.loads(
                (results / "ingestion_summary_regression.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(summary["expected_documents"], 2)
            self.assertEqual(summary["processed_documents"], 2)
            self.assertGreaterEqual(summary["throughput_docs_per_hour"], 0)

    def test_report_fails_when_fast_searches_are_unsuccessful(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = Path(temporary)
            (results / "upload_summary_regression.json").write_text(
                json.dumps(
                    {
                        "total_docs": 10,
                        "success_count": 10,
                        "fail_count": 0,
                        "success_rate_pct": 100,
                        "duration_seconds": 1,
                        "throughput_docs_per_sec": 10,
                    }
                ),
                encoding="utf-8",
            )
            (results / "search_summary_regression.json").write_text(
                json.dumps(
                    {
                        "total_requests": 10,
                        "success_count": 0,
                        "success_rate_pct": 0,
                        "actual_qps": 100,
                        "latency": {
                            "avg_ms": 0,
                            "p50_ms": 1,
                            "p95_ms": 1,
                            "p99_ms": 1,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (results / "ingestion_summary_regression.json").write_text(
                json.dumps(
                    {
                        "expected_documents": 10,
                        "processed_documents": 10,
                        "duration_seconds": 10,
                        "throughput_docs_per_sec": 1,
                        "throughput_docs_per_hour": 3600,
                    }
                ),
                encoding="utf-8",
            )
            result = self.run_bash(
                LOAD_TEST,
                """
                if generate_report; then
                    exit 9
                fi
                """,
                env={
                    "RESULTS_DIR": str(results),
                    "TIMESTAMP": "regression",
                },
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("load-test SLO gates failed", result.stdout)
            report = (results / "load_test_report_regression.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("Search Success Rate", report)
            self.assertIn("✗ FAIL", report)

    def test_e2e_timeout_is_validated_and_bounds_health_wait(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_bin = Path(temporary)
            write_executable(
                fake_bin / "curl",
                """
                #!/bin/sh
                exit 1
                """,
            )
            env = {
                "HEALTH_POLL_INTERVAL": "0.05",
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
            }
            started = time.monotonic()
            result = self.run_bash(
                E2E_TEST,
                """
                parse_args --timeout 1
                DEADLINE=$((SECONDS + TIMEOUT))
                if wait_for_http http://unreachable.invalid/health; then
                    exit 9
                fi
                if parse_args --timeout 0; then
                    exit 10
                fi
                """,
                env=env,
            )
            elapsed = time.monotonic() - started

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("--timeout must be a positive integer", result.stdout)
            self.assertLess(elapsed, 2.5)


if __name__ == "__main__":
    unittest.main()
