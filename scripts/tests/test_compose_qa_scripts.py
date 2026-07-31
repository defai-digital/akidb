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
MINIO_SETUP = ROOT / "deploy" / "compose" / "minio" / "setup-minio.sh"


def write_executable(path: Path, contents: str) -> None:
    path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")
    path.chmod(0o755)


class ComposeQaScriptTests(unittest.TestCase):
    def test_minio_setup_uses_writable_config_and_registered_nats_target(
        self,
    ) -> None:
        compose = COMPOSE_FILE.read_text(encoding="utf-8")
        setup = MINIO_SETUP.read_text(encoding="utf-8")

        self.assertIn("MC_CONFIG_DIR: /tmp/.mc", compose)
        self.assertIn("MINIO_NOTIFY_NATS_ENABLE_PRIMARY", compose)
        self.assertIn("arn:minio:sqs::PRIMARY:nats", setup)
        self.assertNotIn("arn:minio:sqs::primary:nats", setup)

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
