import argparse
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "summarize_agentic_memory_benchmarks.py"
)
SPEC = importlib.util.spec_from_file_location(
    "summarize_agentic_memory_benchmarks", MODULE_PATH
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

COMMIT = "1" * 40
METRICS = "\n".join(MODULE.REQUIRED_METRICS) + "\n"


def dataset_sha(run_id, versions):
    return hashlib.sha256(
        (
            "akidb-memory-bench-v1\0memory-benchmark\0"
            f"benchmark/{run_id}\0memory-benchmark\0{run_id}\0{versions}"
        ).encode()
    ).hexdigest()


def report(run_id, versions, hostname):
    commit_samples = list(range(1, versions + 1))
    recall_samples = list(range(1, 11))
    return {
        "schema_version": 1,
        "report_type": MODULE.EXPECTED_REPORT_TYPE,
        "generated_at_unix_ms": 1,
        "dataset_sha256": dataset_sha(run_id, versions),
        "hardware": {
            "hostname": hostname,
            "machine_id_sha256": hashlib.sha256(hostname.encode()).hexdigest(),
            "os": "Linux",
            "kernel": "Linux test",
            "architecture": "x86_64",
            "cpu": "test",
            "logical_cores": 8,
            "memory_bytes": 32_000_000_000,
        },
        "software": {
            "akidb_version": "0.10.0",
            "git_commit": COMMIT,
            "rustc": "rustc test",
            "git_status_available": True,
            "dirty_worktree": False,
        },
        "configuration": {
            "server": "http://127.0.0.1:50051",
            "workspace": "memory-benchmark",
            "namespace": f"benchmark/{run_id}",
            "purpose": "memory-benchmark",
            "run_id": run_id,
            "host_label": hostname,
            "versions": versions,
            "commit_concurrency": 8,
            "queries": 10,
            "warmup_queries": 2,
            "query_concurrency": 8,
            "top_k": 10,
            "context_tokens": 256,
            "timeout_seconds": 60,
            "token_source": "file",
            "data_dir": "/tmp/data",
            "server_pid": 10,
        },
        "capabilities": {
            "profile_status": "EXPERIMENTAL",
            "supported_rpcs": ["Remember", "Recall"],
            "durability_modes": ["SYNCED"],
            "active_projection_recipes": ["preview-bounded-bm25-v1"],
            "workspace_topology": "ONE_AUTHORITATIVE_WORKSPACE_PER_PROCESS",
            "active_projection_manifest_sha256": "a" * 64,
            "tokenizer_artifact_id": "tokenizer",
            "server_build_id": "build",
            "retention_policy": {
                "raw_event_seconds": 0,
                "memory_version_seconds": 0,
                "compiler_artifact_seconds": 0,
                "index_artifact_seconds": 0,
                "audit_seconds": 0,
                "snapshot_seconds": 0,
                "zero_means_indefinite": True,
                "finite_windows_enforced": False,
            },
        },
        "commit": {
            "requested": versions,
            "succeeded": versions,
            "failed": 0,
            "wall_time_ms": 10,
            "throughput_per_second": 100.0,
            "first_commit_sequence": 1,
            "last_commit_sequence": versions,
            "maximum_visibility_lag_sequences": 0,
            "acknowledgement_through_visible_latency": {
                "count": versions,
                "min_us": 1,
                "mean_us": 1,
                "p50_us": 1,
                "p95_us": 1,
                "p99_us": 1,
                "max_us": versions,
                "samples_us": commit_samples,
            },
            "errors": [],
        },
        "recall": {
            "requested": 10,
            "succeeded": 10,
            "incorrect": 0,
            "failed": 0,
            "wall_time_ms": 10,
            "throughput_per_second": 200.0,
            "latency": {
                "count": 10,
                "min_us": 1,
                "mean_us": 5,
                "p50_us": 5,
                "p95_us": 10,
                "p99_us": 10,
                "max_us": 10,
                "samples_us": recall_samples,
            },
            "errors": [],
        },
        "resources": {
            "disk_bytes_before": 1,
            "disk_bytes_after_commits": 100,
            "disk_bytes_after_queries": 101,
            "disk_bytes_delta": 100,
            "server_rss_bytes_before": 10,
            "server_rss_bytes_after_commits": 20,
            "server_rss_bytes_after_queries": 30,
            "peak_observed_server_rss_bytes": 30,
            "server_rss_sample_interval_ms": 100,
            "server_rss_sample_count": 2,
        },
        "verdict": {"status": "PASS", "failures": []},
    }


def write_evidence(root, run_id, versions, hostname):
    directory = root / run_id
    directory.mkdir()
    report_path = directory / "report.json"
    metrics_path = directory / "metrics.prom"
    log_path = directory / "server.log"
    report_path.write_text(json.dumps(report(run_id, versions, hostname)) + "\n")
    metrics_path.write_text(METRICS)
    log_path.write_text("server ready\n")
    entries = []
    for path in (report_path, metrics_path, log_path):
        entries.append(f"{MODULE.sha256_file(path)}  {path.name}")
    (directory / "SHA256SUMS").write_text("\n".join(entries) + "\n")


def arguments(root):
    return argparse.Namespace(
        evidence_dir=root,
        expected_git_commit=COMMIT,
        expected_versions=[2],
        runs_per_size=2,
        queries=10,
        warmup_queries=2,
        commit_concurrency=8,
        query_concurrency=8,
        minimum_host_count=2,
        expected_hosts=None,
    )


class AgenticMemoryBenchmarkSummaryTests(unittest.TestCase):
    def test_complete_independent_runs_pass_and_retain_raw_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_evidence(root, "run-one", 2, "host-1")
            write_evidence(root, "run-two", 2, "host-2")
            summary = MODULE.summarize(arguments(root))
            repeated = MODULE.summarize(arguments(root))
            self.assertEqual(summary["verdict"]["status"], "PASS")
            self.assertEqual(summary, repeated)
            self.assertEqual(
                [host["host_label"] for host in summary["hosts"]],
                ["host-1", "host-2"],
            )
            throughput = summary["sizes"]["2"]["metrics"][
                "commit_throughput_per_second"
            ]
            self.assertEqual(throughput["run_values"], [100.0, 100.0])
            self.assertEqual(throughput["median"], 100.0)

    def test_dirty_revision_and_checksum_mismatch_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_evidence(root, "run-one", 2, "host-1")
            write_evidence(root, "run-two", 2, "host-2")
            path = root / "run-one" / "report.json"
            value = json.loads(path.read_text())
            value["software"]["dirty_worktree"] = True
            path.write_text(json.dumps(value) + "\n")
            summary = MODULE.summarize(arguments(root))
            self.assertEqual(summary["verdict"]["status"], "FAIL")
            failures = "\n".join(summary["verdict"]["failures"])
            self.assertIn("checksum mismatch", failures)
            self.assertIn("source tree was dirty", failures)

    def test_namespace_in_metrics_and_missing_run_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_evidence(root, "run-one", 2, "host-1")
            metrics = root / "run-one" / "metrics.prom"
            metrics.write_text(METRICS + 'bad{namespace="benchmark/run-one"} 1\n')
            entries = []
            for name in ("report.json", "metrics.prom", "server.log"):
                path = root / "run-one" / name
                entries.append(f"{MODULE.sha256_file(path)}  {name}")
            (root / "run-one" / "SHA256SUMS").write_text(
                "\n".join(entries) + "\n"
            )
            summary = MODULE.summarize(arguments(root))
            self.assertEqual(summary["verdict"]["status"], "FAIL")
            failures = "\n".join(summary["verdict"]["failures"])
            self.assertIn("expected 2 reports, found 1", failures)
            self.assertIn("namespace leaked into metrics", failures)

    def test_bootstrap_interval_is_deterministic(self):
        first = MODULE.bootstrap_median_interval([1.0, 2.0, 3.0, 4.0, 5.0])
        second = MODULE.bootstrap_median_interval([5.0, 4.0, 3.0, 2.0, 1.0])
        self.assertEqual(first, second)
        self.assertLessEqual(first[0], 3.0)
        self.assertGreaterEqual(first[1], 3.0)


if __name__ == "__main__":
    unittest.main()
