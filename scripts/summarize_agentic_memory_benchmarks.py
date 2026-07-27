#!/usr/bin/env python3
"""Fail-closed aggregation for authoritative Memory Linux AMD64 evidence."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import random
import re
import statistics
import time
from pathlib import Path
from typing import Any

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
EXPECTED_REPORT_TYPE = "akidb_authoritative_memory_systems_benchmark"
REQUIRED_METRICS = (
    "akidb_memory_commit_total",
    "akidb_memory_projection_applied_sequence",
    "akidb_memory_projection_lag_sequences",
    "akidb_memory_recall_latency_seconds",
    "akidb_memory_recall_snapshot_total",
    "akidb_memory_authorization_decision_total",
)
SUMMARY_METRICS = {
    "commit_throughput_per_second": ("commit", "throughput_per_second"),
    "commit_p50_us": (
        "commit",
        "acknowledgement_through_visible_latency",
        "p50_us",
    ),
    "commit_p95_us": (
        "commit",
        "acknowledgement_through_visible_latency",
        "p95_us",
    ),
    "commit_p99_us": (
        "commit",
        "acknowledgement_through_visible_latency",
        "p99_us",
    ),
    "recall_throughput_per_second": ("recall", "throughput_per_second"),
    "recall_p50_us": ("recall", "latency", "p50_us"),
    "recall_p95_us": ("recall", "latency", "p95_us"),
    "recall_p99_us": ("recall", "latency", "p99_us"),
    "disk_bytes_delta": ("resources", "disk_bytes_delta"),
    "peak_observed_server_rss_bytes": (
        "resources",
        "peak_observed_server_rss_bytes",
    ),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def nested_value(value: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for component in path:
        if not isinstance(current, dict):
            return None
        current = current.get(component)
    return current


def nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("a percentile requires at least one value")
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def bootstrap_median_interval(values: list[float]) -> tuple[float, float]:
    """Return a deterministic 95% nonparametric interval for the median."""
    ordered = sorted(values)
    if not ordered:
        raise ValueError("a confidence interval requires at least one value")
    sample_count = len(ordered)
    medians: list[float] = []
    if sample_count <= 6:
        for indexes in itertools.product(range(sample_count), repeat=sample_count):
            medians.append(
                float(statistics.median(ordered[index] for index in indexes))
            )
    else:
        seed = int.from_bytes(
            hashlib.sha256(
                json.dumps(ordered, separators=(",", ":")).encode()
            ).digest()[:8],
            "big",
        )
        generator = random.Random(seed)
        for _ in range(20_000):
            medians.append(
                float(
                    statistics.median(
                        generator.choice(ordered) for _ in range(sample_count)
                    )
                )
            )
    return nearest_rank(medians, 0.025), nearest_rank(medians, 0.975)


def distribution(values: list[float]) -> dict[str, Any]:
    lower, upper = bootstrap_median_interval(values)
    return {
        "runs": len(values),
        "minimum": min(values),
        "median": float(statistics.median(values)),
        "median_bootstrap_95_percent_interval": [lower, upper],
        "maximum": max(values),
        "run_values": values,
    }


def parse_checksum_manifest(
    path: Path, evidence_dir: Path, failures: list[str]
) -> dict[str, str]:
    expected_names = {"report.json", "metrics.prom", "server.log"}
    entries: dict[str, str] = {}
    try:
        lines = path.read_text().splitlines()
    except OSError as error:
        failures.append(f"{path}: cannot read checksum manifest: {error}")
        return entries
    for line in lines:
        fields = line.split()
        if len(fields) != 2:
            failures.append(f"{path}: malformed checksum line")
            continue
        digest, name = fields
        name = name.removeprefix("*")
        if name not in expected_names or name in entries:
            failures.append(f"{path}: unexpected or duplicate checksum target {name}")
            continue
        if SHA256_RE.fullmatch(digest) is None:
            failures.append(f"{path}: invalid SHA-256 for {name}")
            continue
        entries[name] = digest
    require(
        set(entries) == expected_names,
        f"{path}: checksum targets are incomplete",
        failures,
    )
    for name, expected in entries.items():
        target = evidence_dir / name
        if not target.is_file():
            failures.append(f"{target}: evidence file is missing")
            continue
        if sha256_file(target) != expected:
            failures.append(f"{target}: checksum mismatch")
    return entries


def load_report(path: Path, failures: list[str]) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"{path}: invalid report JSON: {error}")
        return {}
    if not isinstance(value, dict):
        failures.append(f"{path}: report root is not an object")
        return {}
    return value


def validate_report(
    report: dict[str, Any],
    path: Path,
    expected_commit: str,
    expected_queries: int,
    expected_warmup_queries: int,
    expected_commit_concurrency: int,
    expected_query_concurrency: int,
    failures: list[str],
) -> None:
    prefix = str(path)
    configuration = report.get("configuration", {})
    software = report.get("software", {})
    hardware = report.get("hardware", {})
    capabilities = report.get("capabilities", {})
    commit = report.get("commit", {})
    recall = report.get("recall", {})
    resources = report.get("resources", {})
    verdict = report.get("verdict", {})
    versions = configuration.get("versions")
    run_id = configuration.get("run_id")
    host_label = configuration.get("host_label")
    namespace = configuration.get("namespace")

    require(report.get("schema_version") == 1, f"{prefix}: wrong schema", failures)
    require(
        report.get("report_type") == EXPECTED_REPORT_TYPE,
        f"{prefix}: wrong report type",
        failures,
    )
    require(
        software.get("git_commit") == expected_commit,
        f"{prefix}: source commit differs",
        failures,
    )
    require(
        software.get("git_status_available") is True,
        f"{prefix}: Git source status was unavailable",
        failures,
    )
    require(
        software.get("dirty_worktree") is False,
        f"{prefix}: source tree was dirty",
        failures,
    )
    require(
        isinstance(hardware.get("os"), str) and hardware["os"],
        f"{prefix}: OS description is missing",
        failures,
    )
    require(
        isinstance(hardware.get("kernel"), str)
        and hardware["kernel"].startswith("Linux "),
        f"{prefix}: kernel is not Linux",
        failures,
    )
    require(
        hardware.get("architecture") == "x86_64",
        f"{prefix}: architecture is not x86_64",
        failures,
    )
    require(
        isinstance(hardware.get("hostname"), str) and hardware["hostname"],
        f"{prefix}: hostname is missing",
        failures,
    )
    require(
        isinstance(hardware.get("machine_id_sha256"), str)
        and SHA256_RE.fullmatch(hardware["machine_id_sha256"]) is not None,
        f"{prefix}: machine identity digest is missing or invalid",
        failures,
    )
    require(
        isinstance(hardware.get("logical_cores"), int)
        and not isinstance(hardware.get("logical_cores"), bool)
        and hardware["logical_cores"] >= 1,
        f"{prefix}: logical core count is invalid",
        failures,
    )
    require(
        isinstance(hardware.get("memory_bytes"), int)
        and not isinstance(hardware.get("memory_bytes"), bool)
        and hardware["memory_bytes"] > 0,
        f"{prefix}: memory size is invalid",
        failures,
    )

    require(
        isinstance(versions, int) and not isinstance(versions, bool) and versions > 0,
        f"{prefix}: version count is invalid",
        failures,
    )
    require(
        isinstance(run_id, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id),
        f"{prefix}: run ID is invalid",
        failures,
    )
    require(
        isinstance(host_label, str)
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", host_label),
        f"{prefix}: host label is invalid",
        failures,
    )
    require(
        namespace == f"benchmark/{run_id}",
        f"{prefix}: namespace is not bound to run ID",
        failures,
    )
    require(
        configuration.get("workspace") == "memory-benchmark"
        and configuration.get("purpose") == "memory-benchmark",
        f"{prefix}: benchmark scope differs",
        failures,
    )
    require(
        configuration.get("queries") == expected_queries
        and configuration.get("warmup_queries") == expected_warmup_queries,
        f"{prefix}: query or warmup count differs",
        failures,
    )
    require(
        configuration.get("commit_concurrency") == expected_commit_concurrency
        and configuration.get("query_concurrency") == expected_query_concurrency,
        f"{prefix}: concurrency differs",
        failures,
    )
    require(
        configuration.get("top_k") == 10
        and configuration.get("context_tokens") == 256
        and configuration.get("token_source") == "file",
        f"{prefix}: fixed benchmark settings differ",
        failures,
    )
    if isinstance(run_id, str) and isinstance(versions, int):
        expected_dataset = hashlib.sha256(
            (
                "akidb-memory-bench-v1\0memory-benchmark\0"
                f"benchmark/{run_id}\0memory-benchmark\0{run_id}\0{versions}"
            ).encode()
        ).hexdigest()
        require(
            report.get("dataset_sha256") == expected_dataset,
            f"{prefix}: dataset digest differs",
            failures,
        )

    required_rpcs = {"Remember", "Recall"}
    require(
        capabilities.get("profile_status") == "EXPERIMENTAL",
        f"{prefix}: profile is not experimental",
        failures,
    )
    require(
        capabilities.get("workspace_topology")
        == "ONE_AUTHORITATIVE_WORKSPACE_PER_PROCESS",
        f"{prefix}: workspace topology differs",
        failures,
    )
    require(
        required_rpcs.issubset(set(capabilities.get("supported_rpcs", []))),
        f"{prefix}: required RPC capability is missing",
        failures,
    )
    require(
        "SYNCED" in capabilities.get("durability_modes", []),
        f"{prefix}: synced durability is missing",
        failures,
    )
    retention = capabilities.get("retention_policy", {})
    require(
        isinstance(retention, dict)
        and retention.get("zero_means_indefinite") is True
        and retention.get("finite_windows_enforced") is False
        and all(
            retention.get(name) == 0
            for name in (
                "raw_event_seconds",
                "memory_version_seconds",
                "compiler_artifact_seconds",
                "index_artifact_seconds",
                "audit_seconds",
                "snapshot_seconds",
            )
        ),
        f"{prefix}: retention declaration differs",
        failures,
    )

    require(
        commit.get("requested") == versions
        and commit.get("succeeded") == versions
        and commit.get("failed") == 0
        and commit.get("maximum_visibility_lag_sequences") == 0
        and commit.get("errors") == [],
        f"{prefix}: commit correctness gate failed",
        failures,
    )
    require(
        commit.get("first_commit_sequence") == 1
        and commit.get("last_commit_sequence") == versions,
        f"{prefix}: fresh canonical sequence range differs",
        failures,
    )
    commit_latency = commit.get("acknowledgement_through_visible_latency", {})
    require(
        isinstance(commit_latency, dict)
        and commit_latency.get("count") == versions
        and isinstance(commit_latency.get("samples_us"), list)
        and len(commit_latency["samples_us"]) == versions,
        f"{prefix}: commit latency distribution is incomplete",
        failures,
    )
    require(
        recall.get("requested") == expected_queries
        and recall.get("succeeded") == expected_queries
        and recall.get("incorrect") == 0
        and recall.get("failed") == 0
        and recall.get("errors") == [],
        f"{prefix}: known-answer recall gate failed",
        failures,
    )
    recall_latency = recall.get("latency", {})
    require(
        isinstance(recall_latency, dict)
        and recall_latency.get("count") == expected_queries
        and isinstance(recall_latency.get("samples_us"), list)
        and len(recall_latency["samples_us"]) == expected_queries,
        f"{prefix}: recall latency distribution is incomplete",
        failures,
    )
    for metric_name, metric_path in SUMMARY_METRICS.items():
        require(
            finite_number(nested_value(report, metric_path))
            and nested_value(report, metric_path) >= 0,
            f"{prefix}: {metric_name} is invalid",
            failures,
        )
    require(
        verdict.get("status") == "PASS" and verdict.get("failures") == [],
        f"{prefix}: benchmark verdict did not pass",
        failures,
    )
    require(
        isinstance(resources.get("disk_bytes_before"), int)
        and isinstance(resources.get("disk_bytes_after_queries"), int)
        and resources["disk_bytes_after_queries"] >= resources["disk_bytes_before"],
        f"{prefix}: disk observations are invalid",
        failures,
    )
    require(
        resources.get("server_rss_sample_interval_ms") == 100
        and isinstance(resources.get("server_rss_sample_count"), int)
        and not isinstance(resources.get("server_rss_sample_count"), bool)
        and resources["server_rss_sample_count"] >= 1,
        f"{prefix}: continuous RSS sampling evidence is missing",
        failures,
    )


def failed_summary(args: argparse.Namespace, failures: list[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": "akidb.authoritative-memory-amd64-summary.v1",
        "generated_at_unix_ms": int(time.time() * 1000),
        "source_commit": args.expected_git_commit,
        "qualification_profile": {
            "expected_versions": args.expected_versions,
            "runs_per_size": args.runs_per_size,
            "queries": args.queries,
            "warmup_queries": args.warmup_queries,
            "commit_concurrency": args.commit_concurrency,
            "query_concurrency": args.query_concurrency,
        },
        "hosts": [],
        "runs": [],
        "sizes": {},
        "verdict": {"status": "FAIL", "failures": failures},
    }


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    failures: list[str] = []
    root = args.evidence_dir.resolve()
    require(root.is_dir(), "evidence directory is missing", failures)
    require(
        GIT_COMMIT_RE.fullmatch(args.expected_git_commit) is not None,
        "expected Git commit is not a canonical full SHA",
        failures,
    )
    require(
        args.runs_per_size >= 1
        and args.queries >= 1
        and args.warmup_queries >= 0
        and args.commit_concurrency >= 1
        and args.query_concurrency >= 1,
        "qualification counts must be nonnegative and runs/concurrency positive",
        failures,
    )
    expected_versions = sorted(set(args.expected_versions))
    require(
        expected_versions == args.expected_versions
        and all(value > 0 for value in expected_versions),
        "expected version sizes must be unique, positive, and sorted",
        failures,
    )
    if failures:
        return failed_summary(args, failures)

    paths = sorted(root.rglob("report.json"))
    expected_total = len(expected_versions) * args.runs_per_size
    require(
        len(paths) == expected_total,
        f"expected {expected_total} reports, found {len(paths)}",
        failures,
    )
    reports: list[tuple[Path, dict[str, Any]]] = []
    run_ids: set[str] = set()
    for path in paths:
        parent = path.parent
        manifest = parent / "SHA256SUMS"
        metrics_path = parent / "metrics.prom"
        log_path = parent / "server.log"
        require(manifest.is_file(), f"{manifest}: checksum manifest is missing", failures)
        require(metrics_path.is_file(), f"{metrics_path}: metrics are missing", failures)
        require(log_path.is_file(), f"{log_path}: server log is missing", failures)
        if manifest.is_file():
            parse_checksum_manifest(manifest, parent, failures)
        report = load_report(path, failures)
        if not report:
            continue
        validate_report(
            report,
            path,
            args.expected_git_commit,
            args.queries,
            args.warmup_queries,
            args.commit_concurrency,
            args.query_concurrency,
            failures,
        )
        run_id = nested_value(report, ("configuration", "run_id"))
        if isinstance(run_id, str):
            require(run_id not in run_ids, f"duplicate run ID {run_id}", failures)
            run_ids.add(run_id)
            if metrics_path.is_file():
                try:
                    metrics = metrics_path.read_text()
                except OSError as error:
                    failures.append(f"{metrics_path}: cannot read metrics: {error}")
                else:
                    require(
                        f"benchmark/{run_id}" not in metrics,
                        f"{metrics_path}: namespace leaked into metrics",
                        failures,
                    )
                    for metric in REQUIRED_METRICS:
                        require(
                            metric in metrics,
                            f"{metrics_path}: required metric {metric} is missing",
                            failures,
                        )
        reports.append((path, report))

    grouped: dict[int, list[tuple[Path, dict[str, Any]]]] = {
        value: [] for value in expected_versions
    }
    for path, report in reports:
        versions = nested_value(report, ("configuration", "versions"))
        if versions not in grouped:
            failures.append(f"{path}: unexpected version size {versions}")
        else:
            grouped[versions].append((path, report))
    for versions, group in grouped.items():
        require(
            len(group) == args.runs_per_size,
            f"{versions} versions: expected {args.runs_per_size} runs, found {len(group)}",
            failures,
        )

    host_labels = sorted(
        {
            nested_value(report, ("configuration", "host_label"))
            for _, report in reports
            if isinstance(
                nested_value(report, ("configuration", "host_label")), str
            )
        }
    )
    if args.expected_hosts:
        require(
            host_labels == sorted(args.expected_hosts),
            f"host set differs: expected {sorted(args.expected_hosts)}, found {host_labels}",
            failures,
        )
    else:
        require(
            len(host_labels) >= args.minimum_host_count,
            f"expected at least {args.minimum_host_count} hosts, found {len(host_labels)}",
            failures,
        )
    machine_ids = {
        nested_value(report, ("hardware", "machine_id_sha256"))
        for _, report in reports
        if isinstance(nested_value(report, ("hardware", "machine_id_sha256")), str)
    }
    for host_label in host_labels:
        label_machine_ids = {
            nested_value(report, ("hardware", "machine_id_sha256"))
            for _, report in reports
            if nested_value(report, ("configuration", "host_label")) == host_label
        }
        require(
            len(label_machine_ids) == 1,
            f"host label {host_label} maps to multiple machine identities",
            failures,
        )
    require(
        len(machine_ids) == len(host_labels),
        "qualification host labels do not map one-to-one to machine identities",
        failures,
    )
    if failures:
        return failed_summary(args, failures)

    run_entries = []
    size_entries: dict[str, Any] = {}
    for versions in expected_versions:
        group = sorted(
            grouped[versions],
            key=lambda item: nested_value(item[1], ("configuration", "run_id")),
        )
        metrics: dict[str, Any] = {}
        for metric_name, metric_path in SUMMARY_METRICS.items():
            values = [
                float(nested_value(report, metric_path)) for _, report in group
            ]
            metrics[metric_name] = distribution(values)
        size_entries[str(versions)] = {
            "runs": len(group),
            "total_commits": versions * len(group),
            "total_measured_recalls": args.queries * len(group),
            "commit_failures": 0,
            "maximum_visibility_lag_sequences": 0,
            "incorrect_recalls": 0,
            "recall_failures": 0,
            "metrics": metrics,
        }
        for path, report in group:
            run_entries.append(
                {
                    "relative_report_path": str(path.relative_to(root)),
                    "report_sha256": sha256_file(path),
                    "run_id": nested_value(report, ("configuration", "run_id")),
                    "versions": versions,
                    "host_label": nested_value(
                        report, ("configuration", "host_label")
                    ),
                    "hostname": nested_value(report, ("hardware", "hostname")),
                    "generated_at_unix_ms": report.get("generated_at_unix_ms"),
                    "dataset_sha256": report.get("dataset_sha256"),
                }
            )

    return {
        "schema_version": 1,
        "report_type": "akidb.authoritative-memory-amd64-summary.v1",
        # Derive this from immutable inputs so re-aggregation is byte-stable.
        "generated_at_unix_ms": max(
            report.get("generated_at_unix_ms", 0) for _, report in reports
        ),
        "source_commit": args.expected_git_commit,
        "qualification_profile": {
            "expected_versions": expected_versions,
            "runs_per_size": args.runs_per_size,
            "queries": args.queries,
            "warmup_queries": args.warmup_queries,
            "commit_concurrency": args.commit_concurrency,
            "query_concurrency": args.query_concurrency,
            "top_k": 10,
            "context_tokens": 256,
            "build_profile": "release",
            "platform": "Linux x86_64",
        },
        "hosts": [
            {
                "host_label": host_label,
                "reported_hostnames": sorted(
                    {
                        nested_value(report, ("hardware", "hostname"))
                        for _, report in reports
                        if nested_value(report, ("configuration", "host_label"))
                        == host_label
                    }
                ),
                "machine_id_sha256": sorted(
                    {
                        nested_value(report, ("hardware", "machine_id_sha256"))
                        for _, report in reports
                        if nested_value(report, ("configuration", "host_label"))
                        == host_label
                    }
                ),
            }
            for host_label in host_labels
        ],
        "runs": run_entries,
        "sizes": size_entries,
        "verdict": {"status": "PASS", "failures": []},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--expected-git-commit", required=True)
    parser.add_argument(
        "--expected-versions", type=int, nargs="+", default=[1000, 10_000, 100_000]
    )
    parser.add_argument("--runs-per-size", type=int, default=5)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--warmup-queries", type=int, default=20)
    parser.add_argument("--commit-concurrency", type=int, default=8)
    parser.add_argument("--query-concurrency", type=int, default=8)
    parser.add_argument("--minimum-host-count", type=int, default=4)
    parser.add_argument("--expected-host", dest="expected_hosts", action="append")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = summarize(args)
    payload = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(payload)
    print(payload, end="")
    return 0 if summary["verdict"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
