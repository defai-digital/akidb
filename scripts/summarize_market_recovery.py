#!/usr/bin/env python3
"""Fail-closed summary for mutable AkiDB crash/restart qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import tarfile
import time
from pathlib import Path
from typing import Any

RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]{7,96}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ANN_POINTS = ("baseline", "after-crash", "after-graceful")
PROCESS_POINTS = ("baseline", "mutator", "verify", "after-crash", "after-graceful")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def zero_return_code(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value == 0


def read_evidence_json(
    path: Path, label: str, failures: list[str], default: Any
) -> Any:
    if not path.is_file():
        return default
    try:
        return read_json(path)
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"{label} evidence is invalid: {error}")
        return default


def artifact_manifest(path: Path) -> dict[str, Any]:
    with tarfile.open(path, "r:gz") as archive:
        members = {
            member.name.removeprefix("./"): member
            for member in archive.getmembers()
            if member.isfile()
        }
        member = members.get("manifest.json")
        if member is None:
            raise ValueError("artifact has no manifest.json")
        source = archive.extractfile(member)
        if source is None:
            raise ValueError("artifact manifest cannot be read")
        return json.load(source)


def parse_systemd_properties(lines: Any) -> dict[str, str]:
    if not isinstance(lines, list):
        return {}
    properties: dict[str, str] = {}
    for line in lines:
        if not isinstance(line, str) or "=" not in line:
            continue
        key, value = line.split("=", 1)
        properties[key] = value
    return properties


def ann_signature(report: dict[str, Any]) -> dict[str, Any]:
    dataset = report.get("dataset", {})
    return {
        "name": dataset.get("name"),
        "dimensions": dataset.get("dimensions"),
        "train_vectors": dataset.get("train_vectors"),
        "query_vectors": dataset.get("query_vectors"),
        "ground_truth_width": dataset.get("ground_truth_width"),
        "metric": dataset.get("metric"),
        "train_sha256": dataset.get("train", {}).get("sha256"),
        "train_bytes": dataset.get("train", {}).get("bytes"),
        "queries_sha256": dataset.get("queries", {}).get("sha256"),
        "queries_bytes": dataset.get("queries", {}).get("bytes"),
        "neighbors_sha256": dataset.get("neighbors", {}).get("sha256"),
        "neighbors_bytes": dataset.get("neighbors", {}).get("bytes"),
    }


def empty_result(args: argparse.Namespace, failures: list[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": "akidb.market-recovery-summary.v1",
        "generated_at_unix_ms": int(time.time() * 1000),
        "run_id": args.run_id,
        "source_ann_run_id": None,
        "artifact": {},
        "dataset": {},
        "recovery": {},
        "ann_points": [],
        "evidence_sha256": {},
        "verdict": {"status": "fail", "failures": failures},
    }


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    failures: list[str] = []
    evidence_dir = args.evidence_dir.resolve()
    artifact = args.artifact.resolve()
    source_summary_path = args.source_ann_summary.resolve()
    require(RUN_ID_RE.fullmatch(args.run_id) is not None, "run id is not canonical", failures)
    require(artifact.is_file(), "artifact is missing", failures)
    require(source_summary_path.is_file(), "source ANN summary is missing", failures)
    if failures:
        return empty_result(args, failures)

    artifact_sha256 = sha256_file(artifact)
    try:
        manifest = artifact_manifest(artifact)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        failures.append(f"artifact manifest is invalid: {error}")
        manifest = {}
    try:
        source_summary = read_json(source_summary_path)
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"source ANN summary is invalid: {error}")
        source_summary = {}

    require(
        source_summary.get("report_type") == "akidb.market-ann-summary.v1",
        "source ANN summary type is invalid",
        failures,
    )
    require(
        source_summary.get("verdict", {}).get("status") == "pass",
        "source ANN qualification did not pass",
        failures,
    )
    source_run_id = source_summary.get("run_id")
    require(
        isinstance(source_run_id, str) and RUN_ID_RE.fullmatch(source_run_id) is not None,
        "source ANN run id is invalid",
        failures,
    )
    require(
        source_summary.get("artifact", {}).get("sha256") == artifact_sha256,
        "source ANN and recovery artifacts differ",
        failures,
    )
    require(
        source_summary.get("artifact", {}).get("manifest", {}).get("release_id")
        == manifest.get("release_id"),
        "source ANN and recovery release ids differ",
        failures,
    )
    require(
        manifest.get("target") == "x86_64-unknown-linux-gnu",
        "artifact target is not Linux AMD64",
        failures,
    )

    paths = {
        "environment": evidence_dir / f"{args.run_id}-environment.json",
        "processes": evidence_dir / f"{args.run_id}-process-results.json",
        "mutate": evidence_dir / f"{args.run_id}-mutate.json",
        "verification": evidence_dir / f"{args.run_id}-verification.json",
        "journal": evidence_dir / f"{args.run_id}-journal.ndjson",
        "service_journal": evidence_dir / f"{args.run_id}-service-journal.log",
    }
    for name, path in paths.items():
        require(path.is_file(), f"{name} evidence is missing", failures)
    ann_paths = {
        name: evidence_dir / f"{args.run_id}-{name}.json" for name in ANN_POINTS
    }
    for name, path in ann_paths.items():
        require(path.is_file(), f"{name} ANN evidence is missing", failures)

    environment = read_evidence_json(
        paths["environment"], "environment", failures, {}
    )
    processes = read_evidence_json(paths["processes"], "process", failures, [])
    mutate = read_evidence_json(paths["mutate"], "mutation", failures, {})
    verification = read_evidence_json(
        paths["verification"], "verification", failures, {}
    )

    require(
        environment.get("report_type") == "akidb.market-recovery-environment.v1",
        "environment report type is invalid",
        failures,
    )
    require(environment.get("run_id") == args.run_id, "environment run id differs", failures)
    require(
        environment.get("source_market_run_id") == source_run_id,
        "environment source ANN run id differs",
        failures,
    )
    require(
        environment.get("release_id") == manifest.get("release_id"),
        "environment release id differs",
        failures,
    )
    require(
        environment.get("artifact_sha256") == artifact_sha256,
        "environment artifact checksum differs",
        failures,
    )
    require(environment.get("distribution") == "Ubuntu", "server is not Ubuntu", failures)
    try:
        ubuntu_major = int(str(environment.get("distribution_version", "")).split(".", 1)[0])
    except ValueError:
        ubuntu_major = 0
    require(ubuntu_major >= 24, "Ubuntu version is older than 24.04", failures)
    require(environment.get("architecture") == "x86_64", "server is not AMD64", failures)
    require(
        isinstance(environment.get("processor_vcpus"), int)
        and environment["processor_vcpus"] >= 4,
        "server has fewer than four vCPUs",
        failures,
    )
    require(
        isinstance(environment.get("memory_mb"), int) and environment["memory_mb"] >= 8192,
        "server has less than 8 GiB RAM",
        failures,
    )
    require(
        isinstance(environment.get("source_data_bytes"), int)
        and environment["source_data_bytes"] > 0,
        "source data size is invalid",
        failures,
    )
    require(
        isinstance(environment.get("config_sha256"), str)
        and SHA256_RE.fullmatch(environment["config_sha256"]) is not None,
        "configuration checksum is invalid",
        failures,
    )
    crash_ms = environment.get("crash_recovery_ms")
    graceful_ms = environment.get("graceful_restart_ms")
    require(
        finite_number(crash_ms) and 0 < crash_ms <= args.max_crash_recovery_ms,
        f"crash recovery exceeds {args.max_crash_recovery_ms} ms",
        failures,
    )
    require(
        finite_number(graceful_ms) and 0 < graceful_ms <= args.max_graceful_restart_ms,
        f"graceful restart exceeds {args.max_graceful_restart_ms} ms",
        failures,
    )

    before = parse_systemd_properties(environment.get("service_before_crash"))
    after_crash = parse_systemd_properties(environment.get("service_after_crash"))
    after_graceful = parse_systemd_properties(environment.get("service_after_graceful"))
    try:
        before_pid = int(before.get("MainPID", "0"))
        crash_pid = int(after_crash.get("MainPID", "0"))
        graceful_pid = int(after_graceful.get("MainPID", "0"))
        before_restarts = int(before.get("NRestarts", "-1"))
        crash_restarts = int(after_crash.get("NRestarts", "-1"))
    except ValueError:
        before_pid = crash_pid = graceful_pid = 0
        before_restarts = crash_restarts = -1
    require(before_pid > 0, "pre-crash MainPID is invalid", failures)
    require(crash_pid > 0 and crash_pid != before_pid, "automatic restart PID did not change", failures)
    require(
        graceful_pid > 0 and graceful_pid != crash_pid,
        "graceful restart PID did not change",
        failures,
    )
    require(
        crash_restarts >= before_restarts + 1,
        "systemd did not record an automatic restart",
        failures,
    )
    require(
        before.get("InvocationID")
        and after_crash.get("InvocationID")
        and before["InvocationID"] != after_crash["InvocationID"],
        "crash restart invocation identity did not change",
        failures,
    )
    require(
        after_crash.get("InvocationID")
        and after_graceful.get("InvocationID")
        and after_crash["InvocationID"] != after_graceful["InvocationID"],
        "graceful restart invocation identity did not change",
        failures,
    )
    for name, properties in (
        ("after-crash", after_crash),
        ("after-graceful", after_graceful),
    ):
        require(properties.get("ActiveState") == "active", f"{name} service is not active", failures)
        require(properties.get("SubState") == "running", f"{name} service is not running", failures)

    process_by_name = {
        item.get("name"): item
        for item in processes
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    require(
        set(process_by_name) == set(PROCESS_POINTS),
        "recovery process evidence is incomplete or unexpected",
        failures,
    )
    for name in PROCESS_POINTS:
        require(
            zero_return_code(process_by_name.get(name, {}).get("rc")),
            f"{name} process did not exit successfully",
            failures,
        )

    require(
        mutate.get("report_type") == "akidb.market-recovery-mutate.v1",
        "mutation report type is invalid",
        failures,
    )
    require(mutate.get("run_id") == args.run_id, "mutation run id differs", failures)
    require(mutate.get("journal_failures") == 0, "mutation journal failures observed", failures)
    require(
        isinstance(mutate.get("rpc_failures"), int) and mutate["rpc_failures"] >= 1,
        "mutator did not observe the intentional crash",
        failures,
    )
    require(
        mutate.get("termination_reason") == "rpc_interruption",
        "mutator was not interrupted by the server crash",
        failures,
    )

    require(
        verification.get("report_type") == "akidb.market-recovery-verification.v1",
        "verification report type is invalid",
        failures,
    )
    require(
        verification.get("verdict", {}).get("status") == "pass",
        "recovery verification failed",
        failures,
    )
    require(
        verification.get("insert_acks", 0) >= args.min_insert_acks,
        "insert acknowledgement floor was not met",
        failures,
    )
    require(
        verification.get("update_acks", 0) >= args.min_update_acks,
        "update acknowledgement floor was not met",
        failures,
    )
    require(
        verification.get("delete_acks", 0) >= args.min_delete_acks,
        "delete acknowledgement floor was not met",
        failures,
    )
    require(
        verification.get("acknowledged_states_verified")
        == verification.get("allocated_cycles"),
        "not every allocated crash-boundary state was verified",
        failures,
    )
    require(verification.get("cleanup_requested") is True, "probe cleanup was not requested", failures)
    require(
        verification.get("health_after_cleanup", {}).get("active_vectors") == 1_000_000,
        "active-vector count did not return to the SIFT1M baseline",
        failures,
    )
    require(
        isinstance(verification.get("accepted_unacknowledged_advances"), int)
        and verification["accepted_unacknowledged_advances"] <= args.max_inflight_advances,
        "too many unacknowledged in-flight advances were accepted",
        failures,
    )
    if paths["journal"].is_file():
        require(
            verification.get("journal_sha256") == sha256_file(paths["journal"]),
            "verification and fetched journal hashes differ",
            failures,
        )

    ann_results: list[dict[str, Any]] = []
    signature: dict[str, Any] | None = None
    for name, path in ann_paths.items():
        if not path.is_file():
            continue
        try:
            report = read_json(path)
        except (OSError, json.JSONDecodeError) as error:
            failures.append(f"{name}: invalid ANN report: {error}")
            continue
        require(report.get("schema_version") == 2, f"{name}: ANN schema differs", failures)
        require(
            report.get("report_type") == "akidb.market-ann-benchmark.v2",
            f"{name}: ANN report type differs",
            failures,
        )
        require(
            report.get("verdict", {}).get("status") == "pass",
            f"{name}: ANN verdict failed",
            failures,
        )
        require(report.get("load", {}).get("skipped") is True, f"{name}: corpus was reloaded", failures)
        query = report.get("query", {})
        require(query.get("unique_queries") == 10_000, f"{name}: query set is incomplete", failures)
        require(query.get("measurement_rounds") == 1, f"{name}: rounds differ", failures)
        require(query.get("requested") == 10_000, f"{name}: request count differs", failures)
        require(query.get("succeeded") == 10_000, f"{name}: successful count differs", failures)
        require(query.get("failed") == 0, f"{name}: query failures observed", failures)
        require(
            finite_number(query.get("recall_at_k"))
            and query["recall_at_k"] >= args.min_recall,
            f"{name}: recall is below {args.min_recall}",
            failures,
        )
        require(
            finite_number(query.get("latency", {}).get("p99_ms"))
            and query["latency"]["p99_ms"] <= args.max_p99_ms,
            f"{name}: p99 exceeds {args.max_p99_ms} ms",
            failures,
        )
        for health_name in ("health_before", "health_after"):
            health_report = report.get(health_name, {})
            require(
                health_report.get("healthy") is True and health_report.get("ready") is True,
                f"{name}: {health_name} is not ready",
                failures,
            )
            require(
                health_report.get("active_vectors") == 1_000_000,
                f"{name}: {health_name} active count differs",
                failures,
            )
        current_signature = ann_signature(report)
        if signature is None:
            signature = current_signature
        require(current_signature == signature, f"{name}: dataset identity differs", failures)
        ann_results.append(
            {
                "name": name,
                "recall_at_k": query.get("recall_at_k"),
                "qps": query.get("qps"),
                "p99_ms": query.get("latency", {}).get("p99_ms"),
            }
        )

    source_dataset = source_summary.get("dataset", {})
    if signature is not None:
        require(signature == source_dataset, "recovery and source ANN datasets differ", failures)
        require(signature.get("name") == "sift-128-euclidean", "dataset is not SIFT1M", failures)
        require(signature.get("train_vectors") == 1_000_000, "SIFT1M row count differs", failures)

    evidence_hashes = {
        path.name: sha256_file(path)
        for path in [*paths.values(), *ann_paths.values()]
        if path.is_file()
    }
    return {
        "schema_version": 1,
        "report_type": "akidb.market-recovery-summary.v1",
        "generated_at_unix_ms": int(time.time() * 1000),
        "run_id": args.run_id,
        "source_ann_run_id": source_run_id,
        "gates": {
            "min_recall": args.min_recall,
            "max_p99_ms": args.max_p99_ms,
            "max_crash_recovery_ms": args.max_crash_recovery_ms,
            "max_graceful_restart_ms": args.max_graceful_restart_ms,
            "min_insert_acks": args.min_insert_acks,
            "min_update_acks": args.min_update_acks,
            "min_delete_acks": args.min_delete_acks,
            "max_inflight_advances": args.max_inflight_advances,
        },
        "artifact": {
            "path": str(artifact),
            "sha256": artifact_sha256,
            "manifest": manifest,
        },
        "dataset": signature or {},
        "recovery": {
            "crash_recovery_ms": crash_ms,
            "graceful_restart_ms": graceful_ms,
            "allocated_cycles": verification.get("allocated_cycles"),
            "insert_acks": verification.get("insert_acks"),
            "update_acks": verification.get("update_acks"),
            "delete_acks": verification.get("delete_acks"),
            "accepted_unacknowledged_advances": verification.get(
                "accepted_unacknowledged_advances"
            ),
        },
        "ann_points": ann_results,
        "evidence_sha256": evidence_hashes,
        "verdict": {
            "status": "pass" if not failures else "fail",
            "failures": failures,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-ann-summary", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--min-recall", type=float, default=0.95)
    parser.add_argument("--max-p99-ms", type=float, default=250.0)
    parser.add_argument("--max-crash-recovery-ms", type=float, default=900_000.0)
    parser.add_argument("--max-graceful-restart-ms", type=float, default=900_000.0)
    parser.add_argument("--min-insert-acks", type=int, default=100)
    parser.add_argument("--min-update-acks", type=int, default=50)
    parser.add_argument("--min-delete-acks", type=int, default=25)
    parser.add_argument("--max-inflight-advances", type=int, default=16)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = summarize(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.{time.time_ns()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(args.output)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["verdict"]["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
