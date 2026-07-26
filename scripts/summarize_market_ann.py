#!/usr/bin/env python3
"""Validate and summarize an immutable AkiDB market ANN qualification run."""

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


def expected_points() -> dict[str, tuple[int, int, int | None]]:
    points: dict[str, tuple[int, int, int | None]] = {}
    for nprobe in (32, 64, 128, 256):
        for concurrency in (1, 8, 32, 64):
            suffix = "-load" if (nprobe, concurrency) == (32, 1) else ""
            points[f"k10-n{nprobe}-c{concurrency}{suffix}"] = (
                10,
                nprobe,
                None,
            )
    for nprobe in (64, 128, 256):
        points[f"k100-n{nprobe}-c8"] = (100, nprobe, None)
    points["k10-n256-c8-filter2"] = (10, 256, 2)
    points["k1-n256-c8-filter20"] = (1, 256, 20)
    points["k1-n256-c8-filter100"] = (1, 256, 100)
    return points


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
    return isinstance(value, (int, float)) and math.isfinite(value)


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


def dataset_signature(report: dict[str, Any]) -> dict[str, Any]:
    dataset = report["dataset"]
    return {
        "name": dataset["name"],
        "dimensions": dataset["dimensions"],
        "train_vectors": dataset["train_vectors"],
        "query_vectors": dataset["query_vectors"],
        "ground_truth_width": dataset["ground_truth_width"],
        "metric": dataset["metric"],
        "train_sha256": dataset["train"]["sha256"],
        "train_bytes": dataset["train"]["bytes"],
        "queries_sha256": dataset["queries"]["sha256"],
        "queries_bytes": dataset["queries"]["bytes"],
        "neighbors_sha256": dataset["neighbors"]["sha256"],
        "neighbors_bytes": dataset["neighbors"]["bytes"],
    }


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    failures: list[str] = []
    points = expected_points()
    evidence_dir = args.evidence_dir.resolve()
    artifact = args.artifact.resolve()

    require(RUN_ID_RE.fullmatch(args.run_id) is not None, "run id is not canonical", failures)
    require(artifact.is_file(), f"artifact is missing: {artifact}", failures)
    if failures:
        return result(args, failures, {}, [], {}, {})

    artifact_sha256 = sha256_file(artifact)
    try:
        manifest = artifact_manifest(artifact)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        failures.append(f"artifact manifest is invalid: {error}")
        manifest = {}

    environment_path = evidence_dir / f"{args.run_id}-environment.json"
    process_path = evidence_dir / f"{args.run_id}-process-results.json"
    require(environment_path.is_file(), "environment evidence is missing", failures)
    require(process_path.is_file(), "process evidence is missing", failures)
    environment = read_json(environment_path) if environment_path.is_file() else {}
    processes = read_json(process_path) if process_path.is_file() else []

    require(
        environment.get("report_type") == "akidb.market-ann-environment.v1",
        "environment report type is invalid",
        failures,
    )
    require(environment.get("run_id") == args.run_id, "environment run id differs", failures)
    require(
        environment.get("release_id") == manifest.get("release_id"),
        "environment and artifact release ids differ",
        failures,
    )
    require(
        environment.get("artifact_sha256") == artifact_sha256,
        "environment and controller artifact checksums differ",
        failures,
    )
    require(
        manifest.get("target") == "x86_64-unknown-linux-gnu",
        "artifact target is not Linux AMD64",
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
        isinstance(environment.get("config_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", environment["config_sha256"]) is not None,
        "benchmark configuration checksum is missing",
        failures,
    )
    require(
        environment.get("max_postfilter_candidates") == 16_384,
        "bounded post-filter candidate window differs from 16384",
        failures,
    )

    process_by_name = {
        item.get("name"): item
        for item in processes
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    require(set(process_by_name) == set(points), "process matrix is incomplete or unexpected", failures)
    for name in points:
        require(
            process_by_name.get(name, {}).get("rc") == 0,
            f"{name}: benchmark process did not exit successfully",
            failures,
        )

    reports: list[dict[str, Any]] = []
    report_hashes: dict[str, str] = {}
    signature: dict[str, Any] | None = None
    for name, (top_k, nprobe, filter_modulus) in points.items():
        path = evidence_dir / f"{args.run_id}-{name}.json"
        if not path.is_file():
            failures.append(f"{name}: report is missing")
            continue
        report_hashes[path.name] = sha256_file(path)
        try:
            report = read_json(path)
        except (OSError, json.JSONDecodeError) as error:
            failures.append(f"{name}: invalid JSON: {error}")
            continue

        require(report.get("schema_version") == 2, f"{name}: report schema is not v2", failures)
        require(
            report.get("report_type") == "akidb.market-ann-benchmark.v2",
            f"{name}: report type is invalid",
            failures,
        )
        require(report.get("verdict", {}).get("status") == "pass", f"{name}: verdict failed", failures)
        query = report.get("query", {})
        require(query.get("top_k") == top_k, f"{name}: top-k differs", failures)
        require(query.get("nprobe") == nprobe, f"{name}: nprobe differs", failures)
        require(query.get("unique_queries") == 10_000, f"{name}: unique query count differs", failures)
        require(query.get("measurement_rounds") == 3, f"{name}: measurement rounds differ", failures)
        require(query.get("requested") == 30_000, f"{name}: measured request count differs", failures)
        require(query.get("succeeded") == 30_000, f"{name}: successful request count differs", failures)
        require(query.get("failed") == 0, f"{name}: query failures observed", failures)
        require(query.get("filter_violations") == 0, f"{name}: filter violations observed", failures)
        for field in (
            "result_count_violations",
            "duplicate_results",
            "unparseable_results",
            "invalid_scores",
        ):
            require(query.get(field) == 0, f"{name}: {field} observed", failures)
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
        if filter_modulus is None:
            require(
                finite_number(query.get("qps")) and query["qps"] >= args.min_unfiltered_qps,
                f"{name}: QPS is below {args.min_unfiltered_qps}",
                failures,
            )
        filter_report = report.get("filter", {})
        require(
            filter_report.get("enabled") == (filter_modulus is not None),
            f"{name}: filter enablement differs",
            failures,
        )
        require(
            filter_report.get("modulus") == filter_modulus,
            f"{name}: filter modulus differs",
            failures,
        )

        current_signature = dataset_signature(report)
        if signature is None:
            signature = current_signature
        require(current_signature == signature, f"{name}: dataset identity differs", failures)
        load = report.get("load", {})
        if name.endswith("-load"):
            require(load.get("inserted") == 1_000_000, f"{name}: import count differs", failures)
            require(load.get("failed") == 0, f"{name}: import failures observed", failures)
            require(
                finite_number(load.get("vectors_per_second"))
                and load["vectors_per_second"] >= args.min_import_vps,
                f"{name}: import throughput is below {args.min_import_vps} vectors/s",
                failures,
            )
            require(
                report.get("post_load_settle_seconds", 0) >= 60,
                f"{name}: post-load quiescence window is too short",
                failures,
            )
        else:
            require(load.get("skipped") is True, f"{name}: unexpectedly reloaded corpus", failures)

        reports.append(
            {
                "name": name,
                "top_k": top_k,
                "nprobe": nprobe,
                "concurrency": query.get("concurrency"),
                "filter_modulus": filter_modulus,
                "recall_at_k": query.get("recall_at_k"),
                "qps": query.get("qps"),
                "p50_ms": query.get("latency", {}).get("p50_ms"),
                "p95_ms": query.get("latency", {}).get("p95_ms"),
                "p99_ms": query.get("latency", {}).get("p99_ms"),
            }
        )

    if signature is not None:
        require(signature["name"] == "sift-128-euclidean", "dataset is not SIFT1M", failures)
        require(signature["dimensions"] == 128, "dataset dimensions differ", failures)
        require(signature["train_vectors"] == 1_000_000, "dataset train count differs", failures)
        require(signature["query_vectors"] == 10_000, "dataset query count differs", failures)
        require(signature["ground_truth_width"] >= 100, "ground truth width is below 100", failures)
        require(signature["metric"] == "l2", "dataset metric is not L2", failures)

    eligible = [
        point
        for point in reports
        if point["filter_modulus"] is None
        and point["top_k"] == 10
        and finite_number(point["recall_at_k"])
        and point["recall_at_k"] >= args.min_recall
        and finite_number(point["p99_ms"])
        and point["p99_ms"] <= min(args.max_p99_ms, 100.0)
    ]
    recommended = max(eligible, key=lambda point: point["qps"], default=None)
    return result(
        args,
        failures,
        {
            "path": str(artifact),
            "sha256": artifact_sha256,
            "manifest": manifest,
        },
        reports,
        signature or {},
        {
            "path": environment_path.name,
            "sha256": sha256_file(environment_path) if environment_path.is_file() else None,
            "facts": environment,
            "recommended_operating_point": recommended,
        },
        report_hashes,
    )


def result(
    args: argparse.Namespace,
    failures: list[str],
    artifact: dict[str, Any],
    points: list[dict[str, Any]],
    dataset: dict[str, Any],
    environment: dict[str, Any],
    report_hashes: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": "akidb.market-ann-summary.v1",
        "generated_at_unix_ms": int(time.time() * 1000),
        "run_id": args.run_id,
        "gates": {
            "min_recall": args.min_recall,
            "min_unfiltered_qps": args.min_unfiltered_qps,
            "max_p99_ms": args.max_p99_ms,
            "min_import_vps": args.min_import_vps,
        },
        "artifact": artifact,
        "dataset": dataset,
        "environment": environment,
        "points": points,
        "evidence_sha256": report_hashes or {},
        "verdict": {
            "status": "pass" if not failures else "fail",
            "failures": failures,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--min-recall", type=float, default=0.95)
    parser.add_argument("--min-unfiltered-qps", type=float, default=100.0)
    parser.add_argument("--max-p99-ms", type=float, default=250.0)
    parser.add_argument("--min-import-vps", type=float, default=500.0)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = summarize(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "verdict": report["verdict"]["status"],
                "failures": report["verdict"]["failures"],
            }
        )
    )
    return 0 if report["verdict"]["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
