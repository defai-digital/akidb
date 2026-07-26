#!/usr/bin/env python3
"""Fail-closed parity verdict for AkiDB, Milvus, and Weaviate SIFT1M runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import time
from pathlib import Path
from typing import Any


CORRECTNESS_FIELDS = (
    "failed",
    "filter_violations",
    "result_count_violations",
    "duplicate_results",
    "unparseable_results",
    "invalid_scores",
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aki-evidence-dir", type=Path, required=True)
    parser.add_argument("--aki-run-id", required=True)
    parser.add_argument("--milvus-report", type=Path, required=True)
    parser.add_argument("--milvus-environment", type=Path, required=True)
    parser.add_argument("--weaviate-report", type=Path, required=True)
    parser.add_argument("--weaviate-environment", type=Path, required=True)
    parser.add_argument("--min-recall", type=float, default=0.95)
    parser.add_argument("--min-qps-ratio", type=float, default=0.70)
    parser.add_argument("--max-p99-ratio", type=float, default=1.50)
    parser.add_argument("--max-build-ratio", type=float, default=2.00)
    parser.add_argument("--max-storage-ratio", type=float, default=2.00)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def dataset_signature(report: dict[str, Any]) -> dict[str, Any]:
    dataset = report["dataset"]
    return {
        "name": dataset["name"],
        "dimensions": dataset["dimensions"],
        "train_vectors": dataset["train_vectors"],
        "query_vectors": dataset["query_vectors"],
        "ground_truth_width": dataset["ground_truth_width"],
        "metric": dataset["metric"],
        "train": (dataset["train"]["bytes"], dataset["train"]["sha256"]),
        "queries": (dataset["queries"]["bytes"], dataset["queries"]["sha256"]),
        "neighbors": (
            dataset["neighbors"]["bytes"],
            dataset["neighbors"]["sha256"],
        ),
    }


def point_is_correct(point: dict[str, Any]) -> bool:
    return (
        point.get("requested") == 30_000
        and point.get("succeeded") == 30_000
        and point.get("unique_queries") == 10_000
        and point.get("measurement_rounds") == 3
        and point.get("concurrency") == 8
        and all(point.get(field) == 0 for field in CORRECTNESS_FIELDS)
        and finite(point.get("qps"))
        and finite(point.get("recall_at_k"))
        and finite(point.get("latency", {}).get("p99_ms"))
    )


def choose_point(
    engine: str,
    points: list[dict[str, Any]],
    min_recall: float,
    failures: list[str],
) -> dict[str, Any] | None:
    eligible = [
        point
        for point in points
        if point.get("top_k") == 10
        and not point.get("filter", {}).get("enabled", False)
        and point_is_correct(point)
        and point["recall_at_k"] >= min_recall
    ]
    require(bool(eligible), f"{engine}: no correct Recall@10 parity point", failures)
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda value: (value["qps"], -value["latency"]["p99_ms"]),
    )


def aki_reports(args: argparse.Namespace, failures: list[str]) -> list[dict[str, Any]]:
    paths = sorted(
        args.aki_evidence_dir.glob(f"{args.aki_run_id}-k10-n*-c8*.json")
    )
    reports = []
    for path in paths:
        if "filter" in path.name:
            continue
        try:
            report = read_json(path)
        except (OSError, json.JSONDecodeError) as error:
            failures.append(f"AkiDB report {path.name} is invalid: {error}")
            continue
        if report.get("report_type") != "akidb.market-ann-benchmark.v2":
            failures.append(f"AkiDB report {path.name} has the wrong type")
            continue
        query = dict(report.get("query", {}))
        query["search_ef"] = query.get("nprobe")
        query["filter"] = report.get("filter", {})
        reports.append(query)
    require(len(reports) == 4, "AkiDB c8 Recall@10 matrix is incomplete", failures)
    return reports


def competitor(
    expected_engine: str,
    report_path: Path,
    environment_path: Path,
    failures: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        report = read_json(report_path)
        environment = read_json(environment_path)
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"{expected_engine}: evidence cannot be read: {error}")
        return {}, {}
    require(
        report.get("report_type") == "competitor-ann-ground-truth",
        f"{expected_engine}: report type differs",
        failures,
    )
    require(
        report.get("engine") == expected_engine,
        f"{expected_engine}: report engine differs",
        failures,
    )
    require(
        report.get("verdict", {}).get("status") == "pass",
        f"{expected_engine}: driver verdict failed",
        failures,
    )
    require(
        environment.get("report_type") == "competitor-ann-environment.v1",
        f"{expected_engine}: environment type differs",
        failures,
    )
    require(
        environment.get("engine") == expected_engine,
        f"{expected_engine}: environment engine differs",
        failures,
    )
    require(
        environment.get("report_sha256") == sha256(report_path),
        f"{expected_engine}: environment does not bind the report hash",
        failures,
    )
    require(
        isinstance(environment.get("image_id"), str)
        and environment["image_id"].startswith("sha256:"),
        f"{expected_engine}: image ID is missing",
        failures,
    )
    require(
        isinstance(environment.get("repo_digests"), list)
        and any("@sha256:" in value for value in environment["repo_digests"]),
        f"{expected_engine}: immutable image digest is missing",
        failures,
    )
    require(
        isinstance(environment.get("index_bytes"), int)
        and environment["index_bytes"] > 0,
        f"{expected_engine}: index byte count is invalid",
        failures,
    )
    return report, environment


def filtered_points_pass(
    engine: str,
    points: list[dict[str, Any]],
    min_recall: float,
    failures: list[str],
) -> None:
    expected = {(10, 2), (1, 20), (1, 100)}
    seen = set()
    for point in points:
        modulus = point.get("filter", {}).get("modulus")
        key = (point.get("top_k"), modulus)
        if key not in expected:
            continue
        seen.add(key)
        require(point_is_correct(point), f"{engine}: filtered point {key} failed", failures)
        require(
            finite(point.get("recall_at_k"))
            and point["recall_at_k"] >= min_recall,
            f"{engine}: filtered point {key} recall is below {min_recall}",
            failures,
        )
    require(seen == expected, f"{engine}: filtered matrix is incomplete", failures)


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    failures: list[str] = []
    for value, name in (
        (args.min_recall, "min recall"),
        (args.min_qps_ratio, "minimum QPS ratio"),
        (args.max_p99_ratio, "maximum P99 ratio"),
        (args.max_build_ratio, "maximum build ratio"),
        (args.max_storage_ratio, "maximum storage ratio"),
    ):
        require(finite(value) and value > 0, f"{name} must be positive", failures)

    aki_environment_path = (
        args.aki_evidence_dir / f"{args.aki_run_id}-environment.json"
    )
    aki_load_path = (
        args.aki_evidence_dir
        / f"{args.aki_run_id}-k10-n32-c1-load.json"
    )
    try:
        aki_environment = read_json(aki_environment_path)
        aki_load = read_json(aki_load_path)
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"AkiDB base evidence cannot be read: {error}")
        aki_environment = {}
        aki_load = {}

    milvus, milvus_environment = competitor(
        "milvus",
        args.milvus_report,
        args.milvus_environment,
        failures,
    )
    weaviate, weaviate_environment = competitor(
        "weaviate",
        args.weaviate_report,
        args.weaviate_environment,
        failures,
    )
    aki_points = aki_reports(args, failures)
    milvus_points = milvus.get("points", [])
    weaviate_points = weaviate.get("points", [])
    filtered_points_pass("milvus", milvus_points, args.min_recall, failures)
    filtered_points_pass("weaviate", weaviate_points, args.min_recall, failures)

    signatures = []
    for label, report in (
        ("AkiDB", aki_load),
        ("Milvus", milvus),
        ("Weaviate", weaviate),
    ):
        try:
            signatures.append((label, dataset_signature(report)))
        except KeyError:
            failures.append(f"{label}: dataset identity is incomplete")
    if signatures:
        baseline = signatures[0][1]
        for label, signature in signatures[1:]:
            require(signature == baseline, f"{label}: dataset identity differs", failures)

    for label, environment in (
        ("Milvus", milvus_environment),
        ("Weaviate", weaviate_environment),
    ):
        require(
            environment.get("server_host") == aki_environment.get("server_host"),
            f"{label}: server host differs from AkiDB",
            failures,
        )
        require(
            environment.get("driver_host") == aki_environment.get("driver_host"),
            f"{label}: driver host differs from AkiDB",
            failures,
        )
        for key in (
            "distribution",
            "distribution_version",
            "architecture",
            "kernel",
            "processor_vcpus",
            "memory_mb",
        ):
            require(
                environment.get(key) == aki_environment.get(key),
                f"{label}: hardware field {key} differs from AkiDB",
                failures,
            )

    selected = {
        "akidb": choose_point("AkiDB", aki_points, args.min_recall, failures),
        "milvus": choose_point("Milvus", milvus_points, args.min_recall, failures),
        "weaviate": choose_point(
            "Weaviate", weaviate_points, args.min_recall, failures
        ),
    }

    measurements: dict[str, Any] = {}
    if all(selected.values()):
        competitor_qps = [
            selected["milvus"]["qps"],
            selected["weaviate"]["qps"],
        ]
        competitor_p99 = [
            selected["milvus"]["latency"]["p99_ms"],
            selected["weaviate"]["latency"]["p99_ms"],
        ]
        competitor_build_ms = [
            milvus["load"]["duration_ms"],
            weaviate["load"]["duration_ms"],
        ]
        competitor_storage = [
            milvus_environment["index_bytes"],
            weaviate_environment["index_bytes"],
        ]
        medians = {
            "qps": statistics.median(competitor_qps),
            "p99_ms": statistics.median(competitor_p99),
            "build_ms": statistics.median(competitor_build_ms),
            "index_bytes": statistics.median(competitor_storage),
        }
        ratios = {
            "qps": selected["akidb"]["qps"] / medians["qps"],
            "p99": selected["akidb"]["latency"]["p99_ms"] / medians["p99_ms"],
            "build": aki_load["load"]["duration_ms"] / medians["build_ms"],
            "storage": aki_environment["index_bytes"] / medians["index_bytes"],
        }
        require(
            ratios["qps"] >= args.min_qps_ratio,
            f"AkiDB QPS ratio {ratios['qps']:.4f} < {args.min_qps_ratio}",
            failures,
        )
        require(
            ratios["p99"] <= args.max_p99_ratio,
            f"AkiDB P99 ratio {ratios['p99']:.4f} > {args.max_p99_ratio}",
            failures,
        )
        require(
            ratios["build"] <= args.max_build_ratio,
            f"AkiDB build ratio {ratios['build']:.4f} > {args.max_build_ratio}",
            failures,
        )
        require(
            ratios["storage"] <= args.max_storage_ratio,
            f"AkiDB storage ratio {ratios['storage']:.4f} > {args.max_storage_ratio}",
            failures,
        )
        measurements = {"competitor_medians": medians, "ratios": ratios}

    return {
        "schema_version": 1,
        "report_type": "akidb.market-ann-parity-summary.v1",
        "generated_at_unix_ms": time.time_ns() // 1_000_000,
        "selection_rule": (
            "highest measured QPS at concurrency 8, top-k 10, "
            f"exact Recall@10 >= {args.min_recall}"
        ),
        "gates": {
            "min_recall": args.min_recall,
            "min_qps_ratio": args.min_qps_ratio,
            "max_p99_ratio": args.max_p99_ratio,
            "max_build_ratio": args.max_build_ratio,
            "max_storage_ratio": args.max_storage_ratio,
        },
        "selected_points": selected,
        "measurements": measurements,
        "evidence_sha256": {
            "aki_environment": (
                sha256(aki_environment_path)
                if aki_environment_path.is_file()
                else None
            ),
            "aki_load": sha256(aki_load_path) if aki_load_path.is_file() else None,
            "milvus_report": (
                sha256(args.milvus_report) if args.milvus_report.is_file() else None
            ),
            "milvus_environment": (
                sha256(args.milvus_environment)
                if args.milvus_environment.is_file()
                else None
            ),
            "weaviate_report": (
                sha256(args.weaviate_report)
                if args.weaviate_report.is_file()
                else None
            ),
            "weaviate_environment": (
                sha256(args.weaviate_environment)
                if args.weaviate_environment.is_file()
                else None
            ),
        },
        "verdict": {
            "status": "pass" if not failures else "fail",
            "failures": failures,
        },
    }


def main() -> None:
    args = arguments()
    result = summarize(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        json.dumps(
            {
                "status": result["verdict"]["status"],
                "failures": len(result["verdict"]["failures"]),
                "output": str(args.output),
            },
            separators=(",", ":"),
        )
    )
    if result["verdict"]["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
