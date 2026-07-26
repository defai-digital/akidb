#!/usr/bin/env python3
"""Validate AkiDB search quality during paced insert/update/delete cycles."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from pathlib import Path
from typing import Any


FRACTIONS = {"mixed10": 0.10, "mixed50": 0.50}
ZERO_FIELDS = (
    "failed",
    "result_count_violations",
    "duplicate_results",
    "unparseable_results",
    "invalid_scores",
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--min-recall", type=float, default=0.95)
    parser.add_argument("--max-p99-ms", type=float, default=250.0)
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


def summarize(args: argparse.Namespace) -> dict[str, Any]:
    failures: list[str] = []
    require(args.duration_seconds >= 60, "mixed duration is shorter than 60s", failures)
    require(
        finite(args.min_recall) and 0 < args.min_recall <= 1,
        "minimum recall is invalid",
        failures,
    )
    require(
        finite(args.max_p99_ms) and args.max_p99_ms > 0,
        "maximum p99 is invalid",
        failures,
    )
    load_path = args.evidence_dir / f"{args.run_id}-k10-n32-c1-load.json"
    process_path = args.evidence_dir / f"{args.run_id}-mixed-process-results.json"
    try:
        load = read_json(load_path)
        processes = read_json(process_path)
        signature = dataset_signature(load)
        import_vps = float(load["load"]["vectors_per_second"])
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        failures.append(f"base import evidence is invalid: {error}")
        load = {}
        processes = []
        signature = {}
        import_vps = 0.0
    require(finite(import_vps) and import_vps > 0, "import throughput is invalid", failures)
    process_by_name = {
        item.get("name"): item
        for item in processes
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    require(
        set(process_by_name) == set(FRACTIONS),
        "mixed process matrix is incomplete or unexpected",
        failures,
    )

    reports = {}
    hashes = {}
    for name, fraction in FRACTIONS.items():
        path = args.evidence_dir / f"{args.run_id}-{name}.json"
        if not path.is_file():
            failures.append(f"{name}: report is missing")
            continue
        hashes[path.name] = sha256(path)
        try:
            report = read_json(path)
        except (OSError, json.JSONDecodeError) as error:
            failures.append(f"{name}: report is invalid: {error}")
            continue
        reports[name] = report
        require(
            process_by_name.get(name, {}).get("rc") == 0,
            f"{name}: process failed",
            failures,
        )
        require(
            report.get("report_type") == "akidb.market-ann-benchmark.v2",
            f"{name}: report type differs",
            failures,
        )
        require(
            report.get("verdict", {}).get("status") == "pass",
            f"{name}: benchmark verdict failed",
            failures,
        )
        try:
            require(
                dataset_signature(report) == signature,
                f"{name}: dataset identity differs",
                failures,
            )
        except KeyError:
            failures.append(f"{name}: dataset identity is incomplete")
        require(report.get("load", {}).get("skipped") is True, f"{name}: reloaded data", failures)
        query = report.get("query", {})
        require(query.get("unique_queries") == 10_000, f"{name}: query set differs", failures)
        require(query.get("measurement_rounds") == 1, f"{name}: preflight rounds differ", failures)
        require(query.get("requested") == 10_000, f"{name}: preflight count differs", failures)
        require(query.get("succeeded") == 10_000, f"{name}: preflight failures", failures)
        require(
            finite(query.get("recall_at_k"))
            and query["recall_at_k"] >= args.min_recall,
            f"{name}: preflight recall is below {args.min_recall}",
            failures,
        )
        mixed = report.get("mixed")
        require(isinstance(mixed, dict), f"{name}: mixed report is missing", failures)
        if not isinstance(mixed, dict):
            continue
        expected_rate = max(1, math.floor(import_vps * fraction))
        require(
            mixed.get("duration_seconds") == args.duration_seconds,
            f"{name}: duration differs",
            failures,
        )
        require(
            mixed.get("requested_cycle_qps") == expected_rate,
            f"{name}: requested rate is not {fraction:.0%} of import",
            failures,
        )
        mutation = mixed.get("mutation", {})
        expected_cycles = expected_rate * args.duration_seconds
        require(
            mutation.get("requested_cycles") == expected_cycles,
            f"{name}: requested cycle count differs",
            failures,
        )
        require(
            mutation.get("completed_cycles") == expected_cycles,
            f"{name}: mutation cycles are incomplete",
            failures,
        )
        for field in (
            "failed_cycles",
            "insert_failures",
            "update_failures",
            "delete_failures",
        ):
            require(mutation.get(field) == 0, f"{name}: {field} observed", failures)
        require(
            finite(mutation.get("cycles_per_second"))
            and mutation["cycles_per_second"] >= expected_rate * 0.90,
            f"{name}: achieved cycle rate is below 90% of target",
            failures,
        )
        search = mixed.get("search", {})
        require(
            isinstance(search.get("requested"), int) and search["requested"] > 0,
            f"{name}: no concurrent searches were measured",
            failures,
        )
        require(
            search.get("succeeded") == search.get("requested"),
            f"{name}: concurrent search count differs",
            failures,
        )
        for field in ZERO_FIELDS:
            require(search.get(field) == 0, f"{name}: search {field} observed", failures)
        require(
            finite(search.get("recall_at_k"))
            and search["recall_at_k"] >= args.min_recall,
            f"{name}: concurrent recall is below {args.min_recall}",
            failures,
        )
        require(
            finite(search.get("latency", {}).get("p99_ms"))
            and search["latency"]["p99_ms"] <= args.max_p99_ms,
            f"{name}: concurrent p99 exceeds {args.max_p99_ms}ms",
            failures,
        )
        before = mixed.get("health_before", {})
        after = mixed.get("health_after", {})
        require(
            before.get("active_vectors") == after.get("active_vectors") == 1_000_000,
            f"{name}: active vector counts do not reconcile",
            failures,
        )
        require(
            after.get("healthy") is True and after.get("ready") is True,
            f"{name}: server is not ready after cleanup",
            failures,
        )

    return {
        "schema_version": 1,
        "report_type": "akidb.market-ann-mixed-summary.v1",
        "generated_at_unix_ms": time.time_ns() // 1_000_000,
        "run_id": args.run_id,
        "import_vectors_per_second": import_vps,
        "fractions": FRACTIONS,
        "duration_seconds": args.duration_seconds,
        "report_sha256": hashes,
        "reports": reports,
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
