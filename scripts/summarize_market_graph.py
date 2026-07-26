#!/usr/bin/env python3
"""Validate and summarize the persistent G1/G2/G3 graph qualification."""

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
TIERS = {
    "g1": {"documents": 10_000, "entities": 1_000, "queries": 10_000},
    "g2": {"documents": 100_000, "entities": 10_000, "queries": 100_000},
    "g3": {"documents": 1_000_000, "entities": 100_000, "queries": 100_000},
}
CONCURRENCIES = (1, 8, 32, 64)


def expected_points() -> dict[str, tuple[str, int, bool]]:
    return {
        f"{tier}-c{concurrency}{'-load' if concurrency == 1 else ''}": (
            tier,
            concurrency,
            concurrency == 1,
        )
        for tier in TIERS
        for concurrency in CONCURRENCIES
    }


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
        environment.get("report_type") == "akidb.market-graph-environment.v1",
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
    tier_bytes = environment.get("tier_bytes", {})
    for tier in TIERS:
        require(
            isinstance(tier_bytes.get(tier), int) and tier_bytes[tier] > 0,
            f"{tier}: persisted byte count is missing",
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

    summaries: list[dict[str, Any]] = []
    report_hashes: dict[str, str] = {}
    for name, (tier, concurrency, built) in points.items():
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
        spec = TIERS[tier]
        expected_nodes = spec["documents"] * 5 + spec["entities"]
        expected_edges = spec["documents"] * 8
        workload = report.get("workload", {})
        build = report.get("build", {})
        integrity = report.get("integrity", {})
        query = report.get("query", {})

        require(report.get("schema_version") == 2, f"{name}: report schema is not v2", failures)
        require(
            report.get("report_type") == "akidb.bounded-graph-benchmark.v2",
            f"{name}: report type is invalid",
            failures,
        )
        require(report.get("verdict", {}).get("status") == "pass", f"{name}: verdict failed", failures)
        require(workload.get("workspace") == "qualification", f"{name}: workspace differs", failures)
        require(
            workload.get("documents") == spec["documents"],
            f"{name}: document count differs",
            failures,
        )
        require(workload.get("chunks_per_document") == 4, f"{name}: chunk count differs", failures)
        require(workload.get("entities") == spec["entities"], f"{name}: entity count differs", failures)
        require(build.get("skipped") == (not built), f"{name}: build mode differs", failures)
        require(build.get("nodes") == expected_nodes, f"{name}: node count differs", failures)
        require(build.get("edges") == expected_edges, f"{name}: edge count differs", failures)
        require(
            isinstance(build.get("persisted_bytes"), int) and build["persisted_bytes"] > 0,
            f"{name}: persisted bytes are missing",
            failures,
        )
        if built:
            require(
                finite_number(build.get("nodes_per_second")) and build["nodes_per_second"] > 0,
                f"{name}: build throughput is invalid",
                failures,
            )
        for field in (
            "stats_match",
            "cross_workspace_rejected_atomically",
            "excessive_depth_rejected",
            "incident_edges_deleted",
        ):
            require(integrity.get(field) is True, f"{name}: integrity field {field} failed", failures)
        require(query.get("requested") == spec["queries"], f"{name}: query count differs", failures)
        require(query.get("succeeded") == spec["queries"], f"{name}: successes differ", failures)
        require(query.get("incorrect") == 0, f"{name}: incorrect answers observed", failures)
        require(query.get("errors") == 0, f"{name}: query errors observed", failures)
        require(query.get("concurrency") == concurrency, f"{name}: concurrency differs", failures)
        require(query.get("known_answer_accuracy") == 1.0, f"{name}: accuracy is not 1.0", failures)
        require(
            sum(query.get("operations", {}).values()) == spec["queries"],
            f"{name}: operation mix count differs",
            failures,
        )
        require(
            finite_number(query.get("latency", {}).get("p99_ms"))
            and query["latency"]["p99_ms"] <= args.max_p99_ms,
            f"{name}: p99 exceeds {args.max_p99_ms} ms",
            failures,
        )
        if name == "g2-c8":
            require(
                query.get("latency", {}).get("p99_ms", math.inf) <= args.max_g2_c8_p99_ms,
                f"{name}: p99 exceeds {args.max_g2_c8_p99_ms} ms",
                failures,
            )
        summaries.append(
            {
                "name": name,
                "tier": tier,
                "concurrency": concurrency,
                "nodes": expected_nodes,
                "edges": expected_edges,
                "qps": query.get("qps"),
                "p50_ms": query.get("latency", {}).get("p50_ms"),
                "p95_ms": query.get("latency", {}).get("p95_ms"),
                "p99_ms": query.get("latency", {}).get("p99_ms"),
                "build_nodes_per_second": build.get("nodes_per_second"),
                "persisted_bytes": build.get("persisted_bytes"),
            }
        )

    return result(
        args,
        failures,
        {
            "path": str(artifact),
            "sha256": artifact_sha256,
            "manifest": manifest,
        },
        summaries,
        {
            tier: {
                **spec,
                "nodes": spec["documents"] * 5 + spec["entities"],
                "edges": spec["documents"] * 8,
            }
            for tier, spec in TIERS.items()
        },
        {
            "path": environment_path.name,
            "sha256": sha256_file(environment_path) if environment_path.is_file() else None,
            "facts": environment,
        },
        report_hashes,
    )


def result(
    args: argparse.Namespace,
    failures: list[str],
    artifact: dict[str, Any],
    points: list[dict[str, Any]],
    tiers: dict[str, Any],
    environment: dict[str, Any],
    report_hashes: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": "akidb.market-graph-summary.v1",
        "generated_at_unix_ms": int(time.time() * 1000),
        "run_id": args.run_id,
        "gates": {
            "known_answer_accuracy": 1.0,
            "max_p99_ms": args.max_p99_ms,
            "max_g2_c8_p99_ms": args.max_g2_c8_p99_ms,
        },
        "artifact": artifact,
        "tiers": tiers,
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
    parser.add_argument("--max-p99-ms", type=float, default=250.0)
    parser.add_argument("--max-g2-c8-p99-ms", type=float, default=50.0)
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
