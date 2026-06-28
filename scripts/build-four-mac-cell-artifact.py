#!/usr/bin/env python3
"""Build a four-Mac Thunderbolt cell validation artifact from measured inputs."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any


class BuildError(Exception):
    """Raised when input measurements cannot form a validation artifact."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise BuildError(message)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except OSError as exc:
        raise BuildError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise BuildError(f"{path} is not valid JSON: {exc}") from exc


def benchmark_summary_from_artifact(path: Path, flag_name: str) -> tuple[float, float, float, dict[str, Any]]:
    artifact = read_json(path)
    require(isinstance(artifact, dict), f"{flag_name} must be a JSON object")
    search = artifact.get("search")
    require(isinstance(search, dict), f"{flag_name} missing search object")
    qps = require_float_value(
        search.get("throughput_queries_per_sec"),
        f"{flag_name} search.throughput_queries_per_sec",
        min_value=0.000001,
    )
    latency = search.get("latency")
    require(isinstance(latency, dict), f"{flag_name} missing search.latency object")
    p95_us = require_float_value(
        latency.get("p95_us"),
        f"{flag_name} search.latency.p95_us",
        min_value=0.0,
    )
    p99_us = require_float_value(
        latency.get("p99_us"),
        f"{flag_name} search.latency.p99_us",
        min_value=0.0,
    )
    require(
        p99_us >= p95_us,
        f"{flag_name} search.latency.p99_us must be >= p95_us",
    )

    dataset = artifact.get("dataset")
    software = artifact.get("software")
    reference = {
        "artifact": str(path),
        "git_commit": software.get("git_commit") if isinstance(software, dict) else None,
        "dimension": dataset.get("dimension") if isinstance(dataset, dict) else None,
        "vectors": dataset.get("vectors") if isinstance(dataset, dict) else None,
        "queries": search.get("queries_requested"),
        "top_k": search.get("top_k"),
        "nprobe": search.get("nprobe"),
        "concurrency": search.get("concurrency"),
    }
    return qps, p95_us / 1000.0, p99_us / 1000.0, reference


def one_mac_qps_from_artifact(path: Path) -> tuple[float, dict[str, Any]]:
    qps, _p95_ms, _p99_ms, reference = benchmark_summary_from_artifact(path, "--one-mac-artifact")
    return qps, reference


def cell_benchmark_from_artifact(path: Path) -> tuple[float, float, float, dict[str, Any]]:
    return benchmark_summary_from_artifact(path, "--cell-artifact")


def require_objects(value: Any, name: str) -> list[dict[str, Any]]:
    require(isinstance(value, list), f"{name} must be a JSON list")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(value):
        require(isinstance(item, dict), f"{name}[{idx}] must be an object")
        out.append(item)
    return out


def require_bool(value: Any, path: str) -> bool:
    require(isinstance(value, bool), f"{path} must be a boolean")
    return value


def require_int_value(value: Any, path: str, *, min_value: int | None = None) -> int:
    require(isinstance(value, int) and not isinstance(value, bool), f"{path} must be an integer")
    if min_value is not None:
        require(value >= min_value, f"{path} must be >= {min_value}, got {value}")
    return value


def require_float_value(value: Any, path: str, *, min_value: float | None = None) -> float:
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{path} must be numeric")
    out = float(value)
    require(math.isfinite(out), f"{path} must be finite")
    if min_value is not None:
        require(out >= min_value, f"{path} must be >= {min_value}, got {out}")
    return out


def normalize_nodes(raw: Any) -> list[dict[str, Any]]:
    nodes = require_objects(raw, "nodes")
    require(len(nodes) == 4, f"nodes must contain exactly 4 items, got {len(nodes)}")
    required = {"id", "host", "arch", "mac_model", "memory_bytes", "role", "healthy"}
    seen: set[str] = set()
    for idx, node in enumerate(nodes):
        missing = required - set(node)
        require(not missing, f"nodes[{idx}] missing fields: {sorted(missing)}")
        node_id = str(node["id"])
        require(node_id not in seen, f"duplicate node id: {node_id}")
        seen.add(node_id)
        node["id"] = node_id
        node["host"] = str(node["host"])
        node["arch"] = str(node["arch"])
        node["mac_model"] = str(node["mac_model"])
        node["memory_bytes"] = require_int_value(
            node["memory_bytes"],
            f"nodes[{idx}].memory_bytes",
            min_value=1,
        )
        node["role"] = str(node["role"])
        node["healthy"] = require_bool(node["healthy"], f"nodes[{idx}].healthy")
    return nodes


def normalize_links(raw: Any, node_ids: set[str]) -> list[dict[str, Any]]:
    links = require_objects(raw, "links")
    required = {
        "from",
        "to",
        "transport",
        "healthy",
        "latency_p95_us",
        "bandwidth_gbps",
        "packet_loss_percent",
    }
    for idx, link in enumerate(links):
        missing = required - set(link)
        require(not missing, f"links[{idx}] missing fields: {sorted(missing)}")
        source = str(link["from"])
        target = str(link["to"])
        require(source in node_ids, f"links[{idx}].from {source} is not in nodes")
        require(target in node_ids, f"links[{idx}].to {target} is not in nodes")
        require(source != target, f"links[{idx}] endpoints must be distinct")
        link["from"] = source
        link["to"] = target
        link["transport"] = str(link["transport"])
        link["healthy"] = require_bool(link["healthy"], f"links[{idx}].healthy")
        link["latency_p95_us"] = require_float_value(
            link["latency_p95_us"],
            f"links[{idx}].latency_p95_us",
            min_value=0.0,
        )
        link["bandwidth_gbps"] = require_float_value(
            link["bandwidth_gbps"],
            f"links[{idx}].bandwidth_gbps",
            min_value=0.0,
        )
        link["packet_loss_percent"] = require_float_value(
            link["packet_loss_percent"],
            f"links[{idx}].packet_loss_percent",
            min_value=0.0,
        )
    return links


def normalize_failure_tests(raw: Any) -> list[dict[str, Any]]:
    tests = require_objects(raw, "failure_tests")
    required = {"kind", "passed", "observed_status", "recovery_time_ms"}
    for idx, test in enumerate(tests):
        missing = required - set(test)
        require(not missing, f"failure_tests[{idx}] missing fields: {sorted(missing)}")
        test["kind"] = str(test["kind"])
        test["passed"] = require_bool(test["passed"], f"failure_tests[{idx}].passed")
        test["observed_status"] = str(test["observed_status"])
        test["recovery_time_ms"] = require_float_value(
            test["recovery_time_ms"],
            f"failure_tests[{idx}].recovery_time_ms",
            min_value=0.0,
        )
    return tests


def build_placement(
    *,
    collection_name: str,
    replication_factor: int,
    shard_count: int,
    nodes: list[dict[str, Any]],
) -> dict[str, Any]:
    require(replication_factor >= 2, "--replication-factor must be >= 2")
    require(shard_count >= 1, "--shards must be >= 1")
    require(
        replication_factor <= len(nodes),
        "--replication-factor cannot exceed node count",
    )
    node_ids = [str(node["id"]) for node in nodes]
    shards = []
    for index in range(shard_count):
        primary = node_ids[index % len(node_ids)]
        replicas = [
            node_ids[(index + offset) % len(node_ids)]
            for offset in range(1, replication_factor)
        ]
        shards.append(
            {
                "id": f"shard-{index}",
                "primary": primary,
                "replicas": replicas,
            }
        )
    return {
        "collections": [
            {
                "name": collection_name,
                "replication_factor": replication_factor,
                "shards": shards,
            }
        ]
    }


def build_artifact(args: argparse.Namespace) -> dict[str, Any]:
    if args.input:
        measurements = read_json(args.input)
        require(isinstance(measurements, dict), "--input must be a JSON object")
        require("nodes" in measurements, "--input missing nodes")
        require("links" in measurements, "--input missing links")
        require("failure_tests" in measurements, "--input missing failure_tests")
        raw_nodes = measurements["nodes"]
        raw_links = measurements["links"]
        raw_failure_tests = measurements["failure_tests"]
    else:
        raw_nodes = read_json(args.nodes)
        raw_links = read_json(args.links)
        raw_failure_tests = read_json(args.failure_tests)

    nodes = normalize_nodes(raw_nodes)
    node_ids = {str(node["id"]) for node in nodes}
    links = normalize_links(raw_links, node_ids)
    failure_tests = normalize_failure_tests(raw_failure_tests)
    one_mac_qps = args.one_mac_qps
    one_mac_reference: dict[str, Any] | None = None
    if args.one_mac_artifact:
        one_mac_qps, one_mac_reference = one_mac_qps_from_artifact(args.one_mac_artifact)
    cell_qps = args.cell_qps
    cell_p95_ms = args.cell_p95_ms
    cell_p99_ms = args.cell_p99_ms
    cell_reference: dict[str, Any] | None = None
    if args.cell_artifact:
        cell_qps, cell_p95_ms, cell_p99_ms, cell_reference = cell_benchmark_from_artifact(
            args.cell_artifact
        )
    throughput_ratio = cell_qps / one_mac_qps
    benchmark = {
        "one_mac_qps": one_mac_qps,
        "cell_qps": cell_qps,
        "throughput_ratio": throughput_ratio,
        "cell_p95_ms": cell_p95_ms,
        "cell_p99_ms": cell_p99_ms,
    }
    if one_mac_reference:
        benchmark["one_mac_reference"] = one_mac_reference
    if cell_reference:
        benchmark["cell_reference"] = cell_reference

    return {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "cell": {
            "id": args.cell_id,
            "nodes": nodes,
        },
        "deployment": {
            "orchestrator": args.orchestrator,
        },
        "network": {
            "links": links,
        },
        "placement": build_placement(
            collection_name=args.collection,
            replication_factor=args.replication_factor,
            shard_count=args.shards,
            nodes=nodes,
        ),
        "failure_tests": failure_tests,
        "benchmark": benchmark,
    }


def write_template(path: Path) -> None:
    template = {
        "nodes": [
            {
                "id": "mac-1",
                "host": "mac-1.local",
                "arch": "arm64",
                "mac_model": "Mac15,9",
                "memory_bytes": 68719476736,
                "role": "voter",
                "healthy": True,
            },
            {
                "id": "mac-2",
                "host": "mac-2.local",
                "arch": "arm64",
                "mac_model": "Mac15,9",
                "memory_bytes": 68719476736,
                "role": "voter",
                "healthy": True,
            },
            {
                "id": "mac-3",
                "host": "mac-3.local",
                "arch": "arm64",
                "mac_model": "Mac15,9",
                "memory_bytes": 68719476736,
                "role": "voter",
                "healthy": True,
            },
            {
                "id": "mac-4",
                "host": "mac-4.local",
                "arch": "arm64",
                "mac_model": "Mac15,9",
                "memory_bytes": 68719476736,
                "role": "learner",
                "healthy": True,
            },
        ],
        "links": [
            {
                "from": source,
                "to": target,
                "transport": "thunderbolt",
                "healthy": True,
                "latency_p95_us": 120.0,
                "bandwidth_gbps": 20.0,
                "packet_loss_percent": 0.0,
            }
            for source, target in [
                ("mac-1", "mac-2"),
                ("mac-1", "mac-3"),
                ("mac-1", "mac-4"),
                ("mac-2", "mac-3"),
                ("mac-2", "mac-4"),
                ("mac-3", "mac-4"),
            ]
        ],
        "failure_tests": [
            {
                "kind": "node_loss",
                "passed": True,
                "observed_status": "degraded",
                "recovery_time_ms": 500.0,
            },
            {
                "kind": "link_loss",
                "passed": True,
                "observed_status": "degraded",
                "recovery_time_ms": 250.0,
            },
        ],
    }
    path.write_text(json.dumps(template, indent=2) + "\n")


def validate(output: Path, allow_heterogeneous: bool) -> None:
    command = [
        sys.executable,
        str(Path(__file__).with_name("validate-four-mac-cell.py")),
        str(output),
    ]
    if allow_heterogeneous:
        command.append("--allow-heterogeneous")
    subprocess.run(command, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-template", type=Path, help="Write input template JSON and exit")
    parser.add_argument("--input", type=Path, help="Combined JSON input with nodes, links, and failure_tests")
    parser.add_argument("--output", type=Path, help="Output artifact path")
    parser.add_argument("--nodes", type=Path, help="JSON list of four node inventory objects")
    parser.add_argument("--links", type=Path, help="JSON list of six Thunderbolt link measurements")
    parser.add_argument("--failure-tests", type=Path, help="JSON list of failure-test results")
    parser.add_argument("--cell-id", default="cell-a")
    parser.add_argument("--orchestrator", default="none")
    parser.add_argument("--collection", default="default")
    parser.add_argument("--replication-factor", type=int, default=2)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--one-mac-qps", type=float)
    parser.add_argument("--one-mac-artifact", type=Path, help="Read one-Mac QPS from a benchmark artifact")
    parser.add_argument("--cell-artifact", type=Path, help="Read cell QPS/P95/P99 from a benchmark artifact")
    parser.add_argument("--cell-qps", type=float)
    parser.add_argument("--cell-p95-ms", type=float)
    parser.add_argument("--cell-p99-ms", type=float)
    parser.add_argument("--validate", action="store_true", help="Run validate-four-mac-cell.py after writing")
    parser.add_argument("--allow-heterogeneous", action="store_true", help="Forward to validator")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.write_template:
            write_template(args.write_template)
            print(args.write_template)
            return 0

        required_paths = {"--output": args.output}
        if args.input is None:
            required_paths.update(
                {
                    "--nodes": args.nodes,
                    "--links": args.links,
                    "--failure-tests": args.failure_tests,
                }
            )
        for name, value in required_paths.items():
            require(value is not None, f"{name} is required unless --write-template is used")
        if args.input is not None:
            for name, value in {
                "--nodes": args.nodes,
                "--links": args.links,
                "--failure-tests": args.failure_tests,
            }.items():
                require(value is None, f"{name} cannot be combined with --input")
        require(
            (args.one_mac_qps is None) != (args.one_mac_artifact is None),
            "provide exactly one of --one-mac-qps or --one-mac-artifact",
        )
        cell_metrics = [args.cell_qps, args.cell_p95_ms, args.cell_p99_ms]
        has_cell_metrics = all(value is not None for value in cell_metrics)
        has_partial_cell_metrics = any(value is not None for value in cell_metrics)
        require(
            (not has_partial_cell_metrics or has_cell_metrics)
            and (has_cell_metrics != (args.cell_artifact is not None)),
            "provide either --cell-artifact or all of --cell-qps/--cell-p95-ms/--cell-p99-ms",
        )
        if args.one_mac_qps is not None:
            require(args.one_mac_qps > 0, "--one-mac-qps must be > 0")
        if has_cell_metrics:
            require(args.cell_qps > 0, "--cell-qps must be > 0")
            require(args.cell_p95_ms >= 0, "--cell-p95-ms must be >= 0")
            require(args.cell_p99_ms >= args.cell_p95_ms, "--cell-p99-ms must be >= --cell-p95-ms")

        artifact = build_artifact(args)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(artifact, indent=2) + "\n")
        print(args.output)
        if args.validate:
            validate(args.output, args.allow_heterogeneous)
    except (BuildError, OSError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
