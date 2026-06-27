#!/usr/bin/env python3
"""Validate a four-Mac Thunderbolt cell validation artifact."""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from pathlib import Path
from typing import Any


class ValidationError(Exception):
    """Raised when a cell artifact does not satisfy the requested gate."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def get_path(document: dict[str, Any], path: str, default: Any = None) -> Any:
    value: Any = document
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            if default is not None:
                return default
            raise ValidationError(f"missing required field: {path}")
        value = value[part]
    return value


def require_str(document: dict[str, Any], path: str) -> str:
    value = get_path(document, path)
    require(isinstance(value, str) and value, f"{path} must be a non-empty string")
    return value


def require_int(document: dict[str, Any], path: str, *, min_value: int | None = None) -> int:
    value = get_path(document, path)
    require(isinstance(value, int) and not isinstance(value, bool), f"{path} must be an integer")
    if min_value is not None:
        require(value >= min_value, f"{path} must be >= {min_value}, got {value}")
    return value


def require_float(document: dict[str, Any], path: str, *, min_value: float | None = None) -> float:
    value = get_path(document, path)
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{path} must be numeric")
    out = float(value)
    if min_value is not None:
        require(out >= min_value, f"{path} must be >= {min_value}, got {out}")
    return out


def require_list(document: dict[str, Any], path: str) -> list[Any]:
    value = get_path(document, path)
    require(isinstance(value, list), f"{path} must be a list")
    return value


def node_ids(nodes: list[dict[str, Any]]) -> set[str]:
    return {str(node["id"]) for node in nodes}


def validate_nodes(report: dict[str, Any], *, allow_heterogeneous: bool) -> list[dict[str, Any]]:
    raw_nodes = require_list(report, "cell.nodes")
    require(len(raw_nodes) == 4, f"cell.nodes must contain exactly 4 nodes, got {len(raw_nodes)}")
    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()

    for idx, node in enumerate(raw_nodes):
        require(isinstance(node, dict), f"cell.nodes[{idx}] must be an object")
        node_id = require_str(node, "id")
        require(node_id not in seen, f"duplicate node id: {node_id}")
        seen.add(node_id)
        require_str(node, "host")
        arch = require_str(node, "arch")
        require(arch in {"arm64", "aarch64"}, f"node {node_id} must be Apple Silicon arm64/aarch64, got {arch}")
        require_str(node, "mac_model")
        require_int(node, "memory_bytes", min_value=1)
        require(bool(get_path(node, "healthy")), f"node {node_id} must be healthy")
        role = require_str(node, "role")
        require(role in {"voter", "learner"}, f"node {node_id} role must be voter or learner")
        nodes.append(node)

    voters = [node for node in nodes if node["role"] == "voter"]
    learners = [node for node in nodes if node["role"] == "learner"]
    require(len(voters) == 3, f"cell must have exactly 3 metadata voters, got {len(voters)}")
    require(len(learners) == 1, f"cell must have exactly 1 learner/data-only node, got {len(learners)}")

    if not allow_heterogeneous:
        models = {node["mac_model"] for node in nodes}
        require(len(models) == 1, f"hot cell must use one Mac model, got {sorted(models)}")
        memories = [int(node["memory_bytes"]) for node in nodes]
        min_memory = min(memories)
        max_memory = max(memories)
        require(
            max_memory <= int(min_memory * 1.05),
            "hot cell memory must be within 5% across nodes unless --allow-heterogeneous is set",
        )

    return nodes


def validate_links(report: dict[str, Any], nodes: list[dict[str, Any]], args: argparse.Namespace) -> None:
    links = require_list(report, "network.links")
    ids = node_ids(nodes)
    seen_pairs: set[tuple[str, str]] = set()

    for idx, link in enumerate(links):
        require(isinstance(link, dict), f"network.links[{idx}] must be an object")
        source = require_str(link, "from")
        target = require_str(link, "to")
        require(source in ids, f"link source {source} is not a cell node")
        require(target in ids, f"link target {target} is not a cell node")
        require(source != target, "link endpoints must be distinct")
        pair = tuple(sorted((source, target)))
        seen_pairs.add(pair)
        transport = require_str(link, "transport").lower()
        require(transport == "thunderbolt", f"link {source}-{target} transport must be thunderbolt")
        require(bool(get_path(link, "healthy")), f"link {source}-{target} must be healthy")
        latency_us = require_float(link, "latency_p95_us", min_value=0.0)
        bandwidth_gbps = require_float(link, "bandwidth_gbps", min_value=0.0)
        loss = require_float(link, "packet_loss_percent", min_value=0.0)
        require(latency_us <= args.max_link_p95_us, f"link {source}-{target} P95 {latency_us}us exceeds {args.max_link_p95_us}us")
        require(bandwidth_gbps >= args.min_link_bandwidth_gbps, f"link {source}-{target} bandwidth {bandwidth_gbps}Gbps below {args.min_link_bandwidth_gbps}Gbps")
        require(loss <= args.max_packet_loss_percent, f"link {source}-{target} packet loss {loss}% exceeds {args.max_packet_loss_percent}%")

    expected_pairs = {tuple(sorted(pair)) for pair in itertools.combinations(ids, 2)}
    missing = expected_pairs - seen_pairs
    require(not missing, f"missing Thunderbolt links: {sorted('-'.join(pair) for pair in missing)}")


def validate_placement(report: dict[str, Any], nodes: list[dict[str, Any]]) -> None:
    collections = require_list(report, "placement.collections")
    ids = node_ids(nodes)
    require(collections, "placement.collections cannot be empty")

    for cidx, collection in enumerate(collections):
        require(isinstance(collection, dict), f"placement.collections[{cidx}] must be an object")
        name = require_str(collection, "name")
        rf = require_int(collection, "replication_factor", min_value=2)
        shards = require_list(collection, "shards")
        require(shards, f"collection {name} must include at least one shard")
        for sidx, shard in enumerate(shards):
            require(isinstance(shard, dict), f"collection {name} shard[{sidx}] must be an object")
            shard_id = require_str(shard, "id")
            primary = require_str(shard, "primary")
            replicas = get_path(shard, "replicas")
            require(primary in ids, f"shard {shard_id} primary {primary} is not a cell node")
            require(isinstance(replicas, list), f"shard {shard_id} replicas must be a list")
            require(len(replicas) >= rf - 1, f"shard {shard_id} needs at least {rf - 1} replicas")
            placement_nodes = [primary] + [str(replica) for replica in replicas]
            require(len(set(placement_nodes)) == len(placement_nodes), f"shard {shard_id} primary/replicas must be on distinct nodes")
            for replica in replicas:
                require(str(replica) in ids, f"shard {shard_id} replica {replica} is not a cell node")


def validate_failure_tests(report: dict[str, Any]) -> None:
    tests = require_list(report, "failure_tests")
    required = {"node_loss", "link_loss"}
    seen: set[str] = set()
    for idx, test in enumerate(tests):
        require(isinstance(test, dict), f"failure_tests[{idx}] must be an object")
        kind = require_str(test, "kind")
        seen.add(kind)
        require(bool(get_path(test, "passed")), f"failure test {kind} must pass")
        status = require_str(test, "observed_status")
        require(status in {"healthy", "degraded"}, f"failure test {kind} observed_status must be healthy or degraded")
        require_float(test, "recovery_time_ms", min_value=0.0)
    missing = required - seen
    require(not missing, f"missing failure tests: {sorted(missing)}")


def validate_benchmark(report: dict[str, Any], args: argparse.Namespace) -> None:
    one_mac_qps = require_float(report, "benchmark.one_mac_qps", min_value=0.000001)
    cell_qps = require_float(report, "benchmark.cell_qps", min_value=0.000001)
    ratio = require_float(report, "benchmark.throughput_ratio", min_value=0.0)
    require(abs((cell_qps / one_mac_qps) - ratio) <= 0.05, "benchmark.throughput_ratio must match cell_qps / one_mac_qps within 0.05")
    require(ratio >= args.min_throughput_ratio, f"cell throughput ratio {ratio:.2f} below {args.min_throughput_ratio:.2f}")
    require_float(report, "benchmark.cell_p95_ms", min_value=0.0)
    require_float(report, "benchmark.cell_p99_ms", min_value=0.0)


def validate_artifact(report: dict[str, Any], args: argparse.Namespace) -> list[str]:
    require_int(report, "schema_version", min_value=1)
    cell_id = require_str(report, "cell.id")
    require_str(report, "generated_at")
    orchestrator = str(get_path(report, "deployment.orchestrator", "none")).lower()
    require(orchestrator not in {"kubernetes", "k8s"}, "initial four-Mac cell validation must not require Kubernetes")

    nodes = validate_nodes(report, allow_heterogeneous=args.allow_heterogeneous)
    validate_links(report, nodes, args)
    validate_placement(report, nodes)
    validate_failure_tests(report)
    validate_benchmark(report, args)

    return [
        "validated four-Mac cell artifact: "
        f"cell={cell_id}, nodes=4, throughput_ratio={get_path(report, 'benchmark.throughput_ratio'):.2f}"
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, help="Path to four-Mac cell validation JSON artifact")
    parser.add_argument("--allow-heterogeneous", action="store_true", help="Allow weighted heterogeneous cell hardware")
    parser.add_argument("--max-link-p95-us", type=float, default=500.0)
    parser.add_argument("--min-link-bandwidth-gbps", type=float, default=10.0)
    parser.add_argument("--max-packet-loss-percent", type=float, default=0.01)
    parser.add_argument("--min-throughput-ratio", type=float, default=2.5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = json.loads(args.artifact.read_text())
        require(isinstance(report, dict), "artifact root must be a JSON object")
        for message in validate_artifact(report, args):
            print(message)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
