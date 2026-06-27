#!/usr/bin/env python3
"""Validate a native-vs-Kuzu graph adapter decision artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class ValidationError(Exception):
    """Raised when a Kuzu decision artifact does not satisfy the gate."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def get_path(document: dict[str, Any], path: str) -> Any:
    value: Any = document
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
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


def validate_latency(document: dict[str, Any], path: str) -> None:
    count = require_int(document, f"{path}.count", min_value=1)
    min_ms = require_float(document, f"{path}.min_ms", min_value=0.0)
    p50_ms = require_float(document, f"{path}.p50_ms", min_value=0.0)
    p95_ms = require_float(document, f"{path}.p95_ms", min_value=0.0)
    p99_ms = require_float(document, f"{path}.p99_ms", min_value=0.0)
    max_ms = require_float(document, f"{path}.max_ms", min_value=0.0)
    require(min_ms <= p50_ms <= p95_ms <= p99_ms <= max_ms, f"{path} percentiles are not ordered")
    require(count > 0, f"{path}.count must be positive")


def validate_workload(report: dict[str, Any]) -> None:
    require_int(report, "schema_version", min_value=1)
    require_str(report, "generated_at")
    require_str(report, "hardware.os")
    arch = require_str(report, "hardware.arch")
    require(arch in {"arm64", "aarch64"}, f"expected Apple Silicon artifact, got arch={arch}")
    require_str(report, "hardware.mac_model")
    memory = get_path(report, "hardware.memory_bytes")
    require(memory is None or (isinstance(memory, int) and memory > 0), "hardware.memory_bytes must be null or positive integer")
    require_str(report, "software.akidb_commit")
    require_str(report, "software.rustc")
    require_str(report, "software.kuzu_version")

    require_int(report, "dataset.nodes", min_value=1)
    require_int(report, "dataset.edges", min_value=1)
    require_int(report, "dataset.related_chunk_edges", min_value=0)
    require_str(report, "dataset.shape")

    query_mix = require_list(report, "query_mix")
    require(query_mix, "query_mix cannot be empty")
    kinds: set[str] = set()
    for idx, item in enumerate(query_mix):
        require(isinstance(item, dict), f"query_mix[{idx}] must be an object")
        kind = require_str(item, "kind")
        kinds.add(kind)
        require_int(item, "count", min_value=1)
    required_kinds = {"neighbors", "two_hop", "path_exists", "related_chunks"}
    missing = required_kinds - kinds
    require(not missing, f"query_mix missing required kinds: {sorted(missing)}")


def validate_backend(report: dict[str, Any], backend: str) -> dict[str, float]:
    prefix = f"backends.{backend}"
    require(bool(get_path(report, f"{prefix}.available")), f"{backend} backend must be available")
    require_str(report, f"{prefix}.implementation")
    require_float(report, f"{prefix}.ingest.wall_time_ms", min_value=0.000001)
    require_float(report, f"{prefix}.ingest.nodes_per_sec", min_value=0.000001)
    require_float(report, f"{prefix}.ingest.edges_per_sec", min_value=0.000001)
    storage_bytes = require_int(report, f"{prefix}.storage.bytes", min_value=1)
    rss_bytes = require_int(report, f"{prefix}.memory.peak_rss_bytes", min_value=1)
    validate_latency(report, f"{prefix}.queries.latency")
    qps = require_float(report, f"{prefix}.queries.qps", min_value=0.000001)
    errors = require_int(report, f"{prefix}.queries.errors", min_value=0)
    require(errors == 0, f"{backend} query errors must be 0")

    return {
        "ingest_ms": require_float(report, f"{prefix}.ingest.wall_time_ms"),
        "storage_bytes": float(storage_bytes),
        "rss_bytes": float(rss_bytes),
        "p95_ms": require_float(report, f"{prefix}.queries.latency.p95_ms"),
        "p99_ms": require_float(report, f"{prefix}.queries.latency.p99_ms"),
        "qps": qps,
    }


def validate_correctness(report: dict[str, Any], args: argparse.Namespace) -> None:
    parity = require_float(report, "correctness.result_parity_percent", min_value=0.0)
    require(parity <= 100.0, "correctness.result_parity_percent cannot exceed 100")
    require(
        parity >= args.min_parity_percent,
        f"result parity {parity:.2f}% below {args.min_parity_percent:.2f}%",
    )
    require_int(report, "correctness.native_node_count", min_value=1)
    require_int(report, "correctness.kuzu_node_count", min_value=1)
    require_int(report, "correctness.native_edge_count", min_value=1)
    require_int(report, "correctness.kuzu_edge_count", min_value=1)
    require(
        get_path(report, "correctness.native_node_count") == get_path(report, "correctness.kuzu_node_count"),
        "native and Kuzu node counts must match",
    )
    require(
        get_path(report, "correctness.native_edge_count") == get_path(report, "correctness.kuzu_edge_count"),
        "native and Kuzu edge counts must match",
    )


def collect_ratios(native: dict[str, float], kuzu: dict[str, float]) -> dict[str, float]:
    return {
        "p95": kuzu["p95_ms"] / native["p95_ms"] if native["p95_ms"] else float("inf"),
        "p99": kuzu["p99_ms"] / native["p99_ms"] if native["p99_ms"] else float("inf"),
        "ingest": kuzu["ingest_ms"] / native["ingest_ms"] if native["ingest_ms"] else float("inf"),
        "storage": kuzu["storage_bytes"] / native["storage_bytes"] if native["storage_bytes"] else float("inf"),
        "rss": kuzu["rss_bytes"] / native["rss_bytes"] if native["rss_bytes"] else float("inf"),
        "qps": kuzu["qps"] / native["qps"] if native["qps"] else 0.0,
    }


def require_ratio(name: str, actual: float, limit: float) -> None:
    require(actual <= limit, f"{name} ratio {actual:.2f} exceeds {limit:.2f}")


def enforce_ratio_gates(ratios: dict[str, float], args: argparse.Namespace) -> None:
    require_ratio("Kuzu P95/native P95", ratios["p95"], args.max_p95_ratio)
    require_ratio("Kuzu P99/native P99", ratios["p99"], args.max_p99_ratio)
    require_ratio("Kuzu ingest/native ingest", ratios["ingest"], args.max_ingest_ratio)
    require_ratio("Kuzu storage/native storage", ratios["storage"], args.max_storage_ratio)
    require_ratio("Kuzu RSS/native RSS", ratios["rss"], args.max_rss_ratio)
    require(
        ratios["qps"] >= args.min_qps_ratio,
        f"Kuzu/native QPS ratio {ratios['qps']:.2f} below {args.min_qps_ratio:.2f}",
    )


def apply_mode_defaults(args: argparse.Namespace) -> None:
    if args.mode == "hot-path":
        args.max_p95_ratio = args.max_p95_ratio or 1.25
        args.max_p99_ratio = args.max_p99_ratio or 1.25
        args.max_ingest_ratio = args.max_ingest_ratio or 2.0
        args.max_storage_ratio = args.max_storage_ratio or 2.0
        args.max_rss_ratio = args.max_rss_ratio or 2.0
        args.min_qps_ratio = args.min_qps_ratio if args.min_qps_ratio is not None else 0.80
        args.min_parity_percent = args.min_parity_percent if args.min_parity_percent is not None else 99.9
    else:
        args.max_p95_ratio = args.max_p95_ratio or 3.0
        args.max_p99_ratio = args.max_p99_ratio or 3.0
        args.max_ingest_ratio = args.max_ingest_ratio or 4.0
        args.max_storage_ratio = args.max_storage_ratio or 5.0
        args.max_rss_ratio = args.max_rss_ratio or 4.0
        args.min_qps_ratio = args.min_qps_ratio if args.min_qps_ratio is not None else 0.25
        args.min_parity_percent = args.min_parity_percent if args.min_parity_percent is not None else 99.5


def validate_decision(report: dict[str, Any], args: argparse.Namespace) -> list[str]:
    validate_workload(report)
    native = validate_backend(report, "native")
    kuzu = validate_backend(report, "kuzu")
    ratios = collect_ratios(native, kuzu)

    recommendation = require_str(report, "decision.recommendation")
    allowed = {"reject_kuzu", "ship_optional_kuzu", "promote_kuzu_hot_path"}
    require(recommendation in allowed, f"decision.recommendation must be one of {sorted(allowed)}")

    if args.mode == "hot-path":
        require(
            recommendation == "promote_kuzu_hot_path",
            "hot-path mode requires decision.recommendation=promote_kuzu_hot_path",
        )
        validate_correctness(report, args)
        enforce_ratio_gates(ratios, args)
        require_str(report, "decision.rollback_plan")
    else:
        require(
            recommendation in {"ship_optional_kuzu", "reject_kuzu"},
            "optional-adapter mode must not recommend hot-path promotion",
        )
        if recommendation == "ship_optional_kuzu":
            validate_correctness(report, args)
            enforce_ratio_gates(ratios, args)

    require_str(report, "decision.rationale")
    require_str(report, "decision.rollback_plan")
    require_str(report, "decision.packaging_source")
    require_str(report, "decision.upstream_status")
    require_str(report, "decision.maintenance_owner")

    return [
        "validated Kuzu decision artifact: "
        f"mode={args.mode}, recommendation={recommendation}, "
        f"parity={get_path(report, 'correctness.result_parity_percent'):.2f}%, "
        f"p95_ratio={ratios['p95']:.2f}, qps_ratio={ratios['qps']:.2f}"
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, help="Path to native-vs-Kuzu benchmark decision JSON artifact")
    parser.add_argument("--mode", choices=["optional-adapter", "hot-path"], default="optional-adapter")
    parser.add_argument("--max-p95-ratio", type=float)
    parser.add_argument("--max-p99-ratio", type=float)
    parser.add_argument("--max-ingest-ratio", type=float)
    parser.add_argument("--max-storage-ratio", type=float)
    parser.add_argument("--max-rss-ratio", type=float)
    parser.add_argument("--min-qps-ratio", type=float)
    parser.add_argument("--min-parity-percent", type=float)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    apply_mode_defaults(args)
    try:
        report = json.loads(args.artifact.read_text())
        require(isinstance(report, dict), "artifact root must be a JSON object")
        for message in validate_decision(report, args):
            print(message)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
