#!/usr/bin/env python3
"""Validate a one-Mac benchmark JSON artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class ValidationError(Exception):
    """Raised when a benchmark artifact does not satisfy the requested gate."""


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


def require_str(document: dict[str, Any], path: str) -> str:
    value = get_path(document, path)
    require(isinstance(value, str) and value, f"{path} must be a non-empty string")
    return value


def validate_latency_order(document: dict[str, Any], prefix: str) -> None:
    count = require_int(document, f"{prefix}.count", min_value=0)
    min_us = require_int(document, f"{prefix}.min_us", min_value=0)
    avg_us = require_int(document, f"{prefix}.avg_us", min_value=0)
    p50_us = require_int(document, f"{prefix}.p50_us", min_value=0)
    p95_us = require_int(document, f"{prefix}.p95_us", min_value=0)
    p99_us = require_int(document, f"{prefix}.p99_us", min_value=0)
    max_us = require_int(document, f"{prefix}.max_us", min_value=0)

    if count == 0:
        require(
            min_us == avg_us == p50_us == p95_us == p99_us == max_us == 0,
            f"{prefix} empty latency stats must all be zero",
        )
        return

    require(min_us <= p50_us <= p95_us <= p99_us <= max_us, f"{prefix} percentiles are not ordered")
    require(min_us <= avg_us <= max_us, f"{prefix}.avg_us is outside min/max range")


def apply_reference_defaults(args: argparse.Namespace) -> None:
    if not args.reference:
        return
    args.expected_dimensions = args.expected_dimensions or 768
    args.expected_vectors = args.expected_vectors or 1_000_000
    args.expected_queries = args.expected_queries or 5_000
    args.expected_top_k = args.expected_top_k or 10
    args.expected_nprobe = args.expected_nprobe or 64
    args.expected_concurrency = args.expected_concurrency or 1
    args.expected_slo_ms = args.expected_slo_ms or 50
    args.max_p95_ms = args.max_p95_ms or 50.0
    args.max_p99_ms = args.max_p99_ms or 100.0
    args.min_slo_compliance = args.min_slo_compliance if args.min_slo_compliance is not None else 95.0
    args.require_apple_silicon = True


def validate_artifact(report: dict[str, Any], args: argparse.Namespace) -> list[str]:
    messages: list[str] = []

    require_int(report, "benchmark_version", min_value=1)
    require_int(report, "generated_at_unix_ms", min_value=1)
    require_str(report, "server")

    dimension = require_int(report, "dataset.dimension", min_value=1)
    vectors = require_int(report, "dataset.vectors", min_value=1)
    batch_size = require_int(report, "dataset.batch_size", min_value=1)
    require_int(report, "dataset.seed", min_value=0)
    require_str(report, "dataset.id_prefix")

    if args.expected_dimensions is not None:
        require(dimension == args.expected_dimensions, f"dataset.dimension expected {args.expected_dimensions}, got {dimension}")
    if args.expected_vectors is not None:
        require(vectors == args.expected_vectors, f"dataset.vectors expected {args.expected_vectors}, got {vectors}")
    if args.expected_batch_size is not None:
        require(batch_size == args.expected_batch_size, f"dataset.batch_size expected {args.expected_batch_size}, got {batch_size}")

    require_str(report, "hardware.os")
    arch = require_str(report, "hardware.arch")
    require_str(report, "hardware.mac_model")
    memory = get_path(report, "hardware.memory_bytes")
    require(memory is None or (isinstance(memory, int) and memory > 0), "hardware.memory_bytes must be null or positive integer")
    if args.require_apple_silicon:
        require(arch in {"arm64", "aarch64"}, f"expected Apple Silicon arm64/aarch64 artifact, got arch={arch}")

    require_str(report, "software.akidb_version")
    require_str(report, "software.git_commit")
    require_str(report, "software.rustc")

    require(bool(get_path(report, "health_before.healthy")), "health_before.healthy must be true")
    require(bool(get_path(report, "health_before.ready")), "health_before.ready must be true")
    require(bool(get_path(report, "health_after_insert.healthy")), "health_after_insert.healthy must be true")
    require(bool(get_path(report, "health_after_insert.ready")), "health_after_insert.ready must be true")

    vectors_requested = require_int(report, "insert.vectors_requested", min_value=1)
    vectors_inserted = require_int(report, "insert.vectors_inserted", min_value=0)
    require(vectors_requested == vectors, "insert.vectors_requested must match dataset.vectors")
    require(vectors_inserted == vectors_requested, f"all vectors must insert successfully: {vectors_inserted}/{vectors_requested}")
    require_float(report, "insert.throughput_vectors_per_sec", min_value=0.000001)

    active_after_insert = require_int(report, "health_after_insert.active_vectors", min_value=0)
    require(
        active_after_insert >= vectors_inserted,
        f"health_after_insert.active_vectors {active_after_insert} is less than inserted {vectors_inserted}",
    )

    single_insert_count = require_int(report, "single_insert.count", min_value=0)
    validate_latency_order(report, "single_insert.latency")
    single_insert_latency_count = require_int(report, "single_insert.latency.count", min_value=0)
    require(
        single_insert_latency_count == single_insert_count,
        "single_insert.latency.count must match single_insert.count",
    )

    queries_requested = require_int(report, "search.queries_requested", min_value=1)
    queries_succeeded = require_int(report, "search.queries_succeeded", min_value=0)
    concurrency = require_int(report, "search.concurrency", min_value=1)
    top_k = require_int(report, "search.top_k", min_value=1)
    nprobe = require_int(report, "search.nprobe", min_value=1)
    require_int(report, "search.wall_time_ms", min_value=0)
    require_float(report, "search.throughput_queries_per_sec", min_value=0.000001)
    avg_results = require_float(report, "search.avg_results_per_query", min_value=0.0)
    slo_ms = require_int(report, "search.slo_ms", min_value=1)
    slo_compliance = require_float(report, "search.slo_compliance_percent", min_value=0.0)
    require(slo_compliance <= 100.0, "search.slo_compliance_percent cannot exceed 100")
    validate_latency_order(report, "search.latency")
    latency_count = require_int(report, "search.latency.count", min_value=0)

    require(queries_succeeded == queries_requested, f"all search queries must succeed: {queries_succeeded}/{queries_requested}")
    require(latency_count == queries_succeeded, "search.latency.count must match search.queries_succeeded")
    require(avg_results <= float(top_k), f"search.avg_results_per_query {avg_results} exceeds top_k {top_k}")

    if args.expected_queries is not None:
        require(queries_requested == args.expected_queries, f"search.queries_requested expected {args.expected_queries}, got {queries_requested}")
    if args.expected_top_k is not None:
        require(top_k == args.expected_top_k, f"search.top_k expected {args.expected_top_k}, got {top_k}")
    if args.expected_nprobe is not None:
        require(nprobe == args.expected_nprobe, f"search.nprobe expected {args.expected_nprobe}, got {nprobe}")
    if args.expected_concurrency is not None:
        require(concurrency == args.expected_concurrency, f"search.concurrency expected {args.expected_concurrency}, got {concurrency}")
    if args.expected_slo_ms is not None:
        require(slo_ms == args.expected_slo_ms, f"search.slo_ms expected {args.expected_slo_ms}, got {slo_ms}")

    p95_us = require_int(report, "search.latency.p95_us", min_value=0)
    p99_us = require_int(report, "search.latency.p99_us", min_value=0)
    if args.max_p95_ms is not None:
        max_p95_us = int(args.max_p95_ms * 1000)
        require(p95_us <= max_p95_us, f"search P95 {p95_us / 1000.0:.3f}ms exceeds {args.max_p95_ms:.3f}ms")
    if args.max_p99_ms is not None:
        max_p99_us = int(args.max_p99_ms * 1000)
        require(p99_us <= max_p99_us, f"search P99 {p99_us / 1000.0:.3f}ms exceeds {args.max_p99_ms:.3f}ms")
    if args.min_slo_compliance is not None:
        require(
            slo_compliance >= args.min_slo_compliance,
            f"SLO compliance {slo_compliance:.2f}% below {args.min_slo_compliance:.2f}%",
        )

    messages.append(
        "validated one-Mac benchmark: "
        f"{vectors} vectors, D={dimension}, queries={queries_requested}, "
        f"topK={top_k}, concurrency={concurrency}, "
        f"P95={p95_us / 1000.0:.3f}ms, P99={p99_us / 1000.0:.3f}ms"
    )
    return messages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, help="Path to one-Mac benchmark JSON artifact")
    parser.add_argument("--reference", action="store_true", help="Apply README reference workload gates")
    parser.add_argument("--expected-dimensions", type=int)
    parser.add_argument("--expected-vectors", type=int)
    parser.add_argument("--expected-batch-size", type=int)
    parser.add_argument("--expected-queries", type=int)
    parser.add_argument("--expected-top-k", type=int)
    parser.add_argument("--expected-nprobe", type=int)
    parser.add_argument("--expected-concurrency", type=int)
    parser.add_argument("--expected-slo-ms", type=int)
    parser.add_argument("--max-p95-ms", type=float)
    parser.add_argument("--max-p99-ms", type=float)
    parser.add_argument("--min-slo-compliance", type=float)
    parser.add_argument("--require-apple-silicon", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    apply_reference_defaults(args)
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
