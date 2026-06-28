#!/usr/bin/env python3
"""Validate the complete four-Mac evidence bundle and write the final artifact."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


class EvidenceError(Exception):
    """Raised when evidence bundle validation cannot continue."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def read_json(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text())
    except OSError as exc:
        raise EvidenceError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise EvidenceError(f"{path} is not valid JSON: {exc}") from exc
    require(isinstance(document, dict), f"{path} must contain a JSON object")
    return document


def get_path(document: dict[str, Any], path: str) -> Any:
    value: Any = document
    for part in path.split("."):
        require(isinstance(value, dict) and part in value, f"missing required field: {path}")
        value = value[part]
    return value


def run(command: list[str]) -> None:
    subprocess.run(command, check=True)


def validate_one_mac_reference(args: argparse.Namespace, script_dir: Path) -> dict[str, Any]:
    one_mac = read_json(args.one_mac_artifact)
    command = [
        sys.executable,
        str(script_dir / "validate-one-mac-benchmark.py"),
        str(args.one_mac_artifact),
    ]
    if not args.skip_one_mac_reference_gate:
        command.append("--reference")
    run(command)
    return one_mac


def validate_cell_benchmark(args: argparse.Namespace, script_dir: Path, one_mac: dict[str, Any]) -> None:
    command = [
        sys.executable,
        str(script_dir / "validate-one-mac-benchmark.py"),
        str(args.cell_benchmark_artifact),
        "--expected-dimensions",
        str(get_path(one_mac, "dataset.dimension")),
        "--expected-vectors",
        str(get_path(one_mac, "dataset.vectors")),
        "--expected-queries",
        str(get_path(one_mac, "search.queries_requested")),
        "--expected-top-k",
        str(get_path(one_mac, "search.top_k")),
        "--expected-nprobe",
        str(get_path(one_mac, "search.nprobe")),
        "--expected-concurrency",
        str(get_path(one_mac, "search.concurrency")),
        "--expected-slo-ms",
        str(get_path(one_mac, "search.slo_ms")),
        "--require-apple-silicon",
    ]
    if args.max_cell_p95_ms is not None:
        command.extend(["--max-p95-ms", str(args.max_cell_p95_ms)])
    if args.max_cell_p99_ms is not None:
        command.extend(["--max-p99-ms", str(args.max_cell_p99_ms)])
    if args.min_cell_slo_compliance is not None:
        command.extend(["--min-slo-compliance", str(args.min_cell_slo_compliance)])
    run(command)


def build_and_validate_cell(args: argparse.Namespace, script_dir: Path) -> None:
    command = [
        sys.executable,
        str(script_dir / "build-four-mac-cell-artifact.py"),
        "--input",
        str(args.input),
        "--one-mac-artifact",
        str(args.one_mac_artifact),
        "--cell-artifact",
        str(args.cell_benchmark_artifact),
        "--output",
        str(args.output),
        "--validate",
    ]
    if args.allow_heterogeneous:
        command.append("--allow-heterogeneous")
    run(command)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Combined measured-input JSON")
    parser.add_argument("--one-mac-artifact", type=Path, required=True)
    parser.add_argument("--cell-benchmark-artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="Final four-Mac cell artifact path")
    parser.add_argument("--skip-one-mac-reference-gate", action="store_true")
    parser.add_argument("--allow-heterogeneous", action="store_true")
    parser.add_argument("--max-cell-p95-ms", type=float)
    parser.add_argument("--max-cell-p99-ms", type=float)
    parser.add_argument("--min-cell-slo-compliance", type=float, default=95.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    try:
        one_mac = validate_one_mac_reference(args, script_dir)
        validate_cell_benchmark(args, script_dir, one_mac)
        build_and_validate_cell(args, script_dir)
        print(f"validated four-Mac evidence bundle: {args.output}")
    except (EvidenceError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
