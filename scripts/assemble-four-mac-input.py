#!/usr/bin/env python3
"""Assemble four-Mac measured-input JSON from node/link/failure-test files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class AssembleError(Exception):
    """Raised when measured-input files cannot be assembled."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssembleError(message)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except OSError as exc:
        raise AssembleError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise AssembleError(f"{path} is not valid JSON: {exc}") from exc


def extend_objects(out: list[dict[str, Any]], path: Path, label: str) -> None:
    value = read_json(path)
    if isinstance(value, dict):
        out.append(value)
        return
    if isinstance(value, list):
        for idx, item in enumerate(value):
            require(isinstance(item, dict), f"{path} {label}[{idx}] must be an object")
            out.append(item)
        return
    raise AssembleError(f"{path} must be a JSON object or list of objects")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", action="append", type=Path, default=[], help="Node JSON object/list file")
    parser.add_argument("--link", action="append", type=Path, default=[], help="Link JSON object/list file")
    parser.add_argument(
        "--failure-test",
        action="append",
        type=Path,
        default=[],
        help="Failure-test JSON object/list file",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        require(args.node, "at least one --node file is required")
        require(args.link, "at least one --link file is required")
        require(args.failure_test, "at least one --failure-test file is required")

        nodes: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []
        failure_tests: list[dict[str, Any]] = []
        for path in args.node:
            extend_objects(nodes, path, "node")
        for path in args.link:
            extend_objects(links, path, "link")
        for path in args.failure_test:
            extend_objects(failure_tests, path, "failure_test")

        document = {
            "nodes": nodes,
            "links": links,
            "failure_tests": failure_tests,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(document, indent=2) + "\n")
        print(args.output)
    except (AssembleError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
