#!/usr/bin/env python3
"""Write one failure-test result for four-Mac cell validation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class CollectError(Exception):
    """Raised when a failure-test result is invalid."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CollectError(message)


def collect_failure_test(args: argparse.Namespace) -> dict[str, Any]:
    require(args.recovery_time_ms >= 0, "--recovery-time-ms must be >= 0")
    return {
        "kind": args.kind,
        "passed": args.passed,
        "observed_status": args.observed_status,
        "recovery_time_ms": args.recovery_time_ms,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", required=True, choices=["node_loss", "link_loss"])
    parser.add_argument("--passed", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--observed-status", required=True, choices=["healthy", "degraded"])
    parser.add_argument("--recovery-time-ms", type=float, required=True)
    parser.add_argument("--output", type=Path, help="Write failure-test JSON object to this path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = collect_failure_test(args)
        data = json.dumps(result, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(data)
            print(args.output)
        else:
            print(data, end="")
    except (CollectError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
