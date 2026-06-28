#!/usr/bin/env python3
"""Write one Thunderbolt link measurement for four-Mac cell validation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class CollectError(Exception):
    """Raised when a link measurement is invalid."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CollectError(message)


def collect_link(args: argparse.Namespace) -> dict[str, Any]:
    source = args.source
    target = args.target
    require(source != target, "--from and --to must be distinct")
    require(args.latency_p95_us >= 0, "--latency-p95-us must be >= 0")
    require(args.bandwidth_gbps > 0, "--bandwidth-gbps must be > 0")
    require(args.packet_loss_percent >= 0, "--packet-loss-percent must be >= 0")

    return {
        "from": source,
        "to": target,
        "transport": args.transport,
        "healthy": args.healthy,
        "latency_p95_us": args.latency_p95_us,
        "bandwidth_gbps": args.bandwidth_gbps,
        "packet_loss_percent": args.packet_loss_percent,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="source", required=True, help="Source node id")
    parser.add_argument("--to", dest="target", required=True, help="Target node id")
    parser.add_argument("--transport", default="thunderbolt")
    parser.add_argument("--healthy", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--latency-p95-us", type=float, required=True)
    parser.add_argument("--bandwidth-gbps", type=float, required=True)
    parser.add_argument("--packet-loss-percent", type=float, required=True)
    parser.add_argument("--output", type=Path, help="Write link JSON object to this path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        link = collect_link(args)
        data = json.dumps(link, indent=2) + "\n"
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
