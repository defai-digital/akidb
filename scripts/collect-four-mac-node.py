#!/usr/bin/env python3
"""Collect one Mac node inventory record for four-Mac cell validation."""

from __future__ import annotations

import argparse
import json
import platform
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any


class CollectError(Exception):
    """Raised when local node inventory cannot be collected."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CollectError(message)


def sysctl(name: str) -> str:
    try:
        completed = subprocess.run(
            ["sysctl", "-n", name],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CollectError(f"cannot read sysctl {name}: {exc}") from exc
    value = completed.stdout.strip()
    require(bool(value), f"sysctl {name} returned an empty value")
    return value


def hardware_profile() -> dict[str, Any]:
    try:
        completed = subprocess.run(
            ["system_profiler", "SPHardwareDataType", "-json"],
            check=True,
            capture_output=True,
            text=True,
        )
        data = json.loads(completed.stdout)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        raise CollectError(f"cannot read system_profiler hardware data: {exc}") from exc

    records = data.get("SPHardwareDataType")
    require(isinstance(records, list) and records, "system_profiler returned no hardware records")
    record = records[0]
    require(isinstance(record, dict), "system_profiler hardware record is not an object")
    return record


def detect_mac_model(profile: dict[str, Any] | None) -> str:
    try:
        return sysctl("hw.model")
    except CollectError:
        if profile is None:
            profile = hardware_profile()
        model = profile.get("machine_model")
        require(isinstance(model, str) and model, "system_profiler missing machine_model")
        return model


def detect_memory_bytes(profile: dict[str, Any] | None) -> int:
    try:
        return int(sysctl("hw.memsize"))
    except (CollectError, ValueError):
        if profile is None:
            profile = hardware_profile()
        physical_memory = profile.get("physical_memory")
        require(
            isinstance(physical_memory, str) and physical_memory,
            "system_profiler missing physical_memory",
        )
        amount, _, unit = physical_memory.partition(" ")
        require(amount.isdigit(), f"cannot parse physical_memory: {physical_memory}")
        multiplier = {
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
        }.get(unit.upper())
        require(multiplier is not None, f"unsupported physical_memory unit: {physical_memory}")
        return int(amount) * multiplier


def collect_node(args: argparse.Namespace) -> dict[str, Any]:
    role = args.role
    require(role in {"voter", "learner"}, "--role must be voter or learner")
    node_id = args.id
    require(bool(node_id), "--id is required")

    host = args.host or socket.getfqdn() or platform.node()
    arch = platform.machine()
    if args.arch:
        arch = args.arch
    require(arch in {"arm64", "aarch64"}, f"expected Apple Silicon arm64/aarch64, got {arch}")

    profile: dict[str, Any] | None = None
    if args.mac_model is None or args.memory_bytes is None:
        try:
            profile = hardware_profile()
        except CollectError:
            profile = None

    mac_model = args.mac_model or detect_mac_model(profile)
    memory_bytes = args.memory_bytes
    if memory_bytes is None:
        memory_bytes = detect_memory_bytes(profile)
    require(memory_bytes > 0, "memory_bytes must be > 0")

    return {
        "id": node_id,
        "host": host,
        "arch": arch,
        "mac_model": mac_model,
        "memory_bytes": memory_bytes,
        "role": role,
        "healthy": args.healthy,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True, help="Stable node id, for example mac-1")
    parser.add_argument("--role", required=True, choices=["voter", "learner"])
    parser.add_argument("--host", help="Override detected host name")
    parser.add_argument("--arch", help="Override detected architecture")
    parser.add_argument("--mac-model", help="Override detected Mac model")
    parser.add_argument("--memory-bytes", type=int, help="Override detected memory size")
    parser.add_argument("--healthy", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--output", type=Path, help="Write node JSON object to this path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        node = collect_node(args)
        data = json.dumps(node, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(data)
            print(args.output)
        else:
            print(data, end="")
    except (CollectError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
