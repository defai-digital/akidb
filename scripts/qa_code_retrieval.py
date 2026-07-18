#!/usr/bin/env python3
"""Offline code-retrieval fixture check (Phase B).

Runs against the Rust `chunk_code` contract by invoking cargo tests that cover
symbol chunking + edges for Rust/Python/TS/Go. Also validates sample fixtures
exist with expected symbol tokens.

Usage:
  python3 scripts/qa_code_retrieval.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples" / "code-qa"
OUT = ROOT / "qa-results" / "code-retrieval-qa.json"


def main() -> int:
    required = {
        "demo.rs": ["fn add", "test_add"],
        "demo.go": ["func Add", "func TestAdd"],
    }
    missing = []
    for name, tokens in required.items():
        path = SAMPLES / name
        if not path.exists():
            missing.append(f"missing {path}")
            continue
        text = path.read_text()
        for tok in tokens:
            if tok not in text:
                missing.append(f"{name} missing token {tok!r}")

    # Drive real shipped code path via cargo tests (not re-implemented here).
    cmd = [
        "cargo",
        "test",
        "-p",
        "akidb-retrieval",
        "--lib",
        "code::tests::test_",
        "--",
        "--nocapture",
    ]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    cargo_ok = proc.returncode == 0
    artifact = {
        "gate": "code_retrieval",
        "fixtures_ok": not missing,
        "fixture_problems": missing,
        "cargo_tests_ok": cargo_ok,
        "cargo_stdout_tail": proc.stdout[-2000:],
        "cargo_stderr_tail": proc.stderr[-2000:],
        "passed": cargo_ok and not missing,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(artifact, indent=2) + "\n")
    print(json.dumps(artifact, indent=2))
    return 0 if artifact["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
