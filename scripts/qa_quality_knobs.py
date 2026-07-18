#!/usr/bin/env python3
"""Drive score_threshold + group_by + ACL + graph_hybrid via shipped unit tests."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "qa-results" / "quality-knobs-qa.json"

FILTERS = [
    "test_score_threshold_excludes_low_scores",
    "test_group_by_parent_keeps_one_per_group",
    "test_workspace_acl_isolates_search_results",
    "test_graph_hybrid_expands_related_edges",
]


def main() -> int:
    tails: list[str] = []
    passed = True
    for filt in FILTERS:
        proc = subprocess.run(
            [
                "cargo",
                "test",
                "-p",
                "akidb-grpc",
                "--lib",
                filt,
                "--",
                "--nocapture",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        tails.append(f"=== {filt} rc={proc.returncode} ===\n{proc.stdout[-1500:]}\n{proc.stderr[-800:]}")
        if proc.returncode != 0:
            passed = False
    artifact = {
        "gate": "quality_knobs",
        "passed": passed,
        "filters": FILTERS,
        "output_tail": "\n".join(tails)[-6000:],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(artifact, indent=2) + "\n")
    print(json.dumps({"passed": passed, "gate": "quality_knobs"}, indent=2))
    if not passed:
        print(artifact["output_tail"], file=sys.stderr)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
