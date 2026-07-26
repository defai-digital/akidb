import argparse
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "summarize_market_parity.py"
SPEC = importlib.util.spec_from_file_location("summarize_market_parity", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def point(*, qps=100.0, recall=0.96, p99=10.0):
    return {
        "requested": 30_000,
        "unique_queries": 10_000,
        "measurement_rounds": 3,
        "succeeded": 30_000,
        "failed": 0,
        "concurrency": 8,
        "top_k": 10,
        "filter": {"enabled": False, "modulus": None},
        "qps": qps,
        "recall_at_k": recall,
        "filter_violations": 0,
        "result_count_violations": 0,
        "duplicate_results": 0,
        "unparseable_results": 0,
        "invalid_scores": 0,
        "latency": {"p99_ms": p99},
    }


class MarketParitySummaryTests(unittest.TestCase):
    def test_selection_is_highest_qps_above_recall_gate(self):
        failures = []
        chosen = MODULE.choose_point(
            "engine",
            [
                point(qps=150, recall=0.94),
                point(qps=120, recall=0.96, p99=8),
                point(qps=110, recall=0.99, p99=7),
            ],
            0.95,
            failures,
        )
        self.assertEqual(chosen["qps"], 120)
        self.assertEqual(failures, [])

    def test_incorrect_point_is_never_eligible(self):
        value = point()
        value["duplicate_results"] = 1
        failures = []
        self.assertIsNone(MODULE.choose_point("engine", [value], 0.95, failures))
        self.assertTrue(any("no correct" in failure for failure in failures))

    def test_missing_evidence_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(
                aki_evidence_dir=root,
                aki_run_id="sift1m-run",
                milvus_report=root / "missing-milvus.json",
                milvus_environment=root / "missing-milvus-env.json",
                weaviate_report=root / "missing-weaviate.json",
                weaviate_environment=root / "missing-weaviate-env.json",
                min_recall=0.95,
                min_qps_ratio=0.7,
                max_p99_ratio=1.5,
                max_build_ratio=2.0,
                max_storage_ratio=2.0,
                output=root / "summary.json",
            )
            result = MODULE.summarize(args)
            self.assertEqual(result["verdict"]["status"], "fail")
            self.assertGreater(len(result["verdict"]["failures"]), 0)


if __name__ == "__main__":
    unittest.main()
