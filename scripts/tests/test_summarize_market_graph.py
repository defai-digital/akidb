import argparse
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "summarize_market_graph.py"
SPEC = importlib.util.spec_from_file_location("summarize_market_graph", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class MarketGraphSummaryTests(unittest.TestCase):
    def test_expected_matrix_covers_every_tier_and_concurrency(self):
        points = MODULE.expected_points()
        self.assertEqual(len(points), 12)
        self.assertEqual(points["g1-c1-load"], ("g1", 1, True))
        self.assertEqual(points["g2-c8"], ("g2", 8, False))
        self.assertEqual(points["g3-c64"], ("g3", 64, False))

    def test_missing_artifact_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(
                evidence_dir=root,
                run_id="graph-run-001",
                artifact=root / "missing.tar.gz",
                max_p99_ms=250.0,
                max_g2_c8_p99_ms=50.0,
            )
            report = MODULE.summarize(args)
            self.assertEqual(report["verdict"]["status"], "fail")
            self.assertTrue(
                any("artifact is missing" in item for item in report["verdict"]["failures"])
            )


if __name__ == "__main__":
    unittest.main()
