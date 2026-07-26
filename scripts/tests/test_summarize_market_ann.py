import argparse
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "summarize_market_ann.py"
SPEC = importlib.util.spec_from_file_location("summarize_market_ann", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class MarketAnnSummaryTests(unittest.TestCase):
    def test_expected_matrix_covers_pareto_top100_and_filters(self):
        points = MODULE.expected_points()
        self.assertEqual(len(points), 22)
        self.assertEqual(points["k10-n32-c1-load"], (10, 32, None))
        self.assertEqual(points["k100-n256-c8"], (100, 256, None))
        self.assertEqual(points["k10-n256-c8-filter2"], (10, 256, 2))
        self.assertEqual(points["k1-n256-c8-filter100"], (1, 256, 100))

    def test_missing_artifact_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(
                evidence_dir=root,
                run_id="market-run-001",
                artifact=root / "missing.tar.gz",
                min_recall=0.95,
                min_unfiltered_qps=100.0,
                max_p99_ms=250.0,
                min_import_vps=500.0,
            )
            report = MODULE.summarize(args)
            self.assertEqual(report["verdict"]["status"], "fail")
            self.assertTrue(
                any("artifact is missing" in item for item in report["verdict"]["failures"])
            )


if __name__ == "__main__":
    unittest.main()
