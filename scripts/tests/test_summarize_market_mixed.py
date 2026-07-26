import argparse
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "summarize_market_mixed.py"
SPEC = importlib.util.spec_from_file_location("summarize_market_mixed", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class MarketMixedSummaryTests(unittest.TestCase):
    def test_expected_fractions_are_market_gate(self):
        self.assertEqual(MODULE.FRACTIONS, {"mixed10": 0.10, "mixed50": 0.50})

    def test_missing_evidence_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(
                evidence_dir=root,
                run_id="mixed-run-001",
                duration_seconds=300,
                min_recall=0.95,
                max_p99_ms=250.0,
                output=root / "summary.json",
            )
            result = MODULE.summarize(args)
            self.assertEqual(result["verdict"]["status"], "fail")
            self.assertTrue(result["verdict"]["failures"])


if __name__ == "__main__":
    unittest.main()
