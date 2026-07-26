import argparse
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "summarize_market_recovery.py"
SPEC = importlib.util.spec_from_file_location("summarize_market_recovery", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class MarketRecoverySummaryTests(unittest.TestCase):
    def test_systemd_properties_are_parsed_without_splitting_values(self):
        properties = MODULE.parse_systemd_properties(
            [
                "MainPID=123",
                "InvocationID=abc",
                "Description=value=with=equals",
                "invalid",
            ]
        )
        self.assertEqual(properties["MainPID"], "123")
        self.assertEqual(properties["Description"], "value=with=equals")
        self.assertNotIn("invalid", properties)

    def test_boolean_values_cannot_masquerade_as_numbers_or_return_codes(self):
        self.assertFalse(MODULE.finite_number(True))
        self.assertFalse(MODULE.zero_return_code(False))
        self.assertTrue(MODULE.finite_number(1.25))
        self.assertTrue(MODULE.zero_return_code(0))

    def test_invalid_json_evidence_fails_closed_without_an_exception(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "invalid.json"
            path.write_text("{")
            failures = []
            value = MODULE.read_evidence_json(path, "test", failures, {})
            self.assertEqual(value, {})
            self.assertTrue(any("test evidence is invalid" in item for item in failures))

            path.write_text(json.dumps({"valid": True}))
            failures = []
            value = MODULE.read_evidence_json(path, "test", failures, {})
            self.assertEqual(value, {"valid": True})
            self.assertEqual(failures, [])

    def test_missing_artifact_and_source_summary_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(
                evidence_dir=root,
                run_id="recovery-run-001",
                source_ann_summary=root / "missing-source.json",
                artifact=root / "missing.tar.gz",
                min_recall=0.95,
                max_p99_ms=250.0,
                max_crash_recovery_ms=900_000.0,
                max_graceful_restart_ms=900_000.0,
                min_insert_acks=100,
                min_update_acks=50,
                min_delete_acks=25,
                max_inflight_advances=16,
            )
            report = MODULE.summarize(args)
            self.assertEqual(report["verdict"]["status"], "fail")
            self.assertIn("artifact is missing", report["verdict"]["failures"])
            self.assertIn("source ANN summary is missing", report["verdict"]["failures"])


if __name__ == "__main__":
    unittest.main()
