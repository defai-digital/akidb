from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "qa_feature_matrix.py"


def load_module():
    name = "qa_feature_matrix"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FeatureMatrixUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_module()

    def test_b64_json_roundtrip(self) -> None:
        import base64
        import json

        encoded = self.mod.b64_json({"bucket": "1", "i": 2})
        decoded = json.loads(base64.b64decode(encoded).decode("utf-8"))
        self.assertEqual(decoded["bucket"], "1")
        self.assertEqual(decoded["i"], 2)

    def test_make_vectors_bucket_cycle(self) -> None:
        ids, vectors, metas, texts = self.mod.make_vectors(10, 8, seed=1)
        self.assertEqual(len(ids), 10)
        self.assertEqual(vectors.shape, (10, 8))
        self.assertEqual({m["bucket"] for m in metas}, {"0", "1", "2", "3", "4"})
        self.assertTrue(all("unique_token_" in t for t in texts))

    def test_markdown_lists_checks(self) -> None:
        summary = {
            "passed": False,
            "server": "127.0.0.1:1",
            "collection": "default",
            "features_covered": ["health", "crud_insert"],
            "checks": [
                {
                    "name": "health_ready",
                    "feature": "health",
                    "passed": True,
                    "detail": "ok",
                },
                {
                    "name": "insert_success_rate",
                    "feature": "crud_insert",
                    "passed": False,
                    "detail": "0/1",
                },
            ],
            "failures": [
                {"name": "insert_success_rate", "feature": "crud_insert", "detail": "0/1"}
            ],
            "scope_note": "note",
        }
        md = self.mod.render_markdown(summary)
        self.assertIn("Feature Matrix QA", md)
        self.assertIn("health_ready", md)
        self.assertIn("**FAIL**", md)
        self.assertIn("## Failures", md)


if __name__ == "__main__":
    unittest.main()
