from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "qa_correctness_kpi.py"


def load_module():
    import sys

    name = "qa_correctness_kpi"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Required so @dataclass can resolve annotations on Python 3.14+.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class CorrectnessKpiUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_module()

    def test_exact_topk_is_identity_for_self_query(self) -> None:
        vectors = self.mod.normalize_rows(
            np.array(
                [
                    [1.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0],
                    [0.0, 0.0, 1.0],
                ],
                dtype=np.float32,
            )
        )
        ids = ["a", "b", "c"]
        top = self.mod.exact_topk(vectors, ids, vectors[1], top_k=2)
        self.assertEqual(top[0][0], "b")
        self.assertGreater(top[0][1], top[1][1])

    def test_ndcg_perfect_ranking_is_one(self) -> None:
        relevance = {"a": 1.0, "b": 0.5, "c": 0.1}
        score = self.mod.ndcg_at_k(
            ["a", "b", "c"],
            relevance,
            [1.0, 0.5, 0.1],
            top_k=3,
        )
        self.assertAlmostEqual(score, 1.0, places=6)

    def test_markdown_contains_table_and_status(self) -> None:
        summary = {
            "passed": True,
            "failures": [],
            "kpis": [
                {
                    "kpi": "mean_recall_at_k",
                    "value": 1.0,
                    "unit": "ratio",
                    "gate": ">= 0.98",
                    "status": "PASS",
                    "meaning": "exact neighbors",
                }
            ],
            "details": {
                "server": "127.0.0.1:50051",
                "collection": "demo",
                "dataset": {
                    "vectors": 10,
                    "dimensions": 8,
                    "queries": 5,
                    "top_k": 3,
                    "ground_truth": "exact",
                },
            },
        }
        md = self.mod.render_markdown(summary)
        self.assertIn("| KPI |", md)
        self.assertIn("mean_recall_at_k", md)
        self.assertIn("**PASS**", md)
        self.assertIn("Overall:** `PASS`", md)


if __name__ == "__main__":
    unittest.main()
