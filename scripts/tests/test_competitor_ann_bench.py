import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "competitor_ann_bench.py"
SPEC = importlib.util.spec_from_file_location("competitor_ann_bench", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class CompetitorAnnBenchTests(unittest.TestCase):
    def test_filtered_ground_truth_uses_nearest_neighbor_label(self):
        neighbors = [12, 1, 2, 22, 32, 42]
        self.assertEqual(
            MODULE.filtered_ground_truth(neighbors, 3, 10),
            [12, 2, 22],
        )

    def test_recall_has_fixed_top_k_denominator(self):
        self.assertEqual(MODULE.recall_at_k({1, 2}, [1, 2, 3], 3), 2 / 3)

    def test_percentile_uses_nearest_rank(self):
        self.assertEqual(MODULE.percentile([4, 1, 3, 2], 0.50), 2.0)
        self.assertEqual(MODULE.percentile([4, 1, 3, 2], 0.99), 4.0)

    def test_point_failures_are_fail_closed(self):
        point = {
            "requested": 10,
            "succeeded": 9,
            "failed": 1,
            "filter_violations": 0,
            "result_count_violations": 0,
            "duplicate_results": 0,
            "unparseable_results": 0,
            "invalid_scores": 0,
            "recall_at_k": 0.94,
            "top_k": 10,
        }
        failures = MODULE.point_failures(point, 0.95)
        self.assertTrue(any("failed=1" in value for value in failures))
        self.assertTrue(any("Recall@10" in value for value in failures))

    def test_ground_truth_rejects_insufficient_filtered_width(self):
        with self.assertRaisesRegex(ValueError, "lacks top-3"):
            MODULE.validate_ground_truth(
                [[1, 2, 4, 6]],
                train_rows=10,
                workloads=[(3, 2)],
            )


if __name__ == "__main__":
    unittest.main()
