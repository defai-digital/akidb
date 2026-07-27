#!/usr/bin/env python3

import argparse
import importlib.util
import json
import sqlite3
import stat
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "agentic_memory_import.py"
SPEC = importlib.util.spec_from_file_location("agentic_memory_import", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class AgenticMemoryImportTests(unittest.TestCase):
    def args(self, source: Path, output: Path, source_format: str, **values):
        defaults = {
            "source_format": source_format,
            "input": source,
            "output": output,
            "checkpoint": None,
            "sqlite_table": None,
            "workspace": "workspace-a",
            "namespace": "migration/test",
            "purpose": "migration",
            "entity_prefix": "legacy",
            "data_subject_field": None,
            "max_records": 100,
            "max_input_bytes": MODULE.MAX_INPUT_BYTES,
        }
        defaults.update(values)
        return argparse.Namespace(**defaults)

    def test_mem0_plan_is_deterministic_and_marks_identity_collisions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "mem0.jsonl"
            source.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "id": "m1",
                                "memory": "Drain the queue.",
                                "user_id": "service:ingestion",
                                "type": "recovery procedure",
                            }
                        ),
                        json.dumps(
                            {
                                "id": "m2",
                                "memory": "Restart immediately.",
                                "user_id": "service:ingestion",
                                "type": "recovery procedure",
                            }
                        ),
                    ]
                )
                + "\n"
            )
            args = self.args(source, root / "plan.jsonl", "mem0")
            first = MODULE.build_plan(args)
            second = MODULE.build_plan(args)
            self.assertEqual(first, second)
            self.assertEqual(first["manifest"]["candidate_count"], 2)
            self.assertEqual(first["manifest"]["collision_group_count"], 1)
            for candidate in first["candidates"]:
                self.assertEqual(candidate["collision"]["status"], "review_required")
                self.assertNotIn("source_assurance", candidate)
                self.assertNotIn("decision_authority", candidate)
                self.assertTrue(candidate["idempotency_key"].startswith("memory-import-v1:"))

    def test_checkpoint_skips_completed_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "legacy.jsonl"
            source.write_text(
                json.dumps(
                    {
                        "id": "m1",
                        "text": "Remember this.",
                        "metadata": {"memory_kind": "note"},
                    }
                )
                + "\n"
            )
            base = self.args(
                source,
                root / "plan.jsonl",
                "akidb-memory-document",
                checkpoint=root / "checkpoint.json",
            )
            first = MODULE.build_plan(base)
            MODULE.write_plan(base.output, first)
            self.assertEqual(stat.S_IMODE(base.output.stat().st_mode), 0o600)
            MODULE.atomic_write_json(
                base.checkpoint,
                {
                    "schema_version": 1,
                    "completed_source_sha256": [
                        first["candidates"][0]["original_sha256"]
                    ],
                },
            )
            second = MODULE.build_plan(
                self.args(
                    source,
                    root / "plan-2.jsonl",
                    "akidb-memory-document",
                    checkpoint=base.checkpoint,
                )
            )
            self.assertEqual(second["manifest"]["candidate_count"], 0)
            self.assertEqual(second["manifest"]["checkpoint_skipped_count"], 1)

    def test_ax_studio_sqlite_is_read_only_and_reviewable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "studio ?#.sqlite"
            connection = sqlite3.connect(database)
            connection.execute(
                "CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT, kind TEXT)"
            )
            connection.execute(
                "INSERT INTO memories VALUES (?, ?, ?)",
                ("ax-1", "Use the portable CPU path.", "build preference"),
            )
            connection.commit()
            connection.close()

            args = self.args(database, root / "plan.jsonl", "ax-studio-sqlite")
            plan = MODULE.build_plan(args)
            self.assertEqual(plan["manifest"]["candidate_count"], 1)
            candidate = plan["candidates"][0]
            self.assertEqual(candidate["original_id"], "ax-1")
            self.assertIn("introspected", " ".join(candidate["limitations"]))

    def test_invalid_and_ambiguous_rows_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "bad.jsonl"
            source.write_text(json.dumps({"id": "m1", "memory": "  untrimmed  "}) + "\n")
            with self.assertRaises(MODULE.ImportPlanError):
                MODULE.build_plan(self.args(source, root / "plan", "mem0"))

    def test_input_and_canonical_record_sizes_are_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "large.jsonl"
            source.write_text(
                json.dumps({"id": "m1", "memory": "valid", "metadata": "x" * 100})
                + "\n"
            )
            with self.assertRaisesRegex(MODULE.ImportPlanError, "max-input-bytes"):
                MODULE.build_plan(
                    self.args(
                        source,
                        root / "plan",
                        "mem0",
                        max_input_bytes=10,
                    )
                )

            original_limit = MODULE.MAX_SOURCE_RECORD_BYTES
            MODULE.MAX_SOURCE_RECORD_BYTES = 32
            try:
                with self.assertRaisesRegex(
                    MODULE.ImportPlanError, "canonical source record exceeds"
                ):
                    MODULE.build_plan(
                        self.args(source, root / "plan-2", "mem0")
                    )
            finally:
                MODULE.MAX_SOURCE_RECORD_BYTES = original_limit


if __name__ == "__main__":
    unittest.main()
