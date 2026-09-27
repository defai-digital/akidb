"""Fail-closed canonical backup validation, independent of Docker availability."""

import copy
import hashlib
import importlib.util
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "verify_knowledge_restore.py"
spec = importlib.util.spec_from_file_location("verify_knowledge_restore", SCRIPT)
restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(restore)


class RestoreValidationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        fixtures = SCRIPT.parents[1] / "contracts/fixtures/knowledge/v1/valid"
        self.manifest = json.loads((fixtures / "bundle-manifest.json").read_text())
        self.mutation = json.loads(
            (fixtures / "mutation-upsert-bundle.json").read_text()
        )
        for reference, name in (
            (self.manifest["bundle"], "bundle.ndjson"),
            (self.mutation["payload"], "mutation-payload-upsert.json"),
        ):
            target = self.root / "objects" / reference["uri"].removeprefix("s3://")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((fixtures / name).read_bytes())

    def test_complete_closure_replays_record_and_deletion(self):
        records, deleted = restore.logical_records(
            self.root, "knowledge", self.manifest, 11, [self.mutation]
        )
        self.assertEqual(records["chunk-a"]["chunk_text"], "grounded text")
        self.assertFalse(deleted)
        deletion = {
            **self.mutation,
            "mutation_id": "delete-12",
            "sequence": 12,
            "operation": "delete",
        }
        deletion.pop("payload")
        records, deleted = restore.logical_records(
            self.root, "knowledge", self.manifest, 12, [self.mutation, deletion]
        )
        self.assertEqual(records, {})
        self.assertEqual(deleted, {"chunk-a"})

    def test_missing_truncated_and_corrupt_objects_are_rejected(self):
        reference = self.mutation["payload"]
        path, _ = restore.object_path(self.root, "knowledge", reference)
        original = path.read_bytes()
        for contents in (original[:-1], b"x" + original[1:]):
            path.write_bytes(contents)
            with self.assertRaises(ValueError):
                restore.logical_records(
                    self.root, "knowledge", self.manifest, 11, [self.mutation]
                )
        path.unlink()
        with self.assertRaisesRegex(ValueError, "missing"):
            restore.object_path(self.root, "knowledge", reference)

    def test_gap_scope_and_payload_identity_are_rejected(self):
        for field, value in (
            ("sequence", 12),
            ("workspace_id", "other"),
            ("mutation_id", "other"),
        ):
            changed = copy.deepcopy(self.mutation)
            changed[field] = value
            with self.assertRaises(ValueError):
                restore.logical_records(
                    self.root, "knowledge", self.manifest, 11, [changed]
                )
        with self.assertRaisesRegex(ValueError, "incomplete"):
            restore.logical_records(
                self.root, "knowledge", self.manifest, 12, [self.mutation]
            )

    def test_uri_cannot_escape_backup_or_silently_ignore_version(self):
        for uri in (
            "s3://other/key",
            "s3://knowledge/../secret",
            "s3://knowledge/%2e%2e/secret",
            "s3://knowledge/key?versionId=old",
            "https://example.org/key",
        ):
            with self.assertRaises(ValueError):
                restore.object_path(
                    self.root, "knowledge", {**self.manifest["bundle"], "uri": uri}
                )

    def test_archive_checksum_traversal_and_links_fail_closed(self):
        for name, kind in (
            ("../escape", tarfile.REGTYPE),
            ("backup/link", tarfile.SYMTYPE),
        ):
            archive = self.root / "bad.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                member = tarfile.TarInfo(name)
                member.type = kind
                member.linkname = "/etc/passwd" if kind == tarfile.SYMTYPE else ""
                member.size = 1 if kind == tarfile.REGTYPE else 0
                output.addfile(member, io.BytesIO(b"x") if member.size else None)
            destination = self.root / "expanded"
            destination.mkdir(exist_ok=True)
            with self.assertRaises(ValueError):
                restore.extract_backup(archive, "0" * 64, destination)
            with self.assertRaises(ValueError):
                restore.extract_backup(
                    archive,
                    hashlib.sha256(archive.read_bytes()).hexdigest(),
                    destination,
                )
            self.assertEqual(list(destination.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
