#!/usr/bin/env python3
"""Build a canonical fixture backup, then run the real restore verifier."""

import argparse
import copy
import hashlib
import json
import tarfile
import tempfile
from pathlib import Path

from verify_knowledge_restore import (
    ROOT,
    Lab,
    digest_file,
    postgres,
    sql_literal,
    verify,
)


def make_fixture(destination, postgres_image):
    root = destination / "fixture-backup"
    root.mkdir()
    fixtures = ROOT / "contracts/fixtures/knowledge/v1/valid"
    manifest = json.loads((fixtures / "bundle-manifest.json").read_text())
    mutation = json.loads((fixtures / "mutation-upsert-bundle.json").read_text())
    entries = [
        json.loads(line)
        for line in (fixtures / "bundle.ndjson").read_text().splitlines()
    ]
    removed = copy.deepcopy(entries[1]["record"])
    removed.update(
        chunk_id="chunk-deleted", doc_id="doc-deleted", vector=[0.3, 0.2, 0.1]
    )
    removed["metadata"]["source_uri"] = "s3://knowledge/documents/doc-deleted"
    entries.insert(2, {"entry_type": "record", "record": removed})
    entries.insert(
        next(
            i
            for i, entry in enumerate(entries)
            if entry.get("node", {}).get("node_id") == "entity-a"
        ),
        {
            "entry_type": "node",
            "node": {
                "node_id": "chunk-deleted",
                "kind": "chunk",
                "properties": {"doc_id": "doc-deleted"},
            },
        },
    )
    entries[0]["header"].update(record_count=2, node_count=3)
    manifest["expected_vector_count"] = 2
    manifest["bundle_compression"] = "zstd"
    import zstandard

    bundle = zstandard.ZstdCompressor().compress(
        b"".join(
            json.dumps(entry, separators=(",", ":")).encode() + b"\n"
            for entry in entries
        )
    )
    deletion = {
        **mutation,
        "sequence": 12,
        "mutation_id": "delete-12",
        "operation": "delete",
        "chunk_id": "chunk-deleted",
    }
    deletion.pop("payload")
    for contract, name in (
        (manifest["bundle"], "bundle.ndjson"),
        (mutation["payload"], "mutation-payload-upsert.json"),
    ):
        contents = bundle if name == "bundle.ndjson" else (fixtures / name).read_bytes()
        digest = hashlib.sha256(contents).hexdigest()
        key = f"restore/{digest}/{name}"
        contract.update(
            uri=f"s3://knowledge/{key}", sha256=digest, size_bytes=len(contents)
        )
        target = root / "objects/knowledge" / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)
    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    manifest_sha = hashlib.sha256(raw).hexdigest()
    with Lab() as lab:
        pg, _, _ = postgres(lab, postgres_image)
        lab.sql(
            pg,
            (
                ROOT / "contracts/fixtures/knowledge/postgres/control-schema.sql"
            ).read_text(),
        )
        lab.sql(
            pg,
            f"""
insert into knowledge_streams(workspace_id,collection,next_sequence,active_generation_id,active_manifest_sha256,active_target_sequence,stream_version,minimum_ready_replicas,minimum_failure_domains,heartbeat_ttl_ms)
values ('workspace-a','knowledge',13,'generation-bundle-fixture','{manifest_sha}',12,1,1,1,60000);
insert into knowledge_generations(generation_id,workspace_id,collection,status,manifest,manifest_bytes,manifest_sha256,bundle_uri,bundle_sha256,required_sequence)
values ('generation-bundle-fixture','workspace-a','knowledge','active',{sql_literal(raw.decode())}::jsonb,decode('{raw.hex()}','hex'),'{manifest_sha}',{sql_literal(manifest["bundle"]["uri"])},'{manifest["bundle"]["sha256"]}',12);
insert into knowledge_mutations(workspace_id,collection,sequence,mutation_id,generation_id,contract)
values ('workspace-a','knowledge',11,{sql_literal(mutation["mutation_id"])},'generation-bundle-fixture',{sql_literal(json.dumps(mutation))}::jsonb);
insert into knowledge_mutations(workspace_id,collection,sequence,mutation_id,generation_id,contract)
values ('workspace-a','knowledge',12,'delete-12','generation-bundle-fixture',{sql_literal(json.dumps(deletion))}::jsonb);
""",
        )
        lab.command(
            [
                "docker",
                "exec",
                pg,
                "pg_dump",
                "-U",
                "postgres",
                "-Fc",
                "--no-owner",
                "--no-privileges",
                "-f",
                "/tmp/control.pgdump",
                "restore",
            ]
        )
        lab.command(
            [
                "docker",
                "cp",
                f"{pg}:/tmp/control.pgdump",
                str(root / "knowledge-control.pgdump"),
            ]
        )
    (root / "backup-scope.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "backup_id": "fixture-backup",
                "workspace": "workspace-a",
                "collection": "knowledge",
                "bucket": "knowledge",
                "disposable_local_akidb_indexes_included": False,
            }
        )
    )
    archive = destination / "backup.tar.gz"
    with tarfile.open(archive, "w:gz") as target:
        target.add(root, arcname=root.name)
    return archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--postgres-image", default="postgres:17-alpine")
    parser.add_argument("--seaweedfs-image", default="chrislusf/seaweedfs:4.47")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="akidb-backup-fixture-") as directory:
        args.archive = make_fixture(Path(directory), args.postgres_image)
        args.sha256 = digest_file(args.archive)
        verify(args)
    print(
        "PASS: real PostgreSQL dump and SeaweedFS objects restored into a blank AkiDB replica"
    )


if __name__ == "__main__":
    main()
