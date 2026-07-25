#!/usr/bin/env python3
"""Prepare and exercise the immutable generation-serving preview.

The shell harness owns MinIO and server process lifecycle. This helper creates
two deterministic generation bundles and drives the authenticated gRPC
publication/read/rollback assertions on either side of a real process restart.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

import grpc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdks" / "python"))

from akidb import akidb_pb2 as pb  # noqa: E402
from akidb import akidb_pb2_grpc as pb_grpc  # noqa: E402

WORKSPACE = "workspace-a"
COLLECTION = "knowledge"
AGENT = "generation-serving-qa"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compact_json(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        + b"\n"
    )


def set_toml_value(text: str, section: str, key: str, value: str) -> str:
    lines = text.splitlines()
    current = ""
    replaced = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            current = stripped[1:-1]
            continue
        if current == section and stripped.startswith(f"{key} ="):
            lines[index] = f"{key} = {value}"
            replaced = True
            break
    if not replaced:
        raise RuntimeError(f"missing [{section}] {key} in default configuration")
    return "\n".join(lines) + "\n"


def prepare_generation(
    fixture_manifest: dict[str, Any],
    fixture_entries: list[dict[str, Any]],
    output: Path,
    suffix: str,
    parent_generation_id: str | None,
) -> None:
    generation_id = f"generation-{suffix}"
    chunk_id = f"chunk-{suffix}"
    entity_id = f"entity-{suffix}"
    doc_id = f"doc-{suffix}"
    source_uri = f"s3://knowledge/documents/{doc_id}"

    entries = json.loads(json.dumps(fixture_entries))
    header = entries[0]["header"]
    header["generation_id"] = generation_id

    record = entries[1]["record"]
    record.update(
        {
            "chunk_id": chunk_id,
            "doc_id": doc_id,
            "doc_version": f"version-{suffix}",
            "chunk_text": f"{suffix} grounded text",
            "vector": [1.0, 0.0, 0.0] if suffix == "a" else [0.0, 1.0, 0.0],
        }
    )
    record["metadata"]["source_uri"] = source_uri

    entries[2]["node"]["node_id"] = chunk_id
    entries[2]["node"]["properties"]["doc_id"] = doc_id
    entries[3]["node"]["node_id"] = entity_id

    edge = entries[4]["edge"]
    edge.update(
        {
            "edge_id": f"edge-{suffix}",
            "from_node_id": chunk_id,
            "to_node_id": entity_id,
            "source_uri": source_uri,
            "source_version": f"version-{suffix}",
            "evidence_chunk_ids": [chunk_id],
        }
    )

    bundle = b"".join(compact_json(entry) for entry in entries)
    bundle_digest = sha256(bundle)
    bundle_path = output / f"bundle-{suffix}.ndjson"
    bundle_path.write_bytes(bundle)

    manifest = json.loads(json.dumps(fixture_manifest))
    manifest.update(
        {
            "generation_id": generation_id,
            "parent_generation_id": parent_generation_id,
            "created_at_ms": fixture_manifest["created_at_ms"]
            + (0 if suffix == "a" else 1),
        }
    )
    if parent_generation_id is None:
        manifest.pop("parent_generation_id", None)
    manifest["bundle"].update(
        {
            "uri": (
                f"s3://knowledge/generations/{bundle_digest}/bundle-{suffix}.ndjson"
            ),
            "sha256": bundle_digest,
            "size_bytes": len(bundle),
        }
    )
    (output / f"manifest-{suffix}.json").write_bytes(compact_json(manifest))


def command_prepare(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    fixture_dir = ROOT / "contracts" / "fixtures" / "knowledge" / "v1" / "valid"
    fixture_manifest = json.loads((fixture_dir / "bundle-manifest.json").read_text())
    fixture_entries = [
        json.loads(line)
        for line in (fixture_dir / "bundle.ndjson").read_text().splitlines()
        if line
    ]
    prepare_generation(fixture_manifest, fixture_entries, output, "a", None)
    prepare_generation(
        fixture_manifest,
        fixture_entries,
        output,
        "b",
        "generation-a",
    )

    config = (ROOT / "config" / "default.toml").read_text()
    updates = [
        ("auth", "mode", json.dumps("required")),
        ("auth", "token_file", json.dumps(str(output / "data-plane.token"))),
        ("generation_serving", "enabled", "true"),
        ("generation_serving", "replica_id", json.dumps("generation-qa-replica")),
        (
            "generation_serving",
            "generation_root",
            json.dumps(str(output / "generations")),
        ),
        (
            "generation_serving",
            "control_rocksdb_path",
            json.dumps(str(output / "control")),
        ),
        (
            "generation_serving",
            "download_path",
            json.dumps(str(output / "downloads")),
        ),
        ("generation_serving", "default_collection", json.dumps(COLLECTION)),
        (
            "generation_serving",
            "control_token_file",
            json.dumps(str(output / "generation-control.token")),
        ),
        ("generation_serving", "allowed_buckets", json.dumps(["knowledge"])),
        ("storage", "rocksdb_path", json.dumps(str(output / "legacy-rocksdb"))),
        ("storage", "wal_path", json.dumps(str(output / "legacy-wal"))),
        ("storage.minio", "endpoint", json.dumps(args.minio_endpoint)),
        ("storage.minio", "bucket", json.dumps("knowledge")),
        ("storage.minio", "access_key", json.dumps(args.minio_access_key)),
        ("storage.minio", "secret_key", json.dumps(args.minio_secret_key)),
        ("storage.minio", "use_ssl", "false"),
    ]
    for section, key, value in updates:
        config = set_toml_value(config, section, key, value)
    (output / "akidb.toml").write_text(config)


class GenerationQa:
    def __init__(self, address: str, artifacts: Path, snapshot: Path) -> None:
        self.artifacts = artifacts
        self.snapshot = snapshot
        data_token = os.environ.get("AKIDB_AUTH_TOKEN", "")
        control_token = os.environ.get("AKIDB_GENERATION_CONTROL_TOKEN", "")
        if not data_token or not control_token or data_token == control_token:
            raise RuntimeError("distinct data and generation-control tokens are required")
        self.data_metadata = (
            ("authorization", f"Bearer {data_token}"),
            ("x-akidb-workspace", WORKSPACE),
            ("x-akidb-agent", AGENT),
        )
        self.control_metadata = (
            ("authorization", f"Bearer {control_token}"),
            ("x-akidb-workspace", WORKSPACE),
            ("x-akidb-agent", AGENT),
        )
        self.channel = grpc.insecure_channel(address)
        grpc.channel_ready_future(self.channel).result(timeout=30)
        self.data = pb_grpc.AkidbStub(self.channel)
        self.control = pb_grpc.GenerationManagementStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def manifest(self, suffix: str) -> tuple[bytes, dict[str, Any]]:
        data = (self.artifacts / f"manifest-{suffix}.json").read_bytes()
        return data, json.loads(data)

    def stage(self, suffix: str) -> pb.GenerationReplicaStatus:
        manifest_bytes, _ = self.manifest(suffix)
        return self.control.StageGeneration(
            pb.StageGenerationRequest(
                manifest_json=manifest_bytes,
                manifest_sha256=sha256(manifest_bytes),
            ),
            metadata=self.control_metadata,
            timeout=30,
        )

    def status(self) -> pb.GenerationReplicaStatus:
        return self.control.GetGenerationStatus(
            pb.GetGenerationStatusRequest(
                workspace_id=WORKSPACE,
                collection=COLLECTION,
            ),
            metadata=self.control_metadata,
            timeout=10,
        )

    def activate(
        self,
        generation_id: str,
        expected: str | None,
    ) -> pb.GenerationReplicaStatus:
        precondition = (
            pb.ActiveGenerationPrecondition(no_active=True)
            if expected is None
            else pb.ActiveGenerationPrecondition(generation_id=expected)
        )
        return self.control.ActivateGeneration(
            pb.ActivateGenerationRequest(
                workspace_id=WORKSPACE,
                collection=COLLECTION,
                generation_id=generation_id,
                expected_active=precondition,
            ),
            metadata=self.control_metadata,
            timeout=10,
        )

    def rollback(self, target: str, expected: str) -> pb.GenerationReplicaStatus:
        return self.control.RollbackGeneration(
            pb.RollbackGenerationRequest(
                workspace_id=WORKSPACE,
                collection=COLLECTION,
                target_generation_id=target,
                expected_active=pb.ActiveGenerationPrecondition(
                    generation_id=expected
                ),
            ),
            metadata=self.control_metadata,
            timeout=10,
        )

    def dense(self) -> tuple[str, str]:
        response = self.data.Search(
            pb.SearchRequest(
                collection=COLLECTION,
                query=[1.0, 0.0, 0.0],
                top_k=1,
            ),
            metadata=self.data_metadata,
            timeout=10,
        )
        if len(response.results) != 1 or not response.HasField("serving_generation"):
            raise AssertionError("dense response is missing result or generation evidence")
        return response.serving_generation.generation_id, response.results[0].id

    def capture_alpha(self) -> dict[str, Any]:
        response = self.data.TextSearch(
            pb.TextSearchRequest(
                collection=COLLECTION,
                text="alpha grounded",
                top_k=1,
                retrieval_mode="bm25",
                pack=True,
                pack_token_budget=256,
            ),
            metadata=self.data_metadata,
            timeout=10,
        )
        if [result.id for result in response.results] != ["chunk-a"]:
            raise AssertionError("generation A lexical result differs from fixture")
        evidence = response.serving_generation
        if evidence.generation_id != "generation-a":
            raise AssertionError("generation A evidence is missing")
        return {
            "result_ids": [result.id for result in response.results],
            "result_metadata": [result.metadata for result in response.results],
            "context_pack": response.context_pack,
            "generation_id": evidence.generation_id,
            "manifest_sha256": evidence.manifest_sha256,
            "applied_sequence": evidence.applied_sequence,
        }

    def assert_control_token_is_separate(self) -> None:
        try:
            self.control.GetGenerationStatus(
                pb.GetGenerationStatusRequest(
                    workspace_id=WORKSPACE,
                    collection=COLLECTION,
                ),
                metadata=self.data_metadata,
                timeout=5,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAUTHENTICATED:
                return
            raise
        raise AssertionError("data-plane token was accepted by GenerationManagement")

    def initial(self) -> None:
        self.assert_control_token_is_separate()
        staged_a = self.stage("a")
        if staged_a.HasField("active") or staged_a.staged.generation_id != "generation-a":
            raise AssertionError("staging generation A changed the active pointer")
        active_a = self.activate("generation-a", None)
        if active_a.active.generation_id != "generation-a":
            raise AssertionError("generation A did not activate")
        if self.dense() != ("generation-a", "chunk-a"):
            raise AssertionError("generation A dense read is inconsistent")
        self.snapshot.write_text(json.dumps(self.capture_alpha(), sort_keys=True))

        staged_b = self.stage("b")
        if (
            staged_b.active.generation_id != "generation-a"
            or staged_b.staged.generation_id != "generation-b"
        ):
            raise AssertionError("staging generation B disturbed generation A")
        if self.dense() != ("generation-a", "chunk-a"):
            raise AssertionError("generation A stopped serving during generation B staging")

        observed: list[tuple[str, str]] = [self.dense()]
        stop = threading.Event()
        lock = threading.Lock()
        errors: list[BaseException] = []

        def reader() -> None:
            while not stop.is_set():
                try:
                    value = self.dense()
                    with lock:
                        observed.append(value)
                except BaseException as error:  # surfaced in the parent thread
                    with lock:
                        errors.append(error)
                    stop.set()

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for thread in threads:
            thread.start()
        time.sleep(0.15)
        self.activate("generation-b", "generation-a")
        observed.append(self.dense())
        time.sleep(0.15)
        stop.set()
        for thread in threads:
            thread.join(timeout=10)
        if errors:
            raise errors[0]
        allowed = {
            ("generation-a", "chunk-a"),
            ("generation-b", "chunk-b"),
        }
        if not set(observed).issubset(allowed) or not allowed.issubset(set(observed)):
            raise AssertionError(f"atomic cutover observed invalid pairs: {set(observed)}")

    def after_restart(self) -> None:
        status = self.status()
        if (
            status.active.generation_id != "generation-b"
            or status.previous.generation_id != "generation-a"
        ):
            raise AssertionError("restart did not recover active B and rollback A")
        if self.dense() != ("generation-b", "chunk-b"):
            raise AssertionError("generation B did not resume after restart")
        health = self.data.Health(
            pb.HealthRequest(),
            metadata=self.data_metadata,
            timeout=10,
        )
        if not health.ready or health.serving_generation.generation_id != "generation-b":
            raise AssertionError("health does not report recovered generation B")

        rolled_back = self.rollback("generation-a", "generation-b")
        if rolled_back.active.generation_id != "generation-a":
            raise AssertionError("rollback did not reactivate generation A")
        expected = json.loads(self.snapshot.read_text())
        if self.capture_alpha() != expected:
            raise AssertionError("rollback did not restore exact results and citations")

        try:
            self.data.Insert(
                pb.InsertRequest(
                    collection=COLLECTION,
                    id="forbidden",
                    vector=[1.0, 0.0, 0.0],
                ),
                metadata=self.data_metadata,
                timeout=5,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.FAILED_PRECONDITION:
                return
            raise
        raise AssertionError("mutable write succeeded in generation mode")


def command_exercise(args: argparse.Namespace) -> None:
    qa = GenerationQa(
        args.address,
        Path(args.artifacts).resolve(),
        Path(args.snapshot).resolve(),
    )
    try:
        if args.phase == "initial":
            qa.initial()
        else:
            qa.after_restart()
    finally:
        qa.close()


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare")
    prepare.add_argument("--output", required=True)
    prepare.add_argument("--minio-endpoint", required=True)
    prepare.add_argument("--minio-access-key", required=True)
    prepare.add_argument("--minio-secret-key", required=True)
    prepare.set_defaults(handler=command_prepare)

    exercise = commands.add_parser("exercise")
    exercise.add_argument("--phase", choices=("initial", "after-restart"), required=True)
    exercise.add_argument("--address", required=True)
    exercise.add_argument("--artifacts", required=True)
    exercise.add_argument("--snapshot", required=True)
    exercise.set_defaults(handler=command_exercise)
    return root


def main() -> int:
    args = parser().parse_args()
    args.handler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
