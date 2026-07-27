#!/usr/bin/env python3
"""Build reviewable AkiDB Memory import plans without committing data.

Supported inputs are generic JSONL, the legacy metadata-backed AkiDB memory
document shape, Mem0 exports, Graphiti node exports, and AX Studio SQLite
tables. Every output candidate retains the original ID and canonical source
digest, declares conversion limitations, and receives a deterministic
idempotency key. This tool never contacts AkiDB and never assigns assurance or
decision authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

SCHEMA_VERSION = 1
SUPPORTED_FORMATS = (
    "jsonl",
    "akidb-memory-document",
    "mem0",
    "graphiti",
    "ax-studio-sqlite",
)
MAX_RECORDS = 1_000_000
MAX_INPUT_BYTES = 256 * 1024 * 1024
ABSOLUTE_MAX_INPUT_BYTES = 4 * 1024 * 1024 * 1024
MAX_SOURCE_RECORD_BYTES = 1024 * 1024


class ImportPlanError(ValueError):
    """Input is unsafe, ambiguous, or outside the supported adapter contract."""


@dataclass(frozen=True)
class SourceRecord:
    ordinal: int
    value: dict[str, Any]
    source_location: str


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise ImportPlanError(f"value is not bounded canonical JSON: {error}") from error


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_text(field: str, value: Any, *, maximum: int = 1_048_576) -> str:
    if not isinstance(value, str):
        raise ImportPlanError(f"{field} must be a string")
    if (
        not value
        or value.strip() != value
        or "\x00" in value
        or len(value.encode("utf-8")) > maximum
    ):
        raise ImportPlanError(
            f"{field} must be non-empty, trimmed, NUL-free, and at most {maximum} bytes"
        )
    return value


def optional_text(field: str, value: Any) -> str | None:
    if value is None or value == "":
        return None
    return canonical_text(field, value, maximum=4096)


def read_json_records(path: Path) -> Iterator[SourceRecord]:
    with path.open("r", encoding="utf-8") as handle:
        first_nonspace = ""
        while True:
            character = handle.read(1)
            if not character:
                break
            if not character.isspace():
                first_nonspace = character
                break
        handle.seek(0)
        if first_nonspace == "[":
            value = json.load(handle)
            if not isinstance(value, list):
                raise ImportPlanError("JSON root must be an array")
            for ordinal, record in enumerate(value, start=1):
                if not isinstance(record, dict):
                    raise ImportPlanError(f"record {ordinal} must be an object")
                yield SourceRecord(ordinal, record, f"{path}#record={ordinal}")
            return
        for ordinal, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ImportPlanError(f"{path}:{ordinal}: invalid JSON: {error}") from error
            if not isinstance(record, dict):
                raise ImportPlanError(f"{path}:{ordinal}: record must be an object")
            yield SourceRecord(ordinal, record, f"{path}:line={ordinal}")


def quote_sql_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ImportPlanError("SQLite table name is invalid")
    return '"' + value.replace('"', '""') + '"'


def sqlite_records(path: Path, requested_table: str | None) -> Iterator[SourceRecord]:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
            if not str(row[0]).startswith("sqlite_")
        }
        if requested_table is not None:
            if requested_table not in tables:
                raise ImportPlanError(
                    f"SQLite table {requested_table!r} does not exist; available: {sorted(tables)}"
                )
            table = requested_table
        else:
            table = next(
                (
                    candidate
                    for candidate in ("memories", "memory", "documents", "episodes")
                    if candidate in tables
                ),
                None,
            )
            if table is None:
                raise ImportPlanError(
                    "cannot infer AX Studio memory table; pass --sqlite-table "
                    f"(available: {sorted(tables)})"
                )
        query = f"SELECT rowid AS __akidb_rowid__, * FROM {quote_sql_identifier(table)}"
        for ordinal, row in enumerate(connection.execute(query), start=1):
            value = {
                key: row[key]
                for key in row.keys()
                if isinstance(row[key], (str, int, float, bytes, type(None)))
            }
            for key, item in list(value.items()):
                if isinstance(item, bytes):
                    value[key] = {
                        "encoding": "hex",
                        "sha256": sha256(item),
                        "bytes": len(item),
                    }
            yield SourceRecord(
                ordinal,
                value,
                f"{path}#table={table}&rowid={row['__akidb_rowid__']}",
            )
    finally:
        connection.close()


def first_value(record: dict[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


def decode_metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {"legacy_metadata_text": value}
        return decoded if isinstance(decoded, dict) else {"legacy_metadata_value": decoded}
    return {"legacy_metadata_value": value}


def adapter_fields(
    source_format: str,
    source: SourceRecord,
) -> tuple[str, str, str, dict[str, Any], list[str]]:
    record = source.value
    limitations: list[str] = []
    metadata: dict[str, Any] = {}

    if source_format == "jsonl":
        original_id = first_value(record, ("id", "memory_id", "uuid", "key"))
        text = first_value(record, ("text", "memory", "content", "summary", "value"))
        predicate = first_value(record, ("predicate", "kind", "type"))
        metadata = decode_metadata(record.get("metadata"))
        limitations.append(
            "generic JSONL has no portable assurance, authority, or temporal semantics"
        )
    elif source_format == "akidb-memory-document":
        original_id = first_value(record, ("id", "vector_id", "document_id"))
        text = first_value(record, ("text", "content"))
        metadata = decode_metadata(record.get("metadata"))
        predicate = first_value(
            metadata, ("predicate", "memory_kind", "kind", "type")
        )
        limitations.extend(
            [
                "legacy metadata-backed memory is not an authoritative Memory version",
                "legacy vector embeddings are not imported as canonical evidence",
            ]
        )
    elif source_format == "mem0":
        original_id = first_value(record, ("id", "memory_id", "uuid"))
        text = first_value(record, ("memory", "text", "content"))
        predicate = first_value(record, ("type", "category"))
        metadata = decode_metadata(record.get("metadata"))
        limitations.extend(
            [
                "Mem0 update/history relations require source-specific review",
                "Mem0 scores and inferred identity do not become AkiDB authority",
            ]
        )
    elif source_format == "graphiti":
        original_id = first_value(record, ("uuid", "id", "node_id", "edge_id"))
        text = first_value(record, ("summary", "fact", "name", "content"))
        predicate = first_value(record, ("predicate", "type", "group_id"))
        metadata = decode_metadata(record.get("metadata"))
        limitations.extend(
            [
                "Graphiti graph edges are retained as metadata for separate relation review",
                "Graphiti temporal fields are not inferred when their meaning is ambiguous",
            ]
        )
    elif source_format == "ax-studio-sqlite":
        original_id = first_value(
            record, ("id", "uuid", "memory_id", "document_id", "__akidb_rowid__")
        )
        text = first_value(
            record, ("text", "memory", "content", "summary", "value", "body")
        )
        predicate = first_value(record, ("predicate", "kind", "type", "category"))
        metadata = {
            key: value
            for key, value in record.items()
            if key
            not in {
                "text",
                "memory",
                "content",
                "summary",
                "value",
                "body",
            }
        }
        limitations.extend(
            [
                "AX Studio SQLite schemas are introspected and require field-level review",
                "model/compiler provenance is not inferred from database rows",
            ]
        )
    else:
        raise ImportPlanError(f"unsupported source format: {source_format}")

    if original_id is None:
        original_id = f"ordinal:{source.ordinal}"
        limitations.append("source record had no stable ID; ordinal fallback used")
    original_id = canonical_text("original_id", str(original_id), maximum=4096)
    text = canonical_text("content", text)
    predicate = (
        canonical_text("predicate", str(predicate), maximum=4096)
        if predicate not in (None, "")
        else "legacy imported memory"
    )
    return original_id, predicate, text, metadata, sorted(set(limitations))


def normalized_identity(
    workspace: str, namespace: str, entity_key: str, predicate: str
) -> str:
    return sha256(
        canonical_json(
            {
                "workspace_id": workspace,
                "namespace": namespace,
                "entity_key": entity_key,
                "predicate": " ".join(predicate.casefold().split()),
                "kind": "text_fact",
            }
        )
    )


def build_candidate(
    source_format: str,
    source: SourceRecord,
    *,
    workspace: str,
    namespace: str,
    purpose: str,
    entity_prefix: str,
    data_subject_field: str | None,
) -> dict[str, Any]:
    original_id, predicate, text, metadata, limitations = adapter_fields(
        source_format, source
    )
    source_bytes = canonical_json(source.value)
    if len(source_bytes) > MAX_SOURCE_RECORD_BYTES:
        raise ImportPlanError(
            f"{source.source_location}: canonical source record exceeds "
            f"{MAX_SOURCE_RECORD_BYTES} bytes"
        )
    original_sha256 = sha256(source_bytes)
    entity_hint = first_value(
        source.value, ("entity_key", "entity", "user_id", "agent_id", "group_id")
    )
    entity_key = (
        canonical_text("entity_key", str(entity_hint), maximum=4096)
        if entity_hint not in (None, "")
        else f"{entity_prefix}:{original_id}"
    )
    data_subject_id = None
    if data_subject_field is not None:
        data_subject_id = optional_text(
            data_subject_field, source.value.get(data_subject_field)
        )
    identity_sha256 = normalized_identity(
        workspace, namespace, entity_key, predicate
    )
    return {
        "record_type": "memory_import_candidate",
        "schema_version": SCHEMA_VERSION,
        "source_format": source_format,
        "source_location": source.source_location,
        "original_id": original_id,
        "original_sha256": original_sha256,
        "idempotency_key": f"memory-import-v1:{source_format}:{original_sha256}",
        "scope": {
            "workspace_id": workspace,
            "namespace": namespace,
            "entity_key": entity_key,
            "data_subject_id": data_subject_id,
            "sensitivity": "internal",
            "allowed_purposes": [purpose],
        },
        "predicate": predicate,
        "content": {"type": "text_fact", "text": text},
        "epistemic_formation": "agent_statement",
        "evidence": {
            "source_plane": f"import:{source_format}",
            "source_id": original_id,
            "content_sha256": original_sha256,
        },
        "identity_sha256": identity_sha256,
        "metadata_for_review": metadata,
        "limitations": limitations,
    }


def load_checkpoint(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ImportPlanError("checkpoint schema version is unsupported")
    hashes = value.get("completed_source_sha256")
    if not isinstance(hashes, list) or any(not isinstance(item, str) for item in hashes):
        raise ImportPlanError("checkpoint completed hashes are invalid")
    return set(hashes)


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def build_plan(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_records <= 0 or args.max_records > MAX_RECORDS:
        raise ImportPlanError(f"--max-records must be between 1 and {MAX_RECORDS}")
    max_input_bytes = getattr(args, "max_input_bytes", MAX_INPUT_BYTES)
    if max_input_bytes <= 0 or max_input_bytes > ABSOLUTE_MAX_INPUT_BYTES:
        raise ImportPlanError(
            f"--max-input-bytes must be between 1 and {ABSOLUTE_MAX_INPUT_BYTES}"
        )
    input_bytes = args.input.stat().st_size
    if input_bytes > max_input_bytes:
        raise ImportPlanError(
            f"input has {input_bytes} bytes and exceeds "
            f"--max-input-bytes={max_input_bytes}"
        )
    for field in ("workspace", "namespace", "purpose", "entity_prefix"):
        canonical_text(field, getattr(args, field), maximum=4096)
    if args.source_format == "ax-studio-sqlite":
        records = sqlite_records(args.input, args.sqlite_table)
    else:
        if args.sqlite_table is not None:
            raise ImportPlanError("--sqlite-table is only valid for ax-studio-sqlite")
        records = read_json_records(args.input)

    completed = load_checkpoint(args.checkpoint)
    candidates: list[dict[str, Any]] = []
    skipped = 0
    for source in records:
        if source.ordinal > args.max_records:
            raise ImportPlanError(
                f"input exceeds --max-records={args.max_records}; no partial plan was written"
            )
        candidate = build_candidate(
            args.source_format,
            source,
            workspace=args.workspace,
            namespace=args.namespace,
            purpose=args.purpose,
            entity_prefix=args.entity_prefix,
            data_subject_field=args.data_subject_field,
        )
        if candidate["original_sha256"] in completed:
            skipped += 1
            continue
        candidates.append(candidate)

    collision_members: dict[str, list[str]] = {}
    for candidate in candidates:
        collision_members.setdefault(candidate["identity_sha256"], []).append(
            candidate["original_sha256"]
        )
    for candidate in candidates:
        members = sorted(collision_members[candidate["identity_sha256"]])
        candidate["collision"] = {
            "status": "review_required" if len(members) > 1 else "none",
            "source_sha256": members,
        }

    input_sha256 = sha256_file(args.input)
    manifest = {
        "record_type": "memory_import_manifest",
        "schema_version": SCHEMA_VERSION,
        "source_format": args.source_format,
        "input_path": str(args.input),
        "input_bytes": input_bytes,
        "input_sha256": input_sha256,
        "workspace_id": args.workspace,
        "namespace": args.namespace,
        "request_purpose": args.purpose,
        "candidate_count": len(candidates),
        "checkpoint_skipped_count": skipped,
        "collision_group_count": sum(
            1 for members in collision_members.values() if len(members) > 1
        ),
        "commit_behavior": "DRY_RUN_ONLY",
        "authority_behavior": "NO_ASSURANCE_OR_AUTHORITY_ASSIGNMENT",
    }
    plan_digest = sha256(canonical_json([manifest, *candidates]))
    manifest["plan_sha256"] = plan_digest
    return {"manifest": manifest, "candidates": candidates}


def write_plan(path: Path, plan: dict[str, Any]) -> None:
    if path.exists():
        raise ImportPlanError(
            f"output already exists: {path}; choose a new path to preserve immutable review evidence"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ImportPlanError(
            f"output already exists: {path}; choose a new path to preserve immutable review evidence"
        ) from error
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        for record in [plan["manifest"], *plan["candidates"]]:
            handle.write(canonical_json(record).decode("utf-8"))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-format", choices=SUPPORTED_FORMATS, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--sqlite-table")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--purpose", required=True)
    parser.add_argument("--entity-prefix", default="imported-memory")
    parser.add_argument("--data-subject-field")
    parser.add_argument("--max-records", type=int, default=100_000)
    parser.add_argument("--max-input-bytes", type=int, default=MAX_INPUT_BYTES)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.input.is_file():
        raise ImportPlanError(f"input does not exist or is not a file: {args.input}")
    plan = build_plan(args)
    write_plan(args.output, plan)
    if args.checkpoint is not None:
        completed = load_checkpoint(args.checkpoint)
        completed.update(
            candidate["original_sha256"] for candidate in plan["candidates"]
        )
        atomic_write_json(
            args.checkpoint,
            {
                "schema_version": SCHEMA_VERSION,
                "completed_source_sha256": sorted(completed),
                "last_plan_sha256": plan["manifest"]["plan_sha256"],
            },
        )
    print(json.dumps(plan["manifest"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ImportPlanError, OSError, sqlite3.Error, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2)
