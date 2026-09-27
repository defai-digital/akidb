#!/usr/bin/env python3
"""Verify a canonical backup using isolated PostgreSQL, SeaweedFS, and a blank replica.

Requires Docker, boto3, and the Python SDK's grpc runtime. No production endpoint
is accepted. The archive digest and local generation-postgres binary are explicit.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import secrets
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

import tomllib

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest_file(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def extract_backup(archive, expected, destination, max_bytes=100 * 1024**3):
    require(
        len(expected) == 64 and digest_file(archive) == expected.lower(),
        "backup digest mismatch",
    )
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        require(
            sum(m.size for m in members) <= max_bytes, "backup exceeds extraction limit"
        )
        names = set()
        for member in members:
            path = PurePosixPath(member.name)
            require(
                not path.is_absolute() and ".." not in path.parts, "unsafe backup path"
            )
            require(
                member.isdir() or member.isfile(),
                "backup links and special files are forbidden",
            )
            require(member.name not in names, "duplicate backup path")
            names.add(member.name)
        source.extractall(destination, members=members, filter="data")
    roots = list(destination.iterdir())
    require(
        len(roots) == 1 and roots[0].is_dir(),
        "backup must contain exactly one root directory",
    )
    scope = json.loads((roots[0] / "backup-scope.json").read_text())
    require(scope.get("schema_version") == 2, "unsupported backup scope version")
    require(
        scope.get("disposable_local_akidb_indexes_included") is False,
        "backup must contain canonical inputs",
    )
    for field in ("backup_id", "workspace", "collection", "bucket"):
        require(
            isinstance(scope.get(field), str) and bool(scope[field]),
            "invalid backup scope",
        )
    require(
        (roots[0] / "knowledge-control.pgdump").is_file(), "control dump is missing"
    )
    return roots[0], scope


def object_path(root, bucket, reference):
    uri = urlsplit(reference["uri"])
    require(
        uri.scheme == "s3" and uri.netloc == bucket and not uri.fragment,
        "object outside backup bucket",
    )
    # s3 sync backups do not preserve historical version IDs. Never silently use
    # the current object for a version-specific reference.
    require(
        not uri.query, "version-specific object requires a version-preserving backup"
    )
    key = unquote(uri.path.removeprefix("/"))
    path = PurePosixPath(key)
    require(
        key and not path.is_absolute() and ".." not in path.parts, "unsafe object key"
    )
    target = root / "objects" / bucket / key
    require(
        target.is_file() and not target.is_symlink(), "referenced object is missing"
    )
    require(target.stat().st_size == reference["size_bytes"], "object size mismatch")
    require(digest_file(target) == reference["sha256"], "object digest mismatch")
    return target, key


def logical_records(root, bucket, manifest, required, contracts):
    """Independent oracle: replay canonical records without using AkiDB indexes."""
    path, _ = object_path(root, bucket, manifest["bundle"])
    compression = manifest["bundle_compression"]
    require(compression in ("none", "zstd"), "unsupported bundle compression")
    if compression == "zstd":
        import zstandard

        stream = io.TextIOWrapper(
            zstandard.ZstdDecompressor().stream_reader(path.open("rb"))
        )
    else:
        stream = path.open("rt")
    records = {}
    decoded = 0
    with stream:
        for line in iter(lambda: stream.readline(16 * 1024**2 + 1), ""):
            require(
                len(line.encode()) <= 16 * 1024**2, "bundle line exceeds restore limit"
            )
            decoded += len(line.encode())
            require(
                decoded <= 2 * 1024**3, "restore oracle exceeds decoded byte budget"
            )
            entry = json.loads(line)
            if entry["entry_type"] == "record":
                record = entry["record"]
                require(record["chunk_id"] not in records, "duplicate canonical record")
                records[record["chunk_id"]] = record
    require(
        len(records) == manifest["expected_vector_count"],
        "bundle record count mismatch",
    )
    after = manifest["target_sequence"]
    require(after == manifest["base_sequence"], "unsupported initial mutation range")
    deleted = set()
    for contract in contracts:
        require(contract["sequence"] == after + 1, "mutation sequence gap")
        for key in ("workspace_id", "collection", "generation_id"):
            require(contract[key] == manifest[key], "mutation scope mismatch")
        operation = contract["operation"]
        if operation == "upsert":
            payload_path, _ = object_path(root, bucket, contract["payload"])
            payload = json.loads(payload_path.read_text())
            for key in (
                "workspace_id",
                "collection",
                "generation_id",
                "mutation_id",
                "sequence",
            ):
                require(payload[key] == contract[key], "payload identity mismatch")
            record = payload["record"]
            require(
                record["chunk_id"] == contract["chunk_id"], "payload chunk mismatch"
            )
            records[record["chunk_id"]] = record
            deleted.discard(record["chunk_id"])
        else:
            require(
                operation == "delete" and not contract.get("payload"),
                "invalid mutation operation",
            )
            records.pop(contract["chunk_id"], None)
            deleted.add(contract["chunk_id"])
        after = contract["sequence"]
    require(after == required, "incomplete mutation tail")
    return records, deleted


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Lab:
    def __init__(self, timeout=120):
        self.prefix = "akidb-restore-" + uuid.uuid4().hex[:12]
        self.containers = []
        self.network = False
        self.timeout = timeout

    def command(self, args, *, input=None, timeout=None, environment=None):
        result = subprocess.run(
            args,
            input=input,
            env={**os.environ, **(environment or {})},
            capture_output=True,
            timeout=timeout or self.timeout,
            check=False,
        )
        if result.returncode:
            # Never echo commands or raw service output containing credentials.
            raise RuntimeError(
                f"{args[0]} command failed with exit {result.returncode}"
            )
        return result.stdout

    def __enter__(self):
        self.command(["docker", "network", "create", self.prefix])
        self.network = True
        return self

    def start(self, suffix, image, port, arguments, environment=None):
        name = self.prefix + "-" + suffix
        self.containers.append(name)
        args = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            self.prefix,
            "--publish",
            f"127.0.0.1::{port}",
        ]
        for key in environment or {}:
            args += ["--env", key]
        self.command(args + image + arguments, environment=environment)
        address = (
            self.command(["docker", "port", name, f"{port}/tcp"])
            .decode()
            .strip()
            .splitlines()[0]
        )
        return name, int(address.rsplit(":", 1)[1])

    def sql(self, container, query, database="restore"):
        return (
            self.command(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "psql",
                    "-XAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "postgres",
                    "-d",
                    database,
                ],
                input=query.encode(),
            )
            .decode()
            .strip()
        )

    def rows(self, container, query):
        return [
            json.loads(line) for line in self.sql(container, query).splitlines() if line
        ]

    def __exit__(self, *_):
        for name in reversed(self.containers):
            subprocess.run(
                ["docker", "rm", "-f", name],
                capture_output=True,
                timeout=30,
                check=False,
            )
        if self.network:
            subprocess.run(
                ["docker", "network", "rm", self.prefix],
                capture_output=True,
                timeout=30,
                check=False,
            )


def wait_until(action, timeout, fatal_check=None):
    end = time.monotonic() + timeout
    while True:
        if fatal_check is not None:
            fatal_check()
        try:
            return action()
        except Exception:
            if time.monotonic() >= end:
                raise
            time.sleep(0.25)


def postgres(lab, image):
    password = secrets.token_hex(24)
    name, port = lab.start(
        "pg",
        [image],
        5432,
        [],
        {"POSTGRES_PASSWORD": password, "POSTGRES_DB": "restore"},
    )
    wait_until(lambda: lab.sql(name, "select 1"), 60)
    return name, port, password


def seaweedfs(lab, image):
    import boto3
    from botocore.config import Config

    key, secret = "restore", secrets.token_hex(24)
    identities = {
        "identities": [
            {
                "name": "restore",
                "credentials": [{"accessKey": key, "secretKey": secret}],
                "actions": ["Admin", "Read", "Write", "List", "Tagging"],
            }
        ]
    }
    name, port = lab.start(
        "s3",
        ["--entrypoint", "/bin/sh", image],
        8333,
        [
            "-c",
            'umask 077; printf "%s" "$S3_CONFIG" > /tmp/s3.json; exec weed server -filer -s3 -dir=/data -ip.bind=0.0.0.0 -s3.port=8333 -s3.config=/tmp/s3.json -volume.max=0 -master.volumeSizeLimitMB=256',
        ],
        {"S3_CONFIG": json.dumps(identities)},
    )
    client = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=30,
            retries={"max_attempts": 2},
            request_checksum_calculation="when_required",
        ),
    )
    wait_until(lambda: client.list_buckets(), 60)
    return name, port, key, secret, client


def replica_config(path, scope, replica_id, s3_port, access, secret):
    # All paths and credentials are private, newly generated, and independent of
    # operator runtime configs and environment overrides.
    quote = json.dumps
    text = f"""[auth]
mode = "required"
[auth.acl]
default_workspace = {quote(scope["workspace"])}
enforce_workspace = true
[generation_serving]
enabled = true
replica_id = {quote(replica_id)}
generation_root = {quote(str(path / "generations"))}
control_rocksdb_path = {quote(str(path / "control"))}
download_path = {quote(str(path / "downloads"))}
default_collection = {quote(scope["collection"])}
allowed_buckets = [{quote(scope["bucket"])}]
require_version_or_digest_key = true
minimum_free_bytes_after_build = 0
[generation_serving.replica_control]
enabled = true
postgres_tls_mode = "disable"
endpoint = "http://127.0.0.1:50051"
failure_domain = "isolated-restore"
poll_interval_ms = 100
heartbeat_interval_ms = 1000
generation_gc_enabled = false
[storage]
wal_enabled = false
rocksdb_path = {quote(str(path / "rocksdb"))}
wal_path = {quote(str(path / "wal"))}
[storage.seaweedfs]
endpoint = "http://127.0.0.1:{s3_port}"
bucket = {quote(scope["bucket"])}
access_key = {quote(access)}
secret_key = {quote(secret)}
use_ssl = false
[embedding]
enabled = false
url = "http://127.0.0.1:8081/v1/embeddings"
model = "restore-disabled"
dimensions = 3
timeout_ms = 10000
max_batch_size = 32
"""
    # Start from the canonical complete configuration: several root sections and
    # their fields are required even when generation serving bypasses them.
    lines = (ROOT / "config/default.toml").read_text().splitlines()

    def apply(section, values):
        for key, value in values.items():
            if isinstance(value, dict):
                apply(f"{section}.{key}" if section else key, value)
                continue
            current = ""
            for index, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith("[") and stripped.endswith("]"):
                    current = stripped[1:-1]
                elif current == section and stripped.split("=", 1)[0].strip() == key:
                    lines[index] = f"{key} = {json.dumps(value)}"
                    break
            else:
                raise ValueError(
                    f"missing canonical configuration field: {section}.{key}"
                )

    apply("", tomllib.loads(text))
    config = path / "akidb.toml"
    config.write_text("\n".join(lines) + "\n")
    config.chmod(0o600)
    return config


def probe_replica(
    address, token, scope, manifest, required, records, deleted, timeout, process=None
):
    import grpc

    sys.path.insert(0, str(ROOT / "sdks/python"))
    from akidb import akidb_pb2 as pb
    from akidb import akidb_pb2_grpc as rpc

    metadata = (
        ("authorization", f"Bearer {token}"),
        ("x-akidb-workspace", scope["workspace"]),
    )
    expected_digest = manifest["_manifest_sha256"]
    with grpc.insecure_channel(address) as channel:
        data = rpc.AkidbStub(channel)

        def evidence(response):
            value = response.serving_generation
            require(
                value.workspace_id == scope["workspace"]
                and value.collection == scope["collection"]
                and value.generation_id == manifest["generation_id"]
                and value.manifest_sha256 == expected_digest
                and value.applied_sequence == required,
                "serving generation evidence mismatch",
            )

        def ready():
            result = data.Health(pb.HealthRequest(), metadata=metadata, timeout=5)
            require(result.ready, "replica not ready")
            evidence(result)

        wait_until(
            ready,
            timeout,
            lambda: require(
                process is None or process.poll() is None,
                "restore replica exited before becoming ready",
            ),
        )
        for key, record in records.items():
            result = data.Get(
                pb.GetRequest(collection=scope["collection"], id=key),
                metadata=metadata,
                timeout=10,
            )
            evidence(result)
            require(result.found and result.id == key, "restored record missing")
            require(
                len(result.vector) == len(record["vector"])
                and all(
                    math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-6)
                    for a, b in zip(result.vector, record["vector"])
                ),
                "restored vector differs",
            )
            actual = json.loads(result.metadata)
            for field in (
                "doc_id",
                "doc_version",
                "chunk_hash",
                "pipeline_signature",
                "embedding_model_id",
            ):
                require(
                    actual.get(field) == record.get(field),
                    f"restored metadata differs: {field}",
                )
            for field, value in record["metadata"].items():
                require(actual.get(field) == value, "restored source metadata differs")
        for key in deleted:
            try:
                result = data.Get(
                    pb.GetRequest(collection=scope["collection"], id=key),
                    metadata=metadata,
                    timeout=10,
                )
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.NOT_FOUND:
                    raise
            else:
                require(not result.found, "deleted record was restored")
        # Probe bounded retrieval and validate every returned citation against
        # canonical records. ANN is approximate; do not demand arbitrary top-1 IDs.
        probes = 0
        for record in list(records.values())[:10]:
            result = data.Search(
                pb.SearchRequest(
                    collection=scope["collection"], query=record["vector"], top_k=1
                ),
                metadata=metadata,
                timeout=10,
            )
            evidence(result)
            require(
                bool(result.results) and all(r.id in records for r in result.results),
                "invalid restored vector search",
            )
            if record.get("chunk_text"):
                result = data.TextSearch(
                    pb.TextSearchRequest(
                        collection=scope["collection"],
                        text=record["chunk_text"],
                        top_k=3,
                        retrieval_mode="bm25",
                        pack=True,
                        pack_token_budget=4096,
                    ),
                    metadata=metadata,
                    timeout=10,
                )
                evidence(result)
                require(bool(result.results), "restored lexical search is empty")
                citations = [p.citation for p in result.context_pack_v1.items]
                require(bool(citations), "restored citations are missing")
                for citation in citations:
                    expected = records[citation.chunk_id]
                    require(
                        citation.document_id == expected["doc_id"]
                        and citation.document_version == expected["doc_version"]
                        and citation.source_uri == expected["metadata"]["source_uri"]
                        and citation.content_hash == expected["chunk_hash"]
                        and citation.generation_id == manifest["generation_id"],
                        "restored citation differs",
                    )
            probes += 1
    return {
        "records_verified": len(records),
        "deletions_verified": len(deleted),
        "retrieval_probes": probes,
    }


def verify(args):
    output = Path(args.output).resolve()
    require(not output.exists(), "refusing to overwrite restore evidence")
    with (
        tempfile.TemporaryDirectory(prefix="akidb-restore-") as temporary,
        Lab() as lab,
    ):
        temporary = Path(temporary)
        extracted = temporary / "backup"
        extracted.mkdir()
        root, scope = extract_backup(Path(args.archive), args.sha256, extracted)
        require(
            not getattr(args, "backup_id", None)
            or scope["backup_id"] == args.backup_id,
            "backup ID mismatch",
        )
        pg, pg_port, _ = postgres(lab, args.postgres_image)
        lab.command(
            [
                "docker",
                "cp",
                str(root / "knowledge-control.pgdump"),
                f"{pg}:/tmp/control.pgdump",
            ]
        )
        lab.command(
            [
                "docker",
                "exec",
                pg,
                "pg_restore",
                "--exit-on-error",
                "--no-owner",
                "--no-privileges",
                "-U",
                "postgres",
                "-d",
                "restore",
                "/tmp/control.pgdump",
            ],
            timeout=args.timeout,
        )
        where = f"workspace_id={sql_literal(scope['workspace'])} and collection={sql_literal(scope['collection'])}"
        streams = lab.rows(
            pg,
            f"select row_to_json(t) from (select * from knowledge_streams where {where}) t",
        )
        require(
            len(streams) == 1 and streams[0]["active_generation_id"],
            "backup has no unique active generation",
        )
        stream = streams[0]
        ids = {
            stream["active_generation_id"],
            stream.get("publication_generation_id"),
        } - {None}
        generations = []
        objects = {}
        for generation_id in sorted(ids):
            rows = lab.rows(
                pg,
                f"select row_to_json(t) from (select manifest, encode(manifest_bytes,'hex') as bytes, manifest_sha256, required_sequence from knowledge_generations where {where} and generation_id={sql_literal(generation_id)}) t",
            )
            require(len(rows) == 1, "referenced generation is missing")
            row = rows[0]
            raw = bytes.fromhex(row["bytes"])
            require(
                hashlib.sha256(raw).hexdigest() == row["manifest_sha256"]
                and json.loads(raw) == row["manifest"],
                "manifest identity mismatch",
            )
            manifest = row["manifest"]
            for key, value in (
                ("workspace_id", scope["workspace"]),
                ("collection", scope["collection"]),
                ("generation_id", generation_id),
            ):
                require(manifest[key] == value, "manifest scope mismatch")
            contracts = lab.rows(
                pg,
                f"select contract from knowledge_mutations where {where} and generation_id={sql_literal(generation_id)} and sequence>{int(manifest['target_sequence'])} and sequence<={int(row['required_sequence'])} order by sequence",
            )
            records, deleted = logical_records(
                root, scope["bucket"], manifest, row["required_sequence"], contracts
            )
            for reference in [manifest["bundle"]] + [
                c["payload"] for c in contracts if c.get("payload")
            ]:
                path, key = object_path(root, scope["bucket"], reference)
                objects[key] = path
            if generation_id == stream["active_generation_id"]:
                require(
                    row["manifest_sha256"] == stream["active_manifest_sha256"]
                    and row["required_sequence"] == stream["active_target_sequence"],
                    "active authority mismatch",
                )
                active = (manifest, row["required_sequence"], records, deleted)
            manifest["_manifest_sha256"] = row["manifest_sha256"]
            generations.append(
                {
                    "generation_id": generation_id,
                    "manifest_sha256": row["manifest_sha256"],
                    "required_sequence": row["required_sequence"],
                }
            )
        s3, s3_port, access, secret, client = seaweedfs(lab, args.seaweedfs_image)
        client.create_bucket(Bucket=scope["bucket"])
        for key, path in objects.items():
            client.upload_file(str(path), scope["bucket"], key)
        # Use a restricted worker role in the restored database, never the
        # administrative account. The control schema is already restored.
        password = secrets.token_hex(24)
        lab.sql(
            pg,
            f"create role restore_worker login password {sql_literal(password)}; grant usage on schema public to restore_worker; grant select,insert,update,delete on all tables in schema public to restore_worker; grant usage,select on all sequences in schema public to restore_worker; grant execute on all functions in schema public to restore_worker;",
        )
        replica_id = lab.prefix
        volume = temporary / "replica"
        volume.mkdir()
        config = replica_config(volume, scope, replica_id, s3_port, access, secret)
        token = secrets.token_hex(24)
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("AKIDB_", "SEAWEEDFS_"))
        }
        env.update(
            AKIDB_AUTH_TOKEN=token,
            AKIDB_GENERATION_CONTROL_TOKEN=secrets.token_hex(24),
            AKIDB_KNOWLEDGE_POSTGRES_URL=f"postgres://restore_worker:{password}@127.0.0.1:{pg_port}/restore",
        )
        port = free_port()
        address = f"127.0.0.1:{port}"
        with (temporary / "server.log").open("wb") as log:
            process = subprocess.Popen(
                [
                    str(Path(args.server_bin).resolve()),
                    "server",
                    "--config",
                    str(config),
                    "--listen",
                    address,
                    "--metrics-addr",
                    f"127.0.0.1:{free_port()}",
                ],
                stdout=log,
                stderr=log,
                env=env,
                cwd=volume,
            )
            try:
                checks = probe_replica(
                    address, token, scope, *active, args.timeout, process
                )
                # Real dependency outage: serving must retain the active local revision.
                lab.command(["docker", "stop", "-t", "1", s3, pg])
                probe_replica(address, token, scope, *active, 10)
            except Exception as error:
                diagnostic = (temporary / "server.log").read_text(errors="replace")[
                    -4000:
                ]
                for credential in (
                    token,
                    password,
                    secret,
                    env["AKIDB_GENERATION_CONTROL_TOKEN"],
                ):
                    diagnostic = diagnostic.replace(credential, "[redacted]")
                raise RuntimeError(
                    "restore replica verification failed: " + diagnostic
                ) from error
            finally:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        receipt = {
            "schema_version": 1,
            "backup_id": scope["backup_id"],
            "backup_sha256": args.sha256.lower(),
            "workspace": scope["workspace"],
            "collection": scope["collection"],
            "generations": generations,
            "verified_objects": len(objects),
            "server_sha256": digest_file(Path(args.server_bin)),
            "blank_replica": True,
            "dependency_outage_reads": True,
            **checks,
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x") as evidence:
            evidence.write(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
        return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument(
        "--backup-id", help="Require the expected ID from the backup scope"
    )
    parser.add_argument("--sha256", required=True)
    parser.add_argument(
        "--server-bin",
        required=True,
        help="Local akidb CLI built with generation-postgres",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--postgres-image", default="postgres:17-alpine")
    parser.add_argument("--seaweedfs-image", default="chrislusf/seaweedfs:4.47")
    args = parser.parse_args()
    require(args.timeout > 0, "timeout must be positive")
    verify(args)
    print(
        "PASS: canonical backup rebuilt into a blank replica; retrieval and outage reads verified"
    )


if __name__ == "__main__":
    main()
