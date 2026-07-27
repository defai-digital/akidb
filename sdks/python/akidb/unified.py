"""Exact retained replay for a Memory recall plus Knowledge retrieval.

Memory and Knowledge Serving intentionally remain separate deployment profiles.
This module coordinates two clients, binds both exact protobuf exchanges and
their artifact evidence into one local immutable envelope, and replays the
retained bytes without re-running either retrieval path. Credentials are gRPC
metadata and are never written to the envelope.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional, Union

from google.protobuf.message import Message

from . import akidb_pb2 as pb

UNIFIED_RECALL_SCHEMA = "akidb.unified-recall.v1"


class UnifiedReplayError(ValueError):
    """A unified envelope is incomplete, corrupt, or not exactly replayable."""


@dataclass(frozen=True)
class UnifiedReplayResult:
    """Typed retained responses and the exact composed context."""

    memory_request: pb.MemoryRecallRequest
    memory_response: pb.MemoryRecallResponse
    knowledge_request: pb.TextSearchRequest
    knowledge_response: pb.SearchResponse
    rendered_context: str


@dataclass(frozen=True)
class UnifiedRecallArtifact:
    """Portable, checksum-bound record of one cross-profile retrieval."""

    schema: str
    capture_id: str
    captured_at_ms: int
    memory_snapshot_id: str
    memory_workspace_id: str
    memory_namespace: str
    memory_visible_sequence: int
    memory_projection_set_id: str
    memory_projection_manifest_sha256: str
    memory_policy_manifest_id: str
    memory_tokenizer_artifact_id: str
    memory_context_firewall_artifact_id: str
    memory_server_build_id: str
    knowledge_workspace_id: str
    knowledge_collection: str
    knowledge_generation_id: str
    knowledge_manifest_sha256: str
    knowledge_applied_sequence: int
    memory_request_b64: str
    memory_request_sha256: str
    memory_response_b64: str
    memory_response_sha256: str
    knowledge_request_b64: str
    knowledge_request_sha256: str
    knowledge_response_b64: str
    knowledge_response_sha256: str
    rendered_context_b64: str
    rendered_context_sha256: str
    artifact_sha256: str

    @classmethod
    def capture(
        cls,
        memory_request: pb.MemoryRecallRequest,
        memory_response: pb.MemoryRecallResponse,
        knowledge_request: pb.TextSearchRequest,
        knowledge_response: pb.SearchResponse,
        *,
        captured_at_ms: Optional[int] = None,
    ) -> "UnifiedRecallArtifact":
        """Create and validate an immutable envelope from exact RPC messages."""
        _validate_exchange(
            memory_request,
            memory_response,
            knowledge_request,
            knowledge_response,
        )
        captured_at = (
            int(time.time() * 1000) if captured_at_ms is None else captured_at_ms
        )
        if captured_at <= 0:
            raise UnifiedReplayError("captured_at_ms must be greater than zero")

        memory_request_bytes = _protobuf_bytes(memory_request)
        memory_response_bytes = _protobuf_bytes(memory_response)
        knowledge_request_bytes = _protobuf_bytes(knowledge_request)
        knowledge_response_bytes = _protobuf_bytes(knowledge_response)
        rendered_context = _render_context(memory_response, knowledge_response)
        rendered_context_bytes = rendered_context.encode("utf-8")
        generation = knowledge_response.serving_generation
        capabilities = memory_response.capabilities
        visibility = memory_response.visibility

        artifact = cls(
            schema=UNIFIED_RECALL_SCHEMA,
            capture_id="",
            captured_at_ms=captured_at,
            memory_snapshot_id=memory_response.snapshot_id,
            memory_workspace_id=visibility.workspace_id,
            memory_namespace=memory_request.context.namespace,
            memory_visible_sequence=visibility.visible_sequence,
            memory_projection_set_id=visibility.projection_set_id,
            memory_projection_manifest_sha256=(
                capabilities.active_projection_manifest_sha256
            ),
            memory_policy_manifest_id=capabilities.policy_manifest_id,
            memory_tokenizer_artifact_id=capabilities.tokenizer_artifact_id,
            memory_context_firewall_artifact_id=(
                capabilities.context_firewall_artifact_id
            ),
            memory_server_build_id=capabilities.server_build_id,
            knowledge_workspace_id=generation.workspace_id,
            knowledge_collection=generation.collection,
            knowledge_generation_id=generation.generation_id,
            knowledge_manifest_sha256=generation.manifest_sha256,
            knowledge_applied_sequence=generation.applied_sequence,
            memory_request_b64=_b64(memory_request_bytes),
            memory_request_sha256=_sha256(memory_request_bytes),
            memory_response_b64=_b64(memory_response_bytes),
            memory_response_sha256=_sha256(memory_response_bytes),
            knowledge_request_b64=_b64(knowledge_request_bytes),
            knowledge_request_sha256=_sha256(knowledge_request_bytes),
            knowledge_response_b64=_b64(knowledge_response_bytes),
            knowledge_response_sha256=_sha256(knowledge_response_bytes),
            rendered_context_b64=_b64(rendered_context_bytes),
            rendered_context_sha256=_sha256(rendered_context_bytes),
            artifact_sha256="",
        )
        fingerprint = artifact._fingerprint()
        digest = _sha256(_canonical_json(fingerprint))
        artifact = replace(
            artifact,
            capture_id=f"unified_{digest}",
            artifact_sha256=digest,
        )
        artifact.verify()
        return artifact

    def verify(self) -> None:
        """Fail closed unless every retained byte and evidence binding matches."""
        if self.schema != UNIFIED_RECALL_SCHEMA:
            raise UnifiedReplayError(f"unsupported unified schema {self.schema!r}")
        if self.captured_at_ms <= 0:
            raise UnifiedReplayError("captured_at_ms must be greater than zero")
        for field, value in (
            ("memory_request", self.memory_request_b64),
            ("memory_response", self.memory_response_b64),
            ("knowledge_request", self.knowledge_request_b64),
            ("knowledge_response", self.knowledge_response_b64),
            ("rendered_context", self.rendered_context_b64),
        ):
            raw = _decode_b64(field, value)
            expected = getattr(self, f"{field}_sha256")
            if _sha256(raw) != expected:
                raise UnifiedReplayError(f"{field} digest mismatch")

        expected_artifact_sha256 = _sha256(_canonical_json(self._fingerprint()))
        if self.artifact_sha256 != expected_artifact_sha256:
            raise UnifiedReplayError("unified artifact digest mismatch")
        if self.capture_id != f"unified_{expected_artifact_sha256}":
            raise UnifiedReplayError("capture ID differs from the artifact digest")

        replay = self._parse_messages()
        _validate_exchange(
            replay.memory_request,
            replay.memory_response,
            replay.knowledge_request,
            replay.knowledge_response,
        )
        if (
            replay.memory_response.snapshot_id != self.memory_snapshot_id
            or replay.memory_response.visibility.workspace_id
            != self.memory_workspace_id
            or replay.memory_request.context.namespace != self.memory_namespace
            or replay.memory_response.visibility.visible_sequence
            != self.memory_visible_sequence
            or replay.memory_response.visibility.projection_set_id
            != self.memory_projection_set_id
            or replay.memory_response.capabilities.active_projection_manifest_sha256
            != self.memory_projection_manifest_sha256
            or replay.memory_response.capabilities.policy_manifest_id
            != self.memory_policy_manifest_id
            or replay.memory_response.capabilities.tokenizer_artifact_id
            != self.memory_tokenizer_artifact_id
            or replay.memory_response.capabilities.context_firewall_artifact_id
            != self.memory_context_firewall_artifact_id
            or replay.memory_response.capabilities.server_build_id
            != self.memory_server_build_id
            or replay.knowledge_response.serving_generation.workspace_id
            != self.knowledge_workspace_id
            or replay.knowledge_response.serving_generation.collection
            != self.knowledge_collection
            or replay.knowledge_response.serving_generation.generation_id
            != self.knowledge_generation_id
            or replay.knowledge_response.serving_generation.manifest_sha256
            != self.knowledge_manifest_sha256
            or replay.knowledge_response.serving_generation.applied_sequence
            != self.knowledge_applied_sequence
        ):
            raise UnifiedReplayError("retained evidence differs from envelope metadata")
        expected_context = _render_context(
            replay.memory_response, replay.knowledge_response
        )
        if replay.rendered_context != expected_context:
            raise UnifiedReplayError("retained unified context is not reproducible")

    def replay_exact(self) -> UnifiedReplayResult:
        """Return typed messages reconstructed from the retained exact bytes."""
        self.verify()
        return self._parse_messages()

    def save(self, path: Union[os.PathLike[str], str]) -> Path:
        """Atomically persist a mode-0600 envelope without replacing a path."""
        self.verify()
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        encoded = _canonical_json(self.to_dict()) + b"\n"
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=str(destination.parent),
        )
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary_name, destination)
            except FileExistsError as error:
                raise UnifiedReplayError(
                    f"refusing to replace existing unified artifact {destination}"
                ) from error
            os.unlink(temporary_name)
            temporary_name = ""
            directory_fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except BaseException:
            if temporary_name:
                try:
                    os.unlink(temporary_name)
                except FileNotFoundError:
                    pass
            raise
        return destination

    @classmethod
    def load(
        cls, path: Union[os.PathLike[str], str]
    ) -> "UnifiedRecallArtifact":
        """Load a strict envelope and verify it before returning."""
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise UnifiedReplayError("unified artifact must be a JSON object")
        expected_fields = set(cls.__dataclass_fields__)
        if set(raw) != expected_fields:
            missing = sorted(expected_fields - set(raw))
            unknown = sorted(set(raw) - expected_fields)
            raise UnifiedReplayError(
                f"unified artifact fields differ: missing={missing}, unknown={unknown}"
            )
        try:
            artifact = cls(**raw)
        except TypeError as error:
            raise UnifiedReplayError(f"invalid unified artifact: {error}") from error
        artifact.verify()
        return artifact

    def to_dict(self) -> dict[str, Any]:
        return {
            field: getattr(self, field) for field in self.__dataclass_fields__
        }

    def _fingerprint(self) -> dict[str, Any]:
        values = self.to_dict()
        values["capture_id"] = ""
        values["artifact_sha256"] = ""
        return values

    def _parse_messages(self) -> UnifiedReplayResult:
        memory_request = _parse(
            pb.MemoryRecallRequest, _decode_b64("memory_request", self.memory_request_b64)
        )
        memory_response = _parse(
            pb.MemoryRecallResponse,
            _decode_b64("memory_response", self.memory_response_b64),
        )
        knowledge_request = _parse(
            pb.TextSearchRequest,
            _decode_b64("knowledge_request", self.knowledge_request_b64),
        )
        knowledge_response = _parse(
            pb.SearchResponse,
            _decode_b64("knowledge_response", self.knowledge_response_b64),
        )
        rendered_context = _decode_b64(
            "rendered_context", self.rendered_context_b64
        ).decode("utf-8")
        return UnifiedReplayResult(
            memory_request=memory_request,
            memory_response=memory_response,
            knowledge_request=knowledge_request,
            knowledge_response=knowledge_response,
            rendered_context=rendered_context,
        )


class UnifiedRecallCoordinator:
    """Coordinate one MemoryService client and one Knowledge Serving client."""

    def __init__(self, memory_client: Any, knowledge_client: Any):
        self._memory_client = memory_client
        self._knowledge_client = knowledge_client

    def capture(
        self,
        memory_request: pb.MemoryRecallRequest,
        knowledge_request: pb.TextSearchRequest,
        *,
        output_path: Optional[Union[os.PathLike[str], str]] = None,
        captured_at_ms: Optional[int] = None,
    ) -> UnifiedRecallArtifact:
        """Execute both typed RPCs and optionally retain their unified envelope."""
        memory_response = self._memory_client._invoke(
            self._memory_client._memory_stub.Recall, memory_request
        )
        knowledge_response = self._knowledge_client._invoke(
            self._knowledge_client._stub.TextSearch, knowledge_request
        )
        artifact = UnifiedRecallArtifact.capture(
            memory_request,
            memory_response,
            knowledge_request,
            knowledge_response,
            captured_at_ms=captured_at_ms,
        )
        if output_path is not None:
            artifact.save(output_path)
        return artifact


def _validate_response_evidence(
    memory_response: pb.MemoryRecallResponse,
    knowledge_response: pb.SearchResponse,
) -> None:
    if (
        not memory_response.snapshot_id
        or not memory_response.HasField("visibility")
        or not memory_response.visibility.workspace_id
        or memory_response.visibility.visible_sequence <= 0
        or not memory_response.visibility.projection_set_id
        or not memory_response.HasField("capabilities")
    ):
        raise UnifiedReplayError("Memory response lacks retained snapshot evidence")
    capabilities = memory_response.capabilities
    for field in (
        "active_projection_manifest_sha256",
        "policy_manifest_id",
        "tokenizer_artifact_id",
        "context_firewall_artifact_id",
        "server_build_id",
    ):
        if not getattr(capabilities, field):
            raise UnifiedReplayError(f"Memory response lacks {field}")
    if (
        not knowledge_response.HasField("serving_generation")
        or not knowledge_response.serving_generation.workspace_id
        or not knowledge_response.serving_generation.collection
        or not knowledge_response.serving_generation.generation_id
        or not knowledge_response.serving_generation.manifest_sha256
        or knowledge_response.serving_generation.applied_sequence <= 0
    ):
        raise UnifiedReplayError("Knowledge response lacks immutable generation evidence")
    if (
        not knowledge_response.HasField("context_pack_v1")
        or not knowledge_response.context_pack_v1.schema_version
    ):
        raise UnifiedReplayError("Knowledge response lacks a typed context pack")


def _validate_exchange(
    memory_request: pb.MemoryRecallRequest,
    memory_response: pb.MemoryRecallResponse,
    knowledge_request: pb.TextSearchRequest,
    knowledge_response: pb.SearchResponse,
) -> None:
    _validate_response_evidence(memory_response, knowledge_response)
    generation = knowledge_response.serving_generation
    if (
        not memory_request.HasField("context")
        or not memory_request.context.workspace_id
        or not memory_request.context.namespace
        or memory_request.context.workspace_id
        != memory_response.visibility.workspace_id
        or generation.workspace_id != memory_response.visibility.workspace_id
    ):
        raise UnifiedReplayError(
            "Memory and Knowledge exchanges must bind one exact workspace"
        )
    if (
        not knowledge_request.collection
        or knowledge_request.collection != generation.collection
    ):
        raise UnifiedReplayError(
            "Knowledge request collection differs from generation evidence"
        )


def _render_context(
    memory_response: pb.MemoryRecallResponse,
    knowledge_response: pb.SearchResponse,
) -> str:
    return json.dumps(
        {
            "schema": "akidb.unified-context.v1",
            "channels": [
                {
                    "kind": "memory_quoted_data",
                    "snapshot_id": memory_response.snapshot_id,
                    "text": memory_response.rendered_context,
                },
                {
                    "kind": "knowledge_quoted_data",
                    "generation_id": (
                        knowledge_response.serving_generation.generation_id
                    ),
                    "text": knowledge_response.context_pack_v1.text,
                },
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _protobuf_bytes(message: Message) -> bytes:
    if not isinstance(message, Message):
        raise UnifiedReplayError("unified capture inputs must be protobuf messages")
    return message.SerializeToString(deterministic=True)


def _parse(message_type: Any, raw: bytes) -> Any:
    message = message_type()
    try:
        message.ParseFromString(raw)
    except Exception as error:
        raise UnifiedReplayError(
            f"retained {message_type.__name__} bytes cannot be parsed"
        ) from error
    if message.SerializeToString(deterministic=True) != raw:
        raise UnifiedReplayError(
            f"retained {message_type.__name__} bytes are not canonical"
        )
    return message


def _decode_b64(field: str, value: str) -> bytes:
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError) as error:
        raise UnifiedReplayError(f"{field} is not canonical base64") from error


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
