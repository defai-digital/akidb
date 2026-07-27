"""Exact unified Memory plus Knowledge retained-replay tests."""

import json
import stat
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from akidb import (  # noqa: E402
    AkiDBClient,
    UnifiedRecallArtifact,
    UnifiedRecallCoordinator,
    UnifiedReplayError,
)
from akidb import akidb_pb2 as pb  # noqa: E402


def exchanges():
    memory_request = pb.MemoryRecallRequest(
        context=pb.MemoryRequestContext(
            workspace_id="workspace-a",
            namespace="repo/akidb",
            request_purpose="debugging",
        ),
        query_text="How should ingestion recover?",
        max_items=5,
        deterministic=True,
        recipe="structured+lexical-v2",
    )
    memory_response = pb.MemoryRecallResponse(
        items=[
            pb.MemoryItem(
                assertion_id="assertion-1",
                version_id="version-1",
                namespace="repo/akidb",
                entity_key="service:ingestion",
                predicate="uses recovery procedure",
                content=pb.MemoryContent(
                    text_fact=pb.MemoryTextFact(
                        text="Drain the queue before restarting.", language="en"
                    )
                ),
                state=pb.MEMORY_VERSION_STATE_ACTIVE,
                committed_sequence=9,
                committed_at_ms=1_700_000_000_000,
                reason="exact lexical match",
            )
        ],
        rendered_context="[memory version-1] Drain the queue before restarting.",
        snapshot_id="snapshot-1",
        visibility=pb.MemoryVisibilityReceipt(
            workspace_id="workspace-a",
            commit_sequence=9,
            projection_set_id="projection-set-v2",
            projection_set_version=2,
            visible_sequence=9,
        ),
        policy_decision_id="policy-decision-1",
        capabilities=pb.MemoryServerCapabilities(
            profile_status="BITEMPORAL_QUALIFIED",
            active_projection_manifest_sha256="a" * 64,
            policy_manifest_id="policy:memory-v1",
            tokenizer_artifact_id="tokenizer:unicode-word-v2",
            context_firewall_artifact_id="firewall:quoted-data-v1",
            server_build_id="build:abc123",
        ),
    )
    knowledge_request = pb.TextSearchRequest(
        collection="knowledge",
        text="ingestion recovery",
        top_k=5,
        hybrid=True,
        pack=True,
        pack_token_budget=512,
    )
    knowledge_response = pb.SearchResponse(
        results=[
            pb.SearchResult(id="chunk-1", score=0.9, metadata='{"source":"runbook"}')
        ],
        context_pack="[knowledge chunk-1] Ingestion recovery runbook.",
        serving_generation=pb.ServingGenerationEvidence(
            workspace_id="workspace-a",
            collection="knowledge",
            generation_id="generation-7",
            manifest_sha256="b" * 64,
            applied_sequence=77,
        ),
        context_pack_v1=pb.ContextPackV1(
            schema_version="context-pack-v1",
            items=[
                pb.ContextPackItemV1(
                    chunk_id="chunk-1",
                    text="Ingestion recovery runbook.",
                    score=0.9,
                    reason="hybrid",
                    citation=pb.RetrievalCitationV1(
                        chunk_id="chunk-1",
                        document_id="runbook",
                        document_version="3",
                        source_uri="s3://knowledge/runbook",
                        source_version="3",
                        content_hash="c" * 64,
                        generation_id="generation-7",
                    ),
                )
            ],
            token_budget=512,
            used_tokens=10,
            text="[knowledge chunk-1] Ingestion recovery runbook.",
        ),
    )
    return (
        memory_request,
        memory_response,
        knowledge_request,
        knowledge_response,
    )


def test_unified_artifact_round_trips_exact_typed_messages(tmp_path):
    memory_request, memory_response, knowledge_request, knowledge_response = (
        exchanges()
    )
    artifact = UnifiedRecallArtifact.capture(
        memory_request,
        memory_response,
        knowledge_request,
        knowledge_response,
        captured_at_ms=1_700_000_000_100,
    )
    path = artifact.save(tmp_path / "unified.json")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    loaded = UnifiedRecallArtifact.load(path)
    replay = loaded.replay_exact()
    assert (
        replay.memory_response.SerializeToString(deterministic=True)
        == memory_response.SerializeToString(deterministic=True)
    )
    assert (
        replay.knowledge_response.SerializeToString(deterministic=True)
        == knowledge_response.SerializeToString(deterministic=True)
    )
    assert "snapshot-1" in replay.rendered_context
    assert "generation-7" in replay.rendered_context
    assert artifact.capture_id == f"unified_{artifact.artifact_sha256}"


def test_unified_artifact_save_refuses_to_replace_existing_path(tmp_path):
    memory_request, memory_response, knowledge_request, knowledge_response = (
        exchanges()
    )
    artifact = UnifiedRecallArtifact.capture(
        memory_request,
        memory_response,
        knowledge_request,
        knowledge_response,
        captured_at_ms=1_700_000_000_100,
    )
    path = tmp_path / "unified.json"
    path.write_text("existing evidence", encoding="utf-8")

    with pytest.raises(UnifiedReplayError, match="refusing to replace"):
        artifact.save(path)

    assert path.read_text(encoding="utf-8") == "existing evidence"


def test_unified_coordinator_uses_separate_stubs_and_retains_no_credentials(tmp_path):
    memory_request, memory_response, knowledge_request, knowledge_response = (
        exchanges()
    )
    memory_stub = MagicMock()
    knowledge_stub = MagicMock()
    memory_stub.Recall.return_value = memory_response
    knowledge_stub.TextSearch.return_value = knowledge_response
    memory_client = AkiDBClient(
        stub=MagicMock(), memory_stub=memory_stub, auth_token="memory-secret"
    )
    knowledge_client = AkiDBClient(
        stub=knowledge_stub, auth_token="knowledge-secret"
    )

    path = tmp_path / "unified.json"
    artifact = UnifiedRecallCoordinator(memory_client, knowledge_client).capture(
        memory_request,
        knowledge_request,
        output_path=path,
        captured_at_ms=1_700_000_000_100,
    )
    memory_stub.Recall.assert_called_once()
    knowledge_stub.TextSearch.assert_called_once()
    encoded = path.read_text(encoding="utf-8")
    assert "memory-secret" not in encoded
    assert "knowledge-secret" not in encoded
    assert UnifiedRecallArtifact.load(path) == artifact


def test_unified_artifact_rejects_tampering_and_missing_generation(tmp_path):
    memory_request, memory_response, knowledge_request, knowledge_response = (
        exchanges()
    )
    artifact = UnifiedRecallArtifact.capture(
        memory_request,
        memory_response,
        knowledge_request,
        knowledge_response,
        captured_at_ms=1_700_000_000_100,
    )
    path = artifact.save(tmp_path / "unified.json")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["memory_response_b64"] = raw["memory_response_b64"][:-4] + "AAAA"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(UnifiedReplayError, match="memory_response digest mismatch"):
        UnifiedRecallArtifact.load(path)

    knowledge_response.ClearField("serving_generation")
    with pytest.raises(UnifiedReplayError, match="generation evidence"):
        UnifiedRecallArtifact.capture(
            memory_request,
            memory_response,
            knowledge_request,
            knowledge_response,
        )
