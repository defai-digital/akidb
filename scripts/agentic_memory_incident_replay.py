#!/usr/bin/env python3
"""Real gRPC incident-replay demonstration for authoritative Memory preview.

The script records an incorrect procedure, captures the context that would
cause a wrong action, commits a successor correction, proves later recall has
changed, and then exactly replays the original retained snapshot.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "sdks" / "python"))

from akidb import AkiDBClient, MemoryContext, MemoryScope  # noqa: E402


def text_fact(item) -> str:
    if item.content.WhichOneof("value") != "text_fact":
        raise AssertionError("incident demo expected a text_fact")
    return item.content.text_fact.text


def run(args: argparse.Namespace) -> dict:
    token = args.token_file.read_text().strip()
    if not token:
        raise RuntimeError(f"empty principal token file: {args.token_file}")

    run_id = args.run_id or uuid.uuid4().hex[:12]
    entity_key = f"incident:queue-restart:{run_id}"
    context = MemoryContext(
        workspace_id="memory-preview",
        namespace="memory/incident-demo",
        request_purpose="incident-replay",
        delegated_agent_id="agent:local-preview",
        request_id=f"incident-demo-{run_id}",
    )
    scope = MemoryScope(
        entity_key=entity_key,
        allowed_purposes=("incident-replay",),
        owner_agent_id="agent:local-preview",
        session_id=f"incident-session-{run_id}",
        task_id="incident-replay-demo",
    )
    predicate = "uses queue restart procedure"
    wrong_text = "Restart ingestion immediately; draining the queue is unnecessary."
    corrected_text = "Drain and checkpoint the queue before restarting ingestion."

    with AkiDBClient(
        args.server,
        auth_token=token,
        timeout=args.timeout,
    ) as client:
        capabilities = client.memory_capabilities()
        if capabilities.profile_status not in {"EXPERIMENTAL", "DEVELOPER_PREVIEW"}:
            raise AssertionError(
                f"unexpected Memory profile: {capabilities.profile_status}"
            )
        if "ReplayRecall" not in capabilities.supported_rpcs:
            raise AssertionError("server does not advertise ReplayRecall")

        wrong = client.remember_text(
            context,
            scope,
            predicate,
            wrong_text,
            idempotency_key=f"incident-wrong-{run_id}",
            source_plane="incident-demo",
            source_id=f"operator-note-wrong-{run_id}",
            reason="capture the incorrect instruction that caused the incident",
        )
        before = client.recall(
            context,
            query_text="queue restart procedure",
            entity_keys=(entity_key,),
            max_items=5,
            max_context_tokens=256,
        )
        if [item.version_id for item in before.items] != [wrong.version_ids[0]]:
            raise AssertionError("pre-correction recall did not return the wrong version")
        if text_fact(before.items[0]) != wrong_text:
            raise AssertionError("pre-correction context does not contain the wrong procedure")

        corrected = client.remember_text(
            context,
            scope,
            predicate,
            corrected_text,
            idempotency_key=f"incident-correction-{run_id}",
            source_plane="incident-demo",
            source_id=f"operator-note-correction-{run_id}",
            expected_head_version_ids=tuple(wrong.version_ids),
            reason="correct the procedure after incident review",
        )
        after = client.recall(
            context,
            query_text="queue restart procedure",
            entity_keys=(entity_key,),
            max_items=5,
            max_context_tokens=256,
        )
        if [item.version_id for item in after.items] != [corrected.version_ids[0]]:
            raise AssertionError("post-correction recall did not return the successor")
        if text_fact(after.items[0]) != corrected_text:
            raise AssertionError("post-correction context does not contain the correction")

        replay = client.replay_recall(context, before.snapshot_id)
        if not replay.exact_match or replay.replay_mode != "EXACT_RETAINED":
            raise AssertionError("original snapshot was not replayed exactly")
        if replay.recall.SerializeToString() != before.SerializeToString():
            raise AssertionError("retained replay differs from the original recall response")

    return {
        "profile": "authoritative_memory_developer_preview",
        "run_id": run_id,
        "wrong_action": "agent restarts ingestion without draining the queue",
        "original_version_id": wrong.version_ids[0],
        "original_snapshot_id": before.snapshot_id,
        "original_context": before.rendered_context,
        "correction_version_id": corrected.version_ids[0],
        "corrected_snapshot_id": after.snapshot_id,
        "corrected_context": after.rendered_context,
        "original_snapshot_exact_after_correction": replay.exact_match,
        "replay_response_sha256": replay.response_sha256,
        "dense_status": list(after.partial_status),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="127.0.0.1:50051")
    parser.add_argument(
        "--token-file",
        type=Path,
        default=REPOSITORY_ROOT / "data" / "memory-preview" / "principal.token",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--json-out", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run(args)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(f"{rendered}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
