from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "knowledge_gateway_load",
    ROOT / "scripts" / "knowledge_gateway_load.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class KnowledgeGatewayLoadTest(unittest.TestCase):
    def fixture(self) -> MODULE.Fixture:
        return MODULE.Fixture(
            fixture_id="fixture-a",
            expected=MODULE.ExpectedServing(
                workspace_id="workspace-a",
                collection="knowledge",
                generation_id="generation-a",
                manifest_sha256="a" * 64,
                minimum_sequence=7,
            ),
            cases=(
                MODULE.QueryCase(
                    case_id="case-a",
                    query="grounded query",
                    options={
                        "tokenBudget": 256,
                        "graphMaxDepth": 2,
                        "graphPerSeedFanout": 8,
                        "graphMaxExpandedNodes": 64,
                    },
                    expected_chunk_ids=frozenset({"chunk-a"}),
                    expected_document_ids=frozenset({"document-a"}),
                    expected_edge_ids=frozenset({"edge-a"}),
                    expected_predicates=frozenset({"contains"}),
                    forbidden_chunk_ids=frozenset({"chunk-forbidden"}),
                    forbidden_document_ids=frozenset({"document-forbidden"}),
                    expected_resolved_mode="graph",
                    minimum_graph_expanded_nodes=1,
                ),
            ),
        )

    def response(self) -> dict:
        return {
            "hits": [{"id": "chunk-a", "score": 1.0, "metadata": "{}"}],
            "contextPack": "grounded",
            "contextPackV1": {
                "schemaVersion": "akidb.context-pack.v1",
                "items": [
                    {
                        "chunkId": "chunk-a",
                        "text": "grounded",
                        "score": 1.0,
                        "reason": "direct_match",
                        "citation": {
                            "chunkId": "chunk-a",
                            "documentId": "document-a",
                            "documentVersion": "version-a",
                            "sourceUri": "s3://knowledge/document-a",
                            "sourceVersion": "version-a",
                            "contentHash": "b" * 64,
                            "generationId": "generation-a",
                        },
                    }
                ],
                "tokenBudget": 256,
                "usedTokens": 10,
                "truncated": False,
                "text": "grounded",
            },
            "diagnostics": {
                "resolvedMode": "graph",
                "graphDepth": 2,
                "graphPerSeedFanout": 8,
                "graphExpandedNodes": 1,
                "graphExpansions": [
                    {
                        "resultId": "chunk-a",
                        "path": [
                            {
                                "edgeId": "edge-a",
                                "predicate": "contains",
                                "evidenceChunkIds": ["chunk-a"],
                            }
                        ],
                    }
                ],
            },
            "servingGeneration": {
                "workspaceId": "workspace-a",
                "collection": "knowledge",
                "generationId": "generation-a",
                "manifestSha256": "a" * 64,
                "appliedSequence": "7",
            },
            "route": {
                "replicaId": "replica-a",
                "generationId": "generation-a",
                "manifestSha256": "a" * 64,
                "servedSequence": 7,
                "controlStale": False,
                "attempts": 1,
            },
        }

    def test_valid_response_satisfies_all_contracts(self) -> None:
        fixture = self.fixture()
        result = MODULE.validate_response(
            self.response(),
            fixture.cases[0],
            fixture.expected,
        )
        failures, evidence, documents, relationships = result[:4]
        self.assertEqual(failures, ())
        self.assertEqual(evidence, 1.0)
        self.assertEqual(documents, 1.0)
        self.assertEqual(relationships, 1.0)

    def test_generation_and_forbidden_evidence_fail_closed(self) -> None:
        fixture = self.fixture()
        response = self.response()
        response["servingGeneration"]["generationId"] = "generation-wrong"
        response["hits"].append({"id": "chunk-forbidden"})
        failures = MODULE.validate_response(
            response,
            fixture.cases[0],
            fixture.expected,
        )[0]
        self.assertIn("serving generation generationId mismatch", failures)
        self.assertIn("forbidden evidence returned", failures)

    def test_invalid_citation_and_budget_are_rejected(self) -> None:
        fixture = self.fixture()
        response = self.response()
        response["contextPackV1"]["usedTokens"] = 300
        response["contextPackV1"]["items"][0]["citation"]["contentHash"] = "bad"
        failures = MODULE.validate_response(
            response,
            fixture.cases[0],
            fixture.expected,
        )[0]
        self.assertIn("context token budget exceeded", failures)
        self.assertIn("one or more citations are invalid", failures)

    def test_fixture_loader_rejects_duplicate_case_ids(self) -> None:
        value = {
            "schema_version": 1,
            "fixture_id": "fixture-a",
            "expected_serving": {
                "workspace_id": "workspace-a",
                "collection": "knowledge",
                "generation_id": "generation-a",
                "manifest_sha256": "a" * 64,
                "minimum_sequence": 0,
            },
            "cases": [
                {"case_id": "duplicate", "query": "one"},
                {"case_id": "duplicate", "query": "two"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(
                MODULE.ConfigurationError,
                "duplicate case_id",
            ):
                MODULE.load_fixture(path)

    def test_distribution_uses_nearest_rank_percentiles(self) -> None:
        result = MODULE.distribution([1.0, 2.0, 3.0, 4.0, 100.0])
        self.assertEqual(result["p50"], 3.0)
        self.assertEqual(result["p95"], 100.0)
        self.assertEqual(result["p99"], 100.0)

    def test_failover_gate_requires_observed_request_retry(self) -> None:
        value = {
            "schema_version": 1,
            "min_route_replicas": 2,
            "min_route_failovers": 1,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gate.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            gate = MODULE.load_gate(path)
        self.assertEqual(gate.min_route_replicas, 2)
        self.assertEqual(gate.min_route_failovers, 1)


if __name__ == "__main__":
    unittest.main()
