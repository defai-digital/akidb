#!/usr/bin/env python3
"""Exercise an AX/AkiDB knowledge gateway with release-grade assertions.

The driver intentionally uses only the Python standard library so the exact
artifact can run from an Ubuntu qualification host without installing a load
testing framework.  It supports fixed-concurrency closed-loop traffic and
paced open-loop traffic.  Both service time and schedule-to-completion time
are reported so an overloaded client cannot hide coordinated omission.

Credentials are read from an environment variable and are never written to
the report.  Successful response text is also excluded from evidence files.
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import hashlib
import json
import math
import os
import platform
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
MAX_FAILURE_SAMPLES = 20
CONTENT_HASH = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_SOURCE_SCHEMES = {"s3", "https", "openwiki"}


class ConfigurationError(ValueError):
    """The fixture, gate, or command line is invalid."""


@dataclasses.dataclass(frozen=True)
class ExpectedServing:
    workspace_id: str
    collection: str
    generation_id: str
    manifest_sha256: str
    minimum_sequence: int


@dataclasses.dataclass(frozen=True)
class QueryCase:
    case_id: str
    query: str
    options: dict[str, Any]
    expected_chunk_ids: frozenset[str]
    expected_document_ids: frozenset[str]
    expected_edge_ids: frozenset[str]
    expected_predicates: frozenset[str]
    forbidden_chunk_ids: frozenset[str]
    forbidden_document_ids: frozenset[str]
    expected_resolved_mode: str | None
    minimum_graph_expanded_nodes: int


@dataclasses.dataclass(frozen=True)
class Fixture:
    fixture_id: str
    expected: ExpectedServing
    cases: tuple[QueryCase, ...]


@dataclasses.dataclass(frozen=True)
class Gate:
    max_error_rate: float
    max_contract_failures: int
    min_evidence_recall: float
    min_document_recall: float
    min_relationship_recall: float | None
    min_citation_correctness: float
    max_p95_ms: float
    max_p99_ms: float
    max_end_to_end_p99_ms: float
    min_achieved_qps: float
    min_route_replicas: int
    min_route_failovers: int
    require_all_gateways: bool
    require_zero_stale_routes: bool
    require_security_probes: bool


@dataclasses.dataclass
class Observation:
    case_id: str
    gateway: str
    ok: bool
    http_status: int | None
    service_latency_ms: float
    end_to_end_latency_ms: float
    schedule_delay_ms: float
    evidence_recall: float
    document_recall: float | None
    relationship_recall: float | None
    citations: int
    invalid_citations: int
    route_replica: str | None
    route_attempts: int
    control_stale: bool
    contract_failures: tuple[str, ...]
    error_kind: str | None
    error_message: str | None


class Accumulator:
    """Thread-safe bounded evidence accumulator."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests = 0
        self.successes = 0
        self.http_statuses: collections.Counter[str] = collections.Counter()
        self.error_kinds: collections.Counter[str] = collections.Counter()
        self.gateways: collections.Counter[str] = collections.Counter()
        self.route_replicas: collections.Counter[str] = collections.Counter()
        self.route_failovers = 0
        self.stale_routes = 0
        self.contract_failures = 0
        self.citations = 0
        self.invalid_citations = 0
        self.service_latencies: list[float] = []
        self.end_to_end_latencies: list[float] = []
        self.schedule_delays: list[float] = []
        self.case_totals: dict[str, dict[str, Any]] = {}
        self.failure_samples: list[dict[str, Any]] = []

    def record(self, observation: Observation) -> None:
        with self._lock:
            self.requests += 1
            if observation.ok:
                self.successes += 1
            self.http_statuses[str(observation.http_status or "transport")] += 1
            if observation.error_kind:
                self.error_kinds[observation.error_kind] += 1
            self.gateways[observation.gateway] += 1
            if observation.route_replica:
                self.route_replicas[observation.route_replica] += 1
            if observation.route_attempts > 1:
                self.route_failovers += 1
            if observation.control_stale:
                self.stale_routes += 1
            self.contract_failures += len(observation.contract_failures)
            self.citations += observation.citations
            self.invalid_citations += observation.invalid_citations
            self.service_latencies.append(observation.service_latency_ms)
            self.end_to_end_latencies.append(observation.end_to_end_latency_ms)
            self.schedule_delays.append(observation.schedule_delay_ms)

            case = self.case_totals.setdefault(
                observation.case_id,
                {
                    "requests": 0,
                    "successes": 0,
                    "evidence_recall_sum": 0.0,
                    "minimum_evidence_recall": 1.0,
                    "document_recall_sum": 0.0,
                    "document_recall_count": 0,
                    "relationship_recall_sum": 0.0,
                    "relationship_recall_count": 0,
                },
            )
            case["requests"] += 1
            case["successes"] += int(observation.ok)
            case["evidence_recall_sum"] += observation.evidence_recall
            case["minimum_evidence_recall"] = min(
                case["minimum_evidence_recall"],
                observation.evidence_recall,
            )
            if observation.document_recall is not None:
                case["document_recall_sum"] += observation.document_recall
                case["document_recall_count"] += 1
            if observation.relationship_recall is not None:
                case["relationship_recall_sum"] += observation.relationship_recall
                case["relationship_recall_count"] += 1

            if (
                not observation.ok
                and len(self.failure_samples) < MAX_FAILURE_SAMPLES
            ):
                self.failure_samples.append(
                    {
                        "case_id": observation.case_id,
                        "gateway": observation.gateway,
                        "http_status": observation.http_status,
                        "error_kind": observation.error_kind,
                        "error_message": observation.error_message,
                        "contract_failures": list(observation.contract_failures),
                    }
                )

    def summary(self, wall_seconds: float) -> dict[str, Any]:
        case_summaries: list[dict[str, Any]] = []
        document_values: list[float] = []
        relationship_values: list[float] = []
        evidence_values: list[float] = []
        for case_id, value in sorted(self.case_totals.items()):
            requests = int(value["requests"])
            evidence = float(value["evidence_recall_sum"]) / max(1, requests)
            evidence_values.append(evidence)
            document = (
                float(value["document_recall_sum"])
                / int(value["document_recall_count"])
                if value["document_recall_count"]
                else None
            )
            relationship = (
                float(value["relationship_recall_sum"])
                / int(value["relationship_recall_count"])
                if value["relationship_recall_count"]
                else None
            )
            if document is not None:
                document_values.append(document)
            if relationship is not None:
                relationship_values.append(relationship)
            case_summaries.append(
                {
                    "case_id": case_id,
                    "requests": requests,
                    "successes": int(value["successes"]),
                    "evidence_recall": evidence,
                    "minimum_observed_evidence_recall": float(
                        value["minimum_evidence_recall"]
                    ),
                    "document_recall": document,
                    "relationship_recall": relationship,
                }
            )

        failures = self.requests - self.successes
        return {
            "requests": self.requests,
            "successes": self.successes,
            "failures": failures,
            "error_rate": failures / self.requests if self.requests else 1.0,
            "wall_seconds": wall_seconds,
            "achieved_qps": self.successes / wall_seconds if wall_seconds > 0 else 0.0,
            "http_statuses": dict(sorted(self.http_statuses.items())),
            "error_kinds": dict(sorted(self.error_kinds.items())),
            "gateways": dict(sorted(self.gateways.items())),
            "route_replicas": dict(sorted(self.route_replicas.items())),
            "route_failovers": self.route_failovers,
            "stale_routes": self.stale_routes,
            "contract_failures": self.contract_failures,
            "citation_correctness": (
                (self.citations - self.invalid_citations) / self.citations
                if self.citations
                else 0.0
            ),
            "citations": self.citations,
            "invalid_citations": self.invalid_citations,
            "evidence_recall": average(evidence_values),
            "document_recall": (
                average(document_values) if document_values else None
            ),
            "relationship_recall": (
                average(relationship_values) if relationship_values else None
            ),
            "service_latency_ms": distribution(self.service_latencies),
            "end_to_end_latency_ms": distribution(self.end_to_end_latencies),
            "schedule_delay_ms": distribution(self.schedule_delays),
            "cases": case_summaries,
            "failure_samples": self.failure_samples,
        }


def require_object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be an object")
    return value


def require_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ConfigurationError(f"{name} must be canonical non-empty text")
    return value


def require_int(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigurationError(f"{name} must be an integer >= {minimum}")
    return value


def require_number(
    value: Any,
    name: str,
    minimum: float = 0.0,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number) or number < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}")
    if maximum is not None and number > maximum:
        raise ConfigurationError(f"{name} must be <= {maximum}")
    return number


def string_set(value: Any, name: str) -> frozenset[str]:
    if value is None:
        return frozenset()
    if not isinstance(value, list):
        raise ConfigurationError(f"{name} must be an array")
    result = frozenset(require_text(item, f"{name}[]") for item in value)
    if len(result) != len(value):
        raise ConfigurationError(f"{name} must contain unique IDs")
    return result


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigurationError(f"cannot read JSON from {path}: {error}") from error


def load_fixture(path: Path) -> Fixture:
    value = require_object(load_json(path), "fixture")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ConfigurationError(
            f"fixture schema_version must be {SCHEMA_VERSION}"
        )
    expected_value = require_object(value.get("expected_serving"), "expected_serving")
    expected = ExpectedServing(
        workspace_id=require_text(
            expected_value.get("workspace_id"), "expected_serving.workspace_id"
        ),
        collection=require_text(
            expected_value.get("collection"), "expected_serving.collection"
        ),
        generation_id=require_text(
            expected_value.get("generation_id"), "expected_serving.generation_id"
        ),
        manifest_sha256=require_text(
            expected_value.get("manifest_sha256"),
            "expected_serving.manifest_sha256",
        ),
        minimum_sequence=require_int(
            expected_value.get("minimum_sequence", 0),
            "expected_serving.minimum_sequence",
        ),
    )
    if not CONTENT_HASH.fullmatch(expected.manifest_sha256):
        raise ConfigurationError("expected manifest_sha256 must be lowercase SHA-256")

    raw_cases = value.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        raise ConfigurationError("fixture.cases must be a non-empty array")
    cases: list[QueryCase] = []
    seen: set[str] = set()
    for index, raw_case in enumerate(raw_cases):
        case = require_object(raw_case, f"cases[{index}]")
        case_id = require_text(case.get("case_id"), f"cases[{index}].case_id")
        if case_id in seen:
            raise ConfigurationError(f"duplicate case_id {case_id}")
        seen.add(case_id)
        options = require_object(case.get("options", {}), f"{case_id}.options")
        expected_mode = case.get("expected_resolved_mode")
        if expected_mode is not None:
            expected_mode = require_text(expected_mode, f"{case_id}.expected_mode")
        cases.append(
            QueryCase(
                case_id=case_id,
                query=require_text(case.get("query"), f"{case_id}.query"),
                options=options,
                expected_chunk_ids=string_set(
                    case.get("expected_chunk_ids"), f"{case_id}.expected_chunk_ids"
                ),
                expected_document_ids=string_set(
                    case.get("expected_document_ids"),
                    f"{case_id}.expected_document_ids",
                ),
                expected_edge_ids=string_set(
                    case.get("expected_edge_ids"), f"{case_id}.expected_edge_ids"
                ),
                expected_predicates=string_set(
                    case.get("expected_predicates"),
                    f"{case_id}.expected_predicates",
                ),
                forbidden_chunk_ids=string_set(
                    case.get("forbidden_chunk_ids"),
                    f"{case_id}.forbidden_chunk_ids",
                ),
                forbidden_document_ids=string_set(
                    case.get("forbidden_document_ids"),
                    f"{case_id}.forbidden_document_ids",
                ),
                expected_resolved_mode=expected_mode,
                minimum_graph_expanded_nodes=require_int(
                    case.get("minimum_graph_expanded_nodes", 0),
                    f"{case_id}.minimum_graph_expanded_nodes",
                ),
            )
        )
    return Fixture(
        fixture_id=require_text(value.get("fixture_id"), "fixture_id"),
        expected=expected,
        cases=tuple(cases),
    )


def load_gate(path: Path) -> Gate:
    value = require_object(load_json(path), "gate")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ConfigurationError(f"gate schema_version must be {SCHEMA_VERSION}")
    relationship = value.get("min_relationship_recall")
    return Gate(
        max_error_rate=require_number(
            value.get("max_error_rate", 0), "max_error_rate", 0, 1
        ),
        max_contract_failures=require_int(
            value.get("max_contract_failures", 0), "max_contract_failures"
        ),
        min_evidence_recall=require_number(
            value.get("min_evidence_recall", 1),
            "min_evidence_recall",
            0,
            1,
        ),
        min_document_recall=require_number(
            value.get("min_document_recall", 1),
            "min_document_recall",
            0,
            1,
        ),
        min_relationship_recall=(
            None
            if relationship is None
            else require_number(relationship, "min_relationship_recall", 0, 1)
        ),
        min_citation_correctness=require_number(
            value.get("min_citation_correctness", 1),
            "min_citation_correctness",
            0,
            1,
        ),
        max_p95_ms=require_number(value.get("max_p95_ms", 500), "max_p95_ms"),
        max_p99_ms=require_number(value.get("max_p99_ms", 1_000), "max_p99_ms"),
        max_end_to_end_p99_ms=require_number(
            value.get("max_end_to_end_p99_ms", 1_500),
            "max_end_to_end_p99_ms",
        ),
        min_achieved_qps=require_number(
            value.get("min_achieved_qps", 0), "min_achieved_qps"
        ),
        min_route_replicas=require_int(
            value.get("min_route_replicas", 1), "min_route_replicas", 1
        ),
        min_route_failovers=require_int(
            value.get("min_route_failovers", 0), "min_route_failovers"
        ),
        require_all_gateways=bool(value.get("require_all_gateways", True)),
        require_zero_stale_routes=bool(
            value.get("require_zero_stale_routes", True)
        ),
        require_security_probes=bool(
            value.get("require_security_probes", True)
        ),
    )


def canonical_gateway(value: str) -> str:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.query or parsed.fragment:
        raise ConfigurationError("gateway URLs must be canonical HTTPS origins")
    if parsed.username or parsed.password:
        raise ConfigurationError("gateway URL must not include credentials")
    path = parsed.path.rstrip("/")
    if path:
        raise ConfigurationError("gateway URL must not include a path")
    return value.rstrip("/")


def source_is_canonical(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urllib.parse.urlparse(value)
    return (
        parsed.scheme in ALLOWED_SOURCE_SCHEMES
        and bool(parsed.hostname)
        and parsed.path not in {"", "/"}
        and not parsed.username
        and not parsed.password
        and not parsed.fragment
    )


def request(
    url: str,
    context: ssl.SSLContext,
    timeout_seconds: float,
    token: str | None,
    *,
    method: str = "POST",
    body: bytes | None = None,
    content_type: str = "application/json",
) -> tuple[int, bytes]:
    headers = {"Accept": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(
            req,
            context=context,
            timeout=timeout_seconds,
        ) as response:
            return int(response.status), response.read()
    except urllib.error.HTTPError as error:
        return int(error.code), error.read(16_384)


def validate_response(
    value: Any,
    case: QueryCase,
    expected: ExpectedServing,
) -> tuple[
    tuple[str, ...],
    float,
    float | None,
    float | None,
    int,
    int,
    str | None,
    int,
    bool,
]:
    failures: list[str] = []
    if not isinstance(value, dict):
        return (
            ("response is not an object",),
            0,
            None,
            None,
            0,
            0,
            None,
            0,
            False,
        )
    hits = value.get("hits")
    if not isinstance(hits, list):
        hits = []
        failures.append("hits are missing")
    hit_ids = {
        hit.get("id")
        for hit in hits
        if isinstance(hit, dict) and isinstance(hit.get("id"), str)
    }

    pack = value.get("contextPackV1")
    items: list[Any] = []
    if not isinstance(pack, dict):
        failures.append("typed context pack is missing")
    else:
        if pack.get("schemaVersion") != "akidb.context-pack.v1":
            failures.append("context pack schema is invalid")
        if pack.get("text") != value.get("contextPack"):
            failures.append("typed and legacy context packs disagree")
        items_value = pack.get("items")
        if not isinstance(items_value, list):
            failures.append("context pack items are missing")
        else:
            items = items_value
        token_budget = case.options.get("tokenBudget")
        if isinstance(token_budget, int):
            if (
                not isinstance(pack.get("tokenBudget"), int)
                or pack.get("tokenBudget") > token_budget
                or not isinstance(pack.get("usedTokens"), int)
                or pack.get("usedTokens") > pack.get("tokenBudget", -1)
            ):
                failures.append("context token budget exceeded")

    evidence_ids = set(hit_ids)
    cited_documents: set[str] = set()
    citations = 0
    invalid_citations = 0
    for item in items:
        if not isinstance(item, dict):
            invalid_citations += 1
            continue
        chunk_id = item.get("chunkId")
        if isinstance(chunk_id, str):
            evidence_ids.add(chunk_id)
        citation = item.get("citation")
        citations += 1
        if not isinstance(citation, dict):
            invalid_citations += 1
            continue
        document_id = citation.get("documentId")
        if isinstance(document_id, str):
            cited_documents.add(document_id)
        valid = (
            citation.get("chunkId") == chunk_id
            and isinstance(document_id, str)
            and bool(document_id)
            and isinstance(citation.get("documentVersion"), str)
            and bool(citation.get("documentVersion"))
            and isinstance(citation.get("sourceVersion"), str)
            and bool(citation.get("sourceVersion"))
            and isinstance(citation.get("contentHash"), str)
            and bool(CONTENT_HASH.fullmatch(citation["contentHash"]))
            and citation.get("generationId") == expected.generation_id
            and source_is_canonical(citation.get("sourceUri"))
        )
        if not valid:
            invalid_citations += 1
    if citations == 0:
        failures.append("no citations returned")
    if invalid_citations:
        failures.append("one or more citations are invalid")

    expected_chunks = case.expected_chunk_ids
    evidence_recall = (
        len(evidence_ids & expected_chunks) / len(expected_chunks)
        if expected_chunks
        else 1.0
    )
    document_recall = (
        len(cited_documents & case.expected_document_ids)
        / len(case.expected_document_ids)
        if case.expected_document_ids
        else None
    )
    forbidden_evidence = evidence_ids & case.forbidden_chunk_ids
    forbidden_documents = cited_documents & case.forbidden_document_ids
    if forbidden_evidence or forbidden_documents:
        failures.append("forbidden evidence returned")

    diagnostics = value.get("diagnostics")
    returned_edges: set[str] = set()
    returned_predicates: set[str] = set()
    if not isinstance(diagnostics, dict):
        failures.append("retrieval diagnostics are missing")
    else:
        if (
            case.expected_resolved_mode is not None
            and diagnostics.get("resolvedMode") != case.expected_resolved_mode
        ):
            failures.append("retrieval mode did not resolve as expected")
        budget_pairs = [
            ("graphDepth", "graphMaxDepth"),
            ("graphPerSeedFanout", "graphPerSeedFanout"),
            ("graphExpandedNodes", "graphMaxExpandedNodes"),
        ]
        for observed_name, requested_name in budget_pairs:
            observed = diagnostics.get(observed_name)
            requested = case.options.get(requested_name)
            if isinstance(requested, int) and (
                not isinstance(observed, int) or observed > requested
            ):
                failures.append(f"{observed_name} exceeded the request budget")
        expanded = diagnostics.get("graphExpandedNodes")
        if (
            not isinstance(expanded, int)
            or expanded < case.minimum_graph_expanded_nodes
        ):
            failures.append("graph expansion was below the fixture minimum")
        expansions = diagnostics.get("graphExpansions", [])
        if isinstance(expansions, list):
            for expansion in expansions:
                if not isinstance(expansion, dict):
                    continue
                result_id = expansion.get("resultId")
                if result_id in case.forbidden_chunk_ids:
                    failures.append("graph expansion returned forbidden evidence")
                path = expansion.get("path", [])
                if not isinstance(path, list):
                    continue
                for edge in path:
                    if not isinstance(edge, dict):
                        continue
                    if isinstance(edge.get("edgeId"), str):
                        returned_edges.add(edge["edgeId"])
                    if isinstance(edge.get("predicate"), str):
                        returned_predicates.add(edge["predicate"])
                    evidence = edge.get("evidenceChunkIds", [])
                    if (
                        isinstance(evidence, list)
                        and case.forbidden_chunk_ids.intersection(evidence)
                    ):
                        failures.append("graph path cited forbidden evidence")

    relationship_expected = {
        *(f"edge:{value}" for value in case.expected_edge_ids),
        *(f"predicate:{value}" for value in case.expected_predicates),
    }
    relationship_returned = {
        *(f"edge:{value}" for value in returned_edges),
        *(f"predicate:{value}" for value in returned_predicates),
    }
    relationship_recall = (
        len(relationship_expected & relationship_returned)
        / len(relationship_expected)
        if relationship_expected
        else None
    )

    serving = value.get("servingGeneration")
    if not isinstance(serving, dict):
        failures.append("serving generation evidence is missing")
    else:
        expected_values = {
            "workspaceId": expected.workspace_id,
            "collection": expected.collection,
            "generationId": expected.generation_id,
            "manifestSha256": expected.manifest_sha256,
        }
        for field, expected_value in expected_values.items():
            if serving.get(field) != expected_value:
                failures.append(f"serving generation {field} mismatch")
        try:
            applied_sequence = int(serving.get("appliedSequence", -1))
        except (TypeError, ValueError):
            applied_sequence = -1
        if applied_sequence < expected.minimum_sequence:
            failures.append("serving generation sequence is stale")

    route = value.get("route")
    route_replica: str | None = None
    route_attempts = 0
    control_stale = False
    if not isinstance(route, dict):
        failures.append("gateway route evidence is missing")
    else:
        route_replica = (
            route.get("replicaId")
            if isinstance(route.get("replicaId"), str)
            else None
        )
        if not route_replica:
            failures.append("route replica ID is missing")
        if route.get("generationId") != expected.generation_id:
            failures.append("route generation mismatch")
        if route.get("manifestSha256") != expected.manifest_sha256:
            failures.append("route manifest mismatch")
        served_sequence = route.get("servedSequence")
        if (
            not isinstance(served_sequence, int)
            or served_sequence < expected.minimum_sequence
        ):
            failures.append("route sequence is stale")
        route_attempts = (
            int(route.get("attempts"))
            if isinstance(route.get("attempts"), int)
            else 0
        )
        if route_attempts < 1:
            failures.append("route attempts are invalid")
        control_stale = route.get("controlStale") is True

    return (
        tuple(sorted(set(failures))),
        evidence_recall,
        document_recall,
        relationship_recall,
        citations,
        invalid_citations,
        route_replica,
        route_attempts,
        control_stale,
    )


def execute_case(
    gateway: str,
    case: QueryCase,
    fixture: Fixture,
    context: ssl.SSLContext,
    token: str,
    timeout_seconds: float,
    scheduled_at: float | None,
) -> Observation:
    started = time.monotonic()
    schedule_delay_ms = (
        max(0.0, (started - scheduled_at) * 1_000)
        if scheduled_at is not None
        else 0.0
    )
    body = json.dumps(
        {
            "query": case.query,
            "options": case.options,
            "barrier": {
                "consistency": "at_least",
                "requiredGenerationId": fixture.expected.generation_id,
                "minimumSequence": fixture.expected.minimum_sequence,
            },
            "timeoutMs": max(1, int(timeout_seconds * 1_000)),
        },
        separators=(",", ":"),
    ).encode("utf-8")
    status: int | None = None
    try:
        status, raw = request(
            f"{gateway}/v1/knowledge/search",
            context,
            timeout_seconds,
            token,
            body=body,
        )
        finished = time.monotonic()
        if status != 200:
            message = safe_error_message(raw)
            return failed_observation(
                case,
                gateway,
                status,
                started,
                finished,
                scheduled_at,
                schedule_delay_ms,
                "http",
                message,
            )
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as error:
            return failed_observation(
                case,
                gateway,
                status,
                started,
                finished,
                scheduled_at,
                schedule_delay_ms,
                "invalid_json",
                str(error),
            )
        (
            failures,
            evidence_recall,
            document_recall,
            relationship_recall,
            citations,
            invalid_citations,
            replica,
            attempts,
            stale,
        ) = validate_response(value, case, fixture.expected)
        return Observation(
            case_id=case.case_id,
            gateway=gateway,
            ok=not failures,
            http_status=status,
            service_latency_ms=(finished - started) * 1_000,
            end_to_end_latency_ms=(
                (finished - scheduled_at) * 1_000
                if scheduled_at is not None
                else (finished - started) * 1_000
            ),
            schedule_delay_ms=schedule_delay_ms,
            evidence_recall=evidence_recall,
            document_recall=document_recall,
            relationship_recall=relationship_recall,
            citations=citations,
            invalid_citations=invalid_citations,
            route_replica=replica,
            route_attempts=attempts,
            control_stale=stale,
            contract_failures=failures,
            error_kind="contract" if failures else None,
            error_message="; ".join(failures) if failures else None,
        )
    except (TimeoutError, urllib.error.URLError, OSError) as error:
        finished = time.monotonic()
        return failed_observation(
            case,
            gateway,
            status,
            started,
            finished,
            scheduled_at,
            schedule_delay_ms,
            "transport",
            sanitize_error(error),
        )


def failed_observation(
    case: QueryCase,
    gateway: str,
    status: int | None,
    started: float,
    finished: float,
    scheduled_at: float | None,
    schedule_delay_ms: float,
    error_kind: str,
    error_message: str,
) -> Observation:
    return Observation(
        case_id=case.case_id,
        gateway=gateway,
        ok=False,
        http_status=status,
        service_latency_ms=(finished - started) * 1_000,
        end_to_end_latency_ms=(
            (finished - scheduled_at) * 1_000
            if scheduled_at is not None
            else (finished - started) * 1_000
        ),
        schedule_delay_ms=schedule_delay_ms,
        evidence_recall=0,
        document_recall=None,
        relationship_recall=None,
        citations=0,
        invalid_citations=0,
        route_replica=None,
        route_attempts=0,
        control_stale=False,
        contract_failures=(),
        error_kind=error_kind,
        error_message=error_message,
    )


def run_workload(
    *,
    gateways: tuple[str, ...],
    fixture: Fixture,
    context: ssl.SSLContext,
    token: str,
    timeout_seconds: float,
    concurrency: int,
    request_count: int | None,
    duration_seconds: float | None,
    target_qps: float,
) -> tuple[dict[str, Any], Accumulator]:
    accumulator = Accumulator()
    index = 0
    index_lock = threading.Lock()
    start_barrier = threading.Barrier(concurrency + 1)
    run_started = 0.0
    run_deadline = 0.0

    def next_work() -> tuple[int, float | None] | None:
        nonlocal index
        with index_lock:
            current = index
            if request_count is not None and current >= request_count:
                return None
            scheduled_at = (
                run_started + current / target_qps if target_qps > 0 else None
            )
            if duration_seconds is not None:
                if scheduled_at is not None and scheduled_at >= run_deadline:
                    return None
                if scheduled_at is None and time.monotonic() >= run_deadline:
                    return None
            index += 1
            return current, scheduled_at

    def worker() -> None:
        start_barrier.wait()
        while True:
            work = next_work()
            if work is None:
                return
            current, scheduled_at = work
            if scheduled_at is not None:
                delay = scheduled_at - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            gateway = gateways[current % len(gateways)]
            case = fixture.cases[current % len(fixture.cases)]
            accumulator.record(
                execute_case(
                    gateway,
                    case,
                    fixture,
                    context,
                    token,
                    timeout_seconds,
                    scheduled_at,
                )
            )

    threads = [
        threading.Thread(target=worker, name=f"knowledge-load-{i}", daemon=True)
        for i in range(concurrency)
    ]
    for thread in threads:
        thread.start()
    run_started = time.monotonic()
    run_deadline = (
        run_started + duration_seconds if duration_seconds is not None else math.inf
    )
    start_barrier.wait()
    for thread in threads:
        thread.join()
    wall_seconds = max(0.000_001, time.monotonic() - run_started)
    return accumulator.summary(wall_seconds), accumulator


def security_probes(
    gateways: tuple[str, ...],
    context: ssl.SSLContext,
    token: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    probes: list[dict[str, Any]] = []
    base_body = json.dumps({"query": "security probe"}).encode()
    for gateway in gateways:
        cases = [
            (
                "health_without_auth",
                "GET",
                "/healthz",
                None,
                None,
                "application/json",
                200,
            ),
            (
                "ready_without_auth",
                "GET",
                "/readyz",
                None,
                None,
                "application/json",
                401,
            ),
            (
                "search_without_auth",
                "POST",
                "/v1/knowledge/search",
                None,
                base_body,
                "application/json",
                401,
            ),
            (
                "search_wrong_auth",
                "POST",
                "/v1/knowledge/search",
                "qualification-intentionally-wrong-token",
                base_body,
                "application/json",
                401,
            ),
            (
                "content_type_rejected",
                "POST",
                "/v1/knowledge/search",
                token,
                base_body,
                "text/plain",
                415,
            ),
            (
                "workspace_override_rejected",
                "POST",
                "/v1/knowledge/search",
                token,
                json.dumps(
                    {"query": "security probe", "workspace": "other-tenant"}
                ).encode(),
                "application/json",
                400,
            ),
            (
                "unbounded_graph_depth_rejected",
                "POST",
                "/v1/knowledge/search",
                token,
                json.dumps(
                    {
                        "query": "security probe",
                        "options": {"graphMaxDepth": 4},
                    }
                ).encode(),
                "application/json",
                400,
            ),
            (
                "oversized_query_rejected",
                "POST",
                "/v1/knowledge/search",
                token,
                json.dumps({"query": "q" * 16_385}).encode(),
                "application/json",
                400,
            ),
            (
                "oversized_body_rejected",
                "POST",
                "/v1/knowledge/search",
                token,
                json.dumps({"query": "q" * (1_048_576 + 1)}).encode(),
                "application/json",
                413,
            ),
        ]
        for (
            name,
            method,
            path,
            probe_token,
            body,
            content_type,
            expected_status,
        ) in cases:
            started = time.monotonic()
            error: str | None = None
            status: int | None = None
            try:
                status, _ = request(
                    f"{gateway}{path}",
                    context,
                    timeout_seconds,
                    probe_token,
                    method=method,
                    body=body,
                    content_type=content_type,
                )
            except (TimeoutError, urllib.error.URLError, OSError) as exception:
                error = sanitize_error(exception)
            probes.append(
                {
                    "gateway": gateway,
                    "probe": name,
                    "expected_status": expected_status,
                    "observed_status": status,
                    "latency_ms": (time.monotonic() - started) * 1_000,
                    "passed": status == expected_status,
                    "error": error,
                }
            )
    return {
        "passed": all(probe["passed"] for probe in probes),
        "probes": probes,
    }


def evaluate_gate(
    summary: dict[str, Any],
    warmup: dict[str, Any],
    security: dict[str, Any] | None,
    gate: Gate,
    gateways: tuple[str, ...],
) -> list[str]:
    failures: list[str] = []

    def below(field: str, observed: float | None, required: float) -> None:
        if observed is None or observed < required:
            failures.append(f"{field} {observed!r} is below {required}")

    if summary["error_rate"] > gate.max_error_rate:
        failures.append(
            f"error rate {summary['error_rate']:.6f} exceeds "
            f"{gate.max_error_rate:.6f}"
        )
    if summary["contract_failures"] > gate.max_contract_failures:
        failures.append(
            f"contract failures {summary['contract_failures']} exceed "
            f"{gate.max_contract_failures}"
        )
    below("evidence recall", summary["evidence_recall"], gate.min_evidence_recall)
    below("document recall", summary["document_recall"], gate.min_document_recall)
    if gate.min_relationship_recall is not None:
        below(
            "relationship recall",
            summary["relationship_recall"],
            gate.min_relationship_recall,
        )
    below(
        "citation correctness",
        summary["citation_correctness"],
        gate.min_citation_correctness,
    )
    if summary["service_latency_ms"]["p95"] > gate.max_p95_ms:
        failures.append(
            f"service p95 {summary['service_latency_ms']['p95']:.3f}ms "
            f"exceeds {gate.max_p95_ms:.3f}ms"
        )
    if summary["service_latency_ms"]["p99"] > gate.max_p99_ms:
        failures.append(
            f"service p99 {summary['service_latency_ms']['p99']:.3f}ms "
            f"exceeds {gate.max_p99_ms:.3f}ms"
        )
    if summary["end_to_end_latency_ms"]["p99"] > gate.max_end_to_end_p99_ms:
        failures.append(
            "schedule-to-completion p99 "
            f"{summary['end_to_end_latency_ms']['p99']:.3f}ms exceeds "
            f"{gate.max_end_to_end_p99_ms:.3f}ms"
        )
    below("achieved QPS", summary["achieved_qps"], gate.min_achieved_qps)
    if len(summary["route_replicas"]) < gate.min_route_replicas:
        failures.append(
            f"observed {len(summary['route_replicas'])} route replicas; "
            f"{gate.min_route_replicas} required"
        )
    if summary["route_failovers"] < gate.min_route_failovers:
        failures.append(
            f"observed {summary['route_failovers']} request failovers; "
            f"{gate.min_route_failovers} required"
        )
    if gate.require_all_gateways:
        missing = sorted(set(gateways) - set(summary["gateways"]))
        if missing:
            failures.append(f"gateways received no measured traffic: {missing}")
    if gate.require_zero_stale_routes and summary["stale_routes"]:
        failures.append(f"observed {summary['stale_routes']} stale-control routes")
    if warmup["failures"]:
        failures.append(f"warmup had {warmup['failures']} failures")
    if gate.require_security_probes and (
        security is None or security.get("passed") is not True
    ):
        failures.append("one or more required security probes failed")
    return failures


def distribution(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {
            "count": 0,
            "min": 0.0,
            "mean": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "mean": average(ordered),
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "max": ordered[-1],
    }


def percentile(ordered: list[float], quantile: float) -> float:
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * quantile) - 1))
    return ordered[index]


def average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def safe_error_message(raw: bytes) -> str:
    try:
        value = json.loads(raw)
        if isinstance(value, dict) and isinstance(value.get("error"), str):
            return value["error"][:512]
    except json.JSONDecodeError:
        pass
    return f"HTTP response body SHA-256 {hashlib.sha256(raw).hexdigest()}"


def sanitize_error(error: BaseException) -> str:
    return str(error).replace("\n", " ")[:512]


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", action="append", required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--gate", type=Path, required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument(
        "--token-env", default="AX_KNOWLEDGE_GATEWAY_TOKEN"
    )
    workload = parser.add_mutually_exclusive_group()
    workload.add_argument("--requests", type=int)
    workload.add_argument("--duration-seconds", type=float)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--target-qps", type=float, default=0)
    parser.add_argument("--warmup-requests", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--security-probes", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.requests is not None and args.requests < 1:
        parser.error("--requests must be positive")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        parser.error("--duration-seconds must be positive")
    if args.concurrency < 1 or args.concurrency > 4096:
        parser.error("--concurrency must be between 1 and 4096")
    if args.target_qps < 0 or not math.isfinite(args.target_qps):
        parser.error("--target-qps must be finite and non-negative")
    if args.warmup_requests < 0:
        parser.error("--warmup-requests must be non-negative")
    if args.timeout_seconds <= 0 or args.timeout_seconds > 120:
        parser.error("--timeout-seconds must be in (0, 120]")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", args.token_env):
        parser.error("--token-env must be a canonical environment variable")
    return args


def main() -> int:
    try:
        args = parse_args()
        fixture = load_fixture(args.fixture)
        gate = load_gate(args.gate)
        gateways = tuple(dict.fromkeys(canonical_gateway(url) for url in args.gateway))
        token = os.environ.get(args.token_env, "")
        if not token or token.strip() != token or "\n" in token:
            raise ConfigurationError(
                f"{args.token_env} must contain a canonical gateway token"
            )
        if not args.ca_file.is_file():
            raise ConfigurationError(f"CA file does not exist: {args.ca_file}")
        context = ssl.create_default_context(cafile=str(args.ca_file))
        context.minimum_version = ssl.TLSVersion.TLSv1_2

        default_requests = len(gateways) * len(fixture.cases)
        request_count = (
            args.requests
            if args.requests is not None
            else None if args.duration_seconds is not None else default_requests
        )
        warmup, _ = run_workload(
            gateways=gateways,
            fixture=fixture,
            context=context,
            token=token,
            timeout_seconds=args.timeout_seconds,
            concurrency=args.concurrency,
            request_count=args.warmup_requests,
            duration_seconds=None,
            target_qps=0,
        )
        measured, _ = run_workload(
            gateways=gateways,
            fixture=fixture,
            context=context,
            token=token,
            timeout_seconds=args.timeout_seconds,
            concurrency=args.concurrency,
            request_count=request_count,
            duration_seconds=args.duration_seconds,
            target_qps=args.target_qps,
        )
        security = (
            security_probes(
                gateways,
                context,
                token,
                args.timeout_seconds,
            )
            if args.security_probes
            else None
        )
        failures = evaluate_gate(measured, warmup, security, gate, gateways)
        report = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "akidb.knowledge-gateway-readiness",
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "fixture": {
                "id": fixture.fixture_id,
                "path": str(args.fixture),
                "sha256": file_sha256(args.fixture),
            },
            "gate": {
                "path": str(args.gate),
                "sha256": file_sha256(args.gate),
                **dataclasses.asdict(gate),
            },
            "environment": {
                "driver_hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python": platform.python_version(),
                "gateways": gateways,
            },
            "workload": {
                "request_count": request_count,
                "duration_seconds": args.duration_seconds,
                "concurrency": args.concurrency,
                "target_qps": args.target_qps,
                "warmup_requests": args.warmup_requests,
                "timeout_seconds": args.timeout_seconds,
            },
            "expected_serving": dataclasses.asdict(fixture.expected),
            "warmup": warmup,
            "measured": measured,
            "security": security,
            "verdict": {
                "status": "pass" if not failures else "fail",
                "failures": failures,
            },
        }
        atomic_json(args.output, report)
        print(
            json.dumps(
                {
                    "output": str(args.output),
                    "verdict": report["verdict"]["status"],
                    "requests": measured["requests"],
                    "successes": measured["successes"],
                    "achieved_qps": measured["achieved_qps"],
                    "p95_ms": measured["service_latency_ms"]["p95"],
                    "p99_ms": measured["service_latency_ms"]["p99"],
                    "failures": failures,
                },
                separators=(",", ":"),
            )
        )
        return 0 if not failures else 1
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
