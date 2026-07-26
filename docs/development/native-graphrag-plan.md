# Native GraphRAG Productization Plan

Status: accepted; aligned with the versioned knowledge-serving design

Date: 2026-07-24

## Decision

AkiDB will productize its existing RocksDB-backed graph foundation as an
optional, embedded GraphRAG retrieval layer. It will not require Neo4j,
Memgraph, or another general-purpose graph database at runtime.

The product boundary is:

- Ingestion owns parsing, deterministic relationship discovery, entity
  extraction, canonicalization, and review workflows.
- AkiDB owns the local, rebuildable graph projection, bounded traversal,
  vector/BM25/SQL fusion, reranking, and evidence-bearing context assembly.
- The trust/control plane owns credential-to-workspace authorization, policy,
  audit, retention, and operator review.
- External graph formats may be supported for export or offline analysis, but
  are not a core runtime dependency.

This preserves AkiDB's embedded, local-first deployment model while adding the
relationship retrieval needed by private enterprise AI. In immutable
generation mode, AX Fabric/OpenWiki remain authoritative and the graph is
rebuilt alongside vector, lexical, and payload state from one logical bundle;
see the [knowledge-serving architecture](../architecture/knowledge-serving.md).

## Why this direction

| Choice | Benefits | Costs and risks | Decision |
| --- | --- | --- | --- |
| Vector/BM25 only | Simple, mature, efficient for semantic and exact text retrieval | Cannot reliably answer explicit relationship or multi-hop questions | Keep as the baseline, not the complete product |
| Mandatory external graph database | Rich query language and mature graph tooling | Additional service, memory, operations, security boundary, and failure mode; weakens the single-appliance story | Reject for the core runtime |
| AkiDB native GraphRAG layer | Embedded operation, low-latency fusion, shared provenance and ACL handling, differentiated private-AI product | More indexing, consistency, schema, evaluation, and traversal-security work | Adopt |
| Build a general graph database | Broad market and arbitrary graph workloads | Large scope, competing query language/transaction/cluster expectations, distracts from retrieval quality | Explicitly out of scope |

The native approach has the best product fit, provided AkiDB treats the graph as
an evidence-bearing retrieval projection rather than an unrestricted database.

## Best-practice architecture

### Source of truth and consistency

AX Fabric/OpenWiki identities and versioned source objects in MinIO are
canonical. AkiDB's vector, lexical, payload, and native graph structures are
retrieval projections.

In mutable standalone mode, RocksDB records are the durable local state used
to rebuild in-memory indexes. A graph mutation derived from one chunk must be
atomic inside RocksDB: remove stale projection edges, merge nodes, and add
replacement edges in one batch. Until every mutable projection has a durable
journal/outbox, writes remain idempotent, each chunk records its last edge IDs,
failures are retryable, and operators can rebuild the graph from durable local
records.

In immutable generation mode, the logical bundle binds records, graph nodes,
and graph edges to the same workspace, collection, source versions, embedding
model, and generation. A shadow build is either verified and atomically
activated as a whole or never becomes visible. AkiDB database/index files are
not canonical and are never shared between replicas.

### Deterministic relationships first

Relationships that come directly from source protocols or parser structure
have the highest value and lowest risk:

- document, section, page, and chunk containment;
- email thread membership, sender/recipient, and attachments;
- OpenWiki document/revision identity and typed relationships;
- MinIO object identity and source version;
- PDF page/image extraction;
- ticket creation and source evidence when supplied by a trusted system.

LLM/NER-derived entities come later and must never be indistinguishable from
deterministic facts.

### Evidence-bearing assertions

Every extracted relationship must eventually carry:

- workspace/security scope;
- assertion state: `asserted`, `extracted`, `inferred`,
  `human_verified`, or `rejected`;
- confidence and extraction method;
- evidence chunk, source URI, source version, and source span;
- pipeline/model version;
- observation and validity time;
- a stable assertion ID for correction and retraction.

The first implementation records deterministic provenance on projection edges.
A later assertion ledger will make correction, history, and temporal reasoning
first-class.

### Schema without enum explosion

`Document`, `Chunk`, `Section`, `File`, and other structural `NodeKind` values
remain useful core primitives. Business concepts use `NodeKind::Entity` plus a
registered schema and `entity_type`, rather than adding every customer-specific
concept to the Rust enum.

Domain relationships initially use a small core `EdgeKind` plus a registered
predicate. Once predicate filtering is available, RocksDB needs secondary
indexes for:

- workspace + predicate + source node;
- workspace + predicate + target node;
- workspace + canonical entity ID;
- evidence chunk + assertion ID.

Frequently queried predicates must not require scans of JSON properties.

### Security at every hop

Workspace scope is encoded in graph node IDs. Immutable graph records are also
bound to a collection and generation. Native graph writes reject edges whose
endpoints are in different workspace namespaces, and traversal only follows
nodes in the request's workspace and active generation. Vector, batch, point,
SQL, and context-pack results are filtered through the same boundary.

This is projection isolation, not yet a complete tenant identity boundary. The
current bearer-token runtime accepts a caller-selected workspace header.
Production multi-tenant claims require credentials to be bound to an allowed
workspace/security domain and forwarded by the future AX retrieval gateway.
The existing shard coordinator does not provide that gateway contract.

### Bounded retrieval, not arbitrary traversal

Online traversal is deterministic and bounded by:

- a maximum hop count (currently capped at three);
- edge/predicate allowlists;
- per-hop fan-out;
- result and token budgets;
- workspace scope;
- stable ordering and deduplication.

Arbitrary Cypher compatibility is not required. A small retrieval-oriented
query contract is safer and easier to benchmark.

## Runtime retrieval flow

```text
query
  -> workspace/ACL scope
  -> active generation/checkpoint scope (generation mode)
  -> deterministic intent planner
  -> vector, BM25, or structured SQL seeds
  -> bounded native graph expansion
  -> workspace filter at retrieval boundaries
  -> fusion and optional rerank/MMR
  -> token-budgeted context pack
  -> source URI + version + span citations
```

Explicit identifiers should favor BM25/metadata seeds. Relationship questions
should enable graph expansion. Complex enterprise questions combine structured
filters, semantic/exact seeds, graph traversal, and reranking.

## Delivery phases

### Phase 0 — Projection and isolation guardrails

Implemented in the initial productization change:

- workspace-scoped graph node IDs with legacy default-workspace compatibility;
- cross-workspace edge rejection and same-workspace traversal;
- workspace enforcement for point reads/mutations, batch search/write, and
  structured SQL retrieval;
- per-vector mutation locks around ownership checks and writes;
- serialized graph read/modify/write preparation, atomic RocksDB mutation
  batches, and atomic node deletion;
- per-chunk projection manifests for crash-safe stale-edge replacement;
- bounded multi-hop related-chunk retrieval;
- metadata-derived edge replacement;
- empty-graph bootstrap and full projection rebuild from durable active vectors;
- provenance-aware context citations;
- tests for isolation, atomicity, replacement, rebuild, and two-hop retrieval.

Exit gate: cross-workspace retrieval/mutation tests pass, invalid graph batches
leave no partial state, and graph projections survive restart/rebuild.

### Phase 1 — Deterministic document graph

The initial foundation emits MinIO source identity, document/file containment,
chunk offsets, pipeline version, and deterministic provenance. Complete this
phase with:

- email thread, sender/recipient, and attachment adapters;
- PDF document/page/chunk structure and image/OCR evidence;
- stable source object/version IDs and idempotent batch ingestion;
- deletion/retention propagation through text, vector, SQL, graph, summaries,
  and extracted images;
- orphan-node garbage collection and projection checkpoints;
- a versioned document/email schema registry.

Exit gate: replaying the same source version produces no duplicate assertions;
deleting a source removes all derived evidence; deterministic relationship
precision is 100% on the fixture corpus.

### Phase 2 — Assertion and predicate layer

- Add a versioned `GraphAssertion` contract with predicate, state, confidence,
  provenance, temporal fields, and stable ID.
- Add predicate/entity/evidence secondary indexes.
- Add batch retract/supersede operations and a durable projection outbox.
- Add human verification/rejection APIs and immutable audit events.
- Keep inferred assertions separate from asserted/extracted facts.

Exit gate: every returned relationship resolves to evidence; correction and
retraction are idempotent; crash recovery leaves vector and graph projections
convergent.

### Phase 3 — Entity extraction and canonicalization

- Run NER/LLM extraction in ingestion, not in the graph storage engine.
- Start with person, organization, customer, product/version, ticket, invoice,
  contract, error code, and resolution.
- Add aliases, deterministic normalization, tenant-local canonical IDs,
  duplicate candidates, merge/split history, and review queues.
- Record model and prompt/pipeline versions with every extracted assertion.

Exit gate: entity resolution and relationship precision/recall meet
domain-specific thresholds; a model upgrade can be replayed and rolled back
without losing prior evidence.

### Phase 4 — Graph-aware planning and retrieval quality

- Expand relationship intent coverage using deterministic patterns plus an
  optional local classifier.
- Support vector/BM25 seed to graph expansion, predicate filters, path results,
  graph-aware reranking, dynamic hop/fan-out budgets, and structured citations.
- Treat graph evidence as an explicit ranking feature rather than an
  unconditional score boost.
- Publish graph contribution and traversal traces for evaluation and audit.

Exit gate: GraphRAG materially improves multi-hop answer accuracy and evidence
recall over hybrid retrieval without unacceptable citation or latency
regressions.

### Phase 5 — Temporal, retention, and operational maturity

- Add `observed_at`, `valid_from`, `valid_to`, supersession, and as-of queries.
- Add contradiction/support relations with separate inferred state.
- Add graph compaction, orphan collection, repair tooling, backup/restore
  verification, quotas, and per-workspace metrics.
- Bind credentials to allowed workspaces/security domains and audit every
  sensitive traversal.

Exit gate: cross-workspace leakage remains zero; retention and legal deletion
tests remove all derivatives; as-of answers are reproducible from cited source
versions.

### Phase 6 — Generation-scoped replicas and optional interoperability

- Build the same graph projection independently on each full AkiDB replica
  from the authoritative generation bundle.
- Include generation, manifest digest, checkpoint, and evidence identity in
  readiness and retrieval responses.
- Have the AX gateway forward authorization context and route only to replicas
  that match the active generation/checkpoint.
- Keep graph expansion local to one verified full replica. Do not introduce
  unrestricted distributed graph walks before measured sharding need.
- Add optional GraphML/JSONL/Neo4j export for analysis and migration.

Exit gate: independently rebuilt replicas have identical graph digest/counts
and golden-query evidence; a stale or wrong-generation replica receives no
traffic; P50/P95 latency and one-node failure behavior meet the cell SLO; no
external graph service is required.

## Evaluation gates

Each phase compares:

1. vector only;
2. vector + BM25 + metadata;
3. hybrid + one-hop graph;
4. hybrid + two-hop graph;
5. hybrid + graph + rerank.

Required measures include:

- evidence recall@k and citation correctness;
- multi-hop answer accuracy;
- entity resolution accuracy;
- relationship precision/recall by assertion state;
- cross-workspace leakage (must be zero);
- hallucination/unsupported-claim rate;
- P50/P95 latency and fan-out;
- index size and rebuild time;
- graph expansion token cost;
- deletion/retraction completeness.

The evaluation corpus should include at least 100 relationship questions over
email, PDF, image/OCR, ticket, invoice, contract, product/version, and policy
fixtures. A phase does not advance merely because graph lookup is fast:
GraphRAG must improve grounded answer quality over the hybrid baseline.

## Non-goals

- Neo4j/Cypher compatibility in the core runtime;
- unrestricted user-defined graph transactions;
- putting OCR, LLM inference, or human review inside the storage crate;
- treating inferred edges as facts;
- expanding `NodeKind` for every industry noun;
- distributed graph traversal before single-node quality and isolation gates
  pass.
