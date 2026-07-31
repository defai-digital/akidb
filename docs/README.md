# AkiDB Documentation

## Current design and status

| Area | Status | Canonical document |
| --- | --- | --- |
| Target deployments | Single user: Mac Studio or AMD64 PC. Enterprise: Mac Studio cluster target or AMD64 cloud cluster. Also supported: Mac Mini/MacBook standalone | [Platform Support](platform/SUPPORT.md) |
| Mutable standalone server | Primary supported profile for single-user targets | [Platform Support](platform/SUPPORT.md) |
| Immutable single-node generation serving | Opt-in preview | [Immutable Generation Serving](development/generation-serving-preview.md) |
| Authoritative agent Memory | Experimental single-process developer preview; bounded Linux AMD64 systems profile passed, but production/system-of-record/HA and external product gates remain open | [Authoritative Memory Developer Preview](development/authoritative-memory-preview.md), [Linux AMD64 qualification](quality/linux-amd64-authoritative-memory-qualification.md) |
| PostgreSQL-led full-replica knowledge cell | Implemented; Ubuntu AMD64 qualified for a bounded envelope; Mac Studio cluster is an intended enterprise path | [Agentic Knowledge-Serving Architecture](architecture/knowledge-serving.md), [Ubuntu AMD64 qualification](quality/linux-amd64-knowledge-cell-qualification.md) |
| Market-aligned ANN / graph / competitor parity | Active release gate; automation ready, full evidence verdict not complete | [Market-Readiness Qualification](quality/market-readiness-qualification.md) |
| Multi-shard coordinator | Capacity / fan-out path; optional multi-coordinator entrypoint HA; not shard data replication | [Ansible deployment](../deploy/ansible/README.md) |
| Native GraphRAG | Shipped as bounded retrieval graph, not a general graph DB | [Native GraphRAG Plan](development/native-graphrag-plan.md) |

> Detailed Product Requirements, Architecture Decision Records, research, and
> technical specifications are maintained in the internal-only `.internal/`
> pack. They are intentionally excluded from the public repository; the public
> architecture and qualification documents above are authoritative for released
> capability claims.

## Design

- [Platform Support](platform/SUPPORT.md)
- [Agentic Knowledge-Serving Architecture](architecture/knowledge-serving.md)
- [Immutable Generation Serving](development/generation-serving-preview.md)
- [Authoritative Memory Developer Preview](development/authoritative-memory-preview.md)
- [Native GraphRAG Productization Plan](development/native-graphrag-plan.md)
- [ADR index](adr/README.md)

## Qualification and evidence

- [Market-Aligned Product Readiness Qualification](quality/market-readiness-qualification.md)
- [Ubuntu AMD64 Knowledge-Cell Qualification](quality/linux-amd64-knowledge-cell-qualification.md)
- [Linux AMD64 Authoritative Memory Qualification](quality/linux-amd64-authoritative-memory-qualification.md)
- [Vector Quality Gates](quality/vector-quality.md)
- [One-Mac Benchmark](quality/one-mac-benchmark.md)
- [Four-Mac Cell Validation](quality/four-mac-cell-validation.md)
- [Four-Mac Evidence Manifest](quality/four-mac-evidence-manifest.md)

## Operations

- [Operations Runbook](runbooks/operations.md)
- [Knowledge-Serving Cell Runbook](runbooks/knowledge-serving.md)
- [Incident Response](runbooks/incident-response.md)
- [Ansible Deployment](../deploy/ansible/README.md)
- [Security Review](security/SECURITY_REVIEW.md)
- [Ingestion Orchestrator](services/ingestion-orchestrator/)

## Development notes

- [Agent Configs](development/AGENT_CONFIGS.md)
- [Authoritative Memory Developer Preview](development/authoritative-memory-preview.md)
