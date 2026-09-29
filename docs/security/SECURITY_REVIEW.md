# AkiDB Security Review

**Version:** 1.3
**Date:** 2026-09-26
**Status:** Baseline Review (partial refresh)
**Classification:** Public baseline

> **Notice:** This review was originally written in January 2026 when the vector
> index was FAISS-based. The codebase has since migrated to HNSW (usearch),
> bearer-authenticated gRPC, ax-engine embeddings, and a SeaweedFS object store.
> This refresh corrects the service-boundary, object-storage, and transport
> status, but several recommendation code
> samples remain aspirational templates. Treat open items as a backlog, not as
> a complete current-state audit. The immutable replica threat boundary is
> summarized in the [knowledge-serving
> architecture](../architecture/knowledge-serving.md).

## Executive Summary

This security review covers AkiDB's portable macOS and Ubuntu runtime,
including the core vector database, ingestion pipeline, and supporting
infrastructure. It identifies potential security risks and mitigation
recommendations; the notice above takes precedence where this baseline has not
yet been refreshed against current code.

**Overall Risk Level:** Medium

## Scope

### In Scope

- AkiDB core services (coordinator, shards)
- Immutable generation materializer and privileged control API
- PostgreSQL replica worker and AX read-gateway boundary
- Ingestion orchestrator (Rust)
- Document parser service (Python)
- Upload gateway (Python)
- NATS JetStream cluster
- SeaweedFS object storage (S3 gateway)
- Prometheus/Grafana monitoring
- Docker Compose deployment

### Out of Scope

- Network perimeter security
- Physical security of Mac hardware
- Client application security
- Third-party library vulnerabilities (covered by Dependabot)
- Operation of an external managed PostgreSQL HA service

## Security Assessment

### 1. Authentication & Authorization

#### Current State

| Component | Authentication | Authorization |
|-----------|----------------|---------------|
| AkiDB gRPC | Bearer token from environment or mode-0600 file; optional only on loopback | Workspace filtering, but credentials are not yet bound to an allowlist of workspaces |
| Generation Management gRPC | Separate required bearer token | Workspace/collection validation plus checksum and compare-and-swap controls |
| AX knowledge gateway | Separate required bearer token over HTTPS | One configured workspace/collection and generation/checkpoint barriers |
| PostgreSQL replica control | Database credential from a named environment variable; verified TLS by default | External least-privilege database role is required |
| Upload Gateway | None | None |
| SeaweedFS S3 gateway | S3 access key / secret key from a required identity config (`-s3.config`) | Per-identity action list in that config (`Admin`, `Read`, `Write`, `List`, `Tagging`) |
| NATS | None | None |
| Grafana | Username/Password | Role-based |
| Prometheus | None | None |

#### Risks

| Risk | Severity | Status |
|------|----------|--------|
| Unauthenticated non-loopback AkiDB gRPC | High | Mitigated unless an operator explicitly selects `auth.mode = "disabled"` |
| Bearer credential not bound to allowed workspace set | High | Open |
| Built-in gRPC TLS lacks optional client-certificate identity | Medium | Server TLS implemented; mTLS remains open |
| Unauthenticated upload endpoint | High | Open |
| Unauthenticated SeaweedFS S3 gateway (the gateway serves anonymously unless a credential config is supplied) | Critical | Mitigated by configuration requirement: every deployment must start the gateway with an S3 identity config |
| No NATS authentication | Medium | Open |

#### Recommendations

1. **AkiDB Service Identity**

   Keep bearer authentication enabled, use built-in TLS, bind deployments to
   the intended workspace, and keep coordinator, shard,
   generation-management, and replica endpoints private. Add client
   certificate identity if a deployment requires mTLS.

2. **API Gateway with Auth**
   ```yaml
   # Add authentication middleware
   services:
     api-gateway:
       image: kong:latest
       environment:
         KONG_PLUGINS: jwt,rate-limiting
   ```

3. **NATS Authentication**
   ```conf
   # nats.conf with auth
   authorization {
     users = [
       { user: "ingestion", password: "$NATS_PASSWORD", permissions: { publish: ">", subscribe: ">" } }
     ]
   }
   ```

### 2. Data Protection

#### Data at Rest

| Data Type | Storage | Encryption |
|-----------|---------|------------|
| Vectors | HNSW Index (usearch) | None |
| Metadata | RocksDB | None |
| Immutable generation projection | Local RocksDB/HNSW/BM25/graph paths | None; rely on encrypted host volume |
| Generation bundle | SeaweedFS | Server-side encryption is deployment policy |
| Publication/checkpoint authority | PostgreSQL | Encryption is deployment policy |
| Documents | SeaweedFS | Server-side (optional) |
| Ingestion state | SQLite | None |
| Optional SQL metadata | SQLite/PostgreSQL | None |

#### Data in Transit

| Connection | Protocol | Encryption |
|------------|----------|------------|
| Client → AkiDB | gRPC | Built-in TLS; qualified knowledge cell also uses an encrypted private overlay |
| Coordinator → Shards | gRPC | No TLS; qualified lab uses WireGuard |
| Replica → PostgreSQL | PostgreSQL protocol | Verified TLS by default; plaintext restricted to loopback development |
| Replica → SeaweedFS | S3-compatible HTTPS | Configurable; TLS required for remote deployments |
| Ingestion → NATS | TCP | None |
| Ingestion → SeaweedFS | HTTP | None |
| Ingestion → Embedding | HTTP | None |

#### Recommendations

1. **Enable TLS Everywhere**
   ```yaml
   # docker-compose.prod.yml
   services:
     akidb-server:
       environment:
         TLS_CERT_PATH: /certs/server.crt
         TLS_KEY_PATH: /certs/server.key
   ```

2. **Object-Store Encryption**

   Server-side encryption of generation bundles and documents is deployment
   policy, not an AkiDB setting. Encrypt the object-store volumes or enable the
   store's own encryption at rest, and verify that configuration before
   accepting production data.

3. **Environment-Based Secrets**
   ```bash
   # Use macOS Keychain or a .env file (gitignored) for local secrets.
   # For Compose, use Docker secrets or an env_file with restricted permissions.
   chmod 600 deploy/compose/.env
   ```

### 3. Input Validation

#### Document Upload

| Check | Implemented | Notes |
|-------|-------------|-------|
| File size limit | Yes | 100MB default |
| File type validation | Partial | Extension-based |
| Content scanning | No | Malware risk |
| Filename sanitization | Yes | Path traversal prevention |

#### Search Queries

| Check | Implemented | Notes |
|-------|-------------|-------|
| Query length limit | No | DoS risk |
| Vector dimension validation | Yes | Type checked |
| Result limit (k) | Yes | Max 1000 |

#### Recommendations

1. **Add Malware Scanning**
   ```python
   # In upload-gateway
   import clamd

   def scan_file(file_path: str) -> bool:
       cd = clamd.ClamdNetworkSocket()
       result = cd.scan(file_path)
       return result[file_path][0] == 'OK'
   ```

2. **Query Rate Limiting**
   ```rust
   // Add rate limiter to coordinator
   let rate_limiter = RateLimiter::new(
       NonZeroU32::new(100).unwrap(),  // 100 requests
       Duration::from_secs(1),          // per second
   );
   ```

### 4. Network Security

#### Current Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   Internet      │────▶│  Upload Gateway │
└─────────────────┘     └────────┬────────┘
                                 │
                    ┌────────────┼────────────┐
                    │   akidb-net (bridge)    │
                    │                         │
        ┌───────────┼───────────┐            │
        │           │           │            │
   ┌────▼───┐  ┌────▼───┐  ┌────▼───┐  ┌────▼────┐
   │ NATS-1 │  │ NATS-2 │  │ NATS-3 │  │SeaweedFS│
   └────────┘  └────────┘  └────────┘  └─────────┘
```

#### Risks

| Risk | Severity | Status |
|------|----------|--------|
| All services on same network | Medium | By design |
| Exposed Prometheus metrics | Low | Internal only |
| Exposed Grafana | Low | Password protected |
| No network segmentation | Medium | Open |

#### Recommendations

1. **Network Segmentation**
   ```yaml
   networks:
     frontend:
       driver: bridge
     backend:
       driver: bridge
       internal: true  # No external access
     monitoring:
       driver: bridge
       internal: true
   ```

2. **macOS Application Firewall**
   ```bash
   # Use pf (packet filter) on macOS to restrict local ports
   # Example: block external access to internal-only services
   echo "block in from any to any port {4222, 8333}" | sudo pfctl -ef -
   ```

### 5. Secrets Management

#### Current State

| Secret | Storage | Rotation |
|--------|---------|----------|
| SeaweedFS S3 identities | S3 identity config file (`-s3.config`) | Manual: edit the identity file and reload the S3 gateway with SIGHUP |
| Grafana password | Environment file | Manual |
| TLS certificates | Restricted host files supplied by deployment PKI | External PKI rotation |
| AkiDB/gateway bearer tokens | Restricted environment/token files | Manual or external secret manager |

#### Recommendations

1. **Use HashiCorp Vault**
   ```yaml
   # docker-compose.yml
   services:
     vault:
       image: hashicorp/vault:latest
       cap_add:
         - IPC_LOCK
   ```

2. **Rotate Credentials**
   ```bash
   # Rotate one SeaweedFS S3 identity: rewrite its credentials in the
   # -s3.config identity file, then reload the gateway.
   NEW_SECRET=$(openssl rand -base64 32)
   # edit '"secretKey": "<old>"' -> "$NEW_SECRET" in the identity file, then:
   kill -HUP "$(pgrep -f 'weed .*s3')"
   ```

### 6. Logging & Audit

#### Implemented: access-record emission

`[observability.access_log]` (off by default) makes every data-plane RPC
(`Search`, `TextSearch`, `Get`, `Insert`, `InsertBatch`, `Delete`, `Update`)
emit one content-free access record: principal/credential id (null when the
request presented no principal credential), workspace, collection, a keyed
HMAC-SHA256 query digest (`AKIDB_ACCESS_LOG_HASH_KEY`, or a random
per-process key), hit count, latency, and outcome. Records go through a
non-blocking queue to an append-only local JSONL spool (`0600`, size-bounded
rotation, hard file-count cap).

This is deliberately **not** an audit store: the spool is a delivery buffer
for a governance platform, it is never exposed over any API, and emission is
fail-open (drops are counted, the data plane never blocks). Adjudication,
retention, and immutable storage belong to the consuming governance
platform.

#### Current Logging

| Component | Log Level | Sensitive Data |
|-----------|-----------|----------------|
| AkiDB | INFO | Vector IDs |
| Ingestion | INFO | Document paths |
| Upload Gateway | INFO | Filenames |
| NATS | INFO | Message subjects |

#### Recommendations

1. **Centralized Logging**
   ```yaml
   services:
     loki:
       image: grafana/loki:latest
     promtail:
       image: grafana/promtail:latest
   ```

2. **Ship access records to a governance platform**
   Enable `[observability.access_log]` and forward the spool to the
   deployment's audit/ledger system; that system — not AkiDB — owns
   retention and immutability.

### 7. Container Security

#### Current State

| Aspect | Status | Notes |
|--------|--------|-------|
| Non-root user | Partial | Some containers run as root |
| Read-only filesystem | Partial | doc-parser, upload-gateway use `read_only: true` |
| Resource limits | Yes | In prod compose |
| Security scanning | No | Not implemented |

#### Recommendations

1. **Run as Non-root**
   ```dockerfile
   # In Dockerfiles
   RUN addgroup -S akidb && adduser -S akidb -G akidb
   USER akidb
   ```

2. **Enable Seccomp**
   ```yaml
   services:
     ingestion:
       security_opt:
         - seccomp:seccomp-profile.json
   ```

3. **Image Scanning**
   ```bash
   # Scan images before deployment
   trivy image akidb-ingestion:latest
   ```

## Vulnerability Summary

### Critical

| ID | Description | Mitigation | Status |
|----|-------------|------------|--------|
| SEC-001 | The SeaweedFS S3 gateway serves every request anonymously unless it is started with a credential config | Require an S3 identity config (`-s3.config`) in every deployment and rotate its keys | Mitigated by configuration requirement |

### High

| ID | Description | Mitigation | Status |
|----|-------------|------------|--------|
| SEC-002 | A generic bearer credential is not a multi-workspace allowlist | Use one scoped knowledge-cell deployment per workspace/collection; add credential allowlists before shared multi-tenant service | Mitigated for qualified cell; open for shared multi-tenant use |
| SEC-003 | No upload authentication | Add API gateway | Open |
| SEC-004 | Legacy Compose/coordinator paths may use plaintext internal traffic | Keep those paths isolated; knowledge cell uses gRPC/HTTPS/PostgreSQL TLS plus WireGuard | Mitigated for qualified cell |

### Medium

| ID | Description | Mitigation | Status |
|----|-------------|------------|--------|
| SEC-005 | No NATS authentication | Configure auth | Open |
| SEC-006 | No malware scanning | Add ClamAV | Open |
| SEC-007 | Containers run as root | Add USER directive | Open |

### Low

| ID | Description | Mitigation | Status |
|----|-------------|------------|--------|
| SEC-008 | Exposed metrics endpoints | Network segmentation | Open |
| SEC-009 | No rate limiting | Add rate limiter | Open |

## Knowledge-Cell Release Assessment

The bounded Ubuntu AMD64 knowledge-cell profile has no accepted Critical or
High blocker:

- AkiDB gRPC, gateway HTTP, SeaweedFS, and PostgreSQL links use verified TLS;
- WireGuard and host firewall rules keep service listeners off public
  interfaces;
- read, generation-control, gateway, and object-store identities are distinct:
  the SeaweedFS S3 gateway is configured with separate admin, read-only replica,
  and publisher identities;
- replicas receive read-only object access, while the publisher identity is
  limited to the `ax-fabric/` prefix;
- the cell is configured for one authenticated workspace/collection, and
  traversal/routing never broadens that scope;
- checksum-addressed immutable bundles, exact generation evidence, quorum
  activation, and audit records protect the publication boundary;
- services run as dedicated system users with systemd filesystem hardening.

Production operators must still provide encrypted local volumes, managed HA
PostgreSQL, durable HA object storage, PKI/secret rotation, and monitoring
retention. The upload gateway, legacy coordinator, and Compose/NATS findings
above are separate profiles and are not silently inherited into the qualified
knowledge cell.

## Compliance Checklist

### Production Readiness

- [ ] All services authenticated
- [ ] TLS enabled for all connections
- [ ] Secrets in secure vault
- [x] Access-record emission implemented (opt-in; see §6 — governance
      platform still owns the audit store)
- [ ] Network segmentation implemented
- [ ] Container hardening complete
- [ ] Vulnerability scanning automated

### Security Hardening Priorities

1. **Immediate (Pre-Production)**
   - Enable TLS for gRPC
   - Add authentication to Upload Gateway
   - Configure NATS authentication

2. **Short-term (30 days)**
   - Implement centralized logging
   - Add malware scanning
   - Container hardening

3. **Medium-term (90 days)**
   - Deploy HashiCorp Vault
   - Implement network segmentation
   - Automated security scanning

## Appendix: Security Configuration Templates

### TLS Configuration

```toml
# akidb.toml
[tls]
enabled = true
cert_path = "/certs/server.crt"
key_path = "/certs/server.key"
ca_path = "/certs/ca.crt"
require_client_cert = true
```

### Secure NATS Configuration

```conf
# nats-secure.conf
tls {
    cert_file: "/certs/nats-server.crt"
    key_file: "/certs/nats-server.key"
    ca_file: "/certs/ca.crt"
    verify: true
}

authorization {
    default_permissions = {
        publish = { deny = ">" }
        subscribe = { deny = ">" }
    }
    users = [
        {
            user: "ingestion"
            password: "$ARGON2ID$..."
            permissions = {
                publish = ["akidb.>"]
                subscribe = ["akidb.uploads.>"]
            }
        }
    ]
}
```

---

**Review Sign-off:**

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Security Lead | | | |
| DevOps Lead | | | |
| Project Lead | | | |

---

*This document is confidential and intended for internal use only.*
