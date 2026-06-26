# AkiDB Thor Edition - Security Review

**Version:** 1.0
**Date:** 2026-01-21
**Status:** Review Complete
**Classification:** Internal

## Executive Summary

This security review covers the AkiDB Thor Edition deployment, including the core vector database, ingestion pipeline, and supporting infrastructure. The review identifies potential security risks and provides mitigation recommendations.

**Overall Risk Level:** Medium

## Scope

### In Scope

- AkiDB core services (coordinator, shards)
- Ingestion orchestrator (Rust)
- Document parser service (Python)
- Upload gateway (Python)
- NATS JetStream cluster
- MinIO object storage
- Prometheus/Grafana monitoring
- Docker Compose deployment

### Out of Scope

- Network perimeter security
- Physical security of Thor hardware
- Client application security
- Third-party library vulnerabilities (covered by Dependabot)

## Security Assessment

### 1. Authentication & Authorization

#### Current State

| Component | Authentication | Authorization |
|-----------|----------------|---------------|
| AkiDB gRPC | None | None |
| Upload Gateway | None | None |
| MinIO | Username/Password | IAM Policies |
| NATS | None | None |
| Grafana | Username/Password | Role-based |
| Prometheus | None | None |

#### Risks

| Risk | Severity | Status |
|------|----------|--------|
| Unauthenticated gRPC access | High | Open |
| Unauthenticated upload endpoint | High | Open |
| Default MinIO credentials | Critical | Mitigated |
| No NATS authentication | Medium | Open |

#### Recommendations

1. **gRPC Authentication**
   ```rust
   // Implement mTLS for gRPC
   let tls_config = ServerTlsConfig::new()
       .identity(Identity::from_pem(cert, key))
       .client_ca_root(ca_cert);
   ```

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
| Vectors | FAISS Index | None |
| Metadata | RocksDB | None |
| Documents | MinIO | Server-side (optional) |
| State DB | SQLite | None |

#### Data in Transit

| Connection | Protocol | Encryption |
|------------|----------|------------|
| Client → Coordinator | gRPC | None (TLS optional) |
| Coordinator → Shards | gRPC | None |
| Ingestion → NATS | TCP | None |
| Ingestion → MinIO | HTTP | None |
| Ingestion → vLLM | HTTP | None |

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

2. **MinIO Encryption**
   ```bash
   # Enable server-side encryption
   mc admin config set local/ storage_class standard_sse AES256
   ```

3. **Encrypt Sensitive Configuration**
   ```bash
   # Use Ansible Vault for secrets
   ansible-vault encrypt deploy/ansible/group_vars/all/secrets.yml
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
   ┌────▼───┐  ┌────▼───┐  ┌────▼───┐  ┌────▼───┐
   │ NATS-1 │  │ NATS-2 │  │ NATS-3 │  │  MinIO │
   └────────┘  └────────┘  └────────┘  └────────┘
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

2. **Firewall Rules**
   ```bash
   # Only allow necessary ports
   ufw allow 8081/tcp  # Upload Gateway
   ufw deny 4222/tcp   # NATS (internal only)
   ufw deny 9000/tcp   # MinIO (internal only)
   ```

### 5. Secrets Management

#### Current State

| Secret | Storage | Rotation |
|--------|---------|----------|
| MinIO credentials | Environment file | Manual |
| Grafana password | Environment file | Manual |
| TLS certificates | Not implemented | N/A |
| API keys | Not implemented | N/A |

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
   # Automated rotation script
   #!/bin/bash
   NEW_PASSWORD=$(openssl rand -base64 32)
   mc admin user update local akidb-admin $NEW_PASSWORD
   ```

### 6. Logging & Audit

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

2. **Audit Trail**
   ```rust
   // Log all data access
   tracing::info!(
       action = "search",
       user = ?request.user_id,
       query_vectors = query.len(),
       results = results.len(),
       "Search query executed"
   );
   ```

### 7. Container Security

#### Current State

| Aspect | Status | Notes |
|--------|--------|-------|
| Non-root user | Partial | Some containers run as root |
| Read-only filesystem | No | Writable |
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
| SEC-001 | Default MinIO credentials in prod | Use strong passwords | Mitigated |

### High

| ID | Description | Mitigation | Status |
|----|-------------|------------|--------|
| SEC-002 | No gRPC authentication | Implement mTLS | Open |
| SEC-003 | No upload authentication | Add API gateway | Open |
| SEC-004 | No TLS on internal traffic | Enable TLS | Open |

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

## Compliance Checklist

### Production Readiness

- [ ] All services authenticated
- [ ] TLS enabled for all connections
- [ ] Secrets in secure vault
- [ ] Audit logging enabled
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
