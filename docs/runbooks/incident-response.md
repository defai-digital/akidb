# AkiDB Incident Response Runbook

## Overview
This runbook provides procedures for handling incidents affecting the AkiDB vector database service.

## Severity Levels

| Level | Description | Response Time | Examples |
|-------|-------------|---------------|----------|
| SEV1 | Complete service outage | 15 minutes | All coordinators down, data loss |
| SEV2 | Major degradation | 30 minutes | 50%+ queries failing, high latency |
| SEV3 | Minor degradation | 2 hours | Single shard down, elevated errors |
| SEV4 | Low impact | 24 hours | Non-critical warnings |

---

## Common Incidents

### 1. High Search Latency

**Alert:** `AkiDBHighLatency` or `AkiDBCriticalLatency`

**Symptoms:**
- P95 latency > 50ms
- P99 latency > 100ms
- User complaints about slow search

**Diagnosis:**
```bash
# Check current latency metrics
kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics | grep akidb_search_latency

# Check shard health
kubectl get pods -n akidb -l app.kubernetes.io/component=shard

# Check for backpressure
kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics | grep backpressure

# Check GPU memory
kubectl exec -n akidb akidb-shard-0 -- nvidia-smi
```

**Resolution:**
1. **If backpressure is active:**
   - Scale coordinator replicas: `kubectl scale deployment akidb-coordinator -n akidb --replicas=4`
   - Review incoming traffic patterns

2. **If GPU memory is high (>85%):**
   - Trigger compaction: `grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/TriggerCompaction`
   - Consider scaling shards

3. **If single shard is slow:**
   - Check shard logs: `kubectl logs -n akidb akidb-shard-X`
   - Consider restarting: `kubectl delete pod -n akidb akidb-shard-X`

---

### 2. Shard Down

**Alert:** `AkiDBShardDown`

**Symptoms:**
- Pod in CrashLoopBackOff or not ready
- Partial search results

**Diagnosis:**
```bash
# Check pod status
kubectl describe pod -n akidb akidb-shard-X

# Check logs
kubectl logs -n akidb akidb-shard-X --previous

# Check node health
kubectl describe node $(kubectl get pod -n akidb akidb-shard-X -o jsonpath='{.spec.nodeName}')
```

**Resolution:**
1. **If OOM killed:**
   - Increase memory limits in StatefulSet
   - Trigger compaction to reduce memory usage

2. **If GPU error:**
   - Check nvidia-smi on the node
   - Drain and restart node if necessary

3. **If storage full:**
   - Expand PVC or trigger compaction
   - Archive old snapshots

**Recovery verification:**
```bash
# Verify pod is running
kubectl get pod -n akidb akidb-shard-X

# Verify data integrity
grpcurl -plaintext akidb-shard-X:50051 akidb.Health/Check
```

---

### 3. Coordinator Down

**Alert:** `AkiDBCoordinatorDown`

**Symptoms:**
- gRPC connection refused
- All search requests failing

**Diagnosis:**
```bash
# Check deployment status
kubectl get deployment -n akidb akidb-coordinator

# Check pod events
kubectl describe pod -n akidb -l app.kubernetes.io/component=coordinator

# Check service endpoints
kubectl get endpoints -n akidb akidb-coordinator
```

**Resolution:**
1. **Scale up if needed:**
   ```bash
   kubectl scale deployment akidb-coordinator -n akidb --replicas=3
   ```

2. **Restart deployment:**
   ```bash
   kubectl rollout restart deployment akidb-coordinator -n akidb
   ```

3. **Check for resource issues:**
   ```bash
   kubectl top pods -n akidb -l app.kubernetes.io/component=coordinator
   ```

---

### 4. High Tombstone Ratio

**Alert:** `AkiDBHighTombstoneRatio`

**Symptoms:**
- Tombstone ratio > 15%
- Gradual performance degradation
- Increased memory usage

**Diagnosis:**
```bash
# Check tombstone metrics
kubectl exec -n akidb akidb-shard-0 -- curl -s localhost:9090/metrics | grep tombstone

# Check if compaction is running
kubectl exec -n akidb akidb-shard-0 -- curl -s localhost:9090/metrics | grep rebuild
```

**Resolution:**
1. **Trigger manual compaction:**
   ```bash
   grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/TriggerCompaction
   ```

2. **Wait for compaction to complete:**
   - Monitor `akidb_rebuild_in_progress` metric
   - Expected duration: ~5 minutes per million vectors

3. **If compaction fails:**
   - Check logs for errors
   - May need to increase memory during off-peak hours

---

### 5. cuVS Divergence (Phase 4)

**Alert:** `AkiDBCuVSDivergence`

**Symptoms:**
- cuVS results differ from FAISS
- Divergence rate > 0.1%

**Diagnosis:**
```bash
# Check shadow validation stats
kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics | grep cuvs

# Check gate decision
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/GetCuVSGateStatus
```

**Resolution:**
1. **If divergence is high:**
   - Rollback to FAISS immediately:
     ```bash
     kubectl set env deployment/akidb-coordinator AKIDB_CUVS_ENABLED=false -n akidb
     ```

2. **Investigate root cause:**
   - Check cuVS version compatibility
   - Review recent configuration changes
   - Compare specific divergent queries

---

## Escalation Procedures

### When to Escalate

- SEV1: Immediate escalation to on-call lead
- SEV2: Escalate if not resolved in 30 minutes
- Any data loss or corruption suspected
- Multiple simultaneous incidents

### Escalation Contacts

| Role | Primary | Secondary |
|------|---------|-----------|
| On-call Engineer | PagerDuty | Slack #akidb-oncall |
| Team Lead | - | - |
| Platform Team | Slack #platform | - |

---

## Post-Incident

1. **Create incident ticket** with timeline and actions taken
2. **Schedule blameless post-mortem** within 48 hours
3. **Document lessons learned** and action items
4. **Update runbooks** if new procedures discovered
