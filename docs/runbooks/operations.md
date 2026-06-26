# AkiDB Operations Runbook

## Deployment Operations

### Initial Deployment

```bash
# 1. Create namespace and base resources
kubectl apply -k deploy/kubernetes/

# 2. Wait for shards to be ready
kubectl rollout status statefulset/akidb-shard -n akidb --timeout=10m

# 3. Wait for coordinators to be ready
kubectl rollout status deployment/akidb-coordinator -n akidb --timeout=5m

# 4. Verify health
grpcurl -plaintext $(kubectl get svc akidb-coordinator -n akidb -o jsonpath='{.spec.clusterIP}'):50051 grpc.health.v1.Health/Check
```

### Rolling Update

```bash
# 1. Update image tag
kubectl set image statefulset/akidb-shard akidb-shard=akidb/akidb-server:v1.2.0 -n akidb
kubectl set image deployment/akidb-coordinator akidb-coordinator=akidb/akidb-coordinator:v1.2.0 -n akidb

# 2. Monitor rollout
kubectl rollout status statefulset/akidb-shard -n akidb
kubectl rollout status deployment/akidb-coordinator -n akidb

# 3. Verify health post-deployment
./scripts/health-check.sh
```

### Rollback

```bash
# 1. Rollback deployment
kubectl rollout undo statefulset/akidb-shard -n akidb
kubectl rollout undo deployment/akidb-coordinator -n akidb

# 2. Verify rollback
kubectl rollout status statefulset/akidb-shard -n akidb
```

---

## Scaling Operations

### Scale Coordinators

```bash
# Scale up
kubectl scale deployment akidb-coordinator -n akidb --replicas=4

# Scale down (ensure at least 2 for HA)
kubectl scale deployment akidb-coordinator -n akidb --replicas=2
```

### Add New Shard

Adding a new shard requires data rebalancing:

```bash
# 1. Scale StatefulSet
kubectl scale statefulset akidb-shard -n akidb --replicas=5

# 2. Wait for new shard
kubectl rollout status statefulset/akidb-shard -n akidb

# 3. Update coordinator config with new shard address
kubectl edit configmap akidb-coordinator-config -n akidb
# Add: akidb-shard-4.akidb-shard.akidb.svc.cluster.local:50051

# 4. Restart coordinators to pick up new config
kubectl rollout restart deployment/akidb-coordinator -n akidb

# 5. Trigger data rebalancing (if supported)
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/RebalanceShards
```

---

## Maintenance Operations

### Trigger Manual Compaction

```bash
# Trigger compaction on all shards
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/TriggerCompaction

# Monitor compaction progress
watch 'kubectl exec -n akidb akidb-shard-0 -- curl -s localhost:9090/metrics | grep rebuild'
```

### Create Manual Snapshot

```bash
# Trigger snapshot to MinIO
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/CreateSnapshot

# Verify snapshot in MinIO
mc ls minio/akidb-snapshots/
```

### Restore from Snapshot

```bash
# 1. Scale down to 0
kubectl scale statefulset/akidb-shard -n akidb --replicas=0

# 2. Copy snapshot data to PVCs (implementation-specific)
./scripts/restore-snapshot.sh <snapshot-id>

# 3. Scale back up
kubectl scale statefulset/akidb-shard -n akidb --replicas=4

# 4. Verify data integrity
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/VerifyIntegrity
```

---

## cuVS Operations (Phase 4)

### Enable cuVS (After Gate Validation)

```bash
# 1. Verify gate criteria are met
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/GetCuVSGateStatus

# 2. Enable cuVS
kubectl set env deployment/akidb-coordinator AKIDB_CUVS_ENABLED=true -n akidb

# 3. Monitor for divergence
watch 'kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics | grep cuvs_divergence'
```

### Disable cuVS (Rollback)

```bash
# Immediate rollback to FAISS
kubectl set env deployment/akidb-coordinator AKIDB_CUVS_ENABLED=false -n akidb

# Verify FAISS is active
kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics | grep index_backend
```

### Shadow Mode Validation

```bash
# Enable shadow mode (both FAISS and cuVS run, FAISS results returned)
kubectl set env deployment/akidb-coordinator AKIDB_CUVS_SHADOW_MODE=true -n akidb

# Run for 24 hours, then check results
grpcurl -plaintext akidb-coordinator:50051 akidb.Admin/GetShadowValidationStats
```

---

## Monitoring & Debugging

### Check Cluster Health

```bash
#!/bin/bash
# health-check.sh

echo "=== Checking AkiDB Cluster Health ==="

# Check pods
echo -e "\n--- Pod Status ---"
kubectl get pods -n akidb -o wide

# Check services
echo -e "\n--- Service Endpoints ---"
kubectl get endpoints -n akidb

# Check coordinator health
echo -e "\n--- Coordinator Health ---"
for pod in $(kubectl get pods -n akidb -l app.kubernetes.io/component=coordinator -o name); do
  echo "Checking $pod..."
  kubectl exec -n akidb $pod -- curl -s localhost:9090/metrics | grep -E "^akidb_(requests_total|errors_total)" | head -5
done

# Check shard health
echo -e "\n--- Shard Health ---"
for i in $(seq 0 3); do
  echo "Checking akidb-shard-$i..."
  kubectl exec -n akidb akidb-shard-$i -- curl -s localhost:9090/metrics | grep -E "^akidb_(vectors_total|tombstone)" | head -3
done

echo -e "\n=== Health Check Complete ==="
```

### Get Detailed Metrics

```bash
# All metrics from coordinator
kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics

# Search latency percentiles
kubectl exec -n akidb deploy/akidb-coordinator -- curl -s localhost:9090/metrics | \
  grep akidb_search_latency_seconds

# GPU metrics from shard
kubectl exec -n akidb akidb-shard-0 -- curl -s localhost:9090/metrics | \
  grep akidb_gpu
```

### Debug gRPC Calls

```bash
# Enable gRPC reflection
grpcurl -plaintext akidb-coordinator:50051 list

# Describe service
grpcurl -plaintext akidb-coordinator:50051 describe akidb.VectorService

# Test search
grpcurl -plaintext -d '{"query": [0.1, 0.2, ...], "top_k": 10}' \
  akidb-coordinator:50051 akidb.VectorService/Search
```

---

## Backup & Recovery

### Automated Backup Schedule

Backups are configured via CronJob:

```yaml
# deploy/kubernetes/backup-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: akidb-backup
  namespace: akidb
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: akidb/backup-tool:latest
            args:
            - --destination=s3://akidb-backups/$(date +%Y-%m-%d)
          restartPolicy: OnFailure
```

### Disaster Recovery

1. **Identify backup to restore:**
   ```bash
   mc ls minio/akidb-backups/
   ```

2. **Stop all traffic:**
   ```bash
   kubectl scale deployment akidb-coordinator -n akidb --replicas=0
   ```

3. **Restore data:**
   ```bash
   ./scripts/disaster-recovery.sh <backup-date>
   ```

4. **Verify and resume:**
   ```bash
   kubectl scale deployment akidb-coordinator -n akidb --replicas=2
   ./scripts/health-check.sh
   ```

---

## Capacity Planning

### Current Capacity Check

```bash
# Vectors per shard
kubectl exec -n akidb akidb-shard-0 -- curl -s localhost:9090/metrics | grep vectors_total

# Memory usage
kubectl top pods -n akidb

# GPU memory
for i in $(seq 0 3); do
  echo "=== Shard $i ==="
  kubectl exec -n akidb akidb-shard-$i -- nvidia-smi --query-gpu=memory.used,memory.total --format=csv
done
```

### Scaling Guidelines

| Metric | Threshold | Action |
|--------|-----------|--------|
| Vectors per shard | > 5M | Add shard |
| GPU memory | > 80% | Add shard or trigger compaction |
| P95 latency | > 30ms | Scale coordinators |
| QPS per coordinator | > 200 | Scale coordinators |
