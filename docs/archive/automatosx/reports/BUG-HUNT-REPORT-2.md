# Bug Hunt Report #2 - AkiDB Thor Edition

**Date**: 2026-01-22
**Agent**: bug-hunter (second pass)
**Focus**: WAL recovery, ID mapping, gRPC service, fanout logic, connection pooling

---

## Executive Summary

The bug-hunter agent performed a deep scan focusing on areas not analyzed in the first pass. **4 bugs were identified and fixed**, ranging from critical data correctness issues to connection management problems.

---

## Bugs Fixed

### 1. [CRITICAL] BUG-HUNT-404: ResultMerger Sort Order Bug
**Location**: `crates/coordinator/src/merger.rs:117-133`

**Issue**: During heap compaction, `sorted.sort()` used the reversed `Ord` implementation (designed for min-heap), causing it to keep the **lowest** scores instead of the highest. This resulted in **incorrect search results** being returned to users.

**Fix Applied**:
- Replaced `.sort()` with `.sort_by()` that explicitly sorts by score descending
- Added proper NaN handling in the sort comparator
- Comment: `FIX BUG-HUNT-404`

**Impact**: Search results are now correct.

---

### 2. [CRITICAL] BUG-HUNT-401: Connection Pool Bypass
**Location**: `crates/coordinator/src/bin/coordinator.rs:130-134, 312-315`

**Issue**: The `insert()` and `get()` methods created **new TCP connections per request** instead of using the connection pool. This caused:
- Connection exhaustion under load
- Latency overhead (TCP handshake per request)
- Inconsistent behavior vs search (which used the pool)

**Fix Applied**:
- Changed from `AkidbClient::connect(endpoint)` to `self.fanout.get_shard_client(&shard_address)`
- Now uses the existing connection pool infrastructure
- Comment: `FIX BUG-HUNT-401`

**Impact**: Consistent connection pooling across all operations.

---

### 3. [HIGH] BUG-HUNT-403: WAL Size Limit Mismatch
**Location**: `crates/storage/src/wal.rs:296-309, 420, 467`

**Issue**: The write path allowed entries up to 4GB, but the read path rejected entries >100MB. This meant:
- Large entries could be written successfully
- On recovery, these entries would be treated as corruption and **silently skipped**
- Result: **Data loss on restart**

**Fix Applied**:
- Added `MAX_WAL_ENTRY_BYTES` constant (100MB)
- Added write-time validation to reject entries exceeding the limit
- Updated read path to use the constant for consistency
- Comment: `FIX BUG-HUNT-403`

**Impact**: Write-time failures catch oversized entries early, preventing data loss.

---

### 4. [HIGH] BUG-HUNT-402: insert_batch Silent Vector Loss
**Location**: `crates/coordinator/src/bin/coordinator.rs:369-429`

**Issue**: When a shard connection failed during `insert_batch`, vectors destined for that shard were **silently lost**:
- Only a warning was logged
- Vector IDs were not added to `failed_ids` response
- Caller had no indication of partial failure

**Fix Applied**:
- Capture vector IDs before spawning tasks
- Return `(result, vector_ids)` tuple from each task
- On failure, add all vector IDs from failed shard to `failed_ids`
- Comment: `FIX BUG-HUNT-402`

**Impact**: Callers now receive accurate failure information for retry.

---

## Test Results

```
merger tests: 3 passed, 0 failed
wal tests: 3 passed, 0 failed
coordinator build: success (warnings only)
```

---

## Files Modified

1. `crates/coordinator/src/merger.rs` - Fixed sort order in compaction
2. `crates/coordinator/src/bin/coordinator.rs` - Connection pool usage, batch error tracking
3. `crates/storage/src/wal.rs` - Size limit validation and constants

---

## Risk Assessment

| Category | Before | After |
|----------|--------|-------|
| Data Correctness | **HIGH RISK** (wrong search results) | **RESOLVED** |
| Data Loss | **HIGH RISK** (WAL recovery, batch failures) | **RESOLVED** |
| Performance | **MEDIUM** (connection overhead) | **RESOLVED** |

---

## Remaining Known Issues (Lower Priority)

| Bug ID | Severity | Issue |
|--------|----------|-------|
| BUG-HUNT-406 | MEDIUM | Connection pools for removed shards leak (never cleaned up) |
| BUG-HUNT-407 | MEDIUM | `search_batch` processes queries sequentially, not in parallel |
| BUG-HUNT-408 | LOW | Consistency tracker unbounded memory growth between cleanups |

---

## Recommendations

1. **Add integration tests** for batch failure scenarios
2. **Add fuzz testing** for WAL with various entry sizes
3. **Consider implementing** connection pool cleanup on shard removal
4. **Parallelize** search_batch for better performance
