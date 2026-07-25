# AkiDB TypeScript SDK

A typed TypeScript client for [AkiDB](../../README.md), a portable retrieval
database for private AI systems.

## Install

```bash
npm install   # from sdks/typescript
npm run build
```

## Usage

```ts
import { AkiDBClient } from '@akidb/client';

const client = new AkiDBClient({
  target: 'localhost:50051',
  timeoutMs: 5000,     // per-call deadline
  maxRetries: 3,       // retries on transient errors, exponential backoff
  tls: true,           // TLS channel (optional rootCerts)
  authToken: '…',      // sent as `authorization: Bearer …`
});

await client.insert('doc-1', embedding, { text: 'the source text' });

const result = await client.textSearch('why does token refresh fail?', {
  topK: 5, hybrid: true, rerank: true, diversity: true, pack: true, tokenBudget: 1024,
});
for (const hit of result.hits) console.log(hit.id, hit.score);
console.log(result.contextPack);

// Agent memory
await client.memoryWrite('m1', embedding, 'remember this', { kind: 'note', conversationId: 'c1' });
const hits = await client.memoryRead(queryEmbedding, { conversationId: 'c1' });
```

## Features

- Per-call **deadlines**, **retry with exponential backoff** on transient codes
  (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`).
- **TLS** + **bearer-token auth**; custom metadata.
- **Typed errors** (`NotFoundError`, `InvalidArgumentError`, `UnavailableError`, …)
  mapped from gRPC status; see `./errors`.
- Typed responses and full RPC coverage: insert, insertBatch, update, get, delete,
  search, searchBatch, textSearch, health, clusterState, plus memoryWrite/Read.
- Proto loaded at runtime via `@grpc/proto-loader` (no codegen step).

For unit testing, inject a `rawClient` via the constructor to exercise the wrapper
without a running server.

## Tests

```bash
npm test
```

Includes a proto-drift test that fails if the vendored `proto/akidb.proto` drifts
from the canonical engine proto (`../check-proto-drift.sh` checks both SDKs). A
live integration test (`src/live.test.ts`) runs only when `AKIDB_SERVER_ADDR` is
set; it is exercised in CI (`.github/workflows/sdks.yml`) against a real server.

Resilience/observability options: `timeoutMs`, `maxRetries`, `backoffMs`
(jittered), `onRetry`, `tls`/`rootCerts`, `authToken`, `metadata`.
