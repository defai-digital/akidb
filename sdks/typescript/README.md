# AkiDB TypeScript SDK

A typed TypeScript client for [AkiDB](../../README.md) — a Mac-native retrieval
memory engine for private AI agents.

## Install

```bash
npm install   # from sdks/typescript
npm run build
```

## Usage

```ts
import { AkiDBClient } from '@akidb/client';

const client = new AkiDBClient({ target: 'localhost:50051' });

await client.insert('doc-1', embedding, { text: 'the source text' });

const result = await client.textSearch('why does token refresh fail?', {
  topK: 5,
  hybrid: true,
  rerank: true,
  diversity: true,
  pack: true,
  tokenBudget: 1024,
});
for (const hit of result.hits) console.log(hit.id, hit.score);
console.log(result.contextPack);
```

The proto is loaded at runtime from `proto/akidb.proto` (no codegen step). For
unit testing, inject a `rawClient` via the constructor to exercise the wrapper
without a running server.

## Tests

```bash
npm test
```
