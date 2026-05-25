# AkiDB

AkiDB is an embedded local retrieval engine for private AI systems. It provides
vector, keyword, and hybrid search with local segment storage, SQLite metadata,
write-ahead logging, manifests, and compaction.

This repository is the open-source AkiDB engine. It is separate from AX Fabric,
which is a closed-source commercial runtime built on top of AkiDB.

## Packages

- `packages/akidb-native`: Rust N-API engine.
- `packages/akidb`: TypeScript wrapper.
- `packages/akidb-py`: optional Python binding.
- `packages/contracts`: AkiDB-only schemas and TypeScript types.

## License

MIT. See [LICENSE](./LICENSE).

## Development

```bash
pnpm install
pnpm build
pnpm test
cd packages/akidb-native && cargo test
```
