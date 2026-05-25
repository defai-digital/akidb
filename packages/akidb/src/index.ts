// ─── Public API ─────────────────────────────────────────────────────────────
//
// v2.5: AkiDB is a thin facade over the Rust AkiDbEngine (NAPI-RS).
// All legacy TypeScript implementation modules (query engine, write path,
// segment builder, metadata store, WAL, compaction, index backends) have
// been removed — the Rust engine handles everything.

export { AkiDB } from "./akidb.js";
export type {
  AkiDBOptions,
  CreateCollectionOptions,
  ExplainInfo,
  MetadataFilter,
  PublishOptions,
  SearchOptions,
  SearchResponse,
  SearchResult,
} from "./akidb.js";

export {
  guardCreateCollection,
  guardUpsertBatch,
  guardSearch,
  guardCollectionId,
  guardChunkIds,
} from "./guard.js";
