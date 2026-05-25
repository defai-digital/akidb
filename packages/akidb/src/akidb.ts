/**
 * AkiDB — unified public facade for the AkiDB vector database engine.
 *
 * v2.5: delegates all operations to the Rust AkiDbEngine via NAPI-RS.
 *
 * Usage:
 * ```ts
 * const db = new AkiDB({ storagePath: "./data" });
 * const col = db.createCollection({ collectionId: "docs", dimension: 384, metric: "cosine", ... });
 * await db.upsertBatch("docs", records);
 * const manifest = await db.publish("docs", { embeddingModelId: "...", pipelineSignature: "..." });
 * const { results } = await db.search({ collectionId: "docs", queryVector, topK: 10 });
 * db.close();
 * ```
 */

import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import type {
  Collection,
  DistanceMetric,
  Manifest,
  MetadataFilter,
  Record as AkiRecord,
} from "@defai-digital/akidb-contracts";

import type {
  AkiDbEngine as NativeEngine,
  CollectionJs,
  ManifestJs,
  RecordJs,
} from "@defai-digital/akidb-native";

import {
  guardCreateCollection,
  guardUpsertBatch,
  guardSearch,
  guardCollectionId,
  guardChunkIds,
} from "./guard.js";

// ─── Types (inlined from removed legacy modules) ─────────────────────────────

export interface CreateCollectionOptions {
  collectionId: string;
  dimension: number;
  metric: DistanceMetric;
  embeddingModelId: string;
  /** Vector quantization: "fp16" (default) or "sq8". */
  quantization?: "fp16" | "sq8";
  /** HNSW M parameter (max connections per node). Default: 16, range: 4-64. */
  hnswM?: number;
  /** HNSW efConstruction parameter. Default: 200, range: 50-800. */
  hnswEfConstruction?: number;
  /** HNSW efSearch parameter. Default: 100, range: 10-500. */
  hnswEfSearch?: number;
}

export interface SearchResult {
  chunkId: string;
  score: number;
  committed?: boolean;
  explain?: ExplainInfo;
}

export type { MetadataFilter };

export interface SearchResponse {
  results: SearchResult[];
  manifestVersionUsed: number;
}

// ─── Native module loading ───────────────────────────────────────────────────

// Use createRequire to load the CJS NAPI binding synchronously.
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let _nativeBinding: { AkiDbEngine: new (opts: { storagePath: string; disableWal?: boolean }) => NativeEngine } | undefined;

function getNativeBinding() {
  if (_nativeBinding) return _nativeBinding;
  // Walk up from dist/ to find node_modules, or resolve the workspace package.
  const require = createRequire(join(__dirname, "package.json"));
  _nativeBinding = require("@defai-digital/akidb-native") as typeof _nativeBinding;
  return _nativeBinding!;
}

// ─── Options ─────────────────────────────────────────────────────────────────

export interface AkiDBOptions {
  /** Root directory for segment and object storage. */
  storagePath: string;

  /**
   * Disable the Write-Ahead Log for all collections.
   * When false (default), each collection gets a WAL file for crash recovery.
   */
  disableWal?: boolean;
}

export interface PublishOptions {
  embeddingModelId: string;
  pipelineSignature: string;
}

export interface ExplainInfo {
  vectorScore?: number;
  bm25Score?: number;
  rrfScore?: number;
  vectorRank?: number;
  bm25Rank?: number;
  chunkPreview?: string;
  matchedTerms: string[];
}

export interface SearchOptions {
  collectionId: string;
  queryVector: Float32Array;
  topK: number;
  filters?: MetadataFilter;
  manifestVersion?: number;
  /** Include uncommitted (buffered) records in results. Default: true. */
  includeUncommitted?: boolean;
  /** Search mode: "vector" (default), "keyword", or "hybrid". */
  mode?: "vector" | "keyword" | "hybrid";
  /** Query text for keyword/hybrid search. */
  queryText?: string;
  /** Weight for vector results in hybrid RRF fusion. Default: 1.0. */
  vectorWeight?: number;
  /** Weight for keyword results in hybrid RRF fusion. Default: 1.0. */
  keywordWeight?: number;
  /** When true, include per-result scoring breakdown. */
  explain?: boolean;
  /** Per-query ef_search override (range: 10-500). Overrides collection default. */
  efSearch?: number;
}

// ─── AkiDB ──────────────────────────────────────────────────────────────────

export class AkiDB {
  private readonly engine: NativeEngine;

  constructor(opts: AkiDBOptions) {
    const { AkiDbEngine } = getNativeBinding();
    this.engine = new AkiDbEngine({
      storagePath: opts.storagePath,
      disableWal: opts.disableWal,
    });
  }

  // ─── Collection Management ─────────────────────────────────────────────

  createCollection(opts: CreateCollectionOptions): Collection {
    guardCreateCollection(opts);
    const c = this.engine.createCollection({
      collectionId: opts.collectionId,
      dimension: opts.dimension,
      metric: opts.metric,
      embeddingModelId: opts.embeddingModelId,
      quantization: opts.quantization,
      hnswM: opts.hnswM,
      hnswEfConstruction: opts.hnswEfConstruction,
      hnswEfSearch: opts.hnswEfSearch,
    });
    return collectionFromNative(c);
  }

  getCollection(collectionId: string): Collection {
    guardCollectionId("getCollection", collectionId);
    const c = this.engine.getCollection(collectionId);
    if (!c) {
      throw new Error(`Collection "${collectionId}" not found`);
    }
    return collectionFromNative(c);
  }

  listCollections(): Collection[] {
    return this.engine.listCollections().map(collectionFromNative);
  }

  deleteCollection(collectionId: string): void {
    guardCollectionId("deleteCollection", collectionId);
    this.engine.deleteCollection(collectionId);
  }

  // ─── Write ─────────────────────────────────────────────────────────────

  async upsertBatch(
    collectionId: string,
    records: AkiRecord[],
  ): Promise<{ segmentIds: string[] }> {
    guardUpsertBatch(collectionId, records);
    const nativeRecords: RecordJs[] = records.map((record) => ({
      chunkId: record.chunk_id,
      docId: record.doc_id,
      // record.vector is number[] (validated by Zod); pass it directly to avoid
      // an unnecessary Array.from copy on every upsert call.
      vector: record.vector as number[],
      metadataJson: JSON.stringify(record.metadata, upsertJsonReplacer),
      chunkText: record.chunk_text,
    }));
    const result = this.engine.upsertBatch(collectionId, nativeRecords);
    return { segmentIds: result.segmentIds };
  }

  async flushWrites(collectionId: string): Promise<string[]> {
    guardCollectionId("flushWrites", collectionId);
    return this.engine.flushWrites(collectionId);
  }

  // ─── Manifest ──────────────────────────────────────────────────────────

  async publish(
    collectionId: string,
    opts: PublishOptions,
  ): Promise<Manifest> {
    guardCollectionId("publish", collectionId);
    const m = this.engine.autoPublish(
      collectionId,
      opts.embeddingModelId,
      opts.pipelineSignature,
    );
    return manifestFromNative(m);
  }

  // ─── Search ────────────────────────────────────────────────────────────

  async search(opts: SearchOptions): Promise<SearchResponse> {
    guardSearch(opts);
    const response = this.engine.search({
      collectionId: opts.collectionId,
      queryVector: Array.from(opts.queryVector),
      topK: opts.topK,
      filtersJson: opts.filters ? JSON.stringify(opts.filters) : undefined,
      manifestVersion: opts.manifestVersion,
      includeUncommitted: opts.includeUncommitted,
      mode: opts.mode,
      queryText: opts.queryText,
      vectorWeight: opts.vectorWeight,
      keywordWeight: opts.keywordWeight,
      explain: opts.explain,
      efSearch: opts.efSearch,
    });

    const results: SearchResult[] = response.results.map((r) => ({
      chunkId: r.chunkId,
      score: r.score,
      committed: r.committed,
      explain: r.explain,
    }));

    return {
      results,
      manifestVersionUsed: response.manifestVersionUsed,
    };
  }

  // ─── Rollback ──────────────────────────────────────────────────────────

  rollback(collectionId: string, manifestId: string): Manifest {
    guardCollectionId("rollback", collectionId);
    const m = this.engine.rollback(collectionId, manifestId);
    return manifestFromNative(m);
  }

  // ─── Compaction ────────────────────────────────────────────────────────

  async compact(collectionId: string): Promise<Manifest> {
    guardCollectionId("compact", collectionId);
    const result = this.engine.compact(collectionId);
    return manifestFromNative(result.manifest);
  }

  // ─── Tombstones ────────────────────────────────────────────────────────

  deleteChunks(
    collectionId: string,
    chunkIds: string[],
    reason: "file_deleted" | "file_updated" | "manual_revoke" = "manual_revoke",
  ): void {
    guardCollectionId("deleteChunks", collectionId);
    guardChunkIds("deleteChunks", chunkIds);
    this.engine.deleteChunks(collectionId, chunkIds, reason);
  }

  // ─── Introspection ─────────────────────────────────────────────────────

  getStorageSizeBytes(): number {
    return this.engine.getStorageSizeBytes();
  }

  getTombstoneCount(collectionId: string): number {
    guardCollectionId("getTombstoneCount", collectionId);
    return this.engine.getTombstoneCount(collectionId);
  }

  getSegmentCount(collectionId: string): number {
    guardCollectionId("getSegmentCount", collectionId);
    return this.engine.getSegmentCount(collectionId);
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────

  close(): void {
    this.engine.close();
  }
}

// ─── Conversion helpers (NAPI camelCase → contracts snake_case) ──────────────

function collectionFromNative(c: CollectionJs): Collection {
  return {
    collection_id: c.collectionId,
    dimension: c.dimension,
    metric: c.metric as Collection["metric"],
    embedding_model_id: c.embeddingModelId,
    schema_version: c.schemaVersion,
    created_at: c.createdAt,
    deleted_at: c.deletedAt ?? null,
    quantization: (c.quantization ?? "fp16") as Collection["quantization"],
    hnsw_m: c.hnswM ?? 16,
    hnsw_ef_construction: c.hnswEfConstruction ?? 200,
    hnsw_ef_search: c.hnswEfSearch ?? 100,
  };
}

function manifestFromNative(m: ManifestJs): Manifest {
  return {
    manifest_id: m.manifestId,
    collection_id: m.collectionId,
    version: m.version,
    segment_ids: m.segmentIds,
    tombstone_ids: m.tombstoneIds,
    embedding_model_id: m.embeddingModelId,
    pipeline_signature: m.pipelineSignature,
    created_at: m.createdAt,
    checksum: m.checksum,
  };
}

function upsertJsonReplacer(key: string, value: unknown): unknown {
  // Fast path: avoid per-record object/vector remapping for normal number[] vectors.
  // If a typed array is passed at runtime, convert it to a plain array for stable JSON.
  if (key === "vector" && isNumericArrayView(value)) {
    return Array.from(value);
  }
  return value;
}

function isNumericArrayView(value: unknown): value is ArrayLike<number> {
  if (!ArrayBuffer.isView(value) || value instanceof DataView) return false;
  return typeof value === "object" && value !== null && "length" in value;
}
