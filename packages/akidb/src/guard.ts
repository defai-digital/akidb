/**
 * Guards — validate all inputs at the TypeScript boundary before they cross into Rust.
 *
 * Catches shape errors early with clear messages instead of letting them surface
 * as opaque NAPI panics or Rust assertion failures.
 */

import { AxFabricError } from "@defai-digital/akidb-contracts";
import type { Record as AkiRecord } from "@defai-digital/akidb-contracts";
import type { CreateCollectionOptions, SearchOptions } from "./akidb.js";

// ─── Collection ───────────────────────────────────────────────────────────────

export function guardCreateCollection(opts: CreateCollectionOptions): void {
  if (!opts.collectionId || opts.collectionId.trim().length === 0) {
    throw new AxFabricError("QUERY_ERROR", "createCollection: collectionId must be a non-empty string");
  }
  if (!Number.isInteger(opts.dimension) || opts.dimension <= 0) {
    throw new AxFabricError("QUERY_ERROR", `createCollection: dimension must be a positive integer, got ${String(opts.dimension)}`);
  }
  if (!opts.embeddingModelId || opts.embeddingModelId.trim().length === 0) {
    throw new AxFabricError("QUERY_ERROR", "createCollection: embeddingModelId must be a non-empty string");
  }
  if (opts.hnswM !== undefined && (opts.hnswM < 4 || opts.hnswM > 64)) {
    throw new AxFabricError("QUERY_ERROR", `createCollection: hnswM must be 4–64, got ${String(opts.hnswM)}`);
  }
  if (opts.hnswEfConstruction !== undefined && (opts.hnswEfConstruction < 50 || opts.hnswEfConstruction > 800)) {
    throw new AxFabricError("QUERY_ERROR", `createCollection: hnswEfConstruction must be 50–800, got ${String(opts.hnswEfConstruction)}`);
  }
  if (opts.hnswEfSearch !== undefined && (opts.hnswEfSearch < 10 || opts.hnswEfSearch > 500)) {
    throw new AxFabricError("QUERY_ERROR", `createCollection: hnswEfSearch must be 10–500, got ${String(opts.hnswEfSearch)}`);
  }
}

// ─── Write ────────────────────────────────────────────────────────────────────

export function guardUpsertBatch(
  collectionId: string,
  records: AkiRecord[],
  collectionDimension?: number,
): void {
  guardCollectionId("upsertBatch", collectionId);
  if (records.length === 0) return;

  for (let i = 0; i < records.length; i++) {
    const r = records[i]!;
    if (!r.chunk_id || !r.doc_id || !r.doc_version || !r.chunk_hash || !r.pipeline_signature) {
      throw new AxFabricError(
        "QUERY_ERROR",
        `upsertBatch: record[${String(i)}] missing required field(s): chunk_id, doc_id, doc_version, chunk_hash, pipeline_signature`,
      );
    }
    if (!Array.isArray(r.vector) || r.vector.length === 0) {
      throw new AxFabricError("QUERY_ERROR", `upsertBatch: record[${String(i)}] has an empty or missing vector`);
    }
    if (collectionDimension !== undefined && r.vector.length !== collectionDimension) {
      throw new AxFabricError(
        "QUERY_ERROR",
        `upsertBatch: record[${String(i)}] vector length ${String(r.vector.length)} does not match collection dimension ${String(collectionDimension)}`,
      );
    }
  }
}

// ─── Search ───────────────────────────────────────────────────────────────────

export function guardSearch(opts: SearchOptions): void {
  guardCollectionId("search", opts.collectionId);

  if (!Number.isInteger(opts.topK) || opts.topK <= 0) {
    throw new AxFabricError("QUERY_ERROR", `search: topK must be a positive integer, got ${String(opts.topK)}`);
  }

  const mode = opts.mode ?? "vector";

  if (mode === "vector" || mode === "hybrid") {
    if (!opts.queryVector || opts.queryVector.length === 0) {
      throw new AxFabricError("QUERY_ERROR", `search: mode "${mode}" requires a non-empty queryVector`);
    }
  }
  if (mode === "keyword" || mode === "hybrid") {
    if (!opts.queryText || opts.queryText.trim().length === 0) {
      throw new AxFabricError("QUERY_ERROR", `search: mode "${mode}" requires a non-empty queryText`);
    }
  }

  if (opts.efSearch !== undefined && (opts.efSearch < 10 || opts.efSearch > 500)) {
    throw new AxFabricError("QUERY_ERROR", `search: efSearch must be 10–500, got ${String(opts.efSearch)}`);
  }
}

// ─── Shared ───────────────────────────────────────────────────────────────────

export function guardCollectionId(operation: string, collectionId: string): void {
  if (!collectionId || collectionId.trim().length === 0) {
    throw new AxFabricError("QUERY_ERROR", `${operation}: collectionId must be a non-empty string`);
  }
}

export function guardChunkIds(operation: string, chunkIds: string[]): void {
  if (!Array.isArray(chunkIds) || chunkIds.length === 0) {
    throw new AxFabricError("QUERY_ERROR", `${operation}: chunkIds must be a non-empty array`);
  }
  for (let i = 0; i < chunkIds.length; i++) {
    if (!chunkIds[i] || chunkIds[i]!.trim().length === 0) {
      throw new AxFabricError("QUERY_ERROR", `${operation}: chunkIds[${String(i)}] must be a non-empty string`);
    }
  }
}
