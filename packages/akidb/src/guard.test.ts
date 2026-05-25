/**
 * Unit tests for NAPI boundary guard functions.
 *
 * Guards catch invalid inputs before they cross into Rust, producing
 * clear TypeScript errors instead of opaque NAPI panics.
 */

import { describe, expect, it } from "vitest";
import { AxFabricError, type Record as ContractRecord } from "@defai-digital/akidb-contracts";

import {
  guardCreateCollection,
  guardUpsertBatch,
  guardSearch,
  guardCollectionId,
  guardChunkIds,
} from "./guard.js";
import type { CreateCollectionOptions, SearchOptions } from "./akidb.js";

// ─── guardCollectionId ────────────────────────────────────────────────────────

describe("guardCollectionId", () => {
  it("accepts a valid collection ID", () => {
    expect(() => guardCollectionId("test", "my-collection")).not.toThrow();
  });

  it("throws on empty string", () => {
    expect(() => guardCollectionId("search", "")).toThrowError(AxFabricError);
  });

  it("throws on whitespace-only string", () => {
    expect(() => guardCollectionId("search", "   ")).toThrowError(AxFabricError);
  });

  it("includes operation name in error message", () => {
    expect(() => guardCollectionId("deleteCollection", "")).toThrow("deleteCollection");
  });

  it("has QUERY_ERROR code", () => {
    try {
      guardCollectionId("op", "");
    } catch (e) {
      expect(e).toBeInstanceOf(AxFabricError);
      expect((e as AxFabricError).code).toBe("QUERY_ERROR");
    }
  });
});

// ─── guardChunkIds ────────────────────────────────────────────────────────────

describe("guardChunkIds", () => {
  it("accepts a non-empty array of valid IDs", () => {
    expect(() => guardChunkIds("deleteChunks", ["id-1", "id-2"])).not.toThrow();
  });

  it("throws on empty array", () => {
    expect(() => guardChunkIds("deleteChunks", [])).toThrowError(AxFabricError);
  });

  it("throws when an element is an empty string", () => {
    expect(() => guardChunkIds("deleteChunks", ["id-1", ""])).toThrowError(AxFabricError);
  });

  it("throws when an element is whitespace only", () => {
    expect(() => guardChunkIds("deleteChunks", ["   "])).toThrowError(AxFabricError);
  });

  it("includes the index of the bad element in error message", () => {
    expect(() => guardChunkIds("op", ["good", ""])).toThrow("chunkIds[1]");
  });

  it("includes operation name in error message", () => {
    expect(() => guardChunkIds("rollback", [])).toThrow("rollback");
  });
});

// ─── guardCreateCollection ────────────────────────────────────────────────────

describe("guardCreateCollection", () => {
  const valid: CreateCollectionOptions = {
    collectionId: "my-col",
    dimension: 128,
    metric: "cosine",
    embeddingModelId: "text-embedding-3-small",
  };

  it("accepts valid options", () => {
    expect(() => guardCreateCollection(valid)).not.toThrow();
  });

  it("accepts all optional HNSW params at boundary values", () => {
    expect(() =>
      guardCreateCollection({
        ...valid,
        hnswM: 4,
        hnswEfConstruction: 50,
        hnswEfSearch: 10,
      }),
    ).not.toThrow();

    expect(() =>
      guardCreateCollection({
        ...valid,
        hnswM: 64,
        hnswEfConstruction: 800,
        hnswEfSearch: 500,
      }),
    ).not.toThrow();
  });

  it("throws on empty collectionId", () => {
    expect(() => guardCreateCollection({ ...valid, collectionId: "" })).toThrowError(AxFabricError);
  });

  it("throws on whitespace collectionId", () => {
    expect(() => guardCreateCollection({ ...valid, collectionId: "  " })).toThrowError(AxFabricError);
  });

  it("throws on zero dimension", () => {
    expect(() => guardCreateCollection({ ...valid, dimension: 0 })).toThrowError(AxFabricError);
  });

  it("throws on negative dimension", () => {
    expect(() => guardCreateCollection({ ...valid, dimension: -1 })).toThrowError(AxFabricError);
  });

  it("throws on fractional dimension", () => {
    expect(() => guardCreateCollection({ ...valid, dimension: 1.5 })).toThrowError(AxFabricError);
  });

  it("throws on empty embeddingModelId", () => {
    expect(() => guardCreateCollection({ ...valid, embeddingModelId: "" })).toThrowError(AxFabricError);
  });

  it("throws when hnswM < 4", () => {
    expect(() => guardCreateCollection({ ...valid, hnswM: 3 })).toThrowError(AxFabricError);
  });

  it("throws when hnswM > 64", () => {
    expect(() => guardCreateCollection({ ...valid, hnswM: 65 })).toThrowError(AxFabricError);
  });

  it("throws when hnswEfConstruction < 50", () => {
    expect(() => guardCreateCollection({ ...valid, hnswEfConstruction: 49 })).toThrowError(AxFabricError);
  });

  it("throws when hnswEfConstruction > 800", () => {
    expect(() => guardCreateCollection({ ...valid, hnswEfConstruction: 801 })).toThrowError(AxFabricError);
  });

  it("throws when hnswEfSearch < 10", () => {
    expect(() => guardCreateCollection({ ...valid, hnswEfSearch: 9 })).toThrowError(AxFabricError);
  });

  it("throws when hnswEfSearch > 500", () => {
    expect(() => guardCreateCollection({ ...valid, hnswEfSearch: 501 })).toThrowError(AxFabricError);
  });

  it("does not throw when optional HNSW params are absent", () => {
    expect(() =>
      guardCreateCollection({
        collectionId: "c",
        dimension: 4,
        metric: "cosine",
        embeddingModelId: "m",
      }),
    ).not.toThrow();
  });

  it("has QUERY_ERROR code on all validation failures", () => {
    try {
      guardCreateCollection({ ...valid, collectionId: "" });
    } catch (e) {
      expect((e as AxFabricError).code).toBe("QUERY_ERROR");
    }
  });
});

// ─── guardUpsertBatch ─────────────────────────────────────────────────────────

describe("guardUpsertBatch", () => {
  const makeRecord = (overrides: Partial<ContractRecord> = {}): ContractRecord => ({
    chunk_id: "chunk-1",
    doc_id: "doc-1",
    doc_version: "fp-abc",
    chunk_hash: "sha-abc",
    pipeline_signature: "sig-v1",
    embedding_model_id: "model-v1",
    vector: [0.1, 0.2, 0.3],
    metadata: { source_uri: "/f", content_type: "txt", page_range: null, offset: 0, table_ref: null, created_at: new Date().toISOString() },
    ...overrides,
  });

  it("accepts a valid batch", () => {
    expect(() => guardUpsertBatch("col", [makeRecord()])).not.toThrow();
  });

  it("accepts empty records array (no-op)", () => {
    expect(() => guardUpsertBatch("col", [])).not.toThrow();
  });

  it("throws on invalid collectionId", () => {
    expect(() => guardUpsertBatch("", [makeRecord()])).toThrowError(AxFabricError);
  });

  it("throws on missing chunk_id", () => {
    expect(() => guardUpsertBatch("col", [makeRecord({ chunk_id: "" })])).toThrowError(AxFabricError);
  });

  it("throws on missing doc_id", () => {
    expect(() => guardUpsertBatch("col", [makeRecord({ doc_id: "" })])).toThrowError(AxFabricError);
  });

  it("throws on missing doc_version", () => {
    expect(() => guardUpsertBatch("col", [makeRecord({ doc_version: "" })])).toThrowError(AxFabricError);
  });

  it("throws on missing chunk_hash", () => {
    expect(() => guardUpsertBatch("col", [makeRecord({ chunk_hash: "" })])).toThrowError(AxFabricError);
  });

  it("throws on missing pipeline_signature", () => {
    expect(() => guardUpsertBatch("col", [makeRecord({ pipeline_signature: "" })])).toThrowError(AxFabricError);
  });

  it("throws on empty vector", () => {
    expect(() => guardUpsertBatch("col", [makeRecord({ vector: [] })])).toThrowError(AxFabricError);
  });

  it("throws on dimension mismatch when collectionDimension is provided", () => {
    expect(() =>
      guardUpsertBatch("col", [makeRecord({ vector: [0.1, 0.2, 0.3] })], 4),
    ).toThrowError(AxFabricError);
  });

  it("does not throw on dimension match", () => {
    expect(() =>
      guardUpsertBatch("col", [makeRecord({ vector: [0.1, 0.2, 0.3] })], 3),
    ).not.toThrow();
  });

  it("includes record index in error message for batch failures", () => {
    const records = [makeRecord(), makeRecord({ chunk_id: "" })];
    expect(() => guardUpsertBatch("col", records)).toThrow("record[1]");
  });
});

// ─── guardSearch ──────────────────────────────────────────────────────────────

describe("guardSearch", () => {
  const baseVector: SearchOptions = {
    collectionId: "my-col",
    topK: 10,
    queryVector: new Float32Array([0.1, 0.2, 0.3]),
  };

  const baseKeyword: SearchOptions = {
    collectionId: "my-col",
    topK: 5,
    mode: "keyword",
    queryText: "search query",
    queryVector: new Float32Array([]),
  };

  const baseHybrid: SearchOptions = {
    collectionId: "my-col",
    topK: 5,
    mode: "hybrid",
    queryVector: new Float32Array([0.1, 0.2]),
    queryText: "search query",
  };

  it("accepts valid vector search options", () => {
    expect(() => guardSearch(baseVector)).not.toThrow();
  });

  it("accepts valid keyword search options", () => {
    expect(() => guardSearch(baseKeyword)).not.toThrow();
  });

  it("accepts valid hybrid search options", () => {
    expect(() => guardSearch(baseHybrid)).not.toThrow();
  });

  it("accepts efSearch at boundary values (10 and 500)", () => {
    expect(() => guardSearch({ ...baseVector, efSearch: 10 })).not.toThrow();
    expect(() => guardSearch({ ...baseVector, efSearch: 500 })).not.toThrow();
  });

  it("throws on empty collectionId", () => {
    expect(() => guardSearch({ ...baseVector, collectionId: "" })).toThrowError(AxFabricError);
  });

  it("throws on non-integer topK", () => {
    expect(() => guardSearch({ ...baseVector, topK: 1.5 })).toThrowError(AxFabricError);
  });

  it("throws on zero topK", () => {
    expect(() => guardSearch({ ...baseVector, topK: 0 })).toThrowError(AxFabricError);
  });

  it("throws on negative topK", () => {
    expect(() => guardSearch({ ...baseVector, topK: -1 })).toThrowError(AxFabricError);
  });

  it("throws on vector mode with empty queryVector", () => {
    expect(() =>
      guardSearch({ ...baseVector, mode: "vector", queryVector: new Float32Array([]) }),
    ).toThrowError(AxFabricError);
  });

  it("throws on vector mode with missing queryVector", () => {
    expect(() =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      guardSearch({ collectionId: "col", topK: 5, mode: "vector" } as any),
    ).toThrowError(AxFabricError);
  });

  it("throws on keyword mode with missing queryText", () => {
    expect(() =>
      guardSearch({ collectionId: "col", topK: 5, mode: "keyword", queryVector: new Float32Array([]) }),
    ).toThrowError(AxFabricError);
  });

  it("throws on keyword mode with whitespace-only queryText", () => {
    expect(() =>
      guardSearch({ ...baseKeyword, queryText: "   " }),
    ).toThrowError(AxFabricError);
  });

  it("throws on hybrid mode with empty queryVector", () => {
    expect(() =>
      guardSearch({ ...baseHybrid, queryVector: new Float32Array([]) }),
    ).toThrowError(AxFabricError);
  });

  it("throws on hybrid mode with missing queryText", () => {
    expect(() =>
      guardSearch({ ...baseHybrid, queryText: "" }),
    ).toThrowError(AxFabricError);
  });

  it("throws when efSearch < 10", () => {
    expect(() => guardSearch({ ...baseVector, efSearch: 9 })).toThrowError(AxFabricError);
  });

  it("throws when efSearch > 500", () => {
    expect(() => guardSearch({ ...baseVector, efSearch: 501 })).toThrowError(AxFabricError);
  });

  it("does not throw when efSearch is undefined", () => {
    expect(() => guardSearch({ ...baseVector, efSearch: undefined })).not.toThrow();
  });
});
