import { describe, expect, it } from "vitest";

import {
  AxFabricError,
  CollectionSchema,
  ErrorCodeSchema,
  ManifestSchema,
  MetadataFilterSchema,
  RecordMetadataSchema,
  RecordSchema,
  SegmentMetadataSchema,
  TombstoneSchema,
} from "./index.js";

describe("RecordMetadataSchema", () => {
  const valid = {
    source_uri: "/path/to/file.txt",
    content_type: "txt",
    page_range: null,
    offset: 0,
    table_ref: null,
    chunk_label: "text",
    created_at: new Date().toISOString(),
  };

  it("accepts valid metadata", () => {
    expect(RecordMetadataSchema.parse(valid).source_uri).toBe("/path/to/file.txt");
  });

  it("rejects invalid content type", () => {
    expect(() => RecordMetadataSchema.parse({ ...valid, content_type: "mp4" })).toThrow();
  });

  it("accepts object source provenance metadata", () => {
    const result = RecordMetadataSchema.parse({
      ...valid,
      source_uri: "s3://bucket/key",
      object_source: {
        uri: "s3://bucket/key",
        bucket: "bucket",
        key: "key",
        sha256: "abc123",
        size_bytes: 42,
      },
    });

    expect(result.object_source?.bucket).toBe("bucket");
  });
});

describe("RecordSchema", () => {
  const metadata = {
    source_uri: "/file.txt",
    content_type: "txt",
    page_range: null,
    offset: 0,
    table_ref: null,
    chunk_label: "text",
    created_at: new Date().toISOString(),
  };

  it("accepts a valid record", () => {
    const result = RecordSchema.parse({
      chunk_id: "chunk-1",
      doc_id: "doc-1",
      doc_version: "v1",
      chunk_hash: "sha256-abc",
      pipeline_signature: "sig-1",
      embedding_model_id: "text-embedding-3-small",
      vector: [0.1, 0.2, 0.3],
      metadata,
      chunk_text: "hello",
    });

    expect(result.chunk_text).toBe("hello");
  });
});

describe("MetadataFilterSchema", () => {
  it("accepts scalar and operator filter values", () => {
    expect(() =>
      MetadataFilterSchema.parse({
        source_uri: "/file.txt",
        created_at: { gte: "2026-01-01T00:00:00.000Z" },
      }),
    ).not.toThrow();
  });
});

describe("AkiDB schemas", () => {
  it("accepts collection metadata", () => {
    expect(() =>
      CollectionSchema.parse({
        collection_id: "docs",
        dimension: 384,
        metric: "cosine",
        embedding_model_id: "text-embedding-3-small",
        schema_version: "1",
        created_at: new Date().toISOString(),
        deleted_at: null,
        quantization: "fp16",
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_search: 100,
      }),
    ).not.toThrow();
  });

  it("accepts manifest metadata", () => {
    expect(() =>
      ManifestSchema.parse({
        manifest_id: "manifest-1",
        collection_id: "docs",
        version: 1,
        segment_ids: ["segment-1"],
        tombstone_ids: [],
        embedding_model_id: "text-embedding-3-small",
        pipeline_signature: "sig-1",
        created_at: new Date().toISOString(),
        checksum: "checksum-1",
      }),
    ).not.toThrow();
  });

  it("accepts segment and tombstone metadata", () => {
    expect(() =>
      SegmentMetadataSchema.parse({
        segment_id: "segment-1",
        collection_id: "docs",
        record_count: 1,
        dimension: 384,
        size_bytes: 1024,
        checksum: "checksum-1",
        status: "ready",
        storage_path: "/tmp/segment",
        created_at: new Date().toISOString(),
      }),
    ).not.toThrow();

    expect(() =>
      TombstoneSchema.parse({
        chunk_id: "chunk-1",
        deleted_at: new Date().toISOString(),
        reason_code: "manual_revoke",
      }),
    ).not.toThrow();
  });
});

describe("AxFabricError", () => {
  it("captures an error code and message", () => {
    const err = new AxFabricError("QUERY_ERROR", "bad query");
    expect(err.code).toBe("QUERY_ERROR");
    expect(err.message).toBe("bad query");
    expect(ErrorCodeSchema.parse("QUERY_ERROR")).toBe("QUERY_ERROR");
  });
});
