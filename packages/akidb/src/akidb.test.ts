/**
 * AkiDB integration tests — full lifecycle through the public facade.
 *
 * Exercises: create -> ingest -> publish -> search -> delete -> publish -> rollback -> compact
 */

import type { Record as AkiRecord, RecordMetadata } from "@defai-digital/akidb-contracts";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { AkiDB } from "./akidb.js";

// ─── Fixtures ────────────────────────────────────────────────────────────────

const DIM = 4;

function makeMetadata(index: number): RecordMetadata {
  return {
    source_uri: `file://doc-${String(index)}.txt`,
    content_type: "txt",
    page_range: null,
    offset: index,
    table_ref: null,
    created_at: "2026-01-01T00:00:00.000Z",
  };
}

function makeRecord(index: number): AkiRecord {
  return {
    chunk_id: `chunk-${String(index).padStart(3, "0")}`,
    doc_id: `doc-${String(index)}`,
    doc_version: "1.0.0",
    chunk_hash: `hash-${String(index)}`,
    pipeline_signature: "sha256:pipe-v1",
    embedding_model_id: "test-model",
    vector: Array.from(
      { length: DIM },
      (_, d) => (index + 1) * 0.1 + d * 0.01,
    ),
    metadata: makeMetadata(index),
  };
}

function makeQueryVector(seed: number): Float32Array {
  return new Float32Array(
    Array.from({ length: DIM }, (_, d) => seed * 0.1 + d * 0.01),
  );
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("AkiDB integration", () => {
  let tmpDir: string;
  let db: AkiDB;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "akidb-integration-"));
    db = new AkiDB({
      storagePath: join(tmpDir, "storage"),
    });
  });

  afterEach(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  // ─── Full lifecycle ─────────────────────────────────────────────────────

  it("supports full lifecycle: create -> ingest -> publish -> search -> delete -> publish -> rollback", async () => {
    // 1. Create collection.
    const col = db.createCollection({
      collectionId: "lifecycle-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });
    expect(col.collection_id).toBe("lifecycle-test");
    expect(col.dimension).toBe(DIM);

    // 2. Ingest records.
    const records = Array.from({ length: 10 }, (_, i) => makeRecord(i));
    const upsertResult = await db.upsertBatch("lifecycle-test", records);
    // Records are buffered (threshold 100), so no segment yet.
    expect(upsertResult.segmentIds).toHaveLength(0);

    // 3. Publish manifest (auto-flushes buffered records).
    const manifest0 = await db.publish("lifecycle-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });
    expect(manifest0.version).toBe(0);
    expect(manifest0.segment_ids.length).toBeGreaterThan(0);
    expect(manifest0.tombstone_ids).toEqual([]);

    // 4. Search — should find results.
    const searchResult = await db.search({
      collectionId: "lifecycle-test",
      queryVector: makeQueryVector(5),
      topK: 5,
    });
    expect(searchResult.manifestVersionUsed).toBe(0);
    expect(searchResult.results.length).toBe(5);

    // Verify descending score order.
    for (let i = 1; i < searchResult.results.length; i++) {
      expect(searchResult.results[i - 1]!.score).toBeGreaterThanOrEqual(
        searchResult.results[i]!.score,
      );
    }

    // 5. Delete some chunks.
    db.deleteChunks("lifecycle-test", ["chunk-002", "chunk-007"]);

    // 6. Publish new manifest with tombstones.
    const manifest1 = await db.publish("lifecycle-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });
    expect(manifest1.version).toBe(1);
    expect(manifest1.tombstone_ids).toContain("chunk-002");
    expect(manifest1.tombstone_ids).toContain("chunk-007");

    // 7. Search again — deleted chunks should be excluded.
    const searchResult2 = await db.search({
      collectionId: "lifecycle-test",
      queryVector: makeQueryVector(5),
      topK: 10,
    });
    const returnedIds2 = searchResult2.results.map((r) => r.chunkId);
    expect(returnedIds2).not.toContain("chunk-002");
    expect(returnedIds2).not.toContain("chunk-007");
    expect(searchResult2.results.length).toBe(8);

    // 8. Rollback to manifest v0.
    const rollbackManifest = db.rollback(
      "lifecycle-test",
      manifest0.manifest_id,
    );
    expect(rollbackManifest.version).toBe(2);
    expect(rollbackManifest.segment_ids).toEqual(manifest0.segment_ids);
    expect(rollbackManifest.tombstone_ids).toEqual(manifest0.tombstone_ids);

    // 9. Search after rollback — previously deleted chunks should reappear.
    const searchResult3 = await db.search({
      collectionId: "lifecycle-test",
      queryVector: makeQueryVector(5),
      topK: 10,
    });
    expect(searchResult3.manifestVersionUsed).toBe(2);
    expect(searchResult3.results.length).toBe(10);
  });

  // ─── Compaction ─────────────────────────────────────────────────────────

  it("compacts segments and applies tombstones", async () => {
    db.createCollection({
      collectionId: "compact-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    // Ingest two batches and flush between them to create separate segments.
    const batch1 = Array.from({ length: 5 }, (_, i) => makeRecord(i));
    const batch2 = Array.from({ length: 5 }, (_, i) => makeRecord(i + 5));

    await db.upsertBatch("compact-test", batch1);
    await db.flushWrites("compact-test");
    await db.upsertBatch("compact-test", batch2);
    await db.flushWrites("compact-test");

    const manifest = await db.publish("compact-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });

    // Should have 2 segments.
    expect(manifest.segment_ids.length).toBe(2);

    // Delete a chunk and publish.
    db.deleteChunks("compact-test", ["chunk-003"]);
    const manifest2 = await db.publish("compact-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });
    expect(manifest2.tombstone_ids).toContain("chunk-003");

    // Compact.
    const compactedManifest = await db.compact("compact-test");

    // Should have 1 segment with 9 records.
    expect(compactedManifest.segment_ids.length).toBe(1);
    expect(compactedManifest.tombstone_ids).toEqual([]);

    // Search should return 9 results (chunk-003 excluded).
    const searchResult = await db.search({
      collectionId: "compact-test",
      queryVector: makeQueryVector(5),
      topK: 20,
    });
    expect(searchResult.results.length).toBe(9);
    expect(searchResult.results.map((r) => r.chunkId)).not.toContain(
      "chunk-003",
    );
  });

  it("compacts pending tombstones even before they are published", async () => {
    db.createCollection({
      collectionId: "compact-pending-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    const batch1 = Array.from({ length: 5 }, (_, i) => makeRecord(i));
    const batch2 = Array.from({ length: 5 }, (_, i) => makeRecord(i + 5));

    await db.upsertBatch("compact-pending-test", batch1);
    await db.flushWrites("compact-pending-test");
    await db.upsertBatch("compact-pending-test", batch2);
    await db.flushWrites("compact-pending-test");

    const manifest = await db.publish("compact-pending-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });
    expect(manifest.segment_ids.length).toBe(2);

    db.deleteChunks("compact-pending-test", ["chunk-003"]);

    const compactedManifest = await db.compact("compact-pending-test");
    expect(compactedManifest.segment_ids.length).toBe(1);
    expect(compactedManifest.tombstone_ids).toEqual([]);

    const latestResults = await db.search({
      collectionId: "compact-pending-test",
      queryVector: makeQueryVector(5),
      topK: 20,
    });
    expect(latestResults.results).toHaveLength(9);
    expect(latestResults.results.map((r) => r.chunkId)).not.toContain("chunk-003");

    const historicalResults = await db.search({
      collectionId: "compact-pending-test",
      queryVector: makeQueryVector(5),
      topK: 20,
      manifestVersion: 0,
    });
    expect(historicalResults.results.map((r) => r.chunkId)).toContain("chunk-003");
  });

  // ─── Collection management ──────────────────────────────────────────────

  it("creates and lists collections", () => {
    db.createCollection({
      collectionId: "col-a",
      dimension: 128,
      metric: "cosine",
      embeddingModelId: "model-a",

    });
    db.createCollection({
      collectionId: "col-b",
      dimension: 256,
      metric: "l2",
      embeddingModelId: "model-b",

    });

    const list = db.listCollections();
    expect(list).toHaveLength(2);
    expect(list.map((c) => c.collection_id).sort()).toEqual([
      "col-a",
      "col-b",
    ]);
  });

  it("soft-deletes a collection", () => {
    db.createCollection({
      collectionId: "to-delete",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    db.deleteCollection("to-delete");
    expect(db.listCollections()).toHaveLength(0);
  });

  it("throws when creating a duplicate collection", () => {
    db.createCollection({
      collectionId: "dupe",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    expect(() =>
      db.createCollection({
        collectionId: "dupe",
        dimension: DIM,
        metric: "cosine",
        embeddingModelId: "test-model",
  
      }),
    ).toThrow("already exists");
  });

  // ─── Search edge cases ──────────────────────────────────────────────────

  it("throws when searching without a published manifest", async () => {
    db.createCollection({
      collectionId: "no-manifest",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    await expect(
      db.search({
        collectionId: "no-manifest",
        queryVector: makeQueryVector(1),
        topK: 5,
      }),
    ).rejects.toThrow(/manifest/i);
  });

  it("supports searching a specific manifest version", async () => {
    db.createCollection({
      collectionId: "version-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    const records = Array.from({ length: 5 }, (_, i) => makeRecord(i));
    await db.upsertBatch("version-test", records);

    const manifest0 = await db.publish("version-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });

    // Delete a chunk and publish v1.
    db.deleteChunks("version-test", ["chunk-001"]);
    await db.publish("version-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });

    // Search v0 — should include chunk-001.
    const v0Results = await db.search({
      collectionId: "version-test",
      queryVector: makeQueryVector(1),
      topK: 10,
      manifestVersion: 0,
    });
    expect(v0Results.results.map((r) => r.chunkId)).toContain("chunk-001");

    // Search v1 — should exclude chunk-001.
    const v1Results = await db.search({
      collectionId: "version-test",
      queryVector: makeQueryVector(1),
      topK: 10,
      manifestVersion: 1,
    });
    expect(v1Results.results.map((r) => r.chunkId)).not.toContain("chunk-001");
  });

  // ─── Metadata filtering ────────────────────────────────────────────────

  it("supports metadata filtering in search", async () => {
    db.createCollection({
      collectionId: "filter-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    const records: AkiRecord[] = [
      {
        ...makeRecord(0),
        chunk_id: "pdf-0",
        metadata: { ...makeMetadata(0), content_type: "pdf" },
      },
      {
        ...makeRecord(1),
        chunk_id: "txt-1",
        metadata: { ...makeMetadata(1), content_type: "txt" },
      },
      {
        ...makeRecord(2),
        chunk_id: "pdf-2",
        metadata: { ...makeMetadata(2), content_type: "pdf" },
      },
    ];

    await db.upsertBatch("filter-test", records);
    await db.publish("filter-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });

    const result = await db.search({
      collectionId: "filter-test",
      queryVector: makeQueryVector(1),
      topK: 10,
      filters: { content_type: "pdf" },
    });

    expect(result.results.length).toBe(2);
    expect(
      result.results.every((r) => r.chunkId.startsWith("pdf-")),
    ).toBe(true);
  });

  it("supports operator filters ($gt/$lt) in search", async () => {
    db.createCollection({
      collectionId: "filter-ops-range",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    const records: AkiRecord[] = [
      {
        ...makeRecord(0),
        chunk_id: "off-1",
        metadata: { ...makeMetadata(0), offset: 1, content_type: "txt" },
      },
      {
        ...makeRecord(1),
        chunk_id: "off-5",
        metadata: { ...makeMetadata(1), offset: 5, content_type: "txt" },
      },
      {
        ...makeRecord(2),
        chunk_id: "off-9",
        metadata: { ...makeMetadata(2), offset: 9, content_type: "txt" },
      },
    ];

    await db.upsertBatch("filter-ops-range", records);
    await db.publish("filter-ops-range", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });

    const result = await db.search({
      collectionId: "filter-ops-range",
      queryVector: makeQueryVector(1),
      topK: 10,
      filters: { offset: { $gt: 2, $lt: 8 } },
    });

    const ids = result.results.map((r) => r.chunkId);
    expect(ids).toEqual(["off-5"]);
  });

  it("supports set operators ($in/$nin) in search", async () => {
    db.createCollection({
      collectionId: "filter-ops-set",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    const records: AkiRecord[] = [
      {
        ...makeRecord(0),
        chunk_id: "type-pdf",
        metadata: { ...makeMetadata(0), content_type: "pdf" },
      },
      {
        ...makeRecord(1),
        chunk_id: "type-txt",
        metadata: { ...makeMetadata(1), content_type: "txt" },
      },
      {
        ...makeRecord(2),
        chunk_id: "type-csv",
        metadata: { ...makeMetadata(2), content_type: "csv" },
      },
    ];

    await db.upsertBatch("filter-ops-set", records);
    await db.publish("filter-ops-set", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });

    const result = await db.search({
      collectionId: "filter-ops-set",
      queryVector: makeQueryVector(1),
      topK: 10,
      filters: { content_type: { $in: ["pdf", "txt"], $nin: ["txt"] } },
    });

    const ids = result.results.map((r) => r.chunkId);
    expect(ids).toEqual(["type-pdf"]);
  });

  // ─── Multiple upserts and publishes ─────────────────────────────────────

  // ─── Search modes (v1.4 coverage) ──────────────────────────────────────

  it("supports keyword search mode (BM25 / FTS5)", async () => {
    db.createCollection({
      collectionId: "keyword-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    const records: AkiRecord[] = Array.from({ length: 5 }, (_, i) => ({
      ...makeRecord(i),
      chunk_id: `kw-chunk-${String(i)}`,
      chunk_text: i === 2 ? "authentication token expiry policy" : `unrelated text about topic ${String(i)}`,
    }));

    await db.upsertBatch("keyword-test", records);
    await db.publish("keyword-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });

    const result = await db.search({
      collectionId: "keyword-test",
      queryVector: new Float32Array(DIM),
      queryText: "authentication token",
      topK: 5,
      mode: "keyword",
    });

    expect(result.results.length).toBeGreaterThan(0);
    // The chunk with matching text should appear in results.
    const ids = result.results.map((r) => r.chunkId);
    expect(ids).toContain("kw-chunk-2");
  });

  it("supports hybrid search mode (RRF fusion of vector + keyword)", async () => {
    db.createCollection({
      collectionId: "hybrid-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    const records: AkiRecord[] = Array.from({ length: 5 }, (_, i) => ({
      ...makeRecord(i),
      chunk_id: `hy-chunk-${String(i)}`,
      chunk_text: i === 3 ? "hybrid retrieval reciprocal rank fusion" : `document chunk number ${String(i)}`,
    }));

    await db.upsertBatch("hybrid-test", records);
    await db.publish("hybrid-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });

    const result = await db.search({
      collectionId: "hybrid-test",
      queryVector: makeQueryVector(3),
      queryText: "hybrid retrieval",
      topK: 5,
      mode: "hybrid",
    });

    expect(result.results.length).toBeGreaterThan(0);
    // Scores must be non-negative (RRF scores are always positive).
    for (const r of result.results) {
      expect(r.score).toBeGreaterThanOrEqual(0);
    }
  });

  it("rollback restores keyword search state", async () => {
    db.createCollection({
      collectionId: "rollback-keyword-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    await db.upsertBatch("rollback-keyword-test", [
      {
        ...makeRecord(0),
        chunk_id: "rb-chunk-0",
        chunk_text: "legacy xml parser guidance",
      },
    ]);
    const manifest0 = await db.publish("rollback-keyword-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });

    db.deleteChunks("rollback-keyword-test", ["rb-chunk-0"]);
    await db.publish("rollback-keyword-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v2",
    });

    const afterDelete = await db.search({
      collectionId: "rollback-keyword-test",
      queryVector: new Float32Array(DIM),
      queryText: "xml parser",
      topK: 5,
      mode: "keyword",
    });
    expect(afterDelete.results.map((r) => r.chunkId)).not.toContain("rb-chunk-0");

    db.rollback("rollback-keyword-test", manifest0.manifest_id);

    const afterRollbackKeyword = await db.search({
      collectionId: "rollback-keyword-test",
      queryVector: new Float32Array(DIM),
      queryText: "xml parser",
      topK: 5,
      mode: "keyword",
    });
    expect(afterRollbackKeyword.results.map((r) => r.chunkId)).toContain("rb-chunk-0");

    const afterRollbackHybrid = await db.search({
      collectionId: "rollback-keyword-test",
      queryVector: makeQueryVector(0),
      queryText: "xml parser",
      topK: 5,
      mode: "hybrid",
    });
    expect(afterRollbackHybrid.results.map((r) => r.chunkId)).toContain("rb-chunk-0");
  });

  it("returns explain info when explain flag is set", async () => {
    db.createCollection({
      collectionId: "explain-test",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    const records: AkiRecord[] = Array.from({ length: 3 }, (_, i) => ({
      ...makeRecord(i),
      chunk_id: `ex-chunk-${String(i)}`,
      chunk_text: `explain test chunk number ${String(i)} with searchable content`,
    }));

    await db.upsertBatch("explain-test", records);
    await db.publish("explain-test", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });

    // Explain on hybrid search should populate all scoring breakdown fields.
    const result = await db.search({
      collectionId: "explain-test",
      queryVector: makeQueryVector(1),
      queryText: "searchable content",
      topK: 3,
      mode: "hybrid",
      explain: true,
    });

    expect(result.results.length).toBeGreaterThan(0);
    // At least one result should have explain populated.
    const withExplain = result.results.filter((r) => r.explain !== undefined && r.explain !== null);
    expect(withExplain.length).toBeGreaterThan(0);

    // The explain object should carry score breakdown fields.
    const first = withExplain[0]!.explain!;
    // rrfScore present for hybrid results that contributed to fusion.
    expect(typeof first.rrfScore === "number" || first.rrfScore === undefined).toBe(true);
    expect(Array.isArray(first.matchedTerms)).toBe(true);
  });

  it("explain on vector-only search populates vectorScore", async () => {
    db.createCollection({
      collectionId: "explain-vec",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",
    });

    const records = Array.from({ length: 3 }, (_, i) => makeRecord(i));
    await db.upsertBatch("explain-vec", records);
    await db.publish("explain-vec", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:pipe-v1",
    });

    const result = await db.search({
      collectionId: "explain-vec",
      queryVector: makeQueryVector(1),
      topK: 3,
      mode: "vector",
      explain: true,
    });

    expect(result.results.length).toBeGreaterThan(0);
    const withExplain = result.results.filter((r) => r.explain !== undefined && r.explain !== null);
    expect(withExplain.length).toBeGreaterThan(0);
    for (const r of withExplain) {
      expect(typeof r.explain!.vectorScore).toBe("number");
    }
  });

  it("supports multiple ingest-publish cycles", async () => {
    db.createCollection({
      collectionId: "multi-cycle",
      dimension: DIM,
      metric: "cosine",
      embeddingModelId: "test-model",

    });

    // Cycle 1: ingest 3 records.
    await db.upsertBatch(
      "multi-cycle",
      Array.from({ length: 3 }, (_, i) => makeRecord(i)),
    );
    const m0 = await db.publish("multi-cycle", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });
    expect(m0.version).toBe(0);

    // Cycle 2: ingest 3 more records.
    await db.upsertBatch(
      "multi-cycle",
      Array.from({ length: 3 }, (_, i) => makeRecord(i + 3)),
    );
    const m1 = await db.publish("multi-cycle", {
      embeddingModelId: "test-model",
      pipelineSignature: "sha256:v1",
    });
    expect(m1.version).toBe(1);
    expect(m1.segment_ids.length).toBe(2);

    // Search should see all 6 records.
    const result = await db.search({
      collectionId: "multi-cycle",
      queryVector: makeQueryVector(3),
      topK: 10,
    });
    expect(result.results.length).toBe(6);
  });
});
