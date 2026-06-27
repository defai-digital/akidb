import { describe, it, expect } from 'vitest';
import { status as Status, type ServiceError, Metadata } from '@grpc/grpc-js';
import {
  AkiDBClient,
  buildMemoryMetadata,
  metadataJson,
  type RawClient,
  type SearchHit,
} from './client.js';
import { NotFoundError, UnavailableError } from './errors.js';

const METHODS: (keyof RawClient)[] = [
  'Insert',
  'InsertBatch',
  'Update',
  'Delete',
  'Get',
  'Search',
  'SearchBatch',
  'TextSearch',
  'Health',
  'GetClusterState',
];

interface Recorded {
  request: any;
  metadata: Metadata;
}

/** Build a fake RawClient. `responses` maps method -> canned response or a
 * function returning one (so tests can sequence retries). */
function fakeClient(responses: Partial<Record<keyof RawClient, any | (() => any)>>) {
  const calls: Partial<Record<keyof RawClient, Recorded>> = {};
  const counts: Partial<Record<keyof RawClient, number>> = {};
  const raw = {} as RawClient;
  for (const name of METHODS) {
    raw[name] = (request: any, metadata: Metadata, _options: any, cb: any) => {
      calls[name] = { request, metadata };
      counts[name] = (counts[name] ?? 0) + 1;
      const r = responses[name];
      const value = typeof r === 'function' ? (r as () => any)() : (r ?? {});
      if (value instanceof Error) cb(value, null);
      else cb(null, value);
    };
  }
  return { raw, calls, counts };
}

function rpcError(code: Status, details = 'boom'): ServiceError {
  return Object.assign(new Error(details), { code, details, metadata: new Metadata() }) as ServiceError;
}

describe('AkiDBClient (hardened)', () => {
  it('insert sends request with auth metadata and deadline', async () => {
    const { raw, calls } = fakeClient({ Insert: { success: true, id: 'a' } });
    const client = new AkiDBClient({ rawClient: raw, authToken: 'secret' });
    const resp = await client.insert('a', [1, 2, 3], { text: 'hi' });
    expect(calls.Insert!.request.id).toBe('a');
    expect(calls.Insert!.request.vector).toEqual([1, 2, 3]);
    expect(calls.Insert!.request.text).toBe('hi');
    expect(calls.Insert!.metadata.get('authorization')).toEqual(['Bearer secret']);
    expect(resp.success).toBe(true);
  });

  it('search maps hits and metadataJson parses', async () => {
    const results: SearchHit[] = [
      { id: 'a', score: 0.9, metadata: '{"k":"v"}' },
      { id: 'b', score: 0.5, metadata: '' },
    ];
    const { raw, calls } = fakeClient({ Search: { results } });
    const client = new AkiDBClient({ rawClient: raw });
    const hits = await client.search([0.1, 0.2], 2);
    expect(calls.Search!.request.top_k).toBe(2);
    expect(hits.map((h) => h.id)).toEqual(['a', 'b']);
    expect(metadataJson(hits[0])).toEqual({ k: 'v' });
    expect(metadataJson(hits[1])).toBeUndefined();
  });

  it('textSearch sets flags + budget and returns context pack', async () => {
    const { raw, calls } = fakeClient({
      TextSearch: { results: [{ id: 'x', score: 1, metadata: '' }], context_pack: '[x] ctx' },
    });
    const client = new AkiDBClient({ rawClient: raw });
    const result = await client.textSearch('q', {
      topK: 7,
      hybrid: true,
      rerank: true,
      diversity: true,
      pack: true,
      tokenBudget: 256,
    });
    const r = calls.TextSearch!.request;
    expect([r.top_k, r.hybrid, r.rerank, r.diversity, r.pack]).toEqual([7, true, true, true, true]);
    expect(r.pack_token_budget).toBe(256);
    expect(result.contextPack).toBe('[x] ctx');
  });

  it('textSearch forwards metadata filters', async () => {
    const { raw, calls } = fakeClient({ TextSearch: { results: [] } });
    const client = new AkiDBClient({ rawClient: raw });
    const filter = new Uint8Array([123, 125]);
    const tagFilter = { condition: { key: 'tenant', value: { text: 'a' }, op: 'TAG_OP_EQ' } };

    await client.textSearch('q', { filter, tagFilter, retrievalMode: 'bm25' });

    expect(calls.TextSearch!.request.filter).toBe(filter);
    expect(calls.TextSearch!.request.tag_filter).toBe(tagFilter);
    expect(calls.TextSearch!.request.retrieval_mode).toBe('bm25');
  });

  it('insertBatch / get / update / searchBatch', async () => {
    const { raw, calls } = fakeClient({
      InsertBatch: { success: true, inserted_count: 2, failed_ids: [] },
      Get: { id: 'a', vector: [1, 2], metadata: '{}', found: true },
      Update: { success: true, id: 'a' },
      SearchBatch: { results: [{ results: [{ id: 'a', score: 1 }] }, { results: [] }] },
    });
    const client = new AkiDBClient({ rawClient: raw });

    const ib = await client.insertBatch([
      { id: 'a', vector: [1], text: 't' },
      { id: 'b', vector: [2] },
    ]);
    expect(ib.inserted_count).toBe(2);
    expect(calls.InsertBatch!.request.vectors[0].embedding).toEqual([1]);

    const got = await client.get('a');
    expect(got.found).toBe(true);
    expect(got.vector).toEqual([1, 2]);

    await client.update('a', [3, 4]);
    expect(calls.Update!.request.vector).toEqual([3, 4]);

    const batches = await client.searchBatch([[1], [2]], 1);
    expect(batches[0][0].id).toBe('a');
    expect(batches[1]).toEqual([]);
  });

  it('retries on UNAVAILABLE then succeeds', async () => {
    let n = 0;
    const { raw, counts } = fakeClient({
      Search: () => {
        n++;
        return n < 3 ? rpcError(Status.UNAVAILABLE) : { results: [{ id: 'ok', score: 1 }] };
      },
    });
    const client = new AkiDBClient({ rawClient: raw, backoffMs: 0, maxRetries: 3 });
    const hits = await client.search([0.1]);
    expect(hits[0].id).toBe('ok');
    expect(counts.Search).toBe(3);
  });

  it('invokes onRetry before each retry', async () => {
    let n = 0;
    const seen: number[] = [];
    const { raw } = fakeClient({
      Search: () => {
        n++;
        return n < 3 ? rpcError(Status.UNAVAILABLE) : { results: [] };
      },
    });
    const client = new AkiDBClient({
      rawClient: raw,
      backoffMs: 0,
      maxRetries: 3,
      onRetry: (attempt) => seen.push(attempt),
    });
    await client.search([0.1]);
    expect(seen).toEqual([0, 1]);
  });

  it('returns typed delete/update responses', async () => {
    const { raw } = fakeClient({
      Delete: { success: true, id: 'x', status: 'DELETED', visibility: 'immediate' },
      Update: { success: true, id: 'x', status: 'UPDATED' },
    });
    const client = new AkiDBClient({ rawClient: raw });
    const d = await client.delete('x');
    expect(d.visibility).toBe('immediate');
    const u = await client.update('x', [1, 2]);
    expect(u.status).toBe('UPDATED');
  });

  it('get on a missing id returns found:false instead of throwing', async () => {
    const { raw } = fakeClient({ Get: () => rpcError(Status.NOT_FOUND, 'missing') });
    const client = new AkiDBClient({ rawClient: raw });
    const got = await client.get('nope');
    expect(got.found).toBe(false);
    expect(got.id).toBe('nope');
  });

  it('maps non-retryable errors immediately', async () => {
    const { raw, counts } = fakeClient({ Search: () => rpcError(Status.NOT_FOUND, 'missing') });
    const client = new AkiDBClient({ rawClient: raw, maxRetries: 5 });
    await expect(client.search([0.1])).rejects.toBeInstanceOf(NotFoundError);
    expect(counts.Search).toBe(1);
  });

  it('throws mapped error after exhausting retries', async () => {
    const { raw, counts } = fakeClient({ Search: () => rpcError(Status.UNAVAILABLE) });
    const client = new AkiDBClient({ rawClient: raw, backoffMs: 0, maxRetries: 2 });
    await expect(client.search([0.1])).rejects.toBeInstanceOf(UnavailableError);
    expect(counts.Search).toBe(3); // initial + 2 retries
  });

  it('memoryWrite builds metadata; memoryRead builds tag filter', async () => {
    const { raw, calls } = fakeClient({
      Insert: { success: true, id: 'm1' },
      Search: { results: [{ id: 'm1', score: 1 }] },
    });
    const client = new AkiDBClient({ rawClient: raw });

    await client.memoryWrite('m1', [1], 'remember', { kind: 'note', conversationId: 'c1' });
    const meta = JSON.parse(new TextDecoder().decode(calls.Insert!.request.metadata));
    expect(meta.memory_kind).toBe('note');
    expect(meta.conversation_id).toBe('c1');

    await client.memoryRead([0.1], { conversationId: 'c1', kind: 'note' });
    const tf = calls.Search!.request.tag_filter;
    expect(tf.and.filters.map((f: any) => f.condition.key).sort()).toEqual([
      'conversation_id',
      'memory_kind',
    ]);
  });

  it('memoryRead with one filter uses a plain condition', async () => {
    const { raw, calls } = fakeClient({ Search: { results: [] } });
    const client = new AkiDBClient({ rawClient: raw });
    await client.memoryRead([0.1], { conversationId: 'c1' });
    expect(calls.Search!.request.tag_filter.condition.key).toBe('conversation_id');
  });

  it('buildMemoryMetadata protects reserved keys from tags', () => {
    const meta = buildMemoryMetadata({ kind: 'task', tags: { conversation_id: 'HACK', topic: 'x' } });
    expect(meta.memory_kind).toBe('task');
    expect(meta.conversation_id).toBeUndefined();
    expect(meta.topic).toBe('x');
  });

  it('uses a custom collection', async () => {
    const { raw, calls } = fakeClient({ Delete: { success: true } });
    const client = new AkiDBClient({ rawClient: raw, collection: 'mycoll' });
    await client.delete('gone');
    expect(calls.Delete!.request.collection).toBe('mycoll');
  });
});
