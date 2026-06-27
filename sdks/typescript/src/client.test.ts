import { describe, it, expect } from 'vitest';
import { AkiDBClient, metadataJson, type RawClient, type SearchHit } from './client.js';

/** Build a fake RawClient that records the last request per method and replies
 * with the given canned responses. */
function fakeClient(responses: Partial<Record<keyof RawClient, any>>) {
  const calls: Partial<Record<keyof RawClient, any>> = {};
  const make = (name: keyof RawClient) => (req: any, cb: any) => {
    calls[name] = req;
    cb(null, responses[name] ?? {});
  };
  const raw: RawClient = {
    Insert: make('Insert'),
    Delete: make('Delete'),
    Search: make('Search'),
    TextSearch: make('TextSearch'),
    Health: make('Health'),
  };
  return { raw, calls };
}

describe('AkiDBClient', () => {
  it('insert builds the request', async () => {
    const { raw, calls } = fakeClient({ Insert: { success: true, id: 'a' } });
    const client = new AkiDBClient({ rawClient: raw });
    const resp = await client.insert('a', [1, 2, 3], { text: 'hi' });
    expect(calls.Insert.id).toBe('a');
    expect(calls.Insert.vector).toEqual([1, 2, 3]);
    expect(calls.Insert.text).toBe('hi');
    expect(calls.Insert.collection).toBe('default');
    expect(resp.success).toBe(true);
  });

  it('search maps results to hits', async () => {
    const results: SearchHit[] = [
      { id: 'a', score: 0.9, metadata: '{"k":"v"}' },
      { id: 'b', score: 0.5, metadata: '' },
    ];
    const { raw, calls } = fakeClient({ Search: { results } });
    const client = new AkiDBClient({ rawClient: raw });
    const hits = await client.search([0.1, 0.2], 2);
    expect(calls.Search.top_k).toBe(2);
    expect(calls.Search.query).toEqual([0.1, 0.2]);
    expect(hits.map((h) => h.id)).toEqual(['a', 'b']);
    expect(metadataJson(hits[0])).toEqual({ k: 'v' });
    expect(metadataJson(hits[1])).toBeUndefined();
  });

  it('textSearch sets flags, budget, and returns context pack', async () => {
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
    expect(calls.TextSearch.top_k).toBe(7);
    expect(calls.TextSearch.hybrid).toBe(true);
    expect(calls.TextSearch.rerank).toBe(true);
    expect(calls.TextSearch.diversity).toBe(true);
    expect(calls.TextSearch.pack).toBe(true);
    expect(calls.TextSearch.pack_token_budget).toBe(256);
    expect(result.contextPack).toBe('[x] ctx');
    expect(result.hits).toHaveLength(1);
  });

  it('omits pack_token_budget when not provided', async () => {
    const { raw, calls } = fakeClient({ TextSearch: { results: [] } });
    const client = new AkiDBClient({ rawClient: raw });
    await client.textSearch('q');
    expect(calls.TextSearch.pack_token_budget).toBeUndefined();
  });

  it('uses a custom collection', async () => {
    const { raw, calls } = fakeClient({ Delete: { success: true } });
    const client = new AkiDBClient({ rawClient: raw, collection: 'mycoll' });
    await client.delete('gone');
    expect(calls.Delete.collection).toBe('mycoll');
    expect(calls.Delete.id).toBe('gone');
  });

  it('propagates errors as rejections', async () => {
    const raw: RawClient = {
      Insert: (_req, cb) => cb(new Error('boom') as any, null),
      Delete: (_req, cb) => cb(null, {}),
      Search: (_req, cb) => cb(null, {}),
      TextSearch: (_req, cb) => cb(null, {}),
      Health: (_req, cb) => cb(null, {}),
    };
    const client = new AkiDBClient({ rawClient: raw });
    await expect(client.insert('a', [1])).rejects.toThrow('boom');
  });
});
