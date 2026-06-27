/**
 * Live integration test against a running AkiDB server.
 *
 * Opt-in: set AKIDB_SERVER_ADDR to a running server; AKIDB_TEST_DIM must match
 * the server's index dimension (default 8). Skipped otherwise so the normal
 * (mock-based) suite stays hermetic.
 */
import { describe, it, expect } from 'vitest';
import { AkiDBClient } from './client.js';

const ADDR = process.env.AKIDB_SERVER_ADDR;
const DIM = Number(process.env.AKIDB_TEST_DIM ?? '8');

function unitVector(dim: number): number[] {
  const v = new Array(dim).fill(0);
  v[0] = 1;
  return v;
}

describe('live integration', () => {
  it.skipIf(!ADDR)('round-trips insert/get/search/delete', async () => {
    const client = new AkiDBClient({ target: ADDR!, timeoutMs: 10_000, maxRetries: 5 });
    await client.health();

    const vec = unitVector(DIM);
    const ins = await client.insert('live-ts-1', vec, { text: 'hello live', metadata: new TextEncoder().encode('{"k":"v"}') });
    expect(ins.success).toBe(true);

    const got = await client.get('live-ts-1');
    expect(got.found).toBe(true);

    const hits = await client.search(vec, 5);
    expect(hits.some((h) => h.id === 'live-ts-1')).toBe(true);

    const del = await client.delete('live-ts-1');
    expect(del.success).toBe(true);

    const gone = await client.get('live-ts-1');
    expect(gone.found).toBe(false);
  });
});
