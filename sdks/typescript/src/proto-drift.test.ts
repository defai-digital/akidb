import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const vendored = join(here, '..', 'proto', 'akidb.proto');
const canonical = join(here, '..', '..', '..', 'crates', 'grpc-server', 'proto', 'akidb.proto');

describe('proto drift', () => {
  it('vendored proto matches the canonical engine proto', () => {
    let canon: string;
    try {
      canon = readFileSync(canonical, 'utf8');
    } catch {
      return; // canonical not present (e.g. standalone published package) — skip
    }
    expect(readFileSync(vendored, 'utf8')).toBe(canon);
  });
});
