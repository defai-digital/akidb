/**
 * AkiDB TypeScript SDK (INT-003).
 *
 * A typed client over the AkiDB gRPC API for agent/desktop developers. The proto
 * is loaded at runtime via @grpc/proto-loader (no codegen step). For testing,
 * inject a `rawClient` so the wrapper can be exercised without a server.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

export const DEFAULT_COLLECTION = 'default';

export interface SearchHit {
  id: string;
  score: number;
  metadata: string;
}

/** Parse a hit's metadata field as JSON, or `undefined` if absent/invalid. */
export function metadataJson(hit: SearchHit): unknown | undefined {
  if (!hit.metadata) return undefined;
  try {
    return JSON.parse(hit.metadata);
  } catch {
    return undefined;
  }
}

export interface TextSearchResult {
  hits: SearchHit[];
  contextPack: string;
}

export interface TextSearchOptions {
  topK?: number;
  hybrid?: boolean;
  rerank?: boolean;
  diversity?: boolean;
  pack?: boolean;
  tokenBudget?: number;
}

type Callback = (err: grpc.ServiceError | null, response: any) => void;
type UnaryCall = (request: any, callback: Callback) => void;

/** The subset of the generated gRPC client this SDK uses. */
export interface RawClient {
  Insert: UnaryCall;
  Delete: UnaryCall;
  Search: UnaryCall;
  TextSearch: UnaryCall;
  Health: UnaryCall;
}

/** Load the real gRPC client for `target`, reading the bundled proto. */
export function loadRawClient(target: string, protoPath?: string): RawClient {
  const here = dirname(fileURLToPath(import.meta.url));
  const path = protoPath ?? join(here, '..', 'proto', 'akidb.proto');
  const pkgDef = protoLoader.loadSync(path, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(pkgDef) as any;
  const Ctor = proto.akidb.v1.Akidb;
  return new Ctor(target, grpc.credentials.createInsecure()) as RawClient;
}

export interface AkiDBClientOptions {
  target?: string;
  collection?: string;
  /** Inject a client (for tests); when omitted, a real gRPC client is created. */
  rawClient?: RawClient;
}

export class AkiDBClient {
  private raw: RawClient;
  readonly collection: string;

  constructor(opts: AkiDBClientOptions = {}) {
    this.collection = opts.collection ?? DEFAULT_COLLECTION;
    this.raw = opts.rawClient ?? loadRawClient(opts.target ?? 'localhost:50051');
  }

  private call<T>(fn: UnaryCall, request: unknown): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      fn.call(this.raw, request, (err, response) =>
        err ? reject(err) : resolve(response as T),
      );
    });
  }

  insert(
    id: string,
    vector: number[],
    opts: { metadata?: Uint8Array; text?: string } = {},
  ): Promise<any> {
    return this.call(this.raw.Insert, {
      collection: this.collection,
      id,
      vector,
      metadata: opts.metadata ?? new Uint8Array(),
      text: opts.text ?? '',
    });
  }

  delete(id: string): Promise<any> {
    return this.call(this.raw.Delete, { collection: this.collection, id });
  }

  async search(vector: number[], topK = 10): Promise<SearchHit[]> {
    const resp = await this.call<{ results?: SearchHit[] }>(this.raw.Search, {
      collection: this.collection,
      query: vector,
      top_k: topK,
    });
    return resp.results ?? [];
  }

  async textSearch(text: string, opts: TextSearchOptions = {}): Promise<TextSearchResult> {
    const request: Record<string, unknown> = {
      collection: this.collection,
      text,
      top_k: opts.topK ?? 10,
      hybrid: opts.hybrid ?? true,
      rerank: opts.rerank ?? false,
      diversity: opts.diversity ?? false,
      pack: opts.pack ?? false,
    };
    if (opts.tokenBudget !== undefined) {
      request.pack_token_budget = opts.tokenBudget;
    }
    const resp = await this.call<{ results?: SearchHit[]; context_pack?: string }>(
      this.raw.TextSearch,
      request,
    );
    return { hits: resp.results ?? [], contextPack: resp.context_pack ?? '' };
  }

  health(): Promise<any> {
    return this.call(this.raw.Health, {});
  }
}
