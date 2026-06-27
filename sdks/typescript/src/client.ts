/**
 * AkiDB TypeScript SDK (INT-003) — production-grade gRPC client.
 *
 * Per-call deadlines, retry with exponential backoff on transient errors,
 * optional TLS and bearer-token auth, a typed error hierarchy (see ./errors),
 * typed responses, and full coverage of the service surface. The proto is loaded
 * at runtime via @grpc/proto-loader. For testing, inject a `rawClient`.
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mapError, NotFoundError, RETRYABLE_CODES } from './errors.js';

export const DEFAULT_COLLECTION = 'default';
export const DEFAULT_TIMEOUT_MS = 30_000;
export const DEFAULT_MAX_RETRIES = 3;
export const DEFAULT_BACKOFF_MS = 100;

export interface SearchHit {
  id: string;
  score: number;
  metadata: string;
}

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

export interface GetResult {
  id: string;
  vector: number[];
  metadata: string;
  found: boolean;
}

export interface HealthStatus {
  healthy: boolean;
  ready: boolean;
  message: string;
  total_vectors: string;
  active_vectors: string;
  using_gpu: boolean;
}

export interface DeleteResponse {
  success: boolean;
  id: string;
  status: string;
  visibility: string;
}

export interface UpdateResponse {
  success: boolean;
  id: string;
  status: string;
}

export interface ClusterState {
  coordinators: unknown[];
  shards: unknown[];
  leader_id?: string;
  local_peer_id: string;
  metrics?: unknown;
}

export interface InsertResponse {
  success: boolean;
  id: string;
}

export interface BatchInsertResponse {
  success: boolean;
  inserted_count: number;
  failed_ids: string[];
}

export interface VectorInput {
  id: string;
  vector: number[];
  metadata?: Uint8Array;
  text?: string;
}

export interface TextSearchOptions {
  topK?: number;
  hybrid?: boolean;
  rerank?: boolean;
  diversity?: boolean;
  pack?: boolean;
  tokenBudget?: number;
  filter?: Uint8Array;
  tagFilter?: unknown;
  retrievalMode?: string;
}

export interface MemoryWriteOptions {
  kind?: string;
  conversationId?: string;
  taskId?: string;
  tool?: string;
  sourceUri?: string;
  timestamp?: number;
  tags?: Record<string, string>;
}

const RESERVED = ['memory_kind', 'conversation_id', 'task_id', 'tool', 'source_uri', 'timestamp'];

type Callback = (err: grpc.ServiceError | null, response: any) => void;
type UnaryCall = (
  request: any,
  metadata: grpc.Metadata,
  options: grpc.CallOptions,
  callback: Callback,
) => void;

/** The subset of the generated gRPC client this SDK uses. */
export interface RawClient {
  Insert: UnaryCall;
  InsertBatch: UnaryCall;
  Update: UnaryCall;
  Delete: UnaryCall;
  Get: UnaryCall;
  Search: UnaryCall;
  SearchBatch: UnaryCall;
  TextSearch: UnaryCall;
  Health: UnaryCall;
  GetClusterState: UnaryCall;
}

export interface AkiDBClientOptions {
  target?: string;
  collection?: string;
  timeoutMs?: number;
  maxRetries?: number;
  backoffMs?: number;
  tls?: boolean;
  rootCerts?: Buffer;
  authToken?: string;
  metadata?: Record<string, string>;
  /** Observability hook invoked before each retry sleep. */
  onRetry?: (attempt: number, error: grpc.ServiceError) => void;
  /** Inject a client (for tests); when omitted, a real gRPC client is created. */
  rawClient?: RawClient;
}

function loadRawClient(target: string, creds: grpc.ChannelCredentials): RawClient {
  const here = dirname(fileURLToPath(import.meta.url));
  const path = join(here, '..', 'proto', 'akidb.proto');
  const pkgDef = protoLoader.loadSync(path, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(pkgDef) as any;
  return new proto.akidb.v1.Akidb(target, creds) as RawClient;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Equal-jitter exponential backoff: half fixed, half random (avoids a
 * thundering herd). `base <= 0` yields 0 (used by tests). */
function jitter(base: number, attempt: number): number {
  if (base <= 0) return 0;
  const window = base * 2 ** attempt;
  return window / 2 + Math.random() * (window / 2);
}

export class AkiDBClient {
  private raw: RawClient;
  readonly collection: string;
  private timeoutMs: number;
  private maxRetries: number;
  private backoffMs: number;
  private authToken?: string;
  private metadataEntries: [string, string][];
  private onRetry?: (attempt: number, error: grpc.ServiceError) => void;

  constructor(opts: AkiDBClientOptions = {}) {
    this.collection = opts.collection ?? DEFAULT_COLLECTION;
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.maxRetries = opts.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.backoffMs = opts.backoffMs ?? DEFAULT_BACKOFF_MS;
    this.authToken = opts.authToken;
    this.metadataEntries = Object.entries(opts.metadata ?? {});
    this.onRetry = opts.onRetry;

    if (opts.rawClient) {
      this.raw = opts.rawClient;
    } else {
      const creds = opts.tls
        ? grpc.credentials.createSsl(opts.rootCerts)
        : grpc.credentials.createInsecure();
      this.raw = loadRawClient(opts.target ?? 'localhost:50051', creds);
    }
  }

  private buildMetadata(): grpc.Metadata {
    const md = new grpc.Metadata();
    for (const [k, v] of this.metadataEntries) md.add(k, v);
    if (this.authToken) md.add('authorization', `Bearer ${this.authToken}`);
    return md;
  }

  private callOnce<T>(fn: UnaryCall, request: unknown): Promise<T> {
    const metadata = this.buildMetadata();
    const options: grpc.CallOptions = { deadline: Date.now() + this.timeoutMs };
    return new Promise<T>((resolve, reject) => {
      fn.call(this.raw, request, metadata, options, (err, response) =>
        err ? reject(err) : resolve(response as T),
      );
    });
  }

  private async call<T>(fn: UnaryCall, request: unknown): Promise<T> {
    let attempt = 0;
    for (;;) {
      try {
        return await this.callOnce<T>(fn, request);
      } catch (err) {
        const e = err as grpc.ServiceError;
        if (e.code !== undefined && RETRYABLE_CODES.has(e.code) && attempt < this.maxRetries) {
          this.onRetry?.(attempt, e);
          await sleep(jitter(this.backoffMs, attempt));
          attempt++;
          continue;
        }
        throw mapError(e);
      }
    }
  }

  insert(
    id: string,
    vector: number[],
    opts: { metadata?: Uint8Array; text?: string } = {},
  ): Promise<InsertResponse> {
    return this.call(this.raw.Insert, {
      collection: this.collection,
      id,
      vector,
      metadata: opts.metadata ?? new Uint8Array(),
      text: opts.text ?? '',
    });
  }

  insertBatch(vectors: VectorInput[]): Promise<BatchInsertResponse> {
    return this.call(this.raw.InsertBatch, {
      collection: this.collection,
      vectors: vectors.map((v) => ({
        id: v.id,
        embedding: v.vector,
        metadata: v.metadata ?? new Uint8Array(),
        text: v.text ?? '',
      })),
    });
  }

  update(id: string, vector: number[], opts: { metadata?: Uint8Array } = {}): Promise<UpdateResponse> {
    return this.call(this.raw.Update, {
      collection: this.collection,
      id,
      vector,
      metadata: opts.metadata ?? new Uint8Array(),
    });
  }

  delete(id: string): Promise<DeleteResponse> {
    return this.call(this.raw.Delete, { collection: this.collection, id });
  }

  async get(id: string): Promise<GetResult> {
    // The server signals a missing vector with a NOT_FOUND error; normalize that
    // to a `found: false` result so callers don't branch on exceptions.
    try {
      const r = await this.call<{ id: string; vector?: number[]; metadata: string; found: boolean }>(
        this.raw.Get,
        { collection: this.collection, id },
      );
      return { id: r.id, vector: r.vector ?? [], metadata: r.metadata, found: r.found };
    } catch (e) {
      if (e instanceof NotFoundError) return { id, vector: [], metadata: '', found: false };
      throw e;
    }
  }

  async search(vector: number[], topK = 10, tagFilter?: unknown): Promise<SearchHit[]> {
    const request: Record<string, unknown> = {
      collection: this.collection,
      query: vector,
      top_k: topK,
    };
    if (tagFilter) request.tag_filter = tagFilter;
    const resp = await this.call<{ results?: SearchHit[] }>(this.raw.Search, request);
    return resp.results ?? [];
  }

  async searchBatch(vectors: number[][], topK = 10): Promise<SearchHit[][]> {
    const resp = await this.call<{ results?: { results?: SearchHit[] }[] }>(this.raw.SearchBatch, {
      collection: this.collection,
      queries: vectors.map((v) => ({ vector: v })),
      top_k: topK,
    });
    return (resp.results ?? []).map((r) => r.results ?? []);
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
    if (opts.tokenBudget !== undefined) request.pack_token_budget = opts.tokenBudget;
    if (opts.filter !== undefined) request.filter = opts.filter;
    if (opts.tagFilter !== undefined) request.tag_filter = opts.tagFilter;
    if (opts.retrievalMode !== undefined) request.retrieval_mode = opts.retrievalMode;
    const resp = await this.call<{ results?: SearchHit[]; context_pack?: string }>(
      this.raw.TextSearch,
      request,
    );
    return { hits: resp.results ?? [], contextPack: resp.context_pack ?? '' };
  }

  health(): Promise<HealthStatus> {
    return this.call(this.raw.Health, {});
  }

  clusterState(): Promise<ClusterState> {
    return this.call(this.raw.GetClusterState, {});
  }

  // -- agent memory convenience -------------------------------------------

  memoryWrite(
    id: string,
    vector: number[],
    text: string,
    opts: MemoryWriteOptions = {},
  ): Promise<InsertResponse> {
    const meta = buildMemoryMetadata(opts);
    const bytes = new TextEncoder().encode(JSON.stringify(meta));
    return this.insert(id, vector, { metadata: bytes, text });
  }

  memoryRead(
    queryVector: number[],
    opts: { conversationId?: string; kind?: string; topK?: number } = {},
  ): Promise<SearchHit[]> {
    const conds: unknown[] = [];
    if (opts.conversationId !== undefined) conds.push(eqCondition('conversation_id', opts.conversationId));
    if (opts.kind !== undefined) conds.push(eqCondition('memory_kind', opts.kind));
    return this.search(queryVector, opts.topK ?? 10, combine(conds));
  }
}

export function buildMemoryMetadata(opts: MemoryWriteOptions): Record<string, unknown> {
  const meta: Record<string, unknown> = { memory_kind: opts.kind ?? 'note' };
  if (opts.conversationId !== undefined) meta.conversation_id = opts.conversationId;
  if (opts.taskId !== undefined) meta.task_id = opts.taskId;
  if (opts.tool !== undefined) meta.tool = opts.tool;
  if (opts.sourceUri !== undefined) meta.source_uri = opts.sourceUri;
  if (opts.timestamp !== undefined) meta.timestamp = opts.timestamp;
  for (const [k, v] of Object.entries(opts.tags ?? {})) {
    if (!RESERVED.includes(k)) meta[k] = v;
  }
  return meta;
}

function eqCondition(key: string, value: string): unknown {
  return { condition: { key, value: { text: value }, op: 'TAG_OP_EQ' } };
}

function combine(conds: unknown[]): unknown | undefined {
  if (conds.length === 0) return undefined;
  if (conds.length === 1) return conds[0];
  return { and: { filters: conds } };
}
