import { z } from "zod";

export const ChunkLabelSchema = z.enum([
  "paragraph",
  "heading",
  "table",
  "code",
  "list",
  "text",
]);

export type ChunkLabel = z.infer<typeof ChunkLabelSchema>;

export const ObjectSourceMetadataSchema = z.object({
  uri: z.string().min(1),
  bucket: z.string().min(1).optional(),
  key: z.string().min(1),
  version_id: z.string().min(1).optional(),
  etag: z.string().min(1).optional(),
  sha256: z.string().min(1).optional(),
  updated_at: z.string().optional(),
  size_bytes: z.number().int().nonnegative().optional(),
});

export type ObjectSourceMetadata = z.infer<typeof ObjectSourceMetadataSchema>;

export const RecordMetadataSchema = z.object({
  source_uri: z.string(),
  content_type: z.enum([
    "txt",
    "md",
    "pdf",
    "docx",
    "pptx",
    "xlsx",
    "csv",
    "tsv",
    "json",
    "jsonl",
    "yaml",
    "html",
    "rtf",
    "sql",
    "log",
    "eml",
  ]),
  page_range: z.string().nullable(),
  offset: z.number().int().nonnegative(),
  table_ref: z.string().nullable(),
  chunk_label: ChunkLabelSchema.optional(),
  object_source: ObjectSourceMetadataSchema.optional(),
  created_at: z.string().datetime(),
});

export type RecordMetadata = z.infer<typeof RecordMetadataSchema>;

export const RecordSchema = z.object({
  chunk_id: z.string().min(1),
  doc_id: z.string().min(1),
  doc_version: z.string().min(1),
  chunk_hash: z.string().min(1),
  pipeline_signature: z.string().min(1),
  embedding_model_id: z.string().min(1),
  vector: z.array(z.number()),
  metadata: RecordMetadataSchema,
  chunk_text: z.string().optional(),
});

export type Record = z.infer<typeof RecordSchema>;
