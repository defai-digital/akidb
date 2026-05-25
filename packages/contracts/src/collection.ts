import { z } from "zod";

export const DistanceMetricSchema = z.enum(["cosine", "l2", "dot"]);

export type DistanceMetric = z.infer<typeof DistanceMetricSchema>;

export const QuantizationSchema = z.enum(["fp16", "sq8"]);

export type Quantization = z.infer<typeof QuantizationSchema>;

export const CollectionSchema = z.object({
  collection_id: z.string().min(1),
  dimension: z.number().int().positive(),
  metric: DistanceMetricSchema,
  embedding_model_id: z.string().min(1),
  schema_version: z.string().min(1),
  created_at: z.string().datetime(),
  deleted_at: z.string().datetime().nullable().default(null),
  quantization: QuantizationSchema.default("fp16"),
  hnsw_m: z.number().int().min(4).max(64).default(16),
  hnsw_ef_construction: z.number().int().min(50).max(800).default(200),
  hnsw_ef_search: z.number().int().min(10).max(500).default(100),
});

export type Collection = z.infer<typeof CollectionSchema>;
