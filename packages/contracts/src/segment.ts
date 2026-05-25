import { z } from "zod";

export const SegmentStatusSchema = z.enum(["building", "ready", "archived"]);

export type SegmentStatus = z.infer<typeof SegmentStatusSchema>;

export const SegmentMetadataSchema = z.object({
  segment_id: z.string().min(1),
  collection_id: z.string().min(1),
  record_count: z.number().int().nonnegative(),
  dimension: z.number().int().positive(),
  size_bytes: z.number().int().nonnegative(),
  checksum: z.string().min(1),
  status: SegmentStatusSchema,
  storage_path: z.string().min(1),
  created_at: z.string().datetime(),
});

export type SegmentMetadata = z.infer<typeof SegmentMetadataSchema>;
