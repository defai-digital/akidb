import { z } from "zod";

export const ManifestSchema = z.object({
  manifest_id: z.string().min(1),
  collection_id: z.string().min(1),
  version: z.number().int().nonnegative(),
  segment_ids: z.array(z.string().min(1)),
  tombstone_ids: z.array(z.string().min(1)),
  embedding_model_id: z.string().min(1),
  pipeline_signature: z.string().min(1),
  created_at: z.string().datetime(),
  checksum: z.string().min(1),
});

export type Manifest = z.infer<typeof ManifestSchema>;
