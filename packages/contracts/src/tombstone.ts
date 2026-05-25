import { z } from "zod";

export const TombstoneReasonSchema = z.enum([
  "file_deleted",
  "file_updated",
  "manual_revoke",
]);

export type TombstoneReason = z.infer<typeof TombstoneReasonSchema>;

export const TombstoneSchema = z.object({
  chunk_id: z.string().min(1),
  deleted_at: z.string().datetime(),
  reason_code: TombstoneReasonSchema,
});

export type Tombstone = z.infer<typeof TombstoneSchema>;
