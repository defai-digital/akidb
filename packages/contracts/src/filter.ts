/**
 * Metadata filter operators for AkiDB search.
 *
 * These types document the filter shapes accepted by `filters_json` in the
 * Rust search engine. No runtime validation — Rust validates internally.
 */

import { z } from "zod";

/** Operator-based filter on a single field. Multiple operators are ANDed. */
export interface FilterOperators {
  $gt?: number | string;
  $gte?: number | string;
  $lt?: number | string;
  $lte?: number | string;
  $ne?: string | number | boolean | null;
  $in?: Array<string | number>;
  $nin?: Array<string | number>;
}

/** Possible filter value shapes for a single field. */
export type FilterValue =
  | string
  | number
  | boolean
  | null // exact match
  | Array<string | number> // OR match
  | FilterOperators; // rich operators

/** A metadata filter: keys are field names, values define match criteria. */
export type MetadataFilter = Record<string, FilterValue>;

// ─── Zod schema for runtime validation ───────────────────────────────────────

const FilterOperatorsSchema = z.object({
  $gt: z.union([z.number(), z.string()]).optional(),
  $gte: z.union([z.number(), z.string()]).optional(),
  $lt: z.union([z.number(), z.string()]).optional(),
  $lte: z.union([z.number(), z.string()]).optional(),
  $ne: z.union([z.string(), z.number(), z.boolean(), z.null()]).optional(),
  $in: z.array(z.union([z.string(), z.number()])).optional(),
  $nin: z.array(z.union([z.string(), z.number()])).optional(),
});

const FilterValueSchema: z.ZodType<FilterValue> = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
  z.array(z.union([z.string(), z.number()])),
  FilterOperatorsSchema,
]);

export const MetadataFilterSchema = z.record(z.string(), FilterValueSchema);
