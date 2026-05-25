import { z } from "zod";

export const ErrorCodeSchema = z.enum([
  "EXTRACT_ERROR",
  "EMBED_ERROR",
  "LLM_ERROR",
  "PUBLISH_ERROR",
  "STATE_ERROR",
  "CHECKSUM_ERROR",
  "STORAGE_ERROR",
  "METADATA_ERROR",
  "QUERY_ERROR",
]);

export type ErrorCode = z.infer<typeof ErrorCodeSchema>;

export class AxFabricError extends Error {
  constructor(
    public readonly code: ErrorCode,
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "AxFabricError";
  }
}
