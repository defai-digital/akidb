/** Typed error hierarchy for the AkiDB client. gRPC status codes map to specific
 * error classes so callers can handle failures by kind. */

import { status as Status, type ServiceError } from '@grpc/grpc-js';

export class AkiDBError extends Error {
  readonly code?: Status;
  readonly details?: string;
  constructor(message: string, code?: Status, details?: string) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    this.details = details;
  }
}

export class InvalidArgumentError extends AkiDBError {}
export class NotFoundError extends AkiDBError {}
export class AlreadyExistsError extends AkiDBError {}
export class PermissionDeniedError extends AkiDBError {}
export class UnauthenticatedError extends AkiDBError {}
export class FailedPreconditionError extends AkiDBError {}
export class ResourceExhaustedError extends AkiDBError {}
export class UnavailableError extends AkiDBError {}
export class DeadlineExceededError extends AkiDBError {}
export class InternalError extends AkiDBError {}

/** Status codes that are safe to retry (AkiDB writes are id-keyed upserts). */
export const RETRYABLE_CODES: ReadonlySet<Status> = new Set([
  Status.UNAVAILABLE,
  Status.DEADLINE_EXCEEDED,
  Status.RESOURCE_EXHAUSTED,
]);

const CODE_MAP: Partial<Record<Status, new (m: string, c?: Status, d?: string) => AkiDBError>> = {
  [Status.INVALID_ARGUMENT]: InvalidArgumentError,
  [Status.NOT_FOUND]: NotFoundError,
  [Status.ALREADY_EXISTS]: AlreadyExistsError,
  [Status.PERMISSION_DENIED]: PermissionDeniedError,
  [Status.UNAUTHENTICATED]: UnauthenticatedError,
  [Status.FAILED_PRECONDITION]: FailedPreconditionError,
  [Status.RESOURCE_EXHAUSTED]: ResourceExhaustedError,
  [Status.UNAVAILABLE]: UnavailableError,
  [Status.DEADLINE_EXCEEDED]: DeadlineExceededError,
  [Status.INTERNAL]: InternalError,
};

export function mapError(err: ServiceError): AkiDBError {
  const code = err.code;
  const details = err.details ?? err.message;
  const Ctor = (code !== undefined && CODE_MAP[code]) || AkiDBError;
  const name = code !== undefined ? Status[code] : 'UNKNOWN';
  return new Ctor(`${name}: ${details}`, code, details);
}
