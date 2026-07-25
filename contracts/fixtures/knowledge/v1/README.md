# Agentic Knowledge Contract Fixtures

These JSON documents are the portable, versioned contract examples for
generation publication, ordered mutation replay, and replica checkpoints.

- `valid/` fixtures must deserialize, validate, and round-trip without semantic
  changes.
- `invalid/` fixtures deserialize successfully but must fail contract
  validation.

Changing an existing fixture is a compatibility change. Add a new version
directory when a future schema is not backward compatible.

Immutable object references use durable `s3://` keys or `https://` URLs.
Publishers remain responsible for using checksum-addressed keys or immutable
object versions; validation rejects plain HTTP, embedded credentials, and
zero-length objects.

All JSON integer fields are limited to `9007199254740991` (`2^53 - 1`) so Rust,
TypeScript, and other JSON implementations preserve their values exactly.
