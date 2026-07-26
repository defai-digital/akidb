# Agentic Knowledge Contract Fixtures

These JSON and NDJSON documents are the portable, versioned contract examples
for generation publication, ordered mutation replay, replica checkpoints, and
logical generation bundles.

- `valid/` fixtures must deserialize, validate, and round-trip without semantic
  changes.
- `invalid/` fixtures deserialize successfully but must fail contract
  validation.

`valid/bundle.ndjson` is the byte-stable logical bundle fixture. Its first line
is a header, followed by strictly ID-sorted records, nodes, and edges. The
matching `bundle-manifest.json` pins its byte length and SHA-256. Bundles never
contain engine-specific RocksDB or HNSW directories.

`valid/mutation-payload-upsert.json` is the byte-stable logical replacement for
one chunk and its evidence graph. `mutation-upsert-bundle.json` binds that
object's exact byte length and SHA-256 to sequence 11 of the bundle generation.

Changing an existing fixture is a compatibility change. Add a new version
directory when a future schema is not backward compatible.

Immutable object references use durable `s3://` keys or `https://` URLs.
Publishers remain responsible for using checksum-addressed keys or immutable
object versions; validation rejects plain HTTP, embedded credentials, and
zero-length objects.

All JSON integer fields are limited to `9007199254740991` (`2^53 - 1`) so Rust,
TypeScript, and other JSON implementations preserve their values exactly.
