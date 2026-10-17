# Adoption fixtures

These small English documents are synthetic and distributed under this
repository's Apache-2.0 license. They contain no real customer or deployment data.
Index only `documents/`; this README is not part of the retrieval corpus.

- `Cedar service health endpoint`: expect `runbook.md`, including port 7410.
- `Who owns canonical sources and publication decisions?`: expect `releases.md`.

`scripts/qa_adoption.py` checks CLI/transport/citation behavior with a deterministic
local embedder and BM25 known-answer queries. That test is not evidence of
semantic embedding quality, production capacity, or Fabric replica qualification.
