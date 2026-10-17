# Contributing to AkiDB

AkiDB is an Apache-2.0 retrieval engine that runs independently and supports
AX Fabric. Real workloads, reproducible failures, documentation corrections,
and regression tests all help improve the shared engine. Commercial conversion
is not a project goal. Contributions intentionally submitted for inclusion
are governed by the project's [Apache-2.0 license](LICENSE).

## Report a problem

Use the bug template for crashes, installation failures, incorrect updates,
or isolation problems. Use the retrieval-quality template when a query misses
an expected passage or returns an incorrect citation. Include:

- the exact release or commit and supported OS/architecture;
- standalone or generation mode, SDK version, and retrieval mode;
- minimal reproduction commands, expected result, and actual result;
- a small synthetic or redistributable fixture, when possible.

Run `akidb doctor --json` for a content-free prerequisite report on a build
that includes the command. It does not upload anything. Inspect any attachment
before sharing it. Do not post tokens, private documents, raw production logs,
private paths, or a database dump. A reproduction using synthetic data is enough.
Report suspected security issues privately through the repository's GitHub
security reporting channel when available; do not publish exploit details or
private data in a public issue. If no private channel is available, request one
without including sensitive details.

## Build and test

Supported targets are macOS 26 Apple Silicon and Ubuntu 24.04+ AMD64; see
[prerequisites and platform limits](docs/platform/SUPPORT.md).

```bash
cargo check --workspace
cargo test -p akidb-cli
cargo build -p akidb-cli
python3 scripts/qa_adoption.py --binary target/debug/akidb
```

The adoption smoke uses a real server and a deterministic local test embedder.
It tests contracts and setup, not semantic model quality. For a real-model
check, follow the [standalone tutorial](docs/development/quickstart.md).

For engine changes, run the affected crate's tests and the relevant integration
tests. SDK/proto changes require both SDK suites and
`./sdks/check-proto-drift.sh`; regenerate Python bindings using the provided
generator. Cross-product changes also require a pinned Fabric compatibility
check. Full validation commands are in the [README](README.md#validation).

## Turn a failure into a regression

1. Reproduce the failure on an identified revision with a small fixture.
2. Add a behavior test beside the owning code or under `crates/*/tests/`.
3. Show that the test fails before the fix and passes after it.
4. Run related suites and record their commands and results in the PR.
5. Link the issue, fix commit and eventual release; ask the reporter to retest.

For ranking issues, record the query, eligible corpus, expected passage IDs,
filters, model/dimensions, and budget. Keep index-only latency separate from
embedding and full task cost. A useful regression need not include Rust code:
a public fixture and exact expected result can be contributed first.

## Submit a focused pull request

Use imperative commit subjects, such as `Fix citation offsets after reindex`.
Describe the concrete failure, changed behavior, tests, and compatibility or
operational limits. Format only touched Rust files; run Clippy on the affected
scope. Before adding files, run `python3 scripts/check-sensitive-files.py`;
run it again after staging because it examines Git-tracked paths.

Do not commit local databases, credentials, generated QA output, reference
clones, or internal design records. Do not change platform/capacity claims
without qualification evidence. Memory remains experimental and off by default.

Maintainers track reproduction, regression coverage, fix version and reporter
confirmation separately. Stars, downloads and an unreproduced report are not
evidence that a workload is supported or a defect is fixed.

Review adoption monthly using voluntarily shared, still-active external projects,
time to reproduce and fix confirmed defects, and the recorded Fabric lifecycle
results. Keep reporter confirmation separate from maintainer test results.
No automatic telemetry or report submission is needed for this workflow.
