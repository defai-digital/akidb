#!/usr/bin/env bash
# Run local AkiDB quality gates for the current product surface.
#
# Always-on gates:
#   1) dense vector quality (Recall@K / nDCG)
#   1b) correctness KPI table (ingest / Get / retrieval integrity)
#   1c) feature matrix (CRUD, filters, BM25, score threshold)
#   1d) filtered search live purity + recall
#
# Optional:
#   2) semantic TextSearch when AX_ENGINE_MODEL_DIR is set (--require-text enforces)
#   --with-cargo also runs quality-knobs + code-retrieval cargo-backed gates
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD=false
REQUIRE_TEXT=false
WITH_CARGO=false

for arg in "$@"; do
    case "$arg" in
        --build) BUILD=true ;;
        --require-text) REQUIRE_TEXT=true ;;
        --with-cargo) WITH_CARGO=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT"
mkdir -p qa-results

build_flag=()
if [ "$BUILD" = true ]; then
    build_flag=(--build)
fi

echo "=== QA Gate 1: Vector index quality ==="
python3 scripts/qa_vector_quality.py "${build_flag[@]+"${build_flag[@]}"}"

echo ""
echo "=== QA Gate 1b: Correctness KPI table ==="
python3 scripts/qa_correctness_kpi.py "${build_flag[@]+"${build_flag[@]}"}" \
    --collection default

echo ""
echo "=== QA Gate 1c: Feature matrix (CRUD / filter / BM25) ==="
python3 scripts/qa_feature_matrix.py "${build_flag[@]+"${build_flag[@]}"}"

echo ""
echo "=== QA Gate 1d: Filtered search (live) ==="
python3 scripts/qa_filtered_search.py "${build_flag[@]+"${build_flag[@]}"}" \
    --vectors 150 --queries 15 --dimensions 32 --buckets 10 \
    --output qa-results/filtered-search-quality.json

if [ -n "${AX_ENGINE_MODEL_DIR:-}" ]; then
    echo ""
    echo "=== QA Gate 2: Semantic text retrieval ==="
    python3 scripts/qa_text_retrieval.py "${build_flag[@]+"${build_flag[@]}"}"
else
    echo ""
    echo "=== QA Gate 2: Semantic text retrieval SKIPPED ==="
    echo "AX_ENGINE_MODEL_DIR is not set."
    if [ "$REQUIRE_TEXT" = true ]; then
        echo "ERROR: --require-text requires AX_ENGINE_MODEL_DIR."
        exit 1
    fi
fi

if [ "$WITH_CARGO" = true ]; then
    echo ""
    echo "=== QA Gate 3: Quality knobs (cargo) ==="
    python3 scripts/qa_quality_knobs.py

    echo ""
    echo "=== QA Gate 4: Code retrieval fixtures (cargo) ==="
    python3 scripts/qa_code_retrieval.py
fi

echo ""
echo "=== QA gates complete ==="
echo "See docs/quality/feature-qa-matrix.md for the feature → gate map."
