#!/usr/bin/env bash
# Run local AkiDB quality gates.
#
# Vector quality always runs. Semantic text retrieval runs when
# AX_ENGINE_MODEL_DIR is set, or is required with --require-text.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD=false
REQUIRE_TEXT=false

for arg in "$@"; do
    case "$arg" in
        --build) BUILD=true ;;
        --require-text) REQUIRE_TEXT=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT"
mkdir -p qa-results

echo "=== QA Gate 1: Vector index quality ==="
if [ "$BUILD" = true ]; then
    python3 scripts/qa_vector_quality.py --build
else
    python3 scripts/qa_vector_quality.py
fi

echo ""
echo "=== QA Gate 1b: Correctness KPI table (ingest + Get + recall) ==="
if [ "$BUILD" = true ]; then
    python3 scripts/qa_correctness_kpi.py --build
else
    python3 scripts/qa_correctness_kpi.py
fi

if [ -n "${AX_ENGINE_MODEL_DIR:-}" ]; then
    echo ""
    echo "=== QA Gate 2: Semantic text retrieval ==="
    python3 scripts/qa_text_retrieval.py
else
    echo ""
    echo "=== QA Gate 2: Semantic text retrieval SKIPPED ==="
    echo "AX_ENGINE_MODEL_DIR is not set."
    if [ "$REQUIRE_TEXT" = true ]; then
        echo "ERROR: --require-text requires AX_ENGINE_MODEL_DIR."
        exit 1
    fi
fi

echo ""
echo "=== QA gates complete ==="
