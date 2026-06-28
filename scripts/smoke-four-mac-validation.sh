#!/usr/bin/env bash
# Exercise the four-Mac evidence pipeline with synthetic inputs. This does not
# prove hardware readiness; it verifies that the artifact builders and validators
# still compose end-to-end.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-four-mac-smoke.XXXXXX")"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

ONE_MAC_ARTIFACT="${ONE_MAC_ARTIFACT:-$PROJECT_ROOT/docs/reports/one-mac-768d-1000000v-c1-20260628T002236Z.json}"
CELL_QPS="${CELL_QPS:-2600.0}"

INPUT="$TMP_DIR/four-mac-input.json"
CELL_BENCHMARK="$TMP_DIR/four-mac-benchmark.json"
FINAL_ARTIFACT="$TMP_DIR/four-mac-cell.json"
BAD_CELL_BENCHMARK="$TMP_DIR/four-mac-benchmark-bad-topk.json"
BAD_FINAL_ARTIFACT="$TMP_DIR/four-mac-cell-bad-topk.json"
BAD_BOOLEAN_INPUT="$TMP_DIR/four-mac-input-bad-boolean.json"
BAD_BOOLEAN_FINAL_ARTIFACT="$TMP_DIR/four-mac-cell-bad-boolean.json"
BAD_NUMERIC_INPUT="$TMP_DIR/four-mac-input-bad-numeric.json"
BAD_NUMERIC_FINAL_ARTIFACT="$TMP_DIR/four-mac-cell-bad-numeric.json"

python3 "$PROJECT_ROOT/scripts/build-four-mac-cell-artifact.py" \
    --write-template "$INPUT"

python3 - "$ONE_MAC_ARTIFACT" "$CELL_BENCHMARK" "$CELL_QPS" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
cell_qps = float(sys.argv[3])
document = json.loads(source.read_text())
document["server"] = "synthetic-four-mac-smoke"
document["search"]["throughput_queries_per_sec"] = cell_qps
target.write_text(json.dumps(document, indent=2) + "\n")
PY

python3 "$PROJECT_ROOT/scripts/validate-four-mac-evidence.py" \
    --input "$INPUT" \
    --one-mac-artifact "$ONE_MAC_ARTIFACT" \
    --cell-benchmark-artifact "$CELL_BENCHMARK" \
    --output "$FINAL_ARTIFACT" \
    --max-cell-p95-ms 1000 \
    --max-cell-p99-ms 1000 \
    --min-cell-slo-compliance 0

python3 - "$CELL_BENCHMARK" "$BAD_CELL_BENCHMARK" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
document = json.loads(source.read_text())
document["search"]["top_k"] = int(document["search"]["top_k"]) + 1
target.write_text(json.dumps(document, indent=2) + "\n")
PY

if python3 "$PROJECT_ROOT/scripts/validate-four-mac-evidence.py" \
    --input "$INPUT" \
    --one-mac-artifact "$ONE_MAC_ARTIFACT" \
    --cell-benchmark-artifact "$BAD_CELL_BENCHMARK" \
    --output "$BAD_FINAL_ARTIFACT" \
    --max-cell-p95-ms 1000 \
    --max-cell-p99-ms 1000 \
    --min-cell-slo-compliance 0 >/tmp/akidb-four-mac-smoke-bad.log 2>&1; then
    echo "ERROR: mismatched cell benchmark workload unexpectedly passed"
    cat /tmp/akidb-four-mac-smoke-bad.log
    exit 1
fi

python3 - "$INPUT" "$BAD_BOOLEAN_INPUT" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
document = json.loads(source.read_text())
document["nodes"][0]["healthy"] = "false"
target.write_text(json.dumps(document, indent=2) + "\n")
PY

if python3 "$PROJECT_ROOT/scripts/validate-four-mac-evidence.py" \
    --input "$BAD_BOOLEAN_INPUT" \
    --one-mac-artifact "$ONE_MAC_ARTIFACT" \
    --cell-benchmark-artifact "$CELL_BENCHMARK" \
    --output "$BAD_BOOLEAN_FINAL_ARTIFACT" \
    --max-cell-p95-ms 1000 \
    --max-cell-p99-ms 1000 \
    --min-cell-slo-compliance 0 >/tmp/akidb-four-mac-smoke-bad-boolean.log 2>&1; then
    echo "ERROR: string boolean cell health unexpectedly passed"
    cat /tmp/akidb-four-mac-smoke-bad-boolean.log
    exit 1
fi

python3 - "$INPUT" "$BAD_NUMERIC_INPUT" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
document = json.loads(source.read_text())
document["links"][0]["bandwidth_gbps"] = "20"
target.write_text(json.dumps(document, indent=2) + "\n")
PY

if python3 "$PROJECT_ROOT/scripts/validate-four-mac-evidence.py" \
    --input "$BAD_NUMERIC_INPUT" \
    --one-mac-artifact "$ONE_MAC_ARTIFACT" \
    --cell-benchmark-artifact "$CELL_BENCHMARK" \
    --output "$BAD_NUMERIC_FINAL_ARTIFACT" \
    --max-cell-p95-ms 1000 \
    --max-cell-p99-ms 1000 \
    --min-cell-slo-compliance 0 >/tmp/akidb-four-mac-smoke-bad-numeric.log 2>&1; then
    echo "ERROR: string numeric link measurement unexpectedly passed"
    cat /tmp/akidb-four-mac-smoke-bad-numeric.log
    exit 1
fi

echo "validated four-Mac validation smoke pipeline: $FINAL_ARTIFACT"
