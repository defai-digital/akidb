# Four-Mac Evidence Manifest

This manifest is the operator checklist for the real four-Mac Thunderbolt cell
run. It complements `four-mac-cell-validation.md`: the validation document
defines gates and schema, while this manifest defines the files that must exist
before AkiDB can mark the four-Mac cell validation complete.

Do not use synthetic smoke artifacts to close the README checkbox. The checkbox
can be marked complete only after the final artifact is built from measured
four-Mac hardware inputs and passes `validate-four-mac-evidence.py`.

## Evidence Directory

Use one timestamped directory per run:

```bash
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "docs/reports/four-mac-${RUN_ID}"
```

The completed run should contain:

| File | Producer | Required |
|------|----------|----------|
| `mac-1-node.json` ... `mac-4-node.json` | `collect-four-mac-node.py` | Yes |
| `mac-1-mac-2-link.json` ... `mac-3-mac-4-link.json` | `collect-four-mac-link.py` | Yes |
| `node-loss-test.json` | `collect-four-mac-failure-test.py` | Yes |
| `link-loss-test.json` | `collect-four-mac-failure-test.py` | Yes |
| `four-mac-input.json` | `assemble-four-mac-input.py` | Yes |
| `four-mac-benchmark.json` | `benchmark-four-mac-cell.sh` | Yes |
| `four-mac-cell.json` | `validate-four-mac-evidence.py` | Yes |

The one-Mac reference artifact can stay in `docs/reports/` and should be
referenced by path when building the final evidence bundle.

## Collection Order

1. Confirm all four Macs are Apple Silicon, healthy, and connected through the
   intended Thunderbolt topology.
2. Collect one node inventory JSON on each Mac.
3. Measure all six unique Thunderbolt node pairs and write one link JSON per
   pair.
4. Run at least one node-loss and one link-loss degraded-mode test.
5. Assemble `four-mac-input.json`.
6. Run the cell benchmark against the already-running four-Mac endpoint.
7. Build and validate the final `four-mac-cell.json` artifact.

## Required Commands

Collect node inventory on each target machine:

```bash
python3 scripts/collect-four-mac-node.py \
  --id mac-1 \
  --role voter \
  --output "docs/reports/four-mac-${RUN_ID}/mac-1-node.json"
```

Collect one file for every required link pair:

```bash
python3 scripts/collect-four-mac-link.py \
  --from mac-1 \
  --to mac-2 \
  --latency-p95-us 120 \
  --bandwidth-gbps 20 \
  --packet-loss-percent 0 \
  --output "docs/reports/four-mac-${RUN_ID}/mac-1-mac-2-link.json"
```

Collect degraded-mode failure tests:

```bash
python3 scripts/collect-four-mac-failure-test.py \
  --kind node_loss \
  --observed-status degraded \
  --recovery-time-ms 500 \
  --output "docs/reports/four-mac-${RUN_ID}/node-loss-test.json"

python3 scripts/collect-four-mac-failure-test.py \
  --kind link_loss \
  --observed-status degraded \
  --recovery-time-ms 250 \
  --output "docs/reports/four-mac-${RUN_ID}/link-loss-test.json"
```

Assemble the measured input file:

```bash
python3 scripts/assemble-four-mac-input.py \
  --node "docs/reports/four-mac-${RUN_ID}/mac-1-node.json" \
  --node "docs/reports/four-mac-${RUN_ID}/mac-2-node.json" \
  --node "docs/reports/four-mac-${RUN_ID}/mac-3-node.json" \
  --node "docs/reports/four-mac-${RUN_ID}/mac-4-node.json" \
  --link "docs/reports/four-mac-${RUN_ID}/mac-1-mac-2-link.json" \
  --link "docs/reports/four-mac-${RUN_ID}/mac-1-mac-3-link.json" \
  --link "docs/reports/four-mac-${RUN_ID}/mac-1-mac-4-link.json" \
  --link "docs/reports/four-mac-${RUN_ID}/mac-2-mac-3-link.json" \
  --link "docs/reports/four-mac-${RUN_ID}/mac-2-mac-4-link.json" \
  --link "docs/reports/four-mac-${RUN_ID}/mac-3-mac-4-link.json" \
  --failure-test "docs/reports/four-mac-${RUN_ID}/node-loss-test.json" \
  --failure-test "docs/reports/four-mac-${RUN_ID}/link-loss-test.json" \
  --output "docs/reports/four-mac-${RUN_ID}/four-mac-input.json"
```

Run the benchmark against the existing cell endpoint:

```bash
OUTPUT="docs/reports/four-mac-${RUN_ID}/four-mac-benchmark.json" \
SERVER=http://mac-1.local:50051 \
./scripts/benchmark-four-mac-cell.sh
```

Build and validate the final evidence artifact:

```bash
python3 scripts/validate-four-mac-evidence.py \
  --input "docs/reports/four-mac-${RUN_ID}/four-mac-input.json" \
  --one-mac-artifact docs/reports/one-mac-768d-1000000v-c1-20260628T002236Z.json \
  --cell-benchmark-artifact "docs/reports/four-mac-${RUN_ID}/four-mac-benchmark.json" \
  --output "docs/reports/four-mac-${RUN_ID}/four-mac-cell.json"
```

## Completion Criteria

The four-Mac validation remains pending unless all of these are true:

- `four-mac-cell.json` was generated from measured hardware inputs, not the
  synthetic smoke script.
- `validate-four-mac-evidence.py` exits successfully without skipped gates.
- The final artifact references the checked-in one-Mac baseline.
- The cell benchmark uses the same workload shape as the one-Mac baseline.
- The final throughput ratio is at least `2.5x` the one-Mac reference.
- All six Thunderbolt links and both degraded-mode tests are present.

