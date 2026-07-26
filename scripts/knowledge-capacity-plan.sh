#!/usr/bin/env bash
set -euo pipefail

if (( $# < 4 || $# > 6 )); then
  echo "usage: $0 VECTORS DIMENSIONS f32|f16 HOST_RAM_GIB [AVAILABLE_DISK_GIB] [REPLICAS]" >&2
  exit 1
fi

vectors="$1"
dimensions="$2"
precision="$3"
host_ram_gib="$4"
available_disk_gib="${5:-320}"
replicas="${6:-3}"

for value in "$vectors" "$dimensions" "$host_ram_gib" "$available_disk_gib" "$replicas"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "numeric inputs must be positive integers" >&2
    exit 1
  fi
done

case "$precision" in
  f32) precision_bytes=4 ;;
  f16) precision_bytes=2 ;;
  *)
    echo "precision must be f32 or f16" >&2
    exit 1
    ;;
esac

# The planning model is intentionally conservative. It accounts for vector
# payload, HNSW links (M=16), per-record RocksDB/BM25/metadata allowance,
# immutable active+shadow disk amplification, and a 30% runtime RAM reserve.
awk \
  -v vectors="$vectors" \
  -v dimensions="$dimensions" \
  -v precision="$precision" \
  -v precision_bytes="$precision_bytes" \
  -v host_ram_gib="$host_ram_gib" \
  -v available_disk_gib="$available_disk_gib" \
  -v replicas="$replicas" '
  BEGIN {
    gib = 1024 * 1024 * 1024
    vector_bytes = vectors * dimensions * precision_bytes
    hnsw_bytes = vectors * 16 * 8
    record_bytes = vectors * 768
    steady_ram = vector_bytes + hnsw_bytes + record_bytes
    peak_build_ram = steady_ram * 1.65
    generation_disk = (vector_bytes + hnsw_bytes + record_bytes) * 2.2
    active_shadow_disk = generation_disk * 2
    ram_budget = host_ram_gib * gib * 0.70
    disk_budget = available_disk_gib * gib - 25 * gib
    ram_ok = peak_build_ram <= ram_budget
    disk_ok = active_shadow_disk <= disk_budget
    admitted = ram_ok && disk_ok
    printf "{\n"
    printf "  \"vectors\": %.0f,\n", vectors
    printf "  \"dimensions\": %.0f,\n", dimensions
    printf "  \"precision\": \"%s\",\n", precision
    printf "  \"replicas\": %.0f,\n", replicas
    printf "  \"raw_vector_gib\": %.3f,\n", vector_bytes / gib
    printf "  \"estimated_steady_ram_gib\": %.3f,\n", steady_ram / gib
    printf "  \"estimated_peak_build_ram_gib\": %.3f,\n", peak_build_ram / gib
    printf "  \"estimated_active_plus_shadow_disk_gib\": %.3f,\n", active_shadow_disk / gib
    printf "  \"host_ram_budget_gib\": %.3f,\n", ram_budget / gib
    printf "  \"host_disk_budget_gib\": %.3f,\n", disk_budget / gib
    printf "  \"ram_admitted\": %s,\n", ram_ok ? "true" : "false"
    printf "  \"disk_admitted\": %s,\n", disk_ok ? "true" : "false"
    printf "  \"admitted\": %s,\n", admitted ? "true" : "false"
    printf "  \"note\": \"planning estimate; qualification measurements remain authoritative\"\n"
    printf "}\n"
    exit admitted ? 0 : 2
  }'
