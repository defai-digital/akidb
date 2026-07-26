#!/usr/bin/env python3
"""Run a reproducible SIFT-style ANN benchmark against Milvus or Weaviate.

This is a qualification driver, not a vendor wrapper used by AkiDB at runtime.
It deliberately uses the same fvecs/ivecs inputs, exact Recall@K calculation,
warm-up, measurement rounds, concurrency, and result-integrity checks as
``akidb-ann-bench``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib.metadata
import json
import math
import os
import statistics
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence


FILTER_MODULI = (2, 20, 100)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("milvus", "weaviate"), required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--http-port", type=int)
    parser.add_argument("--grpc-port", type=int, required=True)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--train-fvecs", type=Path, required=True)
    parser.add_argument("--query-fvecs", type=Path, required=True)
    parser.add_argument("--neighbors-ivecs", type=Path, required=True)
    parser.add_argument("--metric", choices=("l2",), default="l2")
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--warmup-queries", type=int, default=1000)
    parser.add_argument("--measurement-rounds", type=int, default=3)
    parser.add_argument("--search-efs", default="32,64,128,256")
    parser.add_argument("--post-load-settle-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--index-ready-timeout-seconds", type=int, default=1800)
    parser.add_argument("--hnsw-m", type=int, default=16)
    parser.add_argument("--hnsw-ef-construction", type=int, default=128)
    parser.add_argument(
        "--min-recall",
        type=float,
        default=0.0,
        help="Optional per-point gate; parity selection applies its own recall band",
    )
    parser.add_argument("--include-filters", action="store_true")
    parser.add_argument("--output-json", type=Path, required=True)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> list[int]:
    if not args.host or args.host.strip() != args.host:
        raise ValueError("--host must be canonical")
    if args.engine == "weaviate" and args.http_port is None:
        raise ValueError("--http-port is required for Weaviate")
    for name in (
        "grpc_port",
        "batch_size",
        "concurrency",
        "warmup_queries",
        "measurement_rounds",
        "timeout_seconds",
        "index_ready_timeout_seconds",
        "hnsw_m",
        "hnsw_ef_construction",
    ):
        if getattr(args, name) < 1:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")
    if args.post_load_settle_seconds < 0:
        raise ValueError("--post-load-settle-seconds cannot be negative")
    if not 0.0 <= args.min_recall <= 1.0:
        raise ValueError("--min-recall must be between zero and one")
    if (
        not args.collection
        or len(args.collection) > 128
        or not args.collection.replace("_", "").isalnum()
    ):
        raise ValueError("--collection must contain only letters, digits, or underscore")
    search_efs = parse_positive_ints(args.search_efs)
    if len(search_efs) != len(set(search_efs)):
        raise ValueError("--search-efs cannot contain duplicates")
    for path in (args.train_fvecs, args.query_fvecs, args.neighbors_ivecs):
        if not path.is_file():
            raise ValueError(f"dataset file does not exist: {path}")
    return search_efs


def parse_positive_ints(value: str) -> list[int]:
    try:
        values = [int(item) for item in value.split(",")]
    except ValueError as error:
        raise ValueError("search breadth values must be integers") from error
    if not values or any(item < 1 for item in values):
        raise ValueError("search breadth values must be positive")
    return values


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def file_identity(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


@dataclass(frozen=True)
class MatrixFile:
    path: Path
    values: Any
    rows: int
    width: int


def map_matrix(path: Path, *, floating: bool) -> MatrixFile:
    import numpy as np

    if path.stat().st_size < 8:
        raise ValueError(f"{path} is too small")
    raw = np.memmap(path, dtype="<i4", mode="r")
    width = int(raw[0])
    if width < 1 or width > 16_384:
        raise ValueError(f"{path} has invalid row width {width}")
    record_width = width + 1
    if raw.size % record_width:
        raise ValueError(f"{path} has a truncated record")
    records = raw.reshape((-1, record_width))
    if not bool(np.all(records[:, 0] == width)):
        raise ValueError(f"{path} has inconsistent row widths")
    values = records[:, 1:]
    if floating:
        values = values.view("<f4")
    return MatrixFile(path=path, values=values, rows=records.shape[0], width=width)


def filtered_ground_truth(
    neighbors: Sequence[int], top_k: int, modulus: int | None
) -> list[int]:
    if modulus is None:
        return [int(value) for value in neighbors[:top_k]]
    target = int(neighbors[0]) % modulus
    return [
        int(value)
        for value in neighbors
        if int(value) % modulus == target
    ][:top_k]


def validate_ground_truth(
    neighbors: Any, train_rows: int, workloads: Sequence[tuple[int, int | None]]
) -> None:
    for row_number, row in enumerate(neighbors):
        values = [int(value) for value in row]
        if any(value < 0 or value >= train_rows for value in values):
            raise ValueError(f"ground truth row {row_number} contains an invalid ID")
        if len(values) != len(set(values)):
            raise ValueError(f"ground truth row {row_number} contains duplicate IDs")
        for top_k, modulus in workloads:
            if len(filtered_ground_truth(values, top_k, modulus)) != top_k:
                label = "unfiltered" if modulus is None else f"modulus {modulus}"
                raise ValueError(
                    f"ground truth row {row_number} lacks top-{top_k} for {label}"
                )


def recall_at_k(returned: set[int], expected: Sequence[int], top_k: int) -> float:
    if top_k < 1:
        raise ValueError("top_k must be positive")
    return len(returned.intersection(expected)) / top_k


def percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return float(ordered[index])


def latency_report(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "min_ms": 0.0,
            "mean_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "count": len(values),
        "min_ms": min(values),
        "mean_ms": statistics.fmean(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": max(values),
    }


@dataclass(frozen=True)
class SearchHit:
    row_id: int
    distance: float
    filter_value: int | None


class Adapter(Protocol):
    client_version: str

    def server_version(self) -> str: ...

    def create(self, dimensions: int, initial_ef: int) -> None: ...

    def insert(self, rows: Any, start_id: int) -> tuple[int, int]: ...

    def finalize(self, expected_count: int, timeout_seconds: int) -> int: ...

    def set_search_ef(self, value: int) -> None: ...

    def search(
        self,
        vector: Sequence[float],
        top_k: int,
        search_ef: int,
        filter_modulus: int | None,
        filter_target: int | None,
    ) -> list[SearchHit]: ...

    def count(self) -> int: ...

    def close(self) -> None: ...


class MilvusAdapter:
    def __init__(self, args: argparse.Namespace):
        from pymilvus import MilvusClient

        self.args = args
        self.client_version = importlib.metadata.version("pymilvus")
        self.client = MilvusClient(
            uri=f"http://{args.host}:{args.grpc_port}",
            timeout=args.timeout_seconds,
        )

    def server_version(self) -> str:
        return str(self.client.get_server_version())

    def create(self, dimensions: int, initial_ef: int) -> None:
        from pymilvus import DataType, MilvusClient

        if self.client.has_collection(self.args.collection):
            raise RuntimeError("Milvus qualification collection already exists")
        schema = MilvusClient.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dimensions)
        for modulus in FILTER_MODULI:
            schema.add_field(f"label_{modulus}", DataType.INT64)
        indexes = self.client.prepare_index_params()
        indexes.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="L2",
            params={
                "M": self.args.hnsw_m,
                "efConstruction": self.args.hnsw_ef_construction,
            },
        )
        for modulus in FILTER_MODULI:
            indexes.add_index(field_name=f"label_{modulus}", index_type="INVERTED")
        self.client.create_collection(
            collection_name=self.args.collection,
            schema=schema,
            index_params=indexes,
            consistency_level="Strong",
        )

    def insert(self, rows: Any, start_id: int) -> tuple[int, int]:
        data = []
        for offset, vector in enumerate(rows):
            row_id = start_id + offset
            data.append(
                {
                    "id": row_id,
                    "vector": vector.tolist(),
                    "label_2": row_id % 2,
                    "label_20": row_id % 20,
                    "label_100": row_id % 100,
                }
            )
        result = self.client.insert(
            collection_name=self.args.collection,
            data=data,
        )
        inserted = int(result.get("insert_count", 0))
        return inserted, len(data) - inserted

    def finalize(self, expected_count: int, timeout_seconds: int) -> int:
        self.client.flush(collection_name=self.args.collection)
        self.client.load_collection(collection_name=self.args.collection)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            count = self.count()
            state = str(
                self.client.get_load_state(collection_name=self.args.collection)
            ).lower()
            if count == expected_count and "loaded" in state:
                return count
            time.sleep(2)
        raise TimeoutError("Milvus did not reach the expected loaded row count")

    def set_search_ef(self, value: int) -> None:
        del value

    def search(
        self,
        vector: Sequence[float],
        top_k: int,
        search_ef: int,
        filter_modulus: int | None,
        filter_target: int | None,
    ) -> list[SearchHit]:
        expression = ""
        output_fields = ["id"]
        if filter_modulus is not None:
            expression = f"label_{filter_modulus} == {filter_target}"
            output_fields.append(f"label_{filter_modulus}")
        values = self.client.search(
            collection_name=self.args.collection,
            data=[vector],
            anns_field="vector",
            filter=expression,
            limit=top_k,
            output_fields=output_fields,
            search_params={"metric_type": "L2", "params": {"ef": search_ef}},
            consistency_level="Strong",
        )
        hits = []
        for value in values[0] if values else []:
            entity = value.get("entity") or {}
            row_id = int(value.get("id", entity.get("id")))
            label = None
            if filter_modulus is not None:
                label = int(entity[f"label_{filter_modulus}"])
            hits.append(
                SearchHit(
                    row_id=row_id,
                    distance=float(value["distance"]),
                    filter_value=label,
                )
            )
        return hits

    def count(self) -> int:
        stats = self.client.get_collection_stats(
            collection_name=self.args.collection
        )
        return int(stats["row_count"])

    def close(self) -> None:
        self.client.close()


class WeaviateAdapter:
    def __init__(self, args: argparse.Namespace):
        import weaviate

        self.args = args
        self.client_version = importlib.metadata.version("weaviate-client")
        self.client = weaviate.connect_to_custom(
            http_host=args.host,
            http_port=args.http_port,
            http_secure=False,
            grpc_host=args.host,
            grpc_port=args.grpc_port,
            grpc_secure=False,
        )
        self.collection: Any = None

    def server_version(self) -> str:
        meta = self.client.get_meta()
        return str(meta.get("version", "unknown"))

    def create(self, dimensions: int, initial_ef: int) -> None:
        import weaviate.classes as wvc

        del dimensions
        if self.client.collections.exists(self.args.collection):
            raise RuntimeError("Weaviate qualification collection already exists")
        properties = [
            wvc.config.Property(
                name="row_id",
                data_type=wvc.config.DataType.INT,
                index_filterable=True,
            )
        ]
        properties.extend(
            wvc.config.Property(
                name=f"label_{modulus}",
                data_type=wvc.config.DataType.INT,
                index_filterable=True,
            )
            for modulus in FILTER_MODULI
        )
        self.collection = self.client.collections.create(
            name=self.args.collection,
            properties=properties,
            vector_config=wvc.config.Configure.Vectors.self_provided(
                vector_index_config=wvc.config.Configure.VectorIndex.hnsw(
                    distance_metric=wvc.config.VectorDistances.L2_SQUARED,
                    ef=initial_ef,
                    ef_construction=self.args.hnsw_ef_construction,
                    max_connections=self.args.hnsw_m,
                    vector_cache_max_objects=2_000_000,
                )
            ),
        )

    def insert(self, rows: Any, start_id: int) -> tuple[int, int]:
        import weaviate.classes as wvc

        objects = []
        for offset, vector in enumerate(rows):
            row_id = start_id + offset
            objects.append(
                wvc.data.DataObject(
                    uuid=uuid.UUID(int=row_id + 1),
                    properties={
                        "row_id": row_id,
                        "label_2": row_id % 2,
                        "label_20": row_id % 20,
                        "label_100": row_id % 100,
                    },
                    vector=vector.tolist(),
                )
            )
        response = self.collection.data.insert_many(objects)
        failures = len(response.errors)
        return len(rows) - failures, failures

    def finalize(self, expected_count: int, timeout_seconds: int) -> int:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            count = self.count()
            if count == expected_count:
                return count
            time.sleep(2)
        raise TimeoutError("Weaviate did not reach the expected object count")

    def set_search_ef(self, value: int) -> None:
        import weaviate.classes as wvc

        self.collection.config.update(
            vector_config=wvc.config.Reconfigure.Vectors.update(
                vector_index_config=wvc.config.Reconfigure.VectorIndex.hnsw(ef=value)
            )
        )

    def search(
        self,
        vector: Sequence[float],
        top_k: int,
        search_ef: int,
        filter_modulus: int | None,
        filter_target: int | None,
    ) -> list[SearchHit]:
        import weaviate.classes as wvc

        del search_ef
        filters = None
        properties = ["row_id"]
        if filter_modulus is not None:
            field = f"label_{filter_modulus}"
            filters = wvc.query.Filter.by_property(field).equal(filter_target)
            properties.append(field)
        result = self.collection.query.near_vector(
            near_vector=vector,
            limit=top_k,
            filters=filters,
            return_metadata=wvc.query.MetadataQuery(distance=True),
            return_properties=properties,
        )
        hits = []
        for value in result.objects:
            label = None
            if filter_modulus is not None:
                label = int(value.properties[f"label_{filter_modulus}"])
            hits.append(
                SearchHit(
                    row_id=int(value.properties["row_id"]),
                    distance=float(value.metadata.distance),
                    filter_value=label,
                )
            )
        return hits

    def count(self) -> int:
        return int(self.collection.aggregate.over_all(total_count=True).total_count)

    def close(self) -> None:
        self.client.close()


@dataclass
class Measurements:
    latencies: list[float]
    recall_sum: float = 0.0
    succeeded: int = 0
    failed: int = 0
    filter_violations: int = 0
    result_count_violations: int = 0
    duplicate_results: int = 0
    unparseable_results: int = 0
    invalid_scores: int = 0

    def merge(self, other: "Measurements") -> None:
        self.latencies.extend(other.latencies)
        for field in (
            "recall_sum",
            "succeeded",
            "failed",
            "filter_violations",
            "result_count_violations",
            "duplicate_results",
            "unparseable_results",
            "invalid_scores",
        ):
            setattr(self, field, getattr(self, field) + getattr(other, field))


def query_once(
    adapter: Adapter,
    vector: Any,
    neighbors: Any,
    *,
    top_k: int,
    search_ef: int,
    filter_modulus: int | None,
) -> tuple[float, list[SearchHit]]:
    target = None if filter_modulus is None else int(neighbors[0]) % filter_modulus
    started = time.perf_counter()
    hits = adapter.search(
        vector,
        top_k,
        search_ef,
        filter_modulus,
        target,
    )
    return (time.perf_counter() - started) * 1000.0, hits


def validate_hits(
    result: Measurements,
    hits: Sequence[SearchHit],
    neighbors: Any,
    *,
    train_rows: int,
    top_k: int,
    filter_modulus: int | None,
) -> None:
    if len(hits) != top_k:
        result.result_count_violations += 1
    rows = []
    target = None if filter_modulus is None else int(neighbors[0]) % filter_modulus
    for hit in hits:
        if not isinstance(hit.row_id, int) or not 0 <= hit.row_id < train_rows:
            result.unparseable_results += 1
            continue
        rows.append(hit.row_id)
        if not math.isfinite(hit.distance):
            result.invalid_scores += 1
        if filter_modulus is not None and (
            hit.row_id % filter_modulus != target or hit.filter_value != target
        ):
            result.filter_violations += 1
    unique = set(rows)
    result.duplicate_results += len(rows) - len(unique)
    expected = filtered_ground_truth(neighbors, top_k, filter_modulus)
    result.recall_sum += recall_at_k(unique, expected, top_k)


def warmup(
    adapter: Adapter,
    queries: Any,
    neighbors: Any,
    *,
    count: int,
    top_k: int,
    search_ef: int,
    filter_modulus: int | None,
) -> int:
    measured = min(count, len(queries))
    for index in range(measured):
        query_once(
            adapter,
            queries[index],
            neighbors[index],
            top_k=top_k,
            search_ef=search_ef,
            filter_modulus=filter_modulus,
        )
    return measured


def measure(
    adapter: Adapter,
    queries: Any,
    neighbors: Any,
    *,
    train_rows: int,
    top_k: int,
    search_ef: int,
    filter_modulus: int | None,
    concurrency: int,
    rounds: int,
    warmup_queries: int,
) -> dict[str, Any]:
    adapter.set_search_ef(search_ef)
    warmed = warmup(
        adapter,
        queries,
        neighbors,
        count=warmup_queries,
        top_k=top_k,
        search_ef=search_ef,
        filter_modulus=filter_modulus,
    )
    total = len(queries) * rounds
    next_task = 0
    task_lock = threading.Lock()

    def worker() -> Measurements:
        nonlocal next_task
        local = Measurements(latencies=[])
        while True:
            with task_lock:
                task = next_task
                next_task += 1
            if task >= total:
                break
            index = task % len(queries)
            try:
                latency, hits = query_once(
                    adapter,
                    queries[index],
                    neighbors[index],
                    top_k=top_k,
                    search_ef=search_ef,
                    filter_modulus=filter_modulus,
                )
                local.latencies.append(latency)
                validate_hits(
                    local,
                    hits,
                    neighbors[index],
                    train_rows=train_rows,
                    top_k=top_k,
                    filter_modulus=filter_modulus,
                )
                local.succeeded += 1
            except Exception:
                local.failed += 1
        return local

    started = time.perf_counter()
    combined = Measurements(latencies=[])
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        for value in pool.map(lambda _: worker(), range(concurrency)):
            combined.merge(value)
    elapsed = time.perf_counter() - started
    recall = (
        combined.recall_sum / combined.succeeded if combined.succeeded else 0.0
    )
    return {
        "requested": total,
        "unique_queries": len(queries),
        "measurement_rounds": rounds,
        "succeeded": combined.succeeded,
        "failed": combined.failed,
        "concurrency": concurrency,
        "warmup_queries": warmed,
        "top_k": top_k,
        "search_ef": search_ef,
        "filter": {
            "enabled": filter_modulus is not None,
            "metadata_key": (
                None if filter_modulus is None else f"label_{filter_modulus}"
            ),
            "modulus": filter_modulus,
            "expected_selectivity": (
                None if filter_modulus is None else 1.0 / filter_modulus
            ),
        },
        "duration_ms": round(elapsed * 1000),
        "qps": combined.succeeded / elapsed,
        "recall_at_k": recall,
        "filter_violations": combined.filter_violations,
        "result_count_violations": combined.result_count_violations,
        "duplicate_results": combined.duplicate_results,
        "unparseable_results": combined.unparseable_results,
        "invalid_scores": combined.invalid_scores,
        "latency": latency_report(combined.latencies),
    }


def point_failures(point: dict[str, Any], min_recall: float) -> list[str]:
    failures = []
    for field in (
        "failed",
        "filter_violations",
        "result_count_violations",
        "duplicate_results",
        "unparseable_results",
        "invalid_scores",
    ):
        if point[field]:
            failures.append(f"{field}={point[field]}")
    if point["succeeded"] != point["requested"]:
        failures.append(
            f"succeeded={point['succeeded']} requested={point['requested']}"
        )
    if min_recall > 0 and point["recall_at_k"] < min_recall:
        failures.append(
            f"Recall@{point['top_k']}={point['recall_at_k']:.6f} < {min_recall}"
        )
    return failures


def adapter_for(args: argparse.Namespace) -> Adapter:
    if args.engine == "milvus":
        return MilvusAdapter(args)
    return WeaviateAdapter(args)


def main() -> None:
    args = arguments()
    try:
        search_efs = validate_args(args)
        train = map_matrix(args.train_fvecs, floating=True)
        queries = map_matrix(args.query_fvecs, floating=True)
        neighbors = map_matrix(args.neighbors_ivecs, floating=False)
        if train.width != queries.width:
            raise ValueError("train and query dimensions differ")
        if queries.rows != neighbors.rows:
            raise ValueError("query and ground-truth row counts differ")
        workloads = [(10, None)]
        if args.include_filters:
            workloads.extend(((10, 2), (1, 20), (1, 100)))
        validate_ground_truth(neighbors.values, train.rows, workloads)
        # Match the Rust driver: query and truth rows are fully decoded before
        # warm-up and are therefore excluded from measured request latency.
        query_values = queries.values.tolist()
        neighbor_values = neighbors.values.tolist()
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error

    adapter = adapter_for(args)
    report: dict[str, Any] = {}
    try:
        server_version = adapter.server_version()
        adapter.create(train.width, search_efs[0])
        load_started = time.perf_counter()
        inserted = 0
        failed = 0
        for start in range(0, train.rows, args.batch_size):
            ok, not_ok = adapter.insert(
                train.values[start : min(start + args.batch_size, train.rows)],
                start,
            )
            inserted += ok
            failed += not_ok
            if not_ok:
                raise RuntimeError(f"batch import failed for {not_ok} rows")
        count = adapter.finalize(train.rows, args.index_ready_timeout_seconds)
        load_elapsed = time.perf_counter() - load_started
        if inserted != train.rows or failed or count != train.rows:
            raise RuntimeError("import and database counts do not reconcile")
        time.sleep(args.post_load_settle_seconds)

        points = []
        for search_ef in search_efs:
            points.append(
                measure(
                    adapter,
                    query_values,
                    neighbor_values,
                    train_rows=train.rows,
                    top_k=10,
                    search_ef=search_ef,
                    filter_modulus=None,
                    concurrency=args.concurrency,
                    rounds=args.measurement_rounds,
                    warmup_queries=args.warmup_queries,
                )
            )
        if args.include_filters:
            for top_k, modulus in ((10, 2), (1, 20), (1, 100)):
                points.append(
                    measure(
                        adapter,
                        query_values,
                        neighbor_values,
                        train_rows=train.rows,
                        top_k=top_k,
                        search_ef=search_efs[-1],
                        filter_modulus=modulus,
                        concurrency=args.concurrency,
                        rounds=args.measurement_rounds,
                        warmup_queries=args.warmup_queries,
                    )
                )
        failures = []
        for point in points:
            failures.extend(
                f"ef={point['search_ef']} k={point['top_k']}: {failure}"
                for failure in point_failures(point, args.min_recall)
            )
        final_count = adapter.count()
        if final_count != train.rows:
            failures.append(f"final row count {final_count} != {train.rows}")
        report = {
            "schema_version": 1,
            "report_type": "competitor-ann-ground-truth",
            "generated_at_unix_ms": time.time_ns() // 1_000_000,
            "engine": args.engine,
            "server": {
                "host": args.host,
                "http_port": args.http_port,
                "grpc_port": args.grpc_port,
                "server_version": server_version,
                "client_version": adapter.client_version,
            },
            "collection": args.collection,
            "dataset": {
                "name": args.dataset_name,
                "dimensions": train.width,
                "train_vectors": train.rows,
                "query_vectors": queries.rows,
                "ground_truth_width": neighbors.width,
                "metric": args.metric,
                "train": file_identity(args.train_fvecs),
                "queries": file_identity(args.query_fvecs),
                "neighbors": file_identity(args.neighbors_ivecs),
            },
            "index": {
                "type": "HNSW",
                "m": args.hnsw_m,
                "ef_construction": args.hnsw_ef_construction,
                "search_efs": search_efs,
            },
            "load": {
                "requested": train.rows,
                "inserted": inserted,
                "failed": failed,
                "duration_ms": round(load_elapsed * 1000),
                "vectors_per_second": inserted / load_elapsed,
                "post_load_settle_seconds": args.post_load_settle_seconds,
            },
            "health_after": {"total_vectors": final_count},
            "points": points,
            "verdict": {
                "status": "pass" if not failures else "fail",
                "failures": failures,
            },
        }
    finally:
        adapter.close()

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output_json.with_name(
        f".{args.output_json.name}.{os.getpid()}.tmp"
    )
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output_json)
    print(
        json.dumps(
            {
                "engine": report["engine"],
                "points": len(report["points"]),
                "status": report["verdict"]["status"],
                "output": str(args.output_json),
            },
            separators=(",", ":"),
        )
    )
    if report["verdict"]["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
