#!/usr/bin/env python3
"""Convert an ANN-Benchmarks HDF5 dataset to streaming fvecs/ivecs files.

This optional preparation utility requires h5py and NumPy.  The release
benchmark binary itself has no Python or HDF5 dependency.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
from pathlib import Path
from typing import Any

try:
    import h5py
    import numpy as np
except ImportError as error:  # pragma: no cover - environment guidance
    raise SystemExit(
        "h5py and numpy are required: python3 -m pip install h5py numpy"
    ) from error


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--chunk-rows", type=int, default=4096)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def write_vectors(
    source: Any,
    destination: Path,
    *,
    dtype: str,
    chunk_rows: int,
) -> dict[str, Any]:
    if len(source.shape) != 2 or source.shape[0] < 1 or source.shape[1] < 1:
        raise ValueError(f"{destination.name} source must be a non-empty matrix")
    rows, dimensions = (int(source.shape[0]), int(source.shape[1]))
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    with temporary.open("wb") as output:
        header = struct.pack("<i", dimensions)
        for offset in range(0, rows, chunk_rows):
            values = np.asarray(
                source[offset : min(rows, offset + chunk_rows)],
                dtype=dtype,
                order="C",
            )
            for row in values:
                output.write(header)
                output.write(row.tobytes(order="C"))
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, destination)
    return {
        "path": str(destination),
        "rows": rows,
        "dimensions": dimensions,
        "bytes": destination.stat().st_size,
        "sha256": sha256(destination),
    }


def main() -> None:
    args = arguments()
    if args.chunk_rows < 1 or args.chunk_rows > 1_000_000:
        raise SystemExit("--chunk-rows must be between 1 and 1000000")
    if not args.input.is_file():
        raise SystemExit(f"input file does not exist: {args.input}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "train": args.output_dir / "train.fvecs",
        "test": args.output_dir / "test.fvecs",
        "neighbors": args.output_dir / "neighbors.ivecs",
    }
    if any(path.exists() for path in outputs.values()):
        raise SystemExit("refusing to overwrite an existing converted dataset")

    with h5py.File(args.input, "r") as source:
        missing = sorted({"train", "test", "neighbors"} - set(source.keys()))
        if missing:
            raise SystemExit(f"ANN dataset is missing matrices: {missing}")
        train = write_vectors(
            source["train"],
            outputs["train"],
            dtype="<f4",
            chunk_rows=args.chunk_rows,
        )
        test = write_vectors(
            source["test"],
            outputs["test"],
            dtype="<f4",
            chunk_rows=args.chunk_rows,
        )
        neighbors = write_vectors(
            source["neighbors"],
            outputs["neighbors"],
            dtype="<i4",
            chunk_rows=args.chunk_rows,
        )
        distance = source.attrs.get("distance", "unknown")
        if isinstance(distance, bytes):
            distance = distance.decode("utf-8", errors="strict")

    if train["dimensions"] != test["dimensions"]:
        raise SystemExit("train and query dimensions differ")
    if test["rows"] != neighbors["rows"]:
        raise SystemExit("query and ground-truth row counts differ")
    manifest = {
        "schema_version": 1,
        "source": {
            "path": str(args.input),
            "bytes": args.input.stat().st_size,
            "sha256": sha256(args.input),
            "distance": str(distance),
        },
        "files": {
            "train": train,
            "test": test,
            "neighbors": neighbors,
        },
    }
    manifest_path = args.output_dir / "dataset-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "manifest": str(manifest_path),
                "train_rows": train["rows"],
                "query_rows": test["rows"],
                "dimensions": train["dimensions"],
                "ground_truth_width": neighbors["dimensions"],
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
