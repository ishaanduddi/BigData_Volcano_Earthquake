"""Shared benchmarking utilities: hardware record, data preparation, timing."""
from __future__ import annotations

import json
import os
import platform
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import psutil

from common.config import ensure_project_dirs, get_path, load_config
from common.io import atomic_write_csv, atomic_write_json, directory_size, ensure_dir, human_size, utc_now_iso
from common.logging_utils import get_logger

log = get_logger("benchmark.common")


def hardware_record() -> Dict[str, Any]:
    vm = psutil.virtual_memory()
    return {
        "timestamp": utc_now_iso(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_logical": psutil.cpu_count(logical=True),
        "cpu_physical": psutil.cpu_count(logical=False),
        "cpu_freq_mhz": psutil.cpu_freq().max if psutil.cpu_freq() else None,
        "ram_total_bytes": int(vm.total),
        "ram_available_bytes": int(vm.available),
        "python": platform.python_version(),
        "disk_free_bytes": int(psutil.disk_usage(str(Path.cwd().anchor or "/")).free) if os.name == "nt" else int(psutil.disk_usage("/").free),
    }


def timed(fn, *args, **kwargs) -> tuple[Any, float]:
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, float(time.perf_counter() - t0)


def prepare_fraction(dataset: str, fraction: float, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create deterministic identical Parquet/CSV inputs for both engines."""
    cfg = cfg or load_config()
    src = get_path(cfg, "data_processed") / "earthquakes" / dataset
    out = get_path(cfg, "data_processed") / ".." / "benchmark" / dataset
    out = (get_path(cfg, "data_processed").parent / "benchmark" / dataset).resolve()
    ensure_dir(out)
    tag = f"fraction_{int(round(fraction * 100)):03d}"
    parquet_path = out / f"{tag}.parquet"
    csv_path = out / f"{tag}.csv"
    if parquet_path.exists() and csv_path.exists():
        df = pd.read_parquet(parquet_path)
        return {"fraction": fraction, "rows": len(df), "parquet": str(parquet_path), "csv": str(csv_path),
                "parquet_bytes": parquet_path.stat().st_size, "csv_bytes": csv_path.stat().st_size, "reused": True}
    df = pd.read_parquet(src)
    if fraction < 1.0:
        df = df.sample(frac=fraction, random_state=int(cfg["project"]["random_seed"])).reset_index(drop=True)
    # PyArrow defaults timestamps to nanoseconds, which Spark 4.2 rejects.
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype("datetime64[us]")
    df.to_parquet(parquet_path, index=False, compression="snappy")
    df.to_csv(csv_path, index=False)
    return {"fraction": fraction, "rows": len(df), "parquet": str(parquet_path), "csv": str(csv_path),
            "parquet_bytes": parquet_path.stat().st_size, "csv_bytes": csv_path.stat().st_size, "reused": False}


def grid_neighbor_cells(lat: float, lon: float, radius_km: float, grid_deg: float = 5.0) -> List[str]:
    """5-degree cell ids that can contain a point within radius (same as Spark join)."""
    import math
    n_lat = int(180 / grid_deg)
    n_lon = int(360 / grid_deg)
    lat_bin = int(math.floor((lat + 90.0) / grid_deg))
    lon_bin = int(math.floor(((lon + 180.0) % 360.0) / grid_deg))
    rings_lat = max(1, int(math.ceil(radius_km / (111.32 * grid_deg))))
    coslat = max(math.cos(math.radians(abs(lat))), 0.01)
    rings_lon = min(12, max(1, int(math.ceil(radius_km / (111.32 * coslat * grid_deg))) + 1))
    out = []
    for dr in range(-rings_lat, rings_lat + 1):
        lb = min(n_lat - 1, max(0, lat_bin + dr))
        for dc in range(-rings_lon, rings_lon + 1):
            lc = (lon_bin + dc) % n_lon
            out.append(f"{lb:03d}_{lc:03d}")
    return list(dict.fromkeys(out))


def load_volcano_frame(cfg: Dict[str, Any]) -> pd.DataFrame:
    return pd.read_parquet(get_path(cfg, "data_processed") / "volcanoes" / "volcanoes")


def ensure_benchmark_dirs(cfg: Dict[str, Any], dataset: str = "") -> Path:
    base = get_path(cfg, "results") / "benchmarks"
    p = base / dataset if dataset else base
    ensure_dir(p)
    (p / "figures").mkdir(exist_ok=True)
    return p
