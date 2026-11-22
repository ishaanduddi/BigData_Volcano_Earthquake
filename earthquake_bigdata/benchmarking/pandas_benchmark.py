"""Pandas implementations of the benchmark operations."""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from analytics.magnitude_completeness import b_value_aki_utsu, estimate_mc_maxc
from benchmarking.common import grid_neighbor_cells
from common.geo import haversine_km


def load(path: str, fmt: str) -> pd.DataFrame:
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def groupby_year(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("year", dropna=False).agg(
        earthquakes=("id", "count"),
        mean_magnitude=("mag", "mean"),
        max_magnitude=("mag", "max"),
    ).reset_index()


def bvalue_grid(df: pd.DataFrame, min_events: int = 50) -> pd.DataFrame:
    rows = []
    for grid, g in df.groupby("grid_id_5", dropna=True):
        mags = pd.to_numeric(g["mag"], errors="coerce").dropna().to_numpy()
        if len(mags) < min_events:
            continue
        mc = estimate_mc_maxc(mags, 0.1, 0.2)
        b = b_value_aki_utsu(mags, mc, 0.1) if mc is not None else None
        rows.append({"grid_id_5": grid, "n": len(mags), "mc": mc, "b_value": b})
    return pd.DataFrame(rows)


def spatial_join(df: pd.DataFrame, volcanoes: pd.DataFrame, radius_km: float = 100.0) -> int:
    """Count volcano-earthquake pairs within radius using a 5-degree grid join."""
    if "grid_id_5" not in df.columns:
        raise ValueError("grid_id_5 required")
    groups = {str(k): np.asarray(v) for k, v in df.groupby("grid_id_5", dropna=True).indices.items()}
    lat = df["latitude"].to_numpy()
    lon = df["longitude"].to_numpy()
    total = 0
    for row in volcanoes.itertuples(index=False):
        vlat, vlon = float(row.latitude), float(row.longitude)
        if not (np.isfinite(vlat) and np.isfinite(vlon)):
            continue
        idx_parts = []
        for cell in grid_neighbor_cells(vlat, vlon, radius_km):
            arr = groups.get(cell)
            if arr is not None and arr.size:
                idx_parts.append(arr)
        if not idx_parts:
            continue
        idx = np.unique(np.concatenate(idx_parts))
        d = haversine_km(vlat, vlon, lat[idx], lon[idx])
        total += int(np.sum(d <= radius_km))
    return total
