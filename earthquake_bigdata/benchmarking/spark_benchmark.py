"""Spark implementations of the benchmark operations."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from analytics.magnitude_completeness import b_value_aki_utsu, estimate_mc_maxc
from benchmarking.common import grid_neighbor_cells


def _uri(path: str) -> str:
    return Path(path).resolve().as_uri()


def load(spark, path: str, fmt: str):
    if fmt == "parquet":
        return spark.read.parquet(_uri(path))
    return (spark.read.option("header", True).option("inferSchema", True)
            .option("multiLine", False).csv(_uri(path)))


def groupby_year(df) -> int:
    out = df.groupBy("year").agg(
        F.count("id").alias("earthquakes"),
        F.avg("mag").alias("mean_magnitude"),
        F.max("mag").alias("max_magnitude"),
    ).collect()
    return len(out)


def bvalue_grid(df, min_events: int = 50) -> int:
    def _b(pdf):
        mags = pd.to_numeric(pdf["mag"], errors="coerce").dropna().to_numpy()
        if len(mags) < min_events:
            return pd.DataFrame(columns=["grid_id_5", "n", "mc", "b_value"])
        mc = estimate_mc_maxc(mags, 0.1, 0.2)
        b = b_value_aki_utsu(mags, mc, 0.1) if mc is not None else None
        return pd.DataFrame([{"grid_id_5": str(pdf["grid_id_5"].iloc[0]), "n": len(mags), "mc": mc, "b_value": b}])

    out = df.groupBy("grid_id_5").applyInPandas(
        _b, schema="grid_id_5 string, n long, mc double, b_value double"
    ).collect()
    return len(out)


def spatial_join(spark, df, volcanoes: pd.DataFrame, radius_km: float = 100.0) -> int:
    rows = []
    for row in volcanoes.itertuples(index=False):
        vlat, vlon = float(row.latitude), float(row.longitude)
        if not (np.isfinite(vlat) and np.isfinite(vlon)):
            continue
        for cell in grid_neighbor_cells(vlat, vlon, radius_km):
            rows.append((int(row.volcano_number), float(vlat), float(vlon), cell))
    vol = spark.createDataFrame(rows, schema="volcano_number int, volcano_latitude double, volcano_longitude double, cell_id string")
    dlat = F.radians(F.col("volcano_latitude") - F.col("latitude"))
    dlon = F.radians(F.col("volcano_longitude") - F.col("longitude"))
    a = (F.sin(dlat / 2.0) ** 2
         + F.cos(F.radians(F.col("latitude"))) * F.cos(F.radians(F.col("volcano_latitude")))
         * F.sin(dlon / 2.0) ** 2)
    dist = 2.0 * 6371.0088 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))))
    joined = df.join(vol, df["grid_id_5"] == vol["cell_id"], "inner").withColumn("distance_km", dist).filter(F.col("distance_km") <= radius_km)
    return int(joined.count())
