"""Detection-corrected earthquake hotspot analysis (research contribution N1).

Raw counts are compared with rates of events above the region/window
magnitude of completeness (Mc).  Mc and b-values are estimated from the actual
data for each 5-degree cell and time window.  Cells that fail the sufficient-
sample or dominant-magnitude-scale checks are retained with flags but excluded
from rankings.

This is a descriptive detection-correction exercise.  It does not claim that
corrected rates are the true seismic moment release or that detection is fully
removed.
"""
from __future__ import annotations

import argparse
import json
import zlib
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from analytics._data import read_processed, results_dir
from analytics.magnitude_completeness import analyze_magnitudes
from common.config import ensure_project_dirs, get_path, load_config
from common.geo import cell_area_from_id
from common.io import atomic_write_json, atomic_write_csv, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from common.spark import create_spark_session, stop_spark
from storage.parquet_manager import write_results_table

log = get_logger("analytics.hotspots")

SCHEMA = StructType([
    StructField("grid_id", StringType(), False),
    StructField("window_id", StringType(), False),
    StructField("start_year", IntegerType(), True),
    StructField("end_year", IntegerType(), True),
    StructField("n_raw", IntegerType(), True),
    StructField("area_km2", DoubleType(), True),
    StructField("raw_rate_per_1000km2_yr", DoubleType(), True),
    StructField("mc", DoubleType(), True),
    StructField("mc_method", StringType(), True),
    StructField("mc_ci_low", DoubleType(), True),
    StructField("mc_ci_high", DoubleType(), True),
    StructField("b_value", DoubleType(), True),
    StructField("b_uncertainty", DoubleType(), True),
    StructField("b_ci_low", DoubleType(), True),
    StructField("b_ci_high", DoubleType(), True),
    StructField("n_above_mc", IntegerType(), True),
    StructField("corrected_rate_per_1000km2_yr", DoubleType(), True),
    StructField("a_value", DoubleType(), True),
    StructField("b_gr", DoubleType(), True),
    StructField("gr_r2", DoubleType(), True),
    StructField("ks_stat", DoubleType(), True),
    StructField("ks_pvalue", DoubleType(), True),
    StructField("median_mag", DoubleType(), True),
    StructField("max_mag", DoubleType(), True),
    StructField("dominant_scale", StringType(), True),
    StructField("dominant_scale_share", DoubleType(), True),
    StructField("scale_policy_pass", BooleanType(), True),
    StructField("sufficient_data", BooleanType(), True),
    StructField("use_in_ranking", BooleanType(), True),
    StructField("center_latitude", DoubleType(), True),
    StructField("center_longitude", DoubleType(), True),
])


def _window_expr(cfg: Dict[str, Any]):
    expr = None
    for w in cfg["hotspots"]["windows"]:
        cond = F.col("year").between(int(w["start_year"]), int(w["end_year"]))
        expr = F.when(cond, F.lit(str(w["id"]))) if expr is None else expr.when(cond, F.lit(str(w["id"])))
    return expr.otherwise(F.lit(None))


def _group_stats(pdf: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    grid = str(pdf["grid_id_5"].iloc[0])
    window = str(pdf["window_id"].iloc[0])
    wmeta = next(w for w in cfg["hotspots"]["windows"] if str(w["id"]) == window)
    years = float(int(wmeta["end_year"]) - int(wmeta["start_year"]) + 1)
    area = float(cell_area_from_id(grid, float(cfg["hotspots"]["grid_size_degrees"])))
    lat_bin, lon_bin = (int(x) for x in grid.split("_"))
    size = float(cfg["hotspots"]["grid_size_degrees"])
    center_lat = -90.0 + (lat_bin + 0.5) * size
    center_lon = -180.0 + (lon_bin + 0.5) * size
    mags = pd.to_numeric(pdf["mag"], errors="coerce").to_numpy(dtype=float)
    mags = mags[np.isfinite(mags)]
    n_raw = int(len(mags))
    family = pdf["magnitude_family"].fillna("unknown").astype(str)
    shares = family.value_counts(normalize=True)
    dominant = str(shares.index[0]) if len(shares) else "unknown"
    dominant_share = float(shares.iloc[0]) if len(shares) else 0.0
    policy = str(cfg["hotspots"].get("magnitude_scale_policy", "dominant_family"))
    allow_mixed = bool(cfg["hotspots"].get("allow_mixed_scales", False))
    scale_pass = bool(allow_mixed or policy != "dominant_family"
                      or dominant_share >= float(cfg["hotspots"].get("min_dominant_scale_share", 0.75)))
    res = analyze_magnitudes(
        mags,
        min_events=int(cfg["hotspots"]["min_events"]),
        bin_width=float(cfg["hotspots"]["mc_bin_width"]),
        correction=float(cfg["hotspots"]["mc_correction"]),
        min_b_events=int(cfg["hotspots"]["min_b_value_events"]),
        bootstrap_iterations=int(cfg["hotspots"]["bootstrap_iterations"]),
        seed=zlib.crc32(f"{grid}|{window}".encode("utf-8")),
    )
    raw_rate = n_raw / (area * years) * 1000.0 if area > 0 and years > 0 else None
    corrected_rate = (
        res["n_above_mc"] / (area * years) * 1000.0
        if area > 0 and years > 0 and res["mc"] is not None else None
    )
    sufficient = bool(res["sufficient_data"] and res["mc"] is not None and res["n_above_mc"] >= 20)
    return pd.DataFrame([{
        "grid_id": grid,
        "window_id": window,
        "start_year": int(wmeta["start_year"]),
        "end_year": int(wmeta["end_year"]),
        "n_raw": n_raw,
        "area_km2": area,
        "raw_rate_per_1000km2_yr": raw_rate,
        "mc": res["mc"],
        "mc_method": res["mc_method"],
        "mc_ci_low": res.get("mc_ci_low"),
        "mc_ci_high": res.get("mc_ci_high"),
        "b_value": res["b_value"],
        "b_uncertainty": res["b_uncertainty"],
        "b_ci_low": res.get("b_ci_low"),
        "b_ci_high": res.get("b_ci_high"),
        "n_above_mc": int(res["n_above_mc"]),
        "corrected_rate_per_1000km2_yr": corrected_rate,
        "a_value": res["a_value"],
        "b_gr": res["b_gr"],
        "gr_r2": res["gr_r2"],
        "ks_stat": res["ks_stat"],
        "ks_pvalue": res["ks_pvalue"],
        "median_mag": res["median_mag"],
        "max_mag": res["max_mag"],
        "dominant_scale": dominant,
        "dominant_scale_share": dominant_share,
        "scale_policy_pass": scale_pass,
        "sufficient_data": sufficient,
        "use_in_ranking": bool(sufficient and scale_pass),
        "center_latitude": center_lat,
        "center_longitude": center_lon,
    }])


def _rank_and_compare(pdf: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    out = []
    for window, g in pdf.groupby("window_id"):
        g = g.copy()
        g["raw_rank"] = g["raw_rate_per_1000km2_yr"].rank(method="min", ascending=False)
        eligible = g["use_in_ranking"] & g["corrected_rate_per_1000km2_yr"].notna()
        g["corrected_rank"] = np.nan
        g.loc[eligible, "corrected_rank"] = g.loc[eligible, "corrected_rate_per_1000km2_yr"].rank(method="min", ascending=False)
        g["rank_change"] = g["raw_rank"] - g["corrected_rank"]
        out.append(g)
    ranked = pd.concat(out, ignore_index=True)
    # Growth comparison between the first and second configured windows.
    wins = [str(w["id"]) for w in cfg["hotspots"]["windows"]]
    if len(wins) >= 2:
        a = ranked[ranked["window_id"] == wins[0]].set_index("grid_id")
        b = ranked[ranked["window_id"] == wins[1]].set_index("grid_id")
        merged = a[["raw_rate_per_1000km2_yr", "corrected_rate_per_1000km2_yr", "raw_rank",
                    "corrected_rank", "use_in_ranking", "center_latitude", "center_longitude",
                    "n_raw", "n_above_mc"]].join(
            b[["raw_rate_per_1000km2_yr", "corrected_rate_per_1000km2_yr", "raw_rank",
               "corrected_rank", "use_in_ranking", "n_raw", "n_above_mc"]],
            lsuffix="_early", rsuffix="_late", how="inner"
        )
        merged = merged[(merged["use_in_ranking_early"]) & (merged["use_in_ranking_late"])].copy()
        for col in ("raw_rate_per_1000km2_yr", "corrected_rate_per_1000km2_yr"):
            merged[f"{col}_change_pct"] = np.where(
                merged[f"{col}_early"] > 0,
                (merged[f"{col}_late"] - merged[f"{col}_early"]) / merged[f"{col}_early"] * 100.0,
                np.nan,
            )
        merged["growth_class"] = np.select(
            [
                (merged["raw_rate_per_1000km2_yr_change_pct"] > 0) & (merged["corrected_rate_per_1000km2_yr_change_pct"] > 0),
                (merged["raw_rate_per_1000km2_yr_change_pct"] > 0) & (merged["corrected_rate_per_1000km2_yr_change_pct"] <= 0),
                (merged["raw_rate_per_1000km2_yr_change_pct"] <= 0) & (merged["corrected_rate_per_1000km2_yr_change_pct"] > 0),
            ],
            ["apparent_growth_persists", "apparent_growth_disappears_after_correction", "apparent_decline_reverses_after_correction"],
            default="decline_persists",
        )
        merged["raw_rank_change_between_windows"] = merged["raw_rank_late"] - merged["raw_rank_early"]
        merged["corrected_rank_change_between_windows"] = merged["corrected_rank_late"] - merged["corrected_rank_early"]
        merged = merged.reset_index()
    else:
        merged = pd.DataFrame()
    return ranked, merged


def run(dataset: str = "sample_10k", cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    spark = create_spark_session(f"hotspots-{dataset}", cfg)
    try:
        df, backend = read_processed(spark, cfg, f"earthquakes/{dataset}")
        df = df.select("year", "mag", "magnitude_family", "grid_id_5", "latitude", "longitude", "type")
        df = df.filter(F.col("mag").isNotNull() & F.col("grid_id_5").isNotNull())
        df = df.withColumn("window_id", _window_expr(cfg)).filter(F.col("window_id").isNotNull())
        df = df.cache()
        n_input = df.count()
        grouped = df.groupBy("grid_id_5", "window_id").applyInPandas(
            lambda pdf: _group_stats(pdf, cfg), schema=SCHEMA
        )
        grouped = grouped.cache()
        out_df = grouped.toPandas()
        if out_df.empty:
            raise RuntimeError("Hotspot analysis produced no groups")
        ranked, growth = _rank_and_compare(out_df, cfg)
        hsdir = results_dir(cfg, "hotspots")
        atomic_write_csv(hsdir / "hotspot_metrics.csv", ranked)
        atomic_write_csv(hsdir / "mc_values.csv", ranked[[
            "grid_id", "window_id", "n_raw", "mc", "mc_ci_low", "mc_ci_high", "dominant_scale",
            "dominant_scale_share", "scale_policy_pass", "ks_stat", "ks_pvalue", "gr_r2"
        ]])
        atomic_write_csv(hsdir / "b_values.csv", ranked[[
            "grid_id", "window_id", "b_value", "b_uncertainty", "b_ci_low", "b_ci_high",
            "n_above_mc", "a_value", "b_gr", "gr_r2"
        ]])
        raw_cols = ["grid_id", "window_id", "n_raw", "raw_rate_per_1000km2_yr", "raw_rank",
                    "center_latitude", "center_longitude", "dominant_scale", "scale_policy_pass"]
        cor_cols = ["grid_id", "window_id", "n_above_mc", "mc", "corrected_rate_per_1000km2_yr",
                    "corrected_rank", "center_latitude", "center_longitude", "use_in_ranking"]
        atomic_write_csv(hsdir / "raw_density_map.csv", ranked[raw_cols].sort_values(["window_id", "raw_rank"]))
        atomic_write_csv(hsdir / "corrected_density_map.csv", ranked[cor_cols].sort_values(["window_id", "corrected_rank"]))
        for window in ranked["window_id"].unique():
            atomic_write_csv(hsdir / f"raw_hotspot_ranking_{window}.csv",
                             ranked[ranked["window_id"] == window].sort_values("raw_rank")[raw_cols])
            atomic_write_csv(hsdir / f"corrected_hotspot_ranking_{window}.csv",
                             ranked[(ranked["window_id"] == window) & ranked["use_in_ranking"]]
                             .sort_values("corrected_rank")[cor_cols])
        if not growth.empty:
            atomic_write_csv(hsdir / "region_growth_comparison.csv", growth)
        summary = {
            "dataset": dataset,
            "backend": backend,
            "generated_at": utc_now_iso(),
            "input_rows": int(n_input),
            "groups": int(len(out_df)),
            "cells": int(out_df["grid_id"].nunique()),
            "windows": [str(w["id"]) for w in cfg["hotspots"]["windows"]],
            "ranking_eligible_groups": int(out_df["use_in_ranking"].sum()),
            "scale_policy_excluded_groups": int((~out_df["scale_policy_pass"]).sum()),
            "insufficient_groups": int((~out_df["sufficient_data"]).sum()),
            "median_mc": float(out_df["mc"].median()),
            "median_b_value": float(out_df["b_value"].median()),
            "parameters": cfg["hotspots"],
            "growth_comparison_rows": int(len(growth)),
            "growth_classes": {k: int(v) for k, v in growth["growth_class"].value_counts().items()} if not growth.empty else {},
            "notes": [
                "Mc = maximum-curvature mode + 0.2 correction; uncertainty is a bootstrap percentile interval.",
                "b = Aki-Utsu MLE with Utsu binning correction; uncertainty is the Shi-Bolt approximation.",
                "Rates are normalised by spherical cell area and window duration, per 1000 km2 per year.",
                "Cells failing the dominant magnitude-scale policy or minimum sample size are not ranked.",
                "Corrected rates are an approximation, not a direct measurement of true seismic activity.",
            ],
        }
        atomic_write_json(hsdir / "summary.json", summary)
        try:
            write_results_table(grouped, "hotspots/hotspot_metrics", cfg)
        except Exception as exc:
            log.warning("Could not publish hotspot Parquet to HDFS: %s", exc)
        log.info("Hotspot analysis complete: %s groups, %s eligible; median Mc=%s",
                 len(out_df), summary["ranking_eligible_groups"], summary["median_mc"])
        return summary
    finally:
        stop_spark(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Detection-corrected hotspot analysis")
    parser.add_argument("--dataset", default="sample_10k")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    print(json.dumps(run(args.dataset, cfg), indent=2, default=str))


if __name__ == "__main__":
    main()
