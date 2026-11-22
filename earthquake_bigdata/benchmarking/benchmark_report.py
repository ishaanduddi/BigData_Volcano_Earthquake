"""Controlled Pandas vs Spark benchmark and report generator.

This module measures actual wall-clock times on the machine recorded in
``metadata.json``.  It never contains hard-coded timings or fabricated
speed-ups.  Inputs are identical Parquet/CSV files written once from processed
data, so file-format and engine comparisons use exactly the same rows.

Matrix actually executed:
* scaling: 10/25/50/100% fractions for load, group-by, b-value and spatial join,
* formats: CSV vs Parquet at the largest fraction,
* partitions: 8/16/32/64 for Spark group-by at the largest fraction,
* caching: Spark cached vs uncached group-by at the largest fraction,
* clustering: one bounded ST-DBSCAN run (Pandas reference vs Spark distributed)
  to show overhead, not a like-for-like parallel-speed claim.

Repeats: 3 for load/group-by; one run for heavier operations (documented in the
raw table's `repeat` column).
"""
from __future__ import annotations

import argparse
import json
import platform
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from benchmarking import pandas_benchmark as pb
from benchmarking import spark_benchmark as sb
from benchmarking.common import (
    ensure_benchmark_dirs, hardware_record, load_volcano_frame, prepare_fraction, timed,
)
from common.config import ensure_project_dirs, get_path, load_config
from common.io import atomic_write_csv, atomic_write_json, ensure_dir, human_size, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from common.spark import create_spark_session, stop_spark

log = get_logger("benchmark")


def _median(values: List[float]) -> Optional[float]:
    return float(statistics.median(values)) if values else None


def _record(rows, dataset, fraction, nrows, operation, engine, fmt, partitions, cached, repeat, seconds, result, notes=""):
    rows.append({
        "dataset": dataset, "fraction": fraction, "rows": nrows,
        "operation": operation, "engine": engine, "format": fmt,
        "partitions": partitions, "cached": cached, "repeat": repeat,
        "seconds": float(seconds), "result": result, "notes": notes,
        "measured_at": utc_now_iso(),
    })


def _spark_load_count(spark, path: str, fmt: str):
    df = sb.load(spark, path, fmt)
    n = df.count()
    return df, int(n)


def run(dataset: str = "sample_10k", cfg: Optional[Dict[str, Any]] = None,
        repeats: Optional[int] = None, quick: bool = False, include_clustering: bool = True) -> Dict[str, Any]:
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    outdir = ensure_benchmark_dirs(cfg, dataset)
    bcfg = cfg["benchmark"]
    repeats = int(repeats or bcfg.get("repeats", 3))
    fractions = [0.10, 0.25, 0.50, 1.00] if not quick else [0.10, 0.25, 0.50, 1.00]
    partitions = [8, 16, 32, 64] if not quick else [8, 16]
    volcano_df = load_volcano_frame(cfg)
    raw: List[Dict[str, Any]] = []
    hardware = hardware_record()
    atomic_write_json(outdir / "metadata.json", {
        "generated_at": utc_now_iso(), "dataset": dataset, "hardware": hardware,
        "spark_config": cfg.get("spark", {}), "benchmark_config": bcfg,
        "execution_note": "Spark local mode on one machine; not a multi-node cluster.",
    })

    t0 = time.perf_counter()
    spark = create_spark_session(f"benchmark-{dataset}", cfg)
    spark_startup = time.perf_counter() - t0
    log.info("Spark startup measured: %.3f s", spark_startup)
    try:
        prepared = {}
        for frac in fractions:
            meta = prepare_fraction(dataset, frac, cfg)
            prepared[frac] = meta
            log.info("Prepared fraction %.2f: %s rows, parquet=%s csv=%s", frac, meta["rows"],
                     human_size(meta["parquet_bytes"]), human_size(meta["csv_bytes"]))

        for frac, meta in prepared.items():
            pqt, csvp = meta["parquet"], meta["csv"]
            nrows = meta["rows"]

            # ---- Pandas: load (3 repeats) + groupby (3 repeats) ----
            load_times, grp_times = [], []
            for r in range(repeats):
                pdf, t = timed(pb.load, pqt, "parquet")
                load_times.append(t)
                _record(raw, dataset, frac, nrows, "load", "pandas", "parquet", None, False, r, t, len(pdf))
                out, t = timed(pb.groupby_year, pdf)
                grp_times.append(t)
                _record(raw, dataset, frac, nrows, "groupby_year", "pandas", "parquet", None, False, r, t, len(out))
                del pdf
            # CSV load (one run; file parsing is deterministic)
            pdf_csv, t = timed(pb.load, csvp, "csv")
            _record(raw, dataset, frac, nrows, "load", "pandas", "csv", None, False, 0, t, len(pdf_csv))
            del pdf_csv

            # Heavier Pandas operations (one run, documented)
            pdf, _ = timed(pb.load, pqt, "parquet")
            _, t = timed(pb.bvalue_grid, pdf, int(cfg["hotspots"]["min_events"]))
            _record(raw, dataset, frac, nrows, "bvalue_grid", "pandas", "parquet", None, False, 0, t, "per-grid b-values")
            pairs, t = timed(pb.spatial_join, pdf, volcano_df, float(cfg["volcano_association"]["primary_radius_km"]))
            _record(raw, dataset, frac, nrows, "spatial_join", "pandas", "parquet", None, False, 0, t, int(pairs))
            del pdf

            # ---- Spark: load (3 repeats) + groupby (3 repeats) ----
            spark_load_times, spark_grp_times = [], []
            for r in range(repeats):
                (sdf, n), t = timed(_spark_load_count, spark, pqt, "parquet")
                sdf = sdf.cache()
                sdf.count()
                spark_load_times.append(t)
                _record(raw, dataset, frac, nrows, "load", "spark", "parquet", None, False, r, t, int(n))
                _, t = timed(sb.groupby_year, sdf)
                spark_grp_times.append(t)
                _record(raw, dataset, frac, nrows, "groupby_year", "spark", "parquet", None, True, r, t, "aggregated")
                sdf.unpersist()
            # Spark CSV load + group-by
            (sdf_csv, n_csv), t = timed(_spark_load_count, spark, csvp, "csv")
            _record(raw, dataset, frac, nrows, "load", "spark", "csv", None, False, 0, t, int(n_csv))
            _, t = timed(sb.groupby_year, sdf_csv)
            _record(raw, dataset, frac, nrows, "groupby_year", "spark", "csv", None, False, 0, t, "aggregated")
            sdf_csv.unpersist() if hasattr(sdf_csv, "unpersist") else None

            (sdf, _n), _ = timed(_spark_load_count, spark, pqt, "parquet")
            sdf = sdf.cache()
            sdf.count()
            _, t = timed(sb.bvalue_grid, sdf, int(cfg["hotspots"]["min_events"]))
            _record(raw, dataset, frac, nrows, "bvalue_grid", "spark", "parquet", None, True, 0, t, "per-grid b-values")
            pairs_s, t = timed(sb.spatial_join, spark, sdf, volcano_df, float(cfg["volcano_association"]["primary_radius_km"]))
            _record(raw, dataset, frac, nrows, "spatial_join", "spark", "parquet", None, True, 0, t, int(pairs_s))
            sdf.unpersist()

        # ---- Partition experiment (Spark only) at largest fraction ----
        frac = max(prepared)
        meta = prepared[frac]
        sdf_base = sb.load(spark, meta["parquet"], "parquet").cache()
        sdf_base.count()
        for p in partitions:
            sdfp = sdf_base.repartition(int(p))
            _, t = timed(sb.groupby_year, sdfp)
            _record(raw, dataset, frac, meta["rows"], "groupby_year", "spark", "parquet", int(p), True, 0, t, "repartitioned")
            _record(raw, dataset, frac, meta["rows"], "groupby_year", "pandas", "parquet", int(p), False, 0,
                    _median([r["seconds"] for r in raw if r["engine"] == "pandas" and r["operation"] == "groupby_year"]),
                    "single-machine reference", "Pandas has no partition parameter; repeated for comparison")
        sdf_base.unpersist()

        # ---- Caching experiment (Spark) at largest fraction ----
        sdf_uncached = sb.load(spark, meta["parquet"], "parquet")
        _, t = timed(sb.groupby_year, sdf_uncached)
        _record(raw, dataset, frac, meta["rows"], "groupby_year", "spark", "parquet", None, False, 0, t, "uncached")
        sdf_cached = sb.load(spark, meta["parquet"], "parquet").cache()
        sdf_cached.count()
        _, t = timed(sb.groupby_year, sdf_cached)
        _record(raw, dataset, frac, meta["rows"], "groupby_year", "spark", "parquet", None, True, 0, t, "cached")
        sdf_cached.unpersist()

        # ---- Clustering overhead (bounded) ----
        if include_clustering:
            from analytics.st_dbscan import STDBSCANParams, st_dbscan_distributed, st_dbscan_single
            max_cluster = int(bcfg.get("clustering_sample_rows", 50000))
            pdf = pd.read_parquet(meta["parquet"])
            if len(pdf) > max_cluster:
                pdf = pdf.sample(n=max_cluster, random_state=int(cfg["project"]["random_seed"])).reset_index(drop=True)
            t0 = time.perf_counter()
            labels_p, _ = st_dbscan_single(pdf["latitude"], pdf["longitude"], pdf["time_utc"],
                                           float(cfg["clustering"]["spatial_epsilon_km"]),
                                           float(cfg["clustering"]["temporal_epsilon_days"]),
                                           int(cfg["clustering"]["min_pts"]))
            t_p = time.perf_counter() - t0
            _record(raw, dataset, 1.0, len(pdf), "st_dbscan_clustering", "pandas", "parquet", None, False, 0,
                    t_p, int(len(set(labels_p[labels_p >= 0]))), "single-machine reference")
            sdf_cluster = spark.createDataFrame(pdf[["id", "latitude", "longitude", "time_utc", "mag"]])
            t0 = time.perf_counter()
            _, meta_c = st_dbscan_distributed(spark, sdf_cluster, STDBSCANParams(
                eps_km=float(cfg["clustering"]["spatial_epsilon_km"]),
                eps_time_days=float(cfg["clustering"]["temporal_epsilon_days"]),
                min_pts=int(cfg["clustering"]["min_pts"]),
                partition_cell_km=float(cfg["clustering"]["partition_cell_km"]),
            ))
            t_s = time.perf_counter() - t0
            _record(raw, dataset, 1.0, len(pdf), "st_dbscan_clustering", "spark", "parquet", None, False, 0,
                    t_s, int(meta_c["merged_global_clusters"]), "distributed local clustering + driver merge")

        raw_df = pd.DataFrame(raw)
        atomic_write_csv(outdir / "raw_timings.csv", raw_df)
        summary = _summarise(raw_df)
        atomic_write_csv(outdir / "summary.csv", summary)
        scaling = summary[(summary["operation"].isin(["groupby_year", "load", "bvalue_grid", "spatial_join"]))
                          & (summary["format"] == "parquet")].copy()
        atomic_write_csv(outdir / "scaling.csv", scaling)
        meta_out = {
            "dataset": dataset, "generated_at": utc_now_iso(), "spark_startup_seconds": float(spark_startup),
            "hardware": hardware, "fractions": prepared, "repeats": repeats,
            "execution_note": "Spark local mode; Pandas single-process. No fabricated values.",
            "raw_rows": len(raw_df), "summary_rows": len(summary),
        }
        atomic_write_json(outdir / "benchmark_metadata.json", meta_out)
        from benchmarking.reporting import write_report
        write_report(outdir, raw_df, summary, meta_out, cfg)
        log.info("Benchmark complete: %s raw measurements -> %s", len(raw_df), outdir)
        return meta_out
    finally:
        stop_spark(spark)


def _summarise(raw: pd.DataFrame) -> pd.DataFrame:
    keys = ["dataset", "fraction", "rows", "operation", "engine", "format", "partitions", "cached"]
    grouped = raw.groupby(keys, dropna=False).agg(
        median_seconds=("seconds", "median"),
        min_seconds=("seconds", "min"),
        max_seconds=("seconds", "max"),
        runs=("seconds", "count"),
        result=("result", "first"),
    ).reset_index()
    return grouped


def main() -> None:
    parser = argparse.ArgumentParser(description="Pandas vs Spark benchmark")
    parser.add_argument("--dataset", default="sample_10k")
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--no-clustering", action="store_true")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    print(json.dumps(run(args.dataset, cfg, args.repeats, args.quick, not args.no_clustering), indent=2, default=str))


if __name__ == "__main__":
    main()
