"""PySpark ETL for USGS earthquake records.

The raw CSV responses are never edited.  This job:
  1. reads all raw monthly files,
  2. filters to genuine earthquake events,
  3. validates coordinates and required magnitude,
  4. deduplicates by event id keeping the latest `updated` revision,
  5. normalises timestamps to UTC,
  6. retains measurement-quality fields and adds calendar/grid columns,
  7. writes partitioned Parquet to HDFS and a local mirror,
  8. writes a transparent preprocessing report.

Rows with missing latitude/longitude/magnitude are removed and counted.
Missing depth is retained with an explicit `depth_missing` flag; no values are
silently imputed.
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.config import ensure_project_dirs, get_path, load_config
from common.io import atomic_write_json, directory_size, human_size, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from common.spark import create_spark_session, stop_spark
from storage.parquet_manager import write_dataset

log = get_logger("preprocessing.earthquake_etl")


def _time_expr(colname: str):
    """Parse USGS ISO timestamps robustly and force UTC semantics."""
    cleaned = F.regexp_replace(F.regexp_replace(F.col(colname), "T", " "), "Z$", "")
    parsed = F.coalesce(
        F.to_timestamp(cleaned, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(cleaned, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(cleaned),
    )
    return F.to_utc_timestamp(parsed, "UTC")


def _magnitude_family_expr(cfg: Dict[str, Any]):
    mapping = cfg.get("hotspots", {}).get("magnitude_families", {})
    expr = F.lit("other")
    for family, codes in mapping.items():
        expr = F.when(F.lower(F.trim(F.col("magType"))).isin([str(c).lower() for c in codes]), F.lit(family)).otherwise(expr)
    return F.when(F.col("magType").isNull(), F.lit("unknown")).otherwise(expr)


def raw_paths(cfg: Dict[str, Any], dataset: str) -> List[str]:
    if dataset == "sample_10k":
        p = get_path(cfg, "data_sample") / "earthquakes_10k.csv"
        if not p.exists():
            raise FileNotFoundError(f"Sample file not found: {p}")
        return [str(p)]
    if dataset == "global":
        base = get_path(cfg, "data_raw") / "earthquakes" / "global"
    elif dataset == "stress_all":
        base = get_path(cfg, "data_raw") / "earthquakes" / "stress"
        paths = []
        for region in cfg["stress"]["regions"]:
            paths.extend(sorted(glob.glob(str(base / region / "[0-9][0-9][0-9][0-9]" / "*.csv"))))
        if not paths:
            raise FileNotFoundError(f"No raw CSV files found under {base}")
        return paths
    elif dataset.startswith("stress_"):
        base = get_path(cfg, "data_raw") / "earthquakes" / "stress" / dataset.replace("stress_", "")
    else:
        raise ValueError(f"Unknown dataset: {dataset}")
    paths = sorted(glob.glob(str(base / "[0-9][0-9][0-9][0-9]" / "*.csv")))
    if not paths:
        raise FileNotFoundError(f"No raw CSV files found under {base}")
    return paths


def build_clean_dataframe(spark, cfg: Dict[str, Any], dataset: str, raw: List[str], min_magnitude: float) -> tuple[DataFrame, Dict[str, Any]]:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", False)
        .option("escape", '"')
        .option("quote", '"')
        .option("mode", "PERMISSIVE")
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv([Path(p).resolve().as_uri() for p in raw])
    )
    raw_count = df.count()
    required = {"time", "latitude", "longitude", "depth", "mag", "magType", "id", "updated", "type", "status"}
    missing_columns = sorted(required - set(df.columns))
    if missing_columns:
        raise RuntimeError(f"Raw CSV is missing required columns: {missing_columns}")

    casted = df.select(
        F.col("id").cast("string").alias("id"),
        _time_expr("time").alias("time_utc"),
        _time_expr("updated").alias("updated_utc"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("depth").cast("double").alias("depth"),
        F.col("mag").cast("double").alias("mag"),
        F.lower(F.trim(F.col("magType"))).alias("magType"),
        F.col("nst").cast("double").cast("int").alias("nst"),
        F.col("gap").cast("double").alias("gap"),
        F.col("dmin").cast("double").alias("dmin"),
        F.col("rms").cast("double").alias("rms"),
        F.col("net").cast("string").alias("net"),
        F.col("place").cast("string").alias("place"),
        F.lower(F.trim(F.col("type"))).alias("type"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.col("horizontalError").cast("double").alias("horizontalError"),
        F.col("depthError").cast("double").alias("depthError"),
        F.col("magError").cast("double").alias("magError"),
        F.col("magNst").cast("double").cast("int").alias("magNst"),
        F.col("locationSource").cast("string").alias("locationSource"),
        F.col("magSource").cast("string").alias("magSource"),
    )

    # Missingness is measured before filtering so the report is transparent.
    missing_counts = casted.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in casted.columns
    ]).first().asDict()
    total_missing = int(sum(v or 0 for v in missing_counts.values()))

    invalid_coords = casted.filter(
        F.col("latitude").isNull() | F.col("longitude").isNull()
        | ~F.col("latitude").between(-90.0, 90.0)
        | ~F.col("longitude").between(-180.0, 180.0)
    ).count()
    missing_mag = casted.filter(F.col("mag").isNull()).count()
    bad_time = casted.filter(F.col("time_utc").isNull()).count()

    allowed_types = [t.lower() for t in cfg.get("preprocessing", {}).get("earthquake", {}).get("allowed_event_types", ["earthquake"])]
    filtered = casted.filter(F.col("type").isin(allowed_types))
    filtered_count = filtered.count()

    with_flags = filtered.withColumn(
        "depth_missing", F.col("depth").isNull()
    ).withColumn(
        "depth_out_of_range",
        F.col("depth").isNotNull() & ~F.col("depth").between(-10.0, 800.0),
    ).withColumn(
        "magnitude_missing", F.col("mag").isNull()
    )

    valid = with_flags.filter(
        F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
        & F.col("mag").isNotNull()
        & (F.col("mag") >= float(min_magnitude))
    )
    valid_count = valid.count()
    below_min = filtered.filter(
        F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
        & F.col("mag").isNotNull()
        & (F.col("mag") < float(min_magnitude))
    ).count()

    dedup_window = Window.partitionBy("id").orderBy(
        F.col("updated_utc").desc_nulls_last(), F.col("time_utc").desc_nulls_last()
    )
    deduped = (
        valid.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    dedup_count = deduped.count()

    grid = float(cfg.get("preprocessing", {}).get("earthquake", {}).get("grid_size_degrees", 5.0))
    fine = float(cfg.get("preprocessing", {}).get("earthquake", {}).get("fine_grid_degrees", 1.0))

    def grid_expr(size):
        lat_bin = F.greatest(F.lit(0), F.least(F.lit(int(180 / size) - 1), F.floor((F.col("latitude") + 90.0) / size).cast("int")))
        lon_bin = F.greatest(F.lit(0), F.least(F.lit(int(360 / size) - 1), F.floor(((F.col("longitude") + 180.0) % 360.0) / size).cast("int")))
        return F.format_string("%03d_%03d", lat_bin, lon_bin)

    clean = (
        deduped
        .withColumn("year", F.year("time_utc"))
        .withColumn("month", F.month("time_utc"))
        .withColumn("day", F.dayofmonth("time_utc"))
        .withColumn("hour", F.hour("time_utc"))
        .withColumn("day_of_year", F.dayofyear("time_utc"))
        .withColumn("grid_id_5", grid_expr(grid))
        .withColumn("grid_id_1", grid_expr(fine))
        .withColumn("magnitude_family", _magnitude_family_expr(cfg))
        .withColumn("quality_flags", F.concat_ws(";",
            F.when(F.col("depth_missing"), F.lit("depth_missing")),
            F.when(F.col("depth_out_of_range"), F.lit("depth_out_of_range")),
            F.when(F.col("nst").isNull(), F.lit("nst_missing")),
            F.when(F.col("gap").isNull(), F.lit("gap_missing")),
            F.when(F.col("horizontalError").isNull(), F.lit("horizontal_error_missing")),
            F.when(F.col("depthError").isNull(), F.lit("depth_error_missing")),
            F.when(F.col("magError").isNull(), F.lit("mag_error_missing")),
        ))
        .filter(F.col("time_utc").isNotNull())
        .filter(F.col("year").between(2000, 2025))
    )
    final_count = clean.count()

    per_year = {
        str(r["year"]): int(r["count"])
        for r in clean.groupBy("year").count().orderBy("year").collect()
        if r["year"] is not None
    }
    stats = {
        "dataset": dataset,
        "raw_files": len(raw),
        "raw_rows": int(raw_count),
        "filtered_non_earthquake": int(raw_count - filtered_count),
        "filtered_invalid_coordinates": int(invalid_coords),
        "filtered_missing_magnitude": int(missing_mag),
        "filtered_bad_timestamp": int(bad_time),
        "filtered_below_min_magnitude": int(below_min),
        "duplicate_ids_removed": int(valid_count - dedup_count),
        "filtered_outside_period": int(dedup_count - final_count),
        "final_rows": int(final_count),
        "removed_total": int(raw_count - final_count),
        "missing_counts": {k: int(v or 0) for k, v in missing_counts.items()},
        "total_missing_cells": total_missing,
        "min_magnitude": float(min_magnitude),
        "grid_size_degrees": grid,
        "fine_grid_degrees": fine,
        "per_year": per_year,
        "generated_at": utc_now_iso(),
        "notes": [
            "Non-earthquake event types (quarry blasts, explosions, etc.) are removed before deduplication.",
            "Duplicate ids are reduced to the latest `updated` revision.",
            "Missing latitude/longitude/magnitude are removed and counted; missing depth is retained with a flag.",
            "Depth outside -10..800 km is flagged, not silently changed.",
            "Timestamps are standardised to UTC.",
        ],
    }
    return clean, stats


def run(dataset: str, cfg: Optional[Dict[str, Any]] = None, min_magnitude: Optional[float] = None,
        limit: Optional[int] = None) -> Dict[str, Any]:
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    if min_magnitude is None:
        min_magnitude = cfg["usgs"]["min_magnitude"] if dataset in ("global", "sample_10k") else 1.0
    spark = create_spark_session(f"etl-{dataset}", cfg)
    try:
        raw = raw_paths(cfg, dataset)
        log.info("ETL %s: %s raw files, min magnitude %s", dataset, len(raw), min_magnitude)
        clean, stats = build_clean_dataframe(spark, cfg, dataset, raw, min_magnitude)
        if limit:
            clean = clean.limit(int(limit))
            stats["limit_applied"] = int(limit)
            stats["final_rows"] = int(clean.count())
            log.warning("TEST LIMIT APPLIED: %s rows", limit)
        storage_meta = write_dataset(
            clean, f"earthquakes_{dataset}", dataset, f"earthquakes/{dataset}", cfg,
            partition_by=[cfg.get("storage", {}).get("earthquake_partition_column", "year")],
        )
        stats["storage"] = storage_meta
        out = get_path(cfg, "results") / "metadata" / f"preprocessing_earthquakes_{dataset}.json"
        atomic_write_json(out, stats)
        log.info("ETL complete: %s final rows, report %s", stats["final_rows"], out)
        # A compact CSV for quick inspection and dashboard fallback.
        spark.createDataFrame([(k, v) for k, v in stats["per_year"].items()], ["year", "rows"]) \
            .orderBy("year").toPandas().to_csv(get_path(cfg, "results") / "metadata" / f"per_year_{dataset}.csv", index=False)
        return stats
    finally:
        stop_spark(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark earthquake ETL")
    parser.add_argument("--dataset", default="sample_10k",
                        choices=["sample_10k", "global", "stress_alaska", "stress_california", "stress_all"])
    parser.add_argument("--min-magnitude", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None, help="testing only")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    stats = run(args.dataset, cfg, args.min_magnitude, args.limit)
    print(json.dumps({k: stats[k] for k in ("dataset", "raw_rows", "final_rows", "duplicate_ids_removed", "filtered_non_earthquake", "removed_total")}, indent=2))


if __name__ == "__main__":
    main()
