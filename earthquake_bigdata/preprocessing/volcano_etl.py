"""PySpark ETL for Smithsonian GVP volcano and eruption records.

Coordinates and dates are preserved exactly as published.  Where the GVP date
fields are incomplete (month/day = 0 or null) the date precision is recorded
rather than invented.  The original year uncertainty field is retained.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import ensure_project_dirs, get_path, load_config
from common.io import atomic_write_json, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from common.spark import create_spark_session, stop_spark
from storage.parquet_manager import write_dataset

log = get_logger("preprocessing.volcano_etl")


def _read_geojson(spark, path):
    raw = spark.read.option("multiLine", True).json(str(Path(path).resolve().as_uri()))
    return raw.select(F.explode("features").alias("f"))


def _safe_date(year_col, month_col, day_col):
    y = F.col(year_col).cast("int")
    m = F.when(F.col(month_col).cast("int").between(1, 12), F.col(month_col).cast("int")).otherwise(F.lit(1))
    d = F.when(F.col(day_col).cast("int").between(1, 31), F.col(day_col).cast("int")).otherwise(F.lit(1))
    return F.when(y.between(1, 9999), F.make_date(y, m, d)).otherwise(F.lit(None).cast("date"))


def build_volcanoes(spark, cfg):
    path = get_path(cfg, "data_raw") / "volcanoes" / "volcanoes" / "volcanoes.geojson"
    features = _read_geojson(spark, path)
    df = features.select(
        F.col("f.properties.Volcano_Number").cast("int").alias("volcano_number"),
        F.col("f.properties.Volcano_Name").cast("string").alias("volcano_name"),
        F.col("f.properties.Country").cast("string").alias("country"),
        F.col("f.properties.Region").cast("string").alias("region"),
        F.col("f.properties.Subregion").cast("string").alias("subregion"),
        F.col("f.properties.Volcanic_Landform").cast("string").alias("landform"),
        F.col("f.properties.Primary_Volcano_Type").cast("string").alias("primary_volcano_type"),
        F.col("f.properties.Tectonic_Setting").cast("string").alias("tectonic_setting"),
        F.col("f.properties.Elevation").cast("double").alias("elevation_m"),
        F.col("f.properties.Last_Eruption_Year").cast("int").alias("last_eruption_year"),
        F.col("f.properties.Geologic_Epoch").cast("string").alias("geologic_epoch"),
        F.col("f.properties.Evidence_Category").cast("string").alias("evidence_category"),
        F.col("f.geometry.coordinates")[0].cast("double").alias("longitude"),
        F.col("f.geometry.coordinates")[1].cast("double").alias("latitude"),
    ).filter(F.col("volcano_number").isNotNull())
    # Deduplicate only if the source ever exposes the same volcano twice.
    w = Window.partitionBy("volcano_number").orderBy(F.col("volcano_name"))
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    df = df.withColumn("valid_coordinates",
                       F.col("latitude").between(-90.0, 90.0) & F.col("longitude").between(-180.0, 180.0))
    return df


def build_eruptions(spark, cfg):
    path = get_path(cfg, "data_raw") / "volcanoes" / "eruptions" / "eruptions.geojson"
    features = _read_geojson(spark, path)
    df = features.select(
        F.col("f.properties.Volcano_Number").cast("int").alias("volcano_number"),
        F.col("f.properties.Volcano_Name").cast("string").alias("volcano_name"),
        F.col("f.properties.Eruption_Number").cast("int").alias("eruption_number"),
        F.col("f.properties.Activity_Type").cast("string").alias("activity_type"),
        F.col("f.properties.ExplosivityIndexMax").cast("int").alias("vei_max"),
        F.col("f.properties.ExplosivityIndexModifier").cast("string").alias("vei_modifier"),
        F.col("f.properties.StartDateYear").cast("int").alias("start_year"),
        F.col("f.properties.StartDateMonth").cast("int").alias("start_month"),
        F.col("f.properties.StartDateDay").cast("int").alias("start_day"),
        F.col("f.properties.StartDateYearUncertainty").cast("double").alias("start_year_uncertainty"),
        F.col("f.properties.StartDateYearModifier").cast("string").alias("start_year_modifier"),
        F.col("f.properties.StartDateDayModifier").cast("string").alias("start_day_modifier"),
        F.col("f.properties.StartDateDayUncertainty").cast("double").alias("start_day_uncertainty"),
        F.col("f.properties.StartEvidenceMethod").cast("string").alias("start_evidence_method"),
        F.col("f.properties.EndDateYear").cast("int").alias("end_year"),
        F.col("f.properties.EndDateMonth").cast("int").alias("end_month"),
        F.col("f.properties.EndDateDay").cast("int").alias("end_day"),
        F.col("f.properties.EndDateYearUncertainty").cast("double").alias("end_year_uncertainty"),
        F.col("f.geometry.coordinates")[0].cast("double").alias("longitude"),
        F.col("f.geometry.coordinates")[1].cast("double").alias("latitude"),
    ).filter(F.col("volcano_number").isNotNull() & F.col("eruption_number").isNotNull())
    df = (
        df.withColumn("start_date", _safe_date("start_year", "start_month", "start_day"))
        .withColumn("end_date", _safe_date("end_year", "end_month", "end_day"))
        .withColumn("date_precision",
            F.when(F.col("start_day").between(1, 31) & F.col("start_month").between(1, 12), F.lit("day"))
             .when(F.col("start_month").between(1, 12), F.lit("month"))
             .when(F.col("start_year").isNotNull(), F.lit("year"))
             .otherwise(F.lit("unknown")))
        .withColumn("date_is_approximate",
            F.col("start_year_uncertainty").isNotNull() | F.col("start_year_modifier").isNotNull())
        .withColumn("is_confirmed_eruption",
            F.lower(F.coalesce(F.col("activity_type"), F.lit(""))).contains("confirmed"))
        .withColumn("duration_days", F.datediff("end_date", "start_date"))
    )
    w = Window.partitionBy("volcano_number", "eruption_number").orderBy(F.col("start_year").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return df


def run(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    spark = create_spark_session("etl-volcanoes", cfg)
    try:
        volc = build_volcanoes(spark, cfg)
        eruptions = build_eruptions(spark, cfg)
        v_count = volc.count()
        e_count = eruptions.count()
        invalid_v = volc.filter(~F.col("valid_coordinates")).count()
        invalid_e = eruptions.filter(
            ~F.col("latitude").between(-90.0, 90.0) | ~F.col("longitude").between(-180.0, 180.0)
        ).count()
        precision = {r["date_precision"]: int(r["count"]) for r in eruptions.groupBy("date_precision").count().collect()}
        recent = eruptions.filter(F.col("start_year").between(2000, 2025)).count()
        missing = eruptions.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ("start_year", "start_month", "start_day", "start_year_uncertainty")
        ]).first().asDict()

        vm = write_dataset(volc, "volcanoes", "volcanoes", "volcanoes/volcanoes", cfg)
        em = write_dataset(eruptions, "eruptions", "eruptions", "volcanoes/eruptions", cfg)
        report = {
            "generated_at": utc_now_iso(),
            "volcanoes": int(v_count),
            "eruptions": int(e_count),
            "invalid_volcano_coordinates": int(invalid_v),
            "invalid_eruption_coordinates": int(invalid_e),
            "eruptions_2000_2025": int(recent),
            "date_precision": precision,
            "missing_counts": {k: int(v or 0) for k, v in missing.items()},
            "storage": {"volcanoes": vm, "eruptions": em},
            "source": cfg["volcano"]["citation"],
            "notes": [
                "Dates with month/day 0 or null are assigned Jan-1 only for a joinable date column;",
                "the original precision is stored in date_precision and must be respected in analysis.",
                "start_year_uncertainty and date modifiers are preserved.",
                "No missing dates or coordinates were invented.",
            ],
        }
        atomic_write_json(get_path(cfg, "results") / "metadata" / "preprocessing_volcanoes.json", report)
        log.info("Volcano ETL: %s volcanoes, %s eruptions, %s recent eruptions", v_count, e_count, recent)
        return report
    finally:
        stop_spark(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark volcano ETL")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    print(json.dumps(run(cfg), indent=2, default=str))


if __name__ == "__main__":
    main()
