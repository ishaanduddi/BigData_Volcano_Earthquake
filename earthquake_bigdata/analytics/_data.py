"""Shared data-access helpers for analytics modules."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from common.config import get_path, load_config
from common.io import ensure_dir
from common.logging_utils import get_logger
from storage.hdfs_manager import resolve_backend

log = get_logger("analytics.data")


def processed_location(cfg: Dict[str, Any], subdir: str) -> tuple[str, str]:
    """Return (Spark-readable URI, backend) for a processed dataset."""
    backend, client = resolve_backend(cfg)
    if backend == "hdfs" and client is not None:
        name = Path(subdir).name
        if subdir.startswith("earthquakes/"):
            return client.hdfs_path(f"/earthquake/processed/{name}"), "hdfs"
        if subdir.startswith("volcanoes/"):
            return client.hdfs_path(f"/volcano/processed/{name}"), "hdfs"
        return client.hdfs_path(subdir), "hdfs"
    local = get_path(cfg, "data_processed") / subdir
    return local.resolve().as_uri(), "local"


def read_processed(spark, cfg: Dict[str, Any], subdir: str):
    uri, backend = processed_location(cfg, subdir)
    log.info("Reading processed dataset %s from %s", subdir, uri)
    return spark.read.parquet(uri), backend


def results_dir(cfg: Optional[Dict[str, Any]] = None, sub: str = "") -> Path:
    cfg = cfg or load_config()
    p = get_path(cfg, "results") / sub if sub else get_path(cfg, "results")
    ensure_dir(p)
    return p


def save_small_table(df, cfg: Dict[str, Any], name: str, sub: str = "", order_by: Optional[list] = None, limit: Optional[int] = None) -> "pandas.DataFrame":
    """Collect a small aggregated Spark table to CSV and return the DataFrame."""
    if order_by:
        df = df.orderBy(*order_by)
    if limit:
        df = df.limit(limit)
    pdf = df.toPandas()
    out = results_dir(cfg, sub) / name
    ensure_dir(out.parent)
    pdf.to_csv(out, index=False)
    log.info("Saved %s (%s rows) -> %s", name, len(pdf), out)
    return pdf
