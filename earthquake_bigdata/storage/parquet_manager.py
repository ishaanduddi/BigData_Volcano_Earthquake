"""Write Spark datasets to HDFS (primary) and a local mirror (for the dashboard).

The local mirror is a convenience copy, not a substitute for HDFS.  Every
written dataset gets a JSON metadata record stating the real backend used,
row count, partition count, byte size and verification result.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

from common.config import get_path, load_config
from common.io import atomic_write_json, directory_size, ensure_dir, parquet_dataset_stats, utc_now_iso
from common.logging_utils import get_logger
from storage.hdfs_manager import HDFSClient, resolve_backend

log = get_logger("storage.parquet")


def _local_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _write_spark(df, uri: str, partition_by: Optional[list], compression: str, mode: str = "overwrite"):
    writer = df.write.mode(mode).option("compression", compression)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(uri)


def write_dataset(
    df,
    dataset: str,
    hdfs_subdir: str,
    local_subdir: str,
    cfg: Optional[Dict[str, Any]] = None,
    partition_by: Optional[list] = None,
    verify: bool = True,
) -> Dict[str, Any]:
    """Write a Spark DataFrame and return actual storage metadata."""
    cfg = cfg or load_config()
    backend, client = resolve_backend(cfg)
    compression = cfg.get("storage", {}).get("parquet_compression", "snappy")
    local_path = get_path(cfg, "data_processed") / local_subdir
    hdfs_path = str(Path(cfg.get("storage", {}).get("hdfs_layout", {}).get("earthquake_processed", "/earthquake/processed")) / hdfs_subdir)
    if local_subdir.startswith("volcano"):
        hdfs_path = str(Path(cfg.get("storage", {}).get("hdfs_layout", {}).get("volcano_processed", "/volcano/processed")) / Path(local_subdir).name)

    row_count = df.count()
    write_meta: Dict[str, Any] = {
        "dataset": dataset,
        "created_at": utc_now_iso(),
        "rows": row_count,
        "backend_primary": backend,
        "partition_by": partition_by or [],
        "compression": compression,
        "hdfs_uri": None,
        "local_mirror": None,
        "hdfs_bytes": None,
        "local_bytes": None,
        "verified": False,
    }

    if backend == "hdfs" and client is not None:
        hdfs_uri = client.hdfs_path(hdfs_path)
        log.info("Writing %s (%s rows) to HDFS %s", dataset, row_count, hdfs_uri)
        _write_spark(df, hdfs_uri, partition_by, compression, mode="overwrite")
        write_meta["hdfs_uri"] = hdfs_uri
        if verify:
            read_back = df.sparkSession.read.parquet(hdfs_uri)
            back_count = read_back.count()
            write_meta["hdfs_rows_verified"] = back_count
            if back_count != row_count:
                raise RuntimeError(f"HDFS verification failed for {dataset}: wrote {row_count}, read {back_count}")
            write_meta["verified"] = True
        try:
            write_meta["hdfs_bytes"] = int(client.du(hdfs_path).strip().split()[0])
        except Exception as exc:
            log.warning("Could not measure HDFS size for %s: %s", hdfs_path, exc)

    if cfg.get("storage", {}).get("write_local_mirror", True):
        ensure_dir(local_path)
        log.info("Writing local mirror %s -> %s", dataset, local_path)
        # Write to a temporary sibling directory, then replace the old mirror.
        tmp = local_path.parent / (local_path.name + ".writing")
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        _write_spark(df, _local_uri(tmp), partition_by, compression, mode="overwrite")
        if local_path.exists():
            shutil.rmtree(local_path, ignore_errors=True)
        tmp.replace(local_path)
        write_meta["local_mirror"] = str(local_path)
        write_meta["local_bytes"] = directory_size(local_path)
        write_meta["local_files"] = parquet_dataset_stats(local_path)["files"]
        if verify and backend != "hdfs":
            read_back = df.sparkSession.read.parquet(_local_uri(local_path))
            back_count = read_back.count()
            write_meta["local_rows_verified"] = back_count
            if back_count != row_count:
                raise RuntimeError(f"Local verification failed for {dataset}: wrote {row_count}, read {back_count}")
            write_meta["verified"] = True

    if backend == "local":
        write_meta["hdfs_uri"] = None
        write_meta["note"] = "HDFS unavailable or disabled; data is local, not HDFS."

    meta_dir = get_path(cfg, "results") / "metadata"
    ensure_dir(meta_dir)
    atomic_write_json(meta_dir / f"{dataset}_storage.json", write_meta)
    log.info("Dataset %s stored: rows=%s backend=%s verified=%s", dataset, row_count, backend, write_meta["verified"])
    return write_meta


def publish_file_to_hdfs(local: str | Path, remote_subdir: str, cfg: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Upload a raw file to HDFS when available. Returns the HDFS URI or None.

    `remote_subdir` may be either a directory or a full destination file path.
    """
    cfg = cfg or load_config()
    backend, client = resolve_backend(cfg)
    if backend != "hdfs" or client is None:
        return None
    local_path = Path(local)
    remote = str(remote_subdir).replace("\\", "/")
    if not remote.endswith("/" + local_path.name):
        remote = str(Path(remote) / local_path.name).replace("\\", "/")
    client.put_file(local_path, remote)
    return client.hdfs_path(remote)


def write_results_table(df, name: str, cfg: Optional[Dict[str, Any]] = None, partition_by: Optional[list] = None) -> Dict[str, Any]:
    """Write an analytics result table locally and to /results on HDFS."""
    cfg = cfg or load_config()
    backend, client = resolve_backend(cfg)
    local = get_path(cfg, "results") / name
    ensure_dir(local)
    row_count = df.count()
    tmp = local.parent / (local.name + ".writing")
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    _write_spark(df, _local_uri(tmp), partition_by, cfg.get("storage", {}).get("parquet_compression", "snappy"))
    if local.exists():
        shutil.rmtree(local, ignore_errors=True)
    tmp.replace(local)
    meta = {"name": name, "rows": row_count, "local": str(local), "backend": backend}
    if backend == "hdfs" and client is not None:
        hdfs_uri = client.hdfs_path(str(Path(cfg.get("storage", {}).get("hdfs_layout", {}).get("results", "/results")) / name))
        _write_spark(df, hdfs_uri, partition_by, cfg.get("storage", {}).get("parquet_compression", "snappy"))
        meta["hdfs_uri"] = hdfs_uri
    return meta


def local_processed_path(cfg: Dict[str, Any], subdir: str) -> Path:
    return get_path(cfg, "data_processed") / subdir
