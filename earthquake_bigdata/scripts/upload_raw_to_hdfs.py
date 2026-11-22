"""Batch-upload local raw data to HDFS (one CLI call per year directory).

This avoids the startup overhead of invoking the Hadoop CLI for every monthly
file.  It only uploads bytes; the local raw files remain the source of truth and
are never modified.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import get_path, load_config
from common.logging_utils import setup_logging
from storage.hdfs_manager import HDFSClient


def upload_tree(client: HDFSClient, local_base: Path, remote_base: str) -> int:
    n = 0
    for year_dir in sorted(local_base.glob("[0-9][0-9][0-9][0-9]")):
        files = list(year_dir.glob("*.csv"))
        if not files:
            continue
        client.put_dir(year_dir, remote_base)
        n += len(files)
        print(f"Uploaded {len(files)} files from {year_dir.name} -> {remote_base}/{year_dir.name}")
    manifest = local_base / "_manifest.json"
    if manifest.exists():
        client.put_file(manifest, remote_base + "/_manifest.json")
    report = local_base / "_download_report.json"
    if report.exists():
        client.put_file(report, remote_base + "/_download_report.json")
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch upload raw data to HDFS")
    parser.add_argument("--dataset", choices=["global", "stress", "both"], default="global")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    client = HDFSClient(cfg)
    if not client.available():
        raise SystemExit(f"HDFS not reachable at {client.uri}")
    total = 0
    if args.dataset in ("global", "both"):
        total += upload_tree(client, get_path(cfg, "data_raw") / "earthquakes" / "global", "/earthquake/raw/global")
    if args.dataset in ("stress", "both"):
        for name in cfg["stress"]["regions"]:
            local = get_path(cfg, "data_raw") / "earthquakes" / "stress" / name
            if local.exists():
                total += upload_tree(client, local, f"/earthquake/raw/stress/{name}")
    print(f"Uploaded {total} monthly files. HDFS root:\n{client.ls('/earthquake/raw')}")


if __name__ == "__main__":
    main()
