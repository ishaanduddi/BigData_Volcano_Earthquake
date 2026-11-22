"""Resumable USGS FDSN earthquake downloader.

Design notes
------------
* One request per month (or recursively split interval if a response would hit
  the FDSN event limit).
* Raw response bytes are written unchanged.  No filtering or editing occurs at
  ingestion time.
* Every completed chunk is recorded with its count-API value, row count, SHA-256
  and retrieval timestamp.  Re-running skips validated completed chunks.
* The 10,000-row sample is a deterministic stratified sample of real records
  drawn from months spanning 2000-2024, not the first rows returned by the API.
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import io
import json
import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.config import ensure_project_dirs, get_path, load_config
from common.io import (
    atomic_write_csv,
    atomic_write_json,
    atomic_write_text,
    count_csv_rows,
    ensure_dir,
    human_size,
    read_json,
    sha256_bytes,
    sha256_file,
    utc_now_iso,
)
from common.logging_utils import get_logger, setup_logging
from storage.parquet_manager import publish_file_to_hdfs

log = get_logger("ingestion.earthquakes")

EXPECTED_COLUMNS = [
    "time", "latitude", "longitude", "depth", "mag", "magType", "nst", "gap",
    "dmin", "rms", "net", "id", "updated", "place", "type", "status",
    "horizontalError", "depthError", "magError", "magNst", "locationSource",
    "magSource",
]


def _session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "earthquake_bigdata/1.0 (academic Big Data project)",
        "Accept": "text/csv,application/json",
    })
    return session


def month_ranges(start_date: str, end_date: str) -> List[Tuple[str, datetime, datetime]]:
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc) + timedelta(days=1)
    out = []
    cursor = start.replace(day=1)
    while cursor < end:
        if cursor.month == 12:
            nxt = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            nxt = cursor.replace(month=cursor.month + 1, day=1)
        out.append((cursor.strftime("%Y-%m"), cursor, min(nxt, end)))
        cursor = nxt
    return out


def _params_for_interval(cfg: Dict[str, Any], start: datetime, end: datetime, region: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    params = {
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": region.get("minmagnitude", cfg["usgs"]["min_magnitude"]) if region else cfg["usgs"]["min_magnitude"],
        "orderby": "time-asc",
    }
    if region:
        params.update({
            "minlatitude": region["minlatitude"],
            "maxlatitude": region["maxlatitude"],
            "minlongitude": region["minlongitude"],
            "maxlongitude": region["maxlongitude"],
        })
    return params


def _count(session: requests.Session, cfg: Dict[str, Any], params: Dict[str, Any]) -> int:
    p = dict(params)
    p.pop("orderby", None)
    for attempt in range(1, int(cfg["usgs"].get("retries", 5)) + 1):
        try:
            r = session.get(cfg["usgs"]["count_url"], params=p, timeout=cfg["usgs"].get("request_timeout_seconds", 180))
            if r.status_code == 200:
                text = r.text.strip()
                return int(text)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            raise RuntimeError(f"USGS count failed HTTP {r.status_code}: {r.text[:300]}")
        except Exception as exc:
            if attempt >= int(cfg["usgs"].get("retries", 5)):
                raise
            wait = int(cfg["usgs"].get("backoff_seconds", 3)) * attempt
            log.warning("Count request failed (%s); retry %s in %ss", exc, attempt, wait)
            time.sleep(wait)
    return 0


def _download_csv(session: requests.Session, cfg: Dict[str, Any], params: Dict[str, Any]) -> bytes:
    p = dict(params)
    p["format"] = "csv"
    p["limit"] = int(cfg["usgs"].get("max_limit", 20000))
    for attempt in range(1, int(cfg["usgs"].get("retries", 5)) + 1):
        try:
            r = session.get(cfg["usgs"]["query_url"], params=p, timeout=cfg["usgs"].get("request_timeout_seconds", 180))
            if r.status_code == 200 and r.content:
                return r.content
            if r.status_code == 204:
                return b""
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            raise RuntimeError(f"USGS query failed HTTP {r.status_code}: {r.text[:300]}")
        except Exception as exc:
            if attempt >= int(cfg["usgs"].get("retries", 5)):
                raise
            wait = int(cfg["usgs"].get("backoff_seconds", 3)) * attempt
            log.warning("Download failed (%s); retry %s in %ss", exc, attempt, wait)
            time.sleep(wait)
    raise RuntimeError("USGS download exhausted retries")


def _validate_csv_bytes(data: bytes) -> Tuple[int, str]:
    if not data:
        return 0, ""
    text = data.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()
    if not lines:
        return 0, ""
    header = lines[0].strip()
    missing = [c for c in EXPECTED_COLUMNS if c not in header.split(",")]
    if missing:
        raise ValueError(f"USGS CSV missing expected columns: {missing[:5]} ...")
    return max(0, len(lines) - 1), header


def _manifest_path(base: Path) -> Path:
    return base / "_manifest.json"


def _load_manifest(base: Path) -> Dict[str, Any]:
    return read_json(_manifest_path(base), default={"chunks": {}, "created_at": utc_now_iso()}) or {"chunks": {}}


def _save_manifest(base: Path, manifest: Dict[str, Any]) -> None:
    atomic_write_json(_manifest_path(base), manifest)


def _filename(year: int, month: int, suffix: str = ".csv") -> str:
    return f"{year}-{month:02d}{suffix}"


def download_interval(
    session: requests.Session,
    cfg: Dict[str, Any],
    dataset: str,
    base: Path,
    year: int,
    month: int,
    start: datetime,
    end: datetime,
    region: Optional[Dict[str, Any]],
    force: bool = False,
    dry_run: bool = False,
    upload_hdfs: bool = True,
    filename_suffix: str = "",
) -> List[Dict[str, Any]]:
    """Download one calendar month, recursively splitting if count > API limit."""
    params = _params_for_interval(cfg, start, end, region)
    count = _count(session, cfg, params)
    max_limit = int(cfg["usgs"].get("max_limit", 20000))
    if dry_run:
        log.info("[dry-run] %s %s count=%s", dataset, start.date(), count)
        return [{"count": count, "dry_run": True, "start": start.isoformat(), "end": end.isoformat()}]

    if count > max_limit:
        if (end - start) <= timedelta(days=1):
            raise RuntimeError(f"More than {max_limit} events in one day for {dataset} {start}; cannot split further.")
        log.warning("Count %s exceeds limit %s for %s; splitting into daily files", count, max_limit, dataset)
        entries: List[Dict[str, Any]] = []
        cursor = start
        while cursor < end:
            day_end = min(cursor + timedelta(days=1), end)
            suffix = f"-{cursor.day:02d}" if filename_suffix == "" else f"{filename_suffix}-{cursor.day:02d}"
            entries.extend(download_interval(
                session, cfg, dataset, base, year, month, cursor, day_end, region,
                force, dry_run, upload_hdfs, suffix,
            ))
            cursor = day_end
        return entries

    fname = _filename(year, month, filename_suffix + ".csv")
    path = base / str(year) / fname
    manifest = _load_manifest(base)
    key = str(path.relative_to(base)).replace("\\", "/")
    existing = manifest["chunks"].get(key)
    if path.exists() and not force:
        try:
            rows = count_csv_rows(path)
            if existing and existing.get("rows") == rows and existing.get("count_api") == count and existing.get("status") == "complete":
                log.info("Skip validated %s (%s rows)", key, rows)
                return [existing]
            if rows == count:
                entry = {
                    "file": key,
                    "dataset": dataset,
                    "count_api": count,
                    "rows": rows,
                    "sha256": sha256_file(path),
                    "downloaded_at": existing.get("downloaded_at", utc_now_iso()),
                    "status": "complete",
                    "adopted_existing": True,
                }
                manifest["chunks"][key] = entry
                _save_manifest(base, manifest)
                log.info("Adopted existing validated %s (%s rows)", key, rows)
                return [entry]
        except Exception as exc:
            log.warning("Existing file %s failed validation (%s); redownloading", key, exc)

    log.info("Downloading %s %s (count API=%s)", dataset, key, count)
    data = _download_csv(session, cfg, params)
    rows, header = _validate_csv_bytes(data)
    if rows not in (count, 0):
        # The catalogue can change between the count and query calls.  A small
        # mismatch is logged; a large mismatch aborts so it cannot be hidden.
        diff = abs(rows - count)
        if diff > max(10, 0.02 * max(count, 1)):
            raise RuntimeError(f"Row/count mismatch for {key}: CSV={rows}, count API={count}")
        log.warning("Row/count mismatch for %s: CSV=%s count=%s (catalogue changed)", key, rows, count)

    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)
    entry = {
        "file": key,
        "dataset": dataset,
        "count_api": count,
        "rows": rows,
        "sha256": sha256_bytes(data),
        "downloaded_at": utc_now_iso(),
        "status": "complete",
        "url_params": {k: v for k, v in params.items()},
        "bytes": len(data),
    }
    manifest["chunks"][key] = entry
    manifest["updated_at"] = utc_now_iso()
    _save_manifest(base, manifest)

    try:
        if upload_hdfs:
            remote = f"/earthquake/raw/{dataset}/{key}"
            uri = publish_file_to_hdfs(path, remote, cfg)
            if uri:
                entry["hdfs_uri"] = uri
                manifest["chunks"][key] = entry
                _save_manifest(base, manifest)
    except Exception as exc:
        log.warning("HDFS upload failed for %s (raw local copy retained): %s", key, exc)
    return [entry]


def download_dataset(
    dataset: str,
    cfg: Optional[Dict[str, Any]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    region: Optional[Dict[str, Any]] = None,
    force: bool = False,
    dry_run: bool = False,
    max_months: Optional[int] = None,
    upload_hdfs: bool = True,
) -> Dict[str, Any]:
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    start_date = start_date or cfg["usgs"]["start_date"]
    end_date = end_date or cfg["usgs"]["end_date"]
    if dataset == "global" or dataset == "sample":
        base = get_path(cfg, "data_raw") / "earthquakes" / "global"
    else:
        base = get_path(cfg, "data_raw") / "earthquakes" / "stress" / dataset.replace("stress_", "")
    ensure_dir(base)
    session = _session()
    months = month_ranges(start_date, end_date)
    if max_months:
        months = months[:max_months]
    log.info("Starting %s: %s monthly chunks (%s .. %s)", dataset, len(months), start_date, end_date)
    results = []
    for idx, (label, start, end) in enumerate(months, 1):
        log.info("[%s/%s] %s", idx, len(months), label)
        results.extend(download_interval(session, cfg, dataset, base, start.year, start.month, start, end, region, force, dry_run, upload_hdfs))
        time.sleep(0.12)
    manifest = _load_manifest(base)
    total_rows = sum(int(c.get("rows", 0)) for c in manifest.get("chunks", {}).values())
    report = {
        "dataset": dataset,
        "source": cfg["usgs"]["query_url"],
        "count_url": cfg["usgs"]["count_url"],
        "start_date": start_date,
        "end_date": end_date,
        "region": region,
        "generated_at": utc_now_iso(),
        "n_chunks": len(manifest.get("chunks", {})),
        "total_rows": total_rows,
        "chunks_requested_this_run": len(months),
        "dry_run": dry_run,
    }
    atomic_write_json(base / "_download_report.json", report)
    log.info("Finished %s: %s chunks, %s raw rows", dataset, report["n_chunks"], total_rows)
    return report


def _stratified_sample(df: pd.DataFrame, n: int, strata: List[str], seed: int) -> pd.DataFrame:
    """Proportional stratified sample with largest-remainder allocation."""
    if len(df) <= n:
        return df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    groups = list(df.groupby(strata, dropna=False, observed=True))
    sizes = [len(g) for _, g in groups]
    total = sum(sizes)
    raw = [n * s / total for s in sizes]
    alloc = [min(int(math.floor(x)), s) for x, s in zip(raw, sizes)]
    remainder = n - sum(alloc)
    order = sorted(range(len(groups)), key=lambda i: (raw[i] - alloc[i]), reverse=True)
    while remainder > 0:
        progressed = False
        for i in order:
            if remainder <= 0:
                break
            if alloc[i] < sizes[i]:
                alloc[i] += 1
                remainder -= 1
                progressed = True
        if not progressed:
            break
    parts = [g.sample(n=a, random_state=seed + i) for i, ((_, g), a) in enumerate(zip(groups, alloc))]
    sampled = pd.concat(parts).sample(frac=1.0, random_state=seed).reset_index(drop=True)
    return sampled


def create_sample(cfg: Optional[Dict[str, Any]] = None, force: bool = False) -> Dict[str, Any]:
    """Build a deterministic 10k stratified sample from real downloaded months."""
    cfg = cfg or load_config()
    sample_cfg = cfg["usgs"]["sample"]
    global_base = get_path(cfg, "data_raw") / "earthquakes" / "global"
    out_dir = get_path(cfg, "data_sample")
    ensure_dir(out_dir)
    out_csv = out_dir / "earthquakes_10k.csv"
    manifest_path = out_dir / "sample_manifest.json"
    if out_csv.exists() and manifest_path.exists() and not force:
        m = read_json(manifest_path)
        if m and m.get("rows", 0) > 0:
            log.info("Sample already exists (%s rows); use --force to rebuild.", m.get("rows"))
            return m

    frames = []
    source_files = []
    for ym in sample_cfg["months"]:
        year, month = (int(x) for x in ym.split("-"))
        path = global_base / str(year) / f"{year}-{month:02d}.csv"
        if not path.exists():
            log.warning("Sample month %s has not been downloaded yet: %s", ym, path)
            continue
        df = pd.read_csv(path, low_memory=False)
        df["source_month"] = ym
        frames.append(df)
        source_files.append(str(path.relative_to(get_path(cfg, "data_raw"))))
    if not frames:
        raise RuntimeError("No sample-month files found. Run download_earthquakes.py --dataset sample first.")
    all_df = pd.concat(frames, ignore_index=True)
    # Deduplicate exactly as the ETL will: latest 'updated' revision per ID.
    all_df["updated_parsed"] = pd.to_datetime(all_df["updated"], errors="coerce", utc=True)
    all_df = all_df.sort_values("updated_parsed").drop_duplicates("id", keep="last")
    all_df = all_df.drop(columns=["updated_parsed"])
    all_df["_mag_bin"] = pd.cut(
        pd.to_numeric(all_df["mag"], errors="coerce"),
        bins=sample_cfg["magnitude_bins"],
        right=False,
        labels=False,
    )
    sampled = _stratified_sample(
        all_df,
        int(sample_cfg["target_rows"]),
        strata=["source_month", "_mag_bin"],
        seed=int(cfg["project"]["random_seed"]),
    ).drop(columns=["_mag_bin"])
    # Preserve the original columns and append provenance.
    cols = [c for c in EXPECTED_COLUMNS if c in sampled.columns]
    sampled = sampled[cols + [c for c in sampled.columns if c not in cols]]
    atomic_write_csv(out_csv, sampled)
    manifest = {
        "dataset": "sample_10k",
        "source": cfg["usgs"]["query_url"],
        "created_at": utc_now_iso(),
        "seed": int(cfg["project"]["random_seed"]),
        "target_rows": int(sample_cfg["target_rows"]),
        "rows": int(len(sampled)),
        "raw_rows_before_sampling": int(len(all_df)),
        "source_months": sample_cfg["months"],
        "source_files": source_files,
        "sampling": "proportional stratified by source_month and magnitude bin (0.5-1.0 width); real records only",
        "year_counts": {str(k): int(v) for k, v in sampled["time"].str[:4].value_counts().sort_index().items()},
        "magnitude_range": [float(pd.to_numeric(sampled["mag"], errors="coerce").min()), float(pd.to_numeric(sampled["mag"], errors="coerce").max())],
        "file_sha256": sha256_file(out_csv),
        "file_bytes": out_csv.stat().st_size,
    }
    atomic_write_json(manifest_path, manifest)
    try:
        publish_file_to_hdfs(out_csv, "/earthquake/raw/sample", cfg)
    except Exception as exc:
        log.warning("Could not upload sample to HDFS: %s", exc)
    log.info("Created sample %s (%s rows, %s)", out_csv, manifest["rows"], human_size(out_csv.stat().st_size))
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download USGS earthquake data (resumable).")
    parser.add_argument("--dataset", choices=["global", "sample", "stress_alaska", "stress_california", "stress_all"], default="sample")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-months", type=int, default=None)
    parser.add_argument("--rebuild-sample", action="store_true")
    parser.add_argument("--no-hdfs", action="store_true", help="keep raw files local; upload in one batch later")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    upload_hdfs = not args.no_hdfs

    if args.dataset == "sample":
        # Download the selected real months first, then build the 10k sample.
        months = cfg["usgs"]["sample"]["months"]
        for ym in months:
            year, month = (int(x) for x in ym.split("-"))
            start = datetime(year, month, 1, tzinfo=timezone.utc)
            end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
            download_dataset("global", cfg, start.isoformat(), (end - timedelta(days=1)).date().isoformat(),
                             force=args.force, dry_run=args.dry_run, upload_hdfs=upload_hdfs)
        if not args.dry_run:
            create_sample(cfg, force=args.rebuild_sample or args.force)
    elif args.dataset == "stress_all":
        for name, region in cfg["stress"]["regions"].items():
            download_dataset(f"stress_{name}", cfg, start_date=args.start, end_date=args.end, region=region, force=args.force,
                             dry_run=args.dry_run, max_months=args.max_months, upload_hdfs=upload_hdfs)
    elif args.dataset.startswith("stress_"):
        name = args.dataset.replace("stress_", "")
        region = cfg["stress"]["regions"][name]
        download_dataset(args.dataset, cfg, start_date=args.start, end_date=args.end, region=region, force=args.force,
                         dry_run=args.dry_run, max_months=args.max_months, upload_hdfs=upload_hdfs)
    else:
        download_dataset("global", cfg, args.start, args.end, force=args.force,
                         dry_run=args.dry_run, max_months=args.max_months, upload_hdfs=upload_hdfs)


if __name__ == "__main__":
    main()
