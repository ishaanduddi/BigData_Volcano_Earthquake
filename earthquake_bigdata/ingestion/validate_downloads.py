"""Validate downloaded raw datasets without modifying them."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from common.config import ensure_project_dirs, get_path, load_config
from common.io import atomic_write_json, count_csv_rows, read_json, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from ingestion.download_earthquakes import EXPECTED_COLUMNS

log = get_logger("ingestion.validate")


def validate_earthquake_dir(path: Path, dataset: str) -> Dict[str, Any]:
    manifest = read_json(path / "_manifest.json", default={"chunks": {}}) or {"chunks": {}}
    files = sorted(path.glob("[0-9][0-9][0-9][0-9]/*.csv"))
    result: Dict[str, Any] = {
        "dataset": dataset,
        "path": str(path),
        "manifest_chunks": len(manifest.get("chunks", {})),
        "files_found": len(files),
        "total_rows": 0,
        "missing_columns": [],
        "invalid_coordinates": 0,
        "invalid_times": 0,
        "duplicate_ids_within_file": 0,
        "date_min": None,
        "date_max": None,
        "errors": [],
        "files": [],
    }
    date_min = None
    date_max = None
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
        except Exception as exc:
            result["errors"].append(f"{f.name}: cannot read CSV: {exc}")
            continue
        rows = len(df)
        result["total_rows"] += rows
        missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        if missing:
            result["missing_columns"].extend(missing)
        lat = pd.to_numeric(df.get("latitude"), errors="coerce")
        lon = pd.to_numeric(df.get("longitude"), errors="coerce")
        invalid_coord = int(((lat < -90) | (lat > 90) | (lon < -180) | (lon > 180) | lat.isna() | lon.isna()).sum()) if lat is not None and lon is not None else rows
        result["invalid_coordinates"] += invalid_coord
        times = pd.to_datetime(df.get("time"), errors="coerce", utc=True)
        result["invalid_times"] += int(times.isna().sum())
        if times.notna().any():
            tmin = times.min()
            tmax = times.max()
            date_min = tmin if date_min is None or tmin < date_min else date_min
            date_max = tmax if date_max is None or tmax > date_max else date_max
        dup = int(df["id"].duplicated().sum()) if "id" in df.columns else 0
        result["duplicate_ids_within_file"] += dup
        result["files"].append({
            "file": str(f.relative_to(path)),
            "rows": rows,
            "invalid_coordinates": invalid_coord,
            "duplicate_ids": dup,
            "sha256_present_in_manifest": any(
                c.get("sha256") for c in manifest.get("chunks", {}).values()
                if c.get("file", "").endswith(f.name)
            ),
        })
    result["date_min"] = str(date_min) if date_min is not None else None
    result["date_max"] = str(date_max) if date_max is not None else None
    if result["missing_columns"]:
        result["errors"].append(f"Missing columns: {sorted(set(result['missing_columns']))}")
    if not files:
        result["errors"].append("No monthly CSV files found")
    result["ok"] = not result["errors"] and result["invalid_coordinates"] == 0 and result["invalid_times"] == 0
    return result


def validate_sample(cfg: Dict[str, Any]) -> Dict[str, Any]:
    path = get_path(cfg, "data_sample") / "earthquakes_10k.csv"
    manifest = read_json(get_path(cfg, "data_sample") / "sample_manifest.json", default={})
    if not path.exists():
        return {"dataset": "sample_10k", "ok": False, "errors": ["sample file missing"]}
    df = pd.read_csv(path, low_memory=False)
    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")
    mag = pd.to_numeric(df["mag"], errors="coerce")
    times = pd.to_datetime(df["time"], errors="coerce", utc=True)
    result = {
        "dataset": "sample_10k",
        "path": str(path),
        "rows": len(df),
        "manifest_rows": manifest.get("rows"),
        "unique_ids": int(df["id"].nunique()),
        "duplicate_ids": int(df["id"].duplicated().sum()),
        "missing_latitude": int(lat.isna().sum()),
        "missing_longitude": int(lon.isna().sum()),
        "missing_magnitude": int(mag.isna().sum()),
        "invalid_coordinates": int(((lat < -90) | (lat > 90) | (lon < -180) | (lon > 180) | lat.isna() | lon.isna()).sum()),
        "magnitude_min": float(mag.min()),
        "magnitude_max": float(mag.max()),
        "date_min": str(times.min()),
        "date_max": str(times.max()),
        "source_months": int(df["source_month"].nunique()) if "source_month" in df.columns else None,
        "errors": [],
    }
    if result["rows"] < 9000:
        result["errors"].append(f"Sample has only {result['rows']} rows (target 10000)")
    if result["duplicate_ids"]:
        result["errors"].append("Duplicate IDs in sample")
    if result["invalid_coordinates"]:
        result["errors"].append("Invalid coordinates in sample")
    if result["manifest_rows"] != result["rows"]:
        result["errors"].append("Manifest row count does not match file")
    result["ok"] = not result["errors"]
    return result


def validate_volcanoes(cfg: Dict[str, Any]) -> Dict[str, Any]:
    base = get_path(cfg, "data_raw") / "volcanoes"
    out = {"layers": {}, "ok": True, "errors": []}
    required_v = ["Volcano_Number", "Volcano_Name", "Latitude", "Longitude"]
    required_e = ["Volcano_Number", "StartDateYear"]
    for layer, required in (("volcanoes", required_v), ("eruptions", required_e)):
        path = base / layer / f"{layer}.geojson"
        manifest = read_json(base / layer / "_manifest.json", default={})
        if not path.exists():
            out["layers"][layer] = {"ok": False, "error": "combined GeoJSON missing"}
            out["errors"].append(f"{layer}: missing")
            continue
        data = read_json(path)
        features = data.get("features", [])
        props = [f.get("properties", {}) for f in features]
        missing = {k: sum(1 for p in props if p.get(k) is None) for k in required}
        info = {
            "features": len(features),
            "number_matched": manifest.get("number_matched"),
            "missing_required": missing,
            "retrieved_at": manifest.get("retrieved_at"),
        }
        if layer == "volcanoes":
            lat = pd.to_numeric(pd.Series([p.get("Latitude") for p in props]), errors="coerce")
            lon = pd.to_numeric(pd.Series([p.get("Longitude") for p in props]), errors="coerce")
            info["invalid_coordinates"] = int(((lat < -90) | (lat > 90) | (lon < -180) | (lon > 180) | lat.isna() | lon.isna()).sum())
            info["unique_volcano_numbers"] = int(pd.Series([p.get("Volcano_Number") for p in props]).nunique())
        else:
            years = pd.to_numeric(pd.Series([p.get("StartDateYear") for p in props]), errors="coerce")
            info["year_min"] = None if years.dropna().empty else int(years.min())
            info["year_max"] = None if years.dropna().empty else int(years.max())
            info["uncertainty_years_present"] = int(sum(1 for p in props if p.get("StartDateYearUncertainty") is not None))
        info["ok"] = (
            len(features) > 0
            and info["number_matched"] in (None, len(features))
            and all(v == 0 for v in missing.values())
            and info.get("invalid_coordinates", 0) == 0
        )
        if not info["ok"]:
            out["errors"].append(f"{layer}: validation failed")
        out["layers"][layer] = info
    out["ok"] = not out["errors"]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate raw downloads.")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    ensure_project_dirs(cfg)
    report: Dict[str, Any] = {"generated_at": utc_now_iso(), "datasets": {}}

    global_dir = get_path(cfg, "data_raw") / "earthquakes" / "global"
    if global_dir.exists() and any(global_dir.glob("[0-9][0-9][0-9][0-9]/*.csv")):
        report["datasets"]["global_raw"] = validate_earthquake_dir(global_dir, "global")
    else:
        report["datasets"]["global_raw"] = {"dataset": "global", "ok": False, "errors": ["not downloaded yet"]}

    for name in cfg["stress"]["regions"]:
        p = get_path(cfg, "data_raw") / "earthquakes" / "stress" / name
        if p.exists() and any(p.glob("[0-9][0-9][0-9][0-9]/*.csv")):
            report["datasets"][f"stress_{name}"] = validate_earthquake_dir(p, f"stress_{name}")

    report["datasets"]["sample_10k"] = validate_sample(cfg)
    report["datasets"]["volcanoes"] = validate_volcanoes(cfg)
    report["ok"] = all(d.get("ok", False) for d in report["datasets"].values())
    out = get_path(cfg, "results") / "metadata" / "validation_report.json"
    atomic_write_json(out, report)
    print(json.dumps(report, indent=2, default=str)[:8000])
    if args.strict and not report["ok"]:
        raise SystemExit(1)

    # Summary line without dumping every file.
    for name, d in report["datasets"].items():
        log.info("%s: ok=%s %s", name, d.get("ok"), {k: d.get(k) for k in ("rows", "total_rows", "features", "errors") if k in d})


if __name__ == "__main__":
    main()
