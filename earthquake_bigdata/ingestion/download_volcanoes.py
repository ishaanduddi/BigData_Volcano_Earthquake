"""Download Smithsonian GVP volcano and eruption records from the official WFS.

The public volcano.si.edu website presents a bot-verification challenge, so the
official Smithsonian GeoServer WFS (webservices.volcano.si.edu) is used.  It is
the same GVP-VOTW dataset and requires no key.  Each page is retained as raw
JSON so interrupted downloads can resume without repeating completed pages.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.config import ensure_project_dirs, get_path, load_config
from common.io import atomic_write_json, atomic_write_text, ensure_dir, read_json, sha256_file, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from storage.parquet_manager import publish_file_to_hdfs

log = get_logger("ingestion.volcanoes")


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=8, connect=5, read=5, backoff_factor=3,
                  status_forcelist=(403, 429, 500, 502, 503, 504), allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36 earthquake_bigdata/1.0",
        "Accept": "application/json",
    })
    return s


def _fetch_page(session, cfg, type_name: str, start_index: int, count: int, sort_by: str) -> Dict[str, Any]:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": type_name,
        "outputFormat": "application/json",
        "startIndex": start_index,
        "count": count,
        # GeoServer requires a stable sort key for startIndex paging.
        "sortBy": sort_by,
    }
    r = session.get(cfg["volcano"]["wfs_url"], params=params,
                    timeout=cfg["volcano"].get("request_timeout_seconds", 180))
    r.raise_for_status()
    data = r.json()
    if "features" not in data:
        raise RuntimeError(f"WFS response has no features: {r.text[:500]}")
    return data


def download_layer(cfg: Dict[str, Any], layer: str, type_name: str, force: bool = False) -> Dict[str, Any]:
    base = get_path(cfg, "data_raw") / "volcanoes" / layer
    ensure_dir(base)
    manifest_path = base / "_manifest.json"
    manifest = read_json(manifest_path, default={"pages": {}, "number_matched": None}) or {"pages": {}}
    session = _session()
    page_size = int(cfg["volcano"].get("page_size", 1000))
    sort_by = "Volcano_Number,Eruption_Number" if "Eruption" in type_name else "Volcano_Number"
    first = _fetch_page(session, cfg, type_name, 0, 1, sort_by)
    number_matched = int(first.get("numberMatched") or first.get("totalFeatures") or 0)
    if number_matched == 0:
        raise RuntimeError(f"WFS layer {type_name} reports zero features")
    log.info("%s: %s official records", layer, number_matched)
    manifest["number_matched"] = number_matched
    manifest["type_name"] = type_name
    manifest["wfs_url"] = cfg["volcano"]["wfs_url"]
    manifest["updated_at"] = utc_now_iso()
    atomic_write_json(manifest_path, manifest)

    start = 0
    max_single = int(cfg["volcano"].get("single_request_max_records", 20000))
    full_page_path = base / "page_full.json"
    use_single = number_matched <= max_single
    if use_single:
        if full_page_path.exists() and not force:
            existing = read_json(full_page_path, default={})
            if len(existing.get("features", [])) == number_matched:
                log.info("Using existing complete page_full.json (%s features)", len(existing["features"]))
                manifest["pages"] = {full_page_path.name: {
                    "start_index": 0, "features": len(existing["features"]),
                    "sha256": sha256_file(full_page_path), "status": "complete",
                }}
                start = number_matched
            else:
                use_single = False
        if use_single and start == 0:
            log.info("Requesting full %s layer in one WFS request (count=%s)", layer, number_matched)
            data = _fetch_page(session, cfg, type_name, 0, number_matched, sort_by)
            n = len(data.get("features", []))
            if n != number_matched:
                raise RuntimeError(f"WFS full request returned {n} features, expected {number_matched}")
            atomic_write_text(full_page_path, json.dumps(data, ensure_ascii=False))
            manifest["pages"] = {full_page_path.name: {
                "start_index": 0, "features": n,
                "sha256": sha256_file(full_page_path), "status": "complete",
            }}
            manifest["updated_at"] = utc_now_iso()
            atomic_write_json(manifest_path, manifest)
            log.info("Downloaded %s (%s features)", full_page_path.name, n)
            start = number_matched

    while start < number_matched:
        page = start // page_size
        path = base / f"page_{page:04d}.json"
        key = path.name
        if path.exists() and not force:
            try:
                existing = read_json(path)
                if len(existing.get("features", [])) > 0:
                    log.info("Skip existing %s (%s features)", key, len(existing["features"]))
                    manifest["pages"][key] = {
                        "start_index": start,
                        "features": len(existing["features"]),
                        "sha256": sha256_file(path),
                        "status": "complete",
                    }
                    start += page_size
                    continue
            except Exception:
                pass
        data = _fetch_page(session, cfg, type_name, start, page_size, sort_by)
        atomic_write_text(path, json.dumps(data, ensure_ascii=False))
        n = len(data.get("features", []))
        manifest["pages"][key] = {
            "start_index": start,
            "features": n,
            "sha256": sha256_file(path),
            "status": "complete",
        }
        manifest["updated_at"] = utc_now_iso()
        atomic_write_json(manifest_path, manifest)
        log.info("Downloaded %s (%s features, %s/%s)", key, n, start + n, number_matched)
        start += page_size
        if n == 0:
            break
        time.sleep(2.0)

    # Combine one full page, or all paged files, into one GeoJSON.
    features: List[Dict[str, Any]] = []
    if full_page_path.exists() and use_single:
        features.extend(read_json(full_page_path, default={}).get("features", []))
    else:
        for path in sorted(base.glob("page_[0-9]*.json")):
            data = read_json(path, default={})
            features.extend(data.get("features", []))
    combined = {
        "type": "FeatureCollection",
        "name": layer,
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features,
        "numberMatched": number_matched,
        "source": cfg["volcano"]["wfs_url"],
        "typeName": type_name,
        "retrieved_at": utc_now_iso(),
    }
    out = base / f"{layer}.geojson"
    atomic_write_text(out, json.dumps(combined, ensure_ascii=False))
    manifest["combined_file"] = str(out)
    manifest["combined_features"] = len(features)
    manifest["combined_sha256"] = sha256_file(out)
    manifest["retrieved_at"] = utc_now_iso()
    atomic_write_json(manifest_path, manifest)
    try:
        publish_file_to_hdfs(out, f"/volcano/raw/{layer}", cfg)
    except Exception as exc:
        log.warning("HDFS upload failed for %s: %s", layer, exc)
    log.info("Combined %s features into %s", len(features), out)
    return manifest


def create_volcano_sample(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Create a small real-data volcano sample (50 with recent eruptions)."""
    raw_volc = get_path(cfg, "data_raw") / "volcanoes" / "volcanoes" / "volcanoes.geojson"
    raw_eruptions = get_path(cfg, "data_raw") / "volcanoes" / "eruptions" / "eruptions.geojson"
    out_dir = get_path(cfg, "data_sample")
    ensure_dir(out_dir)
    if not raw_volc.exists() or not raw_eruptions.exists():
        raise FileNotFoundError("Download volcano and eruption layers first.")
    volc = read_json(raw_volc)
    eruptions = read_json(raw_eruptions)
    eprops = [f.get("properties", {}) for f in eruptions.get("features", [])]
    recent = {}
    for p in eprops:
        year = p.get("StartDateYear")
        vnum = p.get("Volcano_Number")
        if year is not None and 2000 <= int(year) <= 2025:
            recent[vnum] = recent.get(vnum, 0) + 1
    ranked = [v for v, _ in sorted(recent.items(), key=lambda kv: (-kv[1], kv[0]))][:50]
    ranked_set = set(ranked)
    selected = [f for f in volc.get("features", []) if f.get("properties", {}).get("Volcano_Number") in ranked_set]
    out = out_dir / "volcanoes_sample.geojson"
    atomic_write_text(out, json.dumps({
        "type": "FeatureCollection",
        "name": "volcanoes_sample",
        "features": selected,
        "source": cfg["volcano"]["wfs_url"],
        "selection": "50 volcanoes with the most eruptions during 2000-2025 (real records)",
        "created_at": utc_now_iso(),
    }, ensure_ascii=False))
    # Also export the corresponding eruption records for the sample volcanoes.
    sample_eruptions = [f for f in eruptions.get("features", [])
                        if f.get("properties", {}).get("Volcano_Number") in ranked_set]
    atomic_write_text(out_dir / "eruptions_sample.geojson", json.dumps({
        "type": "FeatureCollection",
        "name": "eruptions_sample",
        "features": sample_eruptions,
        "source": cfg["volcano"]["wfs_url"],
        "created_at": utc_now_iso(),
    }, ensure_ascii=False))
    manifest = {
        "dataset": "volcano_sample",
        "created_at": utc_now_iso(),
        "volcanoes": len(selected),
        "eruptions": len(sample_eruptions),
        "volcano_numbers": ranked,
        "source": cfg["volcano"]["wfs_url"],
        "note": "Real GVP records only; no dates or coordinates were invented.",
    }
    atomic_write_json(out_dir / "volcano_sample_manifest.json", manifest)
    log.info("Created volcano sample: %s volcanoes, %s eruptions", len(selected), len(sample_eruptions))
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Smithsonian GVP volcano/eruption data.")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--sample", action="store_true", help="also create the 50-volcano sample")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    ensure_project_dirs(cfg)
    v = download_layer(cfg, "volcanoes", cfg["volcano"]["type_volcanoes"], force=args.force)
    e = download_layer(cfg, "eruptions", cfg["volcano"]["type_eruptions"], force=args.force)
    if args.sample:
        create_volcano_sample(cfg)
    print(json.dumps({
        "volcanoes": v.get("combined_features"),
        "eruptions": e.get("combined_features"),
        "number_matched": {"volcanoes": v.get("number_matched"), "eruptions": e.get("number_matched")},
    }, indent=2))


if __name__ == "__main__":
    main()
