from __future__ import annotations

import pandas as pd

from ingestion.download_earthquakes import EXPECTED_COLUMNS
from preprocessing.earthquake_etl import build_clean_dataframe


def _row(eid, time, lat, lon, depth, mag, typ, updated, magtype="ml"):
    return {
        "time": time, "latitude": lat, "longitude": lon, "depth": depth, "mag": mag,
        "magType": magtype, "nst": "10.0", "gap": "90", "dmin": "0.1", "rms": "0.3",
        "net": "ci", "id": eid, "updated": updated, "place": "test",
        "type": typ, "horizontalError": "1.0", "depthError": "1.0", "magError": "0.1",
        "magNst": "4.0", "status": "reviewed", "locationSource": "ci", "magSource": "ci",
    }


def test_etl_filters_and_deduplicates(tmp_path, spark):
    rows = [
        _row("e1", "2020-01-01T00:00:00.000Z", 0, 0, 10, 2.8, "earthquake", "2020-01-02T00:00:00.000Z"),
        _row("e1", "2020-01-01T00:00:00.000Z", 0, 0, 10, 3.0, "earthquake", "2020-01-03T00:00:00.000Z"),
        _row("e2", "2020-01-02T00:00:00.000Z", 1, 1, 10, 3.1, "quarry blast", "2020-01-03T00:00:00.000Z"),
        _row("e3", "2020-01-03T00:00:00.000Z", 95, 1, 10, 3.1, "earthquake", "2020-01-03T00:00:00.000Z"),
        _row("e4", "2020-01-04T00:00:00.000Z", 2, 2, 10, None, "earthquake", "2020-01-04T00:00:00.000Z"),
        _row("e5", "2020-01-05T00:00:00.000Z", 3, 3, 10, 3.2, "explosion", "2020-01-05T00:00:00.000Z"),
    ]
    path = tmp_path / "raw.csv"
    pd.DataFrame(rows)[EXPECTED_COLUMNS].to_csv(path, index=False)
    cfg = {"preprocessing": {"earthquake": {"allowed_event_types": ["earthquake"], "grid_size_degrees": 5.0, "fine_grid_degrees": 1.0}},
           "hotspots": {"magnitude_families": {"local": ["ml"]}}}
    clean, stats = build_clean_dataframe(spark, cfg, "test", [str(path)], 2.5)
    assert stats["raw_rows"] == 6
    assert stats["filtered_non_earthquake"] == 2
    assert stats["filtered_invalid_coordinates"] == 1
    assert stats["filtered_missing_magnitude"] == 1
    assert stats["duplicate_ids_removed"] == 1
    assert stats["final_rows"] == 1
    assert stats["final_rows"] == clean.count()
    row = clean.first()
    assert row["id"] == "e1"
    assert abs(row["mag"] - 3.0) < 1e-9
    assert row["year"] == 2020
    assert row["grid_id_5"] == "018_036"
    assert row["nst"] == 10
