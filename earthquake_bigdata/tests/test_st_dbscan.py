"""Unit tests for ST-DBSCAN / DBSCAN on small synthetic datasets.

Synthetic data are used ONLY in tests and are clearly labelled.  Real-record
integration is tested separately by the clustering runner.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.st_dbscan import (
    STDBSCANParams,
    assign_cells,
    cluster_f1,
    dbscan_spatial_single,
    st_dbscan_single,
)
from common.geo import haversine_km


def _times(days):
    base = np.datetime64("2020-01-01T00:00:00")
    return base + np.asarray(days, dtype="timedelta64[D]")


def test_haversine_known_distance():
    d = float(haversine_km(0.0, 0.0, 0.0, 1.0))
    assert 110.5 < d < 111.5
    # Symmetry and zero distance
    assert haversine_km(10, 20, 10, 20) == pytest.approx(0.0)
    assert haversine_km(10, 20, -10, -160) == pytest.approx(float(haversine_km(-10, -160, 10, 20)))


def test_two_spatial_clusters():
    # Two groups ~222 km apart, each internally ~11 km apart.
    lat = [0.0, 0.0, 0.0, 2.0, 2.0, 2.0]
    lon = [0.0, 0.1, 0.2, 0.0, 0.1, 0.2]
    t = _times([0, 0, 0, 0, 0, 0])
    labels, core = st_dbscan_single(lat, lon, t, eps_km=50, eps_time_days=7, min_pts=3)
    assert len(set(labels)) == 2
    assert set(labels[:3]) != set(labels[3:])
    assert core.all()


def test_connected_chain_one_cluster():
    # Consecutive points are < 50 km apart, ends are far apart.
    lon = [0.0, 0.3, 0.6]
    lat = [0.0, 0.0, 0.0]
    labels, _ = st_dbscan_single(lat, lon, _times([0, 0, 0]), eps_km=50, eps_time_days=7, min_pts=2)
    assert len(set(labels)) == 1


def test_time_separated_same_location():
    labels, _ = st_dbscan_single(
        [0.0, 0.0], [0.0, 0.0], _times([0, 30]), eps_km=50, eps_time_days=7, min_pts=1
    )
    assert len(set(labels)) == 2


def test_space_separated_same_time():
    labels, _ = st_dbscan_single(
        [0.0, 2.0], [0.0, 0.0], _times([0, 0]), eps_km=50, eps_time_days=7, min_pts=1
    )
    assert len(set(labels)) == 2


def test_border_and_noise():
    # Four mutually close core points plus a border point ~45 km from one core
    # and >50 km from the others, and a distant noise point.
    lat = [0.0, 0.0, 0.0, 0.0, 0.44, 20.0]
    lon = [0.0, 0.1, 0.2, 0.3, 0.3, 20.0]
    labels, core = st_dbscan_single(lat, lon, _times([0] * 6), eps_km=50, eps_time_days=7, min_pts=3)
    assert len(set(labels[:5])) == 1
    assert labels[5] == -1
    assert not core[4]
    assert core[0] and core[1] and core[2] and core[3]


def test_spatial_dbscan_ignores_time():
    labels, _ = dbscan_spatial_single([0.0, 0.0], [0.0, 0.0], eps_km=50, min_pts=1)
    assert len(set(labels)) == 1


def test_cell_assignment_contains_primary_and_halo():
    cells = assign_cells(10.0, 20.0, cell_km=75.0, rings=1)
    assert len(cells) >= 1
    assert len(set(cells)) == len(cells)
    assert all("_" in c for c in cells)


def test_cluster_f1_perfect_and_noisy():
    ref = np.array([0, 0, 1, 1, -1])
    pred = np.array([10, 10, 20, 20, -1])
    score = cluster_f1(ref, pred)
    assert score["f1"] == pytest.approx(1.0)
    pred2 = np.array([0, 1, 2, 3, -1])
    score2 = cluster_f1(ref, pred2)
    assert 0.0 <= score2["f1"] < 1.0


def test_distributed_matches_single_on_synthetic_boundary_data(spark):
    """Two clusters deliberately straddling a cell boundary."""
    # Points near 0/0 and 0/0.9 degrees; cell width ~0.67 deg for 75 km.
    lat = np.array([0.0, 0.01, 0.02, 0.03, 0.0, 0.01, 0.02, 0.03, 40.0])
    lon = np.array([0.0, 0.05, 0.10, 0.15, 0.80, 0.85, 0.90, 0.95, 40.0])
    times = [f"2020-01-{d:02d}T00:00:00" for d in (1, 1, 1, 1, 2, 2, 2, 2, 10)]
    pdf = pd.DataFrame({
        "id": [f"e{i}" for i in range(len(lat))],
        "latitude": lat, "longitude": lon,
        "time_utc": pd.to_datetime(times, utc=True),
        "mag": np.full(len(lat), 4.0),
    })
    sdf = spark.createDataFrame(pdf)
    params = STDBSCANParams(eps_km=50, eps_time_days=7, min_pts=2, partition_cell_km=75)
    from analytics.st_dbscan import st_dbscan_distributed
    dist, meta = st_dbscan_distributed(spark, sdf, params)
    ref, _ = st_dbscan_single(pdf["latitude"], pdf["longitude"], pdf["time_utc"], 50, 7, 2)
    from sklearn.metrics import adjusted_rand_score
    assert adjusted_rand_score(ref, dist.sort_values("id")["cluster_id"].to_numpy()) == pytest.approx(1.0)
    assert meta["base_rows"] == len(pdf)
