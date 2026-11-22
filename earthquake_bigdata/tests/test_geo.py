from __future__ import annotations

import numpy as np
import pytest

from common.geo import cell_area_from_id, grid_id, haversine_km, spherical_cell_area_km2


def test_haversine_symmetry_and_zero():
    assert float(haversine_km(0, 0, 0, 0)) == pytest.approx(0.0)
    a = float(haversine_km(12.3, 45.6, -7.8, 90.1))
    b = float(haversine_km(-7.8, 90.1, 12.3, 45.6))
    assert a == pytest.approx(b)
    assert a > 0


def test_grid_area_positive_and_matches_two_cells():
    one = spherical_cell_area_km2(0, 5, 0, 5)
    two = spherical_cell_area_km2(0, 10, 0, 5)
    assert one > 0
    assert two > one
    assert cell_area_from_id("018_036", 5.0) == pytest.approx(one)


def test_grid_ids_are_stable():
    a = grid_id(0.1, 0.1, 5.0)[0]
    b = grid_id(4.9, 4.9, 5.0)[0]
    assert a == b
    assert a == "018_036"
