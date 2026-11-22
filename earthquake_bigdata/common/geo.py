"""Geospatial helpers.

Spatial distances always use the Haversine formula on the WGS84 sphere
(R = 6371.0088 km).  Latitude/longitude are never treated as Cartesian
coordinates for clustering.
"""
from __future__ import annotations

import math
from typing import Iterable, Tuple

import numpy as np

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres (vectorised, broadcastable)."""
    lat1 = np.radians(np.asarray(lat1, dtype=float))
    lon1 = np.radians(np.asarray(lon1, dtype=float))
    lat2 = np.radians(np.asarray(lat2, dtype=float))
    lon2 = np.radians(np.asarray(lon2, dtype=float))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def haversine_km_scalar(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return float(haversine_km(lat1, lon1, lat2, lon2))


def chord_radius_for_km(radius_km: float, earth_radius_km: float = EARTH_RADIUS_KM) -> float:
    """3-D unit-sphere chord length corresponding to a surface arc distance."""
    return 2.0 * math.sin(min(radius_km / earth_radius_km, math.pi) / 2.0)


def latlon_to_unit_xyz(lat, lon):
    """Convert lat/lon (degrees) to 3-D unit vectors.

    Used for k-d tree candidate search and for the K-Means comparison.  The
    final neighbour test is always Haversine, never Euclidean lat/lon.
    """
    lat = np.radians(np.asarray(lat, dtype=float))
    lon = np.radians(np.asarray(lon, dtype=float))
    x = np.cos(lat) * np.cos(lon)
    y = np.cos(lat) * np.sin(lon)
    z = np.sin(lat)
    return np.column_stack([x, y, z])


def grid_indices(lat, lon, size_deg: float) -> Tuple[np.ndarray, np.ndarray]:
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    lat_bin = np.floor((np.clip(lat, -90.0, 90.0) + 90.0) / size_deg).astype(np.int64)
    lon_bin = np.floor(((lon + 180.0) % 360.0) / size_deg).astype(np.int64)
    max_lat_bin = int(math.ceil(180.0 / size_deg)) - 1
    max_lon_bin = int(math.ceil(360.0 / size_deg)) - 1
    lat_bin = np.clip(lat_bin, 0, max_lat_bin)
    lon_bin = np.clip(lon_bin, 0, max_lon_bin)
    return lat_bin, lon_bin


def grid_id(lat, lon, size_deg: float) -> np.ndarray:
    """Stable string cell identifier for a regular lat/lon grid."""
    lat_bin, lon_bin = grid_indices(lat, lon, size_deg)
    lat_bin = np.atleast_1d(lat_bin)
    lon_bin = np.atleast_1d(lon_bin)
    return np.array([f"{a:03d}_{b:03d}" for a, b in zip(lat_bin, lon_bin)], dtype=object)


def grid_bounds(lat_bin: int, lon_bin: int, size_deg: float):
    """Return (lat_min, lat_max, lon_min, lon_max) for a grid cell."""
    lat_min = -90.0 + lat_bin * size_deg
    lat_max = min(90.0, lat_min + size_deg)
    lon_min = -180.0 + lon_bin * size_deg
    lon_max = min(180.0, lon_min + size_deg)
    return lat_min, lat_max, lon_min, lon_max


def spherical_cell_area_km2(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    earth_radius_km: float = EARTH_RADIUS_KM,
) -> float:
    """Area of a lat/lon rectangle on a sphere, in km^2."""
    if lat_max <= lat_min or lon_max <= lon_min:
        return 0.0
    phi1 = math.radians(lat_min)
    phi2 = math.radians(lat_max)
    dlambda = math.radians(lon_max - lon_min)
    return float(earth_radius_km ** 2 * abs(math.sin(phi2) - math.sin(phi1)) * dlambda)


def cell_area_from_id(cell: str, size_deg: float) -> float:
    lat_bin, lon_bin = (int(x) for x in cell.split("_"))
    lat_min, lat_max, lon_min, lon_max = grid_bounds(lat_bin, lon_bin, size_deg)
    return spherical_cell_area_km2(lat_min, lat_max, lon_min, lon_max)


def magnitude_family(mag_type: str, families: dict) -> str:
    """Map a USGS magType code to a broad scale family (lower-case)."""
    if mag_type is None:
        return "unknown"
    mt = str(mag_type).strip().lower()
    for family, codes in families.items():
        if mt in {str(c).lower() for c in codes}:
            return family
    return "other"


def normalize_longitude(lon):
    return ((np.asarray(lon, dtype=float) + 180.0) % 360.0) - 180.0
