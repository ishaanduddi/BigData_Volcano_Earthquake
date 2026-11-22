"""Space–time DBSCAN (ST-DBSCAN) and DBSCAN.

Two implementations are provided:

* ``st_dbscan_single`` — a single-machine reference using a 3-D unit-sphere
  k-d tree for spatial candidate search and exact Haversine tests.  It is the
  correctness oracle for the distributed implementation.
* ``st_dbscan_distributed`` — geographic grid partitioning in Spark with halo
  cells, local ST-DBSCAN in each Spark partition, and a global Union–Find merge
  of local clusters that share at least one event id.

The distributed version is genuinely partitioned: local clustering runs in
Spark ``groupBy(...).applyInPandas`` tasks.  The final cross-partition merge is
a driver-side Union–Find over local-cluster links; this is reported explicitly.

Neighbourhood definition
------------------------
A point q is in p's space–time neighbourhood iff
``haversine(p,q) <= eps_km`` **and** ``|t_p - t_q| <= eps_time_days``.
minPts includes the point itself.  Core points have at least minPts neighbours;
border points are density-reachable but not core; noise points are neither.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from scipy.spatial import cKDTree

from common.geo import EARTH_RADIUS_KM, chord_radius_for_km, haversine_km, latlon_to_unit_xyz


@dataclass
class STDBSCANParams:
    eps_km: float = 50.0
    eps_time_days: float = 7.0
    min_pts: int = 10
    partition_cell_km: float = 75.0
    neighbor_rings: Optional[int] = None
    random_seed: int = 20261119

    def rings(self) -> int:
        if self.neighbor_rings is not None:
            return max(1, int(self.neighbor_rings))
        return max(1, int(math.ceil(self.eps_km / max(self.partition_cell_km, 1.0))))


def _time_days(values) -> np.ndarray:
    if isinstance(values, (pd.Series, pd.Index)):
        series = pd.Series(values)
    else:
        series = pd.Series(np.asarray(values))
    if pd.api.types.is_numeric_dtype(series.dtype):
        return series.to_numpy(dtype=float)
    try:
        parsed = pd.to_datetime(series, utc=True)
        if isinstance(parsed.dtype, pd.DatetimeTZDtype):
            arr = parsed.dt.tz_localize(None).to_numpy(dtype="datetime64[ns]")
        else:
            arr = parsed.to_numpy(dtype="datetime64[ns]")
    except Exception as exc:
        raise TypeError(f"Cannot convert time values to days: {exc}") from exc
    return (arr - arr.min()) / np.timedelta64(1, "D")


def _normalise_input(lat, lon, time_values):
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    t = _time_days(time_values)
    mask = np.isfinite(lat) & np.isfinite(lon) & np.isfinite(t)
    return lat, lon, t, mask


def _neighbours(
    lat: np.ndarray,
    lon: np.ndarray,
    t: np.ndarray,
    eps_km: float,
    eps_time_days: float,
) -> List[np.ndarray]:
    """Exact Haversine + temporal neighbours; k-d tree only prefilters."""
    n = len(lat)
    if n == 0:
        return []
    xyz = latlon_to_unit_xyz(lat, lon)
    tree = cKDTree(xyz)
    chord = chord_radius_for_km(eps_km)
    # Small tolerance prevents a true 50.000 km pair from being dropped by
    # floating-point rounding in chord conversion.
    candidates = tree.query_ball_point(xyz, chord * (1.0 + 1e-9))
    out: List[np.ndarray] = []
    for i, cand in enumerate(candidates):
        if len(cand) == 0:
            out.append(np.empty(0, dtype=np.int64))
            continue
        cand = np.asarray(cand, dtype=np.int64)
        dt = np.abs(t[cand] - t[i])
        keep = dt <= (eps_time_days + 1e-12)
        cand = cand[keep]
        if cand.size:
            d = haversine_km(lat[i], lon[i], lat[cand], lon[cand])
            cand = cand[d <= (eps_km + 1e-9)]
        out.append(np.sort(cand))
    return out


def _dbscan_from_neighbours(neighbors: List[np.ndarray], min_pts: int) -> Tuple[np.ndarray, np.ndarray]:
    n = len(neighbors)
    labels = np.full(n, -1, dtype=np.int64)
    visited = np.zeros(n, dtype=bool)
    core = np.array([len(nb) >= min_pts for nb in neighbors], dtype=bool)
    cluster = 0
    for i in range(n):
        if visited[i]:
            continue
        visited[i] = True
        if not core[i]:
            continue
        labels[i] = cluster
        queue = deque(int(v) for v in neighbors[i])
        while queue:
            q = queue.popleft()
            if not visited[q]:
                visited[q] = True
                if core[q]:
                    queue.extend(int(v) for v in neighbors[q])
            if labels[q] == -1:
                labels[q] = cluster
        cluster += 1
    return labels, core


def st_dbscan_single(
    lat,
    lon,
    time_values,
    eps_km: float = 50.0,
    eps_time_days: float = 7.0,
    min_pts: int = 10,
) -> Tuple[np.ndarray, np.ndarray]:
    """Single-machine ST-DBSCAN reference. Returns (labels, is_core)."""
    lat, lon, t, mask = _normalise_input(lat, lon, time_values)
    if not mask.all():
        # Callers should clean input; fail loudly rather than silently shifting
        # labels relative to the original rows.
        raise ValueError("ST-DBSCAN input contains non-finite coordinates/times")
    neighbors = _neighbours(lat, lon, t, eps_km, eps_time_days)
    return _dbscan_from_neighbours(neighbors, int(min_pts))


def dbscan_spatial_single(
    lat,
    lon,
    eps_km: float = 50.0,
    min_pts: int = 10,
) -> Tuple[np.ndarray, np.ndarray]:
    """Spatial-only DBSCAN with Haversine distance (time ignored)."""
    lat, lon, _t, mask = _normalise_input(lat, lon, np.arange(len(np.asarray(lat))))
    if not mask.all():
        raise ValueError("DBSCAN input contains non-finite coordinates")
    n = len(lat)
    xyz = latlon_to_unit_xyz(lat, lon)
    tree = cKDTree(xyz)
    candidates = tree.query_ball_point(xyz, chord_radius_for_km(eps_km) * (1.0 + 1e-9))
    neighbors = []
    for i, cand in enumerate(candidates):
        cand = np.asarray(cand, dtype=np.int64)
        d = haversine_km(lat[i], lon[i], lat[cand], lon[cand])
        neighbors.append(np.sort(cand[d <= (eps_km + 1e-9)]))
    return _dbscan_from_neighbours(neighbors, int(min_pts))


# ---------------------------------------------------------------------------
# Distributed implementation
# ---------------------------------------------------------------------------

def assign_cells(lat: float, lon: float, cell_km: float, rings: int) -> List[str]:
    """Return the primary cell and all halo cells for one event.

    Latitude bands have a fixed height in km.  Longitude cell width is adjusted
    within each band so that its physical width is approximately cell_km; this
    keeps the halo guarantee valid at high latitudes.
    """
    if not (np.isfinite(lat) and np.isfinite(lon)):
        return []
    lat = float(min(89.999, max(-89.999, lat)))
    lon = ((float(lon) + 180.0) % 360.0) - 180.0
    cell_lat_deg = max(cell_km / 111.32, 1e-6)
    n_lat = max(1, int(math.ceil(180.0 / cell_lat_deg)))
    lat_bin = int(math.floor((lat + 90.0) / cell_lat_deg))
    lat_bin = min(n_lat - 1, max(0, lat_bin))
    cells: List[str] = []
    for dr in range(-rings, rings + 1):
        lb = lat_bin + dr
        if lb < 0 or lb >= n_lat:
            continue
        band_max = min(90.0, -90.0 + (lb + 1) * cell_lat_deg)
        cos_band = max(math.cos(math.radians(band_max)), 0.01)
        target_lon_deg = cell_lat_deg / cos_band
        n_lon = max(1, int(math.ceil(360.0 / target_lon_deg)))
        lon_step = 360.0 / n_lon
        lon_bin = int(math.floor((lon + 180.0) / lon_step))
        lon_bin = min(n_lon - 1, max(0, lon_bin))
        for dc in range(-rings, rings + 1):
            lc = (lon_bin + dc) % n_lon
            cells.append(f"{lb}_{lc}")
    return list(dict.fromkeys(cells))


def _local_group(pdf: pd.DataFrame, params: STDBSCANParams) -> pd.DataFrame:
    cell_id = str(pdf["cell_id"].iloc[0])
    labels, core = st_dbscan_single(
        pdf["latitude"].to_numpy(), pdf["longitude"].to_numpy(),
        pdf["time_utc"].to_numpy(), params.eps_km, params.eps_time_days, params.min_pts,
    )
    return pd.DataFrame({
        "cell_id": cell_id,
        "id": pdf["id"].astype(str).to_numpy(),
        "local_label": labels.astype(np.int64),
        "is_core": core.astype(bool),
    })


class UnionFind:
    def __init__(self):
        self.parent: Dict[str, str] = {}
        self.rank: Dict[str, int] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        self.add(x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != x:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def _merge_clusters(assignments: pd.DataFrame, base: pd.DataFrame) -> Tuple[pd.Series, pd.Series, int]:
    """Union local clusters sharing an event id; return global labels + cell count."""
    uf = UnionFind()
    event_cluster: Dict[str, str] = {}
    non_noise = assignments[assignments["local_label"] >= 0].copy()
    for row in non_noise.itertuples(index=False):
        key = f"{row.cell_id}|{int(row.local_label)}"
        uf.add(key)
        eid = str(row.id)
        if eid in event_cluster:
            uf.union(event_cluster[eid], key)
        else:
            event_cluster[eid] = key
    root_to_label: Dict[str, int] = {}
    # Stable, readable global ids by root string (arbitrary but deterministic).
    for key in sorted({uf.find(k) for k in uf.parent}):
        root_to_label[key] = len(root_to_label)
    labels = []
    cell_counts = []
    all_ids = base["id"].astype(str).to_numpy()
    n_cells = non_noise.groupby("id")["cell_id"].nunique().to_dict()
    for eid in all_ids:
        if eid in event_cluster:
            labels.append(root_to_label[uf.find(event_cluster[eid])])
        else:
            labels.append(-1)
        cell_counts.append(int(n_cells.get(eid, 0)))
    return pd.Series(labels, index=base.index, name="cluster_id"), pd.Series(cell_counts, index=base.index, name="n_cells"), len(root_to_label)


def st_dbscan_distributed(spark, df, params: STDBSCANParams) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Run Spark-partitioned ST-DBSCAN.

    `df` must contain id, latitude, longitude, time_utc.  The returned event
    table is a pandas frame (bounded by the caller's max_points) and the
    metrics record genuine partition/local-cluster counts.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    rings = params.rings()
    cells_udf = F.udf(lambda lat, lon: assign_cells(lat, lon, params.partition_cell_km, rings), ArrayType(StringType()))
    assigned = (
        df.withColumn("cell_ids", cells_udf(F.col("latitude"), F.col("longitude")))
        .withColumn("cell_id", F.explode("cell_ids"))
    )
    base = df.select(
        "id", "latitude", "longitude", "time_utc", "mag",
        F.date_format("time_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("time_utc_utc"),
    ).toPandas()
    base["id"] = base["id"].astype(str)
    assignments = assigned.groupBy("cell_id").applyInPandas(
        lambda pdf: _local_group(pdf, params),
        schema="cell_id string, id string, local_label long, is_core boolean",
    ).toPandas()
    local_clusters = assignments[assignments["local_label"] >= 0].groupby(["cell_id", "local_label"]).ngroups
    labels, cell_counts, merged_clusters = _merge_clusters(assignments, base)
    base = base.copy()
    base["cluster_id"] = labels
    base["n_cells"] = cell_counts
    core_map = assignments.groupby("id")["is_core"].any().to_dict()
    base["is_core"] = base["id"].map(core_map).fillna(False).astype(bool)
    meta = {
        "partition_cell_km": params.partition_cell_km,
        "neighbor_rings": rings,
        "rows_with_halo": int(len(assignments)),
        "base_rows": int(len(base)),
        "spark_partitions": int(assigned.rdd.getNumPartitions()),
        "local_clusters": int(local_clusters),
        "merged_global_clusters": int(merged_clusters),
        "events_in_clusters": int((base["cluster_id"] >= 0).sum()),
        "noise_events": int((base["cluster_id"] < 0).sum()),
    }
    return base, meta


# ---------------------------------------------------------------------------
# Evaluation helpers
# ---------------------------------------------------------------------------

def pairwise_reference_declustering(
    lat, lon, time_values, mags, seed: int = 20261119
) -> np.ndarray:
    """Transparent magnitude-window reference declustering (not ST-DBSCAN).

    Events are processed from largest to smallest magnitude.  An unassigned
    event becomes a seed and claims unassigned events inside the
    Gardner–Knopoff-style windows L(M)=10^(0.1238M+0.983) km and
    T(M)=10^(0.032M+2.738) days.  Seeds with fewer than two events are marked
    noise (-1).  This is a simplified reference, not the full published GK
    algorithm, and is documented as such.
    """
    lat = np.asarray(lat, float)
    lon = np.asarray(lon, float)
    t = _time_days(time_values)
    mags = np.asarray(mags, float)
    n = len(lat)
    xyz = latlon_to_unit_xyz(lat, lon)
    order = np.argsort(-np.nan_to_num(mags, nan=-99.0))
    labels = np.full(n, -1, dtype=np.int64)
    tree = cKDTree(xyz)
    cid = 0
    for i in order:
        if labels[i] != -1:
            continue
        m = mags[i] if np.isfinite(mags[i]) else 2.5
        l_km = 10 ** (0.1238 * m + 0.983)
        t_days = 10 ** (0.032 * m + 2.738)
        cand = np.asarray(tree.query_ball_point(xyz[i], chord_radius_for_km(l_km) * (1 + 1e-9)), dtype=int)
        cand = cand[(np.abs(t[cand] - t[i]) <= t_days) & (labels[cand] == -1)]
        if cand.size:
            d = haversine_km(lat[i], lon[i], lat[cand], lon[cand])
            cand = cand[d <= l_km]
        members = np.unique(np.concatenate(([i], cand)))
        if len(members) >= 2:
            labels[members] = cid
            cid += 1
        # otherwise remains noise
    return labels


def cluster_f1(reference: np.ndarray, predicted: np.ndarray) -> Dict[str, float]:
    """Cluster-level micro F1 after optimal many-to-one matching.

    Noise is treated as a separate label and is never matched across different
    noise sets.  This is a transparent operational score, not a universal
    clustering metric.
    """
    ref = np.asarray(reference)
    pred = np.asarray(predicted)
    ref_ids = [x for x in np.unique(ref) if x >= 0]
    pred_ids = [x for x in np.unique(pred) if x >= 0]
    if not ref_ids or not pred_ids:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0, "matched_events": 0}
    overlap = np.zeros((len(ref_ids), len(pred_ids)), dtype=float)
    for i, r in enumerate(ref_ids):
        rmask = ref == r
        for j, p in enumerate(pred_ids):
            overlap[i, j] = np.sum(rmask & (pred == p))
    from scipy.optimize import linear_sum_assignment
    ri, ci = linear_sum_assignment(-overlap)
    tp = float(overlap[ri, ci].sum())
    precision = tp / max(1.0, float(np.sum(pred >= 0)))
    recall = tp / max(1.0, float(np.sum(ref >= 0)))
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return {"precision": float(precision), "recall": float(recall), "f1": float(f1), "matched_events": int(tp)}


def compare_runs(reference: np.ndarray, distributed: np.ndarray) -> Dict[str, float]:
    from sklearn.metrics import adjusted_rand_score
    score = float(adjusted_rand_score(reference, distributed))
    return {
        "adjusted_rand_index": score,
        "identical_partition": bool(score == 1.0),
    }


# ---------------------------------------------------------------------------
# End-to-end analysis entry point
# ---------------------------------------------------------------------------

def run(dataset: str = "sample_10k", cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    import json
    import os
    import time as _time

    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import adjusted_rand_score

    from analytics._data import read_processed, results_dir
    from common.config import ensure_project_dirs, load_config
    from common.geo import latlon_to_unit_xyz
    from common.io import atomic_write_csv, atomic_write_json, utc_now_iso
    from common.logging_utils import get_logger
    from common.spark import create_spark_session, stop_spark
    from storage.parquet_manager import write_results_table

    log = get_logger("analytics.st_dbscan")
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    ccfg = cfg["clustering"]
    params = STDBSCANParams(
        eps_km=float(ccfg["spatial_epsilon_km"]),
        eps_time_days=float(ccfg["temporal_epsilon_days"]),
        min_pts=int(ccfg["min_pts"]),
        partition_cell_km=float(ccfg["partition_cell_km"]),
        neighbor_rings=ccfg.get("neighbor_rings"),
        random_seed=int(ccfg.get("random_seed", cfg["project"]["random_seed"])),
    )
    spark = create_spark_session(
        f"stdbscan-{dataset}", cfg,
        extra_conf={"spark.driver.memory": str(ccfg.get("driver_memory", "4g"))},
    )
    base = None
    try:
        df, backend = read_processed(spark, cfg, f"earthquakes/{dataset}")
        df = df.select("id", "latitude", "longitude", "time_utc", "mag", "depth")
        df = df.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & F.col("time_utc").isNotNull())
        total = df.count()
        max_points = int(ccfg.get("max_points", 200000))
        if total > max_points:
            frac = max_points / float(total)
            df_sample = df.sample(False, frac, seed=int(ccfg.get("random_seed", 1))).limit(max_points)
            sampled_note = f"deterministic random sample fraction={frac:.6f}, capped at {max_points}"
        else:
            df_sample = df
            sampled_note = "all processed records"
        df_sample = df_sample.cache()
        sample_n = df_sample.count()
        log.info("ST-DBSCAN input: %s rows (%s)", sample_n, sampled_note)

        t0 = _time.perf_counter()
        base, partition_meta = st_dbscan_distributed(spark, df_sample, params)
        distributed_runtime = _time.perf_counter() - t0
        base["id"] = base["id"].astype(str)

        # The equivalence check must use the same full event set as the
        # distributed run; otherwise clusters connected through excluded points
        # would be unfairly split in the reference.
        t0 = _time.perf_counter()
        ref_labels, ref_core = st_dbscan_single(
            base["latitude"].to_numpy(), base["longitude"].to_numpy(),
            base["time_utc"].to_numpy(), params.eps_km, params.eps_time_days, params.min_pts,
        )
        reference_runtime = _time.perf_counter() - t0
        equivalence = compare_runs(ref_labels, base["cluster_id"].to_numpy())
        core_mask = ref_core & base["is_core"].to_numpy()
        if core_mask.sum() > 1:
            equivalence["core_point_adjusted_rand_index"] = float(
                adjusted_rand_score(ref_labels[core_mask], base["cluster_id"].to_numpy()[core_mask])
            )
            equivalence["core_points_compared"] = int(core_mask.sum())
        equivalence["note"] = (
            "ARI on all evaluated events; core-point ARI isolates border-assignment ambiguity. "
            "Border points reachable from two clusters may legitimately receive different labels."
        )
        if not equivalence["identical_partition"]:
            log.warning("Distributed/reference partitions differ: ARI=%.6f", equivalence["adjusted_rand_index"])

        # F1, DBSCAN and K-Means use a deterministic subset to keep the full
        # run within memory while the equivalence check above uses all events.
        eval_n = min(len(base), int(ccfg.get("evaluation_sample_rows", 30000)))
        eval_base = base.iloc[:eval_n].reset_index(drop=True)
        if not equivalence["identical_partition"]:
            log.warning("Distributed/reference partitions differ: ARI=%.6f", equivalence["adjusted_rand_index"])

        # Spatial-only DBSCAN comparison.
        t0 = _time.perf_counter()
        db_labels, db_core = dbscan_spatial_single(
            eval_base["latitude"].to_numpy(), eval_base["longitude"].to_numpy(),
            float(ccfg.get("dbscan_spatial_epsilon_km", 50.0)),
            int(ccfg.get("dbscan_min_pts", 10)),
        )
        dbscan_runtime = _time.perf_counter() - t0

        # K-Means is illustrative: it uses 3-D unit vectors and ignores time.
        k = int(min(int(ccfg.get("kmeans_clusters", 25)), max(1, len(eval_base))))
        xyz = latlon_to_unit_xyz(eval_base["latitude"].to_numpy(), eval_base["longitude"].to_numpy())
        t0 = _time.perf_counter()
        km = KMeans(n_clusters=k, random_state=int(ccfg.get("random_seed", 1)), n_init=10)
        km_labels = km.fit_predict(xyz)
        kmeans_runtime = _time.perf_counter() - t0

        # Transparent magnitude-window reference declustering for F1.
        mags = pd.to_numeric(eval_base["mag"], errors="coerce").fillna(2.5).to_numpy()
        ref_declustered = pairwise_reference_declustering(
            eval_base["latitude"].to_numpy(), eval_base["longitude"].to_numpy(),
            eval_base["time_utc"].to_numpy(), mags, seed=params.random_seed,
        )
        f1_st = cluster_f1(ref_declustered, eval_base["cluster_id"].to_numpy())
        f1_db = cluster_f1(ref_declustered, db_labels)

        out = results_dir(cfg, "clustering")
        labels_out = base.copy()
        labels_out["time_utc"] = labels_out["time_utc_utc"]
        labels_out = labels_out.drop(columns=["time_utc_utc"])
        labels_out["is_core_reference"] = False
        labels_out["dbscan_cluster_id"] = -1
        labels_out["dbscan_is_core"] = False
        labels_out["kmeans_cluster_id"] = -1
        labels_out["reference_declustering_id"] = -1
        idx = labels_out.index[:eval_n]
        labels_out.loc[idx, "is_core_reference"] = ref_core[:eval_n]
        labels_out.loc[idx, "dbscan_cluster_id"] = db_labels
        labels_out.loc[idx, "dbscan_is_core"] = db_core
        labels_out.loc[idx, "kmeans_cluster_id"] = km_labels
        labels_out.loc[idx, "reference_declustering_id"] = ref_declustered
        labels_out = labels_out.sort_values("cluster_id")
        atomic_write_csv(out / "st_dbscan_labels.csv", labels_out)

        clustered = labels_out[labels_out["cluster_id"] >= 0]
        if len(clustered):
            stats = clustered.groupby("cluster_id").agg(
                size=("id", "count"),
                start_time=("time_utc", "min"),
                end_time=("time_utc", "max"),
                mean_latitude=("latitude", "mean"),
                mean_longitude=("longitude", "mean"),
                mean_magnitude=("mag", "mean"),
                max_magnitude=("mag", "max"),
                mean_cells=("n_cells", "mean"),
            ).reset_index().sort_values("size", ascending=False)
        else:
            stats = pd.DataFrame(columns=["cluster_id", "size"])
        atomic_write_csv(out / "cluster_stats.csv", stats)

        # Parameter sensitivity (single-machine reference on a small bounded sample).
        sens_n = min(len(base), 5000)
        eval_base = base.iloc[:sens_n].copy()
        sens_rows = []
        for eps in ccfg["sensitivity"]["spatial_epsilon_km"]:
            for et in ccfg["sensitivity"]["temporal_epsilon_days"]:
                for mp in ccfg["sensitivity"]["min_pts"]:
                    t0 = _time.perf_counter()
                    lab, _ = st_dbscan_single(
                        eval_base["latitude"].to_numpy(), eval_base["longitude"].to_numpy(),
                        eval_base["time_utc"].to_numpy(), float(eps), float(et), int(mp),
                    )
                    sens_rows.append({
                        "eps_km": float(eps), "eps_time_days": float(et), "min_pts": int(mp),
                        "n_events": len(lab), "clusters": int(len(set(lab[lab >= 0]))),
                        "noise": int((lab < 0).sum()), "noise_fraction": float((lab < 0).mean()),
                        "runtime_seconds": float(_time.perf_counter() - t0),
                        "engine": "single_machine_reference",
                    })
        sensitivity = pd.DataFrame(sens_rows).sort_values(["eps_km", "eps_time_days", "min_pts"])
        atomic_write_csv(out / "parameter_sensitivity.csv", sensitivity)

        comparison = pd.DataFrame([
            {"algorithm": "ST-DBSCAN (distributed Spark)", "clusters": int(partition_meta["merged_global_clusters"]),
             "noise_points": int(partition_meta["noise_events"]), "runtime_seconds": float(distributed_runtime),
             "uses_time": True, "uses_haversine": True, "f1_vs_reference": float(f1_st["f1"]),
             "adjusted_rand_vs_reference_impl": float(equivalence["adjusted_rand_index"])},
            {"algorithm": "ST-DBSCAN (single-machine reference)", "clusters": int(len(set(ref_labels[ref_labels >= 0]))),
             "noise_points": int((ref_labels < 0).sum()), "runtime_seconds": float(reference_runtime),
             "uses_time": True, "uses_haversine": True, "f1_vs_reference": float(f1_st["f1"]),
             "adjusted_rand_vs_reference_impl": 1.0},
            {"algorithm": "DBSCAN (spatial only, Haversine)", "clusters": int(len(set(db_labels[db_labels >= 0]))),
             "noise_points": int((db_labels < 0).sum()), "runtime_seconds": float(dbscan_runtime),
             "uses_time": False, "uses_haversine": True, "f1_vs_reference": float(f1_db["f1"]),
             "adjusted_rand_vs_reference_impl": float(adjusted_rand_score(ref_labels[:eval_n], db_labels))},
            {"algorithm": "K-Means (3-D unit vectors, illustrative)", "clusters": int(k),
             "noise_points": 0, "runtime_seconds": float(kmeans_runtime),
             "uses_time": False, "uses_haversine": False, "f1_vs_reference": None,
             "adjusted_rand_vs_reference_impl": float(adjusted_rand_score(ref_labels[:eval_n], km_labels))},
        ])
        atomic_write_csv(out / "algorithm_comparison.csv", comparison)

        metrics = {
            "dataset": dataset,
            "backend": backend,
            "generated_at": utc_now_iso(),
            "sample_rows": int(len(base)),
            "sample_note": sampled_note,
            "evaluation_rows": int(eval_n),
            "params": {
                "eps_km": params.eps_km, "eps_time_days": params.eps_time_days,
                "min_pts": params.min_pts, "partition_cell_km": params.partition_cell_km,
                "neighbor_rings": params.rings(),
            },
            "partition_diagnostics": partition_meta,
            "distributed_runtime_seconds": float(distributed_runtime),
            "reference_runtime_seconds": float(reference_runtime),
            "dbscan_runtime_seconds": float(dbscan_runtime),
            "kmeans_runtime_seconds": float(kmeans_runtime),
            "distributed_vs_reference": equivalence,
            "f1_st_dbscan_vs_window_reference": f1_st,
            "f1_dbscan_vs_window_reference": f1_db,
            "reference_declustering_note": (
                "Simplified magnitude-window reference (Gardner–Knopoff-style L(M), T(M)); "
                "it is not the full published GK algorithm and its labels are not ground truth."
            ),
            "algorithm_comparison_file": "algorithm_comparison.csv",
            "parameter_sensitivity_file": "parameter_sensitivity.csv",
            "environment": {
                "spark_master": spark.sparkContext.master,
                "spark_version": spark.version,
                "note": "Spark local mode on one machine, not a multi-node cluster.",
            },
            "limitations": [
                "Cluster labels are arbitrary; ARI compares partitions, not semantic identities.",
                "The distributed merge is driver-side Union–Find over local cluster links.",
                "K-Means ignores time and uses 3-D unit vectors; it is not a space–time method.",
                "Parameter sensitivity is computed with the single-machine reference on a bounded sample.",
            ],
        }
        atomic_write_json(out / "metrics.json", metrics)
        try:
            labels_spark = spark.createDataFrame(labels_out[["id", "cluster_id", "is_core", "n_cells"]])
            write_results_table(labels_spark, "clustering/st_dbscan_labels", cfg)
        except Exception as exc:
            log.warning("Could not publish cluster labels to HDFS: %s", exc)
        log.info("ST-DBSCAN complete: %s events, %s clusters, ARI=%.6f, noise=%s",
                 len(base), partition_meta["merged_global_clusters"],
                 equivalence["adjusted_rand_index"], partition_meta["noise_events"])
        return metrics
    finally:
        stop_spark(spark)


def main() -> None:
    import argparse
    import json
    from common.config import load_config
    from common.logging_utils import setup_logging

    parser = argparse.ArgumentParser(description="Distributed ST-DBSCAN + DBSCAN/K-Means comparison")
    parser.add_argument("--dataset", default="sample_10k")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    print(json.dumps(run(args.dataset, cfg), indent=2, default=str))


if __name__ == "__main__":
    main()
