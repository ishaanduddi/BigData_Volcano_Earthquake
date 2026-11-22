"""Volcano–earthquake association analysis (research contribution N3).

For every historical eruption inside the earthquake observation period, the
analysis uses a volcano-specific five-year baseline, a 90-day pre-eruption
window and a 180-day post-eruption window.  Earthquakes are matched within
50/100/200 km using an exact Haversine test after a Spark grid join.

Statistics reported:
* baseline, pre- and post-eruption rates per day,
* rate ratios with Poisson log-normal confidence intervals,
* two-sided conditional binomial (Poisson rate-ratio) p-values,
* permutation p-values from shifting the eruption window within the baseline,
* Benjamini–Hochberg FDR q-values,
* radius-sensitivity tables.

This is observational association analysis. It does not predict eruptions and
does not establish causation.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, StringType

from analytics._data import read_processed, results_dir
from analytics.st_dbscan import assign_cells
from common.config import ensure_project_dirs, load_config
from common.geo import haversine_km
from common.io import atomic_write_csv, atomic_write_json, utc_now_iso
from common.logging_utils import get_logger, setup_logging
from common.spark import create_spark_session, stop_spark
from storage.parquet_manager import write_results_table

log = get_logger("analytics.volcano")

EPOCH = date(1970, 1, 1)


def _day_number(d) -> float:
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, pd.Timestamp):
        d = d.date()
    return float((d - EPOCH).days)


def _timestamp_day(t) -> float:
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return float(ts.value / (86400.0 * 1_000_000_000))


def poisson_rate_ratio(a: float, t1: float, b: float, t2: float, alpha: float = 0.05) -> Dict[str, Any]:
    """Rate ratio (a/t1)/(b/t2) with log-normal CI and conditional binomial p.

    A 0.5 continuity correction is used when either count is zero; this is
    reported by the `continuity_correction` flag.
    """
    if t1 <= 0 or t2 <= 0:
        return {"rate_ratio": None, "ci_low": None, "ci_high": None, "p_value": None, "continuity_correction": False}
    continuity = a == 0 or b == 0
    aa = a + 0.5 if continuity else a
    bb = b + 0.5 if continuity else b
    rate1, rate2 = aa / t1, bb / t2
    rr = rate1 / rate2 if rate2 > 0 else None
    if rr is None or rr <= 0:
        return {"rate_ratio": None, "ci_low": None, "ci_high": None, "p_value": None, "continuity_correction": continuity}
    se = math.sqrt(1.0 / aa + 1.0 / bb)
    from scipy.stats import norm
    z = norm.ppf(1 - alpha / 2)
    lo, hi = math.exp(math.log(rr) - z * se), math.exp(math.log(rr) + z * se)
    try:
        from scipy.stats import binomtest
        k = int(round(aa))
        n = int(round(aa + bb))
        p = float(binomtest(k, n, p=t1 / (t1 + t2), alternative="two-sided").pvalue)
    except Exception:
        p = None
    return {
        "rate_ratio": float(rr), "ci_low": float(lo), "ci_high": float(hi),
        "p_value": p, "continuity_correction": bool(continuity),
        "rate_window": float(rate1), "rate_baseline": float(rate2),
    }


def permutation_test(
    event_days: np.ndarray,
    observed_count: float,
    baseline_start_day: float,
    baseline_end_day: float,
    window_days: float,
    excluded_intervals: List[Tuple[float, float]],
    iterations: int,
    seed: int,
) -> Optional[float]:
    """One-sided permutation p-value for a 90-day window rate increase.

    Null windows are placed at random inside the observed baseline, skipping
    any eruption exclusion intervals.  The p-value is the fraction of null
    counts at least as large as the observed count (plus-one correction).
    """
    if iterations <= 0 or baseline_end_day <= baseline_start_day + window_days:
        return None
    rng = np.random.default_rng(seed)
    possible_start = baseline_end_day - window_days
    if possible_start <= baseline_start_day:
        return None
    counts = 0
    valid = 0
    attempts = 0
    while valid < iterations and attempts < iterations * 10:
        attempts += 1
        s = rng.uniform(baseline_start_day, possible_start)
        e = s + window_days
        if any(not (e <= a or s >= b) for a, b in excluded_intervals):
            continue
        valid += 1
        c = int(np.sum((event_days >= s) & (event_days < e)))
        if c >= observed_count:
            counts += 1
    if valid == 0:
        return None
    return float((counts + 1) / (valid + 1))


def bh_fdr(pvals: List[Optional[float]], alpha: float = 0.05) -> List[Optional[float]]:
    from statsmodels.stats.multitest import multipletests
    arr = np.array([np.nan if p is None else p for p in pvals], dtype=float)
    mask = np.isfinite(arr)
    q = np.full(len(arr), np.nan)
    if mask.sum() == 0:
        return [None] * len(arr)
    _, qv, _, _ = multipletests(arr[mask], alpha=alpha, method="fdr_bh")
    q[mask] = qv
    return [None if not np.isfinite(x) else float(x) for x in q]


def _overlap_days(a1: float, a2: float, b1: float, b2: float) -> float:
    return max(0.0, min(a2, b2) - max(a1, b1))


def _available_days(start: float, end: float, excluded: List[Tuple[float, float]], obs_start: float, obs_end: float) -> float:
    lo, hi = max(start, obs_start), min(end, obs_end)
    if hi <= lo:
        return 0.0
    total = hi - lo
    removed = sum(_overlap_days(lo, hi, a, b) for a, b in excluded)
    return float(max(1.0, total - removed))


def run(dataset: str = "sample_10k", cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = cfg or load_config()
    ensure_project_dirs(cfg)
    vcfg = cfg["volcano_association"]
    alpha = float(vcfg["fdr_alpha"])
    spark = create_spark_session(f"volcano-association-{dataset}", cfg)
    try:
        eq, eq_backend = read_processed(spark, cfg, f"earthquakes/{dataset}")
        eq = eq.select("id", "time_utc", "latitude", "longitude", "mag", "depth", "grid_id_5")
        eq = eq.filter(F.col("time_utc").isNotNull() & F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        volcanoes, _ = read_processed(spark, cfg, "volcanoes/volcanoes")
        eruptions, _ = read_processed(spark, cfg, "volcanoes/eruptions")
        volcanoes = volcanoes.select("volcano_number", "volcano_name", "latitude", "longitude",
                                     "country", "region", "elevation_m", "last_eruption_year")
        erupt = eruptions.select(
            "volcano_number", "eruption_number", "activity_type", "is_confirmed_eruption",
            "start_year", "start_date", "end_date", "date_precision", "date_is_approximate",
            "start_year_uncertainty", "vei_max"
        )
        erupt = erupt.filter(F.col("start_date").isNotNull())
        erupt = erupt.filter(F.col("start_year").between(2000, 2025))
        if bool(vcfg.get("confirmed_only", True)):
            erupt = erupt.filter(F.col("is_confirmed_eruption"))
        allowed_precision = ["day", "month"]
        min_precision = str(vcfg.get("min_date_precision", "month"))
        if min_precision == "month":
            allowed_precision = ["day", "month"]
        elif min_precision == "day":
            allowed_precision = ["day"]
        erupt = erupt.filter(F.col("date_precision").isin(allowed_precision))
        erupt = erupt.join(volcanoes.select("volcano_number", F.col("latitude").alias("volcano_latitude"),
                                          F.col("longitude").alias("volcano_longitude")), "volcano_number", "inner")
        erupt = erupt.withColumn("eruption_uid", F.concat_ws("_", F.col("volcano_number"), F.col("eruption_number")))
        erupt = erupt.cache()
        n_eruptions = erupt.count()
        n_volcanoes_with_eruptions = erupt.select("volcano_number").distinct().count()
        if n_eruptions == 0:
            raise RuntimeError("No eruptions pass the date/quality filters")

        # Intervals for baseline exclusion: every pre/post window of the same
        # volcano (including other eruptions).  Small enough to broadcast.
        wrows = erupt.select("volcano_number", "start_date", "end_date").toPandas()
        windows_map: Dict[int, List[Tuple[float, float]]] = {}
        for row in wrows.itertuples(index=False):
            s = _day_number(pd.Timestamp(row.start_date))
            e = _day_number(pd.Timestamp(row.end_date)) if row.end_date is not None and not pd.isna(row.end_date) else s
            e = max(s, e)
            windows_map.setdefault(int(row.volcano_number), []).append((s - float(vcfg["pre_days"]), e + float(vcfg["post_days"])))
        obs_start = _day_number(pd.Timestamp(vcfg["analysis_start"]))
        obs_end = _day_number(pd.Timestamp(vcfg["analysis_end"])) + 1.0

        all_results = []
        summary_by_radius = []
        GRID = 5.0
        N_LAT = int(180 / GRID)
        N_LON = int(360 / GRID)
        for radius in [float(r) for r in vcfg["radii_km"]]:
            # Spark-SQL cell join (no Python UDF).  Earthquakes already carry a
            # 5-degree grid id from ETL.  Eruptions are expanded to all 5-degree
            # cells whose bounds can be within the radius; exact Haversine then
            # filters the candidates.
            eqr = eq.select("id", "time_utc", "latitude", "longitude", "mag", "grid_id_5")
            er_base = (
                erupt.withColumn("lat_bin", F.floor((F.col("volcano_latitude") + 90.0) / GRID).cast("int"))
                .withColumn("lon_bin", F.floor(((F.col("volcano_longitude") + 180.0) % 360.0) / GRID).cast("int"))
                .withColumn("rings_lat", F.lit(max(1, int(math.ceil(radius / (111.32 * GRID))))))
                .withColumn(
                    "rings_lon",
                    F.least(F.lit(12), F.ceil(F.lit(radius) / (
                        111.32 * F.cos(F.radians(F.abs(F.col("volcano_latitude")))) * GRID
                    )).cast("int") + 1),
                )
                .withColumn("dr", F.explode(F.sequence(F.lit(-1) * F.col("rings_lat"), F.col("rings_lat"))))
                .withColumn("dc", F.explode(F.sequence(F.lit(-1) * F.col("rings_lon"), F.col("rings_lon"))))
                .withColumn("lat2", F.least(F.lit(N_LAT - 1), F.greatest(F.lit(0), F.col("lat_bin") + F.col("dr"))))
                .withColumn("lon2", (((F.col("lon_bin") + F.col("dc")) % N_LON) + N_LON) % N_LON)
                .withColumn("cell_id", F.format_string("%03d_%03d", F.col("lat2"), F.col("lon2")))
            )
            dlat = F.radians(F.col("volcano_latitude") - F.col("latitude"))
            dlon = F.radians(F.col("volcano_longitude") - F.col("longitude"))
            hav_a = (
                F.sin(dlat / 2.0) ** 2
                + F.cos(F.radians(F.col("latitude"))) * F.cos(F.radians(F.col("volcano_latitude")))
                * F.sin(dlon / 2.0) ** 2
            )
            distance_expr = 2.0 * 6371.0088 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), hav_a))))
            pairs = (
                er_base.join(eqr, er_base["cell_id"] == eqr["grid_id_5"], "inner")
                .withColumn("distance_km", distance_expr)
                .filter(F.col("distance_km") <= radius)
                .withColumn("event_day", F.unix_timestamp("time_utc") / 86400.0)
                .select("volcano_number", "eruption_uid", "start_date", "date_precision", "id", "time_utc",
                        "latitude", "longitude", "mag", "distance_km", "event_day")
            ).cache()
            n_pairs = pairs.count()

            # Per-eruption event-day lists for exact baseline/pre/post counting
            # and permutation tests.  event_day is UTC days since the Unix epoch.
            event_lists = pairs.groupBy("eruption_uid").agg(
                F.collect_list("event_day").alias("event_days")
            ).toPandas()
            event_map = {str(r.eruption_uid): r.event_days for r in event_lists.itertuples(index=False)}

            base_days = float(vcfg["baseline_years"]) * 365.25
            pre_days = float(vcfg["pre_days"])
            post_days = float(vcfg["post_days"])
            rows = []
            for er in erupt.select("volcano_number", "eruption_uid", "start_date", "end_date", "date_precision", "activity_type", "vei_max").toPandas().itertuples(index=False):
                uid = str(er.eruption_uid)
                vnum = int(er.volcano_number)
                start_day = _day_number(pd.Timestamp(er.start_date))
                b_start = start_day - base_days
                b_end = start_day - pre_days
                # Baseline excludes all eruption windows of the same volcano.
                excluded = windows_map.get(vnum, [])
                own_start = start_day - pre_days
                end_day = _day_number(pd.Timestamp(er.end_date)) if er.end_date is not None and not pd.isna(er.end_date) else start_day
                own_end = max(start_day, end_day) + post_days
                own_interval = (own_start, own_end)
                other_windows = [w for w in excluded if w != own_interval]
                base_available = _available_days(b_start, b_end, excluded, obs_start, obs_end)
                pre_available = _available_days(start_day - pre_days, start_day, other_windows, obs_start, obs_end)
                post_end = start_day + post_days
                post_available = _available_days(start_day, post_end, other_windows, obs_start, obs_end)
                times = event_map.get(uid, [])
                eq_days = np.asarray(times, dtype=float) if times is not None and len(times) > 0 else np.array([])
                n_pre = int(np.sum((eq_days >= start_day - pre_days) & (eq_days < start_day)))
                n_post = int(np.sum((eq_days >= start_day) & (eq_days < post_end)))
                # Baseline: in [b_start,b_end) and not inside any eruption window.
                if eq_days.size:
                    in_base = (eq_days >= max(b_start, obs_start)) & (eq_days < min(b_end, obs_end))
                    keep = []
                    for d in eq_days[in_base]:
                        if not any(a <= d < b for a, b in excluded):
                            keep.append(d)
                    n_base = int(len(keep))
                else:
                    n_base = 0
                base_rate = n_base / base_available if base_available > 0 else None
                pre_rate = n_pre / pre_available if pre_available > 0 else None
                post_rate = n_post / post_available if post_available > 0 else None
                pre_test = poisson_rate_ratio(n_pre, pre_available, n_base, base_available, alpha)
                post_test = poisson_rate_ratio(n_post, post_available, n_base, base_available, alpha)
                perm_p = None
                if base_available >= float(vcfg["min_baseline_days"]):
                    perm_p = permutation_test(
                        eq_days, n_pre, max(b_start, obs_start), min(b_end, obs_end),
                        pre_days, other_windows, int(vcfg["permutations"]), int(cfg["project"]["random_seed"] + vnum)
                    )
                rows.append({
                    "radius_km": radius,
                    "volcano_number": vnum,
                    "eruption_uid": uid,
                    "start_date": str(pd.Timestamp(er.start_date).date()),
                    "date_precision": er.date_precision,
                    "activity_type": er.activity_type,
                    "vei_max": er.vei_max,
                    "baseline_days_available": float(base_available),
                    "pre_days_available": float(pre_available),
                    "post_days_available": float(post_available),
                    "baseline_events": int(n_base),
                    "pre_events": int(n_pre),
                    "post_events": int(n_post),
                    "baseline_rate_per_day": base_rate,
                    "pre_rate_per_day": pre_rate,
                    "post_rate_per_day": post_rate,
                    "pre_rate_ratio": pre_test.get("rate_ratio"),
                    "pre_ci_low": pre_test.get("ci_low"),
                    "pre_ci_high": pre_test.get("ci_high"),
                    "pre_p_value": pre_test.get("p_value"),
                    "post_rate_ratio": post_test.get("rate_ratio"),
                    "post_ci_low": post_test.get("ci_low"),
                    "post_ci_high": post_test.get("ci_high"),
                    "post_p_value": post_test.get("p_value"),
                    "continuity_correction": bool(pre_test.get("continuity_correction")),
                    "permutation_p_value": perm_p,
                    "nearby_earthquakes": int(len(eq_days)),
                    "sufficient_baseline": bool(base_available >= float(vcfg["min_baseline_days"])
                                                 and n_base >= int(vcfg["min_baseline_events"])),
                })
            dfr = pd.DataFrame(rows)
            dfr["pre_q_value_bh"] = bh_fdr(dfr["pre_p_value"].tolist(), alpha)
            dfr["post_q_value_bh"] = bh_fdr(dfr["post_p_value"].tolist(), alpha)
            dfr = dfr.sort_values(["pre_q_value_bh", "pre_rate_ratio"], na_position="last")
            atomic_write_csv(results_dir(cfg, "volcano") / f"eruption_results_{int(radius)}km.csv", dfr)
            all_results.append(dfr)
            relevant = dfr[dfr["sufficient_baseline"]]
            summary_by_radius.append({
                "radius_km": radius,
                "n_eruptions": int(len(dfr)),
                "n_sufficient_baseline": int(relevant.shape[0]),
                "n_pairs": int(n_pairs),
                "median_pre_rate_ratio": float(relevant["pre_rate_ratio"].median()) if len(relevant) else None,
                "median_post_rate_ratio": float(relevant["post_rate_ratio"].median()) if len(relevant) else None,
                "significant_pre_fdr_0.05": int((relevant["pre_q_value_bh"] < alpha).sum()) if len(relevant) else 0,
                "significant_post_fdr_0.05": int((relevant["post_q_value_bh"] < alpha).sum()) if len(relevant) else 0,
            })
            if abs(radius - float(vcfg["primary_radius_km"])) < 1e-9:
                try:
                    map_pairs = pairs.select(
                        "volcano_number", "eruption_uid", "id",
                        F.date_format("time_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("time_utc"),
                        "latitude", "longitude", "mag", "distance_km"
                    ).toPandas()
                    if len(map_pairs) > 300000:
                        map_pairs = map_pairs.sample(n=300000, random_state=int(cfg["project"]["random_seed"]))
                    atomic_write_csv(results_dir(cfg, "volcano") / "nearby_events_primary.csv", map_pairs)
                except Exception as exc:
                    log.warning("Could not save nearby-event map table: %s", exc)
            pairs.unpersist()

        combined = pd.concat(all_results, ignore_index=True)
        atomic_write_csv(results_dir(cfg, "volcano") / "eruption_results_all_radii.csv", combined)
        radius_df = pd.DataFrame(summary_by_radius)
        atomic_write_csv(results_dir(cfg, "volcano") / "radius_sensitivity.csv", radius_df)
        primary_radius = float(vcfg["primary_radius_km"])
        primary = combined[combined["radius_km"] == primary_radius].copy()
        # Volcano-level summary: one row per volcano, using eruptions with a
        # sufficient baseline.  This is a ranking aid, not proof of an anomaly.
        if len(primary):
            vs = primary.groupby(["volcano_number"]).agg(
                eruptions=("eruption_uid", "count"),
                sufficient_eruptions=("sufficient_baseline", "sum"),
                median_pre_rate_ratio=("pre_rate_ratio", "median"),
                max_pre_rate_ratio=("pre_rate_ratio", "max"),
                median_post_rate_ratio=("post_rate_ratio", "median"),
                min_pre_q_value=("pre_q_value_bh", "min"),
                max_pre_events=("pre_events", "max"),
                nearby_earthquakes=("nearby_earthquakes", "max"),
            ).reset_index()
            vinfo = volcanoes.toPandas()
            vs = vs.merge(vinfo, on="volcano_number", how="left")
            # A volcano with no eruption having a sufficient baseline cannot be
            # ranked meaningfully; keep it out of the ranking table.
            vs = vs[vs["sufficient_eruptions"] > 0].copy()
            vs = vs.sort_values(["median_pre_rate_ratio", "min_pre_q_value"], ascending=[False, True])
            atomic_write_csv(results_dir(cfg, "volcano") / f"volcano_summary_{int(primary_radius)}km.csv", vs)
            atomic_write_csv(results_dir(cfg, "volcano") / "volcano_rankings_primary.csv", vs)
        summary = {
            "dataset": dataset,
            "backend": eq_backend,
            "generated_at": utc_now_iso(),
            "eruptions_analysed": int(n_eruptions),
            "volcanoes_with_eruptions": int(n_volcanoes_with_eruptions),
            "radii_km": [float(r) for r in vcfg["radii_km"]],
            "primary_radius_km": primary_radius,
            "baseline_years": float(vcfg["baseline_years"]),
            "pre_days": float(vcfg["pre_days"]),
            "post_days": float(vcfg["post_days"]),
            "permutations": int(vcfg["permutations"]),
            "fdr_alpha": alpha,
            "date_filter": {
                "start": vcfg["analysis_start"], "end": vcfg["analysis_end"],
                "min_precision": min_precision, "confirmed_only": bool(vcfg.get("confirmed_only", True)),
                "excluded_note": "Year-only or unconfirmed eruptions are excluded from the primary rate analysis.",
            },
            "radius_summary": summary_by_radius,
            "warnings": ([
                "This dataset is the sparse 10k sample, not a continuous observation series. "
                "Volcano rates from this run are for pipeline testing, not scientific inference."
            ] if dataset != "global" else []),
            "interpretation": "Observational association only; not eruption prediction and not causation.",
        }
        atomic_write_json(results_dir(cfg, "volcano") / "summary.json", summary)
        try:
            spark_df = spark.createDataFrame(combined)
            write_results_table(spark_df, "volcano/eruption_results", cfg)
        except Exception as exc:
            log.warning("Could not publish volcano results to HDFS: %s", exc)
        log.info("Volcano association complete: %s eruptions, radii %s", n_eruptions, vcfg["radii_km"])
        return summary
    finally:
        stop_spark(spark)


def main() -> None:
    parser = argparse.ArgumentParser(description="Volcano–earthquake association analysis")
    parser.add_argument("--dataset", default="sample_10k")
    args = parser.parse_args()
    cfg = load_config()
    setup_logging(cfg)
    print(json.dumps(run(args.dataset, cfg), indent=2, default=str))


if __name__ == "__main__":
    main()
