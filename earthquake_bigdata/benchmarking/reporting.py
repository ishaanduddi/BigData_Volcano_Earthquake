"""Turn benchmark measurements into tables and figures. No values are invented."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common.io import atomic_write_csv, atomic_write_json, ensure_dir, utc_now_iso


def _safe_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_No measurements available._"
    try:
        return df.to_markdown(index=False)
    except Exception:
        return "```\n" + df.to_string(index=False) + "\n```"


def write_report(outdir: Path, raw: pd.DataFrame, summary: pd.DataFrame, meta: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    outdir = Path(outdir)
    ensure_dir(outdir)
    figs = outdir / "figures"
    ensure_dir(figs)
    hw = meta.get("hardware", {})

    # Speed-up table: default configuration (Parquet, cached for Spark).
    sp = summary[summary["format"] == "parquet"].copy()
    sp = sp[sp["partitions"].isna()]
    speed_rows = []
    for (frac, op), g in sp.groupby(["fraction", "operation"]):
        p = g[g["engine"] == "pandas"]["median_seconds"]
        s = g[(g["engine"] == "spark") & (g["cached"] == True)]["median_seconds"]
        if len(p) and len(s):
            pt, st = float(p.iloc[0]), float(s.iloc[0])
            speed_rows.append({
                "fraction": float(frac), "operation": op, "pandas_seconds": pt,
                "spark_seconds": st, "speedup_spark_over_pandas": (pt / st if st > 0 else None),
                "faster_engine": "Spark" if st < pt else "Pandas",
            })
    speedup = pd.DataFrame(speed_rows).sort_values(["operation", "fraction"])
    atomic_write_csv(outdir / "speedup.csv", speedup)

    # Format comparison at largest fraction for group-by.
    fmt_source = summary[(summary["operation"] == "groupby_year") & (summary["partitions"].isna())]
    fmt_rows = []
    for engine in ("pandas", "spark"):
        for fmt in ("csv", "parquet"):
            g = fmt_source[(fmt_source["engine"] == engine) & (fmt_source["format"] == fmt)]
            if len(g):
                r = g.sort_values("fraction").iloc[-1]
                fmt_rows.append({"engine": engine, "format": fmt, "fraction": float(r["fraction"]),
                                 "median_seconds": float(r["median_seconds"])})
    formats = pd.DataFrame(fmt_rows)
    atomic_write_csv(outdir / "format_comparison.csv", formats)

    part = summary[(summary["operation"] == "groupby_year") & (summary["engine"] == "spark")
                   & (summary["partitions"].notna())].copy()
    atomic_write_csv(outdir / "partition_comparison.csv", part)

    cache = summary[(summary["operation"] == "groupby_year") & (summary["engine"] == "spark")
                    & (summary["format"] == "parquet") & (summary["partitions"].isna())].copy()
    atomic_write_csv(outdir / "caching_comparison.csv", cache)

    clust = raw[raw["operation"] == "st_dbscan_clustering"].copy()
    atomic_write_csv(outdir / "clustering_comparison.csv", clust)

    # ---- figures ----
    try:
        gb = sp[sp["operation"] == "groupby_year"]
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for engine in ("pandas", "spark"):
            x = gb[gb["engine"] == engine].sort_values("fraction")
            ax.plot(x["fraction"] * 100, x["median_seconds"], marker="o", label=engine.title())
        ax.set_xlabel("Fraction of processed data (%)")
        ax.set_ylabel("Median wall-clock time (s)")
        ax.set_title("Group-by scaling: Pandas vs Spark")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(figs / "scaling_groupby.png", dpi=160)
        plt.close(fig)
    except Exception:
        pass

    try:
        if not formats.empty:
            fig, ax = plt.subplots(figsize=(7, 4.2))
            labels = [f"{r.engine}\n{r.format}" for r in formats.itertuples(index=False)]
            ax.bar(labels, formats["median_seconds"])
            ax.set_ylabel("Median group-by time (s)")
            ax.set_title("CSV vs Parquet (largest fraction)")
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            fig.savefig(figs / "format_comparison.png", dpi=160)
            plt.close(fig)
    except Exception:
        pass

    try:
        if not part.empty:
            fig, ax = plt.subplots(figsize=(7, 4.2))
            x = part.sort_values("partitions")
            ax.plot(x["partitions"].astype(int), x["median_seconds"], marker="o")
            ax.set_xlabel("Spark shuffle partitions")
            ax.set_ylabel("Group-by time (s)")
            ax.set_title("Partition-count sensitivity (largest fraction)")
            ax.grid(alpha=0.3)
            fig.tight_layout()
            fig.savefig(figs / "partition_comparison.png", dpi=160)
            plt.close(fig)
    except Exception:
        pass

    try:
        if not cache.empty:
            fig, ax = plt.subplots(figsize=(6, 4))
            x = cache.groupby("cached")["median_seconds"].median()
            ax.bar(["uncached", "cached"], [x.get(False, np.nan), x.get(True, np.nan)])
            ax.set_ylabel("Group-by time (s)")
            ax.set_title("Spark caching: cached vs uncached")
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            fig.savefig(figs / "caching_comparison.png", dpi=160)
            plt.close(fig)
    except Exception:
        pass

    md = [
        "# Pandas vs Spark benchmark — measured results",
        "",
        f"Generated: {meta.get('generated_at')}",
        "",
        "## Hardware and execution mode",
        "",
        f"* CPU: {hw.get('processor')} ({hw.get('cpu_physical')} physical / {hw.get('cpu_logical')} logical cores)",
        f"* RAM: {hw.get('ram_total_bytes', 0) / (1024**3):.1f} GiB",
        f"* Platform: {hw.get('platform')}",
        f"* Python: {hw.get('python')}",
        f"* Spark: local mode, see `benchmark_metadata.json`; **not a multi-node cluster**.",
        f"* Measured Spark session startup: {meta.get('spark_startup_seconds'):.3f} s",
        "",
        "## Method",
        "",
        "* Identical Parquet and CSV files were written once from the processed dataset.",
        "* Fractions are deterministic random subsets of the same source rows.",
        "* Load and group-by use 3 repeats and the median; heavier b-value/spatial-join",
        "  operations use one measured run (shown in `raw_timings.csv`).",
        "* Spark load timing includes a `count()` action so lazy evaluation is not hidden.",
        "* Pandas is single-process; Spark is `local[6]`. This is not a like-for-like core-count claim.",
        "",
        "## Speed-up (Parquet, Spark cached; positive means Spark faster)",
        "",
        _safe_table(speedup),
        "",
        "## CSV vs Parquet (group-by)",
        "",
        _safe_table(formats),
        "",
        "## Partition sensitivity",
        "",
        _safe_table(part[["fraction", "partitions", "median_seconds"]] if not part.empty else part),
        "",
        "## Caching",
        "",
        _safe_table(cache[["fraction", "cached", "median_seconds"]] if not cache.empty else cache),
        "",
        "## Clustering overhead (bounded sample)",
        "",
        _safe_table(clust[["rows", "engine", "seconds", "result", "notes"]] if not clust.empty else clust),
        "",
        "## Interpretation rules",
        "",
        "* A speed-up below 1 means **Pandas was faster**; this is reported, not suppressed.",
        "* Spark's fixed startup and scheduling overhead dominates small datasets.",
        "* Timings are machine-specific and should be reproduced with `benchmark_report.py`.",
        "* No benchmark value in this file is hard-coded; all values come from `raw_timings.csv`.",
        "",
        "Figures: `figures/scaling_groupby.png`, `figures/format_comparison.png`,",
        "`figures/partition_comparison.png`, `figures/caching_comparison.png`.",
    ]
    (outdir / "benchmark_report.md").write_text("\n".join(md), encoding="utf-8")
