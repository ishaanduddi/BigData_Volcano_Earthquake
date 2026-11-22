"""Page 6 — actual Pandas vs Spark benchmark results."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.utils import RESULTS, fmt_float, fmt_int, header, page_config, read_csv, read_json

page_config("Performance Benchmarking")
header("Performance benchmarking", "Measured Pandas vs Spark timings; no hard-coded speed-ups")

meta = read_json("benchmarks/global/benchmark_metadata.json")
bench_prefix = "benchmarks/global"
if not meta:
    meta = read_json("benchmarks/sample_10k/benchmark_metadata.json")
    bench_prefix = "benchmarks/sample_10k"
if not meta:
    meta = read_json("benchmarks/benchmark_metadata.json")
    bench_prefix = "benchmarks"
if not meta:
    st.warning("Benchmark results are not available yet.")
    st.code(".venv\\Scripts\\python.exe -m benchmarking.benchmark_report --dataset global", language="powershell")
    st.stop()

hw = meta.get("hardware", {})
metric_row = [
    ("Spark startup", f"{fmt_float(meta.get('spark_startup_seconds'), 2)} s", "Measured before benchmark work"),
    ("CPU", f"{hw.get('cpu_physical')}C / {hw.get('cpu_logical')}T", hw.get("processor", "")),
    ("RAM", f"{(hw.get('ram_total_bytes') or 0) / (1024**3):.1f} GiB", "Host memory"),
    ("Raw measurements", fmt_int(meta.get("raw_rows")), "All rows in raw_timings.csv"),
]
cols = st.columns(4)
for c, (label, value, help_text) in zip(cols, metric_row):
    c.metric(label, value, help=help_text)

st.caption(meta.get("execution_note", ""))

speed = read_csv(f"{bench_prefix}/speedup.csv")
if speed is not None and not speed.empty:
    st.subheader("Speed-up by operation (Parquet, Spark cached)")
    fig = px.bar(speed, x="operation", y="speedup_spark_over_pandas", color="fraction", barmode="group",
                 title="Speed-up > 1 means Spark faster; < 1 means Pandas faster")
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray")
    fig.update_layout(height=420, xaxis_title="", yaxis_title="Pandas time / Spark time",
                      margin=dict(l=10, r=10, t=45, b=10))
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(speed, use_container_width=True, hide_index=True)

st.subheader("Scaling")
scaling = read_csv(f"{bench_prefix}/scaling.csv")
if scaling is not None:
    g = scaling[(scaling["operation"] == "groupby_year") & (scaling["format"] == "parquet")]
    if not g.empty:
        fig = px.line(g, x="fraction", y="median_seconds", color="engine", markers=True,
                      title="Group-by scaling with data fraction")
        fig.update_layout(height=400, xaxis_title="Fraction of processed data", yaxis_title="Median seconds",
                          margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    st.subheader("CSV vs Parquet")
    fmt = read_csv(f"{bench_prefix}/format_comparison.csv")
    if fmt is not None:
        st.dataframe(fmt, use_container_width=True, hide_index=True)
with c2:
    st.subheader("Caching")
    cache = read_csv(f"{bench_prefix}/caching_comparison.csv")
    if cache is not None:
        st.dataframe(cache[["fraction", "cached", "median_seconds"]], use_container_width=True, hide_index=True)

st.subheader("Partitions")
part = read_csv(f"{bench_prefix}/partition_comparison.csv")
if part is not None and not part.empty:
    st.dataframe(part[["fraction", "partitions", "median_seconds"]], use_container_width=True, hide_index=True)

st.subheader("Clustering overhead")
clust = read_csv(f"{bench_prefix}/clustering_comparison.csv")
if clust is not None and not clust.empty:
    st.dataframe(clust[["rows", "engine", "seconds", "result", "notes"]], use_container_width=True, hide_index=True)

st.subheader("Saved figures")
figdir = RESULTS / bench_prefix / "figures"
for name in ("scaling_groupby.png", "format_comparison.png", "partition_comparison.png", "caching_comparison.png"):
    p = figdir / name
    if p.exists():
        st.image(str(p), caption=name.replace("_", " ").replace(".png", ""), use_container_width=True)

with st.expander("Fair-comparison notes"):
    st.markdown(
        """
- Identical Parquet/CSV files are written once; both engines read the same rows.
- Fractions are deterministic subsets; Spark load timing includes a `count()` action.
- Pandas is single-process; Spark runs in local[6] mode — not a multi-node cluster.
- Load and group-by use 3 repeats and medians; heavier operations use one run (recorded in `raw_timings.csv`).
- Cases where Pandas is faster are displayed, not hidden.
"""
    )
