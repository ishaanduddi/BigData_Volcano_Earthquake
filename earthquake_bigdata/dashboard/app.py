"""Page 1 — project overview (reads saved results only)."""
from __future__ import annotations

import sys
from pathlib import Path
for _parent in Path(__file__).resolve().parents:
    if (_parent / "dashboard" / "utils.py").exists():
        if str(_parent) not in sys.path:
            sys.path.insert(0, str(_parent))
        break


import streamlit as st

from dashboard.utils import (
    fmt_float, fmt_int, geo_scatter, header, metric_row, page_config, read_csv, read_json,
)

page_config("Overview")
header(
    "🌍 Global Earthquake Patterns and Volcanic Activity",
    "Big Data analytics with USGS + Smithsonian GVP · HDFS + Apache Spark · statistical association, not prediction",
)

basic = read_json("basic/summary.json")
volcano_report = read_json("metadata/preprocessing_volcanoes.json")
storage = read_json("metadata/earthquakes_global_storage.json") or read_json("metadata/earthquakes_sample_10k_storage.json")
bench = read_json("benchmarks/global/benchmark_metadata.json") or read_json("benchmarks/sample_10k/benchmark_metadata.json") or read_json("benchmarks/benchmark_metadata.json")

if not basic:
    st.warning("Basic statistics have not been generated yet.")
    st.code(
        ".venv\\Scripts\\python.exe -m analytics.basic_statistics --dataset sample_10k",
        language="powershell",
    )
    st.stop()

n_volc = (volcano_report or {}).get("volcanoes")
backend = (storage or {}).get("backend_primary", basic.get("backend", "unknown"))
metric_row([
    ("Earthquake records", fmt_int(basic.get("total_records")), "Processed records in this result set"),
    ("Volcanoes", fmt_int(n_volc) if n_volc is not None else "—", "GVP Holocene volcanoes"),
    ("Average magnitude", fmt_float(basic.get("mean_magnitude"), 2), "Reported magnitude (mixed scales; see limitations)"),
    ("Storage backend", str(backend).upper(), "HDFS primary when available; metadata records the truth"),
])
metric_row([
    ("Date range", f"{str(basic.get('date_min'))[:10]} → {str(basic.get('date_max'))[:10]}", "UTC"),
    ("Median magnitude", fmt_float(basic.get("median_magnitude"), 2), "50th percentile"),
    ("Median depth", f"{fmt_float(basic.get('median_depth_km'), 1)} km", "Depth is retained as reported"),
    ("Occupied 5° cells", fmt_int(basic.get("occupied_5deg_cells")), "Used by hotspot analysis"),
])

st.divider()
left, right = st.columns([1.25, 1])
with left:
    st.subheader("Major earthquake regions (5° cells)")
    top = read_csv("basic/top_regions.csv")
    if top is not None and not top.empty:
        top = top.head(50)
        fig = geo_scatter(
            top, "center_latitude", "center_longitude",
            color_col="earthquakes", size_col="earthquakes",
            hover_cols=["grid_id_5", "earthquakes", "mean_magnitude", "max_magnitude", "example_place"],
            title="Raw earthquake counts by 5° cell",
            colorbar_title="Events", colorscale="Turbo",
        )
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top.head(12), use_container_width=True, hide_index=True)
    else:
        st.info("Top-region table is not available.")
with right:
    st.subheader("Pipeline status")
    st.markdown(
        f"""
| Component | Status |
|---|---|
| Raw USGS data | ✅ downloaded + validated |
| Raw GVP data | {('✅ ' + str(volcano_report.get('eruptions')) + ' eruptions') if volcano_report else '—'} |
| Processed Parquet | {'✅' if storage else '—'} |
| Basic statistics | ✅ |
| Hotspot analysis | {'✅' if read_json('hotspots/summary.json') else '—'} |
| ST-DBSCAN | {'✅' if read_json('clustering/metrics.json') else '—'} |
| Volcano association | {'✅' if read_json('volcano/summary.json') else '—'} |
| Benchmarks | {'✅' if bench else '—'} |
"""
    )
    st.subheader("Dataset provenance")
    st.markdown(
        "- **Earthquakes:** USGS FDSN Event Web Service, monthly requests, M ≥ 2.5, 2000–2025.\n"
        "- **Volcanoes/eruptions:** Smithsonian GVP official GeoServer WFS.\n"
        "- Raw responses are stored unchanged; the ETL report records every removal.\n"
        "- **Stress-test** Alaska/California records are excluded from these global findings."
    )
    if bench:
        st.caption(f"Spark startup measured: {bench.get('spark_startup_seconds', 0):.2f} s · "
                   f"{bench.get('raw_rows', 0)} benchmark measurements")

st.divider()
st.caption(
    "Interpretation: this project describes patterns and statistical associations. "
    "It does not predict earthquakes or volcanic eruptions and does not establish causation."
)
