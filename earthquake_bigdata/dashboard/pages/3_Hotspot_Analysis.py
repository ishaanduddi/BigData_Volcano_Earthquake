"""Page 3 — detection-corrected hotspot analysis."""
from __future__ import annotations

import sys
from pathlib import Path
for _parent in Path(__file__).resolve().parents:
    if (_parent / "dashboard" / "utils.py").exists():
        if str(_parent) not in sys.path:
            sys.path.insert(0, str(_parent))
        break


import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.utils import fmt_float, fmt_int, geo_scatter, header, metric_row, page_config, read_csv, read_json

page_config("Hotspot Analysis")
header(
    "Corrected earthquake hotspots",
    "Raw counts vs rates above the estimated magnitude of completeness (Mc), with b-values and uncertainty",
)

summary = read_json("hotspots/summary.json")
if not summary:
    st.warning("Hotspot results are not available yet.")
    st.code(".venv\\Scripts\\python.exe -m analytics.hotspot_analysis --dataset sample_10k", language="powershell")
    st.stop()

metric_row([
    ("Groups analysed", fmt_int(summary.get("groups")), "5° cell × time window"),
    ("Ranking eligible", fmt_int(summary.get("ranking_eligible_groups")), "Sufficient sample + dominant scale"),
    ("Median Mc", fmt_float(summary.get("median_mc"), 2), "Maximum curvature + 0.2"),
    ("Median b-value", fmt_float(summary.get("median_b_value"), 2), "Aki–Utsu MLE"),
])

metrics = read_csv("hotspots/hotspot_metrics.csv")
if metrics is None or metrics.empty:
    st.error("hotspot_metrics.csv is missing.")
    st.stop()

windows = sorted(metrics["window_id"].dropna().unique().tolist())
window = st.selectbox("Time window", windows, index=len(windows) - 1)
wm = metrics[metrics["window_id"] == window].copy()

st.subheader("Raw vs corrected density")
c1, c2 = st.columns(2)
with c1:
    raw = read_csv("hotspots/raw_density_map.csv")
    if raw is not None:
        raw = raw[raw["window_id"] == window]
        fig = geo_scatter(raw, "center_latitude", "center_longitude", color_col="raw_rate_per_1000km2_yr",
                          hover_cols=["grid_id", "n_raw", "raw_rank"],
                          title="Raw rate per 1000 km² per year", colorbar_title="Raw rate", colorscale="Turbo")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
with c2:
    cor = read_csv("hotspots/corrected_density_map.csv")
    if cor is not None:
        cor = cor[cor["window_id"] == window]
        fig = geo_scatter(cor, "center_latitude", "center_longitude", color_col="corrected_rate_per_1000km2_yr",
                          hover_cols=["grid_id", "n_above_mc", "mc", "corrected_rank"],
                          title="Corrected rate above Mc", colorbar_title="Corrected rate", colorscale="Turbo")
        if fig:
            st.plotly_chart(fig, use_container_width=True)

st.subheader("Ranking comparison")
c1, c2 = st.columns(2)
with c1:
    st.markdown("**Top raw counts**")
    rt = wm.sort_values("raw_rank").head(15)[["grid_id", "n_raw", "raw_rate_per_1000km2_yr", "raw_rank", "dominant_scale", "scale_policy_pass"]]
    st.dataframe(rt, use_container_width=True, hide_index=True)
with c2:
    st.markdown("**Top corrected rates above Mc**")
    ct = wm[wm["use_in_ranking"]].sort_values("corrected_rank").head(15)[
        ["grid_id", "n_above_mc", "mc", "corrected_rate_per_1000km2_yr", "corrected_rank", "b_value"]
    ]
    st.dataframe(ct, use_container_width=True, hide_index=True)

st.subheader("Mc and b-value distributions")
c1, c2 = st.columns(2)
with c1:
    d = wm.dropna(subset=["mc"])
    if not d.empty:
        fig = px.histogram(d, x="mc", nbins=30, title="Mc by 5° cell")
        fig.update_layout(height=350, xaxis_title="Mc", yaxis_title="Cells", margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)
with c2:
    d = wm.dropna(subset=["b_value"])
    if not d.empty:
        fig = px.histogram(d, x="b_value", nbins=30, title="Gutenberg–Richter b-value")
        fig.update_layout(height=350, xaxis_title="b", yaxis_title="Cells", margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)

st.subheader("Did apparent growth survive correction?")
growth = read_csv("hotspots/region_growth_comparison.csv")
if growth is not None and not growth.empty:
    show = growth.sort_values("raw_rate_per_1000km2_yr_change_pct", ascending=False)
    st.dataframe(show, use_container_width=True, hide_index=True, height=320)
    st.caption(
        "Classes compare the first and second configured windows. "
        "'apparent_growth_disappears_after_correction' means raw rates rose while Mc-corrected rates did not."
    )
else:
    st.info("No cell had both windows eligible for a growth comparison.")

with st.expander("Method and limitations"):
    st.markdown(
        """
- **Mc:** maximum-curvature mode of the non-cumulative FMD plus the standard +0.2 correction; bootstrap intervals are reported.
- **b-value:** Aki–Utsu maximum likelihood with Utsu's binning correction; Shi–Bolt uncertainty.
- **Rates:** events above Mc per spherical cell area per year, per 1000 km².
- **Scale policy:** a cell/window is ranked only when one magnitude-scale family accounts for at least the configured share.
- Corrected rates are an approximation, not a direct measurement of true seismic activity or moment release.
"""
    )
