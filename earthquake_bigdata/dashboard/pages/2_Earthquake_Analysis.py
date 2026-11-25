"""Page 2 — earthquake trends, distributions and geography."""
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
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils import fmt_float, fmt_int, geo_scatter, header, metric_row, page_config, read_csv, read_json

page_config("Earthquake Analysis")
header("Earthquake analysis", "Yearly and monthly trends, magnitude/depth distributions, geographic distribution")

summary = read_json("basic/summary.json")
if not summary:
    st.warning("Run basic statistics first.")
    st.code(".venv\\Scripts\\python.exe -m analytics.basic_statistics --dataset sample_10k", language="powershell")
    st.stop()

metric_row([
    ("Records", fmt_int(summary.get("total_records")), "Processed earthquakes"),
    ("Mean magnitude", fmt_float(summary.get("mean_magnitude")), "Reported magnitude"),
    ("Mean depth", f"{fmt_float(summary.get('mean_depth_km'), 1)} km", "All depths"),
    ("Magnitude types", fmt_int(summary.get("magnitude_types")), "Mixed scales are not blindly combined in hotspots"),
])

yearly = read_csv("basic/yearly_counts.csv")
monthly = read_csv("basic/monthly_counts.csv")
maghist = read_csv("basic/magnitude_histogram.csv")
depthhist = read_csv("basic/depth_histogram.csv")

st.subheader("Temporal trends")
c1, c2 = st.columns(2)
with c1:
    if yearly is not None and not yearly.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=yearly["year"], y=yearly["earthquakes"], name="Earthquakes"))
        fig.add_trace(go.Scatter(x=yearly["year"], y=yearly["max_magnitude"], name="Max magnitude", yaxis="y2", mode="lines+markers"))
        fig.update_layout(
            title="Yearly earthquake counts (bars) and maximum magnitude (line)",
            yaxis=dict(title="Events"), yaxis2=dict(title="Max magnitude", overlaying="y", side="right"),
            legend=dict(orientation="h"), height=420, margin=dict(l=10, r=10, t=45, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Yearly counts unavailable.")
with c2:
    if monthly is not None and not monthly.empty:
        fig = px.line(monthly, x="year_month", y="earthquakes", markers=True,
                      title="Monthly earthquake counts (available months)")
        fig.update_layout(height=420, xaxis_title="", yaxis_title="Events", margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Monthly counts unavailable.")

st.subheader("Magnitude and depth")
c1, c2 = st.columns(2)
with c1:
    if maghist is not None and not maghist.empty:
        fig = px.bar(maghist, x="magnitude_bin", y="count", title="Magnitude distribution (0.5 bins)")
        fig.update_layout(height=380, xaxis_title="Magnitude", yaxis_title="Events", margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Magnitude histogram unavailable.")
with c2:
    if depthhist is not None and not depthhist.empty:
        fig = px.bar(depthhist, x="depth_bin_km", y="count", title="Depth distribution (25 km bins)")
        fig.update_layout(height=380, xaxis_title="Depth (km)", yaxis_title="Events", margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Depth histogram unavailable.")

st.subheader("Geographic distribution")
events = read_csv("basic/event_map_sample.csv")
if events is not None and not events.empty:
    events = events.copy()
    events["mag"] = pd.to_numeric(events["mag"], errors="coerce")
    f1, f2, f3 = st.columns([1, 1, 2])
    year_min, year_max = int(events["year"].min()), int(events["year"].max())
    yr = f1.slider("Year range", year_min, year_max, (year_min, year_max))
    mag_min, mag_max = float(events["mag"].min()), float(events["mag"].max())
    mr = f2.slider("Magnitude range", mag_min, mag_max, (mag_min, min(mag_max, mag_min + 4.0)))
    filtered = events[events["year"].between(*yr) & events["mag"].between(*mr)]
    f3.metric("Events shown", fmt_int(len(filtered)))
    fig = geo_scatter(
        filtered, "longitude", "latitude", color_col="mag", size_col=None,
        hover_cols=["time_utc", "mag", "depth", "latitude", "longitude"],
        title="Earthquake epicentres (bounded dashboard sample)", colorbar_title="Magnitude",
        colorscale="Turbo", marker_size=5, opacity=0.6,
    )
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    st.caption("This map is a bounded sample of real processed records for dashboard responsiveness; it is not a density estimate.")
else:
    top = read_csv("basic/top_regions.csv")
    if top is not None:
        fig = geo_scatter(top, "center_latitude", "center_longitude", color_col="earthquakes",
                          size_col="earthquakes", hover_cols=["grid_id_5", "earthquakes", "mean_magnitude"],
                          title="5° cell counts", colorbar_title="Events")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    st.info("Event-level dashboard map not available; showing cell counts.")

st.divider()
st.caption("Magnitude scales are reported per event; see the hotspot analysis for the dominant-scale policy.")
