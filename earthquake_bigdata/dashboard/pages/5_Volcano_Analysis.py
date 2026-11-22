"""Page 5 — volcano-specific earthquake association analysis."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils import fmt_float, fmt_int, header, page_config, read_csv, read_json

page_config("Volcano Analysis")
header(
    "Volcano–earthquake association",
    "Volcano-specific baselines, 90-day pre-eruption and 180-day post-eruption windows; association only",
)

summary = read_json("volcano/summary.json")
if not summary:
    st.warning("Volcano association results are not available yet.")
    st.code(".venv\\Scripts\\python.exe -m analytics.volcano_association --dataset sample_10k", language="powershell")
    st.stop()

vsum = read_csv("volcano/volcano_rankings_primary.csv")
allres = read_csv("volcano/eruption_results_all_radii.csv")
radius_summary = read_csv("volcano/radius_sensitivity.csv")
nearby = read_csv("volcano/nearby_events_primary.csv")

if vsum is None or vsum.empty:
    st.warning("No volcano summary with a sufficient baseline is available for this dataset.")
    st.stop()

for w in summary.get("warnings", []):
    st.warning(w)

st.caption(
    f"Dataset: **{summary.get('dataset')}** · Eruptions analysed: **{summary.get('eruptions_analysed')}** · "
    f"Baseline: **{summary.get('baseline_years')} years** · pre **{summary.get('pre_days')} d** / post **{summary.get('post_days')} d** · "
    f"FDR α={summary.get('fdr_alpha')}"
)

vsum = vsum.copy()
vsum["label"] = vsum["volcano_name"].astype(str) + " (" + vsum["volcano_number"].astype(str) + ")"
label = st.selectbox("Select a volcano", vsum["label"].tolist())
row = vsum[vsum["label"] == label].iloc[0]
vnum = int(row["volcano_number"])
primary_radius = float(summary.get("primary_radius_km", 100))
vr = allres[(allres["volcano_number"] == vnum) & (allres["radius_km"] == primary_radius)].copy() if allres is not None else pd.DataFrame()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Volcano", str(row.get("volcano_name")))
c2.metric("Eruptions used", fmt_int(row.get("sufficient_eruptions")))
c3.metric("Median pre rate ratio", fmt_float(row.get("median_pre_rate_ratio"), 2))
c4.metric("Min pre q-value", fmt_float(row.get("min_pre_q_value"), 4))

st.subheader("Location and nearby earthquakes")
map_rows = []
if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
    map_rows.append({"kind": "Volcano", "latitude": float(row["latitude"]), "longitude": float(row["longitude"]),
                     "mag": None, "time_utc": None, "distance_km": 0.0})
if nearby is not None and not nearby.empty:
    ne = nearby[nearby["volcano_number"] == vnum].copy()
    if len(ne):
        ne = ne.sample(n=min(3000, len(ne)), random_state=11)
        for r in ne.itertuples(index=False):
            map_rows.append({"kind": "Earthquake", "latitude": float(r.latitude), "longitude": float(r.longitude),
                             "mag": float(r.mag) if pd.notna(r.mag) else None,
                             "time_utc": r.time_utc, "distance_km": float(r.distance_km)})
if map_rows:
    mdf = pd.DataFrame(map_rows)
    fig = px.scatter_geo(mdf, lat="latitude", lon="longitude", color="kind", hover_data=["mag", "time_utc", "distance_km"],
                         height=470, title=f"{row.get('volcano_name')} and earthquakes within {primary_radius:.0f} km")
    fig.update_traces(marker=dict(size=5, opacity=0.7))
    fig.update_geos(projection_type="natural orthographic" if False else "natural earth", showland=True,
                    landcolor="#f2f2f2", showocean=True, oceancolor="#eaf3fb")
    fig.update_layout(margin=dict(l=0, r=0, t=45, b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No nearby-event map table for this volcano.")

st.subheader("Eruption-window statistics (primary radius)")
if vr.empty:
    st.info("No eruption rows for this volcano at the primary radius.")
else:
    show_cols = [
        "start_date", "date_precision", "baseline_events", "pre_events", "post_events",
        "baseline_rate_per_day", "pre_rate_per_day", "post_rate_per_day",
        "pre_rate_ratio", "pre_ci_low", "pre_ci_high", "pre_p_value", "pre_q_value_bh",
        "post_rate_ratio", "post_ci_low", "post_ci_high", "post_q_value_bh",
        "permutation_p_value", "sufficient_baseline",
    ]
    st.dataframe(vr[[c for c in show_cols if c in vr.columns]], use_container_width=True, hide_index=True)

    plot = vr.melt(
        id_vars=["eruption_uid", "start_date"],
        value_vars=[c for c in ("baseline_rate_per_day", "pre_rate_per_day", "post_rate_per_day") if c in vr.columns],
        var_name="period", value_name="rate_per_day",
    )
    if not plot.empty:
        fig = px.bar(plot, x="start_date", y="rate_per_day", color="period", barmode="group",
                     title="Earthquake rate per day: baseline / pre / post")
        fig.update_layout(height=390, xaxis_title="Eruption start date", yaxis_title="Events per day",
                          margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)

st.subheader("Radius sensitivity")
if allres is not None and not allres.empty:
    vs = allres[allres["volcano_number"] == vnum].copy()
    if not vs.empty:
        agg = vs.groupby("radius_km").agg(
            eruptions=("eruption_uid", "count"),
            median_pre_rate_ratio=("pre_rate_ratio", "median"),
            median_post_rate_ratio=("post_rate_ratio", "median"),
            min_pre_q=("pre_q_value_bh", "min"),
        ).reset_index()
        st.dataframe(agg, use_container_width=True, hide_index=True)
        fig = px.line(agg, x="radius_km", y=["median_pre_rate_ratio", "median_post_rate_ratio"], markers=True,
                      title="Median rate ratio by search radius")
        fig.update_layout(height=350, xaxis_title="Radius (km)", yaxis_title="Rate ratio", margin=dict(l=10, r=10, t=45, b=10))
        st.plotly_chart(fig, use_container_width=True)
if radius_summary is not None:
    st.dataframe(radius_summary, use_container_width=True, hide_index=True)

with st.expander("Statistical interpretation"):
    st.markdown(
        """
- Baseline is five years before each eruption, clipped to the earthquake observation period and excluding eruption windows of the same volcano.
- Pre/post rates use the actually observed days; they are not forced to 90/180 when the observation period is incomplete.
- Rate-ratio confidence intervals use a log-normal Poisson approximation; p-values use a two-sided conditional binomial test.
- Permutation p-values shift the 90-day window within the baseline. FDR q-values use Benjamini–Hochberg across eruptions for each radius.
- Low baseline counts are common and are flagged with `sufficient_baseline=False`; they are not evidence of an anomaly.
- This is **association analysis**, not eruption prediction and not proof of causation.
"""
    )
