"""Page 4 — space–time clustering and algorithm comparison."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.utils import fmt_float, fmt_int, geo_scatter, header, metric_row, page_config, read_csv, read_json

page_config("Space-Time Clustering")
header(
    "Space–time clustering",
    "Custom distributed ST-DBSCAN (Haversine + temporal proximity), DBSCAN and K-Means comparison",
)

metrics = read_json("clustering/metrics.json")
if not metrics:
    st.warning("Clustering results are not available yet.")
    st.code(".venv\\Scripts\\python.exe -m analytics.st_dbscan --dataset sample_10k", language="powershell")
    st.stop()

p = metrics.get("params", {})
metric_row([
    ("Sample events", fmt_int(metrics.get("sample_rows")), metrics.get("sample_note", "")),
    ("Global clusters", fmt_int(metrics.get("partition_diagnostics", {}).get("merged_global_clusters")), "After Union–Find merge"),
    ("Noise events", fmt_int(metrics.get("partition_diagnostics", {}).get("noise_events")), "Never assigned to a cluster"),
    ("ARI vs reference", fmt_float(metrics.get("distributed_vs_reference", {}).get("adjusted_rand_index"), 6), "1.0 = identical partition; core ARI is also reported"),
])
st.caption(
    f"Parameters: ε spatial = {p.get('eps_km')} km · ε temporal = {p.get('eps_time_days')} days · "
    f"minPts = {p.get('min_pts')} · partition cell = {p.get('partition_cell_km')} km · rings = {p.get('neighbor_rings')}"
)

labels = read_csv("clustering/st_dbscan_labels.csv")
if labels is not None and not labels.empty:
    labels["cluster_label"] = labels["cluster_id"].apply(lambda x: "Noise" if int(x) < 0 else f"Cluster {int(x)}")
    map_df = labels.sample(n=min(25000, len(labels)), random_state=7)
    st.subheader("Cluster map")
    fig = px.scatter_geo(
        map_df, lat="latitude", lon="longitude", color="cluster_label",
        hover_data=["time_utc", "mag", "cluster_id", "n_cells"],
        height=560, title="ST-DBSCAN clusters (bounded display sample)",
    )
    fig.update_traces(marker=dict(size=4, opacity=0.65))
    fig.update_geos(projection_type="natural earth", showland=True, landcolor="#f2f2f2",
                    showocean=True, oceancolor="#eaf3fb", showcountries=True)
    fig.update_layout(margin=dict(l=0, r=0, t=45, b=0), legend=dict(itemsizing="constant"))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Noise is shown in grey. Cluster labels are arbitrary identifiers.")
else:
    st.info("Cluster labels are unavailable.")

st.subheader("Cluster statistics")
stats = read_csv("clustering/cluster_stats.csv")
if stats is not None and not stats.empty:
    st.dataframe(stats.head(100), use_container_width=True, hide_index=True)
else:
    st.info("No cluster statistics available.")

st.subheader("Algorithm comparison")
comp = read_csv("clustering/algorithm_comparison.csv")
if comp is not None and not comp.empty:
    st.dataframe(comp, use_container_width=True, hide_index=True)
    fig = px.bar(comp, x="algorithm", y="runtime_seconds", color="algorithm",
                 title="Measured runtime (single run; Spark local mode)")
    fig.update_layout(showlegend=False, height=380, xaxis_title="", yaxis_title="Seconds",
                      margin=dict(l=10, r=10, t=45, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Parameter sensitivity")
sens = read_csv("clustering/parameter_sensitivity.csv")
if sens is not None and not sens.empty:
    mp = st.selectbox("minPts slice", sorted(sens["min_pts"].unique()), index=0)
    s = sens[sens["min_pts"] == mp]
    fig = px.scatter(s, x="eps_km", y="eps_time_days", color="noise_fraction", size="clusters",
                     title=f"Cluster/noise sensitivity (minPts={mp})", color_continuous_scale="Viridis")
    fig.update_layout(height=400, xaxis_title="Spatial ε (km)", yaxis_title="Temporal ε (days)",
                      margin=dict(l=10, r=10, t=45, b=10))
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(s, use_container_width=True, hide_index=True)
else:
    st.info("Parameter sensitivity table unavailable.")

with st.expander("Correctness and limitations"):
    st.markdown(
        f"""
- **Distributed vs single-machine ARI:** {metrics.get('distributed_vs_reference', {}).get('adjusted_rand_index')}
- Local clustering runs in Spark partitions with overlapping 5° halo cells; local clusters sharing an event are merged with Union–Find.
- The merge itself is driver-side; this is stated in the metrics and is not hidden.
- **F1 against the window reference:** {metrics.get('f1_st_dbscan_vs_window_reference', {}).get('f1')}. The reference is a simplified Gardner–Knopoff-style declustering, not ground truth.
- K-Means uses 3-D unit vectors and ignores time; it is included only as a baseline comparison.
- Spark local mode is one machine, not a multi-node cluster.
"""
    )
