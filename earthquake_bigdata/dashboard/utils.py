"""Shared Streamlit helpers. The dashboard reads only saved result files."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import get_path, load_config

CFG = load_config()
RESULTS = get_path(CFG, "results")


def page_config(title: str) -> None:
    st.set_page_config(page_title=f"{title} | Earthquake Big Data", page_icon="🌍", layout="wide")


def header(title: str, subtitle: str = "") -> None:
    st.title(title)
    if subtitle:
        st.caption(subtitle)


@st.cache_data(show_spinner=False)
def read_csv(rel: str) -> Optional[pd.DataFrame]:
    p = RESULTS / rel
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def read_json(rel: str) -> Optional[Dict[str, Any]]:
    p = RESULTS / rel
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def missing(rel: str, command: str) -> None:
    st.warning(f"Result file `results/{rel}` is not available yet.")
    st.code(command, language="powershell")


def metric_row(items) -> None:
    cols = st.columns(len(items))
    for col, (label, value, help_text) in zip(cols, items):
        col.metric(label, value, help=help_text)


def fmt_int(v) -> str:
    try:
        return f"{int(v):,}"
    except Exception:
        return "—"


def fmt_float(v, digits: int = 2) -> str:
    try:
        return f"{float(v):,.{digits}f}"
    except Exception:
        return "—"


def geo_scatter(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    color_col: Optional[str] = None,
    size_col: Optional[str] = None,
    hover_cols: Optional[list] = None,
    title: str = "",
    height: int = 520,
    colorscale: str = "Viridis",
    colorbar_title: str = "",
    marker_size: int = 5,
    opacity: float = 0.65,
):
    if df is None or df.empty:
        return None
    hover_data = {}
    for c in (hover_cols or []):
        if c in df.columns:
            hover_data[c] = True
    kwargs = dict(
        lat=lat_col, lon=lon_col, hover_data=hover_data,
        height=height, title=title,
    )
    if color_col and color_col in df.columns:
        kwargs["color"] = color_col
        kwargs["color_continuous_scale"] = colorscale
        kwargs["color_continuous_midpoint"] = None
    if size_col and size_col in df.columns:
        kwargs["size"] = size_col
        kwargs["size_max"] = 18
    try:
        import plotly.express as px
        fig = px.scatter_geo(df, **kwargs)
        fig.update_traces(marker=dict(opacity=opacity, line=dict(width=0)))
        fig.update_geos(
            projection_type="natural earth", showland=True, landcolor="#f2f2f2",
            showocean=True, oceancolor="#eaf3fb", showcoastlines=True,
            coastlinecolor="#b0b0b0", showcountries=True, countrycolor="#d0d0d0",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=35 if title else 10, b=0))
        if color_col and colorbar_title:
            fig.update_coloraxes(colorbar_title=colorbar_title)
        return fig
    except Exception:
        # Fallback without geographic projection dependencies.
        fig = go.Figure(go.Scattergeo(lat=df[lat_col], lon=df[lon_col], mode="markers",
                                      marker=dict(size=marker_size, opacity=opacity)))
        fig.update_layout(height=height, title=title)
        return fig


def empty_state(rel: str, command: str) -> None:
    missing(rel, command)
