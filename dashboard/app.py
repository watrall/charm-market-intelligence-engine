# CHARM Dashboard — Skills × Seniority × Job Type (Folium)
# Minimal, explicit, and production-minded. No magic.

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import folium
from folium.plugins import MarkerCluster, HeatMap
from streamlit_folium import st_folium

# ---------- Config ----------
st.set_page_config(page_title="CHARM Dashboard", layout="wide")
BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data" / "processed"
GEO_DIR = BASE / "data" / "geo"

DEFAULT_CENTER = [39.5, -98.35]  # Lower 48 centroid
DEFAULT_ZOOM = 4

# Colors by seniority (neutral, readable)
SENIORITY_COLORS = {
    "entry": "#4c78a8",
    "mid": "#72b7b2",
    "senior": "#f58518",
    "lead/PI": "#e45756",
}
# Icons by job type (Font Awesome names Folium supports)
JOBTYPE_ICONS = {
    "field-tech": "wrench",
    "lab/analyst": "flask",
    "architectural-historian": "landmark",
    "pm/pi": "briefcase",
}

# ---------- Data ----------
def _coerce_skills(value) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = ast.literal_eval(raw)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except (ValueError, SyntaxError):
                pass
        tokens = re.split(r"[;|,]", raw)
        return [t.strip(" []'\"") for t in tokens if t.strip(" []'\"")]
    return []


@st.cache_data(show_spinner=False)
def load_jobs() -> pd.DataFrame:
    fp = DATA_DIR / "jobs.csv"
    if not fp.exists():
        return pd.DataFrame()

    df = pd.read_csv(fp)
    # Expected columns (degrade gracefully if some are missing)
    # id, title, company, url, city, state, lat, lon, date_posted, seniority, job_type, skills
    # skills: semicolon/pipe/comma-separated normalized skills
    # dates to datetime
    if "date_posted" in df.columns:
        df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce", utc=True).dt.date

    # ensure lat/lon numeric
    for col in ("lat", "lon"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # basic cleanliness
    for col in ("title", "company", "state", "city", "seniority", "job_type"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # skills → list[str]
    if "skills" in df.columns:
        df["skills_list"] = df["skills"].apply(_coerce_skills)
    else:
        df["skills_list"] = [[] for _ in range(len(df))]

    # lightweight validity
    if {"lat", "lon"}.issubset(df.columns):
        df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180)]

    return df.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_us_states_geojson():
    """
    Load a US states GeoJSON if available. Supports common property keys:
    STUSPS (state code), NAME, GEOID. We match on two-letter state codes when possible.
    """
    for name in ("us_states_simplified.geojson", "us_states.geojson"):
        fp = GEO_DIR / name
        if fp.exists():
            data = json.loads(fp.read_text(encoding="utf-8"))
            # detect state code key
            code_key = None
            try:
                props = data["features"][0]["properties"]
                for k in ("STUSPS", "state_abbr", "STATE_ABBR", "STATE", "NAME", "GEOID"):
                    if k in props:
                        code_key = k
                        break
            except Exception:
                pass
            return data, code_key
    return None, None


def kpi_cards(df: pd.DataFrame):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total jobs", f"{len(df):,}")
    with c2:
        uniq_org = df["company"].nunique() if "company" in df.columns else 0
        st.metric("Unique employers", f"{uniq_org:,}")
    with c3:
        uniq_locs = df[["city", "state"]].dropna().drop_duplicates().shape[0] if {"city","state"}.issubset(df.columns) else 0
        st.metric("Unique locations", f"{uniq_locs:,}")


def sidebar_filters(df: pd.DataFrame):
    st.sidebar.header("Filters")

    # Date window
    if "date_posted" in df.columns and df["date_posted"].notna().any():
        dmin = df["date_posted"].min()
        dmax = df["date_posted"].max()
        start, end = st.sidebar.date_input(
            "Date range",
            value=(dmin, dmax),
            min_value=dmin, max_value=dmax
        )
        if isinstance(start, tuple) or isinstance(end, tuple):
            # Streamlit guard: sometimes returns tuple on first render
            start, end = dmin, dmax
        df = df[(df["date_posted"] >= start) & (df["date_posted"] <= end)]

    # Skill filter
    all_skills = sorted({s for lst in df["skills_list"] for s in lst})
    selected_skills = st.sidebar.multiselect("Skills", options=all_skills, default=[])
    if selected_skills:
        df = df[df["skills_list"].apply(lambda lst: any(s in lst for s in selected_skills))]

    # Seniority
    seniority_opts = sorted([s for s in df["seniority"].unique() if s]) if "seniority" in df.columns else []
    selected_sen = st.sidebar.multiselect("Seniority", options=seniority_opts, default=seniority_opts)
    if "seniority" in df.columns and selected_sen:
        df = df[df["seniority"].isin(selected_sen)]

    # Job type
    jobtype_opts = sorted([s for s in df["job_type"].unique() if s]) if "job_type" in df.columns else []
    selected_jt = st.sidebar.multiselect("Job type", options=jobtype_opts, default=jobtype_opts)
    if "job_type" in df.columns and selected_jt:
        df = df[df["job_type"].isin(selected_jt)]

    # Map mode
    mode = st.sidebar.radio(
        "Map mode",
        options=("Points (clustered)", "Choropleth (by state)", "Heatmap"),
        index=0
    )

    return df, selected_skills, mode


def draw_points_map(df: pd.DataFrame):
    m = folium.Map(location=DEFAULT_CENTER, zoom_start=DEFAULT_ZOOM, tiles="CartoDB positron")
    cluster = MarkerCluster(name="Jobs").add_to(m)

    # choose sensible defaults
    has_url = "url" in df.columns
    has_city_state = {"city", "state"}.issubset(df.columns)
    has_seniority = "seniority" in df.columns
    has_jobtype = "job_type" in df.columns

    for _, r in df.iterrows():
        if pd.isna(r.get("lat")) or pd.isna(r.get("lon")):
            continue

        color = SENIORITY_COLORS.get(str(r.get("seniority", "")).lower(), "#6c6c6c") if has_seniority else "#6c6c6c"
        icon = JOBTYPE_ICONS.get(str(r.get("job_type", "")).lower(), "circle") if has_jobtype else "circle"

        title = str(r.get("title", "")).strip()
        company = str(r.get("company", "")).strip()
        loc_txt = f"{str(r.get('city','')).strip()}, {str(r.get('state','')).strip()}" if has_city_state else ""
        skills = ", ".join(r.get("skills_list", [])[:5])

        line1 = f"<b>{title}</b>" if title else ""
        line2 = company if company else ""
        line3 = loc_txt if loc_txt else ""
        line4 = skills if skills else ""
        url = str(r.get("url", "")).strip() if has_url else ""

        links = f'<br><a href="{url}" target="_blank">Job posting</a>' if has_url and url else ""
        html = "<br>".join([x for x in (line1, line2, line3, line4) if x]) + links

        folium.Marker(
            location=[r["lat"], r["lon"]],
            icon=folium.Icon(color="lightgray", icon=icon, prefix="fa"),
        ).add_to(cluster)

        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=6,
            color=color,
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(html, max_width=320),
        ).add_to(m)

    folium.LayerControl(collapsed=True).add_to(m)
    return m


def draw_choropleth(df: pd.DataFrame, selected_skills: list[str]):
    if "state" not in df.columns:
        st.info("No `state` field available for choropleth.")
        return None

    gjson, code_key = load_us_states_geojson()
    if not gjson or not code_key:
        st.info("US states GeoJSON not found in `data/geo/`. Skipping choropleth mode.")
        return None

    # Aggregate counts by state; filter by skills if any were selected
    if selected_skills:
        df = df[df["skills_list"].apply(lambda lst: any(s in lst for s in selected_skills))]

    tbl = (
        df.assign(state=df["state"].str.upper().str.strip())
          .groupby("state", dropna=True)
          .size()
          .reset_index(name="count")
    )

    m = folium.Map(location=DEFAULT_CENTER, zoom_start=DEFAULT_ZOOM, tiles="CartoDB positron")
    folium.Choropleth(
        geo_data=gjson,
        name="Skill intensity" if selected_skills else "Job intensity",
        data=tbl,
        columns=["state", "count"],
        key_on=f"feature.properties.{code_key}",
        fill_color="YlGnBu",
        fill_opacity=0.85,
        line_opacity=0.2,
        nan_fill_opacity=0.05,
        legend_name="Postings"
    ).add_to(m)

    # Simple tooltip
    def _state_map():
        d = {}
        for _, r in tbl.iterrows():
            d[str(r["state"]).upper()] = int(r["count"])
        return d
    state_count = _state_map()

    folium.GeoJson(
        gjson,
        name="States",
        style_function=lambda x: {"fillOpacity": 0, "color": "#999", "weight": 0.4},
        tooltip=folium.features.GeoJsonTooltip(
            fields=[code_key],
            aliases=["State"],
            labels=True,
            sticky=True
        ),
        highlight_function=lambda x: {"weight": 1.5, "color": "#333"},
        popup=folium.GeoJsonPopup(
            fields=[code_key],
            aliases=["State"],
            localize=True
        )
    ).add_to(m)

    # Add counts as markers (optional, readable)
    for _, r in tbl.iterrows():
        # We'll place a label at approximate state centroid using bounds (kept simple)
        # For a small, robust implementation, we skip dynamic centroids to avoid heavy deps.
        pass  # Intentionally minimal; choropleth color carries the message.

    folium.LayerControl(collapsed=True).add_to(m)
    return m


def draw_heatmap(df: pd.DataFrame):
    if not {"lat", "lon"}.issubset(df.columns):
        st.info("No coordinates available for heatmap.")
        return None

    m = folium.Map(location=DEFAULT_CENTER, zoom_start=5, tiles="CartoDB positron")
    points = df[["lat", "lon"]].dropna().values.tolist()
    if not points:
        st.info("No points to render.")
        return None

    HeatMap(points, radius=14, blur=18, max_zoom=6).add_to(m)
    return m


def main():
    st.title("CHARM — Market Intelligence")
    df = load_jobs()

    if df.empty:
        st.info("No data yet. Run the pipeline to generate `data/processed/jobs.csv`.")
        return

    kpi_cards(df)
    st.divider()

    filtered, selected_skills, mode = sidebar_filters(df)

    tabs = st.tabs(["Map", "Tables"])
    with tabs[0]:
        if mode == "Points (clustered)":
            m = draw_points_map(filtered)
        elif mode == "Choropleth (by state)":
            m = draw_choropleth(filtered, selected_skills)
        else:
            m = draw_heatmap(filtered)

        if m is not None:
            st_folium(m, width=1100, height=640)

    with tabs[1]:
        st.subheader("Filtered rows")
        show_cols = [c for c in ["date_posted","title","company","city","state","seniority","job_type","skills","url"] if c in filtered.columns]
        st.dataframe(filtered[show_cols].reset_index(drop=True), use_container_width=True)
        st.download_button(
            "Download filtered CSV",
            filtered.to_csv(index=False).encode("utf-8"),
            "filtered_jobs.csv",
            "text/csv"
        )


if __name__ == "__main__":
    main()
