from __future__ import annotations

import html
import json
import os
import re
import time
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from folium.plugins import HeatMap, MarkerCluster
from streamlit_folium import st_folium

from dashboard.pipeline_runner import acquire_lock, allow_pipeline_run, release_lock, run_pipeline

st.set_page_config(page_title="CHARM Dashboard", layout="wide")

BASE = Path(__file__).resolve().parents[1]
REAL_DATA_DIR = BASE / "data" / "processed"
DEMO_DATA_DIR = BASE / "demo" / "processed"
GEO_DIR = BASE / "data" / "geo"

DEFAULT_CENTER = [39.5, -98.35]
DEFAULT_ZOOM = 4

SENIORITY_COLORS = {
    "entry": "#4c78a8",
    "mid": "#72b7b2",
    "senior": "#f58518",
    "lead/PI": "#e45756",
}

JOBTYPE_ICONS = {
    "field-tech": "wrench",
    "lab/analyst": "flask",
    "architectural-historian": "landmark",
    "pm/pi": "briefcase",
}


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def app_mode() -> str:
    """Return 'demo' or 'real'."""
    if _truthy(os.getenv("DEMO_MODE")):
        return "demo"
    if (DEMO_DATA_DIR / "jobs.csv").exists():
        return "demo"
    return "real"


def processed_dir() -> Path:
    return DEMO_DATA_DIR if app_mode() == "demo" else REAL_DATA_DIR


def reports_dir() -> Path:
    if app_mode() == "demo":
        return BASE / "demo" / "reports"
    return BASE / "reports"


def _coerce_skills(value) -> list[str]:
    """Safely parse skills from various formats without code execution risk."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        # Try JSON parsing first (safe, no code execution)
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except (json.JSONDecodeError, TypeError):
                pass
        tokens = re.split(r"[;|,]", raw)
        return [t.strip(" []'\"") for t in tokens if t.strip(" []'\"")]
    return []


@st.cache_data(show_spinner=False)
def load_jobs(proc_dir: Path) -> pd.DataFrame:
    fp = proc_dir / "jobs.csv"
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
def load_analysis(proc_dir: Path) -> dict:
    fp = proc_dir / "analysis.json"
    if not fp.exists():
        return {}
    try:
        return json.loads(fp.read_text(encoding="utf-8"))
    except Exception:
        return {}


@st.cache_data(show_spinner=False)
def load_insights(proc_dir: Path) -> str:
    fp = proc_dir / "insights.md"
    if not fp.exists():
        return ""
    try:
        return fp.read_text(encoding="utf-8")
    except Exception:
        return ""


@st.cache_data(show_spinner=False)
def load_us_states_geojson():
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

        # Sanitize all user-controlled content to prevent XSS
        title = html.escape(str(r.get("title", "")).strip())
        company = html.escape(str(r.get("company", "")).strip())
        city = html.escape(str(r.get("city", "")).strip()) if has_city_state else ""
        state = html.escape(str(r.get("state", "")).strip()) if has_city_state else ""
        loc_txt = f"{city}, {state}" if has_city_state else ""
        skills = ", ".join(html.escape(s) for s in r.get("skills_list", [])[:5])

        line1 = f"<b>{title}</b>" if title else ""
        line2 = company if company else ""
        line3 = loc_txt if loc_txt else ""
        line4 = skills if skills else ""
        url = str(r.get("url", "")).strip() if has_url else ""

        # Validate URL scheme before including in HTML
        links = ""
        if has_url and url and url.startswith(("http://", "https://")):
            safe_url = html.escape(url)
            links = f'<br><a href="{safe_url}" target="_blank" rel="noopener noreferrer">Job posting</a>'
        popup_html = "<br>".join([x for x in (line1, line2, line3, line4) if x]) + links

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
            popup=folium.Popup(popup_html, max_width=320),
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
    _state_map()  # Validate data but don't store unused result

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

    # Choropleth color carries the count information; no additional markers needed

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


def _wizard_header(mode: str, proc_dir: Path):
    if mode == "demo":
        st.warning(
            "Demo mode is on. This uses demo data and simulates the pipeline run. "
            "It does not scrape, geocode, call Google Sheets, or call any LLM."
        )
    else:
        st.info("Real mode is on. This reads from data/processed and can run the pipeline if enabled.")

    jobs_fp = proc_dir / "jobs.csv"
    analysis_fp = proc_dir / "analysis.json"
    insights_fp = proc_dir / "insights.md"
    cols = st.columns(3)
    with cols[0]:
        st.caption("Jobs data")
        st.write("Found" if jobs_fp.exists() else "Not found")
    with cols[1]:
        st.caption("Analysis")
        st.write("Found" if analysis_fp.exists() else "Not found")
    with cols[2]:
        st.caption("Insights")
        st.write("Found" if insights_fp.exists() else "Not found")


def _wizard_stepper(steps: list[str]) -> int:
    if "wizard_step" not in st.session_state:
        st.session_state["wizard_step"] = 0

    c1, c2 = st.columns([3, 1])
    with c1:
        step = st.radio(
            "Step",
            options=list(range(len(steps))),
            format_func=lambda i: steps[i],
            index=int(st.session_state["wizard_step"]),
            horizontal=True,
            label_visibility="collapsed",
        )
        st.session_state["wizard_step"] = int(step)
    with c2:
        st.caption(f"{st.session_state['wizard_step'] + 1} of {len(steps)}")
    return int(st.session_state["wizard_step"])


def _render_ingest_step(mode: str):
    st.subheader("Ingest")
    st.write("Upload PDF reports. In real mode they go into the reports folder.")
    if mode == "demo":
        st.write("In demo mode uploads are accepted so you can see the flow, but they are not processed.")

    rpt_dir = reports_dir()
    rpt_dir.mkdir(parents=True, exist_ok=True)

    uploaded = st.file_uploader("Upload PDF reports", type=["pdf"], accept_multiple_files=True)
    saved = 0
    if uploaded:
        for f in uploaded:
            try:
                out = rpt_dir / f.name
                out.write_bytes(f.getbuffer())
                saved += 1
            except Exception:
                continue
        if saved:
            st.success(f"Saved {saved} file(s).")

    pdfs = sorted([p.name for p in rpt_dir.glob("*.pdf")])
    if not pdfs:
        st.caption("No PDFs found yet.")
        return
    st.caption("PDFs available")
    st.write("\n".join([f"- {name}" for name in pdfs[:30]]))
    if len(pdfs) > 30:
        st.caption(f"And {len(pdfs) - 30} more.")


def _render_config_step(mode: str):
    st.subheader("Configure")
    st.write("Choose what to run. Defaults match the full pipeline.")
    if mode == "demo":
        st.write("These settings are shown for demonstration. The demo run is simulated.")

    defaults = {
        "PIPELINE_SCRAPE": True,
        "PIPELINE_REPORTS": True,
        "PIPELINE_NLP": True,
        "PIPELINE_SENTIMENT": True,
        "PIPELINE_GEOCODE": True,
        "USE_SQLITE": _truthy(os.getenv("USE_SQLITE", "true")),
        "USE_SHEETS": _truthy(os.getenv("USE_SHEETS", "false")),
        "USE_LLM": _truthy(os.getenv("USE_LLM", "false")),
    }

    if "wizard_config" not in st.session_state:
        st.session_state["wizard_config"] = defaults

    cfg = st.session_state["wizard_config"]
    cfg["PIPELINE_SCRAPE"] = st.checkbox("Scrape job boards", value=bool(cfg.get("PIPELINE_SCRAPE", True)))
    cfg["PIPELINE_REPORTS"] = st.checkbox("Parse PDFs in reports folder", value=bool(cfg.get("PIPELINE_REPORTS", True)))
    cfg["PIPELINE_NLP"] = st.checkbox("Run NLP and skills extraction", value=bool(cfg.get("PIPELINE_NLP", True)))
    cfg["PIPELINE_SENTIMENT"] = st.checkbox("Add sentiment", value=bool(cfg.get("PIPELINE_SENTIMENT", True)))
    cfg["PIPELINE_GEOCODE"] = st.checkbox("Geocode locations", value=bool(cfg.get("PIPELINE_GEOCODE", True)))
    cfg["USE_SQLITE"] = st.checkbox("Persist to SQLite", value=bool(cfg.get("USE_SQLITE", True)))
    cfg["USE_SHEETS"] = st.checkbox("Sync to Google Sheets", value=bool(cfg.get("USE_SHEETS", False)))
    cfg["USE_LLM"] = st.checkbox("Include LLM brief", value=bool(cfg.get("USE_LLM", False)))

    st.caption("LLM provider")
    provider = os.getenv("LLM_PROVIDER", "openai")
    cfg["LLM_PROVIDER"] = st.selectbox(
        "Provider",
        options=["openai", "openai_compat", "ollama", "hf_inference"],
        index=["openai", "openai_compat", "ollama", "hf_inference"].index(provider)
        if provider in {"openai", "openai_compat", "ollama", "hf_inference"}
        else 0,
        disabled=mode == "demo",
    )
    cfg["LLM_MODEL"] = st.text_input("Model", value=os.getenv("LLM_MODEL", "gpt-4o-mini"), disabled=mode == "demo")
    cfg["LLM_BASE_URL"] = st.text_input(
        "OpenAI compatible base URL",
        value=os.getenv("LLM_BASE_URL", ""),
        disabled=mode == "demo",
    )
    cfg["OLLAMA_BASE_URL"] = st.text_input(
        "Ollama base URL",
        value=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        disabled=mode == "demo",
    )
    cfg["HF_MODEL"] = st.text_input("Hugging Face model", value=os.getenv("HF_MODEL", ""), disabled=mode == "demo")

    st.session_state["wizard_config"] = cfg


def _simulate_run():
    steps = [
        "Scrape jobs",
        "Clean and dedupe",
        "Parse PDFs",
        "NLP and skills",
        "Sentiment",
        "Geocode",
        "Analysis",
        "Insights",
        "Save outputs",
    ]
    bar = st.progress(0)
    status = st.empty()
    for i, name in enumerate(steps, start=1):
        status.write(f"Simulating. {name}")
        bar.progress(int(i / len(steps) * 100))
        time.sleep(0.35)
    status.write("Simulation complete. Loading demo outputs.")


def _render_run_step(mode: str):
    st.subheader("Run")

    if mode == "demo":
        st.write("This is a simulation. It shows the same steps as a real run.")
        if st.button("Run pipeline", type="primary"):
            _simulate_run()
            st.cache_data.clear()
            st.success("Done. Go to Results.")
        return

    if not allow_pipeline_run():
        st.warning(
            "Running the pipeline from the app is disabled. "
            "Set ALLOW_PIPELINE_RUN=true in your .env file if you want to run from the wizard."
        )
        return

    if not st.button("Run pipeline", type="primary"):
        st.write("When you run, outputs will be written into data/processed.")
        return

    ok, msg = acquire_lock(BASE)
    if not ok:
        st.warning(msg)
        return

    try:
        cfg = dict(st.session_state.get("wizard_config", {}))
        env_overrides = {
            "PIPELINE_SCRAPE": str(bool(cfg.get("PIPELINE_SCRAPE", True))).lower(),
            "PIPELINE_REPORTS": str(bool(cfg.get("PIPELINE_REPORTS", True))).lower(),
            "PIPELINE_NLP": str(bool(cfg.get("PIPELINE_NLP", True))).lower(),
            "PIPELINE_SENTIMENT": str(bool(cfg.get("PIPELINE_SENTIMENT", True))).lower(),
            "PIPELINE_GEOCODE": str(bool(cfg.get("PIPELINE_GEOCODE", True))).lower(),
            "USE_SQLITE": str(bool(cfg.get("USE_SQLITE", True))).lower(),
            "USE_SHEETS": str(bool(cfg.get("USE_SHEETS", False))).lower(),
            "USE_LLM": str(bool(cfg.get("USE_LLM", False))).lower(),
            "LLM_PROVIDER": str(cfg.get("LLM_PROVIDER", os.getenv("LLM_PROVIDER", "openai"))),
            "LLM_MODEL": str(cfg.get("LLM_MODEL", os.getenv("LLM_MODEL", "gpt-4o-mini"))),
            "LLM_BASE_URL": str(cfg.get("LLM_BASE_URL", os.getenv("LLM_BASE_URL", ""))),
            "OLLAMA_BASE_URL": str(cfg.get("OLLAMA_BASE_URL", os.getenv("OLLAMA_BASE_URL", ""))),
            "HF_MODEL": str(cfg.get("HF_MODEL", os.getenv("HF_MODEL", ""))),
            "HF_INFERENCE_URL": os.getenv("HF_INFERENCE_URL", ""),
        }

        st.write("Running the pipeline. This can take a while.")
        output_box = st.empty()

        result = run_pipeline(BASE, env_overrides=env_overrides)
        output_box.code(result.output[-12000:] if result.output else "", language="text")

        st.cache_data.clear()
        if result.returncode == 0:
            st.success("Pipeline finished.")
        else:
            st.error(f"Pipeline exited with code {result.returncode}.")
    finally:
        release_lock(BASE)


def _render_results_step(proc_dir: Path):
    st.subheader("Results")
    df = load_jobs(proc_dir)
    if df.empty:
        st.info("No data yet. Run the pipeline to generate jobs.csv.")
        return

    analysis = load_analysis(proc_dir)
    if analysis:
        cols = st.columns(4)
        with cols[0]:
            st.metric("Jobs", f"{int(analysis.get('num_jobs', len(df))):,}")
        with cols[1]:
            st.metric("Employers", f"{int(analysis.get('unique_employers', 0)):,}")
        with cols[2]:
            st.metric("Geocoded", f"{int(analysis.get('geocoded', 0)):,}")
        with cols[3]:
            st.caption("Last run")
            st.write(str(analysis.get("run_timestamp", ""))[:19])
        st.divider()

    kpi_cards(df)
    st.divider()
    filtered, selected_skills, mode = sidebar_filters(df)

    tabs = st.tabs(["Map", "Tables", "Insights"])
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
        show_cols = [
            c
            for c in ["date_posted", "title", "company", "city", "state", "seniority", "job_type", "skills", "url"]
            if c in filtered.columns
        ]
        st.dataframe(filtered[show_cols].reset_index(drop=True), use_container_width=True)
        st.download_button(
            "Download filtered CSV",
            filtered.to_csv(index=False).encode("utf-8"),
            "filtered_jobs.csv",
            "text/csv",
        )

    with tabs[2]:
        insights = load_insights(proc_dir)
        if not insights:
            st.info("No insights.md yet.")
        else:
            st.markdown(insights)


def main():
    st.title("CHARM Market Intelligence")

    mode = app_mode()
    proc_dir = processed_dir()

    tabs = st.tabs(["Wizard", "Explore"])
    with tabs[0]:
        _wizard_header(mode, proc_dir)
        st.divider()
        steps = ["Start", "Ingest", "Configure", "Run", "Results"]
        idx = _wizard_stepper(steps)

        if idx == 0:
            st.subheader("Start")
            st.write("This wizard guides you through ingestion, processing, and visualization.")
            st.write("Use the next steps to upload reports, configure a run, and view results.")
        elif idx == 1:
            _render_ingest_step(mode)
        elif idx == 2:
            _render_config_step(mode)
        elif idx == 3:
            _render_run_step(mode)
        else:
            _render_results_step(proc_dir)

    with tabs[1]:
        st.caption("Explore the current dataset.")
        _render_results_step(proc_dir)


if __name__ == "__main__":
    main()
