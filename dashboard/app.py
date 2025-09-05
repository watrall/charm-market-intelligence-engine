import json, pandas as pd, streamlit as st
from pathlib import Path
import plotly.express as px
from streamlit_folium import st_folium
import folium
from datetime import datetime
from scripts.nlp_entities import get_nlp  # for lazy model check

# ---------- Page setup ----------
st.set_page_config(page_title="CHARM Dashboard", layout="wide")
base = Path(__file__).resolve().parents[1]
proc = base / "data" / "processed"
jobs_p = proc / "jobs.csv"
analysis_p = proc / "analysis.json"
insights_p = proc / "insights.md"
wc_p = proc / "wordcloud.png"

# ---------- Global CSS (minimalist look) ----------
st.markdown('''
    <style>
        /* Hide Streamlit default chrome for a cleaner look */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}

        /* Typography & spacing */
        .app-title { font-size: 1.6rem; font-weight: 700; margin: 0.25rem 0 1rem; }
        .subtle { color: #6B7280; font-size: 0.9rem; }

        /* Card components */
        .card {
            background: var(--secondaryBackgroundColor, #F6F8FA);
            border: 1px solid #E5E7EB;
            border-radius: 14px;
            padding: 16px 18px;
        }
        .kpi-title { font-size: 0.85rem; color: #6B7280; margin-bottom: 0.3rem; }
        .kpi-value { font-size: 1.4rem; font-weight: 700; margin: 0; }

        /* Section spacing */
        .section { margin-top: 1.2rem; }
    </style>
''', unsafe_allow_html=True)

# ---------- Sidebar filters (clean UX) ----------
with st.sidebar:
    st.markdown("### Filters")
    try:
        _ = get_nlp()
    except Exception as e:
        st.warning(f"spaCy model missing. Run: `python -m spacy download en_core_web_sm`\n({e})")

    if not jobs_p.exists():
        st.info("Run the pipeline to populate data.")
    else:
        jobs = pd.read_csv(jobs_p)
        companies = ["(All)"] + sorted([x for x in jobs['company'].dropna().unique() if x])
        company = st.selectbox("Employer", companies, index=0)
        # Build unique skills list
        skill_set = sorted(set(sum([[s.strip() for s in (x or '').split(',') if s.strip()] for x in jobs['skills'].fillna('')], [])))
        skill = st.selectbox("Skill", ["(All)"] + skill_set, index=0)
        st.caption("Note: Use sidebar filters to slice charts, map, and table.")

st.markdown('<div class="app-title">CHARM — CRM/Heritage Market Dashboard</div>', unsafe_allow_html=True)
if jobs_p.exists():
    ts = datetime.fromtimestamp(jobs_p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    st.markdown(f'<div class="subtle">Last updated: {ts}</div>', unsafe_allow_html=True)

if not jobs_p.exists():
    st.stop()

jobs = pd.read_csv(jobs_p)

# Apply filters
jv = jobs.copy()
if company != "(All)":
    jv = jv[jv["company"] == company]
if skill != "(All)":
    jv = jv[jv["skills"].fillna("").str.contains(fr"(^|,){skill}(,|$)")]

# ---------- Top toolbar (download actions) ----------
cta1, cta2, cta3 = st.columns([1,1,6])
with cta1:
    st.download_button("Download filtered CSV", data=jv.to_csv(index=False), file_name="charm_jobs_filtered.csv")
with cta2:
    if analysis_p.exists():
        st.download_button("Download analysis.json", data=analysis_p.read_bytes(), file_name="analysis.json")

# ---------- KPI cards ----------
k1, k2, k3 = st.columns(3)
k1.markdown(f'<div class="card"><div class="kpi-title">Total Postings</div><div class="kpi-value">{len(jv)}</div></div>', unsafe_allow_html=True)
k2.markdown(f'<div class="card"><div class="kpi-title">Unique Employers</div><div class="kpi-value">{jv["company"].nunique() if "company" in jv else 0}</div></div>', unsafe_allow_html=True)
k3.markdown(f'<div class="card"><div class="kpi-title">Geocoded</div><div class="kpi-value">{jv[["lat","lon"]].dropna().shape[0] if {"lat","lon"}.issubset(jv.columns) else 0}</div></div>', unsafe_allow_html=True)

# ---------- Top Skills (Plotly minimal) ----------
st.markdown('<div class="section"></div>', unsafe_allow_html=True)
st.subheader("Top Skills")
exploded = []
for s in jv["skills"].fillna("").tolist():
    exploded.extend([x.strip() for x in s.split(",") if x.strip()])
top = pd.Series(exploded).value_counts().head(20).reset_index()
top.columns = ["skill","count"]
if not top.empty:
    fig = px.bar(top.sort_values("count"), x="count", y="skill", orientation="h", template="plotly_white")
    fig.update_layout(margin=dict(l=8,r=8,t=6,b=6), height=420, xaxis_title="", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No skills to show for current filters.")

# ---------- Map (Folium, clean) ----------
st.subheader("Job Locations")
if {'lat','lon'}.issubset(jv.columns) and not jv.dropna(subset=['lat','lon']).empty:
    center = [float(jv['lat'].mean()), float(jv['lon'].mean())]
    m = folium.Map(location=center, zoom_start=4, control_scale=True, tiles='OpenStreetMap')
    from folium.plugins import HeatMap, MarkerCluster
    HeatMap(jv[['lat','lon']].values.tolist(), radius=15, blur=20).add_to(m)
    mc = MarkerCluster().add_to(m)
    for _, r in jv.dropna(subset=['lat','lon']).iterrows():
        popup = folium.Popup(f"<b>{r.get('title','')}</b><br/>{r.get('company','')}", max_width=300)
        folium.Marker([r['lat'], r['lon']], popup=popup).add_to(mc)
    st_folium(m, height=520, width=None)
else:
    st.info("No geocoded rows for current filters.")

# ---------- Insights & Word Cloud (minimal text block) ----------
c_left, c_right = st.columns([2,1])
with c_left:
    st.subheader("Insights")
    if insights_p.exists():
        st.markdown(insights_p.read_text())
    else:
        st.caption("Run the pipeline to generate insights.md.")

with c_right:
    st.subheader("Word Cloud")
    if wc_p.exists():
        st.image(str(wc_p))
    else:
        st.caption("Run the pipeline to generate wordcloud.png.")

# ---------- Raw Table (sticky to bottom) ----------
st.subheader("Raw Table")
st.dataframe(jv[['source','title','company','location','date_posted','job_url','skills','sentiment','salary_min','salary_max','currency']], use_container_width=True)
