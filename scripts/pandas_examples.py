from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
PROC = BASE / "data" / "processed"
DB   = BASE / "data" / "charm.db"

def load_jobs_from_csv() -> pd.DataFrame:
    p = PROC / "jobs.csv"
    if not p.exists():
        raise FileNotFoundError("Run the pipeline first to create data/processed/jobs.csv")
    return pd.read_csv(p)

def load_jobs_from_sqlite() -> pd.DataFrame:
    if not DB.exists():
        raise FileNotFoundError("SQLite DB not found. Run the pipeline once with USE_SQLITE=true.")
    with sqlite3.connect(str(DB)) as conn:
        return pd.read_sql_query("""
            SELECT j.*, GROUP_CONCAT(s.skill, ',') AS skills_norm
            FROM jobs j
            LEFT JOIN job_skills s ON s.job_id = j.id
            GROUP BY j.id
        """, conn)

def _explode_skills(df: pd.DataFrame, col: str = "skills") -> pd.DataFrame:
    s = df[col].fillna("")
    if "skills_norm" in df.columns and df["skills_norm"].notna().any():
        s = df["skills_norm"].fillna("")
    return (df.assign(skill_list=s.str.split(","))
              .explode("skill_list")
              .assign(skill=lambda d: d["skill_list"].str.strip())
              .drop(columns=["skill_list"])
              .query("skill != ''"))

def top_skills(df: pd.DataFrame, n=25) -> pd.DataFrame:
    ex = _explode_skills(df)
    return (ex.groupby("skill", as_index=False)
             .size()
             .rename(columns={"size":"count"})
             .sort_values("count", ascending=False)
             .head(n))

def monthly_postings(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["date_posted"] = pd.to_datetime(d["date_posted"], errors="coerce")
    d["month"] = d["date_posted"].dt.to_period("M").astype(str)
    return (d.groupby("month", as_index=False)
             .size()
             .rename(columns={"size":"postings"})
             .sort_values("month"))

_STATE_RE = re.compile(r"\b([A-Z]{2})\b\s*$")
def jobs_by_state(df: pd.DataFrame) -> pd.DataFrame:
    def state_of(loc: str) -> str:
        if not isinstance(loc, str):
            return ""
        m = _STATE_RE.search(loc.strip())
        return m.group(1) if m else ""
    d = df.copy()
    d["state"] = d["location"].map(state_of)
    return (d.query("state != ''")
             .groupby("state", as_index=False)
             .size()
             .rename(columns={"size":"postings"})
             .sort_values("postings", ascending=False))

def salary_by_skill(df: pd.DataFrame, min_n=3) -> pd.DataFrame:
    ex = _explode_skills(df)
    ex["low"] = pd.to_numeric(ex["salary_min"], errors="coerce")
    ex["high"] = pd.to_numeric(ex["salary_max"], errors="coerce")
    g = (ex.groupby("skill").agg(n=("skill","size"),
                                 avg_low=("low","mean"),
                                 avg_high=("high","mean"))
             .reset_index())
    return g.query("n >= @min_n").sort_values("n", ascending=False)

def export_all():
    PROC.mkdir(parents=True, exist_ok=True)
    try:
        jobs = load_jobs_from_sqlite()
    except Exception:
        jobs = load_jobs_from_csv()

    top_skills(jobs).to_csv(PROC / "top_skills_pandas.csv", index=False)
    monthly_postings(jobs).to_csv(PROC / "monthly_postings.csv", index=False)
    jobs_by_state(jobs).to_csv(PROC / "jobs_by_state.csv", index=False)
    salary_by_skill(jobs).to_csv(PROC / "salary_by_skill.csv", index=False)
    print("Wrote: top_skills_pandas.csv, monthly_postings.csv, jobs_by_state.csv, salary_by_skill.csv")

if __name__ == "__main__":
    export_all()
