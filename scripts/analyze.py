import os
import json
import itertools
from collections import Counter

import pandas as pd
from wordcloud import WordCloud


def _ensure_skill_lists(series: pd.Series):
    lists = []
    for entry in series.dropna():
        if isinstance(entry, list):
            lists.append(entry)
        elif isinstance(entry, str):
            entry = entry.strip()
            if not entry:
                continue
            if entry.startswith("["):
                try:
                    parsed = json.loads(entry)
                    if isinstance(parsed, list):
                        lists.append([str(v) for v in parsed])
                        continue
                except Exception:
                    pass
            lists.append([token.strip() for token in entry.split(";") if token.strip()])
    return lists


def analyze_market(jobs_df: pd.DataFrame, reports_df: pd.DataFrame | None) -> pd.Series:
    jobs_df = jobs_df if jobs_df is not None else pd.DataFrame()
    out = {
        "num_jobs": int(len(jobs_df)),
        "unique_employers": 0,
        "top_skills": [],
        "geocoded": 0,
        "report_skills": [],
        "top_employers": [],
        "run_timestamp": pd.Timestamp.utcnow().isoformat(),
    }

    if not jobs_df.empty:
        if "company" in jobs_df.columns:
            out["unique_employers"] = int(jobs_df["company"].nunique())
            top_employers = Counter(
                [c.strip() for c in jobs_df["company"].fillna("").tolist() if c.strip()]
            ).most_common(20)
            out["top_employers"] = top_employers
        if "skills" in jobs_df.columns:
            skills_series = _ensure_skill_lists(jobs_df["skills"])
            if skills_series:
                all_sk = list(itertools.chain.from_iterable(skills_series))
                out["top_skills"] = Counter(all_sk).most_common(30)
        if {"lat", "lon"}.issubset(jobs_df.columns):
            out["geocoded"] = int(jobs_df[["lat", "lon"]].dropna().shape[0])

        if os.getenv("USE_CLUSTERING", "false").lower() == "true":
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.cluster import KMeans

                vec = TfidfVectorizer(max_features=2000, ngram_range=(1, 2), min_df=2)
                X = vec.fit_transform(jobs_df["description"].fillna(""))
                km = KMeans(n_clusters=3, n_init=10, random_state=42)
                labels = km.fit_predict(X)
                out["cluster_counts"] = Counter(labels)
            except Exception as exc:
                out["cluster_error"] = str(exc)

    if reports_df is not None and not reports_df.empty and "skills" in reports_df.columns:
        rep_sk = list(itertools.chain.from_iterable(reports_df["skills"].dropna().tolist()))
        out["report_skills"] = Counter(rep_sk).most_common(30)

    return pd.Series(out)

def save_wordcloud(jobs_df: pd.DataFrame, out_path):
    texts = jobs_df["description"].fillna("").tolist()
    joined = " ".join(texts)
    if not joined.strip(): return
    wc = WordCloud(width=1400, height=800, background_color="white").generate(joined)
    wc.to_file(str(out_path))
