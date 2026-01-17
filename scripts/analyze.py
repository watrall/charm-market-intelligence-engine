import itertools
import json
import logging
import os
import random
from collections import Counter

import pandas as pd
from wordcloud import WordCloud

logger = logging.getLogger(__name__)

# Schema version for tracking output format changes
SCHEMA_VERSION = "1.0"

# Determinism: seed for reproducible results
def _get_seed() -> int:
    """Get seed from environment or use default for reproducibility."""
    return int(os.getenv("CHARM_SEED", "42"))


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
    """Analyze job market data and return summary statistics.
    
    Output is deterministic given the same input data (except for run_timestamp).
    """
    jobs_df = jobs_df if jobs_df is not None else pd.DataFrame()
    
    # Ordered dict ensures consistent JSON key ordering
    out = {
        "schema_version": SCHEMA_VERSION,
        "num_jobs": int(len(jobs_df)),
        "unique_employers": 0,
        "geocoded": 0,
        "top_skills": [],
        "top_employers": [],
        "report_skills": [],
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
                from sklearn.cluster import KMeans
                from sklearn.feature_extraction.text import TfidfVectorizer

                vec = TfidfVectorizer(max_features=2000, ngram_range=(1, 2), min_df=2)
                X = vec.fit_transform(jobs_df["description"].fillna(""))
                km = KMeans(n_clusters=3, n_init=10, random_state=42)
                labels = km.fit_predict(X)
                out["cluster_counts"] = Counter(labels)
            except ImportError:
                logger.warning("scikit-learn not installed; skipping clustering")
            except ValueError as exc:
                out["cluster_error"] = str(exc)

    if reports_df is not None and not reports_df.empty and "skills" in reports_df.columns:
        rep_sk = list(itertools.chain.from_iterable(reports_df["skills"].dropna().tolist()))
        out["report_skills"] = Counter(rep_sk).most_common(30)

    return pd.Series(out)

def save_wordcloud(jobs_df: pd.DataFrame, out_path):
    """Generate word cloud from job descriptions.
    
    Uses seeded random for reproducible layout.
    """
    texts = jobs_df["description"].fillna("").tolist()
    joined = " ".join(texts)
    if not joined.strip():
        return
    
    seed = _get_seed()
    random.seed(seed)
    
    wc = WordCloud(
        width=1400,
        height=800,
        background_color="white",
        random_state=seed,
    ).generate(joined)
    wc.to_file(str(out_path))
