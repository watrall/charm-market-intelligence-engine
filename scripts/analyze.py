import os, itertools, pandas as pd
from collections import Counter
from wordcloud import WordCloud

def analyze_market(jobs_df: pd.DataFrame, reports_df: pd.DataFrame | None) -> pd.Series:
    out = {}
    if jobs_df is not None and not jobs_df.empty:
        out["num_jobs"] = len(jobs_df)
        out["unique_employers"] = int(jobs_df['company'].nunique())
        all_sk = list(itertools.chain.from_iterable(jobs_df["skills"].dropna().tolist()))
        out["top_skills"] = Counter(all_sk).most_common(30)
        out["geocoded"] = int(jobs_df[["lat","lon"]].dropna().shape[0])
        if os.getenv("USE_CLUSTERING","false").lower() == "true":
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.cluster import KMeans
                vec = TfidfVectorizer(max_features=2000, ngram_range=(1,2), min_df=2)
                X = vec.fit_transform(jobs_df["description"].fillna(""))
                km = KMeans(n_clusters=3, n_init=10, random_state=42)
                labels = km.fit_predict(X)
                jobs_df["cluster"] = labels
                out["cluster_counts"] = Counter(labels)
            except Exception as e:
                out["cluster_error"] = str(e)
    if reports_df is not None and not reports_df.empty:
        rep_sk = list(itertools.chain.from_iterable(reports_df["skills"].dropna().tolist()))
        out["report_skills"] = Counter(rep_sk).most_common(30)
    return pd.Series(out)

def save_wordcloud(jobs_df: pd.DataFrame, out_path):
    texts = jobs_df["description"].fillna("").tolist()
    joined = " ".join(texts)
    if not joined.strip(): return
    wc = WordCloud(width=1400, height=800, background_color="white").generate(joined)
    wc.to_file(str(out_path))
