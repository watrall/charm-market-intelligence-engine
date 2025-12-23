import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from scripts.analyze import analyze_market, save_wordcloud
from scripts.data_cleaning import clean_and_dedupe
from scripts.db import get_conn, init_db, upsert_jobs, upsert_reports
from scripts.geocode import geocode_locations
from scripts.insights import generate_insights
from scripts.nlp_entities import nlp_enrich
from scripts.parse_reports import parse_all_reports
from scripts.scrape_jobs import scrape_sources
from scripts.sentiment_salience import add_sentiment_and_terms


def enrich_report_metadata(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    df = df.copy()
    df["word_count"] = df["text"].fillna("").astype(str).apply(lambda t: len(t.split()))

    def summarize_entities(row):
        entities = []
        for collection in (row.get("orgs") or [], row.get("places") or []):
            for value in collection:
                cleaned = str(value).strip()
                if cleaned and cleaned not in entities:
                    entities.append(cleaned)
        return ", ".join(entities[:10])

    df["top_entities"] = df.apply(summarize_entities, axis=1)
    return df


def _skills_to_string(value):
    if isinstance(value, list):
        return ";".join(value)
    if isinstance(value, str):
        return value
    return ""


def _skills_to_json(value):
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str) and value.strip():
        parts = [part.strip() for part in value.split(";") if part.strip()]
        return json.dumps(parts, ensure_ascii=False)
    return "[]"


def ensure_dirs(base: Path):
    (base / "data").mkdir(exist_ok=True)
    (base / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (base / "data" / "cache").mkdir(parents=True, exist_ok=True)
    (base / "reports").mkdir(exist_ok=True)

def main():
    load_dotenv()
    base = Path(__file__).resolve().parents[1]
    ensure_dirs(base)

    jobs_df = scrape_sources()
    if jobs_df is None or jobs_df.empty:
        print("No jobs scraped; continuing with empty dataset.")
        jobs_df = pd.DataFrame(columns=[
            "source", "title", "company", "location", "date_posted", "job_url", "description"
        ])

    jobs_df = clean_and_dedupe(jobs_df)
    reports_df = parse_all_reports(base / "reports")

    jobs_df = nlp_enrich(jobs_df, is_job=True)
    if reports_df is not None and not reports_df.empty:
        reports_df = nlp_enrich(reports_df, is_job=False)
        reports_df = enrich_report_metadata(reports_df)

    jobs_df = add_sentiment_and_terms(jobs_df, text_col="description")
    jobs_df = geocode_locations(jobs_df)

    proc = base / "data" / "processed"
    _save_processed_data(jobs_df, reports_df, proc)

    analysis = analyze_market(jobs_df, reports_df)
    with open(proc / "analysis.json", "w", encoding="utf-8") as f:
        f.write(analysis.to_json(indent=2))
    save_wordcloud(jobs_df, out_path=proc / "wordcloud.png")

    insights = generate_insights(jobs_df, reports_df, analysis)
    with open(proc / "insights.md", "w", encoding="utf-8") as f:
        f.write(insights)

    _persist_to_sqlite(base, jobs_df, reports_df)
    _sync_to_google_sheets(jobs_df, reports_df)

    print("CHARM pipeline complete.")


def _save_processed_data(jobs_df, reports_df, proc):
    jobs_to_save = jobs_df.copy()
    jobs_to_save["skills_list"] = jobs_to_save["skills"].apply(_skills_to_json)
    jobs_to_save["skills"] = jobs_to_save["skills"].apply(_skills_to_string)
    jobs_to_save.to_csv(proc / "jobs.csv", index=False)
    if reports_df is not None and not reports_df.empty:
        reports_df.to_csv(proc / "reports.csv", index=False)


def _persist_to_sqlite(base, jobs_df, reports_df):
    if os.getenv("USE_SQLITE", "true").lower() != "true":
        print("SQLite disabled via USE_SQLITE=false")
        return

    db_path = base / "data" / "charm.db"
    conn = get_conn(db_path)
    init_db(conn)
    upsert_jobs(conn, jobs_df)
    if reports_df is not None and not reports_df.empty:
        upsert_reports(conn, reports_df)
    print(f"SQLite persisted to {db_path}")


def _sync_to_google_sheets(jobs_df, reports_df):
    if os.getenv("USE_SHEETS", "true").lower() != "true":
        return

    try:
        from scripts.gsheets_sync import sync_jobs_to_google_sheets, sync_reports_to_google_sheets

        job_rows = sync_jobs_to_google_sheets(jobs_df)
        print(f"Google Sheets: appended {job_rows} new job rows.")

        if reports_df is not None and not reports_df.empty:
            report_rows = sync_reports_to_google_sheets(reports_df)
            print(f"Google Sheets: appended {report_rows} report rows.")
    except (ImportError, RuntimeError) as e:
        print(f"Sheets sync skipped: {e}")

if __name__ == "__main__":
    main()
