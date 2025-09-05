import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

from scripts.scrape_jobs import scrape_sources
from scripts.data_cleaning import clean_and_dedupe
from scripts.parse_reports import parse_all_reports
from scripts.nlp_entities import nlp_enrich
from scripts.sentiment_salience import add_sentiment_and_terms
from scripts.geocode import geocode_locations
from scripts.analyze import analyze_market, save_wordcloud
from scripts.insights import generate_insights
from scripts.db import get_conn, init_db, upsert_jobs, upsert_reports


def ensure_dirs(base: Path):
    (base / "data").mkdir(exist_ok=True)
    (base / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (base / "reports").mkdir(exist_ok=True)
    (base / "data").joinpath("geocache.csv").touch(exist_ok=True)

def main():
    load_dotenv()
    base = Path(__file__).resolve().parents[1]
    ensure_dirs(base)

    # 1) Scrape
    jobs_df = scrape_sources()
    if jobs_df.empty:
        print("No jobs scraped."); return

    # 2) Clean + Dedupe
    jobs_df = clean_and_dedupe(jobs_df)

    # 3) Parse reports (PDFs dropped into /reports)
    reports_df = parse_all_reports(base / "reports")

    # 4) NLP enrichment
    jobs_df = nlp_enrich(jobs_df, is_job=True)
    if reports_df is not None and not reports_df.empty:
        reports_df = nlp_enrich(reports_df, is_job=False)

    # 5) Sentiment
    jobs_df = add_sentiment_and_terms(jobs_df, text_col="description")

    # 6) Geocode
    jobs_df = geocode_locations(jobs_df)

    # 7) Save processed
    proc = base / "data" / "processed"
    jobs_df.to_csv(proc / "jobs.csv", index=False)
    if reports_df is not None and not reports_df.empty:
        reports_df.to_csv(proc / "reports.csv", index=False)

    # 8) Analysis + wordcloud
    analysis = analyze_market(jobs_df, reports_df)
    with open(proc / "analysis.json","w",encoding="utf-8") as f:
        f.write(analysis.to_json(indent=2))
    save_wordcloud(jobs_df, out_path=proc / "wordcloud.png")

    # 9) Insights (rules + optional LLM)
    insights = generate_insights(jobs_df, reports_df, analysis)
    with open(proc / "insights.md","w",encoding="utf-8") as f:
        f.write(insights)

    # 10) SQLite persistence (recommended)
    if os.getenv("USE_SQLITE","true").lower() == "true":
        db_path = base / "data" / "charm.db"
        conn = get_conn(db_path); init_db(conn)
        upsert_jobs(conn, jobs_df)
        if reports_df is not None and not reports_df.empty:
            upsert_reports(conn, reports_df)
        print(f"SQLite persisted to {db_path}")
    else:
        print("SQLite disabled via USE_SQLITE=false")

    # 11) Optional Google Sheets sync
    if os.getenv("USE_SHEETS","true").lower() == "true":
        try:
            from scripts.gsheets_sync import sync_to_google_sheets, sync_to_google_sheets_with_count, sync_reports_to_google_sheets
            cnt = 0
            try:
                cnt = sync_to_google_sheets_with_count(jobs_df)
            except Exception:
                sync_to_google_sheets(jobs_df)
            print(f"Google Sheets: appended {cnt} new job rows.")
                # sync reports (if any)
                if reports_df is not None and not reports_df.empty:
                    rcnt = sync_reports_to_google_sheets(reports_df)
                    print(f"Google Sheets: appended {rcnt} report rows.")
        except Exception as e:
            print(f"Sheets sync skipped: {e}")

    print("CHARM slim pipeline complete.")

if __name__ == "__main__":
    main()
