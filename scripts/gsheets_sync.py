from __future__ import annotations

import os
import re
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _authorize():
    service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not (service_account and sheet_id):
        raise RuntimeError("Sheets env not configured")

    # Validate service account path to prevent path traversal
    sa_path = Path(service_account).resolve()
    if not sa_path.is_file() or not str(sa_path).endswith(".json"):
        raise RuntimeError(f"Invalid service account file: {service_account}")

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(sa_path), scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)


def connect_sheet():
    return _authorize()


def ensure_jobs_worksheet():
    sheet = connect_sheet()
    ws = _get_or_create_ws(
        sheet,
        os.getenv("GOOGLE_SHEET_WORKSHEET", "jobs"),
        ["source", "title", "company", "location", "date_posted", "job_url", "skills", "lat", "lon", "sentiment"],
    )
    return sheet, ws


def ensure_reports_worksheet():
    sheet = connect_sheet()
    ws = _get_or_create_ws(
        sheet,
        os.getenv("GOOGLE_SHEET_WORKSHEET_REPORTS", "reports"),
        ["report_name", "word_count", "skills"],
        cols=10,
    )
    return sheet, ws


def _get_or_create_ws(sheet, title: str, header: list[str], rows: int = 2000, cols: int = 20):
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        ws.append_row(header)
        return ws


def _normalize_skills(value) -> str:
    if isinstance(value, list):
        entries = value
    elif isinstance(value, str):
        entries = [s.strip() for s in re.split(r"[;,|]", value) if s.strip()]
    else:
        entries = []
    return ",".join(entries)


def _load_cached_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def _persist_cached_ids(path: Path, values: set[str]):
    path.write_text("\n".join(sorted(values)), encoding="utf-8")


def _chunked(iterable, size=500):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def sync_jobs_to_google_sheets(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    sheet = _authorize()
    ws = _get_or_create_ws(
        sheet,
        os.getenv("GOOGLE_SHEET_WORKSHEET", "jobs"),
        ["source", "title", "company", "location", "date_posted", "job_url", "skills", "lat", "lon", "sentiment"],
    )

    cache_path = CACHE_DIR / "gsheets_jobs_urls.txt"
    existing_urls = _load_cached_ids(cache_path)
    if not existing_urls:
        existing_urls = set(filter(None, ws.col_values(6)[1:]))
        _persist_cached_ids(cache_path, existing_urls)
    new_rows = []
    for _, record in df.iterrows():
        url = str(record.get("job_url", "")).strip()
        if not url or url in existing_urls:
            continue
        new_rows.append([
            record.get("source", ""),
            record.get("title", ""),
            record.get("company", ""),
            record.get("location", ""),
            record.get("date_posted", ""),
            url,
            _normalize_skills(record.get("skills")),
            record.get("lat", ""),
            record.get("lon", ""),
            record.get("sentiment", ""),
        ])

    appended = 0
    for chunk in _chunked(new_rows, size=500):
        ws.append_rows(chunk, value_input_option="RAW")
        appended += len(chunk)
    if appended:
        new_ids = {row[5] for row in new_rows if len(row) >= 6 and row[5]}
        existing_urls.update(new_ids)
        _persist_cached_ids(cache_path, existing_urls)
    return appended


def sync_reports_to_google_sheets(reports_df: pd.DataFrame) -> int:
    if reports_df is None or reports_df.empty:
        return 0

    sheet = _authorize()
    ws = _get_or_create_ws(
        sheet,
        os.getenv("GOOGLE_SHEET_WORKSHEET_REPORTS", "reports"),
        ["report_name", "word_count", "skills"],
        cols=10,
    )

    cache_path = CACHE_DIR / "gsheets_report_names.txt"
    existing = _load_cached_ids(cache_path)
    if not existing:
        existing = set(filter(None, ws.col_values(1)[1:]))
        _persist_cached_ids(cache_path, existing)
    new_rows = []
    for _, record in reports_df.iterrows():
        name = str(record.get("report_name", "")).strip()
        if not name or name in existing:
            continue
        text = str(record.get("text", ""))
        word_count = record.get("word_count")
        if pd.isna(word_count) or not word_count:
            word_count = len(text.split())
        new_rows.append([
            name,
            int(word_count),
            _normalize_skills(record.get("skills")),
        ])

    appended = 0
    for chunk in _chunked(new_rows, size=500):
        ws.append_rows(chunk, value_input_option="RAW")
        appended += len(chunk)
    if appended:
        new_ids = {row[0] for row in new_rows if row and row[0]}
        existing.update(new_ids)
        _persist_cached_ids(cache_path, existing)
    return appended
