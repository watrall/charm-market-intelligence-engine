"""Google Sheets helpers for CHARM."""

from __future__ import annotations

import os
import re

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


def _authorize():
    service_account = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not (service_account and sheet_id):
        raise RuntimeError("Sheets env not configured")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(service_account, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)


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

    existing_urls = set(filter(None, ws.col_values(6)[1:]))
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

    existing = set(filter(None, ws.col_values(1)[1:]))
    new_rows = []
    for _, record in reports_df.iterrows():
        name = str(record.get("report_name", "")).strip()
        if not name or name in existing:
            continue
        text = str(record.get("text", ""))
        new_rows.append([
            name,
            len(text.split()),
            _normalize_skills(record.get("skills")),
        ])

    appended = 0
    for chunk in _chunked(new_rows, size=500):
        ws.append_rows(chunk, value_input_option="RAW")
        appended += len(chunk)
    return appended
