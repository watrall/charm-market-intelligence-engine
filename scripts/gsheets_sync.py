import os, gspread, pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

def _ws():
    sa = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    sid = os.getenv("GOOGLE_SHEET_ID")
    wsname = os.getenv("GOOGLE_SHEET_WORKSHEET","jobs")
    if not (sa and sid): raise RuntimeError("Sheets env not configured")
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(sa, scope)
    gc = gspread.authorize(creds); sh = gc.open_by_key(sid)
    try: return sh.worksheet(wsname)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=wsname, rows=2000, cols=20)
        ws.append_row(["source","title","company","location","date_posted","job_url","skills","lat","lon","sentiment"])
        return ws

def sync_to_google_sheets(df: pd.DataFrame):
    ws = _ws()
    existing_urls = set(ws.col_values(6)[1:])
    rows = []
    for _, r in df.iterrows():
        url = str(r.get("job_url",""))
        if not url or url in existing_urls: continue
        rows.append([
            r.get("source",""), r.get("title",""), r.get("company",""),
            r.get("location",""), r.get("date_posted",""), url,
            ",".join(r.get("skills",[]) or []), r.get("lat",""), r.get("lon",""),
            r.get("sentiment","")
        ])
    if rows:
        ws.append_rows(rows, value_input_option="RAW")

def sync_to_google_sheets_with_count(df: pd.DataFrame) -> int:
    ws = _ws()
    existing_urls = set(ws.col_values(6)[1:])
    rows = []
    for _, r in df.iterrows():
        url = str(r.get("job_url",""))
        if not url or url in existing_urls: continue
        rows.append([
            r.get("source",""), r.get("title",""), r.get("company",""),
            r.get("location",""), r.get("date_posted",""), url,
            ",".join(r.get("skills",[]) or []), r.get("lat",""), r.get("lon",""),
            r.get("sentiment","")
        ])
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


def _ws_reports():
    sa = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    sid = os.getenv("GOOGLE_SHEET_ID")
    wsname = os.getenv("GOOGLE_SHEET_WORKSHEET_REPORTS","reports")
    if not (sa and sid): raise RuntimeError("Sheets env not configured")
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(sa, scope)
    gc = gspread.authorize(creds); sh = gc.open_by_key(sid)
    try: return sh.worksheet(wsname)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=wsname, rows=2000, cols=10)
        ws.append_row(["report_name","word_count","skills"])
        return ws

def sync_reports_to_google_sheets(reports_df: pd.DataFrame) -> int:
    if reports_df is None or reports_df.empty: return 0
    ws = _ws_reports()
    names = set([n for n in ws.col_values(1)[1:]])
    appended = 0
    for _, r in reports_df.iterrows():
        name = str(r.get("report_name","")).strip()
        if not name or name in names: continue
        text = str(r.get("text",""))
        wc = len(text.split())
        skills = r.get("skills", [])
        if isinstance(skills, list): skills = ",".join(skills)
        ws.append_row([name, wc, skills], value_input_option="RAW")
        appended += 1
    return appended
