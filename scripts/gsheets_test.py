from scripts.gsheets_sync import ensure_jobs_worksheet, ensure_reports_worksheet


if __name__ == "__main__":
    try:
        sheet, jobs_ws = ensure_jobs_worksheet()
        _, reports_ws = ensure_reports_worksheet()
        print(
            f"OK: Connected to '{sheet.title}'. Worksheets ready: '{jobs_ws.title}', '{reports_ws.title}'."
        )
    except Exception as e:
        print(f"Google Sheets test failed: {e}")
