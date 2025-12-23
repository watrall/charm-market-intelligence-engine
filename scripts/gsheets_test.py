from scripts.gsheets_sync import ensure_jobs_worksheet, ensure_reports_worksheet


def main():
    try:
        sheet, jobs_ws = ensure_jobs_worksheet()
        _, reports_ws = ensure_reports_worksheet()
        print(
            f"OK: Connected to '{sheet.title}'. "
            f"Worksheets ready: '{jobs_ws.title}', '{reports_ws.title}'."
        )
    except RuntimeError as e:
        print(f"Google Sheets test failed: {e}")


if __name__ == "__main__":
    main()
