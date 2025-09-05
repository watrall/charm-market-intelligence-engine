import os
from scripts.gsheets_sync import _ws
if __name__ == "__main__":
    try:
        ws = _ws()
        print(f"OK: Connected to Google Sheet '{ws.title}'. Try running the pipeline to append data.")
    except Exception as e:
        print(f"Google Sheets test failed: {e}")
