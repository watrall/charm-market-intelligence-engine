import sys
import types

import pandas as pd

import scripts.pipeline as pipeline


def test_sync_to_google_sheets_handles_failures(monkeypatch):
    """Sheets sync errors should not crash the pipeline."""

    def _boom(_df):
        raise ValueError("boom")

    fake_module = types.ModuleType("scripts.gsheets_sync")
    fake_module.sync_jobs_to_google_sheets = _boom
    fake_module.sync_reports_to_google_sheets = _boom
    monkeypatch.setitem(sys.modules, "scripts.gsheets_sync", fake_module)
    monkeypatch.setenv("USE_SHEETS", "true")

    jobs = pd.DataFrame([{"job_url": "https://example.com"}])
    reports = pd.DataFrame([{"report_name": "r1", "text": "body"}])

    # Should swallow errors and continue
    pipeline._sync_to_google_sheets(jobs, reports)
