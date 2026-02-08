"""Tests for pipeline data-saving with missing optional columns."""

import pandas as pd

import scripts.pipeline as pipeline
from scripts.db import get_conn, init_db, upsert_jobs, upsert_reports


class TestSaveProcessedDataMissingSkills:
    """_save_processed_data must not crash when skills column is absent."""

    def test_saves_csv_without_skills_column(self, tmp_path):
        df = pd.DataFrame([{
            "source": "ACRA",
            "title": "Field Tech",
            "company": "TestCo",
            "location": "Phoenix, AZ",
            "date_posted": "2025-01-15",
            "job_url": "https://example.com/1",
            "description": "A job.",
        }])
        pipeline._save_processed_data(df, None, tmp_path)
        result = pd.read_csv(tmp_path / "jobs.csv")
        assert len(result) == 1
        assert "skills" not in result.columns
        assert "skills_list" not in result.columns

    def test_saves_csv_with_skills_column(self, tmp_path):
        df = pd.DataFrame([{
            "source": "ACRA",
            "title": "Field Tech",
            "company": "TestCo",
            "location": "Phoenix, AZ",
            "skills": ["GIS", "NEPA"],
            "job_url": "https://example.com/1",
            "description": "A job.",
        }])
        pipeline._save_processed_data(df, None, tmp_path)
        result = pd.read_csv(tmp_path / "jobs.csv")
        assert len(result) == 1
        assert "skills" in result.columns


class TestUpsertJobsMissingColumns:
    """upsert_jobs must handle DataFrames missing optional enrichment columns."""

    def test_upsert_without_geocode_or_sentiment(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_conn(db_path)
        init_db(conn)
        try:
            df = pd.DataFrame([{
                "source": "ACRA",
                "title": "Field Tech",
                "company": "TestCo",
                "location": "Phoenix, AZ",
                "date_posted": "2025-01-15",
                "job_url": "https://example.com/1",
                "description": "A job.",
                "salary_min": 50000,
                "salary_max": 70000,
                "currency": "USD",
            }])
            # Should not raise KeyError for missing lat/lon/sentiment
            upsert_jobs(conn, df)
            row = conn.execute("SELECT title, lat, sentiment FROM jobs").fetchone()
            assert row[0] == "Field Tech"
            assert row[1] is None  # lat not provided
            assert row[2] is None  # sentiment not provided
        finally:
            conn.close()

    def test_upsert_reports_without_enrichment(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_conn(db_path)
        init_db(conn)
        try:
            df = pd.DataFrame([{
                "report_name": "test_report.pdf",
                "text": "Some report text.",
            }])
            # Should not raise KeyError for missing word_count/top_entities
            upsert_reports(conn, df)
            row = conn.execute("SELECT report_name, word_count FROM reports").fetchone()
            assert row[0] == "test_report.pdf"
            assert row[1] is None  # word_count not provided
        finally:
            conn.close()


class TestAnalyzeReportSkillsAsStrings:
    """analyze_market must handle report skills stored as strings, not lists."""

    def test_string_skills_parsed_correctly(self):
        from scripts.analyze import analyze_market

        jobs_df = pd.DataFrame(columns=["source", "title", "company"])
        reports_df = pd.DataFrame([
            {"report_name": "r1", "text": "body", "skills": "GIS;NEPA;Section 106"},
            {"report_name": "r2", "text": "body2", "skills": "GIS;ArcGIS"},
        ])
        result = analyze_market(jobs_df, reports_df)
        skill_dict = dict(result["report_skills"])
        # GIS appears in both reports
        assert skill_dict.get("GIS") == 2
        # Should NOT contain single characters (the bug produced 'G', 'I', 'S', etc.)
        assert "G" not in skill_dict
        assert "N" not in skill_dict
