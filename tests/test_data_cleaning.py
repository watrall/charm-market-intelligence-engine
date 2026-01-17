"""
Tests for data_cleaning module.

These tests verify that the cleaning and deduplication logic works correctly
without requiring external services.
"""

import pandas as pd
import pytest

# Import the functions we're testing
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.data_cleaning import (
    extract_salary,
    _hash_row,
    _parse_city_state,
    clean_and_dedupe,
)


class TestExtractSalary:
    """Tests for salary extraction from job descriptions."""

    def test_salary_range_with_dollar_sign(self):
        """Should extract min/max from '$65,000-$80,000'."""
        low, high, currency = extract_salary("Salary: $65,000-$80,000 per year")
        assert low == 65000.0
        assert high == 80000.0

    def test_salary_single_value(self):
        """Should handle single salary value."""
        low, high, currency = extract_salary("Starting at $50,000")
        assert low == 50000.0
        assert high is None

    def test_no_salary_returns_none(self):
        """Should return None tuple when no salary found."""
        low, high, currency = extract_salary("Great benefits and growth opportunities")
        assert low is None
        assert high is None
        assert currency is None

    def test_empty_string(self):
        """Should handle empty input gracefully."""
        low, high, currency = extract_salary("")
        assert low is None
        assert high is None


class TestHashRow:
    """Tests for content hashing used in deduplication."""

    def test_same_content_same_hash(self):
        """Identical content should produce identical hashes."""
        hash1 = _hash_row("Field Tech", "Company A", "Description text here")
        hash2 = _hash_row("Field Tech", "Company A", "Description text here")
        assert hash1 == hash2

    def test_different_content_different_hash(self):
        """Different content should produce different hashes."""
        hash1 = _hash_row("Field Tech", "Company A", "Description 1")
        hash2 = _hash_row("Field Tech", "Company A", "Description 2")
        assert hash1 != hash2

    def test_case_insensitive(self):
        """Hashing should be case-insensitive."""
        hash1 = _hash_row("Field Tech", "Company A", "Description")
        hash2 = _hash_row("FIELD TECH", "COMPANY A", "DESCRIPTION")
        assert hash1 == hash2

    def test_handles_none_values(self):
        """Should handle None values without crashing."""
        hash1 = _hash_row(None, "Company", "Desc")
        hash2 = _hash_row("", "Company", "Desc")
        assert hash1 == hash2


class TestParseCityState:
    """Tests for location parsing."""

    def test_city_state_comma_separated(self):
        """Should parse 'Phoenix, AZ' correctly."""
        city, state = _parse_city_state("Phoenix, AZ")
        assert city == "Phoenix"
        assert state == "AZ"

    def test_city_state_full_name(self):
        """Should handle full state names."""
        city, state = _parse_city_state("Denver, Colorado")
        assert city == "Denver"
        assert state == "CO"

    def test_empty_location(self):
        """Should return empty strings for empty input."""
        city, state = _parse_city_state("")
        assert city == ""
        assert state == ""

    def test_city_only(self):
        """Should handle city without state."""
        city, state = _parse_city_state("Remote")
        assert city == "Remote"
        assert state == ""


class TestCleanAndDedupe:
    """Integration tests for the full cleaning pipeline."""

    def test_removes_duplicate_urls(self, sample_jobs_df):
        """Should remove rows with duplicate job_url."""
        # Add a duplicate URL
        df = pd.concat([sample_jobs_df, sample_jobs_df.iloc[[0]]], ignore_index=True)
        assert len(df) == 4  # 3 original + 1 duplicate
        
        cleaned = clean_and_dedupe(df)
        assert len(cleaned) == 3  # Duplicate removed

    def test_adds_expected_columns(self, sample_jobs_df):
        """Should add city, state, job_type, seniority columns."""
        cleaned = clean_and_dedupe(sample_jobs_df)
        
        expected_columns = ["city", "state", "job_type", "seniority", "url"]
        for col in expected_columns:
            assert col in cleaned.columns, f"Missing column: {col}"

    def test_extracts_salary_fields(self, sample_jobs_df):
        """Should extract salary_min and salary_max."""
        cleaned = clean_and_dedupe(sample_jobs_df)
        
        assert "salary_min" in cleaned.columns
        assert "salary_max" in cleaned.columns
        
        # The third job has salary info
        job_with_salary = cleaned[cleaned["title"] == "Project Manager"].iloc[0]
        assert job_with_salary["salary_min"] == 65000.0

    def test_handles_empty_dataframe(self, empty_jobs_df):
        """Should handle empty input gracefully."""
        cleaned = clean_and_dedupe(empty_jobs_df)
        assert len(cleaned) == 0
        assert "city" in cleaned.columns

    def test_output_columns_are_stable(self, sample_jobs_df):
        """Column order should be consistent across runs."""
        cleaned1 = clean_and_dedupe(sample_jobs_df.copy())
        cleaned2 = clean_and_dedupe(sample_jobs_df.copy())
        
        assert list(cleaned1.columns) == list(cleaned2.columns)
