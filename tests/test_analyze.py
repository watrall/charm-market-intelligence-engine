"""
Tests for analyze module.

These tests verify that market analysis produces deterministic,
correctly-structured outputs.
"""

import json
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.analyze import analyze_market, _ensure_skill_lists


class TestEnsureSkillLists:
    """Tests for skill list parsing."""

    def test_parses_semicolon_separated(self):
        """Should parse 'ArcGIS;NEPA;Section 106' into list."""
        series = pd.Series(["ArcGIS;NEPA;Section 106"])
        result = _ensure_skill_lists(series)
        assert len(result) == 1
        assert result[0] == ["ArcGIS", "NEPA", "Section 106"]

    def test_parses_json_array(self):
        """Should parse JSON array strings."""
        series = pd.Series(['["ArcGIS", "NEPA"]'])
        result = _ensure_skill_lists(series)
        assert len(result) == 1
        assert result[0] == ["ArcGIS", "NEPA"]

    def test_handles_list_input(self):
        """Should pass through actual lists."""
        series = pd.Series([["ArcGIS", "NEPA"]])
        result = _ensure_skill_lists(series)
        assert result[0] == ["ArcGIS", "NEPA"]

    def test_handles_empty_strings(self):
        """Should skip empty strings."""
        series = pd.Series(["", "ArcGIS;NEPA"])
        result = _ensure_skill_lists(series)
        assert len(result) == 1

    def test_handles_nan(self):
        """Should skip NaN values."""
        series = pd.Series([None, "ArcGIS"])
        result = _ensure_skill_lists(series)
        assert len(result) == 1


class TestAnalyzeMarket:
    """Tests for the main analysis function."""

    def test_returns_expected_keys(self, sample_jobs_with_skills):
        """Output should contain all expected keys."""
        result = analyze_market(sample_jobs_with_skills, None)
        
        expected_keys = [
            "num_jobs",
            "unique_employers", 
            "top_skills",
            "geocoded",
            "report_skills",
            "top_employers",
            "run_timestamp",
        ]
        for key in expected_keys:
            assert key in result.index, f"Missing key: {key}"

    def test_counts_jobs_correctly(self, sample_jobs_with_skills):
        """Should count total jobs."""
        result = analyze_market(sample_jobs_with_skills, None)
        assert result["num_jobs"] == 2

    def test_counts_unique_employers(self, sample_jobs_with_skills):
        """Should count unique companies."""
        result = analyze_market(sample_jobs_with_skills, None)
        assert result["unique_employers"] == 2

    def test_aggregates_skills(self, sample_jobs_with_skills):
        """Should aggregate and count skills across all jobs."""
        result = analyze_market(sample_jobs_with_skills, None)
        
        top_skills = result["top_skills"]
        skill_dict = dict(top_skills)
        
        # Section 106 appears in both jobs
        assert skill_dict.get("Section 106") == 2

    def test_handles_empty_dataframe(self, empty_jobs_df):
        """Should handle empty input gracefully."""
        result = analyze_market(empty_jobs_df, None)
        
        assert result["num_jobs"] == 0
        assert result["unique_employers"] == 0
        assert result["top_skills"] == []

    def test_handles_none_input(self):
        """Should handle None input."""
        result = analyze_market(None, None)
        assert result["num_jobs"] == 0

    def test_output_is_serializable(self, sample_jobs_with_skills):
        """Output should be JSON-serializable."""
        result = analyze_market(sample_jobs_with_skills, None)
        
        # Convert to JSON and back
        json_str = result.to_json()
        parsed = json.loads(json_str)
        
        assert parsed["num_jobs"] == 2

    def test_top_skills_ordered_by_frequency(self, sample_jobs_with_skills):
        """Skills should be sorted by count descending."""
        result = analyze_market(sample_jobs_with_skills, None)
        
        top_skills = result["top_skills"]
        counts = [count for _, count in top_skills]
        
        # Should be in descending order
        assert counts == sorted(counts, reverse=True)

    def test_deterministic_output(self, sample_jobs_with_skills):
        """Same input should produce same output (except timestamp)."""
        result1 = analyze_market(sample_jobs_with_skills.copy(), None)
        result2 = analyze_market(sample_jobs_with_skills.copy(), None)
        
        # Compare everything except timestamp
        assert result1["num_jobs"] == result2["num_jobs"]
        assert result1["top_skills"] == result2["top_skills"]
        assert result1["top_employers"] == result2["top_employers"]
