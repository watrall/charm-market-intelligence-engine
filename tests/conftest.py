"""
CHARM Test Configuration

Shared fixtures for testing pipeline components without external dependencies.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

# Test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_jobs_df() -> pd.DataFrame:
    """Minimal job postings DataFrame for testing."""
    return pd.DataFrame([
        {
            "source": "ACRA",
            "title": "Field Technician",
            "company": "Heritage Research Inc",
            "location": "Phoenix, AZ",
            "date_posted": "2025-01-15",
            "job_url": "https://example.com/job/1",
            "description": "Looking for field technician with ArcGIS and Section 106 experience. OSHA 10 required.",
        },
        {
            "source": "AAA",
            "title": "Senior Archaeologist",
            "company": "Cultural Resource Consultants",
            "location": "Denver, CO",
            "date_posted": "2025-01-14",
            "job_url": "https://example.com/job/2",
            "description": "Senior position requiring NEPA compliance experience and GIS skills.",
        },
        {
            "source": "ACRA",
            "title": "Project Manager",
            "company": "Heritage Research Inc",
            "location": "Tucson, AZ",
            "date_posted": "2025-01-13",
            "job_url": "https://example.com/job/3",
            "description": "Project management role. Salary: $65,000-$80,000 per year.",
        },
    ])


@pytest.fixture
def sample_jobs_with_skills() -> pd.DataFrame:
    """Jobs DataFrame with skills column already populated."""
    return pd.DataFrame([
        {
            "source": "ACRA",
            "title": "Field Technician",
            "company": "Heritage Research",
            "location": "Phoenix, AZ",
            "skills": ["ArcGIS", "Section 106", "OSHA 10"],
        },
        {
            "source": "AAA", 
            "title": "Senior Archaeologist",
            "company": "CRC",
            "location": "Denver, CO",
            "skills": ["NEPA", "GIS", "Section 106"],
        },
    ])


@pytest.fixture
def empty_jobs_df() -> pd.DataFrame:
    """Empty DataFrame with expected columns."""
    return pd.DataFrame(columns=[
        "source", "title", "company", "location", "date_posted", "job_url", "description"
    ])


@pytest.fixture
def job_patterns_config() -> dict:
    """Sample job patterns for testing classification."""
    return {
        "job_type": {
            "field-tech": ["field (tech|technician)", "crew (chief|lead)"],
            "pm/pi": ["project manager", "principal investigator"],
        },
        "seniority": {
            "senior": ["\\bsenior\\b", "lead"],
            "entry": ["entry", "technician"],
        }
    }


@pytest.fixture
def tmp_job_patterns(tmp_path, job_patterns_config) -> Path:
    """Write job patterns to a temporary file."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    patterns_file = config_dir / "job_patterns.json"
    patterns_file.write_text(json.dumps(job_patterns_config))
    return patterns_file
