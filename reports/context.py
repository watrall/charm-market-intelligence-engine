"""Build a normalized, serializable report context from processed artifacts.

Exports: build_report_context, compute_report_fingerprint.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------

def _file_sha256(path: Path) -> str:
    """Return hex SHA-256 of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_report_fingerprint(proc_dir: Path | str, filters: dict | None = None) -> str:
    """Deterministic fingerprint that changes when inputs change.

    Hashes the contents of ``jobs.csv``, ``analysis.json``, and
    ``insights.md`` (if present) plus the filter scope.
    """
    proc_dir = Path(proc_dir)
    parts: list[str] = []

    for name in ("jobs.csv", "analysis.json", "insights.md"):
        fp = proc_dir / name
        if fp.exists():
            parts.append(_file_sha256(fp))
        else:
            parts.append(f"missing:{name}")

    if filters:
        # Canonical JSON serialization for deterministic hashing
        parts.append(json.dumps(filters, sort_keys=True, default=str))

    combined = "|".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Data loading helpers (no Streamlit dependency)
# ---------------------------------------------------------------------------

def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "date_posted" in df.columns:
        df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce", utc=True).dt.date
    return df


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _coerce_skills(value) -> list[str]:
    """Parse skills from various formats."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except (json.JSONDecodeError, TypeError):
                pass
        return [t.strip(" []'\"") for t in re.split(r"[;|,]", raw) if t.strip(" []'\"")]
    return []


# ---------------------------------------------------------------------------
# Context extraction helpers
# ---------------------------------------------------------------------------

def _compute_key_findings(df: pd.DataFrame, analysis: dict) -> list[dict]:
    """Build list of key-finding dicts from data."""
    findings: list[dict] = []

    num_jobs = analysis.get("num_jobs", len(df))
    findings.append({"label": "Total postings analyzed", "value": f"{num_jobs:,}"})

    ue = analysis.get("unique_employers", 0)
    if not ue and "company" in df.columns:
        ue = df["company"].nunique()
    findings.append({"label": "Unique employers", "value": f"{ue:,}"})

    # Date range
    if "date_posted" in df.columns and not df.empty and df["date_posted"].notna().any():
        dmin = df["date_posted"].min()
        dmax = df["date_posted"].max()
        findings.append({"label": "Date range", "value": f"{dmin} to {dmax}"})

    # Geocoded
    geocoded = analysis.get("geocoded", 0)
    if geocoded and num_jobs:
        pct = round(geocoded / num_jobs * 100)
        findings.append({"label": "Geocoded", "value": f"{geocoded:,} ({pct}%)"})

    # Top region
    if "state" in df.columns and not df.empty:
        top_state = df["state"].value_counts().head(1)
        if not top_state.empty:
            findings.append({
                "label": "Top region",
                "value": f"{top_state.index[0]} ({top_state.values[0]:,} postings)",
            })

    # Top skill
    top_skills = analysis.get("top_skills", [])
    if top_skills:
        findings.append({
            "label": "Top skill",
            "value": f"{top_skills[0][0]} ({top_skills[0][1]:,})",
        })

    # Unique locations
    if {"city", "state"}.issubset(df.columns) and not df.empty:
        uniq_locs = df[["city", "state"]].dropna().drop_duplicates().shape[0]
        findings.append({"label": "Unique locations", "value": f"{uniq_locs:,}"})

    # Top employer
    top_employers = analysis.get("top_employers", [])
    if top_employers:
        findings.append({
            "label": "Top employer",
            "value": f"{top_employers[0][0]} ({top_employers[0][1]:,})",
        })

    return findings[:9]  # cap at 9


def _extract_executive_summary(analysis: dict, insights_text: str, df: pd.DataFrame) -> list[str]:
    """Derive exactly 5 executive summary bullets."""
    bullets: list[str] = []
    num_jobs = analysis.get("num_jobs", len(df))
    ue = analysis.get("unique_employers", 0)
    top_skills = analysis.get("top_skills", [])

    # Bullet 1: overview
    if num_jobs:
        bullets.append(
            f"This report analyzes {num_jobs:,} job postings from "
            f"{ue:,} unique employers in the cultural resource management "
            f"and heritage sector."
        )
    else:
        bullets.append("No job postings were available for analysis in this run.")

    # Bullet 2: top skill signal
    if len(top_skills) >= 2:
        s1, c1 = top_skills[0]
        s2, c2 = top_skills[1]
        bullets.append(
            f"{s1} ({c1:,} mentions) and {s2} ({c2:,} mentions) are the most "
            f"frequently cited skills, indicating strong demand in compliance-related work."
        )
    else:
        bullets.append("Skill signal data is not available in this run.")

    # Bullet 3: geographic coverage
    geocoded = analysis.get("geocoded", 0)
    if geocoded and "state" in df.columns and not df.empty:
        n_states = df["state"].nunique()
        bullets.append(
            f"Postings span {n_states} states/territories, with {geocoded:,} "
            f"successfully geocoded for spatial analysis."
        )
    else:
        bullets.append("Geographic coverage data is not available in this run.")

    # Bullet 4: employer diversity
    top_employers = analysis.get("top_employers", [])
    if ue and top_employers:
        top3 = [e[0] for e in top_employers[:3]]
        bullets.append(
            f"The employer mix includes {ue:,} organizations, with "
            f"{', '.join(top3)} among the most active."
        )
    else:
        bullets.append("Employer diversity data is not available in this run.")

    # Bullet 5: program opportunity / insight
    if insights_text and "Program Recommendations" in insights_text:
        bullets.append(
            "Program mapping analysis identifies opportunities to align "
            "curriculum offerings with the most in-demand competency areas."
        )
    elif len(top_skills) >= 5:
        mid_skills = [s[0] for s in top_skills[2:5]]
        bullets.append(
            f"Beyond top compliance skills, demand signals for {', '.join(mid_skills)} "
            f"suggest emerging training and program opportunities."
        )
    else:
        bullets.append("Program opportunity analysis is not available in this run.")

    return bullets[:5]


def _extract_implications(insights_text: str, analysis: dict) -> list[dict]:
    """Derive 3-6 actionable implications for leadership."""
    implications: list[dict] = []
    top_skills = analysis.get("top_skills", [])

    # Derive from available data
    if len(top_skills) >= 2:
        s1 = top_skills[0][0]
        s2 = top_skills[1][0]
        implications.append({
            "title": "Strengthen compliance training",
            "explanation": (
                f"{s1} and {s2} dominate the skills distribution. "
                f"Programs that build practical competency in these areas "
                f"will align directly with employer demand."
            ),
            "why": "Compliance skills appear in the majority of postings and span all seniority levels.",
        })

    # GIS / digital skills
    gis_skills = [s for s, c in top_skills if "gis" in s.lower() or "arcgis" in s.lower()]
    if gis_skills:
        implications.append({
            "title": "Expand geospatial and digital skills offerings",
            "explanation": (
                "GIS and related digital tools appear consistently across job types, "
                "not only in specialist roles. Short-format refreshers tied to real "
                "deliverables could reach a wider audience."
            ),
            "why": "Geospatial competency is increasingly expected as a baseline, not a specialization.",
        })

    # Project management
    pm_skills = [s for s, c in top_skills if "project" in s.lower() or "management" in s.lower()]
    if pm_skills:
        implications.append({
            "title": "Develop project management and leadership tracks",
            "explanation": (
                "Project management signals increase with seniority and are often paired "
                "with client communication and budgeting. A structured track supports "
                "career progression in the field."
            ),
            "why": "Mid-career and senior roles increasingly require management alongside technical skills.",
        })

    # Program recommendations from insights
    if insights_text and "Program Recommendations" in insights_text:
        implications.append({
            "title": "Align program formats with demand signals",
            "explanation": (
                "Rules-based mapping of top skills to program formats (certificates, "
                "workshops, microlearning) provides an evidence-based starting point "
                "for curriculum development."
            ),
            "why": "Matching delivery format to skill type improves enrollment and completion rates.",
        })

    # Lab / collections
    lab_skills = [s for s, c in top_skills if "lab" in s.lower() or "collection" in s.lower() or "curation" in s.lower()]
    if lab_skills:
        implications.append({
            "title": "Support lab and collections career pathways",
            "explanation": (
                "Lab, collections, and curation roles appear consistently, "
                "suggesting stable demand beyond field crews. Dedicated training "
                "tracks can support this workforce segment."
            ),
            "why": "Collections and lab roles offer year-round, location-stable employment.",
        })

    # Emerging / digital heritage
    digital_skills = [s for s, c in top_skills if "digital" in s.lower() or "omeka" in s.lower() or "lidar" in s.lower()]
    if digital_skills:
        implications.append({
            "title": "Invest in emerging digital heritage skills",
            "explanation": (
                "LiDAR, digital exhibits, and related technologies appear as "
                "differentiators. Early investment in these areas positions programs "
                "ahead of growing demand."
            ),
            "why": "Digital heritage technologies are still maturing, creating a first-mover advantage for prepared programs.",
        })

    return implications[:6]


def _build_methods() -> dict:
    """Build methods & governance metadata."""
    return {
        "data_sources": [
            "Professional association job boards (ACRA, AAA)",
            "Industry PDF reports and publications",
        ],
        "approach": (
            "Automated scraping, deduplication, and enrichment pipeline "
            "with NLP-based skill extraction using a curated taxonomy of "
            "400+ domain-specific skills. Geocoding via Nominatim. "
            "Optional LLM-assisted interpretation."
        ),
        "limitations": [
            "Coverage is limited to indexed sources and may not reflect all open positions.",
            "Skill extraction relies on pattern matching and may miss synonyms or novel terminology.",
        ],
        "governance": (
            "Data is refreshed on each pipeline run. Results should be interpreted "
            "in context of coverage limitations. No personally identifiable information "
            "is collected. Standard bias and coverage considerations apply to any "
            "labor-market analysis."
        ),
    }


def _build_appendix_definitions() -> list[dict]:
    """Standard definitions for the appendix."""
    return [
        {"term": "Posting", "definition": "A single job listing from a professional association job board."},
        {"term": "Skill extraction", "definition": "Automated identification of competency terms using a curated taxonomy of 400+ domain-specific skill aliases."},
        {"term": "Geocoding", "definition": "Conversion of location text (city, state) to geographic coordinates using the Nominatim API."},
        {"term": "Emerging skill", "definition": "A skill appearing in the mid-frequency range of demand signals, suggesting growing but not yet dominant relevance."},
        {"term": "Demand signal", "definition": "The frequency with which a skill appears across analyzed job postings, used as a proxy for market demand."},
        {"term": "Program mapping", "definition": "Rules-based alignment of in-demand skills to educational program formats (certificate, workshop, degree, etc.)."},
    ]


def build_report_context(proc_dir: Path | str, filters: dict | None = None) -> dict:
    """Build a report context dict from proc_dir artifacts.

    proc_dir should contain jobs.csv, analysis.json, and insights.md.
    filters (optional) narrows the scope (date_start, date_end, skills,
    seniority, job_type). Returns a dict for render_report_pdf().
    """
    proc_dir = Path(proc_dir)

    # Load canonical artifacts
    df = _load_csv(proc_dir / "jobs.csv")
    analysis = _load_json(proc_dir / "analysis.json")
    insights_text = _load_text(proc_dir / "insights.md")

    # Prepare skills list column
    if "skills" in df.columns and "skills_list" not in df.columns:
        df["skills_list"] = df["skills"].apply(_coerce_skills)

    # Apply filters if provided
    if filters:
        df = _apply_filters(df, filters)

    # Date range
    date_range = ""
    if "date_posted" in df.columns and not df.empty and df["date_posted"].notna().any():
        dmin = df["date_posted"].min()
        dmax = df["date_posted"].max()
        date_range = f"{dmin} to {dmax}"

    # Run timestamp
    run_ts = analysis.get("run_timestamp", "")
    if run_ts:
        try:
            run_ts = str(run_ts)[:19]
        except Exception:
            pass

    # Top skills
    top_skills_raw: list[list] = analysis.get("top_skills", [])
    top_skills = [(str(s[0]), int(s[1])) for s in top_skills_raw if len(s) >= 2]

    # Emerging: next tier after top 12
    top_12 = top_skills[:12]
    emerging = top_skills[12:22]

    # Build context
    context: dict[str, Any] = {
        "title": "CHARM Market Intelligence Report",
        "date_range": date_range,
        "run_date": run_ts or datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
        "fingerprint": compute_report_fingerprint(proc_dir, filters),
        "key_findings": _compute_key_findings(df, analysis),
        "executive_summary": _extract_executive_summary(analysis, insights_text, df),
        "top_skills": top_12,
        "emerging_skills": emerging,
        "implications": _extract_implications(insights_text, analysis),
        "methods": _build_methods(),
        "appendix_definitions": _build_appendix_definitions(),
        "appendix_sources": analysis.get("top_employers", []),
        "filters": filters,
    }
    return context


def _apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Apply filter dict to DataFrame."""
    if df.empty:
        return df

    if "date_start" in filters and "date_posted" in df.columns:
        df = df[df["date_posted"] >= pd.Timestamp(filters["date_start"]).date()]
    if "date_end" in filters and "date_posted" in df.columns:
        df = df[df["date_posted"] <= pd.Timestamp(filters["date_end"]).date()]

    if "skills" in filters and filters["skills"] and "skills_list" in df.columns:
        selected = set(filters["skills"])
        df = df[df["skills_list"].apply(lambda lst: bool(set(lst) & selected))]

    if "seniority" in filters and filters["seniority"] and "seniority" in df.columns:
        df = df[df["seniority"].isin(filters["seniority"])]

    if "job_type" in filters and filters["job_type"] and "job_type" in df.columns:
        df = df[df["job_type"].isin(filters["job_type"])]

    return df
