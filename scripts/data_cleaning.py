from __future__ import annotations

import hashlib
import json
import os
import re
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import pandas as pd

_SAL_RE = re.compile(
    r"(\$|USD\s*)?\s*(\d{2,3}(?:[,\.]\d{3})?)"
    r"(?:\s*(?:-|–|to)\s*(?:\$|USD\s*)?\s*(\d{2,3}(?:[,\.]\d{3})?))?"
    r"\s*(?:per\s*(year|yr|hour|hr|annum))?",
    re.I,
)

US_STATE_MAP = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}
STATE_NAME_TO_ABBR = {name.lower(): abbr for abbr, name in US_STATE_MAP.items()}

_JOB_PATTERNS: dict[str, re.Pattern[str]] | None = None
_SENIORITY_PATTERNS: list[tuple[str, re.Pattern[str]]] | None = None
_PATTERN_SOURCE: Path | None = None


def _reset_pattern_cache() -> None:
    """Reset cached job/seniority patterns (used in tests and overrides)."""
    global _JOB_PATTERNS, _SENIORITY_PATTERNS, _PATTERN_SOURCE
    _JOB_PATTERNS = None
    _SENIORITY_PATTERNS = None
    _PATTERN_SOURCE = None


def _compile_entries(entries: Sequence[object], bucket: str, errors: list[str]) -> re.Pattern[str]:
    compiled = []
    for entry in entries:
        pattern = entry
        if isinstance(entry, dict):
            pattern = entry.get("pattern")
        if not pattern:
            errors.append(f"{bucket}: missing 'pattern' entry; skipped")
            continue
        compiled.append(f"(?:{pattern})")
    try:
        return re.compile("|".join(compiled), re.I) if compiled else re.compile(r"(?!)")
    except re.error as exc:
        errors.append(f"{bucket}: invalid regex ({exc}); skipped bucket")
        return re.compile(r"(?!)")


def _patterns_path(config_path: Path | None = None) -> Path:
    override = os.getenv("JOB_PATTERNS_PATH")
    base = Path(__file__).resolve().parents[1]
    return Path(config_path or override or (base / "config" / "job_patterns.json")).resolve()


def _load_patterns(config_path: Path | None = None) -> tuple[dict[str, re.Pattern[str]], list[tuple[str, re.Pattern[str]]]]:
    global _JOB_PATTERNS, _SENIORITY_PATTERNS, _PATTERN_SOURCE
    path = _patterns_path(config_path)
    if (
        _JOB_PATTERNS is not None
        and _SENIORITY_PATTERNS is not None
        and _PATTERN_SOURCE == path
    ):
        return _JOB_PATTERNS, _SENIORITY_PATTERNS

    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        warnings.warn(f"Job pattern config missing at {path}; classification skipped.", stacklevel=2)
        _JOB_PATTERNS, _SENIORITY_PATTERNS, _PATTERN_SOURCE = {}, [], path
        return _JOB_PATTERNS, _SENIORITY_PATTERNS
    except json.JSONDecodeError as exc:
        warnings.warn(f"Invalid JSON in {path}: {exc}; classification skipped.", stacklevel=2)
        _JOB_PATTERNS, _SENIORITY_PATTERNS, _PATTERN_SOURCE = {}, [], path
        return _JOB_PATTERNS, _SENIORITY_PATTERNS

    errors: list[str] = []
    job_patterns = {
        str(bucket): _compile_entries(entries, bucket=str(bucket), errors=errors)
        for bucket, entries in data.get("job_type", {}).items()
    }
    seniority_patterns = [
        (str(bucket), _compile_entries(entries, bucket=str(bucket), errors=errors))
        for bucket, entries in data.get("seniority", {}).items()
    ]
    if errors:
        warnings.warn("; ".join(errors), stacklevel=2)
    _JOB_PATTERNS, _SENIORITY_PATTERNS, _PATTERN_SOURCE = job_patterns, seniority_patterns, path
    return _JOB_PATTERNS, _SENIORITY_PATTERNS

def extract_salary(text: str) -> tuple[float | None, float | None, str | None]:
    if not text:
        return None, None, None
    m = _SAL_RE.search(text)
    if not m:
        return None, None, None

    cur = "USD" if m.group(1) else None
    low_str = m.group(2).replace(",", "")
    high_str = m.group(3).replace(",", "") if m.group(3) else None

    try:
        low = float(low_str)
        high = float(high_str) if high_str else None
    except ValueError:
        return None, None, None
    return low, high, cur

def _hash_row(title, company, description):
    s = f"{(title or '').strip().lower()}|{(company or '').strip().lower()}|{(description or '')[:280].strip().lower()}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _parse_city_state(loc: str) -> tuple[str, str]:
    if not loc:
        return "", ""
    cleaned = re.sub(r"\s+", " ", loc)
    parts = [p.strip() for p in re.split(r"[,\-/;|]", cleaned) if p.strip()]
    city = parts[0] if parts else cleaned.strip()
    state = ""
    for token in parts[1:]:
        upper = token.upper()
        lower = token.lower()
        if upper in US_STATE_MAP:
            state = upper
            break
        if lower in STATE_NAME_TO_ABBR:
            state = STATE_NAME_TO_ABBR[lower]
            break
    return city, state

def _infer_job_type(title: str, description: str) -> str:
    job_patterns, _ = _load_patterns()
    text = f"{title or ''} {description or ''}"
    for bucket, pattern in job_patterns.items():
        if pattern.search(text):
            return bucket
    return ""

def _infer_seniority(title: str, description: str) -> str:
    _, seniority_patterns = _load_patterns()
    text = f"{title or ''} {description or ''}"
    for bucket, pattern in seniority_patterns:
        if pattern.search(text):
            return bucket
    return ""

def clean_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["title", "company", "location", "date_posted", "job_url", "description"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["dedupe_key"] = [_hash_row(r["title"], r["company"], r["description"]) for _, r in df.iterrows()]
    df = df.drop_duplicates(subset=["job_url"]).drop_duplicates(subset=["dedupe_key"])

    salary_tuples: list[tuple[float | None, float | None, str | None]] = [
        extract_salary(desc) for desc in df["description"].fillna("").astype(str).tolist()
    ]
    if salary_tuples:
        mins, maxs, currencies = zip(*salary_tuples)
        df["salary_min"] = list(mins)
        df["salary_max"] = list(maxs)
        df["currency"] = list(currencies)
    else:
        df["salary_min"] = [None] * len(df)
        df["salary_max"] = [None] * len(df)
        df["currency"] = [None] * len(df)

    cities, states = zip(*[ _parse_city_state(loc) for loc in df["location"].tolist() ]) if len(df) else ([], [])
    if len(df):
        df["city"] = list(cities)
        df["state"] = list(states)
        df["job_type"] = [
            _infer_job_type(row["title"], row["description"])
            for _, row in df.iterrows()
        ]
        df["seniority"] = [
            _infer_seniority(row["title"], row["description"])
            for _, row in df.iterrows()
        ]
        df["url"] = df["job_url"]
    else:
        for col in ("city", "state", "job_type", "seniority", "url"):
            df[col] = []

    return cast(pd.DataFrame, df.drop(columns=["dedupe_key"]))
