import hashlib, pandas as pd, re, json
from typing import Tuple
from pathlib import Path

_SAL_RE = re.compile(r'(\$|USD\s*)?\s*(\d{2,3}[,\.]?\d{0,3})(?:\s*[-–to]{1,3}\s*(\d{2,3}[,\.]?\d{0,3}))?\s*(?:per\s*(year|yr|hour|hr|annum))?', re.I)

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

_JOB_PATTERNS = None
_SENIORITY_PATTERNS = None


def _compile_entries(entries):
    compiled = []
    for entry in entries:
        pattern = entry
        if isinstance(entry, dict):
            pattern = entry.get("pattern")
        if not pattern:
            raise ValueError("Each job pattern entry must include a 'pattern'.")
        compiled.append(f"(?:{pattern})")
    return re.compile("|".join(compiled), re.I) if compiled else re.compile(r"(?!x)")


def _load_patterns():
    global _JOB_PATTERNS, _SENIORITY_PATTERNS
    if _JOB_PATTERNS is not None and _SENIORITY_PATTERNS is not None:
        return _JOB_PATTERNS, _SENIORITY_PATTERNS
    config_path = Path(__file__).resolve().parents[1] / "config" / "job_patterns.json"
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing job pattern config: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {config_path}: {exc}") from exc

    job_patterns = {
        bucket: _compile_entries(entries)
        for bucket, entries in data.get("job_type", {}).items()
    }
    seniority_patterns = [
        (bucket, _compile_entries(entries))
        for bucket, entries in data.get("seniority", {}).items()
    ]
    _JOB_PATTERNS, _SENIORITY_PATTERNS = job_patterns, seniority_patterns
    return _JOB_PATTERNS, _SENIORITY_PATTERNS

def extract_salary(text: str):
    if not text: return None, None, None
    m = _SAL_RE.search(text)
    if not m: return None, None, None
    cur = "USD" if m.group(1) else None
    low = m.group(2).replace(',', '')
    high = m.group(3).replace(',', '') if m.group(3) else None
    try:
        low = float(low)
        high = float(high) if high else None
    except Exception:
        return None, None, None
    return low, high, cur

def _hash_row(title, company, description):
    s = f"{(title or '').strip().lower()}|{(company or '').strip().lower()}|{(description or '')[:280].strip().lower()}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _parse_city_state(loc: str) -> Tuple[str, str]:
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

    salary_tuples = df["description"].fillna("").map(extract_salary).tolist()
    if salary_tuples:
        df["salary_min"], df["salary_max"], df["currency"] = zip(*salary_tuples)
    else:
        df["salary_min"] = []
        df["salary_max"] = []
        df["currency"] = []

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

    return df.drop(columns=["dedupe_key"])
