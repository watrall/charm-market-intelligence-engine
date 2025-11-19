import hashlib, pandas as pd, re
from typing import Tuple

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

JOB_TYPE_KEYWORDS = {
    "field-tech": ["field technician", "field tech", "archaeology technician", "archaeological technician", "crew chief", "crew lead"],
    "lab/analyst": ["laboratory", "lab ", "collections specialist", "collections manager", "artifact analyst", "osteology"],
    "architectural-historian": ["architectural historian", "architectural history", "historic preservation"],
    "pm/pi": ["project manager", "principal investigator", "pi ", "program manager", "project director"],
}
SENIORITY_KEYWORDS = [
    ("lead/PI", ["principal investigator", "pi ", "project director", "senior manager", "practice lead"]),
    ("senior", ["senior", "lead ", "manager", "director"]),
    ("mid", ["mid-level", "mid level", "specialist", "coordinator"]),
    ("entry", ["entry", "assistant", "technician", "intern"]),
]

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
    text = f"{title or ''} {description or ''}".lower()
    for bucket, keywords in JOB_TYPE_KEYWORDS.items():
        if any(k in text for k in keywords):
            return bucket
    return ""

def _infer_seniority(title: str, description: str) -> str:
    text = f"{title or ''} {description or ''}".lower()
    for bucket, keywords in SENIORITY_KEYWORDS:
        if any(k in text for k in keywords):
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
