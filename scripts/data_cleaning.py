import hashlib, pandas as pd, re

_SAL_RE = re.compile(r'(\$|USD\s*)?\s*(\d{2,3}[,\.]?\d{0,3})(?:\s*[-–to]{1,3}\s*(\d{2,3}[,\.]?\d{0,3}))?\s*(?:per\s*(year|yr|hour|hr|annum))?', re.I)

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

def clean_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["title","company","location","date_posted","job_url","description"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["dedupe_key"] = [ _hash_row(r["title"], r["company"], r["description"]) for _, r in df.iterrows() ]
    df = df.drop_duplicates(subset=["job_url"]).drop_duplicates(subset=["dedupe_key"])
    df["salary_min"], df["salary_max"], df["currency"] = zip(*df["description"].fillna("").map(extract_salary))
    return df.drop(columns=["dedupe_key"])
