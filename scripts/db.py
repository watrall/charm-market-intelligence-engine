import sqlite3
from pathlib import Path
import pandas as pd

SCHEMA = {
    "jobs": """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            date_posted TEXT,
            job_url TEXT UNIQUE,
            description TEXT,
            sentiment REAL,
            lat REAL, lon REAL,
            salary_min REAL, salary_max REAL, currency TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """,
    "job_skills": """
        CREATE TABLE IF NOT EXISTS job_skills (
            job_id INTEGER,
            skill TEXT,
            UNIQUE(job_id, skill)
        );
    """,
    "reports": """
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_name TEXT UNIQUE,
            text TEXT,
            word_count INTEGER,
            top_entities TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """
}

INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_date ON jobs(date_posted)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(location)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_latlon ON jobs(lat,lon)",
    "CREATE INDEX IF NOT EXISTS idx_jobskills_skill ON job_skills(skill)",
    "CREATE INDEX IF NOT EXISTS idx_jobskills_jobid ON job_skills(job_id)"
]

def get_conn(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db(conn):
    cur = conn.cursor()
    for sql in SCHEMA.values():
        cur.execute(sql)
    for idx in INDICES:
        cur.execute(idx)
    conn.commit()

def _clean_record(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def upsert_jobs(conn, jobs_df: pd.DataFrame):
    cols = ["source","title","company","location","date_posted","job_url","description","sentiment","lat","lon","salary_min","salary_max","currency"]
    records = [
        tuple(_clean_record(r[c]) for c in cols)
        for _, r in jobs_df[cols].iterrows()
    ]
    if records:
        conn.executemany(
            """INSERT INTO jobs (source,title,company,location,date_posted,job_url,description,sentiment,lat,lon,salary_min,salary_max,currency)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(job_url) DO UPDATE SET
                    source=excluded.source, title=excluded.title, company=excluded.company,
                    location=excluded.location, date_posted=excluded.date_posted,
                    description=excluded.description, sentiment=excluded.sentiment,
                    lat=excluded.lat, lon=excluded.lon,
                    salary_min=excluded.salary_min, salary_max=excluded.salary_max, currency=excluded.currency
            """,
            records
        )
        conn.commit()
    if "skills" in jobs_df.columns:
        cur = conn.cursor()
        for _, r in jobs_df.iterrows():
            url = r.get("job_url","")
            if not url: continue
            job_id = cur.execute("SELECT id FROM jobs WHERE job_url=?", (url,)).fetchone()
            if not job_id: continue
            job_id = job_id[0]
            for s in (r.get("skills") or []):
                try:
                    cur.execute("INSERT OR IGNORE INTO job_skills (job_id, skill) VALUES (?,?)", (job_id, s))
                except Exception:
                    pass
        conn.commit()

def upsert_reports(conn, reports_df: pd.DataFrame):
    if reports_df is None or reports_df.empty: return
    cols = ["report_name", "text", "word_count", "top_entities"]
    records = [
        tuple(_clean_record(r.get(c)) for c in cols)
        for _, r in reports_df[cols].iterrows()
    ]
    if records:
        conn.executemany(
            """INSERT INTO reports (report_name, text, word_count, top_entities)
               VALUES (?,?,?,?)
               ON CONFLICT(report_name) DO UPDATE SET
                    text=excluded.text,
                    word_count=excluded.word_count,
                    top_entities=excluded.top_entities
            """,
            records
        )
        conn.commit()
