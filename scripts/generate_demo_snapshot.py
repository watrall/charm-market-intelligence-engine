"""Generate a publish safe demo snapshot under demo/processed.

This is intended for the demo branch and Streamlit Community Cloud deployment.
It creates a realistic but synthetic dataset so the demo is stable and does not
depend on scraping, API keys, or external services.
"""

import csv
import json
import random
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DEMO_DIR = BASE / "demo"
PROC_DIR = DEMO_DIR / "processed"


@dataclass(frozen=True)
class Place:
    city: str
    state: str
    lat: float
    lon: float


PLACES = [
    Place("Phoenix", "AZ", 33.4484, -112.0740),
    Place("Tucson", "AZ", 32.2226, -110.9747),
    Place("Denver", "CO", 39.7392, -104.9903),
    Place("Albuquerque", "NM", 35.0844, -106.6504),
    Place("Salt Lake City", "UT", 40.7608, -111.8910),
    Place("Las Vegas", "NV", 36.1699, -115.1398),
    Place("San Diego", "CA", 32.7157, -117.1611),
    Place("Los Angeles", "CA", 34.0522, -118.2437),
    Place("Sacramento", "CA", 38.5816, -121.4944),
    Place("Portland", "OR", 45.5152, -122.6784),
    Place("Seattle", "WA", 47.6062, -122.3321),
    Place("Boise", "ID", 43.6150, -116.2023),
    Place("Chicago", "IL", 41.8781, -87.6298),
    Place("Minneapolis", "MN", 44.9778, -93.2650),
    Place("Austin", "TX", 30.2672, -97.7431),
    Place("San Antonio", "TX", 29.4241, -98.4936),
    Place("Atlanta", "GA", 33.7490, -84.3880),
    Place("Raleigh", "NC", 35.7796, -78.6382),
    Place("Washington", "DC", 38.9072, -77.0369),
    Place("Boston", "MA", 42.3601, -71.0589),
]

SENIORITY = ["entry", "mid", "senior", "lead/PI"]
JOB_TYPES = ["field-tech", "lab/analyst", "architectural-historian", "pm/pi"]


def _read_skill_aliases(limit: int = 200) -> list[str]:
    fp = BASE / "skills" / "skills_taxonomy.csv"
    if not fp.exists():
        return ["ArcGIS", "NEPA", "Section 106", "LiDAR", "NAGPRA", "QGIS"]
    out: list[str] = []
    with fp.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            alias = (row.get("normalized_skill") or row.get("alias") or "").strip()
            if alias and alias not in out:
                out.append(alias)
            if len(out) >= limit:
                break
    # Add a few very common CRM terms so the demo feels familiar.
    for s in ["ArcGIS", "NEPA", "Section 106", "LiDAR", "NAGPRA", "QGIS", "Project Management"]:
        if s not in out:
            out.insert(0, s)
    return out[:limit]


def _pick_skills(skill_pool: list[str]) -> list[str]:
    n = random.randint(3, 8)
    picks = random.sample(skill_pool, k=min(n, len(skill_pool)))
    # Make sure some common signals show up across the dataset.
    for must in ["Section 106", "NEPA"]:
        if random.random() < 0.35 and must in skill_pool and must not in picks:
            picks.append(must)
    return sorted(set(picks))


def _salary_for(seniority: str) -> tuple[float | None, float | None]:
    if seniority == "entry":
        low = random.randint(42000, 56000)
        high = low + random.randint(4000, 12000)
    elif seniority == "mid":
        low = random.randint(55000, 75000)
        high = low + random.randint(7000, 18000)
    elif seniority == "senior":
        low = random.randint(72000, 98000)
        high = low + random.randint(10000, 25000)
    else:
        low = random.randint(90000, 125000)
        high = low + random.randint(15000, 35000)
    if random.random() < 0.25:
        return None, None
    return float(low), float(high)


def _job_title(job_type: str, seniority: str) -> str:
    titles = {
        "field-tech": ["Field Technician", "Archaeology Technician", "Crew Chief", "Field Supervisor"],
        "lab/analyst": ["Lab Analyst", "Collections Specialist", "Artifact Analyst", "Zooarchaeology Analyst"],
        "architectural-historian": ["Architectural Historian", "Historic Preservation Specialist", "Cultural Resources Planner"],
        "pm/pi": ["Project Manager", "Principal Investigator", "Project Director", "Program Manager"],
    }
    base = random.choice(titles.get(job_type, ["CRM Specialist"]))
    if seniority in {"senior", "lead/PI"} and not base.lower().startswith(("senior", "lead")):
        if random.random() < 0.6:
            base = f"Senior {base}"
    return base


def _company_name(i: int) -> str:
    names = [
        "Heritage Research Group",
        "Cultural Resource Consultants",
        "Western CRM Partners",
        "ArchaeoLab Services",
        "Preservation Strategy Studio",
        "GeoCultural Analytics",
        "Regional Heritage Associates",
        "Frontier Cultural Resources",
        "Sagebrush Archaeology",
        "Bay Area Preservation",
        "Capitol Heritage Advisors",
        "Great Lakes CRM",
        "High Plains Cultural Resources",
        "Sunbelt Heritage Solutions",
    ]
    return names[i % len(names)]


def generate(n_jobs: int = 240, seed: int = 42) -> None:
    random.seed(seed)
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    (DEMO_DIR / "reports").mkdir(parents=True, exist_ok=True)

    skill_pool = _read_skill_aliases()

    start = date.today() - timedelta(days=120)
    jobs: list[dict[str, object]] = []

    for i in range(n_jobs):
        place = random.choice(PLACES)
        seniority = random.choice(SENIORITY)
        job_type = random.choice(JOB_TYPES)
        title = _job_title(job_type, seniority)
        company = _company_name(i)
        skills = _pick_skills(skill_pool)
        low, high = _salary_for(seniority)
        url = f"https://example.com/demo/job/{i+1}"
        desc = (
            f"{title} at {company}. "
            f"Key skills include {', '.join(skills[:5])}. "
            "This is synthetic demo data."
        )
        posted = start + timedelta(days=random.randint(0, 120))
        jobs.append(
            {
                "source": random.choice(["ACRA", "AAA"]),
                "title": title,
                "company": company,
                "location": f"{place.city}, {place.state}",
                "city": place.city,
                "state": place.state,
                "lat": place.lat,
                "lon": place.lon,
                "date_posted": posted.isoformat(),
                "job_type": job_type,
                "seniority": seniority,
                "skills": ";".join(skills),
                "skills_list": json.dumps(skills),
                "salary_min": low,
                "salary_max": high,
                "currency": "USD" if low or high else "",
                "url": url,
                "description": desc,
                "sentiment": round(random.uniform(-0.2, 0.5), 3),
            }
        )

    # Write jobs.csv
    job_cols = [
        "source",
        "title",
        "company",
        "location",
        "city",
        "state",
        "lat",
        "lon",
        "date_posted",
        "job_type",
        "seniority",
        "skills",
        "skills_list",
        "salary_min",
        "salary_max",
        "currency",
        "url",
        "description",
        "sentiment",
    ]
    with (PROC_DIR / "jobs.csv").open("w", encoding="utf-8", newline="") as handle:
        w = csv.DictWriter(handle, fieldnames=job_cols)
        w.writeheader()
        for row in jobs:
            w.writerow({k: row.get(k, "") for k in job_cols})

    # Compute analysis.json
    skill_counts: Counter[str] = Counter()
    employer_counts: Counter[str] = Counter()
    geocoded = 0
    for j in jobs:
        employer_counts[str(j.get("company", "")).strip()] += 1
        skills = str(j.get("skills", "")).split(";") if j.get("skills") else []
        for s in skills:
            s = s.strip()
            if s:
                skill_counts[s] += 1
        if j.get("lat") and j.get("lon"):
            geocoded += 1

    analysis = {
        "schema_version": "1.0",
        "num_jobs": len(jobs),
        "unique_employers": len(employer_counts),
        "geocoded": geocoded,
        "top_skills": skill_counts.most_common(30),
        "top_employers": employer_counts.most_common(20),
        "report_skills": [],
        "run_timestamp": f"{date.today().isoformat()}T12:00:00Z",
    }
    (PROC_DIR / "analysis.json").write_text(json.dumps(analysis, indent=2), encoding="utf-8")

    # Create a small reports.csv so the demo feels complete.
    reports = [
        {"report_name": "CRM_Industry_Report_2025.pdf", "word_count": 4200, "skills": "NEPA;Section 106;Project Management", "top_entities": "BLM, USFS, SHPO", "text": "Synthetic demo report text."},
        {"report_name": "Heritage_Tech_Trends_2025.pdf", "word_count": 3100, "skills": "LiDAR;Photogrammetry (3D);GIS", "top_entities": "NPS, State DOT", "text": "Synthetic demo report text."},
        {"report_name": "Collections_and_Curation_Brief_2025.pdf", "word_count": 2200, "skills": "Collections Management;Conservation Planning;Accessioning", "top_entities": "University Museum", "text": "Synthetic demo report text."},
    ]
    with (PROC_DIR / "reports.csv").open("w", encoding="utf-8", newline="") as handle:
        w = csv.DictWriter(handle, fieldnames=["report_name", "word_count", "skills", "top_entities", "text"])
        w.writeheader()
        for row in reports:
            w.writerow(row)

    # Write insights.md with a pre generated LLM brief section.
    top_skills = "\n".join([f"- {s} - {n}" for s, n in analysis["top_skills"][:12]])
    insights = "\n".join(
        [
            "# CHARM Market Insights",
            "",
            f"- Total job postings: **{analysis['num_jobs']}**",
            f"- Unique employers: **{analysis['unique_employers']}**",
            f"- Geocoded postings: **{analysis['geocoded']}**",
            "",
            "## In-demand Skills",
            top_skills or "- (no skill signals found)",
            "",
            "## Program Recommendations",
            "- Strengthen compliance focused offerings that cover NEPA and Section 106 with applied case work.",
            "- Offer short GIS refreshers that map to real deliverables such as field forms, maps, and reporting.",
            "- Add a project management track that supports budgeting, QA, and client communication.",
            "",
            "## LLM Brief",
            "This brief is pre generated for the public demo. The hosted demo does not call any LLM.",
            "",
            "Trends",
            "- Compliance work remains a steady baseline across regions, with NEPA and Section 106 recurring in many postings.",
            "- GIS stays central and appears across job types, not only specialist roles.",
            "- Project management signals increase with seniority and is often paired with client and schedule ownership.",
            "- Lab and collections roles appear consistently, suggesting stable demand beyond field crews.",
            "- Multi skill expectations are common, so modular training options are more useful than single topic courses.",
            "",
            "Emerging skills",
            "- LiDAR and photogrammetry appear as differentiators for survey and documentation work.",
            "- Data management and reporting automation show up in roles that touch deliverables and QA.",
            "- Stakeholder and public facing communication is showing up more often in program and lead roles.",
            "",
            "Program gaps and opportunities",
            "- A practical compliance studio that pairs NEPA and Section 106 with deliverable writing and review.",
            "- A certificate path that combines GIS, QA, and project workflows with small portfolio outputs.",
            "- Microlearning modules for field safety, data collection standards, and basic lab workflows.",
            "",
        ]
    )
    (PROC_DIR / "insights.md").write_text(insights, encoding="utf-8")


if __name__ == "__main__":
    generate()
