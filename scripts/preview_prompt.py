import json
from pathlib import Path
import pandas as pd
from scripts.analyze import analyze_market
from scripts.insights import _render_prompt  # uses the same function as the pipeline

BASE = Path(__file__).resolve().parents[1]
proc = BASE / "data" / "processed" / "jobs.csv"

if not proc.exists():
    raise SystemExit("Run the pipeline first to generate data/processed/jobs.csv")

jobs = pd.read_csv(proc)
analysis = analyze_market(jobs, None)
prompt = _render_prompt(analysis)
print("\n=== Rendered Insight Prompt ===\n")
print(prompt)
