import sys
from pathlib import Path

import pandas as pd

from scripts.analyze import analyze_market
from scripts.insights import _render_prompt

BASE = Path(__file__).resolve().parents[1]


def main():
    proc = BASE / "data" / "processed" / "jobs.csv"
    if not proc.exists():
        sys.exit("Run the pipeline first to generate data/processed/jobs.csv")

    jobs = pd.read_csv(proc)
    analysis = analyze_market(jobs, None)
    analysis_dict = analysis.to_dict() if hasattr(analysis, "to_dict") else dict(analysis)
    prompt = _render_prompt(analysis_dict)
    print("\n=== Rendered Insight Prompt ===\n")
    print(prompt)


if __name__ == "__main__":
    main()
