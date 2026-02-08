import json

from reports.context import build_report_context


def test_build_report_context_handles_malformed_top_skills(tmp_path):
    proc_dir = tmp_path
    (proc_dir / "jobs.csv").write_text(
        "source,title,company,location,date_posted,job_url,description\n",
        encoding="utf-8",
    )
    (proc_dir / "analysis.json").write_text(
        json.dumps({"top_skills": ["oops"], "num_jobs": 0}),
        encoding="utf-8",
    )
    (proc_dir / "insights.md").write_text("", encoding="utf-8")

    context = build_report_context(proc_dir)

    assert context["top_skills"] == []
    assert isinstance(context["implications"], list)
