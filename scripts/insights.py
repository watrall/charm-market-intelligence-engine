    import os, requests, pandas as pd

    def _rules(top_skills_counts):
        lines = ["## Program Recommendations"]
        mapping = {
            "ArcGIS": ["certificate","microlearning","undergrad"],
            "QGIS": ["certificate","microlearning","undergrad"],
            "NAGPRA": ["workshop","post-bacc","grad"],
            "LiDAR": ["certificate","undergrad"],
            "Photogrammetry (3D)": ["certificate","workshop","undergrad"],
            "Project Management": ["certificate","post-bacc","workshop"],
            "Collections Management": ["certificate","post-bacc","workshop"],
            "OSHA 10": ["microlearning","workshop","certificate"]
        }
        for skill, count in top_skills_counts[:12]:
            recs = mapping.get(skill, ["workshop","certificate"])
            lines.append(f"- **{skill}** (demand signal: {count}) → {', '.join(recs)}")
        return "\n".join(lines)

    def _llm_call(prompt: str) -> str:
        if os.getenv("USE_LLM","false").lower() != "true":
            return ""
        provider = os.getenv("LLM_PROVIDER","").lower()
        model = os.getenv("LLM_MODEL","gpt-4o-mini")
        max_tokens = int(os.getenv("LLM_MAX_TOKENS","1200"))

        try:
            if provider == "openai" and os.getenv("OPENAI_API_KEY"):
                from openai import OpenAI
                client = OpenAI()
                resp = client.chat.completions.create(model=model, messages=[
                    {"role":"system","content":"You are a concise market analyst for CRM/Heritage education."},
                    {"role":"user","content":prompt}
                ], temperature=0.2, max_tokens=max_tokens)
                return resp.choices[0].message.content
            if provider == "ollama":
                base = os.getenv("OLLAMA_BASE_URL","http://localhost:11434").rstrip("/")
                r = requests.post(f"{base}/api/generate", json={"model": model, "prompt": prompt, "stream": False}, timeout=120)
                r.raise_for_status(); return r.json().get("response","")
        except Exception as e:
            return f"(LLM failed, falling back) {e}"
        return ""

    from datetime import date
from pathlib import Path

def _render_prompt(analysis, industry: str = "Cultural Resource & Heritage Management") -> str:
    base = Path(__file__).resolve().parents[1]
    template_path = base / "config" / "insight_prompt.md"
    if template_path.exists():
        tpl = template_path.read_text(encoding="utf-8")
    else:
        # Fallback to the previous inline prompt (kept concise)
        tpl = ("Summarize CRM/Heritage labor-market signals for program design. "
               "Return 5 trend statements, 3 emerging skills with rationale, "
               "and 3 program gaps/opportunities across UG/Grad/Certificate/Post-Bacc/Workshop/Microlearning. Max 300 words.")
    # Build replacements
    top_skills = analysis.get("top_skills", [])
    bullets = "
".join([f"- {s} — {c}" for s, c in top_skills[:20]])
    rep = {
        "{{INDUSTRY}}": industry,
        "{{DATE_TODAY}}": str(date.today()),
        "{{NUM_JOBS}}": str(analysis.get("num_jobs", 0)),
        "{{UNIQUE_EMPLOYERS}}": str(analysis.get("unique_employers", 0)),
        "{{GEOCODED}}": str(analysis.get("geocoded", 0)),
        "{{TOP_SKILLS_BULLETS}}": bullets or "- (no skill signals found)",
    }
    for k, v in rep.items():
        tpl = tpl.replace(k, v)
    return tpl

def generate_insights(jobs_df: pd.DataFrame, reports_df: pd.DataFrame, analysis: pd.Series) -> str:
    import os
    import requests
    from openai import OpenAI
        lines = ["# CHARM Market Insights",""]
        lines.append(f"- Total job postings: **{analysis.get('num_jobs',0)}**")
        lines.append(f"- Unique employers: **{analysis.get('unique_employers',0)}**")
        lines.append(f"- Geocoded postings: **{analysis.get('geocoded',0)}**\n")
        top_skills = analysis.get("top_skills", [])
        lines.append("## In-demand Skills")
        for s, c in top_skills[:20]:
            lines.append(f"- {s} — {c}")
        lines.append("")
        lines.append(_rules(top_skills)); lines.append("")
        prompt = _render_prompt(analysis)
        llm = _llm_call(prompt)
        if llm:
            lines.append("## LLM Brief")
            lines.append(llm)
        return "\n".join(lines)
