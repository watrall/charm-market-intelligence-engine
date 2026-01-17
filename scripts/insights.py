from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import pandas as pd
import requests

TopSkill = Tuple[str, int]


def _normalize_top_skills(values: Iterable[Sequence]) -> List[TopSkill]:
    normalized: List[TopSkill] = []
    for item in values or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            skill = str(item[0])
            try:
                count = int(item[1])
            except (TypeError, ValueError):
                continue
            normalized.append((skill, count))
    return normalized


def _rules(top_skills_counts: List[TopSkill], limit: int = 12) -> str:
    lines = ["## Program Recommendations"]
    mapping = {
        "ArcGIS": ["certificate", "microlearning", "undergrad"],
        "QGIS": ["certificate", "microlearning", "undergrad"],
        "NAGPRA": ["workshop", "post-bacc", "grad"],
        "LiDAR": ["certificate", "undergrad"],
        "Photogrammetry (3D)": ["certificate", "workshop", "undergrad"],
        "Project Management": ["certificate", "post-bacc", "workshop"],
        "Collections Management": ["certificate", "post-bacc", "workshop"],
        "OSHA 10": ["microlearning", "workshop", "certificate"],
    }
    for skill, count in top_skills_counts[:limit]:
        recs = mapping.get(skill, ["workshop", "certificate"])
        lines.append(f"- **{skill}** (demand signal: {count}) → {', '.join(recs)}")
    return "\n".join(lines)


def _llm_call(prompt: str) -> str:
    if os.getenv("USE_LLM", "false").lower() != "true":
        return ""

    provider = os.getenv("LLM_PROVIDER", "").lower()
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1200"))

    try:
        if provider in {"openai", "openai_compat"} and os.getenv("OPENAI_API_KEY"):
            from openai import OpenAI

            client_kwargs = {}
            if provider == "openai_compat":
                base_url = os.getenv("LLM_BASE_URL", "").strip()
                if base_url:
                    client_kwargs["base_url"] = base_url
            client = OpenAI(**client_kwargs)
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a concise market analyst for CRM/Heritage education.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=max_tokens,
            )
            return response.choices[0].message.content or ""

        if provider == "ollama":
            base = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
            # Basic SSRF protection: only allow localhost/127.0.0.1 for Ollama
            from urllib.parse import urlparse
            parsed = urlparse(base)
            allowed_hosts = {"localhost", "127.0.0.1", "::1"}
            if parsed.hostname not in allowed_hosts:
                return "(Ollama URL must be localhost for security)"
            response = requests.post(
                f"{base}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
                timeout=120,
            )
            response.raise_for_status()
            return response.json().get("response", "")
    except Exception as exc:
        return f"(LLM call failed) {type(exc).__name__}"

    return ""


def _render_prompt(analysis: dict, industry: str = "Cultural Resource & Heritage Management") -> str:
    base = Path(__file__).resolve().parents[1]
    template_path = base / "config" / "insight_prompt.md"
    if template_path.exists():
        template = template_path.read_text(encoding="utf-8")
    else:
        template = (
            "Summarize CRM/Heritage labor-market signals for program design. "
            "Return 5 trend statements, 3 emerging skills with rationale, and 3 program gaps/opportunities across UG/Grad/Certificate/Post-Bacc/Workshop/Microlearning. Max 300 words."
        )

    top_skills = _normalize_top_skills(analysis.get("top_skills", []))
    bullets = "\n".join([f"- {skill} — {count}" for skill, count in top_skills[:20]])

    replacements = {
        "{{INDUSTRY}}": industry,
        "{{DATE_TODAY}}": str(date.today()),
        "{{NUM_JOBS}}": str(analysis.get("num_jobs", 0)),
        "{{UNIQUE_EMPLOYERS}}": str(analysis.get("unique_employers", 0)),
        "{{GEOCODED}}": str(analysis.get("geocoded", 0)),
        "{{TOP_SKILLS_BULLETS}}": bullets or "- (no skill signals found)",
    }
    for needle, value in replacements.items():
        template = template.replace(needle, value)
    return template


def generate_insights(
    jobs_df: pd.DataFrame,
    reports_df: pd.DataFrame,
    analysis: pd.Series | dict,
) -> str:
    analysis_dict = analysis.to_dict() if hasattr(analysis, "to_dict") else dict(analysis)
    top_skills = _normalize_top_skills(analysis_dict.get("top_skills", []))

    lines: List[str] = ["# CHARM Market Insights", ""]
    lines.append(f"- Total job postings: **{analysis_dict.get('num_jobs', 0)}**")
    lines.append(f"- Unique employers: **{analysis_dict.get('unique_employers', 0)}**")
    lines.append(f"- Geocoded postings: **{analysis_dict.get('geocoded', 0)}**\n")

    lines.append("## In-demand Skills")
    if not top_skills:
        lines.append("- (no skill signals found)")
    else:
        for skill, count in top_skills[:20]:
            lines.append(f"- {skill} — {count}")
    lines.append("")

    lines.append(_rules(top_skills))
    lines.append("")

    prompt = _render_prompt(analysis_dict)
    llm_output = _llm_call(prompt)
    if llm_output:
        lines.append("## LLM Brief")
        lines.append(llm_output)

    return "\n".join(lines)
