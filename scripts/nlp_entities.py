import re
from pathlib import Path

import pandas as pd
import spacy

_nlp = None
_skills_df_cache: pd.DataFrame | None = None


def get_nlp():
    global _nlp
    if _nlp is None:
        try:
            _nlp = spacy.load("en_core_web_sm")
        except OSError as exc:  # pragma: no cover - setup guard
            raise RuntimeError("Install spaCy model: python -m spacy download en_core_web_sm") from exc
    return _nlp


def _load_taxonomy(base: Path) -> pd.DataFrame:
    global _skills_df_cache
    if _skills_df_cache is not None:
        return _skills_df_cache

    df = pd.read_csv(base / "skills" / "skills_taxonomy.csv")
    df = df.rename(columns={c: c.lower() for c in df.columns})
    alias_col = "alias" if "alias" in df.columns else ("skill" if "skill" in df.columns else None)
    if alias_col is None:
        raise ValueError("skills_taxonomy.csv must include an 'alias' column")
    norm_col = "normalized_skill" if "normalized_skill" in df.columns else alias_col
    df = df.rename(columns={alias_col: "alias", norm_col: "normalized_skill"})
    df["alias"] = df["alias"].fillna("").astype(str)
    df["normalized_skill"] = df["normalized_skill"].fillna(df["alias"]).astype(str)
    _skills_df_cache = df[["alias", "normalized_skill"]].dropna()
    return _skills_df_cache


def _match_skills(text: str, skills_df: pd.DataFrame):
    text_l = text.lower()
    found = set()
    for _, row in skills_df.iterrows():
        alias = row["alias"].strip()
        normalized = row["normalized_skill"].strip() or alias
        if not alias:
            continue
        pattern = r"(?<![a-zA-Z])" + re.escape(alias.lower()) + r"(?![a-zA-Z])"
        if re.search(pattern, text_l):
            found.add(normalized)
    return sorted(found)


def nlp_enrich(df: pd.DataFrame, is_job: bool) -> pd.DataFrame:
    if df is None:
        return df

    df = df.copy()
    if df.empty:
        for col in ("entities", "orgs", "places", "skills"):
            if col not in df.columns:
                df[col] = []
        return df

    nlp = get_nlp()
    base = Path(__file__).resolve().parents[1]
    skills_df = _load_taxonomy(base)

    text_col = "description" if is_job else "text"
    if text_col not in df.columns:
        df[text_col] = ""

    ents_list, orgs, places, skills_col = [], [], [], []
    for txt in df[text_col].fillna("").tolist():
        doc = nlp(txt[:100000])
        ents_list.append([(e.text, e.label_) for e in doc.ents])
        orgs.append([e.text for e in doc.ents if e.label_ == "ORG"])
        places.append([e.text for e in doc.ents if e.label_ in ("GPE", "LOC")])
        skills_col.append(_match_skills(txt, skills_df))

    df["entities"] = ents_list
    df["orgs"] = orgs
    df["places"] = places
    df["skills"] = skills_col
    return df
