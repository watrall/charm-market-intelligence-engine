import re
from pathlib import Path
import pandas as pd
import spacy

_nlp = None
def get_nlp():
    global _nlp
    if _nlp is None:
        try:
            _nlp = spacy.load("en_core_web_sm")
        except OSError:
            raise RuntimeError("Install spaCy model: python -m spacy download en_core_web_sm")
    return _nlp

def _load_taxonomy(base:Path):
    return pd.read_csv(base / "skills" / "skills_taxonomy.csv")

def _match_skills(text:str, skills_df:pd.DataFrame):
    text_l = text.lower()
    found = set()
    for skill in skills_df["skill"].dropna().unique():
        pat = r"(?<![a-zA-Z])" + re.escape(skill.lower()) + r"(?![a-zA-Z])"
        if re.search(pat, text_l):
            found.add(skill)
    return sorted(found)

def nlp_enrich(df: pd.DataFrame, is_job: bool) -> pd.DataFrame:
    if df is None or df.empty: return df
    nlp = get_nlp()
    base = Path(__file__).resolve().parents[1]
    skills_df = _load_taxonomy(base)

    text_col = "description" if is_job else "text"
    ents_list, orgs, places, skills_col = [], [], [], []
    for txt in df[text_col].fillna("").tolist():
        doc = nlp(txt[:100000])
        ents_list.append([(e.text, e.label_) for e in doc.ents])
        orgs.append([e.text for e in doc.ents if e.label_=="ORG"])
        places.append([e.text for e in doc.ents if e.label_ in ("GPE","LOC")])
        skills_col.append(_match_skills(txt, skills_df))

    out = df.copy()
    out["entities"] = ents_list
    out["orgs"] = orgs
    out["places"] = places
    out["skills"] = skills_col
    return out
