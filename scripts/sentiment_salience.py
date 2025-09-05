import re, pandas as pd
try:
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    _nltk_ok = True
except Exception:
    _nltk_ok = False
    SentimentIntensityAnalyzer = None

def _ensure_vader():
    if not _nltk_ok: return None
    try:
        from nltk.data import find
        find('sentiment/vader_lexicon')
    except LookupError:
        try:
            import nltk; nltk.download('vader_lexicon')
        except Exception:
            return None
    try:
        return SentimentIntensityAnalyzer()
    except Exception:
        return None

def add_sentiment_and_terms(df: pd.DataFrame, text_col:str):
    if df is None or df.empty: return df
    sia = _ensure_vader()
    sentiments = []
    for txt in df[text_col].fillna("").tolist():
        if sia:
            score = sia.polarity_scores(txt).get("compound", 0.0)
        else:
            score = 0.0
        sentiments.append(score)
    out = df.copy()
    out["sentiment"] = sentiments
    return out
