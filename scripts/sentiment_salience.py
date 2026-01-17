import pandas as pd

try:
    from nltk.sentiment import SentimentIntensityAnalyzer
    _NLTK_AVAILABLE = True
except ImportError:
    SentimentIntensityAnalyzer = None
    _NLTK_AVAILABLE = False


def _ensure_vader():
    if not _NLTK_AVAILABLE:
        return None
    try:
        from nltk.data import find
        find("sentiment/vader_lexicon")
    except LookupError:
        import nltk
        nltk.download("vader_lexicon", quiet=True)
    return SentimentIntensityAnalyzer()

def add_sentiment_and_terms(df: pd.DataFrame, text_col: str):
    if df is None or df.empty:
        return df
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
