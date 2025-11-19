from pathlib import Path
import json
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "reports_cache.json"


def extract_text_pdf(path: Path) -> str:
    import fitz  # PyMuPDF

    with fitz.open(path) as doc:
        return "\n".join(page.get_text() for page in doc)


def _load_cache():
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return {}
    return {}


def _save_cache(cache: dict):
    CACHE_FILE.write_text(json.dumps(cache), encoding="utf-8")


def parse_all_reports(report_dir: Path) -> pd.DataFrame:
    report_dir.mkdir(exist_ok=True, parents=True)
    cache = _load_cache()
    dirty = False
    rows = []

    for p in report_dir.iterdir():
        if p.suffix.lower() != ".pdf":
            continue
        key = str(p.resolve())
        meta = cache.get(key, {})
        stat = p.stat()
        cached_sig = (meta.get("mtime"), meta.get("size"))
        sig = (stat.st_mtime, stat.st_size)
        txt = meta.get("text") if cached_sig == sig else None
        if txt is None:
            try:
                txt = extract_text_pdf(p)
                cache[key] = {"mtime": stat.st_mtime, "size": stat.st_size, "text": txt}
                dirty = True
            except Exception as e:
                print(f"PDF parse failed for {p.name}: {e}")
                continue
        rows.append({"report_name": p.name, "text": txt})

    if dirty:
        _save_cache(cache)

    if not rows:
        return pd.DataFrame(columns=["report_name", "text"])
    return pd.DataFrame(rows)
