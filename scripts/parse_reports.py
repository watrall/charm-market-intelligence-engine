import hashlib
import json
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "reports_cache.json"
TEXT_DIR = CACHE_DIR / "reports_text"
TEXT_DIR.mkdir(parents=True, exist_ok=True)


def extract_text_pdf(path: Path) -> str:
    import fitz

    with fitz.open(path) as doc:
        return "\n".join(page.get_text() for page in doc)


def _load_cache():
    """Load report cache with safe JSON deserialization."""
    if CACHE_FILE.exists():
        try:
            raw = CACHE_FILE.read_text(encoding="utf-8")
            # Limit cache size to prevent DoS
            if len(raw) > 10_000_000:  # 10MB limit
                print("Warning: Reports cache too large, resetting")
                return {}
            loaded = json.loads(raw)
            # Validate structure: must be dict
            if isinstance(loaded, dict):
                return loaded
            return {}
        except (json.JSONDecodeError, OSError, TypeError):
            return {}
    return {}


def _save_cache(cache: dict):
    CACHE_FILE.write_text(json.dumps(cache), encoding="utf-8")


def _checksum(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _write_text_file(filename: str, text: str, checksum: str) -> str:
    path = TEXT_DIR / f"{checksum}-{hashlib.sha256(filename.encode('utf-8')).hexdigest()}.txt"
    path.write_text(text, encoding="utf-8")
    return path.name


def _load_text_file(filename: str | None) -> str | None:
    if not filename:
        return None
    path = TEXT_DIR / filename
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def parse_all_reports(report_dir: Path) -> pd.DataFrame:
    report_dir.mkdir(exist_ok=True, parents=True)
    report_dir_resolved = report_dir.resolve()
    cache = _load_cache()
    dirty = False
    rows = []
    seen_files = set()

    for p in report_dir.iterdir():
        if p.suffix.lower() != ".pdf":
            continue
        # Security: skip symlinks and ensure file is within report_dir
        if p.is_symlink():
            print(f"Skipping symlink: {p.name}")
            continue
        resolved = p.resolve()
        if not str(resolved).startswith(str(report_dir_resolved)):
            print(f"Skipping file outside report directory: {p.name}")
            continue
        seen_files.add(str(resolved))
        key = str(resolved)
        meta = cache.get(key, {})
        checksum = _checksum(p)
        txt = _load_text_file(meta.get("text_file")) if meta.get("checksum") == checksum else None
        if txt is None:
            try:
                txt = extract_text_pdf(p)
                text_file = _write_text_file(p.name, txt, checksum)
                cache[key] = {"checksum": checksum, "text_file": text_file}
                dirty = True
            except Exception as e:
                print(f"PDF parse failed for {p.name}: {e}")
                continue
        rows.append({"report_name": p.name, "text": txt})

    if dirty:
        _save_cache(cache)

    # prune text files and cache entries no longer tied to live PDFs
    stale_keys = [k for k in list(cache.keys()) if k not in seen_files]
    for key in stale_keys:
        meta = cache.pop(key, {})
        fname = meta.get("text_file")
        if fname:
            path = TEXT_DIR / fname
            if path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass
    if stale_keys:
        _save_cache(cache)
    # remove orphaned text blobs
    referenced = {meta.get("text_file") for meta in cache.values() if meta.get("text_file")}
    for txt_file in TEXT_DIR.glob("*.txt"):
        if txt_file.name not in referenced:
            try:
                txt_file.unlink()
            except OSError:
                pass

    if not rows:
        return pd.DataFrame(columns=["report_name", "text"])
    return pd.DataFrame(rows)
