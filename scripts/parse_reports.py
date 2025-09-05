from pathlib import Path
import pandas as pd

def extract_text_pdf(path: Path) -> str:
    import fitz  # PyMuPDF
    doc = fitz.open(path); return "\n".join([p.get_text() for p in doc])

def parse_all_reports(report_dir: Path) -> pd.DataFrame:
    report_dir.mkdir(exist_ok=True, parents=True)
    rows = []
    for p in report_dir.iterdir():
        if p.suffix.lower() == ".pdf":
            try:
                txt = extract_text_pdf(p)
                if txt:
                    rows.append({"report_name": p.name, "text": txt})
            except Exception as e:
                print(f"PDF parse failed for {p.name}: {e}")
    return pd.DataFrame(rows)
