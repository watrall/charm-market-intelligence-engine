from pathlib import Path

import scripts.parse_reports as parse_reports


def _use_tmp_cache(monkeypatch, tmp_path: Path) -> None:
    cache_file = tmp_path / "reports_cache.json"
    text_dir = tmp_path / "reports_text"
    text_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(parse_reports, "CACHE_FILE", cache_file)
    monkeypatch.setattr(parse_reports, "TEXT_DIR", text_dir)


def test_parse_all_reports_skips_oversized_pdf(monkeypatch, tmp_path):
    _use_tmp_cache(monkeypatch, tmp_path)
    monkeypatch.setenv("REPORT_PDF_MAX_BYTES", "10")
    monkeypatch.setenv("REPORT_PDF_MAX_PAGES", "300")

    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "large.pdf").write_bytes(b"x" * 11)

    def _unexpected_extract(path, max_pages=300):
        raise AssertionError("extract_text_pdf should not run for oversized files")

    monkeypatch.setattr(parse_reports, "extract_text_pdf", _unexpected_extract)

    result = parse_reports.parse_all_reports(report_dir)

    assert result.empty


def test_parse_all_reports_passes_page_limit_to_extractor(monkeypatch, tmp_path):
    _use_tmp_cache(monkeypatch, tmp_path)
    monkeypatch.setenv("REPORT_PDF_MAX_BYTES", "1024")
    monkeypatch.setenv("REPORT_PDF_MAX_PAGES", "3")

    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "sample.pdf").write_bytes(b"pdf")

    seen = {}

    def _fake_extract(path, max_pages=300):
        seen["max_pages"] = max_pages
        return "report text"

    monkeypatch.setattr(parse_reports, "extract_text_pdf", _fake_extract)

    result = parse_reports.parse_all_reports(report_dir)

    assert seen["max_pages"] == 3
    assert list(result["report_name"]) == ["sample.pdf"]
