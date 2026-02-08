# QUALITY_AUDIT

## Repo Profile (Phase 0)
- Stack: Python 3.10; data/ETL in `scripts/`, Streamlit UI in `dashboard/app.py`.
- Packaging/tooling: `pyproject.toml` with ruff, mypy, pytest; deps in `requirements*.txt`.
- Entry: `scripts/pipeline.py` orchestrates scrape → clean → NLP → sentiment → geocode → persist.
- Data/config: `config/job_patterns.json`, `skills/skills_taxonomy.csv`, env via `.env`/`.env.example`.
- Outputs: CSV/analysis/insights in `data/processed`; SQLite at `data/charm.db`; geocache at `data/geocache.csv`.
- Optional services: Nominatim geocoding, Google Sheets sync, spaCy `en_core_web_sm`, optional clustering via scikit-learn.
- Tests: pytest suite in `tests/`; fixtures in `tests/conftest.py`.
- Commands: `make test`, `make lint`, `make typecheck`, `make ci`; runtime `make run-pipeline`, `make run-dashboard`.
- Conventions: stable column ordering in pipeline CSVs; deterministic random seed via `CHARM_SEED`.
- Risks noted: external API reliance (Nominatim/Sheets), cached globals for patterns/NLP, SQLite WAL writes.

## Findings
| Severity | Issue | Status | Notes |
| --- | --- | --- | --- |
| P1 | Streamlit multipage import called `set_page_config` twice, crashing the Ingest page | Fixed | Config now idempotent and only set inside Explore entrypoint (`dashboard/app.py`) |
| P1 | Wizard uploads allowed unbounded file sizes, risking disk exhaustion on hosted deploys | Fixed | Added 25 MB per-file cap with explicit skips and helper for safe persistence (`dashboard/app.py`) |
| P1 | Geocoder could issue unbounded Nominatim lookups, risking throttling/ban | Fixed | New `GEOCODE_MAX_NEW` limit (default 500) truncates new requests and logs when hit (`scripts/geocode.py`) |
| P1 | Non-numeric `CHARM_SEED` crashes analysis seeding | Fixed | `_get_seed` now defaults safely with warning |
| P1 | Missing/invalid `config/job_patterns.json` stops classification pipeline | Fixed | Pattern loader now falls back to empty patterns with warnings and cache reset |
| P1 | Google Sheets sync exceptions halt pipeline | Fixed | Sync errors now logged and skipped without aborting run |
| P1 | Streamlit uploads could overwrite/traverse paths | Fixed | Filenames are sanitized and de-duplicated before write (`dashboard/app.py`) |
| P1 | Dashboard pipeline run could grow unbounded output in memory | Fixed | Output capture capped and flagged when truncated (`dashboard/pipeline_runner.py`) |
| P1 | OpenAI-compatible / HF inference URLs allowed non-http schemes | Fixed | Scheme/host validation added to block file/ftp SSRF vectors (`scripts/insights.py`) |
| P2 | SQLite connection left open after pipeline completes | Fixed | `_persist_to_sqlite` now closes connections via `finally` |
| P1 | `_save_processed_data` crashes when NLP disabled (no `skills` column) | Fixed | Guarded skills transform behind column existence check (`scripts/pipeline.py`) |
| P1 | `upsert_jobs` / `upsert_reports` crash when optional columns missing | Fixed | Switched from `df[cols]` to `row.get(c)` iteration, returns `None` for absent columns (`scripts/db.py`) |
| P1 | Report skills analysis silently corrupts data when skills are strings | Fixed | Used `_ensure_skill_lists()` instead of raw `.tolist()` to prevent character-level iteration (`scripts/analyze.py`) |
| P2 | Pipeline output buffer truncation is O(n^2) due to `list.pop(0)` | Fixed | Replaced list with `collections.deque` and `popleft()` for O(1) removals (`dashboard/pipeline_runner.py`) |
| P2 | Duplicate `_load_previous_jobs_csv` / `_load_previous_reports_csv` | Fixed | Merged into single `_load_previous_csv(proc_dir, name)` helper (`scripts/pipeline.py`) |

## Fixes Applied
- Streamlit page config is now applied only once per session to prevent multipage crashes (dashboard/app.py).
- Ingest uploads enforce a 25 MB cap, skipping oversize files with user feedback; extracted helper for safer saves (dashboard/app.py; tests/test_dashboard_upload_limits.py).
- Geocoding is bounded via `GEOCODE_MAX_NEW` and accepts an injectable cache path for safer tests; added limit regression test (scripts/geocode.py; tests/test_geocode_limits.py).
- Hardened `_get_seed` against invalid env input to keep analysis deterministic (scripts/analyze.py); added regression tests (tests/test_analyze.py).
- Pattern loading tolerates missing/invalid configs via env override and cache reset helpers (scripts/data_cleaning.py); added safeguards tests (tests/test_data_cleaning.py).
- Google Sheets sync now catches API/runtime errors so optional integrations cannot crash pipeline (scripts/pipeline.py); regression test added (tests/test_pipeline.py).
- SQLite persistence now closes connections even on errors to avoid leaked file handles (scripts/pipeline.py).
- Streamlit uploads now sanitize filenames and avoid overwrites to prevent traversal/overwrite (dashboard/app.py; tests/test_dashboard_uploads.py).
- Pipeline output capture bounded with truncation flag to avoid memory blow-ups in UI runs (dashboard/pipeline_runner.py; tests/test_pipeline_runner_limits.py).
- LLM provider URL validation blocks non-http schemes for OpenAI-compatible and HF inference backends (scripts/insights.py; tests/test_insights_security.py).
- `_save_processed_data` now checks for `skills` column before transforming, preventing KeyError when NLP step is disabled (scripts/pipeline.py; tests/test_pipeline_save.py).
- `upsert_jobs` and `upsert_reports` use `row.get(c)` instead of `df[cols]` indexing, returning NULL for columns not present in the DataFrame (scripts/db.py; tests/test_pipeline_save.py).
- Report skills analysis now uses `_ensure_skill_lists()` to handle string-format skills, preventing `chain.from_iterable` from iterating characters instead of tokens (scripts/analyze.py; tests/test_pipeline_save.py).
- Pipeline output buffer uses `collections.deque` with `popleft()` instead of `list.pop(0)` for O(1) removals during truncation (dashboard/pipeline_runner.py).
- Duplicate `_load_previous_jobs_csv` / `_load_previous_reports_csv` consolidated into `_load_previous_csv(proc_dir, name)` (scripts/pipeline.py).

## How to Validate
- Run `make test` for pytest suite.
- Run `make lint` and `make typecheck` for static checks.
- Run `make run-pipeline` (requires data/services) for end-to-end smoke.
- Ensure the virtualenv uses Python 3.10+ (pyproject requirement); a 3.9 venv will fail to import due to `|` unions.
- Targeted checks run during this audit: `pytest tests/test_dashboard_upload_limits.py tests/test_geocode_limits.py tests/test_pipeline_save.py`.
