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
| P1 | Non-numeric `CHARM_SEED` crashes analysis seeding | Fixed | `_get_seed` now defaults safely with warning |
| P1 | Missing/invalid `config/job_patterns.json` stops classification pipeline | Fixed | Pattern loader now falls back to empty patterns with warnings and cache reset |
| P1 | Google Sheets sync exceptions halt pipeline | Fixed | Sync errors now logged and skipped without aborting run |
| P2 | SQLite connection left open after pipeline completes | Fixed | `_persist_to_sqlite` now closes connections via `finally` |

## Fixes Applied
- Hardened `_get_seed` against invalid env input to keep analysis deterministic (scripts/analyze.py); added regression tests (tests/test_analyze.py).
- Pattern loading tolerates missing/invalid configs via env override and cache reset helpers (scripts/data_cleaning.py); added safeguards tests (tests/test_data_cleaning.py).
- Google Sheets sync now catches API/runtime errors so optional integrations cannot crash pipeline (scripts/pipeline.py); regression test added (tests/test_pipeline.py).
- SQLite persistence now closes connections even on errors to avoid leaked file handles (scripts/pipeline.py).

## How to Validate
- Run `make test` for pytest suite.
- Run `make lint` and `make typecheck` for static checks.
- Run `make run-pipeline` (requires data/services) for end-to-end smoke.
- Ensure the virtualenv uses Python 3.10+ (pyproject requirement); a 3.9 venv will fail to import due to `|` unions.
