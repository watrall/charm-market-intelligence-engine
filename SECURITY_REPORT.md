# SECURITY REPORT

## Baseline
- Date: 2026-02-02 (repo time)
- Branch: main (no new branch created)
- Baseline `git status`: clean before changes
- README.*: read-only, not modified

## System Map
- Stack: Python 3.10+ backend scripts; Streamlit dashboard UI (`dashboard/app.py`); data pipeline under `scripts/`.
- Entry points: `scripts/pipeline.py` (ETL), `dashboard/app.py` (Streamlit wizard + explore tabs), Docker/compose targets for dashboard and pipeline.
- Data stores: CSVs under `data/processed`, SQLite `data/charm.db`, cached JSON/text in `data/cache`, demo snapshot in `demo/processed`.
- External deps/services: HTTP scraping (ACRA/AAA), Nominatim geocoding, optional Google Sheets, optional LLM backends (OpenAI / OpenAI-compatible / Ollama / HF Inference).
- Trust boundaries: user uploads via Streamlit (reports PDFs); environment configuration (.env) for enabling pipeline run/LLM/Sheets; outbound HTTP to LLM endpoints; pipeline subprocess invoked from dashboard.
- Critical assets: processed datasets (`jobs.csv`, `analysis.json`, `insights.md`), uploaded reports, API credentials (OpenAI/HF/Sheets), SQLite DB.

## Findings (ID → OWASP 2025)
- F1 (High, A01/A06): Streamlit file uploads wrote user-controlled filenames directly into `reports/`, enabling path traversal/overwrite.
- F2 (High, A10 Availability): Pipeline logs captured unbounded stdout in memory when run via dashboard, enabling UI-triggered memory exhaustion.
- F3 (High, A05/A06): LLM provider URLs accepted non-http(s) schemes (file/ftp) for OpenAI-compatible/HF inference, enabling SSRF/local file access attempts.

## Commands Run
| Command | Purpose | Result | Notes |
| --- | --- | --- | --- |
| git status -sb | Baseline cleanliness | pass | clean before edits |
| make test | Existing test suite | fail | Python 3.9 venv incompatible with py3.10 `|` types; tests aborted at collection. Code changes unrelated. |
| .venv/bin/python -m ruff check scripts/ dashboard/ tests/ | Lint/sec style gate | pass | All lint findings resolved |

## OWASP Top 10:2025 Matrix
| Item | Applicable | Status | Evidence | Findings | Remediation Summary |
| --- | --- | --- | --- | --- | --- |
| A01 Broken Access Control | Y | Partial | Streamlit wizard gating by env; no auth overall | — | Not addressed in scope (demo/local app) |
| A02 Security Misconfiguration | Y | Partial | `.env.example`, Docker defaults | — | Ensure prod hardening; not modified |
| A03 Software Supply Chain Failures | Y | Unknown | Requirements files present | — | No lockfile audit run |
| A04 Cryptographic Failures | N | — | No custom crypto | — | — |
| A05 Injection | Y | Partial | LLM URL validation added | F3 | Validated provider URLs |
| A06 Insecure Design | Y | Partial | File upload handling | F1,F3 | Added sanitization and URL checks |
| A07 Authentication Failures | Y | Partial | No auth for dashboard | — | Out of current scope |
| A08 Software or Data Integrity Failures | Y | Partial | Pipeline subprocess | F2 | Output capping to prevent DoS |
| A09 Security Logging & Alerting Failures | Y | Unknown | Minimal logging | — | Not changed |
| A10 Mishandling of Exceptional Conditions | Y | Partial | Error handling improved | F1-F3 | Safer handling around uploads/LLM/logs |

## OWASP Mobile Top 10:2024
- Not applicable (no mobile code detected).
