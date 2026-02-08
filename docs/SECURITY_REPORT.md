# SECURITY REPORT

## Baseline
- Date: 2026-02-08 (second audit pass)
- Branch: main (no new branch created)
- Baseline `git status`: dirty (prior Task 4 modifications present, not committed)
- README.*: read-only, not modified

## System Map
- Stack: Python 3.10+ backend scripts; Streamlit dashboard UI (`dashboard/app.py`); data pipeline under `scripts/`.
- Entry points: `scripts/pipeline.py` (ETL), `dashboard/app.py` (Streamlit wizard + explore tabs), `scripts/docker/entrypoint.sh` (Docker container).
- Data stores: CSVs under `data/processed`, SQLite `data/charm.db`, cached JSON/text in `data/cache`, demo snapshot in `demo/processed`.
- External deps/services: HTTP scraping (ACRA/AAA), Nominatim geocoding, optional Google Sheets (gspread), optional LLM backends (OpenAI / OpenAI-compatible / Ollama / HF Inference).
- Trust boundaries: user uploads via Streamlit (report PDFs); environment configuration (.env) for enabling pipeline run/LLM/Sheets; outbound HTTP to LLM endpoints; pipeline subprocess invoked from dashboard.
- Critical assets: processed datasets (`jobs.csv`, `analysis.json`, `insights.md`), uploaded reports, API credentials (OpenAI/HF/Sheets), SQLite DB.

## Findings (Audit 2 -- 2026-02-08)

| ID | Severity | OWASP | Description | Status |
|----|----------|-------|-------------|--------|
| F1 | High | A02 | Dockerfile ran all processes as root; container compromise gives full filesystem access | Fixed |
| F2 | High | A03 | CI workflow pinned gitleaks action to mutable `@v2` tag; supply-chain risk from tag hijack | Fixed |
| F3 | High | A01 | Google Sheets OAuth scope requested full Drive access (`auth/drive`) instead of minimum required | Fixed |
| F4 | High | A05 | `_load_text_file` in `scripts/parse_reports.py` did not validate filenames; path traversal possible from corrupted cache entries | Fixed |
| F5 | Medium | A02 | Docker entrypoint passed unknown commands directly to `exec`, allowing arbitrary command execution inside container | Fixed |
| F6 | Medium | A10 | `LLM_MAX_TOKENS` env var parsed with bare `int()` crash on non-numeric input | Fixed |

## Prior Findings (Audit 1 -- 2026-02-02)

| ID | Severity | OWASP | Description | Status |
|----|----------|-------|-------------|--------|
| F1-prev | High | A01/A06 | Streamlit file uploads wrote user-controlled filenames directly into `reports/` | Fixed (audit 1) |
| F2-prev | High | A10 | Pipeline logs captured unbounded stdout in memory | Fixed (audit 1) |
| F3-prev | High | A05/A06 | LLM provider URLs accepted non-http(s) schemes | Fixed (audit 1) |

## Commands Run
| Command | Purpose | Result |
|---------|---------|--------|
| `git status -sb` | Baseline status | pass (dirty from Task 4) |
| `.venv/bin/python -m ruff check .` | Lint + security style gate | pass (0 errors after fixes) |
| `.venv/bin/python -m pytest tests/ -q` | Full test suite | pass (56 tests, 0 failures) |
| `git grep -rn 'password\|secret\|api_key' --include='*.py'` | Hardcoded secrets scan | pass (no secrets found) |

## OWASP Top 10:2025 Matrix

| Category | Applicable | Status | Evidence | Findings | Remediation |
|----------|-----------|--------|----------|----------|-------------|
| A01 Broken Access Control | Y | Improved | Sheets scope was over-privileged; path traversal in cache loader | F3, F4, F1-prev | Narrowed OAuth scope to `spreadsheets`; added path validation to `_load_text_file`; upload filenames sanitized (audit 1) |
| A02 Security Misconfiguration | Y | Improved | Dockerfile ran as root; entrypoint allowed arbitrary exec | F1, F5 | Added non-root USER directive; entrypoint rejects unknown commands |
| A03 Software Supply Chain | Y | Improved | CI action used mutable tag | F2 | Pinned `gitleaks/gitleaks-action` to `@v2.3.9` |
| A04 Cryptographic Failures | N | N/A | No custom crypto; TLS handled by runtime | -- | N/A |
| A05 Injection | Y | Improved | Cache filename not validated; LLM URL schemes unrestricted | F4, F3-prev | Path traversal guard in `_load_text_file`; URL scheme validation (audit 1) |
| A06 Insecure Design | Y | Partial | No auth on dashboard (demo/local tool) | F1-prev | Upload sanitization (audit 1); auth out of scope for local tool |
| A07 Authentication Failures | Y | Accepted | No dashboard auth | -- | Accepted risk for local/demo deployment |
| A08 Software & Data Integrity | Y | Improved | CI supply chain risk | F2 | Pinned action version |
| A09 Logging & Monitoring | Y | Unchanged | Standard Python logging; no centralized alerting | -- | Recommend structured logging for production |
| A10 Exceptional Conditions | Y | Improved | Env var parsing crash; unbounded memory | F6, F2-prev | Safe int parsing with fallback; log truncation (audit 1) |

## OWASP Mobile Top 10:2024
- Not applicable (no mobile code detected).
