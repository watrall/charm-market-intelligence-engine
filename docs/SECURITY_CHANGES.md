# SECURITY_CHANGES

## Overview (Audit 2 -- 2026-02-08)
Second-pass security hardening covering container security, CI supply-chain integrity, OAuth scope reduction, path traversal prevention, entrypoint hardening, and safe environment variable parsing. Added regression tests for all fixes.

## Changes by Severity

### High
- **F1 -- Dockerfile non-root user (A02):** Added `useradd appuser` + `USER appuser` to run container processes as non-root. Reordered `chmod` before `USER` switch.
- **F2 -- CI action pinning (A03):** Changed `gitleaks/gitleaks-action@v2` to `gitleaks/gitleaks-action@v2.3.9` to prevent mutable tag hijack.
- **F3 -- Google Sheets scope narrowing (A01):** Replaced `https://www.googleapis.com/auth/drive` (full Drive access) with `https://www.googleapis.com/auth/spreadsheets` (minimum required).
- **F4 -- Path traversal prevention (A05):** Added filename validation to `_load_text_file`: rejects `/`, `\`, dot-prefixed names, and paths that resolve outside `TEXT_DIR`.

### Medium
- **F5 -- Entrypoint hardening (A02):** Replaced open-ended `exec "$cmd"` fallback with explicit error message and `exit 1` for unknown commands.
- **F6 -- Safe env parsing (A10):** Wrapped `int(os.getenv("LLM_MAX_TOKENS", ...))` in try/except with fallback to default 1200.

### Low
- Removed unused `json` import from `dashboard/header.py` (lint cleanup).
- Removed unused `sqlite3` and `Path` imports from `tests/test_pipeline_save.py` (lint cleanup).
- Removed unused `os` and `TEXT_DIR` imports from `tests/test_security_fixes.py` (lint cleanup).

## Changes by Area

### Container & Infrastructure
- `Dockerfile`: Non-root user, proper build ordering.
- `scripts/docker/entrypoint.sh`: Reject unknown commands.

### CI/CD
- `.github/workflows/ci.yml`: Pinned third-party action to immutable version tag.

### Credentials & OAuth
- `scripts/gsheets_sync.py`: Narrowed OAuth scope from full Drive to spreadsheets-only.

### Input Validation
- `scripts/parse_reports.py`: Path traversal guard in `_load_text_file`.
- `scripts/insights.py`: Safe int() parsing for `LLM_MAX_TOKENS`.

### Testing
- `tests/test_security_fixes.py`: 8 new regression tests (7 path traversal cases, 1 env parsing case).

### Lint Cleanup
- `dashboard/header.py`, `tests/test_pipeline_save.py`, `tests/test_security_fixes.py`: Removed unused imports.

## File-by-File Change List
| File | Change |
|------|--------|
| `Dockerfile` | Added non-root `appuser` user; reordered `chmod` before `USER` switch |
| `.github/workflows/ci.yml` | Pinned `gitleaks/gitleaks-action` from `@v2` to `@v2.3.9` |
| `scripts/gsheets_sync.py` | Changed OAuth scope from `auth/drive` to `auth/spreadsheets` |
| `scripts/parse_reports.py` | Added path traversal validation to `_load_text_file` |
| `scripts/docker/entrypoint.sh` | Replaced exec fallback with error + exit for unknown commands |
| `scripts/insights.py` | Wrapped `LLM_MAX_TOKENS` parsing in try/except |
| `tests/test_security_fixes.py` | New file: 8 security regression tests |
| `tests/test_pipeline_save.py` | Removed unused imports (`sqlite3`, `Path`) |
| `dashboard/header.py` | Removed unused import (`json`) |
| `docs/SECURITY_REPORT.md` | Updated with audit 2 findings, OWASP matrix |
| `docs/SECURITY_VERIFICATION.md` | Updated with audit 2 commands and results |
| `docs/SECURITY_CHANGES.md` | This changelog |
| `docs/security.md` | Updated with new guidance sections |

## Commands Executed
- `git status -sb`
- `.venv/bin/python -m ruff check .`
- `.venv/bin/python -m pytest tests/ -q`
- `git grep -rn 'password|secret|api_key' --include='*.py'`

## README Pending Items
- README.* unchanged (read-only per instructions). No pending proposals.
