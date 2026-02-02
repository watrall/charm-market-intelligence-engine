# SECURITY_CHANGES

## Overview
Security hardening for Streamlit file ingest, LLM endpoint validation, and dashboard-run pipeline output bounding. Added targeted tests and documentation without touching README.*.

## Changes by Severity
- High: Sanitized upload filenames and prevented overwrite/path traversal (F1). Added endpoint scheme validation for OpenAI-compatible/HF backends to block SSRF vectors (F3). Bounded pipeline stdout capture to avoid DoS via unbounded memory (F2).
- Medium: None.
- Low: None.

## Changes by Area
- Validation: Upload filename sanitization; LLM URL scheme/host checks.
- Config/Execution: Pipeline log capture capped with truncation flag.
- Docs: Security report, verification log, guidance, and changelog updates.
- Testing: Added unit tests for new guards.

## File-by-File Change List
- `dashboard/app.py`: Added `sanitize_upload_name` helper; normalized and de-duplicated uploaded filenames to prevent traversal/overwrite.
- `dashboard/pipeline_runner.py`: Capped captured stdout with truncation notice to avoid memory exhaustion from pipeline runs.
- `scripts/insights.py`: Validated OpenAI-compatible and HF inference URLs (http/https only, host required) before outbound calls.
- `tests/test_dashboard_uploads.py`: New tests for upload filename sanitization.
- `tests/test_pipeline_runner_limits.py`: New test ensuring pipeline output truncation signaling.
- `tests/test_insights_security.py`: New tests validating rejection of unsafe LLM endpoint schemes.
- `SECURITY_REPORT.md`: Baseline, system map, findings, OWASP matrix, command log.
- `SECURITY_VERIFICATION.md`: Commands run and outcomes.
- `SECURITY_CHANGES.md`: This changelog.
- `docs/security.md`: Added concise security guidance and operational guardrails.

## Commands Executed
- `git status -sb`
- `make test` (fails on Python 3.9; project requires 3.10+)

## README Pending Items
- README.* unchanged (read-only per instructions). No pending proposals.

