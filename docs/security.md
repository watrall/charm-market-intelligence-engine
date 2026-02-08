# Security Guidance

## Deployment hardening
- Run with Python 3.10+ to align with type usage and avoid runtime/type errors.
- Keep `ALLOW_PIPELINE_RUN=false` by default; enable only in trusted, authenticated environments.
- Set `DEMO_MODE=0` for real data and ensure uploads are restricted to trusted users.
- Provide least-privilege API keys for Nominatim/Sheets/LLM; store them in `.env`, not source control.

## Container security
- The Docker image runs as a non-root `appuser` for defense in depth.
- The entrypoint only accepts `dashboard` or `pipeline` commands; unknown commands are rejected.
- Mount only necessary volumes (`data/`, `reports/`, `secrets/`); avoid mounting the host root.

## File handling
- Streamlit uploads are sanitized and de-duplicated before writing to `reports/`; avoid mounting untrusted shared volumes there.
- Reports parsing skips symlinks and files outside the reports directory; keep filesystem permissions restrictive on `data/` and `reports/`.
- The text cache loader (`_load_text_file`) rejects filenames containing path separators, dot-prefixed names, and resolved paths outside the cache directory.

## OAuth and API scopes
- Google Sheets integration uses `auth/spreadsheets` (not `auth/drive`) for minimum required access.
- If switching to a service account, grant only the specific spreadsheet rather than org-wide access.

## LLM endpoints
- Only allow http/https for OpenAI-compatible and HF endpoints. Do not point these to untrusted hosts.
- Ollama endpoints must remain on localhost by design; do not expose to the internet.
- `LLM_MAX_TOKENS` tolerates non-numeric values gracefully (defaults to 1200).

## CI/CD
- Third-party GitHub Actions are pinned to specific version tags (not mutable major tags) to prevent supply-chain hijack.
- The `gitleaks` secrets scanner runs on every push and PR.

## Logging & monitoring
- Pipeline output from UI is truncated when large; for full logs, run the pipeline via CLI and persist logs to disk.
- Monitor outbound traffic to LLM providers and job board sources for rate limits and abuse detection.

## Testing & validation
- Run `make test` under Python 3.10+ to exercise security regressions (path traversal, upload sanitization, log truncation, LLM URL validation, env parsing).
- Use `make lint` / `make typecheck` to catch static issues before deploy.
