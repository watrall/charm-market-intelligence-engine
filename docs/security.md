# Security Guidance

## Deployment hardening
- Run with Python 3.10+ to align with type usage and avoid runtime/type errors.
- Keep `ALLOW_PIPELINE_RUN=false` by default; enable only in trusted, authenticated environments.
- Set `DEMO_MODE=0` for real data and ensure uploads are restricted to trusted users.
- Provide least-privilege API keys for Nominatim/Sheets/LLM; store them in `.env`, not source control.

## File handling
- Streamlit uploads are sanitized and de-duplicated before writing to `reports/`; avoid mounting untrusted shared volumes there.
- Reports parsing skips symlinks and files outside the reports directory; keep filesystem permissions restrictive on `data/` and `reports/`.

## LLM endpoints
- Only allow http/https for OpenAI-compatible and HF endpoints. Do not point these to untrusted hosts.
- Ollama endpoints must remain on localhost by design; do not expose to the internet.

## Logging & monitoring
- Pipeline output from UI is now truncated when large; for full logs, run the pipeline via CLI and persist logs to disk.
- Monitor outbound traffic to LLM providers and job board sources for rate limits and abuse detection.

## Testing & validation
- Run `make test` under Python 3.10+ to exercise security regressions (upload sanitization, log truncation, LLM URL validation).
- Use `make lint` / `make typecheck` to catch static issues before deploy.

