# Changelog

## 2025-09-15 — Schema alignment & reliability
- Added `config/.env.example` so Quick Start instructions work out of the box and documented defaults are real.
- `jobs.csv` now includes the documented fields (`city`, `state`, `job_type`, `seniority`, canonical `url`, salary hints) via lightweight parsing/heuristics. `analysis.json` also contains `top_employers` plus a `run_timestamp`.
- Reports parsing caches extracted text, records `word_count`/`top_entities`, and dedupes inserts in SQLite; unchanged PDFs no longer incur reprocessing.
- Scraper reliability: paginators resolve relative links correctly, job descriptions reuse a persistent cache, and `README` reflects the actual job-board coverage.
- Geocoding uses a configurable user agent with contact email to satisfy Nominatim terms, and Google Sheets tests now call supported helpers to verify credentials/worksheets.
- SQLite inserts keep numeric fields as real `NULL`/float values, and report upserts are idempotent.

## 2025-09-08 — Geospatial map updates
- Added Folium map modes to visualize **skills × seniority × job type**:
  - **Points (clustered):** color by seniority, icon by job type, popup with title/org/skills and a link.
  - **Choropleth (by state):** shows posting intensity; works with or without a skill filter. Uses a local US states GeoJSON if present.
  - **Heatmap:** quick view for dense areas.
- Sidebar filters for date range, skills, seniority, job type, and map mode.
- Kept the UI minimal and readable. If the GeoJSON file isn’t available, the choropleth option is skipped with a clear message.


## 2025-09-05

- A complete, working pipeline for the CHARM project (Cultural Heritage & Archaeological Resource Management).
- Scrapes job postings from ACRA and the AAA Career Center, follows pagination, and fetches full descriptions.
- Lets you drop industry reports (PDFs) into `/reports/`; the pipeline parses the text and runs the same entity/skills pass on it.
- Cleans and de-duplicates results, pulls out salary mentions when available, and geocodes locations with a local cache.
- Extracts entities/skills with spaCy and a small skills taxonomy so “GIS”, “ArcGIS”, and “ArcGIS Pro” roll up consistently.
- Saves to SQLite for durability and to CSV for the dashboard; optionally appends both jobs and reports to Google Sheets.
- Runs a small set of pandas summaries and generates an insight write‑up: a rules-based summary plus an optional LLM brief (OpenAI or Ollama). The prompt lives in `config/insight_prompt.md` so it’s easy to edit without touching code.
- Ships a clean Streamlit dashboard (Plotly + Folium) with a map, top skills, KPIs, and quick downloads.
- Includes n8n workflows for scheduled or manual runs, plus a Mattermost notification that posts a short summary and optional alerts based on changes since the last run.

### Why I built it this way
- **SQLite + CSV + Sheets**: SQLite makes repeat analysis easy and reliable; CSV powers the dashboard; Sheets makes it simple to share raw, structured data with others.
- **n8n on Synology**: clear orchestration you can see, schedule, and explain in one place.
- **Two LLM paths**: OpenAI (cloud) for quality and Ollama (self‑hosted) for cost/governance and offline runs. Same prompt, switchable in `.env`.
- **External prompt**: non‑developers can change what the brief asks for without touching Python.

### Notable details
- Pagination is capped and the scraper de‑duplicates by `job_url` and a content hash to avoid noise.
- Zero‑job runs still write a header‑only CSV and a minimal analysis/insights file so the dashboard and notifications don’t break.
- The dashboard uses a neutral theme (no product branding).
- The scraper’s `USER_AGENT` comes from `.env` so you can advertise a contact URL for responsible use.

### Setup notes
- Copy `config/.env.example` to `.env` and fill in the placeholders (API keys, Google Sheet ID, dashboard URL, user agent, etc.).
- If you use Google Sheets, share the Sheet with your service account email.
- The n8n Execute Command node runs from: `/data/CHARM-Market-Intelligence-Engine` on Synology.

### What’s next (ideas)
- Cluster related skills and show how those clusters move over time.
- Normalize salaries by region and role where possible.
- Optional “history” tab (e.g., in Sheets) for a simple time series of key metrics.
- Alert rules that trigger only when a change crosses a threshold you set (the basics are already included in the n8n notification).
