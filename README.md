# CHARM — Streamlined Market Intelligence Engine

This is a clean, runnable reference implementation for automated market analysis in the cultural resource & heritage management space (designed originally to be hosted and run on a local Synology NAS).
CHARM = Cultural Heritage & Archaeologcial Resource Management

**Outcomes:** scrape jobs → clean/dedupe → parse PDFs → spaCy NER + skills → sentiment → geocode → analysis → insights → SQLite/CSVs → optional Google Sheets → Streamlit dashboard (Folium + Plotly).

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.example .env

# Optional models
python -m spacy download en_core_web_sm
python -c "import nltk; nltk.download('vader_lexicon')"

# Run pipeline
python scripts/pipeline.py

# Start dashboard
streamlit run dashboard/app.py
```

## Environment placeholders
The sample `.env` uses **explicit placeholders** anywhere a key/ID/secret is needed. Replace these with real values:

- `GOOGLE_SERVICE_ACCOUNT_FILE=ENTER_PATH_TO_SERVICE_ACCOUNT_JSON_HERE`
  - Path to your service account key file (e.g., `secrets/service_account.json`).
- `GOOGLE_SHEET_ID=ENTER_GOOGLE_SHEET_ID_HERE`
  - The ID from your Sheet URL.
- `OPENAI_API_KEY=ENTER_OPENAI_API_KEY_HERE`
  - Only needed if `USE_LLM=true` and `LLM_PROVIDER=openai`.

After editing, verify:
```bash
python scripts/gsheets_test.py   # check Google Sheets access
python scripts/pipeline.py       # run the end-to-end pipeline
```

## Architecture
- **n8n orchestration**: Cron + webhook → one Execute Command (runs the pipeline).
- **Python pipeline**: scraping → cleaning/dedupe → report parsing → NLP → sentiment → geocoding → analysis → insights → persistence.
- **Storage**: CSVs for the dashboard + **SQLite** for durable querying; **Google Sheets** for sharing raw rows.
- **Dashboard**: Streamlit with **Plotly** charts and a **Folium** map (heatmap + clustered markers).
- **LLM (optional)**: Brief insights; off by default.

## n8n Scheduling (Synology)
Import `n8n/charm_workflow.json` and point Execute Command to:
```bash
bash -lc "cd /data/charm-market-intelligence-engine && source .venv/bin/activate && python scripts/pipeline.py"
```

## Adapting to Other Industries
- Add parsers in `scripts/scrape_jobs.py` for new job boards.
- Update `skills/skills_taxonomy.csv` with additional skills/aliases.
- Expand rules in `scripts/insights.py` to map skills → program formats.

## Google Sheets Integration (ON by default)
1. Enable **Google Sheets API** and **Google Drive API** in GCP.
2. Create a **Service Account**, download the JSON key to `secrets/service_account.json` (or your path).
3. Set `GOOGLE_SHEET_ID` and `GOOGLE_SERVICE_ACCOUNT_FILE` in `.env` (replace the placeholders).
4. Share the Sheet with the service account email as **Editor**.
5. Test:
```bash
python scripts/gsheets_test.py
```

## Troubleshooting
- **No jobs scraped**: Update CSS selectors in `scripts/scrape_jobs.py`.
- **spaCy model missing**: `python -m spacy download en_core_web_sm`.
- **Sheets append fails**: Check `GOOGLE_SHEET_ID`, service account permissions, and network egress.
- **Geocoding slow**: Nominatim is rate-limited; a geocache reduces repeat lookups.

## Insight prompt (external & editable)
The LLM question set lives in `config/insight_prompt.md`. It’s plain text with **{{variables}}** you can edit:
- `{{INDUSTRY}}`, `{{DATE_TODAY}}`, `{{NUM_JOBS}}`, `{{UNIQUE_EMPLOYERS}}`, `{{GEOCODED}}`
- `{{TOP_SKILLS_BULLETS}}` → a bullet list of top skills and counts

To see the fully rendered prompt (before sending to an LLM):
```bash
python scripts/preview_prompt.py
```

If the file is missing, the workflow falls back to a concise built-in prompt.

## Dashboard design choices
Dashboard design notes:
- **Single column rhythm** with clear section spacing; primary actions (filters, downloads) are easy to find.
- **Subtle cards** for KPIs; no heavy boxes or loud colors.
- **Plotly (plotly_white)** with reduced chart chrome; labels kept concise.
- **Folium map** with heatmap + clustered markers for fast spatial scanning.
- Hidden default Streamlit menu/footer to keep focus on data.
- Sidebar filters drive all sections, so the page stays uncluttered.


## LLM options (ON by default)
The pipeline sets `USE_LLM=true` by default and will render the external prompt (`config/insight_prompt.md`) with your current data.

Choose a provider in `.env`:
- `LLM_PROVIDER=openai` → set `OPENAI_API_KEY=ENTER_OPENAI_API_KEY_HERE`
- `LLM_PROVIDER=ollama` → set `OLLAMA_BASE_URL` (default `http://localhost:11434`) and `LLM_MODEL` (e.g., `llama3:instruct`)

If no key is present or the call fails, the pipeline still produces **rules-based insights**.


### Google Sheets — reports worksheet
The pipeline can also append a **reports** tab with parsed report metadata:
- Worksheet name (default): `reports` (configure via `GOOGLE_SHEET_WORKSHEET_REPORTS`)
- Columns: report_name, word_count, skills (comma-separated)


## Makefile quick commands
Use the included `Makefile` to run common tasks with short commands:

```bash
make setup        # venv + requirements + models
make run          # scrape → process → analyze → insights → SQLite/CSVs → Sheets
make dash         # launch the Streamlit dashboard
make sheets-test  # verify Google Sheets setup
make prompt       # preview rendered LLM prompt
make reset-db     # delete data/charm.db (keeps CSVs)
make clean        # clear caches
```

On macOS/Linux it works out of the box. On Windows, use **Git Bash** or **WSL**.

## How the workflow runs (step‑by‑step)
1. **Scrape job boards** (AAA + ACRA) with pagination → `scripts/scrape_jobs.py`
   - Collects: `source, title, company, location, date_posted, job_url, description`
   - Walks “Next” pages safely (limit=10) and de‑dupes by `job_url`.
2. **Clean & de‑duplicate** → `scripts/data_cleaning.py`
   - Normalizes text, hashes `(title|company|desc-snippet)` to drop dupes.
   - Extracts **salary** hints (`salary_min`, `salary_max`, `currency`) when present.
3. **Parse industry reports (PDFs)** → `scripts/parse_reports.py`
   - Reads PDFs from `/reports/` with PyMuPDF; outputs one row per report.
4. **NLP enrichment (jobs + reports)** → `scripts/nlp_entities.py`
   - spaCy NER (ORG/GPE/LOC) and **skills taxonomy** matching (`skills/skills_taxonomy.csv`).
5. **Sentiment** (optional) → `scripts/sentiment_salience.py`
6. **Geocode locations** with Nominatim + on‑disk cache → `scripts/geocode.py`
7. **Persist** results
   - CSVs to `data/processed/` (for the dashboard)
   - **SQLite** to `data/charm.db` (for durable querying and auditing)
8. **Share** (optional): append **jobs** + **reports** to Google Sheets
9. **Analyze** → `scripts/analyze.py` (top skills, counts; optional clustering)
10. **Generate insights** → `scripts/insights.py`
    - Rules‑based recommendations (always)
    - **LLM brief** using the external prompt (`config/insight_prompt.md`)
11. **Visualize** → `dashboard/app.py` (Streamlit + Plotly + Folium)

## Working with industry reports (PDFs)
- Drop **.pdf** files into the `reports/` folder.
- On the next run, the pipeline will extract text with PyMuPDF, enrich with NER + skills, and:
  - write `data/processed/reports.csv`
  - upsert into `data/charm.db` (`reports` table)
  - append a concise row to Google Sheets (worksheet: `reports`) with `report_name`, `word_count`, and aggregated `skills`.
- Reports are combined with job data in analysis and in the LLM prompt context to surface **trends and gaps**.

## Program mapping & outcomes
The insights module translates demand signals into **program formats**:
- Undergraduate (online), Graduate (online)
- Certificate, Post‑baccalaureate
- Workshop, Microlearning

How it works:
- `scripts/insights.py` contains simple, transparent **rules** that map top skills to program formats.
- If `USE_LLM=true`, the external prompt (`config/insight_prompt.md`) requests:
  - **5 trend statements**, **3 emerging skills**, **3 program gaps/opportunities**, explicitly referencing those formats.
- To tailor outputs for different catalogs or brands, adjust:
  - `skills/skills_taxonomy.csv` (aliases & categories)
  - mapping rules in `scripts/insights.py`
  - the prompt language in `config/insight_prompt.md`

## Scraping notes & governance
- **Pagination:** the scrapers follow “Next” links (rel/aria/title/text) with a safe page limit.
- **Politeness:** conservative request pacing; user‑agent identifies tool purpose.
- **Dedupe:** by `job_url` and content hash to avoid churn and inflated counts.
- **Respect sites:** review **robots.txt** and Terms of Service; scale cautiously and cache where possible.
- **No PII:** the pipeline collects job‑level, non‑personal data only.


## Mattermost notifications
The pipeline can post a completion message to a Mattermost channel using an **incoming webhook**.

**Setup:**
1. Create an **Incoming Webhook** in your Mattermost workspace (bound to a channel).
2. In `.env`, set:

   - `MATTERMOST_WEBHOOK_URL=ENTER_MATTERMOST_WEBHOOK_URL_HERE`

   - `DASHBOARD_URL=ENTER_DASHBOARD_URL_HERE`  (public or internal URL for your Streamlit app)

3. Run `make run` (or `python scripts/pipeline.py`).

**What it sends:**
- A check-marked completion line
- A short summary (total postings, employers, geocoded count, top 5 skills)
- A link to the dashboard
- An optional short snippet from `insights.md` (“LLM Brief” if present)

**Where to customize:** `scripts/notify_mattermost.py` → `build_summary()`



### Mattermost in n8n (post-run notification)
Import `n8n/charm_workflow_mattermost.json` for a version of the workflow that **notifies Mattermost after each run**.

**Configure in the “Notification Config” node:**
- `webhookUrl` → your Mattermost **Incoming Webhook URL**
- `dashboardUrl` → link to your Streamlit app (public or internal)
- `mention` → optional (`@channel`, `@here`, or empty)
- `thresholdSkillsCsv` → comma-separated skills to track (e.g., `ArcGIS,Section 106,NEPA`)
- `thresholdPercent` → percentage change to trigger an alert (default `20`)

**What the message includes:**
- ✅ Completion line
- Totals (postings, employers, geocoded)
- Top 5 skills
- Dashboard link
- **Alerts** section if thresholds are hit (↑/↓ with % change vs previous run)
- A short **Brief** snippet extracted from `insights.md`

**How thresholds work:**
- The workflow reads the previous snapshot from `data/processed/analysis_prev.json` (if present)
- It writes the current `analysis.json` to that file after posting the message, so the next run compares properly
- Zero results trigger a “No jobs scraped” alert automatically


## LLM options (self-hosted vs. cloud)
This pipeline supports two classes of LLM backends:

**Cloud (commercial): OpenAI**  
- Set `LLM_PROVIDER=openai` and `OPENAI_API_KEY=ENTER_OPENAI_API_KEY_HERE`  
- Pros: highest quality, simple setup, scalable.  
- Cons: usage cost; data governance requires key management.

**Self-hosted:**
1) **Ollama** (simple local runner on CPU/GPU; great for demos)
   - Set `LLM_PROVIDER=ollama`, `OLLAMA_BASE_URL=http://localhost:11434`, `LLM_MODEL=llama3:instruct` (or similar)
   - Pros: easiest local setup; good developer ergonomics.
   - Cons: slower on CPU-only; fewer enterprise durability knobs.

2) **OpenAI-compatible server** (e.g., vLLM on GPU)
   - Set `LLM_PROVIDER=openai_compat`, `LLM_BASE_URL=http://YOUR-HOST:PORT/v1`, `LLM_MODEL=YourModelName`
   - Pros: production-friendly throughput and token cost control; keeps data in your infra.
   - Cons: requires GPU provisioning & ops (e.g., vLLM/TGI deployment).

**Recommendation:** For a Synology/NAS demo, Ollama is the fastest path to a working self-host. For higher throughput or larger prompts, deploy **vLLM** with an OpenAI-compatible endpoint and switch to `LLM_PROVIDER=openai_compat`.

### Why offer both (OpenAI + Ollama)
Using **both** a commercial cloud model and a self‑hosted local model is intentional:

- **Reliability & failover**: If the cloud key hits a rate limit or there’s an outage, the pipeline can still produce insights with a local model.
- **Cost control**: Cloud LLMs are convenient but metered. A local runner limits variable cost while preserving functionality for routine runs.
- **Data governance**: Some organizations prefer keeping text artifacts on‑prem. A local model reduces external exposure and aids compliance reviews.
- **Portability for reviewers**: Anyone can clone the repo and enable insights with Ollama—even without a paid key—making the demo reproducible.
- **Performance trade‑offs**: Cloud models usually have stronger quality; local models are good enough for structured, well‑scoped prompts like our insight brief.
- **Demonstrates architecture skill**: The same pipeline supports multiple providers behind a stable interface, which is exactly what “agentic”/API‑driven workflows require.

## Components & responsibilities (what each piece does)

This repository is organized so a reviewer can read it top‑down and understand exactly how the system works. Every piece below has a clear, single responsibility.

### Orchestration (n8n)
- `n8n/charm_workflow.json` — Minimal scheduler/trigger that runs the Python pipeline via **Execute Command** from a Cron or Webhook.
- `n8n/charm_workflow_mattermost.json` — Same as above, with post‑run **Mattermost notifications**. Reads `analysis.json` and `insights.md`, composes a short message (totals, top skills, optional alerts, brief), posts to your incoming webhook, and snapshots the current analysis for next‑run comparisons.

### Configuration
- `config/.env.example` — Environment variables with explicit placeholders (e.g., `ENTER_GOOGLE_SHEET_ID_HERE`). Copy to `.env` and fill in. Includes LLM provider switches, user agent, and dashboard URL.
- `config/insight_prompt.md` — The human‑editable prompt template used when `USE_LLM=true`. It’s rendered with live variables (date, counts, top skills) before calling the model.

### Data definitions / taxonomy
- `skills/skills_taxonomy.csv` — Deterministic mapping of common terms/aliases to normalized skill names and (optionally) categories. This keeps “GIS” vs “ArcGIS” vs “ArcGIS Pro” consistent in analysis.

### Pipeline (Python)
- `scripts/pipeline.py` — The orchestrator. Runs end‑to‑end: scrape → clean/dedupe → parse reports → NLP/skills → sentiment → geocode → analyze → insights → persist (CSV/SQLite) → optional Google Sheets append.
- `scripts/scrape_jobs.py` — Scrapers for ACRA + AAA with **pagination** and per‑item description fetching. Uses a configurable `USER_AGENT` and polite defaults.
- `scripts/data_cleaning.py` — Normalization and duplicate detection (content hashing across title/company/description snippet). Also extracts salary hints when present.
- `scripts/parse_reports.py` — Reads **PDFs** from `/reports/` with PyMuPDF; emits one record per report with the raw text for downstream NLP.
- `scripts/nlp_entities.py` — spaCy NER for organizations and locations + taxonomy‑based **skill extraction**. Produces a comma‑separated `skills` column.
- `scripts/sentiment_salience.py` — Optional lightweight sentiment using VADER (useful for qualitative clustering or future labeling).
- `scripts/geocode.py` — Geocodes the `location` field using Nominatim with on‑disk caching; attaches `lat` and `lon` for mapping.
- `scripts/analyze.py` — **Pandas‑based** summaries (top skills, counts, employers, geocoded totals). Can be extended to clustering or time‑series.
- `scripts/insights.py` — Generates a short, human‑readable brief. Always emits rule‑based recommendations; when `USE_LLM=true`, renders `config/insight_prompt.md` and calls the selected provider (OpenAI or Ollama).
- `scripts/gsheets_sync.py` — Appends **jobs** and **reports** to Google Sheets. Handles worksheet creation and de‑dupe by URL or name.
- `scripts/gsheets_test.py` — One‑liner connectivity check for Sheets credentials and permissions.
- `scripts/preview_prompt.py` — Renders the final LLM prompt (with current data) so you can review or paste it elsewhere.
- `scripts/pandas_examples.py` — Extra recipes for ad‑hoc analysis; helpful for quick CSV exports during exploration.

### Storage / outputs
- `data/charm.db` — SQLite database created on first run (durable auditing and ad‑hoc queries).
- `data/processed/` — CSV and artifacts used by the dashboard: `jobs.csv`, `reports.csv`, `analysis.json`, `insights.md`, and `wordcloud.png`.
- `docs/sql_examples.sql` — A few ready‑to‑use SQL queries against `charm.db` (e.g., salary by skill, recent Section 106/NEPA postings).

### Dashboard (Streamlit + Folium + Plotly)
- `dashboard/app.py` — Single‑page, minimalist UI:
  - KPI cards (postings, employers, geocoded)
  - Top skills bar chart (Plotly)
  - Job map with heatmap + clustered markers (Folium)
  - Insights panel and word cloud
  - Sidebar filters and simple download actions (filtered CSV, analysis JSON)
- `.streamlit/config.toml` — Neutral, brand‑agnostic theme.

### Tooling
- `Makefile` — Short commands for setup, running the pipeline, launching the dashboard, testing Sheets, previewing the prompt, and cleanup.
- `requirements.txt` — Python dependencies (scraping, NLP, analysis, dashboard, LLM providers).
- `LICENSE` — MIT license.
- `CHANGELOG.md` — A concise record of what’s included in this release and why certain decisions were made.

---

## How the pieces interact (at a glance)

1. **n8n** triggers the run (Cron or Webhook) → executes `scripts/pipeline.py` in the repo directory on your NAS.
2. The **pipeline** scrapes jobs (with pagination), parses any PDFs in `/reports/`, enriches with NLP/skills, geocodes, analyzes, and writes outputs to both CSV and SQLite.
3. **Google Sheets** (optional) is updated with appended rows for jobs and reports (so stakeholders can view raw structured data).
4. The **dashboard** reads `data/processed/*` and refreshes automatically when files change.
5. The **n8n Mattermost** workflow (optional) reads the latest outputs, composes a short message (totals, top skills, alerts), posts to your channel, and snapshots the analysis for the next run.

Everything is idempotent: duplicates are filtered, pagination is capped, geocoding is cached, and runs can be scheduled safely.
