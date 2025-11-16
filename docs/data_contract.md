# CHARM Data Contract

This document describes the structure of the primary artifacts produced by `scripts/pipeline.py`. Field names are stable unless explicitly versioned in the changelog.

## `data/processed/jobs.csv`
| Column | Type | Notes |
| --- | --- | --- |
| `source` | string | Job board identifier (e.g., `ACRA`, `AAA`). |
| `title` | string | Posting title as scraped. |
| `company` | string | Hiring organization. |
| `location` | string | Free-form city/state text exactly as scraped. |
| `city` | string | Parsed city (best-effort). |
| `state` | string | Parsed state/region abbreviation. |
| `lat`, `lon` | float | Geocoded coordinates (nullable). |
| `date_posted` | date | ISO date, UTC normalized when available. |
| `job_type` | string | Normalized job type bucket (e.g., `field-tech`). |
| `seniority` | string | Normalized seniority bucket (`entry`, `mid`, `senior`, `lead/PI`). |
| `skills` | string | Semicolon-delimited normalized skills. |
| `skills_list` | list[string] | Helper column used by the dashboard (generated when loading). |
| `salary_min`, `salary_max` | float | Parsed USD salary bounds when available. |
| `currency` | string | Salary currency code. |
| `url` | string | Canonical job URL. |
| `description` | string | Cleaned HTML/plain text snippet. |
| `sentiment` | float | VADER sentiment score (−1..1). |

## `data/processed/reports.csv`
| Column | Type | Notes |
| --- | --- | --- |
| `report_name` | string | File stem of the PDF report. |
| `word_count` | integer | Tokenized word count of extracted text. |
| `skills` | string | Semicolon-delimited normalized skills detected in the report. |
| `top_entities` | string | Optional organizations/locations surfaced by NER (comma separated). |
| `text` | string | Full extracted plain text (used for NLP; optionally trimmed before sharing). |

## `data/processed/analysis.json`
Top-level keys:
- `num_jobs` (int)
- `unique_employers` (int)
- `geocoded` (int) – number of postings with coordinates
- `top_skills` (list of `[skill, count]`)
- `top_employers` (list of `[org, count]`)
- `run_timestamp` (ISO8601 string)

Consumers should treat all arrays as ordered (already sorted by frequency) but avoid assuming a fixed length.

## `data/processed/insights.md`
Markdown document combining:
1. Header metrics (jobs, employers, geocoded count).
2. `## In-demand Skills` section mirroring `analysis.json`.
3. `## Program Recommendations` derived from rules in `scripts/insights.py`.
4. Optional `## LLM Brief` when `USE_LLM=true` and the call succeeds.

Because the brief can include generated text, downstream automation should treat it as unstructured Markdown.
