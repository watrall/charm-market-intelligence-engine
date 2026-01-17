# CHARM Market Intelligence Engine
# Cross-platform Makefile for macOS/Linux

PY := .venv/bin/python
PIP := .venv/bin/pip
STREAMLIT := .venv/bin/streamlit
SHELL := /bin/bash

.PHONY: help setup venv install run-pipeline run-dashboard run dash sheets-test prompt clean reset-db lint typecheck test ci validate-patterns

help:
	@echo "CHARM Market Intelligence Engine"
	@echo ""
	@echo "Quick Start:"
	@echo "  make setup          Create venv, install deps, download models"
	@echo "  make run-pipeline   Run the full data pipeline"
	@echo "  make run-dashboard  Launch the Streamlit dashboard"
	@echo ""
	@echo "Development:"
	@echo "  make lint           Run Ruff linter"
	@echo "  make typecheck      Run mypy type checker"
	@echo "  make test           Run pytest test suite"
	@echo "  make ci             Run all checks (lint + typecheck + test)"
	@echo ""
	@echo "Utilities:"
	@echo "  make sheets-test    Verify Google Sheets access"
	@echo "  make prompt         Preview the rendered LLM insight prompt"
	@echo "  make validate-patterns  Validate job pattern regexes"
	@echo "  make clean          Remove caches and build artifacts"
	@echo "  make reset-db       Delete SQLite DB (keeps processed CSVs)"

# --- Setup ---

venv:
	@test -d .venv || python3 -m venv .venv
	@$(PIP) install --upgrade pip --quiet

install: venv
	@$(PIP) install -r requirements.txt --quiet
	@$(PY) -m spacy download en_core_web_sm 2>/dev/null || true
	@$(PY) -c "import nltk; nltk.download('vader_lexicon', quiet=True)" 2>/dev/null || true

install-dev: install
	@$(PIP) install -r requirements-dev.txt --quiet

setup: install
	@echo "✓ Environment ready. Run 'make run-pipeline' to start."

# --- Primary Commands ---

run-pipeline:
	@if [ ! -f .env ] && [ -f .env.example ]; then cp .env.example .env; echo "Created .env from .env.example"; fi
	@$(PY) scripts/pipeline.py

run-dashboard:
	@$(STREAMLIT) run dashboard/app.py --server.headless true

# Aliases for backward compatibility
run: run-pipeline
dash: run-dashboard

# --- Development ---

lint:
	@$(PY) -m ruff check scripts/ dashboard/ tests/ --fix

typecheck:
	@$(PY) -m mypy scripts/ --ignore-missing-imports

test:
	@$(PY) -m pytest tests/ -v --tb=short

ci: lint typecheck test
	@echo "✓ All CI checks passed."

# --- Utilities ---

sheets-test:
	@$(PY) scripts/gsheets_test.py

prompt:
	@$(PY) scripts/preview_prompt.py

validate-patterns:
	@$(PY) scripts/validate_patterns.py

clean:
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@find . -name ".ruff_cache" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -name ".mypy_cache" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -name ".pytest_cache" -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Cleaned caches."

reset-db:
	@rm -f data/charm.db
	@echo "✓ Removed data/charm.db"
