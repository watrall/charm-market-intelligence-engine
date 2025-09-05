    # ---- Config ----
    PY := .venv/bin/python
    PIP := .venv/bin/pip
    STREAMLIT := .venv/bin/streamlit

    SHELL := /bin/bash

    # ---- Meta ----
    .PHONY: help setup venv install run dash sheets-test prompt clean reset-db lint

    help:
	@echo "Targets:"
	@echo "  make setup        Create venv and install requirements"
	@echo "  make run          Run the full CHARM pipeline"
	@echo "  make dash         Launch the Streamlit dashboard"
	@echo "  make sheets-test  Verify Google Sheets access"
	@echo "  make prompt       Preview the rendered LLM insight prompt"
	@echo "  make clean        Remove caches and build artifacts"
	@echo "  make reset-db     Delete SQLite DB (keeps processed CSVs)"
	@echo "  make lint         Basic style checks (optional)"

    # ---- Environment ----
    venv:
	@test -d .venv || python -m venv .venv
	@$(PIP) install --upgrade pip

    install: venv
	@$(PIP) install -r requirements.txt
	@$(PY) -m spacy download en_core_web_sm || true
	@$(PY) -c "import nltk; nltk.download('vader_lexicon')" || true

    setup: install
	@echo "Environment ready."

    # ---- Main actions ----
    run:
	@$(PY) scripts/pipeline.py

    dash:
	@$(STREAMLIT) run dashboard/app.py

    sheets-test:
	@$(PY) scripts/gsheets_test.py

    prompt:
	@$(PY) scripts/preview_prompt.py

    # ---- Maintenance ----
    clean:
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	@find . -name "*.pyc" -delete
	@echo "Cleaned caches."

    reset-db:
	@rm -f data/charm.db
	@echo "Removed data/charm.db"

    lint:
	@echo "Note: add flake8/black/isort to requirements if you want stricter checks."
	@echo "No linters configured yet."
