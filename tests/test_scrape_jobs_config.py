import importlib

import scripts.scrape_jobs as scrape_jobs


def test_scrape_jobs_invalid_env_uses_defaults(monkeypatch):
    monkeypatch.setenv("SCRAPER_MAX_WORKERS", "bad")
    monkeypatch.setenv("SCRAPER_REQUEST_INTERVAL", "bad")

    module = importlib.reload(scrape_jobs)

    assert module.MAX_WORKERS == 4
    assert module.REQUEST_INTERVAL == 0.8
    assert module.REFILL_RATE == 5.0


def test_scrape_jobs_minimum_bounds(monkeypatch):
    monkeypatch.setenv("SCRAPER_MAX_WORKERS", "0")
    monkeypatch.setenv("SCRAPER_REQUEST_INTERVAL", "0")

    module = importlib.reload(scrape_jobs)

    assert module.MAX_WORKERS == 4
    assert module.REQUEST_INTERVAL == 0.8
