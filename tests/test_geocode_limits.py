import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import geocode


def test_geocode_respects_max_new(monkeypatch, tmp_path):
    calls = []

    class FakeLocator:
        def geocode(self, loc):
            calls.append(loc)
            return SimpleNamespace(latitude=1.0, longitude=2.0)

    monkeypatch.setenv("GEOCODE_MAX_NEW", "3")
    monkeypatch.setattr(geocode, "Nominatim", lambda user_agent: FakeLocator())
    # RateLimiter becomes a passthrough to keep call counting simple
    monkeypatch.setattr(geocode, "RateLimiter", lambda func, **kwargs: func)

    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame({"location": [f"Loc{i}" for i in range(6)]})

    geocode.geocode_locations(df, cache_path=cache_path)

    assert len(calls) == 3
    assert cache_path.exists()
