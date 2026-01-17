import os
from pathlib import Path

import pandas as pd
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


def _load_cache(path: Path) -> pd.DataFrame:
    """Load geocache with validation."""
    if not path.exists():
        return pd.DataFrame(columns=["location", "lat", "lon"])
    try:
        # Limit file size to prevent DoS
        if path.stat().st_size > 50_000_000:  # 50MB limit
            print("Warning: Geocache too large, resetting")
            return pd.DataFrame(columns=["location", "lat", "lon"])
        df = pd.read_csv(path)
        # Validate expected columns exist
        if not {"location", "lat", "lon"}.issubset(df.columns):
            return pd.DataFrame(columns=["location", "lat", "lon"])
        # Validate coordinate bounds
        df = df[df["lat"].between(-90, 90) | df["lat"].isna()]
        df = df[df["lon"].between(-180, 180) | df["lon"].isna()]
        return df
    except (pd.errors.EmptyDataError, OSError, ValueError):
        return pd.DataFrame(columns=["location", "lat", "lon"])


def geocode_locations(df: pd.DataFrame) -> pd.DataFrame:
    base = Path(__file__).resolve().parents[1]
    cache_path = base / "data" / "geocache.csv"
    cache = _load_cache(cache_path)

    contact = os.getenv("GEOCODE_CONTACT_EMAIL", "").strip()
    custom_agent = os.getenv("GEOCODE_USER_AGENT", "").strip()
    agent = custom_agent or (f"CHARM-geo/1.1 ({contact})" if contact else "CHARM-geo/1.1")

    geolocator = Nominatim(user_agent=agent)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=True)

    df = df.copy()
    df["location_norm"] = df["location"].fillna("").astype(str).str.strip()

    known = {row["location"]: (row["lat"], row["lon"]) for _, row in cache.iterrows()}
    new_entries = []

    for loc in [loc for loc in df["location_norm"].unique() if loc and loc not in known]:
        try:
            result = geocode(loc)
            lat = result.latitude if result else None
            lon = result.longitude if result else None
        except (GeocoderServiceError, GeocoderTimedOut):
            lat = lon = None
        known[loc] = (lat, lon)
        new_entries.append({"location": loc, "lat": lat, "lon": lon})

    if new_entries:
        cache = pd.concat([cache, pd.DataFrame(new_entries)], ignore_index=True)
        cache.to_csv(cache_path, index=False)

    df["lat"] = df["location_norm"].map(lambda loc: known.get(loc, (None, None))[0])
    df["lon"] = df["location_norm"].map(lambda loc: known.get(loc, (None, None))[1])
    return df.drop(columns=["location_norm"])
