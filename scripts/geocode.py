import time, pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from pathlib import Path

def geocode_locations(df: pd.DataFrame) -> pd.DataFrame:
    base = Path(__file__).resolve().parents[1]
    cache_path = base / "data" / "geocache.csv"
    cache = pd.read_csv(cache_path) if cache_path.exists() else pd.DataFrame(columns=["location","lat","lon"])

    geolocator = Nominatim(user_agent="CHARM-geo")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=True)

    lats, lons = [], []
    for loc in df["location"].fillna("").astype(str).str.strip():
        if not loc:
            lats.append(None); lons.append(None); continue
        hit = cache[cache["location"]==loc]
        if not hit.empty:
            lats.append(hit.iloc[0]["lat"]); lons.append(hit.iloc[0]["lon"]); continue
        try:
            g = geocode(loc)
            lat = g.latitude if g else None
            lon = g.longitude if g else None
            lats.append(lat); lons.append(lon)
            cache = pd.concat([cache, pd.DataFrame([{"location":loc,"lat":lat,"lon":lon}])], ignore_index=True)
        except Exception:
            lats.append(None); lons.append(None)
        time.sleep(0.1)
    out = df.copy()
    out["lat"] = lats; out["lon"] = lons
    cache.to_csv(cache_path, index=False)
    return out
