import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, local
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

MAX_WORKERS = int(os.getenv("SCRAPER_MAX_WORKERS", "4"))
REQUEST_INTERVAL = float(os.getenv("SCRAPER_REQUEST_INTERVAL", "0.8"))
REFILL_RATE = MAX_WORKERS / max(REQUEST_INTERVAL, 0.1)
_rate_lock = Lock()
_thread_state = local()
_tokens = float(MAX_WORKERS)
_last_refill = time.monotonic()

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DESC_CACHE_PATH = CACHE_DIR / "job_descriptions.json"
_DESC_CACHE = None
_DESC_CACHE_DIRTY = False


def _load_desc_cache():
    global _DESC_CACHE
    if _DESC_CACHE is not None:
        return _DESC_CACHE
    if DESC_CACHE_PATH.exists():
        try:
            _DESC_CACHE = json.loads(DESC_CACHE_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            _DESC_CACHE = {}
    else:
        _DESC_CACHE = {}
    return _DESC_CACHE


def _save_desc_cache():
    global _DESC_CACHE_DIRTY
    if not _DESC_CACHE_DIRTY or _DESC_CACHE is None:
        return
    DESC_CACHE_PATH.write_text(json.dumps(_DESC_CACHE), encoding="utf-8")
    _DESC_CACHE_DIRTY = False


def _find_next_page(soup, base):
    cand = None
    # rel="next"
    link = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if link and link.get("href"):
        cand = link["href"]
    if not cand:
        link = (
            soup.find("a", attrs={"aria-label": re.compile("next", re.I)})
            or soup.find("a", title=re.compile("next", re.I))
        )
        if link and link.get("href"):
            cand = link["href"]
    if not cand:
        for a in soup.find_all("a", href=True):
            t = (a.get_text(strip=True) or "").lower()
            if "next" in t or t in {">", "»"}:
                cand = a["href"]
                break
    if not cand:
        pagers = soup.find_all(class_=re.compile(r"pagination|pager|nav", re.I))
        for p in pagers:
            cur = p.find(class_=re.compile(r"active|current", re.I))
            if cur:
                sib = cur.find_next("a", href=True)
                if sib:
                    cand = sib["href"]
                    break
    if not cand:
        return None

    nxt = urljoin(base, cand)
    try:
        nxt_host = urlparse(nxt).netloc
        base_host = urlparse(base).netloc
        if nxt_host and nxt_host != base_host:
            return None
    except ValueError:
        pass
    return nxt

def _acquire_slot():
    """Token bucket: allow up to MAX_WORKERS requests per REQUEST_INTERVAL on average."""
    global _tokens, _last_refill
    while True:
        with _rate_lock:
            now = time.monotonic()
            elapsed = now - _last_refill
            if elapsed > 0:
                _tokens = min(MAX_WORKERS, _tokens + elapsed * REFILL_RATE)
                _last_refill = now
            if _tokens >= 1.0:
                _tokens -= 1.0
                return
            # Need to wait for enough tokens to accumulate
            missing = 1.0 - _tokens
            wait_time = missing / REFILL_RATE
        time.sleep(max(wait_time, 0.05))


def _fetch(url):
    _acquire_slot()
    session = getattr(_thread_state, "session", None)
    if session is None:
        session = requests.Session()
        _thread_state.session = session
    user_agent = os.getenv("USER_AGENT", "CHARM/1.0 (research)")
    session.headers.update({"User-Agent": user_agent})
    backoff = 0.5
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=25)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(backoff)
            backoff *= 2
    return ""

def _parse_generic(soup, base, source):
    jobs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "job" not in href.lower() and "/jobs/" not in href.lower():
            continue
        text = a.get_text(strip=True)
        jobs.append({
            "source": source,
            "title": text or "Job",
            "company": "",
            "location": "",
            "date_posted": "",
            "job_url": urljoin(base, href),
        })
    return jobs

def parse_acra(html, base):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(".job_listings .job_listing, article.job_listing") or soup.select("article")
    jobs = []
    for it in items:
        a = it.find("a", href=True)
        if not a:
            continue
        link = urljoin(base, a["href"])
        title_el = it.find("h3") or it.find("h2") or a
        title = title_el.get_text(strip=True)

        company_el = (
            it.find("div", class_=re.compile(r"company", re.I))
            or it.find("span", class_=re.compile(r"company", re.I))
        )
        company = company_el.get_text(strip=True) if company_el else ""

        location_el = (
            it.find("div", class_=re.compile(r"location", re.I))
            or it.find("span", class_=re.compile(r"location", re.I))
        )
        location = location_el.get_text(strip=True) if location_el else ""

        date_el = it.find("time") or it.find("span", class_=re.compile(r"date", re.I))
        if date_el and date_el.has_attr("datetime"):
            date_posted = date_el.get("datetime", "")
        else:
            date_posted = date_el.get_text(strip=True) if date_el else ""

        jobs.append({
            "source": "ACRA",
            "title": title,
            "company": company,
            "location": location,
            "date_posted": date_posted,
            "job_url": link,
        })
    return jobs or _parse_generic(soup, base, "ACRA")

def parse_aaa(html, base):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("[data-automation='job-card']") or soup.select(".job-preview") or []
    jobs = []
    for c in cards:
        a = c.find("a", href=True)
        if not a:
            continue
        link = urljoin(base, a["href"])

        title_el = (
            c.select_one("[data-automation='job-title']")
            or c.find("h3")
            or a
        )
        title = title_el.get_text(strip=True)

        company_el = c.select_one("[data-automation='job-company']") or c.find("h4")
        company = company_el.get_text(strip=True) if company_el else ""

        loc_el = (
            c.select_one("[data-automation='job-location']")
            or c.find("span", class_=re.compile(r"location", re.I))
        )
        location = loc_el.get_text(strip=True) if loc_el else ""

        date_el = c.find("time") or c.find("span", class_=re.compile(r"date", re.I))
        if date_el and date_el.has_attr("datetime"):
            date_posted = date_el.get("datetime", "")
        else:
            date_posted = date_el.get_text(strip=True) if date_el else ""

        jobs.append({
            "source": "AAA",
            "title": title,
            "company": company,
            "location": location,
            "date_posted": date_posted,
            "job_url": link,
        })
    return jobs or _parse_generic(soup, base, "AAA")

def _fetch_job_desc(url):
    cache = _load_desc_cache()
    cached = cache.get(url)
    if cached is not None:
        return cached

    global _DESC_CACHE_DIRTY
    try:
        html = _fetch(url)
        soup = BeautifulSoup(html, "html.parser")
        container = (
            soup.find("article")
            or soup.find("div", id="job-description")
            or soup.find("div", class_=re.compile(r"description|content", re.I))
        )
        txt = container.get_text(" ", strip=True) if container else soup.get_text(" ", strip=True)
        snippet = txt[:20000]
        cache[url] = snippet
        _DESC_CACHE_DIRTY = True
        return snippet
    except (requests.RequestException, OSError) as exc:
        cache[url] = ""
        _DESC_CACHE_DIRTY = True
        return ""

def scrape_sources():
    sources = [
        ("https://acra-crm.org/jobs/", parse_acra),
        ("https://careercenter.americananthro.org/jobs/cultural-resource-management/", parse_aaa),
    ]
    rows = []
    for base, parser in sources:
        try:
            page_rows = _walk_pages(base, parser, max_pages=10)
            rows.extend(page_rows)
        except Exception as e:
            print(f"Scrape failed for {base}: {e}")
    df = pd.DataFrame(rows)
    if df.empty: return df
    urls = df["job_url"].tolist()
    descriptions = [""] * len(urls)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(_fetch_job_desc, u): idx for idx, u in enumerate(urls)}
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                descriptions[idx] = future.result()
            except Exception:
                descriptions[idx] = ""
    df["description"] = descriptions
    _save_desc_cache()
    return df


def _walk_pages(start_url, parser, max_pages=10):
    visited = set()
    url = start_url
    rows = []

    for _ in range(max_pages):
        if not url or url in visited:
            break
        visited.add(url)

        try:
            html = _fetch(url)
        except requests.RequestException as e:
            print(f"Fetch failed for {url}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        try:
            page_rows = parser(html, url)
            rows.extend(page_rows)
        except (AttributeError, TypeError) as e:
            print(f"Parse failed for {url}: {e}")

        url = _find_next_page(soup, url)

    seen = set()
    unique = []
    for r in rows:
        job_url = r.get("job_url", "")
        if job_url and job_url not in seen:
            unique.append(r)
            seen.add(job_url)
    return unique
