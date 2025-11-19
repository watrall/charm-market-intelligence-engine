import time, re, os, json, requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DESC_CACHE_PATH = CACHE_DIR / "job_descriptions.json"
SESSION = requests.Session()
_DESC_CACHE = None
_DESC_CACHE_DIRTY = False


def _load_desc_cache():
    global _DESC_CACHE
    if _DESC_CACHE is not None:
        return _DESC_CACHE
    if DESC_CACHE_PATH.exists():
        try:
            _DESC_CACHE = json.loads(DESC_CACHE_PATH.read_text(encoding="utf-8"))
        except (ValueError, OSError):
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
    # Try common patterns for "Next" links
    from urllib.parse import urljoin, urlparse
    cand = None
    # rel="next"
    link = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if link and link.get("href"): cand = link["href"]
    # aria-label or title contains next
    if not cand:
        link = soup.find("a", attrs={"aria-label": re.compile("next", re.I)}) or soup.find("a", title=re.compile("next", re.I))
        if link and link.get("href"):
            cand = link["href"]
    # text contains next » or >
    if not cand:
        for a in soup.find_all("a", href=True):
            t = (a.get_text(strip=True) or "").lower()
            if "next" in t or t in {">", "»"}:
                cand = a["href"]; break
    if not cand:
        # Look for pagination containers and pick the next active+1
        pagers = soup.find_all(class_=re.compile("pagination|pager|nav", re.I))
        for p in pagers:
            cur = p.find(class_=re.compile("active|current", re.I))
            if cur:
                sib = cur.find_next("a", href=True)
                if sib: cand = sib["href"]; break
    if not cand: return None
    # join and ensure stays on same host
    nxt = urljoin(base, cand)
    try:
        if urlparse(nxt).netloc and urlparse(nxt).netloc != urlparse(base).netloc:
            return None
    except Exception:
        pass
    return nxt

def _fetch(url, sleep=1.0):
    time.sleep(sleep)
    user_agent = os.getenv("USER_AGENT", "CHARM/1.0 (research)")
    SESSION.headers.update({"User-Agent": user_agent})
    r = SESSION.get(url, timeout=25); r.raise_for_status(); return r.text

def _parse_generic(soup, base, source):
    jobs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]; text = a.get_text(strip=True)
        if "job" in href.lower() or "/jobs/" in href.lower():
            jobs.append(dict(source=source, title=text or "Job", company="", location="", date_posted="", job_url=urljoin(base, href)))
    return jobs

def parse_acra(html, base):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(".job_listings .job_listing, article.job_listing") or soup.select("article")
    jobs = []
    for it in items:
        a = it.find("a", href=True); 
        if not a: continue
        link = urljoin(base, a["href"])
        title = (it.find("h3") or it.find("h2") or a).get_text(strip=True)
        company = (it.find("div", class_=re.compile("company", re.I)) or it.find("span", class_=re.compile("company", re.I)))
        company = company.get_text(strip=True) if company else ""
        location = (it.find("div", class_=re.compile("location", re.I)) or it.find("span", class_=re.compile("location", re.I)))
        location = location.get_text(strip=True) if location else ""
        date_posted = (it.find("time") or it.find("span", class_=re.compile("date", re.I)))
        date_posted = date_posted.get("datetime","") if date_posted and date_posted.has_attr("datetime") else (date_posted.get_text(strip=True) if date_posted else "")
        jobs.append(dict(source="ACRA", title=title, company=company, location=location, date_posted=date_posted, job_url=link))
    return jobs or _parse_generic(soup, base, "ACRA")

def parse_aaa(html, base):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("[data-automation='job-card']") or soup.select(".job-preview") or []
    jobs = []
    for c in cards:
        a = c.find("a", href=True)
        if not a: continue
        link = urljoin(base, a["href"])
        title = (c.select_one("[data-automation='job-title']") or c.find("h3") or a).get_text(strip=True)
        company_el = c.select_one("[data-automation='job-company']") or c.find("h4")
        company = company_el.get_text(strip=True) if company_el else ""
        loc_el = c.select_one("[data-automation='job-location']") or c.find("span", class_=re.compile("location", re.I))
        location = loc_el.get_text(strip=True) if loc_el else ""
        date_el = c.find("time") or c.find("span", class_=re.compile("date", re.I))
        date_posted = date_el.get("datetime","") if date_el and date_el.has_attr("datetime") else (date_el.get_text(strip=True) if date_el else "")
        jobs.append(dict(source="AAA", title=title, company=company, location=location, date_posted=date_posted, job_url=link))
    return jobs or _parse_generic(soup, base, "AAA")

def _fetch_job_desc(url):
    cache = _load_desc_cache()
    cached = cache.get(url, None)
    if cached is not None:
        return cached
    try:
        html = _fetch(url, sleep=0.6)
        soup = BeautifulSoup(html, "html.parser")
        cont = soup.find("article") or soup.find("div", id="job-description") or soup.find("div", class_=re.compile("description|content", re.I))
        txt = cont.get_text(" ", strip=True) if cont else soup.get_text(" ", strip=True)
        snippet = txt[:20000]
        cache[url] = snippet
        global _DESC_CACHE_DIRTY
        _DESC_CACHE_DIRTY = True
        return snippet
    except Exception:
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
    df["description"] = [ _fetch_job_desc(u) for u in df["job_url"].tolist() ]
    _save_desc_cache()
    return df


def _walk_pages(start_url, parser, max_pages=10):
    visited = set()
    url = start_url
    rows = []
    for _ in range(max_pages):
        if not url or url in visited: break
        visited.add(url)
        try:
            html = _fetch(url)
        except Exception as e:
            print(f"Fetch failed for {url}: {e}")
            break
        soup = BeautifulSoup(html, "html.parser")
        # parse using site-specific parser
        try:
            page_rows = parser(html, url)
            rows.extend(page_rows)
        except Exception as e:
            print(f"Parse failed for {url}: {e}")
        # find next
        nxt = _find_next_page(soup, url)
        url = nxt
    # de-dupe by job_url in-memory
    seen = set(); uniq = []
    for r in rows:
        u = r.get("job_url","")
        if u and u not in seen:
            uniq.append(r); seen.add(u)
    return uniq
