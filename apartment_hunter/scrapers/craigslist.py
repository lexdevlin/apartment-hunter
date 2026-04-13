"""
Craigslist scraper — NYC apartments.

Search pages are JS-rendered so we use Playwright (headless Chromium) to load
them.  Individual post/detail pages are server-side rendered, so those are
fetched with plain requests — much faster than spinning up a browser per post.

Requires:
    pip install playwright
    playwright install chromium
"""

import re
import time
from datetime import datetime
from typing import Optional

from curl_cffi import requests
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False

from apartment_hunter.models import Listing

BASE_URL      = "https://newyork.craigslist.org"
SEARCH_URL    = BASE_URL + "/search/apa"
PAGE_DELAY    = 2.5   # seconds between search pages
DETAIL_DELAY  = 1.5   # seconds between detail page requests
MAX_PAGES     = 8     # 120 results max (15 cards/page in gallery view)

# Set to True temporarily to watch the browser and diagnose selector issues
DEBUG_VISIBLE = False

DETAIL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def scrape(config: dict, existing_rows: dict | None = None) -> list[Listing]:
    if not _PLAYWRIGHT_AVAILABLE:
        print("  [Craigslist] Playwright not installed. Run:")
        print("    pip install playwright && playwright install chromium")
        return []

    neighborhoods = config["search"]["neighborhoods"]
    max_price     = config["search"]["max_price"]
    min_bedrooms  = config["search"]["min_bedrooms"]
    existing_rows = existing_rows or {}

    all_listings: list[Listing] = []
    seen_urls: set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not DEBUG_VISIBLE)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        # Warmup — let CL set session cookies
        print("  [Craigslist] warming up browser session...")
        page.goto(BASE_URL, wait_until="networkidle", timeout=30_000)
        page.wait_for_timeout(2000)

        for page_num in range(MAX_PAGES):
            offset = page_num * 120  # gallery view shows ~120 per page
            url = (
                f"{SEARCH_URL}?min_bedrooms={min_bedrooms}"
                f"&max_price={max_price}&s={offset}"
            )
            print(f"  [Craigslist] search page {page_num + 1} (offset={offset})")

            try:
                page.goto(url, wait_until="networkidle", timeout=30_000)
                page.wait_for_timeout(2000)  # extra buffer for late JS renders
            except PlaywrightTimeout:
                print(f"  [Craigslist] timeout loading page {page_num + 1}")
                break

            cards = _extract_cards(page)
            if not cards:
                print(f"  [Craigslist] no cards found on page {page_num + 1} — stopping")
                _dump_diagnostics(page)
                break

            new_on_page  = 0
            dupe_on_page = 0
            for listing in cards:
                if listing.url not in seen_urls:
                    seen_urls.add(listing.url)
                    all_listings.append(listing)
                    new_on_page += 1
                else:
                    dupe_on_page += 1

            dupe_note = f", {dupe_on_page} duplicate(s) skipped" if dupe_on_page else ""
            print(f"  [Craigslist] {new_on_page} new listings{dupe_note} (total: {len(all_listings)})")

            if new_on_page == 0:
                break  # all dupes — we've reached the end

            page.wait_for_timeout(int(PAGE_DELAY * 1000))

        browser.close()

    # Drop unwanted listing types early (based on card title)
    all_listings = [l for l in all_listings if not _is_unwanted(l.title)]
    print(f"  [Craigslist] {len(all_listings)} after filtering unwanted types (sublets, shared rooms, etc.)")

    # Filter to target neighborhoods
    matched = [l for l in all_listings if _matches_neighborhoods(l, neighborhoods)]
    print(f"  [Craigslist] {len(matched)} matched neighborhood filter "
          f"(from {len(all_listings)} total)")

    # Enrich with detail pages using plain requests (server-side rendered).
    # Skip listings already enriched in the CSV.
    needs_enrich = [l for l in matched
                    if not _is_enriched_cl(l.url, existing_rows)
                    and (existing_rows.get(l.url) or {}).get("delisted", "").lower() != "true"]
    cached       = len(matched) - len(needs_enrich)
    print(f"  [Craigslist] enriching {len(needs_enrich)} listing(s) via detail pages"
          + (f" ({cached} skipped — already in CSV)" if cached else "") + "...")

    detail_session = requests.Session(impersonate="chrome136")
    detail_session.headers.update(DETAIL_HEADERS)
    enriched = []
    for listing in matched:
        if _is_enriched_cl(listing.url, existing_rows):
            _restore_from_row_cl(listing, existing_rows[listing.url])
        else:
            listing = _enrich_listing(detail_session, listing)
            time.sleep(DETAIL_DELAY)
        # Re-check after enrichment (title may have been updated from detail page)
        if _is_unwanted(listing.title):
            continue
        if listing.delisted:
            continue
        # Refine generic borough neighborhood from title if possible
        _refine_neighborhood(listing, neighborhoods)
        enriched.append(listing)

    n_filtered = len(matched) - len(enriched)
    if n_filtered:
        print(f"  [Craigslist] {n_filtered} additional listing(s) filtered after detail enrichment")

    return enriched


# ---------------------------------------------------------------------------
# Card extraction from rendered search page
# ---------------------------------------------------------------------------

def _extract_cards(page) -> list[Listing]:
    """
    Extract listing data from the rendered Craigslist search results page.
    Tries multiple known CL designs; dumps diagnostics if nothing matches.
    """
    html = page.content()
    soup = BeautifulSoup(html, "lxml")

    # Newer CL design (2023+): gallery/list view
    cards = soup.find_all("li", class_="cl-search-result")
    if cards:
        print(f"  [Craigslist] found {len(cards)} cards (new design)")
        return [r for c in cards for r in [_parse_new_card(c)] if r]

    # Older CL design
    cards = soup.find_all("li", class_="result-row")
    if cards:
        print(f"  [Craigslist] found {len(cards)} cards (old design)")
        return [r for c in cards for r in [_parse_old_card(c)] if r]

    # Any element with a data-pid attribute
    cards = soup.find_all(attrs={"data-pid": True})
    if cards:
        print(f"  [Craigslist] found {len(cards)} data-pid elements")
        return [r for c in cards for r in [_parse_new_card(c)] if r]

    # Nothing found — dump diagnostics to identify actual selectors
    _dump_diagnostics(page, soup)
    return []


def _parse_new_card(card) -> Optional[Listing]:
    """Parse a card from CL's newer design (cl-search-result)."""
    # URL + title
    anchor = card.find("a", class_=re.compile(r"posting-title|cl-app-anchor"))
    if not anchor:
        anchor = card.find("a", href=re.compile(r"/apa/|/brk/|/mnh/|/que/"))
    if not anchor:
        return None

    url = anchor.get("href", "")
    if url and not url.startswith("http"):
        url = BASE_URL + url

    title = anchor.get_text(strip=True)

    # Price
    price: Optional[int] = None
    price_el = card.find("span", class_=re.compile(r"priceinfo|price"))
    if price_el:
        m = re.search(r"\$([\d,]+)", price_el.get_text())
        if m:
            price = int(m.group(1).replace(",", ""))

    # Beds from meta/housing text
    meta = card.find(class_=re.compile(r"housing|meta"))
    meta_text = meta.get_text() if meta else card.get_text()
    beds_m = re.search(r"(\d+)\s*(?:br|bd|bdr)\b", meta_text, re.IGNORECASE)
    bedrooms = int(beds_m.group(1)) if beds_m else None

    # Neighborhood from location span
    hood_el = card.find(class_=re.compile(r"location|hood|neighborhood"))
    neighborhood = hood_el.get_text(strip=True) if hood_el else None

    # Date
    date_el = card.find("time")
    date_listed = _parse_iso_date(date_el.get("datetime", "")) if date_el else None

    return Listing(
        url=url,
        source="craigslist",
        title=title,
        price=price,
        neighborhood=neighborhood,
        address=None,
        floor=None,
        bedrooms=bedrooms,
        bathrooms=None,
        rent_stabilized=None,
        date_listed=date_listed,
    )


def _parse_old_card(card) -> Optional[Listing]:
    """Parse a card from CL's older design (result-row)."""
    anchor = card.find("a", class_="result-title")
    if not anchor:
        return None

    url   = anchor.get("href", "")
    title = anchor.get_text(strip=True)

    price: Optional[int] = None
    price_el = card.find("span", class_="result-price")
    if price_el:
        m = re.search(r"\$([\d,]+)", price_el.get_text())
        if m:
            price = int(m.group(1).replace(",", ""))

    housing_el = card.find("span", class_="housing")
    bedrooms = None
    if housing_el:
        m = re.search(r"(\d+)\s*br", housing_el.get_text(), re.IGNORECASE)
        if m:
            bedrooms = int(m.group(1))

    hood_el = card.find("span", class_="result-hood")
    neighborhood = hood_el.get_text(strip=True).strip("() ") if hood_el else None

    date_el = card.find("time", class_="result-date")
    date_listed = _parse_iso_date(date_el.get("datetime", "")) if date_el else None

    return Listing(
        url=url,
        source="craigslist",
        title=title,
        price=price,
        neighborhood=neighborhood,
        address=None,
        floor=None,
        bedrooms=bedrooms,
        bathrooms=None,
        rent_stabilized=None,
        date_listed=date_listed,
    )


def _dump_diagnostics(page, soup=None) -> None:
    """Print structural info to help identify the right selectors."""
    try:
        if soup is None:
            soup = BeautifulSoup(page.content(), "lxml")

        # All unique <li> class combinations on the page
        li_classes = set()
        for li in soup.find_all("li")[:50]:
            cls = tuple(li.get("class") or [])
            if cls:
                li_classes.add(cls)
        print(f"  [Craigslist] <li> classes seen: {li_classes}")

        # All unique <div> / <ol> / <ul> classes that might be result containers
        for tag in ("ol", "ul"):
            for el in soup.find_all(tag)[:10]:
                cls = el.get("class")
                if cls:
                    print(f"  [Craigslist] <{tag}> class: {cls}")

        # Page text snippet
        body = soup.find("body")
        if body:
            print(f"  [Craigslist] page text preview: {body.get_text(' ', strip=True)[:300]}")
    except Exception as e:
        print(f"  [Craigslist] diagnostics error: {e}")


# ---------------------------------------------------------------------------
# Enrichment cache helpers
# ---------------------------------------------------------------------------

def _is_enriched_cl(url: str, existing_rows: dict) -> bool:
    """True if the URL exists in the CSV and has already been through enrichment.
    For CL, price is always set by the detail page — use it as the sentinel.
    """
    row = existing_rows.get(url)
    if not row:
        return False
    return bool((row.get("price") or "").strip())


def _restore_from_row_cl(listing: Listing, row: dict) -> None:
    """Copy enriched fields from a CSV row back onto a freshly-scraped listing."""
    def _bool(val):
        return True if str(val).strip().lower() == "true" else None

    def _int(val):
        try: return int(str(val).strip())
        except (ValueError, TypeError): return None

    def _float(val):
        try: return float(str(val).strip())
        except (ValueError, TypeError): return None

    if listing.price is None:
        raw = str(row.get("price") or "").replace("$", "").replace(",", "").strip()
        try: listing.price = int(raw)
        except (ValueError, TypeError): pass
    if listing.bedrooms is None:
        listing.bedrooms   = _int(row.get("bedrooms"))
    if listing.bathrooms is None:
        listing.bathrooms  = _float(row.get("bathrooms"))
    if listing.address is None:
        listing.address    = row.get("address") or None
    if listing.dishwasher is None:
        listing.dishwasher  = _bool(row.get("dishwasher"))
    if listing.washer_dryer is None:
        listing.washer_dryer = _bool(row.get("washer_dryer"))
    if listing.rent_stabilized is None:
        listing.rent_stabilized = _bool(row.get("rent_stabilized"))
    if listing.image_url is None:
        listing.image_url = row.get("image_url") or None
    # Restore title from CSV (detail page may have cleaned it up)
    stored_title = (row.get("title") or "").strip()
    if stored_title:
        listing.title = stored_title


# ---------------------------------------------------------------------------
# Detail page enrichment (plain requests — CL posts are server-side rendered)
# ---------------------------------------------------------------------------

def _enrich_listing(session: requests.Session, listing: Listing) -> Listing:
    try:
        resp = session.get(listing.url, timeout=15)
    except requests.RequestsError as e:
        print(f"  [Craigslist] detail error: {e}")
        return listing

    if resp.status_code == 404:
        listing.delisted = True
        return listing

    if resp.status_code != 200:
        return listing

    soup = BeautifulSoup(resp.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # Price (if card didn't have one)
    if listing.price is None:
        price_el = soup.find("span", class_="price")
        if price_el:
            m = re.search(r"\$([\d,]+)", price_el.get_text())
            if m:
                listing.price = int(m.group(1).replace(",", ""))

    # Beds / baths from housing span: "2br / 1ba"
    housing = soup.find("span", class_="housing")
    if housing:
        h = housing.get_text()
        if listing.bedrooms is None:
            m = re.search(r"(\d+)\s*br", h, re.IGNORECASE)
            if m:
                listing.bedrooms = int(m.group(1))
        if listing.bathrooms is None:
            m = re.search(r"([\d.]+)\s*ba", h, re.IGNORECASE)
            if m:
                listing.bathrooms = float(m.group(1))

    # Address
    if listing.address is None:
        addr_el = soup.find("div", class_="mapaddress")
        if addr_el:
            listing.address = addr_el.get_text(strip=True) or None

    # Canonical title
    title_el = (
        soup.find("span", id="titletextonly")
        or soup.find("h1", class_="postingtitle")
    )
    if title_el:
        clean = title_el.get_text(strip=True)
        if clean:
            listing.title = clean

    # Cross street — extract from body text if no numbered address was found
    if listing.address is None:
        listing.address = _extract_cross_street(full_text)

    # Images: CL only sets one og:image but embeds all full-size URLs in the
    # thumbnail anchor hrefs inside #thumbs — these are always present in the
    # static HTML and point directly to the _600x450.jpg full-size images.
    if not listing.image_url:
        thumbs_div = soup.find("div", id="thumbs")
        if thumbs_div:
            img_urls = [
                a["href"] for a in thumbs_div.find_all("a", class_="thumb")
                if a.get("href", "").startswith("http")
            ]
        else:
            # Fallback: single og:image
            og = soup.find("meta", property="og:image")
            img_urls = [og["content"]] if og and og.get("content", "").startswith("http") else []
        if img_urls:
            listing.image_url = ",".join(img_urls[:8])

    # Amenities
    if listing.dishwasher is None:
        if re.search(r"\bdishwasher\b", full_text, re.IGNORECASE):
            listing.dishwasher = True

    if listing.washer_dryer is None:
        if re.search(
            r"\bwasher[/\-\s]*dryer\b|\bw/?d\b|\bin.?unit\s+laundry\b|\blaundry\s+in.?unit\b",
            full_text, re.IGNORECASE,
        ):
            listing.washer_dryer = True

    if listing.rent_stabilized is None:
        if re.search(r"rent.?stabiliz", full_text, re.IGNORECASE):
            listing.rent_stabilized = True

    return listing


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SUFFIX = r"(?:Ave(?:nue)?|Blvd|Boulevard|St(?:reet)?|Rd|Road|Pl(?:ace)?|Dr(?:ive)?|Ln|Lane|Pkwy|Parkway|Ct|Court|Ter(?:race)?|Way)"
_PART   = r"(?:\w+(?:\s+\w+){0,3})"  # 1–4 words
_SEP    = r"\s*(?:and|&|/|@|near|at)\s*"

# Case 3 helpers: uppercase-anchored words (≥4 chars) for suffixless intersections
# e.g. "Stanhope near Knickerbocker" — uppercase anchoring filters noise like "2BR near subway"
_UC_WORD = r"[A-Z]\w{3,}"                     # one uppercase-initial word, ≥4 chars total
_UC_PART = _UC_WORD + r"(?:\s+" + _UC_WORD + r")?"  # 1 or 2 such words
_LIKELY_NOT_STREET = re.compile(
    r"^(?:subway|metro|school|church|hospital|supermarket|market|"
    r"station|stop|corner|block|floor|level|unit|suite|building|"
    r"laundry|parking|elevator|basement|bedroom|bathroom|kitchen)$",
    re.IGNORECASE,
)


def _extract_cross_street(text: str) -> Optional[str]:
    """
    Extract a cross-street intersection from free-form listing text.
    Returns a string like "Myrtle Ave & Broadway" suitable for Nominatim geocoding,
    or None if no plausible intersection is found.

    _SEP covers: and / & / @ / near / at  — so "X Avenue near Y Avenue" is handled.

    Case 1: first street has a recognised suffix (second may or may not).
      "342 Rutland Avenue near Nostrand Avenue"
      "near Myrtle Ave and Broadway"
      "corner of Grand St / Lorimer St"

    Case 2: second street has suffix, keyword required to anchor.
      "near Broadway and Myrtle Ave"
      "at Broadway near Troutman St"

    Case 3: neither street has a suffix — uses uppercase-word anchoring to
    distinguish proper street names from noise ("near subway", "2BR near park").
      "Stanhope near Knickerbocker"
      "Troutman at Jefferson"
    """
    keyword = r"(?:(?:corner\s+of|near|at|between)\s+)?"

    # Case 1: first street has suffix, second may or may not
    m = re.search(
        keyword +
        r"(" + _PART + r"\s+" + _SUFFIX + r")" +
        _SEP +
        r"(" + _PART + r"(?:\s+" + _SUFFIX + r")?)" +
        r"(?=[\s,.\n]|$)",
        text, re.IGNORECASE,
    )
    if m:
        st1 = _clean_street(_trim_to_suffix(m.group(1)))
        st2 = _clean_street(_trim_to_suffix(m.group(2)))
        if len(st1) >= 4 and len(st2) >= 4:
            return f"{st1} & {st2}"

    # Case 2: second street has suffix, first may not (keyword required to anchor)
    m = re.search(
        r"(?:corner\s+of|near|at|between)\s+" +
        r"(" + _PART + r"(?:\s+" + _SUFFIX + r")?)" +
        _SEP +
        r"(" + _PART + r"\s+" + _SUFFIX + r")" +
        r"(?=[\s,.\n]|$)",
        text, re.IGNORECASE,
    )
    if m:
        st1 = _clean_street(_trim_to_suffix(m.group(1)))
        st2 = _clean_street(_trim_to_suffix(m.group(2)))
        if len(st1) >= 4 and len(st2) >= 4:
            return f"{st1} & {st2}"

    # Case 3: neither street has a recognised suffix.
    # Uppercase anchoring (_UC_WORD = [A-Z]\w{3,}) limits matches to proper-noun-style
    # words, filtering out noise like "2BR near subway" or "apartment near park".
    # Note: no re.IGNORECASE so that lowercase words ("bedroom near elevator") are excluded.
    m = re.search(
        r"(?<!\w)(" + _UC_PART + r")"
        r"\s+(?:near|at|@)\s+"
        r"(" + _UC_PART + r")"
        r"(?=[\s,.\n]|$)",
        text,
    )
    if m:
        st1 = _clean_street(m.group(1))
        st2 = _clean_street(m.group(2))
        if (len(st1) >= 4 and len(st2) >= 4
                and not _LIKELY_NOT_STREET.match(st1)
                and not _LIKELY_NOT_STREET.match(st2)):
            return f"{st1} & {st2}"

    return None


def _trim_to_suffix(s: str) -> str:
    """Trim a street string to end at its last recognised suffix word.
    E.g. "Troutman St in Bushwick" → "Troutman St".
    If no suffix is present (e.g. "Broadway"), return the string as-is.
    """
    m = re.search(r"\b" + _SUFFIX + r"\b", s, re.IGNORECASE)
    return s[: m.end()].strip() if m else s.strip()


def _clean_street(s: str) -> str:
    """Strip leading noise (digits, non-street keywords) from an extracted street fragment.
    E.g. "2BR near Broadway" → "Broadway".
    """
    s = re.sub(r"^[\d\W]+", "", s).strip()  # leading digits / punctuation
    s = re.sub(r"^(?:near|at|corner|of|between|and)\s+", "", s, flags=re.IGNORECASE).strip()
    return s


# ---------------------------------------------------------------------------
# Listing-type filter — skip sublets, shared rooms, furnished rooms, etc.
# ---------------------------------------------------------------------------

_UNWANTED_RE = re.compile(
    r"\bsublet\b|subleas|sub-let|sub-lease"
    r"|\bshared\s+room\b|\broom\s+share\b|\broommate\b"
    r"|\bfurnished\s+room\b|\bfurnished\s+bedroom\b"
    r"|\broom\s+for\s+rent\b",
    re.IGNORECASE,
)


def _is_unwanted(title: Optional[str]) -> bool:
    """Return True if the listing title signals a sublet, shared room, etc."""
    return bool(title and _UNWANTED_RE.search(title))


# ---------------------------------------------------------------------------
# Neighborhood refinement — promote borough → specific neighbourhood from title
# ---------------------------------------------------------------------------

_BOROUGH_NAMES = {"brooklyn", "queens", "manhattan", "bronx", "staten island", "new york"}


def _refine_neighborhood(listing: Listing, neighborhoods: list[str]) -> None:
    """
    If the listing's neighborhood is a generic borough name (or unrecognised),
    search the title for a known target neighbourhood and use that instead.
    """
    current = (listing.neighborhood or "").lower().replace("-", " ").strip()
    known   = [n.lower().replace("-", " ") for n in neighborhoods]

    # Already a recognised target neighbourhood — nothing to do
    if current in known:
        return

    # Only refine if the neighbourhood is blank or a generic borough label
    if current and current not in _BOROUGH_NAMES:
        return

    title_lower = (listing.title or "").lower().replace("-", " ")
    for hood, canonical in zip(known, neighborhoods):
        if hood in title_lower:
            listing.neighborhood = canonical
            return


def _matches_neighborhoods(listing: Listing, neighborhoods: list[str]) -> bool:
    haystack = " ".join(filter(None, [
        listing.neighborhood or "",
        listing.title or "",
    ])).lower().replace("-", " ")
    for hood in neighborhoods:
        if hood.lower().replace("-", " ") in haystack:
            return True
    return False


def _parse_iso_date(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
