"""
StreetEasy scraper.

Approach: StreetEasy is a Next.js app that embeds full search results
in a <script id="__NEXT_DATA__"> JSON blob in the initial HTML response.
No headless browser required -a plain requests.get() works as long as
we send realistic headers and respect rate limits.

Limitations / known fragility:
  - StreetEasy occasionally restructures their JSON schema. The _find_listings()
    function walks the tree looking for a list that looks like listings, which
    makes it more resilient to minor schema changes.
  - If StreetEasy adds bot detection (e.g. Cloudflare), this will start
    returning 403s. At that point, switch to a paid proxy or the RapidAPI
    StreetEasy endpoint.
  - Pagination is handled up to MAX_PAGES per neighborhood batch.
"""

import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from curl_cffi import requests
from bs4 import BeautifulSoup

from apartment_hunter.models import Listing

BASE_URL = "https://streeteasy.com"
# Correct URL format: /for-rent/{neighborhood-slug} per neighborhood.
# Batching all neighborhoods into one comma-separated URL path causes 404s.
# Filters use StreetEasy's pipe-delimited path syntax: price:-3400|beds:2
# OR can be passed as query params -we use query params as fallback.
SEARCH_URL = BASE_URL + "/for-rent/{neighborhood}/price:-{max_price}|beds>={bedrooms}"
MAX_PAGES = 10
REQUEST_DELAY = 4.0   # base seconds between search-page requests
DETAIL_DELAY  = 2.0   # base seconds between detail-page fetches
HOOD_DELAY    = 8.0   # base seconds between neighborhoods

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://streeteasy.com/",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}


def scrape(config: dict, existing_rows: dict | None = None) -> list[Listing]:
    neighborhoods = config["search"]["neighborhoods"]
    max_price = config["search"]["max_price"]
    min_bedrooms = config["search"]["min_bedrooms"]
    existing_rows = existing_rows or {}

    all_listings: list[Listing] = []
    seen_urls: set[str] = set()

    for hood in neighborhoods:
        # Fresh session per neighborhood — StreetEasy tracks a bot score per
        # session and starts 403ing after ~8-10 requests in one session.
        session = requests.Session(impersonate="chrome136")
        session.headers.update(HEADERS)
        try:
            warmup = session.get(BASE_URL + "/", timeout=15)
            if warmup.status_code != 200:
                print(f"  [StreetEasy] warmup {warmup.status_code} for {hood} — continuing anyway")
            time.sleep(REQUEST_DELAY + random.uniform(1, 2))
        except requests.RequestsError:
            pass

        hood_listings = _scrape_neighborhood(session, hood, max_price, min_bedrooms)
        n_deduped = 0
        for l in hood_listings:
            if l.url not in seen_urls:
                seen_urls.add(l.url)
                all_listings.append(l)
            else:
                n_deduped += 1
        if n_deduped:
            print(f"  [StreetEasy] {n_deduped} duplicate URL(s) skipped for {hood}")
        time.sleep(HOOD_DELAY + random.uniform(0, 4))

    # Enrich each unique listing with its detail page — skip listings already
    # enriched in the CSV (bedrooms non-blank is the "enrichment done" signal).
    needs_enrich = [l for l in all_listings
                    if not _is_enriched(l.url, existing_rows)
                    and (existing_rows.get(l.url) or {}).get("delisted", "").lower() != "true"]
    cached       = len(all_listings) - len(needs_enrich)
    print(f"  [StreetEasy] enriching {needs_enrich and len(needs_enrich) or 0} listing(s) via detail pages"
          + (f" ({cached} skipped — already in CSV)" if cached else "") + "...")

    # Restore stored values for listings we're skipping
    for listing in all_listings:
        if _is_enriched(listing.url, existing_rows):
            _restore_from_row(listing, existing_rows[listing.url])

    enrich_session = requests.Session(impersonate="chrome136")
    enrich_session.headers.update(HEADERS)
    for i, listing in enumerate(needs_enrich):
        if i > 0 and i % 10 == 0:
            enrich_session = requests.Session(impersonate="chrome136")
            enrich_session.headers.update(HEADERS)
            time.sleep(REQUEST_DELAY + random.uniform(1, 3))
        idx = all_listings.index(listing)
        all_listings[idx] = _enrich_listing(enrich_session, listing)
        time.sleep(DETAIL_DELAY + random.uniform(0, 1.5))

    print(f"  [StreetEasy] {len(all_listings)} listings scraped")
    return all_listings


def _scrape_neighborhood(
    session: requests.Session,
    hood: str,
    max_price: int,
    bedrooms: int,
) -> list[Listing]:
    listings = []

    for page in range(1, MAX_PAGES + 1):
        url = SEARCH_URL.format(
            neighborhood=hood,
            max_price=max_price,
            bedrooms=bedrooms,
        )
        params = {"page": str(page)} if page > 1 else {}
        print(f"  [StreetEasy] {hood} page {page}: {url}")

        try:
            resp = session.get(url, params=params, timeout=15)
        except requests.RequestsError as e:
            print(f"  [StreetEasy] request error ({hood}): {e}")
            break

        if resp.status_code == 403:
            print(f"  [StreetEasy] 403 -bot detection triggered for {hood}.")
            break
        if resp.status_code == 404:
            print(f"  [StreetEasy] 404 for '{hood}' -neighborhood slug may be wrong. "
                  "Check config.yaml neighborhood names match StreetEasy slugs exactly.")
            break
        if resp.status_code != 200:
            print(f"  [StreetEasy] unexpected status {resp.status_code} for {hood}")
            break

        page_listings, total = _parse_page(resp.text, max_price, bedrooms, hood)
        listings.extend(page_listings)

        if not page_listings:
            break

        if total and len(listings) >= total:
            break

        time.sleep(REQUEST_DELAY + random.uniform(0, 2))

    print(f"  [StreetEasy] {hood}: {len(listings)} listings")
    return listings


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_page(html: str, max_price: int, bedrooms: int, hood: str = "") -> tuple[list[Listing], Optional[int]]:
    """
    Parse one page of StreetEasy search results.

    StreetEasy migrated from Next.js Pages Router (__NEXT_DATA__) to the
    App Router (RSC streaming via self.__next_f.push). The listing data
    is now rendered directly into the HTML as React components.

    We parse the rendered HTML cards using data-testid="listing-card",
    which is a stable selector that doesn't depend on hashed CSS class names.
    """
    soup = BeautifulSoup(html, "lxml")

    # Total count is in the <h1>: "7 Williamsburg, Brooklyn NY Apartments..."
    total = _extract_total_from_h1(soup)

    cards = soup.find_all("div", attrs={"data-testid": "listing-card"})
    if not cards:
        print("  [StreetEasy] no listing cards found (data-testid='listing-card')")
        return [], total

    results = []
    for card in cards:
        listing = _parse_card(card, hood)
        if listing:
            results.append(listing)

    return results, total


def _extract_total_from_h1(soup) -> Optional[int]:
    h1 = soup.find("h1")
    if h1:
        m = re.search(r"^(\d+)", h1.get_text(strip=True))
        if m:
            return int(m.group(1))
    return None


# Tokens that may appear as the trailing slug in a StreetEasy building URL
# instead of the actual street address.  These get stripped before the address
# is reconstructed.  Includes both borough names (most listings) and the few
# neighbourhood slugs that StreetEasy uses instead (e.g. Ridgewood, which
# straddles the Brooklyn/Queens border and is never labelled "-queens").
_LOCATION_SUFFIXES = {
    "brooklyn", "manhattan", "queens", "bronx",   # boroughs
    "ridgewood",                                   # Queens neighbourhood slug
}


def _ordinal(n: int) -> str:
    """Return the ordinal string for a floor number: 1 -> '1st', 3 -> '3rd', etc."""
    suffix = (
        "th" if 11 <= (n % 100) <= 13
        else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    )
    return f"{n}{suffix}"


def _infer_floor(unit: str) -> Optional[str]:
    """
    Infer the floor number from an apartment unit designator.

    NYC convention examples:
      "3R", "3L"      -> 3rd   (leading digit = floor)
      "53"            -> 5th   (first digit = floor, second = unit on floor)
      "704"           -> 7th   (first digit = floor for 3-digit codes)
      "1204"          -> 12th  (first two digits = floor for 4-digit codes)
      "3RD-FLOOR"     -> 3rd
      "GROUND-FLOOR"  -> 1st
    """
    u = unit.upper()

    # Named ground floor
    if "GROUND" in u:
        return "1st"

    # Single letter (e.g. "#A", "#B") -NYC convention for 1st-floor units
    if re.match(r"^[A-Z]$", u):
        return "1st"

    # Explicit "Nth floor" label: "3RD-FLOOR", "2ND-FLOOR", etc.
    m = re.match(r"(\d+)(?:ST|ND|RD|TH)[^0-9]*FLOOR", u)
    if m:
        return _ordinal(int(m.group(1)))

    # Extract leading digit run (handles "3R", "704", "17_11" edge cases)
    m = re.match(r"(\d+)", u)
    if not m:
        return None

    digits = m.group(1)
    n = len(digits)
    if n == 1:
        floor_num = int(digits)
    elif n <= 3:
        # 2-3 digit codes: first digit is the floor (e.g. 53 -> 5, 704 -> 7)
        floor_num = int(digits[0])
    else:
        # 4+ digit codes: first two digits are the floor (e.g. 1204 -> 12)
        floor_num = int(digits[:2])

    return _ordinal(floor_num)


def _parse_url_address(url: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extract address (with unit) and ordinal floor from a StreetEasy building URL.

    Examples:
      /building/304-evergreen-avenue-brooklyn/3r      -> ("304 Evergreen Avenue #3R", "3rd")
      /building/17_11-hancock-street-ridgewood/308    -> ("17-11 Hancock Street #308", "3rd")
      /building/8-palmetto-street-brooklyn/704        -> ("8 Palmetto Street #704", "7th")
      /building/39-troutman-street-brooklyn/rental/x  -> ("39 Troutman Street", None)
    """
    m = re.search(r"/building/([^/?#]+)(?:/([^/?#]+))?", url.lower())
    if not m:
        return None, None

    building_slug = m.group(1)   # e.g. "304-evergreen-avenue-brooklyn"
    unit_slug     = m.group(2)   # e.g. "3r" | "rental" | None

    parts = building_slug.split("-")
    if not parts or not re.match(r"^\d", parts[0]):
        return None, None

    # Underscores in the first token encode a hyphenated Queens-style number (e.g. 17_11 -> 17-11)
    number = parts[0].replace("_", "-")
    rest   = parts[1:]

    # Strip trailing borough — not shown in address (unit replaces it)
    if rest and rest[-1] in _LOCATION_SUFFIXES:
        rest = rest[:-1]

    if not rest:
        return None, None

    # Some StreetEasy slugs embed a company token instead of the street type
    # (e.g. "81-beadel-llc-brooklyn" where the building manager is an LLC).
    # Replace known non-street tokens so the address comes out correct.
    rest = ["street" if p.lower() == "llc" else p for p in rest]

    street = " ".join(p.title() for p in rest)

    has_unit = bool(unit_slug and unit_slug not in ("rental", "for-rent"))
    address = f"{number} {street} #{unit_slug.upper()}" if has_unit else f"{number} {street}"

    floor = _infer_floor(unit_slug) if has_unit else None

    return address, floor


def _parse_card(card, hood: str = "") -> Optional[Listing]:
    """
    Extract listing data from a StreetEasy HTML listing card.

    StreetEasy uses hashed CSS module class names that change with each
    build, so we rely on structural cues (first <a> href, text patterns)
    rather than class names. data-testid attributes are stable.
    """
    # --- URL ---
    link = card.find("a", href=re.compile(r"/rental/|/for-rent/\d|/nyc/\d|/building/"))
    if not link:
        link = card.find("a", href=True)
    url = link["href"] if link and link.get("href") else ""
    if not url:
        return None
    if url.startswith("/"):
        url = BASE_URL + url
    # Strip query params — removes ?featured=1 and any similar tracking flags
    # so featured and non-featured versions of the same unit share one canonical URL.
    parts = urlsplit(url)
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    # --- Address and floor from URL ---
    address, floor = _parse_url_address(url)

    # --- All visible text in the card ---
    card_text = card.get_text(" ", strip=True)

    # --- Price: match "$2,800" or "$2800" ---
    price = None
    price_m = re.search(r"\$([\d,]+)", card_text)
    if price_m:
        price = int(price_m.group(1).replace(",", ""))

    # --- Beds / baths ---
    # StreetEasy uses "2 Beds", "1 Bed", "2 bd", "2 BR" etc.
    beds_m = re.search(r"(\d+)\s*(?:beds?|bd|br)\b", card_text, re.IGNORECASE)
    bedrooms = int(beds_m.group(1)) if beds_m else None

    baths_m = re.search(r"([\d.]+)\s*bath", card_text, re.IGNORECASE)
    bathrooms = float(baths_m.group(1)) if baths_m else None

    # --- Floor: from URL first, then card text ---
    if not floor:
        floor_m = re.search(
            r"(?:floor|fl\.?)\s*(\d+)|(\d+)(?:st|nd|rd|th)\s*floor",
            card_text, re.IGNORECASE,
        )
        floor = (floor_m.group(1) or floor_m.group(2)) if floor_m else None

    # --- Neighborhood: use the search hood slug, prettified ---
    neighborhood: Optional[str] = None
    if hood:
        neighborhood = hood.replace("-", " ").title()

    # --- Rent stabilized ---
    rent_stab = True if re.search(r"rent.?stabiliz", card_text, re.IGNORECASE) else None

    # --- Amenities ---
    dishwasher = True if re.search(r"\bdishwasher\b", card_text, re.IGNORECASE) else None
    washer_dryer = (
        True if re.search(
            r"\bwasher[/\-\s]*dryer\b|\bw/?d\b|\bin.?unit\s+laundry\b|\blaundry\s+in.?unit\b",
            card_text, re.IGNORECASE,
        ) else None
    )

    # date_listed is handled by _enrich_listing (detail page is more reliable)

    # --- Title ---
    title_parts = []
    if bedrooms is not None:
        title_parts.append(f"{bedrooms}BR")
    if address:
        title_parts.append(address)
    elif neighborhood:
        title_parts.append(neighborhood)
    title = " - ".join(title_parts) if title_parts else "StreetEasy listing"

    return Listing(
        url=url,
        source="streeteasy",
        title=title,
        price=price,
        neighborhood=neighborhood,
        address=address,
        floor=floor,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        rent_stabilized=rent_stab,
        dishwasher=dishwasher,
        washer_dryer=washer_dryer,
        date_listed=None,  # filled in by _enrich_listing
    )


def _is_enriched(url: str, existing_rows: dict) -> bool:
    """True if the URL exists in the CSV and has already been through enrichment."""
    row = existing_rows.get(url)
    if not row:
        return False
    # bedrooms is always set by the detail page when available — use it as the
    # enrichment sentinel. A listing with no bedrooms field was never enriched.
    return bool((row.get("bedrooms") or "").strip())


def _restore_from_row(listing: Listing, row: dict) -> None:
    """Copy enriched fields from a CSV row back onto a freshly-scraped listing."""
    def _bool(val):
        return True if str(val).strip().lower() == "true" else None

    def _int(val):
        try: return int(str(val).strip())
        except (ValueError, TypeError): return None

    def _float(val):
        try: return float(str(val).strip())
        except (ValueError, TypeError): return None

    def _date(val):
        from datetime import datetime
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try: return datetime.strptime(str(val).strip(), fmt)
            except (ValueError, TypeError): pass
        return None

    if listing.address is None:
        listing.address    = row.get("address") or None
    if listing.floor is None:
        listing.floor      = row.get("floor") or None
    if listing.bedrooms is None:
        listing.bedrooms   = _int(row.get("bedrooms"))
    if listing.bathrooms is None:
        listing.bathrooms  = _float(row.get("bathrooms"))
    if listing.date_listed is None:
        listing.date_listed = _date(row.get("date_listed"))
    if listing.dishwasher is None:
        listing.dishwasher  = _bool(row.get("dishwasher"))
    if listing.washer_dryer is None:
        listing.washer_dryer = _bool(row.get("washer_dryer"))
    if listing.rent_stabilized is None:
        listing.rent_stabilized = _bool(row.get("rent_stabilized"))
    if listing.image_url is None:
        _img = row.get("image_url")
        if _img is not None and not (isinstance(_img, float) and _img != _img):
            _img = str(_img).strip()
            listing.image_url = _img if _img.lower() not in ("nan", "none", "") else None


def _enrich_listing(session: requests.Session, listing: Listing) -> Listing:
    """
    Fetch the listing's detail page and backfill any fields that were absent
    or unreliable from the search card.

    Priority rules:
      - address / floor / bedrooms / bathrooms: only set if still None
      - date_listed: detail page preferred; overrides card-derived value
      - amenities (dishwasher, washer_dryer, rent_stabilized): upgrade None->True;
        never downgrade a True that the card already found
    """
    try:
        resp = session.get(listing.url, timeout=15)
    except requests.RequestsError as e:
        print(f"  [StreetEasy] detail error: {e}")
        return listing

    if resp.status_code != 200:
        return listing

    soup = BeautifulSoup(resp.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # --- Off-market detection ---
    # StreetEasy shows an "Unavailable" badge with "Delisted on MM/DD/YYYY" or
    # "Rented on MM/DD/YYYY" on the detail page of units no longer available.
    # Mark these now so the upsert step can persist the flag rather than
    # treating a re-sighted URL as proof the listing is still active.
    _body = full_text.lower()
    if any(p in _body for p in ("unavailable", "delisted", "rented on")):
        listing.delisted = True
        return listing

    # --- Address / unit from <h1> when URL parsing didn't produce them ---
    h1 = soup.find("h1")
    if h1:
        h1_text = h1.get_text(" ", strip=True)
        # H1 typically: "304 Evergreen Ave #3R" or "304 Evergreen Ave, Apt 3R, Brooklyn"
        # Take the first comma-delimited segment as the street+unit portion.
        address_part = h1_text.split(",")[0].strip()
        if not listing.address and address_part:
            listing.address = address_part
        if not listing.floor and address_part:
            unit_m = re.search(r"#(\w[\w-]*)", address_part)
            if unit_m:
                listing.floor = _infer_floor(unit_m.group(1))

    # --- Beds / baths (only if missing) ---
    if listing.bedrooms is None:
        m = re.search(r"(\d+)\s*(?:beds?|bd|br)\b", full_text, re.IGNORECASE)
        if m:
            listing.bedrooms = int(m.group(1))

    if listing.bathrooms is None:
        m = re.search(r"([\d.]+)\s*baths?", full_text, re.IGNORECASE)
        if m:
            listing.bathrooms = float(m.group(1))

    # --- Date listed ---
    # Primary: price history table — most recent "Listed" event.
    # StreetEasy renders entries like "3/19/2026 $3,300 Listed by Doorway NYC"
    # An apartment re-listed after a prior rental cycle has multiple Listed rows;
    # we want the most recent one (the current listing), not the historical first.
    _date_listed = None
    for _m in re.finditer(r"(\d{1,2}/\d{1,2}/\d{4})\s+\$[\d,]+\s+Listed", full_text):
        try:
            _d = datetime.strptime(_m.group(1), "%m/%d/%Y")
            if _date_listed is None or _d > _date_listed:
                _date_listed = _d   # keep the most recent Listed date
        except ValueError:
            pass

    # Fallback: "Days on market 6 days" — number comes AFTER the label
    if _date_listed is None:
        _m = re.search(r"Days on market\s+(\d+)\s+days?", full_text, re.IGNORECASE)
        if _m:
            _days = int(_m.group(1))
            if 0 <= _days <= 365:
                _today = datetime.utcnow().date()
                _d = _today - timedelta(days=_days)
                _date_listed = datetime(_d.year, _d.month, _d.day)

    if _date_listed is not None:
        listing.date_listed = _date_listed

    # --- Amenities: upgrade None -> True, never clobber existing True ---
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

    # --- Listing images ---
    # StreetEasy embeds photo URLs as JSON inside <script> tags (React/Next.js),
    # not as <img src> or og:image meta tags. Extract them directly from raw HTML.
    if not listing.image_url:
        # Full-size photos live on zillowstatic CDN, named <hash>-full.<ext>
        photo_urls = re.findall(
            r"https://photos\.zillowstatic\.com/fp/[a-f0-9]+-full\.[a-z]+",
            resp.text,
        )
        img_urls = list(dict.fromkeys(photo_urls))  # deduplicate, preserve order
        # Fallback: any og:image meta tags (older page formats)
        if not img_urls:
            img_urls = [
                m["content"] for m in soup.find_all("meta", attrs={"property": "og:image"})
                if m.get("content", "").startswith("http")
            ]
        if img_urls:
            listing.image_url = ",".join(img_urls[:8])

    # --- Rebuild title now that we may have better address / bedrooms ---
    title_parts = []
    if listing.bedrooms is not None:
        title_parts.append(f"{listing.bedrooms}BR")
    if listing.address:
        title_parts.append(listing.address)
    elif listing.neighborhood:
        title_parts.append(listing.neighborhood)
    if title_parts:
        listing.title = " - ".join(title_parts)

    return listing


