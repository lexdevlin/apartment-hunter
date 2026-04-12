"""
Zillow scraper — NYC rental listings.

Approach: Zillow's internal PUT API (/async-create-search-page-state) is
monitored aggressively by Akamai Bot Manager and returns 403 without a valid
authenticated session. Instead we scrape the standard HTML search page, which
embeds full results in a <script id="__NEXT_DATA__"> JSON blob — the same
pattern used by StreetEasy.

Key requirements for Zillow to return a 200:
  1. The session must first warm up via a GET to the homepage, which sets
     the zguid and zgsession cookies Zillow expects to see on subsequent requests.
  2. The search GET must be made in the same session (cookies carried over).
  3. Referer and Sec-Fetch-Site must be set to look like same-site navigation.

If the 200 comes back but contains "captcha" in the body, Zillow's bot scorer
has still flagged the session — we log and exit cleanly.

Overlap with StreetEasy:
  Zillow's NYC rental inventory largely mirrors StreetEasy. This scraper is
  useful for confirming that overlap or catching listings that appear on Zillow
  first. The source field is "zillow" so they can be correlated by address.
"""

import json
import re
import time
import random
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

from curl_cffi import requests

from apartment_hunter.models import Listing

BASE_URL = "https://www.zillow.com"
REQUEST_DELAY = 4.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# Zillow neighborhood URL slugs → canonical neighborhood name for Listing objects.
# Format: /slug/rentals/
# These are the same target areas as our other scrapers.
NEIGHBORHOOD_SLUGS = {
    "bushwick-brooklyn-new-york-ny":                  "bushwick",
    "bedford-stuyvesant-brooklyn-new-york-ny":        "bedford-stuyvesant",
    "williamsburg-brooklyn-new-york-ny":              "williamsburg",
    "east-williamsburg-brooklyn-new-york-ny":         "east-williamsburg",
    "greenpoint-brooklyn-new-york-ny":                "greenpoint",
    "clinton-hill-brooklyn-new-york-ny":              "clinton-hill",
    "crown-heights-brooklyn-new-york-ny":             "crown-heights",
    "prospect-lefferts-gardens-brooklyn-new-york-ny": "prospect-lefferts-gardens",
    "ridgewood-new-york-ny":                          "ridgewood",
}


def scrape(config: dict, existing_rows: dict | None = None) -> list[Listing]:
    neighborhoods = config["search"]["neighborhoods"]
    max_price     = config["search"]["max_price"]
    min_bedrooms  = config["search"]["min_bedrooms"]
    existing_rows = existing_rows or {}

    # Build set of target neighborhood canonical names for filtering
    target = {n.lower() for n in neighborhoods}

    session = requests.Session(impersonate="chrome136")
    session.headers.update(HEADERS)

    # Warmup: GET the homepage to establish cookies (zguid, zgsession).
    # Without these Zillow's bot scorer rejects the next request.
    print("  [Zillow] warming up session (homepage)...")
    try:
        warmup = session.get(BASE_URL + "/", timeout=15)
        if warmup.status_code != 200:
            print(f"  [Zillow] warmup {warmup.status_code} — continuing anyway")
        else:
            cookies = list(warmup.cookies.keys())
            print(f"  [Zillow] warmup OK, cookies: {cookies}")
    except requests.RequestsError as e:
        print(f"  [Zillow] warmup error: {e}")

    # Switch to same-origin navigation headers for search requests
    session.headers["Referer"]        = BASE_URL + "/"
    session.headers["Sec-Fetch-Site"] = "same-origin"

    time.sleep(REQUEST_DELAY + random.uniform(1, 2))

    all_listings: list[Listing] = []

    for slug, hood in NEIGHBORHOOD_SLUGS.items():
        if hood not in target:
            continue

        listings = _scrape_neighborhood(session, slug, hood, max_price, min_bedrooms)
        all_listings.extend(listings)
        time.sleep(REQUEST_DELAY + random.uniform(1, 3))

    # Deduplicate by URL
    seen: set[str] = set()
    unique = []
    for l in all_listings:
        if l.url not in seen:
            seen.add(l.url)
            unique.append(l)

    print(f"  [Zillow] {len(unique)} listings total")
    return unique


def _scrape_neighborhood(
    session: requests.Session,
    slug: str,
    hood: str,
    max_price: int,
    min_bedrooms: int,
) -> list[Listing]:
    # Build Zillow search URL.
    # searchQueryState encodes filters as a JSON object in the query string.
    search_state = {
        "isMapVisible": False,
        "isListVisible": True,
        "filterState": {
            "isForRent":            {"value": True},
            "isForSaleByAgent":     {"value": False},
            "isForSaleByOwner":     {"value": False},
            "isNewConstruction":    {"value": False},
            "isAuction":            {"value": False},
            "isForSaleForeclosure": {"value": False},
            "price":                {"max": max_price},
            "monthlyPayment":       {"max": max_price},
            "beds":                 {"min": min_bedrooms},
        },
    }
    params = {"searchQueryState": json.dumps(search_state, separators=(",", ":"))}
    url = f"{BASE_URL}/{slug}/rentals/?" + urlencode(params)
    print(f"  [Zillow] {hood}: {url[:100]}...")

    try:
        resp = session.get(url, timeout=20)
    except requests.RequestsError as e:
        print(f"  [Zillow] request error ({hood}): {e}")
        return []

    if resp.status_code == 403:
        print(f"  [Zillow] 403 for {hood} — bot detection. "
              "Try increasing REQUEST_DELAY or running fewer neighborhoods per session.")
        return []
    if resp.status_code != 200:
        print(f"  [Zillow] unexpected status {resp.status_code} for {hood}")
        return []

    # Even on 200, Zillow may serve a CAPTCHA page
    if "captcha" in resp.text.lower() and "__NEXT_DATA__" not in resp.text:
        print(f"  [Zillow] CAPTCHA page for {hood} — bot scorer flagged this session")
        return []

    data = _extract_next_data(resp.text)
    if data is None:
        print(f"  [Zillow] could not find __NEXT_DATA__ for {hood}")
        return []

    raw_listings = _find_listings_in_data(data)
    if not raw_listings:
        print(f"  [Zillow] 0 listings found in __NEXT_DATA__ for {hood} "
              "(schema may have changed or session still fingerprinted)")
        return []

    results = []
    for raw in raw_listings:
        listing = _parse_listing(raw, hood)
        if listing:
            results.append(listing)

    print(f"  [Zillow] {hood}: {len(results)} listings")
    return results


def _extract_next_data(html: str) -> Optional[dict]:
    """Extract and parse the __NEXT_DATA__ JSON blob from the page HTML."""
    m = re.search(
        r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.+?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _find_listings_in_data(data: dict) -> list:
    """
    Navigate the __NEXT_DATA__ tree to find the listings array.

    Zillow's Next.js page structure nests listings under:
      props.pageProps.searchPageState.cat1.searchResults.listResults
    but this path changes with builds. We try the known path first,
    then fall back to a depth-first search for any list of dicts with
    a 'zpid' key (Zillow's stable property identifier).
    """
    # Known path (as of 2025)
    try:
        return (data["props"]["pageProps"]["searchPageState"]
                    ["cat1"]["searchResults"]["listResults"])
    except (KeyError, TypeError):
        pass

    # Fallback DFS
    return _dfs_find(data, 0)


def _dfs_find(node, depth: int) -> list:
    if depth > 10:
        return []
    if isinstance(node, list) and len(node) >= 1:
        if all(isinstance(i, dict) and "zpid" in i for i in node[:3]):
            return node
    if isinstance(node, dict):
        for v in node.values():
            result = _dfs_find(v, depth + 1)
            if result:
                return result
    return []


def _parse_listing(raw: dict, hood: str) -> Optional[Listing]:
    zpid       = raw.get("zpid")
    detail_url = raw.get("detailUrl") or raw.get("url") or ""
    if not detail_url:
        if zpid:
            detail_url = f"{BASE_URL}/homedetails/{zpid}_zpid/"
        else:
            return None
    if detail_url.startswith("/"):
        detail_url = BASE_URL + detail_url

    # Price — may be a formatted string "$2,800/mo" or a raw int
    price_raw = (
        raw.get("unformattedPrice")
        or raw.get("price")
        or _nested_get(raw, "hdpData", "homeInfo", "price")
        or ""
    )
    price = _parse_price(str(price_raw))

    # Beds / baths
    bedrooms  = _safe_int(
        raw.get("beds") or raw.get("bedrooms")
        or _nested_get(raw, "hdpData", "homeInfo", "bedrooms")
    )
    bathrooms = _safe_float(
        raw.get("baths") or raw.get("bathrooms")
        or _nested_get(raw, "hdpData", "homeInfo", "bathrooms")
    )

    # Address
    address = (
        raw.get("address")
        or raw.get("streetAddress")
        or _nested_get(raw, "hdpData", "homeInfo", "streetAddress")
        or ""
    )

    # Neighborhood from listing data if available, else use the search slug name
    neighborhood = (
        raw.get("neighborhood")
        or _nested_get(raw, "hdpData", "homeInfo", "neighborhood")
        or hood
    )

    # Title
    title = raw.get("statusText") or raw.get("name") or ""
    if not title:
        parts = []
        if bedrooms:
            parts.append(f"{bedrooms}BR")
        if address:
            parts.append(address)
        title = " — ".join(parts) if parts else "Zillow listing"

    # Date listed
    listed_raw = (
        raw.get("listedDate")
        or _nested_get(raw, "hdpData", "homeInfo", "daysOnZillow")
        or ""
    )
    date_listed = _parse_date_str(str(listed_raw))

    return Listing(
        url=detail_url,
        source="zillow",
        title=title,
        price=price,
        neighborhood=neighborhood or None,
        address=address or None,
        floor=None,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        rent_stabilized=None,
        date_listed=date_listed,
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _nested_get(d: dict, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _parse_price(raw: str) -> Optional[int]:
    # Strip everything except digits (handles "$2,800/mo", "2800", etc.)
    cleaned = re.sub(r"[^\d]", "", raw.split("/")[0])
    return int(cleaned) if cleaned else None


def _safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _parse_date_str(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw[:10], fmt)
        except ValueError:
            pass
    return None
