"""
Apartments.com scraper — NYC rental listings.

Approach: Apartments.com renders listings server-side and embeds structured
data in two places we can extract without a headless browser:
  1. JSON-LD <script type="application/ld+json"> blocks (schema.org markup)
  2. A JavaScript variable `window.__PRELOADED_STATE__` in the page HTML

We try both. If neither yields results, the scraper logs a warning and
returns an empty list rather than crashing. Dynamic content loaded after
page load (React hydration) will NOT be captured here — a Playwright-based
upgrade would be needed to catch those listings.

Limitations:
  - Apartments.com has bot detection that becomes more aggressive over time.
    If you start seeing empty results, that's the likely cause.
  - Their search URL structure can change; see _build_url() comments.
  - Floor and rent-stabilized fields are not reliably available.
"""

import json
import re
import time
from datetime import datetime
from typing import Optional

from curl_cffi import requests
from bs4 import BeautifulSoup

from apartment_hunter.models import Listing

BASE_URL = "https://www.apartments.com"
REQUEST_DELAY = 3.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.apartments.com/",
}

# Apartments.com uses its own borough/neighborhood slugs in URLs.
# These map our config neighborhood names to their URL segments.
NEIGHBORHOOD_SLUGS = {
    "bushwick":                   "bushwick-brooklyn-ny",
    "bedford-stuyvesant":         "bedford-stuyvesant-brooklyn-ny",
    "williamsburg":               "williamsburg-brooklyn-ny",
    "greenpoint":                 "greenpoint-brooklyn-ny",
    "east-williamsburg":          "east-williamsburg-brooklyn-ny",
    "clinton-hill":               "clinton-hill-brooklyn-ny",
    "crown-heights":              "crown-heights-brooklyn-ny",
    "prospect-lefferts-gardens":  "prospect-lefferts-gardens-brooklyn-ny",
    "ridgewood":                  "ridgewood-queens-ny",
}


def scrape(config: dict, existing_rows: dict | None = None) -> list[Listing]:
    neighborhoods = config["search"]["neighborhoods"]
    max_price = config["search"]["max_price"]
    bedrooms = config["search"]["bedrooms"]

    session = requests.Session(impersonate="chrome136")
    session.headers.update(HEADERS)

    all_listings: list[Listing] = []

    for hood in neighborhoods:
        slug = NEIGHBORHOOD_SLUGS.get(hood.lower())
        if not slug:
            print(f"  [Apartments.com] no URL slug for '{hood}', skipping")
            continue

        url = _build_url(slug, bedrooms, max_price)
        print(f"  [Apartments.com] fetching {hood}: {url}")

        try:
            resp = session.get(url, timeout=15)
        except requests.RequestsError as e:
            print(f"  [Apartments.com] request error ({hood}): {e}")
            time.sleep(REQUEST_DELAY)
            continue

        if resp.status_code in (403, 429):
            print(f"  [Apartments.com] {resp.status_code} — bot detection. "
                  "This scraper may need a proxy or Playwright upgrade.")
            time.sleep(REQUEST_DELAY)
            continue
        if resp.status_code != 200:
            print(f"  [Apartments.com] status {resp.status_code} for {hood}")
            time.sleep(REQUEST_DELAY)
            continue

        listings = _parse_page(resp.text, hood)
        print(f"  [Apartments.com] {len(listings)} listings for {hood}")
        all_listings.extend(listings)
        time.sleep(REQUEST_DELAY)

    # Deduplicate
    seen: set[str] = set()
    unique = []
    for l in all_listings:
        if l.url not in seen:
            seen.add(l.url)
            unique.append(l)

    print(f"  [Apartments.com] {len(unique)} listings total")
    return unique


def _build_url(neighborhood_slug: str, bedrooms: int, max_price: int) -> str:
    """
    Apartments.com URL format (as of 2025):
      /apartments/{neighborhood-slug}/{beds}-bedrooms/?max-price={price}
    Bedroom count maps to: 1 → "1-bedroom", 2 → "2-bedrooms", etc.
    """
    beds_str = f"{bedrooms}-bedroom{'s' if bedrooms != 1 else ''}"
    return f"{BASE_URL}/apartments/{neighborhood_slug}/{beds_str}/?max-price={max_price}"


def _parse_page(html: str, hood: str) -> list[Listing]:
    soup = BeautifulSoup(html, "lxml")

    # Strategy 1: JSON-LD structured data (schema.org/Apartment or schema.org/Product)
    listings = _parse_json_ld(soup, hood)
    if listings:
        return listings

    # Strategy 2: Preloaded state embedded in JS
    listings = _parse_preloaded_state(html, hood)
    if listings:
        return listings

    # Strategy 3: Parse article/li elements in the HTML directly
    listings = _parse_html_cards(soup, hood)
    return listings


def _parse_json_ld(soup: BeautifulSoup, hood: str) -> list[Listing]:
    results = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except json.JSONDecodeError:
            continue

        # May be a single object or a list
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            schema_type = item.get("@type", "")
            if schema_type in ("Apartment", "ApartmentComplex", "Product", "Offer"):
                listing = _parse_json_ld_item(item, hood)
                if listing:
                    results.append(listing)
    return results


def _parse_json_ld_item(item: dict, hood: str) -> Optional[Listing]:
    url = item.get("url") or item.get("@id") or ""
    if not url or not url.startswith("http"):
        return None

    name = item.get("name") or ""
    description = item.get("description") or ""

    # Price — look inside "offers" if present
    price = None
    offers = item.get("offers") or item.get("priceSpecification")
    if isinstance(offers, dict):
        price = _parse_price(str(offers.get("price") or offers.get("lowPrice") or ""))
    elif isinstance(offers, list) and offers:
        price = _parse_price(str(offers[0].get("price") or ""))
    if price is None:
        price = _parse_price(str(item.get("price") or ""))

    # Address
    addr_obj = item.get("address") or {}
    if isinstance(addr_obj, dict):
        street = addr_obj.get("streetAddress") or ""
        city   = addr_obj.get("addressLocality") or ""
        address = f"{street}, {city}".strip(", ") or None
    else:
        address = str(addr_obj) or None

    # Beds/baths — often in numberOfRooms or description
    bedrooms = _safe_int(item.get("numberOfRooms") or item.get("bedrooms"))
    bathrooms = _safe_float(item.get("numberOfBathroomsTotal") or item.get("bathrooms"))

    return Listing(
        url=url,
        source="apartments_com",
        title=name or description[:80] or "Apartments.com listing",
        price=price,
        neighborhood=hood,
        address=address,
        floor=None,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        rent_stabilized=None,
        date_listed=None,
    )


def _parse_preloaded_state(html: str, hood: str) -> list[Listing]:
    """Try to extract window.__PRELOADED_STATE__ or similar JS variables."""
    match = re.search(
        r"window\.__(?:PRELOADED_STATE|INITIAL_STATE|APP_STATE)__\s*=\s*(\{.+?\});",
        html,
        re.DOTALL,
    )
    if not match:
        return []
    try:
        state = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []

    raw_listings = _dfs_find_listings(state, 0)
    results = []
    for raw in raw_listings:
        listing = _parse_preloaded_listing(raw, hood)
        if listing:
            results.append(listing)
    return results


def _dfs_find_listings(node, depth: int) -> list:
    if depth > 8:
        return []
    if isinstance(node, list) and len(node) > 2:
        if all(isinstance(i, dict) and ("price" in i or "rent" in i) for i in node[:3]):
            return node
    if isinstance(node, dict):
        for v in node.values():
            r = _dfs_find_listings(v, depth + 1)
            if r:
                return r
    return []


def _parse_preloaded_listing(raw: dict, hood: str) -> Optional[Listing]:
    url = raw.get("url") or raw.get("listingUrl") or raw.get("propertyUrl") or ""
    if not url:
        return None
    if url.startswith("/"):
        url = BASE_URL + url

    price = _parse_price(str(raw.get("price") or raw.get("rent") or raw.get("askingRent") or ""))
    bedrooms = _safe_int(raw.get("bedrooms") or raw.get("beds") or raw.get("bedroomCount"))
    bathrooms = _safe_float(raw.get("bathrooms") or raw.get("baths") or raw.get("bathroomCount"))
    address = raw.get("address") or raw.get("streetAddress") or raw.get("street") or None
    title = raw.get("name") or raw.get("title") or raw.get("propertyName") or ""
    if not title:
        parts = [f"{bedrooms}BR" if bedrooms else "", address or "", hood]
        title = " — ".join(p for p in parts if p)

    return Listing(
        url=url,
        source="apartments_com",
        title=title,
        price=price,
        neighborhood=hood,
        address=address,
        floor=None,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        rent_stabilized=None,
        date_listed=None,
    )


def _parse_html_cards(soup: BeautifulSoup, hood: str) -> list[Listing]:
    """
    Last-resort: parse visible listing cards from the HTML.
    Apartments.com uses <article> tags with data-listingid attributes.
    """
    results = []
    for article in soup.find_all("article", {"data-listingid": True}):
        listing = _parse_article(article, hood)
        if listing:
            results.append(listing)
    return results


def _parse_article(article, hood: str) -> Optional[Listing]:
    # URL
    link_tag = article.find("a", {"class": re.compile(r"property-link|listing-link", re.I)})
    if not link_tag:
        link_tag = article.find("a", href=True)
    url = link_tag["href"] if link_tag else ""
    if not url:
        return None
    if url.startswith("/"):
        url = BASE_URL + url

    # Title / name
    name_tag = article.find(class_=re.compile(r"property-title|js-placardTitle", re.I))
    title = name_tag.get_text(strip=True) if name_tag else ""

    # Price
    price_tag = article.find(class_=re.compile(r"property-pricing|price-range", re.I))
    price_text = price_tag.get_text(strip=True) if price_tag else ""
    price = _parse_price(price_text)

    # Beds/baths from the spec text like "2 Beds | 1 Bath"
    spec_tag = article.find(class_=re.compile(r"property-beds|property-specs", re.I))
    spec_text = spec_tag.get_text(strip=True) if spec_tag else ""
    bedrooms = _safe_int(_regex_first(r"(\d)\s*[Bb]ed", spec_text))
    bathrooms = _safe_float(_regex_first(r"([\d.]+)\s*[Bb]ath", spec_text))

    # Address
    addr_tag = article.find(class_=re.compile(r"property-address|property-sub-title", re.I))
    address = addr_tag.get_text(strip=True) if addr_tag else None

    if not title:
        parts = [f"{bedrooms}BR" if bedrooms else "", address or "", hood]
        title = " — ".join(p for p in parts if p) or "Apartments.com listing"

    return Listing(
        url=url,
        source="apartments_com",
        title=title,
        price=price,
        neighborhood=hood,
        address=address,
        floor=None,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        rent_stabilized=None,
        date_listed=None,
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _parse_price(raw: str) -> Optional[int]:
    cleaned = re.sub(r"[^\d]", "", raw.split("–")[0].split("-")[0])
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


def _regex_first(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text)
    return m.group(1) if m else None
