"""
NYC subway proximity enrichment.

For each new listing with a parseable street address, geocodes it using the
Nominatim API (OpenStreetMap, free, no key required), then finds the nearest
subway stations from the MTA GTFS static feed, computing walking-time
estimates via the haversine formula with a Manhattan grid correction.

Station data is downloaded once from the MTA, processed into a small cache
file (subway_stations.csv), and reused on every subsequent run.

Populates listing.nearest_subway, e.g.:
  "DeKalb Av (L) ~4 min; Myrtle-Wyckoff Avs (L/M) ~9 min; Jefferson St (L) ~11 min"
"""

import csv
import io
import json
import re
import time
import zipfile
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path
from typing import Optional

import requests

from apartment_hunter.models import Listing

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATA_DIR       = Path(__file__).parent / "data"
_STATIONS_CSV   = _DATA_DIR / "subway_stations.csv"
_GEOCODE_CACHE  = _DATA_DIR / "geocode_cache.json"

# MTA GTFS static feed for NYC Subway — public, no API key required.
_MTA_GTFS_URL = (
    "https://web.mta.info/developers/data/nyct/subway/google_transit.zip"
)

# Nominatim (OpenStreetMap) — free, no API key, 1 req/sec rate limit.
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Maps config neighborhood slugs to their NYC borough, used to narrow geocoding
# queries (e.g. "1505 Broadway, Brooklyn, NY" beats "1505 Broadway, New York City, NY"
# because Nominatim would otherwise match the more prominent Manhattan Broadway).
_NEIGHBORHOOD_BOROUGH: dict[str, str] = {
    "bushwick":                   "Brooklyn",
    "bedford-stuyvesant":         "Brooklyn",
    "clinton-hill":               "Brooklyn",
    "williamsburg":               "Brooklyn",
    "east-williamsburg":          "Brooklyn",
    "greenpoint":                 "Brooklyn",
    "crown-heights":              "Brooklyn",
    "prospect-lefferts-gardens":  "Brooklyn",
    "ridgewood":                  "Queens",
}

# Walking pace ~3.1 mph = 83 m/min.  Manhattan grid adds ~30% to crow-flies dist.
_WALK_M_PER_MIN = 83.0
_GRID_FACTOR    = 1.3

# Nominatim requires >= 1 req/sec; use 1.1s to stay safely within the limit.
_GEOCODE_SLEEP  = 1.1

_TOP_N = 3


# ---------------------------------------------------------------------------
# Haversine (stdlib only)
# ---------------------------------------------------------------------------

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# ---------------------------------------------------------------------------
# Station data — built from MTA GTFS, cached as subway_stations.csv
# ---------------------------------------------------------------------------

def _read_csv_from_zip(zf: zipfile.ZipFile, filename: str):
    """Return a csv.DictReader for a named file inside a ZipFile."""
    return csv.DictReader(io.TextIOWrapper(zf.open(filename), encoding="utf-8"))


def _build_stations_from_gtfs(zip_bytes: bytes) -> None:
    """
    Parse the MTA GTFS zip and write subway_stations.csv with columns:
      name, routes, lat, lon

    Strategy:
      1. stops.txt     → station coordinates + parent/child relationships
      2. routes.txt    → route_id → short name (A, C, E, L, 1, 2, ...)
      3. trips.txt     → trip_id → route_id; pick one rep trip per route
      4. stop_times.txt → for each rep trip, map child stop → route name
      5. Roll child stops up to their parent station
      6. Write parent stations (location_type=1) to cache CSV
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:

        # 1. Stops
        stops = {}  # stop_id → {name, lat, lon, parent, is_parent}
        for row in _read_csv_from_zip(zf, "stops.txt"):
            try:
                stops[row["stop_id"]] = {
                    "name": row["stop_name"].strip(),
                    "lat":  float(row["stop_lat"]),
                    "lon":  float(row["stop_lon"]),
                    "parent":    row.get("parent_station", "").strip(),
                    "is_parent": row.get("location_type", "0").strip() == "1",
                }
            except (ValueError, KeyError):
                continue

        # 2. Routes: route_id → short name
        route_names = {}
        for row in _read_csv_from_zip(zf, "routes.txt"):
            route_names[row["route_id"]] = row.get("route_short_name", row["route_id"]).strip()

        # 3. Trips: trip_id → route_id; one representative trip per route
        trip_to_route = {}
        route_to_rep_trip = {}
        for row in _read_csv_from_zip(zf, "trips.txt"):
            trip_to_route[row["trip_id"]] = row["route_id"]
            if row["route_id"] not in route_to_rep_trip:
                route_to_rep_trip[row["route_id"]] = row["trip_id"]

        representative_trips = set(route_to_rep_trip.values())

        # 4. Stop times — only process the one representative trip per route
        #    to minimise work (we just need route membership, not schedules).
        stop_routes: dict[str, set[str]] = {}  # child_stop_id → set of route short names
        for row in _read_csv_from_zip(zf, "stop_times.txt"):
            if row["trip_id"] not in representative_trips:
                continue
            route_id = trip_to_route.get(row["trip_id"], "")
            short    = route_names.get(route_id, route_id)
            if short:
                stop_routes.setdefault(row["stop_id"], set()).add(short)

    # 5. Roll child stops up to their parent station
    parent_routes: dict[str, set[str]] = {}
    for child_id, routes in stop_routes.items():
        parent_id = stops.get(child_id, {}).get("parent") or child_id
        if parent_id not in stops:
            parent_id = child_id
        parent_routes.setdefault(parent_id, set()).update(routes)

    # 6. Write CSV — only parent stations (location_type=1)
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(_STATIONS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "routes", "lat", "lon"])
        for stop_id, info in stops.items():
            if not info["is_parent"]:
                continue
            routes = parent_routes.get(stop_id, set())
            # Sort: letters before numbers for consistent display (A/C/E not 1/A/C)
            routes_str = "/".join(sorted(routes, key=lambda r: (r.isdigit(), r)))
            writer.writerow([info["name"], routes_str, info["lat"], info["lon"]])
            count += 1

    print(f"  [subway] built {count} stations from GTFS, saved {_STATIONS_CSV.name}")


def _download_stations() -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("  [subway] downloading MTA GTFS subway data (one-time setup)...")
    resp = requests.get(
        _MTA_GTFS_URL,
        headers={"User-Agent": "apartment-hunter/1.0"},
        timeout=60,
    )
    resp.raise_for_status()
    print(f"  [subway] processing GTFS zip ({len(resp.content) // 1024} KB)...")
    _build_stations_from_gtfs(resp.content)


def _load_stations() -> list[tuple[str, str, float, float]]:
    """
    Returns (name, routes, lat, lon) for every parent subway station.
    Downloads and builds the cache on first call; fast on subsequent calls.
    """
    if not _STATIONS_CSV.exists():
        _download_stations()

    stations = []
    with open(_STATIONS_CSV, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            try:
                stations.append((row["name"], row["routes"], float(row["lat"]), float(row["lon"])))
            except (ValueError, KeyError):
                continue
    return stations


# ---------------------------------------------------------------------------
# Street name normalisation (applied before geocoding)
# ---------------------------------------------------------------------------

_STREET_TYPES = r"(Avenue|Street|Place|Boulevard|Road|Drive|Lane|Court|Terrace|Way)"


def _ordinal(n: int) -> str:
    """1 → '1st', 2 → '2nd', 22 → '22nd', 11 → '11th', etc."""
    if 11 <= (n % 100) <= 13:   # 11th / 12th / 13th exception
        return f"{n}th"
    return f"{n}" + {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")


def _normalize_street(s: str) -> str:
    """
    Fix known street name formatting issues before geocoding.

    1. LLC suffix (scraper artefact): "Beadel Llc" → "Beadel Street"
    2. Split Mac/Mc prefix: "Mac Dougal" → "MacDougal", "Mc Kibbin" → "McKibbin"
       Negative lookahead prevents collapsing "Mac Avenue" → "MacAvenue".
    3. Named De-prefix fixes (not generalised — "De Sales" is correctly two words):
         "De Kalb" → "DeKalb"
    4. Numbered street/avenue/place names without ordinal suffix:
         "70 Avenue" → "70th Avenue", "62 Street" → "62nd Street"
       Lookbehind (?<=\\s) skips the building number at the start of the string.
    """
    # 1. LLC → Street
    s = re.sub(r"\bLLC\b", "Street", s, flags=re.IGNORECASE)

    # 2. Mac / Mc prefix — broad rule, excludes street-type words via negative lookahead
    _street_type_initials = r"(?!Avenue|Street|Place|Boulevard|Road|Drive|Lane|Court|Terrace|Way\b)"
    s = re.sub(r"\b(Mac|Mc)\s+" + _street_type_initials + r"([A-Z])", r"\1\2", s)

    # 3. Named De-prefix fixes
    s = re.sub(r"\bDe\s+Kalb\b", "DeKalb", s, flags=re.IGNORECASE)

    # 4. Ordinal suffixes for numbered streets/avenues/places
    def _add_ordinal(m: re.Match) -> str:
        return f"{_ordinal(int(m.group(1)))} {m.group(2)}"
    s = re.sub(r"(?<=\s)(\d+)\s+" + _STREET_TYPES + r"\b", _add_ordinal, s)

    return s


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def _nominatim(query: str, verbose: bool, label: str) -> Optional[tuple[float, float]]:
    """Single Nominatim lookup. Returns (lat, lon) or None."""
    if verbose:
        print(f"    geocoding ({label}): {query!r}")
    try:
        resp = requests.get(
            _NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "us"},
            headers={"User-Agent": "apartment-hunter/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if not results:
            if verbose:
                print("    → no results")
            return None
        lat = float(results[0]["lat"])
        lon = float(results[0]["lon"])
        if verbose:
            display = results[0].get("display_name", "")[:80]
            print(f"    → ({lat:.5f}, {lon:.5f})  matched: {display!r}")
        return lat, lon
    except Exception as e:
        if verbose:
            print(f"    → exception: {e}")
        return None


def _is_intersection(address: str) -> bool:
    """True if address looks like a cross-street intersection rather than a numbered address."""
    return bool(re.search(r"\s+(?:&|and|/)\s+", address, re.IGNORECASE))


def _geocode(address: str, neighborhood: Optional[str], verbose: bool = False) -> Optional[tuple[float, float]]:
    """
    Returns (lat, lon) using Nominatim (OpenStreetMap).

    Handles two address forms:
      - Numbered: "304 Evergreen Ave #3R" → strip unit, normalise, geocode
      - Intersection: "Myrtle Ave & Broadway" → geocode as-is (Nominatim supports intersections)

    Query strategy (stops at first hit):
      1. address + neighborhood + borough, NY
      2. address + borough, NY  (fallback if #1 returns nothing)
    """
    borough = _NEIGHBORHOOD_BOROUGH.get((neighborhood or "").lower(), "New York City")
    neighborhood_label = (neighborhood or "").replace("-", " ").title()

    if _is_intersection(address):
        # Normalise all intersection separators to "&" for Nominatim
        street = re.sub(r"\s*/\s*|\s+and\s+|\s+near\s+|\s+at\s+|\s*@\s*", " & ", address.strip(), flags=re.IGNORECASE)
    else:
        # Strip unit designator then normalise street name
        street = re.sub(r"\s*#\S+$", "", address).strip()
        street = _normalize_street(street)

    q1 = f"{street}, {neighborhood_label}, {borough}, NY" if neighborhood_label else f"{street}, {borough}, NY"
    coords = _nominatim(q1, verbose, "with neighborhood")
    if coords:
        return coords

    time.sleep(_GEOCODE_SLEEP)
    q2 = f"{street}, {borough}, NY"
    if q2 != q1:
        coords = _nominatim(q2, verbose, "borough fallback")
    return coords


# ---------------------------------------------------------------------------
# Nearest stations
# ---------------------------------------------------------------------------

def _nearest(
    lat: float,
    lon: float,
    stations: list[tuple[str, str, float, float]],
) -> list[tuple[str, str, int]]:
    """Returns the _TOP_N closest stations as (name, routes, walk_minutes)."""
    scored = []
    for name, routes, slat, slon in stations:
        dist = _haversine_m(lat, lon, slat, slon) * _GRID_FACTOR
        mins = max(1, round(dist / _WALK_M_PER_MIN))
        scored.append((name, routes, mins))
    scored.sort(key=lambda x: x[2])
    return scored[:_TOP_N]


def _format(stations: list[tuple[str, str, int]]) -> str:
    parts = []
    for name, routes, mins in stations:
        suffix = f" ({routes})" if routes else ""
        parts.append(f"{name}{suffix} ~{mins} min")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Geocode cache — persists (address, neighborhood) → (lat, lon) across runs
# ---------------------------------------------------------------------------

def _load_geocode_cache() -> dict:
    if not _GEOCODE_CACHE.exists():
        return {}
    try:
        with open(_GEOCODE_CACHE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_geocode_cache(cache: dict) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(_GEOCODE_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def enrich(listings: list[Listing], verbose: bool = False) -> int:
    """
    Geocode each listing and populate nearest_subway with the closest stations.
    Skips listings that already have nearest_subway set, or whose address
    doesn't start with a street number (too vague to geocode reliably).
    Returns the number of listings enriched.
    """
    no_address  = [l for l in listings if not l.address]
    no_geocode  = [
        l for l in listings
        if l.address
        and not re.match(r"^\d", l.address.strip())
        and not _is_intersection(l.address)
    ]
    addressable = [
        l for l in listings
        if l.address
        and not l.nearest_subway
        and (re.match(r"^\d", l.address.strip()) or _is_intersection(l.address))
    ]

    if verbose:
        print(f"  [subway] {len(listings)} total listings")
        print(f"  [subway]   {len(no_address)} skipped — no address")
        print(f"  [subway]   {len(no_geocode)} skipped — address not geocodeable")
        print(f"  [subway]   {len(addressable)} will be geocoded")
        for l in no_geocode:
            print(f"    skipped address: {l.address!r}")

    if not addressable:
        print("  [subway] nothing to geocode")
        return 0

    try:
        stations = _load_stations()
    except Exception as e:
        print(f"  [subway] could not load station data: {e} — skipping")
        return 0

    print(f"  [subway] {len(stations)} stations loaded; geocoding {len(addressable)} listing(s)...")

    geocode_cache = _load_geocode_cache()
    enriched   = 0
    cache_hits = 0
    api_calls  = 0

    for listing in addressable:
        cache_key = f"{listing.address}|{listing.neighborhood or ''}"

        if cache_key in geocode_cache:
            cached = geocode_cache[cache_key]
            coords = tuple(cached) if cached is not None else None
            cache_hits += 1
        else:
            coords = _geocode(listing.address, listing.neighborhood, verbose=verbose)
            geocode_cache[cache_key] = list(coords) if coords is not None else None
            _save_geocode_cache(geocode_cache)
            api_calls += 1
            time.sleep(_GEOCODE_SLEEP)  # Nominatim: max 1 req/sec

        if coords is None:
            continue
        lat, lon = coords
        nearest = _nearest(lat, lon, stations)
        if nearest:
            listing.nearest_subway = _format(nearest)
            enriched += 1

    if cache_hits:
        print(f"  [subway] {cache_hits} address(es) from cache, {api_calls} new API call(s)")
    print(f"  [subway] enriched {enriched} listing(s) with subway proximity")
    return enriched
