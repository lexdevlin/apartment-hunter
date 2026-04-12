"""
NYC DHCR Rent Stabilized Building crosscheck.

Downloads the annual DHCR building files for Brooklyn and Queens, parses
them with pdfplumber, and checks each scraped listing's address against
the resulting lookup table.

DHCR PDF columns used:
  BLDGNO1 — building street number (e.g. "304", "17-11")
  STREET1 — street name without suffix (e.g. "EVERGREEN", "TROUTMAN")
  STSUFX1 — suffix abbreviation (e.g. "AVE", "ST", "PKWY")
  CITY    — neighborhood (e.g. "BROOKLYN", "RIDGEWOOD")

PDF source (updated annually):
  https://rentguidelinesboard.cityofnewyork.us/
"""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pdfplumber
import requests

from apartment_hunter.models import Listing

# ---------------------------------------------------------------------------
# URLs + cache location
# ---------------------------------------------------------------------------

BROOKLYN_URL = (
    "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/"
    "2024-DHCR-Bldg-File-Brooklyn.pdf"
)
QUEENS_URL = (
    "https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2025/12/"
    "2024-DHCR-Bldg-File-Queens.pdf"
)

_DATA_DIR   = Path(__file__).parent / "data"
_CACHE_PATH = _DATA_DIR / "dhcr_cache.json"

# ---------------------------------------------------------------------------
# Suffix normalisation
# ---------------------------------------------------------------------------

_SUFFIX_MAP = {
    "AVENUE": "AVE",     "AVE": "AVE",
    "STREET": "ST",      "ST": "ST",
    "BOULEVARD": "BLVD", "BLVD": "BLVD",
    "PLACE": "PL",       "PL": "PL",
    "ROAD": "RD",        "RD": "RD",
    "DRIVE": "DR",       "DR": "DR",
    "PARKWAY": "PKWY",   "PKWY": "PKWY",
    "COURT": "CT",       "CT": "CT",
    "LANE": "LN",        "LN": "LN",
    "TERRACE": "TER",    "TER": "TER",
    "WAY": "WAY",
    "HIGHWAY": "HWY",    "HWY": "HWY",
    "BROADWAY": "BROADWAY",
    "LOOP": "LOOP",
    "WALK": "WALK",
    "SQUARE": "SQ",      "SQ": "SQ",
    "PLAZA": "PLZ",      "PLZ": "PLZ",
    "EXPRESSWAY": "EXPY","EXPY": "EXPY",
}


def _norm_suffix(s: str) -> str:
    return _SUFFIX_MAP.get(s.upper().strip(), s.upper().strip())


def _norm_street(s: str) -> str:
    return re.sub(r"\s+", " ", s.upper().strip())


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

AddressKey = tuple[str, str, str]         # (building_no, street_name, suffix)
RangeKey   = tuple[int, int, str, str]   # (bldg_no_start, bldg_no_end, street, suffix)

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _download(url: str, dest: Path) -> None:
    """Download to dest; skip if already cached."""
    if dest.exists():
        return
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  [DHCR] downloading {dest.name}...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    print(f"  [DHCR] saved {dest.name} ({dest.stat().st_size // 1024} KB)")

# ---------------------------------------------------------------------------
# Parse-result cache (avoids re-parsing PDFs on every run)
# ---------------------------------------------------------------------------

def _load_parse_cache(
    bk_path: Path, qn_path: Path
) -> Optional[tuple[set, list]]:
    """
    Return cached (exact_keys, range_keys) if the cache exists and both PDF
    files have the same mtime as when the cache was built.  Returns None if
    the cache is missing, stale, or corrupt.
    """
    if not _CACHE_PATH.exists():
        return None
    try:
        with open(_CACHE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        mtimes = data.get("mtimes", {})
        for key, path in (("brooklyn", bk_path), ("queens", qn_path)):
            if not path.exists():
                return None
            if abs(mtimes.get(key, 0) - path.stat().st_mtime) > 1:
                return None  # PDF was re-downloaded
        exact  = set(tuple(k) for k in data["exact"])
        ranges = [tuple(r) for r in data["ranges"]]
        return exact, ranges
    except Exception:
        return None


def _save_parse_cache(
    bk_path: Path, qn_path: Path,
    exact: set, ranges: list,
) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "mtimes": {
            "brooklyn": bk_path.stat().st_mtime if bk_path.exists() else 0,
            "queens":   qn_path.stat().st_mtime if qn_path.exists() else 0,
        },
        "exact":  [list(k) for k in exact],
        "ranges": [list(r) for r in ranges],
    }
    with open(_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print(f"  [DHCR] parse results cached → {_CACHE_PATH.name}")


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

_TARGET_COLS = {"BLDGNO1", "STREET1", "STSUFX1"}


def _parse_pdf(pdf_path: Path, verbose: bool = False) -> tuple[set[AddressKey], list[RangeKey]]:
    """
    Extract address records from a DHCR building file.

    Returns (exact_keys, range_keys):
      exact_keys  — simple building numbers: ("304", "EVERGREEN", "AVE")
      range_keys  — range entries like "1867 TO 1875 ATLANTIC AVE":
                    (1867, 1875, "ATLANTIC", "AVE")

    Strategy: extract_words() gives each word's actual PDF x-coordinate.  The
    header row is detected by content (BLDGNO1 / STREET1 / STSUFX1 present).
    Column centers are computed as the midpoint of each header word's bounding
    box; column boundaries are the midpoints between adjacent column centers.
    Each data word is assigned to whichever column's range its own center falls
    in.  This is robust to headers that are centered or offset relative to the
    left-aligned data beneath them — which is why character-offset slicing on
    extract_text() output fails (the header label widths don't match data widths).
    ZIP is included when present so its boundary with BLDGNO1 is computed
    correctly and ZIP codes don't bleed into the building-number field.
    """
    keys:   set[AddressKey] = set()
    ranges: list[RangeKey]  = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            if not words:
                continue

            # Group words into lines by y-position (3-pt buckets)
            line_map: dict[int, list] = {}
            for w in words:
                y_key = round(w["top"] / 3) * 3
                line_map.setdefault(y_key, []).append(w)

            col_names: Optional[list[str]] = None
            bounds:    Optional[list[float]] = None

            for y_key in sorted(line_map):
                line = sorted(line_map[y_key], key=lambda w: w["x0"])
                texts_upper = {w["text"].upper(): w for w in line}

                # ---- Detect header line ----
                if col_names is None and _TARGET_COLS.issubset(set(texts_upper.keys())):
                    col_centers: dict[str, float] = {}
                    for col in ("ZIP", "BLDGNO1", "STREET1", "STSUFX1", "CITY"):
                        if col in texts_upper:
                            hw = texts_upper[col]
                            col_centers[col] = (hw["x0"] + hw["x1"]) / 2.0
                    if "CITY" not in col_centers:
                        col_centers["CITY"] = float(page.bbox[2])

                    sorted_cols = sorted(col_centers.items(), key=lambda x: x[1])
                    col_names = [c for c, _ in sorted_cols]
                    centers   = [cx for _, cx in sorted_cols]
                    bounds    = [(centers[i] + centers[i + 1]) / 2.0
                                 for i in range(len(centers) - 1)]

                    if verbose:
                        print(f"  [DHCR] col centers (pts): "
                              f"{ {c: round(cx) for c, cx in sorted_cols} }")
                        print(f"  [DHCR] boundaries (pts):  "
                              f"{[round(b) for b in bounds]}")
                    continue

                if col_names is None:
                    continue

                # ---- Assign each word to a column by center x ----
                col_words: dict[str, list[str]] = {c: [] for c in col_names}
                for w in line:
                    wc = (w["x0"] + w["x1"]) / 2.0
                    assigned = col_names[-1]
                    for i, bound in enumerate(bounds):
                        if wc < bound:
                            assigned = col_names[i]
                            break
                    col_words[assigned].append(w["text"])

                bno_words = col_words.get("BLDGNO1", [])
                st_words  = col_words.get("STREET1", [])

                # Range entries like "1867 TO 1875" are wider than a normal
                # building number, so "1875" can spill past the column boundary
                # into STREET1.  Reclaim it if BLDGNO1 ends with "TO" and the
                # first STREET1 word is a number.
                if (bno_words and bno_words[-1].upper() == "TO"
                        and st_words and re.match(r"^\d", st_words[0])):
                    bno_words.append(st_words.pop(0))

                bno    = " ".join(bno_words).strip().upper()
                street = " ".join(st_words).strip().upper()
                # STSUFX1 is always a single abbreviation; secondary-address
                # columns (BLDGNO2, STREET2) can spill into the same x-band,
                # so take only the first word.
                sfx_words = col_words.get("STSUFX1", [])
                sfx = sfx_words[0].strip().upper() if sfx_words else ""

                if not (bno and street and re.match(r"^\d", bno)):
                    continue

                street_n = _norm_street(street)
                sfx_n    = _norm_suffix(sfx)

                # BLDGNO1 can hold a range: "1867 TO 1875"
                range_m = re.match(r"^(\d+)\s+TO\s+(\d+)$", bno)
                if range_m:
                    ranges.append((int(range_m.group(1)), int(range_m.group(2)), street_n, sfx_n))
                else:
                    keys.add((bno, street_n, sfx_n))

    return keys, ranges

# ---------------------------------------------------------------------------
# Listing address parser
# ---------------------------------------------------------------------------

def _parse_listing_address(address: str) -> Optional[AddressKey]:
    """
    Parse a listing address into (building_no, street_name, suffix).

    Examples:
      "304 Evergreen Avenue #3R"   → ("304", "EVERGREEN", "AVE")
      "17-11 Hancock Street #308"  → ("17-11", "HANCOCK", "ST")
      "8 Palmetto Street"          → ("8", "PALMETTO", "ST")
      "99 Eastern Parkway"         → ("99", "EASTERN", "PKWY")
    """
    # Strip unit designator (#3R, #308, etc.)
    addr = re.sub(r"\s*#\S+.*$", "", address).strip()
    # Strip borough after comma ("...Avenue, Brooklyn" → "...Avenue")
    addr = re.sub(r",.*$", "", addr).strip()

    tokens = addr.split()
    if len(tokens) < 3:
        return None

    building_no = tokens[0].upper()
    if not re.match(r"^\d", building_no):
        return None

    suffix = _norm_suffix(tokens[-1])
    street = _norm_street(" ".join(tokens[1:-1]))

    return (building_no, street, suffix)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def crosscheck(listings: list[Listing]) -> int:
    """
    Download DHCR PDFs (cached after first run), build address lookup, then
    set rent_stabilized=True on any listing whose address appears in the
    database.  Never downgrades an existing True.

    Returns the number of listings newly flagged.
    """
    bk_path = _DATA_DIR / "DHCR-Brooklyn.pdf"
    qn_path  = _DATA_DIR / "DHCR-Queens.pdf"

    try:
        _download(BROOKLYN_URL, bk_path)
        _download(QUEENS_URL,   qn_path)
    except Exception as e:
        print(f"  [DHCR] download failed: {e} — crosscheck skipped")
        return 0

    cached = _load_parse_cache(bk_path, qn_path)
    if cached:
        exact, ranges = cached
        print(f"  [DHCR] loaded from cache: {len(exact):,} exact + {len(ranges):,} range entries")
    else:
        print("  [DHCR] parsing building files (this takes ~30 s; result will be cached)...")
        exact:  set[AddressKey] = set()
        ranges: list[RangeKey]  = []
        for path in (bk_path, qn_path):
            try:
                e, r = _parse_pdf(path)
                exact.update(e)
                ranges.extend(r)
                print(f"  [DHCR] {path.name}: {len(e):,} exact + {len(r):,} range entries loaded")
            except Exception as e:
                print(f"  [DHCR] parse error for {path.name}: {e}")
        if exact or ranges:
            _save_parse_cache(bk_path, qn_path, exact, ranges)

    if not exact and not ranges:
        print("  [DHCR] warning: no addresses parsed — check pdfplumber output")
        return 0

    # Build a fast (street, suffix) → [(start, end), ...] lookup for range entries
    range_lookup: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for start, end, street, sfx in ranges:
        range_lookup[(street, sfx)].append((start, end))

    flagged   = 0
    confirmed = 0
    for listing in listings:
        if not listing.address:
            continue
        key = _parse_listing_address(listing.address)
        if not key:
            continue
        bno, street, sfx = key

        in_db = key in exact
        if not in_db and re.match(r"^\d+$", bno):
            bno_int = int(bno)
            for start, end in range_lookup.get((street, sfx), []):
                if start <= bno_int <= end:
                    in_db = True
                    break

        if not in_db:
            continue

        if listing.rent_stabilized:
            confirmed += 1   # already True from listing description — DHCR agrees
        else:
            listing.rent_stabilized = True
            flagged += 1

    print(f"  [DHCR] {flagged} listing(s) newly flagged as rent-stabilized")
    if confirmed:
        print(f"  [DHCR] {confirmed} self-described rent-stabilized listing(s) confirmed in DHCR database")
    else:
        print(f"  [DHCR] 0 self-described rent-stabilized listings found in DHCR database (address mismatch?)")
    return flagged
