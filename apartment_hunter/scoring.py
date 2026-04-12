"""
Priority scoring for apartment listings.

Each field contributes up to a fixed number of points (defined in config.yaml).
Fields with no data (None / blank) are excluded from both the numerator AND the
denominator, so the score is always normalised to 100 over available fields only.

A listing is flagged is_priority when its score >= priority_scoring.threshold.
"""

import re
from typing import Optional

from apartment_hunter.models import Listing

# Expand common street-type abbreviations to their full form before matching.
# Applied to both the haystack and config strings so "Ave", "Ave.", and "Avenue"
# all resolve to "avenue" and match each other.
_ABBREV_MAP = [
    (r"\bave?\.?\b",   "avenue"),
    (r"\bst\.?\b",     "street"),
    (r"\bblvd\.?\b",   "boulevard"),
    (r"\brd\.?\b",     "road"),
    (r"\bpl\.?\b",     "place"),
    (r"\bdr\.?\b",     "drive"),
    (r"\bct\.?\b",     "court"),
    (r"\bln\.?\b",     "lane"),
    (r"\bpkwy\.?\b",   "parkway"),
    (r"\bter\.?\b",    "terrace"),
]


def _normalize_street(text: str) -> str:
    """Lowercase and expand street-type abbreviations."""
    s = text.lower()
    for pattern, replacement in _ABBREV_MAP:
        s = re.sub(pattern, replacement, s, flags=re.IGNORECASE)
    return s


def is_priority_override(listing: Listing, config: dict) -> bool:
    """
    Return True if the listing matches a priority address or intersection,
    regardless of its computed score.

    Priority addresses: any substring of the listing's address (case-insensitive).
    Priority intersections: both street names must appear in the address or title.
      This catches cross-street listings like "69th Ave near Onderdonk Ave"
      even when no house number is known.
    """
    haystack = _normalize_street(
        f"{listing.address or ''} {listing.title or ''}"
    )

    for pat in config.get("priority_addresses", []) or []:
        if pat and _normalize_street(pat) in haystack:
            return True

    for pair in config.get("priority_intersections", []) or []:
        if not isinstance(pair, list) or len(pair) < 2:
            continue
        s1, s2 = _normalize_street(pair[0]), _normalize_street(pair[1])
        if s1 in haystack and s2 in haystack:
            return True

    return False


def compute_score(listing: Listing, cfg: dict) -> float:
    """Return a 0–100 priority score for a listing."""
    earned = 0.0
    available = 0.0

    def _add(pts_earned: float, pts_max: float) -> None:
        nonlocal earned, available
        earned    += pts_earned
        available += pts_max

    # --- Price ---------------------------------------------------------------
    pcfg = cfg.get("price", {})
    if listing.price is not None and pcfg:
        hi = pcfg["max_price"]   # $3300 → 0 pts
        lo = pcfg["min_price"]   # $2800 → max pts
        mx = pcfg["max_points"]  # 24
        if listing.price <= lo:
            pts = mx
        elif listing.price >= hi:
            pts = 0.0
        else:
            pts = mx * (hi - listing.price) / (hi - lo)
        _add(pts, mx)

    # --- Floor ---------------------------------------------------------------
    fcfg = cfg.get("floor", {})
    floor_num = _parse_floor_num(listing.floor)
    if floor_num is not None and fcfg:
        mx = fcfg["points_3rd_plus"]
        if floor_num >= 3:
            pts = float(mx)
        elif floor_num == 2:
            pts = float(fcfg["points_2nd"])
        else:
            pts = 0.0
        _add(pts, mx)

    # --- Subway --------------------------------------------------------------
    scfg = cfg.get("subway", {})
    if listing.subway_lines and scfg:
        pts = _score_subway(listing.subway_lines, scfg)
        _add(pts, scfg["points_high"])

    # --- Neighborhood --------------------------------------------------------
    ncfg = cfg.get("neighborhood", {})
    if listing.neighborhood and ncfg:
        pts = _score_neighborhood(listing.neighborhood, ncfg)
        _add(pts, ncfg["high_points"])

    # --- Bedrooms ------------------------------------------------------------
    brcfg = cfg.get("bedrooms", {})
    if listing.bedrooms is not None and brcfg:
        mx = brcfg["points_3_plus"]
        if listing.bedrooms >= 3:
            pts = float(mx)
        elif listing.bedrooms == 2:
            pts = float(brcfg["points_2"])
        else:
            pts = 0.0
        _add(pts, mx)

    # --- Bathrooms -----------------------------------------------------------
    bathcfg = cfg.get("bathrooms", {})
    if listing.bathrooms is not None and bathcfg:
        mx = bathcfg["points_gt1"]
        _add(float(mx) if listing.bathrooms > 1 else 0.0, mx)

    # --- Dishwasher ----------------------------------------------------------
    dw_max = cfg.get("dishwasher", 0)
    if listing.dishwasher is not None and dw_max:
        _add(float(dw_max) if listing.dishwasher else 0.0, dw_max)

    # --- Washer / dryer ------------------------------------------------------
    wd_max = cfg.get("washer_dryer", 0)
    if listing.washer_dryer is not None and wd_max:
        _add(float(wd_max) if listing.washer_dryer else 0.0, wd_max)

    # --- Rent stabilized -----------------------------------------------------
    rs_max = cfg.get("rent_stabilized", 0)
    if listing.rent_stabilized is not None and rs_max:
        _add(float(rs_max) if listing.rent_stabilized else 0.0, rs_max)

    if available == 0:
        return 0.0

    return round((earned / available) * 100, 1)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_floor_num(floor) -> Optional[int]:
    if not floor:
        return None
    s = str(floor).lower().strip()
    if "garden" in s or "ground" in s:
        return 1
    m = re.match(r"(\d+)", s)
    return int(m.group(1)) if m else None


def _score_subway(subway_lines: str, cfg: dict) -> float:
    """Parse '(J/M/Z) ~4 min | (M) ~7 min' and return subway points."""
    times = sorted(int(m.group(1)) for m in re.finditer(r"~(\d+)\s*min", subway_lines))
    if not times:
        return 0.0

    # High: any single line ≤ 3 min
    if times[0] <= 3:
        return float(cfg["points_high"])

    # High: two closest lines sum ≤ 11 min
    if len(times) >= 2 and times[0] + times[1] <= 11:
        return float(cfg["points_high"])

    # Medium: any single line ≤ 5 min
    if times[0] <= 5:
        return float(cfg["points_medium"])

    return 0.0


def _score_neighborhood(neighborhood: str, cfg: dict) -> float:
    hood = neighborhood.lower().replace("-", " ")
    for h in cfg.get("high", []):
        if h.lower().replace("-", " ") in hood:
            return float(cfg["high_points"])
    for h in cfg.get("medium", []):
        if h.lower().replace("-", " ") in hood:
            return float(cfg["medium_points"])
    return float(cfg.get("low_points", 0))
