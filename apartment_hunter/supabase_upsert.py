"""
Supabase upsert for apartment listings.

Called at the end of each scraper run to sync the full merged dataset
into the Supabase `listings` table so the UI always has current data.

Required env vars:
  SUPABASE_URL  — your project URL, e.g. https://xyzxyz.supabase.co
  SUPABASE_KEY  — service role secret key (Settings → API → service_role)

The service role key bypasses Row-Level Security, which is what we want
for a server-side scraper. Never expose this key in the frontend.

Upsert rules (mirrors the CSV logic in main.py):
  - url is the conflict key (PRIMARY KEY)
  - user_status is never included in the payload — it is owned by the UI
  - All other columns are written on every upsert; the merge/preserve logic
    is already applied upstream in _upsert_listings() before this is called,
    so merged_rows is already the correctly reconciled dataset.
"""

import os
import re
from typing import Optional

from supabase import create_client, Client


def _get_client() -> Client:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip()
    if not url or not key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY must be set in the environment. "
            "Find them in your Supabase project under Settings → API."
        )
    return create_client(url, key)


# ---------------------------------------------------------------------------
# Type coercion: CSV rows are all strings; Supabase needs proper types
# ---------------------------------------------------------------------------

def _is_nan(v) -> bool:
    """True for float NaN — pandas uses these for missing cells in DataFrames."""
    try:
        return v is not None and float(v) != float(v)  # NaN != NaN
    except (TypeError, ValueError):
        return False


def _clean(v):
    """Normalise a value: turn NaN / 'nan' / 'None' / '' into None."""
    if v is None or _is_nan(v):
        return None
    s = str(v).strip()
    if s.lower() in ("nan", "none", ""):
        return None
    return s


def _bool(v) -> Optional[bool]:
    s = _clean(v)
    if s is None:
        return None
    if s.lower() in ("true", "1", "yes"):
        return True
    if s.lower() in ("false", "0", "no"):
        return False
    return None


def _int(v) -> Optional[int]:
    s = _clean(v)
    if s is None:
        return None
    # Strip currency formatting: "$2,800/mo" → 2800
    digits = re.sub(r"[^\d]", "", s.split("/")[0])
    return int(digits) if digits else None


def _float(v) -> Optional[float]:
    s = _clean(v)
    if s is None:
        return None
    try:
        result = float(s)
        # Guard against inf / -inf which are also non-JSON-compliant
        return result if result == result and abs(result) != float("inf") else None
    except (ValueError, TypeError):
        return None


def _str(v) -> Optional[str]:
    return _clean(v)


def _date(v) -> Optional[str]:
    """Pass through date/datetime strings already normalised by the CSV pipeline."""
    return _clean(v)


def _coerce(row: dict) -> dict:
    """Convert a CSV-style string dict into a typed dict for Supabase.

    user_status is intentionally omitted — it is owned by the UI and must
    never be overwritten by the scraper.
    """
    return {
        "url":             row.get("url", ""),
        "date_listed":     _date(row.get("date_listed")),
        "date_found":      _date(row.get("date_found")),
        "delisted":        _bool(row.get("delisted")) or False,
        "source":          _str(row.get("source")),
        "priority_score":  _float(row.get("priority_score")),
        "is_priority":     _bool(row.get("is_priority")),
        "reviewed":        _bool(row.get("reviewed")),
        "last_seen":       _date(row.get("last_seen")),
        "address":         _str(row.get("address")),
        "neighborhood":    _str(row.get("neighborhood")),
        "price":           _int(row.get("price")),
        "floor":           _str(row.get("floor")),
        "bedrooms":        _int(row.get("bedrooms")),
        "bathrooms":       _float(row.get("bathrooms")),
        "rent_stabilized": _bool(row.get("rent_stabilized")),
        "dishwasher":      _bool(row.get("dishwasher")),
        "washer_dryer":    _bool(row.get("washer_dryer")),
        "subway_lines":    _str(row.get("subway_lines")),
        "nearest_subway":  _str(row.get("nearest_subway")),
        "title":           _str(row.get("title")),
        "listing_id":      _str(row.get("listing_id")),
        "image_url":       _str(row.get("image_url")),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_BATCH_SIZE = 200  # Supabase recommends < 500 rows per request


def upsert_listings(merged_rows: list[dict]) -> int:
    """
    Upsert the full merged dataset into Supabase.

    Args:
        merged_rows: list of dicts as produced by _upsert_listings() in
                     main.py — all values are strings (CSV format).

    Returns:
        Total number of rows sent to Supabase.
    """
    client = _get_client()
    rows = [_coerce(r) for r in merged_rows if r.get("url")]

    total = 0
    for i in range(0, len(rows), _BATCH_SIZE):
        batch = rows[i : i + _BATCH_SIZE]
        (
            client.table("listings")
            .upsert(batch, on_conflict="url")
            .execute()
        )
        total += len(batch)

    return total
