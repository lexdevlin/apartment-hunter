"""
Shared data model for apartment listings across all scrapers.
"""

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Listing:
    url: str
    source: str                          # "streeteasy" | "craigslist" | "zillow" | "apartments_com"
    title: str
    price: Optional[int] = None          # monthly rent in USD
    neighborhood: Optional[str] = None
    address: Optional[str] = None
    floor: Optional[str] = None          # floor number or description, e.g. "3" or "Garden"
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    rent_stabilized: Optional[bool] = None  # None = unknown
    dishwasher: Optional[bool] = None       # None = unknown
    washer_dryer: Optional[bool] = None     # None = unknown; True = confirmed in-unit W/D
    date_listed: Optional[datetime] = None  # when the listing was created on the source site
    nearest_subway: Optional[str] = None    # e.g. "DeKalb Av (L) ~4 min; Myrtle-Wyckoff Avs (L/M) ~9 min"
    date_found: datetime = field(default_factory=datetime.utcnow)
    last_seen: Optional[str] = None   # "YYYY-MM-DD" of most recent scrape where listing appeared
    delisted: bool = False            # True only when URL returned a definitive "not available" signal
    priority_score: Optional[float] = None  # 0–100, computed each run; None until first scored
    is_priority: bool = False
    reviewed: bool = False
    image_url: Optional[str] = None   # og:image from the listing detail page

    @property
    def listing_id(self) -> str:
        """Stable 12-char ID derived from the canonical URL. Used for deduplication."""
        return hashlib.md5(self.url.strip().lower().encode()).hexdigest()[:12]

    @property
    def subway_lines(self) -> Optional[str]:
        """Compact subway summary — lines + walk time only, no station names.

        e.g. "Myrtle Av (J/M/Z) ~4 min; Central Av (M) ~7 min"
          →  "(J/M/Z) ~4 min | (M) ~7 min"
        """
        if not self.nearest_subway:
            return None
        parts = []
        for segment in self.nearest_subway.split(";"):
            m = re.search(r"(\([^)]+\)\s*~\d+\s*min)", segment)
            if m:
                parts.append(m.group(1).strip())
        return " | ".join(parts) if parts else None

    def to_dict(self) -> dict:
        def _fmt_date(dt):
            return dt.strftime("%Y-%m-%d") if dt else None

        def _fmt_datetime(dt):
            return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

        return {
            "listing_id": self.listing_id,
            "title": self.title,
            "source": self.source,
            "price": f"${self.price:,}" if self.price is not None else None,
            "neighborhood": self.neighborhood,
            "address": self.address,
            "floor": self.floor,
            "bedrooms": self.bedrooms,
            "bathrooms": self.bathrooms,
            "rent_stabilized": self.rent_stabilized,
            "dishwasher": self.dishwasher,
            "washer_dryer": self.washer_dryer,
            "date_listed": _fmt_date(self.date_listed),
            "subway_lines": self.subway_lines,
            "nearest_subway": self.nearest_subway,
            "date_found": _fmt_datetime(self.date_found),
            "last_seen": self.last_seen,
            "delisted": self.delisted or None,
            "priority_score": self.priority_score,
            "url": self.url,
            "is_priority": self.is_priority,
            "reviewed": self.reviewed,
            "image_url": self.image_url,
        }


# Column order for the Excel sheet — keeps things consistent across runs.
EXCEL_COLUMNS = [
    "url",
    "date_listed",
    "date_found",
    "delisted",
    "source",
    "priority_score",
    "is_priority",
    "reviewed",
    "last_seen",
    "address",
    "neighborhood",
    "price",
    "floor",
    "bedrooms",
    "bathrooms",
    "rent_stabilized",
    "dishwasher",
    "washer_dryer",
    "subway_lines",
    "nearest_subway",
    "title",
    "listing_id",
    "image_url",
]
