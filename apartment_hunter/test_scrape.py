"""
Quick scraper test — no OneDrive or Azure credentials needed.

Runs all enabled scrapers and writes results to apartment_listings.csv
in the repo root so you can review the raw data.

Usage:
  python apartment_hunter/test_scrape.py

Optional flags:
  --source streeteasy         run only one source
  --limit 20                  cap results per source (useful for quick checks)
  --subway-only               skip scraping; reload existing CSV and re-run subway proximity
  --rent-stabilized-only      skip scraping; reload existing CSV and re-run DHCR crosscheck
  --enrich-only               skip scraping; re-fetch detail pages for existing StreetEasy rows
                              to fill blank fields and detect newly-delisted listings
"""

import argparse
import csv
import os
import random
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

from curl_cffi import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from apartment_hunter.scrapers import streeteasy, craigslist, zillow, apartments_com
from apartment_hunter.models import Listing, EXCEL_COLUMNS
from apartment_hunter import rent_stabilized, subway, scoring

CONFIG_PATH = Path(__file__).parent / "config.yaml"
OUTPUT_PATH = Path(__file__).parent.parent / "apartment_listings.csv"

SOURCE_MAP = {
    "streeteasy":     streeteasy.scrape,
    "craigslist":     craigslist.scrape,
    "zillow":         zillow.scrape,
    "apartments_com": apartments_com.scrape,
}


def _elapsed(t0: float) -> str:
    """Format seconds since t0 as e.g. '1m 23.4s' or '45.2s'."""
    secs = time.perf_counter() - t0
    if secs >= 60:
        m, s = divmod(secs, 60)
        return f"{int(m)}m {s:.1f}s"
    return f"{secs:.1f}s"


def _normalize_date(raw: str) -> str:
    """Normalise last_seen to 'YYYY-MM-DD HH:MM:SS' regardless of input format."""
    if not raw:
        return raw
    raw = raw.strip()
    # Old MM/DD/YYYY format
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", raw)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)} 00:00:00"
    # Date-only YYYY-MM-DD (no time component yet)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return f"{raw} 00:00:00"
    return raw


def _load_csv_rows() -> dict[str, dict]:
    """Load existing CSV rows keyed by URL. Returns {} if the file doesn't exist.

    - Normalises last_seen to YYYY-MM-DD (older rows may use MM/DD/YYYY).
    - Drops stale Craigslist pseudo-URLs (?ll=lat,lng search keys) — these were
      stored by an earlier version of the scraper and are not real post permalinks.
    """
    if not OUTPUT_PATH.exists():
        return {}
    dropped = 0
    rows: dict[str, dict] = {}
    with open(OUTPUT_PATH, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            url = row.get("url", "")
            if not url:
                continue
            if row.get("source") == "craigslist" and "?ll=" in url:
                dropped += 1
                continue
            if row.get("last_seen"):
                row["last_seen"] = _normalize_date(row["last_seen"])
            # Migrate old column name
            if "possibly_gone" in row and "delisted" not in row:
                row["delisted"] = row.pop("possibly_gone")
            rows[url] = row
    if dropped:
        print(f"  (dropped {dropped} stale Craigslist pseudo-URL row(s) from CSV)")
    return rows


# Fields never overwritten when updating an existing row
_NEVER_OVERWRITE = {"listing_id", "date_found", "reviewed", "delisted"}
# Fields always refreshed even if the existing row already has a value
_ALWAYS_UPDATE   = {"price", "last_seen", "priority_score"}


# ---------------------------------------------------------------------------
# Gone-detection: source-specific "listing no longer available" signals
# ---------------------------------------------------------------------------

_GONE_PATTERNS: dict[str, list[str]] = {
    # StreetEasy: unavailable listings return 200 with an "Unavailable" badge and either
    # "Delisted MM/DD/YYYY" or "Rented on MM/DD/YYYY" in the HTML.
    # Note: StreetEasy returns 403 on scraping sessions but 200 on direct permalink requests.
    "streeteasy": [
        "delisted",
        "rented on",
        "unavailable",
    ],
    # Craigslist: deleted posts return HTTP 410 (handled in _is_gone directly).
    # Text patterns cover the rare case where CL serves a 200 "deleted" page.
    "craigslist": [
        "this posting has been deleted by its author",
        "this posting has been flagged for removal",
        "this post has expired",
    ],
    "zillow": [
        "this home is no longer listed",
        "listing is no longer available",
    ],
    "apartments_com": [
        "listing is no longer available",
    ],
}

_GONE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _is_gone(url: str, source: str, verbose: bool = False) -> bool:
    """
    Return True only when we have a definitive signal that the listing is gone.
    Return False on any ambiguous result (403, timeout, network error).

    Status codes treated as definitive:
      404 — standard "not found"
      410 — "Gone" (used by Craigslist for deleted posts)
    """
    # Craigslist pseudo-URLs (/apa/?ll=...) are lat/lng search keys, not post
    # permalinks — they always 404 and tell us nothing about the listing.
    if source == "craigslist" and "?ll=" in url:
        if verbose:
            print("      → skipped (Craigslist pseudo-URL, not a real post permalink)")
        return False

    try:
        resp = requests.get(url, headers=_GONE_HEADERS, timeout=10, allow_redirects=True,
                            impersonate="chrome136")
    except requests.RequestsError as e:
        if verbose:
            print(f"      network error: {e}")
        return False

    if verbose:
        redirected = resp.url != url
        print(f"      status={resp.status_code}"
              + (f"  final_url={resp.url[:100]}" if redirected else ""))

    if resp.status_code in (404, 410):
        if verbose:
            print(f"      → GONE ({resp.status_code})")
        return True

    if resp.status_code != 200:
        if verbose:
            print(f"      → ambiguous ({resp.status_code}) — not flagging")
        return False

    body = resp.text.lower()
    patterns = _GONE_PATTERNS.get(source, [])
    matched = [p for p in patterns if p in body]
    if verbose:
        if matched:
            print(f"      → GONE (matched: {matched})")
        else:
            print("      → still live (no gone patterns matched)")
    return bool(matched)


def _check_gone_listings(unseen_rows: list[dict], verbose: bool = False) -> int:
    """
    For rows not seen in the current scrape, check each URL directly.
    Mutates rows in-place: sets delisted='True' on confirmed-gone listings.
    Returns count of newly confirmed-gone listings.
    """
    if not unseen_rows:
        return 0

    # Per-source summary
    by_source: dict[str, list[dict]] = {}
    for row in unseen_rows:
        by_source.setdefault(row.get("source", "unknown"), []).append(row)
    for src, src_rows in sorted(by_source.items()):
        already = sum(1 for r in src_rows if r.get("delisted", "").lower() == "true")
        checkable = len(src_rows) - already
        print(f"  [{src}] {len(src_rows)} unseen "
              + (f"({already} already delisted, " if already else "(")
              + f"{checkable} to check)")

    newly_gone = 0
    for row in unseen_rows:
        url    = row.get("url", "")
        source = row.get("source", "")
        if not url or row.get("delisted", "").lower() == "true":
            continue

        if verbose:
            print(f"  [{source}] last_seen={row.get('last_seen', '?')}  {url[:80]}")
        gone = _is_gone(url, source, verbose=verbose)
        if gone:
            row["delisted"] = "True"
            newly_gone += 1
            if not verbose:
                print(f"    [delisted] {url[:80]}")
        time.sleep(random.uniform(1.0, 2.5))

    return newly_gone


def _content_key(source: str, title: str, price: str, neighborhood: str) -> tuple:
    """
    Dedup key for detecting reposts: same listing, new CL post ID.
    Normalises title (lowercase, strip punctuation/whitespace) so minor
    formatting differences don't prevent matching.
    """
    norm = re.sub(r"[^a-z0-9 ]", " ", title.lower())
    norm = re.sub(r"\s+", " ", norm).strip()
    return (source, norm, (price or "").strip(), (neighborhood or "").strip())


def _upsert_listings(existing: dict[str, dict], new_listings: list, columns: list) -> list[dict]:
    """
    Merge freshly scraped listings into the existing CSV rows.

    Rules per field on an existing row:
      - NEVER_OVERWRITE fields  → always keep the stored value
      - ALWAYS_UPDATE fields    → always take the new value (if non-blank)
      - rent_stabilized         → never downgrade True → blank/False
      - everything else         → fill in only when the stored value is blank

    New URLs are appended after all existing rows.
    Rows not seen in this scrape are kept unchanged.

    Repost dedup: if a new listing matches an existing row by
    (source, normalised title, price, neighborhood) but has a different URL,
    it is treated as a repost — the existing row is updated in-place and the
    new URL is discarded, so no duplicate row is added.
    """
    merged: dict[str, dict] = dict(existing)  # preserves insertion order

    # Build a content-key → canonical url index from existing rows
    content_index: dict[tuple, str] = {}
    for url, row in existing.items():
        ck = _content_key(
            row.get("source", ""),
            row.get("title", ""),
            row.get("price", ""),
            row.get("neighborhood", ""),
        )
        content_index.setdefault(ck, url)  # first (oldest) row wins

    n_reposts = 0
    for listing in new_listings:
        url     = listing.url
        new_row = listing.to_dict()

        # Check for repost: same content, different URL
        ck = _content_key(listing.source, listing.title,
                          new_row.get("price", ""), listing.neighborhood or "")
        if url not in merged and ck in content_index:
            canonical_url = content_index[ck]
            url = canonical_url   # redirect to the existing row
            n_reposts += 1

        if url not in merged:
            merged[url] = new_row
            content_index.setdefault(ck, url)
            continue

        old_row = merged[url]
        # Preserve a delisted flag set by enrichment (off-market detail page);
        # only clear it when the freshly-scraped listing is itself still live.
        if str(new_row.get("delisted") or "").lower() != "true":
            old_row["delisted"] = ""
        for col in columns:
            if col in _NEVER_OVERWRITE:
                continue
            new_val = str(new_row.get(col, "") or "").strip()
            old_val = str(old_row.get(col, "") or "").strip()

            if col in _ALWAYS_UPDATE:
                if new_val:
                    old_row[col] = new_row[col]
            elif col in ("rent_stabilized", "is_priority"):
                # Never downgrade: keep True if already True (preserves manual overrides)
                if old_val.lower() != "true" and new_val:
                    old_row[col] = new_row[col]
            else:
                # Fill-blank: only write if existing cell is empty
                if not old_val and new_val:
                    old_row[col] = new_row[col]

    if n_reposts:
        print(f"  {n_reposts} repost(s) detected and merged into existing rows")

    return list(merged.values())


def _load_listings_from_csv() -> list[Listing]:
    """Read apartment_listings.csv and rebuild minimal Listing objects."""
    if not OUTPUT_PATH.exists():
        print(f"No CSV found at {OUTPUT_PATH} — run a full scrape first.")
        return []
    listings = []
    with open(OUTPUT_PATH, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            addr = (row.get("address") or "").strip()
            rs_raw = (row.get("rent_stabilized") or "").strip().lower()
            listings.append(Listing(
                url=row.get("url", ""),
                source=row.get("source", ""),
                title=row.get("title", ""),
                address=addr or None,
                neighborhood=(row.get("neighborhood") or "").strip() or None,
                rent_stabilized=True if rs_raw == "true" else None,
                nearest_subway=None,  # force re-enrichment for --subway-only
            ))
    return listings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=SOURCE_MAP.keys(), help="Run only one source")
    parser.add_argument("--limit", type=int, default=0, help="Max listings per source (0 = no limit)")
    parser.add_argument("--subway-only", action="store_true",
                        help="Skip scraping; reload existing CSV and re-run subway proximity")
    parser.add_argument("--rent-stabilized-only", action="store_true",
                        help="Skip scraping; reload existing CSV and re-run DHCR crosscheck")
    parser.add_argument("--check-gone-only", action="store_true",
                        help="Skip scraping; check every existing CSV row for availability (verbose, no writes)")
    parser.add_argument("--enrich-only", action="store_true",
                        help="Skip scraping; re-fetch StreetEasy detail pages to fill blank fields "
                             "and detect newly-delisted listings (writes results)")
    args = parser.parse_args()

    if args.check_gone_only:
        rows = _load_csv_rows()
        if not rows:
            print(f"No CSV found at {OUTPUT_PATH}")
            return
        all_rows = list(rows.values())
        limit = args.limit or len(all_rows)
        to_check = [r for r in all_rows if r.get("delisted", "").lower() != "true"][:limit]
        print(f"Loaded {len(all_rows)} row(s) from {OUTPUT_PATH}")
        print(f"Checking {len(to_check)} row(s) (skipping already delisted)"
              + (f"  [limit={limit}]" if args.limit else ""))
        print("No changes will be written.\n")
        gone_count = 0
        for row in to_check:
            url    = row.get("url", "")
            source = row.get("source", "")
            print(f"[{source}] {url[:90]}")
            gone = _is_gone(url, source, verbose=True)
            if gone:
                gone_count += 1
            time.sleep(random.uniform(1.0, 2.5))
        print(f"\n{'='*50}")
        print(f"Would flag {gone_count} of {len(to_check)} checked listing(s) as delisted.")
        return

    if args.rent_stabilized_only:
        listings = _load_listings_from_csv()
        if not listings:
            return
        print(f"Loaded {len(listings)} listings from {OUTPUT_PATH}")
        n_pre = sum(1 for l in listings if l.rent_stabilized)
        print(f"  {n_pre} already flagged as rent-stabilized in CSV")
        print(f"\n{'='*50}")
        print("Crosschecking rent stabilization (DHCR)")
        print(f"{'='*50}")
        rent_stabilized.crosscheck(listings)

        # Patch rent_stabilized back into the CSV for newly flagged listings
        rs_by_url = {l.url: l.rent_stabilized for l in listings if l.rent_stabilized}
        rows = []
        fieldnames = []
        with open(OUTPUT_PATH, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for row in reader:
                if row.get("url", "") in rs_by_url:
                    row["rent_stabilized"] = "True"
                rows.append(row)
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        n_post = sum(1 for l in listings if l.rent_stabilized)
        print(f"\n{n_post - n_pre} newly flagged listing(s) written to {OUTPUT_PATH}")
        return

    if args.subway_only:
        listings = _load_listings_from_csv()
        if not listings:
            return
        print(f"Loaded {len(listings)} listings from {OUTPUT_PATH}")
        print(f"\n{'='*50}")
        print("Subway proximity")
        print(f"{'='*50}")
        subway.enrich(listings, verbose=True)

        # Patch nearest_subway back into the CSV without disturbing any other columns
        subway_by_url = {l.url: l.nearest_subway for l in listings if l.nearest_subway}
        rows = []
        fieldnames = []
        with open(OUTPUT_PATH, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for row in reader:
                url = row.get("url", "")
                if url in subway_by_url:
                    row["nearest_subway"] = subway_by_url[url]
                rows.append(row)
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        n_written = sum(1 for l in listings if l.nearest_subway)
        print(f"\nUpdated {n_written} nearest_subway value(s) in {OUTPUT_PATH}")
        print("\nResults:")
        for l in listings:
            addr = l.address or "(no address)"
            sub  = l.nearest_subway or "(not enriched)"
            print(f"  {addr[:45]:<45}  {sub}")
        return

    if args.enrich_only:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        import pandas as pd
        from apartment_hunter import onedrive as _onedrive

        # Sources that have a dedicated detail-page enrichment step.
        # Each entry: (headers_dict, enrich_fn, restore_fn, detail_delay, session_rotate_every)
        _ENRICH_SOURCES = {
            "streeteasy": (
                streeteasy.HEADERS,
                streeteasy._enrich_listing,
                streeteasy._restore_from_row,
                streeteasy.DETAIL_DELAY,
                10,
            ),
            "craigslist": (
                craigslist.DETAIL_HEADERS,
                craigslist._enrich_listing,
                craigslist._restore_from_row_cl,
                craigslist.DETAIL_DELAY,
                20,
            ),
        }

        sources_to_run = (
            [args.source] if args.source else list(_ENRICH_SOURCES.keys())
        )
        unknown = [s for s in sources_to_run if s not in _ENRICH_SOURCES]
        if unknown:
            print(f"--enrich-only not supported for: {', '.join(unknown)}")
            print(f"  Supported: {', '.join(_ENRICH_SOURCES)}")
            return

        print(f"\n{'='*50}")
        print("OneDrive download")
        print(f"{'='*50}")
        df = _onedrive.download_listings(config)
        if df.empty:
            print("No listings found on OneDrive.")
            return

        rows = {r["url"]: r for r in df.to_dict("records") if r.get("url")}
        print(f"Loaded {len(rows)} row(s) from OneDrive")

        n_delisted_total = 0
        n_updated_total  = 0
        n_checked_total  = 0

        for source_name in sources_to_run:
            headers, enrich_fn, restore_fn, detail_delay, rotate_every = \
                _ENRICH_SOURCES[source_name]

            to_enrich = [
                row for row in rows.values()
                if row.get("source") == source_name
                and str(row.get("delisted") or "").lower() != "true"
            ]
            if args.limit:
                to_enrich = to_enrich[: args.limit]

            print(f"\n{'='*50}")
            print(f"Re-enriching: {source_name}  ({len(to_enrich)} listing(s)"
                  + (f"  limit={args.limit}" if args.limit else "") + ")")
            print(f"{'='*50}")

            session = requests.Session(impersonate="chrome136")
            session.headers.update(headers)

            n_delisted = 0
            n_updated  = 0

            for i, row in enumerate(to_enrich):
                if i > 0 and i % rotate_every == 0:
                    session = requests.Session(impersonate="chrome136")
                    session.headers.update(headers)
                    time.sleep(detail_delay + random.uniform(2, 4))

                url     = row.get("url", "")
                listing = Listing(url=url, source=source_name, title=row.get("title", ""))
                restore_fn(listing, row)
                enriched = enrich_fn(session, listing)

                changed = False

                if enriched.delisted:
                    row["delisted"] = "True"
                    n_delisted += 1
                    changed = True
                    print(f"  [delisted] {url[:80]}")

                def _patch(field: str, val) -> None:
                    nonlocal changed
                    if val is None:
                        return
                    existing = str(row.get(field) or "").strip()
                    if existing.lower() == "true":
                        return
                    if existing:
                        return
                    row[field] = val if isinstance(val, str) else str(val)
                    changed = True

                _patch("address",         enriched.address)
                _patch("floor",           enriched.floor)
                _patch("bedrooms",        enriched.bedrooms)
                _patch("bathrooms",       enriched.bathrooms)
                _patch("date_listed",     enriched.date_listed.strftime("%Y-%m-%d") if enriched.date_listed else None)
                _patch("dishwasher",      "True" if enriched.dishwasher else None)
                _patch("washer_dryer",    "True" if enriched.washer_dryer else None)
                _patch("rent_stabilized", "True" if enriched.rent_stabilized else None)
                _patch("image_url",       enriched.image_url)
                _patch("title",           enriched.title)

                if changed and not enriched.delisted:
                    n_updated += 1

                time.sleep(detail_delay + random.uniform(0, 1.5))

            print(f"  {n_delisted} newly delisted, {n_updated} field(s) updated")
            n_delisted_total += n_delisted
            n_updated_total  += n_updated
            n_checked_total  += len(to_enrich)

        print(f"\n{'='*50}")
        print("OneDrive upload")
        print(f"{'='*50}")
        updated_df = pd.DataFrame(list(rows.values()), columns=EXCEL_COLUMNS)
        _onedrive.upload_listings(config, updated_df)

        print(f"\nTotal: {n_delisted_total} newly delisted, {n_updated_total} field(s) updated  "
              f"({n_checked_total} listing(s) checked across {len(sources_to_run)} source(s))")
        return

    t_start = time.perf_counter()

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Load existing CSV up front so we can report new vs re-seen counts per source
    existing_rows = _load_csv_rows()
    if existing_rows:
        print(f"\n{len(existing_rows)} existing listing(s) loaded from {OUTPUT_PATH}")

    sources_cfg = config.get("sources", {})
    all_listings = []

    sources_to_run = {args.source: SOURCE_MAP[args.source]} if args.source else SOURCE_MAP

    for name, scrape_fn in sources_to_run.items():
        if not args.source and sources_cfg.get(name, True) is False:
            print(f"\n[{name}] disabled in config — skipping")
            continue

        print(f"\n{'='*50}")
        print(f"Scraping: {name}")
        print(f"{'='*50}")

        t_source = time.perf_counter()
        try:
            listings = scrape_fn(config, existing_rows=existing_rows)
        except Exception as e:
            print(f"ERROR: {e}")
            listings = []

        if args.limit and listings:
            listings = listings[: args.limit]

        n_new_here    = sum(1 for l in listings if l.url not in existing_rows)
        n_reseen_here = len(listings) - n_new_here
        print(f"→ {len(listings)} listings from {name}  "
              f"({n_new_here} new, {n_reseen_here} previously seen)  [{_elapsed(t_source)}]")
        for l in listings:
            tag = "NEW" if l.url not in existing_rows else "   "
            print(f"   {tag} {l.source:<15} ${l.price or '?':>6}  "
                  f"{(l.neighborhood or '?'):<28} "
                  f"{(l.address or '?')[:40]}")

        all_listings.extend(listings)
        time.sleep(1)

    n_new_total    = sum(1 for l in all_listings if l.url not in existing_rows)
    n_reseen_total = len(all_listings) - n_new_total
    print(f"\n{'='*50}")
    print(f"Total: {len(all_listings)} listings across all sources  "
          f"({n_new_total} new, {n_reseen_total} previously seen / will re-enrich blank fields)")

    if not all_listings:
        print("No listings returned — nothing to write.")
        return

    # Exclude confirmed-delisted listings from all enrichment and scoring
    active_listings = [l for l in all_listings if not l.delisted]
    n_delisted_this_run = len(all_listings) - len(active_listings)
    if n_delisted_this_run:
        print(f"  ({n_delisted_this_run} listing(s) flagged delisted during enrichment — skipping further processing)")

    # Crosscheck against DHCR rent stabilized database
    print(f"\n{'='*50}")
    print("Crosschecking rent stabilization (DHCR)")
    print(f"{'='*50}")
    t_rs = time.perf_counter()
    rent_stabilized.crosscheck(active_listings)
    print(f"  [{_elapsed(t_rs)}]")

    # Subway proximity
    print(f"\n{'='*50}")
    print("Subway proximity")
    print(f"{'='*50}")
    t_sub = time.perf_counter()
    subway.enrich(active_listings)
    print(f"  [{_elapsed(t_sub)}]")

    # Priority scoring
    scoring_cfg = config.get("priority_scoring", {})
    threshold   = scoring_cfg.get("threshold", 65)
    print(f"\n{'='*50}")
    print(f"Priority scoring  (threshold ≥ {threshold})")
    print(f"{'='*50}")
    t_score = time.perf_counter()
    n_priority = 0
    for listing in active_listings:
        if scoring.is_priority_override(listing, config):
            listing.priority_score = 100.0
            listing.is_priority    = True
        else:
            listing.priority_score = scoring.compute_score(listing, scoring_cfg)
            listing.is_priority    = listing.priority_score >= threshold
        if listing.is_priority:
            n_priority += 1
            print(f"  ★ {listing.priority_score:5.1f}  {(listing.address or listing.title)[:55]}")
    print(f"  {n_priority} priority listing(s) found  [{_elapsed(t_score)}]")

    # Stamp last_seen = now on every listing we found this run
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for listing in all_listings:
        listing.last_seen = now

    # Upsert into existing CSV: update existing rows, append new ones, keep unseen rows
    csv_columns  = EXCEL_COLUMNS
    scraped_urls = {l.url for l in all_listings}
    merged_rows   = _upsert_listings(existing_rows, all_listings, csv_columns)

    # Check whether listings not seen this run are actually gone
    unseen = [r for r in merged_rows if r.get("url") not in scraped_urls]
    print(f"\n{'='*50}")
    print("Availability check for previously-seen listings")
    print(f"{'='*50}")
    t_gone = time.perf_counter()
    n_gone = _check_gone_listings(unseen)
    print(f"  [{_elapsed(t_gone)}]")

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=csv_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged_rows)

    print(f"\nWritten to: {OUTPUT_PATH}")
    print(f"  {n_new_total} new, {n_reseen_total} re-enriched, {n_gone} newly delisted  "
          f"({len(merged_rows)} total rows)")

    # Sync to Supabase if credentials are available
    print(f"\n{'='*50}")
    print("Supabase sync")
    print(f"{'='*50}")
    if os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"):
        try:
            from apartment_hunter import supabase_upsert
            t_sb = time.perf_counter()
            n_synced = supabase_upsert.upsert_listings(merged_rows)
            print(f"  {n_synced} row(s) synced  [{_elapsed(t_sb)}]")
        except Exception as e:
            print(f"  [Supabase] sync failed: {e}")
    else:
        print("  SUPABASE_URL / SUPABASE_KEY not set — skipping.")
        print("  Add them to .env to enable (see apartment_hunter/supabase_schema.sql).")

    # Upload to OneDrive if credentials are available
    print(f"\n{'='*50}")
    print("OneDrive upload")
    print(f"{'='*50}")
    if os.environ.get("APARTMENT_ONEDRIVE_REFRESH_TOKEN"):
        try:
            import pandas as pd
            from apartment_hunter import onedrive
            t_od = time.perf_counter()
            df = pd.DataFrame(merged_rows, columns=csv_columns)
            onedrive.upload_listings(config, df)
            print(f"  [{_elapsed(t_od)}]")
        except Exception as e:
            print(f"  [OneDrive] upload failed: {e}")
    else:
        print("  APARTMENT_ONEDRIVE_REFRESH_TOKEN not set — skipping.")
        print("  Run setup_onedrive_auth.py once and paste the token into .env to enable.")

    print(f"\n  Total runtime: {_elapsed(t_start)}")
    print("Open in Excel or any spreadsheet app to review.")


if __name__ == "__main__":
    main()
