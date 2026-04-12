"""
Apartment Hunter — main orchestrator.

Usage:
  python -m apartment_hunter.main

  Or from the repo root:
  python apartment_hunter/main.py

Steps per run:
  1. Load config.yaml
  2. Download existing listings Excel from OneDrive
  3. Scrape all enabled sources
  4. Identify truly new listings (not already in the spreadsheet)
  5. Flag any that match priority addresses
  6. Upload merged Excel back to OneDrive
  7. Regenerate HTML dashboard and upload to Azure Blob
"""

import sys
import time
from pathlib import Path

import yaml

# Allow running as a script from the repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from apartment_hunter import onedrive, dashboard, rent_stabilized, subway
from apartment_hunter.scrapers import streeteasy, craigslist, zillow, apartments_com

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def scrape_all(config: dict) -> list:
    source_map = {
        "streeteasy":    streeteasy.scrape,
        "craigslist":    craigslist.scrape,
        "zillow":        zillow.scrape,
        "apartments_com": apartments_com.scrape,
    }

    sources_cfg = config.get("sources", {})
    all_listings = []

    for name, scrape_fn in source_map.items():
        if sources_cfg.get(name, True) is False:
            print(f"\n[{name}] disabled in config — skipping")
            continue

        print(f"\n[{name}] scraping...")
        try:
            listings = scrape_fn(config)
            all_listings.extend(listings)
        except Exception as e:
            print(f"[{name}] ERROR: {e}")

        time.sleep(2)  # brief pause between sources

    return all_listings


def flag_priority(listings: list, priority_addresses: list[str]) -> None:
    """Mutates listings in-place, setting is_priority=True for address matches."""
    if not priority_addresses:
        return
    for listing in listings:
        addr = (listing.address or "").lower()
        if any(p.lower() in addr for p in priority_addresses if p):
            listing.is_priority = True


def main() -> None:
    print("=" * 60)
    print("Apartment Hunter")
    print("=" * 60)

    config = load_config()
    print(f"\nConfig: {config['search']['min_bedrooms']}+ BR, "
          f"max ${config['search']['max_price']:,}, "
          f"{len(config['search']['neighborhoods'])} neighborhoods")

    # 1. Download existing data
    print("\n--- Loading existing listings from OneDrive ---")
    existing_df = onedrive.download_listings(config)
    existing_ids = set(existing_df["listing_id"].tolist()) if not existing_df.empty else set()
    print(f"  {len(existing_ids)} previously seen listings (used for dedup)")

    # 2. Scrape
    print("\n--- Scraping sources ---")
    all_listings = scrape_all(config)
    print(f"\nTotal scraped across all sources: {len(all_listings)}")

    # 3. Deduplicate
    new_listings = [l for l in all_listings if l.listing_id not in existing_ids]
    print(f"New listings (not previously seen): {len(new_listings)}")

    # 4. Crosscheck against DHCR rent stabilized building database
    print("\n--- Crosschecking rent stabilization (DHCR) ---")
    rent_stabilized.crosscheck(new_listings)

    # 5. Subway proximity
    print("\n--- Subway proximity ---")
    subway.enrich(new_listings)

    # 7. Flag priority addresses
    priority_addresses = config.get("priority_addresses") or []
    flag_priority(new_listings, priority_addresses)
    n_priority = sum(1 for l in new_listings if l.is_priority)
    if n_priority:
        print(f"  ★ {n_priority} match a priority address!")

    # 8. Merge and upload to OneDrive
    print("\n--- Updating OneDrive Excel ---")
    if new_listings:
        merged_df = onedrive.merge_listings(existing_df, new_listings)
        onedrive.upload_listings(config, merged_df)
    else:
        merged_df = existing_df
        print("  No new listings — OneDrive file unchanged")

    # 9. Regenerate dashboard
    print("\n--- Generating dashboard ---")
    dashboard.generate(merged_df, config)
    dashboard.upload_to_azure(config)

    print("\n" + "=" * 60)
    print(f"Done. {len(new_listings)} new listings added.")
    if n_priority:
        print(f"★ {n_priority} PRIORITY listings found — check your dashboard!")
    print("=" * 60)


if __name__ == "__main__":
    main()
