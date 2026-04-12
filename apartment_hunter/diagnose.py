"""
Raw response diagnostic — shows exactly what each site sends back to Python.
Run this BEFORE touching scraper logic so we know what we're actually dealing with.

Usage:
  python apartment_hunter/diagnose.py
"""

import json
import re
import sys
from pathlib import Path

from curl_cffi import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

SEP = "=" * 70


def diagnose_streeteasy():
    print(f"\n{SEP}")
    print("STREETEASY")
    print(SEP)
    url = "https://streeteasy.com/for-rent/williamsburg/price:-3400|beds:2"
    print(f"URL: {url}\n")

    resp = requests.get(url, headers=HEADERS, timeout=15, impersonate="chrome136")
    print(f"Status:        {resp.status_code}")
    print(f"Content-Type:  {resp.headers.get('Content-Type', '?')}")
    print(f"Final URL:     {resp.url}")
    print(f"Body length:   {len(resp.text)} chars")

    soup = BeautifulSoup(resp.text, "lxml")

    next_data = soup.find("script", {"id": "__NEXT_DATA__"})
    print(f"\n__NEXT_DATA__ present: {next_data is not None}")

    if next_data:
        raw = next_data.string or ""
        print(f"__NEXT_DATA__ length: {len(raw)} chars")
        # Print top-level keys
        try:
            data = json.loads(raw)
            print(f"Top-level keys: {list(data.keys())}")
            props = data.get("props", {})
            page_props = props.get("pageProps", {})
            print(f"pageProps keys: {list(page_props.keys())[:15]}")
        except Exception as e:
            print(f"JSON parse error: {e}")
            print(f"First 500 chars: {raw[:500]}")
    else:
        title = soup.find("title")
        h1 = soup.find("h1")
        print(f"\nPage <title>: {title.get_text()[:120] if title else '(none)'}")
        print(f"Page <h1>:    {h1.get_text()[:120] if h1 else '(none)'}")

        # Search for any inline script tags containing JSON listing data
        print("\nInline scripts containing 'price' or 'listing':")
        for s in soup.find_all("script"):
            content = s.string or ""
            if ("price" in content.lower() or "listing" in content.lower()) and len(content) > 100:
                print(f"  id={s.get('id','')!r}  len={len(content)}  snippet: {content[:120].replace(chr(10),' ')!r}")

        # Look for listing card elements by common patterns
        print("\nElement probe (looking for listing containers):")
        probes = [
            ("article",                    lambda: soup.find_all("article")),
            ("[data-testid]",              lambda: soup.find_all(attrs={"data-testid": True})),
            ("[data-listing-id]",          lambda: soup.find_all(attrs={"data-listing-id": True})),
            ("[data-listingid]",           lambda: soup.find_all(attrs={"data-listingid": True})),
            ("a[href*='/rental/']",        lambda: soup.find_all("a", href=re.compile(r"/rental/"))),
            ("a[href*='/for-rent/']",      lambda: soup.find_all("a", href=re.compile(r"/for-rent/\d"))),
            ("div[class*='listing']",      lambda: soup.find_all("div", class_=re.compile(r"listing", re.I))),
            ("div[class*='UnitCard']",     lambda: soup.find_all("div", class_=re.compile(r"UnitCard|unit-card", re.I))),
            ("div[class*='SearchResult']", lambda: soup.find_all("div", class_=re.compile(r"SearchResult|search-result", re.I))),
            ("li[class*='result']",        lambda: soup.find_all("li", class_=re.compile(r"result", re.I))),
        ]
        for label, finder in probes:
            results = finder()
            if results:
                sample = results[0]
                cls = sample.get("class", [])
                did = sample.get("data-testid", sample.get("data-listing-id", ""))
                href = sample.get("href", "")[:60] if sample.name == "a" else ""
                print(f"  {label:<40} -> {len(results):>3} found  class={cls[:2]}  testid={did!r}  href={href!r}")
            else:
                print(f"  {label:<40} ->   0 found")

        # Print a 3000-char window from mid-body where listing HTML likely lives
        body = resp.text
        mid = len(body) // 4  # listings usually in first quarter of body
        print(f"\nHTML slice [{mid}:{mid+3000}]:\n{body[mid:mid+3000]}")


def diagnose_craigslist():
    print(f"\n{SEP}")
    print("CRAIGSLIST")
    print(SEP)
    url = "https://newyork.craigslist.org/search/apa"
    params = {"min_bedrooms": "2", "max_bedrooms": "2", "max_price": "3400"}
    print(f"URL: {url}")
    print(f"Params: {params}\n")

    resp = requests.get(url, params=params, headers=HEADERS, timeout=15, impersonate="chrome136")
    print(f"Status:        {resp.status_code}")
    print(f"Content-Type:  {resp.headers.get('Content-Type', '?')}")
    print(f"Final URL:     {resp.url}")
    print(f"Body length:   {len(resp.text)} chars")

    soup = BeautifulSoup(resp.text, "lxml")

    # Try every likely container selector
    selectors = [
        ("li[class*='cl-search-result']",  lambda: soup.find_all("li", class_=re.compile(r"cl-search-result"))),
        ("li.result-row",                  lambda: soup.find_all("li", class_="result-row")),
        ("a[class*='posting-title']",      lambda: soup.find_all("a", class_=re.compile(r"posting-title"))),
        ("span.priceinfo",                 lambda: soup.find_all("span", class_="priceinfo")),
        ("span.result-price",              lambda: soup.find_all("span", class_="result-price")),
        ("div.gallery-card",               lambda: soup.find_all("div", class_="gallery-card")),
        ("ol.rows",                        lambda: soup.find_all("ol", id="search-results")),
    ]

    print("\nSelector probe:")
    for label, finder in selectors:
        try:
            results = finder()
            print(f"  {label:<45} -> {len(results)} found")
        except Exception as e:
            print(f"  {label:<45} -> ERROR: {e}")

    title = soup.find("title")
    print(f"\nPage <title>: {title.get_text()[:120] if title else '(none)'}")
    # Parse and print the JSON-LD listing data
    ld_tag = soup.find("script", {"id": "ld_searchpage_results"})
    if ld_tag:
        print("\n--- ld_searchpage_results JSON-LD (first 3000 chars) ---")
        print(ld_tag.string[:3000])
    else:
        print("\nNo ld_searchpage_results tag found.")
        print(f"\nFirst 2000 chars of body:\n{resp.text[:2000]}")


def diagnose_zillow():
    print(f"\n{SEP}")
    print("ZILLOW")
    print(SEP)
    url = "https://www.zillow.com/async-create-search-page-state"
    payload = {
        "searchQueryState": {
            "pagination": {},
            "isMapVisible": False,
            "isListVisible": True,
            "mapBounds": {
                "west": -74.042, "east": -73.833,
                "south": 40.570, "north": 40.740,
            },
            "filterState": {
                "isForRent":            {"value": True},
                "isForSaleForeclosure": {"value": False},
                "isMultiFamily":        {"value": False},
                "isAuction":            {"value": False},
                "isNewConstruction":    {"value": False},
                "price":                {"max": 3400},
                "monthlyPayment":       {"max": 3400},
                "beds":                 {"min": 2, "max": 2},
            },
        },
        "wants": {"cat1": ["listResults", "mapResults"], "cat2": ["total"]},
        "requestId": 1,
    }
    headers = {**HEADERS, "Content-Type": "application/json"}
    print(f"URL: {url}  (PUT)\n")

    resp = requests.put(url, json=payload, headers=headers, timeout=15)
    print(f"Status:        {resp.status_code}")
    print(f"Content-Type:  {resp.headers.get('Content-Type', '?')}")
    print(f"Body length:   {len(resp.text)} chars")

    if resp.status_code == 200:
        content_type = resp.headers.get("Content-Type", "")
        if "json" in content_type:
            try:
                data = resp.json()
                print(f"Top-level keys: {list(data.keys())}")
                cat1 = data.get("cat1", {})
                print(f"cat1 keys: {list(cat1.keys()) if isinstance(cat1, dict) else type(cat1)}")
                search_results = cat1.get("searchResults", {}) if isinstance(cat1, dict) else {}
                print(f"searchResults keys: {list(search_results.keys()) if isinstance(search_results, dict) else type(search_results)}")
                list_results = search_results.get("listResults", []) if isinstance(search_results, dict) else []
                print(f"listResults count: {len(list_results)}")
                if list_results:
                    print(f"First result keys: {list(list_results[0].keys())[:15]}")
                    print(f"First result sample:\n{json.dumps(list_results[0], indent=2)[:800]}")
            except Exception as e:
                print(f"JSON parse error: {e}")
                print(f"First 500 chars: {resp.text[:500]}")

            # Retry without beds filter to isolate the issue
            print("\n--- Retrying WITHOUT beds filter ---")
            payload2 = json.loads(json.dumps(payload))
            del payload2["searchQueryState"]["filterState"]["beds"]
            try:
                resp2 = requests.put(url, json=payload2, headers=headers, timeout=15)
                data2 = resp2.json()
                lr2 = data2.get("cat1", {}).get("searchResults", {}).get("listResults", [])
                print(f"Without beds filter: {len(lr2)} results")
                if lr2:
                    print(f"First result keys: {list(lr2[0].keys())[:10]}")
            except Exception as e:
                print(f"Error on retry: {e}")
        else:
            print("Got HTML (likely bot detection or redirect)")
            print(f"First 1000 chars:\n{resp.text[:1000]}")
    else:
        print(f"First 500 chars of error body:\n{resp.text[:500]}")


def diagnose_streeteasy_detail(url: str):
    """
    Fetch a single StreetEasy listing detail page and dump every line that
    contains date/time/market-related keywords so we can find the exact
    phrasing used for 'days on market'.

    Pass any listing URL from your CSV, e.g.:
      https://streeteasy.com/building/304-evergreen-avenue-brooklyn/3r
    """
    print(f"\n{SEP}")
    print("STREETEASY DETAIL PAGE DIAGNOSTIC")
    print(SEP)
    print(f"URL: {url}\n")

    headers = {**HEADERS, "Referer": "https://streeteasy.com/"}
    resp = requests.get(url, headers=headers, timeout=15, impersonate="chrome136")
    print(f"Status:       {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', '?')}")
    print(f"Body length:  {len(resp.text)} chars\n")

    if resp.status_code != 200:
        print(f"First 500 chars:\n{resp.text[:500]}")
        return

    soup = BeautifulSoup(resp.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # --- Print every line containing relevant keywords ---
    keywords = re.compile(
        r"day|market|listed|dom|availab|since|ago|date|time|posted|new",
        re.IGNORECASE,
    )
    print("Lines containing date/market keywords:")
    lines = [l.strip() for l in full_text.replace(". ", ".\n").splitlines() if l.strip()]
    matches = [l for l in lines if keywords.search(l)]
    for line in matches[:60]:          # cap at 60 lines
        print(f"  {line[:200]}")

    # --- Also show a 3000-char window from mid-page where listing details live ---
    mid = len(resp.text) // 3
    print(f"\nRaw HTML slice [{mid}:{mid+3000}]:")
    print(resp.text[mid:mid + 3000])


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Run detail diagnostic on a URL passed as argument:
        # python apartment_hunter/diagnose.py https://streeteasy.com/building/...
        diagnose_streeteasy_detail(sys.argv[1])
    else:
        diagnose_streeteasy()
        diagnose_craigslist()
        diagnose_zillow()
