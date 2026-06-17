"""
Apartment Hunter — Streamlit UI

Displays listings synced from the scraper into Supabase, sorted by priority
score. Users can save or skip listings; actions persist to the database.

Credentials — set ONE of:
  • .streamlit/secrets.toml  (local dev, never commit this file)
  • Environment variables     (CI / Streamlit Community Cloud Secrets panel)

Keys required:
  SUPABASE_URL   — https://your-project-ref.supabase.co
  SUPABASE_KEY   — service role key (Settings → API → service_role)
"""

import csv
import os
import re
from datetime import datetime
from pathlib import Path

import folium
from streamlit_folium import st_folium
import streamlit.components.v1 as _components
import pandas as pd
import streamlit as st
from supabase import create_client, Client

st.set_page_config(
    page_title="Apartment Hunter",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_client() -> Client:
    url = (st.secrets.get("SUPABASE_URL") or os.environ.get("SUPABASE_URL", "")).strip()
    key = (st.secrets.get("SUPABASE_KEY") or os.environ.get("SUPABASE_KEY", "")).strip()
    if not url or not key:
        st.error(
            "**Missing credentials.** "
            "Set `SUPABASE_URL` and `SUPABASE_KEY` in `.streamlit/secrets.toml` "
            "or as environment variables."
        )
        st.stop()
    return create_client(url, key)


def _date_ts(d) -> float:
    """Timestamp for a date/datetime string ('YYYY-MM-DD' or full ISO);
    0.0 when missing or unparseable, so undated rows sort last under desc order."""
    try:
        return datetime.fromisoformat(str(d)[:19]).timestamp() if d else 0.0
    except ValueError:
        return 0.0


@st.cache_data(ttl=300)
def load_listings() -> list[dict]:
    result = _get_client().table("listings").select("*").or_("delisted.is.null,delisted.is.false").execute()
    data = result.data or []
    # Sort: priority first, then by score descending, then by date_found descending
    data.sort(key=lambda l: (
        not l.get("is_priority", False),
        -(l.get("priority_score") or 0),
        -_date_ts(l.get("date_found")),
    ))
    # Normalize neighborhood names for display (handles dirty Craigslist free-text)
    for listing in data:
        if listing.get("neighborhood"):
            listing["neighborhood"] = _normalize_hood(listing["neighborhood"])
    return data


@st.cache_data(ttl=86400)
def _load_stations() -> list[dict]:
    """Load subway_stations.csv → list of {name, routes, lat, lon}."""
    p = Path(__file__).parent / "apartment_hunter" / "data" / "subway_stations.csv"
    if not p.exists():
        return []
    stations = []
    try:
        with open(p, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    stations.append({
                        "name":   row["name"],
                        "routes": row["routes"],
                        "lat":    float(row["lat"]),
                        "lon":    float(row["lon"]),
                    })
                except (KeyError, ValueError):
                    pass
    except Exception:
        return []
    return stations


# ---------------------------------------------------------------------------
# Neighborhood normalization
# ---------------------------------------------------------------------------

# Maps lowercased, hyphen-stripped neighborhood strings → canonical display name.
# Handles Craigslist free-text variants, abbreviations, and slug formats.
_HOOD_ALIASES: dict[str, str] = {
    "bushwick":                  "Bushwick",
    "ridgewood":                 "Ridgewood",
    "bedford stuyvesant":        "Bedford-Stuyvesant",
    "bedford-stuyvesant":        "Bedford-Stuyvesant",
    "bed stuy":                  "Bedford-Stuyvesant",
    "bedstuy":                   "Bedford-Stuyvesant",
    "bed-stuy":                  "Bedford-Stuyvesant",
    "clinton hill":              "Clinton Hill",
    "clinton-hill":              "Clinton Hill",
    "prospect lefferts gardens": "Prospect Lefferts Gardens",
    "prospect-lefferts-gardens": "Prospect Lefferts Gardens",
    "prospect lefferts":         "Prospect Lefferts Gardens",
    "plg":                       "Prospect Lefferts Gardens",
    "williamsburg":              "Williamsburg",
    "wburg":                     "Williamsburg",
    "greenpoint":                "Greenpoint",
    "east williamsburg":         "East Williamsburg",
    "east-williamsburg":         "East Williamsburg",
    "e williamsburg":            "East Williamsburg",
    "crown heights":             "Crown Heights",
    "crown-heights":             "Crown Heights",
}


def _normalize_hood(hood: str) -> str:
    """
    Normalize a raw neighborhood string to a canonical display name.
    For slash-separated combos (e.g. "Williamsburg/Bedstuy"), picks the first
    recognised part. Falls back to title-casing the original if no match found.
    """
    if not hood:
        return hood
    for part in re.split(r"\s*[/\\|,&]\s*", hood.strip()):
        key = part.lower().replace("-", " ").strip()
        if key in _HOOD_ALIASES:
            return _HOOD_ALIASES[key]
    return hood.strip()


# Official MTA line colors (hex)
_MTA_COLORS: dict[str, str] = {
    "1": "#EE352E", "2": "#EE352E", "3": "#EE352E",   # Red
    "4": "#00933C", "5": "#00933C", "6": "#00933C",   # Green
    "7": "#B933AD",                                    # Purple
    "A": "#0039A6", "C": "#0039A6", "E": "#0039A6",   # Blue
    "B": "#FF6319", "D": "#FF6319", "F": "#FF6319", "M": "#FF6319",  # Orange
    "G": "#6CBE45",                                    # Light green
    "J": "#996633", "Z": "#996633",                    # Brown
    "L": "#A7A9AC",                                    # Gray
    "N": "#FCCC0A", "Q": "#FCCC0A", "R": "#FCCC0A", "W": "#FCCC0A",  # Yellow
    "S": "#808183", "SIR": "#0039A6",                  # Shuttle gray / SIR blue
}


def _station_color(routes: str) -> str:
    """Return the MTA color for a station based on its first listed route."""
    if not routes:
        return "#808183"
    first = re.split(r"[,/\s]+", routes.strip())[0].upper()
    return _MTA_COLORS.get(first, "#808183")


def _nearest_stations(lat: float, lon: float, n: int = 5) -> list[dict]:
    """Return the n closest subway stations by straight-line distance."""
    from math import radians, sin, cos, sqrt, atan2
    stations = _load_stations()
    scored = []
    for s in stations:
        dlat = radians(s["lat"] - lat)
        dlon = radians(s["lon"] - lon)
        a = sin(dlat/2)**2 + cos(radians(lat)) * cos(radians(s["lat"])) * sin(dlon/2)**2
        dist_m = 6_371_000 * 2 * atan2(sqrt(a), sqrt(1-a))
        scored.append((dist_m, s))
    scored.sort(key=lambda x: x[0])
    return [s for _, s in scored[:n]]


def _listing_map(lat: float, lon: float) -> folium.Map:
    """Build a Folium map centred on the listing with nearby subway stops."""
    m = folium.Map(
        location=[lat, lon],
        zoom_start=15,
        tiles="CartoDB positron",
        scrollWheelZoom=False,
    )

    # Listing marker — blue star
    folium.Marker(
        location=[lat, lon],
        icon=folium.DivIcon(
            html=(
                '<div style="font-size:33px;color:#1e78dc;'
                'text-shadow:0 1px 3px rgba(0,0,0,0.6);line-height:1">★</div>'
            ),
            icon_size=(33, 33),
            icon_anchor=(17, 17),
        ),
        tooltip="Listing",
    ).add_to(m)

    # Subway station markers — colored by MTA line
    nearby = _nearest_stations(lat, lon, n=5)
    all_points = [[lat, lon]]
    for s in nearby:
        label = f"🚇 {s['name']} ({s['routes']})" if s["routes"] else f"🚇 {s['name']}"
        color = _station_color(s["routes"])
        folium.CircleMarker(
            location=[s["lat"], s["lon"]],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.9,
            weight=2,
            tooltip=label,
        ).add_to(m)
        all_points.append([s["lat"], s["lon"]])

    # Fit viewport to include all markers
    if len(all_points) > 1:
        lats = [p[0] for p in all_points]
        lons = [p[1] for p in all_points]
        m.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]],
                     padding=(20, 20))

    return m


@st.cache_data(ttl=86400, show_spinner=False)
def _listing_map_html(lat: float, lon: float) -> str:
    """Render the listing map to a standalone HTML string, memoised per coordinate.

    Served as a static iframe via st.components.v1.html instead of the bidirectional
    st_folium component — so a rerun (skip / load-more) just re-injects pre-rendered
    HTML rather than re-mounting a component and waiting on its round-trip handshake.
    The map is still fully interactive client-side; we just don't read its state back.
    """
    return _listing_map(lat, lon).get_root().render()


# Listing-marker colors for the all-listings map.
_PIN_PRIORITY = "#d63b3b"   # red
_PIN_SAVED    = "#2e9e5b"   # green
_PIN_DEFAULT  = "#1e78dc"   # blue


def _pin_icon(color: str) -> folium.DivIcon:
    """A small colored star marker for a listing."""
    return folium.DivIcon(
        html=(
            f'<div style="font-size:21px;color:{color};line-height:1;'
            f'text-shadow:0 1px 2px rgba(0,0,0,0.6),0 0 2px #fff">★</div>'
        ),
        icon_size=(21, 21),
        icon_anchor=(10, 11),
    )


@st.cache_resource(ttl=600, max_entries=8, show_spinner="Building map…")
def _build_all_map(signature: tuple) -> folium.Map:
    """Build one Folium map with every filtered listing plus the full subway network.

    `signature` is a tuple of per-listing tuples built by the caller from the
    filtered set, so the cache key changes whenever that set (or any listing's
    price / status / cover image) changes. Cached as a resource — the same map
    object is reused across reruns (e.g. a marker click) when nothing changed.
    """
    coords = [(lat, lon) for (_u, lat, lon, *_rest) in signature
              if lat is not None and lon is not None]

    if coords:
        center = [sum(c[0] for c in coords) / len(coords),
                  sum(c[1] for c in coords) / len(coords)]
    else:
        center = [40.68, -73.94]

    m = folium.Map(location=center, zoom_start=13, tiles="CartoDB positron")

    # Subway stations — small colored circles, drawn first so listings sit on top.
    for s in _load_stations():
        color = _station_color(s["routes"])
        label = f"🚇 {s['name']} ({s['routes']})" if s["routes"] else f"🚇 {s['name']}"
        folium.CircleMarker(
            location=[s["lat"], s["lon"]],
            radius=6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.9,
            weight=1,
            tooltip=label,
        ).add_to(m)

    # Listing markers — small circular pins (red=priority, green=saved, blue=other).
    for (url, lat, lon, price, date_listed, hood, source,
         is_priority, status, cover) in signature:
        if lat is None or lon is None:
            continue
        price_s = _fmt_price(price)
        date_s  = _fmt_date_listed((date_listed or "")[:10])
        color   = (_PIN_PRIORITY if is_priority
                   else _PIN_SAVED if status == "saved"
                   else _PIN_DEFAULT)

        tip = price_s + (f" · listed {date_s}" if date_s else "")
        popup_html = (
            '<div style="font-size:0.85rem;width:180px">'
            + (f'<img src="{cover}" style="width:100%;height:118px;object-fit:cover;'
               f'border-radius:4px;margin-bottom:5px"/>' if cover else "")
            + f'<b>{price_s}</b><br>'
            + (f'Listed {date_s}<br>' if date_s else "")
            + (f'{hood}' if hood else "")
            + '</div>'
        )
        folium.Marker(
            location=[lat, lon],
            tooltip=tip,
            popup=folium.Popup(popup_html, max_width=220),
            icon=_pin_icon(color),
        ).add_to(m)

    # Fit the viewport to the listings (ignore stations so it doesn't zoom out to
    # the whole network when listings cluster in a few neighborhoods).
    if len(coords) > 1:
        lats = [c[0] for c in coords]
        lons = [c[1] for c in coords]
        m.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]],
                     padding=(30, 30))

    return m


def _set_status(url: str, status: "str | None") -> None:
    _get_client().table("listings").update({"user_status": status}).eq("url", url).execute()
    # Reflect the change immediately via a session overlay instead of clearing the
    # whole listings cache — clearing forced a full ~1.7s Supabase refetch on every
    # Save/Skip.  The overlay is applied to the cached data right after it loads.
    st.session_state.setdefault("_status_overrides", {})[url] = status


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _badge(text: str, color: str) -> str:
    return (
        f'<span style="background:{color};color:#fff;padding:2px 8px;'
        f'border-radius:12px;font-size:0.72rem;margin-right:4px;'
        f'white-space:nowrap">{text}</span>'
    )


def _fmt_price(price) -> str:
    return f"${int(price):,}/mo" if price else "?"


def _fmt_beds_baths_floor(beds, baths, floor_) -> str:
    parts = []
    if beds is not None:
        parts.append(f"{int(beds)} bed{'s' if int(beds) != 1 else ''}")
    if baths is not None:
        parts.append(f"{float(baths):g} bath{'s' if float(baths) != 1 else ''}")
    if floor_:
        parts.append(f"{floor_} floor")
    return "  ·  ".join(parts)


def _fmt_date_listed(date_str: str) -> str:
    """Format a YYYY-MM-DD string as 'Mar 15, 2026'."""
    if not date_str:
        return ""
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return f"{d.strftime('%b')} {d.day}, {d.year}"
    except ValueError:
        return ""


# Human-readable labels for the StreetEasy listing_status badge.
_STATUS_LABELS = {
    "available":              "Available",
    "temporarily_off_market": "Temporarily off market",
    "delisted":               "Delisted",
    "rented":                 "Rented",
    "unavailable":            "Unavailable",
}


def _fmt_listing_status(status: str) -> str:
    """Format the stored listing_status into a display label ('' if unknown)."""
    return _STATUS_LABELS.get((status or "").strip().lower(), "")


_SOURCE_LABELS = {
    "streeteasy":     "StreetEasy",
    "craigslist":     "Craigslist",
    "zillow":         "Zillow",
    "apartments_com": "Apartments.com",
}


def _source_label(source: str) -> str:
    return _SOURCE_LABELS.get(source, source.title())


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("🏠 Apartment Hunter")

view = st.sidebar.radio("View", ["List", "Map"], index=0, horizontal=True)
st.sidebar.markdown("---")

status_view = st.sidebar.radio(
    "Show listings",
    ["Unreviewed", "Saved", "Saved & Unreviewed", "Skipped", "All active"],
    index=0,
)

priority_only = st.sidebar.checkbox("Priority only", value=False)

sort_by = st.sidebar.selectbox(
    "Sort by",
    ["Score", "Score + Date listed", "Date listed", "Price ↑", "Price ↓"],
    index=0,
)

st.sidebar.markdown("**Amenities**")
filter_rent_stab  = st.sidebar.checkbox("Rent stabilized", value=False, key="f_rs")
filter_dishwasher = st.sidebar.checkbox("Dishwasher", value=False, key="f_dw")
filter_wd         = st.sidebar.checkbox("W/D in unit", value=False, key="f_wd")

st.sidebar.markdown("---")

# Load data (needed to populate filter options)
all_listings = load_listings()

# Apply this session's pending Save/Skip changes without a Supabase refetch.
# (Supabase was already updated in _set_status; this just keeps the cached view
# in sync until the cache naturally refreshes.)
_overrides = st.session_state.get("_status_overrides")
if _overrides:
    for _l in all_listings:
        if _l.get("url") in _overrides:
            _l["user_status"] = _overrides[_l["url"]]

sources       = sorted({l["source"] for l in all_listings if l.get("source")})
neighborhoods = sorted({l["neighborhood"] for l in all_listings if l.get("neighborhood")})

st.sidebar.markdown("**Source**")
selected_sources = [src for src in sources
                    if st.sidebar.checkbox(_source_label(src), value=True, key=f"src_{src}")]

selected_hoods   = st.sidebar.multiselect("Neighborhood", neighborhoods, default=neighborhoods)

st.sidebar.markdown("---")

if st.sidebar.button("↺ Refresh data"):
    load_listings.clear()
    st.session_state.pop("_status_overrides", None)  # reset overlay to Supabase truth
    st.rerun()

st.sidebar.caption(
    f"Data auto-refreshes every 5 min. "
    f"Last loaded: {len(all_listings)} active listings."
)

# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def _apply_filters(listings: list[dict]) -> list[dict]:
    out = []
    for l in listings:
        if l.get("delisted"):
            continue
        status = l.get("user_status")
        if status_view == "Unreviewed" and status is not None:
            continue
        if status_view == "Saved" and status != "saved":
            continue
        if status_view == "Saved & Unreviewed" and status == "skipped":
            continue  # keep unreviewed (None) + saved; exclude only skipped
        if status_view == "Skipped" and status != "skipped":
            continue
        if priority_only and not l.get("is_priority"):
            continue
        if filter_rent_stab and not l.get("rent_stabilized"):
            continue
        if filter_dishwasher and not l.get("dishwasher"):
            continue
        if filter_wd and not l.get("washer_dryer"):
            continue
        if selected_sources and l.get("source") not in selected_sources:
            continue
        if selected_hoods and l.get("neighborhood") not in selected_hoods:
            continue
        out.append(l)
    return out


filtered = _apply_filters(all_listings)

if sort_by == "Price ↑":
    filtered.sort(key=lambda l: l.get("price") or 999_999)
elif sort_by == "Price ↓":
    filtered.sort(key=lambda l: -(l.get("price") or 0))
elif sort_by == "Date listed":
    # Newest listings first; undated rows fall to the bottom.
    filtered.sort(key=lambda l: -_date_ts(l.get("date_listed")))
elif sort_by == "Score + Date listed":
    # Nested: priority first, then score desc, then most-recently-listed first.
    filtered.sort(key=lambda l: (
        not l.get("is_priority", False),
        -(l.get("priority_score") or 0),
        -_date_ts(l.get("date_listed")),
    ))
# "Score" → keep the priority/score/date_found order from load_listings()

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Back-to-top button — injected into the parent page DOM via a hidden iframe.
# Uses setInterval polling rather than scroll events, which don't reliably
# bubble across the iframe boundary in Streamlit.
# ---------------------------------------------------------------------------
_components.html("""
<script>
(function() {
  try {
    var pw, pd;
    try { pw = window.parent; pd = window.parent.document; }
    catch(e) { pw = window.top;  pd = window.top.document;  }

    // ── Clean up previous renders ─────────────────────────────────────────────
    ['apt-btt', 'apt-sentinel', 'apt-lightbox'].forEach(function(id) {
      var el = pd.getElementById(id);
      if (el) el.remove();
    });

    // ── Back-to-top ───────────────────────────────────────────────────────────
    var sentinel = pd.createElement('div');
    sentinel.id = 'apt-sentinel';
    sentinel.style.cssText = 'position:absolute;top:0;left:0;width:1px;height:1px;pointer-events:none;';
    pd.body.insertBefore(sentinel, pd.body.firstChild);

    var btn = pd.createElement('button');
    btn.id = 'apt-btt';
    btn.textContent = '↑';
    btn.title = 'Back to top';
    btn.style.cssText =
      'display:none;position:fixed;bottom:28px;right:28px;z-index:99999;' +
      'width:44px;height:44px;border-radius:50%;border:none;' +
      'background:#1e78dc;color:#fff;font-size:22px;line-height:44px;' +
      'text-align:center;cursor:pointer;' +
      'box-shadow:0 2px 10px rgba(0,0,0,0.4);';
    btn.onclick = function() {
      var best = null, bestVal = pw.scrollY || pw.pageYOffset || 0;
      pd.querySelectorAll('*').forEach(function(el) {
        if (el.scrollTop > bestVal) { bestVal = el.scrollTop; best = el; }
      });
      if (best) { best.scrollTo({top: 0, behavior: 'smooth'}); }
      else       { pw.scrollTo({top: 0, behavior: 'smooth'}); }
    };
    pd.body.appendChild(btn);
    if (window.IntersectionObserver) {
      new IntersectionObserver(function(entries) {
        btn.style.display = entries[0].isIntersecting ? 'none' : 'block';
      }, { threshold: 0 }).observe(sentinel);
    }

    // ── Lightbox ──────────────────────────────────────────────────────────────
    var lb = pd.createElement('div');
    lb.id = 'apt-lightbox';
    lb.style.cssText =
      'display:none;position:fixed;inset:0;z-index:100000;' +
      'background:rgba(0,0,0,0.93);align-items:center;justify-content:center;';
    lb.innerHTML =
      '<button id="apt-lb-x" title="Close (Esc)" style="' +
        'position:absolute;top:16px;right:20px;z-index:100001;' +
        'width:44px;height:44px;border-radius:50%;border:none;' +
        'background:rgba(0,0,0,0.55);color:#fff;font-size:24px;line-height:44px;' +
        'text-align:center;cursor:pointer;padding:0;opacity:0.9;' +
        'box-shadow:0 2px 8px rgba(0,0,0,0.5);">✕</button>' +
      '<button id="apt-lb-prev" style="' +
        'position:absolute;left:0;top:50%;transform:translateY(-50%);' +
        'background:none;border:none;color:#fff;font-size:72px;' +
        'cursor:pointer;padding:0 22px;line-height:1;opacity:0.7;user-select:none;">&#8249;</button>' +
      '<img id="apt-lb-img" src="" alt="" style="' +
        'width:90vw;height:85vh;object-fit:contain;' +
        'border-radius:4px;display:block;" />' +
      '<button id="apt-lb-next" style="' +
        'position:absolute;right:0;top:50%;transform:translateY(-50%);' +
        'background:none;border:none;color:#fff;font-size:72px;' +
        'cursor:pointer;padding:0 22px;line-height:1;opacity:0.7;user-select:none;">&#8250;</button>' +
      '<div id="apt-lb-ctr" style="' +
        'position:absolute;bottom:18px;left:0;right:0;text-align:center;' +
        'color:#aaa;font-size:0.85rem;pointer-events:none;"></div>';
    pd.body.appendChild(lb);

    var _lbImgs = [], _lbIdx = 0;

    function _lbUpgrade(u) {
      return u.replace(/-cc_ft_\d+\./, '-cc_ft_1536.');
    }
    function _lbRender() {
      var img    = pd.getElementById('apt-lb-img');
      var ctr    = pd.getElementById('apt-lb-ctr');
      var prev   = pd.getElementById('apt-lb-prev');
      var next   = pd.getElementById('apt-lb-next');
      var rawSrc = _lbImgs[_lbIdx] || '';
      var hiSrc  = _lbUpgrade(rawSrc);
      if (img) {
        img.onerror = null;
        img.src = hiSrc;
        if (hiSrc !== rawSrc) {
          img.onerror = function() { this.onerror = null; this.src = rawSrc; };
        }
      }
      if (ctr)  ctr.textContent = (_lbIdx + 1) + ' of ' + _lbImgs.length;
      if (prev) prev.style.opacity = _lbIdx === 0                ? '0.2' : '0.75';
      if (next) next.style.opacity = _lbIdx >= _lbImgs.length-1 ? '0.2' : '0.75';
    }
    function _lbOpen(imgs, startIdx) {
      _lbImgs = imgs || [];
      _lbIdx  = startIdx || 0;
      lb.style.display = 'flex';
      _lbRender();
    }
    function _lbClose() { lb.style.display = 'none'; }
    function _lbPrev()  { if (_lbIdx > 0)                  { _lbIdx--; _lbRender(); } }
    function _lbNext()  { if (_lbIdx < _lbImgs.length - 1) { _lbIdx++; _lbRender(); } }

    lb.onclick = function(e) { if (e.target === lb) _lbClose(); };
    var _lbX = pd.getElementById('apt-lb-x');
    _lbX.onclick      = function(e) { e.stopPropagation(); _lbClose(); };
    _lbX.onmouseenter = function() { this.style.opacity = '1';   this.style.background = 'rgba(0,0,0,0.85)'; };
    _lbX.onmouseleave = function() { this.style.opacity = '0.9'; this.style.background = 'rgba(0,0,0,0.55)'; };
    pd.getElementById('apt-lb-prev').onclick = function(e) { e.stopPropagation(); _lbPrev(); };
    pd.getElementById('apt-lb-next').onclick = function(e) { e.stopPropagation(); _lbNext(); };
    pd.addEventListener('keydown', function(e) {
      if (lb.style.display !== 'none') {
        if (e.key === 'Escape')     { e.preventDefault(); _lbClose(); }
        if (e.key === 'ArrowLeft')  { e.preventDefault(); _lbPrev(); }
        if (e.key === 'ArrowRight') { e.preventDefault(); _lbNext(); }
      }
    });

    pw.__aptShowLightbox = _lbOpen;

    console.log('[apt-btt+lb] injected');
  } catch(e) {
    console.error('[apt-btt+lb] error:', e);
  }
})();
</script>
""", height=1)

today_str     = datetime.utcnow().strftime("%Y-%m-%d")
total_active  = len(all_listings)
total_priority = sum(1 for l in all_listings if l.get("is_priority"))
total_saved   = sum(1 for l in all_listings if l.get("user_status") == "saved")
total_new     = sum(1 for l in all_listings if (l.get("date_found") or "")[:10] == today_str)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Active listings", total_active)
m2.metric("Priority",        total_priority)
m3.metric("Saved",           total_saved)
m4.metric("New today",       total_new)

st.markdown(f"**Showing {len(filtered)} listing(s)**")
st.divider()

# ---------------------------------------------------------------------------
# Image carousel
# ---------------------------------------------------------------------------

def _image_carousel(images: list[str], key: str) -> None:
    """Render a horizontally-scrollable carousel showing 3 images at a time."""
    if not images:
        _components.html(
            '<div style="background:#1e1e1e;height:200px;border-radius:6px;'
            'display:flex;align-items:center;justify-content:center;'
            'color:#555;font-size:0.78rem">No image</div>',
            height=190,
        )
        return

    imgs_json = str(images).replace("'", '"')
    _components.html(f"""
<style>
  .carousel-wrap {{
    position: relative;
    width: 100%;
    overflow: hidden;
    border-radius: 6px;
    user-select: none;
  }}
  .carousel-track {{
    display: flex;
    gap: 6px;
    transition: transform 0.3s ease;
  }}
  .carousel-track img {{
    flex: 0 0 calc((100% - 12px) / 3);
    width: calc((100% - 12px) / 3);
    height: 200px;
    object-fit: contain;
    background: #111;
    border-radius: 6px;
    cursor: zoom-in;
  }}
  .carousel-btn {{
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    background: rgba(0,0,0,0.55);
    color: #fff;
    border: none;
    border-radius: 50%;
    width: 32px;
    height: 32px;
    font-size: 18px;
    line-height: 30px;
    text-align: center;
    cursor: pointer;
    z-index: 10;
    padding: 0;
  }}
  .carousel-btn:hover {{ background: rgba(0,0,0,0.8); }}
  .carousel-btn.prev {{ left: 4px; }}
  .carousel-btn.next {{ right: 4px; }}
  .carousel-counter {{
    text-align: center;
    font-size: 0.72rem;
    color: #888;
    margin-top: 4px;
  }}
</style>
<div class="carousel-wrap" id="cw-{key}">
  <button class="carousel-btn prev" onclick="move_{key}(-1)">&#8249;</button>
  <div class="carousel-track" id="ct-{key}"></div>
  <button class="carousel-btn next" onclick="move_{key}(1)">&#8250;</button>
</div>
<div class="carousel-counter" id="cc-{key}"></div>
<script>
(function() {{
  const imgs = {imgs_json};
  const track = document.getElementById('ct-{key}');
  const counter = document.getElementById('cc-{key}');
  let idx = 0;
  const visible = 3;

  imgs.forEach(function(src, i) {{
    const img = document.createElement('img');
    const hi = src.replace(/-cc_ft_\d+\./, '-cc_ft_1536.');
    img.src = hi;
    if (hi !== src) {{
      img.onerror = function() {{ this.onerror = null; this.src = src; }};
    }}
    img.onclick = function() {{
      try {{ window.parent.__aptShowLightbox(imgs, i); }}
      catch(e) {{ window.open(src, '_blank'); }}
    }};
    track.appendChild(img);
  }});

  function render() {{
    const pct = idx * (100 / visible + 2);
    track.style.transform = 'translateX(-' + (idx * (100 + 2) / visible) + '%)';
    const end = Math.min(idx + visible, imgs.length);
    counter.textContent = imgs.length > visible
      ? (idx + 1) + '–' + end + ' of ' + imgs.length + ' photos'
      : imgs.length + ' photo' + (imgs.length > 1 ? 's' : '');
  }}

  window['move_{key}'] = function(dir) {{
    idx = Math.max(0, Math.min(idx + dir, imgs.length - visible));
    render();
  }};

  render();

  // Size images to 4:3 of their rendered width, re-run on resize
  function sizeImgs() {{
    const wrap = document.getElementById('cw-{key}');
    if (!wrap) return;
    const imgW = Math.floor((wrap.clientWidth - 12) / 3);
    const imgH = Math.floor(imgW * 0.75);
    track.querySelectorAll('img').forEach(function(img) {{
      img.style.height = imgH + 'px';
    }});
  }}
  sizeImgs();
  if (window.ResizeObserver) {{
    new ResizeObserver(sizeImgs).observe(document.getElementById('cw-{key}'));
  }}
}})();
</script>
""", height=380)


# ---------------------------------------------------------------------------
# Map view — all filtered listings on one map (then stop; skip the list UI)
# ---------------------------------------------------------------------------

if view == "Map":
    st.markdown(f"### 🗺️ {len(filtered)} listing(s) on the map")
    st.caption(
        "Hover a marker for price & date listed; click one to open its photos and "
        "save/skip below the map. Red = priority, green = saved, blue = other. "
        "Use the sidebar filters to narrow the set."
    )

    if not filtered:
        st.info("No listings match the current filters.")
        st.stop()

    def _cover(listing: dict) -> str:
        imgs = [u.strip() for u in (listing.get("image_url") or "").split(",") if u.strip()]
        return imgs[0] if imgs else ""

    # Signature drives the cached map build; include status + cover so the map
    # rebuilds (and a saved marker turns green) right after a Save/Skip.
    _sig = tuple(
        (
            l.get("url"),
            None if l.get("latitude")  is None else float(l["latitude"]),
            None if l.get("longitude") is None else float(l["longitude"]),
            l.get("price"), l.get("date_listed"), l.get("neighborhood"),
            l.get("source"), bool(l.get("is_priority")), l.get("user_status"),
            _cover(l),
        )
        for l in filtered
    )

    # Listing points, for resolving a clicked marker back to its listing.
    _listing_pts = [
        (float(l["latitude"]), float(l["longitude"]), l)
        for l in filtered
        if l.get("latitude") is not None and l.get("longitude") is not None
    ]

    # ── The map ─────────────────────────────────────────────────────────────────
    # Rendered first so the selected-listing card appears *below* it. Reading the
    # click straight from st_folium's return (no manual st.rerun) keeps the map's
    # pan/zoom stable between clicks — the cached map object is unchanged, so
    # st_folium doesn't re-fit the view.
    _map_state = st_folium(
        _build_all_map(_sig),
        use_container_width=True,
        height=640,
        returned_objects=["last_object_clicked"],
        key="all_map",
    )

    # ── Selected-listing card (below the map, like a normal list card) ──────────
    # Match the clicked marker to the nearest listing (robust to float drift);
    # clicks on subway circles fall outside the threshold and select nothing.
    _clicked = (_map_state or {}).get("last_object_clicked") or {}
    _selected = None
    _cl, _cn = _clicked.get("lat"), _clicked.get("lng")
    if _cl is not None and _cn is not None and _listing_pts:
        _best = min(_listing_pts, key=lambda p: (p[0] - _cl) ** 2 + (p[1] - _cn) ** 2)
        if (_best[0] - _cl) ** 2 + (_best[1] - _cn) ** 2 <= 0.0006 ** 2:
            _selected = _best[2]

    st.divider()
    if not _selected:
        st.caption("👆 Click a marker to open the listing here.")
        st.stop()

    s_url    = _selected.get("url", "")
    s_addr   = _selected.get("address") or _selected.get("title") or "Listing"
    s_status = _selected.get("user_status")
    s_imgs   = [u.strip() for u in (_selected.get("image_url") or "").split(",") if u.strip()]
    with st.container(border=True):
        st.markdown(f"#### {('★ ' if _selected.get('is_priority') else '')}{s_addr}")
        meta = [_source_label(_selected.get("source", "")), _fmt_price(_selected.get("price"))]
        _dl = _fmt_date_listed((_selected.get("date_listed") or "")[:10])
        if _selected.get("neighborhood"):
            meta.insert(1, _selected["neighborhood"])
        if _dl:
            meta.append(f"Listed {_dl}")
        st.caption("  ·  ".join(meta))

        _image_carousel(s_imgs, key="map_" + (_selected.get("listing_id") or s_url[-12:]))

        b_link, b_save, b_skip, b_undo = st.columns([2, 1, 1, 1])
        with b_link:
            st.link_button(f"View on {_source_label(_selected.get('source',''))} ↗",
                           s_url, use_container_width=True)
        with b_save:
            if st.button("★ Saved" if s_status == "saved" else "Save",
                         key=f"map_save_{s_url}",
                         type="primary" if s_status == "saved" else "secondary",
                         use_container_width=True):
                _set_status(s_url, "saved")
                st.rerun()
        with b_skip:
            if st.button("Skipped" if s_status == "skipped" else "Skip",
                         key=f"map_skip_{s_url}", use_container_width=True):
                _set_status(s_url, "skipped")
                st.rerun()
        with b_undo:
            if st.button("Undo", key=f"map_undo_{s_url}",
                         use_container_width=True, disabled=s_status is None):
                _set_status(s_url, None)
                st.rerun()

    st.stop()


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

_PAGE_SIZE = 10

# Reset to page 1 whenever any filter or sort changes
_filter_key = (status_view, priority_only, sort_by,
               tuple(selected_sources), tuple(selected_hoods),
               filter_rent_stab, filter_dishwasher, filter_wd)
if st.session_state.get("_filter_key") != _filter_key:
    st.session_state["_filter_key"] = _filter_key
    st.session_state["_page"] = 1

_visible_count = st.session_state.get("_page", 1) * _PAGE_SIZE
visible_listings = filtered[:_visible_count]

# ---------------------------------------------------------------------------
# Listing cards
# ---------------------------------------------------------------------------

if not filtered:
    st.info("No listings match the current filters.")
else:
    for listing in visible_listings:
        url        = listing.get("url", "")
        source     = listing.get("source", "")
        address    = listing.get("address") or listing.get("title") or "Listing"
        hood       = listing.get("neighborhood") or ""
        price      = listing.get("price")
        beds       = listing.get("bedrooms")
        baths      = listing.get("bathrooms")
        floor_     = listing.get("floor")
        subway     = listing.get("subway_lines") or listing.get("nearest_subway") or ""
        image_url  = listing.get("image_url")
        is_priority = listing.get("is_priority", False)
        score      = listing.get("priority_score")
        rent_stab  = listing.get("rent_stabilized")
        dishwasher = listing.get("dishwasher")
        wd         = listing.get("washer_dryer")
        user_status = listing.get("user_status")
        date_listed = (listing.get("date_listed") or "")[:10]
        # Default to "Available": the app only renders active (non-delisted)
        # listings, so one with no known badge is available by definition.
        # Real statuses (e.g. "Temporarily off market") override the default as
        # the scraper backfills them. (Craigslist has no badge → always Available.)
        status_fmt  = _fmt_listing_status(listing.get("listing_status")) or "Available"

        with st.container(border=True):
            # ── Top row: details (left) + map (right) ───────────────────────
            top_left, top_right = st.columns(2)

            with top_left:
                priority_prefix = "★ " if is_priority else ""
                st.markdown(f"#### {priority_prefix}{address}")
                meta_parts = [_source_label(source)]
                if hood:
                    meta_parts.append(hood)
                if score is not None:
                    meta_parts.append(f"score: {score:.0f}")
                st.caption("  ·  ".join(meta_parts))

                st.markdown(
                    f'<p style="font-size:1.4rem;font-weight:700;margin:4px 0">'
                    f'{_fmt_price(price)}</p>',
                    unsafe_allow_html=True,
                )

                detail_parts = [_fmt_beds_baths_floor(beds, baths, floor_)]
                date_fmt = _fmt_date_listed(date_listed)
                if date_fmt:
                    detail_parts.append(f"Listed {date_fmt}")
                if status_fmt:
                    detail_parts.append(status_fmt)
                detail_line = "  ·  ".join(p for p in detail_parts if p)
                if detail_line:
                    st.markdown(detail_line)
                if subway:
                    st.markdown(f"🚇 {subway}")

                badges = []
                if is_priority:
                    badges.append(_badge("★ Priority", "#8B0000"))
                if rent_stab:
                    badges.append(_badge("Rent stabilized", "#5c4a00"))
                if dishwasher:
                    badges.append(_badge("Dishwasher", "#1a5c33"))
                if wd:
                    badges.append(_badge("W/D in unit", "#1a3a6b"))
                if user_status == "saved":
                    badges.append(_badge("★ Saved", "#3a3a8b"))
                if badges:
                    st.markdown("&nbsp;" + " ".join(badges), unsafe_allow_html=True)

            with top_right:
                # Coordinates are geocoded by the scraper and stored on the listing
                # (Supabase), so the map renders with no geocoding at render time.
                # Static HTML (cached per coordinate) rather than st_folium — display
                # only, no per-rerun component handshake.
                _lat, _lon = listing.get("latitude"), listing.get("longitude")
                if _lat is not None and _lon is not None:
                    _components.html(_listing_map_html(float(_lat), float(_lon)), height=300)
                else:
                    st.caption("📍 No location available to map.")

            # ── Bottom row: image carousel (full width) ──────────────────────
            images = [u.strip() for u in (image_url or "").split(",") if u.strip()]
            _image_carousel(images, key=listing.get("listing_id") or url[-12:])

            # ── Actions ──────────────────────────────────────────────────────
            st.markdown("")
            act_link, act_save, act_skip, act_undo = st.columns([2, 1, 1, 1])

            with act_link:
                st.link_button(f"View on {_source_label(source)} ↗", url, use_container_width=True)

            with act_save:
                if user_status == "saved":
                    if st.button("★ Saved", key=f"save_{url}", type="primary",
                                 use_container_width=True, help="Click to unsave"):
                        _set_status(url, None)
                        st.rerun()
                else:
                    if st.button("☆ Save", key=f"save_{url}",
                                 use_container_width=True):
                        _set_status(url, "saved")
                        st.rerun()

            with act_skip:
                if user_status == "skipped":
                    # Skipped listings only show in "All active" view
                    if st.button("✕ Skipped", key=f"skip_{url}",
                                 use_container_width=True, help="Click to restore"):
                        _set_status(url, None)
                        st.rerun()
                else:
                    if st.button("✕ Skip", key=f"skip_{url}",
                                 use_container_width=True):
                        _set_status(url, "skipped")
                        st.rerun()

            # Empty column for spacing
            with act_undo:
                pass

    # Load more
    if _visible_count < len(filtered):
        remaining = len(filtered) - _visible_count
        st.markdown("")
        if st.button(f"Load {min(_PAGE_SIZE, remaining)} more  ({remaining} remaining)", use_container_width=True):
            st.session_state["_page"] = st.session_state.get("_page", 1) + 1
            st.rerun()
    elif len(filtered) > _PAGE_SIZE:
        st.caption(f"All {len(filtered)} listings shown.")
