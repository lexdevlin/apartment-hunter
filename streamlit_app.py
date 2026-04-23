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
import json
import os
import re
from datetime import datetime
from pathlib import Path

import folium
import streamlit.components.v1 as _components
import pandas as pd
import requests as _requests
import streamlit as st
from streamlit_folium import st_folium
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


@st.cache_data(ttl=300)
def load_listings() -> list[dict]:
    result = _get_client().table("listings").select("*").or_("delisted.is.null,delisted.is.false").execute()
    data = result.data or []
    # Sort: priority first, then by score descending, then by date_found descending
    def _date_ts(d) -> float:
        try:
            return datetime.fromisoformat(str(d)[:19]).timestamp() if d else 0.0
        except ValueError:
            return 0.0

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


@st.cache_resource
def _load_geocode_cache() -> dict:
    """Load the scraper's geocode_cache.json (keyed 'address|neighborhood')."""
    p = Path(__file__).parent / "apartment_hunter" / "data" / "geocode_cache.json"
    if not p.exists():
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


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


def _resolve_coords(address: str, neighborhood: str) -> "tuple[float, float] | None":
    """
    Look up (lat, lon) for a listing:
      1. Geocode cache (address|neighborhood key — same format as subway.py)
      2. Live Nominatim call as fallback
    """
    cache = _load_geocode_cache()
    key = f"{address}|{neighborhood}" if address and neighborhood else (address or "")
    if key in cache and cache[key]:
        return tuple(cache[key])

    # Strip unit number before geocoding
    clean = re.sub(r"\s*#\S+$", "", address or "").strip()
    if not clean:
        return None
    query = clean if re.search(r"\bNY\b|\bBrooklyn\b", clean, re.IGNORECASE) \
        else f"{clean}, Brooklyn, NY"
    try:
        resp = _requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1},
            headers={"User-Agent": "apartment-hunter-app/1.0"},
            timeout=5,
        )
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None


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
                '<div style="font-size:22px;color:#1e78dc;'
                'text-shadow:0 1px 3px rgba(0,0,0,0.6);line-height:1">★</div>'
            ),
            icon_size=(22, 22),
            icon_anchor=(11, 11),
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


def _set_status(url: str, status: "str | None") -> None:
    _get_client().table("listings").update({"user_status": status}).eq("url", url).execute()
    load_listings.clear()


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

status_view = st.sidebar.radio(
    "Show listings",
    ["Unreviewed", "Saved", "Skipped", "All active"],
    index=0,
)

priority_only = st.sidebar.checkbox("Priority only", value=False)

sort_by = st.sidebar.selectbox("Sort by", ["Score", "Price ↑", "Price ↓"], index=0)

st.sidebar.markdown("**Amenities**")
filter_rent_stab  = st.sidebar.checkbox("Rent stabilized", value=False, key="f_rs")
filter_dishwasher = st.sidebar.checkbox("Dishwasher", value=False, key="f_dw")
filter_wd         = st.sidebar.checkbox("W/D in unit", value=False, key="f_wd")

st.sidebar.markdown("---")

# Load data (needed to populate filter options)
all_listings = load_listings()

sources       = sorted({l["source"] for l in all_listings if l.get("source")})
neighborhoods = sorted({l["neighborhood"] for l in all_listings if l.get("neighborhood")})

st.sidebar.markdown("**Source**")
selected_sources = [src for src in sources
                    if st.sidebar.checkbox(_source_label(src), value=True, key=f"src_{src}")]

selected_hoods   = st.sidebar.multiselect("Neighborhood", neighborhoods, default=neighborhoods)

st.sidebar.markdown("---")

if st.sidebar.button("↺ Refresh data"):
    load_listings.clear()
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

# ---------------------------------------------------------------------------
# DEBUG (temporary) — remove once map is confirmed working
# ---------------------------------------------------------------------------
with st.expander("🔧 Map debug info", expanded=False):
    _stations_path = Path(__file__).parent / "apartment_hunter" / "data" / "subway_stations.csv"
    st.write(f"**CSV path:** `{_stations_path}`")
    st.write(f"**CSV exists:** {_stations_path.exists()}")
    if st.button("Clear station cache"):
        _load_stations.clear()
        st.rerun()
    stations = _load_stations()
    st.write(f"**Stations loaded:** {len(stations)}")
    if all_listings:
        first = all_listings[0]
        addr = first.get("address") or ""
        hood = first.get("neighborhood") or ""
        coords = _resolve_coords(addr, hood)
        st.write(f"**First listing:** `{addr}` / `{hood}`")
        st.write(f"**Resolved coords:** {coords}")
        if coords:
            nearest = _nearest_stations(coords[0], coords[1], n=5)
            st.write(f"**Nearest stations ({len(nearest)}):**")
            for s in nearest:
                st.write(f"  - {s['name']} ({s['routes']}) @ {s['lat']:.5f}, {s['lon']:.5f}")

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
  var pd = window.parent.document;
  var pw = window.parent;

  var existing = pd.getElementById('apt-btt');
  if (existing) existing.remove();

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

  // Returns [scrollTop, scrollElement] for whichever container is actually scrolling
  function getScroll() {
    var candidates = [
      pd.querySelector('[data-testid="stAppViewContainer"]'),
      pd.querySelector('section.main'),
      pd.documentElement,
      pd.body
    ];
    for (var i = 0; i < candidates.length; i++) {
      if (candidates[i] && candidates[i].scrollTop > 0) {
        return [candidates[i].scrollTop, candidates[i]];
      }
    }
    return [pw.scrollY || pw.pageYOffset || 0, null];
  }

  btn.onclick = function() {
    var s = getScroll();
    if (s[1]) { s[1].scrollTo({top: 0, behavior: 'smooth'}); }
    else       { pw.scrollTo({top: 0, behavior: 'smooth'}); }
  };

  pd.body.appendChild(btn);

  // Poll every 250 ms — more reliable than cross-iframe scroll events
  setInterval(function() {
    btn.style.display = getScroll()[0] > 400 ? 'block' : 'none';
  }, 250);
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
    cursor: pointer;
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

  imgs.forEach(function(src) {{
    const img = document.createElement('img');
    img.src = src;
    img.onclick = function() {{ window.open(src, '_blank'); }};
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
                if date_listed:
                    meta_parts.append(f"listed {date_listed}")
                st.caption("  ·  ".join(meta_parts))

                st.markdown(
                    f'<p style="font-size:1.4rem;font-weight:700;margin:4px 0">'
                    f'{_fmt_price(price)}</p>',
                    unsafe_allow_html=True,
                )

                detail_line = _fmt_beds_baths_floor(beds, baths, floor_)
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
                _coords = _resolve_coords(listing.get("address") or "", hood or "")
                if _coords:
                    _lat, _lon = _coords
                    st_folium(
                        _listing_map(_lat, _lon),
                        use_container_width=True,
                        height=300,
                        returned_objects=[],
                        key=f"map_{listing.get('listing_id') or url[-12:]}",
                    )
                else:
                    st.caption("📍 No address available to map.")

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
