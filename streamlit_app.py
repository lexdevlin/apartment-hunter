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

import os
import re
from datetime import datetime

import streamlit.components.v1 as _components
import pandas as pd
import pydeck as pdk
import requests as _requests
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


@st.cache_data(ttl=300)
def load_listings() -> list[dict]:
    result = _get_client().table("listings").select("*").or_("delisted.is.null,delisted.eq.false").execute()
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
    return data


@st.cache_data(ttl=86400)
def _geocode(query: str) -> "tuple[float, float] | None":
    """Geocode an address string via Nominatim. Results cached for 24 h."""
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
        parts.append(f"floor {floor_}")
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
    ["Unreviewed", "Saved", "All active"],
    index=0,
)

priority_only = st.sidebar.checkbox("Priority only", value=False)

st.sidebar.markdown("---")

# Load data (needed to populate filter options)
all_listings = load_listings()

sources       = sorted({l["source"] for l in all_listings if l.get("source")})
neighborhoods = sorted({l["neighborhood"] for l in all_listings if l.get("neighborhood")})

selected_sources = st.sidebar.multiselect("Source", sources, default=sources)
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
        status = l.get("user_status")
        if status_view == "Unreviewed" and status is not None:
            continue
        if status_view == "Saved" and status != "saved":
            continue
        if priority_only and not l.get("is_priority"):
            continue
        if selected_sources and l.get("source") not in selected_sources:
            continue
        if selected_hoods and l.get("neighborhood") not in selected_hoods:
            continue
        out.append(l)
    return out


filtered = _apply_filters(all_listings)

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------


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
            '<div style="background:#1e1e1e;height:180px;border-radius:6px;'
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
    aspect-ratio: 1 / 1;
    height: auto;
    object-fit: cover;
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
}})();
</script>
""", height=280)


# ---------------------------------------------------------------------------
# Listing cards
# ---------------------------------------------------------------------------

if not filtered:
    st.info("No listings match the current filters.")
else:
    for listing in filtered:
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
                _loc = re.sub(r"\s*#\w+$", "", listing.get("address") or "").strip()
                _q = ""
                if _loc:
                    _q = _loc if re.search(r"\bNY\b|\bBrooklyn\b", _loc, re.IGNORECASE) \
                        else f"{_loc}, Brooklyn, NY"
                elif hood:
                    _q = f"{hood}, Brooklyn, NY"

                if _q:
                    _coords = _geocode(_q)
                    if _coords:
                        _lat, _lon = _coords
                        st.pydeck_chart(pdk.Deck(
                            initial_view_state=pdk.ViewState(
                                latitude=_lat, longitude=_lon,
                                zoom=15, pitch=0,
                            ),
                            layers=[pdk.Layer(
                                "ScatterplotLayer",
                                data=[{"lat": _lat, "lon": _lon}],
                                get_position="[lon, lat]",
                                get_fill_color=[30, 120, 220, 220],
                                get_radius=8,
                                radius_units="pixels",
                                radius_min_pixels=5,
                                radius_max_pixels=8,
                                pickable=False,
                            )],
                            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                            tooltip=False,
                        ), use_container_width=True, height=300)
                    else:
                        st.caption(f"📍 Could not locate: {_q}")
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
