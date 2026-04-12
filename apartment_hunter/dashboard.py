"""
Dashboard generator.

Produces a self-contained HTML page (Bootstrap + DataTables via CDN)
showing all unreviewed apartment listings, with priority listings
highlighted at the top. Uploads the result to Azure Blob Storage.

The dashboard is read-only — "reviewed" is managed in the OneDrive Excel.
Each run re-generates the page from the current state of the spreadsheet.
"""

import os
from datetime import datetime

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings

SOURCE_LABELS = {
    "streeteasy":    ("StreetEasy",    "#0080C6"),
    "craigslist":    ("Craigslist",    "#9B1D20"),
    "zillow":        ("Zillow",        "#006AFF"),
    "apartments_com": ("Apartments.com", "#FF6D00"),
}


def generate(df: pd.DataFrame, config: dict) -> str:
    """
    Build the HTML dashboard string and write it to data/apartment_dashboard.html.
    Returns the file path.
    """
    os.makedirs("data", exist_ok=True)

    # Work with a copy; coerce reviewed column to bool
    df = df.copy()
    df["reviewed"] = df["reviewed"].apply(_is_truthy)
    df["is_priority"] = df["is_priority"].apply(_is_truthy)

    unreviewed = df[~df["reviewed"]].copy()

    # Sort: priority first, then newest date_found first
    unreviewed["_date_found_sort"] = pd.to_datetime(
        unreviewed["date_found"], errors="coerce"
    ).fillna(pd.Timestamp.min)
    unreviewed = unreviewed.sort_values(
        ["is_priority", "_date_found_sort"],
        ascending=[False, False],
    ).drop(columns=["_date_found_sort"])

    n_unreviewed  = len(unreviewed)
    n_priority    = int(unreviewed["is_priority"].sum())
    n_total       = len(df)
    updated_at    = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    rows_html = "\n".join(_render_row(row) for _, row in unreviewed.iterrows())

    html = _HTML_TEMPLATE.format(
        updated_at=updated_at,
        n_unreviewed=n_unreviewed,
        n_priority=n_priority,
        n_total=n_total,
        rows=rows_html,
    )

    out_path = "data/apartment_dashboard.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  [Dashboard] wrote {out_path} ({n_unreviewed} unreviewed, {n_priority} priority)")
    return out_path


def upload_to_azure(config: dict) -> None:
    """Upload the dashboard HTML to Azure Blob Storage."""
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        print("  [Dashboard] AZURE_STORAGE_CONNECTION_STRING not set — skipping upload")
        return

    container = config.get("azure", {}).get("container", "$web")
    blob_name  = config.get("azure", {}).get("blob_name", "apartment_dashboard.html")
    local_path = "data/apartment_dashboard.html"

    client = BlobServiceClient.from_connection_string(conn_str)
    with open(local_path, "rb") as f:
        client.get_blob_client(container, blob_name).upload_blob(
            f,
            overwrite=True,
            content_settings=ContentSettings(content_type="text/html; charset=utf-8"),
        )
    print(f"  [Dashboard] uploaded to Azure Blob '{container}/{blob_name}'")


# ---------------------------------------------------------------------------
# Row rendering
# ---------------------------------------------------------------------------

def _render_row(row: pd.Series) -> str:
    priority     = _is_truthy(row.get("is_priority"))
    source       = str(row.get("source") or "")
    label, color = SOURCE_LABELS.get(source, (source.title(), "#6c757d"))
    price        = _fmt_price(row.get("price"))
    neighborhood = _safe(row.get("neighborhood"))
    address      = _safe(row.get("address"))
    floor_       = _safe(row.get("floor"))
    beds         = _safe_int(row.get("bedrooms"))
    baths        = _safe_float(row.get("bathrooms"))
    rent_stab    = _fmt_bool(row.get("rent_stabilized"))
    nearest_sub  = _safe(row.get("nearest_subway"))
    date_listed  = _fmt_date(row.get("date_listed"))
    date_found   = _fmt_date(row.get("date_found"))
    url          = _safe(row.get("url"))
    title        = _safe(row.get("title")) or "—"
    row_class    = "priority-row" if priority else ""
    priority_icon = "★" if priority else ""

    beds_baths = "/".join(filter(None, [
        f"{beds}bd" if beds else None,
        f"{baths}ba" if baths else None,
    ])) or "—"

    link_html = (
        f'<a href="{url}" target="_blank" rel="noopener noreferrer">View ↗</a>'
        if url else "—"
    )
    source_badge = (
        f'<span class="badge" style="background:{color}">{label}</span>'
    )

    return f"""
    <tr class="{row_class}">
      <td class="text-center">{priority_icon}</td>
      <td>{source_badge}</td>
      <td>{price}</td>
      <td>{neighborhood}</td>
      <td>{address}</td>
      <td class="text-center">{beds_baths}</td>
      <td class="text-center">{floor_}</td>
      <td class="text-center">{rent_stab}</td>
      <td class="subway-cell">{nearest_sub}</td>
      <td>{date_listed}</td>
      <td>{date_found}</td>
      <td>{link_html}</td>
    </tr>"""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _is_truthy(v) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() in ("true", "1", "yes", "y")


def _safe(v) -> str:
    if v is None or (isinstance(v, float) and v != v):  # NaN check
        return "—"
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "none", "") else "—"


def _safe_int(v) -> str:
    try:
        return str(int(float(v))) if v is not None and str(v).strip() not in ("", "nan", "None") else ""
    except (ValueError, TypeError):
        return ""


def _safe_float(v) -> str:
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else str(round(f, 1))
    except (ValueError, TypeError):
        return ""


def _fmt_price(v) -> str:
    try:
        return f"${int(float(v)):,}"
    except (ValueError, TypeError):
        return "—"


def _fmt_date(v) -> str:
    if v is None or str(v).strip() in ("", "nan", "None", "NaT"):
        return "—"
    try:
        dt = pd.to_datetime(v)
        return dt.strftime("%b %d, %Y")
    except Exception:
        return str(v)[:10]


def _fmt_bool(v) -> str:
    if v is None or str(v).strip() in ("", "nan", "None"):
        return "?"
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return "✓"
    if s in ("false", "0", "no"):
        return "✗"
    return "?"


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" data-bs-theme="auto">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Apartment Listings</title>

  <!-- Bootstrap 5 -->
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    crossorigin="anonymous">
  <!-- DataTables + Bootstrap 5 integration -->
  <link rel="stylesheet"
    href="https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css">

  <style>
    body {{ font-family: system-ui, -apple-system, sans-serif; font-size: 0.875rem; }}
    .priority-row {{ background-color: #fff8e1 !important; }}
    [data-bs-theme=dark] .priority-row {{ background-color: #3d3200 !important; }}
    th {{ white-space: nowrap; }}
    td {{ vertical-align: middle; }}
    .badge {{ font-size: 0.75rem; padding: 0.25em 0.5em; border-radius: 4px; color: #fff; }}
    .subway-cell {{ font-size: 0.8rem; min-width: 220px; }}
    a {{ text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .stat-card {{ border-radius: 8px; padding: 12px 20px; background: var(--bs-secondary-bg); }}
  </style>
</head>
<body>
<div class="container-fluid py-4 px-4">

  <!-- Header -->
  <div class="d-flex align-items-baseline gap-3 mb-3 flex-wrap">
    <h4 class="mb-0">Apartment Listings</h4>
    <small class="text-muted">Last updated: {updated_at}</small>
  </div>

  <!-- Stats -->
  <div class="d-flex gap-3 mb-4 flex-wrap">
    <div class="stat-card">
      <div class="fw-semibold fs-4">{n_unreviewed}</div>
      <div class="text-muted small">Unreviewed</div>
    </div>
    <div class="stat-card">
      <div class="fw-semibold fs-4" style="color:#b8860b">★ {n_priority}</div>
      <div class="text-muted small">Priority</div>
    </div>
    <div class="stat-card">
      <div class="fw-semibold fs-4">{n_total}</div>
      <div class="text-muted small">Total seen</div>
    </div>
  </div>

  <p class="text-muted small mb-3">
    Mark listings as <strong>reviewed = TRUE</strong> in the OneDrive Excel file.
    They will disappear from this view on the next scraper run.
  </p>

  <!-- Table -->
  <div class="table-responsive">
    <table id="listings" class="table table-striped table-hover table-sm" style="width:100%">
      <thead class="table-dark">
        <tr>
          <th title="Priority match">★</th>
          <th>Source</th>
          <th>Price</th>
          <th>Neighborhood</th>
          <th>Address</th>
          <th>Beds/Baths</th>
          <th>Floor</th>
          <th title="Rent Stabilized">Stab.</th>
          <th>Subway</th>
          <th>Listed</th>
          <th>Found</th>
          <th>Link</th>
        </tr>
      </thead>
      <tbody>
{rows}
      </tbody>
    </table>
  </div>
</div>

<!-- Scripts -->
<script src="https://code.jquery.com/jquery-3.7.1.min.js" crossorigin="anonymous"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"
  crossorigin="anonymous"></script>

<script>
  // Auto dark mode
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  document.documentElement.setAttribute('data-bs-theme', prefersDark ? 'dark' : 'light');

  $(document).ready(function () {{
    $('#listings').DataTable({{
      pageLength: 50,
      order: [[0, 'desc'], [9, 'desc']],  // priority first, then date_found desc
      columnDefs: [
        {{ targets: [0, 5, 6, 7], className: 'text-center' }},
        {{ targets: 11, orderable: false }},  // Link column
      ],
      language: {{
        search: "Filter:",
        lengthMenu: "Show _MENU_ listings",
        info: "Showing _START_–_END_ of _TOTAL_ unreviewed listings",
      }}
    }});
  }});
</script>
</body>
</html>
"""
