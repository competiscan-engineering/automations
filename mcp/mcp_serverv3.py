"""
mcp_serverv3.py

CURRENTLY RUNNING IN PRODUCTION mcp.competiscan.com/mcp

─────────────────────────────────────────────────────────────────────────────
FastMCP server — Competiscan archive (minimal 3-tool build).

Tools:
  search_archive    → Primary discovery. Retrieve products + their OCR text by
                      company / sector / channel / audience / keyword (REST).
  search_by_date    → Entries for a company within a date range (SQL).
  get_product_pdf   → Fallback. Full metadata + PDF URL for specific entry_ids
                      (SQL), for when the OCR text is not enough and the answer
                      is in the visual layer.

Run (stdio):       python mcp_serverv3.py
Run (HTTP):        python mcp_serverv3.py --http
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import requests
from fastmcp import FastMCP
from typing import Any, Optional

# This module lives in mcp/; ConnectToDB_VPN_utils and config live in the
# project root. Add the root to sys.path so they resolve regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ConnectToDB_VPN_utils import (
    ssh_connect, ssh_disconnect,
    build_query,
)
import pandas as pd
from io import StringIO
import config

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DATAGRID_URL = "https://api2.competiscan.com/product-admin-datagrid-service/v1/tempproduct"

# Media channel name → datagrid enum
CHANNEL_MAP = {
    "Direct Mail":           "DIRECT_MAIL",
    "Email":                 "EMAIL",
    "Online Display":        "ONLINE_DISPLAY",
    "Online Video":          "ONLINE_VIDEO",
    "Print":                 "PRINT",
    "Search Engine Marketing": "SEARCH_ENGINE_MARKETING",
    "Social Media":          "SOCIAL_MEDIA",
    "UX - Desktop":          "UX_DESKTOP",
    "UX - Mobile":           "UX_MOBILE",
    "Website/URL":           "WEBSITE_URL",
}

# Sector name → sector ID
SECTOR_MAP = {
    "Automotive":            372,
    "Banking":               87,
    "Consumer Services":     559,
    "Credit Cards":          90,
    "Energy":                315,
    "HR/Payroll/HCM/PEO":   530,
    "Insurance":             4,
    "Investments/Annuities": 5,
    "Mortgage & Loan":       6,
    "Non-Profit":            560,
    "Real Estate":           522,
    "Retail":                266,
    "Shipping":              525,
    "Technology":            1578,
    "Telecom":               9,
    "Travel & Leisure":      219,
}

# Audience name → panel ID
AUDIENCE_MAP = {
    "Consumer":                          1,
    "Employer/Business Owner":           2,
    "Insurance Producer/Financial Advisor": 4,
    "Mortgage Broker":                   6,
    "Provider":                          5,
}

# Default allowed sectors (all)
ALL_SECTORS = list(SECTOR_MAP.values()) + [
    220, 224, 226, 260, 225, 499, 3721,
    1524,1525,1526,1527,1532,1533,1547,
    1545,1546,1552,1578,1581,1582,1580,1583,1579
]

ARCHIVE_MAP = {
    "Competiscan Archive": "APPROVED",
    "Glacier Archive":    "GLACIER",
}

mcp = FastMCP(
    name="competiscan-db",
    instructions=(
        "Query the Competiscan direct marketing archive (20+ years, 420k+ campaigns). "
        "Three tools cover the full research workflow:\n"
        "\n"
        "1. search_archive — THE main tool. Retrieve products and their OCR text by any mix of "
        "company names, sectors, media channels, audience, and OCR keyword. Returns OCR snippets "
        "plus an entry_id per match. Use it for company research, competitive comparisons, "
        "sector/channel/audience analysis, and keyword discovery. For most questions the OCR text "
        "it returns is all you need. It searches the Competiscan Archive by default — always use the "
        "default; the Glacier Archive holds uncategorized entries and is rarely relevant to "
        "competitive research, so do not use it unless the user explicitly asks.\n"
        "\n"
        "2. search_by_date — retrieve a specific company's entries within a date range. Use it when "
        "the question is scoped to a time window (quarterly analysis, recent activity, historical "
        "comparisons). entry_id is YYYY-MM-DD-NNNN, so date filtering is precise.\n"
        "\n"
        "3. get_product_pdf — a FALLBACK, only when the OCR text from search_archive is not enough, "
        "typically because the answer is visual (icons, logos, imagery, star ratings, charts, layout) "
        "and not captured by OCR. Pass specific entry_ids to get their metadata and PDF URL. Do not "
        "call it for every result — pull the PDF only for the entries where the answer is visual.\n"
        "\n"
        "Typical flow: search_archive to find and read campaigns via OCR (optionally scoped with "
        "search_by_date), then get_product_pdf only for the specific entries whose answer lives in "
        "the PDF's visual layer."
    ),
)


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _run_sql(query: str) -> pd.DataFrame:
    """Execute a SQL query via SSH tunnel and return a DataFrame."""
    ssh = ssh_connect()
    try:
        command = f'mysql -h {config.mysql_host} -u {config.mysql_user} -p{config.mysql_passwd} -e "{query}"'
        _, stdout, _ = ssh.exec_command(command)
        raw = stdout.read().decode("utf-8")
        if not raw.strip():
            return pd.DataFrame()
        df = pd.read_csv(StringIO(raw), sep="\t")
        return df.where(df.notna(), other=None)
    finally:
        ssh_disconnect(ssh)


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return df.to_dict(orient="records")


def _get_company_ids(company_names: list[str]) -> dict[str, int]:
    """Resolve a list of company names to their DB company IDs."""
    if not company_names:
        return {}
    names_str = ", ".join(f"'{n.replace(chr(39), chr(39)*2)}'" for n in company_names)
    query = f"""
    SELECT companyID, companyName
    FROM csv2_master_lookup.cscan_company
    WHERE companyName IN ({names_str});
    """
    df = _run_sql(query)
    if df.empty:
        return {}
    return dict(zip(df["companyName"].tolist(), df["companyID"].tolist()))


def _datagrid_search(
    company_ids: list[int] = None,
    sector_ids: list[int] = None,
    channel_enums: list[str] = None,
    keyword: str = "",
    limit: int = 10,
    page: int = 1,
    archive: str = "Competiscan Archive",
) -> list[dict[str, Any]]:
    """Call the datagrid REST endpoint and return cleaned records.

    How filters work (confirmed from ES query inspection):
    - allowed_sectors → becomes ES filter on sector_id (sector filter)
    - media_channels  → becomes ES must on mchanne_id (channel filter)
    - companies       → becomes ES must on competi_id (company filter)
    - ocr             → becomes ES full-text search on dts_val
    - panelist field  → does NOT filter by audience, omitted
    """
    # Use provided sector IDs or fall back to all sectors
    active_sectors = sector_ids if sector_ids else ALL_SECTORS

    payload = {
        "request": {
            "page": page,
            "perpage": min(limit, 200),
            "totproducts": "",
            "current_page_start_date": "",
            "current_page_start_muid": "",
            "current_page_start_productid": "",
        },
        "search": {
            "product_id": "",
            "product_name": "",
            "companies": [str(c) for c in (company_ids or [])],
            "states": "",
            "users": "",
            "country": "all",
            "ocr": keyword or "",
            "product_status": [ARCHIVE_MAP.get(archive, "APPROVED")],
            "sectors": [],
            "media_channels": channel_enums or [],
            "sorting": [{"field": "modify_date", "direction": True}],
            "paging_obj": {},
            "muid": "",
            "entry_id": "",
            "panelist": "",
            "dmtmsource": "",
            "allowed_sectors": active_sectors,
            "annotations": [],
        },
    }
    try:
        r = requests.post(DATAGRID_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return [{"error": str(e)}]

    products = data.get("payload", {}).get("payload", [])
    extra    = data.get("payload", {}).get("extra_data", {})

    results = []
    for p in products:
        pid  = str(p.get("product_id", ""))
        ext  = extra.get(pid, {})
        results.append({
            "entry_id":        p.get("entry_id"),
            "product_id":      p.get("product_id"),
            "product_headline": p.get("product_headline"),
            "media_channel":   p.get("mChannelName"),
            "audience":        ext.get("audience") or p.get("mpanel_id"),
            "company_name":    ext.get("company_name") or p.get("product_name"),
            "sectors":         ext.get("sector_name", []),
            "approved_date":   p.get("approved_date"),
            "ocr_text":        p.get("dts_val", "")[:500],  # first 500 chars
            "document_exists": ext.get("document_exist", 0),
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1 — get_product_pdf
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_product_pdf(entry_ids: list[str]) -> list[dict[str, Any]]:
    """
    Retrieve full product location records from the Competiscan MySQL archive.

    Returns PDF location data (bucket, path, filename) plus company, channel,
    audience, and sector metadata for each entry_id.

    Args:
        entry_ids: List of entry ID strings, e.g. ["2026-05-13-4265", "2026-05-13-4181"]

    Returns:
        List of records with entry_id, product_id, media_channel, audience,
        bucket_name, document_path, document_filename, companyName, sectors, pdf_url.
    """
    if not entry_ids:
        return []
    query = build_query(entry_ids)
    records = _df_to_records(_run_sql(query))

    # Attach the PDF URL for each product so callers can fetch / display it
    for record in records:
        pid = record.get("product_id")
        if pid:
            record["pdf_url"] = f"https://www.competiscan.com/productDocuments.php?id={pid}"

    return records


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 2 — search_by_date
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def search_by_date(
    company_name: str,
    start_date: str,
    end_date: str,
    media_channel: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Retrieve entries for a company within a date range.

    entry_id format is YYYY-MM-DD-NNNN so date filtering works on it directly.

    Args:
        company_name  : Exact company name, e.g. "Visa"
        start_date    : Start date as YYYY-MM-DD, e.g. "2026-01-01"
        end_date      : End date as YYYY-MM-DD, e.g. "2026-03-31"
        media_channel : Optional channel filter
        limit         : Max entries to return (default 20, max 100)

    Returns:
        List of records within the date range, most recent first.
    """
    limit = min(int(limit), 100)
    channel_filter = f"AND mc.mChannelName = '{media_channel}'" if media_channel else ""

    query = f"""
    SELECT
        p.entry_id,
        p.product_id,
        mc.mChannelName AS media_channel,
        mp.mPanelName   AS audience,
        d.bucket_name,
        d.document_path,
        d.document_filename,
        co.companyName,
        (
            SELECT GROUP_CONCAT(DISTINCT s.sectorName ORDER BY s.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm
            JOIN csv2_master_lookup.cscan_sector s ON s.sectorID = psm.sector_id
            WHERE psm.product_id = p.product_id AND s.parentID = 0
        ) AS sectors
    FROM csv2_product.cscan_document d
    JOIN csv2_product.cscan_product p ON p.product_id = d.productID
    JOIN csv2_product.cscan_product_company_mapping cmap ON cmap.product_id = p.product_id
    JOIN csv2_master_lookup.cscan_company co ON co.companyID = cmap.company_id
    JOIN csv2_master_lookup.cscan_mchannel mc ON mc.mChannelID = p.mchanne_id
    JOIN csv2_master_lookup.cscan_mpanel mp ON mp.mPanelID = p.mpanel_id
    WHERE co.companyName = '{company_name}'
    AND p.entry_id >= '{start_date}'
    AND p.entry_id <= '{end_date}-9999'
    AND cmap.default_img = 1
    {channel_filter}
    ORDER BY p.entry_id DESC
    LIMIT {limit};
    """
    return _df_to_records(_run_sql(query.strip()))


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 3 — search_archive (REST — no SSH needed)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def search_archive(
    company_names: Optional[list[str]] = None,
    sectors: Optional[list[str]] = None,
    media_channels: Optional[list[str]] = None,
    audience: Optional[str] = None,
    keyword: Optional[str] = None,
    limit: int = 10,
    archive: str = "Competiscan Archive",
) -> list[dict[str, Any]]:
    """
    Full PowerSearch replacement — search the archive by any combination of
    company, sector, channel, audience, and OCR keyword.

    This tool hits the REST API directly (no SSH tunnel) and returns OCR text
    snippets so Claude can reason about campaign content without fetching PDFs.

    Companies    : exact names, e.g. ["Visa", "Mastercard"]
    Sectors      : Automotive, Banking, Credit Cards, Insurance, Retail, Telecom, etc.
    Channels     : Direct Mail, Email, Social Media, Online Display, Online Video,
                   Print, Search Engine Marketing, UX - Desktop, UX - Mobile, Website/URL
    Audience     : Consumer, Employer/Business Owner,
                   Insurance Producer/Financial Advisor, Mortgage Broker, Provider
    Keyword      : OCR full-text search with AND/OR/NOT, e.g. '"travel rewards" and "5x points"'
    Archive      : "Competiscan Archive" (recently approved, default) or
                   "Glacier Archive" (older/cold-storage campaigns)

    Args:
        company_names  : List of company names to filter by
        sectors        : List of sector names to filter by
        media_channels : List of channel names to filter by
        audience       : Single audience filter
        keyword        : OCR search string
        limit          : Max results (default 10, max 50)
        archive        : Which archive to search — "Competiscan Archive" or "Glacier Archive"

    Returns:
        List of records with entry_id, headline, company, channel, sectors,
        audience, approved_date, and ocr_text snippet (first 500 chars).
    """
    # Resolve company names → IDs via SQL
    company_ids = []
    if company_names:
        id_map = _get_company_ids(company_names)
        company_ids = list(id_map.values())

    # Map sector names → IDs
    sector_ids = []
    if sectors:
        for s in sectors:
            sid = SECTOR_MAP.get(s)
            if sid:
                sector_ids.append(sid)

    # Map channel names → enums
    channel_enums = []
    if media_channels:
        for c in media_channels:
            ce = CHANNEL_MAP.get(c)
            if ce:
                channel_enums.append(ce)

    # Map audience → ID
    audience_ids = []
    if audience:
        aid = AUDIENCE_MAP.get(audience)
        if aid:
            audience_ids = [aid]

    return _datagrid_search(
        company_ids=company_ids,
        sector_ids=sector_ids,
        channel_enums=channel_enums,
        keyword=keyword or "",
        limit=limit,
        archive=archive,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--http" in sys.argv:
        mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
    else:
        mcp.run()