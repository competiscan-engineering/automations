"""
mcp_server.py
─────────────────────────────────────────────────────────────────────────────
FastMCP server exposing Competiscan's RDS MySQL archive + PPT Builder.

Tools:
  DB:
    - get_product_pdf   → PDF location records for a list of entry_ids

  PPT Builder (localhost:5000/api/generate-ppt):
    - build_deck_default      → Default template deck
    - build_deck_sos          → SOS template deck
    - build_deck_chase        → Chase template deck

Run for Claude desktop (stdio):
  python mcp_server.py

Run for Amazon Q / remote clients (HTTP):
  python mcp_server.py --http
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

from ConnectToDB_VPN_utils import ssh_connect, ssh_disconnect, build_query
import pandas as pd
from io import StringIO
import config

PPT_API = "http://localhost:5001/api/generate-ppt"
# Deck rendering fetches a thumbnail per entry ID, so it can take a while.
# Override with the PPT_BUILDER_TIMEOUT env var (seconds).
PPT_TIMEOUT = int(os.environ.get("PPT_BUILDER_TIMEOUT", "300"))

mcp = FastMCP(
    name="competiscan-db",
    instructions=(
        "Tools to query the Competiscan direct mail archive and generate "
        "PowerPoint decks. Use get_product_pdf to retrieve campaign data. "
        "Use build_deck_default, build_deck_sos, or build_deck_chase to create "
        "presentation decks from that data."
    ),
)


# ─────────────────────────────────────────────────────────────────────────────
# DB TOOL
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_product_pdf(entry_ids: list[str]) -> list[dict[str, Any]]:
    """
    Retrieve product location records from the Competiscan MySQL archive.

    For each entry_id, returns:
      - entry_id         : the unique campaign entry identifier
      - product_id       : internal product ID
      - media_channel    : e.g. 'Direct Mail', 'Online Video'
      - audience         : e.g. 'Consumer', 'Business'
      - bucket_name      : S3 bucket where the PDF lives
      - document_path    : path inside the bucket
      - document_filename: PDF filename
      - companyName      : advertiser name
      - sectors          : pipe-separated sector names (e.g. 'Credit Cards||Banking')

    Args:
        entry_ids: List of entry ID strings, e.g. ["2026-05-13-4265", "2026-05-13-4181"]

    Returns:
        List of records as dicts. Empty list if no results found.
    """
    if not entry_ids:
        return []

    ssh = None
    try:
        ssh = ssh_connect()
        query = build_query(entry_ids)
        command = f'mysql -h {config.mysql_host} -u {config.mysql_user} -p{config.mysql_passwd} -e "{query}"'
        _, stdout, _ = ssh.exec_command(command)
        raw_output = stdout.read().decode("utf-8")

        if not raw_output.strip():
            return []

        df = pd.read_csv(StringIO(raw_output), sep="\t")
        if df.empty:
            return []

        df = df.where(df.notna(), other=None)
        return df.to_dict(orient="records")

    finally:
        if ssh:
            ssh_disconnect(ssh)


# ─────────────────────────────────────────────────────────────────────────────
# PPT BUILDER HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _post_deck(meta: dict, slides: list[dict]) -> dict[str, Any]:
    payload = {"meta": meta, "slides": slides}
    try:
        r = requests.post(PPT_API, json=payload, timeout=PPT_TIMEOUT)
        r.raise_for_status()

        content_type = r.headers.get("Content-Type", "")

        # pptbuilder returns the PPTX binary directly
        is_pptx = (
            "officedocument" in content_type
            or "octet-stream" in content_type
            or (r.content and r.content[:2] == b"PK")
        )
        if is_pptx:
            import os, time, base64
            # Save to disk
            out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ppt_outputs")
            os.makedirs(out_dir, exist_ok=True)
            # Use filename from Content-Disposition if available
            cd = r.headers.get("Content-Disposition", "")
            if "filename=" in cd:
                filename = cd.split("filename=")[-1].strip().strip('"')
            else:
                filename = f"deck_{int(time.time())}.pptx"
            filepath = os.path.join(out_dir, filename)
            with open(filepath, "wb") as f:
                f.write(r.content)
            # Base64-encode so Claude can present it as a downloadable artifact
            b64 = base64.b64encode(r.content).decode("utf-8")
            return {
                "status": "ok",
                "filename": filename,
                "filepath": filepath,
                "pptx_base64": b64,
            }

        # JSON response
        if r.text.strip():
            return r.json()

        return {"status": "ok", "message": "Deck generated. Check ppt_outputs folder."}

    except requests.exceptions.ConnectionError:
        return {"error": "PPT builder not reachable at localhost:5000. Is it running?"}
    except requests.exceptions.HTTPError:
        return {"error": f"PPT builder returned {r.status_code}: {r.text}"}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# PPT TOOL — DEFAULT TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def build_deck_default(
    deck_title: str,
    slides: list[dict[str, Any]],
    author: Optional[str] = "IA.ai",
) -> dict[str, Any]:
    """
    Build a PowerPoint deck using the Default Competiscan template.

    The slides parameter is an ordered list of slide objects. Each must have:
      - type : slide type string (see below)
      - data : dict of fields for that slide type

    Available slide types and their data fields:
      title               : { title, date? }
      agenda              : { sections: [str, ...] }
      newSection          : { title }
      needToKnow          : { title1, text1, title2, text2 }
      entry_ids           : { slideTitle, entryIds: [str,...], insight? }  1-5 IDs
      sixPieceEntryID     : { title, entryIds: [str,...], insight? }  exactly 6 or 8 IDs
      fourEntries2Insights: { mainTitle, entryIds1: [str,str], entryIds2: [str,str], title1?, insight1?, title2?, insight2? }
      table               : { title, subtitle, csv }
      map                 : { title, insight, csv }  csv has abbr + saturation columns
      clientJourney       : { title, insight, panelistId, items: [{topic, entryId},...], excelUrl? }
      digitalMarketing    : { title, insight, entryIds: [str,...] }  1-8 IDs
      brandImpressionsReport: { company, mediaChannel, entryIds: [str,...] }  1-50 IDs
      closing             : {}

    Example slides:
      [
        {"type": "title",      "data": {"title": "Q1 Competitive Scan", "date": "June 2026"}},
        {"type": "agenda",     "data": {"sections": ["Overview", "Key Findings"]}},
        {"type": "newSection", "data": {"title": "Overview"}},
        {"type": "entry_ids",  "data": {"slideTitle": "Top Campaigns", "entryIds": ["2026-05-13-4265"]}},
        {"type": "closing",    "data": {}}
      ]

    Args:
        deck_title : Title of the deck
        slides     : Ordered list of slide dicts with type and data keys
        author     : Author name (default: IA.ai)

    Returns:
        API response dict with download URL or status.
    """
    meta = {"deckTitle": deck_title, "author": author or "IA.ai", "theme": "classic", "template": "Default"}
    slide_list = [{"id": str(i + 1), "type": s["type"], "data": s.get("data", {})} for i, s in enumerate(slides)]
    return _post_deck(meta, slide_list)


# ─────────────────────────────────────────────────────────────────────────────
# PPT TOOL — SOS TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def build_deck_sos(
    deck_title: str,
    sections: list[str],
    include_sales_pack: Optional[bool] = True,
    extra_slides: Optional[list[dict[str, Any]]] = None,
    author: Optional[str] = "IA.ai",
) -> dict[str, Any]:
    """
    Build a PowerPoint deck using the Competiscan SOS template.

    Always generates: titleSOS → agenda → [sales pack] → [extra_slides] → closing.

    Sales pack (include_sales_pack=True) inserts: features, channels, roadmap, retrieval.

    Extra slides available (same data shapes as Default):
      newSectionSOS, capabilities, newsletter, entry_ids, sixPieceEntryID,
      fourEntries2Insights, table, clientJourney, digitalMarketing

    Args:
        deck_title         : Title of the deck
        sections           : Agenda section names
        include_sales_pack : Insert features/channels/roadmap/retrieval (default: True)
        extra_slides       : Additional slides before closing, list of {type, data} dicts
        author             : Author name

    Returns:
        API response dict with download URL or status.
    """
    meta = {"deckTitle": deck_title, "author": author or "IA.ai", "theme": "classic", "template": "SOS"}

    slides = [
        {"type": "titleSOS", "data": {}},
        {"type": "agenda",   "data": {"sections": sections}},
    ]
    if include_sales_pack:
        slides += [
            {"type": "features",  "data": {}},
            {"type": "channels",  "data": {}},
            {"type": "roadmap",   "data": {}},
            {"type": "retrieval", "data": {}},
        ]
    if extra_slides:
        slides += extra_slides
    slides.append({"type": "closing", "data": {}})

    slide_list = [{"id": str(i + 1), "type": s["type"], "data": s.get("data", {})} for i, s in enumerate(slides)]
    return _post_deck(meta, slide_list)


# ─────────────────────────────────────────────────────────────────────────────
# PPT TOOL — CHASE TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def build_deck_chase(
    deck_title: str,
    background_subtitle: str,
    insights: list[dict[str, str]],
    observation_bullets: Optional[str] = "American Express\nBank of America\nCapital One\nCiti\nDiscover\nWells Fargo",
    brand_slides: Optional[list[dict[str, Any]]] = None,
    conclusion_name: Optional[str] = None,
    conclusion_title: Optional[str] = None,
    conclusion_email: Optional[str] = None,
    extra_slides: Optional[list[dict[str, Any]]] = None,
    author: Optional[str] = "IA.ai",
    date: Optional[str] = None,
) -> dict[str, Any]:
    """
    Build a PowerPoint deck using the Chase template (13.333 x 7.5 in canvas).

    Structure: titleCHASE → backgroundCHASE → observationsCHASE →
               [brand_slides (entryIDCHASE)] → [extra_slides] → conclusionCHASE

    insights format: list of {"title": "Brand Name", "text": "Finding."} — 1 to 4 items.

    brand_slides format: list of {"slideTitle": "Amex", "insight": "...", "entryIds": ["111","222"]}
      Each becomes an entryIDCHASE slide with AI-generated captions per image card.

    extra_slides: any Default slide types appended before conclusion.

    Args:
        deck_title           : Title of the deck
        background_subtitle  : The request brief shown in the blue band
        insights             : 1-4 key findings with title + text
        observation_bullets  : Newline-separated brand names
        brand_slides         : Per-brand entryIDCHASE slides
        conclusion_name      : Presenter name
        conclusion_title     : Presenter job title
        conclusion_email     : Presenter email
        extra_slides         : Additional Default slides before conclusion
        author               : Author name
        date                 : Date shown on title slide

    Returns:
        API response dict with download URL or status.
    """
    meta = {"deckTitle": deck_title, "author": author or "IA.ai", "theme": "classic", "template": "Chase"}

    title_data = {"title": deck_title}
    if date:
        title_data["date"] = date

    # Normalize insights: Claude may send {brand/insight} or {title/text} — API needs {title, text}
    normalized_insights = [
        {
            "title": ins.get("title") or ins.get("brand") or "",
            "text":  ins.get("text")  or ins.get("insight") or "",
        }
        for ins in insights
    ]

    slides = [
        {"type": "titleCHASE", "data": title_data},
        {"type": "backgroundCHASE", "data": {
            "slideTitle": "BACKGROUND & KEY FINDINGS",
            "subtitle": background_subtitle,
            "insights": normalized_insights,
        }},
        {"type": "observationsCHASE", "data": {
            "slideTitle": "Marketing Observations",
            "bullets": observation_bullets or "",
        }},
    ]

    if brand_slides:
        for b in brand_slides:
            # Normalize: Claude may send {brand, entry_ids} — API needs {slideTitle, entryIds}
            slides.append({"type": "entryIDCHASE", "data": {
                "slideTitle": b.get("slideTitle") or b.get("brand") or "",
                "insight":    b.get("insight", ""),
                "entryIds":   b.get("entryIds") or b.get("entry_ids") or [],
            }})

    if extra_slides:
        slides += extra_slides

    conclusion_data = {}
    if conclusion_name:  conclusion_data["name"]     = conclusion_name
    if conclusion_title: conclusion_data["jobTitle"]  = conclusion_title
    if conclusion_email: conclusion_data["email"]     = conclusion_email
    slides.append({"type": "conclusionCHASE", "data": conclusion_data})

    slide_list = [{"id": str(i + 1), "type": s["type"], "data": s.get("data", {})} for i, s in enumerate(slides)]
    return _post_deck(meta, slide_list)


# ─────────────────────────────────────────────────────────────────────────────
# FILE TOOL — present a saved PPTX to the user
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_deck_file(filepath: str) -> dict[str, Any]:
    """
    Read a saved PPTX file from disk and return it as base64 so the caller
    can present it as a downloadable file.

    Args:
        filepath: Absolute path to the .pptx file returned by a build_deck_* tool.

    Returns:
        Dict with filename, filepath, and pptx_base64 string.
    """
    import os, base64
    if not os.path.isfile(filepath):
        return {"error": f"File not found: {filepath}"}
    with open(filepath, "rb") as f:
        data = f.read()
    return {
        "status": "ok",
        "filename": os.path.basename(filepath),
        "filepath": filepath,
        "pptx_base64": base64.b64encode(data).decode("utf-8"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--http" in sys.argv:
        mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
    else:
        mcp.run()