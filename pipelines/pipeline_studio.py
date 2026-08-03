#!/usr/bin/env python3
"""
report_studio.py — a node-based, low-code builder for Competiscan trend-report pipelines
═══════════════════════════════════════════════════════════════════════════════════════

WHAT IT IS
    A local web app (no pip install, stdlib only) where a researcher lays out a report
    as a node graph, tests it against the real archive, and exports a readable Python
    pipeline in the same house style as report_HarborstoneWeekly.py — which they send to
    Engineering to deploy and schedule.

THE ONE ARCHITECTURAL RULE
    "Test" does NOT run a separate interpreter. It generates the .py and runs THAT file
    in a subprocess. There is exactly one execution path, so what a researcher tests is
    byte-identical to what gets deployed. No drift, ever.

RUN
    python report_studio.py                 # then open http://127.0.0.1:8787
    python report_studio.py --port 9000
    python report_studio.py --selftest      # codegen every template + ast-parse it

WHERE THINGS GO
    Generated pipelines  ->  <project_root>/pipelines/generated/
    Saved graphs (JSON)  ->  <project_root>/pipelines/generated/_graphs/
    Generated files are written INTO pipelines/ so `import pipelines.report_lib` resolves
    exactly the way the hand-written pipelines' sys.path shim expects.

WHAT IT DELIBERATELY DOES NOT DO
    It does not invent bespoke logic. The three reference pipelines each needed one-off
    code (Harborstone's channel-tier priors and Auto/Home sub-category split;
    SupplyHouse's hd_pro/hd_consumer subtraction). Those are NOT expressible as nodes and
    should not be faked. Researchers describe them in an "Engineer note" node, which the
    generator lifts verbatim into the exported file's docstring as a TODO for Engineering.
    A generated pipeline is a correct, runnable, house-style DRAFT — not a replacement for
    review on reports with real bespoke requirements.

THE GUARDRAILS IT ENCODES (the actual point of the tool)
    Each of these is a rule that was learned the hard way in an existing pipeline and that
    a researcher building report #47 would otherwise silently re-break:
      1. Searches run SEQUENTIALLY. The REST backend cross-contaminates results under
         concurrent calls with different channels. Not a toggle.
      2. Three different date fields exist (entry_id date / approved_date /
         added_to_database). The graph must state which one bounds the window.
      3. A search that hits its result cap yields "at least N", never an exact count.
         Cap-hit AND zero-in-window is flagged SUSPECT, not reported as a true zero.
      4. Sector alone is too coarse; sub-category filtering requires SQL enrichment.
      5. Excel sheets require SQL enrichment — raw search records lack the columns.
      6. Deck slides hold at most 5 entries; more must chunk with a "(cont.)" title.
      7. Callouts are trimmed to whole sentences under the builder's char limit.
      8. Deterministic work (counts, dedup, chunking) stays in Python. The LLM only
         picks entry_ids and writes prose.
      9. Email is opt-in via an environment variable and addresses a NAMED ENV VAR, never
         a literal recipient baked into a graph.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ═══════════════════════════════════════════════════════════════════════════════════════
# Paths — find the project root by locating report_lib.py
# ═══════════════════════════════════════════════════════════════════════════════════════

STUDIO_FILE = Path(__file__).resolve()


def _find_pipelines_dir() -> Path | None:
    """Locate the pipelines/ dir containing report_lib.py, searching outward from here."""
    candidates = [
        STUDIO_FILE.parent,
        STUDIO_FILE.parent / "pipelines",
        STUDIO_FILE.parent.parent / "pipelines",
        Path.cwd(),
        Path.cwd() / "pipelines",
    ]
    for c in candidates:
        if (c / "report_lib.py").is_file():
            return c.resolve()
    return None


PIPELINES_DIR = _find_pipelines_dir()
GENERATED_DIR = (PIPELINES_DIR / "generated") if PIPELINES_DIR else (STUDIO_FILE.parent / "generated")
GRAPHS_DIR = GENERATED_DIR / "_graphs"

# ═══════════════════════════════════════════════════════════════════════════════════════
# Vocabulary — dropdown options mirroring what the archive actually accepts
# ═══════════════════════════════════════════════════════════════════════════════════════

CHANNELS = ["Direct Mail", "Email", "Online Display", "Online Video", "Print",
            "Search Engine Marketing", "Social Media", "Website/URL"]

SECTORS = ["Banking", "Credit Cards", "Insurance", "Mortgage & Loan", "Retail",
           "Automotive", "Telecom", "Investment", "Healthcare"]

AUDIENCES = ["", "Consumer", "Employer/Business Owner",
             "Insurance Producer/Financial Advisor", "Mortgage Broker", "Provider"]

# The three date fields. Naming them explicitly in the UI is guardrail #2.
WINDOW_FIELDS = [
    ("entry_id", "entry_id date — when the piece was mailed/captured (deck default)"),
    ("added_to_database", "added_to_database — when it entered the archive (Excel default)"),
    ("approved_date", "approved_date — when it was approved for PowerSearch"),
]

SLIDE_TYPES = ["title", "agenda", "needToKnow", "newSection", "entry_ids", "table", "closing"]

HEADER_PRESETS = {
    "banking_19": [
        "Primary Company", "Additional Companies", "Primary Sector", "Primary Category",
        "Primary Sub Category", "EntryID", "Quarter", "Headline", "Product", "PDF Content",
        "Media Channel", "Market", "State/Province", "Age", "Income", "Mailing Type",
        "Publication", "Network Name", "Social Media Ad Type",
    ],
    "lending_21": [
        "Primary Company", "Additional Companies", "Primary Sector", "Primary Category",
        "Primary Sub Category", "EntryID", "Quarter", "Headline", "Product", "PDF Content",
        "Media Channel", "Market", "State/Province", "Age", "Income", "Mailing Type",
        "Pre-Screen", "Mortgage & Loan - Application Type",
        "Publication", "Network Name", "Social Media Ad Type",
    ],
    "retail_10": [
        "Primary Company", "Additional Companies", "Primary Sector", "Primary Category",
        "Primary Sub Category", "Primary Sub Sub Category", "EntryID", "Headline",
        "Product", "PDF Content",
    ],
}

# Filter presets — every one of these was extracted from a shipped pipeline.
FILTER_PRESETS = {
    "cu_only": "Credit unions only (company name matches Credit Union / FCU / CU)",
    "name_regex": "Company-name include/exclude regex",
    "subcategory": "Sub-category include/exclude keywords (REQUIRES SQL enrichment)",
    "creative_dedupe": "Collapse repeated creative (max N per theme)",
}

# ═══════════════════════════════════════════════════════════════════════════════════════
# NODE SPECS — the 12 node types. Field metadata drives the whole inspector UI.
# ═══════════════════════════════════════════════════════════════════════════════════════

def _f(key, label, kind, default=None, **kw):
    d = {"key": key, "label": label, "kind": kind, "default": default}
    d.update(kw)
    return d


NODE_SPECS: dict[str, dict] = {
    "period": {
        "label": "Period", "color": "#6b7fd7", "max": 1, "inputs": 0, "outputs": 1,
        "blurb": "The reporting window. Every search is bounded by this.",
        "fields": [
            _f("client", "Client / report name", "text", "Harborstone"),
            _f("kind", "Cadence", "select", "week", options=["week", "month"]),
            _f("anchor", "Window", "select", "prior_complete",
               options=["prior_complete", "rolling"],
               help="prior_complete = last finished week/month (recommended, reproducible). "
                    "rolling = the last 7/30 days ending today."),
            _f("window_field", "Window is bounded by", "select", "entry_id",
               options=[k for k, _ in WINDOW_FIELDS],
               help="GUARDRAIL: three different date fields exist and they disagree. "
                    "Pick deliberately."),
        ],
    },
    "search": {
        "label": "Search", "color": "#3f8f5f", "inputs": 1, "outputs": 1,
        "blurb": "One archive query. Fans out one call per channel, sequentially.",
        "fields": [
            _f("group_key", "Group key (code identifier)", "text", "membership"),
            _f("title", "Display title (slide/sheet label)", "text", "Membership Acquisition"),
            _f("company_names", "Company names (one per line, blank = any)", "textarea", ""),
            _f("sectors", "Sectors", "multiselect", ["Banking"], options=SECTORS),
            _f("channels", "Media channels", "multiselect", ["Email"], options=CHANNELS),
            _f("keyword", "OCR keyword query", "textarea", "",
               help='Boolean-ish, e.g. "join" or "new member" not "join us". '
                    'NOTE: the backend does NOT reliably honour NOT — verify before relying on it.'),
            _f("audience", "Audience", "select", "", options=AUDIENCES),
            _f("limit", "Result limit per channel", "number", 200,
               help="A search that returns exactly this many hit the cap: the true total is "
                    "unknown and gets reported as 'at least N'."),
        ],
    },
    "filter": {
        "label": "Filter", "color": "#b5842f", "inputs": 1, "outputs": 1,
        "blurb": "A preset hard-filter. Chain several if needed.",
        "fields": [
            _f("preset", "Preset", "select", "cu_only", options=list(FILTER_PRESETS)),
            _f("name_include", "Name include regex", "text", "",
               help="Used by name_regex."),
            _f("name_exclude", "Name exclude regex", "text", ""),
            _f("subcat_include", "Sub-category include keywords (comma sep)", "text", "",
               help="Used by subcategory. Needs an Enrich node upstream."),
            _f("subcat_exclude", "Sub-category exclude keywords (comma sep)", "text", ""),
            _f("max_per_theme", "Max per creative theme", "number", 2,
               help="Used by creative_dedupe."),
        ],
    },
    "enrich": {
        "label": "SQL Enrich", "color": "#8a5fb0", "inputs": 1, "outputs": 1,
        "blurb": "entry_ids -> full column rows via SSH/MySQL. Required for Excel and for "
                 "sub-category filtering.",
        "fields": [
            _f("window_field", "Re-bound rows by", "select", "added_to_database",
               options=[k for k, _ in WINDOW_FIELDS] + ["none"],
               help="Excel sheets are conventionally bounded by added_to_database, which is "
                    "NOT the same as the deck's entry_id window."),
        ],
    },
    "curate": {
        "label": "Curate (LLM)", "color": "#c2603f", "inputs": 1, "outputs": 1,
        "blurb": "Two Claude calls: pick entry_ids, then write the callout. Deterministic "
                 "fixups (fallback, dedup, chunking) stay in Python.",
        "fields": [
            _f("want", "Entries to feature", "number", 4),
            _f("max_shown", "Hard cap on shown entries", "number", 5),
            _f("guidance", "Selection guidance for the model", "textarea",
               "Prefer content that encourages online sign-up. Work only from the provided "
               "OCR text; never invent details."),
            _f("narrative_style", "Callout style instructions", "textarea",
               "One analyst-voice paragraph summarising all featured pieces. Name each "
               "company and its specific offer."),
            _f("callout_limit", "Callout char limit", "number", 374,
               help="Builder's insight field limit. Trimmed to whole sentences, no ellipsis."),
            _f("dedupe_company", "One piece per company", "bool", True),
            _f("cross_slide_dedupe", "Never reuse an entry on another slide", "bool", True),
        ],
    },
    "sheet": {
        "label": "Excel Sheet", "color": "#2f7f7f", "inputs": 1, "outputs": 1,
        "blurb": "One worksheet. Connect one or more enriched searches into it.",
        "fields": [
            _f("name", "Sheet name", "text", "Membership"),
            _f("headers_preset", "Column set", "select", "banking_19",
               options=list(HEADER_PRESETS) + ["custom"]),
            _f("headers_custom", "Custom headers (one per line)", "textarea", ""),
            _f("filter_row", "A1 filter description", "textarea",
               "Sector: {sectors} | Media Channel: {channels} | Window: {start} .. {end}",
               help="Tokens: {sectors} {channels} {keyword} {start} {end} {companies}"),
            _f("highlight_market", "Highlight the Market column", "bool", False),
        ],
    },
    "slide": {
        "label": "Slide", "color": "#4a6fa5", "inputs": 1, "outputs": 0,
        "blurb": "A deck slide. Order is computed top-to-bottom, then left-to-right.",
        "fields": [
            _f("slide_type", "Slide type", "select", "entry_ids", options=SLIDE_TYPES),
            _f("title", "Slide title", "text", "Membership Acquisition"),
            _f("chunk_over_cap", "Split into (cont.) slides past 5 entries", "bool", True),
            _f("agenda_sections", "Agenda items (one per line)", "textarea", "",
               help="agenda slides only."),
        ],
    },
    "synthesize": {
        "label": "Synthesize (final LLM)", "color": "#a04070", "inputs": 1, "outputs": 1,
        "blurb": "Reads the finished callouts and writes the two-column summary. Always the "
                 "LAST model call.",
        "fields": [
            _f("title1", "Left column heading", "text", "{period} Email Activity"),
            _f("title2", "Right column heading", "text", "{period} Other Channel Activity"),
            _f("max_words", "Max words per column", "number", 50),
            _f("system", "System prompt", "textarea",
               "You are a competitive-intelligence analyst. Synthesise the supplied "
               "per-company findings into two tight paragraphs. Use only what is given."),
        ],
    },
    "deck": {
        "label": "Deck", "color": "#2d3a53", "max": 1, "inputs": 1, "outputs": 0,
        "blurb": "Builds and saves the PPTX.",
        "fields": [
            _f("deck_title", "Deck title", "text", "{client} Weekly Update — {period}"),
            _f("filename", "Filename", "text", "{client}_Weekly_Report_{stamp}.pptx"),
        ],
    },
    "excel": {
        "label": "Workbook", "color": "#1f6f4a", "max": 1, "inputs": 1, "outputs": 0,
        "blurb": "Writes all connected sheets into one .xlsx.",
        "fields": [
            _f("filename", "Filename", "text",
               "{client}_Competiscan_MarketingTopics_{mmddyy}.xlsx"),
        ],
    },
    "email": {
        "label": "Email (opt-in)", "color": "#7a5c3a", "max": 1, "inputs": 1, "outputs": 0,
        "blurb": "Sends deliverables ONLY when the named env var is set at run time.",
        "fields": [
            _f("to_env_var", "Recipient environment variable", "text", "RS_EMAIL_TO",
               help="GUARDRAIL: a graph names an env var, never a literal address. The "
                    "generated pipeline stays silent unless it is set."),
            _f("report_name", "Report name in the email", "text", "{client} Weekly Update"),
        ],
    },
    "note": {
        "label": "Engineer note", "color": "#5a5a5a", "inputs": 0, "outputs": 0,
        "blurb": "Anything the nodes can't express. Lifted verbatim into the generated "
                 "file's docstring as a TODO for Engineering.",
        "fields": [
            _f("text", "What needs custom code?", "textarea",
               "e.g. Home Depot Pro vs Consumer needs a client-side subtraction because the "
               "OCR search does not honour boolean NOT."),
        ],
    },
}

# ═══════════════════════════════════════════════════════════════════════════════════════
# Templates — the three shipped reports, as starting graphs
# ═══════════════════════════════════════════════════════════════════════════════════════

def _n(nid, ntype, x, y, **params):
    return {"id": nid, "type": ntype, "x": x, "y": y, "params": params}


def _template_regional_weekly() -> dict:
    """Harborstone shape: 4 categories -> 4 sheets + 4 slides, weekly."""
    nodes = [
        _n("p1", "period", 40, 40, client="Harborstone", kind="week",
           anchor="prior_complete", window_field="entry_id"),
        _n("t1", "slide", 40, 190, slide_type="title", title="{client} Weekly Update"),
        _n("note1", "note", 40, 300,
           text="Auto and Home both search the Mortgage & Loan sector, which also contains "
                "business/personal/BNPL loans. They need the SQL sub-category hard filter "
                "(vehicle financing vs mortgage/HELOC) — the subcategory Filter node covers "
                "this, but please verify the keyword lists against live data."),
        _n("dk", "deck", 1180, 40, deck_title="{client} Weekly Update — {period}",
           filename="{client}_Weekly_Report_{stamp}.pptx"),
        _n("xl", "excel", 1180, 150,
           filename="{client}_Competiscan_MarketingTopics_{mmddyy}.xlsx"),
        _n("em", "email", 1180, 260, to_env_var="HARBOR_EMAIL_TO",
           report_name="{client} Weekly Update"),
    ]
    edges = [{"from": "t1", "to": "dk"}]
    cats = [
        ("membership", "Membership Acquisition", "Membership", ["Banking"],
         '"join" or "become" or "new member" or "new members" or "members" not "join us"',
         "banking_19",
         "Credit unions ONLY. General membership growth, not a specific product. Strongly "
         "avoid pieces advertising one named account or loan. Prefer referral campaigns and "
         "broad 'why join a credit union' brand messaging."),
        ("checking", "Checking Acquisition", "Checking", ["Banking"], "", "banking_19",
         "Credit unions only. Checking-account acquisition that encourages opening an "
         "account or signing up online."),
        ("auto", "Auto Lending", "Auto", ["Mortgage & Loan"], "", "lending_21",
         "Credit unions only. Vehicle financing and refinancing acquisition content only."),
        ("home", "Home Lending", "Home Lending", ["Mortgage & Loan"], "", "lending_21",
         "Credit unions only. Home-equity and mortgage acquisition content only."),
    ]
    y = 420
    for key, title, sheet, sectors, kw, preset, guidance in cats:
        s, f, e, c, sh, sl = (f"s_{key}", f"f_{key}", f"e_{key}", f"c_{key}",
                              f"sh_{key}", f"sl_{key}")
        nodes += [
            _n(s, "search", 240, y, group_key=key, title=title, sectors=sectors,
               channels=CHANNELS[:], keyword=kw, audience="Consumer", limit=200,
               company_names=""),
            _n(f, "filter", 440, y, preset="cu_only"),
            _n(e, "enrich", 620, y, window_field="none"),
            _n(sh, "sheet", 800, y + 60, name=sheet, headers_preset=preset,
               highlight_market=True,
               filter_row="{keyword} | Sector: {sectors} | Media Channel: {channels} "
                          "| Entry Date: {start} .. {end} | Credit Unions only"),
            _n(c, "curate", 800, y, want=4, max_shown=4, guidance=guidance,
               callout_limit=374, dedupe_company=True, cross_slide_dedupe=True,
               narrative_style="One analyst-voice paragraph summarising all featured "
                               "pieces. Name each institution and its specific offer."),
            _n(sl, "slide", 990, y, slide_type="entry_ids", title=title,
               chunk_over_cap=True),
        ]
        edges += [
            {"from": "p1", "to": s}, {"from": s, "to": f}, {"from": f, "to": e},
            {"from": e, "to": c}, {"from": e, "to": sh}, {"from": c, "to": sl},
            {"from": sl, "to": "dk"}, {"from": sh, "to": "xl"},
        ]
        y += 150
    nodes.append(_n("cl", "slide", 990, y, slide_type="closing", title="Closing"))
    edges.append({"from": "cl", "to": "dk"})
    edges += [{"from": "dk", "to": "em"}, {"from": "xl", "to": "em"}]
    return {"name": "Regional weekly (Harborstone shape)", "nodes": nodes, "edges": edges}


def _template_competitor_monthly() -> dict:
    """SupplyHouse shape: companies x channel sections, monthly, with synthesis."""
    companies = [
        ("ferguson", "Ferguson", "Ferguson Enterprises, LLC\nFerguson plc"),
        ("grainger", "Grainger", "Grainger"),
        ("home_depot", "The Home Depot", "The Home Depot\nHome Depot"),
        ("lowes", "Lowe's", "Lowe's\nLowes"),
        ("supplyhouse", "SupplyHouse", "SupplyHouse.com\nSupplyHouse"),
        ("zoro", "Zoro Tools", "Zoro Tools\nZoro"),
    ]
    sections = [
        ("email", "Email", ["Email"]),
        ("dm", "Direct Mail", ["Direct Mail"]),
        ("digital", "Digital Ads", ["Online Display", "Online Video",
                                   "Search Engine Marketing", "Social Media", "Website/URL"]),
    ]
    nodes = [
        _n("p1", "period", 40, 40, client="SupplyHouse.com", kind="month",
           anchor="prior_complete", window_field="entry_id"),
        _n("t1", "slide", 40, 190, slide_type="title", title="{client} Competitor Ads"),
        _n("ag", "slide", 40, 260, slide_type="agenda", title="Navigation",
           agenda_sections="Key Takeaways\nEmail\nDirect Mail\nDigital Ads"),
        _n("sy", "synthesize", 40, 340, title1="{period} Email Activity",
           title2="{period} Other Channel Activity", max_words=50,
           system="You are a competitive-intelligence analyst writing for a client deck. "
                  "Synthesise the supplied per-company findings into two tight paragraphs: "
                  "one for email, one for direct mail plus digital. Name companies and "
                  "their specific offers. Use only what is given. Never mention a company "
                  "that had no activity."),
        _n("ntk", "slide", 240, 340, slide_type="needToKnow", title="What you need to know"),
        _n("note1", "note", 40, 470,
           text="Home Depot's deck needs TWO email buckets (Pro & Pro Xtra vs Consumer to "
                "Pro). The OCR search does not honour boolean NOT, so this must be a "
                "client-side subtraction: fetch ALL Home Depot email, fetch the "
                "'Pro Xtra'/'Home Depot Pro' keyword subset, and subtract. Not expressible "
                "as nodes — please wire this by hand after export."),
        _n("dk", "deck", 1420, 40, deck_title="{client} Competitor Ads — {period}",
           filename="SupplyHouse_Competitor_Ads_{month_year}.pptx"),
        _n("em", "email", 1420, 150, to_env_var="SH_EMAIL_TO",
           report_name="{client} Competitor Ads"),
    ]
    edges = [{"from": "t1", "to": "dk"}, {"from": "ag", "to": "dk"},
             {"from": "sy", "to": "ntk"}, {"from": "ntk", "to": "dk"},
             {"from": "dk", "to": "em"}]
    y = 620
    for sec_key, sec_label, sec_channels in sections:
        div = f"div_{sec_key}"
        nodes.append(_n(div, "slide", 240, y, slide_type="newSection", title=sec_label))
        edges.append({"from": div, "to": "dk"})
        y += 90
        for ck, cname, aliases in companies:
            gk = f"{sec_key}_{ck}"
            s, d, c, sl = f"s_{gk}", f"d_{gk}", f"c_{gk}", f"sl_{gk}"
            nodes += [
                _n(s, "search", 420, y, group_key=gk, title=cname, company_names=aliases,
                   sectors=["Retail"], channels=sec_channels[:], keyword="", audience="",
                   limit=200),
                _n(d, "filter", 640, y, preset="creative_dedupe", max_per_theme=2),
                _n(c, "curate", 830, y, want=10, max_shown=10, callout_limit=374,
                   dedupe_company=False, cross_slide_dedupe=False,
                   guidance="Pick the pieces that best show the RANGE of what this company "
                            "sent — varied themes and products, not just the newest.",
                   narrative_style="Open by stating the true count, spelling out numbers "
                                   "under one hundred ('sent fifty emails'). Then summarise "
                                   "the actual themes and offers in one or two sentences."),
                _n(sl, "slide", 1060, y, slide_type="entry_ids", title=cname,
                   chunk_over_cap=True),
            ]
            edges += [
                {"from": "p1", "to": s}, {"from": s, "to": d}, {"from": d, "to": c},
                {"from": c, "to": sl}, {"from": sl, "to": "dk"}, {"from": c, "to": "sy"},
            ]
            y += 80
        y += 30
    nodes.append(_n("cl", "slide", 1060, y, slide_type="closing", title="Closing"))
    edges.append({"from": "cl", "to": "dk"})
    return {"name": "Competitor monthly (SupplyHouse shape)", "nodes": nodes, "edges": edges}


def _template_minimal() -> dict:
    """Merger shape: one keyword across two channels, deck only, no Excel."""
    nodes = [
        _n("p1", "period", 40, 40, client="Monthly Banking Merger", kind="month",
           anchor="prior_complete", window_field="entry_id"),
        _n("t1", "slide", 40, 200, slide_type="title", title="{client}"),
        _n("s_email", "search", 260, 300, group_key="email", title="Recent Observations",
           sectors=["Banking"], channels=["Email"],
           keyword='"merger" or "acquisition" or "merged" or "acquired"',
           audience="", limit=200, company_names=""),
        _n("c_email", "curate", 470, 300, want=4, max_shown=4, callout_limit=374,
           guidance="Acquisition announcements and operational updates. Work only from the "
                    "provided OCR text.",
           narrative_style="One analyst paragraph covering all featured pieces.",
           dedupe_company=True, cross_slide_dedupe=True),
        _n("sl_email", "slide", 680, 300, slide_type="entry_ids",
           title="Recent Observations", chunk_over_cap=True),
        _n("s_social", "search", 260, 400, group_key="social",
           title="Social Media Observations", sectors=["Banking"],
           channels=["Social Media"],
           keyword='"merger" or "acquisition" or "merged" or "acquired"',
           audience="", limit=200, company_names=""),
        _n("c_social", "curate", 470, 400, want=4, max_shown=4, callout_limit=374,
           guidance="Social-first merger messaging. Work only from the provided OCR text.",
           narrative_style="One analyst paragraph covering all featured pieces.",
           dedupe_company=True, cross_slide_dedupe=True),
        _n("sl_social", "slide", 680, 400, slide_type="entry_ids",
           title="Social Media Observations", chunk_over_cap=True),
        _n("cl", "slide", 680, 500, slide_type="closing", title="Closing"),
        _n("dk", "deck", 900, 40, deck_title="{client} — {period}",
           filename="Monthly_Banking_Merger_Report_{stamp}.pptx"),
    ]
    edges = [
        {"from": "p1", "to": "s_email"}, {"from": "s_email", "to": "c_email"},
        {"from": "c_email", "to": "sl_email"}, {"from": "sl_email", "to": "dk"},
        {"from": "p1", "to": "s_social"}, {"from": "s_social", "to": "c_social"},
        {"from": "c_social", "to": "sl_social"}, {"from": "sl_social", "to": "dk"},
        {"from": "t1", "to": "dk"}, {"from": "cl", "to": "dk"},
    ]
    return {"name": "Minimal (merger shape, deck only)", "nodes": nodes, "edges": edges}


TEMPLATES = {
    "regional_weekly": _template_regional_weekly,
    "competitor_monthly": _template_competitor_monthly,
    "minimal": _template_minimal,
}

# ═══════════════════════════════════════════════════════════════════════════════════════
# Graph helpers
# ═══════════════════════════════════════════════════════════════════════════════════════

def _by_id(graph) -> dict[str, dict]:
    return {n["id"]: n for n in graph.get("nodes", [])}


def _of_type(graph, t) -> list[dict]:
    return [n for n in graph.get("nodes", []) if n["type"] == t]


def _downstream(graph, nid) -> list[str]:
    return [e["to"] for e in graph.get("edges", []) if e["from"] == nid]


def _upstream(graph, nid) -> list[str]:
    return [e["from"] for e in graph.get("edges", []) if e["to"] == nid]


def _param(node, key, default=None):
    spec = NODE_SPECS.get(node["type"], {})
    for f in spec.get("fields", []):
        if f["key"] == key:
            default = f.get("default") if default is None else default
            break
    v = node.get("params", {}).get(key)
    return default if v is None or v == "" else v


def _walk_forward(graph, start, stop_types, seen=None) -> list[dict]:
    """Collect nodes reachable from `start` until (and including) a stop-type node."""
    nodes = _by_id(graph)
    seen = seen if seen is not None else set()
    out = []
    for nxt in _downstream(graph, start):
        if nxt in seen or nxt not in nodes:
            continue
        seen.add(nxt)
        n = nodes[nxt]
        out.append(n)
        if n["type"] not in stop_types:
            out.extend(_walk_forward(graph, nxt, stop_types, seen))
    return out


def slide_order(graph) -> list[str]:
    """Deck order: top-to-bottom by row, then left-to-right. Shown as a badge on every
    slide node so the ordering is never a mystery."""
    slides = _of_type(graph, "slide")
    slides.sort(key=lambda n: (round(n.get("y", 0) / 60), n.get("x", 0)))
    return [n["id"] for n in slides]


def resolve_groups(graph) -> list[dict]:
    """Turn each Search node into a fully-resolved 'group' by walking downstream:
    filters -> enrich -> curate -> slide(s). This is the join that codegen emits."""
    nodes = _by_id(graph)
    groups = []
    for s in sorted(_of_type(graph, "search"), key=lambda n: (round(n.get("y", 0) / 60),
                                                             n.get("x", 0))):
        chain = _walk_forward(graph, s["id"], {"curate", "sheet", "slide"})
        filters = [n for n in chain if n["type"] == "filter"]
        enrich = next((n for n in chain if n["type"] == "enrich"), None)
        curate = next((n for n in chain if n["type"] == "curate"), None)
        slides = []
        if curate:
            slides = [nodes[i] for i in _downstream(graph, curate["id"])
                      if nodes.get(i, {}).get("type") == "slide"]
        groups.append({
            "search": s, "filters": filters, "enrich": enrich, "curate": curate,
            "slides": slides,
            "key": _param(s, "group_key") or s["id"],
            "title": _param(s, "title") or _param(s, "group_key") or s["id"],
        })
    return groups


def resolve_sheets(graph) -> list[dict]:
    """Each Sheet node plus the group keys feeding it (walking back through enrich/filter)."""
    nodes = _by_id(graph)
    out = []
    for sh in sorted(_of_type(graph, "sheet"), key=lambda n: (round(n.get("y", 0) / 60),
                                                             n.get("x", 0))):
        keys, seen, stack = [], set(), list(_upstream(graph, sh["id"]))
        while stack:
            nid = stack.pop()
            if nid in seen or nid not in nodes:
                continue
            seen.add(nid)
            n = nodes[nid]
            if n["type"] == "search":
                keys.append(_param(n, "group_key") or n["id"])
            else:
                stack.extend(_upstream(graph, nid))
        out.append({"node": sh, "group_keys": list(reversed(keys))})
    return out


# ═══════════════════════════════════════════════════════════════════════════════════════
# Validation — the guardrails
# ═══════════════════════════════════════════════════════════════════════════════════════

def validate(graph) -> dict:
    errors: list[str] = []
    warnings: list[str] = []
    nodes = graph.get("nodes", [])

    for t, spec in NODE_SPECS.items():
        if "max" in spec:
            n = len(_of_type(graph, t))
            if n > spec["max"]:
                errors.append(f"{spec['label']}: only {spec['max']} allowed, found {n}.")

    if not _of_type(graph, "period"):
        errors.append("No Period node. Every pipeline needs exactly one reporting window.")
    if not _of_type(graph, "search"):
        errors.append("No Search node. Nothing would be retrieved.")
    if not _of_type(graph, "deck") and not _of_type(graph, "excel"):
        errors.append("No Deck and no Workbook node — the pipeline would produce no "
                      "deliverable.")

    groups = resolve_groups(graph)
    keys = [g["key"] for g in groups]
    dupes = {k for k in keys if keys.count(k) > 1}
    if dupes:
        errors.append(f"Duplicate group keys: {', '.join(sorted(dupes))}. Each Search needs "
                      f"a unique group key — it becomes a dict key in the generated code.")

    for g in groups:
        s, title = g["search"], g["title"]
        if not _param(s, "channels"):
            errors.append(f"Search '{title}': no media channels selected.")
        if not _param(s, "sectors") and not _param(s, "company_names"):
            warnings.append(f"Search '{title}': neither sector nor company set — this will "
                            f"pull the whole archive for those channels.")
        try:
            lim = int(_param(s, "limit", 200))
            if lim > 200:
                warnings.append(f"Search '{title}': limit {lim} exceeds the backend's "
                                f"practical cap; it will silently return fewer.")
        except (TypeError, ValueError):
            errors.append(f"Search '{title}': limit must be a number.")

        if re.search(r"\bnot\b", str(_param(s, "keyword", "")), re.IGNORECASE):
            warnings.append(f"Search '{title}': keyword uses NOT. The OCR backend does not "
                            f"reliably honour negation — a positive query plus a "
                            f"client-side subtraction is the proven approach. Describe it "
                            f"in an Engineer note.")

        subcat = [f for f in g["filters"] if _param(f, "preset") == "subcategory"]
        if subcat and not g["enrich"]:
            errors.append(f"Search '{title}': a sub-category filter needs a SQL Enrich node "
                          f"upstream of it — raw search records carry no sub-category tags.")

        reaches_sheet = any(
            n["type"] == "sheet"
            for n in _walk_forward(graph, s["id"], {"curate", "slide"})
        )
        if not g["curate"] and not reaches_sheet:
            warnings.append(f"Search '{title}': feeds neither a Curate nor a Sheet, so its "
                            f"results are fetched and discarded.")

        if g["curate"]:
            c = g["curate"]
            try:
                want, cap = int(_param(c, "want", 4)), int(_param(c, "max_shown", 5))
            except (TypeError, ValueError):
                errors.append(f"Curate for '{title}': want/max_shown must be numbers.")
                want = cap = 0
            if want > cap:
                warnings.append(f"Curate for '{title}': wants {want} but caps at {cap}; "
                                f"{cap} wins.")
            elif cap > want:
                warnings.append(f"Curate for '{title}': the cap ({cap}) is above 'entries to "
                                f"feature' ({want}), so it does nothing — only {want} will "
                                f"ever be shown. Raise 'entries to feature' to {cap} if you "
                                f"want {cap} across (cont.) slides.")
            if cap > 5:
                slides = g["slides"]
                if slides and not all(_param(sl, "chunk_over_cap", True) for sl in slides):
                    errors.append(f"'{title}' can show {cap} entries but its slide does not "
                                  f"chunk. The builder holds 5 per slide — enable "
                                  f"'Split into (cont.) slides'.")
            try:
                if int(_param(c, "callout_limit", 374)) > 374:
                    warnings.append(f"Curate for '{title}': callout limit above 374 chars "
                                    f"will overflow the builder's insight field.")
            except (TypeError, ValueError):
                pass
            if not g["slides"]:
                warnings.append(f"Curate for '{title}' feeds no Slide — its LLM calls would "
                                f"cost money and go nowhere.")

    for sh in resolve_sheets(graph):
        name = _param(sh["node"], "name", "?")
        if not sh["group_keys"]:
            errors.append(f"Sheet '{name}': nothing connected into it.")
        upstream_ids, seen, stack = set(), set(), list(_upstream(graph, sh["node"]["id"]))
        allnodes = _by_id(graph)
        while stack:
            nid = stack.pop()
            if nid in seen:
                continue
            seen.add(nid)
            upstream_ids.add(allnodes.get(nid, {}).get("type"))
            stack.extend(_upstream(graph, nid))
        if "enrich" not in upstream_ids:
            errors.append(f"Sheet '{name}': needs a SQL Enrich node upstream. Raw search "
                          f"records do not have the Excel columns.")
        if _param(sh["node"], "headers_preset") == "custom" and \
                not _param(sh["node"], "headers_custom"):
            errors.append(f"Sheet '{name}': custom column set selected but no headers given.")

    if _of_type(graph, "sheet") and not _of_type(graph, "excel"):
        errors.append("Sheets exist but there is no Workbook node to write them into.")
    if _of_type(graph, "excel") and not _of_type(graph, "sheet"):
        errors.append("A Workbook node with no Sheets would produce an empty file.")

    for sy in _of_type(graph, "synthesize"):
        feeders = [i for i in _upstream(graph, sy["id"])
                   if _by_id(graph).get(i, {}).get("type") == "curate"]
        if not feeders:
            errors.append("Synthesize: connect the Curate nodes whose callouts it should "
                          "summarise.")
        targets = [i for i in _downstream(graph, sy["id"])
                   if _by_id(graph).get(i, {}).get("type") == "slide"]
        if not targets:
            errors.append("Synthesize: connect it to a needToKnow Slide.")
        else:
            for t in targets:
                if _param(_by_id(graph)[t], "slide_type") != "needToKnow":
                    warnings.append("Synthesize normally feeds a 'needToKnow' slide; "
                                    f"'{_param(_by_id(graph)[t], 'slide_type')}' may not "
                                    f"render two columns.")

    deck = _of_type(graph, "deck")
    if deck:
        wired = {e["from"] for e in graph.get("edges", []) if e["to"] == deck[0]["id"]}
        orphans = [n for n in _of_type(graph, "slide") if n["id"] not in wired]
        if orphans:
            warnings.append(f"{len(orphans)} slide(s) are not connected to the Deck and will "
                            f"be omitted: " +
                            ", ".join(_param(o, 'title', o['id']) for o in orphans[:4]) +
                            ("…" if len(orphans) > 4 else ""))

    for em in _of_type(graph, "email"):
        var = str(_param(em, "to_env_var", ""))
        if "@" in var:
            errors.append("Email: put the NAME of an environment variable here, not an "
                          "address. Recipients are supplied at run time so a graph can "
                          "never carry a client's inbox.")
        elif not re.fullmatch(r"[A-Z_][A-Z0-9_]*", var or ""):
            warnings.append(f"Email: '{var}' is an unusual env-var name; UPPER_SNAKE_CASE "
                            f"is conventional.")

    notes = [_param(n, "text", "") for n in _of_type(graph, "note")]
    if notes:
        warnings.append(f"{len(notes)} engineer note(s) will be exported as TODOs — this "
                        f"pipeline needs manual work before it is production-ready.")

    if PIPELINES_DIR is None:
        warnings.append("report_lib.py was not found near this script, so Test cannot run. "
                        "Move report_studio.py into the project root or alongside "
                        "pipelines/.")

    return {"errors": errors, "warnings": warnings, "order": slide_order(graph),
            "groups": [g["key"] for g in groups]}


# ═══════════════════════════════════════════════════════════════════════════════════════
# Code generation — readable, house-style Python
# ═══════════════════════════════════════════════════════════════════════════════════════

def _lit(v) -> str:
    """A PYTHON literal — not JSON.

    json.dumps() looks close enough but emits true/false/null, which are valid
    *identifiers* in Python and therefore sail straight past ast.parse() only to
    blow up as NameError at import time. So scalars go through repr() and
    containers are assembled by hand. Long prose strings are wrapped into an
    implicitly-concatenated block so the generated file stays readable.
    """
    if isinstance(v, str):
        if len(v) > 78 or "\n" in v:
            words, line, out = v.replace("\r", "").split(" "), "", []
            for word in words:
                if len(line) + len(word) + 1 > 70:
                    out.append(line)
                    line = word
                else:
                    line = f"{line} {word}".strip()
            if line:
                out.append(line)
            body = "\n".join(
                "        " + _pystr(p + (" " if i < len(out) - 1 else ""))
                for i, p in enumerate(out))
            return "(\n" + body + "\n    )"
        return _pystr(v)
    if isinstance(v, bool) or v is None:
        return repr(v)                      # True / False / None
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_lit(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{_lit(k)}: {_lit(val)}" for k, val in v.items()) + "}"
    return _pystr(str(v))


def _pystr(s: str) -> str:
    """Double-quoted Python string literal (house style), correctly escaped."""
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(s)).strip("_") or "report"


def codegen(graph) -> tuple[str, str]:
    """Return (python_source, suggested_filename)."""
    period = (_of_type(graph, "period") or [None])[0]
    client = _param(period, "client", "Report") if period else "Report"
    kind = _param(period, "kind", "week") if period else "week"
    anchor = _param(period, "anchor", "prior_complete") if period else "prior_complete"
    win_field = _param(period, "window_field", "entry_id") if period else "entry_id"

    groups = resolve_groups(graph)
    sheets = resolve_sheets(graph)
    deck = (_of_type(graph, "deck") or [None])[0]
    excel = (_of_type(graph, "excel") or [None])[0]
    email = (_of_type(graph, "email") or [None])[0]
    synth = (_of_type(graph, "synthesize") or [None])[0]
    notes = [str(_param(n, "text", "")).strip() for n in _of_type(graph, "note")]
    notes = [n for n in notes if n]

    order = slide_order(graph)
    nodes = _by_id(graph)
    deck_wired = ({e["from"] for e in graph.get("edges", []) if deck and e["to"] == deck["id"]}
                  if deck else set())
    slide_to_group = {}
    for g in groups:
        for sl in g["slides"]:
            slide_to_group[sl["id"]] = g["key"]

    needs_enrich = any(g["enrich"] for g in groups) or bool(sheets)
    needs_curate = any(g["curate"] for g in groups)
    uses_market = any(_param(sh["node"], "highlight_market", False) for sh in sheets)
    L: list[str] = []
    w = L.append

    # ── docstring ───────────────────────────────────────────────────────────────────
    w('#!/usr/bin/env python3')
    w('"""')
    w(f'{client} — GENERATED BY REPORT STUDIO')
    w('─' * 78)
    w(f'Graph      : {graph.get("name", "untitled")}')
    w(f'Generated  : {datetime.now():%Y-%m-%d %H:%M}')
    w(f'Cadence    : {kind} ({anchor})')
    w(f'Window from: {win_field}')
    w('')
    w('This file was produced from a node graph. It follows the house pipeline pattern')
    w('(report_HarborstoneWeekly.py) and is safe to edit by hand — but remember that')
    w('edits here do not flow back into the graph.')
    w('')
    w('RUN')
    w(f'    python pipelines/generated/{_slug(client)}.py               # full run')
    w('    python ... --only search      # searches + counts only (no LLM, no deck)')
    if sheets:
        w('    python ... --only excel       # searches + enrich + workbook')
    w('    python ... --only deck        # everything')
    w('    python ... --limit 20         # cap results per search while testing')
    w('')
    if notes:
        w('╔══════════════════════════════════════════════════════════════════════════╗')
        w('║  ENGINEER NOTES — bespoke work the node editor could not express.        ║')
        w('║  These are NOT implemented below. Please wire them by hand.              ║')
        w('╚══════════════════════════════════════════════════════════════════════════╝')
        for i, n in enumerate(notes, 1):
            w(f'  {i}. ' + n.replace("\n", "\n     "))
        w('')
    w('GUARDRAILS BAKED IN')
    w('  * Searches run sequentially — the REST backend cross-contaminates results')
    w('    when different channels are requested concurrently.')
    w('  * A search returning exactly its limit hit the cap: the true total is unknown')
    w('    and is reported as "at least N". Cap-hit with zero in-window is SUSPECT.')
    w(f'  * The window is bounded by {win_field}. The three date fields disagree.')
    w('  * Counts, dedup and chunking are computed in Python; the model only picks')
    w('    entry_ids and writes prose.')
    if email:
        w('  * Email sends only when the env var below is set. Never automatic.')
    w('"""')
    w('')

    # ── imports / bootstrap ─────────────────────────────────────────────────────────
    w('import argparse')
    w('import os')
    w('import re')
    w('import sys')
    w('from datetime import date, timedelta')
    w('from pathlib import Path')
    w('')
    w('try:  # a cp1252 console must not crash on the box-drawing glyphs above')
    w('    sys.stdout.reconfigure(encoding="utf-8", errors="replace")')
    w('except Exception:')
    w('    pass')
    w('')
    w('PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent')
    w('sys.path.insert(0, str(PROJECT_ROOT))')
    w('')
    w('# Raise the builder timeout BEFORE the builder module is imported.')
    w('os.environ.setdefault("PPT_BUILDER_TIMEOUT", "300")')
    w('')
    w('import pipelines.report_lib as L  # noqa: E402')
    if needs_enrich:
        w('import pipelines.report_lib_excel_helper as XH  # noqa: E402')
    w('')
    w('search_archive     = L.load_tool("mcp_serverv4", "search_archive")')
    if deck:
        w('build_deck_default = L.load_tool("mcp_pptbuilder", "build_deck_default")')
    if needs_enrich:
        w('_run_sql           = L.load_tool("mcp_serverv3", "_run_sql")')
    w('')
    w('')

    # ── config ──────────────────────────────────────────────────────────────────────
    w('# ── Config ' + '─' * 66)
    w(f'CLIENT        = {_lit(client)}')
    w(f'PERIOD_KIND   = {_lit(kind)}          # week | month')
    w(f'PERIOD_ANCHOR = {_lit(anchor)}')
    w(f'WINDOW_FIELD  = {_lit(win_field)}')
    w('PERIOD_START  = os.environ.get("RS_PERIOD_START") or None   # "2026-07-07" overrides')
    w('PERIOD_END    = os.environ.get("RS_PERIOD_END") or None')
    w('SLIDE_CAP     = 5     # builder hard limit: 5 entries per slide')
    w('OUTPUT_DIR    = PROJECT_ROOT / "output"')
    if email:
        var = _param(email, "to_env_var", "RS_EMAIL_TO")
        w(f'EMAIL_TO      = os.environ.get({_lit(var)}) or None   # opt-in; unset = no send')
    w('')
    if uses_market:
        w('MARKET_HEADER_COLOR = "FFA500"')
        w('IN_MARKET_STATES = {"wa", "or", "ca", "washington", "oregon", "california"}')
        w('')

    for name, cols in HEADER_PRESETS.items():
        if any(_param(sh["node"], "headers_preset") == name for sh in sheets):
            w(f'HEADERS_{name.upper()} = [')
            line = '    '
            for c in cols:
                piece = _pystr(c) + ', '
                if len(line) + len(piece) > 92:
                    w(line.rstrip())
                    line = '    '
                line += piece
            w(line.rstrip().rstrip(','))
            w(']')
            w('')
    if sheets:
        w('HYPERLINKS = {')
        w('    "EntryID":     ("https://cp.competiscan.com/productdetail?id={pid}", "{entry_id}"),')
        w('    "PDF Content": ("https://www.competiscan.com/productDocuments.php?id={pid}", "PDF Content"),')
        w('}')
        w('')

    # ── GROUPS literal ──────────────────────────────────────────────────────────────
    w('# ── Groups (one Search node each) ' + '─' * 44)
    w('GROUPS = [')
    for g in groups:
        s, c = g["search"], g["curate"]
        companies = [x.strip() for x in str(_param(s, "company_names", "")).splitlines()
                     if x.strip()]
        w('    {')
        w(f'        "key": {_lit(g["key"])},')
        w(f'        "title": {_lit(g["title"])},')
        w(f'        "company_names": {_lit(companies)},')
        w(f'        "sectors": {_lit(_param(s, "sectors", []) or [])},')
        w(f'        "channels": {_lit(_param(s, "channels", []) or [])},')
        w(f'        "keyword": {_lit(_param(s, "keyword", "") or "")},')
        w(f'        "audience": {_lit(_param(s, "audience", "") or "")},')
        w(f'        "limit": {int(_param(s, "limit", 200))},')
        fl = []
        for f in g["filters"]:
            p = _param(f, "preset", "cu_only")
            d = {"preset": p}
            if p == "name_regex":
                d["include"] = _param(f, "name_include", "") or ""
                d["exclude"] = _param(f, "name_exclude", "") or ""
            elif p == "subcategory":
                d["include"] = [x.strip().lower() for x in
                                str(_param(f, "subcat_include", "")).split(",") if x.strip()]
                d["exclude"] = [x.strip().lower() for x in
                                str(_param(f, "subcat_exclude", "")).split(",") if x.strip()]
            elif p == "creative_dedupe":
                d["max_per_theme"] = int(_param(f, "max_per_theme", 2))
            fl.append(d)
        w(f'        "filters": {_lit(fl)},')
        w(f'        "enrich": {_lit(bool(g["enrich"]))},')
        if g["enrich"]:
            w(f'        "enrich_window": {_lit(_param(g["enrich"], "window_field", "none"))},')
        if c:
            w(f'        "want": {int(_param(c, "want", 4))},')
            w(f'        "max_shown": {int(_param(c, "max_shown", 5))},')
            w(f'        "callout_limit": {int(_param(c, "callout_limit", 374))},')
            w(f'        "dedupe_company": {_lit(bool(_param(c, "dedupe_company", True)))},')
            w(f'        "cross_slide_dedupe": {_lit(bool(_param(c, "cross_slide_dedupe", True)))},')
            w(f'        "guidance": {_lit(str(_param(c, "guidance", "") or ""))},')
            w(f'        "narrative_style": {_lit(str(_param(c, "narrative_style", "") or ""))},')
        else:
            w('        "want": 0,   # no Curate node: fetched for the workbook only')
        w('    },')
    w(']')
    w('')

    if sheets:
        w('# ── Excel sheets ' + '─' * 61)
        w('SHEETS = [')
        for sh in sheets:
            n = sh["node"]
            preset = _param(n, "headers_preset", "banking_19")
            hdrs = (f'HEADERS_{preset.upper()}' if preset in HEADER_PRESETS else
                    _lit([x.strip() for x in
                          str(_param(n, "headers_custom", "")).splitlines() if x.strip()]))
            w('    {')
            w(f'        "name": {_lit(_param(n, "name", "Sheet"))},')
            w(f'        "group_keys": {_lit(sh["group_keys"])},')
            w(f'        "headers": {hdrs},')
            w(f'        "filter_row": {_lit(str(_param(n, "filter_row", "") or ""))},')
            w(f'        "highlight_market": {_lit(bool(_param(n, "highlight_market", False)))},')
            w('    },')
        w(']')
        w('')

    # ── deck plan ───────────────────────────────────────────────────────────────────
    if deck:
        w('# ── Deck plan (order computed from the canvas: top-to-bottom, left-to-right) ──')
        w('DECK_PLAN = [')
        for sid in order:
            if sid not in deck_wired:
                continue
            sl = nodes[sid]
            st = _param(sl, "slide_type", "entry_ids")
            entry = {"type": st, "title": _param(sl, "title", "") or ""}
            if st == "entry_ids":
                entry["group"] = slide_to_group.get(sid)
                entry["chunk"] = bool(_param(sl, "chunk_over_cap", True))
                if entry["group"] is None:
                    entry["_orphan"] = True
            elif st == "agenda":
                entry["sections"] = [x.strip() for x in
                                     str(_param(sl, "agenda_sections", "")).splitlines()
                                     if x.strip()]
            elif st == "needToKnow":
                entry["from_synthesize"] = bool(
                    synth and synth["id"] in _upstream(graph, sid))
            w(f'    {_lit(entry)},')
        w(']')
        w('')

    if synth:
        feeders = [nodes[i] for i in _upstream(graph, synth["id"])
                   if nodes.get(i, {}).get("type") == "curate"]
        fkeys = []
        for g in groups:
            if g["curate"] and any(f["id"] == g["curate"]["id"] for f in feeders):
                fkeys.append(g["key"])
        w('# ── Final synthesis (the LAST model call) ' + '─' * 36)
        w(f'SYNTH_GROUPS = {_lit(fkeys)}')
        w(f'SYNTH_TITLE1 = {_lit(str(_param(synth, "title1", "")))}')
        w(f'SYNTH_TITLE2 = {_lit(str(_param(synth, "title2", "")))}')
        w(f'SYNTH_MAX_WORDS = {int(_param(synth, "max_words", 50))}')
        w(f'SYNTH_SYSTEM = {_lit(str(_param(synth, "system", "")))}')
        w('')

    # ── static helpers ──────────────────────────────────────────────────────────────
    w('')
    w('# ── Helpers ' + '─' * 66)
    w('_CU_RE = re.compile(r"credit union|\\bFCU\\b|\\bF\\.?C\\.?U\\.?\\b|\\bCU\\b", re.I)')
    w('')
    w('')
    w('def _parse_args():')
    w('    p = argparse.ArgumentParser(description=f"{CLIENT} report pipeline")')
    only = ["search", "excel", "deck", "all"] if sheets else ["search", "deck", "all"]
    w(f'    p.add_argument("--only", default="all", choices={_lit(only)},')
    w('                   help="Stop after a stage — cheap iteration while testing.")')
    w('    p.add_argument("--limit", type=int, default=None,')
    w('                   help="Override every search limit (small = fast test).")')
    w('    return p.parse_args()')
    w('')
    w('')
    w('def _period_window():')
    w('    """Returns (start, end). Explicit env overrides win; otherwise the configured')
    w('    cadence and anchor decide. prior_complete is reproducible — re-running next')
    w('    Tuesday gives the same window."""')
    w('    if PERIOD_START:')
    w('        s = date.fromisoformat(PERIOD_START)')
    w('        e = date.fromisoformat(PERIOD_END) if PERIOD_END else (')
    w('            s + timedelta(days=7) if PERIOD_KIND == "week" else _month_end(s))')
    w('        return s, e')
    w('    today = date.today()')
    w('    if PERIOD_KIND == "week":')
    w('        if PERIOD_ANCHOR == "rolling":')
    w('            return today - timedelta(days=7), today')
    w('        this_monday = today - timedelta(days=today.weekday())')
    w('        return this_monday - timedelta(days=7), this_monday')
    w('    if PERIOD_ANCHOR == "rolling":')
    w('        return today - timedelta(days=30), today')
    w('    first_this = today.replace(day=1)')
    w('    prev_end = first_this - timedelta(days=1)')
    w('    return prev_end.replace(day=1), prev_end')
    w('')
    w('')
    w('def _month_end(d):')
    w('    nxt = (d.replace(day=28) + timedelta(days=4)).replace(day=1)')
    w('    return nxt - timedelta(days=1)')
    w('')
    w('')
    w('def _ordinal(n):')
    w('    return f"{n}{\'th\' if 11 <= n % 100 <= 13 else {1: \'st\', 2: \'nd\', 3: \'rd\'}.get(n % 10, \'th\')}"')
    w('')
    w('')
    w('def _entry_date(entry_id):')
    w('    """entry_id is YYYY-MM-DD-NNNN. This is the MAILED/CAPTURED date, which is')
    w('    NOT approved_date and NOT added_to_database."""')
    w('    try:')
    w('        y, m, d = str(entry_id).split("-")[:3]')
    w('        return date(int(y), int(m), int(d))')
    w('    except (ValueError, AttributeError):')
    w('        return None')
    w('')
    w('')
    w('def _dedup(records):')
    w('    seen, out = set(), []')
    w('    for r in records:')
    w('        eid = r.get("entry_id")')
    w('        if eid and eid not in seen:')
    w('            seen.add(eid)')
    w('            out.append(r)')
    w('    return out')
    w('')
    w('')
    w('def _theme_key(record):')
    w('    """Coarse creative fingerprint: company + the first few headline words. Stops a')
    w('    company that reused one evergreen creative from filling every image slot."""')
    w('    co = (record.get("company_name") or "").lower().strip()')
    w('    head = re.sub(r"[^a-z0-9 ]", "", (record.get("headline") or "").lower())')
    w('    return f"{co}|{\' \'.join(head.split()[:6])}"')
    w('')
    w('')
    w('def _apply_filters(records, group, subcats_by_id=None):')
    w('    """Preset hard-filters, in graph order. Every one of these came from a shipped')
    w('    pipeline; none of them ask the model to do the filtering."""')
    w('    out = list(records)')
    w('    for f in group.get("filters", []):')
    w('        p = f["preset"]')
    w('        before = len(out)')
    w('        if p == "cu_only":')
    w('            out = [r for r in out if _CU_RE.search(r.get("company_name") or "")]')
    w('        elif p == "name_regex":')
    w('            inc, exc = f.get("include") or "", f.get("exclude") or ""')
    w('            if inc:')
    w('                out = [r for r in out')
    w('                       if re.search(inc, r.get("company_name") or "", re.I)]')
    w('            if exc:')
    w('                out = [r for r in out')
    w('                       if not re.search(exc, r.get("company_name") or "", re.I)]')
    w('        elif p == "subcategory":')
    w('            inc, exc = f.get("include") or [], f.get("exclude") or []')
    w('            kept = []')
    w('            for r in out:')
    w('                tags = (subcats_by_id or {}).get(r.get("entry_id"), "").lower()')
    w('                # A blank tag matches NOTHING: dropping an unverifiable entry beats')
    w('                # putting it on the wrong slide.')
    w('                if not tags:')
    w('                    continue')
    w('                if any(k in tags for k in exc):')
    w('                    continue')
    w('                if inc and not any(k in tags for k in inc):')
    w('                    continue')
    w('                kept.append(r)')
    w('            out = kept')
    w('        elif p == "creative_dedupe":')
    w('            cap, counts, kept = f.get("max_per_theme", 2), {}, []')
    w('            for r in out:')
    w('                k = _theme_key(r)')
    w('                if counts.get(k, 0) < cap:')
    w('                    counts[k] = counts.get(k, 0) + 1')
    w('                    kept.append(r)')
    w('            out = kept')
    w('        if len(out) != before:')
    w('            print(f"      filter {p}: {before} -> {len(out)}")')
    w('    return out')
    w('')
    w('')
    w('def _run_search(group, channel, limit):')
    w('    """ONE call, ONE channel. Fanning out per channel multiplies the per-call cap.')
    w('    Callers must keep this sequential (see the module docstring)."""')
    w('    kwargs = {"media_channels": [channel], "limit": limit}')
    w('    if group["sectors"]:')
    w('        kwargs["sectors"] = group["sectors"]')
    w('    if group["company_names"]:')
    w('        kwargs["company_names"] = group["company_names"]')
    w('    if group["keyword"]:')
    w('        kwargs["keyword"] = group["keyword"]')
    w('    if group["audience"]:')
    w('        kwargs["audience"] = group["audience"]')
    w('    try:')
    w('        res = search_archive(**kwargs)')
    w('    except Exception as exc:  # a dead VPN must not kill the whole run')
    w('        return [], False, str(exc)')
    w('    if res and isinstance(res[0], dict) and "error" in res[0]:')
    w('        return [], False, res[0]["error"]')
    w('    rows = [r for r in (res or []) if r.get("entry_id")]')
    w('    return rows, len(rows) >= limit, None')
    w('')
    w('')
    if needs_enrich:
        w('def _enrich(entry_ids):')
        w('    """entry_ids -> full Excel rows via SSH/MySQL. Only ids with BOTH a document')
        w('    and a primary-company mapping come back (the query inner-joins them)."""')
        w('    if not entry_ids:')
        w('        return []')
        w('    df = _run_sql(XH.build_query(entry_ids))')
        w('    if df is None or getattr(df, "empty", True):')
        w('        return []')
        w('    text_cols = {"additional_companies", "sectors", "categories", "sub_categories",')
        w('                 "states", "ages", "incomes", "primary_company", "product_name",')
        w('                 "product_headline", "media_channel", "mailing_type", "entry_id"}')
        w('')
        w('    def _clean(k, v):')
        w('        # pandas types GROUP_CONCAT oddly (all-NULL -> NaN, all-numeric -> float).')
        w('        # Flag columns MUST stay ints: "0" is truthy and would flip Pre-Screen.')
        w('        if v is None or (isinstance(v, float) and v != v):')
        w('            return None')
        w('        return str(v) if k in text_cols else v')
        w('')
        w('    raw = [{k: _clean(k, v) for k, v in rec.items()}')
        w('           for rec in df.to_dict("records")]')
        w('    rows = XH.complete_rows(raw)')
        w('    for row, r in zip(rows, raw):')
        w('        row["pid"] = r.get("product_id", "")')
        w('        row["entry_id"] = r.get("entry_id", "")')
        w('        row["_sub_categories"] = r.get("sub_categories") or ""')
        if uses_market:
            w('        st = (row.get("State/Province") or "").lower()')
            w('        row["Market"] = ("In Market"')
            w('                         if any(s in st for s in IN_MARKET_STATES) else "National")')
        w('    return rows')
        w('')
        w('')

    if needs_curate:
        w('# ── Prompts ' + '─' * 66)
        w('SELECT_SYSTEM = (')
        w('    "You are a competitive-intelligence analyst choosing which archive pieces to "')
        w('    "feature on a client slide.\\n\\n"')
        w('    "RULES\\n"')
        w('    "- Work ONLY from the OCR text supplied. Never invent an offer, rate or detail.\\n"')
        w('    "- Choose from the candidate entry_ids given. Do not output any other id.\\n"')
        w('    "- Prefer variety: different institutions and different offers beat near-"')
        w('    "duplicates.\\n\\n"')
        w('    "SLIDE GUIDANCE\\n{guidance}\\n\\n"')
        w('    \'Reply with ONE JSON object: {{"entry_ids": ["..."], "reasoning": "one line"}}\'')
        w(')')
        w('')
        w('CALLOUT_SYSTEM = (')
        w('    "You are writing the callout paragraph for a client-facing slide.\\n\\n"')
        w('    "RULES\\n"')
        w('    "- Describe ONLY the pieces supplied. Never invent details.\\n"')
        w('    "- Under {limit} characters. Whole sentences.\\n"')
        w('    "- Analyst voice. No bullet points, no preamble.\\n\\n"')
        w('    "STYLE\\n{style}\\n\\n"')
        w('    \'Reply with ONE JSON object: {{"callout": "..."}}\'')
        w(')')
        w('')
        w('')
        w('def _shortlist(records):')
        w('    """Compact candidate view. OCR is truncated hard — the model needs enough to')
        w('    judge relevance, not the whole document."""')
        w('    lines = []')
        w('    for r in records:')
        w('        lines.append(')
        w('            f\'- {r.get("entry_id")} | {r.get("company_name") or "?"} | \'')
        w('            f\'{r.get("media_channel") or "?"} | {(r.get("state") or "")} | \'')
        w('            f\'{L.clean_cell(r.get("headline"))[:180]} | \'')
        w('            f\'{L.clean_cell(r.get("ocr_text"))[:400]}\'')
        w('        )')
        w('    return "\\n".join(lines)')
        w('')
        w('')
        w('def _select(group, records):')
        w('    if not records:')
        w('        return {"entry_ids": []}')
        w('    system = SELECT_SYSTEM.replace("{guidance}", group.get("guidance") or "None.")')
        w('    prompt = (f\'Choose up to {group["want"]} pieces for the "{group["title"]}" \'')
        w('              f\'slide.\\n\\nCANDIDATES\\n{_shortlist(records)}\')')
        w('    try:')
        w('        return L.extract_json(L.call_claude(system, prompt))')
        w('    except Exception as exc:')
        w('        return {"error": str(exc), "entry_ids": []}')
        w('')
        w('')
        w('def _callout(group, chosen, true_count, cap_hit):')
        w('    if not chosen:')
        w('        return {"callout": ""}')
        w('    limit = group.get("callout_limit", 374)')
        w('    system = (CALLOUT_SYSTEM.replace("{limit}", str(limit))')
        w('              .replace("{style}", group.get("narrative_style") or "Plain analyst prose."))')
        w('    # The count is computed in Python and STATED to the model. A capped search')
        w('    # means the true total is unknown, so the model must say "at least N".')
        w('    count_note = (f"At least {true_count} pieces were found (the search hit its "')
        w('                  f"result cap, so the true total is higher). Phrase it as \'at "')
        w('                  f"least {true_count}\'." if cap_hit else')
        w('                  f"Exactly {true_count} piece(s) were found this period.")')
        w('    prompt = (f\'Slide: "{group["title"]}".\\n{count_note}\\n\\n\'')
        w('              f\'FEATURED PIECES\\n{_shortlist(chosen)}\')')
        w('    try:')
        w('        return L.extract_json(L.call_claude(system, prompt))')
        w('    except Exception as exc:')
        w('        return {"error": str(exc), "callout": ""}')
        w('')
        w('')

    if synth:
        w('def _synthesize(period_label, findings):')
        w('    """The LAST model call. It reads the FINISHED callouts rather than raw OCR —')
        w('    cheaper, and it cannot contradict what the deck already says."""')
        w('    prompt = (f"Period: {period_label}\\n\\nFINDINGS ALREADY WRITTEN INTO THE DECK\\n"')
        w('              + "\\n".join(findings)')
        w('              + \'\\n\\nReply with ONE JSON object: \'')
        w('                \'{"column1": "...", "column2": "..."}\')')
        w('    try:')
        w('        return L.extract_json(L.call_claude(SYNTH_SYSTEM, prompt))')
        w('    except Exception as exc:')
        w('        return {"error": str(exc)}')
        w('')
        w('')

    # ── main ────────────────────────────────────────────────────────────────────────
    w('# ── Pipeline ' + '─' * 65)
    w('def main() -> int:')
    w('    args = _parse_args()')
    w('    start, end = _period_window()')
    if kind == "week":
        w('    period_label = f"{end:%B} {_ordinal(end.day)}, {end.year}"')
    else:
        w('    period_label = f"{start:%B} {start.year}"')
    w('    stamp = end.strftime("%Y%m%d")')
    w('    mmddyy = end.strftime("%m%d%y")')
    w('    month_year = f"{start:%B}{start.year}"')
    w('    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)')
    w('    print(f"{CLIENT} — {period_label}")')
    w(f'    print(f"  window {{start}} .. {{end}}  (bounded by {win_field})")')
    w('    print(f"  mode --only={args.only}"')
    w('          + (f" --limit={args.limit}" if args.limit else ""))')
    w('')
    w('    # ── Step 1 — search, SEQUENTIALLY (see docstring guardrail) ─────────────')
    w('    total_calls = sum(len(g["channels"]) for g in GROUPS)')
    w('    print(f"\\nStep 1  Searching ({total_calls} group x channel calls, sequential)…")')
    w('    found = {}')
    w('    for group in GROUPS:')
    w('        limit = args.limit or group["limit"]')
    w('        records, cap_hit, err = [], False, None')
    w('        for channel in group["channels"]:')
    w('            rows, hit, e = _run_search(group, channel, limit)')
    w('            if e:')
    w('                err = e')
    w('                print(f"   ! {group[\'title\']} / {channel}: {e}")')
    w('                continue')
    w('            records.extend(rows)')
    w('            cap_hit = cap_hit or hit')
    w('        raw_n = len(records)')
    w('        records = _dedup(records)')
    w('        in_window = []')
    w('        for r in records:')
    w('            d = _entry_date(r.get("entry_id"))')
    w('            if d and start <= d <= end:')
    w('                in_window.append(r)')
    w('        print(f"   {group[\'title\'][:34]:34} {raw_n:>4} raw"')
    w('              f"{\' (CAP HIT)\' if cap_hit else \'\':10} -> {len(in_window):>4} in window")')
    w('        if cap_hit and not in_window:')
    w('            # All capped records missed the window: implausible for a real company.')
    w('            print(f"   !! SUSPECT: {group[\'title\']} hit the cap but 0 landed in the "')
    w('                  f"window. This is probably NOT a true zero — verify in PowerSearch "')
    w('                  f"before reporting it.")')
    w('        found[group["key"]] = {"group": group, "records": in_window,')
    w('                              "cap_hit": cap_hit, "error": err}')
    w('')
    w('    if not any(v["records"] for v in found.values()):')
    w('        print("\\nERROR: every group came back empty. Is the VPN up and the archive "')
    w('              "reachable? Aborting rather than shipping an empty report.")')
    w('        return 1')
    w('')

    if needs_enrich:
        w('    # ── Step 2 — SQL enrichment ─────────────────────────────────────────────')
        w('    print("\\nStep 2  Enriching entry_ids via SQL…")')
        w('    enriched, subcats_by_id = {}, {}')
        w('    for key, v in found.items():')
        w('        if not v["group"].get("enrich"):')
        w('            enriched[key] = []')
        w('            continue')
        w('        rows = _enrich([r["entry_id"] for r in v["records"]])')
        w('        for row in rows:')
        w('            subcats_by_id[row.get("entry_id")] = row.get("_sub_categories", "")')
        w('        enriched[key] = rows')
        w('        print(f"   {v[\'group\'][\'title\'][:34]:34} "')
        w('              f"{len(v[\'records\']):>4} ids -> {len(rows):>4} rows")')
        w('')
    else:
        w('    enriched, subcats_by_id = {}, {}')
        w('')

    w('    # ── Step 3 — preset hard-filters ────────────────────────────────────────')
    w('    print("\\nStep 3  Applying filters…")')
    w('    for key, v in found.items():')
    w('        if not v["group"].get("filters"):')
    w('            continue')
    w('        print(f"   {v[\'group\'][\'title\']}")')
    w('        v["records"] = _apply_filters(v["records"], v["group"], subcats_by_id)')
    w('')
    w('    if args.only == "search":')
    w('        print("\\n── Counts (sanity-check against PowerSearch) ──")')
    w('        for key, v in found.items():')
    w('            n = len(v["records"])')
    w('            print(f"   {v[\'group\'][\'title\'][:38]:38} "')
    w('                  f"{\'at least \' if v[\'cap_hit\'] else \'\'}{n}")')
    w('        return 0')
    w('')

    if sheets:
        w('    # ── Step 4 — the workbook ───────────────────────────────────────────────')
        w('    print("\\nStep 4  Writing the workbook…")')
        w('    sheet_specs = []')
        w('    for sh in SHEETS:')
        w('        rows = [row for k in sh["group_keys"] for row in enriched.get(k, [])]')
        w('        g0 = found.get(sh["group_keys"][0], {}).get("group", {}) if sh["group_keys"] else {}')
        w('        filter_row = (sh["filter_row"]')
        w('                      .replace("{sectors}", ", ".join(g0.get("sectors") or []))')
        w('                      .replace("{channels}", ", ".join(g0.get("channels") or []))')
        w('                      .replace("{keyword}", g0.get("keyword") or "")')
        w('                      .replace("{companies}", ", ".join(')
        w('                          sorted({c for k in sh["group_keys"]')
        w('                                  for c in (found.get(k, {}).get("group", {})')
        w('                                            .get("company_names") or [])})))')
        w('                      .replace("{start}", str(start))')
        w('                      .replace("{end}", str(end)))')
        w('        spec = {"name": sh["name"], "headers": sh["headers"], "rows": rows,')
        w('                "filter_row": filter_row, "hyperlinks": HYPERLINKS}')
        if uses_market:
            w('        if sh.get("highlight_market"):')
            w('            spec["header_fills"] = {"Market": MARKET_HEADER_COLOR}')
        w('        sheet_specs.append(spec)')
        w('        print(f"   {sh[\'name\'][:34]:34} {len(rows):>4} rows")')
        w('')
        fn = _param(excel, "filename", "{client}_{stamp}.xlsx") if excel else \
            "{client}_{stamp}.xlsx"
        w(f'    xlsx_name = ({_lit(fn)}')
        w('                 .replace("{client}", CLIENT.replace(" ", "_"))')
        w('                 .replace("{stamp}", stamp).replace("{mmddyy}", mmddyy)')
        w('                 .replace("{month_year}", month_year).replace("{period}", period_label))')
        w('    xlsx_path = L.write_workbook(OUTPUT_DIR / xlsx_name, sheet_specs)')
        w('    print(f"        saved {xlsx_path}")')
        w('')
        w('    if args.only == "excel":')
        w('        return 0')
        w('')
    else:
        w('    xlsx_path = None')
        w('')

    if needs_curate:
        w('    # ── Step 5 — selection, in parallel (Bedrock handles concurrency fine;')
        w('    #             it is only the archive REST layer that must stay sequential) ──')
        w('    curated = [g for g in GROUPS if g.get("want")]')
        w('    print(f"\\nStep 5  Selecting featured pieces ({len(curated)} parallel calls)…")')
        w('    sel_results = L.run_parallel([')
        w('        (lambda g=g: _select(g, found[g["key"]]["records"])) for g in curated')
        w('    ])')
        w('')
        w('    # Deterministic fixups. The model SUGGESTS; Python decides. pick_ids drops')
        w('    # hallucinated ids and tops up from the real pool.')
        w('    used_ids, final_ids = set(), {}')
        w('    for group, sel in zip(curated, sel_results):')
        w('        recs = found[group["key"]]["records"]')
        w('        sel = sel if isinstance(sel, dict) else {}')
        w('        if "error" in sel:')
        w('            print(f"   ! {group[\'title\']}: selection failed — {sel[\'error\']}")')
        w('        exclude = used_ids if group.get("cross_slide_dedupe") else None')
        w('        ids = L.pick_ids(sel.get("entry_ids"), recs, group["want"],')
        w('                         max_ids=group.get("max_shown", 5), exclude=exclude)')
        w('        if group.get("dedupe_company"):')
        w('            seen_co, kept = set(), []')
        w('            by_id = {r["entry_id"]: r for r in recs}')
        w('            for eid in ids:')
        w('                co = (by_id.get(eid, {}).get("company_name") or eid).lower()')
        w('                if co not in seen_co:')
        w('                    seen_co.add(co)')
        w('                    kept.append(eid)')
        w('            if len(kept) < len(ids):')
        w('                print(f"   ! {group[\'title\']}: dropped "')
        w('                      f"{len(ids) - len(kept)} same-company pick(s)")')
        w('            ids = kept')
        w('        used_ids.update(ids)')
        w('        final_ids[group["key"]] = ids')
        w('        print(f"   {group[\'title\'][:34]:34} {ids}")')
        w('')
        w('    # ── Step 6 — callouts, in parallel ──────────────────────────────────────')
        w('    print("\\nStep 6  Writing callouts…")')
        w('')
        w('    def _callout_job(group):')
        w('        ids = set(final_ids.get(group["key"], []))')
        w('        recs = [r for r in found[group["key"]]["records"]')
        w('                if r.get("entry_id") in ids]')
        w('        return _callout(group, recs, len(found[group["key"]]["records"]),')
        w('                        found[group["key"]]["cap_hit"])')
        w('')
        w('    callouts = {}')
        w('    call_results = L.run_parallel([(lambda g=g: _callout_job(g)) for g in curated])')
        w('    for group, cdata in zip(curated, call_results):')
        w('        cdata = cdata if isinstance(cdata, dict) else {}')
        w('        if "error" in cdata:')
        w('            print(f"   ! {group[\'title\']}: callout failed — {cdata[\'error\']}")')
        w('        # fit_text trims to WHOLE SENTENCES — never a mid-word ellipsis.')
        w('        text = L.fit_text(L.as_text(cdata.get("callout")),')
        w('                          group.get("callout_limit", 374))')
        w('        callouts[group["key"]] = text')
        w('        print(f"   {group[\'title\'][:34]:34} {len(text):>4} chars")')
        w('')

    if synth:
        w('    # ── Step 7 — synthesis: the LAST model call, reading finished callouts ──')
        w('    print("\\nStep 7  Final synthesis…")')
        w('    findings = [f"- {found[k][\'group\'][\'title\']}: {callouts.get(k, \'\')}"')
        w('                for k in SYNTH_GROUPS if found.get(k, {}).get("records")]')
        w('    if not findings:')
        w('        findings = ["(no activity in this period)"]')
        w('    synth = _synthesize(period_label, findings)')
        w('    if "error" in synth:')
        w('        print(f"   ! synthesis failed — {synth[\'error\']}")')
        w('    synth_col1 = L.cap_words(L.as_text(synth.get("column1")) or "No findings.",')
        w('                             SYNTH_MAX_WORDS)')
        w('    synth_col2 = L.cap_words(L.as_text(synth.get("column2")) or "No findings.",')
        w('                             SYNTH_MAX_WORDS)')
        w('')

    if deck:
        w('    # ── Step 8 — assemble and build the deck ────────────────────────────────')
        w('    print("\\nStep 8  Building the deck…")')
        w('    slides = []')
        w('    for item in DECK_PLAN:')
        w('        t = item["type"]')
        w('        title = (item.get("title") or "").replace("{client}", CLIENT) \\')
        w('                                        .replace("{period}", period_label)')
        w('        if t == "title":')
        w('            slides.append({"type": "title",')
        w('                           "data": {"title": title, "date": period_label}})')
        w('        elif t == "agenda":')
        w('            slides.append({"type": "agenda",')
        w('                           "data": {"sections": item.get("sections") or []}})')
        w('        elif t == "newSection":')
        w('            slides.append({"type": "newSection", "data": {"title": title}})')
        w('        elif t == "needToKnow":')
        if synth:
            w('            slides.append({"type": "needToKnow", "data": {')
            w('                "title1": SYNTH_TITLE1.replace("{period}", period_label),')
            w('                "text1": synth_col1,')
            w('                "title2": SYNTH_TITLE2.replace("{period}", period_label),')
            w('                "text2": synth_col2}})')
        else:
            w('            print("   ! needToKnow slide has no Synthesize node — skipped")')
        w('        elif t == "closing":')
        w('            slides.append({"type": "closing", "data": {}})')
        w('        elif t == "entry_ids":')
        w('            key = item.get("group")')
        w('            if not key or key not in found:')
        w('                print(f"   ! slide {title!r} has no group wired — skipped")')
        w('                continue')
        w('            ids = final_ids.get(key, [])')
        w('            if not ids:')
        w('                print(f"   ! {title}: no entries — slide skipped")')
        w('                continue')
        w('            text = callouts.get(key, "")')
        w('            # The builder holds SLIDE_CAP entries. Overflow becomes "(cont.)"')
        w('            # slides that repeat the same callout verbatim.')
        w('            chunks = (L.chunk_ids(ids, size=SLIDE_CAP) if item.get("chunk")')
        w('                      else [ids[:SLIDE_CAP]])')
        w('            for i, chunk in enumerate(chunks):')
        w('                slides.append({"type": "entry_ids", "data": {')
        w('                    "slideTitle": title + (" (cont.)" if i else ""),')
        w('                    "entryIds": chunk, "insight": text}})')
        w('        else:')
        w('            print(f"   ! unsupported slide type {t!r} — skipped")')
        w('')
        w('    print(f"   {len(slides)} slides")')
        dt = _param(deck, "deck_title", "{client} — {period}")
        w(f'    deck_title = ({_lit(dt)}')
        w('                  .replace("{client}", CLIENT).replace("{period}", period_label))')
        w('    result = build_deck_default(deck_title=deck_title, slides=slides)')
        w('')
        fn = _param(deck, "filename", "{client}_{stamp}.pptx")
        w(f'    pptx_name = ({_lit(fn)}')
        w('                 .replace("{client}", CLIENT.replace(" ", "_"))')
        w('                 .replace("{stamp}", stamp).replace("{mmddyy}", mmddyy)')
        w('                 .replace("{month_year}", month_year).replace("{period}", period_label))')
        w('    try:')
        w('        saved = L.save_pptx(result, OUTPUT_DIR / pptx_name)')
        w('    except RuntimeError as exc:')
        w('        print(f"ERROR: {exc}")')
        w('        print("       Check PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env and "')
        w('              "that csresearchhub.com is reachable.")')
        if sheets:
            w('        if xlsx_path:')
            w('            print(f"       (The workbook was still written: {xlsx_path})")')
        w('        return 1')
        w('    print(f"\\n  Deck:  {saved}")')
        if sheets:
            w('    if xlsx_path:')
            w('        print(f"  Excel: {xlsx_path}")')
        w('')

    if email:
        rn = _param(email, "report_name", "{client}")
        w('    # ── Email — opt-in only. A real send has inbox-facing consequences, so it')
        w('    #    happens only when the env var is present at run time. ────────────────')
        w('    if EMAIL_TO:')
        w(f'        report_name = {_lit(rn)}.replace("{{client}}", CLIENT)')
        w('        print(f"\\nEmailing deliverables to {EMAIL_TO}…")')
        attach = ('[p for p in (saved, xlsx_path) if p]' if (deck and sheets)
                  else ('[saved]' if deck else '[xlsx_path]'))
        w('        res = L.notify_report_ready(report_name=report_name,')
        w('                                    period_label=period_label,')
        w(f'                                    attachment_paths={attach},')
        w('                                    to_addr=EMAIL_TO)')
        w('        if res.get("status") == "sent":')
        w('            print(f"   sent (message_id={res.get(\'message_id\')})")')
        w('        else:')
        w('            print(f"   !! email FAILED: {res.get(\'error\')} — files are still "')
        w('                  f"saved locally, nothing lost")')
        w('    else:')
        var = _param(email, "to_env_var", "RS_EMAIL_TO")
        w(f'        print("\\nSkipped emailing — {var} is not set. Files saved locally only.")')
        w('')

    if notes:
        w('    print("\\n!! This pipeline has ENGINEER NOTES in its docstring — bespoke work "')
        w('          "is still required before it is production-ready.")')
    w('    print("\\nDone.")')
    w('    return 0')
    w('')
    w('')
    w('if __name__ == "__main__":')
    w('    sys.exit(main())')

    return "\n".join(L) + "\n", f"{_slug(client)}.py"


# ═══════════════════════════════════════════════════════════════════════════════════════
# Test runner — generates the file, then runs THAT file. One execution path.
# ═══════════════════════════════════════════════════════════════════════════════════════

RUNS: dict[str, dict] = {}
RUNS_LOCK = threading.Lock()


def _prune_test_files(keep: int = 5) -> None:
    """Test runs write a throwaway copy of the generated pipeline. Keep the last few
    (handy for debugging a failed run) and delete the rest."""
    try:
        old = sorted(GENERATED_DIR.glob("_test_*.py"), key=lambda p: p.stat().st_mtime)
        for p in old[:-keep]:
            p.unlink(missing_ok=True)
    except OSError:
        pass


def start_run(graph, mode, limit) -> str:
    run_id = uuid.uuid4().hex[:12]
    code, fname = codegen(graph)
    ast.parse(code)  # fail fast in the UI rather than as a subprocess traceback
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    _prune_test_files()
    target = GENERATED_DIR / f"_test_{run_id}_{fname}"
    target.write_text(code, encoding="utf-8")

    with RUNS_LOCK:
        RUNS[run_id] = {"lines": [], "done": False, "rc": None, "file": str(target)}

    def _log(msg):
        with RUNS_LOCK:
            RUNS[run_id]["lines"].append(msg)

    def _worker():
        if PIPELINES_DIR is None:
            _log("ERROR: report_lib.py was not found, so the generated pipeline cannot "
                 "import it.")
            _log("Move report_studio.py into the project root (next to pipelines/) and "
                 "restart.")
            with RUNS_LOCK:
                RUNS[run_id]["done"], RUNS[run_id]["rc"] = True, 1
            return
        cmd = [sys.executable, "-u", str(target), "--only", mode]
        if limit:
            cmd += ["--limit", str(limit)]
        _log(f"$ {' '.join(cmd)}")
        _log(f"(generated file: {target})")
        _log("")
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    encoding="utf-8", errors="replace",
                                    cwd=str(PIPELINES_DIR.parent))
            for line in proc.stdout:
                _log(line.rstrip("\n"))
            proc.wait()
            rc = proc.returncode
        except Exception as exc:
            _log(f"RUNNER ERROR: {exc}")
            rc = 1
        with RUNS_LOCK:
            RUNS[run_id]["done"], RUNS[run_id]["rc"] = True, rc

    threading.Thread(target=_worker, daemon=True).start()
    return run_id


# ═══════════════════════════════════════════════════════════════════════════════════════
# Web UI
# ═══════════════════════════════════════════════════════════════════════════════════════

HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Pipelines Studio</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#151821;--panel:#1d2130;--line:#2e3448;--ink:#e6e9f2;--dim:#8b93ab;
--accent:#5b8def;--ok:#3fa96b;--warn:#d2a03c;--err:#d2564b}
body{background:var(--bg);color:var(--ink);font:13px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;height:100vh;display:flex;flex-direction:column;overflow:hidden}
#bar{display:flex;gap:8px;align-items:center;padding:8px 12px;background:var(--panel);border-bottom:1px solid var(--line);flex-wrap:wrap}
#bar b{font-size:14px;letter-spacing:.3px;margin-right:6px}
button,select,input,textarea{font:inherit;color:var(--ink);background:#262c3d;border:1px solid var(--line);border-radius:6px;padding:5px 9px}
button{cursor:pointer}button:hover{border-color:var(--accent)}
button.primary{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
button.warn{background:#3a2d20;border-color:#5c4526}
#main{flex:1;display:flex;min-height:0}
#palette{width:180px;background:var(--panel);border-right:1px solid var(--line);overflow:auto;padding:8px}
#palette h4{font-size:10px;text-transform:uppercase;color:var(--dim);margin:8px 4px 6px;letter-spacing:.8px}
.pitem{display:flex;align-items:center;gap:7px;padding:6px 8px;border-radius:6px;cursor:pointer;margin-bottom:2px}
.pitem:hover{background:#262c3d}.dot{width:9px;height:9px;border-radius:50%;flex:none}
#wrap{flex:1;position:relative;overflow:auto;background:
radial-gradient(circle,#232838 1px,transparent 1px) 0 0/22px 22px}
#canvas{position:relative;width:4200px;height:3600px}
#wires{position:absolute;inset:0;pointer-events:none;overflow:visible}
.node{position:absolute;width:186px;background:var(--panel);border:1px solid var(--line);
border-radius:9px;box-shadow:0 3px 12px #0007;user-select:none}
.node.sel{border-color:var(--accent);box-shadow:0 0 0 2px #5b8def55,0 3px 12px #0007}
.nhead{display:flex;align-items:center;gap:6px;padding:6px 9px;border-bottom:1px solid var(--line);
border-radius:8px 8px 0 0;cursor:move;font-weight:600;font-size:12px}
.nbody{padding:6px 9px;font-size:11px;color:var(--dim);word-break:break-word;min-height:20px}
.badge{position:absolute;top:-9px;right:-9px;background:var(--accent);color:#fff;font-size:10px;
font-weight:700;border-radius:10px;padding:1px 6px}
.port{position:absolute;width:13px;height:13px;border-radius:50%;background:#39405a;
border:2px solid var(--line);cursor:crosshair;top:50%;transform:translateY(-50%)}
.port:hover{background:var(--accent);border-color:#fff}
.port.in{left:-8px}.port.out{right:-8px}
.port.armed{background:var(--ok);border-color:#fff}
#side{width:330px;background:var(--panel);border-left:1px solid var(--line);overflow:auto;padding:11px}
#side h3{font-size:12px;text-transform:uppercase;color:var(--dim);letter-spacing:.8px;margin-bottom:4px}
#side .blurb{font-size:11px;color:var(--dim);margin-bottom:10px;line-height:1.45}
.fld{margin-bottom:10px}.fld label{display:block;font-size:11px;color:var(--dim);margin-bottom:3px}
.fld input,.fld select,.fld textarea{width:100%}
.fld textarea{min-height:62px;resize:vertical;font-family:ui-monospace,monospace;font-size:11px}
.fld .help{font-size:10px;color:#727a94;margin-top:3px;line-height:1.4}
.chips{display:flex;flex-wrap:wrap;gap:4px}
.chip{padding:2px 7px;border:1px solid var(--line);border-radius:11px;cursor:pointer;font-size:11px}
.chip.on{background:var(--accent);border-color:var(--accent);color:#fff}
#log{height:190px;background:#101320;border-top:1px solid var(--line);overflow:auto;
padding:8px 12px;font:11px/1.55 ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap}
.e{color:#ff8b80}.w{color:#e6bf6a}.o{color:#7fd6a0}.d{color:var(--dim)}
#status{margin-left:auto;font-size:11px;color:var(--dim)}
#logo{position:fixed;top:8px;right:12px;height:34px;z-index:1000;opacity:.92;pointer-events:none}
</style></head><body>
<img id="logo" src="data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBT/wAARCAIvAq8DASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD9U6KKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACivjL/AIKg/EbxF4L+EvhbSfD+r3miJr2sNb38+nztBNLbpbyuYfMUhlVn2btpBIXaTtZgfzBXV9VVj/xOdU55P+nzDn/vuvWw2XzxFP2idkediMbGhLkauf0F0V/Pt/bWrf8AQa1X/wAGM/8A8XR/bWrf9BrVf/BhP/8AF11/2PP+f8Dm/tOP8v4n9BNFfz7HWdWx/wAhrU//AAPn/wDi6Qa1q3/Qa1T/AMGM/wD8XR/Y8/5/wD+04/y/if0FUV/Pt/bWrf8AQa1P/wAD5/8A4unjW9Wx/wAhjU//AAPn/wDi6P7Hn/P+Af2nH+X8T+gaiv5/IPEet2dxFPb+Idatp4mEkc1vqlzG8bA5DKwcEEHkEV+0/wCyX471j4l/s4+AvEfiC5+261d6ftu7vaFM8kbtEZGA4DNs3HGBknAA4rgxWClhUpN3TOzD4uOIbSVrHrlFFFeadwUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAFFFFABRRRQAUUUUAfMX7fn7PXiP4/fC3Rk8IwxXviDQdTF/Hp8sqRG8iaJ4pI0dyFV/nVhuYKdhBIzkflX4++HHin4U+If7A8Y6Hc+HdZNul2LO6lidjC7OqOGid1ILRuOuflr98a/Jz/AIKf/wDJ0tp/2Kmn/wDpVf19BlWIlzewe254+YUIuDq9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4FEf8kj8H/8Agnh/+Jpv/DGXwJ/6JH4Q/wDBRD/8TXb/AGvT/lf4HL/Zj/m/A/FHP+zRmv2u/wCGMvgT/wBEj8If+CiH/wCJpf8AhjP4E/8ARI/B3/gnh/8AiaX9rw/lf4B/Zj/m/A/FD/gJpydR8pr9rf8AhjP4E/8ARI/B/wD4J4f/AImj/hjP4E/9Ek8If+CiH/4mms4h/K/wD+zP734H4scZGVb8sV+yv7C1rNafslfDVZ43jZ9OaZA4xujeaR42HsyMpHsRWpH+xt8C4pUkHwj8HFlIYBtGgZT9QVwR7GvYIokgiSONFjjQBVRBgKB0AHYV5uOxyxUVFK1jswuD+rScr3uPoooryD0gooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACivkrxF/wUy+FPhrxJrGiXWl+KpLrSr640+d4bCEoZIZWifaTOCRuRsHA4qiP+CpXwiP8AzCfF3/gvg/8Aj9dSwtdq6gzneIpJ2ckfYdFfHh/4Kk/CTH/IH8Xf+AFv/wDJFT2f/BUL4PXEoW4s/FNhGess2mI4H4Ryu36U/qlf+R/cL6zR/mR9eUV438Mf2wPhB8XdRt9M8O+N7A6xcHbDpeoq9jdzNz8scU6o0h4Jwm7ivZK55QlB2krG6kpK6YUUVyHxa+J+k/Bn4e6v4y1yG7uNL0xY2misI1eZt8ixjarMoPLjqRxmpSbdkNtJXZ19FfHS/wDBUv4TP00Pxh+Nhbf/ACRX0x8KPiXpfxi+HeieM9Ehu7fStXhM8EV8ipMqhmX5lVmAOVPQmtalGpSV5xaM4VYVHaLudbRRXzn8av26/h/8B/iDdeDvEOmeIrnVLe2huml060hkhKShioBaZTkbTnj8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jCirQVgooornNgooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooA//9k=" alt="Pipelines">
<div id="bar">
  <b>Report Studio</b>
  <input id="gname" style="width:190px" placeholder="graph name">
  <select id="tpl"></select><button onclick="loadTpl()">Load template</button>
  <button onclick="validateGraph()">Validate</button>
  <button onclick="showCode()">Generate code</button>
  <select id="mode">
    <option value="search">Test: searches only</option>
    <option value="excel">Test: + workbook</option>
    <option value="deck">Test: full run</option>
  </select>
  <input id="tlimit" type="number" value="10" style="width:62px" title="limit per search">
  <button class="primary" onclick="runTest()">Run test</button>
  <button class="warn" onclick="exportPy()">Export .py</button>
  <button onclick="saveGraph()">Save</button>
  <select id="saved"></select><button onclick="loadSaved()">Open</button>
  <button onclick="delSel()">Delete node</button>
  <span id="status"></span>
</div>
<div id="main">
  <div id="palette"><h4>Add node</h4><div id="plist"></div>
    <h4 style="margin-top:14px">Connecting</h4>
    <div style="font-size:11px;color:var(--dim);padding:0 4px;line-height:1.5">
      Click a right-hand port, then a left-hand port to wire. Click a wire to delete it.
      Drag headers to move. Slide badges show deck order.</div>
  </div>
  <div id="wrap"><div id="canvas"><svg id="wires"></svg></div></div>
  <div id="side"><div class="blurb">Select a node to edit it.</div></div>
</div>
<div id="log"><span class="d">Ready. Load a template to see a working graph.</span></div>
<script>
let SPEC=null, G={name:"untitled",nodes:[],edges:[]}, sel=null, armed=null, poll=null, seq=1;
const $=s=>document.querySelector(s);

async function boot(){
  SPEC=await (await fetch("/api/spec")).json();
  $("#tpl").innerHTML=Object.entries(SPEC.templates).map(([k,v])=>`<option value="${k}">${v}</option>`).join("");
  $("#plist").innerHTML=Object.entries(SPEC.nodes).map(([t,s])=>
    `<div class="pitem" onclick="addNode('${t}')"><span class="dot" style="background:${s.color}"></span>${s.label}</div>`).join("");
  await refreshSaved(); loadTpl();
}
async function refreshSaved(){
  const r=await (await fetch("/api/graphs")).json();
  $("#saved").innerHTML=r.graphs.length?r.graphs.map(n=>`<option>${n}</option>`).join("")
    :'<option value="">(none saved)</option>';
}
function log(t,cls){const d=$("#log");d.innerHTML+=`\n<span class="${cls||''}">${esc(t)}</span>`;d.scrollTop=d.scrollHeight}
function clearLog(){$("#log").innerHTML=""}
function esc(s){return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;")}
function uid(t){return t+"_"+(seq++)+Math.random().toString(36).slice(2,5)}

async function loadTpl(){
  const r=await (await fetch("/api/template?name="+$("#tpl").value)).json();
  G=r.graph; $("#gname").value=G.name; sel=null; draw(); validateGraph();
}
function addNode(t){
  const s=SPEC.nodes[t];
  if(s.max && G.nodes.filter(n=>n.type===t).length>=s.max){log("Only "+s.max+" "+s.label+" node allowed.","w");return}
  const p={}; (s.fields||[]).forEach(f=>p[f.key]=f.default);
  const w=$("#wrap");
  G.nodes.push({id:uid(t),type:t,x:w.scrollLeft+300,y:w.scrollTop+120,params:p});
  draw();
}
function delSel(){
  if(!sel){log("No node selected.","w");return}
  G.nodes=G.nodes.filter(n=>n.id!==sel);
  G.edges=G.edges.filter(e=>e.from!==sel&&e.to!==sel);
  sel=null; draw(); validateGraph();
}

function draw(){
  const c=$("#canvas");
  [...c.querySelectorAll(".node")].forEach(n=>n.remove());
  const order=slideOrder();
  G.nodes.forEach(n=>{
    const s=SPEC.nodes[n.type]||{label:n.type,color:"#666",fields:[]};
    const d=document.createElement("div");
    d.className="node"+(sel===n.id?" sel":""); d.id="N"+n.id;
    d.style.left=n.x+"px"; d.style.top=n.y+"px";
    const pos=order.indexOf(n.id);
    d.innerHTML=`${n.type==="slide"&&pos>=0?`<div class="badge">${pos+1}</div>`:""}
      <div class="nhead" style="background:${s.color}22;border-bottom-color:${s.color}55">
        <span class="dot" style="background:${s.color}"></span>${s.label}</div>
      <div class="nbody">${esc(summary(n))}</div>
      ${s.inputs?'<div class="port in" title="input"></div>':""}
      ${s.outputs?'<div class="port out" title="output"></div>':""}`;
    d.querySelector(".nhead").onmousedown=ev=>startDrag(ev,n);
    d.onclick=ev=>{if(!ev.target.classList.contains("port")){sel=n.id;draw()}};
    const po=d.querySelector(".port.out"), pi=d.querySelector(".port.in");
    if(po)po.onclick=ev=>{ev.stopPropagation();armed=n.id;draw();log("Wiring from "+summary(n)+" — now click a left-hand port.","d")};
    if(pi)pi.onclick=ev=>{ev.stopPropagation();connect(n.id)};
    if(armed===n.id&&po)po.classList.add("armed");
    c.appendChild(d);
  });
  wires();
  side();
}
function summary(n){
  const p=n.params||{};
  switch(n.type){
    case "period": return `${p.client||"?"} · ${p.kind} · ${p.window_field}`;
    case "search": return `${p.title||p.group_key||"?"}\n${(p.channels||[]).length} channel(s)`;
    case "filter": return p.preset||"?";
    case "enrich": return "SQL · "+(p.window_field||"none");
    case "curate": return `want ${p.want} · cap ${p.max_shown}`;
    case "sheet": return `${p.name||"?"} · ${p.headers_preset||""}`;
    case "slide": return `${p.slide_type}\n${p.title||""}`;
    case "synthesize": return "final LLM call";
    case "deck": return p.filename||"deck";
    case "excel": return p.filename||"workbook";
    case "email": return "env: "+(p.to_env_var||"?");
    case "note": return (p.text||"").slice(0,90)+((p.text||"").length>90?"…":"");
  }
  return "";
}
function connect(to){
  if(!armed){log("Click an output port first.","w");return}
  if(armed===to){armed=null;draw();return}
  if(!G.edges.some(e=>e.from===armed&&e.to===to)) G.edges.push({from:armed,to:to});
  armed=null; draw(); validateGraph();
}
function portXY(id,side){
  const el=document.getElementById("N"+id); if(!el)return null;
  const x=el.offsetLeft+(side==="out"?el.offsetWidth:0), y=el.offsetTop+el.offsetHeight/2;
  return [x,y];
}
function wires(){
  const svg=$("#wires"); svg.innerHTML="";
  G.edges.forEach((e,i)=>{
    const a=portXY(e.from,"out"), b=portXY(e.to,"in"); if(!a||!b)return;
    const dx=Math.max(34,Math.abs(b[0]-a[0])/2);
    const p=document.createElementNS("http://www.w3.org/2000/svg","path");
    p.setAttribute("d",`M${a[0]},${a[1]} C${a[0]+dx},${a[1]} ${b[0]-dx},${b[1]} ${b[0]},${b[1]}`);
    p.setAttribute("stroke","#4a5170");
    p.setAttribute("fill","none"); p.setAttribute("stroke-width","2");
    p.style.pointerEvents="stroke"; p.style.cursor="pointer";
    p.onmouseenter=()=>p.setAttribute("stroke","#d2564b");
    p.onmouseleave=()=>p.setAttribute("stroke","#4a5170");
    p.onclick=()=>{G.edges.splice(i,1);draw();validateGraph()};
    svg.appendChild(p);
  });
}
function startDrag(ev,n){
  ev.preventDefault(); sel=n.id; draw();
  const sx=ev.clientX, sy=ev.clientY, ox=n.x, oy=n.y;
  const mv=e=>{n.x=Math.max(0,ox+e.clientX-sx); n.y=Math.max(0,oy+e.clientY-sy);
    const el=document.getElementById("N"+n.id); el.style.left=n.x+"px"; el.style.top=n.y+"px"; wires()};
  const up=()=>{document.removeEventListener("mousemove",mv);document.removeEventListener("mouseup",up);draw()};
  document.addEventListener("mousemove",mv); document.addEventListener("mouseup",up);
}
function slideOrder(){
  return G.nodes.filter(n=>n.type==="slide")
    .sort((a,b)=>(Math.round(a.y/60)-Math.round(b.y/60))||(a.x-b.x)).map(n=>n.id);
}

function side(){
  const box=$("#side");
  const n=G.nodes.find(x=>x.id===sel);
  if(!n){box.innerHTML='<div class="blurb">Select a node to edit it.</div>';return}
  const s=SPEC.nodes[n.type];
  let h=`<h3>${s.label}</h3><div class="blurb">${esc(s.blurb||"")}</div>`;
  (s.fields||[]).forEach(f=>{
    const v=n.params[f.key]!==undefined?n.params[f.key]:f.default;
    h+=`<div class="fld"><label>${esc(f.label)}</label>`;
    if(f.kind==="textarea") h+=`<textarea data-k="${f.key}">${esc(v==null?"":v)}</textarea>`;
    else if(f.kind==="number") h+=`<input type="number" data-k="${f.key}" value="${v==null?"":v}">`;
    else if(f.kind==="bool") h+=`<select data-k="${f.key}"><option value="1"${v?" selected":""}>yes</option><option value="0"${v?"":" selected"}>no</option></select>`;
    else if(f.kind==="select") h+=`<select data-k="${f.key}">`+f.options.map(o=>
      `<option value="${esc(o)}"${String(v)===String(o)?" selected":""}>${esc(o===""?"(any)":o)}</option>`).join("")+`</select>`;
    else if(f.kind==="multiselect"){
      const on=Array.isArray(v)?v:[];
      h+=`<div class="chips" data-multi="${f.key}">`+f.options.map(o=>
        `<span class="chip${on.includes(o)?" on":""}" data-v="${esc(o)}">${esc(o)}</span>`).join("")+`</div>`;
    }
    else h+=`<input data-k="${f.key}" value="${esc(v==null?"":v)}">`;
    if(f.help) h+=`<div class="help">${esc(f.help)}</div>`;
    h+=`</div>`;
  });
  box.innerHTML=h;
  box.querySelectorAll("[data-k]").forEach(el=>{
    el.oninput=el.onchange=()=>{
      const f=(s.fields||[]).find(x=>x.key===el.dataset.k);
      let val=el.value;
      if(f.kind==="number") val=val===""?null:Number(val);
      if(f.kind==="bool") val=el.value==="1";
      n.params[el.dataset.k]=val;
      const nd=document.getElementById("N"+n.id);
      if(nd) nd.querySelector(".nbody").textContent=summary(n);
    };
  });
  box.querySelectorAll("[data-multi]").forEach(box2=>{
    box2.querySelectorAll(".chip").forEach(ch=>{
      ch.onclick=()=>{
        const k=box2.dataset.multi;
        let arr=Array.isArray(n.params[k])?n.params[k].slice():[];
        const v=ch.dataset.v;
        arr.includes(v)?arr=arr.filter(x=>x!==v):arr.push(v);
        n.params[k]=arr; ch.classList.toggle("on");
        const nd=document.getElementById("N"+n.id);
        if(nd) nd.querySelector(".nbody").textContent=summary(n);
      };
    });
  });
}

function payload(){G.name=$("#gname").value||"untitled";return JSON.stringify({graph:G})}
async function post(url,body){
  const r=await fetch(url,{method:"POST",headers:{"Content-Type":"application/json"},body:body});
  return await r.json();
}
async function validateGraph(){
  const r=await post("/api/validate",payload());
  clearLog();
  r.errors.forEach(e=>log("ERROR   "+e,"e"));
  r.warnings.forEach(e=>log("WARNING "+e,"w"));
  if(!r.errors.length&&!r.warnings.length) log("Valid. No warnings.","o");
  $("#status").textContent=`${G.nodes.length} nodes · ${r.groups.length} groups · `+
    `${r.errors.length} errors · ${r.warnings.length} warnings`;
  return r.errors.length===0;
}
async function showCode(){
  const r=await post("/api/codegen",payload());
  if(r.error){clearLog();log("CODEGEN FAILED: "+r.error,"e");return}
  clearLog(); log("── generated "+r.filename+" ──","o"); log(r.code);
}
async function exportPy(){
  if(!await validateGraph()){log("\nFix the errors above before exporting.","e");return}
  const r=await post("/api/export",payload());
  if(r.error){log("EXPORT FAILED: "+r.error,"e");return}
  log("\nWrote "+r.path,"o");
  log("Send that file to Engineering to deploy and schedule.","d");
}
async function saveGraph(){
  const r=await post("/api/graphs/save",payload());
  if(r.error){log("SAVE FAILED: "+r.error,"e");return}
  log("Saved graph as "+r.name,"o"); refreshSaved();
}
async function loadSaved(){
  const n=$("#saved").value; if(!n)return;
  const r=await (await fetch("/api/graphs/load?name="+encodeURIComponent(n))).json();
  if(r.error){log("LOAD FAILED: "+r.error,"e");return}
  G=r.graph; $("#gname").value=G.name||n; sel=null; draw(); validateGraph();
}
async function runTest(){
  if(!await validateGraph()){log("\nFix the errors above before testing.","e");return}
  const mode=$("#mode").value, lim=$("#tlimit").value;
  const r=await post("/api/test",JSON.stringify({graph:G,mode:mode,limit:lim?Number(lim):null}));
  if(r.error){log("TEST FAILED TO START: "+r.error,"e");return}
  clearLog(); log("Running the GENERATED pipeline (this is the same file you export)…","o");
  if(poll)clearInterval(poll);
  let seen=0;
  poll=setInterval(async()=>{
    const s=await (await fetch("/api/test/status?id="+r.run_id)).json();
    s.lines.slice(seen).forEach(l=>log(l));
    seen=s.lines.length;
    if(s.done){clearInterval(poll);poll=null;
      log("\n── finished, exit code "+s.rc+" ──",s.rc===0?"o":"e")}
  },700);
}
boot();
</script></body></html>
"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code, body: bytes, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj).encode("utf-8"))

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path in ("/", "/index.html"):
            return self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
        if u.path == "/api/spec":
            return self._json({
                "nodes": NODE_SPECS,
                "templates": {k: v().get("name", k) for k, v in TEMPLATES.items()},
            })
        if u.path == "/api/template":
            name = (q.get("name") or ["minimal"])[0]
            if name not in TEMPLATES:
                return self._json({"error": "unknown template"}, 404)
            return self._json({"graph": TEMPLATES[name]()})
        if u.path == "/api/graphs":
            GRAPHS_DIR.mkdir(parents=True, exist_ok=True)
            return self._json({"graphs": sorted(p.stem for p in GRAPHS_DIR.glob("*.json"))})
        if u.path == "/api/graphs/load":
            name = (q.get("name") or [""])[0]
            p = GRAPHS_DIR / f"{_slug(name)}.json"
            if not p.is_file():
                return self._json({"error": "not found"}, 404)
            return self._json({"graph": json.loads(p.read_text("utf-8"))})
        if u.path == "/api/test/status":
            rid = (q.get("id") or [""])[0]
            with RUNS_LOCK:
                r = RUNS.get(rid)
                if not r:
                    return self._json({"error": "unknown run"}, 404)
                return self._json({"lines": list(r["lines"]), "done": r["done"],
                                   "rc": r["rc"]})
        return self._json({"error": "not found"}, 404)

    def do_POST(self):
        u = urlparse(self.path)
        try:
            body = self._body()
        except Exception as exc:
            return self._json({"error": f"bad JSON: {exc}"}, 400)
        graph = body.get("graph") or {}

        if u.path == "/api/validate":
            return self._json(validate(graph))
        if u.path == "/api/codegen":
            try:
                code, fname = codegen(graph)
                return self._json({"code": code, "filename": fname})
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/export":
            try:
                code, fname = codegen(graph)
                ast.parse(code)  # never hand out a file that will not import
                GENERATED_DIR.mkdir(parents=True, exist_ok=True)
                p = GENERATED_DIR / fname
                p.write_text(code, encoding="utf-8")
                return self._json({"path": str(p)})
            except SyntaxError as exc:
                return self._json({"error": f"generated code did not parse: {exc}"}, 500)
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/test":
            try:
                rid = start_run(graph, body.get("mode") or "search", body.get("limit"))
                return self._json({"run_id": rid})
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/graphs/save":
            GRAPHS_DIR.mkdir(parents=True, exist_ok=True)
            name = _slug(graph.get("name") or "untitled")
            (GRAPHS_DIR / f"{name}.json").write_text(
                json.dumps(graph, indent=2), encoding="utf-8")
            return self._json({"name": name})
        return self._json({"error": "not found"}, 404)


# ═══════════════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════════════

def _json_isms(tree) -> list[str]:
    """Find bare true/false/null identifiers.

    These are the signature of using json.dumps() where a Python literal was
    needed. They are syntactically legal identifiers, so ast.parse() accepts
    them happily and the file only dies with NameError once someone runs it.
    Caught here instead.
    """
    return sorted({n.id for n in ast.walk(tree)
                   if isinstance(n, ast.Name) and n.id in {"true", "false", "null"}})


def _undefined_names(tree, code: str) -> list[str]:
    """Names referenced at module level that are never bound anywhere in the file.
    Catches emitters that reference a constant only generated on some branches
    (e.g. a HEADERS_* block that a graph shape happens to skip)."""
    bound = {n.name for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
            bound.add(n.id)
        elif isinstance(n, ast.alias):
            bound.add((n.asname or n.name).split(".")[0])
        elif isinstance(n, ast.arg):
            bound.add(n.arg)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
    interesting = re.compile(r"^(HEADERS_|SYNTH_|HYPERLINKS$|MARKET_|IN_MARKET_)")
    used = {n.id for n in ast.walk(tree)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
            and interesting.match(n.id)}
    return sorted(used - bound)


def selftest() -> int:
    """Codegen every template, validate it, parse it, and screen the AST for the
    failure modes that parsing alone cannot see."""
    bad = 0
    for name, fn in TEMPLATES.items():
        g = fn()
        v = validate(g)
        code, fname = codegen(g)
        notes = []
        try:
            tree = ast.parse(code)
            notes.append("parses")
            isms = _json_isms(tree)
            if isms:
                notes.append(f"JSON-ISM {isms}")
                bad += 1
            undef = _undefined_names(tree, code)
            if undef:
                notes.append(f"UNDEFINED {undef}")
                bad += 1
        except SyntaxError as exc:
            notes.append(f"SYNTAX ERROR line {exc.lineno}: {exc.msg}")
            bad += 1
        print(f"{name:22} nodes={len(g['nodes']):>3} groups={len(v['groups']):>2} "
              f"errors={len(v['errors'])} warnings={len(v['warnings'])} "
              f"lines={len(code.splitlines()):>4} -> {', '.join(notes)}")
        for e in v["errors"]:
            print(f"    ERROR  {e}")
            bad += 1
    print("\nSELFTEST", "FAILED" if bad else "PASSED")
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Report Studio — node editor for Competiscan "
                                             "report pipelines")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--selftest", action="store_true",
                    help="Codegen + ast-parse every template, then exit.")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    print("Report Studio")
    print(f"  project root    : {PIPELINES_DIR.parent if PIPELINES_DIR else '(not found)'}")
    print(f"  pipelines/      : {PIPELINES_DIR or '(not found — Test disabled)'}")
    print(f"  generated to    : {GENERATED_DIR}")
    if PIPELINES_DIR is None:
        print("\n  ! report_lib.py was not found next to this script. The editor, codegen")
        print("    and export all work, but Test cannot run the generated pipeline.")
        print("    Put report_studio.py in the project root (beside pipelines/).")
    print(f"\n  open http://{args.host}:{args.port}\n")
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
    return 0


if __name__ == "__main__":
    sys.exit(main())
