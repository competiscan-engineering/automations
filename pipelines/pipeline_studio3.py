#!/usr/bin/env python3
"""
pipeline_studio3.py — build a Competiscan trend report without writing code (v3)
═══════════════════════════════════════════════════════════════════════════════════════

WHAT CHANGED FROM v2, AND WHY
    v2 searched through mcp_serverv4.search_archive, which accepts five filters and caps
    at ~200 rows per call. Everything a researcher actually needed was therefore bolted
    on in Python afterwards: a regex over company names to approximate "credit unions
    only", a client-side date filter parsed out of entry_id, a SQL round-trip to fake
    sub-category filtering, and an "at least N" fudge because the response carried no
    total.

    v3 talks to platform-api.competiscan.com, which publishes the whole PowerSearch
    surface: 80 filters over 9 groups, live vocabulary, the sector/category tree, exact
    counts, and server-side date windowing. Every one of those four hacks is deleted,
    not ported.

        v2                                        v3
        --------------------------------------    ----------------------------------
        _CU_RE regex on company_name              credit_union: true
        client-side window on entry_id            date_field + date_from/date_to
        sub-category via a SQL round-trip         subcategory taxonomy filter
        company_must_match regex                  company_match: "contains"
        "at least N" when a call hit its cap      total / truncated / total_is_capped
        hardcoded 8 channels, 9 sectors           GET /v1/filters, GET /v1/taxonomy

WHAT DID NOT CHANGE
    The shape of a report. Every section is still the same four steps, never wired:

        SEARCH  ->  WORKSHEET  ->  FEATURE  ->  SLIDE

    And the one architectural rule: the Studio does NOT run an interpreter. It generates
    the .py and runs THAT file, so what a researcher runs is exactly what Engineering
    deploys. Every run mode preserves it, including the one that pauses halfway.

TWO JOBS, ONE STUDIO
    A researcher picks, per report, which of these they are doing. Both are first-class
    and neither is the other with something bolted on.

      An ONGOING report    designed here, handed to Engineering, deployed, scheduled.
                           The deliverable is the generated .py file, and the thing that
                           matters most is reproducibility: it must produce the same
                           window every time it runs unattended, months from now.
                           Finished by  ->  Send to Eng.

      A ONE-TIME report    a deck or a workbook needed now, for a one-off client ask.
                           Nothing is deployed and nothing is scheduled. The deliverable
                           is the files, and what matters is speed, control, and being
                           able to look at the output.
                           Finished by  ->  Run

    Nothing asks which job you are doing. It is inferred from the settings that actually
    differ — a fixed date range versus a cadence, a recipient typed at run time versus a
    variable Engineering sets — because those are choices a researcher makes for a real
    reason, and asking a second time is asking the same question twice. Both terminal
    actions stay available on every report; a report that was never sent to Engineering
    is not unfinished, it may simply never need to be.

THE FOUR RUN MODES ARE A LADDER
    Each rung does everything the one above does and a little more. A one-time report
    climbs it to a finished deliverable in one sitting; an ongoing one climbs the same
    rungs to satisfy itself it is right before being sent.

      1  Run the searches                just the searches, so you can see what the
                                         archive holds before spending anything
      2  Run the workbook                + the database lookup and the .xlsx
      3  Run and edit the deliverables   + the pick, then it STOPS and shows you what it
                                         chose. Swap what you do not want; the deck is
                                         written from what you keep
      4  Run the pipeline                start to finish, no stops. What a schedule does

    No mode passes a row limit. The Studio used to inject a small one to keep testing
    quick, which meant a researcher never saw what their own row caps did until
    Engineering ran it for real. The section limits are the only limits in effect, and
    the request estimate in the run bar is how the cost is made visible instead.

HOW MODE 3 PAUSES WITHOUT A SECOND EXECUTION PATH
    The pause is inside the GENERATED FILE, not in this process. The pipeline gains a
    resumable two-phase mode backed by a run-state file:

        --phase pick  --state s.json        search, workbook, choose, write s.json, exit
        --phase build --state s.json --approved a.json    write-ups, deck, email
        --phase replace --state s.json --section <id> --keep a,b --reject c

    build never searches: it reads the records pick already fetched, so approving costs
    nothing and rejecting costs a replacement drawn from that same pool. And the
    write-up moved into build on purpose — generated next to the pick, it would describe
    pieces the researcher then rejected.

THREE SOURCES, AND WHICH NEEDS WHAT
    Filtering is 100% API — no tunnel is involved in choosing rows, ever.

    Printing is where it splits. The search response is a fixed 15 columns with no
    output projection, so anything else has to be fetched:

      POST /v1/search/enhanced   the 15 columns, free with the search
      GET  /v1/ocr?entry_id=     the scanned text, ONE REQUEST PER PIECE
      the database (tunnel)      State/Province, Additional Companies, the taxonomy
                                 names, Age, Income, Pre-Screen, Mortgage app type,
                                 Social ad type

    Only that last row needs the tunnel now. The scanned text used to be read out of
    cscan_document_text_search; it is an endpoint, so a deck-only report needs no
    database at all.

    What the OCR endpoint does cost is a request per piece, with no batch form — so the
    pipeline spends them where they change the output. An agent CHOOSES from the product
    name and headline, which on this archive is usually a whole sentence of offer detail;
    it then READS the page for the three-to-five pieces it actually chose, because the
    rate and the term are on the page rather than in the headline. Printing the text as
    a worksheet column reads every row of the tab, so that one is capped.

RUN
    python pipelines/pipeline_studio3.py               # then open http://127.0.0.1:8788
    python pipelines/pipeline_studio3.py --selftest    # codegen + screen every shape
    python pipelines/pipeline_studio3.py --selftest --offline  # also RUN every saved
                                                       # report end to end against
                                                       # pipelines/mock_archive.py
    python pipelines/pipeline_studio3.py --selftest --live     # also resolve every
                                                       # filter against the archive

WHERE THINGS GO
    Generated pipelines  ->  <project_root>/pipelines/generated/
    Saved projects       ->  <project_root>/pipelines/generated/_projects/
                             One JSON per report. Delete moves the file into
                             _projects/_trash/ rather than removing it. Every read and
                             write goes through ProjectStore, which is the seam a
                             shared S3 shelf drops into later.
    Cached vocabulary    ->  <project_root>/pipelines/generated/_cache/filters.json
    One run's own files  ->  <project_root>/pipelines/generated/_runs/<run_id>/
                             state.json, approved.json, and output/ — one directory per
                             run, set through RS_OUTPUT_DIR, so two runs of the same
                             report on the same day cannot overwrite each other and a
                             deck built on Tuesday still downloads on Thursday.

WHAT IT STILL WILL NOT DO
    It does not invent bespoke logic. Anything the four steps cannot express goes in the
    "Notes for Engineering" box, which is lifted verbatim into the generated file's
    docstring as a to-do. A generated pipeline is a correct, house-style DRAFT.
"""

from __future__ import annotations

import argparse
import ast
import collections
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

try:  # a cp1252 console must not crash on the box-drawing glyphs below
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

STUDIO_FILE = Path(__file__).resolve()
sys.path.insert(0, str(STUDIO_FILE.parent))

import cs_api as CS  # noqa: E402 — the shared client; stdlib only, so this cannot fail


def _find_pipelines_dir() -> Path | None:
    for c in [STUDIO_FILE.parent, STUDIO_FILE.parent / "pipelines",
              STUDIO_FILE.parent.parent / "pipelines", Path.cwd(), Path.cwd() / "pipelines"]:
        if (c / "report_lib.py").is_file():
            return c.resolve()
    return None


PIPELINES_DIR = _find_pipelines_dir()
GENERATED_DIR = (PIPELINES_DIR / "generated") if PIPELINES_DIR else (
    STUDIO_FILE.parent / "generated")
LOGO_FILE = (PIPELINES_DIR.parent if PIPELINES_DIR else STUDIO_FILE.parent.parent) \
    / "logo_pipelines.jpg"
PROJECTS_DIR = GENERATED_DIR / "_projects"
RUNS_DIR = GENERATED_DIR / "_runs"
SCHEMA = 4

# ═══════════════════════════════════════════════════════════════════════════════════════
# Vocabulary — read from the API, never hardcoded
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# v2 kept CHANNELS / SECTORS / AUDIENCES as module constants and drifted: it listed 8
# channels where the archive has 10, and 9 sectors where it has 17. A vocabulary that
# lives in the service is the whole reason this rewrite is worth doing, so it is fetched.
# cs_api disk-caches the catalog, so the Studio still opens when the API is unreachable —
# a stale vocabulary that renders beats a blank screen — and the UI says how old it is.

DATE_FIELDS = [
    {"key": "search_date", "label": "Mailed / captured date",
     "note": "The date the piece itself carries. The usual choice for a deck, and the "
             "date its entry_id is built from."},
    {"key": "approved_date", "label": "Approved for PowerSearch",
     "note": "When it was approved for release. Later than the mailed date, often by "
             "weeks."},
    {"key": "added_to_database", "label": "Added to the database",
     "note": "When it entered the archive. Catches older pieces loaded recently."},
]

_CATALOG_LOCK = threading.Lock()

# Filters the API publishes that the Studio does not offer.
#
# The archive's credit-score surface is two halves of one filter: credit_risk_band is a
# 300-850 band, and credit_score_type says whether that band is read as a FICO or a
# Vantage score. A band picked without its scale is a filter that quietly means something
# other than what its label says, so the pair goes out together rather than one of them
# staying behind.
#
# Dropped here because catalog() is the one chokepoint every reader goes through — the
# filter picker, validate(), and the request body the generated pipeline sends all read
# the surface from this function. A filter absent here cannot be picked, cannot pass
# validation, and cannot reach the API.
_DROPPED_FILTERS = frozenset({"credit_risk_band"})
_DROPPED_CORE = frozenset({"credit_score_type"})


def _without_dropped(cat: dict) -> dict:
    """`cat` with _DROPPED_FILTERS gone from every group and _DROPPED_CORE gone from core.

    A group left with nothing in it goes too, so the picker does not render an empty
    heading for a group whose every filter was dropped.
    """
    groups = {}
    for group, items in (cat.get("groups") or {}).items():
        kept = {name: spec for name, spec in (items or {}).items()
                if name not in _DROPPED_FILTERS}
        if kept:
            groups[group] = kept
    core = {k: v for k, v in (cat.get("core") or {}).items() if k not in _DROPPED_CORE}
    return {**cat, "groups": groups, "core": core}


def catalog() -> dict:
    """The filter surface, from disk when it is fresh and from the archive when it is not.

    There is no manual refresh and nothing in the UI offers one. What this returns is the
    set of filters that EXIST and the values they accept, which is a schema rather than
    data — it changes when Competiscan adds a filter, not when a campaign is captured.
    Re-fetching it on a researcher's say-so would be a button that never needed pressing.
    A day's cache life means it comes back on its own.
    """
    with _CATALOG_LOCK:
        try:
            return _without_dropped(CS.catalog(max_age=86_400))
        except CS.ApiError as exc:
            return {"source": "error", "error": exc.hint(), "core": {}, "groups": {},
                    "sectors": [], "fetched_at": 0}


def core_values(key: str) -> list:
    """One core filter's enumerated values, or [] when it has no listable vocabulary —
    company, entry_id and ocr_text are free text or too large to list."""
    val = (catalog().get("core") or {}).get(key)
    return val if isinstance(val, list) else []


# ═══════════════════════════════════════════════════════════════════════════════════════
# The column catalog: every worksheet column and where its value comes from
# ═══════════════════════════════════════════════════════════════════════════════════════
#   api      : straight off the 15-column search row — no database round-trip
#   derived  : computed from an api field
#   sql      : from the database via XH.build_query, keyed on entry_id
#   ocr      : GET /v1/ocr, one request per piece. Still not on the search row (the text
#              is a longtext — the biggest single chunk in the archive is over a million
#              characters) but it no longer needs the tunnel.

COLUMNS = [
    {"name": "EntryID", "source": "api", "default": True},
    {"name": "Product ID", "source": "api", "default": False,
     "note": "The archive's numeric id. Useful when reconciling against PowerSearch."},
    {"name": "Primary Company", "source": "api", "default": True,
     "note": "The company the row DISPLAYS, which is one of the piece's mappings. A row "
             "matched by a company filter may display a different one."},
    {"name": "Product", "source": "api", "default": True},
    {"name": "Headline", "source": "api", "default": True},
    {"name": "Media Channel", "source": "api", "default": True},
    {"name": "Audience", "source": "api", "default": False},
    {"name": "Mailing Type", "source": "api", "default": False},
    {"name": "Delivery Type", "source": "api", "default": False},
    {"name": "Postage", "source": "api", "default": False},
    {"name": "Country", "source": "api", "default": False},
    {"name": "Mailed/Captured Date", "source": "api", "default": False,
     "note": "search_date — the date the piece carries."},
    {"name": "Approved Date", "source": "api", "default": False},
    {"name": "Added to Database", "source": "api", "default": False},
    {"name": "PDF Content", "source": "api", "default": True,
     "note": "Clickable link to the scanned piece."},
    {"name": "Quarter", "source": "derived", "default": True,
     "note": "Computed from the mailed date."},
    {"name": "State/Province", "source": "sql", "default": False,
     "note": "The state or province the campaign targeted."},
    {"name": "Additional Companies", "source": "sql", "default": False},
    {"name": "Primary Sector", "source": "sql", "default": False},
    {"name": "Primary Category", "source": "sql", "default": False},
    {"name": "Primary Sub Category", "source": "sql", "default": False},
    {"name": "Primary Sub Sub Category", "source": "sql", "default": False},
    {"name": "Age", "source": "sql", "default": False},
    {"name": "Income", "source": "sql", "default": False},
    {"name": "Pre-Screen", "source": "sql", "default": False},
    {"name": "Mortgage & Loan - Application Type", "source": "sql", "default": False,
     "note": "Refinance / VA / FHA / Conventional. Lending reports only."},
    {"name": "Social Media Ad Type", "source": "sql", "default": False},
    {"name": "OCR Text", "source": "ocr", "default": False,
     "note": "The scanned text of each piece, printed in the workbook. Long, and it "
             "costs one request per row, so it is capped — see the run output. Agents "
             "read it for the pieces they write up whether or not it is printed."},
]

DEFAULT_COLUMNS = [c["name"] for c in COLUMNS if c["default"]]
SQL_COLUMNS = {c["name"] for c in COLUMNS if c["source"] == "sql"}
OCR_COLUMNS = {c["name"] for c in COLUMNS if c["source"] == "ocr"}
# OCR used to be a database column too. It is an API endpoint now, so the only thing
# that still needs the tunnel is the XH.build_query set.
DB_COLUMNS = SQL_COLUMNS

# Worksheet column -> the search-row key it reads. Everything else is SQL or derived.
API_FIELD = {
    "EntryID": "entry_id", "Product ID": "product_id",
    "Primary Company": "company", "Product": "product_name",
    "Headline": "product_headline", "Media Channel": "media_channel",
    "Audience": "audience", "Mailing Type": "mailing_type",
    "Delivery Type": "delivery_type", "Postage": "postage", "Country": "country",
    "Mailed/Captured Date": "search_date", "Approved Date": "approved_date",
    "Added to Database": "added_to_database", "PDF Content": "pdf_url",
}


def needs_database(columns) -> bool:
    """The database lookup is not a setting — it is implied by the columns asked for."""
    return any(c in DB_COLUMNS for c in (columns or []))


def needs_sql(columns) -> bool:
    """The entry_id-keyed XH.build_query lookup specifically."""
    return any(c in SQL_COLUMNS for c in (columns or []))


def needs_ocr(section) -> bool:
    """Whether this section reads any scanned text at all.

    Always, when the section features pieces on a slide — an agent writing the paragraph
    under a slide describes the offers, and the offers are on the page rather than in the
    headline. Also when a worksheet column asks to PRINT the text, which is a separate
    want: a write-up can be informed by the scanned page without it filling a cell.

    A worksheet-only section with no OCR column reads nothing, and skips it entirely.
    """
    sh = section.get("sheet") or {}
    cols = sh.get("columns") or [] if sh.get("enabled") else []
    return (any(c in OCR_COLUMNS for c in cols)
            or bool((section.get("feature") or {}).get("enabled")))


def prints_ocr(section) -> bool:
    """Whether the worksheet asks for the text in a cell.

    Kept apart from needs_ocr because the two cost wildly different amounts: writing up
    a slide reads the 3-5 pieces that were chosen, while printing a column reads every
    row in the tab, at one request each.
    """
    sh = section.get("sheet") or {}
    if not sh.get("enabled"):
        return False
    return any(c in OCR_COLUMNS for c in (sh.get("columns") or []))


# ═══════════════════════════════════════════════════════════════════════════════════════
# Project schema — one flat object. No nodes, no edges, nothing to wire.
# ═══════════════════════════════════════════════════════════════════════════════════════

def new_section(title="New section") -> dict:
    return {
        "id": uuid.uuid4().hex[:8],
        "title": title,
        "heading": "",
        "search": {
            # ── core filters, all API-native ──────────────────────────────────────
            "sector": [], "category": [], "subcategory": [], "subsubcategory": [],
            "media_channel": ["Email"], "audience": [], "country": "",
            "company": [], "company_match": "exact",
            "ocr_text": [], "ocr_text_match": "all",
            "entry_id": [], "panelist_id": [], "panelist_type": "all",

            # ── the 69 enhanced filters: only what the researcher added ───────────
            # `filters` is the list of filters ADDED, by base field name and in the
            # order they were added, so a filter can show an empty row before it has
            # a value. `enhanced` holds the values that actually go in the request —
            # a range occupies two keys there (loan_amount_min / _max) and one entry
            # here. Only `enhanced` is ever sent.
            #
            # A flag left on "Any" is ABSENT from enhanced, never False. Flags are
            # tri-state and omitting is the default; false narrows to pieces
            # explicitly recorded as NOT carrying the flag, which is a real filter.
            "filters": [],
            "enhanced": {},

            # ── execution ─────────────────────────────────────────────────────────
            "row_cap": 5000,

            # ── the only narrowings that are NOT expressible as API filters ───────
            "company_must_not_match": "",   # the API has no negation
            "collapse_repeats": True, "max_per_creative": 2,
        },
        "sheet": {"enabled": True, "tab": "", "columns": list(DEFAULT_COLUMNS)},
        "feature": {
            "enabled": True, "count": 4,
            "how_to_choose": "", "what_to_say": "",
            "callout_limit": 374,
            "one_per_company": True, "never_reuse": True,
            "mention_total": True,
        },
    }


def new_project(name="Untitled report") -> dict:
    return {
        "schema": SCHEMA,
        "name": name,
        "client": "",

        # ── the window ────────────────────────────────────────────────────────────
        # Two shapes, and which one is set is the single strongest signal of which
        # JOB this report is doing.
        #
        #   mode "cadence"  a repeatable window. cadence + anchor decide it fresh on
        #                   every run, so a scheduled pipeline covers the right dates
        #                   months from now with nobody watching. This is the only
        #                   shape that is safe to deploy.
        #   mode "range"    two fixed dates, for a one-off client ask — "Q2", "March
        #                   1 through April 15". Deployed on a schedule it would
        #                   silently reproduce the same report forever, so Send to
        #                   Engineering blocks on an explicit acknowledgement and the
        #                   generated file says so in its docstring.
        "cadence": "month",
        "anchor": "prior_complete",
        "window": {"mode": "cadence", "start": "", "end": ""},

        "date_field": "search_date",
        "deck": {
            "enabled": True,
            "title": "{client} — {period}",
            "filename": "{client}_Report_{stamp}.pptx",
            "title_slide": True, "summary_slide": False,
            "section_headings": False, "closing_slide": True,
        },
        "workbook": {"enabled": True, "filename": "{client}_Data_{stamp}.xlsx"},

        # A literal address is NEVER stored in a project. What is stored is the NAME
        # of an environment variable Engineering sets on the box that runs it, so a
        # saved report cannot carry a recipient it was never meant to keep. A one-off
        # address is typed in the run panel and lives only for that one run.
        "email": {"enabled": False, "env_var": "RS_EMAIL_TO"},

        "notes": "",

        # Bookkeeping, not content: deliberately excluded from the content hash, so
        # running or sending a report never makes it look edited.
        "status": {"sent": None, "runs": [], "saved_as": ""},

        "sections": [new_section("Section 1")],
    }


def _example_project() -> dict:
    """One filled-in example, so a new user sees a complete report without it implying
    that reports have to look like this. It deliberately uses the filters v2 could not
    express — credit_union, mailing_type, a taxonomy drill — because those are the point."""
    p = new_project("Example — credit union monthly")
    p["client"] = "Example Client"
    p["deck"].update({"summary_slide": True, "section_headings": True,
                      "title": "{client} Market Update — {period}",
                      "filename": "{client}_Market_Update_{stamp}.pptx"})
    p["workbook"]["filename"] = "{client}_Offers_{stamp}.xlsx"
    out = []
    for title, tab, heading, sector, enhanced, guidance in [
        ("Checking Acquisition", "Checking", "Deposits", ["Banking"],
         {"credit_union": True, "mailing_type": ["Acquisition"]},
         "Offers that push someone to open a new account. Acquisition only — the "
         "mailing_type filter already excludes statements and retention pieces."),
        ("Savings & CDs", "Savings", "Deposits", ["Banking"],
         {"credit_union": True},
         "Rate-led offers. Prefer pieces that state an actual APY."),
        ("Card Offers", "Cards", "Lending", ["Credit Cards"],
         {"card_network": ["Visa", "MasterCard"], "rewards_program": True},
         "Rewards cards from Visa and Mastercard. The card_network and rewards_program "
         "filters do the narrowing, so no keyword guessing is needed."),
    ]:
        s = new_section(title)
        s["heading"] = heading
        s["sheet"]["tab"] = tab
        s["search"].update({"sector": sector,
                            "media_channel": ["Direct Mail", "Email", "Social Media"],
                            "audience": ["Consumer"],
                            "enhanced": enhanced})
        s["feature"].update({
            "count": 4, "how_to_choose": guidance,
            "what_to_say": "One paragraph in an analyst voice. Name each institution and "
                           "its specific offer. Do not invent details.",
        })
        out.append(s)
    p["sections"] = out
    return p


# ═══════════════════════════════════════════════════════════════════════════════════════
# Templates — generic shapes to start from, never a researcher's saved work
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# A template is a PATTERN, so none of them names a real client. Opening one hands back a
# brand-new unsaved project: there is no endpoint that writes a template, so a researcher
# cannot edit or overwrite one by any route through this Studio. The client field is left
# blank on purpose — validate() then asks for it, which is the first thing a researcher
# should be thinking about anyway.


def _t_single_topic() -> dict:
    """One topic, once a month. The smallest report that is still a real report."""
    p = new_project("Single topic, monthly")
    s = p["sections"][0]
    s["title"] = "The topic"
    s["sheet"]["tab"] = "Results"
    s["search"].update({"media_channel": ["Direct Mail", "Email"],
                        "audience": ["Consumer"]})
    s["feature"].update({
        "count": 4,
        "how_to_choose": "Say in one sentence what belongs on this slide — the offers "
                         "worth showing a client, not just the newest pieces.",
        "what_to_say": "One paragraph in an analyst voice. Name each company and its "
                       "specific offer. Do not invent details.",
    })
    return p


def _t_categories_workbook() -> dict:
    """Several categories, each its own tab. The shape of a regional deposit/lending
    report: the workbook is the deliverable and the slides summarise it."""
    p = new_project("Several categories with a workbook")
    p["deck"].update({"summary_slide": True, "section_headings": True})
    out = []
    for title, tab, heading in [("Checking", "Checking", "Deposits"),
                                ("Savings & CDs", "Savings", "Deposits"),
                                ("Mortgage", "Mortgage", "Lending"),
                                ("Auto & Personal Loans", "Loans", "Lending")]:
        sec = new_section(title)
        sec["heading"] = heading
        sec["sheet"]["tab"] = tab
        sec["sheet"]["columns"] = list(DEFAULT_COLUMNS) + ["State/Province",
                                                           "Mailed/Captured Date"]
        sec["search"].update({"media_channel": ["Direct Mail", "Email"],
                              "audience": ["Consumer"]})
        sec["feature"]["count"] = 3
        out.append(sec)
    p["sections"] = out
    return p


def _t_competitor_roster() -> dict:
    """One section per competitor per channel, grouped under the competitor's name.

    The combinatorial shape — a roster of N competitors across M channels is N x M
    sections — so it is worth seeing that the tool expresses it before building one by
    hand. The names are placeholders, not a client's roster.
    """
    p = new_project("Competitor roster across channels")
    p["deck"].update({"section_headings": True, "summary_slide": True})
    out = []
    for who in ("Competitor A", "Competitor B", "Competitor C"):
        for channel in ("Direct Mail", "Email"):
            sec = new_section(f"{who} — {channel}")
            sec["heading"] = who
            sec["sheet"]["tab"] = who
            sec["search"].update({"media_channel": [channel],
                                  "company": [],           # put the real name here
                                  "company_match": "contains"})
            sec["feature"].update({
                "count": 3, "one_per_company": False,
                "how_to_choose": "The pieces that best show what this competitor is "
                                 "saying on this channel this period.",
            })
            out.append(sec)
    p["sections"] = out
    p["notes"] = ("Put each competitor's real name in its sections' company filter, and "
                  "check it with Preview first — the archive files companies under "
                  "their full legal names, which are usually longer than the brand.")
    return p


def _t_keyword_watch() -> dict:
    """Slides only, no workbook. A watching brief rather than a data pull."""
    p = new_project("Keyword watch, slides only")
    p["workbook"]["enabled"] = False
    p["cadence"] = "week"
    s = p["sections"][0]
    s["title"] = "What we are watching for"
    s["sheet"]["enabled"] = False
    s["search"].update({"media_channel": ["Direct Mail", "Email"],
                        "ocr_text": [], "ocr_text_match": "any"})
    s["feature"].update({
        "count": 5,
        "how_to_choose": "The pieces that actually use the language we are watching "
                         "for, not the ones that merely come close.",
    })
    p["notes"] = ("Add the words to watch for under \"Words in the piece\", and keep a "
                  "sector or channel on the section as well — the archive has no "
                  "full-text index, so words alone cannot be a section's only filter.")
    return p





TEMPLATES = {
    "blank": ("Blank report", new_project),
    "single": ("Single topic, monthly", _t_single_topic),
    "categories": ("Several categories with a workbook", _t_categories_workbook),
    "roster": ("Competitor roster across channels", _t_competitor_roster),
    "keyword": ("Keyword watch, slides only", _t_keyword_watch),
    "example": ("Worked example — credit union monthly", _example_project),
}

TEMPLATE_NOTES = {
    "blank": "One empty section. Everything else is yours to fill in.",
    "single": "The smallest report that is still a report: one topic, one tab, one "
              "slide, every month.",
    "categories": "Four sections grouped under two headings, each writing its own "
                  "worksheet tab. The shape of a regional deposit and lending report.",
    "roster": "One section per competitor per channel, grouped under the competitor's "
              "name. Put the real company names in before running it.",
    "keyword": "Weekly, slides only, no workbook — a watching brief rather than a data "
               "pull.",
    "example": "A filled-in report that uses the filters worth knowing about — "
               "credit_union, mailing_type, and a drill into the taxonomy.",
}


# ═══════════════════════════════════════════════════════════════════════════════════════
# Migration — a v2 project opens in v3, and its hacks become real filters
# ═══════════════════════════════════════════════════════════════════════════════════════

def _sync_filters(section: dict) -> None:
    """Make sure every value in `enhanced` has a row in `filters`.

    One-directional on purpose. A name in `filters` with no value is a real state — the
    researcher added the filter and has not chosen a value yet, and validate() says so.
    A value with no name is not: it would be applied to every search while being
    invisible in the card, which is the one failure mode worth engineering away.
    """
    q = section.get("search") or {}

    # A project saved while a filter was still offered still carries it. Take it off the
    # section as well as out of the catalog, or it would keep reaching the API from a
    # card that no longer has a control to show it — applied to every search, invisible
    # in the UI, which is the exact failure this function exists to prevent.
    enh = q.get("enhanced") or {}
    for key in [k for k in enh if re.sub(r"_(min|max)$", "", k) in _DROPPED_FILTERS]:
        enh.pop(key)
    for key in _DROPPED_CORE:
        q.pop(key, None)

    have = [f for f in (q.get("filters") or []) if f not in _DROPPED_FILTERS]
    for key in enh:
        base = re.sub(r"_(min|max)$", "", key)
        if base not in have:
            have.append(base)
    q["filters"] = have

    # read_ocr was a per-section switch before agents always read the scanned text.
    # A saved project still carries it; drop it so it cannot look like it still decides
    # anything.
    (section.get("feature") or {}).pop("read_ocr", None)


def migrate(p: dict) -> dict:
    """Bring a v2 project up to the v3 schema.

    The interesting cases are the ones where a v2 hack has a real filter waiting for it:
    only_credit_unions becomes credit_union, and a plain company fragment that used to be
    a regex becomes company_match="contains". Anything with no API equivalent is either
    kept as a documented post-filter or written into Notes for Engineering — never
    silently dropped, because a filter that quietly stops applying changes the numbers.
    """
    was = int(p.get("schema") or 0)
    if was >= SCHEMA:
        for s in p.get("sections") or []:
            _sync_filters(s)
        return p

    p = json.loads(json.dumps(p))  # never mutate the caller's dict
    carried: list[str] = []
    if was < 3:
        carried += _migrate_v2_to_v3(p)
    carried += _migrate_v3_to_v4(p)
    p["schema"] = SCHEMA

    for s in p.get("sections") or []:
        _sync_filters(s)

    if carried:
        note = ("Carried over when this project was opened:\n"
                + "\n".join(f"  - {c}" for c in carried))
        p["notes"] = (str(p.get("notes") or "").rstrip() + "\n\n" + note).strip()
    return p


def _migrate_v3_to_v4(p: dict) -> list[str]:
    """Add the v4 keys to an older project, defaulting rather than demanding a rewrite.

    Nothing here is destructive except the email address, and that one is REPORTED
    rather than silently dropped: v3 stored a literal recipient in the project file and
    baked it into the generated pipeline, which is exactly the thing the email guardrail
    says must never happen. A project carrying one keeps working, but the address moves
    out of the file and into an environment variable Engineering sets on the box.
    """
    carried: list[str] = []
    p.setdefault("cadence", "month")
    p.setdefault("anchor", "prior_complete")
    p.setdefault("date_field", "search_date")

    win = p.get("window") if isinstance(p.get("window"), dict) else {}
    p["window"] = {
        "mode": win.get("mode") if win.get("mode") in ("cadence", "range") else "cadence",
        "start": str(win.get("start") or ""),
        "end": str(win.get("end") or ""),
    }

    em = p.get("email") if isinstance(p.get("email"), dict) else {}
    addr = str(em.get("to_addr") or "").strip()
    p["email"] = {"enabled": bool(em.get("enabled")),
                  "env_var": str(em.get("env_var") or "").strip() or "RS_EMAIL_TO"}
    if addr:
        carried.append(
            f'the email recipient "{addr}" is no longer stored in the project. A saved '
            f"report keeps the NAME of an environment variable instead "
            f'({p["email"]["env_var"]}), which Engineering sets on the box that runs it. '
            f"To email a one-off to that address now, type it into the run panel — it is "
            f"used for that run and kept nowhere afterwards.")

    st = p.get("status") if isinstance(p.get("status"), dict) else {}
    p["status"] = {
        "sent": st.get("sent") if isinstance(st.get("sent"), dict) else None,
        "runs": [r for r in (st.get("runs") or []) if isinstance(r, dict)],
        "saved_as": str(st.get("saved_as") or ""),
    }
    return carried


def _migrate_v2_to_v3(p: dict) -> list[str]:
    """The v2 hacks that have a real API filter waiting for them."""
    carried: list[str] = []
    p["date_field"] = {"entry_id": "search_date",
                       "approved_date": "approved_date",
                       "added_to_database": "added_to_database"
                       }.get(p.pop("window_field", "entry_id"), "search_date")

    for s in p.get("sections") or []:
        old = s.get("search") or {}
        new = new_section(s.get("title") or "")["search"]

        new["sector"] = list(old.get("sectors") or [])
        new["media_channel"] = list(old.get("channels") or [])
        aud = old.get("audience")
        new["audience"] = [aud] if isinstance(aud, str) and aud.strip() else list(aud or [])
        new["company"] = [x.strip() for x in str(old.get("companies") or "").splitlines()
                          if x.strip()]
        if new["company"]:
            # v2 resolved company names through its own lookup. Here an exact name
            # the archive does not hold is a 400 that fails the whole section, and
            # the archive is stricter than it looks: it has no company called
            # "BECU" -- what it holds is "Boeing Employees' Credit Union (BECU)".
            names = ", ".join(new["company"])
            carried.append(
                f'{s.get("title")}: check the company names ({names}) with Preview. '
                f"An exact name the archive does not hold is rejected outright, and "
                f"its names are often longer than the brand -- BECU is filed as "
                f"\"Boeing Employees' Credit Union (BECU)\".")
        new["collapse_repeats"] = bool(old.get("collapse_repeats", True))
        new["max_per_creative"] = int(old.get("max_per_creative") or 2)
        new["row_cap"] = max(int(old.get("limit") or 200), 1000)

        # v2's keyword box was quoted phrases joined by "or". The API takes a term list
        # plus an all/any switch, which is what that syntax was imitating.
        kw = str(old.get("keyword") or "")
        terms = re.findall(r'"([^"]+)"', kw) or [t for t in re.split(r"\bor\b", kw, flags=re.I)
                                                 if t.strip()]
        terms = [t.strip() for t in terms if len(t.strip()) >= 3][:5]
        if terms:
            new["ocr_text"] = terms
            new["ocr_text_match"] = "all" if re.search(r"\band\b", kw, re.I) else "any"

        # The headline case: a regex over company names becomes one boolean.
        if old.get("only_credit_unions"):
            new["enhanced"]["credit_union"] = True
            carried.append(f'{s.get("title")}: "credit unions only" is now the '
                           f"credit_union filter, not a regex over company names.")

        # company_must_match is NOT folded into the company list. In v2 it was an AND
        # applied after the search, so it narrowed; values inside one API filter OR, so
        # moving it there would broaden instead. company_match="contains" is the nearest
        # real equivalent, but only the researcher can say whether it means the same
        # thing for their report — so it is reported, never guessed at.
        cmm = str(old.get("company_must_match") or "").strip()
        if cmm:
            carried.append(
                f'{s.get("title")}: company_must_match "{cmm}" is NOT carried over. In v2 '
                f"it narrowed the results after the search; on the archive, company values "
                f'OR together, so adding it would widen them. Use "Name contains" with '
                f"that fragment if that is what you meant, and check it with Preview.")
        new["company_must_not_match"] = str(old.get("company_must_not_match") or "")

        # Sub-category was faked with a SQL lookup and a substring test. It is a real
        # taxonomy filter now, but the v2 text was free-form, so a human has to confirm
        # which node it meant.
        for key, label in (("subcategory_must_include", "must include"),
                           ("subcategory_must_exclude", "must exclude")):
            raw = str(old.get(key) or "").strip()
            if raw:
                carried.append(f'{s.get("title")}: sub-category {label} "{raw}" — pick the '
                               f"real taxonomy node in the Sub-category picker; v2 matched "
                               f"this as a substring of a SQL field.")

        s["search"] = new
        fe = s.get("feature") or {}
        if "mention_cap" in fe:
            # v2 phrased a capped search as "at least N". The API gives an exact total,
            # so the choice is now simply whether to state the count at all.
            fe["mention_total"] = bool(fe.pop("mention_cap"))

    # v2 carried a home_states list that nothing in v3 reads — the archive publishes a
    # real `state` filter now. Reported rather than dropped in silence, because a report
    # that quietly stops being state-scoped returns different numbers.
    homes = [x for x in (p.pop("home_states", None) or []) if str(x).strip()]
    if homes:
        carried.append(
            f'the v2 "home states" list ({", ".join(map(str, homes))}) is not carried '
            f"over — it never reached the archive in v2 either. If this report is meant "
            f'to be state-scoped, add the real "state" filter to each section.')
    return carried


# ═══════════════════════════════════════════════════════════════════════════════════════
# Checking — runs continuously, no button. Issues attach to the section they concern.
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# Most of these are the API's own documented rules, checked here so a researcher reads
# them in the card rather than as a 400 three minutes into a Test run.

def validate(p: dict) -> dict:
    # codegen migrates too, so checking the un-migrated shape would report problems the
    # generated file does not have — a v2 project would look like it had no channels.
    p = migrate(p)
    issues: list[dict] = []
    cat = catalog()
    flat = CS.flat_filters(cat) if cat.get("groups") else {}

    def err(msg, section=None):
        issues.append({"level": "error", "msg": msg, "section": section})

    def warn(msg, section=None):
        issues.append({"level": "warn", "msg": msg, "section": section})

    if cat.get("source") == "error":
        warn(f"The filter vocabulary could not be loaded ({cat.get('error')}), so filter "
             f"values are not being checked. Everything else still works.")

    if not str(p.get("client") or "").strip():
        err("Give the report a client name — it goes in the file names and the deck "
            "title.")

    win = p.get("window") or {}
    if win.get("mode") == "range":
        from datetime import date as _date
        start = end = None
        for key, label in (("start", "start"), ("end", "end")):
            raw = str(win.get(key) or "").strip()
            if not raw:
                err(f"Give the date range an {label} date, or switch back to a repeating "
                    f"period.")
                continue
            try:
                got = _date.fromisoformat(raw)
            except ValueError:
                err(f'"{raw}" is not a date the archive understands. Use YYYY-MM-DD.')
                continue
            if key == "start":
                start = got
            else:
                end = got
        if start and end and start > end:
            err(f"The range starts after it ends ({start} .. {end}).")
        elif start and end:
            warn(f"This report covers a fixed window, {start} .. {end}. That is right "
                 f"for a one-off, and wrong for anything deployed on a schedule — a "
                 f"scheduled run would produce this same period forever. Use “Make "
                 f"it recurring” before sending it to Engineering.")
            if (end - start).days > 400:
                warn(f"{(end - start).days} days is a wide window. Every section is "
                     f"searched a month at a time past the row ceiling, so expect this "
                     f"to take a while.")

    sections = p.get("sections") or []
    if not sections:
        err("Add at least one section. A section is one topic in the report.")

    deck = p.get("deck") or {}
    deck_on = bool(deck.get("enabled"))
    book_on = bool((p.get("workbook") or {}).get("enabled"))
    if not deck_on and not book_on:
        err("Turn on slides, the workbook, or both — otherwise this report produces "
            "nothing.")

    scoped = bool(deck_on and deck.get("section_headings"))
    titles: dict[tuple, int] = {}
    tabs: dict[str, list] = {}

    for s in sections:
        sid = s.get("id")
        title = (s.get("title") or "").strip() or "(untitled)"
        se = s.get("search") or {}
        sh = s.get("sheet") or {}
        fe = s.get("feature") or {}
        enh = se.get("enhanced") or {}

        if not (s.get("title") or "").strip():
            err("This section needs a name.", sid)
        key = ((s.get("heading") or "").strip(), title) if scoped else ("", title)
        titles[key] = titles.get(key, 0) + 1

        # ── channels: one request per channel, so no channels means no search ──────
        if not se.get("media_channel"):
            err("Pick at least one media channel. Each one is searched separately, "
                "because a single call shared between channels loses the quiet ones "
                "when the result is truncated.", sid)

        # ── the API's unbounded-query guard ───────────────────────────────────────
        # The report always sends a date window, so the guard is satisfied. What it
        # cannot catch is a section that relies on the window alone.
        narrowing = [k for k in ("sector", "category", "subcategory", "subsubcategory",
                                 "media_channel", "audience", "company", "entry_id",
                                 "panelist_id") if se.get(k)]
        if narrowing == ["media_channel"] and not enh:
            warn("This section is only narrowed by channel and the report's date window, "
                 "so it searches every sector in the archive. That is slow, and almost "
                 "certainly wider than the section is meant to be.", sid)

        # ── ocr_text: the most expensive filter the API offers ────────────────────
        terms = [t for t in (se.get("ocr_text") or []) if str(t).strip()]
        if terms:
            if len(terms) > 5:
                err(f"The archive accepts at most 5 words-in-the-piece terms; this has "
                    f"{len(terms)}.", sid)
            short = [t for t in terms if len(str(t).strip()) < 3]
            if short:
                err(f"Each term needs at least 3 characters: {', '.join(short)}", sid)
            punct = [t for t in terms if re.search(r"[^\w\s]", str(t))]
            if punct:
                warn(f'The OCR pipeline strips punctuation and collapses whitespace, so '
                     f'{", ".join(punct)} will not match. Write "pre approved", never '
                     f'"pre-approved".', sid)
            if not narrowing:
                err("Words-in-the-piece cannot be a section's only filter — there is no "
                    "full-text index behind it. Add a sector, channel or company.", sid)

        # ── company ───────────────────────────────────────────────────────────────
        companies = [c for c in (se.get("company") or []) if str(c).strip()]
        if len(companies) > 50:
            err(f"At most 50 company values per search; this has {len(companies)}.", sid)
        if se.get("company_match") == "contains":
            tiny = [c for c in companies if len(str(c).strip()) < 3]
            if tiny:
                err(f'A "contains" fragment needs 3 characters or more: '
                    f'{", ".join(tiny)}', sid)
            generic = [c for c in companies if len(str(c).split()) == 1
                       and str(c).lower() in ("bank", "insurance", "credit", "financial",
                                              "mortgage", "loan", "card", "union")]
            if generic:
                warn(f'"{", ".join(generic)}" is a word that occurs in company names '
                     f'rather than a company. A fragment resolving to more than 500 '
                     f'companies is rejected outright — check it with Preview.', sid)

        # ── enhanced filters, against the live vocabulary ─────────────────────────
        for base in (se.get("filters") or []):
            spec = flat.get(base)
            keys = CS.filter_fields(spec) if spec else [base]
            if not any(k in enh for k in keys):
                label = (spec or {}).get("label") or base
                warn(f"{label} was added but has no value, so it narrows nothing. Give "
                     f"it a value or remove it.", sid)

        for field, val in enh.items():
            base = re.sub(r"_(min|max)$", "", field)
            spec = flat.get(field) or flat.get(base)
            if flat and not spec:
                err(f'"{field}" is not a filter this archive publishes. Remove it and '
                    f'pick it again from Add filter.', sid)
                continue
            if not spec:
                continue
            label = spec.get("label") or field
            kind = spec.get("type")

            if kind == "boolean":
                if val is False:
                    warn(f'{label} is set to No, which matches only pieces explicitly '
                         f'recorded as NOT carrying it — that is a real filter, not '
                         f'"no filter". Set it to Any to stop filtering on it.', sid)
            elif kind == "range":
                if val == 0:
                    warn(f"{label}: zero means “not stated” in this column and "
                         f"never matches, so a range starting at 0 excludes every piece "
                         f"whose value was never recorded.", sid)
            elif kind == "multi-select":
                vals = val if isinstance(val, list) else [val]
                options = spec.get("options") or []
                truncated = spec.get("count") and len(options) < spec["count"]
                if options and not truncated:
                    allowed = {str(o).lower() for o in options}
                    allowed |= {str(o).lower() for o in (spec.get("also_accepts") or [])}
                    unknown = [v for v in vals if str(v).lower() not in allowed
                               and not str(v).isdigit()]
                    if unknown:
                        err(f'{label}: {", ".join(map(str, unknown))} is not a value this '
                            f'filter accepts. Valid: {", ".join(map(str, options[:8]))}'
                            + (f" (+{len(options) - 8} more)" if len(options) > 8 else ""),
                            sid)
                if not vals:
                    warn(f"{label} is added but has nothing selected, so it does not "
                         f"narrow anything.", sid)

            if spec.get("cost") == "expensive" or spec.get("requires_date_range"):
                warn(f"{label} is a slow filter — expect this section to take "
                     f"noticeably longer than the others.", sid)

        # ── row cap ───────────────────────────────────────────────────────────────
        try:
            cap = int(se.get("row_cap") or CS.LIMIT_MAX)
            if cap > CS.LIMIT_MAX:
                warn(f"{cap} is above the {CS.LIMIT_MAX:,}-row ceiling for one call, so "
                     f"the window will be split by month to get past it.", sid)
            elif cap < 20:
                warn(f"{cap} rows per channel is low — you may miss pieces that belong "
                     f"in the report.", sid)
        except (TypeError, ValueError):
            err("Max rows must be a number.", sid)

        if str(se.get("company_must_not_match") or "").strip():
            warn("Company exclusion is applied after the search, so it narrows the "
                 "results without narrowing the search itself.", sid)

        if not sh.get("enabled") and not fe.get("enabled"):
            warn("This section has no worksheet tab and nothing on a slide, so it is "
                 "searched and then thrown away.", sid)

        if sh.get("enabled"):
            if not book_on:
                warn("This section writes to a worksheet tab, but the workbook is turned "
                     "off in report settings.", sid)
            cols = sh.get("columns") or []
            if not cols:
                err("Pick at least one column for the worksheet tab.", sid)
            tab = (sh.get("tab") or "").strip() or title
            tabs.setdefault(tab, []).append((sid, tuple(cols)))

        if fe.get("enabled"):
            if not deck_on:
                warn("This section features pieces on a slide, but slides are turned off "
                     "in report settings.", sid)
            try:
                n = int(fe.get("count") or 0)
                if n < 1:
                    err("Feature at least one piece, or untick the slide.", sid)
                elif n > 20:
                    warn(f"{n} pieces means {-(-n // 5)} slides for this one section.", sid)
            except (TypeError, ValueError):
                err("How many pieces must be a number.", sid)
            if not str(fe.get("how_to_choose") or "").strip():
                warn("With no guidance on how to choose, the pick is close to arbitrary. "
                     "One sentence about what belongs here makes a big difference.", sid)

    for (heading, t), n in titles.items():
        if n > 1:
            where = f' under "{heading}"' if heading else ""
            err(f'{n} sections{where} are called "{t}". Section names become slide '
                f'titles, so give them distinct names.')

    for tab, entries in tabs.items():
        if len({c for _, c in entries}) > 1:
            warn(f'{len(entries)} sections share the tab "{tab}" but ask for different '
                 f'columns. The first section\'s columns are used for the whole tab.',
                 entries[0][0])

    if scoped:
        missing = [s for s in sections
                   if (s.get("feature") or {}).get("enabled")
                   and not str(s.get("heading") or "").strip()]
        if missing:
            warn(f"Headings are on but {len(missing)} section(s) have none, so they will "
                 f"appear before the first divider.")

    if deck_on and deck.get("summary_slide"):
        if not any((s.get("feature") or {}).get("enabled") for s in sections):
            warn("The summary slide has nothing to summarise — no section puts anything "
                 "on a slide.")

    em = p.get("email") or {}
    if em.get("enabled"):
        var = str(em.get("env_var") or "").strip()
        if not var:
            err("Name the environment variable that holds the recipient. A report never "
                "stores the address itself — Engineering sets the variable on the box "
                "that runs it.")
        elif not re.fullmatch(r"[A-Z][A-Z0-9_]*", var):
            err(f'"{var}" is not a usable environment-variable name. Use capitals, '
                f"digits and underscores, e.g. RS_EMAIL_TO.")
        elif "@" in var:
            err("That is an address, not a variable name. To email a one-off right now, "
                "type the address into the run panel instead — it is used for that run "
                "and kept nowhere.")

    if str(p.get("notes") or "").strip():
        warn("This report has notes for Engineering, so it needs hand work before it is "
             "production-ready.")

    if PIPELINES_DIR is None:
        warn("report_lib.py was not found next to this script, so Test cannot run. Put "
             "pipeline_studio3.py in pipelines/, beside report_lib.py.")

    # Quota is per calendar month and every request counts, errors included. Two per
    # section x channel per window slice: one probe, one fetch.
    calls = sum(len((s.get("search") or {}).get("media_channel") or []) for s in sections)
    try:
        w_start, w_end = window(p)
        win_desc = {"start": str(w_start), "end": str(w_end),
                    "mode": (p.get("window") or {}).get("mode") or "cadence"}
    except Exception:
        win_desc = {"start": "", "end": "", "mode": "cadence"}
    return {
        "issues": issues,
        "content_hash": content_hash(p),
        "badge": status_badge(p),
        "window": win_desc,
        "errors": sum(1 for i in issues if i["level"] == "error"),
        "warnings": sum(1 for i in issues if i["level"] == "warn"),
        "database": any(needs_sql((s.get("sheet") or {}).get("columns"))
                        for s in sections if (s.get("sheet") or {}).get("enabled")),
        "api_calls": calls * 2,
        "catalog": {"source": cat.get("source"), "fetched_at": cat.get("fetched_at"),
                    "error": cat.get("error")},
    }


# ═══════════════════════════════════════════════════════════════════════════════════════
# Preview — the same body the generated pipeline will send, counted for real
# ═══════════════════════════════════════════════════════════════════════════════════════

def preview(project: dict, section_id: str) -> dict:
    """Exact totals per channel for one section, plus what every name resolved to.

    This is the whole point of Preview: a count is not trustworthy until you can see
    which ids your names became. A sector matches every node beneath it and a parent
    affinity category matches its children, so a total can move without the request
    changing at all.

    It runs CS.count() on CS.build_body() — the same two functions the generated pipeline
    calls — so a preview that says 116 and a run that says something else is impossible.
    """
    sec = next((s for s in (project.get("sections") or [])
                if s.get("id") == section_id), None)
    if not sec:
        return {"error": "unknown section"}

    start, end = window(project)
    search = sec.get("search") or {}
    channels = list(search.get("media_channel") or [])
    if not channels:
        return {"error": "This section has no media channel, so there is nothing to count."}

    out = {"section": sec.get("title"), "start": str(start), "end": str(end),
           "date_field": project.get("date_field") or "search_date",
           "channels": [], "total": 0, "any_capped": False, "resolved": None,
           "spent": 0, "row_cap": int(search.get("row_cap") or CS.LIMIT_MAX)}
    for channel in channels:
        body = CS.build_body(search, channel=channel,
                             date_field=out["date_field"], date_from=start, date_to=end)
        row = {"channel": channel, "body": body}
        try:
            t0 = time.time()
            res = CS.count(body)
            row.update(total=res.get("total"), capped=bool(res.get("total_is_capped")),
                       took_ms=res.get("took_ms"), cached=bool(res.get("cached")),
                       elapsed_ms=int((time.time() - t0) * 1000))
            out["total"] += int(res.get("total") or 0)
            out["any_capped"] = out["any_capped"] or row["capped"]
            out["resolved"] = out["resolved"] or res.get("resolved_filters")
        except CS.ApiError as exc:
            row.update(error=exc.code, message=exc.hint(), request_id=exc.request_id)
        out["spent"] += 1
        out["channels"].append(row)
    return out


def window(p: dict):
    """(start, end) for the report's window. The Studio computes this so a Preview covers
    exactly the dates the generated pipeline will, and the generated file carries the same
    logic rather than importing it.

    Two shapes. A fixed range is returned as it stands — that is the whole point of a
    one-off. A cadence is recomputed from today, which is what makes it safe to schedule.
    """
    from datetime import date, timedelta
    win = p.get("window") or {}
    if win.get("mode") == "range":
        try:
            start = date.fromisoformat(str(win.get("start") or ""))
            end = date.fromisoformat(str(win.get("end") or ""))
            if start <= end:
                return start, end
        except ValueError:
            pass
        # An unparseable range must not silently become "last month" — validate() has
        # already flagged it as an error, and falling through here keeps Preview honest
        # about which dates it actually counted.
    today = date.today()
    cadence, anchor = p.get("cadence") or "month", p.get("anchor") or "prior_complete"
    if cadence == "week":
        if anchor == "rolling":
            return today - timedelta(days=7), today
        monday = today - timedelta(days=today.weekday())
        return monday - timedelta(days=7), monday
    if anchor == "rolling":
        return today - timedelta(days=30), today
    prev_end = today.replace(day=1) - timedelta(days=1)
    return prev_end.replace(day=1), prev_end


# ═══════════════════════════════════════════════════════════════════════════════════════
# Status — where a report stands, for BOTH jobs
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# Two jobs share this Studio and a badge that only understood one of them would be
# actively misleading half the time:
#
#   a report is finished when Engineering has it  ->  Delivered
#
# Delivered means ONE thing: Send to Engineering happened. Running a report is not
# delivering it — the files land on this machine and nobody else has them — so a report
# that has produced a deck stays Draft and says so on the second line. The run history
# is still carried on the badge, it just no longer claims a hand-off that never
# happened.
#
# Everything here is computed from facts already recorded — a sent receipt and a run
# history. Nothing asks the researcher to declare which job they are doing.

_HASH_SKIP = ("status",)


def content_hash(p: dict) -> str:
    """A stable fingerprint of a report's CONTENT.

    Bookkeeping is excluded, so running a report or sending it does not make it look
    edited afterwards. Everything else counts: rename a section, change a filter, retitle
    the deck, and Engineering's copy is out of date — which is exactly what the third
    badge state exists to say.
    """
    import hashlib
    body = {k: v for k, v in (p or {}).items() if k not in _HASH_SKIP}
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _day(iso: str) -> str:
    try:
        return datetime.fromisoformat(str(iso)).strftime("%d %b")
    except (ValueError, TypeError):
        return ""


def status_badge(p: dict) -> dict:
    """{state, label, detail, tone} — one glance, either job."""
    st = (p or {}).get("status") or {}
    sent = st.get("sent") if isinstance(st.get("sent"), dict) else None
    runs = [r for r in (st.get("runs") or []) if r.get("produced")]
    last_run = runs[-1] if runs else None
    ran_on = _day(last_run.get("at")) if last_run else ""

    if sent:
        sent_on = _day(sent.get("at"))
        stale = content_hash(p) != str(sent.get("hash") or "")
        detail = f"Engineering has {sent.get('file') or 'the pipeline'}, sent {sent_on}."
        if stale:
            return {"state": "edited", "label": "Edited since sent", "tone": "warn",
                    "detail": detail + " This report has changed since — Engineering does"
                                       " not have the latest version."}
        return {"state": "sent", "label": f"Delivered · {sent_on}", "tone": "sent",
                "detail": detail + (f" Last run {ran_on}." if ran_on else "")}

    # A run that produced files is still a draft: the deck is on this machine and
    # Engineering has nothing. The run is reported, the hand-off is not claimed.
    if last_run:
        files = last_run.get("produced") or []
        what = ", ".join(files) if len(files) <= 2 else f"{len(files)} files"
        return {"state": "draft", "label": "Draft", "tone": "dim",
                "detail": f"Last run {ran_on} produced {what}. Not sent to Engineering"
                          f" — it reads as Delivered once it has been."}

    return {"state": "draft", "label": "Draft", "tone": "dim",
            "detail": "Not run to a finished deliverable yet, and not sent to"
                      " Engineering."}


def promote(p: dict) -> dict:
    """Turn a one-time report into one that is safe to schedule.

    A one-off becoming recurring is a normal, frequent outcome — "that was useful,
    let's do it monthly" — so it is a deliberate action rather than something a
    researcher rebuilds from scratch. It changes as little as it can, and it reports
    every change rather than making any of them quietly.

    The one setting that cannot survive being scheduled is a fixed date range. A
    pipeline pinned to 1 April .. 30 June, run on the 3rd of every month, produces the
    second quarter forever and looks like it is working. That is the single most
    likely way a report footguns Engineering, so it is the change this exists for.
    """
    p = migrate(p)
    changes: list[str] = []
    warnings: list[str] = []

    win = p.get("window") or {}
    if win.get("mode") == "range":
        from datetime import date
        span = None
        try:
            span = (date.fromisoformat(str(win.get("end")))
                    - date.fromisoformat(str(win.get("start")))).days
        except ValueError:
            pass
        # A short window was almost certainly a week's worth of work; anything longer
        # reads as a month. Both are a guess, which is why it is reported.
        cadence = "week" if span is not None and span <= 10 else "month"
        was = f'{win.get("start")} .. {win.get("end")}'
        p["window"] = {"mode": "cadence", "start": "", "end": ""}
        p["cadence"], p["anchor"] = cadence, "prior_complete"
        changes.append(
            f"The window was the fixed range {was}. It is now the last complete "
            f"{'week' if cadence == 'week' else 'month'}, worked out fresh on every "
            f"run — which is the only kind of window that is safe to put on a "
            f"schedule.")
        warnings.append(
            f"The dates {was} are gone. The next run will cover a different period, "
            f"and so will every run after it. If the report was only ever meant to "
            f"cover {was}, it is a one-off and should not be scheduled at all.")
        if span is not None and cadence == "month" and span > 45:
            warnings.append(
                f"The old range was {span} days, which is wider than one month. A "
                f"monthly run will return proportionally fewer pieces per run than "
                f"the one-off did.")
    else:
        changes.append("The window was already on a repeating cadence, so nothing "
                       "about the dates changed.")

    em = p.get("email") or {}
    if em.get("enabled"):
        changes.append(
            f'Deliverables will be emailed to whoever {em.get("env_var")} names on the '
            f"box that runs this. Give Engineering that address — it is deliberately "
            f"not stored in the report.")
    else:
        warnings.append(
            "Nothing is emailed when this runs. A scheduled report that emails nobody "
            "produces files on a server and tells no one. Turn on email in report "
            "settings if somebody should receive it.")

    if p.get("anchor") == "rolling":
        warnings.append(
            "The period is \u201cthe last 7 / 30 days\u201d, which moves with the day it "
            "runs on. That is reproducible enough to schedule but it will not line up "
            "with calendar months, so two runs can cover overlapping dates.")

    if str(p.get("notes") or "").strip():
        warnings.append(
            "This report has notes for Engineering, so it still needs hand work before "
            "it is production-ready.")

    return {"project": p, "changes": changes, "warnings": warnings}


# ═══════════════════════════════════════════════════════════════════════════════════════
# Code generation — readable, house-style Python
# ═══════════════════════════════════════════════════════════════════════════════════════

def _pystr(s) -> str:
    return '"' + str(s).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def _lit(v, indent=8) -> str:
    """A PYTHON literal, not JSON.

    json.dumps looks close enough but emits true/false/null, which are legal Python
    *identifiers*: they sail straight past ast.parse and then die as NameError the first
    time anyone runs the file. So scalars go through repr and containers are built by
    hand. Long prose is wrapped so the generated file stays readable.
    """
    if isinstance(v, str):
        if len(v) > 78 or "\n" in v:
            words, line, out = v.replace("\r", "").split(" "), "", []
            for word in words:
                if len(line) + len(word) + 1 > 68:
                    out.append(line)
                    line = word
                else:
                    line = f"{line} {word}".strip()
            if line:
                out.append(line)
            pad = " " * (indent + 4)
            body = "\n".join(pad + _pystr(part + (" " if i < len(out) - 1 else ""))
                             for i, part in enumerate(out))
            return "(\n" + body + "\n" + " " * indent + ")"
        return _pystr(v)
    if isinstance(v, bool) or v is None:
        return repr(v)
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_lit(x, indent) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{_lit(k, indent)}: {_lit(val, indent)}"
                               for k, val in v.items()) + "}"
    return _pystr(str(v))


def _slug(s) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(s)).strip("_") or "report"


def _wrap(text, prefix="  ", width=76) -> list[str]:
    out, line = [], prefix
    for word in str(text).split():
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = " " * len(prefix) + word
        else:
            line = f"{line} {word}" if line.strip() else prefix + word
    if line.strip():
        out.append(line)
    return out


def _int(v, default):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# Search keys emitted unconditionally, because the generated code indexes them directly.
_ALWAYS = ("media_channel", "enhanced", "row_cap", "company_must_not_match",
           "collapse_repeats", "max_per_creative")
# Emitted only when set. build_body reads everything with .get(), so absence is fine and
# a section's block stays short enough to actually read.
_IF_SET = ("sector", "category", "subcategory", "subsubcategory", "audience",
           "company", "company_match", "ocr_text", "ocr_text_match", "country",
           "entry_id", "panelist_id", "panelist_type")

_DATE_COLS = ("Mailed/Captured Date", "Approved Date", "Added to Database")


def _plan(p: dict) -> dict:
    """Everything both halves of the emitter need to agree on, worked out once."""
    deck = p.get("deck") or {}
    book = p.get("workbook") or {}
    sections = p.get("sections") or []
    deck_on = bool(deck.get("enabled"))
    book_on = bool(book.get("enabled"))

    # Worksheet tabs. Sections sharing a tab name merge into one sheet; the first one's
    # columns win for the whole tab (validate() warns when they differ).
    tabs: list[dict] = []
    seen: dict[str, dict] = {}
    for s in sections:
        sh = s.get("sheet") or {}
        if not (book_on and sh.get("enabled")):
            continue
        name = (sh.get("tab") or "").strip() or (s.get("title") or "Sheet").strip()
        if name in seen:
            seen[name]["section_ids"].append(s["id"])
        else:
            spec = {"name": name, "columns": list(sh.get("columns") or []),
                    "section_ids": [s["id"]]}
            seen[name] = spec
            tabs.append(spec)

    featured = [s for s in sections if (s.get("feature") or {}).get("enabled")] \
        if deck_on else []
    all_cols = {c for t in tabs for c in t["columns"]}
    win = p.get("window") or {}
    fixed = win.get("mode") == "range" and win.get("start") and win.get("end")
    return {
        "client": str(p.get("client") or "Report").strip() or "Report",
        "cadence": p.get("cadence") or "month",
        "anchor": p.get("anchor") or "prior_complete",
        "date_field": p.get("date_field") or "search_date",
        "fixed_window": bool(fixed),
        "range_start": str(win.get("start") or "") if fixed else "",
        "range_end": str(win.get("end") or "") if fixed else "",
        "email_var": str((p.get("email") or {}).get("env_var")
                         or "RS_EMAIL_TO").strip() or "RS_EMAIL_TO",
        "deck": deck, "book": book, "email": p.get("email") or {},
        "sections": sections, "notes": str(p.get("notes") or "").strip(),
        "deck_on": deck_on, "book_on": book_on, "tabs": tabs, "featured": featured,
        "all_cols": all_cols,
        "any_sql": any(needs_sql(t["columns"]) for t in tabs),
        "any_ocr": any(needs_ocr(s) for s in sections),
        "prints_ocr": any(prints_ocr(s) for s in sections),
        "date_cols": [c for c in _DATE_COLS if c in all_cols],
        "summary_on": bool(deck_on and deck.get("summary_slide") and featured),
        "headings_on": bool(deck_on and deck.get("section_headings")),
        "excludes": any(str((s.get("search") or {}).get("company_must_not_match")
                            or "").strip() for s in sections),
        "collapses": any(bool((s.get("search") or {}).get("collapse_repeats"))
                         for s in sections),
    }


def _emit_head(p: dict, g: dict, w) -> None:
    """Docstring, imports, settings, the SECTIONS table, and every helper."""
    client, tabs, deck, email = g["client"], g["tabs"], g["deck"], g["email"]
    any_db = g["any_sql"]
    O = []

    # ── docstring ───────────────────────────────────────────────────────────────────
    w("#!/usr/bin/env python3")
    w('"""')
    w(f"{client} — generated by Pipelines Studio v3")
    w("─" * 78)
    w(f"Project    : {p.get('name') or 'untitled'}")
    w(f"Generated  : {datetime.now():%Y-%m-%d %H:%M}")
    if g["fixed_window"]:
        w(f"Window     : FIXED {g['range_start']} .. {g['range_end']}  (a one-off)")
    else:
        w(f"Cadence    : {g['cadence']} ({g['anchor']})")
    w(f"Window from: {g['date_field']}")
    w(f"Sections   : {len(g['sections'])}    "
      f"Slides: {'yes' if g['deck_on'] else 'no'}    "
      f"Workbook: {len(tabs) if g['book_on'] else 0} tab(s)")
    w("")
    w("Every section is the same four steps: search the archive through the Competiscan")
    w("Platform API, optionally write the results to a worksheet tab, optionally have")
    w("Agents pick the best pieces and write the paragraph underneath, and put those on")
    w("a slide.")
    w("")
    w("RUN")
    w(f"    python pipelines/generated/{_slug(client)}.py")
    w("    python ... --only search    # counts only: no model calls, no deliverables")
    if tabs:
        w("    python ... --only excel     # search + workbook, still no model calls")
    if g["deck_on"]:
        w("    python ... --only deck      # everything")
    w("    python ... --limit 50       # cap rows per channel while testing")
    w("")
    if g["featured"]:
        w("RUN IT IN TWO HALVES, so a human can approve the pieces before they are")
        w("written up")
        w("    python ... --phase pick  --state run/state.json")
        w("    (a person edits run/approved.json)")
        w("    python ... --phase build --state run/state.json"
          " --approved run/approved.json")
        w("")
        w("    The split exists because the write-up must describe the pieces that were")
        w("    APPROVED, not the ones the model first suggested. So `pick` stops after")
        w("    the selection and `build` does every model call that produces prose. The")
        w("    build half never searches — it reads the records `pick` already wrote —")
        w("    so approving is free and rejecting costs nothing but a replacement drawn")
        w("    from the same cached pool:")
        w("")
        w("    python ... --phase replace --state run/state.json --section <id> "
          "--keep a,b --reject c")
        w("")
        w("    Without --phase the pipeline runs start to finish exactly as before. That")
        w("    is the mode Engineering schedules.")
        w("")
    if g["fixed_window"]:
        # Boxed, first thing after RUN, and impossible to skim past. A pipeline pinned
        # to two dates and put on a schedule is the single most likely way a report
        # footguns Engineering: it produces the same period for ever and looks like it
        # is working, so nothing downstream ever complains.
        w("┌" + "─" * 74 + "┐")
        for line in [
            "!! DO NOT SCHEDULE THIS FILE AS IT STANDS",
            "",
            f"Its window is pinned to {g['range_start']} .. {g['range_end']}, which is",
            "right for the one-off client ask it was built for. Run on a schedule, it",
            "would produce that same period over and over, for ever, and give no sign",
            "that anything was wrong.",
            "",
            "To deploy it, put it back on a cadence: WINDOW_MODE below, or have the",
            'researcher press "Make it recurring" in Pipelines Studio and send it',
            "again.",
        ]:
            w("│ " + line.ljust(72) + " │")
        w("└" + "─" * 74 + "┘")
        w("")

    if g["notes"]:
        # Lifted out of the "Notes for Engineering" box and into the docstring. The
        # researcher's own line breaks are kept — only a line too long for the file is
        # wrapped — because a note is usually a list and reflowing it into a paragraph
        # loses the list.
        w("┌" + "─" * 74 + "┐")
        w("│ NOTES FOR ENGINEERING — not implemented below. Please wire these by hand. │")
        w("└" + "─" * 74 + "┘")
        for line in g["notes"].splitlines():
            text = line.rstrip()
            if not text.strip():
                O.append("")
            elif len(text) <= 76:
                O.append("  " + text.strip() if not text.startswith(" ") else text)
            else:
                O.extend(_wrap(text.strip()))
        for line in O:
            w(line)
        w("")
    w("GUARDRAILS BAKED IN")
    w("  1. ONE REQUEST PER SECTION x CHANNEL — never one request listing every channel.")
    w("     Channels do OR correctly inside the filter, but when a result truncates the")
    w("     cut is taken in date order, so one shared cap starves the quiet channel.")
    w("     Measured on this archive: Banking / Consumer / credit unions over July 2026,")
    w("     three channels, limit 300 -> 258 Social Media, 41 Email, 1 Direct Mail,")
    w("     against true totals of 1583 / 367 / 116.")
    w("  2. limit IS THE PROBED EXACT TOTAL, never rounded up. Rounding up is the one")
    w("     performance mistake this API makes easy: with nothing to stop it the query")
    w("     reads to the end of the index (documented at 41s, versus 1.6s).")
    w(f"  3. The window is bounded server-side by {g['date_field']}. The three date")
    w("     fields disagree — search_date is the date the piece carries, approved_date")
    w("     is when it was released, added_to_database is when it was loaded.")
    w("  4. total / truncated / total_is_capped are READ off the response. There is no")
    w('     "at least N" guessing left. total_is_capped means slice the window, not')
    w("     retry with a bigger limit.")
    w("  5. Flags are tri-state. Omitting one matches either; false matches only pieces")
    w("     explicitly recorded as NOT carrying it. Only flags the researcher actually")
    w("     set appear in SECTIONS below.")
    w('  6. Range filters: 0 means "not stated" and never matches, so a range starting')
    w("     at 0 is not the same as no filter.")
    w("  7. resolved_filters is printed per section. A sector matches every node beneath")
    w("     it and a parent affinity category matches its children, so a count can move")
    w("     without the request changing.")
    w("  8. 429 and 503 retry with backoff inside cs_api; quota_exceeded stops the run,")
    w("     because retrying cannot succeed until the month rolls over. Every request")
    w("     counts against the quota, errors included.")
    w("  9. Counts, dedup and chunking are computed in Python. The model only picks")
    w("     entry_ids and writes prose.")
    if g["deck_on"]:
        w('  10. Slides hold at most 5 entries; overflow becomes "(cont.)" slides.')
    if g["any_ocr"]:
        w("  11. The scanned text comes from GET /v1/ocr, one request per piece and no")
        w("      batch form. So it is read for the pieces that were CHOSEN, after the")
        w("      pick — not for every candidate before it. Printing it as a worksheet")
        w("      column reads every row of the tab instead, and is capped.")
    if any_db:
        w("  12. The API row is 15 columns, and there is no output projection. The few")
        w("      worksheet columns that are not on it and not on /v1/ocr come from the")
        w("      database, which needs the tunnel. FILTERING never needs it.")
    if email.get("enabled"):
        w(f"  * Deliverables are emailed when the run finishes, to whoever"
          f" {g['email_var']} names on the box that runs this. No address is stored"
          f" in this file.")
    w('"""')
    w("")

    # ── imports ─────────────────────────────────────────────────────────────────────
    w("import argparse")
    w("import json")
    w("import os")
    if g["excludes"] or g["collapses"]:
        w("import re")
    w("import sys")
    w("from datetime import date, datetime, timedelta")
    w("from pathlib import Path")
    w("")
    w("try:  # a cp1252 console must not crash on the glyphs above")
    w('    sys.stdout.reconfigure(encoding="utf-8", errors="replace")')
    w("except Exception:")
    w("    pass")
    w("")
    w("PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent")
    w("sys.path.insert(0, str(PROJECT_ROOT))")
    w("")
    if g["deck_on"]:
        w("# Raise the builder timeout BEFORE the builder module is imported.")
        w('os.environ.setdefault("PPT_BUILDER_TIMEOUT", "300")')
        w("")
    w("# The SAME client Pipelines Studio previews with, so a previewed count and this")
    w("# run cannot disagree. Stdlib only, so it adds no dependency of its own.")
    w("import pipelines.cs_api as CS  # noqa: E402")
    w("import pipelines.report_lib as L  # noqa: E402")
    if g["any_sql"]:
        w("import pipelines.report_lib_excel_helper as XH  # noqa: E402")
    w("")
    if g["deck_on"]:
        w('build_deck_default = L.load_tool("mcp_pptbuilder", "build_deck_default")')
    if any_db:
        w('_run_sql           = L.load_tool("mcp_serverv3", "_run_sql")')
    if g["deck_on"] or any_db:
        w("")
    w("")

    # ── settings ────────────────────────────────────────────────────────────────────
    w("# ── Report settings " + "─" * 58)
    w(f"CLIENT       = {_lit(client)}")
    if g["fixed_window"]:
        w("# A ONE-OFF. This window is two fixed dates, so every run of this file covers")
        w("# the same period — which is what a one-time client ask wants and what a")
        w('# schedule must never have. Put WINDOW_MODE back to "cadence" before')
        w("# deploying it. See the banner at the top of this file.")
        w('WINDOW_MODE  = "range"')
        w(f"RANGE_START  = {_lit(g['range_start'])}")
        w(f"RANGE_END    = {_lit(g['range_end'])}")
    else:
        w('WINDOW_MODE  = "cadence"')
    w(f"CADENCE      = {_lit(g['cadence'])}       # week | month")
    w(f"ANCHOR       = {_lit(g['anchor'])}")
    w(f"DATE_FIELD   = {_lit(g['date_field'])}")
    w('PERIOD_START = os.environ.get("RS_PERIOD_START") or None   # "2026-06-01" overrides')
    w('PERIOD_END   = os.environ.get("RS_PERIOD_END") or None')
    if g["deck_on"]:
        w("SLIDE_CAP    = 5     # builder hard limit: 5 entries per slide")
    if g["featured"]:
        w("# How many pieces the model SEES when choosing. Nothing to do with how many")
        w("# the search returned or the workbook prints — those can be thousands, and a")
        w("# candidate list that long is a six-figure-token prompt per section for a")
        w("# decision that picks four. Measured on this archive: a candidate line runs")
        w("# ~180 characters, so 3,000 of them is ~534,000 characters (~133k tokens).")
        w("CANDIDATE_CAP = 300")
    if g["any_ocr"]:
        # Emitted wherever _state_write writes an ocr block, which is any_ocr rather
        # than featured — a workbook-only report with an OCR column has no picks to
        # approve but still writes its text into the state file.
        w("# A run written to a state file carries the scanned text it already read, so")
        w("# the second half does not pay for it twice. The text is trimmed on the way")
        w("# in: the largest single piece in this archive runs past a million")
        w("# characters, and no reader downstream looks past 900 — the worksheet cell")
        w("# and the write-up candidate line both cut there. So the trim is documented,")
        w("# changes no output, and the state file is NOT a copy of the archive's text.")
        w("STATE_OCR_CHARS = 1000")
    if g["prints_ocr"]:
        w("# The OCR Text column reads one piece per request, so a wide tab could spend")
        w("# hundreds of quota units on a column nobody reads to the end. Raise it if you")
        w("# genuinely need the text for every row.")
        w("OCR_ROW_CAP  = 150")
    w("# RS_OUTPUT_DIR lets one run write into its own directory — Pipelines Studio")
    w("# sets it so a run's files cannot overwrite the previous run's. Unset (which is")
    w("# how a deployed pipeline runs) everything lands in the usual output/ folder.")
    w('OUTPUT_DIR   = Path(os.environ.get("RS_OUTPUT_DIR") or (PROJECT_ROOT / "output"))')
    # ── the recipient, by NAME ─────────────────────────────────────────────────
    # This file never carries an address. That is the whole guardrail: a report saved
    # in the Studio, mailed to Engineering and committed to a repo cannot leak a
    # client contact it was never meant to keep. Two ways in, both from the
    # environment, both optional, and the run succeeds with neither:
    #
    #   RS_EMAIL_TO   set by whoever starts one run — a researcher emailing a one-off
    #                 to a colleague. Held for that process and nowhere else.
    #   the report's own variable, set by Engineering on the box that runs it.
    var = g["email_var"]
    if email.get("enabled"):
        w("# Opt-in, and by variable NAME only — see the note in the Studio. Engineering")
        w(f"# sets {var} on the box that runs this. RS_EMAIL_TO overrides it for a single")
        w("# run. Neither set means nothing is emailed and the run still succeeds; the")
        w("# files are on disk either way.")
    else:
        w("# This report does not email anything on a schedule. A one-off run can still")
        w("# supply a recipient in RS_EMAIL_TO, which is held for that run and nowhere")
        w("# else. No address is ever stored in this file.")
    if email.get("enabled") and var != "RS_EMAIL_TO":
        w(f'EMAIL_TO     = (os.environ.get("RS_EMAIL_TO")')
        w(f'                or os.environ.get({_lit(var)}) or None)')
    else:
        w('EMAIL_TO     = os.environ.get("RS_EMAIL_TO") or None')
    w("")
    if tabs:
        w("# pdf_url is on the search row now, so neither link is reassembled from parts.")
        w("HYPERLINKS = {")
        w('    "EntryID":     ("https://cp.competiscan.com/productdetail?id={pid}",')
        w('                    "{entry_id}"),')
        w('    "PDF Content": ("{pdf_url}", "PDF Content"),')
        w("}")
        w("")
        w("# Worksheet column -> the key it reads off the API search row. Anything absent")
        w("# from here is either derived or comes from the database.")
        w("API_FIELD = {")
        for col, field in API_FIELD.items():
            if col in g["all_cols"]:
                w(f"    {_pystr(col)}: {_pystr(field)},")
        w("}")
        w("")
        if g["date_cols"]:
            w("# The API returns full ISO timestamps; a worksheet wants the day.")
            w(f"DATE_COLUMNS = {_lit(sorted(g['date_cols']), 15)}")
            w("")

    # ── sections ────────────────────────────────────────────────────────────────────
    w("# ── Sections: search -> worksheet -> feature -> slide " + "─" * 25)
    w("SECTIONS = [")
    for s in g["sections"]:
        se = s.get("search") or {}
        sh = s.get("sheet") or {}
        fe = s.get("feature") or {}
        tab_name = ((sh.get("tab") or "").strip() or (s.get("title") or "Sheet").strip()) \
            if (g["book_on"] and sh.get("enabled")) else None
        w("    {")
        w(f'        "id": {_lit(s.get("id"))},')
        w(f'        "title": {_lit(s.get("title") or "")},')
        if g["headings_on"]:
            w(f'        "heading": {_lit(s.get("heading") or "")},')
        w('        "search": {')
        for key in _IF_SET:
            val = se.get(key)
            if val in (None, "", [], {}):
                continue
            # A match-mode scalar carries no information without the list it modifies.
            if key == "company_match" and not se.get("company"):
                continue
            if key == "ocr_text_match" and not se.get("ocr_text"):
                continue
            if key == "panelist_type" and not se.get("panelist_id"):
                continue
            w(f"            {_pystr(key)}: {_lit(val, 12)},")
        for key in _ALWAYS:
            val = se.get(key)
            if key == "row_cap":
                val = max(1, min(_int(val, CS.LIMIT_MAX), CS.LIMIT_MAX))
            elif key == "max_per_creative":
                val = _int(val, 2)
            elif key == "collapse_repeats":
                val = bool(val)
            elif key == "company_must_not_match":
                val = str(val or "")
            elif key == "enhanced":
                # A flag on "Any" was removed in the UI; this is the belt-and-braces
                # version, so an empty selection can never become a real filter.
                val = {k: v for k, v in (val or {}).items()
                       if v is not None and v != [] and v != ""}
            else:
                val = val or []
            # `enhanced` is the block Engineering reviews. Rendered inline it runs
            # past 600 characters on a busy section, so it breaks into one filter
            # per line once it stops fitting.
            if key == "enhanced" and len(_lit(val, 12)) > 58:
                w(f"            {_pystr(key)}: {{")
                for name in sorted(val):
                    w(f"                {_pystr(name)}: {_lit(val[name], 16)},")
                w("            },")
            else:
                w(f"            {_pystr(key)}: {_lit(val, 12)},")
        w("        },")
        w(f'        "tab": {_lit(tab_name)},')
        w(f'        "ocr": {_lit(bool(needs_ocr(s)))},')
        if prints_ocr(s):
            w('        "print_ocr": True,')
        w(f'        "feature": {_lit(bool(fe.get("enabled") and g["deck_on"]))},')
        if fe.get("enabled") and g["deck_on"]:
            w(f'        "count": {_int(fe.get("count"), 4)},')
            w(f'        "callout_limit": {_int(fe.get("callout_limit"), 374)},')
            w(f'        "one_per_company": {_lit(bool(fe.get("one_per_company")))},')
            w(f'        "never_reuse": {_lit(bool(fe.get("never_reuse")))},')
            w(f'        "mention_total": {_lit(bool(fe.get("mention_total")))},')
            w(f'        "how_to_choose": {_lit(str(fe.get("how_to_choose") or ""))},')
            w(f'        "what_to_say": {_lit(str(fe.get("what_to_say") or ""))},')
        w("    },")
    w("]")
    w("")

    if tabs:
        w("# ── Worksheet tabs. Sections sharing a name write into the same sheet. ──────")
        w("TABS = [")
        for t in tabs:
            w("    {")
            w(f'        "name": {_lit(t["name"])},')
            w(f'        "section_ids": {_lit(t["section_ids"])},')
            w('        "columns": [')
            line = "            "
            for c in t["columns"]:
                piece = _pystr(c) + ", "
                if len(line) + len(piece) > 90:
                    w(line.rstrip())
                    line = "            "
                line += piece
            if line.strip():
                w(line.rstrip().rstrip(","))
            w("        ],")
            w(f'        "database": {_lit(needs_sql(t["columns"]))},')
            w("    },")
        w("]")
        w("")

    if g["summary_on"]:
        w("# ── Summary slide: the LAST model call, reading the finished write-ups ──────")
        w(f'SUMMARY_TITLE1 = {_lit("{period} — what stood out")}')
        w(f'SUMMARY_TITLE2 = {_lit("{period} — also worth noting")}')
        w("SUMMARY_MAX_WORDS = 55")
        w("SUMMARY_SYSTEM = (")
        w('    "You are a competitive-intelligence analyst writing the opening summary of "')
        w('    "a client deck. You are given the write-ups that are already in the deck. "')
        w('    "Distil them into two short paragraphs: the first for the most important "')
        w('    "themes, the second for secondary observations. Name companies and their "')
        w('    "specific offers. Use ONLY what you are given.\\n\\n"')
        w('    "Never claim that anything was absent, quiet, flat or unchanged, and "')
        w('    "never say a competitor or category had no activity. These write-ups are "')
        w('    "a slice of one period of one archive, so they are not evidence that "')
        w('    "anything did NOT happen.\\n\\n"')
        w('    \'Reply with ONE JSON object: {"column1": "...", "column2": "..."}\'')
        w(")")
        w("")

    # ── helpers ─────────────────────────────────────────────────────────────────────
    w("")
    w("# ── Helpers " + "─" * 66)
    w("def _parse_args():")
    w('    p = argparse.ArgumentParser(description=f"{CLIENT} report")')
    modes = ["search"] + (["excel"] if tabs else []) + (
        ["deck"] if g["deck_on"] else []) + ["all"]
    w(f'    p.add_argument("--only", default="all", choices={_lit(modes)},')
    w('                   help="Stop after a stage — cheap iteration while testing.")')
    w('    p.add_argument("--limit", type=int, default=None,')
    w('                   help="Cap rows per channel (small = fast test).")')
    w('    p.add_argument("--state", default=None,')
    w('                   help="Where this run writes, or reads back, its state.")')
    if g["featured"]:
        w('    p.add_argument("--phase", default="all",')
        w('                   choices=["all", "pick", "build", "replace"],')
        w('                   help="all runs start to finish, and is what a schedule "')
        w('                        "uses. pick stops after the selection so a person "')
        w('                        "can approve it. build carries on from a state file "')
        w('                        "without searching again. replace prints the next "')
        w('                        "valid candidate for a rejected pick.")')
        w('    p.add_argument("--approved", default=None,')
        w('                   help="--phase build: a JSON file mapping section id to "')
        w('                        "the entry_ids a person approved. The write-ups "')
        w('                        "describe THOSE pieces and no others.")')
        w('    p.add_argument("--section", default=None,')
        w('                   help="--phase replace: which section to replace within.")')
        w('    p.add_argument("--keep", default="",')
        w('                   help="--phase replace: ids still approved on this "')
        w('                        "section, comma-separated.")')
        w('    p.add_argument("--reject", default="",')
        w('                   help="--phase replace: ids rejected on this section, "')
        w('                        "which must never come back.")')
        w('    p.add_argument("--used", default="",')
        w('                   help="--phase replace: ids already on OTHER sections.")')
    w("    return p.parse_args()")
    w("")
    w("")
    w("def _window():")
    w('    """(start, end). The env vars win; then a fixed range when this report')
    w("    has one; otherwise cadence and anchor decide. prior_complete is reproducible:")
    w('    running it again tomorrow covers the same dates."""')
    w("    if PERIOD_START:")
    w("        s = date.fromisoformat(PERIOD_START)")
    w("        if PERIOD_END:")
    w("            return s, date.fromisoformat(PERIOD_END)")
    w('        return s, (s + timedelta(days=7) if CADENCE == "week" else _month_end(s))')
    if g["fixed_window"]:
        w('    if WINDOW_MODE == "range":')
        w("        # Deliberately NOT recomputed from today. See the banner above.")
        w("        return date.fromisoformat(RANGE_START), date.fromisoformat(RANGE_END)")
    w("    today = date.today()")
    w('    if CADENCE == "week":')
    w('        if ANCHOR == "rolling":')
    w("            return today - timedelta(days=7), today")
    w("        monday = today - timedelta(days=today.weekday())")
    w("        return monday - timedelta(days=7), monday")
    w('    if ANCHOR == "rolling":')
    w("        return today - timedelta(days=30), today")
    w("    prev_end = today.replace(day=1) - timedelta(days=1)")
    w("    return prev_end.replace(day=1), prev_end")
    w("")
    w("")
    w("def _month_end(d):")
    w("    nxt = (d.replace(day=28) + timedelta(days=4)).replace(day=1)")
    w("    return nxt - timedelta(days=1)")
    w("")
    w("")
    if g["cadence"] == "week":
        w("def _ordinal(n):")
        w('    suffix = "th" if 11 <= n % 100 <= 13 else {1: "st", 2: "nd",')
        w('                                               3: "rd"}.get(n % 10, "th")')
        w('    return f"{n}{suffix}"')
        w("")
        w("")
    if "Quarter" in g["all_cols"]:
        w("def _quarter(entry_id):")
        w('    """entry_id is YYYY-MM-DD-NNNN, and that date is search_date — the date the')
        w('    piece itself carries."""')
        w("    try:")
        w('        y, m = str(entry_id).split("-")[:2]')
        w('        return f"{int(y)} Q{(int(m) - 1) // 3 + 1}"')
        w("    except (ValueError, TypeError):")
        w('        return ""')
        w("")
        w("")
    w("def _dedup(records):")
    w('    """product_id is the archive\'s primary key, so it is what identifies a piece.')
    w("    A row with no entry_id is dropped: it cannot go on a slide and it cannot be")
    w('    looked up in the database, so it has nowhere to go."""')
    w("    seen, out = set(), []")
    w("    for r in records:")
    w('        pid = r.get("product_id")')
    w('        if pid in seen or not r.get("entry_id"):')
    w("            continue")
    w("        seen.add(pid)")
    w("        out.append(r)")
    w("    return out")
    w("")
    w("")
    if g["excludes"] or g["collapses"]:
        w("def _theme(record):")
        w('    """A coarse creative fingerprint: company plus the first few headline words.')
        w("    Stops one recycled evergreen ad from filling every slot on a slide.\"\"\"")
        w('    co = (record.get("company") or "").lower().strip()')
        w('    head = re.sub(r"[^a-z0-9 ]", "",')
        w('                  (record.get("product_headline") or "").lower())')
        w("    return f\"{co}|{' '.join(head.split()[:6])}\"")
        w("")
        w("")
        w("def _filter(records, sec):")
        w('    """The narrowings the archive cannot express as filters. This is a short')
        w("    list on purpose — everything else was done server-side — and neither of")
        w('    these asks the model to do the filtering."""')
        w('    search = sec["search"]')
        w("    out = list(records)")
        w("")
        w("    def report(label, before):")
        w("        if len(out) != before:")
        w('            print(f"      {label}: {before} -> {len(out)}")')
        w("")
        if g["excludes"]:
            w('    if search["company_must_not_match"]:')
            w("        # The archive has no negation filter, so an exclusion cannot make")
            w("        # the search cheaper — only the result narrower.")
            w("        n = len(out)")
            w("        out = [r for r in out")
            w('               if not re.search(search["company_must_not_match"],')
            w('                                r.get("company") or "", re.I)]')
            w('        report("company must not match", n)')
        w('    if search["collapse_repeats"]:')
        w("        n, counts, kept = len(out), {}, []")
        w("        for r in out:")
        w("            k = _theme(r)")
        w('            if counts.get(k, 0) < search["max_per_creative"]:')
        w("                counts[k] = counts.get(k, 0) + 1")
        w("                kept.append(r)")
        w("        out = kept")
        w('        report("collapse repeated creative", n)')
        w("    return out")
        w("")
        w("")

    # ── the search core ─────────────────────────────────────────────────────────────
    w("def _collect(sec, channel, start, end, cap):")
    w('    """Probe for the exact total, then fetch exactly that many rows.')
    w("")
    w("    The probe is one cheap request at limit=1. It is not only a count: the count")
    w("    runs BEFORE the rows, so an exact, uncapped total no greater than the row")
    w("    limit is PROOF the limit cannot fill, and that proof is what lets the server")
    w("    drive the row query off the date index instead of walking the primary key to")
    w("    the end of it. Asking for the count is what makes the fetch fast, not slow.")
    w("")
    w("    Past the per-call ceiling the window is split by month and each slice repeats")
    w("    both steps. That is the only way to get a whole result set: there is no cursor")
    w("    and no offset to loop over, and narrowing is cheap because every date column")
    w("    is indexed.")
    w("")
    w("    cap is a budget for this channel across every slice, so the row cap means")
    w('    what it says however many times the window is split."""')
    w('    base = CS.build_body(sec["search"], channel=channel, date_field=DATE_FIELD)')
    w("    rows, notes, resolved = [], [], None")
    w("    # cap is a budget for the whole CHANNEL, not for each slice. Spent per")
    w("    # slice, a 500-row cap would quietly become 500 rows a MONTH as soon as")
    w("    # the window splits.")
    w("    budget = cap")
    w("    archive_total = 0   # what the archive holds, before any cap of ours")
    w("    # True when a slice came back past the archive's own count cap and could not")
    w("    # be split any finer. The total is then a LOWER BOUND, and every consumer of")
    w('    # it — the printout, the state file, the deck — must say "at least N" rather')
    w("    # than quote it as a fact.")
    w("    lower_bound = False")
    w("    pending = [(start, end)]")
    w("    while pending:")
    w("        s, e = pending.pop(0)")
    w('        body = {**base, "date_from": str(s), "date_to": str(e)}')
    w("        try:")
    w("            probe = CS.count(body)")
    w("        except CS.ApiError as exc:")
    w('            notes.append(f"{channel} {s}..{e}: {exc.code} — {exc.hint()}")')
    w('            if exc.code == "quota_exceeded":')
    w("                raise")
    w("            continue")
    w("        if resolved is None:")
    w('            resolved = probe.get("resolved_filters") or {}')
    w('        total = int(probe.get("total") or 0)')
    w('        capped = bool(probe.get("total_is_capped"))')
    w("        # Counted BEFORE the row cap and the per-call ceiling touch it: this")
    w("        # is the number that reconciles against PowerSearch, and the only one")
    w("        # a write-up may state as the volume for the period.")
    w("        archive_total += total")
    w("        if not total and not capped:")
    w("            continue")
    w("        if capped or total > CS.LIMIT_MAX:")
    w("            parts = CS.month_slices(s, e)")
    w("            if len(parts) > 1:")
    w("                pending = parts + pending")
    w("                continue")
    w("            # One month and still past the ceiling. Say so, rather than let a")
    w("            # truncated slice look complete.")
    w('            notes.append(')
    w('                f"{s}..{e}: {\'past the count cap\' if capped else total} in a "')
    w('                f"single month, over the {CS.LIMIT_MAX:,}-row ceiling — took the "')
    w('                f"newest {CS.LIMIT_MAX:,}. Narrow this section.")')
    w("            total = CS.LIMIT_MAX")
    w("            lower_bound = lower_bound or capped")
    w("        want = min(total, budget)")
    w("        if want < total:")
    w('            notes.append(f"{channel} {s}..{e}: {total} matched, the row cap "')
    w('                         f"held it to {want}.")')
    w("        if not want:")
    w("            # Budget spent. Stop probing the slices still pending rather")
    w("            # than paying for counts whose rows would be thrown away.")
    w('            notes.append(f"{channel}: row cap of {cap} reached — "')
    w('                         f"{len(pending) + 1} window slice(s) not fetched.")')
    w("            break")
    w("        try:")
    w('            res = CS.search({**body, "limit": want, "include_total": True,')
    w('                             "sort": "date"})')
    w("        except CS.ApiError as exc:")
    w('            notes.append(f"{channel} {s}..{e}: {exc.code} — {exc.hint()}")')
    w('            if exc.code == "quota_exceeded":')
    w("                raise")
    w("            continue")
    w('        got = res.get("results") or []')
    w("        rows.extend(got)")
    w("        budget -= len(got)")
    w('        if res.get("truncated") and want == total:')
    w("            # limit == the exact total, so this should not happen. When it does,")
    w("            # the archive gained matching rows between the probe and the fetch.")
    w('            notes.append(f"{channel} {s}..{e}: fetch came back truncated even "')
    w('                         f"at the exact total — the archive gained rows "')
    w('                         f"mid-run.")')
    w("    return rows, notes, resolved or {}, archive_total, lower_bound")
    w("")
    w("")

    if g["any_sql"]:
        w("def _lookup(entry_ids):")
        w('    """entry_ids -> the worksheet values that are NOT on the API search row.')
        w("    Only printing comes here; filtering never does. Only ids that have both a")
        w("    document and a primary-company mapping come back — the query inner-joins")
        w("    them.\"\"\"")
        w("    if not entry_ids:")
        w("        return {}")
        w("    df = _run_sql(XH.build_query(entry_ids))")
        w('    if df is None or getattr(df, "empty", True):')
        w("        return {}")
        w('    text_cols = {"additional_companies", "sectors", "categories",')
        w('                 "sub_categories", "states", "ages", "incomes",')
        w('                 "primary_company", "product_name", "product_headline",')
        w('                 "media_channel", "mailing_type", "entry_id"}')
        w("")
        w("    def clean(k, v):")
        w("        # pandas types GROUP_CONCAT oddly: all-NULL becomes NaN and all-numeric")
        w('        # becomes float. Flag columns MUST stay ints — "0" is truthy and would')
        w("        # flip Pre-Screen.")
        w("        if v is None or (isinstance(v, float) and v != v):")
        w("            return None")
        w("        return str(v) if k in text_cols else v")
        w("")
        w("    raw = [{k: clean(k, v) for k, v in rec.items()}")
        w('           for rec in df.to_dict("records")]')
        w("    out = {}")
        w("    for row, src in zip(XH.complete_rows(raw), raw):")
        w('        out[src.get("entry_id")] = row')
        w("    return out")
        w("")
        w("")

    if g["any_ocr"]:
        w("# ── Reading a piece " + "─" * 58)
        w("# entry_id -> the words read off its scanned pages, for pieces already")
        w("# fetched. Kept module-level because two stages want it: the worksheet")
        w("# column prints it and the write-up reads it, and neither should pay for")
        w("# the other's pieces twice.")
        w("OCR = {}          # entry_id -> text")
        w("OCR_TRIED = set() # asked already, hit or miss, so we never ask twice")
        w("")
        w("")
        w("def _read_ocr(entry_ids, cap=None):")
        w('    """Fill OCR for these ids. Returns notes worth printing.')
        w("")
        w("    GET /v1/ocr takes one entry_id and there is no batch form — each")
        w("    call is two keyed lookups, so a loop costs what a batch would. What a")
        w("    loop does cost is ONE QUOTA UNIT PER PIECE, which is the whole reason")
        w("    cap exists and the reason the caller is told when it bit rather than")
        w('    being handed a quietly short dictionary."""')
        w("    want = [e for e in dict.fromkeys(entry_ids) if e and e not in OCR_TRIED]")
        w("    if not want:")
        w("        return []")
        w("    tried = want[:cap] if cap else want")
        w("    got, notes = CS.ocr_texts(tried)")
        w("    OCR.update(got)")
        w("    OCR_TRIED.update(tried)")
        w("    if cap and len(want) > cap:")
        w('        notes.insert(0, f"{len(want)} pieces to read, capped at {cap}. '
          'Raise OCR_ROW_CAP or narrow the section.")')
        w("    return notes")
        w("")
        w("")
        w("")

    if tabs:
        w("def _row(record, sql, columns):")
        w('    """Build one worksheet row. A database value wins when we have one;')
        w("    otherwise the value comes off the API search row or is derived from it. A")
        w('    column with no source renders blank rather than being guessed at."""')
        w("    out = {}")
        w("    for col in columns:")
        w("        field = API_FIELD.get(col)")
        w("        if sql and sql.get(col):")
        w("            out[col] = sql[col]")
        w("        elif field:")
        w("            val = record.get(field)")
        if g["date_cols"]:
            w("            if col in DATE_COLUMNS and val:")
            w("                val = str(val)[:10]")
        w("            out[col] = L.clean_cell(val)")
        if "Quarter" in g["all_cols"]:
            w('        elif col == "Quarter":')
            w('            out[col] = _quarter(record.get("entry_id"))')
        if "OCR Text" in g["all_cols"]:
            w('        elif col == "OCR Text":')
            w('            out[col] = L.clean_cell(OCR.get(')
            w('                record.get("entry_id"), ""))[:900]')
        w("        else:")
        w('            out[col] = ""')
        w('    out["entry_id"] = record.get("entry_id") or ""')
        w('    out["pid"] = record.get("product_id") or ""')
        w('    out["pdf_url"] = record.get("pdf_url") or ""')
        w("    return out")
        w("")
        w("")

    if g["featured"]:
        w("# ── Prompts " + "─" * 66)
        w("CHOOSE_SYSTEM = (")
        w('    "You are a competitive-intelligence analyst choosing which pieces from the "')
        w('    "archive to feature on one slide of a client deck.\\n\\n"')
        w('    "RULES\\n"')
        w('    "- Work ONLY from the text supplied. Never invent an offer, rate or "')
        w('    "detail.\\n"')
        w('    "- Choose only from the candidate entry_ids given. Never output another "')
        w('    "id.\\n"')
        w('    "- Prefer variety: different companies and different offers beat "')
        w('    "near-duplicates of the same creative.\\n"')
        w('    "- Return them RANKED, strongest first. Only the first few reach "')
        w('    "the slide; the rest are held as replacements in case a reviewer "')
        w('    "rejects one. Rank the whole list on merit rather than padding "')
        w('    "the tail.\\n\\n"')
        w('    "WHAT BELONGS ON THIS SLIDE\\n{guidance}\\n\\n"')
        w('    \'Reply with ONE JSON object: {"entry_ids": ["..."], '
          '"reasoning": "one line"}\'')
        w(")")
        w("")
        w("WRITEUP_SYSTEM = (")
        w('    "You are writing the short paragraph under a slide of archive pieces.\\n\\n"')
        w('    "RULES\\n"')
        w('    "- Describe ONLY the pieces supplied. Never invent details.\\n"')
        w('    "- Under {limit} characters, in whole sentences.\\n"')
        w('    "- Analyst voice. No bullet points and no preamble.\\n"')
        w('    "- Only state a piece count if one is given to you below, and only in "')
        w('    "those exact words.\\n\\n"')
        w('    "- Never say a company, product or theme was absent, quiet or unchanged. "')
        w('    "You are shown the pieces chosen for this one slide, which is not "')
        w('    "evidence about anything else.\\n"')
        w('    "STYLE\\n{style}\\n\\n"')
        w('    \'Reply with ONE JSON object: {"callout": "..."}\'')
        w(")")
        w("")
        w("")
        w("def _candidates(records, read=False):")
        w('    """A compact candidate list.')
        w("")
        w("    The API row carries the product name and the headline, and on this")
        w("    archive the headline is usually a whole sentence of offer detail —")
        w("    enough to pick four pieces out of thirty, which is what choosing")
        w("    needs.")
        w("")
        w("    read=True appends the scanned page, which is what a WRITE-UP needs:")
        w("    the rate, the term and the fine print are on the page rather than in")
        w("    the headline. It uses only what is already in OCR and never triggers")
        w('    a fetch of its own."""')
        w("    lines = []")
        w("    for r in records:")
        w('        line = (f\'- {r.get("entry_id")} | {r.get("company") or "?"}\'')
        w('                f\' | {r.get("media_channel") or "?"}\'')
        w('                f\' | {L.clean_cell(r.get("product_name"))[:90]}\'')
        w('                f\' | {L.clean_cell(r.get("product_headline"))[:320]}\')')
        w("        if read:")
        w('            text = OCR.get(r.get("entry_id"))')
        w("            if text:")
        w('                line += f" | {L.clean_cell(text)[:900]}"')
        w("        lines.append(line)")
        w('    return "\\n".join(lines)')
        w("")
        w("")
        w("def _shortlist(records, cap):")
        w('    """At most `cap` pieces, spread across companies.')
        w("")
        w("    Straight truncation would bias the pick twice over: rows arrive in")
        w("    date order, so the tail is simply dropped, and one prolific issuer can")
        w("    fill the whole list. Since the model is asked to prefer variety, the")
        w("    shortlist gives every company a turn before any gets a second slot.")
        w("")
        w("    The pieces left out are still eligible: pick_ids tops the final")
        w('    selection up from the FULL pool, not from this shortlist."""')
        w("    if len(records) <= cap:")
        w("        return records, None")
        w("    by_co = {}")
        w("    for r in records:")
        w('        by_co.setdefault((r.get("company") or "?").lower(), []).append(r)')
        w("    queues = list(by_co.values())")
        w("    out = []")
        w("    while len(out) < cap and any(queues):")
        w("        for q in queues:")
        w("            if q and len(out) < cap:")
        w("                out.append(q.pop(0))")
        w('    note = (f"{len(records)} candidates; showed the model {len(out)} of '
          'them, "')
        w('            f"spread across {len(by_co)} companies. The rest stay '
          'eligible.")')
        w("    return out, note")
        w("")
        w("")
        w("# A slide holds `count` pieces, and the model is asked for RESERVE_FACTOR")
        w("# times that many, ranked. The extras cost nothing extra — it is one call")
        w("# either way, with a longer list — and they are what a reviewer gets when")
        w("# they reject a pick: the next piece the MODEL ranked, rather than whatever")
        w("# happened to be next in date order.")
        w("RESERVE_FACTOR = 2")
        w("")
        w("")
        w("def _choose(sec, records):")
        w("    # These run on a thread pool, so a long stage would otherwise print")
        w("    # nothing at all until every section had finished. One line as each")
        w("    # lands is what makes a two-minute stage look alive rather than hung.")
        w("    if not records:")
        w("        print(f\"      {sec['title'][:34]:34} nothing to choose from\")")
        w('        return {"entry_ids": []}')
        w('    guidance = sec["how_to_choose"] or "No specific guidance was given."')
        w('    system = CHOOSE_SYSTEM.replace("{guidance}", guidance)')
        w('    want = sec["count"] * RESERVE_FACTOR')
        w("    title, n = sec[\"title\"], sec[\"count\"]")
        w("    prompt = (f'Choose up to {want} pieces for the \"{title}\" slide, '")
        w("              f'ranked strongest first. The first {n} go on the slide; '")
        w("              f'the rest are held in reserve, in case a reviewer '")
        w("              f'rejects one.\\n\\n'")
        w("              f'CANDIDATES\\n{_candidates(records)}')")
        w("    try:")
        w("        out = L.extract_json(L.call_claude(system, prompt))")
        w("    except Exception as exc:")
        w('        out = {"error": str(exc), "entry_ids": []}')
        w("    print(f\"      {sec['title'][:34]:34} chose\"")
        w("          f\" {len(out.get('entry_ids') or [])} of {len(records)}\")")
        w("    return out")
        w("")
        w("")
        w("def _writeup(sec, chosen, archive_total, at_least=False):")
        w("    if not chosen:")
        w('        return {"callout": ""}')
        w('    style = sec["what_to_say"] or "Plain analyst prose."')
        w('    system = (WRITEUP_SYSTEM.replace("{limit}", str(sec["callout_limit"]))')
        w('              .replace("{style}", style))')
        w("    # archive_total is what the ARCHIVE holds for this section and period,")
        w("    # counted by the archive itself BEFORE the row cap trimmed what we")
        w("    # actually pulled down. It is the only count that reconciles against")
        w("    # PowerSearch, so it is the only one a client deck may state.")
        w("    #")
        w("    # at_least means the archive stopped counting before it finished. The")
        w("    # number is then a floor, and the model is handed it as a floor: a deck")
        w("    # may say 'at least 25,000' and may never say '25,000'.")
        w('    if sec["mention_total"]:')
        w("        if at_least:")
        w('            note = (f"At least {archive_total} piece(s) were captured in "')
        w('                    f"this period — the archive stopped counting, so that "')
        w('                    f"figure is a floor. Say so if you state it.")')
        w("        else:")
        w('            note = f"{archive_total} piece(s) were captured in this period."')
        w("    else:")
        w('        note = f"{len(chosen)} piece(s) are featured on this slide."')
        w('    prompt = (f\'Slide: "{sec["title"]}". {note}\\n\\n\'')
        w('              f\'FEATURED PIECES\\n{_candidates(chosen, read=True)}\')')
        w("    try:")
        w("        out = L.extract_json(L.call_claude(system, prompt))")
        w("    except Exception as exc:")
        w('        out = {"error": str(exc), "callout": ""}')
        w("    print(f\"      {sec['title'][:34]:34} written\")")
        w("    return out")
        w("")
        w("")

    if g["summary_on"]:
        w("def _summary(period_label, writeups):")
        w('    """The LAST model call. It reads the finished write-ups rather than the raw')
        w("    archive text: cheaper, and it cannot contradict the rest of the deck.\"\"\"")
        w('    prompt = (f"Period: {period_label}\\n\\n"')
        w('              "WRITE-UPS ALREADY IN THE DECK\\n" + "\\n".join(writeups))')
        w("    try:")
        w("        return L.extract_json(L.call_claude(SUMMARY_SYSTEM, prompt))")
        w("    except Exception as exc:")
        w('        return {"error": str(exc)}')
        w("")
        w("")

    # ── labels and the run-state file ─────────────────────────────────────────────
    w("# ── Labels and the run-state file " + "─" * 44)
    w("def _labels(start, end):")
    w('    """Every name the deliverables are built from, worked out once."""')
    if g["cadence"] == "week":
        w('    period = f"{end:%B} {_ordinal(end.day)}, {end.year}"')
    else:
        w('    period = f"{start:%B} {start.year}"')
    w("    return {")
    w('        "period": period,')
    w('        "stamp": end.strftime("%Y%m%d"),')
    w('        "mmddyy": end.strftime("%m%d%y"),')
    w('        "month_year": f"{start:%B}{start.year}",')
    w("    }")
    w("")
    w("")
    w("def _state_write(path, start, end, lab, found, sql_rows, final, reserve, why,")
    w("                 xlsx_path):")
    w('    """Write everything the second half of this run needs, and nothing it does not.')
    w("")
    w("    A HANDOVER, not a cache and not a log. It carries the records that were")
    w("    already fetched, which is what lets --phase build carry on without searching:")
    w("    approving a pick costs nothing, and rejecting one costs a replacement drawn")
    w("    from this same pool rather than a fresh archive request.")
    if g["any_ocr"]:
        w("")
        w("    The scanned text is whitespace-collapsed and cut at STATE_OCR_CHARS on the")
        w("    way in. It is NOT the full document — the longest piece in this archive")
        w("    runs past a million characters. Nothing downstream reads past 900, so the")
        w("    cut changes no output; it only keeps this file a sane size.")
    w('    """')
    w("    state = {")
    w('        "version": 1,')
    w('        "pipeline": Path(__file__).name,')
    w('        "client": CLIENT,')
    w('        "written_at": datetime.now().isoformat(timespec="seconds"),')
    w('        "period_label": lab["period"],')
    w('        "start": str(start), "end": str(end), "date_field": DATE_FIELD,')
    w('        "workbook": str(xlsx_path) if xlsx_path else None,')
    if g["any_ocr"]:
        w('        "ocr_trimmed_to": STATE_OCR_CHARS,')
        w('        "ocr_note": ("Whitespace-collapsed and cut at ocr_trimmed_to '
          'characters. "')
        w('                     "This is NOT the full scanned document."),')
    w('        "sections": [],')
    w('        "sql_rows": sql_rows,')
    if g["any_ocr"]:
        w('        "ocr": {k: L.clean_cell(v)[:STATE_OCR_CHARS]')
        w("                for k, v in OCR.items() if v},")
    w("    }")
    w("    for sec in SECTIONS:")
    w('        v = found.get(sec["id"]) or {}')
    w('        state["sections"].append({')
    w('            "id": sec["id"], "title": sec["title"], "tab": sec.get("tab"),')
    w('            "feature": bool(sec["feature"]),')
    if g["featured"]:
        w('            "count": sec.get("count"),')
        w('            "one_per_company": bool(sec.get("one_per_company")),')
        w('            "never_reuse": bool(sec.get("never_reuse")),')
    w('            "archive_total": v.get("archive_total") or 0,')
    w("            # True means the archive stopped counting, so the total above is a")
    w('            # LOWER BOUND and every reader must say "at least N" rather than')
    w("            # quote it as a fact.")
    w('            "at_least": bool(v.get("at_least")),')
    w('            "fetched": v.get("fetched") or 0,')
    w('            "kept": len(v.get("records") or []),')
    w('            "picks": list(final.get(sec["id"]) or []),')
    w("            # What the model ranked below the slide. A swap spends these, in")
    w("            # order, before it falls back to walking the archive.")
    w('            "reserve": list(reserve.get(sec["id"]) or []),')
    w('            "reasoning": why.get(sec["id"]) or "",')
    w('            "records": v.get("records") or [],')
    w("        })")
    w("    path = Path(path)")
    w("    path.parent.mkdir(parents=True, exist_ok=True)")
    w('    path.write_text(json.dumps(state, indent=1, default=str), encoding="utf-8")')
    w("    size = path.stat().st_size")
    w('    print(f"   state  {path}  ({size / 1e6:.1f} MB)")')
    w("    return path")
    w("")
    w("")

    if g["featured"]:
        w("def _state_read(path):")
        w('    return json.loads(Path(path).read_text(encoding="utf-8"))')
        w("")
        w("")
        w("# ── Which pieces a section may still use " + "─" * 37)
        w("# The selection rules are written down HERE and nowhere else, so the first")
        w("# pick, the top-up behind a short model answer, and a replacement for a piece")
        w("# a person rejected all obey exactly the same ones.")
        w("def _eligible(records, taken, rejected):")
        w('    """Candidates still open to this section, in the order the archive')
        w("    returned them.")
        w("")
        w("      - not already on another section's slide, when never_reuse is set")
        w("      - not already chosen for this one")
        w("      - not rejected by a person")
        w("")
        w("    one_per_company is deliberately NOT here: it depends on what has been")
        w('    kept so far rather than on the piece itself, so it gets its own pass."""')
        w("    block = set(taken) | set(rejected)")
        w("    return [r for r in records")
        w('            if r.get("entry_id") and r["entry_id"] not in block]')
        w("")
        w("")
        w("def _one_per_company(ids, records):")
        w('    """Keep the first piece from each company and drop the rest."""')
        w('    by_id = {r["entry_id"]: r for r in records if r.get("entry_id")}')
        w("    seen, kept = set(), []")
        w("    for eid in ids:")
        w('        co = (by_id.get(eid, {}).get("company") or eid).lower()')
        w("        if co not in seen:")
        w("            seen.add(co)")
        w("            kept.append(eid)")
        w("    return kept")
        w("")
        w("")
        w("def _card(r):")
        w('    """One piece, with as much of it as a person needs to judge the pick."""')
        w("    return {")
        w('        "entry_id": r.get("entry_id"), "product_id": r.get("product_id"),')
        w('        "company": r.get("company"), "channel": r.get("media_channel"),')
        w('        "date": str(r.get("search_date") or "")[:10],')
        w('        "headline": L.clean_cell(r.get("product_headline"))[:300],')
        w('        "product": L.clean_cell(r.get("product_name"))[:120],')
        w('        "pdf_url": r.get("pdf_url"),')
        w("    }")
        w("")
        w("")
        w("def _replace(args):")
        w('    """Print the next valid candidate for a rejected pick. Nothing else.')
        w("")
        w("    This lives in the pipeline rather than in the Studio so a replacement")
        w("    obeys the SAME rules the original pick obeyed — there is one copy of them")
        w("    and it is _eligible, above. It never searches: the candidate comes out of")
        w("    the pool --phase pick already fetched, so a rejection costs nothing.")
        w('    """')
        w("    state = _state_read(args.state)")
        w('    sec = next((x for x in (state.get("sections") or [])')
        w('                if x.get("id") == args.section), None)')
        w("    if sec is None:")
        w('        print(json.dumps(')
        w('            {"error": f"no section {args.section} in the state file"}))')
        w("        return 1")
        w("")
        w("    def ids(raw):")
        w('        return [x.strip() for x in str(raw or "").split(",") if x.strip()]')
        w("")
        w("    keep, rejected = ids(args.keep), ids(args.reject)")
        w("    # Whether other sections' picks are off-limits is the SECTION's rule to")
        w("    # decide, not the caller's, so it is read here rather than trusted in.")
        w('    used = ids(args.used) if sec.get("never_reuse") else []')
        w('    records = sec.get("records") or []')
        w('    by_id = {r["entry_id"]: r for r in records if r.get("entry_id")}')
        w("")
        w("    # The model was asked for RESERVE_FACTOR times what the slide holds, and")
        w("    # ranked the lot. So the first place to look for a replacement is the")
        w("    # part of its own ranking that did not fit — a piece it actually judged,")
        w("    # in the order it judged it. Only once that is used up does this fall")
        w("    # back to the archive's own order, which is date, not merit.")
        w('    ranked = [by_id[e] for e in (sec.get("reserve") or []) if e in by_id]')
        w("    seen = {r[\"entry_id\"] for r in ranked}")
        w("    ordered = ranked + [r for r in records")
        w('               if r.get("entry_id") and r["entry_id"] not in seen]')
        w("    pool = _eligible(ordered, set(keep) | set(used), rejected)")
        w('    if sec.get("one_per_company"):')
        w('        by_id = {r["entry_id"]: r for r in records if r.get("entry_id")}')
        w('        blocked = {(by_id.get(e, {}).get("company") or "").lower()')
        w("                   for e in keep}")
        w("        pool = [r for r in pool")
        w('                if (r.get("company") or "").lower() not in blocked]')
        w("    if not pool:")
        w('        why = (f"The model\u2019s {len(sec.get(\'reserve\') or [])} reserve "')
        w('               f"pick(s) and all {len(records)} piece(s) fetched for this ")')
        w('        why += "section are used up: every one has been shown"')
        w('        why += " or rejected"')
        w('        if used:')
        w('            why += ", or is already on another slide"')
        w('        if sec.get("one_per_company"):')
        w('            why += ", or comes from a company already on this one"')
        w('        why += ". Widen the section or feature fewer pieces."')
        w('        print(json.dumps({"section": args.section, "replacement": None,')
        w('                          "exhausted": True, "reason": why}))')
        w("        return 0")
        w('    from_reserve = pool[0]["entry_id"] in set(sec.get("reserve") or [])')
        w('    print(json.dumps({"section": args.section, "replacement": _card(pool[0]),')
        w('                      "exhausted": False, "remaining": len(pool) - 1,')
        w('                      "from_reserve": from_reserve}))')
        w("    return 0")
        w("")
        w("")


def _emit_main(p: dict, g: dict, w) -> None:
    """The pipeline itself, as four named stages plus the entry point.

    v3 wrote one flat main(). It is split here because a researcher has to be able to
    stop the run between choosing the pieces and writing about them — and the only
    honest place to put that pause is INSIDE the generated file, so that what a person
    approves is judged by exactly the code Engineering deploys. Splitting it also puts
    every prose-writing model call on one side of the line, which is what makes the
    write-up describe the APPROVED set rather than the model's first suggestion.
    """
    tabs, deck, email = g["tabs"], g["deck"], g["email"]
    any_db = g["any_sql"]

    # ── Stage 1: search ─────────────────────────────────────────────────────────────
    w("# ── Pipeline " + "─" * 65)
    w("def stage_search(args, start, end):")
    w('    """Search, look up, post-filter. No model calls and no deliverables.')
    w("")
    w("    Returns (found, sql_rows), or (None, None) when every section came back")
    w('    empty and there is nothing worth continuing with."""')
    w("    # ── Step 1 — one probe + one fetch per section x channel ────────────────")
    w('    calls = sum(len(s["search"]["media_channel"]) for s in SECTIONS)')
    w('    print(f"\\nStep 1  Searching ({calls} section x channel, 2 requests each)…")')
    w("    found = {}")
    w("    for sec in SECTIONS:")
    w('        cap = min(args.limit or sec["search"]["row_cap"], sec["search"]["row_cap"])')
    w("        records, notes, resolved, archive = [], [], {}, 0")
    w("        at_least = False")
    w("        print(f\"   {sec['title']}\")")
    w('        for channel in sec["search"]["media_channel"]:')
    w("            # Sequentially, one channel at a time. The archive's REST backend")
    w("            # cross-contaminates results between concurrent calls carrying")
    w("            # different channels, so this loop must never become parallel.")
    w("            try:")
    w("                rows, ns, res, at, lb = _collect(sec, channel, start, end, cap)")
    w("            except CS.ApiError as exc:")
    w("                # quota_exceeded is the only error _collect re-raises. Nothing")
    w("                # downstream can succeed either, so stop while the numbers are")
    w("                # still honest.")
    w('                print(f"\\nERROR: {exc.hint()}")')
    w("                return None, None")
    w('            print(f"      {channel[:26]:26} {len(rows):>5} of "')
    w('                  f"{\'at least \' if lb else \'\'}{at} in the archive")')
    w("            records.extend(rows)")
    w("            notes.extend(ns)")
    w("            resolved = resolved or res")
    w("            archive += at")
    w("            at_least = at_least or lb")
    w("        for note in notes:")
    w('            print(f"      ! {note}")')
    w("        in_window = _dedup(records)")
    w("        if len(in_window) != len(records):")
    w('            print(f"      de-duplicated: {len(records)} -> {len(in_window)}")')
    w("        # Guardrail: the archive stopped counting AND nothing landed in the")
    w("        # window. That is not a quiet period — it is a number we cannot stand")
    w("        # behind, so it is flagged rather than reported as a true zero.")
    w("        if at_least and not in_window:")
    w("            print(f\"      !! SUSPECT: {sec['title']} passed the archive's count\"")
    w('                  f" cap but nothing landed in the window. Treat this as an"')
    w('                  f" unknown, not as a zero.")')
    w("        # A sector matches every node beneath it, so print what the names")
    w("        # actually became before anyone trusts the count.")
    w("        # resolved comes from the first channel's probe, so media_channel_ids")
    w("        # would name only that one. The channel is already on every row above.")
    w('        skip = ("taxonomy_ids_queried", "media_channel_ids")')
    w('        ids = {k: v for k, v in resolved.items()')
    w('               if v and k.endswith("_ids") and k not in skip}')
    w("        if ids:")
    w('            print("      resolved " + "  ".join(')
    w('                f"{k[:-4]}={v}" for k, v in ids.items()))')
    w('        if resolved.get("enhanced"):')
    w('            print(f"      resolved enhanced={resolved[\'enhanced\']}")')
    w('        found[sec["id"]] = {"sec": sec, "records": in_window,')
    w('                            "fetched": len(in_window), "archive_total": archive,')
    w('                            "at_least": at_least}')
    w("")
    w('    if not any(v["records"] for v in found.values()):')
    w('        print("\\nERROR: every section came back empty. Check the filters against"')
    w('              " PowerSearch — an empty report is more likely a wrong filter than a"')
    w('              " quiet month. Aborting rather than shipping it.")')
    w("        return None, None")
    w("")

    # sql_rows is always defined: the workbook indexes it whether or not anything
    # filled it. Step 2 runs when a worksheet column needs the database, Step 2b
    # when one needs the scanned text — separate sources, so either can happen
    # without the other.
    w("    sql_rows = {}")
    if g["any_sql"]:
        w("")
        w("    # ── Step 2 — database lookup, only where a column needs one ─────────")
        w("    needed = set()")
        w("    for t in TABS:")
        w('        if t["database"]:')
        w('            needed.update(t["section_ids"])')
        w("    if needed:")
        w('        print(f"\\nStep 2  Worksheet columns from the database '
          '({len(needed)} section(s))…")')
        w('        for sec in [s for s in SECTIONS if s["id"] in needed]:')
        w('            ids = [r["entry_id"] for r in found[sec["id"]]["records"]]')
        w("            rows = _lookup(ids)")
        w("            sql_rows.update(rows)")
        w("            print(f\"   {sec['title'][:34]:34} {len(ids):>5} ids ->\"")
        w('                  f" {len(rows):>5} matched")')
    if g["prints_ocr"]:
        w("")
        w("    # The OCR Text column costs one request per ROW, so it is capped. The")
        w("    # write-ups read their pieces separately, after they are chosen —")
        w("    # 3-5 a section rather than every row of a tab.")
        w('    printing = [s for s in SECTIONS if s.get("print_ocr")]')
        w("    if printing:")
        w('        print(f"\\nStep 2b Scanned text for the worksheet '
          '({len(printing)} section(s))…")')
        w("        for sec in printing:")
        w('            ids = [r["entry_id"] for r in found[sec["id"]]["records"]]')
        w("            notes = _read_ocr(ids, cap=OCR_ROW_CAP)")
        w("            print(f\"   {sec['title'][:34]:34} {len(ids):>5} rows ->\"")
        w('                  f" {sum(1 for e in ids if OCR.get(e)):>5} read")')
        w("            for n in notes:")
        w('                print(f"      ! {n}")')
    if g["excludes"] or g["collapses"]:
        w("")
        w("    # ── Step 3 — the narrowings the archive cannot express ──────────────")
        w('    print("\\nStep 3  Post-filters…")')
        w("    for sid, v in found.items():")
        w('        sec = v["sec"]')
        w('        if not (sec["search"]["company_must_not_match"]')
        w('                or sec["search"]["collapse_repeats"]):')
        w("            continue")
        w("        print(f\"   {sec['title']}\")")
        w('        v["records"] = _filter(v["records"], sec)')
    w("    return found, sql_rows")
    w("")
    w("")

    # ── the counts printout ─────────────────────────────────────────────────────────
    w("def print_counts(found):")
    w('    """What --only search is for: the numbers, to be checked against PowerSearch')
    w("    before anyone trusts a deck built on them.")
    w("")
    w("    A total the archive stopped counting is printed as an explicit lower")
    w("    bound. Saying otherwise would put a number in a client deck that nobody")
    w('    can stand behind."""')
    w('    print("\\n── Counts (check these against PowerSearch) ──")')
    w("    for sid, v in found.items():")
    w('        kept, arch = len(v["records"]), v["archive_total"]')
    w('        extra = ""')
    w('        fetched = v["fetched"]')
    w("        if kept != arch:")
    w('            extra = f"   fetched {fetched}, kept {kept}"')
    w('        shown = f"{\'at least \' if v.get(\'at_least\') else \'\'}{arch}"')
    w("        print(f\"   {v['sec']['title'][:38]:38} {shown:>16} in the archive{extra}\")")
    w("")
    w("")

    # ── Stage 2: the workbook ───────────────────────────────────────────────────────
    if tabs:
        w("def stage_workbook(found, sql_rows, start, end, lab):")
        w('    """Write the .xlsx. It depends only on what was searched, never on which')
        w("    pieces are featured, so it lands here in every run mode — including the")
        w('    first half of a two-phase run, where it is ready at the pause."""')
        w('    print("\\nStep 4  Workbook…")')
        w("    specs = []")
        w("    for t in TABS:")
        w("        rows = []")
        w('        for sid in t["section_ids"]:')
        w('            for r in found[sid]["records"]:')
        w('                rows.append(_row(r, sql_rows.get(r["entry_id"]),')
        w('                                 t["columns"]))')
        w('        titles = [found[s]["sec"]["title"] for s in t["section_ids"]]')
        w('        first = found[t["section_ids"][0]]["sec"]["search"]')
        w('        described = " / ".join(titles)')
        w('        sectors = ", ".join(first.get("sector") or []) or "any sector"')
        w('        channels = ", ".join(first.get("media_channel") or [])')
        w('        extra = ", ".join(f"{k}={v}" for k, v in')
        w('                          (first.get("enhanced") or {}).items())')
        w("        spec = {")
        w('            "name": t["name"], "headers": t["columns"], "rows": rows,')
        w('            "hyperlinks": HYPERLINKS,')
        w('            "filter_row": (f"{described} | {sectors}"')
        w('                           f" | Channels: {channels}"')
        w('                           + (f" | {extra}" if extra else "")')
        w('                           + f" | {DATE_FIELD}: {start} .. {end}"),')
        w("        }")
        w("        specs.append(spec)")
        w("        print(f\"   {t['name'][:30]:30} {len(rows):>5} rows\"")
        w("              f\"  ({len(t['columns'])} columns)\")")
        w("")
        w(f'    xlsx_name = ({_lit(g["book"].get("filename") or "{client}_Data_{stamp}.xlsx", 17)}')
        w('                 .replace("{client}", CLIENT.replace(" ", "_"))')
        w('                 .replace("{stamp}", lab["stamp"])')
        w('                 .replace("{mmddyy}", lab["mmddyy"])')
        w('                 .replace("{month_year}", lab["month_year"])')
        w('                 .replace("{period}", lab["period"]))')
        w("    xlsx_path = L.write_workbook(OUTPUT_DIR / xlsx_name, specs)")
        w('    print(f"        saved {xlsx_path}")')
        w("    return xlsx_path")
        w("")
        w("")

    # ── Stage 3: choose ─────────────────────────────────────────────────────────────
    if g["featured"]:
        w("def stage_select(found):")
        w('    """Choose which pieces go on each slide. Returns the ids and the model\'s')
        w("    stated reason, and writes NO prose — every sentence that reaches a deck is")
        w("    written in stage_deliver, after a person has had the chance to change")
        w('    this list."""')
        w('    picked = [s for s in SECTIONS if s["feature"]]')
        w('    print(f"\\nStep 5  Choosing pieces ({len(picked)} parallel calls)…")')
        w("    shortlists = {}")
        w("    for sec in picked:")
        w('        recs, note = _shortlist(found[sec["id"]]["records"], '
          'CANDIDATE_CAP)')
        w('        shortlists[sec["id"]] = recs')
        w("        if note:")
        w("            print(f\"   ! {sec['title']}: {note}\")")
        w("    choices = L.run_parallel(")
        w('        [(lambda s=s: _choose(s, shortlists[s["id"]])) for s in picked])')
        w("")
        w("    # The model suggests; Python decides. _eligible says what this section")
        w("    # may still use, and pick_ids drops anything invented and tops the list")
        w("    # up from the real pool. The ranked result is then split in two: the")
        w("    # slide, and the reserve standing behind it.")
        w("    used, final, reserve, why = set(), {}, {}, {}")
        w("    for sec, choice in zip(picked, choices):")
        w('        records = found[sec["id"]]["records"]')
        w('        by_id = {r["entry_id"]: r for r in records if r.get("entry_id")}')
        w("        choice = choice if isinstance(choice, dict) else {}")
        w('        if "error" in choice:')
        w("            print(f\"   ! {sec['title']}: choosing failed — {choice['error']}\")")
        w('        why[sec["id"]] = L.as_text(choice.get("reasoning"))[:400]')
        w('        want = sec["count"] * RESERVE_FACTOR')
        w('        pool = _eligible(records, used if sec["never_reuse"] else (), ())')
        w('        ranked = L.pick_ids(choice.get("entry_ids"), pool, want,')
        w("                            max_ids=want)")
        w("")
        w("        # Walk the ranking once. A piece goes on the slide if there is room")
        w("        # and its company is not already up there; everything else keeps its")
        w("        # rank and waits. So one_per_company no longer costs the slide a")
        w("        # slot — it moves the piece down the list instead of deleting it.")
        w("        ids, rest, seen_co = [], [], set()")
        w("        for eid in ranked:")
        w('            co = (by_id.get(eid, {}).get("company") or eid).lower()')
        w('            room = len(ids) < sec["count"]')
        w('            clash = sec["one_per_company"] and co in seen_co')
        w("            if room and not clash:")
        w("                seen_co.add(co)")
        w("                ids.append(eid)")
        w("            else:")
        w("                rest.append(eid)")
        w("")
        w("        # Only what is ON a slide is off-limits to the other sections. A")
        w("        # reserve piece has not been spent, so holding it must not quietly")
        w("        # remove it from another section's pool.")
        w("        used.update(ids)")
        w('        final[sec["id"]] = ids')
        w('        reserve[sec["id"]] = rest')
        w("        print(f\"   {sec['title'][:34]:34} {len(ids)} piece(s) {ids}\"")
        w('              + (f"  +{len(rest)} held in reserve" if rest else ""))')
        w("    return final, reserve, why")
        w("")
        w("")

    # ── Stage 4: deliver ────────────────────────────────────────────────────────────
    w("def stage_deliver(found, sql_rows, final, xlsx_path, start, end, lab):")
    w('    """Read the chosen pieces, write the prose, build the deck, email it.')
    w("")
    w("    EVERY model call that produces a sentence lives on this side of the line. In")
    w("    a two-phase run `final` is the list a person approved, so the paragraph under")
    w('    a slide can only ever describe pieces that survived that review."""')
    if g["featured"]:
        w('    picked = [s for s in SECTIONS if s["feature"]]')
        w("")
        w("    # Read only the pieces that were actually chosen. This is where the")
        w("    # scanned page earns its request: 3-5 pieces a section, against every")
        w("    # candidate if it were done before the pick.")
        w("    chosen_ids = [e for ids in final.values() for e in ids]")
        w("    if chosen_ids:")
        w('        print(f"\\nStep 5b Reading {len(chosen_ids)} chosen piece(s)…")')
        w("        for note in _read_ocr(chosen_ids):")
        w('            print(f"   ! {note}")')
        w('        print(f"   {sum(1 for e in chosen_ids if OCR.get(e))}'
          ' of {len(chosen_ids)} had text on file")')
        w("")
        w("    # ── Step 6 — the write-ups (parallel model calls) ───────────────────────")
        w('    print("\\nStep 6  Write-ups…")')
        w("")
        w("    def writeup_job(sec):")
        w('        ids = set(final.get(sec["id"], []))')
        w('        chosen = [r for r in found[sec["id"]]["records"]')
        w('                  if r.get("entry_id") in ids]')
        w('        return _writeup(sec, chosen, found[sec["id"]]["archive_total"],')
        w('                        found[sec["id"]].get("at_least"))')
        w("")
        w("    texts = {}")
        w("    results = L.run_parallel([(lambda s=s: writeup_job(s)) for s in picked])")
        w("    for sec, data in zip(picked, results):")
        w("        data = data if isinstance(data, dict) else {}")
        w('        if "error" in data:')
        w("            print(f\"   ! {sec['title']}: write-up failed — {data['error']}\")")
        w("        # fit_text trims to whole sentences — never a mid-word ellipsis.")
        w('        text = L.fit_text(L.as_text(data.get("callout")), sec["callout_limit"])')
        w('        texts[sec["id"]] = text')
        w("        print(f\"   {sec['title'][:34]:34} {len(text):>4} chars\")")
        w("")

    if g["summary_on"]:
        w("    # ── Step 7 — the summary: the LAST model call ───────────────────────────")
        w('    print("\\nStep 7  Summary slide…")')
        w("    lines = [f\"- {s['title']}: {texts.get(s['id'], '')}\"")
        w('             for s in picked if found[s["id"]]["records"]]')
        w("    if not lines:")
        w('        lines = ["(nothing was found this period)"]')
        w('    summary = _summary(lab["period"], lines)')
        w('    if "error" in summary:')
        w("        print(f\"   ! summary failed — {summary['error']}\")")
        w('    sum1 = L.cap_words(L.as_text(summary.get("column1")) or "No findings.",')
        w("                       SUMMARY_MAX_WORDS)")
        w('    sum2 = L.cap_words(L.as_text(summary.get("column2")) or "No findings.",')
        w("                       SUMMARY_MAX_WORDS)")
        w("")

    if g["deck_on"]:
        w("    # ── Step 8 — build the deck ─────────────────────────────────────────────")
        w('    print("\\nStep 8  Deck…")')
        w("    slides = []")
        if deck.get("title_slide"):
            w('    slides.append({"type": "title",')
            w('                   "data": {"title": CLIENT, "date": lab["period"]}})')
        if g["headings_on"]:
            w("    headings = []")
            w("    for sec in SECTIONS:")
            w('        h = (sec.get("heading") or "").strip()')
            w('        if h and h not in headings and sec["feature"]:')
            w("            headings.append(h)")
            w("    if headings:")
            w('        slides.append({"type": "agenda", "data": {"sections": headings}})')
        if g["summary_on"]:
            w('    slides.append({"type": "needToKnow", "data": {')
            w('        "title1": SUMMARY_TITLE1.replace("{period}", lab["period"]),')
            w('        "text1": sum1,')
            w('        "title2": SUMMARY_TITLE2.replace("{period}", lab["period"]),')
            w('        "text2": sum2}})')
        if g["featured"]:
            w("")
            if g["headings_on"]:
                w("    current = None")
            w("    for sec in SECTIONS:")
            w('        if not sec["feature"]:')
            w("            continue")
            if g["headings_on"]:
                w('        h = (sec.get("heading") or "").strip()')
                w("        if h and h != current:")
                w('            slides.append({"type": "newSection", "data": {"title": h}})')
                w("            current = h")
            w('        ids = final.get(sec["id"], [])')
            w("        if not ids:")
            w("            print(f\"   ! {sec['title']}: nothing to show — slide skipped\")")
            w("            continue")
            w('        text = texts.get(sec["id"], "")')
            w("        # The builder holds SLIDE_CAP entries, so overflow always rolls onto")
            w('        # "(cont.)" slides carrying the same write-up.')
            w("        for i, chunk in enumerate(L.chunk_ids(ids, size=SLIDE_CAP)):")
            w('            slides.append({"type": "entry_ids", "data": {')
            w('                "slideTitle": sec["title"] + (" (cont.)" if i else ""),')
            w('                "entryIds": chunk, "insight": text}})')
        if deck.get("closing_slide"):
            w("")
            w('    slides.append({"type": "closing", "data": {}})')
        w("")
        w('    print(f"   {len(slides)} slides")')
        w(f'    deck_title = ({_lit(deck.get("title") or "{client} — {period}", 18)}')
        w('                  .replace("{client}", CLIENT)')
        w('                  .replace("{period}", lab["period"]))')
        w("    result = build_deck_default(deck_title=deck_title, slides=slides)")
        w("")
        w(f'    pptx_name = ({_lit(deck.get("filename") or "{client}_{stamp}.pptx", 17)}')
        w('                 .replace("{client}", CLIENT.replace(" ", "_"))')
        w('                 .replace("{stamp}", lab["stamp"])')
        w('                 .replace("{mmddyy}", lab["mmddyy"])')
        w('                 .replace("{month_year}", lab["month_year"])')
        w('                 .replace("{period}", lab["period"]))')
        w("    try:")
        w("        saved = L.save_pptx(result, OUTPUT_DIR / pptx_name)")
        w("    except RuntimeError as exc:")
        w('        print(f"ERROR: {exc}")')
        w('        print("       Check PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env"')
        w('              " and that csresearchhub.com is reachable.")')
        if tabs:
            w("        if xlsx_path:")
            w('            print(f"       (The workbook was still written: {xlsx_path})")')
        w("        return 1")
        w('    print(f"\\n  Deck:  {saved}")')
        if tabs:
            w("    if xlsx_path:")
            w('        print(f"  Excel: {xlsx_path}")')
        w("")
    else:
        w("    saved = None")
        w("")

    # ── email: always present, always opt-in, never a literal address ───────────────
    w("    # ── Email — opt-in, and by variable NAME only. This file carries no")
    w("    #    recipient: a scheduled run reads one from the environment, and a one-off")
    w("    #    run gets one from whoever started it. Nothing is emailed when neither")
    w("    #    is set, and the files are on disk either way. ─────────────────────────")
    w("    if EMAIL_TO:")
    w('        print(f"\\nEmailing deliverables to {EMAIL_TO}…")')
    if g["deck_on"] and tabs:
        attach = "[a for a in (saved, xlsx_path) if a]"
    elif g["deck_on"]:
        attach = "[a for a in (saved,) if a]"
    else:
        attach = "[a for a in (xlsx_path,) if a]"
    w("        res = L.notify_report_ready(")
    w('            report_name=f"{CLIENT} report", period_label=lab["period"],')
    w(f"            attachment_paths={attach}, to_addr=EMAIL_TO)")
    w('        if res.get("status") == "sent":')
    w("            print(f\"   sent (message_id={res.get('message_id')})\")")
    w("        else:")
    w("            print(f\"   !! email FAILED: {res.get('error')} — the files are\"")
    w('                  f" still saved locally, nothing is lost")')
    w("    else:")
    w('        print("\\nNothing emailed — no recipient was set. The files are saved'
      ' locally.")')
    w("")
    if g["notes"]:
        w('    print("\\n!! This report has NOTES FOR ENGINEERING in its docstring —"')
        w('          " bespoke work is still needed before it is production-ready.")')
    w('    print("\\nDone.")')
    w("    return 0")
    w("")
    w("")

    # ── resume ──────────────────────────────────────────────────────────────────────
    if g["featured"]:
        w("def stage_resume(args):")
        w('    """Rebuild the first half of a run from its state file. No searching.')
        w("")
        w("    The SECTIONS table in this file wins over the copy in the state, so a fix")
        w("    made between the two halves takes effect. Only the records come from the")
        w('    state — those are what was fetched, and refetching them is the one thing')
        w('    this phase must never do."""')
        w("    state = _state_read(args.state)")
        w('    print(f"  resuming {args.state} — written {state.get(\'written_at\')}")')
        w("    approved = {}")
        w("    if args.approved:")
        w("        approved = json.loads(")
        w('            Path(args.approved).read_text(encoding="utf-8"))')
        w('    by_id = {x["id"]: x for x in (state.get("sections") or [])}')
        w("    found, final = {}, {}")
        w("    for sec in SECTIONS:")
        w('        st = by_id.get(sec["id"])')
        w("        if st is None:")
        w("            print(f\"   ! {sec['title']}: not in the state file — skipped.\")")
        w("            continue")
        w('        records = st.get("records") or []')
        w('        found[sec["id"]] = {')
        w('            "sec": sec, "records": records,')
        w('            "fetched": st.get("fetched") or len(records),')
        w('            "archive_total": st.get("archive_total") or 0,')
        w('            "at_least": bool(st.get("at_least")),')
        w("        }")
        w('        if not sec["feature"]:')
        w("            continue")
        w('        found[sec["id"]]["reserve"] = list(st.get("reserve") or [])')
        w('        want = list(dict.fromkeys(approved.get(sec["id"])')
        w('                                  if sec["id"] in approved')
        w('                                  else (st.get("picks") or [])))')
        w('        have = {r.get("entry_id") for r in records}')
        w("        keep = [e for e in want if e in have]")
        w("        if len(keep) != len(want):")
        w("            print(f\"   ! {sec['title']}: dropped {len(want) - len(keep)}\"")
        w('                  f" approved id(s) that are not in this run\'s records.")')
        w('        final[sec["id"]] = keep')
        if g["any_ocr"]:
            w("    # Text already read in the first half. Marked as tried too, so the")
            w("    # second half does not pay a second request for a piece it has.")
            w('    OCR.update(state.get("ocr") or {})')
            w("    OCR_TRIED.update(OCR)")
        w('    xlsx = state.get("workbook")')
        w('    return found, state.get("sql_rows") or {}, final, (Path(xlsx) if xlsx'
          ' else None)')
        w("")
        w("")

    # ── main ────────────────────────────────────────────────────────────────────────
    w("def main() -> int:")
    w("    args = _parse_args()")
    if g["featured"]:
        w('    if args.phase == "replace":')
        w("        return _replace(args)")
        w('    if args.phase in ("pick", "build") and not args.state:')
        w("        # Checked before anything is spent, not after the searching is done.")
        w('        print(f"ERROR: --phase {args.phase} needs --state — the file the two"')
        w('              f" halves of the run hand over through.")')
        w("        return 1")
        w("")
    w("    start, end = _window()")
    w("    lab = _labels(start, end)")
    w("    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)")
    w('    print(f"{CLIENT} — {lab[\'period\']}")')
    w('    print(f"  window {start} .. {end}  (bounded server-side by {DATE_FIELD})")')
    if g["fixed_window"]:
        w('    print("  !! this window is FIXED — every run covers these same dates."')
        w('          " Right for a one-off, wrong for anything on a schedule.")')
    w('    print(f"  mode --only={args.only}"')
    if g["featured"]:
        w('          + (f" --phase={args.phase}" if args.phase != "all" else "")')
    w('          + (f" --limit={args.limit}" if args.limit else ""))')
    w("")
    if g["featured"]:
        w('    if args.phase == "build":')
        w("        found, sql_rows, final, xlsx_path = stage_resume(args)")
        w("        return stage_deliver(found, sql_rows, final, xlsx_path, start, end,")
        w("                             lab)")
        w("")
    w("    found, sql_rows = stage_search(args, start, end)")
    w("    if found is None:")
    w("        return 1")
    w("")
    w("    def save_state(final, reserve, why, xlsx_path):")
    w("        if args.state:")
    w("            _state_write(args.state, start, end, lab, found, sql_rows, final,")
    w("                         reserve, why, xlsx_path)")
    w("")
    w('    if args.only == "search":')
    w("        print_counts(found)")
    w("        save_state({}, {}, {}, None)")
    w("        return 0")
    w("")
    if tabs:
        w("    xlsx_path = stage_workbook(found, sql_rows, start, end, lab)")
        w('    if args.only == "excel":')
        w("        save_state({}, {}, {}, xlsx_path)")
        w("        return 0")
    else:
        w("    xlsx_path = None")
    w("")
    if g["featured"]:
        w("    final, reserve, why = stage_select(found)")
        w('    if args.phase == "pick":')
        w("        save_state(final, reserve, why, xlsx_path)")
        w('        print("\\nPaused. Approve the picks, then run --phase build with the"')
        w('              " state file above.")')
        w("        return 0")
    else:
        w("    final, reserve, why = {}, {}, {}")
        w("    save_state(final, reserve, why, xlsx_path)")
    w("    return stage_deliver(found, sql_rows, final, xlsx_path, start, end, lab)")
    w("")
    w("")
    w('if __name__ == "__main__":')
    w("    sys.exit(main())")


def codegen(p: dict) -> tuple[str, str]:
    """A project -> (source, filename). Migration runs first, so a v2 project generates
    a v3 pipeline rather than half of each."""
    p = migrate(p)
    g = _plan(p)
    out: list[str] = []
    _emit_head(p, g, out.append)
    _emit_main(p, g, out.append)
    return "\n".join(out) + "\n", f"{_slug(g['client'])}.py"


# ═══════════════════════════════════════════════════════════════════════════════════════
# Test runner — generates the file, then runs THAT file
# ═══════════════════════════════════════════════════════════════════════════════════════

# ── Which interpreter runs the generated pipeline ──────────────────────────────────
#
# Not necessarily this one. The Studio is deliberately stdlib-only so a researcher can
# launch it with whatever `python` is on their PATH, but a generated pipeline imports
# report_lib, which needs requests, pandas and boto3. Handing it sys.executable is how
# you get `ModuleNotFoundError: No module named 'requests'` three seconds into a Test.
#
# So the runner is resolved by asking candidate interpreters whether they can actually
# import what a pipeline needs. Set PIPELINES_PYTHON to skip the search.

RUNNER_NEEDS = ("requests", "pandas", "boto3")
_RUNNER: list = []          # cached (path, why) — probing spawns processes


def _can_run(exe) -> bool:
    """Ask the interpreter itself, rather than guessing from its path."""
    try:
        return subprocess.run(
            [str(exe), "-c", f"import {', '.join(RUNNER_NEEDS)}"],
            capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _candidates():
    """Explicit override first, then this interpreter, then the usual places a project
    keeps its environment. Ordered by how likely it is to be the one intended."""
    seen = set()

    def offer(path, why):
        path = str(path)
        if path and path not in seen and Path(path).exists():
            seen.add(path)
            yield path, why

    override = (os.environ.get("PIPELINES_PYTHON") or "").strip().strip('"')
    if override:
        yield from offer(override, "PIPELINES_PYTHON")
    yield from offer(sys.executable, "running this Studio")

    win = os.name == "nt"
    leaf = "Scripts/python.exe" if win else "bin/python"
    roots = [PIPELINES_DIR.parent if PIPELINES_DIR else Path.cwd(), Path.cwd(),
             Path.home()]
    for root in roots:
        for name in ("venv", ".venv", "env", ".env"):
            yield from offer(root / name / leaf, f"{name} beside the project")
    for base in (Path.home() / "miniconda3", Path.home() / "anaconda3",
                 Path("C:/miniconda3"), Path("C:/anaconda3"),
                 Path("C:/ProgramData/miniconda3"), Path("C:/ProgramData/anaconda3"),
                 Path("/opt/conda")):
        env_dir = base / "envs"
        if env_dir.is_dir():
            for env in sorted(env_dir.iterdir()):
                yield from offer(env / ("python.exe" if win else "bin/python"),
                                 f"conda env {env.name}")
        yield from offer(base / ("python.exe" if win else "bin/python"), "conda base")


def runner() -> tuple[str, str]:
    """(interpreter, why). Falls back to this one so Test still runs and still reports
    the real error, rather than refusing before it has anything to say."""
    if _RUNNER:
        return _RUNNER[0]
    for path, why in _candidates():
        if _can_run(path):
            _RUNNER.append((path, why))
            return _RUNNER[0]
    _RUNNER.append((sys.executable, "no interpreter found with "
                                    + ", ".join(RUNNER_NEEDS)))
    return _RUNNER[0]


RUNS: dict[str, dict] = {}
RUNS_LOCK = threading.Lock()

# report_lib prefixes every line of its model-call trace with this. The Studio pulls
# those lines out of the subprocess stream and prints them to its OWN stdout, so a
# researcher's output panel stays about their report and the engineer who started the
# Studio can watch the token usage and the raw responses go by.
LLM_TRACE_PREFIX = "[LLM]"

# Every trace line carries cost=$0.000000 for that one call. The Studio adds them up
# per RUN rather than per process, because a run that pauses for review is two
# processes — the pick half and the build half — and what a researcher wants to know
# is what the DELIVERABLE cost, not what half of it cost.
_COST_RE = re.compile(r"cost=\$([0-9.]+)")
_TOK_RE = re.compile(r"\bin=(\d+) out=(\d+)\b")


def _llm_zero() -> dict:
    return {"calls": 0, "in": 0, "out": 0, "usd": 0.0}


def _llm_absorb(run_id: str, line: str) -> None:
    """Pull the numbers out of one trace line."""
    m = _COST_RE.search(line)
    if not m:
        return
    with RUNS_LOCK:
        acc = RUNS.setdefault(run_id, {}).setdefault("llm", _llm_zero())
        acc["calls"] += 1
        try:
            acc["usd"] += float(m.group(1))
        except ValueError:
            pass
        t = _TOK_RE.search(line)
        if t:
            acc["in"] += int(t.group(1))
            acc["out"] += int(t.group(2))


def _llm_report(run_id: str) -> None:
    """What this run spent on the model, and on what. Printed to the terminal the
    Studio was launched from, once, when the run is over."""
    with RUNS_LOCK:
        acc = dict((RUNS.get(run_id) or {}).get("llm") or {})
        mode = (RUNS.get(run_id) or {}).get("mode")
    if not acc.get("calls"):
        return
    files = [f["name"] for f in _artifacts(run_id)
             if not f["name"].startswith("_")]
    label = MODES.get(mode, {}).get("label", mode)
    print(f"{LLM_TRACE_PREFIX} == run {run_id} ({label}) TOTAL: "
          f"{acc['calls']} model call(s)  in={acc['in']:,}  out={acc['out']:,}  "
          f"${acc['usd']:.4f}", flush=True)
    if files:
        # "Per deliverable" in the only sense that is not misleading: this is what the
        # whole run cost, and these are the files it handed over. Dividing it between
        # a deck and a workbook would be invention — the workbook costs no model calls
        # at all, and the deck's prose is what every one of them paid for.
        print(f"{LLM_TRACE_PREFIX}    delivered: {', '.join(files)}", flush=True)
        print(f"{LLM_TRACE_PREFIX}    ${acc['usd']:.4f} for that deliverable"
              f" ({acc['calls']} call(s))", flush=True)
    else:
        print(f"{LLM_TRACE_PREFIX}    no deliverable produced", flush=True)

# ── The four run modes ─────────────────────────────────────────────────────────────
#
# A ladder, not a menu. Each rung does everything the one above it does and a little
# more, and a researcher climbs it while producing a real deliverable in one sitting:
# see what the archive returns, get the workbook, curate the deck piece by piece,
# produce and send the finished thing. Someone building a recurring pipeline climbs
# the same ladder to satisfy themselves it is right before pressing Send to Eng.
#
# NONE of them passes --limit. The Studio used to inject a small one to keep testing
# quick, which meant a researcher never once saw what their own row caps did until
# Engineering ran it for real. The limits configured on the sections are the only
# limits in effect, and the request estimate in the top bar is how the cost is made
# visible instead.
MODES = {
    "search": {
        "label": "Run the searches",
        "help": "",
        "args": ["--only", "search"], "state": True, "pauses": False,
    },
    "excel": {
        "label": "Run the workbook",
        "help": "",
        "args": ["--only", "excel"], "state": True, "pauses": False,
    },
    "curate": {
        "label": "Run and edit the deliverables",
        "help": "",
        "args": ["--phase", "pick"], "state": True, "pauses": True,
    },
    "full": {
        "label": "Run the pipeline",
        "help": "",
        "args": [], "state": False, "pauses": False,
    },
}


def mode_problem(project: dict, mode: str) -> str:
    """Why this report cannot be run in this mode, or "" if it can.

    The generated pipeline only offers the stages it actually has — a report with no
    workbook has no --only excel, and one with nothing on a slide has no --phase pick.
    Handing it a flag it does not define is an argparse traceback in the output panel,
    which tells a researcher nothing. So the mismatch is caught here and explained.
    """
    g = _plan(migrate(project))
    if mode == "excel" and not g["tabs"]:
        return ('This report does not build a workbook, so there is nothing for "Run '
                'the workbook" to make. Turn the workbook on in report settings, or '
                'pick a different way to run it.')
    if mode == "curate" and not g["featured"]:
        return ('Nothing in this report goes on a slide, so there are no pieces to '
                'approve. "Run and edit the deliverables" is for choosing what appears '
                'on the slides — use "Run the workbook" instead, or turn on slides for '
                "at least one section.")
    if mode == "full" and not g["tabs"] and not g["deck_on"]:
        return ("This report produces nothing — no slides and no workbook. Turn on one "
                "of them in report settings.")
    return ""


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def _prune_runs(keep: int = 20) -> None:
    """Keep the last `keep` run directories and their pipeline files.

    Run history offers downloads, so a run's files have to outlive the run itself —
    but not forever. When a directory has been pruned the history row stays and says
    the files are gone, which is more useful than the row disappearing too.
    """
    try:
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        dirs = sorted((d for d in RUNS_DIR.iterdir() if d.is_dir()),
                      key=lambda d: d.stat().st_mtime)
        import shutil
        for d in dirs[:-keep]:
            shutil.rmtree(d, ignore_errors=True)
        old = sorted(GENERATED_DIR.glob("_run_*.py"), key=lambda q: q.stat().st_mtime)
        for q in old[:-keep]:
            q.unlink(missing_ok=True)
        # v3 wrote its throwaway pipelines here. Clear them out on the way past.
        for q in GENERATED_DIR.glob("_test_*.py"):
            q.unlink(missing_ok=True)
    except OSError:
        pass


def _artifacts(run_id: str) -> list[dict]:
    """The files this run produced.

    Read off the run's own output directory rather than scraped out of the log. The
    directory belongs to exactly one run — RS_OUTPUT_DIR is set per run precisely so
    that two runs of the same report on the same day cannot overwrite each other — so
    everything in it is this run's, and nothing has to be parsed to know that.
    """
    out = run_dir(run_id) / "output"
    if not out.is_dir():
        return []
    files = []
    for f in sorted(out.iterdir(), key=lambda q: q.name.lower()):
        if f.is_file():
            files.append({"name": f.name, "size": f.stat().st_size,
                          "kind": f.suffix.lstrip(".").lower()})
    return files


def _run_record(run_id: str) -> dict:
    with RUNS_LOCK:
        r = RUNS.get(run_id) or {}
        return {"id": run_id, "mode": r.get("mode"), "at": r.get("at"),
                "done": r.get("done"), "rc": r.get("rc"),
                "stopped": bool(r.get("stopped")),
                "emailed": bool(r.get("emailed")),
                "produced": [f["name"] for f in _artifacts(run_id)]}


def start_run(project: dict, mode: str, email_to: str = "") -> str:
    """Generate the pipeline, then run THAT file. The whole architecture in one line.

    The Studio never interprets a report. It writes the .py and executes it, so what a
    researcher runs here is exactly what Engineering deploys — including the two-phase
    pause, which lives inside the generated file rather than in this process.
    """
    spec_mode = MODES.get(mode) or MODES["search"]
    run_id = uuid.uuid4().hex[:12]
    code, fname = codegen(project)
    ast.parse(code)  # surface a generator bug here, not as a subprocess traceback
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    _prune_runs()
    target = GENERATED_DIR / f"_run_{run_id}_{fname}"
    target.write_text(code, encoding="utf-8")

    d = run_dir(run_id)
    (d / "output").mkdir(parents=True, exist_ok=True)

    with RUNS_LOCK:
        RUNS[run_id] = {"lines": [], "done": False, "rc": None, "mode": mode,
                        "at": datetime.now().isoformat(timespec="seconds"),
                        "proc": None, "stopped": False, "target": str(target),
                        "emailed": bool(email_to), "paused": False,
                        "llm": _llm_zero(),
                        "project_name": str(project.get("name") or "")}

    cmd = [None, "-u", str(target)] + list(spec_mode["args"])
    if spec_mode["state"]:
        cmd += ["--state", str(d / "state.json")]
    _spawn(run_id, cmd, email_to)
    return run_id


def continue_run(run_id: str, approved: dict, email_to: str = "") -> bool:
    """The second half of a paused run: build the deliverables from the approved ids.

    Same run, same directory, same generated file — the pipeline reads back the state
    its own first half wrote. Nothing is searched again.
    """
    with RUNS_LOCK:
        r = RUNS.get(run_id)
        if not r or not r.get("target"):
            return False
        target = r["target"]
        r["done"], r["rc"], r["paused"] = False, None, False
        r["emailed"] = r.get("emailed") or bool(email_to)
    d = run_dir(run_id)
    (d / "approved.json").write_text(json.dumps(approved, indent=1), encoding="utf-8")
    cmd = [None, "-u", target, "--phase", "build",
           "--state", str(d / "state.json"),
           "--approved", str(d / "approved.json")]
    _spawn(run_id, cmd, email_to)
    return True


def _spawn(run_id: str, cmd: list, email_to: str = "") -> None:
    def log(msg):
        with RUNS_LOCK:
            RUNS[run_id]["lines"].append(msg)

    def worker():
        if PIPELINES_DIR is None:
            log("Cannot run: report_lib.py was not found next to pipeline_studio3.py.")
            with RUNS_LOCK:
                RUNS[run_id]["done"], RUNS[run_id]["rc"] = True, 1
            return
        exe, why = runner()
        if "no interpreter found" in why:
            log(f"! Could not find a Python with {', '.join(RUNNER_NEEDS)} installed.")
            log(f"  Trying {exe} anyway — if it fails on an import, either")
            log(f"  pip install {' '.join(RUNNER_NEEDS)} into it, or set")
            log("  PIPELINES_PYTHON to the interpreter that already has them.")
            log("")
        argv = [exe if c is None else c for c in cmd]

        env = dict(os.environ)
        # One directory per run, so a run's files can never overwrite the previous
        # run's — which is what makes the history downloads still work on Thursday
        # for a deck built on Tuesday.
        env["RS_OUTPUT_DIR"] = str(run_dir(run_id) / "output")
        # Every model call reports its token usage and what it said. It goes to the
        # terminal the Studio was launched from, NOT into the researcher's output
        # panel — see the reader below. Engineering telemetry, not a researcher's
        # business, and off entirely for a scheduled run because nothing sets it.
        env["RS_LLM_TRACE"] = "1"
        if email_to:
            # Held for the life of THIS child process and nowhere else. It is not
            # written to the project, not saved, and not recorded in the history.
            env["RS_EMAIL_TO"] = email_to
        else:
            env.pop("RS_EMAIL_TO", None)

        shown = [c for c in argv]
        log("$ " + " ".join(shown))
        if email_to:
            log(f"  (emailing this run to {email_to} — the address is not saved)")
        log("")
        try:
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    encoding="utf-8", errors="replace",
                                    cwd=str(PIPELINES_DIR.parent), env=env)
            with RUNS_LOCK:
                RUNS[run_id]["proc"] = proc
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line.startswith(LLM_TRACE_PREFIX):
                    # Intercepted, not logged: this is the one kind of output that
                    # belongs on the operator's console rather than in the page. It
                    # is written where the Studio itself was started.
                    _llm_absorb(run_id, line)
                    print(line, flush=True)
                    continue
                log(line)
            proc.wait()
            rc = proc.returncode
        except Exception as exc:
            log(f"RUNNER ERROR: {exc}")
            rc = 1
        with RUNS_LOCK:
            r = RUNS[run_id]
            r["proc"] = None
            r["done"], r["rc"] = True, rc
            if r.get("stopped"):
                r["rc"] = rc if rc not in (None,) else 1
            paused = bool((MODES.get(r.get("mode")) or {}).get("pauses")
                          and rc == 0 and not r.get("stopped"))
        # A paused run is halfway through, so its total is not final yet — it is
        # reported when the build half finishes and the deliverables exist.
        if not paused:
            _llm_report(run_id)

    threading.Thread(target=worker, daemon=True).start()


def stop_run(run_id: str) -> dict:
    """Kill the subprocess. Nothing else in this process is left believing it is alive.

    Terminate, give it five seconds, then kill. The generated pipeline spawns no child
    processes of its own, so terminating it is enough; CTRL_BREAK is deliberately not
    used because a Python child would turn it into a KeyboardInterrupt traceback that
    reads like a crash rather than a stop.

    Whatever the run already printed stays in the log, and whatever it already wrote
    into its own output directory stays downloadable — a workbook that was finished
    before the deck stage began is still a workbook.
    """
    with RUNS_LOCK:
        r = RUNS.get(run_id)
        if not r:
            return {"error": "unknown run"}
        proc = r.get("proc")
        r["stopped"] = True
        if not proc:
            r["done"], r["rc"] = True, r.get("rc") if r.get("rc") is not None else 130
            return {"stopped": True, "was_running": False}
    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    except Exception as exc:
        return {"error": str(exc)}
    with RUNS_LOCK:
        RUNS[run_id]["lines"].append("")
        RUNS[run_id]["lines"].append("!! Stopped. Anything already written is still in "
                                     "this run's files.")
    return {"stopped": True, "was_running": True}


# ── The results panel's data ────────────────────────────────────────────────────
PANEL_PIECE_CAP = 300

# A run's state file is large — around 7 MB and 8,000 records for a three-section
# report — and the by-hand lookup runs on a keystroke debounce as somebody types an
# entry_id. Re-reading and re-parsing per keystroke would spend most of a second on
# every character. So it is parsed once per change, alongside an entry_id index that
# turns each lookup into a dict hit rather than a scan of every record in the run.
_STATE_CACHE: "OrderedDict[str, tuple]" = collections.OrderedDict()
_STATE_CACHE_LOCK = threading.Lock()
_STATE_CACHE_KEEP = 2      # the run being reviewed, and the one before it


def _state_indexed(run_id: str) -> tuple[dict, dict]:
    """(state, index) for a run, where index maps entry_id -> [(sec_id, title, rec)].

    Keyed on the state file's own mtime and size, so a run that writes a new state
    file — the second half of a paused run does exactly that — is re-read rather than
    answered from a stale index.

    Raises OSError or ValueError; every caller already has to say something useful
    about a state file it cannot read.
    """
    path = run_dir(run_id) / "state.json"
    st = path.stat()
    key = f"{path}|{st.st_mtime_ns}|{st.st_size}"
    with _STATE_CACHE_LOCK:
        hit = _STATE_CACHE.get(key)
        if hit is not None:
            _STATE_CACHE.move_to_end(key)
            return hit

    state = json.loads(path.read_text(encoding="utf-8"))
    index: dict = {}
    for sec in state.get("sections") or []:
        sid, title = sec.get("id"), sec.get("title") or ""
        for r in sec.get("records") or []:
            index.setdefault(str(r.get("entry_id")), []).append((sid, title, r))

    with _STATE_CACHE_LOCK:
        _STATE_CACHE[key] = (state, index)
        _STATE_CACHE.move_to_end(key)
        while len(_STATE_CACHE) > _STATE_CACHE_KEEP:
            _STATE_CACHE.popitem(last=False)
    return state, index


def _card(r: dict) -> dict:
    """One piece, as the results panel shows it.

    Hoisted out of panel() because a hand-picked entry_id is resolved by the same
    rules and has to arrive in the same shape. Two builders would be two shapes, and
    the row that came from the lookup would quietly lack a field the panel draws.
    """
    return {
        "entry_id": r.get("entry_id"),
        "product_id": r.get("product_id"),
        "company": r.get("company") or "",
        "channel": r.get("media_channel") or "",
        "date": str(r.get("search_date") or "")[:10],
        "headline": " ".join(str(r.get("product_headline") or "").split())[:280],
        "product": " ".join(str(r.get("product_name") or "").split())[:140],
        # A link needs the archive's internal product id, which only exists on a row
        # the search returned. Absent, the piece is listed by entry_id and headline
        # rather than given a link that would not open.
        "pdf_url": r.get("pdf_url") or "",
    }


def panel(run_id: str) -> dict:
    """What the right-hand panel shows, read off the state file the PIPELINE wrote.

    One shape for both of the panel's states, because there is one file behind them:
    after "Run the searches" it lists everything retrieved, and at the pause in "Run
    and edit the deliverables" it lists what was picked out of that same list. The
    Studio parses nothing out of the log to get here.
    """
    path = run_dir(run_id) / "state.json"
    if not path.is_file():
        return {"error": "this run has no results file yet"}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"error": f"could not read the results file: {exc}"}

    sections = []
    for sec in state.get("sections") or []:
        records = sec.get("records") or []
        picks = list(sec.get("picks") or [])
        by_id = {r.get("entry_id"): r for r in records}
        shown = [_card(r) for r in records[:PANEL_PIECE_CAP]]
        sections.append({
            "id": sec.get("id"), "title": sec.get("title"),
            "tab": sec.get("tab"), "feature": bool(sec.get("feature")),
            "count": sec.get("count") or 0,
            "one_per_company": bool(sec.get("one_per_company")),
            "never_reuse": bool(sec.get("never_reuse")),
            "archive_total": sec.get("archive_total") or 0,
            "at_least": bool(sec.get("at_least")),
            "kept": sec.get("kept") or len(records),
            "reasoning": sec.get("reasoning") or "",
            "shown": len(shown),
            "pieces": shown,
            "picks": [_card(by_id[e]) for e in picks if e in by_id],
        })
    with RUNS_LOCK:
        r = RUNS.get(run_id) or {}
        mode = r.get("mode")
    return {"run_id": run_id, "mode": mode,
            "period": state.get("period_label"),
            "start": state.get("start"), "end": state.get("end"),
            "workbook": bool(state.get("workbook")),
            "piece_cap": PANEL_PIECE_CAP,
            "sections": sections, "files": _artifacts(run_id)}


def lookup_pick(run_id: str, section: str, entry_id: str) -> dict:
    """Resolve one entry_id a researcher typed in, against what this run fetched.

    The panel only ever receives the first PANEL_PIECE_CAP rows of a section, and the
    workbook has all of them — so an id copied out of the .xlsx is very often one the
    browser has never seen. The lookup therefore reads the state file, which holds
    every record the run retrieved, rather than searching what was sent to the page.

    It insists the id is in THIS section's records, and that is not fussiness. The
    build phase intersects the approved list with the section's own records and
    silently drops the rest, so an id accepted here but absent there would vanish from
    the deck with nothing but a line on a console nobody is reading. Refusing it at
    the point of typing is the only place the researcher can still act on it.

    Called on a keystroke debounce while the researcher types, so it answers off the
    cached index rather than re-reading seven megabytes per character.
    """
    if not (run_dir(run_id) / "state.json").is_file():
        return {"error": "this run has no results file"}
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", str(entry_id or "")):
        return {"error": f"'{entry_id}' is not an entry_id. They look like "
                         f"2026-07-31-4074."}
    try:
        state, index = _state_indexed(run_id)
    except (OSError, ValueError) as exc:
        return {"error": f"could not read the results file: {exc}"}

    target = next((x for x in (state.get("sections") or [])
                   if x.get("id") == section), None)
    if target is None:
        return {"error": "that section is not in this run"}

    where = index.get(entry_id) or []
    hit = next((r for (sid, _t, r) in where if sid == section), None)
    if hit is not None:
        return {"card": _card(hit), "section": section,
                "title": target.get("title") or ""}

    elsewhere = [t for (sid, t, _r) in where if sid != section]
    n = len(target.get("records") or [])
    tail = (f" It is in {' and '.join(elsewhere)}, which is a different section of "
            f"this report — a piece can only go on a slide it was searched for."
            if elsewhere else
            " Nothing in this run has that id. Check it against the workbook, and "
            "that this run's date window covers the piece.")
    return {"error": f"{entry_id} is not among the {n:,} piece(s) this run fetched "
                     f"for {target.get('title') or 'this section'}.{tail}"}


def replace_pick(run_id: str, section: str, keep: list, reject: list,
                 used: list) -> dict:
    """Ask the GENERATED PIPELINE for the next valid candidate.

    Deliberately not computed here. The selection rules — one per company, never
    reused across sections, never something already shown or already rejected — are
    written down once, in the file that does the picking. A second copy in the Studio
    would be a second answer waiting to disagree with the first.
    """
    with RUNS_LOCK:
        r = RUNS.get(run_id)
        target = (r or {}).get("target")
    if not target:
        return {"error": "unknown run"}
    exe, _why = runner()
    cmd = [exe, str(target), "--phase", "replace",
           "--state", str(run_dir(run_id) / "state.json"),
           "--section", section,
           "--keep", ",".join(keep or []),
           "--reject", ",".join(reject or []),
           "--used", ",".join(used or [])]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                             encoding="utf-8", errors="replace",
                             cwd=str(PIPELINES_DIR.parent))
    except (OSError, subprocess.SubprocessError) as exc:
        return {"error": str(exc)}
    out = (res.stdout or "").strip().splitlines()
    for line in reversed(out):
        try:
            return json.loads(line)
        except ValueError:
            continue
    return {"error": (res.stderr or res.stdout or "no answer").strip()[:400]}


# ── Thumbnails ────────────────────────────────────────────────────────────────────
#
# The API publishes one URL per row, pdf_url, and it is a PowerSearch page behind a
# login rather than an asset — there is no public image URL to point an <img> at. The
# pages themselves are in S3, at a location only the database knows, and the current
# bucket answers 403 without credentials. So the Studio fetches the bytes through
# pipelines/thumbs.py (which runs under the pipeline interpreter, because it needs
# boto3 and the tunnel and this file is stdlib-only) and serves them from its own
# origin, cached per run.
THUMB_BATCH = 60


def thumbs_dir(run_id: str) -> Path:
    return run_dir(run_id) / "thumbs"


def _cached_thumb(out: Path, entry_id: str) -> bool:
    """Is there a usable cached image for this piece?

    Size matters as much as existence. A zero-byte file is what a read that died
    halfway used to leave behind, and treating it as a hit meant the panel served an
    empty picture for the rest of the run.
    """
    f = out / f"{entry_id}.jpg"
    try:
        return f.is_file() and f.stat().st_size > 0
    except OSError:
        return False


def fetch_thumbs(run_id: str, entry_ids: list) -> dict:
    """Get the cover image for these pieces. Cached, so a re-render is free.

    Three answers per piece, never two. `thumbs` is what is on disk now, `missing` is
    what the archive genuinely has no cover image for, and `failed` is what this
    attempt could not get but a later one might — a shut tunnel, a 403, a dropped
    read. Anything asked for and named in none of the three is also unresolved, so the
    caller retries it rather than recording it as a piece without a picture.
    """
    if not re.fullmatch(r"[0-9a-f]{12}", str(run_id or "")):
        return {"error": "unknown run", "thumbs": {}, "missing": [], "failed": []}
    if PIPELINES_DIR is None:
        return {"error": "pipelines/ not found", "thumbs": {}, "missing": [],
                "failed": list(entry_ids or [])}
    out = thumbs_dir(run_id)
    out.mkdir(parents=True, exist_ok=True)

    clean = [e for e in dict.fromkeys(entry_ids or [])
             if re.fullmatch(r"[A-Za-z0-9._-]{1,64}", str(e or ""))]
    have = {e for e in clean if _cached_thumb(out, e)}
    want = [e for e in clean if e not in have][:THUMB_BATCH]
    if not want:
        return {"thumbs": {e: True for e in have}, "missing": [], "failed": [],
                "cached": len(have)}

    exe, _why = runner()
    req = json.dumps({"entry_ids": want, "out": str(out)})
    try:
        res = subprocess.run([exe, str(PIPELINES_DIR / "thumbs.py")],
                             input=req, capture_output=True, text=True,
                             encoding="utf-8", errors="replace", timeout=300,
                             cwd=str(PIPELINES_DIR.parent))
    except (OSError, subprocess.SubprocessError) as exc:
        # The interpreter never ran, so nothing was learned about any of these.
        return {"error": str(exc), "thumbs": {e: True for e in have},
                "missing": [], "failed": want}
    answer = {}
    for line in reversed((res.stdout or "").strip().splitlines()):
        try:
            answer = json.loads(line)
            break
        except ValueError:
            continue
    got = {e: True for e in have}
    got.update({e: True for e in (answer.get("thumbs") or {})})
    err = answer.get("error")
    if not answer:
        # It printed no manifest at all — a traceback, or nothing. That is a broken
        # attempt, not an archive without pictures.
        err = (res.stderr or res.stdout or "thumbs.py returned no answer").strip()[:400]
    missing = [e for e in (answer.get("missing") or []) if e not in got]
    settled = set(got) | set(missing)
    failed = [e for e in want if e not in settled]
    return {"thumbs": got, "missing": missing, "failed": failed,
            "error": err, "cached": len(have)}


def thumb_file(run_id: str, entry_id: str) -> Path | None:
    """One cached cover image, or None.

    The same two independent checks the download endpoint uses: the name is built from
    a validated entry_id rather than taken from the caller, and the resolved path is
    re-checked to be inside this run's own thumbnail directory.
    """
    if not re.fullmatch(r"[0-9a-f]{12}", str(run_id or "")):
        return None
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", str(entry_id or "")):
        return None
    base = thumbs_dir(run_id)
    if not base.is_dir():
        return None
    base = base.resolve()
    try:
        path = (base / f"{entry_id}.jpg").resolve()
        path.relative_to(base)
    except (OSError, ValueError):
        return None
    return path if path.is_file() else None


def run_file(run_id: str, name: str) -> Path | None:
    """The path to one of a run's own output files, or None.

    Two independent checks, because one of them is always the one that fails to hold:

      1. `name` is never joined onto a path. It is looked up by exact equality in the
         list of files this run actually produced, so "..\\..\\.env" is simply not a
         name in that list.
      2. The path that comes out is resolved and re-checked to be inside this run's
         own directory, which catches a symlink placed there by something else.
    """
    if not re.fullmatch(r"[0-9a-f]{12}", str(run_id or "")):
        return None
    if run_id not in RUNS and not run_dir(run_id).is_dir():
        return None
    allowed = {f["name"] for f in _artifacts(run_id)}
    if name not in allowed:
        return None
    base = (run_dir(run_id) / "output").resolve()
    try:
        path = (base / name).resolve()
        path.relative_to(base)
    except (OSError, ValueError):
        return None
    return path if path.is_file() else None


def run_zip(run_id: str) -> tuple[str, bytes] | None:
    """Everything one run produced, as a single download, or None if it produced nothing.

    A finished run leaves four or five files behind — the deck, the workbook, the
    scores, the insights — and fetching them one at a time is the part people give up
    on halfway through. Built in memory rather than written next to the outputs, so a
    zip can never be picked up by _artifacts as if it were one of the run's own
    deliverables and end up inside the next one.

    Every member is resolved through run_file, so the same two checks that guard a
    single download guard each entry here; nothing is read off the directory listing
    and trusted.
    """
    if not re.fullmatch(r"[0-9a-f]{12}", str(run_id or "")):
        return None
    files = _artifacts(run_id)
    if not files:
        return None
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            path = run_file(run_id, f["name"])
            if path is not None:
                zf.write(path, arcname=f["name"])
    with RUNS_LOCK:
        title = str((RUNS.get(run_id) or {}).get("project_name") or "")
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", title).strip("-.")[:60] or "deliverables"
    return f"{slug}-{run_id}.zip", buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════════════════
# Project store — the one door in and out of a researcher's saved work
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# Nothing else in this file builds a path into _projects/. Listing, opening, saving,
# duplicating and deleting all go through STORE, and the reason is that these reports
# are going to move: today they are JSON files in a folder on one laptop, and the plan
# is an S3 bucket so the shelf is the same shelf from any machine. When that happens the
# swap is one more subclass here and one line at STORE = ... — not five request handlers
# each with a bucket key spliced through it.
#
# So the surface is deliberately the intersection of what a directory and a bucket can
# both do cheaply, and no wider:
#
#     list()             -> [{name, raw, error, modified}]   name order, one pass
#     read(name)         -> dict | None     None for "not there", never an exception
#     write(name, proj)  -> None            unconditional; the caller decides if the
#                                           overwrite is wanted
#     delete(name)       -> str | None      where the copy went, None if it was absent
#     exists(name)       -> bool
#
# Three rules an S3 implementation has to keep, because the rest of the file leans on
# them:
#
#   * A name is a slug, never a path. _slug() leaves nothing but [A-Za-z0-9_], so a name
#     can neither climb out of the store nor need escaping as an object key.
#   * Deleting never destroys. It moves the report aside — into _trash/ here, into a
#     _trash/ prefix or a delete marker in a bucket — and returns where it went, so the
#     page can tell the researcher how to get it back. A one-click delete that is
#     genuinely final is not something anybody asked for.
#   * The store parses JSON and stops there: no migrate(), no badges, no validation.
#     Schema is the caller's business, so a store cannot fall behind the schema.
#
# list() returns name order rather than newest-first on purpose — it is the order a
# bucket listing already comes back in, and it keeps the selftest deterministic. Sorting
# for display is the page's job, and /api/projects does it.


class ProjectStore:
    """Saved reports as JSON files in a directory. The only implementation today."""

    def __init__(self, root: Path):
        self.root = root
        self.trash = root / "_trash"

    def _path(self, name) -> Path:
        return self.root / f"{_slug(name)}.json"

    def exists(self, name) -> bool:
        return self._path(name).is_file()

    def list(self) -> list[dict]:
        self.root.mkdir(parents=True, exist_ok=True)
        out = []
        # _trash/ is a directory, so a non-recursive glob steps over it — deleted work
        # stays recoverable without ever showing up as a report again.
        for f in sorted(self.root.glob("*.json")):
            rec = {"name": f.stem, "raw": None, "error": "", "modified": 0.0}
            try:
                rec["modified"] = f.stat().st_mtime
                rec["raw"] = json.loads(f.read_text("utf-8"))
            except (OSError, ValueError) as exc:
                # An unreadable file is still a row. Hiding it would leave a researcher
                # staring at a shelf that quietly lost something.
                rec["error"] = f"{type(exc).__name__}: {exc}"
            out.append(rec)
        return out

    def read(self, name) -> dict | None:
        try:
            return json.loads(self._path(name).read_text("utf-8"))
        except (OSError, ValueError):
            return None

    def write(self, name, project: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        # Written beside the target and moved into place. A crash halfway through a
        # save then costs the new version, never the version already on the shelf.
        path = self._path(name)
        tmp = path.parent / (path.name + ".part")
        tmp.write_text(json.dumps(project, indent=2), encoding="utf-8")
        os.replace(tmp, path)

    def delete(self, name) -> str | None:
        path = self._path(name)
        if not path.is_file():
            return None
        self.trash.mkdir(parents=True, exist_ok=True)
        # Stamped, so deleting two reports of the same name a week apart keeps both
        # copies and the page can name the exact file it moved.
        dest = self.trash / f"{path.stem}.{datetime.now():%Y%m%d-%H%M%S}.json"
        os.replace(path, dest)
        return str(dest)

    def free_name(self, title: str) -> tuple[str, str]:
        """A title nobody is using yet, and its slug. Used by duplicate, which must not
        land on top of the report it was copied from."""
        base, cand, i = str(title).strip() or "untitled", str(title).strip(), 2
        while self.exists(cand):
            cand = f"{base} {i}"
            i += 1
        return cand, _slug(cand)


STORE = ProjectStore(PROJECTS_DIR)


# ═══════════════════════════════════════════════════════════════════════════════════════
# Hand-off — Export saves the file AND emails it to Engineering
# ═══════════════════════════════════════════════════════════════════════════════════════

ENGINEERING_RECIPIENTS = ["hgquijano@competiscan.com"]


def _email_engineering(project: dict, path: Path, deploy_when: str = "") -> dict:
    """Attach the just-written pipeline file and hand it to Engineering directly, so a
    researcher hitting Export never has to open their own email client."""
    if PIPELINES_DIR is None:
        return {"status": "error", "error": "report_lib.py not found — cannot send email"}
    try:
        sys.path.insert(0, str(PIPELINES_DIR))
        import report_lib as L  # noqa: E402 — only needed here, not at Studio startup
    except Exception as exc:
        return {"status": "error", "error": f"report_lib unavailable: {exc}"}

    client = str(project.get("client") or project.get("name") or "Untitled").strip()
    subject = f"{client} — pipeline ready for deployment"
    schedule_line = f"Requested schedule: {deploy_when}\n\n" if deploy_when else ""
    body = (
        f"Hey Engineering Team,\n\n"
        f'A researcher just finished building the "{client}" pipeline in Pipelines '
        f"Studio v3 and it's ready to be reviewed, deployed and scheduled.\n\n"
        f"{schedule_line}"
        f"The generated file is attached, zipped ({path.name}) — mail servers commonly "
        f"block raw .py attachments.\n\n"
        f"— Sent automatically by Pipelines Studio"
    )
    # Exchange/Microsoft 365 blocks raw .py attachments by default; a .zip isn't on
    # that blocklist, so this is the reliable way to actually get the file through.
    with tempfile.TemporaryDirectory() as tmp:
        zip_path = Path(tmp) / f"{path.stem}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(path, arcname=path.name)

        sent, errors = [], []
        for to_addr in ENGINEERING_RECIPIENTS:
            try:
                res = L.send_email(str(zip_path), to_addr=to_addr, subject=subject,
                                   body=body)
            except Exception as exc:
                res = {"status": "error", "error": str(exc)}
            (sent if res.get("status") == "sent" else errors).append(
                {"to": to_addr, **res})
    return {"sent": sent, "errors": errors}


# ═══════════════════════════════════════════════════════════════════════════════════════
# Web UI — one settings pane, one list of section cards. No canvas, no wiring.
# ═══════════════════════════════════════════════════════════════════════════════════════

HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Pipelines Studio</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#f6f7fa;--card:#fff;--line:#dfe3ec;--ink:#1e2434;--dim:#6c7489;
--accent:#3563d6;--soft:#eaf0ff;--ok:#1c8a56;--warn:#a5661a;--err:#c0392f}
body{background:var(--bg);color:var(--ink);height:100vh;display:flex;flex-direction:column;
overflow:hidden;font:14px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
button,select,input,textarea{font:inherit;color:var(--ink);background:#fff;
border:1px solid var(--line);border-radius:7px;padding:7px 10px}
button{cursor:pointer}
button:hover:not(:disabled){border-color:var(--accent);color:var(--accent)}
button:disabled{opacity:.35;cursor:default}
button.primary{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
button.primary:hover{background:#2a52b8;color:#fff}
button.ghost{border:none;background:none;color:var(--dim);padding:4px 8px}
button.ghost:hover:not(:disabled){color:var(--accent);background:var(--soft)}
input,select,textarea{width:100%}
textarea{min-height:56px;resize:vertical}

#top{display:flex;align-items:center;gap:10px;padding:10px 16px;background:#fff;
border-bottom:1px solid var(--line)}
#brandLogo{height:44px;width:auto;display:block}
#top .logo{font-weight:700;letter-spacing:-.2px}
#top .v{font-size:11px;font-weight:700;color:var(--accent);background:var(--soft);
padding:1px 6px;border-radius:5px;margin-left:-4px}
#rname{width:200px;font-weight:600;border-color:transparent;background:#f2f4f9}
#rname:focus{border-color:var(--accent);background:#fff}
.sp{flex:1}
#health{font-size:12.5px;color:var(--dim);display:flex;gap:9px;align-items:center}
.pill{padding:2px 9px;border-radius:20px;font-size:12px;font-weight:600}
.pill.err{background:#fdecea;color:var(--err)}
.pill.wr{background:#fdf3e3;color:var(--warn)}
.pill.ok{background:#e8f6ee;color:var(--ok)}
.pill.dim{background:#eef1f7;color:var(--dim)}

#body{flex:1;display:flex;min-height:0}
#pane{width:290px;background:#fff;border-right:1px solid var(--line);overflow:auto;
padding:16px;flex:none}
#pane.hide{display:none}
/* The report's own settings fold away the same way the results panel and the output
   log do, so the piece review can have the whole window. */
#paneTab{width:26px;background:#fff;border-right:1px solid var(--line);display:none;
align-items:center;justify-content:center;cursor:pointer;flex:none;color:var(--dim)}
#paneTab.show{display:flex}
#paneTab span{writing-mode:vertical-rl;font-size:11.5px;font-weight:700;
letter-spacing:.6px}
#gen:not(:empty){margin-bottom:12px}
/* min-width:0 so the wider results panel makes this column shrink rather than
   push the window into a horizontal scrollbar. */
#stage{flex:1;min-width:0;overflow:auto;padding:20px 24px}
.wrapper{max-width:720px;margin:0 auto}

h2{font-size:12px;text-transform:uppercase;letter-spacing:.7px;color:var(--dim);margin:0 0 10px}
h2.mt{margin-top:22px}
h2 .sub{text-transform:none;letter-spacing:0;font-weight:400}
.f{margin-bottom:13px}
.f label{display:block;font-size:12.5px;color:var(--dim);margin-bottom:4px}
.hint{font-size:11.5px;color:#8b93a6;margin-top:4px;line-height:1.45}
.check{display:flex;align-items:flex-start;gap:8px;margin-bottom:9px;cursor:pointer;
font-size:13.5px}
.check input{width:auto;margin-top:3px;flex:none;cursor:pointer}
.chips{display:flex;flex-wrap:wrap;gap:5px}
.chip{padding:3px 10px;border:1px solid var(--line);border-radius:16px;cursor:pointer;
font-size:12.5px;background:#fff;user-select:none}
.chip:hover{border-color:var(--accent)}
.chip.on{background:var(--accent);border-color:var(--accent);color:#fff}
.chip.mini{font-size:11.5px;padding:2px 8px}

.sec{background:var(--card);border:1px solid var(--line);border-radius:11px;
margin-bottom:12px;overflow:hidden}
.sec.bad{border-color:#eab9b3}
.sechead{display:flex;align-items:center;gap:12px;padding:13px 15px;cursor:pointer}
.sechead:hover{background:#fafbfe}
.sechead .name{font-weight:650;font-size:15px}
.sechead .sub2{font-size:12.5px;color:var(--dim)}
.num{width:24px;height:24px;border-radius:50%;background:#eef1f7;color:var(--dim);
font-size:12px;font-weight:700;display:flex;align-items:center;justify-content:center;
flex:none}
.flow{display:flex;gap:4px;align-items:center;margin-left:auto}
.step{font-size:11px;padding:2px 8px;border-radius:5px;background:#eef1f7;color:#9aa2b5;
font-weight:600;white-space:nowrap}
.step.on{background:var(--soft);color:var(--accent)}
.arr{color:#c8cee0;font-size:10px}
.secbody{padding:2px 15px 16px;border-top:1px solid var(--line)}
.grp{padding:14px 0;border-bottom:1px dashed var(--line)}
.grp:last-child{border-bottom:none}
.grp h3{font-size:13px;font-weight:700;margin-bottom:3px;display:flex;align-items:center;
gap:10px}
.why{font-size:12px;color:var(--dim);margin-bottom:11px}
.row{display:flex;gap:11px}.row>*{flex:1}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:1px 14px;margin-top:4px}
.col{display:flex;align-items:center;gap:7px;font-size:12.5px;padding:3px 0;cursor:pointer}
.col input{width:auto;flex:none;cursor:pointer}
.msg{font-size:12.5px;padding:7px 11px;border-radius:7px;margin:3px 0;line-height:1.45}
.msg.error{background:#fdecea;color:var(--err)}
.msg.warn{background:#fdf6e9;color:var(--warn)}
.secmsgs{padding:0 15px 12px}
details summary{cursor:pointer;font-size:12.5px;color:var(--dim);padding:5px 0;
user-select:none}
details summary:hover{color:var(--accent)}
.addbtn{width:100%;padding:12px;border-style:dashed;color:var(--dim)}

/* ── the filter list ─────────────────────────────────────────────────────── */
.flt{border:1px solid var(--line);border-radius:9px;padding:9px 11px;margin-bottom:7px;
background:#fbfcfe}
.flt .fh{display:flex;align-items:center;gap:9px;margin-bottom:6px}
.flt .fh b{font-size:12.5px;font-weight:650}
.flt .fh .grp2{font-size:10px;font-weight:700;color:var(--dim);background:#eef1f7;
padding:1px 6px;border-radius:4px;letter-spacing:.3px;text-transform:uppercase}
.flt .fh .cost{font-size:10px;font-weight:700;color:var(--warn);background:#fdf3e3;
padding:1px 6px;border-radius:4px}
.flt .note{font-size:11px;color:#8b93a6;margin-top:5px;line-height:1.45}
.flt select,.flt input{width:auto;min-width:120px}
.tri{display:flex;gap:4px}
.tri span{padding:3px 12px;border:1px solid var(--line);border-radius:16px;cursor:pointer;
font-size:12px;background:#fff;user-select:none}
.tri span.on{background:var(--accent);border-color:var(--accent);color:#fff}
/* One row per thing you can narrow by, in three fixed columns: the label, the values
   you have picked as removable tags, and the control to add another. The third column
   sits second, at a fixed width, so every add control is the same size and in the same
   place — it must not drift sideways as tags come and go. The tags flow after it. */
.srow{display:grid;grid-template-columns:104px 150px 1fr;gap:9px 12px;
align-items:start;margin-bottom:9px}
.srow .lbl{font-size:12.5px;color:var(--dim);padding-top:6px}
.aside{position:relative}
.aside>*,.aside select,.aside input,.aside button{width:100%}
.pick{display:flex;flex-wrap:wrap;gap:5px;align-items:center;padding-top:3px}
.tag{display:inline-flex;align-items:center;gap:6px;padding:3px 9px;border-radius:6px;
background:var(--soft);color:var(--accent);font-size:12.5px;font-weight:600}
.tag b{cursor:pointer;opacity:.5;font-weight:700}
.tag b:hover{opacity:1}
select.add,input.add{padding:5px 9px;font-size:12.5px}
select.add{color:var(--dim)}
.tri.sm span{padding:2px 9px;font-size:11.5px}
.lookup{position:relative}
.lookres{position:absolute;left:0;top:calc(100% + 4px);z-index:20;min-width:290px;
border:1px solid var(--line);border-radius:8px;max-height:190px;overflow:auto;
background:#fff;box-shadow:0 10px 28px #0002}
.lookres div{padding:5px 9px;font-size:12.5px;cursor:pointer;border-bottom:1px solid #f0f2f7}
.lookres div:hover{background:var(--soft)}
.lookres div:last-child{border-bottom:none}
.pvw{background:#141824;color:#dfe4f0;border-radius:8px;padding:10px 12px;margin-top:9px;
font:11.5px/1.6 ui-monospace,Menlo,monospace;white-space:pre-wrap;max-height:230px;
overflow:auto}
.picklist{max-height:52vh;overflow:auto}
.pickgrp{font-size:10.5px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
color:var(--dim);padding:11px 2px 4px}
.pickitem{display:flex;align-items:center;gap:9px;padding:6px 9px;border-radius:7px;
cursor:pointer;font-size:13px}
.pickitem:hover{background:var(--soft)}
.pickitem.has{opacity:.4;cursor:default}
.pickitem .k{font-size:10px;font-weight:700;color:var(--dim);background:#eef1f7;
padding:1px 6px;border-radius:4px;margin-left:auto;flex:none}
.pickitem code{font-size:11px;color:var(--dim)}

/* The deliverables drawer, at the right edge beside the results strip.

   NOT in the terminal, which is a transcript: it scrolls away, it gets cleared, and a
   link in it goes with it. And not a bar above the terminal either — that stole a
   band of height from every window for something only a finished run has. A strip on
   the edge costs 26px, stays put, and is where the eye already goes for RESULTS. */
#deliv{width:272px;min-width:272px;background:var(--card);border-left:1px solid
var(--line);overflow:auto;padding:14px;flex:none;display:flex;flex-direction:column;
gap:7px}
#deliv.hide{display:none}
#delivTab{width:26px;background:#fff;border-left:1px solid var(--line);display:none;
align-items:center;justify-content:center;cursor:pointer;flex:none;color:var(--dim)}
#delivTab.show{display:flex}
#delivTab.ready{color:var(--accent);background:var(--soft)}
#delivTab span{writing-mode:vertical-rl;font-size:11.5px;font-weight:700;
letter-spacing:.6px}
#deliv .dhead{display:flex;align-items:baseline;gap:8px;font-size:13px;font-weight:700}
#deliv .dhead .sp{flex:1}
#deliv .note{font-size:11.5px;line-height:1.5;color:var(--dim);margin-bottom:2px}
#deliv a{text-decoration:none;border:1px solid var(--line);border-radius:8px;
padding:8px 10px;font-size:13px;font-weight:600;color:var(--accent);background:#fff;
display:flex;flex-direction:column;gap:3px;word-break:break-all}
#deliv a:hover{border-color:var(--accent);background:var(--soft)}
#deliv a.all{border-color:var(--accent);background:var(--soft);align-items:center;
flex-direction:row;justify-content:center;word-break:normal}
#deliv .k{font-size:9.5px;font-weight:800;letter-spacing:.4px;color:var(--dim);
background:#eef1f7;border-radius:4px;padding:1px 5px;align-self:flex-start}
#deliv .sz{font-size:11px;font-weight:500;color:var(--dim)}
/* Full width borrows the whole window for the review, and this goes with the rest. */
#body.panelfull #deliv,#body.panelfull #delivTab{display:none}

#logbar{display:flex;align-items:center;gap:8px;padding:6px 16px;background:#1d2233;
color:#8b93a6;font-size:12px;border-top:1px solid #2a3145}
#log{height:176px;background:#141824;color:#dfe4f0;overflow:auto;padding:11px 16px;
font:12px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap}
#log.min{display:none}
#log .e{color:#ff9a90}#log .w{color:#f0c977}#log .o{color:#8ee0ae}#log .d{color:#7b849b}

.overlay{position:fixed;inset:0;background:#1e243455;display:none;align-items:center;
justify-content:center;z-index:50}
.overlay.show{display:flex}
.panel{background:#fff;border-radius:13px;width:670px;max-width:92vw;max-height:84vh;
display:flex;flex-direction:column;box-shadow:0 18px 50px #0003}
.panel header{padding:15px 19px;border-bottom:1px solid var(--line);display:flex;
align-items:center;gap:10px}
.panel header b{font-size:15px;flex:1}
.panel .content{padding:17px 19px;overflow:auto}
.panel footer{padding:13px 19px;border-top:1px solid var(--line);display:flex;gap:9px;
justify-content:flex-end}
.plist{list-style:none}
.plist li{display:flex;align-items:center;gap:10px;padding:9px 11px;border-radius:8px;
border:1px solid var(--line);margin-bottom:7px}
.plist li b{flex:1;font-weight:600;min-width:0}
/* The report on screen right now, marked in the list — so Delete is never a surprise
   about which one is about to go. */
.plist li.here{border-color:var(--accent);background:#f7f9ff}
.plist li .here-pill{font-size:11px;font-weight:700;color:var(--accent)}
.plist li .when{color:var(--dim)}
pre.code{background:#141824;color:#dfe4f0;padding:13px;border-radius:8px;overflow:auto;
max-height:48vh;font:11.5px/1.55 ui-monospace,Menlo,monospace;margin-top:8px}

/* ── status badge: where a report stands, for either job ───────────────────── */
.badge{padding:2px 9px;border-radius:20px;font-size:11.5px;font-weight:700;
white-space:nowrap;cursor:help}
.badge.dim{background:#eef1f7;color:#6c7489}
.badge.ok{background:#e8f6ee;color:var(--ok)}
.badge.sent{background:var(--soft);color:var(--accent)}
.badge.warn{background:#fdf3e3;color:var(--warn)}

/* ── the run bar: the ladder, and what one rung costs ──────────────────────── */
#runbar{display:flex;align-items:center;gap:9px;padding:7px 16px;background:#fbfcfe;
border-bottom:1px solid var(--line);font-size:12.5px}
#runbar .modehelp{color:var(--dim);font-size:12px;flex:1;min-width:80px;
overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#runbar input{width:190px;font-size:12.5px;padding:5px 8px}
#runbar select{font-size:13px;padding:5px 8px;width:auto;font-weight:600}
#runbar .est{color:var(--dim);white-space:nowrap}
.spin{display:inline-block;width:11px;height:11px;border:2px solid #c8cee0;
border-top-color:var(--accent);border-radius:50%;animation:sp .7s linear infinite;
vertical-align:-1px}
@keyframes sp{to{transform:rotate(360deg)}}

/* ── results panel: one component, two states ──────────────────────────────── */
/* This is where the judgement is made — is this the right piece for the slide — so it
   gets the room. A 64px thumbnail of a direct mail piece is a smudge; nobody can tell
   a rate-led envelope from a cash-back one at that size. Wide by default, and a full
   width mode for the review itself. */
#panel{width:min(58vw,900px);min-width:520px;background:#fff;
border-left:1px solid var(--line);overflow:auto;padding:18px;flex:none;font-size:14px}
#panel.hide{display:none}
/* Full width: while a slate is being settled, the settings pane and the section list
   are not what anyone is looking at. */
#panel.full{width:auto;min-width:0;flex:1}
#body.panelfull #pane,#body.panelfull #paneTab,#body.panelfull #stage{display:none}
#panel .pwrap{max-width:1200px;margin:0 auto}
/* While the deck is being written the panel is a record of a decision already made,
   not something to act on. Greyed and inert says that without taking it off screen —
   the researcher can still see which pieces are being built from. */
#panel.building .psec{filter:grayscale(1);opacity:.55;pointer-events:none}
#panel h2{font-size:13px}
.phead{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
.phead .sp{flex:1;min-width:12px}
#panelTab{width:26px;background:#fff;border-left:1px solid var(--line);display:none;
align-items:center;justify-content:center;cursor:pointer;flex:none;color:var(--dim)}
#panelTab.show{display:flex}
#panelTab span{writing-mode:vertical-rl;font-size:11.5px;font-weight:700;
letter-spacing:.6px}
.psec{border:1px solid var(--line);border-radius:10px;margin-bottom:14px;overflow:hidden}
.psec>h4{font-size:14.5px;padding:11px 14px;background:#f7f8fc;font-weight:700;
display:flex;gap:10px;align-items:baseline}
.psec>h4 .cnt{margin-left:auto;font-weight:600;color:var(--dim);font-size:12.5px}
.piece{border-top:1px solid var(--line);padding:12px 14px;font-size:14px;
display:flex;gap:14px;align-items:flex-start}
.piece.gone{opacity:.4}
.piece .meta{flex:1;min-width:0}
.piece .eid{font:13px ui-monospace,Menlo,monospace;color:var(--accent);cursor:pointer}
.piece .eid:hover{text-decoration:underline}
.piece .co{font-weight:650;font-size:14.5px}
.piece .hl{color:var(--dim);font-size:13.5px;line-height:1.5;margin-top:2px;
display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.piece .yn{display:flex;gap:6px;flex:none}
.piece .yn button{padding:6px 13px;font-size:13px;border-radius:7px;font-weight:600}
.piece .yn button.on{background:var(--accent);border-color:var(--accent);color:#fff}
.piece .yn button.no.on{background:var(--err);border-color:var(--err);color:#fff}
.piece .thumb{width:136px;height:176px;flex:none;border:1px solid var(--line);
border-radius:7px;background:#f2f4f9 no-repeat center/cover;overflow:hidden;
display:flex;align-items:center;justify-content:center;cursor:zoom-in}
#panel.full .piece .thumb{width:200px;height:258px}
.piece .thumb img{width:100%;height:100%;object-fit:cover;object-position:top;
display:block}
.piece .thumb .none{font-size:12px;color:#9aa2b5;text-align:center;line-height:1.4;
padding:4px;cursor:default}
/* A picture that failed to load is not a piece without a picture, and does not get to
   look like one. */
.piece .thumb.bad{cursor:pointer;border-color:#eab9b3;background:#fdf3f1}
.piece .thumb.bad .none{color:var(--err);cursor:pointer}
.piece .thumb.bad b{display:inline-block;margin-top:3px;text-decoration:underline}
.piece .swapped{font-size:13px;color:var(--dim);margin-top:6px;
display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.piece .swapped b{font-weight:600;color:var(--ink)}
.piece .swapped button{padding:3px 10px;font-size:12.5px;border-radius:6px}
.piece .why{color:#8b93a6;font-size:13.5px;line-height:1.55}
/* Naming the piece you want, rather than taking the next one the pipeline offers.
   It is a dialog and not a box wedged into the row, because the point of it is to
   SHOW you the piece you named before it goes on the slide. An id is not something a
   person can check by reading it back; the picture and the headline are. */
.byidlbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
color:var(--dim);margin:0 0 6px}
.byidrow{border:1px solid var(--line);border-radius:9px;overflow:hidden;background:#fff}
.byidrow .piece{border-top:none}
.byidrow.out{opacity:.7}
.byidrow.in{border-color:var(--accent);box-shadow:0 0 0 3px var(--soft)}
.byidarrow{text-align:center;color:var(--dim);font-size:12.5px;font-weight:600;
padding:9px 0 3px}
#byIdEid{font-family:ui-monospace,Menlo,monospace;font-size:15px;padding:9px 11px}
.byidmsg{border-radius:9px;padding:12px 14px;font-size:13.5px;line-height:1.55}
.byidmsg.bad{border:1px solid #eab9b3;background:#fdecea;color:var(--err);
font-weight:600}
.byidmsg.idle{border:1px dashed var(--line);background:#fafbfe;color:var(--dim)}
.byidwarn{border:1px solid #e8cf9a;background:#fdf3e3;border-radius:8px;
padding:10px 12px;font-size:13px;color:var(--warn);margin-top:10px;line-height:1.5}
/* The piece that just landed, so a replacement is something you SEE happen rather
   than something you have to go looking for in a list of four. */
.piece.landed{animation:landed 1.8s ease-out}
@keyframes landed{
  0%{background:#d7e5ff}
  55%{background:#ecf2ff}
  100%{background:transparent}}
#lightbox{position:fixed;inset:0;background:#141824dd;display:none;align-items:center;
justify-content:center;z-index:80;cursor:zoom-out}
#lightbox.show{display:flex}
#lightbox img{max-width:92vw;max-height:92vh;box-shadow:0 8px 40px #0008;border-radius:6px}
.atleast{background:#fdf3e3;color:var(--warn);border-radius:5px;padding:1px 6px;
font-size:11px;font-weight:700}
.dl{display:flex;flex-wrap:wrap;gap:6px;margin:4px 0 12px}
.dl a{text-decoration:none;border:1px solid var(--line);border-radius:7px;
padding:6px 12px;font-size:13.5px;font-weight:600;color:var(--accent);background:#fff;
display:inline-flex;align-items:center;gap:7px}
.dl a:hover{border-color:var(--accent);background:var(--soft)}
.dl a.all{border-color:var(--accent);background:var(--soft)}
.dl .k{font-size:9.5px;font-weight:800;letter-spacing:.4px;color:var(--dim);
background:#eef1f7;border-radius:4px;padding:1px 5px}
.dl .sz{font-size:11px;font-weight:500;color:var(--dim)}
.tsec{border:1px solid var(--line);border-radius:9px;padding:10px 12px;margin-bottom:8px;
cursor:pointer}
.tsec:hover{border-color:var(--accent);background:#fbfcff}
.tsec b{display:block;font-size:13.5px}
.tsec span{font-size:11.5px;color:var(--dim);line-height:1.45}
.plist li .meta2{font-size:11.5px;color:var(--dim)}
.warnbox{border:1px solid #eab9b3;background:#fdecea;border-radius:9px;padding:12px 15px;
margin-bottom:13px;font-size:14px;line-height:1.55}
.warnbox b{color:var(--err)}
.okbox{border:1px solid #b8ddc7;background:#eef8f2;border-radius:9px;padding:12px 15px;
margin-bottom:13px;font-size:14px;line-height:1.55}
button.ghost.warn{color:var(--warn);font-weight:700}
button.ghost.warn:hover:not(:disabled){color:var(--err);background:#fdecea}
</style></head><body>

<div id="top">
  <img id="brandLogo" src="/logo.jpg" alt="Pipelines">
  <span class="logo">Pipelines Studio</span>
  <input id="rname" placeholder="Report name">
  <span id="badge"></span>
  <div class="sp"></div>
  <div id="health"></div>
  <button onclick="openExport()">Send to Eng.</button>
  <button class="ghost" onclick="openProjects()">Projects</button>
</div>

<!-- The ladder. Two terminal actions live in this Studio, not one: Run finishes a
     one-time report, Send to Engineering finishes an ongoing one. A report that was
     never sent is not unfinished — it may simply never need to be. -->
<div id="runbar">
  <span class="modehelp" id="modeHelp"></span>
  <input id="runEmail" placeholder="Email this run to… (optional)"
    title="Used for this run only. It is never saved with the report.">
  <span class="est" id="est"></span>
  <!-- Which rung of the ladder sits next to the button that climbs it. The two are one
       decision — "run, and what exactly" — so they are read together, at the point of
       pressing, rather than at opposite ends of the bar. -->
  <select id="mode" onchange="modeChanged()"
    title="What this run does. Pick the rung, then press Run."></select>
  <button class="primary" id="runBtn" onclick="runNow()">Run</button>
  <button id="stopBtn" onclick="stopNow()" style="display:none">Stop</button>
  <button class="ghost" id="histBtn" onclick="openHistory()">Recent runs</button>
</div>

<div id="body">
  <div id="paneTab" onclick="togglePane()"><span>THE REPORT</span></div>
  <div id="pane"><div id="gen"></div><div id="settings"></div></div>
  <div id="stage"><div class="wrapper" id="sections"></div></div>
  <div id="panelTab" onclick="togglePanel()"><span>RESULTS</span></div>
  <div id="panel" class="hide" onscroll="panelScrolled()"></div>
  <div id="deliv" class="hide"></div>
  <div id="delivTab" onclick="toggleDeliv()"><span>DELIVERABLES</span></div>
</div>

<div id="logbar"><span>Output</span>
  <span id="logstate" style="color:#6c7489"></span><div class="sp"></div>
  <button class="ghost" style="color:#8b93a6" onclick="clearLog()">clear</button>
  <button class="ghost" style="color:#8b93a6" id="logTog"
    onclick="toggleLog()">show</button></div>
<div id="log" class="min"><span class="d">Describe the report on the left, add sections in the middle,
then press Run. Preview count on a section checks its filters against the archive
without generating anything.</span></div>

<div class="overlay" id="ovProjects"><div class="panel">
  <header><b>Projects</b><button class="ghost" onclick="hide('Projects')">close</button></header>
  <div class="content">
    <h2>Templates to start from <span class="sub">— generic shapes, not client
      work. Opening one gives you a new unsaved report; the template itself is never
      changed.</span></h2>
    <div id="tplList"></div>
    <h2 class="mt">Saved reports to return to <span class="sub">— your own work, the
      one you touched last at the top. Copy starts this quarter's report from last
      quarter's without touching the original. </span></h2>
    <ul class="plist" id="savedList"></ul>
  </div>
  <footer><input id="saveAs" placeholder="Save the current report as…" style="flex:1">
    <button class="primary" onclick="saveProject()">Save</button></footer>
</div></div>

<div class="overlay" id="ovHistory"><div class="panel">
  <header><b>Recent runs</b><button class="ghost" onclick="hide('History')">close</button></header>
  <div class="content" id="historyBody"></div>
</div></div>

<div class="overlay" id="ovPromote"><div class="panel">
  <header><b>Make this report recurring</b>
    <button class="ghost" onclick="hide('Promote')">close</button></header>
  <div class="content" id="promoteBody"></div>
  <footer><button class="ghost" onclick="hide('Promote')">Cancel</button>
    <button class="primary" id="promoteGo" onclick="applyPromote()">Make it recurring</button></footer>
</div></div>

<div class="overlay" id="ovExport"><div class="panel">
  <header><b>Send to Engineering</b><button class="ghost" onclick="hide('Export')">close</button></header>
  <div class="content" id="exportBody"></div>
  <footer><button class="ghost" onclick="hide('Export')">Cancel</button>
    <button class="primary" id="exportGo"
      onclick="sendToEngineering()">Send to Engineering</button></footer>
</div></div>

<div id="lightbox" onclick="this.classList.remove('show')"><img alt=""></div>

<!-- Naming the piece you want. Shows it before it goes on the slide, because an
     entry_id read back to yourself proves nothing and a cover image does. -->
<div class="overlay" id="ovById"><div class="panel">
  <header><b>Replace this piece</b>
    <span id="byIdWhere" class="pill dim"></span>
    <button class="ghost" onclick="closeById()">close</button></header>
  <div class="content" id="byIdBody"></div>
  <footer><button class="ghost" onclick="closeById()">Cancel</button>
    <button class="primary" id="byIdGo" onclick="useById()" disabled>Use this piece
      </button></footer>
</div></div>

<div class="overlay" id="ovPick"><div class="panel">
  <header><b>Add a filter</b>
    <span id="pickCount" class="pill dim"></span>
    <button class="ghost" onclick="hide('Pick')">close</button></header>
  <div class="content">
    <div class="f"><input id="pickSearch" placeholder="Search all filters — name, group, or what it does"
      oninput="renderPick()" autocomplete="off"></div>
    <div class="picklist" id="pickList"></div>
  </div>
</div></div>
"""

HTML += r"""
<script>
let SPEC=null,P=null,ISSUES=[],OPEN={},poll=null,deb=null,CHK={};
let FLAT={},TAXO={},PICKFOR=null,PVW={},FSEARCH={},LOOKRES={},lookDeb={};
/* the run and its results panel */
let RUNID=null,PANEL=null,PSTATE={},T0=null;
/* What was approved at the pause, section id -> the cards, and whether the build from
   them is still going.

   Confirming used to empty PSTATE and nothing took its place, so renderPanel fell
   straight back to sec.pieces — and the panel a researcher had just finished narrowing
   to three pieces reopened as the entire retrieved pool, hundreds of rows, at the
   exact moment the deck was being built from three. Holding on to the slate keeps the
   panel showing the decision that was actually made. */
let BUILT=null,BUILDING=false;
/* entry_id -> one of four states. Cleared whenever a new run starts, because the
   pictures are cached per run.

     undefined  not asked about yet
     "ok"       fetched; there is an image to show
     "none"     the archive genuinely holds no cover image for this piece. Final.
     "retry"    the attempt failed - a shut tunnel, a 403, a dropped read. NOT an
                answer about the piece, so it is asked again.

   The three-state split is the whole point. This used to be a boolean, so every
   transient failure was written down as false and rendered as "no image on file" for
   the rest of the run - a definite claim about the archive, made from a timeout, and
   never retried. Pieces that plainly did have a picture kept the placeholder until
   they happened to be swapped, which is what asked for them a second time. */
let THUMBS={},thumbBusy=false,thumbAgain=false,THUMBWARNED=false;
/* How many times each piece has been asked about, and the pending retry timer. */
let THUMBTRY={},thumbRetry=null;
const THUMB_TRIES=5;
/* The entry_id of a piece that has just landed on a slate, to be flashed once. */
let FLASH="";
const $=s=>document.querySelector(s);
const esc=s=>String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;")
  .replace(/"/g,"&quot;");
/* JSON safe to sit inside an HTML attribute — filter names and option values are
   archive data, and several of them carry quotes, slashes and ampersands. */
const jq=o=>JSON.stringify(o).replace(/"/g,"&quot;");
/* An entry_id inside a CSS attribute selector. They are validated to
   [A-Za-z0-9._-] server side, so this only has to survive the quoting rules. */
const cssq=s=>String(s==null?"":s).replace(/["\\]/g,"\\$&");
const KIDOF={category:"sector",subcategory:"category",subsubcategory:"subcategory"};
const LEVELS=["sector","category","subcategory","subsubcategory"];
const LEVELLBL={sector:"Sector",category:"Category",subcategory:"Sub-category",
  subsubcategory:"Sub-sub"};

async function boot(){
  SPEC=await (await fetch("/api/spec")).json();
  FLAT={};
  for(const [g,items] of Object.entries(SPEC.groups||{}))
    for(const [f,spec] of Object.entries(items||{})) FLAT[f]={...spec,group:g,field:f};
  TAXO["__root__"]=SPEC.sectors||[];
  $("#mode").innerHTML=(SPEC.modes||[]).map(m=>
    `<option value="${m.key}">${esc(m.label)}</option>`).join("");
  try{
    const last=localStorage.getItem("rs.mode");
    if(last&&(SPEC.modes||[]).some(m=>m.key===last))$("#mode").value=last;
  }catch(e){}
  modeChanged();
  renderTemplates();
  restoreLog();
  P=(await (await fetch("/api/template?name=example")).json()).project;
  await prefetchTaxo();
  render(); refreshSaved(); restoreFiles();
}
function render(){$("#rname").value=P.name||"";renderSettings();renderSections();check()}

/* ── taxonomy: the tree replaces v2's flat, hardcoded sector list ─────────── */
async function taxo(parent){
  const k=parent||"__root__";
  if(TAXO[k])return TAXO[k];
  try{
    const d=await (await fetch("/api/taxonomy?parent="+
      encodeURIComponent(parent||""))).json();
    TAXO[k]=d.children||[];
  }catch(e){TAXO[k]=[]}
  return TAXO[k];
}
async function prefetchTaxo(){
  for(const s of (P.sections||[]))
    for(const lv of ["sector","category","subcategory"])
      for(const v of (s.search[lv]||[])) await taxo(v);
}
function taxoOpts(s,level){
  if(level==="sector")return (SPEC.sectors||[]).map(x=>x.name);
  const out=[];
  for(const pv of (s.search[KIDOF[level]]||[]))
    for(const c of (TAXO[pv]||[])) out.push(c.name);
  return [...new Set(out)];
}
async function taxoTog(id,level,val){
  const q=S(id).search, a=q[level]||(q[level]=[]);
  const i=a.indexOf(val);
  if(i<0){a.push(val);await taxo(val)}else{a.splice(i,1)}
  /* A node whose parent is no longer selected cannot apply — the archive rejects an
     id outside the parent level you also sent, so drop it rather than send a 400. */
  for(let k=LEVELS.indexOf(level)+1;k<LEVELS.length;k++){
    const allowed=new Set(taxoOpts(S(id),LEVELS[k]));
    q[LEVELS[k]]=(q[LEVELS[k]]||[]).filter(x=>allowed.has(x));
  }
  renderSections();check();
}

/* ── report settings ─────────────────────────────────────────────────────── */
function renderSettings(){
  const d=P.deck,w=P.workbook,e=P.email;
  const win=P.window||{mode:"cadence",start:"",end:""};
  const df=SPEC.date_fields.map(f=>`<option value="${f.key}"${
    P.date_field===f.key?" selected":""}>${esc(f.label)}</option>`).join("");
  const dnote=(SPEC.date_fields.find(f=>f.key===P.date_field)||{}).note||"";
  $("#settings").innerHTML=`
  <h2>The report<button class="ghost" style="float:right;margin-top:-3px"
    onclick="hidePane()" title="Fold this pane away. It comes back from the strip on
the left.">minimise</button></h2>
  <div class="f"><label>Client</label>
    <input value="${esc(P.client)}" oninput="setP('client',this.value)"
      placeholder="e.g. Harborstone"></div>
  <div class="f"><label>Which dates it covers</label>
    <select onchange="setWin('mode',this.value)">
      <option value="cadence"${win.mode!=="range"?" selected":""}>A repeating period</option>
      <option value="range"${win.mode==="range"?" selected":""}>Specific dates (one-off)</option>
    </select></div>
  ${win.mode==="range"?`
  <div class="f"><label>From</label>
    <input type="date" value="${esc(win.start)}" onchange="setWin('start',this.value)"></div>
  <div class="f"><label>To</label>
    <input type="date" value="${esc(win.end)}" onchange="setWin('end',this.value)">
    <div class="hint">Right for a one-off — "Q2", or a client asking about March. Every
      run covers exactly these dates, which is also why a report like this must not be
      put on a schedule: it would produce the same period for ever. Use
      <b>Make it recurring</b> below before sending it to Engineering.</div></div>
  <button style="width:100%" onclick="openPromote()">Make it recurring…</button>`:`
  <div class="f"><label>How often it runs</label>
    <select onchange="setP('cadence',this.value)">
      <option value="month"${P.cadence==="month"?" selected":""}>Monthly</option>
      <option value="week"${P.cadence==="week"?" selected":""}>Weekly</option>
    </select></div>
  <div class="f"><label>Which period it covers</label>
    <select onchange="setP('anchor',this.value)">
      <option value="prior_complete"${P.anchor==="prior_complete"?" selected":""}>The last complete one</option>
      <option value="rolling"${P.anchor==="rolling"?" selected":""}>The last 7 / 30 days</option>
    </select>
    <div class="hint">The last complete period is reproducible: running it again tomorrow
      covers the same dates, which is what makes it safe to schedule.</div></div>`}
  <div class="f"><label>Which date the archive filters on</label>
    <select onchange="setP('date_field',this.value)">${df}</select>
    <div class="hint">${esc(dnote)} The archive applies this itself now — nothing is
      filtered by date after the fact.</div></div>

  <h2 class="mt">Slides</h2>
  <label class="check"><input type="checkbox" ${d.enabled?"checked":""}
    onchange="setD('enabled',this.checked)"><span>Build a slide deck</span></label>
  ${d.enabled?`
  <label class="check"><input type="checkbox" ${d.title_slide?"checked":""}
    onchange="setD('title_slide',this.checked)"><span>Title slide</span></label>
  <label class="check"><input type="checkbox" ${d.summary_slide?"checked":""}
    onchange="setD('summary_slide',this.checked)"><span>Summary slide up front
    <span style="color:#8b93a6">— written last, from the section paragraphs</span></span></label>
  <label class="check"><input type="checkbox" ${d.section_headings?"checked":""}
    onchange="setD('section_headings',this.checked)"><span>Group sections under headings</span></label>
  <label class="check"><input type="checkbox" ${d.closing_slide?"checked":""}
    onchange="setD('closing_slide',this.checked)"><span>Closing slide</span></label>
  <div class="f" style="margin-top:11px"><label>Deck title</label>
    <input value="${esc(d.title)}" oninput="setD('title',this.value)"></div>
  <div class="f"><label>File name</label>
    <input value="${esc(d.filename)}" oninput="setD('filename',this.value)">
    <div class="hint">{client} {period} {stamp} {mmddyy} {month_year}</div></div>`:""}

  <h2 class="mt">Workbook</h2>
  <label class="check"><input type="checkbox" ${w.enabled?"checked":""}
    onchange="setW('enabled',this.checked)"><span>Build an Excel workbook</span></label>
  ${w.enabled?`<div class="f"><label>File name</label>
    <input value="${esc(w.filename)}" oninput="setW('filename',this.value)"></div>`:""}

  <h2 class="mt">When it finishes</h2>
  <label class="check"><input type="checkbox" ${e.enabled?"checked":""}
    onchange="setE('enabled',this.checked)"><span>Email the files on every scheduled
    run</span></label>
  ${e.enabled?`<div class="f"><label>Recipient comes from</label>
    <input value="${esc(e.env_var||"RS_EMAIL_TO")}" oninput="setE('env_var',this.value)"
      placeholder="RS_EMAIL_TO">
    <div class="hint">The NAME of an environment variable, not an address. Engineering
      sets it on the box that runs this, so the report itself never carries a
      recipient.</div></div>`:""}
  <div class="hint" style="margin-top:-4px">For a one-off, put the address in the
    <b>Email this run to…</b> box at the top instead. It is used for that run and kept
    nowhere.</div>

  <h2 class="mt">Notes for Engineering</h2>
  <div class="f"><textarea oninput="setP('notes',this.value)"
    placeholder="Anything this tool cannot express — an unusual split, a one-off rule. It is copied into the exported file as a to-do.">${esc(P.notes)}</textarea></div>`;
}
function setP(k,v){P[k]=v;soft(k==="date_field"||k==="cadence")}
function setWin(k,v){
  P.window=P.window||{mode:"cadence",start:"",end:""};
  P.window[k]=v;
  soft(k==="mode");
}
function setD(k,v){P.deck[k]=v;soft(typeof v==="boolean")}
function setW(k,v){P.workbook[k]=v;soft(typeof v==="boolean")}
function setE(k,v){P.email[k]=v;soft(typeof v==="boolean")}
function soft(hard){if(hard){renderSettings();renderSections()}check()}

/* ── sections ────────────────────────────────────────────────────────────── */
function renderSections(){
  const a=document.activeElement;
  const infield=a&&(a.tagName==="INPUT"||a.tagName==="TEXTAREA")&&$("#sections").contains(a);
  const key=infield?(a.getAttribute("oninput")||a.getAttribute("onchange")):null;
  let sel=null;
  if(infield){try{
    if(typeof a.selectionStart==="number")sel=[a.selectionStart,a.selectionEnd];
  }catch(e){}}
  $("#sections").innerHTML=
    `<h2>Sections <span class="sub">— one per topic. Each is searched, then written to
      the workbook and onto a slide.</span></h2>`
    + (P.sections||[]).map((s,i)=>card(s,i)).join("")
    + `<button class="addbtn" onclick="addSection()">+ Add a section</button>`;
  if(key){
    const again=[...document.querySelectorAll("#sections input,#sections textarea")]
      .find(el=>(el.getAttribute("oninput")||el.getAttribute("onchange"))===key);
    if(again){
      again.focus();
      if(sel){try{again.setSelectionRange(sel[0],sel[1])}catch(e){}}
    }
  }
}
function card(s,i){
  const open=!!OPEN[s.id];
  const mine=ISSUES.filter(x=>x.section===s.id);
  const bad=mine.some(x=>x.level==="error");
  const n=(s.search.media_channel||[]).length;
  const nf=(s.search.filters||[]).length;
  const sub=[n+" channel"+(n===1?"":"s"),
    nf?(nf+" filter"+(nf===1?"":"s")):"no extra filters",
    s.sheet.enabled?("tab: "+((s.sheet.tab||"").trim()||s.title||"—")):"no worksheet tab",
    s.feature.enabled?(s.feature.count+" featured"):"not on a slide"].join("  ·  ");
  return `<div class="sec${bad?" bad":""}">
   <div class="sechead" onclick="toggle('${s.id}')">
    <span class="num">${i+1}</span>
    <div><div class="name">${esc(s.title||"(untitled)")}</div>
      <div class="sub2">${esc(sub)}</div></div>
    <div class="flow">
      <span class="step on">Search</span><span class="arr">›</span>
      <span class="step${s.sheet.enabled?" on":""}">Worksheet</span><span class="arr">›</span>
      <span class="step${s.feature.enabled?" on":""}">Feature</span><span class="arr">›</span>
      <span class="step${s.feature.enabled?" on":""}">Slide</span></div>
    <button class="ghost" title="move up" ${i===0?"disabled":""}
      onclick="event.stopPropagation();move('${s.id}',-1)">↑</button>
    <button class="ghost" title="move down" ${i===P.sections.length-1?"disabled":""}
      onclick="event.stopPropagation();move('${s.id}',1)">↓</button>
    <button class="ghost" title="remove"
      onclick="event.stopPropagation();delSection('${s.id}')">✕</button>
   </div>
   ${mine.length?`<div class="secmsgs">${mine.map(m=>
     `<div class="msg ${m.level==="error"?"error":"warn"}">${esc(m.msg)}</div>`).join("")}</div>`:""}
   ${open?body(s):""}</div>`;
}
"""

HTML += r"""
/* ── one section's open body ──────────────────────────────────────────────── */
function body(s){
  const q=s.search;

  /* label | the one control that adds more | what is already picked. The add cell is
     always emitted, even when there is nothing left to add, so it never moves. */
  const row=(label,body,aside)=>`<div class="srow"><div class="lbl">${label}</div>
    <div class="aside">${aside||""}</div><div>${body}</div></div>`;
  const tags=(key,list)=>list.map(o=>`<span class="tag">${esc(o)}<b title="remove"
    onclick="drop('${s.id}','${key}',${jq(o)})">×</b></span>`).join("");
  const addSelect=(key,options,fn,label)=>{
    const left=options.filter(o=>!(s.search[key]||[]).includes(o));
    return left.length?`<select class="add" onchange="if(this.value){
      ${fn}('${s.id}',this.value);this.value=''}">
      <option value="">${label}</option>`+left.map(o=>
        `<option value="${esc(o)}">${esc(o)}</option>`).join("")+`</select>`:"";
  };
  /* Sector > Category > Sub > Sub-sub. A level only appears once its parent has a
     value, so the deeper ones cost nothing until they are relevant. */
  const taxoRows=LEVELS.map(lv=>{
    const opts=taxoOpts(s,lv);
    if(lv!=="sector"&&!opts.length)return "";
    return row(LEVELLBL[lv],`<div class="pick">${tags(lv,q[lv]||[])}</div>`,
      addSelect(lv,opts,"tx"+lv,(q[lv]||[]).length?"+ add":"Choose…"));
  }).join("");

  const cols=SPEC.columns.map(c=>{
    const on=(s.sheet.columns||[]).includes(c.name);
    return `<label class="col" title="${esc(c.note||"")}"><input type="checkbox"
      ${on?"checked":""} onchange="col('${s.id}',${jq(c.name)},this.checked)">
      <span>${esc(c.name)}</span></label>`;
  }).join("");

  const active=(q.filters||[]).map(f=>filterRow(s,f)).join("");

  return `<div class="secbody">
  <div class="grp"><h3>Name</h3><div class="row">
    <div><input value="${esc(s.title)}" oninput="setS('${s.id}','title',this.value)"
      placeholder="e.g. Checking Acquisition">
      <div class="hint">This becomes the slide title.</div></div>
    ${P.deck.section_headings?`<div><input value="${esc(s.heading)}"
      oninput="setS('${s.id}','heading',this.value)" placeholder="Heading, e.g. Deposits">
      <div class="hint">Sections sharing a heading sit behind one divider slide.</div></div>`:""}
  </div></div>

  <div class="grp"><h3>1 · Search</h3>
   <div class="why">What to pull out of the archive for this section.</div>
   ${taxoRows}
   ${row("Channels",`<div class="pick">${tags("media_channel",q.media_channel||[])}</div>`,
     addSelect("media_channel",SPEC.channels,"chanAdd",
       (q.media_channel||[]).length?"+ add":"Choose…"))}
   ${row("Audience",`<div class="pick">${tags("audience",q.audience||[])}</div>`,
     addSelect("audience",SPEC.audiences,"audAdd",
       (q.audience||[]).length?"+ add":"Anyone"))}
   ${row("Companies",`<div class="pick">${tags("company",q.company||[])}
     ${(q.company||[]).length?`<div class="tri sm" title="Exact matches one whole company name. Contains matches every company whose name includes the text.">
       <span class="${q.company_match!=="contains"?"on":""}"
         onclick="setSS('${s.id}','company_match','exact')">Exact</span>
       <span class="${q.company_match==="contains"?"on":""}"
         onclick="setSS('${s.id}','company_match','contains')">Contains</span></div>`:""}
     </div>`,
     `<input class="add" placeholder="Any company"
        value="${esc(FSEARCH[s.id+"|__company"]||"")}"
        oninput="doCoLookup('${s.id}',this.value)"
        onkeydown="if(event.key==='Enter'){event.preventDefault();
          addCompany('${s.id}',this.value)}">
      ${(LOOKRES[s.id+"|__company"]||[]).length?`<div class="lookres">`
        +(LOOKRES[s.id+"|__company"]||[]).map(r=>
          `<div onclick="addCompany('${s.id}',${jq(r)})">${esc(r)}</div>`).join("")
        +`</div>`:""}`)}
   ${row("Words in it",`<div class="pick">${tags("ocr_text",q.ocr_text||[])}
     ${(q.ocr_text||[]).length>1?`<div class="tri sm">
       <span class="${q.ocr_text_match!=="any"?"on":""}"
         onclick="setSS('${s.id}','ocr_text_match','all')">All</span>
       <span class="${q.ocr_text_match==="any"?"on":""}"
         onclick="setSS('${s.id}','ocr_text_match','any')">Any</span></div>`:""}
     </div>`,
     `<input class="add" placeholder="Any wording"
        onkeydown="if(event.key==='Enter'){event.preventDefault();
          add('${s.id}','ocr_text',this.value);this.value=''}"
        title="Up to 5 words or phrases. Punctuation is ignored, so write &quot;pre approved&quot;.">`)}
   ${row(`Filters${(q.filters||[]).length?" ("+(q.filters||[]).length+")":""}`,
     active||`<div class="hint" style="margin:3px 0 0">None yet.</div>`,
     `<button onclick="openPick('${s.id}')">+ Add filter</button>`)}

   <details><summary>More options</summary>
     <div class="row" style="margin-top:6px">
       <div class="f"><label>Max rows per channel</label>
         <input type="number" value="${esc(q.row_cap)}"
           oninput="setSS('${s.id}','row_cap',Number(this.value))"></div>
       <div class="f"><label>Skip companies matching</label>
         <input value="${esc(q.company_must_not_match)}"
           oninput="setSS('${s.id}','company_must_not_match',this.value)"
           placeholder="e.g. Chase"></div>
     </div>
     <label class="check"><input type="checkbox" ${q.collapse_repeats?"checked":""}
       onchange="setSS('${s.id}','collapse_repeats',this.checked)">
       <span>Collapse repeats of the same creative
       <span style="color:#8b93a6">— stops one recycled ad filling the slide</span></span></label>
     ${q.collapse_repeats?`<div class="f" style="max-width:210px">
       <label>At most this many per creative</label>
       <input type="number" value="${esc(q.max_per_creative)}"
         oninput="setSS('${s.id}','max_per_creative',Number(this.value))"></div>`:""}
   </details>

   <div style="margin-top:10px">
     <button onclick="doPreview('${s.id}')">Preview count</button></div>
   ${previewBox(s)}
  </div>

  <div class="grp"><h3>2 · Worksheet
    <label class="check" style="margin:0;font-weight:400"><input type="checkbox"
      ${s.sheet.enabled?"checked":""} onchange="setSH('${s.id}','enabled',this.checked)">
      <span>include</span></label></h3>
   <div class="why">Everything this section found, as rows in the workbook.</div>
   ${s.sheet.enabled?`
   <div class="f"><label>Tab name</label>
     <input value="${esc(s.sheet.tab)}" oninput="setSH('${s.id}','tab',this.value)"
       placeholder="${esc(s.title||"Sheet")}">
     <div class="hint">Give two sections the same tab name to combine them into one
       sheet.</div></div>
   <div class="f"><label>Columns</label><div class="cols">${cols}</div></div>`:""}</div>

  <div class="grp"><h3>3 · Feature on a slide
    <label class="check" style="margin:0;font-weight:400"><input type="checkbox"
      ${s.feature.enabled?"checked":""} onchange="setF('${s.id}','enabled',this.checked)">
      <span>include</span></label></h3>
   <div class="why">Agents read what was found — including the scanned text of each
     piece — pick the best ones, and write the paragraph underneath. Five fit on a
     slide; more roll onto a "(cont.)" slide.</div>
   ${s.feature.enabled?`
   <div class="f" style="max-width:210px"><label>How many pieces</label>
     <input type="number" value="${esc(s.feature.count)}"
       oninput="setF('${s.id}','count',Number(this.value))"></div>
   <div class="f"><label>How to choose them</label>
     <textarea oninput="setF('${s.id}','how_to_choose',this.value)"
       placeholder="e.g. Offers that push someone to open an account online. Skip anything aimed at existing customers.">${esc(s.feature.how_to_choose)}</textarea>
     <div class="hint">Plain English. This is the difference between a good slide and an
       arbitrary one.</div></div>
   <div class="f"><label>What the paragraph should say</label>
     <textarea oninput="setF('${s.id}','what_to_say',this.value)"
       placeholder="e.g. One paragraph naming each company and its specific offer.">${esc(s.feature.what_to_say)}</textarea></div>
   <details><summary>Fine tuning</summary>
     <label class="check"><input type="checkbox" ${s.feature.one_per_company?"checked":""}
       onchange="setF('${s.id}','one_per_company',this.checked)">
       <span>At most one piece per company</span></label>
     <label class="check"><input type="checkbox" ${s.feature.never_reuse?"checked":""}
       onchange="setF('${s.id}','never_reuse',this.checked)">
       <span>Never show a piece another section already used</span></label>
     <label class="check"><input type="checkbox" ${s.feature.mention_total?"checked":""}
       onchange="setF('${s.id}','mention_total',this.checked)">
       <span>State how many pieces were captured
       <span style="color:#8b93a6">— the archive counts them exactly, so the number is
         not a guess</span></span></label>
     <div class="f" style="max-width:210px"><label>Paragraph character limit</label>
       <input type="number" value="${esc(s.feature.callout_limit)}"
         oninput="setF('${s.id}','callout_limit',Number(this.value))">
       <div class="hint">374 is what the slide template holds.</div></div>
   </details>`:""}</div>
  </div>`;
}

/* ── one filter row, rendered by what kind of filter it is ────────────────── */
function fkeys(sp){
  if(!sp)return [];
  return sp.type==="range"?(sp.fields||[sp.field+"_min",sp.field+"_max"]):[sp.field];
}
function filterRow(s,field){
  const sp=FLAT[field];
  if(!sp)return `<div class="flt"><div class="fh">
    <b style="color:#c0392f">${esc(field)}</b>
    <span class="grp2">unknown</span><div class="sp"></div>
    <button class="ghost" onclick="delFilter('${s.id}',${jq(field)})">remove</button></div>
    <div class="note">This archive does not publish a filter by that name any more.</div>
    </div>`;
  const E=s.search.enhanced||{}, note=sp.description||sp.note||"";
  const cost=sp.cost==="expensive"||sp.requires_date_range;
  let ctl="";
  if(sp.type==="boolean"){
    const v=E[field];
    const opt=(lbl,val)=>`<span class="${
      (val===null&&v===undefined)||v===val?"on":""}"
      onclick="setFlag('${s.id}',${jq(field)},${val===null?"null":val})">${lbl}</span>`;
    ctl=`<div class="tri">${opt("Any",null)}${opt("Yes",true)}${opt("No",false)}</div>`;
  }else if(sp.type==="range"){
    const [lo,hi]=fkeys(sp);
    ctl=`<div class="row" style="max-width:330px">
      <input type="number" placeholder="min" value="${E[lo]??""}"
        oninput="setRange('${s.id}',${jq(lo)},this.value)">
      <input type="number" placeholder="max" value="${E[hi]??""}"
        oninput="setRange('${s.id}',${jq(hi)},this.value)"></div>`;
  }else{
    const cur=E[field]||[], opts=sp.options||[];
    const truncated=sp.count&&opts.length<sp.count;
    if(truncated) ctl=vocabLookup(s,field,sp,cur);
    else if(opts.length<=14) ctl=optChips(s,field,opts,cur);
    else ctl=optSearch(s,field,sp,cur);
  }
  return `<div class="flt"><div class="fh">
    <b>${esc(sp.label||field)}</b>
    <span class="grp2">${esc(String(sp.group||"").replace(/_/g," "))}</span>
    ${cost?`<span class="cost">slow</span>`:""}
    <div class="sp" style="flex:1"></div>
    <button class="ghost" onclick="delFilter('${s.id}',${jq(field)})">remove</button>
   </div>${ctl}
   ${note?`<div class="note">${esc(note)}</div>`:""}</div>`;
}
function optChips(s,field,opts,cur){
  return `<div class="chips">`+opts.map(o=>
    `<span class="chip mini${cur.includes(o)?" on":""}"
      onclick="togEnh('${s.id}',${jq(field)},${jq(o)})">${esc(String(o))}</span>`
    ).join("")+`</div>`;
}
function optSearch(s,field,sp,cur){
  const key=s.id+"|"+field, q=(FSEARCH[key]||"").toLowerCase();
  const all=sp.options||[];
  const shown=(q?all.filter(o=>String(o).toLowerCase().includes(q)):all).slice(0,40);
  return `${cur.length?optChips(s,field,cur,cur):""}
    <input placeholder="Filter ${all.length} options…" value="${esc(FSEARCH[key]||"")}"
      oninput="setFS('${s.id}',${jq(field)},this.value)" style="margin-top:5px">
    <div class="chips" style="margin-top:5px">`+shown.filter(o=>!cur.includes(o)).map(o=>
      `<span class="chip mini" onclick="togEnh('${s.id}',${jq(field)},${jq(o)})"
        >${esc(String(o))}</span>`).join("")+`</div>`
    +(shown.length<all.length?`<div class="note">showing ${shown.length} of
      ${all.length} — type to narrow</div>`:"");
}
function vocabLookup(s,field,sp,cur){
  const key=s.id+"|"+field, res=LOOKRES[key]||[];
  return `${cur.length?optChips(s,field,cur,cur):""}
    <div class="lookup" style="margin-top:5px">
      <input placeholder="Search ${sp.count} names…" value="${esc(FSEARCH[key]||"")}"
        oninput="doLookup('${s.id}',${jq(field)},this.value)">
      ${res.length?`<div class="lookres">`+res.map(r=>
        `<div onclick="togEnh('${s.id}',${jq(field)},${jq(r)})">${esc(r)}</div>`
        ).join("")+`</div>`:""}
    </div>
    <div class="note">Too many to list — this searches the archive's own vocabulary.</div>`;
}
"""

HTML += r"""
/* ── state edits ─────────────────────────────────────────────────────────── */
const S=id=>P.sections.find(x=>x.id===id);
function toggle(id){OPEN[id]=!OPEN[id];renderSections()}
function setS(id,k,v){S(id)[k]=v;check();bump(id)}
function bump(id){
  const i=P.sections.findIndex(x=>x.id===id);
  const el=document.querySelectorAll(".sec")[i];
  if(el){const n=el.querySelector(".name");if(n)n.textContent=S(id).title||"(untitled)"}
}
function setSS(id,k,v){S(id).search[k]=v;check();
  if(typeof v==="boolean"||k==="company_match"||k==="ocr_text_match")renderSections()}
function setSH(id,k,v){S(id).sheet[k]=v;if(k==="enabled")renderSections();check()}
function setF(id,k,v){S(id).feature[k]=v;if(typeof v==="boolean")renderSections();check()}
/* Each taxonomy level gets its own handler, so a dropdown can name the level it
   belongs to without threading it through every call site. */
LEVELS.forEach(lv=>{window["tx"+lv]=(id,v)=>taxoTog(id,lv,v)});
function chanAdd(id,v){add(id,"media_channel",v)}
function audAdd(id,v){add(id,"audience",v)}
function col(id,name,on){
  const a=S(id).sheet.columns,i=a.indexOf(name),order=SPEC.columns.map(c=>c.name);
  if(on&&i<0){a.push(name);a.sort((x,y)=>order.indexOf(x)-order.indexOf(y))}
  else if(!on&&i>=0)a.splice(i,1);
  renderSections();check();
}

/* Flags are tri-state: "Any" DELETES the key, because omitting is the archive's default
   and false is a real filter that matches only pieces recorded as not carrying it. */
function setFlag(id,field,val){
  const E=S(id).search.enhanced;
  if(val===null)delete E[field];else E[field]=val;
  renderSections();check();
}
function setRange(id,key,val){
  const E=S(id).search.enhanced;
  if(val===""||val===null)delete E[key];else E[key]=Number(val);
  check();
}
function togEnh(id,field,val){
  const E=S(id).search.enhanced;
  const a=E[field]||(E[field]=[]);
  const i=a.indexOf(val); i<0?a.push(val):a.splice(i,1);
  if(!a.length)delete E[field];
  renderSections();check();
}
function setFS(id,field,v){FSEARCH[id+"|"+field]=v;renderSections()}
function add(id,key,v){
  v=String(v||"").trim();
  if(!v)return;
  const a=S(id).search[key]||(S(id).search[key]=[]);
  if(!a.includes(v))a.push(v);
  renderSections();check();
}
function drop(id,key,v){
  const q=S(id).search;
  q[key]=(q[key]||[]).filter(x=>x!==v);
  /* a sector that is gone cannot leave its categories behind */
  if(LEVELS.includes(key)){
    for(let k=LEVELS.indexOf(key)+1;k<LEVELS.length;k++){
      const ok=new Set(taxoOpts(S(id),LEVELS[k]));
      q[LEVELS[k]]=(q[LEVELS[k]]||[]).filter(x=>ok.has(x));
    }
  }
  renderSections();check();
}
function addCompany(id,name){
  name=String(name||"").trim();
  if(!name)return;
  const a=S(id).search.company;
  if(!a.includes(name))a.push(name);
  FSEARCH[id+"|__company"]="";LOOKRES[id+"|__company"]=[];
  renderSections();check();
}
function delFilter(id,field){
  const s=S(id);
  s.search.filters=(s.search.filters||[]).filter(x=>x!==field);
  for(const k of fkeys(FLAT[field]))delete s.search.enhanced[k];
  renderSections();check();
}

/* ── vocabulary lookups ──────────────────────────────────────────────────── */
function lookup(key,field,q,cb){
  FSEARCH[key]=q;
  clearTimeout(lookDeb[key]);
  if(q.trim().length<2){LOOKRES[key]=[];renderSections();return}
  lookDeb[key]=setTimeout(async()=>{
    try{
      const d=await (await fetch(`/api/lookup?field=${encodeURIComponent(field)}&q=`
        +encodeURIComponent(q))).json();
      LOOKRES[key]=cb(d);
    }catch(e){LOOKRES[key]=[]}
    renderSections();
  },260);
  renderSections();
}
function doLookup(id,field,q){
  lookup(id+"|"+field,field,q,d=>(d.matches||[]).map(m=>
    m[field]||m.name||m.value||Object.values(m).find(v=>typeof v==="string")).filter(Boolean));
}
function doCoLookup(id,q){
  lookup(id+"|__company","company",q,d=>(d.matches||[]).map(m=>m.company).filter(Boolean));
}

/* ── the filter picker ───────────────────────────────────────────────────── */
const KINDLBL={boolean:"flag","multi-select":"select",range:"range"};
function openPick(id){
  PICKFOR=id;$("#pickSearch").value="";renderPick();show("Pick");
  setTimeout(()=>$("#pickSearch").focus(),50);
}
function renderPick(){
  const q=($("#pickSearch").value||"").toLowerCase().trim();
  const have=new Set((S(PICKFOR).search.filters)||[]);
  const groups={};
  for(const sp of Object.values(FLAT)){
    const hay=(sp.field+" "+(sp.label||"")+" "+sp.group+" "
      +(sp.description||sp.note||"")+" "+(sp.options||[]).join(" ")).toLowerCase();
    if(q&&!hay.includes(q))continue;
    (groups[sp.group]=groups[sp.group]||[]).push(sp);
  }
  const shown=Object.values(groups).reduce((n,a)=>n+a.length,0);
  $("#pickCount").textContent=`${shown} of ${Object.keys(FLAT).length}`;
  $("#pickList").innerHTML=Object.keys(groups).sort().map(g=>
    `<div class="pickgrp">${esc(g.replace(/_/g," "))}</div>`
    +groups[g].sort((a,b)=>(a.label||a.field).localeCompare(b.label||b.field)).map(sp=>{
      const on=have.has(sp.field);
      return `<div class="pickitem${on?" has":""}"
        ${on?"":`onclick="addFilter(${jq(sp.field)})"`}>
        <div><div>${esc(sp.label||sp.field)}${on?" — already added":""}</div>
          <code>${esc(sp.field)}${sp.count?" · "+sp.count+" values":""}</code></div>
        <span class="k">${KINDLBL[sp.type]||sp.type}</span></div>`;
    }).join("")).join("")
    ||`<div class="hint">Nothing matches "${esc(q)}".</div>`;
}
function addFilter(field){
  const q=S(PICKFOR).search;
  q.filters=q.filters||[];
  if(!q.filters.includes(field))q.filters.push(field);
  hide("Pick");renderSections();check();
}

/* ── preview: the same body the pipeline will send, counted for real ─────── */
async function doPreview(id){
  PVW[id]={loading:true};renderSections();
  try{
    const r=await fetch("/api/preview",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({project:P,section_id:id})});
    PVW[id]=await r.json();
  }catch(e){PVW[id]={error:String(e)}}
  renderSections();
}
function previewBox(s){
  const v=PVW[s.id];
  if(!v)return "";
  if(v.loading)return `<div class="pvw">counting…</div>`;
  if(v.error)return `<div class="pvw"><span style="color:#ff9a90">${esc(v.error)}</span></div>`;
  const L=[];
  L.push(`${v.date_field}  ${v.start} .. ${v.end}`);
  L.push("");
  for(const c of (v.channels||[])){
    if(c.error){
      L.push(`  ${c.channel.padEnd(24)} !! ${c.error} — ${c.message}`);
    }else{
      L.push(`  ${c.channel.padEnd(24)} ${String(c.total).padStart(7)}`
        +(c.capped?"  (CAPPED — a lower bound, slice the window)":"")
        +`   ${c.took_ms}ms${c.cached?" cached":""}`);
    }
  }
  L.push("");
  L.push(`  ${"TOTAL".padEnd(24)} ${String(v.total).padStart(7)}`
    +(v.any_capped?"  (at least — one channel was capped)":"  exact"));
  const over=(v.channels||[]).filter(c=>c.total>v.row_cap).length;
  if(over)L.push(`  ${over} channel(s) hold more than this section's row cap of `
    +`${v.row_cap}, so the run will fetch fewer rows than the totals above. The `
    +`totals are still what a write-up states.`);
  const r=v.resolved||{};
  const nonEmpty=x=>Array.isArray(x)?x.length>0:!!x;   /* [] is truthy in JS */
  const ids=Object.entries(r).filter(([k,x])=>nonEmpty(x)&&k.endsWith("_ids")
    &&k!=="taxonomy_ids_queried"&&k!=="media_channel_ids");
  if(ids.length||r.enhanced){
    L.push("");
    L.push("  resolved — check this before trusting the count:");
    for(const [k,x] of ids)L.push(`    ${k.replace(/_ids$/,"")} = ${JSON.stringify(x)}`);
    if(r.enhanced)L.push(`    enhanced = ${JSON.stringify(r.enhanced)}`);
    if(r.company_names)L.push(`    companies = ${JSON.stringify(r.company_names)}`);
  }
  L.push("");
  L.push(`  ${v.spent} request(s) spent.`);
  /* element text, not an attribute: escaping " would render it as &quot; */
  const escText=t=>String(t).replace(/&/g,"&amp;").replace(/</g,"&lt;");
  return `<div class="pvw">${escText(L.join("\n"))}</div>`;
}

/* ── section list plumbing ───────────────────────────────────────────────── */
async function addSection(){
  const d=await (await fetch("/api/section")).json();
  P.sections.push(d.section);OPEN[d.section.id]=true;renderSections();check();
}
function delSection(id){
  if(!confirm("Remove this section?"))return;
  P.sections=P.sections.filter(x=>x.id!==id);delete PVW[id];renderSections();check();
}
function move(id,d){
  const i=P.sections.findIndex(x=>x.id===id),j=i+d;
  if(j<0||j>=P.sections.length)return;
  [P.sections[i],P.sections[j]]=[P.sections[j],P.sections[i]];
  renderSections();check();
}

/* ── checking, continuously ──────────────────────────────────────────────── */
function check(){
  clearTimeout(deb);
  deb=setTimeout(async()=>{
    const r=await fetch("/api/check",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({project:P})});
    const d=await r.json();
    CHK=d;ISSUES=d.issues||[];
    const g=[];
    g.push(d.errors?`<span class="pill err">${d.errors} to fix</span>`
      :`<span class="pill ok">ready</span>`);
    if(d.warnings)g.push(`<span class="pill wr">${d.warnings} to look at</span>`);
    $("#health").innerHTML=g.join("");
    renderBadge();
    renderEstimate();
    /* One container, always assigned. Anything that only ADDS a node has to have an
       equally reliable way of taking it away, and the old version did not: it left the
       last message it rendered on screen even once the issue was gone. */
    $("#gen").innerHTML=ISSUES.filter(x=>!x.section).map(m=>
      `<div class="msg ${m.level==="error"?"error":"warn"}">${esc(m.msg)}</div>`).join("");
    renderSections();
  },240);
}

/* The cost of one run, in front of the researcher BEFORE they press Run rather than
   in the log afterwards. Nothing injects a smaller limit any more, so a wide report
   really does spend this. */
function renderEstimate(){
  const n=CHK.api_calls||0;
  const w=CHK.window||{};
  const dates=w.start?`${w.start} .. ${w.end}`:"";
  $("#est").innerHTML=`<span class="pill dim" title="Two requests per section x channel:`
    +` one to count, one to fetch. Every request counts against the monthly quota.">`
    +`${n} archive requests</span>`
    +(dates?` <span title="The window this run will cover">${esc(dates)}</span>`:"");
}

function renderBadge(){
  const b=CHK.badge||{state:"draft",label:"Draft",tone:"dim",detail:""};
  $("#badge").innerHTML=`<span class="badge ${b.tone}" title="${esc(b.detail)}">`
    +`${esc(b.label)}</span>`;
}

/* ── the output terminal ─────────────────────────────────────────────────── */
function log(t,cls){
  const el=$("#log");
  el.insertAdjacentHTML("beforeend",`<span class="${cls||""}">${esc(t)}</span>\n`);
  el.scrollTop=el.scrollHeight;
}
function clearLog(){$("#log").innerHTML=""}
function cls(line){
  if(/^ERROR|RUNNER ERROR|Traceback|^\s*!!/.test(line))return "e";
  if(/^\s*!|SUSPECT|WARN/.test(line))return "w";
  if(/^\s*(Deck|Excel|state):|saved |^Done\.|sent \(|^Paused/.test(line))return "o";
  if(/^\$ |^Step |^── /.test(line))return "d";
  return "";
}
/* The terminal starts minimised on every page load, whatever it was left as: the
   page opens on the report, not on a transcript of the last run. It is never
   minimised while a run is starting — output must not be hidden behind a panel
   nobody meant to leave closed — so runNow() and friends call openLog(). Deliberately
   NOT remembered across loads; the toggle only lasts as long as the page. */
function setLog(min){
  $("#log").classList.toggle("min",min);
  $("#logTog").textContent=min?"show":"minimise";
}
function toggleLog(){setLog(!$("#log").classList.contains("min"))}
function openLog(){setLog(false)}
function restoreLog(){setLog(true)}

/* ── running ─────────────────────────────────────────────────────────────── */
function modeChanged(){
  const key=$("#mode").value;
  const m=(SPEC.modes||[]).find(x=>x.key===key)||{};
  $("#modeHelp").textContent=m.help||"";
  $("#modeHelp").title=m.help||"";
  /* Only the two modes that produce deliverables ever reach the email step. Offering
     the box on the other two would invite a researcher to fill in something that
     silently does nothing. */
  const canEmail=key==="curate"||key==="full";
  $("#runEmail").style.display=canEmail?"":"none";
  if(!canEmail)$("#runEmail").value="";
  try{localStorage.setItem("rs.mode",key)}catch(e){}
}
function running(on,label){
  $("#runBtn").disabled=on;
  $("#runBtn").innerHTML=on?`<span class="spin"></span> ${esc(label||"Running")}`:"Run";
  $("#stopBtn").style.display=on?"":"none";
  $("#mode").disabled=on;
}
function elapsed(){
  if(!T0)return "";
  const s=Math.round((Date.now()-T0)/1000);
  return s<60?`${s}s`:`${Math.floor(s/60)}m ${s%60}s`;
}

async function runNow(){
  if(ISSUES.some(x=>x.level==="error")){
    openLog();log("Fix the errors listed on the left before running.","e");return;
  }
  const mode=$("#mode").value;
  const n=CHK.api_calls||0;
  if(n>40&&!confirm(`This report makes about ${n} archive requests every time it runs,`
    +` and no limit is applied. That is real quota and it will take a while.\n\n`
    +`Run it anyway?`))return;
  const email=($("#runEmail").value||"").trim();
  openLog();clearLog();clearFiles();
  killPanel();PSTATE={};PANEL=null;BUILT=null;BUILDING=false;
  THUMBS={};THUMBWARNED=false;
  THUMBTRY={};clearTimeout(thumbRetry);thumbRetry=null;
  thumbAgain=false;FLASH="";
  T0=Date.now();running(true,"Running");
  $("#logstate").textContent="";
  log(`${(SPEC.modes.find(m=>m.key===mode)||{}).label} — generating the pipeline and`
    +` running that file…`,"d");
  const r=await fetch("/api/run",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({project:P,mode:mode,email_to:email})});
  const d=await r.json();
  if(d.error){log("ERROR "+d.error,"e");running(false);return}
  RUNID=d.run_id;
  /* The address went to one child process and is held nowhere else. Clearing the box
     is the visible half of that promise; reloading the project shows the other. */
  if(email)$("#runEmail").value="";
  watch(0);
}

async function stopNow(){
  const r=await fetch("/api/run/stop",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({run_id:RUNID})});
  const d=await r.json();
  if(d.error)log("ERROR "+d.error,"e");
}

function watch(seen){
  clearInterval(poll);
  poll=setInterval(async()=>{
    let s;
    try{s=await (await fetch("/api/run/status?id="+RUNID)).json()}catch(e){return}
    for(const line of (s.lines||[]).slice(seen))log(line,cls(line));
    seen=(s.lines||[]).length;
    $("#logstate").textContent=s.done?"":`· running ${elapsed()}`;
    if(!s.done)return;
    clearInterval(poll);
    running(false);
    $("#logstate").textContent="";
    log("","");
    if(s.stopped){
      log("Stopped.","w");
    }else if(s.paused){
      log("Paused for review — the picks are in the panel on the right.","o");
    }else{
      log(s.rc===0?`Finished cleanly in ${elapsed()}.`
        :`Exited with code ${s.rc}.`,s.rc===0?"o":"e");
    }
    remember(s);
    /* A paused run has only written half of what it will write, so the bar says the
       workbook is here and the deck is not yet — rather than looking like the run
       finished with a file missing. */
    showFiles(s.files||[],RUNID,
      s.paused?"Written so far — the deck follows once you confirm the picks."
      :(s.record&&s.record.emailed?"Emailed too. Kept under History for this report."
        :"Kept under History for this report."));
    /* The build is over, however it ended: the grey pass comes off and the banner
       stops saying the deck is being written. The slate itself stays on screen. */
    BUILDING=false;
    /* The panel carries its own copy of the list. After the build half of a curated
       run it is the pause-time list, which is now short by a deck. */
    if(PANEL){PANEL.files=s.files||[];renderPanel()}
    /* Only the mode that stops for review has a review to show. The other three were
       leaving a RESULTS strip on the edge of the window that opened onto a list
       nobody had asked to see. */
    if(s.paused)loadPanel();
  },700);
}

/* Run history lives on the project, so it comes back when the project is reopened.
   It records what was produced, never who it was emailed to. */
function remember(s){
  if(!s.record)return;
  P.status=P.status||{sent:null,runs:[],saved_as:""};
  P.status.runs=(P.status.runs||[]).filter(r=>r.id!==s.record.id);
  P.status.runs.push({id:s.record.id,mode:s.record.mode,at:s.record.at,
    rc:s.record.rc,stopped:s.record.stopped,emailed:!!s.record.emailed,
    produced:s.record.produced||[]});
  P.status.runs=P.status.runs.slice(-8);
  check();
  if(P.status.saved_as)silentSave();
}
async function silentSave(){
  try{
    await fetch("/api/projects/save",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({project:P})});
  }catch(e){}
}

/* ── the deliverables bar ────────────────────────────────────────────────── */
/* What a run made, somewhere it can be clicked.

   Every mode that writes a file ends up here, not only the one that pauses for
   review: the panel opens on two of the four modes, and the log is text. Before this,
   the deck from "Run the pipeline" existed on disk and in an email and nowhere a
   researcher could reach without asking Engineering. The bar outlives clearing the
   log, and is emptied when the next run starts so it never shows the last run's deck
   beside this run's output. */
const KIND={pptx:"deck",ppt:"deck",xlsx:"workbook",xls:"workbook",docx:"document",
  doc:"document",csv:"csv",json:"json",pdf:"pdf",txt:"text",md:"notes",png:"image",
  jpg:"image",jpeg:"image",zip:"archive"};
function fileSize(n){
  n=Number(n)||0;
  if(n<1024)return `${n} B`;
  if(n<1048576)return `${Math.round(n/1024)} KB`;
  return `${(n/1048576).toFixed(1)} MB`;
}
function fileLink(runId,name,size,kind){
  const k=KIND[String(kind||name.split(".").pop()||"").toLowerCase()];
  return `<a href="/api/run/file?id=${encodeURIComponent(runId)}`
    +`&name=${encodeURIComponent(name)}" download title="${esc(name)}">`
    +(k?`<span class="k">${esc(k)}</span>`:"")+esc(name)
    +(size?`<span class="sz">${fileSize(size)}</span>`:"")+`</a>`;
}
function zipLink(runId,n,cls){
  return `<a class="${cls||""}" href="/api/run/zip?id=${encodeURIComponent(runId)}"
    download title="All ${n} files in one zip">Download all (${n})</a>`;
}
/* Open, folded away, and gone — the same three states the results panel has, so the
   two strips on the right edge behave the same way as each other. */
function openDeliv(){
  $("#deliv").classList.remove("hide");
  $("#delivTab").classList.remove("ready");$("#delivTab").classList.add("show");
}
function hideDeliv(){
  $("#deliv").classList.add("hide");$("#delivTab").classList.add("show");
}
function toggleDeliv(){
  if($("#deliv").classList.contains("hide"))openDeliv();else hideDeliv();
}
function clearFiles(){
  $("#deliv").innerHTML="";$("#deliv").classList.add("hide");
  $("#delivTab").classList.remove("show","ready");
}
/* Reopening a report puts its last run's files back on the shelf.

   The run history has held them all along, but only behind a modal nobody opens on
   the way past — and the common ask is the obvious one: the deck from the last run,
   now. Newest first, and the first run whose files are still on disk wins; older ones
   are pruned, so a hit is not guaranteed and no message is shown when there is none. */
async function restoreFiles(){
  clearFiles();
  const runs=((P.status||{}).runs||[]).slice().reverse();
  for(const r of runs){
    if(!(r.produced||[]).length)continue;
    let files=[];
    try{
      files=(await (await fetch("/api/run/files?id="
        +encodeURIComponent(r.id))).json()).files||[];
    }catch(e){continue}
    if(!files.length)continue;
    const label=((SPEC.modes||[]).find(m=>m.key===r.mode)||{}).label||r.mode;
    showFiles(files,r.id,`From ${label} on `
      +`${String(r.at||"").replace("T"," ").slice(0,16)}.`,true);
    return;
  }
}
function showFiles(files,runId,note,quiet){
  const el=$("#deliv");
  files=files||[];
  if(!files.length){clearFiles();return}
  const H=[`<div class="dhead">Deliverables<span class="sp"></span>
    <button class="ghost" onclick="hideDeliv()">hide</button></div>`];
  if(note)H.push(`<div class="note">${esc(note)}</div>`);
  if(files.length>1)H.push(zipLink(runId,files.length,"all"));
  for(const f of files)H.push(fileLink(runId,f.name,f.size,f.kind));
  el.innerHTML=H.join("");
  /* A run that just finished opens the drawer; one restored with the project only
     lights the strip, so reopening a report does not shove the window sideways for
     files nobody asked for yet. */
  if(quiet){
    el.classList.add("hide");
    $("#delivTab").classList.add("show","ready");
    return;
  }
  openDeliv();
  /* Still named in the transcript, so a log someone copies out still says what the
     run produced — but the transcript is no longer the only way to get at it. */
  log("","");
  for(const f of files)log(`  ${f.name}  (${fileSize(f.size)})`,"o");
}

/* ── the results panel ───────────────────────────────────────────────────── */
/* More pictures as the list is scrolled, rather than all of them at once. */
function panelScrolled(){
  clearTimeout(thumbDeb);
  thumbDeb=setTimeout(wantThumbs,180);
}
let thumbDeb=null;
function togglePanel(){
  if($("#panel").classList.contains("hide"))openPanel();else hidePanel();
}
/* Folded away, with the strip left behind to bring it back. */
function hidePanel(){
  $("#panel").classList.add("hide");$("#panelTab").classList.add("show");
  /* Full width borrowed the pane and the stage. Closing the panel gives them back,
     otherwise hiding it would leave an empty window. */
  panelFull(false);
}
function openPanel(){
  $("#panel").classList.remove("hide");$("#panelTab").classList.remove("show");
}
/* Gone entirely, strip included — there are no results behind it to go back to. */
function killPanel(){
  $("#panel").classList.add("hide");$("#panelTab").classList.remove("show");
  $("#panel").innerHTML="";panelFull(false);
}
/* The results panel taking over the whole window, and giving it back. */
function panelFull(on){
  $("#panel").classList.toggle("full",!!on);
  $("#body").classList.toggle("panelfull",!!on);
}
function togglePanelFull(){
  panelFull(!$("#panel").classList.contains("full"));
  renderPanel();
}
/* The report's settings pane folds away like everything else in this window. */
function togglePane(){
  if($("#pane").classList.contains("hide"))openPane();else hidePane();
}
function hidePane(){
  $("#pane").classList.add("hide");$("#paneTab").classList.add("show");
}
function openPane(){
  $("#pane").classList.remove("hide");$("#paneTab").classList.remove("show");
}
async function loadPanel(){
  const d=await (await fetch("/api/run/panel?id="+RUNID)).json();
  if(d.error){log("Results panel: "+d.error,"w");return}
  PANEL=d;
  /* Reaching the pause means this is a fresh review, so nothing built earlier is
     still standing. */
  BUILT=null;BUILDING=false;
  /* One slate per featured section, seeded with what the pipeline picked. Every id in
     it has to be resolved before the deck can be built. */
  PSTATE={};
  for(const sec of d.sections){
    if(!sec.feature)continue;
    PSTATE[sec.id]={slate:sec.picks.slice(),ok:{},rejected:[],exhausted:""};
  }
  openPanel();renderPanel();
  wantThumbs();
}
function pickedElsewhere(sid){
  const out=[];
  for(const k of Object.keys(PSTATE))
    if(k!==sid)for(const c of PSTATE[k].slate)out.push(c.entry_id);
  return out;
}
/* The picture of the piece.

   Only "none" — the archive's own answer that it holds no cover image — is allowed to
   print "no image on file". A failed fetch says so instead and offers the retry,
   because it is a statement about this attempt and not about the piece.

   An <img> the browser cannot draw is treated the same way. That covers a file
   truncated on disk and a cache cleared underneath a panel still open on it. */
function thumbCell(c){
  const e=c.entry_id;
  const st=THUMBS[e];
  if(st==="ok"){
    const u=`/api/thumb?id=${encodeURIComponent(RUNID)}&entry_id=${encodeURIComponent(e)}`
      +`&v=${THUMBTRY[e]||0}`;
    return `<div class="thumb" onclick="zoom('${esc(u)}')" title="click to enlarge">
      <img src="${esc(u)}" alt="${esc(e)}" loading="lazy"
        onerror="thumbBroke('${esc(e)}')"></div>`;
  }
  if(st==="none")
    return `<div class="thumb"><span class="none">no image<br>on file</span></div>`;
  if(st==="retry")
    return `<div class="thumb bad" onclick="retryThumb('${esc(e)}')"
      title="The picture did not load. This says nothing about whether the archive has
one — click to try again.">
      <span class="none">couldn’t load<br><b>retry</b></span></div>`;
  return `<div class="thumb"><span class="none">…</span></div>`;
}
/* The file was there and the browser could not draw it. Drop the verdict, ask again. */
function thumbBroke(e){
  if(THUMBS[e]!=="ok")return;
  THUMBS[e]="retry";
  renderPanel();
  scheduleThumbRetry(1200);
}
function retryThumb(e){
  delete THUMBS[e];THUMBTRY[e]=0;
  clearTimeout(thumbRetry);thumbRetry=null;
  renderPanel();wantThumbs();
}
/* Every piece on screen the panel is still unsure about, and has tries left for. */
function thumbsPending(){
  const out=[];
  for(const sec of (PANEL?PANEL.sections:[])){
    const st=PSTATE[sec.id];
    const list=st?st.slate:sec.pieces.slice(0,THUMB_PAGE);
    for(const c of list)
      if(c&&c.entry_id&&THUMBS[c.entry_id]==="retry"
        &&(THUMBTRY[c.entry_id]||0)<THUMB_TRIES)out.push(c.entry_id);
  }
  return out;
}
/* Ask again, backing off. A tunnel that comes up a few seconds after the run ends
   fills the panel in on its own, with nobody clicking anything. */
function scheduleThumbRetry(ms){
  if(thumbRetry)return;
  if(!thumbsPending().length)return;
  thumbRetry=setTimeout(()=>{
    thumbRetry=null;
    for(const e of thumbsPending())delete THUMBS[e];
    wantThumbs();
  },ms);
}
/* The manual version, for when the tries ran out. */
function retryAllThumbs(){
  for(const e of Object.keys(THUMBS))
    if(THUMBS[e]==="retry"){delete THUMBS[e];THUMBTRY[e]=0}
  clearTimeout(thumbRetry);thumbRetry=null;
  THUMBWARNED=false;
  renderPanel();wantThumbs();
}
/* How many pictures on screen are missing because a fetch failed. */
function thumbTrouble(){
  let n=0;
  for(const sec of (PANEL?PANEL.sections:[])){
    const st=PSTATE[sec.id];
    const list=st?st.slate:sec.pieces.slice(0,THUMB_PAGE);
    for(const c of list)if(c&&THUMBS[c.entry_id]==="retry")n++;
  }
  return n;
}
function pieceRow(c,sid){
  const st=sid?PSTATE[sid]:null;
  const dec=st?st.ok[c.entry_id]:undefined;
  const link=c.pdf_url
    ?`<a href="${esc(c.pdf_url)}" target="_blank" rel="noopener">PDF</a>`
    :`<span title="A link needs the archive's internal product id, which this run did
not fetch for this section.">no link</span>`;
  /* What this piece replaced, if anything — so a swap is never a one-way door. */
  const undo=st&&st.swapped&&st.swapped[c.entry_id]
    ?`<div class="swapped">swapped in for
        <b>${esc(st.swapped[c.entry_id].company||st.swapped[c.entry_id].entry_id)}</b>
        <button onclick="unswap('${sid}','${esc(c.entry_id)}')"
          title="Put the original piece back">put it back</button></div>`:"";
  return `<div class="piece" data-eid="${esc(c.entry_id)}">
    ${thumbCell(c)}
    <div class="meta">
      <span class="eid" onclick="copy('${esc(c.entry_id)}')"
        title="click to copy">${esc(c.entry_id)}</span>
      <span class="co"> ${esc(c.company||"?")}</span>
      <span style="color:#8b93a6"> · ${esc(c.channel||"?")} · ${esc(c.date||"")}
        · ${link}</span>
      <div class="hl">${esc(c.headline||c.product||"")}</div>
      ${undo}
    </div>
    ${st?`<div class="yn">
      <button class="${dec===true?"on":""}" onclick="approve('${sid}','${esc(c.entry_id)}')"
        title="Keep this piece">Keep</button>
      <button class="no" onclick="reject('${sid}','${esc(c.entry_id)}')"
        title="Swap it for the next valid candidate the pipeline offers">Swap</button>
      <button onclick="openById('${sid}','${esc(c.entry_id)}')"
        title="Name the piece you want instead, by entry_id — the one you found in the
workbook. You see it before it goes on the slide.">By ID</button></div>`:""}
  </div>`;
}
/* ── naming the piece you want ─────────────────────────────────────────────────

   Deliberately alongside Swap rather than instead of it. Swap is the fast judgement
   — this one is wrong, give me the next one — and it is untouched. This is the other
   half: the researcher has already found the piece they want in the workbook and
   wants THAT piece, not the next one in the pipeline's order.

   It shows the piece before committing it, which is the whole reason it is a dialog.
   An entry_id is not something a person can verify by re-reading it — 4559 and 4595
   look alike and mean different companies — but a cover image and a headline are.
   Fetching the picture during the preview also means the row on the slate has it the
   instant the swap lands, instead of showing a placeholder while S3 is asked. */
let BYID={sid:"",eid:"",out:null,val:"",err:"",found:null,busy:false};
let byIdDeb=null;

function openById(sid,eid){
  const st=PSTATE[sid];
  const out=(st.slate||[]).find(c=>c.entry_id===eid)||null;
  if(!out)return;
  BYID={sid:sid,eid:eid,out:out,val:"",err:"",found:null,busy:false};
  clearTimeout(byIdDeb);byIdDeb=null;
  renderById();
  show("ById");
  const box=$("#byIdEid");
  if(box)box.focus();
}
function closeById(){
  clearTimeout(byIdDeb);byIdDeb=null;
  BYID={sid:"",eid:"",out:null,val:"",err:"",found:null,busy:false};
  hide("ById");
}
/* Typed into rather than pasted-and-submitted, so the lookup is debounced and only
   runs on something that could actually be an entry_id. */
function byIdTyped(v){
  BYID.val=v;
  BYID.found=null;BYID.err="";
  clearTimeout(byIdDeb);
  const want=String(v||"").trim();
  BYID.busy=want.length>=6;
  renderByIdResult();
  if(!BYID.busy)return;
  byIdDeb=setTimeout(byIdLookup,320);
}
async function byIdLookup(){
  const want=String(BYID.val||"").trim();
  if(!want)return;
  const sid=BYID.sid,eid=BYID.eid;
  /* Answered without asking the Studio, because these two are about the slate in
     front of us and not about the archive. */
  if(want===eid){
    BYID.busy=false;BYID.err="That is the piece already in this slot.";
    return renderByIdResult();
  }
  if((PSTATE[sid].slate||[]).some(c=>c.entry_id===want)){
    BYID.busy=false;
    BYID.err=`${want} is already on this slate —\u00a0it cannot be on the slide twice.`;
    return renderByIdResult();
  }
  let d;
  try{
    const r=await fetch("/api/run/pick",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({run_id:RUNID,section:sid,entry_id:want})});
    d=await r.json();
  }catch(e){
    d={error:"The Studio did not answer. Try again."};
  }
  /* The box moved on while this was in flight — the answer is about an id nobody is
     looking at any more. */
  if(String(BYID.val||"").trim()!==want||BYID.sid!==sid)return;
  BYID.busy=false;
  if(d.error||!d.card){BYID.err=d.error||"not found";BYID.found=null}
  else{BYID.err="";BYID.found=d.card}
  renderByIdResult();
  /* Warm the picture now, so the slate row is complete the moment it lands. */
  if(BYID.found&&THUMBS[BYID.found.entry_id]===undefined)byIdThumb(BYID.found.entry_id);
}
async function byIdThumb(e){
  try{
    const r=await fetch("/api/thumbs",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({run_id:RUNID,entry_ids:[e]})});
    const d=await r.json();
    THUMBTRY[e]=(THUMBTRY[e]||0)+1;
    THUMBS[e]=(d.thumbs||{})[e]?"ok":((d.missing||[]).includes(e)?"none":"retry");
  }catch(err){/* the panel's own retry pass will pick it up */}
  if(BYID.found&&BYID.found.entry_id===e)renderByIdResult();
}
/* Built once, when the dialog opens. The <input> is deliberately not part of what
   gets redrawn as somebody types: re-creating the element on every keystroke means
   putting the caret back by hand, and any such attempt sends it to the end of the
   line, so correcting the middle of a pasted id becomes impossible. Only the answer
   underneath changes, and it is a separate element for exactly that reason. */
function renderById(){
  const b=$("#byIdBody");
  if(!b||!BYID.out)return;
  const sec=(PANEL&&PANEL.sections.find(x=>x.id===BYID.sid))||{};
  const slate=(PSTATE[BYID.sid]||{}).slate||[];
  const n=slate.findIndex(c=>c.entry_id===BYID.eid)+1;
  $("#byIdWhere").textContent=`${sec.title||""}${n?` — slot ${n} of ${slate.length}`:""}`;
  b.innerHTML=`
    <div class="byidlbl">Out — this leaves the slide</div>
    <div class="byidrow out">${pieceRow(BYID.out,null)}</div>
    <div class="byidarrow">replace it with</div>
    <div class="f"><label>entry_id of the piece you want</label>
      <input id="byIdEid" value="${esc(BYID.val)}" autocomplete="off" spellcheck="false"
        placeholder="e.g. 2026-07-31-4074"
        oninput="byIdTyped(this.value)"
        onkeydown="byIdKey(event)"></div>
    <div class="hint">It has to be a piece this run already fetched for
      <b>${esc(sec.title||"this section")}</b> — the deck is built from the run's own
      records, so an id from anywhere else would be dropped without a word. Copy one
      out of the workbook, or click any entry_id in the results panel.</div>
    <div id="byIdRes"></div>`;
  renderByIdResult();
}
/* The half that changes while typing: the piece, the refusal, or the prompt. */
function renderByIdResult(){
  const r=$("#byIdRes");
  if(!r)return;
  let res;
  if(BYID.busy)res=`<div class="byidmsg idle"><span class="spin"></span>
    looking it up…</div>`;
  else if(BYID.err)res=`<div class="byidmsg bad">${esc(BYID.err)}</div>`;
  else if(BYID.found){
    const warn=pickWarnings(BYID.sid,BYID.found,BYID.out)
      .map(w=>`<div class="byidwarn">! ${esc(w)}</div>`).join("");
    res=`<div class="byidlbl">In — this goes on the slide</div>
      <div class="byidrow in">${pieceRow(BYID.found,null)}</div>${warn}`;
  }
  else res=`<div class="byidmsg idle">Paste or type the entry_id and the piece appears
    here, so you can check it is the one you meant before it goes on the slide.</div>`;
  r.innerHTML=res;
  $("#byIdGo").disabled=!BYID.found;
}
function byIdKey(ev){
  if(ev.key==="Enter"){
    ev.preventDefault();
    if(BYID.found)useById();
  }else if(ev.key==="Escape"){
    ev.preventDefault();closeById();
  }
}
/* Commit. The displaced piece is remembered exactly the way a swap remembers it, so
   "put it back" works the same and a valid-but-wrong id is not a one-way door. It is
   NOT added to the rejected list: it was displaced by a choice about another piece,
   not judged unfit, so a later swap may legitimately offer it again. */
function useById(){
  const sid=BYID.sid,card=BYID.found;
  if(!card)return;
  const st=PSTATE[sid];
  const i=st.slate.findIndex(c=>c.entry_id===BYID.eid);
  if(i<0)return closeById();
  const original=st.slate[i];
  st.slate[i]=card;
  st.swapped=st.swapped||{};
  st.swapped[card.entry_id]=original;
  /* An explicit pick overrides an earlier rejection of the same piece. Leaving it on
     the rejected list would be a contradiction the next swap would act on. */
  st.rejected=st.rejected.filter(x=>x!==card.entry_id);
  delete st.ok[card.entry_id];
  st.exhausted="";
  const warns=pickWarnings(sid,card,original);
  const sec=PANEL.sections.find(x=>x.id===sid)||{};
  log(`${sec.title||"section"}: put ${card.entry_id}`
    +`${card.company?` (${card.company})`:""} in place of ${original.entry_id}`
    +`${original.company?` (${original.company})`:""}.`,"o");
  /* The report's own rules are not enforced here — naming a piece outright IS the
     override. They are said out loud instead, because a rule broken by accident and a
     rule broken on purpose look identical on the slide. */
  for(const w of warns)log(`  ! ${w}`,"w");
  closeById();
  FLASH=card.entry_id;
  renderPanel();
  wantThumbs();
}
/* What a by-hand pick quietly breaks, if anything. Said, not blocked. */
function pickWarnings(sid,card,original){
  const sec=(PANEL&&PANEL.sections.find(x=>x.id===sid))||{};
  const out=[];
  if(sec.one_per_company&&card.company){
    const clash=(PSTATE[sid].slate||[]).filter(c=>c.entry_id!==card.entry_id
      &&c.entry_id!==(original||{}).entry_id
      &&(c.company||"").toLowerCase()===card.company.toLowerCase());
    if(clash.length)out.push(`${sec.title} is set to one piece per company, and`
      +` ${card.company} would be on it twice.`);
  }
  if(sec.never_reuse&&pickedElsewhere(sid).includes(card.entry_id))
    out.push(`${card.entry_id} is also on another section's slate, and this report is`
      +` set never to reuse a piece across sections.`);
  return out;
}
function zoom(u){
  const b=$("#lightbox");
  b.querySelector("img").src=u;
  b.classList.add("show");
}
function renderPanel(){
  if(!PANEL){$("#panel").innerHTML="";return}
  const paused=Object.keys(PSTATE).length>0;
  const H=[];
  const full=$("#panel").classList.contains("full");
  const bad=thumbTrouble();
  H.push(`<h2 class="phead">Results <span class="sub">— ${esc(PANEL.start||"")} .. `
    +`${esc(PANEL.end||"")}</span><span class="sp"></span>`
    +(bad?`<button class="ghost warn" onclick="retryAllThumbs()"
        title="These pieces do have a picture in the archive — the fetch failed.">
        retry ${bad} picture${bad>1?"s":""}</button>`:"")
    +`<button class="ghost" onclick="togglePanelFull()"
      title="Give the review the whole window — the pictures get bigger with it.">
      ${full?"exit full width":"full width"}</button>`
    +`<button class="ghost" onclick="togglePanel()">hide</button></h2>`);
  if((PANEL.files||[]).length){
    H.push(`<div class="dl">`
      +PANEL.files.map(f=>fileLink(PANEL.run_id,f.name,f.size,f.kind)).join("")
      +(PANEL.files.length>1?zipLink(PANEL.run_id,PANEL.files.length):"")+`</div>`);
  }
  if(paused){
    let need=0,have=0;
    for(const sid of Object.keys(PSTATE)){
      need+=PSTATE[sid].slate.length;
      have+=PSTATE[sid].slate.filter(c=>PSTATE[sid].ok[c.entry_id]===true).length;
    }
    H.push(`<div class="${have===need?"okbox":"warnbox"}">
      <b>${have} of ${need} pieces settled.</b> Keep the ones that belong on the slide
      and swap the ones that do not.</div>`);
    H.push(`<button class="primary" style="width:100%;margin-bottom:12px"
      ${have===need&&need>0?"":"disabled"} onclick="confirmPicks()">
      Build the deck from these ${need} piece(s)</button>`);
  }else if(BUILT){
    const kept=Object.values(BUILT).reduce((n,l)=>n+l.length,0);
    H.push(BUILDING
      ? `<div class="okbox"><span class="spin"></span> <b>Building the deck from the
          ${kept} piece(s) you kept.</b> The write-up under each slide is being drafted
          now — this is the part that costs, so it is left to run.</div>`
      : `<div class="okbox"><b>Built from these ${kept} piece(s).</b> What is below is
          what went on the slides. The files are under Deliverables on the right.</div>`);
  }
  for(const sec of PANEL.sections){
    const st=PSTATE[sec.id];
    /* Once a build has been asked for, the pool is not shown again. A section that was
       never featured has no slate to show instead, so it is summarised rather than
       listed — hundreds of rows nobody chose are exactly what this panel stopped
       being at the moment the picks were confirmed. */
    const done=BUILT?(BUILT[sec.id]||null):null;
    const summary=!st&&BUILT&&!done;
    const list=st?st.slate:(done||sec.pieces);
    /* A capped total is a LOWER BOUND: the archive stopped counting. It is said once,
       in the count itself, so it cannot be read as an exact figure. */
    const n=Number(sec.archive_total||0).toLocaleString();
    const total=sec.at_least?`at least ${n}`:n;
    H.push(`<div class="psec"><h4>${esc(sec.title)}
      <span class="cnt${sec.at_least?" atleast":""}"
        title="${sec.at_least?"The archive stopped counting, so this is a floor, not a total."
          :"Counted by the archive itself, before any row cap."}">${esc(total)} in the
        archive</span>${st||done?(done?`<span class="cnt">${done.length} on the
          deck</span>`:""):`<span class="cnt">${sec.kept} kept${
          sec.shown<sec.kept?` · showing ${sec.shown}`:""}</span>`}</h4>`);
    if(st&&st.exhausted)H.push(`<div class="piece"><div class="meta"
      style="color:#a5661a">${esc(st.exhausted)}</div></div>`);
    if(summary){
      H.push(`<div class="piece"><div class="meta" style="color:#8b93a6">
        ${sec.kept} piece(s) went to the workbook. Nothing here goes on a slide, so
        there was nothing to approve.</div></div>`);
    }else{
      if(!list.length)H.push(`<div class="piece"><div class="meta"
        style="color:#8b93a6">nothing here</div></div>`);
      for(const c of list)H.push(pieceRow(c,st?sec.id:null));
    }
    if(!st&&!BUILT&&sec.kept)H.push(`<div class="piece"><div class="meta">
      <button class="ghost" onclick="copyIds('${sec.id}')">copy all
        ${sec.shown} entry_ids</button>
      <button class="ghost" onclick="moreThumbs('${sec.id}')">load more
        pictures</button></div></div>`);
    if((st||done)&&sec.reasoning)H.push(`<div class="piece"><div class="meta why">
      why these: ${esc(sec.reasoning)}</div></div>`);
    H.push(`</div>`);
  }
  /* Replacing innerHTML throws the scroll position away: the content height goes to
     zero for an instant and the browser clamps scrollTop to 0. This function is called
     on every keep, every swap and every background thumbnail retry, so without this
     the panel jumped to the top constantly and a decision made halfway down the list
     lost its place — which read as "nothing happened". */
  const el=$("#panel");
  el.classList.toggle("building",!!BUILDING);
  const at=el.scrollTop;
  /* Centred and capped in full width, so a headline does not run the width of a
     27-inch monitor. */
  el.innerHTML=`<div class="pwrap">`+H.join("")+`</div>`;
  el.scrollTop=at;
  /* A piece that just arrived is scrolled to and flashed, so a replacement is
     something you watch happen rather than something you go hunting for. */
  if(FLASH){
    const row=el.querySelector(`.piece[data-eid="${cssq(FLASH)}"]`);
    FLASH="";
    if(row){
      row.classList.add("landed");
      try{row.scrollIntoView({block:"nearest",behavior:"smooth"})}catch(e){}
      setTimeout(()=>row.classList.remove("landed"),1900);
    }
  }
}
/* Ask for the pictures the panel is currently showing, in one batch, and only for
   pieces nothing has asked about yet.

   Lazy on purpose. There is no image URL on a search row — the location lives in the
   database and the bucket refuses anonymous readers — so every picture costs a query
   and an S3 read through the pipeline interpreter. Fetching a whole 300-piece section
   up front would spend minutes on rows nobody scrolls to. */
async function wantThumbs(){
  if(!RUNID||!PANEL)return;
  /* A call that arrives mid-flight used to be dropped on the floor. Nothing else
     re-triggered it — the retry timer only looks at pieces already marked "retry",
     never at ones nobody has asked about yet — so a piece added to the slate while a
     batch was in flight kept its "..." placeholder for good. Remembered and re-run
     instead. */
  if(thumbBusy){thumbAgain=true;return}
  /* The pieces on the slate go first. They are the ones being decided on, there are
     few of them, and a long unfeatured section used to fill the batch ahead of them. */
  const want=[],seen=new Set();
  const add=c=>{
    if(!c||!c.entry_id||seen.has(c.entry_id))return;
    if(THUMBS[c.entry_id]!==undefined)return;
    seen.add(c.entry_id);want.push(c.entry_id);
  };
  for(const sec of PANEL.sections)if(PSTATE[sec.id])PSTATE[sec.id].slate.forEach(add);
  for(const sec of PANEL.sections)
    if(!PSTATE[sec.id])sec.pieces.slice(0,THUMB_PAGE).forEach(add);
  if(!want.length){scheduleThumbRetry(4000);return}
  const asked=want.slice(0,THUMB_PAGE);
  thumbBusy=true;
  try{
    const r=await fetch("/api/thumbs",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({run_id:RUNID,entry_ids:asked})});
    const d=await r.json();
    const got=d.thumbs||{},none=new Set(d.missing||[]);
    let retrying=0,tries=1;
    for(const e of asked){
      THUMBTRY[e]=(THUMBTRY[e]||0)+1;
      tries=Math.max(tries,THUMBTRY[e]);
      /* Only the archive's own "no cover image for this piece" is recorded as final.
         Anything else is this attempt failing, and gets another one. */
      if(got[e])THUMBS[e]="ok";
      else if(none.has(e))THUMBS[e]="none";
      else{THUMBS[e]="retry";retrying++}
    }
    if(d.error&&!THUMBWARNED){
      THUMBWARNED=true;
      /* The tunnel being shut is the ordinary case, not an incident. Say it once. */
      log(`Pictures unavailable: ${d.error}. Everything else works — the pictures live`
        +` in the database and S3, not on the search row. Retrying in the background.`,"w");
    }
    renderPanel();
    /* Two reasons to keep going: some failed, or the batch cap left some unasked. */
    if(retrying)scheduleThumbRetry(2000*Math.min(5,tries));
    else if(want.length>asked.length)setTimeout(wantThumbs,0);
  }catch(e){
    /* The Studio itself was unreachable. Nothing is written down, so the next pass
       asks for exactly the same pieces again. */
    scheduleThumbRetry(3000);
  }
  thumbBusy=false;
  if(thumbAgain){thumbAgain=false;setTimeout(wantThumbs,0)}
}
const THUMB_PAGE=60;

function copy(t){
  try{navigator.clipboard.writeText(t)}catch(e){}
  log(`copied ${t}`,"d");
}
/* Pull in the pictures for the rest of one section, a batch at a time. */
async function moreThumbs(sid){
  const sec=PANEL.sections.find(s=>s.id===sid);
  const want=sec.pieces.map(c=>c.entry_id)
    .filter(e=>THUMBS[e]===undefined||THUMBS[e]==="retry");
  if(!want.length){log("Every piece in this section either has its picture or is known"
    +" not to have one.","d");return}
  const asked=want.slice(0,THUMB_PAGE);
  thumbBusy=true;
  try{
    const r=await fetch("/api/thumbs",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({run_id:RUNID,entry_ids:asked})});
    const d=await r.json();
    const got=d.thumbs||{},none=new Set(d.missing||[]);
    for(const e of asked){
      THUMBTRY[e]=(THUMBTRY[e]||0)+1;
      THUMBS[e]=got[e]?"ok":(none.has(e)?"none":"retry");
    }
  }catch(e){/* nothing written down; the retry pass asks again */}
  thumbBusy=false;
  renderPanel();
  scheduleThumbRetry(2500);
}
function copyIds(sid){
  const sec=PANEL.sections.find(s=>s.id===sid);
  copy(sec.pieces.map(c=>c.entry_id).join("\n"));
}
function approve(sid,eid){
  PSTATE[sid].ok[eid]=true;renderPanel();
}
async function reject(sid,eid){
  const st=PSTATE[sid];
  const i=st.slate.findIndex(c=>c.entry_id===eid);
  const original=st.slate[i];
  st.rejected.push(eid);
  delete st.ok[eid];
  const keep=st.slate.filter(c=>c.entry_id!==eid).map(c=>c.entry_id);
  const r=await fetch("/api/run/replace",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({run_id:RUNID,section:sid,keep:keep,
      reject:st.rejected,used:pickedElsewhere(sid)})});
  const d=await r.json();
  if(d.error){log("Swap failed: "+d.error,"e");return}
  if(d.exhausted||!d.replacement){
    st.slate.splice(i,1);
    st.gone=(st.gone||[]).concat([original]);
    st.exhausted=d.reason||"Nothing left to swap in.";
  }else{
    st.slate[i]=d.replacement;
    /* Remember what this replaced. A swap is a judgement call made in a second, and
       the piece it displaced is the one thing that becomes hard to find again once it
       is off the list. */
    st.swapped=st.swapped||{};
    st.swapped[d.replacement.entry_id]=original;
    st.exhausted="";
    FLASH=d.replacement.entry_id;
  }
  renderPanel();
  wantThumbs();
}
/* Put back the piece a swap displaced. The replacement leaves the slate and, since it
   was never rejected, stays eligible — swap again and it can come back. */
function unswap(sid,eid){
  const st=PSTATE[sid];
  const original=(st.swapped||{})[eid];
  if(!original)return;
  const i=st.slate.findIndex(c=>c.entry_id===eid);
  if(i<0)return;
  st.slate[i]=original;
  delete st.swapped[eid];
  delete st.ok[eid];
  st.rejected=st.rejected.filter(x=>x!==original.entry_id);
  st.exhausted="";
  FLASH=original.entry_id;
  renderPanel();
  wantThumbs();
}
async function confirmPicks(){
  const approved={};
  /* The slate is kept, not discarded. It is what the deck is being built from, so it
     is what the panel goes on showing while that happens. */
  BUILT={};
  for(const sid of Object.keys(PSTATE)){
    approved[sid]=PSTATE[sid].slate.map(c=>c.entry_id);
    BUILT[sid]=PSTATE[sid].slate.slice();
  }
  const email=($("#runEmail").value||"").trim();
  openLog();
  log("","");log("Building the deliverables from the pieces you kept…","d");
  PSTATE={};BUILDING=true;renderPanel();
  T0=Date.now();running(true,"Building");
  const r=await fetch("/api/run/continue",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({run_id:RUNID,approved:approved,email_to:email})});
  const d=await r.json();
  if(d.error){
    log("ERROR "+d.error,"e");running(false);BUILDING=false;renderPanel();return;
  }
  if(email)$("#runEmail").value="";
  watchFrom();
}
function watchFrom(){
  /* Continue re-uses the run id, so the line buffer keeps growing. Start from what is
     already on screen rather than replaying the first half. */
  fetch("/api/run/status?id="+RUNID).then(r=>r.json()).then(s=>{
    watch((s.lines||[]).length);
  });
}

/* ── projects, templates, history ────────────────────────────────────────── */
function show(n){$("#ov"+n).classList.add("show")}
function hide(n){$("#ov"+n).classList.remove("show")}
function openProjects(){refreshSaved();show("Projects")}
function renderTemplates(){
  $("#tplList").innerHTML=(SPEC.templates||[]).map(t=>
    `<div class="tsec" onclick="newFrom(${jq(t.key)})">
      <b>${esc(t.label)}</b><span>${esc(t.note||"")}</span></div>`).join("");
}
/* The last listing, kept so the buttons on a row can name the report they are about
   to copy or delete without going back to the server for its title. */
let SAVED=[];
/* Mirrors _slug() on the server, and only to warn before Save lands on top of somebody
   else's report. The server is still the one that decides the filename; if the two
   ever disagree the cost is a missed warning, never a wrong file. */
function slugOf(s){
  return String(s||"").replace(/[^A-Za-z0-9]+/g,"_").replace(/^_+|_+$/g,"")||"report";
}
/* Rough on purpose. "3d ago" is what the eye is scanning for; the exact timestamp is
   on the hover, where somebody who needs it can find it. */
function ago(sec){
  if(!sec)return "";
  const d=Date.now()/1000-sec;
  if(d<90)return "saved just now";
  if(d<5400)return `saved ${Math.max(1,Math.round(d/60))} min ago`;
  if(d<172800)return `saved ${Math.round(d/3600)}h ago`;
  return `saved ${Math.round(d/86400)}d ago`;
}
async function refreshSaved(){
  const d=await (await fetch("/api/projects")).json();
  SAVED=d.projects||[];
  const here=(P&&(P.status||{}).saved_as)||"";
  $("#savedList").innerHTML=SAVED.map(r=>{
    const b=r.badge||{},open=r.name===here;
    const stamp=r.modified?new Date(r.modified*1000).toLocaleString():"";
    /* Joined rather than concatenated with separators baked in: a report with no
       client used to lead with a stray dot. */
    const meta=[esc(r.client||""),
      r.sections?`${r.sections} section(s)`:"",
      r.fixed_window?"fixed date range":"",
      ago(r.modified)
        ? `<span class="when" title="${esc(stamp)}">${esc(ago(r.modified))}</span>`:""
    ].filter(Boolean).join(" · ");
    return `<li class="${open?"here":""}"><b>${esc(r.title||r.name)}`
      +`${open?` <span class="here-pill">· open now</span>`:""}`
      +`<div class="meta2">${meta}</div></b>`
      +`<span class="badge ${esc(b.tone||"dim")}" title="${esc(b.detail||"")}">`
      +`${esc(b.label||"Draft")}</span>`
      +`<button onclick="loadProject(${jq(r.name)})">Open</button>`
      +`<button class="ghost" title="Save a fresh copy of this report. The original is`
      +` not touched." onclick="dupProject(${jq(r.name)})">Copy</button>`
      +`<button class="ghost warn" title="Move this report to the _trash folder."`
      +` onclick="delProject(${jq(r.name)})">Delete</button></li>`;
  }).join("")||`<div class="hint">Nothing saved yet.</div>`;
}
async function dupProject(n){
  const r=SAVED.find(x=>x.name===n)||{};
  const d=await (await fetch("/api/projects/duplicate",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({name:n})})).json();
  if(d.error){alert(d.error);return}
  refreshSaved();
  /* Deliberately does NOT open the copy: whatever is on screen may be unsaved, and
     swapping it out from under somebody who asked for a copy would lose that work. */
  log(`Copied "${r.title||n}" to "${d.title}". It is in Projects, unopened — nothing `
    +`on screen changed.`,"o");
}
async function delProject(n){
  const r=SAVED.find(x=>x.name===n)||{};
  const open=((P.status||{}).saved_as||"")===n;
  if(!confirm(`Delete "${r.title||n}"?\n\n`
    +(open?`This is the report open on screen. The saved copy goes; what you are `
          +`looking at stays, unsaved.\n\n`:"")
    +`It is moved to the _trash folder next to the other saved reports, so it can be `
    +`put back by hand.`))return;
  const d=await (await fetch("/api/projects/delete",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({name:n})})).json();
  if(d.error){alert(d.error);return}
  /* The open report no longer points at a file. Without this, the next finished run
     calls silentSave() and quietly writes the report straight back onto the shelf. */
  if(open){P.status=P.status||{sent:null,runs:[],saved_as:""};P.status.saved_as=""}
  refreshSaved();check();
  log(`Deleted "${r.title||n}". The file is at ${d.trash} if you want it back.`,"w");
}
async function newFrom(k){
  const d=await (await fetch("/api/template?name="+encodeURIComponent(k))).json();
  P=d.project;
  OPEN={};PVW={};PANEL=null;PSTATE={};BUILT=null;BUILDING=false;killPanel();
  await prefetchTaxo();hide("Projects");render();
  log(`Started a new report from the "${k}" template. It is unsaved — the template `
    +`itself is untouched.`,"d");
}
async function loadProject(n){
  const d=await (await fetch("/api/projects/load?name="+encodeURIComponent(n))).json();
  if(d.error){alert(d.error);return}
  P=d.project;OPEN={};PVW={};PANEL=null;PSTATE={};BUILT=null;BUILDING=false;
  killPanel();restoreFiles();
  await prefetchTaxo();hide("Projects");render();
  if(d.migrated)log("Opened an older project and brought it up to date — check Notes "
    +"for Engineering for anything that had no equivalent here.","w");
}
async function saveProject(){
  const n=($("#saveAs").value||"").trim();
  /* Saving under a name somebody else's report already has used to replace it with no
     warning at all. The name that matters is the slug, not what was typed, because
     "Q3 Cards" and "q3 cards" are the same file. */
  const slug=slugOf(n||P.name||"untitled");
  const clash=SAVED.find(x=>x.name===slug);
  if(clash&&slug!==((P.status||{}).saved_as||"")
    &&!confirm(`"${clash.title}" is already saved under that name.\n\n`
      +`Saving replaces it. Its old version is not kept — use a different name if you `
      +`meant to keep both.`))return;
  if(n)P.name=n;
  const r=await fetch("/api/projects/save",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({project:P})});
  const d=await r.json();
  if(d.project)P=d.project;
  $("#rname").value=P.name;$("#saveAs").value="";
  refreshSaved();log(`Saved as ${d.name}.`,"o");check();
}

async function openHistory(){
  const runs=((P.status||{}).runs||[]).slice().reverse();
  if(!runs.length){
    $("#historyBody").innerHTML=`<div class="hint">This report has not been run yet.
      Runs are listed here with their files, so a deck built on Tuesday can be handed
      over again on Thursday without running anything.</div>`;
    show("History");return;
  }
  const live=await Promise.all(runs.map(async r=>{
    try{
      const d=await (await fetch("/api/run/files?id="+encodeURIComponent(r.id))).json();
      return d.files||[];
    }catch(e){return []}
  }));
  $("#historyBody").innerHTML=runs.map((r,i)=>{
    const here=live[i];
    const label=((SPEC.modes||[]).find(m=>m.key===r.mode)||{}).label||r.mode;
    const when=String(r.at||"").replace("T"," ").slice(0,16);
    let files;
    if(here.length){
      files=`<div class="dl">`
        +here.map(f=>fileLink(r.id,f.name,f.size,f.kind)).join("")
        +(here.length>1?zipLink(r.id,here.length):"")+`</div>`;
    }else if((r.produced||[]).length){
      /* The row outlives the files on purpose. Saying the files are gone is more use
         than the run vanishing from the list. */
      files=`<div class="hint">${esc(r.produced.join(", "))} — no longer on disk.
        Older runs are cleared out as new ones are made. Run it again to rebuild.</div>`;
    }else{
      files=`<div class="hint">No files${r.stopped?" — this run was stopped":""}.</div>`;
    }
    return `<div class="tsec" style="cursor:default"><b>${esc(label)}
      <span style="font-weight:400;color:#8b93a6">· ${esc(when)}${
        r.stopped?" · stopped":r.rc===0?"":` · exit ${r.rc}`}${
        r.emailed?" · emailed":""}</span></b>${files}</div>`;
  }).join("");
  show("History");
}

/* ── promote: a one-off becomes something safe to schedule ───────────────── */
let PROMOTED=null;
async function openPromote(){
  const r=await fetch("/api/promote",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({project:P})});
  const d=await r.json();
  PROMOTED=d.project;
  $("#promoteBody").innerHTML=
    `<p>An ongoing report is one Engineering deploys and schedules. This gets it ready,
      and changes nothing else. It does not send anything.</p>`
    +`<h2 class="mt">What changes</h2>`
    +(d.changes||[]).map(c=>`<div class="okbox">${esc(c)}</div>`).join("")
    +((d.warnings||[]).length?`<h2 class="mt">Worth knowing</h2>`
      +d.warnings.map(c=>`<div class="warnbox">${esc(c)}</div>`).join(""):"");
  show("Promote");
}
function applyPromote(){
  if(!PROMOTED)return;
  P=PROMOTED;PROMOTED=null;hide("Promote");render();
  log("This report is now on a repeating cadence. Save it, then Send to Eng. when you"
    +" are happy with it.","o");
}

/* ── send to engineering ─────────────────────────────────────────────────── */
function openExport(){
  const errs=ISSUES.filter(x=>x.level==="error");
  const fixed=(P.window||{}).mode==="range";
  let html="";
  if(errs.length){
    html=`<div class="msg error">There ${errs.length===1?"is":"are"} ${errs.length}
      thing${errs.length===1?"":"s"} to fix first.</div>`
      +errs.map(m=>`<div class="msg error">${esc(m.msg)}</div>`).join("");
  }else{
    if(fixed){
      /* The single most likely way a researcher footguns Engineering: a pipeline
         pinned to two dates, put on a schedule, producing the same period forever
         while looking like it works. It cannot be missed and it cannot be clicked
         past without saying so. */
      html+=`<div class="warnbox"><b>This report has a fixed date range
        (${esc(P.window.start)} .. ${esc(P.window.end)}).</b><br>
        Scheduled, it would produce that same period on every run, for ever, and give
        no sign that anything was wrong. A one-off should be run here and downloaded —
        it does not need Engineering at all.<br><br>
        <button class="primary" onclick="hide('Export');openPromote()">Put it on a
          repeating cadence instead</button></div>
        <label class="check"><input type="checkbox" id="ackFixed"
          onchange="$('#exportGo').disabled=!this.checked">
          <span>I know it has a fixed window and I am sending it anyway — this is not
            going on a schedule as it stands.</span></label>`;
    }
    html+=`
      <div class="f" style="margin-top:13px"><label>When should it run? (optional)</label>
      <input id="deployWhen" placeholder="e.g. the 3rd of each month"></div>`;
  }
  $("#exportBody").innerHTML=html;
  $("#exportGo").disabled=!!errs.length||fixed;
  show("Export");
}
async function sendToEngineering(){
  if(ISSUES.some(x=>x.level==="error")){hide("Export");return}
  const when=($("#deployWhen")||{}).value||"";
  const r=await fetch("/api/export",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({project:P,deploy_when:when})});
  const d=await r.json();
  hide("Export");
  if(d.error){log("ERROR "+d.error,"e");return}
  log(`Wrote ${d.path}`,"o");
  const em=d.email||{};
  for(const s of (em.sent||[]))log(`Emailed Engineering at ${s.to}.`,"o");
  for(const e of (em.errors||[]))log(`Email to ${e.to} FAILED: ${e.error}`,"e");
  if(em.error)log(`Email FAILED: ${em.error}`,"e");
  /* Only a real success becomes a receipt. A report that failed to send must not
     start claiming Engineering has it. */
  if(d.sent&&(em.sent||[]).length){
    P.status=P.status||{sent:null,runs:[],saved_as:""};
    P.status.sent=d.sent;
    check();
    await keepReceipt();
  }
}
/* A hand-off is the one thing that cannot live only in this tab. The receipt used to be
   written back only when the report already pointed at a file, so sending a report
   nobody had saved yet put Delivered on the top bar, wrote nothing to the shelf, and
   lost the fact entirely when the tab closed — Projects went on saying Draft about a
   report Engineering had. Sending now saves the report, under its own name if it does
   not have a file yet. */
async function keepReceipt(){
  const isNew=!((P.status||{}).saved_as);
  try{
    const r=await fetch("/api/projects/save",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({project:P})});
    const d=await r.json();
    if(d.project)P=d.project;
    $("#rname").value=P.name||"";
    check();refreshSaved();
    log(isNew?`Saved as ${d.name} — a report Engineering has is kept in Projects, so `
              +`the hand-off is on the record and not just on this screen.`
             :`Recorded in Projects: ${d.name} now reads Delivered.`,"o");
  }catch(e){
    log("The send succeeded but the report could not be saved — Projects will still "
      +"show it as a draft. Save it by hand from Projects.","w");
  }
}

document.addEventListener("keydown",e=>{
  if(e.key==="Escape")["Projects","Export","Pick","History","Promote"].forEach(hide);
});
boot();
</script></body></html>
"""


# ═══════════════════════════════════════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════════════════════════════════════

def spec() -> dict:
    cat = catalog()
    return {
        "channels": core_values("media_channel"),
        "audiences": core_values("audience"),
        "countries": core_values("country"),
        "sectors": cat.get("sectors") or [],
        "groups": cat.get("groups") or {},
        "core": cat.get("core") or {},
        "date_fields": DATE_FIELDS,
        "columns": COLUMNS,
        "limit_max": CS.LIMIT_MAX,
        "templates": [{"key": k, "label": v[0], "note": TEMPLATE_NOTES.get(k, "")}
                      for k, v in TEMPLATES.items()],
        "modes": [{"key": k, "label": v["label"], "help": v["help"],
                   "pauses": v["pauses"]} for k, v in MODES.items()],
        "catalog": {"source": cat.get("source"), "fetched_at": cat.get("fetched_at"),
                    "error": cat.get("error")},
    }


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
        one = lambda k, d="": (q.get(k) or [d])[0]  # noqa: E731

        if u.path in ("/", "/index.html"):
            return self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
        if u.path == "/logo.jpg":
            try:
                return self._send(200, LOGO_FILE.read_bytes(), "image/jpeg")
            except OSError:
                return self._send(404, b"not found")
        if u.path == "/api/spec":
            return self._json(spec())
        if u.path == "/api/taxonomy":
            try:
                return self._json(CS.taxonomy(one("parent") or None))
            except CS.ApiError as exc:
                return self._json({"children": [], "error": exc.hint()})
        if u.path == "/api/lookup":
            field = one("field")
            if field not in CS.LOOKUP_FIELDS:
                return self._json({"matches": [],
                                   "error": f"{field} has no lookup endpoint"})
            try:
                return self._json(CS.lookup(field, one("q"), 30))
            except CS.ApiError as exc:
                return self._json({"matches": [], "error": exc.hint()})
        if u.path == "/api/template":
            name = one("name", "blank")
            if name not in TEMPLATES:
                return self._json({"error": "unknown template"}, 404)
            # Through migrate() so a template's filters list is synced from its
            # enhanced values the same way a loaded project's is. A fresh object every
            # time, and no endpoint writes one back — a template cannot be edited or
            # overwritten from this Studio, only started from.
            return self._json({"project": migrate(TEMPLATES[name][1]()),
                               "note": TEMPLATE_NOTES.get(name, "")})
        if u.path == "/api/section":
            return self._json({"section": new_section()})
        if u.path == "/api/projects":
            out = []
            for rec in STORE.list():
                row = {"name": rec["name"], "title": rec["name"], "badge": None,
                       "modified": rec["modified"]}
                if rec["raw"] is None:
                    row["badge"] = {"state": "draft", "label": "Unreadable",
                                    "tone": "warn",
                                    "detail": f"This file could not be parsed: "
                                              f"{rec['error']}"}
                else:
                    raw = migrate(rec["raw"])
                    row["title"] = str(raw.get("name") or rec["name"])
                    row["client"] = str(raw.get("client") or "")
                    row["sections"] = len(raw.get("sections") or [])
                    row["badge"] = status_badge(raw)
                    row["fixed_window"] = ((raw.get("window") or {}).get("mode")
                                           == "range")
                out.append(row)
            # Newest first, because the report somebody is halfway through is the one
            # they came back for. Ties fall back to the title so the order is stable
            # rather than whatever the filesystem felt like.
            out.sort(key=lambda r: (-(r["modified"] or 0), r["title"].lower()))
            return self._json({"projects": out})
        if u.path == "/api/projects/load":
            raw = STORE.read(one("name"))
            if raw is None:
                return self._json({"error": "not found"}, 404)
            was = int(raw.get("schema") or 0)
            return self._json({"project": migrate(raw), "migrated": was < SCHEMA})
        if u.path == "/api/run/status":
            with RUNS_LOCK:
                r = RUNS.get(one("id"))
                if not r:
                    return self._json({"error": "unknown run"}, 404)
                out = {"lines": list(r["lines"]), "done": r["done"], "rc": r["rc"],
                       "mode": r.get("mode"), "stopped": bool(r.get("stopped")),
                       "running": r.get("proc") is not None}
            if out["done"]:
                # A paused run is a SUCCESSFUL one that stopped on purpose. The panel
                # opens on it, so the UI has to be able to tell it apart from a run
                # that simply finished.
                spec_mode = MODES.get(out["mode"]) or {}
                out["paused"] = bool(spec_mode.get("pauses") and out["rc"] == 0
                                     and not out["stopped"])
                out["files"] = _artifacts(one("id"))
                out["record"] = _run_record(one("id"))
            return self._json(out)
        if u.path == "/api/run/panel":
            return self._json(panel(one("id")))
        if u.path == "/api/run/file":
            path = run_file(one("id"), one("name"))
            if path is None:
                return self._send(404, b"not found", "text/plain")
            try:
                body = path.read_bytes()
            except OSError:
                return self._send(410, b"the file is no longer on disk", "text/plain")
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition",
                             f'attachment; filename="{path.name}"')
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return None
        if u.path == "/api/run/zip":
            packed = run_zip(one("id"))
            if packed is None:
                return self._send(404, b"this run has no files", "text/plain")
            name, body = packed
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition", f'attachment; filename="{name}"')
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return None
        if u.path == "/api/thumb":
            path = thumb_file(one("id"), one("entry_id"))
            if path is None:
                return self._send(404, b"no thumbnail", "text/plain")
            try:
                body = path.read_bytes()
            except OSError:
                return self._send(404, b"no thumbnail", "text/plain")
            self.send_response(200)
            self.send_header("Content-Type", "image/jpeg")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "private, max-age=86400")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return None
        if u.path == "/api/run/files":
            # Run history asks this before offering a download, so a row whose files
            # have been pruned says so instead of handing over a dead link.
            return self._json({"files": _artifacts(one("id"))})
        return self._json({"error": "not found"}, 404)

    def do_POST(self):
        u = urlparse(self.path)
        try:
            body = self._body()
        except Exception as exc:
            return self._json({"error": f"bad JSON: {exc}"}, 400)
        project = body.get("project") or {}

        if u.path == "/api/check":
            return self._json(validate(project))
        if u.path == "/api/preview":
            try:
                return self._json(preview(project, body.get("section_id") or ""))
            except CS.ApiError as exc:
                return self._json({"error": exc.hint()})
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"})
        if u.path == "/api/export":
            try:
                code, fname = codegen(project)
                ast.parse(code)  # never hand over a file that will not import
                GENERATED_DIR.mkdir(parents=True, exist_ok=True)
                path = GENERATED_DIR / fname
                path.write_text(code, encoding="utf-8")
                deploy_when = str(body.get("deploy_when") or "").strip()
                email = _email_engineering(project, path, deploy_when)
                # The receipt the badge is computed from. The hash is of the project as
                # it was sent, so any later edit shows as "Edited since sent" rather
                # than letting a researcher assume Engineering has the latest version.
                sent = {"at": datetime.now().isoformat(timespec="seconds"),
                        "file": path.name, "hash": content_hash(project)}
                return self._json({"path": str(path), "email": email, "sent": sent})
            except SyntaxError as exc:
                return self._json({"error": f"generated code did not parse: {exc}"}, 500)
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/run":
            mode = body.get("mode") or "search"
            if mode not in MODES:
                return self._json({"error": f"unknown run mode {mode}"}, 400)
            # A literal address reaches this process, is put on ONE child process's
            # environment, and is never written anywhere. It is not in the project the
            # client just posted, it is not saved, and it is not in the run history.
            email_to = str(body.get("email_to") or "").strip()
            if email_to and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email_to):
                return self._json({"error": f'"{email_to}" does not look like an email '
                                            f"address."}, 400)
            problem = mode_problem(project, mode)
            if problem:
                return self._json({"error": problem}, 400)
            try:
                rid = start_run(project, mode, email_to=email_to)
                return self._json({"run_id": rid, "mode": mode})
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/run/stop":
            return self._json(stop_run(str(body.get("run_id") or "")))
        if u.path == "/api/run/continue":
            approved = body.get("approved")
            if not isinstance(approved, dict):
                return self._json({"error": "approved must be an object of "
                                            "section_id -> [entry_id]"}, 400)
            email_to = str(body.get("email_to") or "").strip()
            ok = continue_run(str(body.get("run_id") or ""), approved, email_to)
            return self._json({"ok": ok} if ok else {"error": "unknown run"})
        if u.path == "/api/thumbs":
            return self._json(fetch_thumbs(str(body.get("run_id") or ""),
                                           list(body.get("entry_ids") or [])))
        if u.path == "/api/run/pick":
            return self._json(lookup_pick(
                str(body.get("run_id") or ""), str(body.get("section") or ""),
                str(body.get("entry_id") or "").strip()))
        if u.path == "/api/run/replace":
            return self._json(replace_pick(
                str(body.get("run_id") or ""), str(body.get("section") or ""),
                list(body.get("keep") or []), list(body.get("reject") or []),
                list(body.get("used") or [])))
        if u.path == "/api/promote":
            return self._json(promote(project))
        if u.path == "/api/projects/save":
            name = _slug(project.get("name") or "untitled")
            project = migrate(project)
            # The slug is remembered inside the project so a later run can append
            # itself to the history without asking the researcher to save again.
            project.setdefault("status", {})["saved_as"] = name
            STORE.write(name, project)
            return self._json({"name": name, "project": project})
        if u.path == "/api/projects/duplicate":
            # "Last quarter's report, but for this quarter" is how most reports here
            # actually start, and rebuilding one card by card to get it is silly. The
            # copy keeps the whole shape and none of the history: it has produced
            # nothing, Engineering has never seen it, and it is a file of its own.
            raw = STORE.read(str(body.get("name") or ""))
            if raw is None:
                return self._json({"error": "not found"}, 404)
            copy = migrate(raw)
            title, name = STORE.free_name(
                f"{copy.get('name') or body.get('name')} copy")
            copy["name"] = title
            copy["status"] = {"sent": None, "runs": [], "saved_as": name}
            STORE.write(name, copy)
            return self._json({"name": name, "title": title, "project": copy})
        if u.path == "/api/projects/delete":
            # Moved aside, not shredded — see ProjectStore.delete. The path comes back
            # so the page can say where it went instead of "deleted" and nothing else.
            where = STORE.delete(str(body.get("name") or ""))
            if where is None:
                return self._json({"error": "not found"}, 404)
            return self._json({"name": _slug(body.get("name")), "trash": where})
        return self._json({"error": "not found"}, 404)


# ═══════════════════════════════════════════════════════════════════════════════════════
# Selftest — codegen every branch shape and screen the output
# ═══════════════════════════════════════════════════════════════════════════════════════

def _json_isms(tree) -> list[str]:
    """Bare true/false/null identifiers: the signature of json.dumps where a Python
    literal was needed. Legal identifiers, so ast.parse accepts them and the file only
    dies with NameError when someone runs it."""
    return sorted({n.id for n in ast.walk(tree)
                   if isinstance(n, ast.Name) and n.id in ("true", "false", "null")})


def _undefined(tree) -> list[str]:
    """Names read at module level or inside a function that were never bound anywhere in
    the file and are not builtins. Catches an emitter branch that references a constant
    another branch was supposed to define — API_FIELD without tabs, _filter without a
    post-filter, _ocr without an OCR column."""
    import builtins
    bound = set(dir(builtins)) | {"__name__", "__file__", "__doc__"}
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(n.name)
            bound.update(a.arg for a in n.args.args)
            bound.update(a.arg for a in n.args.kwonlyargs)
            if n.args.vararg:
                bound.add(n.args.vararg.arg)
            if n.args.kwarg:
                bound.add(n.args.kwarg.arg)
        elif isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            bound.add(n.id)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            for a in n.names:
                bound.add((a.asname or a.name).split(".")[0])
        elif isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
        elif isinstance(n, ast.comprehension):
            for t in ast.walk(n.target):
                if isinstance(t, ast.Name):
                    bound.add(t.id)
        elif isinstance(n, (ast.Lambda,)):
            bound.update(a.arg for a in n.args.args)
    read = {n.id for n in ast.walk(tree)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    return sorted(read - bound)


def _guardrails(code: str, project: dict) -> list[str]:
    """Screens for the things a parse check and a name check both sail past.

    Every one of these is a guardrail that was expensive to learn and is cheap to break
    with an innocent-looking edit, so each has a test that fails loudly rather than a
    comment that hopes.
    """
    p = migrate(project)
    g = _plan(p)
    out = []

    # Searches run one channel at a time. The archive's REST backend cross-contaminates
    # concurrent calls carrying different channels.
    if "for channel in sec[" not in code:
        out.append("the per-channel loop is gone")
    if "run_parallel" in code:
        # It is legitimate for the MODEL calls and nowhere else.
        head = code.split("def stage_search")[1].split("def print_counts")[0] \
            if "def stage_search" in code else ""
        if "run_parallel" in head:
            out.append("searching went parallel")
    if "import concurrent" in code or "ThreadPool" in code:
        out.append("a thread pool reached the search path")

    # A capped total is a lower bound and must be said so.
    if "at least" not in code:
        out.append("nothing says 'at least' for a capped total")
    if "SUSPECT" not in code:
        out.append("the capped-but-empty SUSPECT flag is gone")

    # Slides hold five, and prose is trimmed to whole sentences.
    if g["deck_on"] and g["featured"]:
        if "SLIDE_CAP    = 5" not in code:
            out.append("the 5-per-slide cap is gone")
        if "chunk_ids" not in code or "(cont.)" not in code:
            out.append("slide overflow no longer rolls onto (cont.) slides")
        if "L.fit_text" not in code:
            out.append("write-ups are no longer trimmed to whole sentences")
        if "pick_ids" not in code:
            out.append("pick_ids is gone, so an invented entry_id could reach a slide")
        if "_eligible" not in code or "_one_per_company" not in code:
            out.append("the selection rules are no longer in one place")
        # The write-up must be written AFTER the approval point, never before.
        select = code.split("def stage_select")[1].split("def stage_deliver")[0] \
            if "def stage_select" in code else ""
        if "_writeup" in select:
            out.append("a write-up is generated in the selection stage, so it would "
                       "describe pieces a researcher then rejects")

    # No literal recipient, ever. The address is read from the environment at run
    # time or it does not exist — a project mailed to Engineering and committed to a
    # repo must not be able to carry a client contact.
    assign = [ln for ln in code.splitlines() if ln.startswith("EMAIL_TO")]
    if not assign:
        out.append("EMAIL_TO is not defined, so a one-off run could not email anyone")
    else:
        whole = "\n".join(code.splitlines()[code.splitlines().index(assign[0]):][:3])
        if "os.environ.get" not in whole:
            out.append("the recipient is not read from the environment")
        if "@" in assign[0] or (len(whole.split(chr(10))) > 1 and "@" in whole):
            out.append("a literal email address is baked into the generated file")

    # A fixed window has to be impossible to miss.
    if g["fixed_window"]:
        if "DO NOT SCHEDULE THIS FILE" not in code:
            out.append("a fixed window carries no warning in the docstring")
        if 'WINDOW_MODE  = "range"' not in code:
            out.append("the fixed window is not visible in the settings block")

    # Notes for Engineering are lifted into the docstring.
    if g["notes"]:
        first = g["notes"].split()[0]
        head = code.split('"""')[1] if '"""' in code else ""
        if "NOTES FOR ENGINEERING" not in head or first not in head:
            out.append("Notes for Engineering did not reach the docstring")

    # The settings block stays at the top, above the sections.
    if "# ── Report settings" not in code:
        out.append("the settings block is gone")
    elif code.index("# ── Report settings") > code.index("SECTIONS = ["):
        out.append("the settings block is no longer above the sections")
    return out


def _variants() -> list[tuple[str, dict]]:
    """One project per shape the emitter can take. Every `if` in codegen needs a variant
    that exercises both sides of it, or a broken branch ships silently."""
    out: list[tuple[str, dict]] = []

    def base(name):
        p = new_project(name)
        p["client"] = "Selftest"
        return p

    out.append(("blank default", base("blank")))
    out.append(("example template", _example_project()))

    p = base("deck off")
    p["deck"]["enabled"] = False
    out.append(("no deck, workbook only", p))

    p = base("workbook off")
    p["workbook"]["enabled"] = False
    out.append(("no workbook, deck only", p))

    p = base("everything on")
    p["deck"].update({"summary_slide": True, "section_headings": True})
    p["sections"][0]["heading"] = "Deposits"
    p["email"] = {"enabled": True, "env_var": "RS_EMAIL_TO"}
    p["notes"] = "Split the lending section by application type before the deck is built."
    out.append(("summary + headings + email + notes", p))

    p = base("weekly")
    p["cadence"], p["anchor"] = "week", "rolling"
    p["date_field"] = "added_to_database"
    out.append(("weekly, rolling, added_to_database", p))

    p = base("flag true")
    p["sections"][0]["search"]["filters"] = ["credit_union"]
    p["sections"][0]["search"]["enhanced"] = {"credit_union": True}
    out.append(("boolean flag = true", p))

    p = base("flag false")
    p["sections"][0]["search"]["filters"] = ["rewards_program"]
    p["sections"][0]["search"]["enhanced"] = {"rewards_program": False}
    out.append(("boolean flag = false", p))

    p = base("range min only")
    p["sections"][0]["search"]["filters"] = ["loan_amount"]
    p["sections"][0]["search"]["enhanced"] = {"loan_amount_min": 250000}
    out.append(("range, min only", p))

    p = base("range zero")
    p["sections"][0]["search"]["filters"] = ["energy_offer_price"]
    p["sections"][0]["search"]["enhanced"] = {"energy_offer_price_min": 0,
                                              "energy_offer_price_max": 12}
    out.append(("range starting at zero", p))

    p = base("taxonomy drill")
    p["sections"][0]["search"].update({
        "sector": ["Credit Cards"], "category": ["Payment Cards"]})
    out.append(("taxonomy drilled to category", p))

    p = base("ocr terms")
    p["sections"][0]["search"].update({
        "sector": ["Banking"], "ocr_text": ["high yield", "no minimum"],
        "ocr_text_match": "any"})
    out.append(("ocr_text terms", p))

    p = base("company contains")
    p["sections"][0]["search"].update({"company": ["Capital One"],
                                       "company_match": "contains"})
    out.append(("company contains", p))

    p = base("sql columns")
    p["sections"][0]["sheet"]["columns"] = [
        "EntryID", "Primary Company", "State/Province", "Additional Companies",
        "Primary Sector", "Age", "Income", "Pre-Screen"]
    out.append(("sql-only worksheet columns", p))

    p = base("ocr column")
    p["sections"][0]["sheet"]["columns"] = ["EntryID", "Headline", "OCR Text"]
    out.append(("OCR Text worksheet column", p))

    p = base("featured, ocr not printed")
    p["sections"][0]["sheet"]["columns"] = ["EntryID", "Headline"]
    out.append(("featured: OCR fetched, not printed", p))

    p = base("worksheet only, no ocr")
    p["sections"][0]["feature"]["enabled"] = False
    p["sections"][0]["sheet"]["columns"] = ["EntryID", "Headline"]
    out.append(("worksheet only: no OCR fetch at all", p))

    p = base("all api columns")
    p["sections"][0]["sheet"]["columns"] = [c["name"] for c in COLUMNS
                                            if c["source"] in ("api", "derived")]
    out.append(("every api + derived column", p))

    p = base("no quarter")
    p["sections"][0]["sheet"]["columns"] = ["EntryID", "Headline"]
    out.append(("no Quarter column", p))

    p = base("post filters")
    p["sections"][0]["search"].update({"company_must_not_match": "Bank of America",
                                       "collapse_repeats": True})
    out.append(("company exclusion + collapse", p))

    p = base("no collapse")
    p["sections"][0]["search"]["collapse_repeats"] = False
    out.append(("no post-filters at all", p))

    p = base("shared tab")
    s2 = new_section("Second")
    s2["sheet"]["tab"] = p["sections"][0]["sheet"]["tab"] = "Shared"
    p["sections"].append(s2)
    out.append(("two sections, one tab", p))

    p = base("feature off")
    p["sections"][0]["feature"]["enabled"] = False
    out.append(("worksheet only, nothing featured", p))

    p = base("overflow cap")
    p["sections"][0]["search"]["row_cap"] = CS.LIMIT_MAX
    p["sections"][0]["search"]["media_channel"] = core_values("media_channel") or ["Email"]
    out.append(("every channel at the row ceiling", p))

    p = base("many filters")
    heavy = {"credit_union": True, "card_network": ["Visa", "MasterCard"],
             "card_level": ["Platinum"], "mailing_type": ["Acquisition"],
             "state": ["Illinois", "New York"], "income": ["$100k-$149k"],
             "age": ["30-39"], "target_market": ["Hispanic"],
             "response_mechanism": ["QR Code"], "communication_type": ["Application"],
             "affinity_category": ["Airline"], "package_type": ["Envelope"],
             "postage": ["Permit #"], "presorted": ["Yes"], "delivery_type":
             ["First Class"], "rewards_emphasis": ["Cash-Back"], "military": True,
             "handwriting": False, "loan_amount_min": 1000}
    p["sections"][0]["search"]["enhanced"] = heavy
    p["sections"][0]["search"]["filters"] = sorted(
        {re.sub(r"_(min|max)$", "", k) for k in heavy})
    out.append(("19 enhanced filters at once", p))

    p = base("fixed range")
    p["window"] = {"mode": "range", "start": "2026-04-01", "end": "2026-06-30"}
    out.append(("fixed date range (a one-off)", p))

    p = base("fixed range weekly")
    p["cadence"] = "week"
    p["window"] = {"mode": "range", "start": "2026-03-02", "end": "2026-03-08"}
    out.append(("fixed range on a weekly report", p))

    p = base("email by variable")
    p["email"] = {"enabled": True, "env_var": "HARBORSTONE_REPORT_TO"}
    out.append(("email to a named variable", p))

    p = base("no downloads, nothing featured, no book")
    p["workbook"]["enabled"] = False
    p["sections"][0]["feature"]["enabled"] = False
    out.append(("nothing to produce at all", p))

    p = base("already sent")
    p["status"] = {"sent": {"at": "2026-08-01T09:00:00", "file": "Selftest.py",
                            "hash": "deadbeef"}, "runs": [], "saved_as": "x"}
    out.append(("carries a sent receipt", p))

    p = base("has run")
    p["status"] = {"sent": None, "saved_as": "x", "runs": [
        {"id": "a" * 12, "mode": "full", "at": "2026-08-20T11:00:00", "rc": 0,
         "stopped": False, "emailed": False, "produced": ["X_Report.pptx"]}]}
    out.append(("has produced deliverables", p))

    # A v2 project, unmigrated, exactly as it sits on disk.
    v2 = {
        "name": "v2 legacy", "client": "Legacy Co", "cadence": "month",
        "anchor": "prior_complete", "window_field": "entry_id",
        "deck": {"enabled": True, "title": "{client} — {period}",
                 "filename": "{client}_{stamp}.pptx", "title_slide": True,
                 "summary_slide": False, "section_headings": False,
                 "closing_slide": True},
        "workbook": {"enabled": True, "filename": "{client}_{stamp}.xlsx"},
        "email": {"enabled": False, "to_addr": "someone@competiscan.com"},
        "home_states": ["Washington", "Oregon"], "notes": "",
        "sections": [{
            "id": "aaaa1111", "title": "Credit Unions", "heading": "",
            "search": {"companies": "Harborstone Credit Union", "sectors": ["Banking"],
                       "channels": ["Email", "Direct Mail"],
                       "keyword": '"new member" or "join today"',
                       "audience": "Consumer", "limit": 200,
                       "only_credit_unions": True,
                       "company_must_match": "Federal",
                       "company_must_not_match": "Chase",
                       "subcategory_must_include": "vehicle financing",
                       "subcategory_must_exclude": "business loan",
                       "collapse_repeats": True, "max_per_creative": 2},
            "sheet": {"enabled": True, "tab": "CUs",
                      "columns": ["EntryID", "Primary Company", "Headline"]},
            "feature": {"enabled": True, "count": 4, "how_to_choose": "Rate offers.",
                        "what_to_say": "Analyst voice.", "callout_limit": 374,
                        "one_per_company": True, "never_reuse": True,
                        "mention_cap": True},
        }],
    }
    out.append(("v2 project, migrated", v2))
    return out


def selftest(live: bool = False, offline: bool = False) -> int:
    bad = 0
    for label, project in _variants():
        v = validate(project)
        try:
            code, _fname = codegen(project)
        except Exception as exc:
            print(f"{label:40} CODEGEN CRASHED: {type(exc).__name__}: {exc}")
            bad += 1
            continue
        notes = []
        try:
            tree = ast.parse(code)
            isms, und = _json_isms(tree), _undefined(tree)
            if isms:
                notes.append(f"JSON-ISM {isms}")
                bad += 1
            if und:
                notes.append(f"UNDEFINED {und}")
                bad += 1
            broken = _guardrails(code, project)
            if broken:
                notes.append("GUARDRAIL " + "; ".join(broken))
                bad += len(broken)
            if not notes:
                notes.append("ok")
        except SyntaxError as exc:
            notes.append(f"SYNTAX line {exc.lineno}: {exc.msg}")
            bad += 1
        print(f"{label:40} {len(code.splitlines()):>4} lines  "
              f"err={v['errors']} warn={v['warnings']}  {', '.join(notes)}")
        for i in v["issues"]:
            if i["level"] == "error":
                print(f"      ERROR {i['msg'][:92]}")
                bad += 1

    print("\n── Every saved project still opens, migrates and generates ──")
    saved = STORE.list()
    if not saved:
        print("  (none on disk)")
    for rec in saved:
        f = rec["name"]
        try:
            raw = rec["raw"]
            if raw is None:
                raise ValueError(rec["error"])
            was = int(raw.get("schema") or 0)
            proj = migrate(raw)
            code, _fn = codegen(proj)
            tree = ast.parse(code)
            isms, und = _json_isms(tree), _undefined(tree)
            broken = _guardrails(code, proj)
            badge = status_badge(proj)
            note = "ok"
            if isms or und or broken:
                note = f"ISMS {isms} UNDEF {und} {'; '.join(broken)}"
                bad += 1
            print(f"  {f:32} v{was}->v{proj['schema']}  "
                  f"{len(proj.get('sections') or []):>2} section(s)  "
                  f"{badge['label']:<20} {note}")
        except Exception as exc:
            print(f"  {f:32} FAILED: {type(exc).__name__}: {exc}")
            bad += 1

    if offline:
        bad += _selftest_offline()

    if live:
        print("\n── Resolving every variant's filters against the archive ──")
        bad += _selftest_live()

    print("\nSELFTEST", "FAILED" if bad else "PASSED")
    return 1 if bad else 0


# ═══════════════════════════════════════════════════════════════════════════════════════
# Offline selftest — RUN every generated pipeline, do not just parse it
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# A parse check proves a file imports. It does not prove that --phase build reads back
# what --phase pick wrote, that a replacement obeys one-per-company, or that a workbook
# comes out the same either way. Those need the file to actually run, so it does — with
# every boundary that leaves this machine replaced by pipelines/mock_archive.py and the
# generated file itself completely untouched.

OFFLINE_DIR = GENERATED_DIR / "_offline"


def _offline_setup() -> Path:
    """A scratch directory Python will import a mock-installing sitecustomize from."""
    import shutil
    if OFFLINE_DIR.exists():
        shutil.rmtree(OFFLINE_DIR, ignore_errors=True)
    (OFFLINE_DIR / "shim").mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(PIPELINES_DIR.parent))
    import pipelines.mock_archive as M  # noqa: E402 — test-only, never imported at boot
    (OFFLINE_DIR / "shim" / "sitecustomize.py").write_text(M.SITECUSTOMIZE,
                                                           encoding="utf-8")
    return OFFLINE_DIR


def _offline_run(project: dict, args: list, tag: str, *, rows: int = 25,
                 capped: bool = False, empty: bool = False,
                 email_to: str = "") -> dict:
    """Generate a pipeline and run it against the mock. Returns what it produced."""
    root = PIPELINES_DIR.parent
    out = OFFLINE_DIR / tag / "output"
    out.mkdir(parents=True, exist_ok=True)
    code, fname = codegen(project)
    target = GENERATED_DIR / f"_offline_{_slug(tag)}_{fname}"
    target.write_text(code, encoding="utf-8")

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(OFFLINE_DIR / "shim"), str(root), env.get("PYTHONPATH", "")])
    env["RS_MOCK_ROOT"] = str(root)
    env["RS_MOCK_ROWS"] = str(rows)
    env["RS_OUTPUT_DIR"] = str(out)
    env["PYTHONIOENCODING"] = "utf-8"
    for flag, on in (("RS_MOCK_CAPPED", capped), ("RS_MOCK_EMPTY", empty)):
        if on:
            env[flag] = "1"
        else:
            env.pop(flag, None)
    if email_to:
        env["RS_EMAIL_TO"] = email_to
    else:
        env.pop("RS_EMAIL_TO", None)

    exe, _why = runner()
    res = subprocess.run([exe, "-u", str(target)] + args, capture_output=True,
                         text=True, encoding="utf-8", errors="replace",
                         cwd=str(root), env=env, timeout=900)
    return {"rc": res.returncode, "out": (res.stdout or "") + (res.stderr or ""),
            "dir": OFFLINE_DIR / tag, "output": out, "target": target}


def _slides(out: Path) -> list:
    """The slide list the run decided on. A .pptx is a zip whose bytes differ between
    two identical builds, so the mock writes the decision itself beside it."""
    for f in sorted(out.glob("*.slides.json")):
        return json.loads(f.read_text(encoding="utf-8"))
    return []


def _xlsx(out: Path) -> dict:
    """{sheet name: sha256 of its rows} for the workbook in `out`.

    Read straight out of the zip with the stdlib, so the Studio keeps its no-dependency
    promise even in its own tests. Sheet CONTENT is compared, never the zip's bytes —
    a zip carries timestamps and two identical workbooks would differ on those.
    """
    import hashlib
    import re as _re
    books = [f for f in sorted(out.glob("*.xlsx"))]
    if not books:
        return {}
    with zipfile.ZipFile(books[0]) as z:
        names = {}
        try:
            wb = z.read("xl/workbook.xml").decode("utf-8", "replace")
            order = _re.findall(r'<sheet[^>]*name="([^"]*)"', wb)
        except KeyError:
            order = []
        shared = z.read("xl/sharedStrings.xml") if "xl/sharedStrings.xml" \
            in z.namelist() else b""
        for i, sheet in enumerate(order, start=1):
            member = f"xl/worksheets/sheet{i}.xml"
            data = z.read(member) if member in z.namelist() else b""
            names[sheet] = hashlib.sha256(data + shared).hexdigest()[:16]
    return names


def _tabs(out: Path) -> int:
    return len(_xlsx(out))


def _selftest_offline() -> int:
    """Run every saved report, prove the two phases agree, prove replace obeys the
    rules, and prove a stored address never reaches a saved project."""
    if PIPELINES_DIR is None:
        print("  ! pipelines/ not found — cannot run the offline selftest.")
        return 1
    bad = 0
    _offline_setup()
    exe, why = runner()
    print(f"\n── Running generated pipelines against the mock ({why}) ──")

    # ── 1. every saved report, end to end ──────────────────────────────────────────
    print(f"{'report':32} {'mode':10} {'rc':>3} {'slides':>7} {'tabs':>5}  database")
    rows = []
    for rec in STORE.list():
        f = rec["name"]
        if rec["raw"] is None:
            print(f"{f:32} LOAD FAILED: {rec['error']}")
            bad += 1
            continue
        proj = migrate(rec["raw"])
        r = _offline_run(proj, [], f"all_{f}")
        db = "Step 2  Worksheet columns from the database" in r["out"]
        n_slides, n_tabs = len(_slides(r["output"])), _tabs(r["output"])
        rows.append((f, r["rc"], n_slides, n_tabs, db))
        print(f"{f:32} {'full':10} {r['rc']:>3} {n_slides:>7} {n_tabs:>5}"
              f"  {'yes' if db else 'no'}")
        if r["rc"] != 0:
            bad += 1
            print("      " + "\n      ".join(r["out"].strip().splitlines()[-6:]))

    # ── 2. the two phases must produce the same deliverables ───────────────────────
    print("\n── Two-phase equivalence: single-shot vs pick + build ──")
    proj = migrate(STORE.read("v3_credit_union_watch") or {})
    a = _offline_run(proj, [], "eq_single")
    state = OFFLINE_DIR / "eq_two" / "state.json"
    b1 = _offline_run(proj, ["--phase", "pick", "--state", str(state)], "eq_two")
    if b1["rc"] != 0 or not state.is_file():
        print("  FAILED: --phase pick did not write a state file")
        print("  " + "\n  ".join(b1["out"].strip().splitlines()[-8:]))
        return bad + 1
    st = json.loads(state.read_text("utf-8"))
    # Approve every id exactly as it stands. Nothing else may differ.
    approved = {s["id"]: list(s.get("picks") or [])
                for s in st["sections"] if s.get("feature")}
    ap = OFFLINE_DIR / "eq_two" / "approved.json"
    ap.write_text(json.dumps(approved, indent=1), encoding="utf-8")
    b2 = _offline_run(proj, ["--phase", "build", "--state", str(state),
                             "--approved", str(ap)], "eq_two")
    sa, sb = _slides(a["output"]), _slides(b1["dir"] / "output")
    xa, xb = _xlsx(a["output"]), _xlsx(b1["dir"] / "output")
    print(f"  single-shot : rc={a['rc']} {len(sa)} slides, {len(xa)} tab(s)")
    print(f"  pick        : rc={b1['rc']} state {state.stat().st_size / 1e6:.2f} MB, "
          f"{sum(len(s.get('records') or []) for s in st['sections'])} records held")
    print(f"  build       : rc={b2['rc']} {len(sb)} slides, {len(xb)} tab(s)")
    if b2["rc"] != 0:
        print("  " + "\n  ".join(b2["out"].strip().splitlines()[-8:]))
    if sa and sa == sb:
        print("  slides       IDENTICAL")
    else:
        print("  slides       DIFFER")
        bad += 1
        for i, (x, y) in enumerate(zip(sa, sb)):
            if x != y:
                print(f"    slide {i}: {json.dumps(x)[:150]}")
                print(f"          vs {json.dumps(y)[:150]}")
                break
    if xa and xa == xb:
        print(f"  workbook     IDENTICAL ({', '.join(xa)})")
    else:
        print(f"  workbook     DIFFER  {xa} vs {xb}")
        bad += 1

    # ── 3. a replacement obeys the selection rules ─────────────────────────────────
    print("\n── Replacement: the rules still apply ──")
    sec = next((s for s in st["sections"]
                if s.get("feature") and len(s.get("picks") or []) > 1), None)
    if sec is None:
        print("  SKIPPED: no section featured more than one piece")
    else:
        by_id = {r["entry_id"]: r for r in sec["records"]}
        picks = list(sec["picks"])
        rejected = picks[0]
        keep = picks[1:]
        used = [e for s in st["sections"] if s["id"] != sec["id"]
                for e in (s.get("picks") or [])]
        env_target = b1["target"]
        root = PIPELINES_DIR.parent
        res = subprocess.run(
            [exe, str(env_target), "--phase", "replace", "--state", str(state),
             "--section", sec["id"], "--keep", ",".join(keep),
             "--reject", rejected, "--used", ",".join(used)],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(root), timeout=120)
        try:
            ans = json.loads([l for l in res.stdout.strip().splitlines() if l][-1])
        except (ValueError, IndexError):
            print(f"  FAILED: replace printed nothing usable: {res.stdout[:200]}"
                  f" {res.stderr[:200]}")
            return bad + 1
        rep = ans.get("replacement") or {}
        eid = rep.get("entry_id")
        print(f"  section      {sec['title']}  (one_per_company="
              f"{sec.get('one_per_company')}, never_reuse={sec.get('never_reuse')})")
        print(f"  rejected     {rejected}  ({by_id.get(rejected, {}).get('company')})")
        print(f"  replacement  {eid}  ({rep.get('company')})   "
              f"{ans.get('remaining')} left in the pool")
        checks = [
            ("comes from the cached pool, not a new search", eid in by_id),
            ("is not the rejected piece", eid != rejected),
            ("is not one already shown on this slide", eid not in keep),
        ]
        if sec.get("never_reuse"):
            checks.append(("is not on another section's slide", eid not in used))
        if sec.get("one_per_company"):
            kept_co = {(by_id.get(e, {}).get("company") or "").lower() for e in keep}
            checks.append(("honours one-per-company",
                           (rep.get("company") or "").lower() not in kept_co))
        for label, ok in checks:
            print(f"    {'PASS' if ok else 'FAIL'}  {label}")
            if not ok:
                bad += 1

        # And rejecting again must never hand back something already rejected.
        res2 = subprocess.run(
            [exe, str(env_target), "--phase", "replace", "--state", str(state),
             "--section", sec["id"], "--keep", ",".join(keep),
             "--reject", ",".join([rejected, eid]), "--used", ",".join(used)],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(root), timeout=120)
        try:
            ans2 = json.loads([l for l in res2.stdout.strip().splitlines() if l][-1])
            second = (ans2.get("replacement") or {}).get("entry_id")
        except (ValueError, IndexError):
            second = None
        ok = second not in (rejected, eid)
        print(f"    {'PASS' if ok else 'FAIL'}  a second rejection skips both rejects"
              f" (got {second})")
        if not ok:
            bad += 1

    # ── 3b. a swap spends the model's own reserve before it walks the archive ─────
    print("\n── Swaps come from the model's reserve first ──")
    if sec is not None:
        res = sec.get("reserve") or []
        picks_n = len(sec.get("picks") or [])
        print(f"  slide        {picks_n} piece(s)")
        print(f"  reserve      {len(res)} piece(s), ranked below them by the model")
        good = len(res) > 0 and picks_n > 0
        print(f"    {'PASS' if good else 'FAIL'}  the model was asked for more than the"
              f" slide holds, and the extras were kept")
        bad += not good

        def swap_run(state_path, section_id, keep, rejected, used_ids):
            r = subprocess.run(
                [exe, str(b1["target"]), "--phase", "replace",
                 "--state", str(state_path), "--section", section_id,
                 "--keep", ",".join(keep), "--reject", ",".join(rejected),
                 "--used", ",".join(used_ids)],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=str(root), timeout=120)
            try:
                return json.loads([x for x in r.stdout.strip().splitlines() if x][-1])
            except (ValueError, IndexError):
                return {}

        # With one_per_company ON, a reserve pick whose company is already on the slide
        # is legitimately skipped — the rule outranks the ranking. So the ORDER is
        # tested against a copy with that rule off, where nothing else can intervene,
        # and the rule's own effect is checked separately below.
        loose = json.loads(state.read_text("utf-8"))
        for x in loose["sections"]:
            x["one_per_company"] = False
        loose_path = OFFLINE_DIR / "eq_two" / "state_loose.json"
        loose_path.write_text(json.dumps(loose), encoding="utf-8")

        keep2, rejected2, sources = list(sec["picks"]), [], []
        for _ in range(len(res) + 2):
            drop = keep2[0]
            rejected2.append(drop)
            rest = keep2[1:]
            ans2 = swap_run(loose_path, sec["id"], rest, rejected2, used)
            rep2 = (ans2.get("replacement") or {}).get("entry_id")
            if not rep2:
                sources.append("exhausted")
                break
            sources.append("reserve" if ans2.get("from_reserve") else "archive")
            keep2 = rest + [rep2]

        print(f"  swaps        {' -> '.join(sources)}")
        head = sources[:len(res)]
        good = bool(head) and all(x == "reserve" for x in head)
        print(f"    {'PASS' if good else 'FAIL'}  the first {len(res)} swap(s) all come"
              f" from the model's reserve, in its own ranking order")
        bad += not good
        tail = sources[len(res):]
        good = all(x in ("archive", "exhausted") for x in tail)
        print(f"    {'PASS' if good else 'FAIL'}  and only once it is spent does it fall"
              f" back to archive order ({', '.join(tail) or 'nothing left to test'})")
        bad += not good

        # And with the rule back on: the reserve is still preferred, but never at the
        # cost of putting two pieces from one company on the same slide.
        keep3 = list(sec["picks"])[1:]
        ans3 = swap_run(state, sec["id"], keep3, [sec["picks"][0]], used)
        rep3 = ans3.get("replacement") or {}
        by_id2 = {r["entry_id"]: r for r in sec["records"]}
        kept_co = {(by_id2.get(e, {}).get("company") or "").lower() for e in keep3}
        good = bool(ans3.get("from_reserve"))
        print(f"    {'PASS' if good else 'FAIL'}  with one-per-company on, the first"
              f" swap still comes from the reserve")
        bad += not good
        good = (rep3.get("company") or "").lower() not in kept_co
        print(f"    {'PASS' if good else 'FAIL'}  and it is not from a company already"
              f" on the slide ({rep3.get('company')})")
        bad += not good

    # ── 4. a capped count is reported as a lower bound, never as a fact ────────────
    print("\n── A capped count is a lower bound ──")
    tiny = new_project("capped")
    tiny["client"] = "Capped"
    tiny["deck"]["enabled"] = False
    # Capped AND nothing in the window: the case where a true zero would be a lie.
    r = _offline_run(tiny, ["--only", "search"], "capped", capped=True, empty=True)
    at_least = "at least" in r["out"]
    suspect = "SUSPECT" in r["out"]
    print(f"    {'PASS' if at_least else 'FAIL'}  the total is printed as \"at least N\"")
    print(f"    {'PASS' if suspect else 'FAIL'}  cap-hit with nothing in the window is"
          f" flagged SUSPECT, not reported as a true zero")
    bad += (not at_least) + (not suspect)

    # ── 5. a run-time address delivers, and is stored nowhere ──────────────────────
    print("\n── A run-time email address is held for one run and no longer ──")
    one = migrate(STORE.read("v3_credit_union_watch") or {})
    r = _offline_run(one, [], "email", email_to="colleague@competiscan.com")
    rec = r["output"] / "_email.json"
    delivered = rec.is_file() and "colleague@competiscan.com" in rec.read_text("utf-8")
    print(f"    {'PASS' if delivered else 'FAIL'}  the run delivered to the address"
          f" given at run time")
    blob = json.dumps(one)
    clean = "colleague@competiscan.com" not in blob
    print(f"    {'PASS' if clean else 'FAIL'}  the address is nowhere in the project"
          f" object the Studio holds")
    src = r["target"].read_text("utf-8")
    clean2 = "colleague@competiscan.com" not in src
    print(f"    {'PASS' if clean2 else 'FAIL'}  the address is nowhere in the generated"
          f" pipeline")
    bad += (not delivered) + (not clean) + (not clean2)

    # ── 6. and the Studio itself, over HTTP, exactly as the browser drives it ──────
    #
    # A generated pipeline can be perfect and the tool still useless: pressing Run has
    # to reach the right stage of it, mode 3 has to actually pause and actually resume,
    # a download must not be walkable out of, Stop must kill a live process, and a
    # run-time address must be gone afterwards. None of that is visible from codegen.
    print("\n── Driving the Studio over HTTP ──")
    res = subprocess.run([sys.executable, str(PIPELINES_DIR / "studio_tests.py")],
                         capture_output=True, text=True, encoding="utf-8",
                         errors="replace", cwd=str(PIPELINES_DIR.parent), timeout=1800)
    for line in (res.stdout or "").splitlines():
        if line.strip():
            print(line if line.startswith(" ") else f"  {line}")
    if res.returncode:
        bad += 1
        print("  " + "\n  ".join((res.stderr or "").strip().splitlines()[-6:]))

    print("\n  The browser half of the page is a separate file, because a JavaScript")
    print("  error is invisible to every Python test here and takes the whole page")
    print("  down. Start the Studio, then:")
    print("      npm install jsdom && node pipelines/studio_dom_test.js")
    return bad


def _selftest_live() -> int:
    """Send each distinct filter body to the archive as a count probe.

    codegen only proves the file parses. This proves the archive ACCEPTS the filters —
    the failure this catches is unknown_filter_value, which a syntax check cannot see and
    which otherwise surfaces three minutes into someone's Test run.
    """
    bad, seen = 0, set()
    start, end = window({"cadence": "month", "anchor": "prior_complete"})
    for label, project in _variants():
        p = migrate(project)
        for s in p.get("sections") or []:
            search = s.get("search") or {}
            for channel in (search.get("media_channel") or [None])[:1]:
                body = CS.build_body(search, channel=channel,
                                     date_field=p.get("date_field") or "search_date",
                                     date_from=start, date_to=end)
                key = json.dumps(body, sort_keys=True)
                if key in seen:
                    continue
                seen.add(key)
                what = f"{label} / {s.get('title')}"
                try:
                    res = CS.count(body)
                    print(f"  {what[:52]:52} ok    total={res.get('total')}")
                except CS.ApiError as exc:
                    print(f"  {what[:52]:52} {exc.code}: {exc.hint()[:70]}")
                    bad += 1
    print(f"  {len(seen)} distinct bodies, {CS.CALLS['n']} quota units spent")
    if bad:
        print("  A failure here is real: unknown_filter_value means the archive rejects a")
        print("  value the Studio was willing to emit. A parse check cannot see that.")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Pipelines Studio v3 — build a Competiscan trend report without "
                    "writing code, on the Platform API")
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--live", action="store_true",
                    help="With --selftest: also resolve every filter set for real.")
    ap.add_argument("--offline", action="store_true",
                    help="With --selftest: also RUN every generated pipeline end to "
                         "end against pipelines/mock_archive.py — no archive, no "
                         "Bedrock, no tunnel, no deck builder, no email.")
    args = ap.parse_args()

    if args.selftest:
        return selftest(live=args.live, offline=args.offline)

    print("Pipelines Studio v3")
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  project root : {PIPELINES_DIR.parent if PIPELINES_DIR else '(not found)'}")
    print(f"  pipelines/   : {PIPELINES_DIR or '(not found — Test disabled)'}")
    print(f"  writes to    : {GENERATED_DIR}")
    print(f"  run files    : {RUNS_DIR}")
    print(f"  archive      : {CS.BASE}")
    exe, why = runner()
    if "no interpreter found" in why:
        print("")
        print(f"  ! No Python found with {', '.join(RUNNER_NEEDS)} installed, so"
              f" Test will fail on an import.")
        print(f"    Set PIPELINES_PYTHON to an interpreter that has them, or pip"
              f" install them into {sys.executable}")
    else:
        print(f"  runs test as : {exe}  ({why})")
    cat = catalog()
    if cat.get("source") == "error":
        print(f"\n  ! The filter vocabulary could not be loaded: {cat.get('error')}")
        print("    Editing still works; filter values will not be checked.")
    else:
        n = len(CS.flat_filters(cat))
        print(f"  filters      : {n} enhanced + {len(cat.get('core') or {})} core "
              f"({cat.get('source')})")
    if PIPELINES_DIR is None:
        print("\n  ! report_lib.py was not found next to this script, so Test cannot run.")
        print("    Put pipeline_studio3.py in pipelines/, beside report_lib.py.")
    print(f"\n  open http://{args.host}:{args.port}\n")
    try:
        ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
    return 0


if __name__ == "__main__":
    sys.exit(main())
