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

    And the one architectural rule: Test does NOT run an interpreter. It generates the
    .py and runs THAT file, so what a researcher tests is exactly what Engineering
    deploys.

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
    python pipelines/pipeline_studio3.py --selftest --live   # also resolve every filter

WHERE THINGS GO
    Generated pipelines  ->  <project_root>/pipelines/generated/
    Saved projects       ->  <project_root>/pipelines/generated/_projects/
    Cached vocabulary    ->  <project_root>/pipelines/generated/_cache/filters.json

WHAT IT STILL WILL NOT DO
    It does not invent bespoke logic. Anything the four steps cannot express goes in the
    "Notes for Engineering" box, which is lifted verbatim into the generated file's
    docstring as a to-do. A generated pipeline is a correct, house-style DRAFT.
"""

from __future__ import annotations

import argparse
import ast
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
SCHEMA = 3

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
        "cadence": "month",
        "anchor": "prior_complete",
        "date_field": "search_date",
        "deck": {
            "enabled": True,
            "title": "{client} — {period}",
            "filename": "{client}_Report_{stamp}.pptx",
            "title_slide": True, "summary_slide": False,
            "section_headings": False, "closing_slide": True,
        },
        "workbook": {"enabled": True, "filename": "{client}_Data_{stamp}.xlsx"},
        "email": {"enabled": False, "to_addr": ""},
        "notes": "",
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


TEMPLATES = {
    "blank": ("Start from scratch", new_project),
    "example": ("Example — credit union monthly", _example_project),
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
    if int(p.get("schema") or 0) >= SCHEMA:
        for s in p.get("sections") or []:
            _sync_filters(s)
        return p

    carried: list[str] = []
    p = json.loads(json.dumps(p))  # never mutate the caller's dict
    p["schema"] = SCHEMA
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
        _sync_filters(s)
        fe = s.get("feature") or {}
        if "mention_cap" in fe:
            # v2 phrased a capped search as "at least N". The API gives an exact total,
            # so the choice is now simply whether to state the count at all.
            fe["mention_total"] = bool(fe.pop("mention_cap"))

    if carried:
        note = "Carried over from the v2 project:\n" + "\n".join(f"  - {c}" for c in carried)
        p["notes"] = (str(p.get("notes") or "").rstrip() + "\n\n" + note).strip()
    return p


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
        addr = str(em.get("to_addr") or "").strip()
        if not addr:
            err("Enter the email address to send the files to.")
        elif not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", addr):
            err(f'"{addr}" does not look like a valid email address.')

    if str(p.get("notes") or "").strip():
        warn("This report has notes for Engineering, so it needs hand work before it is "
             "production-ready.")

    if PIPELINES_DIR is None:
        warn("report_lib.py was not found next to this script, so Test cannot run. Put "
             "pipeline_studio3.py in pipelines/, beside report_lib.py.")

    # Quota is per calendar month and every request counts, errors included. Two per
    # section x channel per window slice: one probe, one fetch.
    calls = sum(len((s.get("search") or {}).get("media_channel") or []) for s in sections)
    return {
        "issues": issues,
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
    """(start, end) for the report's cadence and anchor. The Studio computes this so a
    Preview covers exactly the dates the generated pipeline will, and the generated file
    carries the same logic rather than importing it."""
    from datetime import date, timedelta
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
    return {
        "client": str(p.get("client") or "Report").strip() or "Report",
        "cadence": p.get("cadence") or "month",
        "anchor": p.get("anchor") or "prior_complete",
        "date_field": p.get("date_field") or "search_date",
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
    if g["notes"]:
        w("┌" + "─" * 74 + "┐")
        w("│ NOTES FOR ENGINEERING — not implemented below. Please wire these by hand. │")
        w("└" + "─" * 74 + "┘")
        for line in g["notes"].splitlines():
            if line.strip():
                O.extend(_wrap(line.strip()))
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
        w(f"  * Deliverables are emailed to {email.get('to_addr')} when the run finishes.")
    w('"""')
    w("")

    # ── imports ─────────────────────────────────────────────────────────────────────
    w("import argparse")
    w("import os")
    if g["excludes"] or g["collapses"]:
        w("import re")
    w("import sys")
    w("from datetime import date, timedelta")
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
    if g["prints_ocr"]:
        w("# The OCR Text column reads one piece per request, so a wide tab could spend")
        w("# hundreds of quota units on a column nobody reads to the end. Raise it if you")
        w("# genuinely need the text for every row.")
        w("OCR_ROW_CAP  = 150")
    w('OUTPUT_DIR   = PROJECT_ROOT / "output"')
    if email.get("enabled"):
        addr = str(email.get("to_addr") or "").strip()
        w("# Opt-in: RS_EMAIL_TO overrides this at run time; blank means nothing is")
        w("# emailed.")
        w(f'EMAIL_TO     = os.environ.get("RS_EMAIL_TO") or {_lit(addr)} or None')
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
    modes = ["search"] + (["excel"] if tabs else []) + \
        (["deck"] if g["deck_on"] else []) + ["all"]
    w(f'    p.add_argument("--only", default="all", choices={_lit(modes)},')
    w('                   help="Stop after a stage — cheap iteration while testing.")')
    w('    p.add_argument("--limit", type=int, default=None,')
    w('                   help="Cap rows per channel (small = fast test).")')
    w("    return p.parse_args()")
    w("")
    w("")
    w("def _window():")
    w('    """(start, end). The env vars win; otherwise cadence and anchor decide.')
    w("    prior_complete is reproducible: running it again tomorrow covers the same")
    w('    dates."""')
    w("    if PERIOD_START:")
    w("        s = date.fromisoformat(PERIOD_START)")
    w("        if PERIOD_END:")
    w("            return s, date.fromisoformat(PERIOD_END)")
    w('        return s, (s + timedelta(days=7) if CADENCE == "week" else _month_end(s))')
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
    w("    return rows, notes, resolved or {}, archive_total")
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
        w('    "near-duplicates of the same creative.\\n\\n"')
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
        w("def _choose(sec, records):")
        w("    if not records:")
        w('        return {"entry_ids": []}')
        w('    guidance = sec["how_to_choose"] or "No specific guidance was given."')
        w('    system = CHOOSE_SYSTEM.replace("{guidance}", guidance)')
        w('    prompt = (f\'Choose up to {sec["count"]} pieces for the "{sec["title"]}"\'')
        w('              f\' slide.\\n\\nCANDIDATES\\n{_candidates(records)}\')')
        w("    try:")
        w("        return L.extract_json(L.call_claude(system, prompt))")
        w("    except Exception as exc:")
        w('        return {"error": str(exc), "entry_ids": []}')
        w("")
        w("")
        w("def _writeup(sec, chosen, archive_total):")
        w("    if not chosen:")
        w('        return {"callout": ""}')
        w('    style = sec["what_to_say"] or "Plain analyst prose."')
        w('    system = (WRITEUP_SYSTEM.replace("{limit}", str(sec["callout_limit"]))')
        w('              .replace("{style}", style))')
        w("    # archive_total is what the ARCHIVE holds for this section and period,")
        w("    # counted by the archive itself BEFORE the row cap trimmed what we")
        w("    # actually pulled down. It is the only count that reconciles against")
        w("    # PowerSearch, so it is the only one a client deck may state. v2 had to")
        w('    # hedge with "at least N" whenever a call hit its cap; this does not.')
        w('    if sec["mention_total"]:')
        w('        note = (f"{archive_total} piece(s) were captured in this "')
        w('                f"period.")')
        w("    else:")
        w('        note = f"{len(chosen)} piece(s) are featured on this slide."')
        w('    prompt = (f\'Slide: "{sec["title"]}". {note}\\n\\n\'')
        w('              f\'FEATURED PIECES\\n{_candidates(chosen, read=True)}\')')
        w("    try:")
        w("        return L.extract_json(L.call_claude(system, prompt))")
        w("    except Exception as exc:")
        w('        return {"error": str(exc), "callout": ""}')
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

def _emit_main(p: dict, g: dict, w) -> None:
    """The pipeline itself: search, database, filters, workbook, model calls, deck."""
    tabs, deck, email = g["tabs"], g["deck"], g["email"]
    any_db = g["any_sql"]

    w("# ── Pipeline " + "─" * 65)
    w("def main() -> int:")
    w("    args = _parse_args()")
    w("    start, end = _window()")
    if g["cadence"] == "week":
        w('    period_label = f"{end:%B} {_ordinal(end.day)}, {end.year}"')
    else:
        w('    period_label = f"{start:%B} {start.year}"')
    w('    stamp = end.strftime("%Y%m%d")')
    w('    mmddyy = end.strftime("%m%d%y")')
    w('    month_year = f"{start:%B}{start.year}"')
    w("    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)")
    w('    print(f"{CLIENT} — {period_label}")')
    w('    print(f"  window {start} .. {end}  (bounded server-side by {DATE_FIELD})")')
    w('    print(f"  mode --only={args.only}"')
    w('          + (f" --limit={args.limit}" if args.limit else ""))')
    w("")
    w("    # ── Step 1 — search: one probe + one fetch per section x channel ────────")
    w('    calls = sum(len(s["search"]["media_channel"]) for s in SECTIONS)')
    w('    print(f"\\nStep 1  Searching ({calls} section x channel, 2 requests each)…")')
    w("    found = {}")
    w("    for sec in SECTIONS:")
    w('        cap = min(args.limit or sec["search"]["row_cap"], sec["search"]["row_cap"])')
    w("        records, notes, resolved, archive = [], [], {}, 0")
    w("        print(f\"   {sec['title']}\")")
    w('        for channel in sec["search"]["media_channel"]:')
    w("            try:")
    w("                rows, ns, res, at = _collect(sec, channel, start, end, cap)")
    w("            except CS.ApiError as exc:")
    w("                # quota_exceeded is the only error _collect re-raises. Nothing")
    w("                # downstream can succeed either, so stop while the numbers are")
    w("                # still honest.")
    w('                print(f"\\nERROR: {exc.hint()}")')
    w("                return 1")
    w('            print(f"      {channel[:26]:26} {len(rows):>5} of {at:>6} in the archive")')
    w("            records.extend(rows)")
    w("            notes.extend(ns)")
    w("            resolved = resolved or res")
    w("            archive += at")
    w("        for note in notes:")
    w('            print(f"      ! {note}")')
    w("        in_window = _dedup(records)")
    w("        if len(in_window) != len(records):")
    w('            print(f"      de-duplicated: {len(records)} -> {len(in_window)}")')
    w("        # Guardrail 7: a sector matches every node beneath it, so print what the")
    w("        # names actually became before anyone trusts the count.")
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
    w('                            "fetched": len(in_window), "archive_total": archive}')
    w("")
    w('    if not any(v["records"] for v in found.values()):')
    w('        print("\\nERROR: every section came back empty. Check the filters against"')
    w('              " PowerSearch — an empty report is more likely a wrong filter than a"')
    w('              " quiet month. Aborting rather than shipping it.")')
    w("        return 1")
    w("")

    # sql_rows is always defined: the workbook indexes it whether or not anything
    # filled it. Step 2 runs when a worksheet column needs the database, Step 2b
    # when one needs the scanned text — those are now separate sources, so either
    # can happen without the other.
    w("    sql_rows = {}")
    if g["any_sql"] or g["prints_ocr"]:
        w("")
    if g["any_sql"]:
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
        w("")
    if g["prints_ocr"]:
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
        w("")

    if g["excludes"] or g["collapses"]:
        w("    # ── Step 3 — the narrowings the archive cannot express ──────────────────")
        w('    print("\\nStep 3  Post-filters…")')
        w("    for sid, v in found.items():")
        w('        sec = v["sec"]')
        w('        if not (sec["search"]["company_must_not_match"]')
        w('                or sec["search"]["collapse_repeats"]):')
        w("            continue")
        w("        print(f\"   {sec['title']}\")")
        w('        v["records"] = _filter(v["records"], sec)')
        w("")

    w('    if args.only == "search":')
    w('        print("\\n── Counts (check these against PowerSearch) ──")')
    w("        for sid, v in found.items():")
    w('            kept, arch = len(v["records"]), v["archive_total"]')
    w("            extra = \"\"")
    w('            fetched = v["fetched"]')
    w("            if kept != arch:")
    w('                extra = f"   fetched {fetched}, kept {kept}"')
    w("            print(f\"   {v['sec']['title'][:38]:38} {arch:>7} in the archive{extra}\")")
    w("        return 0")
    w("")

    if tabs:
        w("    # ── Step 4 — the workbook ───────────────────────────────────────────────")
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
        w('                 .replace("{stamp}", stamp).replace("{mmddyy}", mmddyy)')
        w('                 .replace("{month_year}", month_year)')
        w('                 .replace("{period}", period_label))')
        w("    xlsx_path = L.write_workbook(OUTPUT_DIR / xlsx_name, specs)")
        w('    print(f"        saved {xlsx_path}")')
        w("")
        w('    if args.only == "excel":')
        w("        return 0")
        w("")
    else:
        w("    xlsx_path = None")
        w("")

    if g["featured"]:
        w("    # ── Step 5 — choose what to feature (parallel model calls) ──────────────")
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
        w("    # The model suggests; Python decides. pick_ids drops anything invented and")
        w("    # tops the list up from the real pool.")
        w("    used, final = set(), {}")
        w("    for sec, choice in zip(picked, choices):")
        w('        records = found[sec["id"]]["records"]')
        w("        choice = choice if isinstance(choice, dict) else {}")
        w('        if "error" in choice:')
        w("            print(f\"   ! {sec['title']}: choosing failed — {choice['error']}\")")
        w('        ids = L.pick_ids(choice.get("entry_ids"), records, sec["count"],')
        w('                         max_ids=sec["count"],')
        w('                         exclude=used if sec["never_reuse"] else None)')
        w('        if sec["one_per_company"]:')
        w('            by_id = {r["entry_id"]: r for r in records}')
        w("            seen_co, kept = set(), []")
        w("            for eid in ids:")
        w('                co = (by_id.get(eid, {}).get("company") or eid).lower()')
        w("                if co not in seen_co:")
        w("                    seen_co.add(co)")
        w("                    kept.append(eid)")
        w("            if len(kept) < len(ids):")
        w("                print(f\"   ! {sec['title']}: dropped {len(ids) - len(kept)}\"")
        w('                      f" same-company pick(s)")')
        w("            ids = kept")
        w("        used.update(ids)")
        w('        final[sec["id"]] = ids')
        w("        print(f\"   {sec['title'][:34]:34} {len(ids)} piece(s) {ids}\")")
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
        w('        return _writeup(sec, chosen, found[sec["id"]]["archive_total"])')
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
        w("    summary = _summary(period_label, lines)")
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
            w('                   "data": {"title": CLIENT, "date": period_label}})')
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
            w('        "title1": SUMMARY_TITLE1.replace("{period}", period_label),')
            w('        "text1": sum1,')
            w('        "title2": SUMMARY_TITLE2.replace("{period}", period_label),')
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
        w('                  .replace("{period}", period_label))')
        w("    result = build_deck_default(deck_title=deck_title, slides=slides)")
        w("")
        w(f'    pptx_name = ({_lit(deck.get("filename") or "{client}_{stamp}.pptx", 17)}')
        w('                 .replace("{client}", CLIENT.replace(" ", "_"))')
        w('                 .replace("{stamp}", stamp).replace("{mmddyy}", mmddyy)')
        w('                 .replace("{month_year}", month_year)')
        w('                 .replace("{period}", period_label))')
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

    if email.get("enabled"):
        w("    # ── Email — opt-in only. A real send reaches a real inbox, so it happens")
        w("    #    only when EMAIL_TO resolves to an address. ──────────────────────────")
        w("    if EMAIL_TO:")
        w('        print(f"\\nEmailing deliverables to {EMAIL_TO}…")')
        if g["deck_on"] and tabs:
            attach = "[a for a in (saved, xlsx_path) if a]"
        elif g["deck_on"]:
            attach = "[saved]"
        else:
            attach = "[xlsx_path]"
        w("        res = L.notify_report_ready(")
        w('            report_name=f"{CLIENT} report", period_label=period_label,')
        w(f"            attachment_paths={attach}, to_addr=EMAIL_TO)")
        w('        if res.get("status") == "sent":')
        w("            print(f\"   sent (message_id={res.get('message_id')})\")")
        w("        else:")
        w("            print(f\"   !! email FAILED: {res.get('error')} — the files are\"")
        w('                  f" still saved locally, nothing is lost")')
        w("    else:")
        w('        print("\\nSkipped emailing — no address is configured."')
        w('              " Files are saved locally.")')
        w("")

    if g["notes"]:
        w('    print("\\n!! This report has NOTES FOR ENGINEERING in its docstring —"')
        w('          " bespoke work is still needed before it is production-ready.")')
    w('    print("\\nDone.")')
    w("    return 0")
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


def _prune(keep: int = 5) -> None:
    try:
        old = sorted(GENERATED_DIR.glob("_test_*.py"), key=lambda q: q.stat().st_mtime)
        for q in old[:-keep]:
            q.unlink(missing_ok=True)
    except OSError:
        pass


def start_run(project, mode, limit) -> str:
    run_id = uuid.uuid4().hex[:12]
    code, fname = codegen(project)
    ast.parse(code)  # surface a generator bug here, not as a subprocess traceback
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    _prune()
    target = GENERATED_DIR / f"_test_{run_id}_{fname}"
    target.write_text(code, encoding="utf-8")

    with RUNS_LOCK:
        RUNS[run_id] = {"lines": [], "done": False, "rc": None}

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
        cmd = [exe, "-u", str(target), "--only", mode]
        if limit:
            cmd += ["--limit", str(limit)]
        log(f"$ {' '.join(cmd)}")
        log("")
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    encoding="utf-8", errors="replace",
                                    cwd=str(PIPELINES_DIR.parent))
            for line in proc.stdout:
                log(line.rstrip("\n"))
            proc.wait()
            rc = proc.returncode
        except Exception as exc:
            log(f"RUNNER ERROR: {exc}")
            rc = 1
        with RUNS_LOCK:
            RUNS[run_id]["done"], RUNS[run_id]["rc"] = True, rc

    threading.Thread(target=worker, daemon=True).start()
    return run_id


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
        f"This one searches through the Competiscan Platform API (pipelines/cs_api.py), "
        f"not the old mcp_serverv4 archive tool.\n\n"
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
<html><head><meta charset="utf-8"><title>Pipelines Studio v3</title>
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
padding:16px}
#gen:not(:empty){margin-bottom:12px}
#stage{flex:1;overflow:auto;padding:20px 24px}
.wrapper{max-width:900px;margin:0 auto}

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

#logbar{display:flex;align-items:center;gap:8px;padding:6px 16px;background:#1d2233;
color:#8b93a6;font-size:12px;border-top:1px solid #2a3145}
#log{height:176px;background:#141824;color:#dfe4f0;overflow:auto;padding:11px 16px;
font:12px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap}
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
.plist li b{flex:1;font-weight:600}
pre.code{background:#141824;color:#dfe4f0;padding:13px;border-radius:8px;overflow:auto;
max-height:48vh;font:11.5px/1.55 ui-monospace,Menlo,monospace;margin-top:8px}
</style></head><body>

<div id="top">
  <img id="brandLogo" src="/logo.jpg" alt="Pipelines">
  <span class="logo">Pipelines Studio</span><span class="v">v3</span>
  <input id="rname" placeholder="Report name">
  <div class="sp"></div>
  <div id="health"></div>
  <select id="mode" style="width:auto">
    <option value="search">Test the searches</option>
    <option value="excel">Test the workbook</option>
    <option value="deck">Test everything</option>
  </select>
  <button class="primary" onclick="runTest()">Test</button>
  <button onclick="openExport()">Send to Eng.</button>
  <button class="ghost" onclick="show('Projects')">Projects</button>
</div>

<div id="body">
  <div id="pane"><div id="gen"></div><div id="settings"></div></div>
  <div id="stage"><div class="wrapper" id="sections"></div></div>
</div>

<div id="logbar"><span>Output</span><div class="sp"></div>
  <button class="ghost" style="color:#8b93a6" onclick="clearLog()">clear</button></div>
<div id="log"><span class="d">Describe the report on the left, add sections in the middle,
then press Test. Preview count on a section checks its filters against the archive
without generating anything.</span></div>

<div class="overlay" id="ovProjects"><div class="panel">
  <header><b>Projects</b><button class="ghost" onclick="hide('Projects')">close</button></header>
  <div class="content">
    <div class="f"><label>Start something new</label>
      <div class="row"><select id="tplPick"></select>
      <button onclick="newFrom()" style="flex:0 0 auto">Create</button></div></div>
    <h2 class="mt">Saved reports</h2>
    <ul class="plist" id="savedList"></ul>
    <div class="hint">A report saved by Studio v2 opens here too — its "credit unions
      only" regex becomes the credit_union filter, and anything with no filter
      equivalent is written into Notes for Engineering.</div>
  </div>
  <footer><input id="saveAs" placeholder="Save the current report as…" style="flex:1">
    <button class="primary" onclick="saveProject()">Save</button></footer>
</div></div>

<div class="overlay" id="ovExport"><div class="panel">
  <header><b>Send to Engineering</b><button class="ghost" onclick="hide('Export')">close</button></header>
  <div class="content" id="exportBody"></div>
  <footer><button class="ghost" onclick="hide('Export')">Cancel</button>
    <button class="primary" onclick="sendToEngineering()">Send to Engineering</button></footer>
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
let SPEC=null,P=null,ISSUES=[],OPEN={},poll=null,deb=null;
let FLAT={},TAXO={},PICKFOR=null,PVW={},FSEARCH={},LOOKRES={},lookDeb={};
const $=s=>document.querySelector(s);
const esc=s=>String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;")
  .replace(/"/g,"&quot;");
/* JSON safe to sit inside an HTML attribute — filter names and option values are
   archive data, and several of them carry quotes, slashes and ampersands. */
const jq=o=>JSON.stringify(o).replace(/"/g,"&quot;");
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
  $("#tplPick").innerHTML=SPEC.templates.map(t=>
    `<option value="${t.key}">${esc(t.label)}</option>`).join("");
  P=(await (await fetch("/api/template?name=example")).json()).project;
  await prefetchTaxo();
  render(); refreshSaved();
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
  const df=SPEC.date_fields.map(f=>`<option value="${f.key}"${
    P.date_field===f.key?" selected":""}>${esc(f.label)}</option>`).join("");
  const dnote=(SPEC.date_fields.find(f=>f.key===P.date_field)||{}).note||"";
  $("#settings").innerHTML=`
  <h2>The report</h2>
  <div class="f"><label>Client</label>
    <input value="${esc(P.client)}" oninput="setP('client',this.value)"
      placeholder="e.g. Harborstone"></div>
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
      covers the same dates.</div></div>
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
    onchange="setE('enabled',this.checked)"><span>Email the files</span></label>
  ${e.enabled?`<div class="f"><label>Send to</label>
    <input value="${esc(e.to_addr)}" oninput="setE('to_addr',this.value)"
      placeholder="name@company.com">
    <div class="hint">This address is saved with the project and baked into the generated
      pipeline. Leave it blank to skip emailing.</div></div>`:""}

  <h2 class="mt">Notes for Engineering</h2>
  <div class="f"><textarea oninput="setP('notes',this.value)"
    placeholder="Anything this tool cannot express — an unusual split, a one-off rule. It is copied into the exported file as a to-do.">${esc(P.notes)}</textarea></div>`;
}
function setP(k,v){P[k]=v;soft(k==="date_field"||k==="cadence")}
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
    ISSUES=d.issues||[];
    const g=[];
    g.push(d.errors?`<span class="pill err">${d.errors} to fix</span>`
      :`<span class="pill ok">ready</span>`);
    if(d.warnings)g.push(`<span class="pill wr">${d.warnings} to look at</span>`);
    g.push(`<span class="pill dim">${d.api_calls} archive requests / run</span>`);
    $("#health").innerHTML=g.join("");
    /* One container, always assigned. Anything that only ADDS a node has to have an
       equally reliable way of taking it away, and the old version did not: it left the
       last message it rendered on screen even once the issue was gone. */
    $("#gen").innerHTML=ISSUES.filter(x=>!x.section).map(m=>
      `<div class="msg ${m.level==="error"?"error":"warn"}">${esc(m.msg)}</div>`).join("");
    renderSections();
  },240);
}

/* ── test ────────────────────────────────────────────────────────────────── */
function log(t,cls){
  const el=$("#log");
  el.insertAdjacentHTML("beforeend",`<span class="${cls||""}">${esc(t)}</span>\n`);
  el.scrollTop=el.scrollHeight;
}
function clearLog(){$("#log").innerHTML=""}
function cls(line){
  if(/^ERROR|RUNNER ERROR|Traceback|^\s*!!/.test(line))return "e";
  if(/^\s*!|SUSPECT|WARN/.test(line))return "w";
  if(/^\s*(Deck|Excel):|saved |^Done\.|sent \(/.test(line))return "o";
  if(/^\$ |^Step |^── /.test(line))return "d";
  return "";
}
async function runTest(){
  clearLog();
  const mode=$("#mode").value;
  log(`Generating the pipeline and running it (--only ${mode})…`,"d");
  const r=await fetch("/api/test",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({project:P,mode:mode,limit:mode==="search"?null:40})});
  const d=await r.json();
  if(d.error){log("ERROR "+d.error,"e");return}
  let seen=0;
  clearInterval(poll);
  poll=setInterval(async()=>{
    const s=await (await fetch("/api/test/status?id="+d.run_id)).json();
    for(const line of (s.lines||[]).slice(seen))log(line,cls(line));
    seen=(s.lines||[]).length;
    if(s.done){
      clearInterval(poll);
      log("",""); log(s.rc===0?"Finished cleanly.":`Exited with code ${s.rc}.`,
        s.rc===0?"o":"e");
    }
  },700);
}

/* ── projects and export ─────────────────────────────────────────────────── */
function show(n){$("#ov"+n).classList.add("show")}
function hide(n){$("#ov"+n).classList.remove("show")}
async function refreshSaved(){
  const d=await (await fetch("/api/projects")).json();
  $("#savedList").innerHTML=(d.projects||[]).map(n=>
    `<li><b>${esc(n)}</b><button onclick="loadProject(${jq(n)})">Open</button></li>`
    ).join("")||`<div class="hint">Nothing saved yet.</div>`;
}
async function newFrom(){
  const k=$("#tplPick").value;
  P=(await (await fetch("/api/template?name="+encodeURIComponent(k))).json()).project;
  OPEN={};PVW={};await prefetchTaxo();hide("Projects");render();
}
async function loadProject(n){
  const d=await (await fetch("/api/projects/load?name="+encodeURIComponent(n))).json();
  if(d.error){alert(d.error);return}
  P=d.project;OPEN={};PVW={};await prefetchTaxo();hide("Projects");render();
  if(d.migrated)log("Opened a Studio v2 project and brought it up to v3 — check Notes "
    +"for Engineering for anything that had no filter equivalent.","w");
}
async function saveProject(){
  const n=($("#saveAs").value||"").trim();
  if(n)P.name=n;
  const r=await fetch("/api/projects/save",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({project:P})});
  const d=await r.json();
  $("#rname").value=P.name;$("#saveAs").value="";
  refreshSaved();log(`Saved as ${d.name}.`,"o");
}
function openExport(){
  const errs=ISSUES.filter(x=>x.level==="error");
  $("#exportBody").innerHTML=errs.length?
    `<div class="msg error">There ${errs.length===1?"is":"are"} ${errs.length} thing${
      errs.length===1?"":"s"} to fix first.</div>`
    +errs.map(m=>`<div class="msg error">${esc(m.msg)}</div>`).join("")
    :`<p>This writes the pipeline into <code>pipelines/generated/</code> and emails it to
      Engineering, zipped.</p>
      <div class="f" style="margin-top:13px"><label>When should it run? (optional)</label>
      <input id="deployWhen" placeholder="e.g. the 3rd of each month, 7am CT"></div>`;
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
}

document.addEventListener("keydown",e=>{
  if(e.key==="Escape")["Projects","Export","Pick"].forEach(hide);
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
        "templates": [{"key": k, "label": v[0]} for k, v in TEMPLATES.items()],
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
            # through migrate() so a template's filters list is synced from its
            # enhanced values the same way a loaded project's is
            return self._json({"project": migrate(TEMPLATES[name][1]())})
        if u.path == "/api/section":
            return self._json({"section": new_section()})
        if u.path == "/api/projects":
            PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
            return self._json({"projects": sorted(x.stem for x in
                                                  PROJECTS_DIR.glob("*.json"))})
        if u.path == "/api/projects/load":
            path = PROJECTS_DIR / f"{_slug(one('name'))}.json"
            if not path.is_file():
                return self._json({"error": "not found"}, 404)
            raw = json.loads(path.read_text("utf-8"))
            was = int(raw.get("schema") or 0)
            return self._json({"project": migrate(raw), "migrated": was < SCHEMA})
        if u.path == "/api/test/status":
            with RUNS_LOCK:
                r = RUNS.get(one("id"))
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
                return self._json({"path": str(path), "email": email})
            except SyntaxError as exc:
                return self._json({"error": f"generated code did not parse: {exc}"}, 500)
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/test":
            try:
                rid = start_run(project, body.get("mode") or "search", body.get("limit"))
                return self._json({"run_id": rid})
            except Exception as exc:
                return self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
        if u.path == "/api/projects/save":
            PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
            name = _slug(project.get("name") or "untitled")
            (PROJECTS_DIR / f"{name}.json").write_text(
                json.dumps(project, indent=2), encoding="utf-8")
            return self._json({"name": name})
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
    p["email"] = {"enabled": True, "to_addr": "someone@competiscan.com"}
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

    # A v2 project, unmigrated, exactly as it sits on disk.
    v2 = {
        "name": "v2 legacy", "client": "Legacy Co", "cadence": "month",
        "anchor": "prior_complete", "window_field": "entry_id",
        "deck": {"enabled": True, "title": "{client} — {period}",
                 "filename": "{client}_{stamp}.pptx", "title_slide": True,
                 "summary_slide": False, "section_headings": False,
                 "closing_slide": True},
        "workbook": {"enabled": True, "filename": "{client}_{stamp}.xlsx"},
        "email": {"enabled": False, "to_addr": ""}, "notes": "",
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


def selftest(live: bool = False) -> int:
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

    if live:
        print("\n── Resolving every variant's filters against the archive ──")
        bad += _selftest_live()

    print("\nSELFTEST", "FAILED" if bad else "PASSED")
    return 1 if bad else 0


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
    args = ap.parse_args()

    if args.selftest:
        return selftest(live=args.live)

    print("Pipelines Studio v3")
    print(f"  project root : {PIPELINES_DIR.parent if PIPELINES_DIR else '(not found)'}")
    print(f"  pipelines/   : {PIPELINES_DIR or '(not found — Test disabled)'}")
    print(f"  writes to    : {GENERATED_DIR}")
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
