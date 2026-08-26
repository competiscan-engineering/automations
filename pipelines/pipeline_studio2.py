#!/usr/bin/env python3
"""
report_studio.py — build a Competiscan trend report without writing code (v2)
═══════════════════════════════════════════════════════════════════════════════════════

WHAT CHANGED FROM v1, AND WHY
    v1 was a node canvas: 12 node types, hand-drawn wires, and about ten ways to build an
    invalid graph. A researcher had to learn a dataflow model before they could describe a
    report. Wrong metaphor.

    Every trend report is really the same four steps, repeated once per topic:

        SEARCH  ->  WORKSHEET  ->  FEATURE  ->  SLIDE

    That is not a graph, it is a row. So v2 is a list of SECTION cards. One section = one
    topic (a product category, a competitor, a region — whatever the report is organised
    by). The four steps are always present and never wired, so an invalid pipeline is not
    expressible.

    Things that used to be nodes and are now nothing at all:
      * "Enrich" — inferred. Tick a column that needs the database and the lookup turns
        itself on. The old "sheet needs enrich" error cannot happen any more.
      * "Curate / LLM" — now two plain fields: how many pieces to feature, and how to
        choose them. Nobody has to think about model calls.
      * Title / agenda / divider / summary / closing slides — report checkboxes, ordered
        automatically.
      * "Split past 5 entries" — gone as a choice. The builder holds 5, so it always
        splits. A setting that must always be on is not a setting.

THE ONE ARCHITECTURAL RULE (unchanged from v1)
    Test does NOT run an interpreter. It generates the .py and runs THAT file. One
    execution path, so what a researcher tests is exactly what Engineering deploys.

RUN
    python report_studio.py                 # then open http://127.0.0.1:8787
    python report_studio.py --selftest      # codegen + screen 15 project shapes

WHERE THINGS GO
    Generated pipelines  ->  <project_root>/pipelines/generated/
    Saved projects       ->  <project_root>/pipelines/generated/_projects/

WHAT IT STILL WILL NOT DO
    It does not invent bespoke logic. Anything the four steps cannot express goes in the
    "Notes for Engineering" box, which is lifted verbatim into the generated file's
    docstring as a to-do. A generated pipeline is a correct, house-style DRAFT.

GUARDRAILS BAKED INTO EVERY GENERATED PIPELINE
    1. Searches run sequentially — the archive's REST backend cross-contaminates results
       when different channels are requested concurrently.
    2. A search returning exactly its limit hit the cap: the true total is unknown, so it
       is reported as "at least N". Cap-hit with zero in-window is flagged SUSPECT.
    3. The three date fields (entry_id / added_to_database / approved_date) disagree, so
       the report states which one bounds the window.
    4. Slides hold at most 5 entries; overflow always becomes "(cont.)" slides.
    5. Write-ups are trimmed to whole sentences, never a mid-word ellipsis.
    6. Counts, dedup and chunking are computed in Python. The model only picks entry_ids
       and writes prose.
    7. Email is opt-in and goes to the address entered in the project's settings; nothing
       is sent when it is left blank.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
import tempfile
import threading
import uuid
import zipfile
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

STUDIO_FILE = Path(__file__).resolve()


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

# ═══════════════════════════════════════════════════════════════════════════════════════
# Vocabulary
# ═══════════════════════════════════════════════════════════════════════════════════════

CHANNELS = ["Direct Mail", "Email", "Online Display", "Online Video", "Print",
            "Search Engine Marketing", "Social Media", "Website/URL"]

SECTORS = ["Banking", "Credit Cards", "Insurance", "Mortgage & Loan", "Retail",
           "Automotive", "Telecom", "Investment", "Healthcare"]

AUDIENCES = ["", "Consumer", "Employer/Business Owner",
             "Insurance Producer/Financial Advisor", "Mortgage Broker", "Provider"]

WINDOW_FIELDS = [
    {"key": "entry_id", "label": "Mailed / captured date",
     "note": "The date the piece itself carries. The usual choice for a deck."},
    {"key": "added_to_database", "label": "Added to the database",
     "note": "When it entered the archive. Catches older pieces loaded recently."},
    {"key": "approved_date", "label": "Approved for PowerSearch",
     "note": "When it was approved. Different again from both of the above."},
]

# ── The column catalog: every worksheet column and where its value comes from ──────────
#   search   : already on the raw search result — no database round-trip
#   derived  : computed from a search field
#   sql      : only available from the database (slower, needs the tunnel)
COLUMNS = [
    {"name": "EntryID", "source": "search", "default": True},
    {"name": "Primary Company", "source": "search", "default": True},
    {"name": "Headline", "source": "search", "default": True},
    {"name": "Media Channel", "source": "search", "default": True},
    {"name": "State/Province", "source": "search", "default": True},
    {"name": "OCR Text", "source": "search", "default": False,
     "note": "The full scanned text. Long — mostly useful for spot-checking."},
    {"name": "Quarter", "source": "derived", "default": True,
     "note": "Computed from the mailed date."},
    {"name": "Product", "source": "sql", "default": True},
    {"name": "PDF Content", "source": "sql", "default": True,
     "note": "Clickable link to the scanned piece."},
    {"name": "Additional Companies", "source": "sql", "default": True},
    {"name": "Primary Sector", "source": "sql", "default": True},
    {"name": "Primary Category", "source": "sql", "default": True},
    {"name": "Primary Sub Category", "source": "sql", "default": True},
    {"name": "Primary Sub Sub Category", "source": "sql", "default": False},
    {"name": "Mailing Type", "source": "sql", "default": False},
    {"name": "Age", "source": "sql", "default": False},
    {"name": "Income", "source": "sql", "default": False},
    {"name": "Pre-Screen", "source": "sql", "default": False},
    {"name": "Mortgage & Loan - Application Type", "source": "sql", "default": False,
     "note": "Refinance / VA / FHA / Conventional. Lending reports only."},
    {"name": "Social Media Ad Type", "source": "sql", "default": False},
]

DEFAULT_COLUMNS = [c["name"] for c in COLUMNS if c["default"]]
SQL_COLUMNS = {c["name"] for c in COLUMNS if c["source"] == "sql"}


def needs_database(columns) -> bool:
    """The database lookup is not a setting — it is implied by the columns asked for."""
    return any(c in SQL_COLUMNS for c in (columns or []))


# ═══════════════════════════════════════════════════════════════════════════════════════
# Project schema — one flat object. No nodes, no edges, nothing to wire.
# ═══════════════════════════════════════════════════════════════════════════════════════

def new_section(title="New section") -> dict:
    return {
        "id": uuid.uuid4().hex[:8],
        "title": title,
        "heading": "",
        "search": {
            "companies": "", "sectors": [], "channels": ["Email"], "keyword": "",
            "audience": "", "limit": 200,
            "only_credit_unions": False,
            "company_must_match": "", "company_must_not_match": "",
            "subcategory_must_include": "", "subcategory_must_exclude": "",
            "collapse_repeats": True, "max_per_creative": 2,
        },
        "sheet": {"enabled": True, "tab": "", "columns": list(DEFAULT_COLUMNS)},
        "feature": {
            "enabled": True, "count": 4,
            "how_to_choose": "", "what_to_say": "",
            "callout_limit": 374,
            "one_per_company": True, "never_reuse": True,
            "mention_cap": False,
        },
    }


def new_project(name="Untitled report") -> dict:
    return {
        "name": name,
        "client": "",
        "cadence": "month",
        "anchor": "prior_complete",
        "window_field": "entry_id",
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
    that reports have to look like this."""
    p = new_project("Example — multi-category monthly")
    p["client"] = "Example Client"
    p["deck"].update({"summary_slide": True, "section_headings": True,
                      "title": "{client} Market Update — {period}",
                      "filename": "{client}_Market_Update_{stamp}.pptx"})
    p["workbook"]["filename"] = "{client}_Offers_{stamp}.xlsx"
    out = []
    for title, tab, heading, sectors, kw, guidance in [
        ("Checking Acquisition", "Checking", "Deposits", ["Banking"], "",
         "Offers that push someone to open a new account, ideally online. Skip anything "
         "aimed at existing customers."),
        ("Savings & CDs", "Savings", "Deposits", ["Banking"],
         '"savings" or "certificate" or "CD"',
         "Rate-led offers. Prefer pieces that state an actual APY."),
        ("Home Lending", "Lending", "Lending", ["Mortgage & Loan"],
         '"mortgage" or "HELOC" or "home equity"',
         "Mortgage and home-equity acquisition. Not auto, not personal loans."),
    ]:
        s = new_section(title)
        s["heading"] = heading
        s["sheet"]["tab"] = tab
        s["search"].update({"sectors": sectors, "keyword": kw,
                            "channels": ["Email", "Direct Mail", "Social Media"],
                            "audience": "Consumer"})
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
    "example": ("Example — multi-category monthly", _example_project),
}

# ═══════════════════════════════════════════════════════════════════════════════════════
# Checking — runs continuously, no button. Issues attach to the section they concern.
# ═══════════════════════════════════════════════════════════════════════════════════════

def validate(p: dict) -> dict:
    issues: list[dict] = []

    def err(msg, section=None):
        issues.append({"level": "error", "msg": msg, "section": section})

    def warn(msg, section=None):
        issues.append({"level": "warn", "msg": msg, "section": section})

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

        if not (s.get("title") or "").strip():
            err("This section needs a name.", sid)
        key = ((s.get("heading") or "").strip(), title) if scoped else ("", title)
        titles[key] = titles.get(key, 0) + 1

        if not se.get("channels"):
            err("Pick at least one media channel to search.", sid)
        if not se.get("sectors") and not str(se.get("companies") or "").strip():
            warn("No sector and no company, so this searches the whole archive for those "
                 "channels. It will be slow and very broad.", sid)
        try:
            lim = int(se.get("limit") or 200)
            if lim > 200:
                warn(f"{lim} is more than the archive returns in one call, so you will "
                     f"quietly get fewer.", sid)
            elif lim < 20:
                warn(f"{lim} results per channel is low — you may miss pieces that belong "
                     f"in the report.", sid)
        except (TypeError, ValueError):
            err("Max results must be a number.", sid)

        if re.search(r"\bnot\b", str(se.get("keyword") or ""), re.I):
            warn('This uses "not". The archive\'s text search does not honour negation '
                 'reliably — the approach that works is a positive search plus a '
                 'subtraction in code. Put what you need in Notes for Engineering.', sid)

        if (str(se.get("subcategory_must_include") or "").strip()
                or str(se.get("subcategory_must_exclude") or "").strip()):
            if not needs_database(sh.get("columns") if sh.get("enabled") else []):
                warn("Sub-category is a database field, so this section will do a lookup "
                     "even though none of its columns need one.", sid)

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
             "report_studio.py in the project root, beside pipelines/.")

    return {
        "issues": issues,
        "errors": sum(1 for i in issues if i["level"] == "error"),
        "warnings": sum(1 for i in issues if i["level"] == "warn"),
        "database": any(needs_database((s.get("sheet") or {}).get("columns"))
                        for s in sections if (s.get("sheet") or {}).get("enabled")),
    }


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


def codegen(p: dict) -> tuple[str, str]:
    client = str(p.get("client") or "Report").strip() or "Report"
    cadence = p.get("cadence") or "month"
    anchor = p.get("anchor") or "prior_complete"
    win = p.get("window_field") or "entry_id"
    deck = p.get("deck") or {}
    book = p.get("workbook") or {}
    email = p.get("email") or {}
    sections = p.get("sections") or []
    notes = str(p.get("notes") or "").strip()

    deck_on = bool(deck.get("enabled"))
    book_on = bool(book.get("enabled"))
    featured = [s for s in sections if (s.get("feature") or {}).get("enabled")] if deck_on \
        else []

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

    uses_subcat = any(
        str((s.get("search") or {}).get("subcategory_must_include") or "").strip()
        or str((s.get("search") or {}).get("subcategory_must_exclude") or "").strip()
        for s in sections)
    any_db = any(needs_database(t["columns"]) for t in tabs) or uses_subcat
    summary_on = bool(deck_on and deck.get("summary_slide") and featured)
    headings_on = bool(deck_on and deck.get("section_headings"))

    O: list[str] = []
    w = O.append

    # ── docstring ───────────────────────────────────────────────────────────────────
    w("#!/usr/bin/env python3")
    w('"""')
    w(f"{client} — generated by Pipelines Studio")
    w("─" * 78)
    w(f"Project    : {p.get('name') or 'untitled'}")
    w(f"Generated  : {datetime.now():%Y-%m-%d %H:%M}")
    w(f"Cadence    : {cadence} ({anchor})")
    w(f"Window from: {win}")
    w(f"Sections   : {len(sections)}    "
      f"Slides: {'yes' if deck_on else 'no'}    "
      f"Workbook: {len(tabs) if book_on else 0} tab(s)")
    w("")
    w("Every section is the same four steps: search the archive, optionally write the")
    w("results to a worksheet tab, optionally have Claude pick the best pieces and write")
    w("the paragraph underneath, and put those on a slide.")
    w("")
    w("RUN")
    w(f"    python pipelines/generated/{_slug(client)}.py")
    w("    python ... --only search    # counts only: no model calls, no deliverables")
    if tabs:
        w("    python ... --only excel     # search + workbook, still no model calls")
    if deck_on:
        w("    python ... --only deck      # everything")
    w("    python ... --limit 20       # cap results per search while testing")
    w("")
    if notes:
        w("┌" + "─" * 74 + "┐")
        w("│ NOTES FOR ENGINEERING — not implemented below. Please wire these by hand. │")
        w("└" + "─" * 74 + "┘")
        for line in notes.splitlines():
            if line.strip():
                O.extend(_wrap(line.strip()))
        w("")
    w("GUARDRAILS BAKED IN")
    w("  * Searches run sequentially — the archive's REST backend cross-contaminates")
    w("    results when different channels are requested concurrently.")
    w("  * A search returning exactly its limit hit the cap: the true total is unknown")
    w('    and is reported as "at least N". Cap-hit with zero in-window is SUSPECT.')
    w(f"  * The window is bounded by {win}. The three date fields disagree.")
    if deck_on:
        w('  * Slides hold 5 entries; overflow becomes "(cont.)" slides automatically.')
    w("  * Counts, dedup and chunking are Python. The model only picks entry_ids and")
    w("    writes prose.")
    if email.get("enabled"):
        w(f"  * Deliverables are emailed to {email.get('to_addr')} when the run finishes.")
    w('"""')
    w("")

    # ── imports ─────────────────────────────────────────────────────────────────────
    w("import argparse")
    w("import os")
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
    w("# Raise the builder timeout BEFORE the builder module is imported.")
    w('os.environ.setdefault("PPT_BUILDER_TIMEOUT", "300")')
    w("")
    w("import pipelines.report_lib as L  # noqa: E402")
    if any_db:
        w("import pipelines.report_lib_excel_helper as XH  # noqa: E402")
    w("")
    w('search_archive     = L.load_tool("mcp_serverv4", "search_archive")')
    if deck_on:
        w('build_deck_default = L.load_tool("mcp_pptbuilder", "build_deck_default")')
    if any_db:
        w('_run_sql           = L.load_tool("mcp_serverv3", "_run_sql")')
    w("")
    w("")

    # ── settings ────────────────────────────────────────────────────────────────────
    w("# ── Report settings " + "─" * 58)
    w(f"CLIENT       = {_lit(client)}")
    w(f"CADENCE      = {_lit(cadence)}       # week | month")
    w(f"ANCHOR       = {_lit(anchor)}")
    w(f"WINDOW_FIELD = {_lit(win)}")
    w('PERIOD_START = os.environ.get("RS_PERIOD_START") or None   # "2026-06-01" overrides')
    w('PERIOD_END   = os.environ.get("RS_PERIOD_END") or None')
    w("SLIDE_CAP    = 5     # builder hard limit: 5 entries per slide")
    w('OUTPUT_DIR   = PROJECT_ROOT / "output"')
    if email.get("enabled"):
        addr = str(email.get("to_addr") or "").strip()
        w(f"# Opt-in: RS_EMAIL_TO overrides this at run time; blank means nothing is")
        w(f"# emailed.")
        w(f'EMAIL_TO     = os.environ.get("RS_EMAIL_TO") or {_lit(addr)} or None')
    w("")
    if tabs:
        w("HYPERLINKS = {")
        w('    "EntryID":     ("https://cp.competiscan.com/productdetail?id={pid}",')
        w('                    "{entry_id}"),')
        w('    "PDF Content": ("https://www.competiscan.com/productDocuments.php?id={pid}",')
        w('                    "PDF Content"),')
        w("}")
        w("")

    # ── sections ────────────────────────────────────────────────────────────────────
    w("# ── Sections: search -> worksheet -> feature -> slide " + "─" * 25)
    w("SECTIONS = [")
    for s in sections:
        se, sh, fe = (s.get("search") or {}), (s.get("sheet") or {}), (s.get("feature") or {})
        companies = [x.strip() for x in str(se.get("companies") or "").splitlines()
                     if x.strip()]
        tab_name = ((sh.get("tab") or "").strip() or (s.get("title") or "Sheet").strip()) \
            if (book_on and sh.get("enabled")) else None
        subcat_in = [x.strip().lower() for x in
                     str(se.get("subcategory_must_include") or "").split(",") if x.strip()]
        subcat_ex = [x.strip().lower() for x in
                     str(se.get("subcategory_must_exclude") or "").split(",") if x.strip()]
        w("    {")
        w(f'        "id": {_lit(s.get("id"))},')
        w(f'        "title": {_lit(s.get("title") or "")},')
        if headings_on:
            w(f'        "heading": {_lit(s.get("heading") or "")},')
        w(f'        "companies": {_lit(companies)},')
        w(f'        "sectors": {_lit(se.get("sectors") or [])},')
        w(f'        "channels": {_lit(se.get("channels") or [])},')
        w(f'        "keyword": {_lit(se.get("keyword") or "")},')
        w(f'        "audience": {_lit(se.get("audience") or "")},')
        w(f'        "limit": {_int(se.get("limit"), 200)},')
        w(f'        "only_credit_unions": {_lit(bool(se.get("only_credit_unions")))},')
        w(f'        "company_must_match": {_lit(se.get("company_must_match") or "")},')
        w(f'        "company_must_not_match": '
          f'{_lit(se.get("company_must_not_match") or "")},')
        w(f'        "subcat_include": {_lit(subcat_in)},')
        w(f'        "subcat_exclude": {_lit(subcat_ex)},')
        w(f'        "collapse_repeats": {_lit(bool(se.get("collapse_repeats")))},')
        w(f'        "max_per_creative": {_int(se.get("max_per_creative"), 2)},')
        w(f'        "tab": {_lit(tab_name)},')
        w(f'        "feature": {_lit(bool(fe.get("enabled") and deck_on))},')
        w(f'        "count": {_int(fe.get("count"), 4)},')
        w(f'        "callout_limit": {_int(fe.get("callout_limit"), 374)},')
        w(f'        "one_per_company": {_lit(bool(fe.get("one_per_company")))},')
        w(f'        "never_reuse": {_lit(bool(fe.get("never_reuse")))},')
        w(f'        "mention_cap": {_lit(bool(fe.get("mention_cap")))},')
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
            w(f'        "database": {_lit(needs_database(t["columns"]))},')
            w("    },")
        w("]")
        w("")

    if summary_on:
        w("# ── Summary slide: the LAST model call, reading the finished write-ups ──────")
        w(f'SUMMARY_TITLE1 = {_lit("{period} — what stood out")}')
        w(f'SUMMARY_TITLE2 = {_lit("{period} — also worth noting")}')
        w("SUMMARY_MAX_WORDS = 55")
        w("SUMMARY_SYSTEM = (")
        w('    "You are a competitive-intelligence analyst writing the opening summary of "')
        w('    "a client deck. You are given the write-ups that are already in the deck. "')
        w('    "Distil them into two short paragraphs: the first for the most important "')
        w('    "themes, the second for secondary observations. Name companies and their "')
        w('    "specific offers. Use ONLY what you are given, and never mention a topic "')
        w('    "that had no activity.\\n\\n"')
        w('    \'Reply with ONE JSON object: {"column1": "...", "column2": "..."}\'')
        w(")")
        w("")

    # ── helpers ─────────────────────────────────────────────────────────────────────
    w("")
    w("# ── Helpers " + "─" * 66)
    if any(s.get("search", {}).get("only_credit_unions") for s in sections):
        w('_CU_RE = re.compile(r"credit union|\\bFCU\\b|\\bF\\.?C\\.?U\\.?\\b|\\bCU\\b",')
        w("                    re.I)")
        w("")
        w("")
    w("def _parse_args():")
    w('    p = argparse.ArgumentParser(description=f"{CLIENT} report")')
    modes = ["search"] + (["excel"] if tabs else []) + (["deck"] if deck_on else []) + \
        ["all"]
    w(f'    p.add_argument("--only", default="all", choices={_lit(modes)},')
    w('                   help="Stop after a stage — cheap iteration while testing.")')
    w('    p.add_argument("--limit", type=int, default=None,')
    w('                   help="Override every search limit (small = fast test).")')
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
    w("def _ordinal(n):")
    w('    suffix = "th" if 11 <= n % 100 <= 13 else {1: "st", 2: "nd",')
    w('                                               3: "rd"}.get(n % 10, "th")')
    w('    return f"{n}{suffix}"')
    w("")
    w("")
    w("def _entry_date(entry_id):")
    w('    """entry_id is YYYY-MM-DD-NNNN. This is the MAILED/CAPTURED date — not')
    w('    approved_date and not added_to_database. The three disagree."""')
    w("    try:")
    w('        y, m, d = str(entry_id).split("-")[:3]')
    w("        return date(int(y), int(m), int(d))")
    w("    except (ValueError, AttributeError):")
    w("        return None")
    w("")
    w("")
    if any("Quarter" in t["columns"] for t in tabs):
        w("def _quarter(entry_id):")
        w("    d = _entry_date(entry_id)")
        w('    return f"{d.year} Q{(d.month - 1) // 3 + 1}" if d else ""')
        w("")
        w("")
    w("def _dedup(records):")
    w("    seen, out = set(), []")
    w("    for r in records:")
    w('        eid = r.get("entry_id")')
    w("        if eid and eid not in seen:")
    w("            seen.add(eid)")
    w("            out.append(r)")
    w("    return out")
    w("")
    w("")
    w("def _theme(record):")
    w('    """A coarse creative fingerprint: company plus the first few headline words.')
    w("    Stops one recycled evergreen ad from filling every slot on a slide.\"\"\"")
    w('    co = (record.get("company_name") or "").lower().strip()')
    w('    head = re.sub(r"[^a-z0-9 ]", "", (record.get("headline") or "").lower())')
    w("    return f\"{co}|{' '.join(head.split()[:6])}\"")
    w("")
    w("")
    w("def _filter(records, sec, subcats=None):")
    w('    """The section\'s narrowing rules, in a fixed order. None of these ask the')
    w('    model to do the filtering."""')
    w("    out = list(records)")
    w("")
    w("    def report(label, before):")
    w("        if len(out) != before:")
    w('            print(f"      {label}: {before} -> {len(out)}")')
    w("")
    if any(s.get("search", {}).get("only_credit_unions") for s in sections):
        w('    if sec["only_credit_unions"]:')
        w("        n = len(out)")
        w('        out = [r for r in out if _CU_RE.search(r.get("company_name") or "")]')
        w('        report("credit unions only", n)')
    w('    if sec["company_must_match"]:')
    w("        n = len(out)")
    w("        out = [r for r in out")
    w('               if re.search(sec["company_must_match"],')
    w('                            r.get("company_name") or "", re.I)]')
    w('        report("company must match", n)')
    w('    if sec["company_must_not_match"]:')
    w("        n = len(out)")
    w("        out = [r for r in out")
    w('               if not re.search(sec["company_must_not_match"],')
    w('                                r.get("company_name") or "", re.I)]')
    w('        report("company must not match", n)')
    w('    if sec["subcat_include"] or sec["subcat_exclude"]:')
    w("        n, kept = len(out), []")
    w("        for r in out:")
    w('            tags = (subcats or {}).get(r.get("entry_id"), "").lower()')
    w("            # A blank tag matches nothing on purpose: dropping a piece we cannot")
    w("            # verify beats putting it in the wrong section.")
    w("            if not tags:")
    w("                continue")
    w('            if any(k in tags for k in sec["subcat_exclude"]):')
    w("                continue")
    w('            if sec["subcat_include"] and not any(k in tags')
    w('                                                for k in sec["subcat_include"]):')
    w("                continue")
    w("            kept.append(r)")
    w("        out = kept")
    w('        report("sub-category", n)')
    w('    if sec["collapse_repeats"]:')
    w("        n, counts, kept = len(out), {}, []")
    w("        for r in out:")
    w("            k = _theme(r)")
    w('            if counts.get(k, 0) < sec["max_per_creative"]:')
    w("                counts[k] = counts.get(k, 0) + 1")
    w("                kept.append(r)")
    w("        out = kept")
    w('        report("collapse repeated creative", n)')
    w("    return out")
    w("")
    w("")
    w("def _search(sec, channel, limit):")
    w('    """One call, one channel. Fanning out per channel multiplies the per-call cap.')
    w('    Callers MUST keep this sequential — see the guardrails above."""')
    w('    kwargs = {"media_channels": [channel], "limit": limit}')
    w('    if sec["sectors"]:')
    w('        kwargs["sectors"] = sec["sectors"]')
    w('    if sec["companies"]:')
    w('        kwargs["company_names"] = sec["companies"]')
    w('    if sec["keyword"]:')
    w('        kwargs["keyword"] = sec["keyword"]')
    w('    if sec["audience"]:')
    w('        kwargs["audience"] = sec["audience"]')
    w("    try:")
    w("        res = search_archive(**kwargs)")
    w("    except Exception as exc:  # a dead tunnel must not kill the whole run")
    w("        return [], False, str(exc)")
    w('    if res and isinstance(res[0], dict) and "error" in res[0]:')
    w('        return [], False, res[0]["error"]')
    w('    rows = [r for r in (res or []) if r.get("entry_id")]')
    w("    return rows, len(rows) >= limit, None")
    w("")
    w("")

    if any_db:
        w("def _lookup(entry_ids):")
        w('    """entry_ids -> canonical column values from the database, keyed by')
        w("    entry_id. Only ids that have both a document and a primary-company")
        w("    mapping come back — the query inner-joins them.\"\"\"")
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
        w('        row["pid"] = src.get("product_id", "")')
        w('        row["_subcats"] = src.get("sub_categories") or ""')
        w('        out[src.get("entry_id")] = row')
        w("    return out")
        w("")
        w("")

    if tabs:
        w("def _row(record, sql, columns):")
        w('    """Build one worksheet row. A database value wins when we have one;')
        w("    otherwise the value comes off the search result or is derived from it. A")
        w('    column with no source renders blank rather than being guessed at."""')
        w("    out = {}")
        w("    for col in columns:")
        w("        if sql and col in sql:")
        w('            out[col] = sql.get(col) or ""')
        w('        elif col == "EntryID":')
        w('            out[col] = record.get("entry_id") or ""')
        w('        elif col == "Primary Company":')
        w('            out[col] = record.get("company_name") or ""')
        w('        elif col == "Media Channel":')
        w('            out[col] = record.get("media_channel") or ""')
        w('        elif col == "State/Province":')
        w('            out[col] = record.get("state") or ""')
        w('        elif col == "Headline":')
        w('            out[col] = L.clean_cell(record.get("headline"))')
        w('        elif col == "OCR Text":')
        w('            out[col] = L.clean_cell(record.get("ocr_text"))[:900]')
        if any("Quarter" in t["columns"] for t in tabs):
            w('        elif col == "Quarter":')
            w('            out[col] = _quarter(record.get("entry_id"))')
        w("        else:")
        w('            out[col] = ""')
        w('    out["entry_id"] = record.get("entry_id") or ""')
        w('    out["pid"] = (sql or {}).get("pid") or record.get("product_id") or ""')
        w("    return out")
        w("")
        w("")

    if featured:
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
        w('    "those exact words. Never invent your own estimate of the true/total "')
        w('    "volume.\\n\\n"')
        w('    "STYLE\\n{style}\\n\\n"')
        w('    \'Reply with ONE JSON object: {"callout": "..."}\'')
        w(")")
        w("")
        w("")
        w("def _candidates(records):")
        w('    """A compact candidate list. The text is truncated hard: enough to judge')
        w('    relevance, not the whole document."""')
        w('    return "\\n".join(')
        w('        f\'- {r.get("entry_id")} | {r.get("company_name") or "?"}\'')
        w('        f\' | {r.get("media_channel") or "?"} | {r.get("state") or ""}\'')
        w('        f\' | {L.clean_cell(r.get("headline"))[:170]}\'')
        w('        f\' | {L.clean_cell(r.get("ocr_text"))[:380]}\'')
        w("        for r in records)")
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
        w("def _writeup(sec, chosen, true_count, cap_hit):")
        w("    if not chosen:")
        w('        return {"callout": ""}')
        w('    style = sec["what_to_say"] or "Plain analyst prose."')
        w('    system = (WRITEUP_SYSTEM.replace("{limit}", str(sec["callout_limit"]))')
        w('              .replace("{style}", style))')
        w("    # mention_cap opts a section into cap-hit phrasing for readers who know")
        w("    # what that means. Default is the exact, unambiguous featured count.")
        w('    if sec["mention_cap"] and cap_hit:')
        w('        note = (f"At least {true_count} pieces were captured across this "')
        w('                f"period, with the true total exceeding the search cap.")')
        w('    elif sec["mention_cap"]:')
        w('        note = f"Exactly {true_count} piece(s) were captured in this period."')
        w("    else:")
        w('        note = f"{len(chosen)} piece(s) are featured on this slide."')
        w('    prompt = (f\'Slide: "{sec["title"]}". {note}\\n\\n\'')
        w('              f\'FEATURED PIECES\\n{_candidates(chosen)}\')')
        w("    try:")
        w("        return L.extract_json(L.call_claude(system, prompt))")
        w("    except Exception as exc:")
        w('        return {"error": str(exc), "callout": ""}')
        w("")
        w("")

    if summary_on:
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

    # ── main ────────────────────────────────────────────────────────────────────────
    w("# ── Pipeline " + "─" * 65)
    w("def main() -> int:")
    w("    args = _parse_args()")
    w("    start, end = _window()")
    if cadence == "week":
        w('    period_label = f"{end:%B} {_ordinal(end.day)}, {end.year}"')
    else:
        w('    period_label = f"{start:%B} {start.year}"')
    w('    stamp = end.strftime("%Y%m%d")')
    w('    mmddyy = end.strftime("%m%d%y")')
    w('    month_year = f"{start:%B}{start.year}"')
    w("    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)")
    w('    print(f"{CLIENT} — {period_label}")')
    w(f'    print(f"  window {{start}} .. {{end}}  (bounded by {win})")')
    w('    print(f"  mode --only={args.only}"')
    w('          + (f" --limit={args.limit}" if args.limit else ""))')
    w("")
    w("    # ── Step 1 — search, SEQUENTIALLY (see guardrails) ──────────────────────")
    w('    calls = sum(len(s["channels"]) for s in SECTIONS)')
    w('    print(f"\\nStep 1  Searching ({calls} section x channel calls, sequential)…")')
    w("    found = {}")
    w("    for sec in SECTIONS:")
    w('        limit = args.limit or sec["limit"]')
    w("        records, cap_hit = [], False")
    w('        for channel in sec["channels"]:')
    w("            rows, hit, err = _search(sec, channel, limit)")
    w("            if err:")
    w("                print(f\"   ! {sec['title']} / {channel}: {err}\")")
    w("                continue")
    w("            records.extend(rows)")
    w("            cap_hit = cap_hit or hit")
    w("        raw_n = len(records)")
    w("        in_window = []")
    w("        for r in _dedup(records):")
    w('            d = _entry_date(r.get("entry_id"))')
    w("            if d and start <= d <= end:")
    w("                in_window.append(r)")
    w("        print(f\"   {sec['title'][:34]:34} {raw_n:>4} raw\"")
    w("              f\"{' (CAP HIT)' if cap_hit else '':10}\"")
    w('              f" -> {len(in_window):>4} in window")')
    w("        if cap_hit and not in_window:")
    w("            # Every capped record missing the window is implausible for real data.")
    w("            print(f\"   !! SUSPECT: {sec['title']} hit the cap but 0 landed in\"")
    w('                  f" the window. This is probably NOT a true zero — check"')
    w('                  f" PowerSearch before reporting it.")')
    w('        found[sec["id"]] = {"sec": sec, "records": in_window, "cap_hit": cap_hit}')
    w("")
    w('    if not any(v["records"] for v in found.values()):')
    w('        print("\\nERROR: every section came back empty. Is the tunnel up and the"')
    w('              " archive reachable? Aborting rather than shipping an empty report.")')
    w("        return 1")
    w("")
    w("    sql_rows, subcats = {}, {}")

    if any_db:
        w("")
        w("    # ── Step 2 — database lookup, only where it is actually needed ──────────")
        w("    needed = set()")
        if tabs:
            w("    for t in TABS:")
            w('        if t["database"]:')
            w('            needed.update(t["section_ids"])')
        if uses_subcat:
            w("    for sec in SECTIONS:")
            w('        if sec["subcat_include"] or sec["subcat_exclude"]:')
            w('            needed.add(sec["id"])')
        w("    if needed:")
        w('        print(f"\\nStep 2  Database lookup for {len(needed)} section(s)…")')
        w('        for sec in [s for s in SECTIONS if s["id"] in needed]:')
        w('            ids = [r["entry_id"] for r in found[sec["id"]]["records"]]')
        w("            rows = _lookup(ids)")
        w("            sql_rows.update(rows)")
        w("            for eid, row in rows.items():")
        w('                subcats[eid] = row.get("_subcats", "")')
        w("            print(f\"   {sec['title'][:34]:34} {len(ids):>4} ids ->\"")
        w('                  f" {len(rows):>4} matched")')
        w("")

    w("    # ── Step 3 — narrowing rules ────────────────────────────────────────────")
    w('    print("\\nStep 3  Filters…")')
    w("    for sid, v in found.items():")
    w('        sec = v["sec"]')
    w('        active = (sec["only_credit_unions"] or sec["company_must_match"]')
    w('                  or sec["company_must_not_match"] or sec["subcat_include"]')
    w('                  or sec["subcat_exclude"] or sec["collapse_repeats"])')
    w("        if not active:")
    w("            continue")
    w("        print(f\"   {sec['title']}\")")
    w('        v["records"] = _filter(v["records"], sec, subcats)')
    w("")
    w('    if args.only == "search":')
    w('        print("\\n── Counts (check these against PowerSearch) ──")')
    w("        for sid, v in found.items():")
    w("            print(f\"   {v['sec']['title'][:38]:38} \"")
    w("                  f\"{'at least ' if v['cap_hit'] else ''}{len(v['records'])}\")")
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
        w('                rows.append(_row(r, sql_rows.get(r["entry_id"]), t["columns"]))')
        w('        titles = [found[s]["sec"]["title"] for s in t["section_ids"]]')
        w('        first = found[t["section_ids"][0]]["sec"]')
        w('        described = " / ".join(titles)')
        w('        sectors = ", ".join(first["sectors"]) or "any"')
        w('        channels = ", ".join(first["channels"])')
        w("        spec = {")
        w('            "name": t["name"], "headers": t["columns"], "rows": rows,')
        w('            "hyperlinks": HYPERLINKS,')
        w('            "filter_row": (f"{described} | Sectors: {sectors}"')
        w('                           f" | Channels: {channels}"')
        w('                           f" | Window: {start} .. {end}"),')
        w("        }")
        w("        specs.append(spec)")
        w("        print(f\"   {t['name'][:30]:30} {len(rows):>4} rows\"")
        w("              f\"  ({len(t['columns'])} columns)\")")
        w("")
        w(f'    xlsx_name = ({_lit(book.get("filename") or "{client}_Data_{stamp}.xlsx", 17)}')
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

    if featured:
        w("    # ── Step 5 — choose what to feature (parallel model calls) ──────────────")
        w('    picked = [s for s in SECTIONS if s["feature"]]')
        w('    print(f"\\nStep 5  Choosing pieces ({len(picked)} parallel calls)…")')
        w("    choices = L.run_parallel(")
        w('        [(lambda s=s: _choose(s, found[s["id"]]["records"])) for s in picked])')
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
        w('                co = (by_id.get(eid, {}).get("company_name") or eid).lower()')
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
        w("    # ── Step 6 — the write-ups (parallel model calls) ───────────────────────")
        w('    print("\\nStep 6  Write-ups…")')
        w("")
        w("    def writeup_job(sec):")
        w('        ids = set(final.get(sec["id"], []))')
        w('        chosen = [r for r in found[sec["id"]]["records"]')
        w('                  if r.get("entry_id") in ids]')
        w('        return _writeup(sec, chosen, len(found[sec["id"]]["records"]),')
        w('                        found[sec["id"]]["cap_hit"])')
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

    if summary_on:
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

    if deck_on:
        w("    # ── Step 8 — build the deck ─────────────────────────────────────────────")
        w('    print("\\nStep 8  Deck…")')
        w("    slides = []")
        if deck.get("title_slide"):
            w('    slides.append({"type": "title",')
            w('                   "data": {"title": CLIENT, "date": period_label}})')
        if headings_on:
            w("    headings = []")
            w("    for sec in SECTIONS:")
            w('        h = (sec.get("heading") or "").strip()')
            w('        if h and h not in headings and sec["feature"]:')
            w("            headings.append(h)")
            w("    if headings:")
            w('        slides.append({"type": "agenda", "data": {"sections": headings}})')
        if summary_on:
            w('    slides.append({"type": "needToKnow", "data": {')
            w('        "title1": SUMMARY_TITLE1.replace("{period}", period_label),')
            w('        "text1": sum1,')
            w('        "title2": SUMMARY_TITLE2.replace("{period}", period_label),')
            w('        "text2": sum2}})')
        if featured:
            w("")
            if headings_on:
                w("    current = None")
            w("    for sec in SECTIONS:")
            w('        if not sec["feature"]:')
            w("            continue")
            if headings_on:
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
        if deck_on and tabs:
            attach = "[a for a in (saved, xlsx_path) if a]"
        elif deck_on:
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

    if notes:
        w('    print("\\n!! This report has NOTES FOR ENGINEERING in its docstring —"')
        w('          " bespoke work is still needed before it is production-ready.")')
    w('    print("\\nDone.")')
    w("    return 0")
    w("")
    w("")
    w('if __name__ == "__main__":')
    w("    sys.exit(main())")

    return "\n".join(O) + "\n", f"{_slug(client)}.py"


# ═══════════════════════════════════════════════════════════════════════════════════════
# Test runner — generates the file, then runs THAT file
# ═══════════════════════════════════════════════════════════════════════════════════════

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
            log("Cannot run: report_lib.py was not found next to report_studio.py.")
            log("Put report_studio.py in the project root, beside pipelines/.")
            with RUNS_LOCK:
                RUNS[run_id]["done"], RUNS[run_id]["rc"] = True, 1
            return
        cmd = [sys.executable, "-u", str(target), "--only", mode]
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
# Hand-off — Export used to mean "save it, then go email Engineering yourself."
# Now Export does the emailing too.
# ═══════════════════════════════════════════════════════════════════════════════════════

ENGINEERING_RECIPIENTS = ["hgquijano@competiscan.com"]


def _email_engineering(project: dict, path: Path, deploy_when: str = "") -> dict:
    """Attach the just-written pipeline file and hand it to Engineering directly,
    so a researcher hitting Export never has to open their own email client."""
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
        f"Studio and it's ready to be reviewed, deployed and scheduled.\n\n"
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
#rname{width:220px;font-weight:600;border-color:transparent;background:#f2f4f9}
#rname:focus{border-color:var(--accent);background:#fff}
.sp{flex:1}
#health{font-size:12.5px;color:var(--dim);display:flex;gap:9px;align-items:center}
.pill{padding:2px 9px;border-radius:20px;font-size:12px;font-weight:600}
.pill.err{background:#fdecea;color:var(--err)}
.pill.wr{background:#fdf3e3;color:var(--warn)}
.pill.ok{background:#e8f6ee;color:var(--ok)}

#body{flex:1;display:flex;min-height:0}
#settings{width:290px;background:#fff;border-right:1px solid var(--line);overflow:auto;
padding:16px}
#stage{flex:1;overflow:auto;padding:20px 24px}
.wrapper{max-width:860px;margin:0 auto}

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
  <span class="logo">Pipelines Studio</span>
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
  <div id="settings"></div>
  <div id="stage"><div class="wrapper" id="sections"></div></div>
</div>

<div id="logbar"><span>Output</span><div class="sp"></div>
  <button class="ghost" style="color:#8b93a6" onclick="clearLog()">clear</button></div>
<div id="log"><span class="d">Describe the report on the left, add sections in the middle,
then press Test.</span></div>

<div class="overlay" id="ovProjects"><div class="panel">
  <header><b>Projects</b><button class="ghost" onclick="hide('Projects')">close</button></header>
  <div class="content">
    <div class="f"><label>Start something new</label>
      <div class="row"><select id="tplPick"></select>
      <button onclick="newFrom()" style="flex:0 0 auto">Create</button></div></div>
    <h2 class="mt">Saved reports</h2>
    <ul class="plist" id="savedList"></ul>
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

<script>
let SPEC=null,P=null,ISSUES=[],OPEN={},poll=null,deb=null;
const $=s=>document.querySelector(s);
const esc=s=>String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;")
  .replace(/"/g,"&quot;");

async function boot(){
  SPEC=await (await fetch("/api/spec")).json();
  $("#tplPick").innerHTML=SPEC.templates.map(t=>
    `<option value="${t.key}">${esc(t.label)}</option>`).join("");
  P=(await (await fetch("/api/template?name=example")).json()).project;
  render(); refreshSaved();
}
function render(){$("#rname").value=P.name||"";renderSettings();renderSections();check()}

/* ── report settings ─────────────────────────────────────────────────────── */
function renderSettings(){
  const d=P.deck,w=P.workbook,e=P.email;
  const wf=SPEC.window_fields.map(f=>`<option value="${f.key}"${
    P.window_field===f.key?" selected":""}>${esc(f.label)}</option>`).join("");
  const wnote=(SPEC.window_fields.find(f=>f.key===P.window_field)||{}).note||"";
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
  <div class="f"><label>Which date to filter on</label>
    <select onchange="setP('window_field',this.value)">${wf}</select>
    <div class="hint">${esc(wnote)}</div></div>

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
function setP(k,v){P[k]=v;soft(k==="window_field"||k==="cadence")}
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
  const n=(s.search.channels||[]).length;
  const sub=[n+" channel"+(n===1?"":"s"),
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
function body(s){
  const chips=(list,cur,fn)=>`<div class="chips">`+list.map(o=>
    `<span class="chip${(cur||[]).includes(o)?" on":""}"
      onclick="${fn}('${s.id}',${JSON.stringify(o).replace(/"/g,"&quot;")})">${esc(o)}</span>`
    ).join("")+`</div>`;
  const aud=SPEC.audiences.map(a=>`<option value="${esc(a)}"${
    s.search.audience===a?" selected":""}>${a===""?"Anyone":esc(a)}</option>`).join("");
  const cols=SPEC.columns.map(c=>{
    const on=(s.sheet.columns||[]).includes(c.name);
    return `<label class="col" title="${esc(c.note||"")}"><input type="checkbox"
      ${on?"checked":""} onchange="col('${s.id}',${
      JSON.stringify(c.name).replace(/"/g,"&quot;")},this.checked)">
      <span>${esc(c.name)}</span></label>`;
  }).join("");
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
   <div class="f"><label>Companies — one per line, blank for any</label>
     <textarea oninput="setSS('${s.id}','companies',this.value)"
       placeholder="Grainger&#10;Zoro Tools">${esc(s.search.companies)}</textarea></div>
   <div class="f"><label>Sectors</label>${chips(SPEC.sectors,s.search.sectors,"sector")}</div>
   <div class="f"><label>Media channels</label>${chips(SPEC.channels,s.search.channels,"channel")}</div>
   <div class="f"><label>Words in the piece — optional</label>
     <textarea oninput="setSS('${s.id}','keyword',this.value)"
       placeholder='"new member" or "join today"'>${esc(s.search.keyword)}</textarea>
     <div class="hint">Quote each phrase and join them with <b>or</b>. Avoid
       <b>not</b> — the archive's text search does not honour it reliably.</div></div>
   <div class="row">
     <div class="f"><label>Audience</label>
       <select onchange="setSS('${s.id}','audience',this.value)">${aud}</select></div>
     <div class="f"><label>Max results per channel</label>
       <input type="number" value="${esc(s.search.limit)}"
         oninput="setSS('${s.id}','limit',Number(this.value))"></div></div>
   <details><summary>Narrow it down further</summary>
     <label class="check"><input type="checkbox" ${s.search.only_credit_unions?"checked":""}
       onchange="setSS('${s.id}','only_credit_unions',this.checked)">
       <span>Credit unions only</span></label>
     <label class="check"><input type="checkbox" ${s.search.collapse_repeats?"checked":""}
       onchange="setSS('${s.id}','collapse_repeats',this.checked)">
       <span>Collapse repeats of the same creative
       <span style="color:#8b93a6">— stops one recycled ad filling the slide</span></span></label>
     <div class="row">
       <div class="f"><label>Company name must match</label>
         <input value="${esc(s.search.company_must_match)}"
           oninput="setSS('${s.id}','company_must_match',this.value)"></div>
       <div class="f"><label>…must not match</label>
         <input value="${esc(s.search.company_must_not_match)}"
           oninput="setSS('${s.id}','company_must_not_match',this.value)"></div></div>
     <div class="row">
       <div class="f"><label>Sub-category must include</label>
         <input value="${esc(s.search.subcategory_must_include)}"
           oninput="setSS('${s.id}','subcategory_must_include',this.value)"
           placeholder="vehicle financing"></div>
       <div class="f"><label>…must exclude</label>
         <input value="${esc(s.search.subcategory_must_exclude)}"
           oninput="setSS('${s.id}','subcategory_must_exclude',this.value)"
           placeholder="business loan, credit card"></div></div>
     <div class="hint">Sub-category comes from the database, so using it adds a lookup.
       Useful when a sector is broader than the section — Mortgage &amp; Loan also holds
       business and personal loans.</div>
   </details></div>

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
   <div class="why">Claude reads what was found, picks the best pieces, and writes the
     paragraph underneath. Five fit on a slide; more roll onto a "(cont.)" slide.</div>
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
     <label class="check"><input type="checkbox" ${s.feature.mention_cap?"checked":""}
       onchange="setF('${s.id}','mention_cap',this.checked)">
       <span>Mention it when the search hit its results cap ("At least N pieces
       were captured...")</span></label>
     <div class="f" style="max-width:210px"><label>Paragraph character limit</label>
       <input type="number" value="${esc(s.feature.callout_limit)}"
         oninput="setF('${s.id}','callout_limit',Number(this.value))">
       <div class="hint">374 is what the slide template holds.</div></div>
   </details>`:""}</div>
  </div>`;
}
const S=id=>P.sections.find(x=>x.id===id);
function toggle(id){OPEN[id]=!OPEN[id];renderSections()}
function setS(id,k,v){S(id)[k]=v;check();bump(id)}
function bump(id){
  const i=P.sections.findIndex(x=>x.id===id);
  const el=document.querySelectorAll(".sec")[i];
  if(el){const n=el.querySelector(".name");if(n)n.textContent=S(id).title||"(untitled)"}
}
function setSS(id,k,v){S(id).search[k]=v;check();
  if(typeof v==="boolean")renderSections()}
function setSH(id,k,v){S(id).sheet[k]=v;if(k==="enabled")renderSections();check()}
function setF(id,k,v){S(id).feature[k]=v;if(k==="enabled")renderSections();check()}
function sector(id,v){tog(S(id).search.sectors,v);renderSections();check()}
function channel(id,v){tog(S(id).search.channels,v);renderSections();check()}
function tog(a,v){const i=a.indexOf(v);i<0?a.push(v):a.splice(i,1)}
function col(id,name,on){
  const a=S(id).sheet.columns,i=a.indexOf(name),order=SPEC.columns.map(c=>c.name);
  if(on&&i<0){a.push(name);a.sort((x,y)=>order.indexOf(x)-order.indexOf(y))}
  else if(!on&&i>=0)a.splice(i,1);
  renderSections();renderSettings();check();
}
async function addSection(){
  const d=await (await fetch("/api/section")).json();
  d.section.title="Section "+(P.sections.length+1);
  P.sections.push(d.section);OPEN[d.section.id]=true;renderSections();check();
}
function delSection(id){
  if(!confirm("Remove this section?"))return;
  P.sections=P.sections.filter(x=>x.id!==id);renderSections();check();
}
function move(id,d){
  const i=P.sections.findIndex(x=>x.id===id),j=i+d;
  if(j<0||j>=P.sections.length)return;
  [P.sections[i],P.sections[j]]=[P.sections[j],P.sections[i]];renderSections();
}

/* ── live checking, no button ─────────────────────────────────────────────── */
function check(){
  P.name=$("#rname").value||"Untitled report";
  clearTimeout(deb);
  deb=setTimeout(async()=>{
    const r=await post("/api/check",{project:P});
    ISSUES=r.issues;
    $("#health").innerHTML=(r.errors?`<span class="pill err">${r.errors} to fix</span>`
      :r.warnings?`<span class="pill wr">${r.warnings} to look at</span>`
      :`<span class="pill ok">ready</span>`);
    renderSections();
    const general=ISSUES.filter(x=>!x.section);
    if(general.length)$("#sections").insertAdjacentHTML("afterbegin",
      general.map(m=>`<div class="msg ${m.level==="error"?"error":"warn"}">${
        esc(m.msg)}</div>`).join(""));
  },220);
}

/* ── plumbing ─────────────────────────────────────────────────────────────── */
async function post(u,o){
  const r=await fetch(u,{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify(o)});
  return await r.json();
}
function log(t,c){$("#log").innerHTML+=`\n<span class="${c||''}">${esc(t)}</span>`;
  $("#log").scrollTop=$("#log").scrollHeight}
function clearLog(){$("#log").innerHTML=""}
function show(k){$("#ov"+k).classList.add("show")}
function hide(k){$("#ov"+k).classList.remove("show")}

async function runTest(){
  const r=await post("/api/check",{project:P});
  if(r.errors){clearLog();log("Fix these first:","e");
    r.issues.filter(i=>i.level==="error").forEach(i=>log("  · "+i.msg,"e"));return}
  const res=await post("/api/test",{project:P,mode:$("#mode").value});
  if(res.error){clearLog();log("Could not start: "+res.error,"e");return}
  clearLog();
  log("Running the real pipeline — the same file Export gives you.","o");
  if(poll)clearInterval(poll);
  let seen=0;
  poll=setInterval(async()=>{
    const s=await (await fetch("/api/test/status?id="+res.run_id)).json();
    s.lines.slice(seen).forEach(l=>log(l));seen=s.lines.length;
    if(s.done){clearInterval(poll);poll=null;
      log(s.rc===0?"\nFinished cleanly.":"\nStopped with exit code "+s.rc,
        s.rc===0?"o":"e")}
  },700);
}
const WEEKDAYS=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
async function openExport(){
  const r=await post("/api/check",{project:P});
  const weekly=P.cadence==="week";
  $("#exportBody").innerHTML=
    (r.errors?`<div class="msg error">${r.errors} thing(s) still need fixing. You can
      send it anyway, but Engineering may hit trouble deploying it.</div>`:"")
    +`<div class="why">This sends "${esc(P.name||"this report")}" to the Engineering
      team so it can be reviewed and set up to run automatically.</div>
      <div class="f"><label>${weekly?"Which day of the week should it run?"
        :"Which day of the month should it run?"}</label>
      ${weekly
        ?`<select id="deployWhen">${WEEKDAYS.map(d=>
            `<option value="${d}">${d}</option>`).join("")}</select>`
        :`<input id="deployWhen" type="number" min="1" max="28" value="1">
          <div class="hint">Pick a day from 1–28 so it falls in every month.</div>`}
      </div>`;
  show("Export");
}
async function sendToEngineering(){
  const weekly=P.cadence==="week";
  const raw=$("#deployWhen").value;
  const deploy_when=weekly?("Every "+raw):("Day "+raw+" of each month");
  const r=await post("/api/export",{project:P,deploy_when});
  hide("Export");clearLog();
  if(r.error){log("Could not send: "+r.error,"e");return}
  log("Sent to Engineering.","o");
  const em=r.email||{};
  (em.sent||[]).forEach(s=>log("  emailed "+s.to,"o"));
  (em.errors||[]).forEach(s=>log("  could not email "+s.to+": "+(s.error||"unknown error"),"e"));
  if(!em.sent&&!em.errors)
    log("Could not email Engineering: "+(em.error||"unknown error"),"e");
}
async function refreshSaved(){
  const r=await (await fetch("/api/projects")).json();
  $("#savedList").innerHTML=r.projects.length
    ?r.projects.map(n=>`<li><b>${esc(n)}</b>
      <button onclick="openProject('${esc(n)}')">Open</button></li>`).join("")
    :`<li style="border:none;color:#8b93a6">Nothing saved yet.</li>`;
}
async function saveProject(){
  const n=$("#saveAs").value.trim()||P.name;
  P.name=n;$("#rname").value=n;
  const r=await post("/api/projects/save",{project:P});
  if(r.error){log("Save failed: "+r.error,"e");return}
  $("#saveAs").value="";refreshSaved();hide("Projects");
  clearLog();log('Saved as "'+r.name+'".',"o");
}
async function openProject(n){
  const r=await (await fetch("/api/projects/load?name="+encodeURIComponent(n))).json();
  if(r.error){log("Could not open: "+r.error,"e");return}
  P=r.project;OPEN={};hide("Projects");render();
}
async function newFrom(){
  const r=await (await fetch("/api/template?name="+$("#tplPick").value)).json();
  P=r.project;OPEN={};hide("Projects");render();
}
$("#rname").addEventListener("input",check);
document.querySelectorAll(".overlay").forEach(o=>o.addEventListener("click",e=>{
  if(e.target===o)o.classList.remove("show")}));
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
        if u.path == "/logo.jpg":
            try:
                return self._send(200, LOGO_FILE.read_bytes(), "image/jpeg")
            except OSError:
                return self._send(404, b"not found")
        if u.path == "/api/spec":
            return self._json({
                "channels": CHANNELS, "sectors": SECTORS, "audiences": AUDIENCES,
                "window_fields": WINDOW_FIELDS, "columns": COLUMNS,
                "sql_columns": sorted(SQL_COLUMNS),
                "templates": [{"key": k, "label": v[0]} for k, v in TEMPLATES.items()],
            })
        if u.path == "/api/template":
            name = (q.get("name") or ["blank"])[0]
            if name not in TEMPLATES:
                return self._json({"error": "unknown template"}, 404)
            return self._json({"project": TEMPLATES[name][1]()})
        if u.path == "/api/section":
            return self._json({"section": new_section()})
        if u.path == "/api/projects":
            PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
            return self._json({"projects": sorted(x.stem for x in
                                                  PROJECTS_DIR.glob("*.json"))})
        if u.path == "/api/projects/load":
            path = PROJECTS_DIR / f"{_slug((q.get('name') or [''])[0])}.json"
            if not path.is_file():
                return self._json({"error": "not found"}, 404)
            return self._json({"project": json.loads(path.read_text("utf-8"))})
        if u.path == "/api/test/status":
            with RUNS_LOCK:
                r = RUNS.get((q.get("id") or [""])[0])
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
                   if isinstance(n, ast.Name) and n.id in {"true", "false", "null"}})


def _undefined(tree) -> list[str]:
    """Generated constants and helpers referenced but never emitted — the failure mode
    when an optional block is skipped for one project shape but still used below."""
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
    watch = re.compile(r"^(HYPERLINKS|TABS|SECTIONS|SUMMARY_|HOME_STATES|MARKET_|"
                       r"CHOOSE_SYSTEM|WRITEUP_SYSTEM|EMAIL_TO|_CU_RE|_quarter|_market|"
                       r"_lookup|_row|_choose|_writeup|_summary|_candidates|XH|"
                       r"build_deck_default|_run_sql)$")
    used = {n.id for n in ast.walk(tree)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and watch.match(n.id)}
    return sorted(used - bound)


def _variants() -> list[tuple[str, dict]]:
    """Project shapes that exercise every optional branch of the generator."""
    out = []
    for key, (_label, fn) in TEMPLATES.items():
        p = fn()
        p["client"] = p["client"] or "Test Client"
        out.append((f"template: {key}", p))

    def base():
        return _example_project()

    p = base(); p["deck"]["enabled"] = False
    out.append(("workbook only, no deck", p))

    p = base(); p["workbook"]["enabled"] = False
    out.append(("deck only, no workbook", p))

    p = base()
    for s in p["sections"]:
        s["sheet"]["columns"] = ["EntryID", "Primary Company", "Headline", "Quarter"]
    out.append(("no database (search columns only)", p))

    p = base()
    for s in p["sections"]:
        s["sheet"]["columns"] = ["EntryID", "Primary Company", "Headline"]
    out.append(("no database and no Quarter column", p))

    p = base()
    for s in p["sections"]:
        s["sheet"]["tab"] = "Everything"
    out.append(("all sections share one tab", p))

    p = base(); p["cadence"] = "week"; p["anchor"] = "rolling"
    out.append(("weekly, rolling window", p))

    p = base(); p["email"].update(enabled=True, to_addr="reviewer@example.com")
    out.append(("email on", p))

    p = base(); p["workbook"]["enabled"] = False
    p["email"].update(enabled=True, to_addr="reviewer@example.com")
    out.append(("email on, deck only", p))

    p = base(); p["deck"]["enabled"] = False
    p["email"].update(enabled=True, to_addr="reviewer@example.com")
    out.append(("email on, workbook only", p))

    p = base(); p["notes"] = "Home Depot needs a Pro vs Consumer split done in code.\n" \
                             "Second line of notes."
    out.append(("notes for engineering", p))

    p = base()
    for s in p["sections"]:
        s["feature"]["enabled"] = False
    p["deck"]["summary_slide"] = False
    out.append(("nothing featured on slides", p))

    p = base()
    p["deck"].update({"section_headings": False, "summary_slide": False,
                      "title_slide": False, "closing_slide": False})
    out.append(("bare deck, no furniture", p))

    p = base()
    p["sections"][0]["search"]["subcategory_must_include"] = "vehicle financing"
    for s in p["sections"]:
        s["sheet"]["columns"] = ["EntryID", "Headline"]
    out.append(("sub-category filter, no db columns", p))

    p = base()
    p["sections"][0]["search"]["only_credit_unions"] = True
    out.append(("credit unions only", p))

    p = base(); p["sections"][0]["feature"]["count"] = 12
    out.append(("12 featured, forces cont. slides", p))

    p = new_project("Minimal"); p["client"] = "Tiny"
    p["sections"][0]["title"] = "Only section"
    p["sections"][0]["sheet"]["enabled"] = False
    out.append(("single section, deck only", p))
    return out


def selftest() -> int:
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
    print("\nSELFTEST", "FAILED" if bad else "PASSED")
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Pipelines Studio — build a Competiscan trend "
                                             "report without writing code")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    print("Pipelines Studio")
    print(f"  project root : {PIPELINES_DIR.parent if PIPELINES_DIR else '(not found)'}")
    print(f"  pipelines/   : {PIPELINES_DIR or '(not found — Test disabled)'}")
    print(f"  writes to    : {GENERATED_DIR}")
    if PIPELINES_DIR is None:
        print("\n  ! report_lib.py was not found next to this script. Editing and export")
        print("    still work, but Test cannot run the generated pipeline. Put")
        print("    report_studio.py in the project root, beside pipelines/.")
    print(f"\n  open http://{args.host}:{args.port}\n")
    try:
        ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
    return 0


if __name__ == "__main__":
    sys.exit(main())
