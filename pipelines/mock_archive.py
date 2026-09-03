#!/usr/bin/env python3
"""
mock_archive.py — stand in for everything outside this machine, for testing only
═══════════════════════════════════════════════════════════════════════════════════════

WHAT THIS IS FOR
    A generated pipeline is only really proved by running it. Running one for real costs
    archive quota, a Bedrock call per section, a deck build over the network and, for
    some columns, an SSH tunnel to production MySQL — so nobody runs eleven of them to
    check a refactor, which means nobody checks the refactor.

    This module replaces exactly the boundaries that leave the machine, and nothing else:

        cs_api.count / .search / .ocr        the archive
        report_lib.call_claude               Bedrock
        report_lib.load_tool                 the deck builder and the SQL tool
        report_lib.save_pptx                 writes the slide list as JSON instead
        report_lib.send_email                records the send instead of making one

    Everything above those lines is the real code: the real request bodies, the real
    window slicing, the real dedup and post-filters, the real pick_ids, the real
    fit_text, the real openpyxl workbook writer, the real XH.complete_rows mapping.
    A generated pipeline runs UNMODIFIED — there is not one test hook in generated code,
    because the one thing this whole tool guarantees is that what a researcher runs is
    what Engineering deploys.

WHY IT IS DETERMINISTIC
    Every answer is a hash of the question. The same request body returns the same rows
    on every machine and every run, and the model returns the same picks and the same
    prose for the same prompt. That is what lets the two-phase equivalence test mean
    something: if a single-shot run and a pick-then-build run differ, it is the plumbing
    that differs, not the weather.

HOW IT IS INSTALLED
    Not by importing it from a pipeline — nothing generated knows this file exists. The
    selftest writes a one-line sitecustomize.py into a scratch directory, puts that
    directory on PYTHONPATH, and Python imports it before the pipeline starts:

        import pipelines.mock_archive as M; M.install()

KNOBS (environment)
    RS_MOCK_ROWS      how many pieces the archive "holds" per section x channel (60)
    RS_MOCK_CAPPED    "1" makes every count come back past the archive's count cap,
                      which is how the "at least N" and SUSPECT paths get exercised
    RS_MOCK_EMPTY     "1" makes every search return nothing
    RS_MOCK_SLOW      seconds to sleep in every count, so a run is long enough to
                      actually be stopped mid-flight
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, timedelta
from pathlib import Path

COMPANIES = [
    "Northgate Savings Bank", "Pinecrest Credit Union", "Harbor Mutual",
    "Cascadia Financial", "Blue Ridge Bancorp", "Meridian Trust",
    "Lakeshore Community Bank", "Summit Federal", "Ironwood Bank",
    "Vantage Point Credit Union", "Copperfield Financial", "Redstone Savings",
]
PRODUCTS = ["Rewards Checking", "High-Yield Savings", "12-Month CD", "Platinum Card",
            "Home Equity Line", "Auto Refinance", "Cash-Back Card", "Money Market"]
HEADLINES = [
    "Open a checking account and earn a $300 bonus when you set up direct deposit",
    "Earn 4.35% APY on balances over $10,000 with no monthly maintenance fee",
    "Lock in 5.10% APY for 12 months — federally insured, no minimum to open",
    "0% intro APR for 18 months on balance transfers, then 18.24%-27.99% variable",
    "Borrow up to 90% of your home's value with no closing costs",
    "Refinance your auto loan and skip two payments",
    "Unlimited 2% cash back on every purchase, every day",
    "Rates as low as 4.99% APR for qualified members",
]


def _seed(*parts) -> int:
    raw = "|".join(str(x) for x in parts)
    return int(hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12], 16)


def _rows_wanted() -> int:
    try:
        return max(1, int(os.environ.get("RS_MOCK_ROWS") or 60))
    except ValueError:
        return 60


def _body_key(body: dict) -> str:
    """A stable name for one request, ignoring the parts that do not change WHICH
    pieces match — the row limit and whether a total was asked for."""
    skip = {"limit", "include_total", "sort"}
    return json.dumps({k: v for k, v in sorted(body.items()) if k not in skip},
                      sort_keys=True, default=str)


def _make_rows(body: dict, n: int) -> list:
    """`n` pieces that satisfy this request, spread across the window and companies."""
    key = _body_key(body)
    seed = _seed(key)
    try:
        start = date.fromisoformat(str(body.get("date_from")))
        end = date.fromisoformat(str(body.get("date_to")))
    except (TypeError, ValueError):
        start, end = date(2026, 1, 1), date(2026, 1, 31)
    span = max((end - start).days, 0)
    channel = (body.get("media_channel") or ["Direct Mail"])[0]

    out = []
    for i in range(n):
        h = _seed(key, i)
        day = start + timedelta(days=(h % (span + 1)) if span else 0)
        company = COMPANIES[(h >> 8) % len(COMPANIES)]
        product = PRODUCTS[(h >> 16) % len(PRODUCTS)]
        headline = HEADLINES[(h >> 24) % len(HEADLINES)]
        eid = f"{day:%Y-%m-%d}-{1000 + ((seed + i) % 9000)}"
        out.append({
            "entry_id": eid,
            "product_id": 100000 + ((seed + i * 7) % 899999),
            "company": company,
            "product_name": f"{company} {product}",
            "product_headline": headline,
            "media_channel": channel,
            "audience": (body.get("audience") or ["Consumer"])[0],
            "mailing_type": "Acquisition",
            "delivery_type": "First Class",
            "postage": "Permit #",
            "country": "United States",
            "search_date": f"{day:%Y-%m-%d}T00:00:00",
            "approved_date": f"{day + timedelta(days=9):%Y-%m-%d}T00:00:00",
            "added_to_database": f"{day + timedelta(days=12):%Y-%m-%d}T00:00:00",
            "pdf_url": f"https://mock.invalid/pdf/{eid}.pdf",
        })
    # The archive sorts newest first, and _collect asks it to. Several guardrails read
    # the ORDER of these rows — the shortlist, the top-up, the replacement pool — so
    # getting it wrong here would quietly make the tests test the wrong thing.
    out.sort(key=lambda r: r["search_date"], reverse=True)
    return out


def install() -> None:
    """Swap out every boundary that leaves this machine. Idempotent."""
    import pipelines.cs_api as CS
    import pipelines.report_lib as L

    if getattr(CS, "_MOCKED", False):
        return
    CS._MOCKED = True

    empty = os.environ.get("RS_MOCK_EMPTY") == "1"
    capped = os.environ.get("RS_MOCK_CAPPED") == "1"
    try:
        slow = float(os.environ.get("RS_MOCK_SLOW") or 0)
    except ValueError:
        slow = 0.0

    # ── the archive ────────────────────────────────────────────────────────────────
    def count(body, *, exact=False, timeout=None):
        if slow:
            import time
            time.sleep(slow)
        n = 0 if empty else _rows_wanted()
        return {"total": CS.COUNT_CAP if capped else n,
                "total_is_capped": capped,
                "took_ms": 1, "cached": False,
                "resolved_filters": {"sector_ids": [1], "enhanced": {}}}

    def search(body, *, timeout=None):
        want = int(body.get("limit") or 100)
        rows = [] if empty else _make_rows(body, min(want, _rows_wanted()))
        return {"results": rows, "total": len(rows),
                "truncated": len(rows) >= want and not empty}

    def ocr(entry_id, *, include_documents=False, timeout=60):
        h = _seed("ocr", entry_id)
        if h % 11 == 0:        # some pieces genuinely have no scanned text on file
            return None
        body = (f"{HEADLINES[h % len(HEADLINES)]} "
                f"Member FDIC. Rate effective {entry_id[:10]}. "
                f"Annual Percentage Yield accurate as of the date shown. "
                f"Offer code {entry_id}. ") * 6
        return {"entry_id": entry_id, "ocr_available": True, "text": body,
                "truncated": False}

    CS.count, CS.search, CS.ocr = count, search, ocr

    # ── Bedrock ────────────────────────────────────────────────────────────────────
    def call_claude(system, prompt, max_tokens=4000):
        """Answer the three prompts the generated pipelines actually send.

        Keyed off the prompt, so the same slide gets the same picks and the same
        sentence every time. The choice is the first N candidate ids in the order they
        were offered — which also means the selection guardrails, not the model, are
        what decide the final list.
        """
        if '"column1"' in system:
            return json.dumps({
                "column1": "Rate-led deposit offers dominated the period, with several "
                           "institutions leading on APY rather than on a cash bonus.",
                "column2": "Card acquisition was steadier, and balance-transfer intro "
                           "periods clustered around eighteen months."})
        if '"callout"' in system:
            names = []
            for line in prompt.splitlines():
                if line.startswith("- ") and "|" in line:
                    names.append(line.split("|")[1].strip())
            who = ", ".join(dict.fromkeys(names)) or "the featured institutions"
            return json.dumps({"callout": (
                f"{who} led this period's activity. Each piece leads with the headline "
                f"rate or bonus rather than with brand messaging, and the terms are "
                f"stated on the face of the creative. The offers are close enough in "
                f"structure that the differentiator is the rate itself.")})
        # "Choose up to N pieces ..." — N is the RANKED list the pipeline wants,
        # which is the slide's size times RESERVE_FACTOR. Returning fewer would make
        # the reserve look empty when it is not.
        import re as _re
        m = _re.search(r"Choose up to (\d+) pieces", prompt)
        want = int(m.group(1)) if m else 4
        ids = []
        for line in prompt.splitlines():
            if line.startswith("- ") and "|" in line:
                ids.append(line[2:].split("|")[0].strip())
        return json.dumps({"entry_ids": ids[:want],
                           "reasoning": "the first candidates offered, in order"})

    L.call_claude = call_claude

    # ── the deck builder and the SQL tool ──────────────────────────────────────────
    def build_deck_default(deck_title=None, slides=None, **kw):
        return {"deck_title": deck_title, "slides": slides or []}

    def _run_sql(query):
        """A DataFrame shaped exactly like build_query's own result, so the real
        XH.complete_rows mapping is what turns it into worksheet columns."""
        import re
        import pandas as pd
        ids = re.findall(r"'([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]+)'", query or "")
        rows = []
        for eid in ids:
            h = _seed("sql", eid)
            if h % 13 == 0:     # not every id has a row: the query inner-joins
                continue
            rows.append({
                "entry_id": eid, "product_id": 100000 + (h % 899999),
                "primary_company": COMPANIES[h % len(COMPANIES)],
                "product_name": PRODUCTS[(h >> 4) % len(PRODUCTS)],
                "product_headline": HEADLINES[(h >> 8) % len(HEADLINES)],
                "media_channel": "Direct Mail", "audience": "Consumer",
                "mailing_type": "Acquisition",
                "bucket_name": "mock-bucket", "document_path": "2026/01/01/",
                "document_filename": f"{eid}.pdf",
                "additional_companies": None,
                "sectors": "Banking", "categories": "Deposits",
                "sub_categories": "Checking", "sub_sub_categories": None,
                "is_prescreen": h % 2, "refinance": h % 3 == 0,
                "jumbo_ncnfg": 0, "va": 0, "fha": 0, "conventional": h % 5 == 0,
                "usda": 0, "socialmedia_adtype": None,
                "states": "Illinois||Wisconsin",
                "ages": "30-39", "incomes": "$100k-$149k",
            })
        return pd.DataFrame(rows)

    real_load_tool = L.load_tool

    def load_tool(module_name, attr):
        if (module_name, attr) == ("mcp_pptbuilder", "build_deck_default"):
            return build_deck_default
        if (module_name, attr) == ("mcp_serverv3", "_run_sql"):
            return _run_sql
        return real_load_tool(module_name, attr)

    L.load_tool = load_tool

    # ── the deliverables that would leave the machine ──────────────────────────────
    def save_pptx(result, out_path):
        """Write the slide list as JSON beside a stub .pptx.

        The JSON is the artefact the equivalence test compares: a real .pptx is a zip
        whose bytes differ between two identical builds, so comparing those would prove
        nothing. The slide list is exactly what the pipeline decided.
        """
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"mock pptx")
        out_path.with_suffix(".slides.json").write_text(
            json.dumps(result.get("slides") if isinstance(result, dict) else result,
                       indent=1, default=str), encoding="utf-8")
        return out_path

    L.save_pptx = save_pptx

    def send_email(attachment_paths, to_addr=None, subject="", body="", from_addr=None):
        out = Path(os.environ.get("RS_OUTPUT_DIR") or ".")
        out.mkdir(parents=True, exist_ok=True)
        paths = ([attachment_paths] if isinstance(attachment_paths, (str, Path))
                 else list(attachment_paths))
        (out / "_email.json").write_text(json.dumps(
            {"to": to_addr, "subject": subject,
             "attachments": [Path(x).name for x in paths]}, indent=1), encoding="utf-8")
        return {"status": "sent", "message_id": "mock-message-id"}

    L.send_email = send_email

    def notify_report_ready(report_name, period_label, attachment_paths,
                            to_addr=None, from_addr=None):
        return send_email(attachment_paths, to_addr=to_addr,
                          subject=f"{report_name} — {period_label}")

    L.notify_report_ready = notify_report_ready


SITECUSTOMIZE = """# written by pipeline_studio3 --selftest --offline; safe to delete
import os
import sys

root = os.environ.get("RS_MOCK_ROOT")
if root and root not in sys.path:
    sys.path.insert(0, root)
try:
    import pipelines.mock_archive as _m
    _m.install()
except Exception as exc:               # never let the shim hide a real failure
    print(f"MOCK INSTALL FAILED: {exc}", file=sys.stderr)
"""
