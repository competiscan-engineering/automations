"""
report_MonthlyBankingMerger.py
─────────────────────────────────────────────────────────────────────────────
Automates the Monthly Banking Merger Report (PPTX).

Specialized, single-purpose pipeline — it only builds this one report:

  Step 1  search_archive  → recent Banking merger EMAIL campaigns (entry IDs + OCR)
  Step 2  search_archive  → recent Banking merger SOCIAL MEDIA campaigns
  Step 3  one Bedrock/Claude call → structures all the report copy as JSON
          (needToKnow texts, per-slide insights, chosen entry IDs, mergers table CSV)
  Step 4  build_deck_default → assembles the slides
  Step 5  save PPTX to  output/Monthly_Banking_Merger_Report_YYYY_MM.pptx
  Step 6  email the PPTX to a reviewer via report_lib.notify_report_ready()
          (AWS SES) — opt-in only, gated on the MERGER_EMAIL_TO env var.

No web search. The LLM writes every word from the archive OCR text it is given.

RUN with the `research` conda env (has fastmcp / anthropic / pandas / boto3):
    C:/miniconda3/envs/research/python.exe report_MonthlyBankingMerger.py

PPT Builder (csresearchhub.com) sits behind an ALB + Cognito login — needs
PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env (report_lib.py loads it and
handles the login; see report_lib.get_ppt_session). Claude is reached through
AWS Bedrock using the same inference-profile ARN as the chat servers, so your
normal AWS credentials (the boto3 default chain) are all that is needed — no
ANTHROPIC_API_KEY.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import sys
import json
import base64
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config

# ── Make the mcp/ tools importable ───────────────────────────────────────────
# The local folder is named `mcp/`, which collides with the installed `mcp`
# SDK that fastmcp depends on. So we add mcp/ to sys.path and import the bare
# module names (mcp_serverv3, mcp_pptbuilder) — NOT `from mcp.xxx import ...`,
# which would shadow the real package and break fastmcp.
ROOT = Path(__file__).resolve().parent.parent
MCP_DIR = ROOT / "mcp"
sys.path.insert(0, str(MCP_DIR))

# Deck rendering fetches a thumbnail per entry ID and routinely exceeds the
# builder's old 60s default. Raise it before importing mcp_pptbuilder, which
# reads PPT_BUILDER_TIMEOUT at import time. Override via the env var if needed.
os.environ.setdefault("PPT_BUILDER_TIMEOUT", "300")

import mcp_serverv3  # noqa: E402  (search_archive)
import mcp_pptbuilder  # noqa: E402  (build_deck_default)
import report_lib as L  # noqa: E402  (email notification only — this script doesn't
                         # otherwise use report_lib, it predates it; report_lib.py is
                         # a sibling of this file, so a bare import just works)

# fastmcp's @mcp.tool() returns the plain function in 3.x, but unwrap defensively
# (a future version could wrap it in a FunctionTool whose original fn is on .fn).
search_archive     = getattr(mcp_serverv3.search_archive, "fn", mcp_serverv3.search_archive)
build_deck_default = getattr(mcp_pptbuilder.build_deck_default, "fn", mcp_pptbuilder.build_deck_default)

# ── Config ───────────────────────────────────────────────────────────────────
# Same Bedrock inference-profile ARN + client config the chat servers use.
MODEL_ARN = "arn:aws:bedrock:us-east-2:078398740737:application-inference-profile/myhse60a8wao"
BEDROCK_CONFIG = Config(
    region_name="us-east-2",
    retries={"max_attempts": 3, "mode": "adaptive"},
    read_timeout=120,
    connect_timeout=10,
)
MAX_TOKENS = 4000  # headroom for the JSON (mergers CSV + 4 text fields); spec said 2000

OUTPUT_DIR   = ROOT / "output"
# Recipient for the end-of-run "report ready" email — unset means "don't
# email, just save the PPTX" (opt-in only, same reasoning as every other
# report pipeline's EMAIL_TO). Set via env: MERGER_EMAIL_TO=someone@competiscan.com
EMAIL_TO     = os.environ.get("MERGER_EMAIL_TO") or None
SECTORS      = ["Banking"]
KEYWORD      = '"merger" or "acquisition" or "merged" or "acquired"'
SEARCH_LIMIT = 50
N_EMAIL      = 4   # Recent Observations
N_SOCIAL     = 4   # Social Media Observations
MAX_ENTRY_IDS = 5  # build_deck_default entry_ids slide accepts 1–5 IDs


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _search(channel: str) -> list[dict]:
    """search_archive for merger campaigns in a single channel; [] on failure."""
    results = search_archive(
        sectors=SECTORS,
        media_channels=[channel],
        keyword=KEYWORD,
        limit=SEARCH_LIMIT,
    )
    # _datagrid_search returns [{"error": ...}] when the REST call fails.
    if results and isinstance(results[0], dict) and "error" in results[0]:
        print(f"   ! search_archive error for {channel}: {results[0]['error']}")
        return []
    # keep only records that actually carry an entry_id
    return [r for r in results if r.get("entry_id")]


def _format_for_prompt(records: list[dict], label: str) -> str:
    """Compact, token-light view of the search results for the LLM."""
    if not records:
        return f"=== {label} (0 results) ===\n(none found in the archive)"
    lines = []
    for i, r in enumerate(records, 1):
        ocr = (r.get("ocr_text") or "").strip().replace("\n", " ")[:400]
        lines.append(
            f'{i}. entry_id={r.get("entry_id")} | company={r.get("company_name")} '
            f'| headline={r.get("product_headline")} | channel={r.get("media_channel")}\n'
            f'   OCR: {ocr}'
        )
    return f"=== {label} ({len(records)} results) ===\n" + "\n".join(lines)


def _call_claude(system: str, prompt: str) -> str:
    """Single non-streaming Bedrock/Claude call. Returns the response text."""
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": system,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS,
    }
    client = boto3.client("bedrock-runtime", config=BEDROCK_CONFIG)
    resp = client.invoke_model(modelId=MODEL_ARN, body=json.dumps(body))
    payload = json.loads(resp["body"].read())
    if payload.get("stop_reason") == "max_tokens":
        print("   ! Warning: response hit max_tokens — JSON may be truncated.")
    return next(
        (b.get("text", "") for b in payload.get("content", []) if b.get("type") == "text"),
        "",
    )


def _extract_json(text: str) -> dict:
    """Parse the model output into a dict, tolerating stray fences/preamble."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        text = text[start:end + 1]
    return json.loads(text)


def _pick_ids(chosen, records: list[dict], want: int) -> list[str]:
    """Keep only chosen IDs that really exist; fall back to the most recent."""
    available = [r["entry_id"] for r in records]
    avail_set = set(available)
    picked = [c for c in (chosen or []) if c in avail_set]
    if not picked:                       # model returned nothing usable
        picked = available[:want]
    return picked[:MAX_ENTRY_IDS]


# The builder hyperlinks the LAST visible column via a hidden `link` column of
# EntryIDs, so "Sample Web Communication" must be the last visible column —
# hence Notes sits before it. The trailing `link` column is not displayed.
MERGERS_HEADER = (
    "Bank/Credit Union/Fintech,Joining With,Expected Completion Date,Notes,"
    "Sample Web Communication,link"
)


def _clean_cell(val) -> str:
    """Builder's CSV parser is naive — strip commas/newlines from every field."""
    return str(val or "").strip().replace("\n", " ").replace(",", ";")


def _cap(text: str, limit: int = 375) -> str:
    """Safety net for the slide's insight fields (max 375 chars). Trims on a
    word boundary with an ellipsis if the model overshoots the prompt limit."""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    cut = text[:limit - 1]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut.rstrip(" .,;:") + "…"


def _save_pptx(raw: bytes, out_path: Path) -> Path:
    """Write the deck, falling back to a numbered sibling if the target is locked
    (usually because it's open in PowerPoint) so a slow render isn't discarded."""
    try:
        out_path.write_bytes(raw)
        return out_path
    except PermissionError:
        for n in range(2, 100):
            alt = out_path.with_name(f"{out_path.stem} ({n}){out_path.suffix}")
            try:
                alt.write_bytes(raw)
                return alt
            except PermissionError:
                continue
        raise


def _build_mergers_csv(mergers, valid_entry_ids: set) -> str:
    """Structured merger rows → builder CSV.

    A row with an archive `entry_id` puts that ID in the hidden `link` column;
    the builder fetches its product PDF link and hyperlinks the "Sample Web
    Communication" cell. A row without one shows its web URL as plain text
    (the builder only hyperlinks via EntryID, not arbitrary URLs).
    """
    rows = [MERGERS_HEADER]
    for m in mergers or []:
        bank = _clean_cell(m.get("bank"))
        if not bank:
            continue
        joining = _clean_cell(m.get("joining_with"))
        date    = _clean_cell(m.get("expected_date")) or "TBD"
        notes   = _clean_cell(m.get("notes"))
        entry_id = m.get("entry_id") if m.get("entry_id") in valid_entry_ids else ""
        if entry_id:
            comm = _clean_cell(m.get("label")) or "Product Communication"
            link = entry_id
        else:
            comm = _clean_cell(m.get("web_url")) or "TBD"
            link = ""
        rows.append(",".join([bank, joining, date, notes, comm, link]))
    return "\n".join(rows)


# ─────────────────────────────────────────────────────────────────────────────
# LLM prompt
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "You are a market research analyst at Competiscan writing the Monthly Banking "
    "Merger Report. You are given real direct-marketing campaigns pulled from "
    "Competiscan's archive (email and social media), each with an entry_id, company, "
    "headline, and OCR text. Work only from this material — do not invent campaigns, "
    "companies, or facts that are not present in the OCR text. Write in a concise, "
    "professional tone suitable for a client-facing slide deck.\n\n"
    "Return ONE valid JSON object and nothing else — no markdown, no code fences, "
    "no commentary. Use EXACTLY these keys:\n"
    '  "acquisition_text"  : 2-3 sentences summarizing acquisition/merger announcements found.\n'
    '  "operational_text"  : 2-3 sentences summarizing operational merger communications (rebrands, system conversions, member notices).\n'
    '  "email_entry_ids"   : list of the 4 best EMAIL entry_ids showing merger member communications (choose only from the EMAIL entry_ids provided).\n'
    '  "email_insight"     : 1-2 sentences on what these email campaigns communicated post-merger. Maximum 375 characters.\n'
    '  "social_entry_ids"  : list of the 3 best SOCIAL MEDIA entry_ids (choose only from the SOCIAL entry_ids provided).\n'
    '  "social_insight"    : 1-2 sentences on the social media merger messaging. Maximum 375 characters.\n'
    '  "mergers"           : a list of objects, one per distinct merger you can identify '
    "from the OCR text (do not invent mergers). No field may contain a comma. Each object has:\n"
    '      "bank"          : the merging or acquired bank, credit union, or fintech.\n'
    '      "joining_with"  : the institution it is joining or being acquired by.\n'
    '      "expected_date" : expected completion, e.g. "Q1 2027"; use "TBD" if unknown.\n'
    '      "notes"         : one short sentence of context (no commas).\n'
    '      "entry_id"      : the entry_id of the archive campaign about this merger, chosen '
    "ONLY from the entry_ids listed above; use null if none of the provided campaigns covers it.\n"
    '      "label"         : short link text for the communication, e.g. "Merger Announcement" '
    'or "Acquisition Announcement" (used when entry_id is set).\n'
    '      "web_url"       : a public press-release URL for this merger, ONLY when entry_id is '
    "null and you are confident the URL is real; otherwise null.\n"
)

PROMPT_TEMPLATE = (
    "Report month: {month}\n\n"
    "{email_block}\n\n"
    "{social_block}\n\n"
    "Produce the JSON object now."
)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    now = datetime.now()
    month_str = now.strftime("%B %Y")          # e.g. "July 2026"
    file_stamp = now.strftime("%Y_%m")         # e.g. "2026_07"
    print(f"Monthly Banking Merger Report — {month_str}")

    # Step 1 & 2 — archive search (no web) ------------------------------------
    print("Step 1/6  Searching archive: Banking merger EMAIL campaigns…")
    email_results = _search("Email")
    print(f"          {len(email_results)} email results")

    print("Step 2/6  Searching archive: Banking merger SOCIAL MEDIA campaigns…")
    social_results = _search("Social Media")
    print(f"          {len(social_results)} social results")

    if not email_results and not social_results:
        print("ERROR: archive returned no merger campaigns for either channel. "
              "Is the VPN/REST reachable? Aborting.")
        return 1

    # Step 3 — single LLM call structures the report copy ---------------------
    print("Step 3/6  Asking Claude (Bedrock) to structure the report copy…")
    prompt = PROMPT_TEMPLATE.format(
        month=month_str,
        email_block=_format_for_prompt(email_results, "EMAIL merger campaigns"),
        social_block=_format_for_prompt(social_results, "SOCIAL MEDIA merger campaigns"),
    )
    raw = _call_claude(SYSTEM_PROMPT, prompt)
    try:
        data = _extract_json(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"ERROR: could not parse JSON from the model response: {exc}")
        print("----- raw response -----")
        print(raw[:2000])
        return 1

    email_ids  = _pick_ids(data.get("email_entry_ids"),  email_results,  N_EMAIL)
    social_ids = _pick_ids(data.get("social_entry_ids"), social_results, N_SOCIAL)
    print(f"          email IDs: {email_ids}")
    print(f"          social IDs: {social_ids}")

    # Step 4 — assemble the slides -------------------------------------------
    print("Step 4/6  Building the deck…")
    slides: list[dict] = [
        {"type": "title", "data": {
            "title": "Monthly Banking Merger Report",
            "date": month_str,
        }},
        {"type": "needToKnow", "data": {
            "title1": "Acquisition Announcement",
            "text1":  data.get("acquisition_text")
                      or "No new acquisition announcements identified in the archive this month.",
            "title2": "Operational Updates",
            "text2":  data.get("operational_text")
                      or "No operational merger communications identified in the archive this month.",
        }},
    ]

    if email_ids:
        slides.append({"type": "entry_ids", "data": {
            "slideTitle": "Recent Observations",
            "entryIds":   email_ids,
            "insight":    _cap(data.get("email_insight", "")),
        }})
    if social_ids:
        slides.append({"type": "entry_ids", "data": {
            "slideTitle": "Social Media Observations",
            "entryIds":   social_ids,
            "insight":    _cap(data.get("social_insight", "")),
        }})

    valid_ids = {r["entry_id"] for r in (email_results + social_results)}
    mergers_csv = _build_mergers_csv(data.get("mergers"), valid_ids)
    slides.append({"type": "newSection", "data": {"title": "Ongoing Mergers"}})
    slides.append({"type": "table", "data": {
        "title":    "Ongoing Mergers",
        "subtitle": "Banking sector merger activity",
        "csv":      mergers_csv,
    }})
    slides.append({"type": "closing", "data": {}})

    result = build_deck_default(
        deck_title=f"Monthly Banking Merger Report — {month_str}",
        slides=slides,
    )

    # Step 5 — save the PPTX to output/ --------------------------------------
    print("Step 5/6  Saving the PPTX…")
    if not isinstance(result, dict) or "error" in result:
        err = result.get("error") if isinstance(result, dict) else result
        print(f"ERROR: PPT builder failed: {err}")
        return 1

    b64 = result.get("pptx_base64")
    if not b64:
        print(f"ERROR: PPT builder returned no PPTX. Response: {result}")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"Monthly_Banking_Merger_Report_{file_stamp}.pptx"
    try:
        saved = _save_pptx(base64.b64decode(b64), out_path)
    except PermissionError:
        print(f"ERROR: could not write to {OUTPUT_DIR} (permission denied).")
        print(f"       The builder also saved a copy at: {result.get('filepath')}")
        return 1
    if saved != out_path:
        print(f"NOTE: {out_path.name} was locked (open in PowerPoint?) — "
              f"saved as {saved.name} instead.")
    print(f"Saved: {saved}")

    # Step 6 — email the deliverable to a reviewer, only if a recipient was
    # explicitly configured (MERGER_EMAIL_TO env var) — an actual send has real
    # inbox-facing consequences, so this is opt-in, never automatic.
    if EMAIL_TO:
        print(f"Step 6/6  Emailing deliverable to {EMAIL_TO}…")
        email_result = L.notify_report_ready(
            report_name="Monthly Banking Merger Report", period_label=month_str,
            attachment_paths=[saved], to_addr=EMAIL_TO,
        )
        if email_result.get("status") == "sent":
            print(f"          sent (message_id={email_result.get('message_id')})")
        else:
            print(f"          !! email FAILED: {email_result.get('error')} — "
                  f"file is still saved locally, nothing lost")
    else:
        print("Step 6/6  Skipped emailing — no MERGER_EMAIL_TO set. File is saved locally only.")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
