"""
report_HarborstoneWeekly.py
─────────────────────────────────────────────────────────────────────────────
Harborstone weekly competitive update — PowerPoint + Excel.

Same shape as report_MonthlyBankingMerger.py: it uses ONLY the search_archive
and build_deck_default tools (no SQL, no product-detail). Everything reusable
lives in report_lib.py.

Four categories, each an OCR-keyword + sector search:
  Membership (credit unions only) · Checking · Auto · Home Lending

  Step 1  category×channel search_archive calls (sequential — see note below)
  Step 2  keep entries whose entry_id DATE falls in the week; CU-filter Membership;
          SQL-enrich for Excel; write the workbook (EntryID + PDF hyperlinks)
  Step 3  pre-sort each category's candidates by (market_tier, channel_tier) — pure
          Python, no LLM — then 4 parallel Claude "Selection" calls pick
          FEATURED_PER_SLIDE entry_ids from that pre-sorted shortlist
  Step 4  cross-slide dedup (sequential, CATEGORIES order): an entry_id may match
          more than one category's search, but must never appear on two slides
  Step 5  4 parallel Claude "Callout" calls write one summary paragraph per slide,
          given ONLY the already-chosen entries (entry_id is not shown to this call,
          so it cannot leak an ID into the callout text)
  Step 6  build the deck (title → 4 category slides → closing) and save both files
  Step 7  email the deck+Excel to a reviewer via L.notify_report_ready() (AWS
          SES) — opt-in only, gated on the HARBOR_EMAIL_TO env var being set.

Market tier (In Market = WA/OR/CA vs National) and channel tier (Email/Direct Mail
top; Online Video/Display/Print/Website mid; Social Media/Search Engine Marketing
lowest — except Membership, where Social Media is promoted to mid-tier since good
broad membership-acquisition content is rare) are both computed deterministically in
Python and used only to PRE-SORT what the Selection call sees — the LLM may still
override that ordering when a piece's content type is a clearly better thematic fit
(this matters most for Membership; see its category guidance below). search_archive
caps at 50 results per call.

RUN (research env — fastmcp / anthropic / pandas / boto3 / openpyxl):
    C:/miniconda3/envs/research/python.exe pipelines/report_HarborstoneWeekly.py
PPT Builder (csresearchhub.com) sits behind an ALB + Cognito login — needs
PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env (report_lib.py loads it and
handles the login; see report_lib.get_ppt_session). Claude via AWS Bedrock (boto3 chain).
─────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

# Console prints use a few non-ASCII glyphs; make stdout UTF-8 so it never
# crashes on a cp1252 Windows console.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# report_lib lives at the project root (parent of pipelines/).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Raise the builder timeout BEFORE the builder module is imported (load_tool).
os.environ.setdefault("PPT_BUILDER_TIMEOUT", "300")

import pipelines.report_lib as L  # noqa: E402
import pipelines.report_lib_excel_helper as XH  # noqa: E402  (SQL enrichment for Excel)

search_archive     = L.load_tool("mcp_serverv4", "search_archive")
build_deck_default = L.load_tool("mcp_pptbuilder", "build_deck_default")
_run_sql           = L.load_tool("mcp_serverv3", "_run_sql")  # SSH → MySQL, for Excel enrichment
#search_by_date        = L.load_tool("mcp_serverv3", "search_by_date")  # for debugging, not used in the pipeline

# ── Config (the only thing that changes per run) ─────────────────────────────
CLIENT             = "Harborstone"
# Explicit window override (or the HARBOR_WEEK_START/END env vars); None -> auto.
WEEK_START         = os.environ.get("HARBOR_WEEK_START") or None   # "2026-07-07"
WEEK_END           = os.environ.get("HARBOR_WEEK_END") or None     # "2026-07-14"
# Recipient for the end-of-run "report ready" email — unset means "don't
# email, just save the files" (opt-in only — an actual send has real inbox-
# facing consequences). Set via env: HARBOR_EMAIL_TO=someone@competiscan.com
EMAIL_TO           = os.environ.get("HARBOR_EMAIL_TO") or None
PRIORITY_STATES    = ["Washington", "Oregon"]
SECONDARY_STATES   = ["California"]
NEARBY_STATES      = ["Idaho", "Nevada", "Alaska", "Montana"]
MEMBERSHIP_CU_ONLY = True
FEATURED_PER_SLIDE = 4
LLM_SHORTLIST_CAP  = 150     # show the LLM ALL in-window records (was 40, which
                             # hid candidates in high-volume weeks and defeated the
                             # WA/OR priority). ~150 covers a week; token-cheap.
SEARCH_LIMIT       = 50      # search_archive hard-caps at 50 per call
CALLOUT_LIMIT      = 374     # each slide callout must be < 375 chars, no ellipsis
OUTPUT_DIR         = PROJECT_ROOT / "output"

# Market column: "In Market" when the piece targets WA/OR/CA, else "National".
MARKET_HEADER_COLOR = "FFA500"   # orange fill for the Market header cell (L2)
IN_MARKET_STATES    = {"wa", "or", "ca", "washington", "oregon", "california"}

# Channel priority tiers used to PRE-SORT candidates before the Selection LLM
# call sees them: 0 = top priority, 1 = mid, 2 = lowest. This is a prior/hint,
# not a hard filter — the LLM may still pick a lower-tier entry when its
# content type is a clearly better fit (see the Membership category guidance).
CHANNEL_TIER = {
    "Email": 0, "Direct Mail": 0,
    "Online Video": 1, "Online Display": 1, "Print": 1, "Website/URL": 1,
    "Social Media": 2, "Search Engine Marketing": 2,
}
# Per-category overrides layered on top of CHANNEL_TIER. Membership promotes
# Social Media to tier 1 — broad membership-acquisition content is rare and
# often lives on social — but Search Engine Marketing stays tier 2 even here.
CHANNEL_TIER_OVERRIDES: dict[str, dict[str, int]] = {
    "Membership": {"Social Media": 1},
}
CHANNEL_TIER_LABEL = {0: "Top", 1: "Mid", 2: "Low"}
UNKNOWN_CHANNEL_TIER = 1  # an unrecognized/blank channel defaults to Mid, not an extreme

# Excel headers (columns search_archive can't supply render blank on purpose,
# so you can see which unmapped values actually matter).
HEADERS_19 = [
    "Primary Company", "Additional Companies", "Primary Sector", "Primary Category",
    "Primary Sub Category", "EntryID", "Quarter", "Headline", "Product", "PDF Content",
    "Media Channel", "Market", "State/Province", "Age", "Income", "Mailing Type",
    "Publication", "Network Name", "Social Media Ad Type",
]
HEADERS_21 = HEADERS_19[:16] + [
    "Pre-Screen", "Mortgage & Loan - Application Type",
] + HEADERS_19[16:]

HYPERLINKS = {
    "EntryID":     ("https://cp.competiscan.com/productdetail?id={pid}", "{entry_id}"),
    "PDF Content": ("https://www.competiscan.com/productDocuments.php?id={pid}", "PDF Content"),
}

# Category specs — sector + OCR keyword stand in for the (now-dropped) category filters.
CATEGORIES = [
    {
        "key": "Membership", "slide_title": "Membership Acquisition", "sheet": "Membership",
        "sectors": ["Banking"],
        "channels": ["Direct Mail", "Email", "Online Display", "Online Video", "Print", "Search Engine Marketing", "Social Media", "Website/URL"],
        "keyword": '"join" or "become" or "new member" or "new members" or "members" not "join us"',
        "cu_only": True, "headers": HEADERS_19,
        "guidance": (
            "Credit unions ONLY. This slide is about general membership growth, not a "
            "specific product — STRONGLY AVOID any piece that advertises one specific "
            "account or product type (e.g. a 'Checking Account' or 'Savings Account' ad, "
            "or anything naming a specific loan or card product); that content belongs on "
            "a different slide, even if it ranks well on market/channel tier. Also AVOID "
            "pieces that read as addressed to an EXISTING member (e.g. account-anniversary "
            "messages, statements, renewal notices) rather than a prospective new member. "
            "PREFER referral campaigns (e.g. 'refer a friend and earn $X when they join'), "
            "and PREFER generalized brand-awareness messaging about why someone should join "
            "a credit union or the 'credit union difference' (community focus, "
            "member-owned values, better rates/service than a bank) — these are the "
            "strongest fits for this slide even when their market/channel tier is average "
            "or low, since genuinely on-theme membership content is rare in the archive."
        ),
    },
    {
        "key": "Checking", "slide_title": "Checking Acquisition", "sheet": "Checking",
        "sectors": ["Banking"],
        "channels": ["Direct Mail", "Email", "Online Display", "Online Video", "Print", "Search Engine Marketing", "Social Media", "Website/URL"],
        "keyword": '',
        "cu_only": False, "headers": HEADERS_19,
        "guidance": "Any financial institution. Checking-account acquisition content; prioritize nearby credit unions and banks.",
    },
    {
        "key": "Auto", "slide_title": "Auto Lending", "sheet": "Auto",
        "sectors": ["Mortgage & Loan"],
        "channels": ["Direct Mail", "Email", "Online Display", "Online Video", "Print", "Search Engine Marketing", "Social Media", "Website/URL"],
        "keyword": '',
        "cu_only": False, "headers": HEADERS_21,
        "guidance": "Any financial institution. Vehicle financing / re-financing acquisition content.",
    },
    {
        "key": "Home", "slide_title": "Home Lending", "sheet": "Home Lending",
        "sectors": ["Mortgage & Loan"],
        "channels": ["Direct Mail", "Email", "Online Display", "Online Video", "Print", "Search Engine Marketing", "Social Media", "Website/URL"],
        "keyword": '',
        "cu_only": False, "headers": HEADERS_21,
        "guidance": "Any financial institution. Home-equity and mortgage acquisition content.",
    },
]

_CU_RE = re.compile(r"credit union|\bFCU\b|\bF\.?C\.?U\.?\b|\bCU\b", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _week_window() -> tuple[date, date]:
    """Return (start, end). Default: most recent completed Mon→Mon window."""
    if WEEK_START:
        start = date.fromisoformat(WEEK_START)
        end = date.fromisoformat(WEEK_END) if WEEK_END else start + timedelta(days=7)
        return start, end
    today = date.today()
    last_monday = today - timedelta(days=today.weekday())
    return last_monday - timedelta(days=7), last_monday


def _entry_date(entry_id):
    """entry_id is YYYY-MM-DD-NNNN — the universal date."""
    try:
        return date.fromisoformat(str(entry_id)[:10])
    except ValueError:
        return None


def _ordinal(n: int) -> str:
    suffix = "th" if 11 <= n % 100 <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _quarter(d) -> str:
    return f"{d.year} Q{(d.month - 1) // 3 + 1}" if d else ""


def _sectors_str(r: dict) -> str:
    s = r.get("sectors")
    return "; ".join(s) if isinstance(s, list) else str(s or "")


def _market(state_str) -> str:
    """'In Market' if any WA/OR/CA state is present in the piece's targeting,
    else 'National'. (State targeting is often blank for recent products, in
    which case this resolves to 'National'.)"""
    parts = [p.strip().lower() for p in (state_str or "").split(",")]
    return "In Market" if any(p in IN_MARKET_STATES for p in parts) else "National"


def _market_rank(record: dict) -> int:
    """0 = In Market (WA/OR/CA), 1 = National. Reuses record["market"] (set from
    _market() in Step 2). Pre-sort hint only — not a hard filter."""
    return 0 if record.get("market") == "In Market" else 1


def _channel_rank(record: dict, category_key: str) -> int:
    """0 = top (Email/Direct Mail), 1 = mid, 2 = lowest (Social/SEM). Membership
    promotes Social Media to tier 1. Unrecognized/missing channels default to
    tier 1 (Mid) rather than being penalized or favored blindly."""
    channel = record.get("media_channel")
    override = CHANNEL_TIER_OVERRIDES.get(category_key, {})
    if channel in override:
        return override[channel]
    return CHANNEL_TIER.get(channel, UNKNOWN_CHANNEL_TIER)


def _priority_sort_key(record: dict, category_key: str) -> tuple:
    """(market_tier, channel_tier), ascending — 0 is best. Used only to PRE-SORT
    the shortlist shown to the Selection LLM call; it may still deviate from
    this order when content-type fit calls for it."""
    return (_market_rank(record), _channel_rank(record, category_key))


def _dedupe_companies(ids: list[str], records: list[dict], want: int,
                       category_key: str, exclude: set) -> list[str]:
    """Enforce "no company twice on one slide": the Selection prompt asks for
    this, but isn't guaranteed to follow it, so also enforce it deterministically.

    For each duplicate-company id (after the first occurrence of that company),
    try to swap in the next-best (tier-sorted), unused, different-company
    candidate. If no such replacement exists, DROP the slot rather than force a
    duplicate company onto the slide — the slide may end up with fewer than
    `want` entries, which is the intended fallback (a repeated company is worse
    than a shorter slide).
    """
    id_to_record = {r["entry_id"]: r for r in records if r.get("entry_id")}

    def _company(eid: str) -> str:
        rec = id_to_record.get(eid)
        return (rec.get("company_name") or "").strip().lower() if rec else ""

    # Alternates, tier-sorted, excluding anything already used by an earlier
    # slide or already present in `ids` (so a replacement can't just be a
    # reshuffle of the same set).
    already = set(ids)
    alternates = sorted(
        (r for r in records
         if r.get("entry_id") and r["entry_id"] not in exclude and r["entry_id"] not in already),
        key=lambda r: _priority_sort_key(r, category_key),
    )

    seen_companies: set = set()
    final: list[str] = []
    for eid in ids:
        company = _company(eid)
        if company and company in seen_companies:
            replacement = next(
                (r for r in alternates
                 if (r.get("company_name") or "").strip().lower() not in seen_companies
                 and r.get("company_name")),
                None,
            )
            if replacement is None:
                continue  # no distinct-company alternative — drop this slot
            final.append(replacement["entry_id"])
            seen_companies.add((replacement.get("company_name") or "").strip().lower())
            alternates.remove(replacement)
            continue
        final.append(eid)
        if company:
            seen_companies.add(company)

    return final[:want]


def _run_search(cat: dict, channel: str):
    """One search_archive call for a single (category, channel). Fanning out per
    channel multiplies the 50-result cap and reaches further back in time."""
    res = search_archive(sectors=cat["sectors"], media_channels=[channel],
                         keyword=cat["keyword"], limit=SEARCH_LIMIT, country="United States")
    if res and isinstance(res[0], dict) and "error" in res[0]:
        return {"error": res[0]["error"]}
    return [r for r in res if r.get("entry_id")]


def _filter_week(records: list[dict], start: date, end: date, cu_only: bool) -> list[dict]:
    out = []
    for r in records:
        d = _entry_date(r.get("entry_id"))
        if not d or not (start <= d <= end):
            continue
        if cu_only and not _CU_RE.search(r.get("company_name") or ""):
            continue
        out.append(r)
    return out


def _excel_rows_via_sql(entry_ids: list[str]) -> list[dict]:
    """Enrich the Step-1 entry_ids into full Excel rows via report_lib_excel_helper
    (build_query → SSH/MySQL → complete_rows). Only entry_ids that have a document
    and a primary-company mapping come back (the query inner-joins those)."""
    if not entry_ids:
        return []
    df = _run_sql(XH.build_query(entry_ids))
    if df is None or df.empty:
        return []
    # pandas types the GROUP_CONCAT columns oddly: all-NULL → float NaN, and
    # all-numeric (e.g. ages "45") → float. complete_row calls .replace/.split on
    # them, so coerce those text columns to str (NaN → None). Flag columns
    # (is_prescreen, refinance, …) MUST stay ints — "0" is truthy and would flip
    # Pre-Screen / Application Type — so leave everything else untouched.
    text_cols = {"additional_companies", "sectors", "categories", "sub_categories",
                 "states", "ages", "incomes", "primary_company", "product_name",
                 "product_headline", "media_channel", "mailing_type", "entry_id"}

    def _clean(k, v):
        if v is None or (isinstance(v, float) and v != v):  # None or NaN
            return None
        return str(v) if k in text_cols else v

    raw = [{k: _clean(k, v) for k, v in rec.items()} for rec in df.to_dict("records")]
    rows = XH.complete_rows(raw)
    for row, r in zip(rows, raw):        # tokens the EntryID / PDF hyperlinks need
        row["pid"] = r.get("product_id", "")
        row["entry_id"] = r.get("entry_id", "")
        row["Market"] = _market(row.get("State/Province"))
    return rows


def _shortlist(records: list[dict], category_key: str) -> str:
    """Format a PRE-SORTED (by _priority_sort_key) list of records for the
    Selection prompt. The numbering IS the tier order, so tier labels are
    shown explicitly rather than making the LLM infer them."""
    lines = []
    for i, r in enumerate(records[:LLM_SHORTLIST_CAP], 1):
        ocr = (r.get("ocr_text") or "").strip().replace("\n", " ")[:400]
        ch_label = f'{CHANNEL_TIER_LABEL[_channel_rank(r, category_key)]}({r.get("media_channel")})'
        lines.append(
            f'{i}. entry_id={r.get("entry_id")} | market={r.get("market", "National")} '
            f'| channel_tier={ch_label} | company={r.get("company_name")} '
            f'| headline={r.get("product_headline")}\n'
            f'   OCR: {ocr}'
        )
    return "\n".join(lines) if lines else "(none)"


def _describe_chosen(records: list[dict]) -> str:
    """Format the FINAL, already-chosen entries for the Callout prompt.
    Deliberately omits entry_id entirely — a dedicated formatter (rather than
    a flag on _shortlist) makes it structurally impossible for the callout
    call to see, and therefore mention, an entry_id."""
    lines = []
    for r in records:
        ocr = (r.get("ocr_text") or "").strip().replace("\n", " ")[:400]
        lines.append(
            f'- {r.get("company_name")} ({r.get("media_channel")}): '
            f'"{r.get("product_headline")}"\n  OCR: {ocr}'
        )
    return "\n".join(lines) if lines else "(none)"


def _as_text(value) -> str:
    """Coerce an LLM JSON field to a printable string, defensively — the prompt
    asks for a single string, but a model can still return a list/dict instead
    (observed live: Auto's "reasoning" came back as an object). This is
    console-debug output only, so never let its shape crash the run."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return "\n".join(_as_text(v) for v in value)
    if isinstance(value, dict):
        return "\n".join(f"{k}: {_as_text(v)}" for k, v in value.items())
    return str(value).strip()


SELECT_SYSTEM_TMPL = (
    "You are a market research analyst at Competiscan selecting which direct-marketing "
    "campaigns to feature on one slide of a weekly competitive update for {client}, a "
    "credit union in the Pacific Northwest (Washington/Oregon). You are given real "
    "campaigns from Competiscan's archive for the {category} category, each tagged with "
    "entry_id, market (In Market = targets Washington/Oregon/California, or National), "
    "channel_tier (Top = Email/Direct Mail, Mid = Online Video/Online Display/Print/"
    "Website, Low = Social Media/Search Engine Marketing), company, headline, and OCR "
    "text. The list is PRE-SORTED best-tier-first (In Market before National; within the "
    "same market, Top channel before Mid before Low). Treat that order as a strong "
    "default, but you may pick a lower-ranked entry over a higher-ranked one when its "
    "CONTENT is a clearly better fit for this slide (see rule 3) — content fit can "
    "outweigh market/channel tier.\n\n"
    "Selection rules, in priority order:\n"
    "  1. Prefer In-Market pieces (WA/OR/CA) over National; within the same market tier, "
    "prefer Top channel over Mid over Low.\n"
    "  2. Prioritize content that encourages online account creation / sign-up.\n"
    "  3. {guidance}\n"
    "  4. AVOID FEATURING THE SAME COMPANY MORE THAN ONCE on this slide. If the top "
    "candidates by rules 1-3 include near-duplicate pieces from one company (e.g. two "
    "emails from the same campaign, or three variants of the same offer), pick at most "
    "ONE of them and fill the remaining slot(s) with the next-best entry from a "
    "DIFFERENT company. Only repeat a company if there are genuinely not enough "
    "distinct companies among the candidates to do otherwise.\n\n"
    "Work ONLY from the provided material — never invent campaigns, companies, or facts.\n\n"
    "Return ONE valid JSON object and nothing else — no markdown, no code fences:\n"
    '  "entry_ids" : list of UP TO {featured} entry_ids (chosen ONLY from the list '
    "provided, in priority order, most important first). Prefer exactly {featured} "
    "distinct-company entries, but returning fewer is correct and expected when that "
    "many distinct companies are not available — never duplicate a company just to "
    "reach {featured}.\n"
    '  "reasoning" : a SINGLE STRING (not a list or object) containing one short '
    "sentence per pick explaining why it was chosen, separated by newlines (internal "
    "review only, never shown to the client).\n"
)

CALLOUT_SYSTEM_TMPL = (
    "You are a market research analyst at Competiscan writing the summary callout for one "
    "slide of a weekly competitive update for {client}. You will be given a list of "
    "campaigns that have ALREADY been selected to feature on this slide — do not second-"
    "guess or add to the selection, and do not mention that they were 'selected' or how "
    "many there are. For each: company, channel, headline, and OCR text.\n\n"
    "Write ONE complete paragraph summarizing what these pieces did, ENTIRELY IN PAST "
    "TENSE (e.g. 'promoted', 'highlighted', 'encouraged' — not 'promotes' / 'is "
    "highlighting'), STRICTLY UNDER {limit} characters (aim for ~300). Write whole "
    "sentences that fit — never trail off or get cut mid-sentence; write fewer sentences "
    "rather than exceed the limit. Never mention entry IDs or any ID-like identifiers — "
    "refer to companies and offers by name only. Work ONLY from the material provided — "
    "never invent facts.\n\n"
    "Return ONE valid JSON object and nothing else — no markdown, no code fences:\n"
    '  "callout" : the paragraph described above.\n'
)


def _select(cat: dict, records: list[dict]) -> dict:
    """Stage 1: pick FEATURED_PER_SLIDE entry_ids from a pre-sorted shortlist."""
    if not records:
        return {"entry_ids": [], "reasoning": ""}
    records = sorted(records, key=lambda r: _priority_sort_key(r, cat["key"]))
    system = SELECT_SYSTEM_TMPL.format(
        client=CLIENT, category=cat["key"], guidance=cat["guidance"], featured=FEATURED_PER_SLIDE,
    )
    prompt = (f"Category: {cat['key']}\nCampaigns ({len(records[:LLM_SHORTLIST_CAP])} shown, "
              f"pre-sorted best-first):\n{_shortlist(records, cat['key'])}\n\nReturn the JSON now.")
    raw = L.call_claude(system, prompt, max_tokens=800)
    try:
        return L.extract_json(raw)
    except Exception:  # noqa: BLE001 — bad JSON shouldn't kill the run
        return {"entry_ids": [], "reasoning": ""}


def _write_callout(cat: dict, chosen_records: list[dict]) -> dict:
    """Stage 2: write the slide's summary paragraph for the FINAL, already-chosen
    entries. entry_id is never shown to this call — see _describe_chosen()."""
    if not chosen_records:
        return {"callout": ""}
    system = CALLOUT_SYSTEM_TMPL.format(client=CLIENT, limit=CALLOUT_LIMIT)
    prompt = (f"Category: {cat['key']}\nFeatured pieces:\n{_describe_chosen(chosen_records)}"
              f"\n\nReturn the JSON now.")
    raw = L.call_claude(system, prompt, max_tokens=600)
    try:
        return L.extract_json(raw)
    except Exception:  # noqa: BLE001
        return {"callout": ""}


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    start, end = _week_window()
    week_label = f"{end:%B} {_ordinal(end.day)}, {end.year}"      # "July 14th, 2026"
    mmddyy = end.strftime("%m%d%y")
    stamp = end.strftime("%Y%m%d")
    print(f"{CLIENT} Weekly Update — {week_label}  (entry-date window {start} … {end})")

    # Step 1 — searches fanned out per (category, channel), run SEQUENTIALLY.
    # NOTE: search_archive's REST backend cross-contaminates results when called
    # concurrently (a request for one channel can come back with another
    # channel's data — confirmed via isolated testing), which made `found`
    # counts vary run-to-run even for an identical, unchanged week window.
    # Sequential calls are deterministic, so we eat the extra latency here.
    jobs = [(cat, ch) for cat in CATEGORIES for ch in cat["channels"]]
    print(f"Step 1/7  Searching the archive ({len(jobs)} category×channel calls, sequential)…")
    found_by_cat: dict[str, dict] = {c["key"]: {} for c in CATEGORIES}  # entry_id -> record
    for cat, ch in jobs:
        res = _run_search(cat, ch)
        if isinstance(res, dict) and "error" in res:
            print(f"   ! {cat['key']} / {ch}: search error — {res['error']}")
            continue
        for r in (res or []):
            found_by_cat[cat["key"]].setdefault(r["entry_id"], r)  # dedup across channels

    by_cat: dict[str, list[dict]] = {}
    for cat in CATEGORIES:
        found = list(found_by_cat[cat["key"]].values())
        kept = _filter_week(found, start, end, cat["cu_only"])
        by_cat[cat["key"]] = kept
        print(f"   {cat['key']:11} {len(found):>3} found → {len(kept):>3} in window"
              f"{' (CU-only)' if cat['cu_only'] else ''}")

    if not any(by_cat.values()):
        print("ERROR: no results in the window for any category. Is the VPN/REST reachable, "
              "or is the window empty? Aborting.")
        return 1

    # Step 2 — Excel (broader offers, enriched via SQL) ----------------------
    print("Step 2/7  Enriching Step-1 entry_ids via SQL + writing the Excel…")
    sheets = []
    market_by_id: dict[str, str] = {}   # entry_id -> "In Market" / "National"
    for cat in CATEGORIES:
        entry_ids = [r["entry_id"] for r in by_cat[cat["key"]]]
        rows = _excel_rows_via_sql(entry_ids)
        for row in rows:
            market_by_id[row.get("entry_id")] = row.get("Market", "National")
        n_in = sum(1 for row in rows if row.get("Market") == "In Market")
        print(f"   {cat['key']:11} {len(entry_ids):>3} entry_ids → {len(rows):>3} enriched rows"
              f"  ({n_in} In Market)")
        filt = (f'{cat["keyword"]} | Sector: {", ".join(cat["sectors"])} '
                f'| Media Channel: {", ".join(cat["channels"])} '
                f'| Entry Date: {start} … {end}'
                f'{" | Credit Unions only" if cat["cu_only"] else ""}')
        sheets.append({
            "name": cat["sheet"], "filter_row": filt, "headers": cat["headers"],
            "rows": rows, "hyperlinks": HYPERLINKS,
            "header_fills": {"Market": MARKET_HEADER_COLOR},
        })

    # Tag the Step-1 records with their Market so the deck curation (Step 3) can
    # prioritize In-Market pieces for the PPTX.
    for cat in CATEGORIES:
        for r in by_cat[cat["key"]]:
            r["market"] = market_by_id.get(r["entry_id"], "National")

    xlsx_path = OUTPUT_DIR / f"{CLIENT}_Competiscan_MarketingTopics_{mmddyy}.xlsx"
    xlsx_path = L.write_workbook(xlsx_path, sheets)
    print(f"          saved {xlsx_path}")

    # Step 3 — parallel LLM Selection calls (candidates pre-sorted by tier) ---
    print("Step 3/7  Selecting featured pieces (4 calls, concurrent)…")
    selections = L.run_parallel([lambda c=c: _select(c, by_cat[c["key"]]) for c in CATEGORIES])

    # Cross-slide dedup happens HERE, between Selection and Callout-writing —
    # the Callout call needs the FINAL entry list to build its input, not the
    # raw pre-dedup suggestion. Sequential + deterministic (CATEGORIES order):
    # an entry_id may match more than one category's search, but must never
    # appear on two slides. Reuses the same exclude-set pattern as before.
    used_ids: set[str] = set()
    final_ids: dict[str, list[str]] = {}
    for cat, sel in zip(CATEGORIES, selections):
        recs = by_cat[cat["key"]]
        sel = sel if isinstance(sel, dict) else {}
        if "error" in sel:
            print(f"   ! {cat['key']}: selection call failed — {sel['error']}")
        ids = L.pick_ids(sel.get("entry_ids"), recs, FEATURED_PER_SLIDE, max_ids=5,
                          exclude=used_ids)
        deduped = _dedupe_companies(ids, recs, FEATURED_PER_SLIDE, cat["key"], exclude=used_ids)
        if len(deduped) < len(ids):
            print(f"   ! {cat['key']}: dropped {len(ids) - len(deduped)} duplicate-company "
                  f"pick(s) with no distinct-company replacement available")
        ids = deduped
        used_ids.update(ids)
        final_ids[cat["key"]] = ids
        n_in = sum(1 for eid in ids if market_by_id.get(eid) == "In Market")
        reasoning = _as_text(sel.get("reasoning"))
        print(f"   {cat['key']:11} featured: {ids} | {n_in} In-Market"
              + (f"\n      reasoning: {reasoning}" if reasoning else ""))

    # Step 4 — parallel LLM Callout calls, only for categories with picks ----
    print("Step 4/7  Writing callouts (concurrent)…")
    def _callout_job(cat):
        ids = final_ids[cat["key"]]
        if not ids:
            return {"callout": ""}
        id_set = set(ids)
        chosen = [r for r in by_cat[cat["key"]] if r.get("entry_id") in id_set]
        return _write_callout(cat, chosen)
    callouts = L.run_parallel([lambda c=c: _callout_job(c) for c in CATEGORIES])

    # Step 5 — assemble the deck ---------------------------------------------
    print("Step 5/7  Building the deck…")
    slides: list[dict] = [
        {"type": "title", "data": {"title": f"{CLIENT} Weekly Update", "date": week_label}},
    ]
    for cat, cdata in zip(CATEGORIES, callouts):
        ids = final_ids[cat["key"]]
        if not ids:
            print(f"   ! {cat['key']}: no entries — skipping slide")
            continue
        cdata = cdata if isinstance(cdata, dict) else {}
        if "error" in cdata:
            print(f"   ! {cat['key']}: callout call failed — {cdata['error']}")
        callout = L.fit_text(_as_text(cdata.get("callout")), CALLOUT_LIMIT)
        print(f"   {cat['key']:11} callout {len(callout)} chars")
        slides.append({"type": "entry_ids", "data": {
            "slideTitle": cat["slide_title"], "entryIds": ids, "insight": callout,
        }})
    slides.append({"type": "closing", "data": {}})

    result = build_deck_default(deck_title=f"{CLIENT} Weekly Update — {week_label}", slides=slides)

    # Step 6 — save the deck --------------------------------------------------
    print("Step 6/7  Saving the deck…")
    pptx_path = OUTPUT_DIR / f"{CLIENT}_Weekly_Report_{stamp}.pptx"
    try:
        saved = L.save_pptx(result, pptx_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        print("       Check PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env and that "
              "csresearchhub.com is reachable.")
        print(f"       (The Excel was still written: {xlsx_path})")
        return 1
    print(f"\n  Excel: {xlsx_path}\n  Deck:  {saved}")

    # Step 7 — email the deliverables to a reviewer, only if a recipient was
    # explicitly configured (HARBOR_EMAIL_TO env var) — an actual send has
    # real inbox-facing consequences, so this is opt-in, never automatic.
    if EMAIL_TO:
        print(f"Step 7/7  Emailing deliverables to {EMAIL_TO}…")
        email_result = L.notify_report_ready(
            report_name=f"{CLIENT} Weekly Update", period_label=week_label,
            attachment_paths=[saved, xlsx_path], to_addr=EMAIL_TO,
        )
        if email_result.get("status") == "sent":
            print(f"          sent (message_id={email_result.get('message_id')})")
        else:
            print(f"          !! email FAILED: {email_result.get('error')} — "
                  f"files are still saved locally, nothing lost")
    else:
        print("Step 7/7  Skipped emailing — no HARBOR_EMAIL_TO set. Files are saved locally only.")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
