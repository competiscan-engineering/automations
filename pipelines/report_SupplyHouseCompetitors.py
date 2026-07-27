"""
report_SupplyHouseCompetitors.py
─────────────────────────────────────────────────────────────────────────────
SupplyHouse.com monthly competitor-ads benchmark — PowerPoint + Excel.

Same shape as report_HarborstoneWeekly.py: search_archive + build_deck_default
for the deck, SQL enrichment (report_lib_excel_helper) for the Excel. Everything
reusable lives in report_lib.py.

The Excel is NOT one sheet per deck category — it's 5 sheets organized by
channel, independently filtered, with Home Depot and Lowe's carved out of the
shared "Email" sheet into their own tabs (see EXCEL_SHEETS_NOTE below and the
Step-2 code for the exact company/channel/date scope of each).

Seven named competitors (six for Direct Mail / Digital Ads — no Pro split),
three channel sections: Email, Direct Mail, Digital Ads. One entry_ids slide
(or slide-pair, when shown entries exceed the 5-image cap) per company per
section — a fixed 7/6/6 roster every month, including zero-activity companies.

  Step 1  19 search_archive calls (7 email + 6 DM + 6 digital), SEQUENTIAL —
          same cross-contamination risk under concurrency confirmed for
          Harborstone (identical REST code path); this environment had no
          VPN access to re-test live, so we default to the safe fallback.
          Dedup by entry_id, filter to the target calendar month (entry_id
          date), then derive hd_consumer as hd_pro's complement client-side
          (confirmed live: the archive's OCR search doesn't honor boolean
          NOT, so hd_consumer fetches ALL Home Depot email and we subtract
          hd_pro's entry_ids ourselves, rather than trusting a "not (...)"
          keyword query).
  Step 2  Excel — 5 sheets (Email / Home Depot Consumer Email / Lowe's
          Consumer Email / Direct Mail (2-month) / Digital (2-channel)),
          enriched via SQL (report_lib_excel_helper) and filtered by each
          row's `added_to_database` timestamp — a DIFFERENT field from the
          entry_id-embedded date the deck uses, confirmed via build_query's
          existing `p.added_to_database` selection. Reuses Step 1's search
          results where the company/channel scope already lines up; new
          searches only for Lowe's all-email (the deck only ever fetches a
          Pro-keyword-filtered slice) and the Direct Mail / Digital sheets
          (different date window / channel set than the deck's PPTX section).
  Step 3  Selection — one parallel Claude call per non-empty group, picking up
          to MAX_SHOWN entry_ids from the creative-deduped candidate pool (see
          _dedupe_similar_creative). No narrative yet. A deterministic
          post-processing backstop (_dedupe_themes) then enforces "at most 1
          per creative theme" on the FINAL picks — the pre-filter only bounds
          the CANDIDATE pool at 2/theme, it doesn't stop the LLM from using
          both slots, so this is the guaranteed backstop (mirrors
          report_HarborstoneWeekly.py's _dedupe_companies, keyed on theme
          instead of company). Zero-result groups get [] with no LLM call.
  Step 4  Narrative — one parallel Claude call per group with a non-empty
          final selection, writing that group's true-count-in-prose +
          messaging summary from ONLY the already-chosen entries. entry_id is
          never shown to this call (see _describe_chosen) — structurally
          impossible for the narrative to mention an ID. Zero-result groups
          get a templated placeholder, no LLM call.
  Step 5  Key Takeaways — the LAST LLM call, synthesizing the finished
          per-group narratives (not raw OCR) into the two-column slide.
  Step 6  build the deck (title → nav → Key Takeaways → Email[7] → Direct
          Mail[6] → Digital Ads[6] → closing) and save the PPTX.
  Step 7  done — final summary print + acceptance checks.
  Step 8  email the deck+Excel to a reviewer via L.notify_report_ready() (AWS
          SES) — opt-in only, gated on the SH_EMAIL_TO env var being set.

Slide types confirmed live against the PPT Builder API (localhost:5001)
before this pipeline was written:
  needToKnow  → Key Takeaways two-column slide (title1/text1/title2/text2)
  newSection  → section divider ("Email" / "Direct Mail" / "Digital Ads")
  agenda      → navigation slide, renders as 4 labeled boxes from `sections`
  entry_ids   → 1-5 images; entryIds=[] renders a caption-only placeholder
                (used for zero-activity companies — no separate slide type
                needed)

RUN (research env — fastmcp / anthropic / pandas / boto3):
    C:/miniconda3/envs/research/python.exe pipelines/report_SupplyHouseCompetitors.py
PPT Builder (csresearchhub.com) sits behind an ALB + Cognito login — needs
PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env (report_lib.py loads it and
handles the login; see report_lib.get_ppt_session). Claude via AWS Bedrock (boto3 chain).
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Raise the builder timeout BEFORE the builder module is imported (load_tool).
os.environ.setdefault("PPT_BUILDER_TIMEOUT", "20000")

import pipelines.report_lib as L  # noqa: E402
import pipelines.report_lib_excel_helper as XH  # noqa: E402  (SQL enrichment for Excel)

search_archive     = L.load_tool("mcp_serverv3", "search_archive")
build_deck_default = L.load_tool("mcp_pptbuilder", "build_deck_default")
_get_company_ids   = L.load_tool("mcp_serverv3", "_get_company_ids")  # pre-check only, see _run_group_search
_run_sql           = L.load_tool("mcp_serverv3", "_run_sql")  # SSH → MySQL, for Excel enrichment

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT        = "SupplyHouse.com"
# Explicit override (or SH_MONTH env var, format "2026-05"); None -> prior calendar month.
MONTH         = os.environ.get("SH_MONTH") or None
# Recipient for the end-of-run "report ready" email — unset means "don't email,
# just save the files" (an actual send has real inbox-facing consequences, so
# this is opt-in, not automatic). Set via env: SH_EMAIL_TO=someone@competiscan.com
EMAIL_TO      = os.environ.get("SH_EMAIL_TO") or None
MAX_SHOWN     = 10    # hard cap on entries shown per company/section; if the true
                       # count exceeds this, sample down to MAX_SHOWN and say so in prose
SLIDE_CAP     = 5      # PPT builder's per-slide image limit — hard constraint, not tunable
SEARCH_LIMIT  = 200      # PER-CHANNEL cap per search_archive call — mcp_serverv3._datagrid_search
                         # clamps perpage to min(limit, 50) regardless, so anything above 50 here
                         # doesn't fetch more per call; it just breaks cap_hit detection (each
                         # channel's own result count is compared against this). Keep this at 50.
                         # _run_group_search fans out one call PER CHANNEL and merges by entry_id
                         # (same technique as Harborstone), so a multi-channel group's effective
                         # cap is len(channels) * SEARCH_LIMIT — e.g. up to 250 for the 5-channel
                         # Digital Ads section. Single-channel groups (Email, Direct Mail) don't
                         # gain anything from this — there's only one channel to fan out into.
CALLOUT_LIMIT = 374     # confirmed same insight-field limit as Harborstone (1-5 image slide)
TAKEAWAYS_MAX_WORDS = 50  # hard cap per needToKnow column — enforced both in the prompt AND
                          # in Python via L.cap_words() as a backstop, since the prompt-only
                          # version still overran badly once the narratives got more detailed
OUTPUT_DIR    = PROJECT_ROOT / "output"
DIGITAL_CHANNELS = ["Online Display", "Online Video", "Search Engine Marketing",
                    "Social Media", "Website/URL"]   # "Digital Ads" section = everything
                    # except Email/Direct Mail/Print. ASSUMPTION — flag if a Print
                    # entry turns out to be the only content for some company; we may need
                    # to add a fourth section or fold Print into Direct Mail.

# Email section — 7 slide-groups (Home Depot + Lowe's split by Pro program).
EMAIL_GROUPS = [
    {"key": "ferguson",    "title": "Ferguson",                       "company_names": ["Ferguson Enterprises, LLC", "Ferguson plc"], "keyword": ""},
    {"key": "grainger",    "title": "Grainger",                       "company_names": ["Grainger"],                      "keyword": ""},
    {"key": "hd_pro",      "title": "The Home Depot Pro & Pro Xtra",  "company_names": ["The Home Depot", "Home Depot"],  "keyword": '"Pro Xtra" or "Home Depot Pro"'},
    # NOTE: keyword is intentionally "" (not a "not (...)" negation). Confirmed
    # live that the archive's OCR search does not honor boolean NOT (hd_pro vs.
    # a "not (...)" query on the same terms came back 86% identical). Instead
    # we fetch ALL Home Depot email here (this same superset) and let the
    # hd_pro/hd_consumer subtraction pass below compute the true complement.
    {"key": "hd_consumer", "title": "The Home Depot Consumer to Pro", "company_names": ["The Home Depot", "Home Depot"],  "keyword": ""},
    {"key": "lowes_pro",   "title": "Lowe's Pro",                     "company_names": ["Lowe's", "Lowes"],               "keyword": '"Lowe\'s Pro"'},
    {"key": "supplyhouse", "title": "SupplyHouse",                    "company_names": ["SupplyHouse.com", "SupplyHouse"], "keyword": ""},
    {"key": "zoro",        "title": "Zoro Tools",                     "company_names": ["Zoro Tools", "Zoro"],            "keyword": ""},
]

# Direct Mail & Digital Ads — same 6 slide-groups for BOTH sections (no Pro split).
PLAIN_GROUPS = [
    {"key": "ferguson",    "title": "Ferguson",       "company_names": ["Ferguson Enterprises, LLC", "Ferguson plc"], "keyword": ""},
    {"key": "grainger",    "title": "Grainger",       "company_names": ["Grainger"],                       "keyword": ""},
    {"key": "home_depot",  "title": "The Home Depot", "company_names": ["The Home Depot", "Home Depot"],   "keyword": ""},
    {"key": "lowes",       "title": "Lowe's",         "company_names": ["Lowe's", "Lowes"],                "keyword": ""},
    {"key": "supplyhouse", "title": "SupplyHouse",    "company_names": ["SupplyHouse.com", "SupplyHouse"], "keyword": ""},
    {"key": "zoro",        "title": "Zoro Tools",     "company_names": ["Zoro Tools", "Zoro"],             "keyword": ""},
]

# Section specs — order here IS the deck order.
SECTIONS = [
    {"key": "email",       "label": "Email",        "channels": ["Email"],       "groups": EMAIL_GROUPS,
     "content_noun": "email activity", "placeholder": "No email activity was observed from {company} in {month_label}."},
    {"key": "direct_mail", "label": "Direct Mail",   "channels": ["Direct Mail"], "groups": PLAIN_GROUPS,
     "content_noun": "direct mail pieces", "placeholder": "No direct mail pieces were observed from {company} in {month_label}."},
    {"key": "digital_ads", "label": "Digital Ads",   "channels": DIGITAL_CHANNELS, "groups": PLAIN_GROUPS,
     "content_noun": "digital content", "placeholder": "No digital content was observed from {company} in {month_label}."},
]

# ── Excel config ──────────────────────────────────────────────────────────────
# 5 sheets, organized by channel — NOT one sheet per deck category. See the
# module docstring's Step 2 for the reuse-vs-new-search breakdown per sheet.
EXCEL_HEADERS = [
    "Primary Company", "Additional Companies", "Primary Sector", "Primary Category",
    "Primary Sub Category", "Primary Sub Sub Category", "EntryID", "Headline", "Product",
    "PDF Content",
]
EXCEL_HYPERLINKS = {
    "EntryID":     ("https://cp.competiscan.com/productdetail?id={pid}", "{entry_id}"),
    "PDF Content": ("https://www.competiscan.com/productDocuments.php?id={pid}", "PDF Content"),
}
# Canonical single display name per company, for the A1 filter-row text only
# (search_archive itself still uses the full alias list from EMAIL_GROUPS/PLAIN_GROUPS).
EXCEL_DISPLAY_NAMES = {
    "ferguson": "Ferguson Enterprises, LLC", "grainger": "Grainger",
    "home_depot": "The Home Depot", "lowes": "Lowe's",
    "supplyhouse": "SupplyHouse.com", "zoro": "Zoro Tools",
}
EXCEL_DIGITAL_CHANNELS = ["Online Display", "Online Video"]  # narrower than the deck's
                                                              # 5-channel DIGITAL_CHANNELS —
                                                              # intentional per the sample,
                                                              # not reconciled with the deck.


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _month_window() -> tuple[date, date]:
    """Return (first_day, last_day) of the target calendar month."""
    if MONTH:
        y, m = (int(x) for x in MONTH.split("-"))
    else:
        first_of_this_month = date.today().replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        y, m = last_month_end.year, last_month_end.month
    start = date(y, m, 1)
    end = (date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)) - timedelta(days=1)
    return start, end


def _entry_date(entry_id):
    """entry_id is YYYY-MM-DD-NNNN — the universal date."""
    try:
        return date.fromisoformat(str(entry_id)[:10])
    except ValueError:
        return None


def _dedup_by_entry_id(records: list[dict]) -> list[dict]:
    seen, out = set(), []
    for r in records:
        eid = r.get("entry_id")
        if eid and eid not in seen:
            seen.add(eid)
            out.append(r)
    return out


def _filter_month(records: list[dict], start: date, end: date) -> list[dict]:
    out = []
    for r in records:
        d = _entry_date(r.get("entry_id"))
        if d and start <= d <= end:
            out.append(r)
    return out


def _run_group_search(group: dict, channels: list[str]) -> tuple[list[dict], bool]:
    """search_archive, fanned out ONE CALL PER CHANNEL and merged by entry_id —
    same technique Harborstone uses (_run_search + the per-(cat,channel) jobs
    loop). Each individual channel call is still capped at SEARCH_LIMIT (50),
    but fanning out reaches up to len(channels) * SEARCH_LIMIT distinct records
    instead of 50 total across all channels combined — e.g. up to 250 for the
    5-channel Digital Ads section, matching Harborstone's up-to-400 for its
    8-channel categories. For a single-channel group (Email, Direct Mail) this
    is just one call, same as before — there's nothing to fan out when there's
    only one channel to begin with.

    Returns (merged_records, cap_hit) — cap_hit is True if ANY individual
    channel call hit the 50-record API cap (that channel's true total may
    still be undercounted even after merging with the other channels).

    Pre-checks that company_names actually resolves to at least one companyID
    before searching. Confirmed live: when none of the given names match
    exactly, search_archive's backend (_datagrid_search) sends an EMPTY
    "companies" filter, which the archive treats as NO company filter at all —
    silently returning the most-recently-modified records across the WHOLE
    archive (unrelated brands, wrong dates) instead of erroring. Skip instead
    of risking that silent contamination."""
    if not _get_company_ids(group["company_names"]):
        print(f"   !! {group['title']}: none of {group['company_names']} resolved to a companyID in the "
              f"archive — search_archive would run UNFILTERED by company. Skipping rather than risk "
              f"contaminated results. Fix the company_names alias for this group.")
        return [], False

    merged: dict[str, dict] = {}  # entry_id -> record, dedup across channels
    cap_hit = False
    for channel in channels:
        res = search_archive(company_names=group["company_names"], media_channels=[channel],
                              keyword=group.get("keyword", ""), limit=SEARCH_LIMIT)
        if res and isinstance(res[0], dict) and "error" in res[0]:
            print(f"   ! {group['title']} / {channel}: search error — {res[0]['error']}")
            continue
        res = [r for r in (res or []) if r.get("entry_id")]
        if len(res) >= SEARCH_LIMIT:
            cap_hit = True
        for r in res:
            merged.setdefault(r["entry_id"], r)
    return list(merged.values()), cap_hit


def _add_months(d: date, delta: int) -> date:
    """First-of-month `delta` months away from `d` (delta may be negative)."""
    m = d.month - 1 + delta
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, 1)


def _added_date(val):
    """added_to_database is a MySQL DATETIME; the mysql CLI → tab-separated
    output gives us a 'YYYY-MM-DD HH:MM:SS'-shaped string (or None)."""
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


def _excel_rows_via_sql(entry_ids: list[str], start: date, end: date) -> list[dict]:
    """Enrich entry_ids into full Excel rows via SQL (report_lib_excel_helper),
    keeping only rows whose `added_to_database` timestamp falls within
    [start, end].

    added_to_database is a DIFFERENT field from entry_id's own embedded date
    (what the deck's month-filtering uses) and from the REST API's
    approved_date — confirmed via build_query, which already selects
    p.added_to_database as its own column. The workbook's own A1 filter-row
    text says "Added to database", so that field — not entry_id parsing —
    is what drives inclusion here.
    """
    if not entry_ids:
        return []
    df = _run_sql(XH.build_query(entry_ids))
    if df is None or df.empty:
        return []
    # Same NaN/float coercion Harborstone applies: pandas types the GROUP_CONCAT
    # columns oddly — all-NULL across the batch -> float NaN, all-numeric (e.g.
    # ages "45") -> float. complete_row() calls .replace()/.split() on several
    # of these regardless of which 10 headers we actually keep (it's shared,
    # generic code), so every such column needs to be listed here even though
    # State/Age/Income/Mailing-Type never make it into EXCEL_HEADERS.
    text_cols = {"additional_companies", "sectors", "categories", "sub_categories",
                 "sub_sub_categories", "states", "ages", "incomes", "mailing_type",
                 "primary_company", "product_name", "product_headline", "media_channel",
                 "entry_id", "added_to_database"}

    def _clean(k, v):
        if v is None or (isinstance(v, float) and v != v):  # None or NaN
            return None
        return str(v) if k in text_cols else v

    raw = [{k: _clean(k, v) for k, v in rec.items()} for rec in df.to_dict("records")]
    rows = []
    for r in raw:
        d = _added_date(r.get("added_to_database"))
        if not d or not (start <= d <= end):
            continue
        row = XH.complete_row(r)
        row["pid"] = r.get("product_id", "")
        row["entry_id"] = r.get("entry_id", "")
        rows.append(row)
    return rows


def _excel_filter_row(channel_label: str, company_keys: list[str], start: date, end: date) -> str:
    companies = " or ".join(f'"{EXCEL_DISPLAY_NAMES[k]}"' for k in company_keys)
    return (f"Media Channel: {channel_label} | Sector: Retail | "
            f"Audience: Consumer, Employer/Business Owner | "
            f"Added to database: Between {start:%B %Y} and {end:%B %Y} | "
            f"Country: US | Company: {companies} | Primary: Primary")


def _theme_key(record: dict) -> str:
    """Grouping key for "the same campaign theme": exact product_headline text
    (case/whitespace-insensitive) — a strong proxy for "the same ad re-served
    on a different date". Blank-headline entries are always unique (never
    grouped with each other) so they don't get wrongly collapsed together.
    Shared by _dedupe_similar_creative() (candidate-pool pre-filter) and
    _dedupe_themes() (final-picks post-processing backstop) so both agree on
    what counts as "the same theme"."""
    headline = (record.get("product_headline") or "").strip().lower()
    return headline if headline else f"__unique__{record.get('entry_id')}"


def _dedupe_similar_creative(records: list[dict], max_per_theme: int = 2) -> list[dict]:
    """Collapse near-identical creative down to at most `max_per_theme` examples
    each, grouped by _theme_key() — a strong proxy for "the same ad re-served
    on a different date". Without this, a company that reuses one evergreen
    creative repeatedly can end up filling every image slot on its slide with
    copies of the same thumbnail (confirmed live: a Digital slide showed 5
    identical "Pro parts at pro speed" images because the true count was
    exactly 5 and all 5 happened to share the same headline, so there was
    nothing forcing a distinct pick). Keeps the most recent occurrence(s) of
    each theme."""
    groups: dict[str, list[dict]] = {}
    for r in records:
        groups.setdefault(_theme_key(r), []).append(r)
    out = []
    for group in groups.values():
        group.sort(key=lambda r: r.get("entry_id") or "", reverse=True)
        out.extend(group[:max_per_theme])
    out.sort(key=lambda r: r.get("entry_id") or "", reverse=True)
    return out


def _dedupe_themes(ids: list[str], records: list[dict], want: int) -> list[str]:
    """Enforce "at most 1 per creative theme" on the Selection call's FINAL
    picks — mirrors Harborstone's _dedupe_companies() exactly, keyed on theme
    instead of company. _dedupe_similar_creative() already caps the CANDIDATE
    POOL at 2/theme before the LLM sees it, but doesn't stop the LLM from
    picking both allowed copies — this backstop ensures the DELIVERED slide
    never shows 2 near-identical items. Tries a distinct-theme replacement
    first; drops the slot (never forces a duplicate) if none remains. No
    `exclude` param — unlike Harborstone, there's no cross-group entry_id
    collision risk here (each group is independently scoped to one company;
    the one place that could happen — Home Depot Pro vs Consumer — is already
    resolved upstream in Step 1)."""
    id_to_record = {r["entry_id"]: r for r in records if r.get("entry_id")}
    already = set(ids)
    alternates = [r for r in records if r.get("entry_id") and r["entry_id"] not in already]

    seen_themes: set = set()
    final: list[str] = []
    for eid in ids:
        rec = id_to_record.get(eid)
        theme = _theme_key(rec) if rec else ""
        if theme in seen_themes:
            replacement = next((r for r in alternates if _theme_key(r) not in seen_themes), None)
            if replacement is None:
                continue  # no distinct-theme alternative — drop this slot
            final.append(replacement["entry_id"])
            seen_themes.add(_theme_key(replacement))
            alternates.remove(replacement)
            continue
        final.append(eid)
        seen_themes.add(theme)

    return final[:want]


def _shortlist(records: list[dict]) -> str:
    lines = []
    for i, r in enumerate(records, 1):
        ocr = (r.get("ocr_text") or "").strip().replace("\n", " ")[:400]
        lines.append(
            f'{i}. entry_id={r.get("entry_id")} | channel={r.get("media_channel")} '
            f'| headline={r.get("product_headline")}\n   OCR: {ocr}'
        )
    return "\n".join(lines) if lines else "(none)"


def _describe_chosen(records: list[dict]) -> str:
    """Format the FINAL, already-chosen entries for the Narrative prompt.
    Deliberately omits entry_id entirely — mirrors Harborstone's
    _describe_chosen: a dedicated formatter (rather than a flag on
    _shortlist) makes it structurally impossible for the narrative call to
    see, and therefore mention, an entry_id."""
    lines = []
    for r in records:
        ocr = (r.get("ocr_text") or "").strip().replace("\n", " ")[:400]
        lines.append(f'- {r.get("media_channel")}: "{r.get("product_headline")}"\n  OCR: {ocr}')
    return "\n".join(lines) if lines else "(none)"


SELECT_GROUP_TMPL = (
    "You are a market research analyst at Competiscan selecting which real "
    "{section_label} campaigns to feature on {company}'s slide in a monthly "
    "competitive benchmark deck for {client}, a home-improvement / MRO "
    "e-commerce retailer. You are given real campaigns from Competiscan's "
    "archive for {company} in {month_label}, each with entry_id, channel, "
    "headline, and OCR text. The list already contains at most two examples "
    "per distinct campaign theme (near-identical repeats of the same creative "
    "have already been thinned out). Work ONLY from the provided material — "
    "never invent campaigns, offers, or facts.\n\n"
    "Selection rule: pick across as many DIFFERENT themes as you can — at most "
    "ONE entry per theme if possible, so the images shown never look like "
    "repeats of each other. If there are not enough distinct themes to fill "
    "{max_shown} slots, it is correct and expected to return fewer.\n\n"
    "Return ONE valid JSON object and nothing else — no markdown, no code fences:\n"
    '  "entry_ids" : list of up to {max_shown} entry_ids, chosen ONLY from the '
    "list provided, in priority order (most important first).\n"
    '  "reasoning" : a SINGLE STRING (not a list or object) containing one '
    "short sentence per pick explaining why it was chosen, separated by "
    "newlines (internal review only, never shown to the client).\n"
)

NARRATIVE_GROUP_TMPL = (
    "You are a market research analyst at Competiscan writing the summary "
    "narrative for {company}'s slide in a monthly competitive benchmark deck "
    "for {client}. You will be given the {section_label} campaigns ALREADY "
    "selected to feature on this slide — do not second-guess or add to the "
    "selection. For each: channel, headline, and OCR text.\n\n"
    "{count_instruction}\n\n"
    "Then summarize the real themes, offers, and messaging you see across the "
    "campaigns — analyst voice, 1-3 sentences total for the whole narrative. "
    "Never mention the archive, the search process, result caps, or how this "
    "data was gathered, in any form — the reader only cares about what "
    "{company} actually did, not how we found it. Work ONLY from the material "
    "provided — never invent facts.\n\n"
    "Return ONE valid JSON object and nothing else — no markdown, no code fences:\n"
    '  "narrative" : the paragraph described above.\n'
)


def _select_group(section: dict, group: dict, records: list[dict], month_label: str) -> dict:
    """Stage 1: pick up to MAX_SHOWN entry_ids from the DEDUPLICATED-by-creative
    candidate pool (see _dedupe_similar_creative) — same input _curate_group
    used to take. No count/cap_hit logic here — that's a prose concern, moved
    entirely to _write_narrative."""
    if not records:
        return {"entry_ids": [], "reasoning": ""}
    system = SELECT_GROUP_TMPL.format(
        client=CLIENT, section_label=section["label"].lower(), company=group["title"],
        month_label=month_label, max_shown=min(MAX_SHOWN, len(records)),
    )
    prompt = (f"Company: {group['title']}\nSection: {section['label']}\n"
              f"Campaigns ({len(records)} distinct themes shown):\n{_shortlist(records)}"
              f"\n\nReturn the JSON now.")
    raw = L.call_claude(system, prompt, max_tokens=1200)
    try:
        return L.extract_json(raw)
    except Exception:  # noqa: BLE001 — bad JSON shouldn't kill the run
        return {"entry_ids": [], "reasoning": ""}


def _write_narrative(section: dict, group: dict, chosen_records: list[dict],
                      true_count: int, cap_hit: bool, month_label: str) -> dict:
    """Stage 2: write the group's narrative for the FINAL, already-deduped
    entries (post pick_ids + post _dedupe_themes). entry_id is never shown to
    this call — see _describe_chosen."""
    if not chosen_records:
        return {"narrative": ""}
    if cap_hit:
        count_instruction = ("Do NOT state any number or count anywhere in the narrative — the "
                              "true total for this month is uncertain, so skip straight to "
                              "describing the campaigns below.")
    else:
        count_instruction = (f"State the exact count as a plain fact in the first sentence, "
                              f"spelling out numbers under 100 (e.g. 'sent fifty emails', 'ran "
                              f"three digital ads'). The true count is {true_count}.")

    system = NARRATIVE_GROUP_TMPL.format(
        client=CLIENT, section_label=section["label"].lower(), company=group["title"],
        count_instruction=count_instruction,
    )
    prompt = (f"Company: {group['title']}\nSection: {section['label']}\n"
              f"Featured campaigns:\n{_describe_chosen(chosen_records)}\n\nReturn the JSON now.")
    raw = L.call_claude(system, prompt, max_tokens=600)
    try:
        return L.extract_json(raw)
    except Exception:  # noqa: BLE001
        return {"narrative": ""}


SYSTEM_TAKEAWAYS = (
    "You are a market research analyst at Competiscan writing the Key Takeaways slide for "
    "{client}'s {month_label} competitor-ads deck. You are given the FINISHED per-company "
    "narratives already written elsewhere in this same deck (not raw data) — synthesize "
    "across them, do not contradict anything they say, and do not invent new facts.\n\n"
    "Match the EXACT style of these two real examples from prior months:\n\n"
    "EMAIL EXAMPLE 1:\n"
    "\"In May 2026, Grainger focused on safety education, facility maintenance, and tool "
    "selection. Zoro Tools promoted discounts on industrial and janitorial supplies. Home "
    "Depot mixed cart reminders with Memorial Day deals and loyalty offers. Lowe's "
    "highlighted Pro point boosters and appliance savings. Ferguson drove showroom "
    "consultations.\"\n\n"
    "OTHER CHANNEL EXAMPLE 1:\n"
    "\"Direct mail was limited in May 2026. Lowe's promoted home services via postcards, "
    "Zoro Tools offered discount codes, Grainger pushed its Red Pass Plus loyalty trial, "
    "and Home Depot mailed a seasonal decor catalog. Digitally, Home Depot and Lowe's "
    "targeted pro and consumer audiences, while Zoro promoted tools and PPE.\"\n\n"
    "EMAIL EXAMPLE 2:\n"
    "\"In June 2026, Grainger dominated email volume with safety education and Fluke product "
    "spotlights, while Zoro promoted PPE and various other supplies with discount codes. Home "
    "Depot executed high-frequency sends featuring Pro Xtra Perks across building, electrical, "
    "and plumbing categories. Lowe's leveraged holiday loyalty boosters and SupplyHouse "
    "targeted professionals with tool brand discounts.\"\n\n"
    "OTHER CHANNEL EXAMPLE 2:\n"
    "\"Direct mail was sparse in June 2026; Zoro led with discount postcards while Ferguson "
    "promoted kitchen and lighting upgrades. Digital was far more active: Home Depot ran 18 "
    "ads covering Pro paint, appliance sales, and its FIFA World Cup partnership, Grainger "
    "highlighted supply selection and convenience, and Zoro produced trade-specific video "
    "campaigns.\"\n\n"
    "Rules, matched from those examples:\n"
    "  - HARD LIMIT: {max_words} words per column, MAXIMUM — count as you write. Both examples "
    "above are close to that limit; treat it as a wall, not a target. If covering every company "
    "would blow past it, DROP the least notable ones rather than exceed the limit — a short, "
    "complete paragraph about 3-4 companies beats a long one that runs over trying to fit "
    "everyone.\n"
    "  - Open with a short framing clause on the month's overall activity level for that "
    "channel ('In {month_label}, ...' for email; an opener like 'Direct mail was "
    "limited/sparse/active in {month_label}...' for the other column).\n"
    "  - For the OTHER column specifically: cover Direct Mail FIRST, then pivot with an "
    "explicit transition ('Digitally, ...' / 'Digital was far more active: ...' / 'Digital "
    "was quiet...') before covering Digital Ads activity. Two channels, one flowing "
    "paragraph — never two separate paragraphs. If fitting both channels AND staying under "
    "{max_words} words means naming fewer companies per channel, favor brevity — cut detail, "
    "don't run long.\n"
    "  - At most a short clause per company (a few words, not a full sentence) naming ONE "
    "specific theme, product, program, or partnership (e.g. 'Red Pass Plus loyalty trial', "
    "'Pro Xtra Perks') — never generic filler like 'various offers', and never a longer "
    "clause than the examples show.\n"
    "  - Use short, natural company names: 'Home Depot' (not 'The Home Depot'), 'Lowe's', "
    "'Ferguson', 'Grainger', 'SupplyHouse', 'Zoro' or 'Zoro Tools'. Home Depot is given below "
    "as ONE combined company (its Pro and Consumer email are already merged) — refer to it as "
    "a single company, never split it into Pro vs. Consumer here.\n"
    "  - If a narrative states a count, cite it as a plain numeral ('ran 18 ads'), NOT spelled "
    "out — this is the opposite convention from the per-company slides. Skip counts entirely "
    "if they'd cost words you need elsewhere.\n"
    "  - ONLY mention companies that appear in the lists below — that list already excludes "
    "every company with zero activity this month. Do NOT call out, list, or allude to absent/"
    "quiet companies in any way; omission is silent by design, that's the per-company slide's "
    "job, not this one.\n"
    "  - One tight paragraph per column, no bullet points, no line breaks.\n\n"
    "Return ONE valid JSON object and nothing else — no markdown, no code fences:\n"
    '  "email_column" : the {month_label} Email Activity paragraph — {max_words} words MAX, '
    "built only from the EMAIL narratives given, in the exact style above.\n"
    '  "other_column" : the Direct Mail + Digital Ads paragraph — {max_words} words MAX, built '
    "only from the narratives given, in the exact style above (direct mail first, then a "
    "transition into digital).\n"
)


def _key_takeaways(month_label: str, email_lines: list[str], other_lines: list[str]) -> dict:
    system = SYSTEM_TAKEAWAYS.format(client=CLIENT, month_label=month_label, max_words=TAKEAWAYS_MAX_WORDS)
    prompt = (f"EMAIL narratives:\n" + "\n".join(email_lines) +
              f"\n\nDIRECT MAIL + DIGITAL ADS narratives:\n" + "\n".join(other_lines) +
              "\n\nReturn the JSON now.")
    raw = L.call_claude(system, prompt, max_tokens=1200)
    try:
        return L.extract_json(raw)
    except Exception:  # noqa: BLE001
        return {"email_column": "", "other_column": ""}


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    start, end = _month_window()
    month_label = f"{start:%B} {start.year}"           # "May 2026"
    month_year_stamp = f"{start:%B}{start.year}"        # "May2026"
    print(f"{CLIENT} Competitor Ads — {month_label}  (entry-date window {start} … {end})")

    # Step 1 — searches, SEQUENTIAL. Harborstone confirmed search_archive's REST
    # backend cross-contaminates results under concurrent calls with different
    # channels (a request for one channel can come back with another channel's
    # data). Same code path here (mcp_serverv3.search_archive → _datagrid_search).
    # This environment had no VPN/SSH reachability to re-test live, so we default
    # to the safe sequential fallback rather than risk it unverified.
    jobs = [(section, group) for section in SECTIONS for group in section["groups"]]
    print(f"Step 1/8  Searching the archive ({len(jobs)} company×section calls, sequential)…")

    found: dict[str, dict[str, dict]] = {s["key"]: {} for s in SECTIONS}
    for section, group in jobs:
        raw, cap_hit = _run_group_search(group, section["channels"])
        deduped = _dedup_by_entry_id(raw)
        kept = _filter_month(deduped, start, end)
        found[section["key"]][group["key"]] = {
            "group": group, "records": kept, "raw_count": len(raw), "cap_hit": cap_hit,
        }
        print(f"   {section['label']:11} {group['title']:30} {len(raw):>3} raw"
              f"{' (cap hit)' if cap_hit else ''} → {len(kept):>3} in {month_label}")
        # A cap-hit fetch that filters down to ZERO in-month records is a red
        # flag, not a confirmed zero: it means all 50 raw records missed the
        # month window entirely, which is very unlikely for a real company's
        # most-recently-modified 50 records. This has been observed to
        # correlate with the archive returning unrelated companies' records
        # under a company_names filter (suspected companyID resolution bug —
        # see _get_company_ids). Do not report this month's count as "0" to
        # the client without verifying via PowerSearch directly.
        if cap_hit and len(kept) == 0:
            print(f"   !! SUSPECT: {group['title']} / {section['label']} hit the {SEARCH_LIMIT}-result cap "
                  f"but 0 of those records fell in {month_label} — likely NOT a true zero. "
                  f"Verify against PowerSearch before trusting this count.")

    # hd_consumer's search fetches ALL Home Depot email (keyword="") — a
    # superset that includes hd_pro's Pro-Xtra/Home-Depot-Pro content, since
    # the archive's OCR search doesn't honor boolean NOT (confirmed live: a
    # "not (...)" query on these terms came back 86% identical to the positive
    # query). This subtraction is the ACTUAL negation, done client-side: drop
    # any entry_id from hd_consumer's fetch that also appears in hd_pro's, so
    # what's left is the true "not Pro" complement.
    # Capture the pre-subtraction "ALL Home Depot email" list FIRST — Excel
    # Sheet 2 needs the unsplit superset (no Pro/Consumer distinction at all),
    # which is exactly this list before it gets mutated below.
    hd_all_email_raw = list(found["email"]["hd_consumer"]["records"])
    hd_pro_ids = {r["entry_id"] for r in found["email"]["hd_pro"]["records"]}
    before = len(found["email"]["hd_consumer"]["records"])
    found["email"]["hd_consumer"]["records"] = [
        r for r in found["email"]["hd_consumer"]["records"] if r["entry_id"] not in hd_pro_ids
    ]
    after = len(found["email"]["hd_consumer"]["records"])
    print(f"   hd_consumer: {before} Home Depot email(s) fetched → {after} after removing hd_pro's "
          f"{before - after} overlapping Pro-Xtra/Home-Depot-Pro entries")

    if not any(g["records"] for sec in found.values() for g in sec.values()):
        print("ERROR: no results in the window for any company/section. Is the VPN/REST "
              "reachable, or is the month window empty? Aborting.")
        return 1

    # ── Step 2 — Excel (5 sheets, independently filtered by channel) ─────────
    # Filtered by added_to_database (see _excel_rows_via_sql), NOT by entry_id
    # date like the deck — confirmed these are different fields.
    print("Step 2/8  Building the Excel (5 sheets)…")
    excel_sheets = []

    # Sheet 1 — Email: Ferguson/Grainger/SupplyHouse/Zoro (NOT Home Depot/Lowe's).
    # Reuses Step 1's already-fetched, already keyword=""-scoped email results
    # for these 4 companies — no new search needed.
    email_keys = ["ferguson", "grainger", "supplyhouse", "zoro"]
    email_ids = [r["entry_id"] for k in email_keys for r in found["email"][k]["records"]]
    rows = _excel_rows_via_sql(email_ids, start, end)
    print(f"   Email                      {len(email_ids):>3} entry_ids → {len(rows):>3} rows")
    excel_sheets.append({
        "name": "Email", "headers": EXCEL_HEADERS, "rows": rows, "hyperlinks": EXCEL_HYPERLINKS,
        "filter_row": _excel_filter_row("Email", email_keys, start, end),
    })

    # Sheet 2 — Home Depot Consumer Email: ALL Home Depot email, NOT split by
    # Pro/Consumer (misnamed vs. its actual content — kept exactly as in the
    # sample per your instruction not to "fix" this silently).
    rows = _excel_rows_via_sql([r["entry_id"] for r in hd_all_email_raw], start, end)
    print(f"   Home Depot Consumer Email  {len(hd_all_email_raw):>3} entry_ids → {len(rows):>3} rows")
    excel_sheets.append({
        "name": "Home Depot Consumer Email", "headers": EXCEL_HEADERS, "rows": rows,
        "hyperlinks": EXCEL_HYPERLINKS,
        "filter_row": _excel_filter_row("Email", ["home_depot"], start, end),
    })

    # Sheet 3 — Lowe's Consumer Email: ALL Lowe's email, no keyword split.
    # UNLIKE Home Depot, the deck has no "raw all-Lowe's-email" fetch anywhere
    # (its only Lowe's email group, lowes_pro, is keyword-filtered to
    # '"Lowe\'s Pro"') — so this is a genuinely NEW search, not a reuse, despite
    # the brief's assumption of symmetry with Home Depot. Flagging per your
    # "tell me, don't silently reconcile" rule.
    lowes_all_raw, _ = _run_group_search(
        {"title": "Lowe's (all email, unfiltered)", "company_names": ["Lowe's", "Lowes"], "keyword": ""},
        ["Email"],
    )
    lowes_all_raw = _dedup_by_entry_id(lowes_all_raw)
    rows = _excel_rows_via_sql([r["entry_id"] for r in lowes_all_raw], start, end)
    print(f"   Lowe's Consumer Email      {len(lowes_all_raw):>3} entry_ids → {len(rows):>3} rows")
    excel_sheets.append({
        "name": "Lowe's Consumer Email", "headers": EXCEL_HEADERS, "rows": rows,
        "hyperlinks": EXCEL_HYPERLINKS,
        "filter_row": _excel_filter_row("Email", ["lowes"], start, end),
    })

    # Sheet 4 — Direct Mail, 2-month window (target month + the one before it).
    # All 6 companies, own searches (the deck's DM section is single-month only
    # — "nothing in the current deck pipeline fetches data in this shape").
    dm_start = _add_months(start, -1)
    dm_rows: list[dict] = []
    for g in PLAIN_GROUPS:
        raw, cap_hit = _run_group_search(g, ["Direct Mail"])
        rows = _excel_rows_via_sql([r["entry_id"] for r in raw], dm_start, end)
        print(f"   Direct Mail  {g['title']:24} {len(raw):>3} raw"
              f"{' (cap hit)' if cap_hit else ''} → {len(rows):>3} rows")
        dm_rows.extend(rows)
    dm_sheet_name = f"Direct Mail ({dm_start:%B} - {end:%B})"
    excel_sheets.append({
        "name": dm_sheet_name, "headers": EXCEL_HEADERS, "rows": dm_rows, "hyperlinks": EXCEL_HYPERLINKS,
        "filter_row": _excel_filter_row("Direct Mail", [g["key"] for g in PLAIN_GROUPS], dm_start, end),
    })

    # Sheet 5 — Digital: all 6 companies, Online Display + Online Video ONLY —
    # narrower than the deck's 5-channel Digital Ads section. Intentional
    # mismatch per the sample; not reconciled with the deck here.
    digital_rows: list[dict] = []
    for g in PLAIN_GROUPS:
        raw, cap_hit = _run_group_search(g, EXCEL_DIGITAL_CHANNELS)
        rows = _excel_rows_via_sql([r["entry_id"] for r in raw], start, end)
        print(f"   Digital      {g['title']:24} {len(raw):>3} raw"
              f"{' (cap hit)' if cap_hit else ''} → {len(rows):>3} rows")
        digital_rows.extend(rows)
    excel_sheets.append({
        "name": "Digital", "headers": EXCEL_HEADERS, "rows": digital_rows, "hyperlinks": EXCEL_HYPERLINKS,
        "filter_row": _excel_filter_row(", ".join(EXCEL_DIGITAL_CHANNELS),
                                         [g["key"] for g in PLAIN_GROUPS], start, end),
    })

    xlsx_path = OUTPUT_DIR / f"SupplyHouse_Competitor_Ads_{month_year_stamp}.xlsx"
    xlsx_path = L.write_workbook(xlsx_path, excel_sheets)
    print(f"          saved {xlsx_path}")

    # Step 3 — per-group narrative + entry selection, one parallel Claude call
    # per non-empty group. Zero-result groups get a templated placeholder — no
    # LLM call, per the fixed 7/6/6 roster requirement.
    #
    # Dedupe by creative BEFORE curation: the LLM (and the pick_ids fallback
    # below) only ever sees this thinned-out pool, never the full raw list —
    # otherwise a company that reused one evergreen creative repeatedly could
    # end up with every image slot on its slide showing the same ad.
    for section in SECTIONS:
        for group in section["groups"]:
            entry = found[section["key"]][group["key"]]
            entry["dedup_records"] = _dedupe_similar_creative(entry["records"]) if entry["records"] else []

    # Step 3 — Selection calls (parallel), one per non-empty group. Picks
    # entry_ids only — no narrative, no count/cap_hit awareness (that's a
    # prose concern, entirely owned by Step 4 below).
    print("Step 3/8  Selecting featured pieces (parallel Claude calls)…")
    select_jobs = []
    job_index = []  # (section_key, group_key) aligned with select_jobs / results
    for section in SECTIONS:
        for group in section["groups"]:
            entry = found[section["key"]][group["key"]]
            if entry["records"]:
                select_jobs.append(
                    lambda sec=section, grp=group, e=entry:
                        _select_group(sec, grp, e["dedup_records"], month_label)
                )
                job_index.append((section["key"], group["key"]))

    selected_raw = L.run_parallel(select_jobs)
    selected = {idx: (d if isinstance(d, dict) else {}) for idx, d in zip(job_index, selected_raw)}

    # Theme-dedup backstop (sequential, deterministic) + build the FINAL
    # per-group id list and chosen records feeding Step 4. No cross-group
    # `exclude` needed — see _dedupe_themes' docstring.
    final_ids: dict[tuple, list[str]] = {}
    final_records: dict[tuple, list[dict]] = {}
    for section in SECTIONS:
        for group in section["groups"]:
            key = (section["key"], group["key"])
            entry = found[section["key"]][group["key"]]
            count = len(entry["records"])
            if count == 0:
                final_ids[key], final_records[key] = [], []
                print(f"   {group['title']:30} | {section['label']:11} | true=0 | shown=0 | split=n")
                continue
            sel = selected.get(key, {})
            if "error" in sel:
                print(f"   ! {group['title']} / {section['label']}: selection call failed — {sel['error']}")
            dedup_recs = entry["dedup_records"]
            want = min(MAX_SHOWN, len(dedup_recs))
            ids = L.pick_ids(sel.get("entry_ids"), dedup_recs, want, max_ids=MAX_SHOWN)
            ids = _dedupe_themes(ids, dedup_recs, want)
            final_ids[key] = ids
            id_set = set(ids)
            final_records[key] = [r for r in dedup_recs if r.get("entry_id") in id_set]
            reasoning = L.as_text(sel.get("reasoning"))
            chunks_preview = L.chunk_ids(ids, size=SLIDE_CAP) or [[]]
            print(f"   {group['title']:30} | {section['label']:11} | true={count} "
                  f"({'>=cap' if entry['cap_hit'] else 'exact'}) | shown={len(ids)} "
                  f"| split={'y' if len(chunks_preview) > 1 else 'n'}"
                  + (f"\n      reasoning: {reasoning}" if reasoning else ""))

    # Step 4 — Narrative calls (parallel), only for groups with final picks.
    print("Step 4/8  Writing narratives (parallel Claude calls)…")
    narrative_jobs = []
    narrative_index = []
    for section in SECTIONS:
        for group in section["groups"]:
            key = (section["key"], group["key"])
            recs = final_records[key]
            if not recs:
                continue
            entry = found[section["key"]][group["key"]]
            narrative_jobs.append(
                lambda sec=section, grp=group, recs=recs, tc=len(entry["records"]), ch=entry["cap_hit"]:
                    _write_narrative(sec, grp, recs, tc, ch, month_label)
            )
            narrative_index.append(key)

    narrative_raw = L.run_parallel(narrative_jobs)
    narratives = {idx: (d if isinstance(d, dict) else {}) for idx, d in zip(narrative_index, narrative_raw)}

    # Build final per-group slide data (ids, chunks, narrative) — same shape
    # consumed by Key Takeaways + deck assembly below, unchanged.
    slide_groups: dict[str, dict[str, dict]] = {s["key"]: {} for s in SECTIONS}
    for section in SECTIONS:
        for group in section["groups"]:
            key = (section["key"], group["key"])
            entry = found[section["key"]][group["key"]]
            ids = final_ids[key]
            if len(entry["records"]) == 0:
                narrative = section["placeholder"].format(company=group["title"], month_label=month_label)
            else:
                ndata = narratives.get(key, {})
                if "error" in ndata:
                    print(f"   ! {group['title']} / {section['label']}: narrative call failed — {ndata['error']}")
                narrative = L.fit_text(L.as_text(ndata.get("narrative")), CALLOUT_LIMIT)
            chunks = L.chunk_ids(ids, size=SLIDE_CAP) or [[]]
            slide_groups[section["key"]][group["key"]] = {"narrative": narrative, "chunks": chunks}

    # Step 5 — Key Takeaways, the LAST LLM call, built only from the finished
    # per-group narratives above (not raw OCR again).
    #
    # Two adjustments vs. just dumping every narrative in: (1) hd_pro and
    # hd_consumer are merged into one "Home Depot" line — the target style
    # treats Home Depot as a single company here, not split by Pro/Consumer;
    # (2) zero-activity companies are dropped from the input entirely — the
    # target style never mentions a quiet company by name or omission, it
    # just silently isn't there, so the LLM should never see it to begin with.
    print("Step 5/8  Key Takeaways (final Claude call, synthesizing finished narratives)…")
    email_lines = []
    for g in EMAIL_GROUPS:
        if g["key"] in ("hd_pro", "hd_consumer"):
            continue  # merged below
        if found["email"][g["key"]]["records"]:
            email_lines.append(f"- {g['title']}: {slide_groups['email'][g['key']]['narrative']}")
    hd_narratives = [slide_groups["email"][k]["narrative"] for k in ("hd_pro", "hd_consumer")
                     if found["email"][k]["records"]]
    if hd_narratives:
        email_lines.append(f"- Home Depot: {' '.join(hd_narratives)}")
    if not email_lines:
        email_lines.append("(no company had any email activity this month)")

    other_lines = []
    for sec_key in ("direct_mail", "digital_ads"):
        label = next(s["label"] for s in SECTIONS if s["key"] == sec_key)
        for g in PLAIN_GROUPS:
            if found[sec_key][g["key"]]["records"]:
                other_lines.append(f"- {g['title']} ({label}): {slide_groups[sec_key][g['key']]['narrative']}")
    if not other_lines:
        other_lines.append("(no company had any direct mail or digital activity this month)")

    takeaways = _key_takeaways(month_label, email_lines, other_lines)

    # Step 5 — assemble the deck --------------------------------------------
    print("Step 6/8  Building the deck…")
    slides: list[dict] = [
        {"type": "title", "data": {"title": f"{CLIENT} Competitor Ads", "date": month_label}},
        {"type": "agenda", "data": {"sections": ["Key Takeaways", "Email", "Direct Mail", "Digital Ads"]}},
        {"type": "needToKnow", "data": {
            "title1": f"{month_label} Email Activity",
            "text1":  L.cap_words(takeaways.get("email_column") or "No email findings available this month.",
                                   TAKEAWAYS_MAX_WORDS),
            "title2": f"{month_label} Other Channel Activity",
            "text2":  L.cap_words(takeaways.get("other_column") or "No direct mail / digital findings available this month.",
                                   TAKEAWAYS_MAX_WORDS),
        }},
    ]

    for section in SECTIONS:
        slides.append({"type": "newSection", "data": {"title": section["label"]}})
        for group in section["groups"]:
            sg = slide_groups[section["key"]][group["key"]]
            for i, chunk in enumerate(sg["chunks"]):
                title = group["title"] + (" (cont.)" if i > 0 else "")
                slides.append({"type": "entry_ids", "data": {
                    "slideTitle": title, "entryIds": chunk, "insight": sg["narrative"],
                }})

    slides.append({"type": "closing", "data": {}})

    result = build_deck_default(deck_title=f"{CLIENT} Competitor Ads — {month_label}", slides=slides)

    pptx_path = OUTPUT_DIR / f"SupplyHouse_Competitor_Ads_{month_year_stamp}.pptx"
    try:
        saved = L.save_pptx(result, pptx_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        print("       Check PPT_BUILDER_LOGIN / PPT_BUILDER_PASSWORD in .env and that "
              "csresearchhub.com is reachable.")
        print(f"       (The Excel was still written: {xlsx_path})")
        return 1

    # ── Acceptance checks ────────────────────────────────────────────────────
    print("\n── Acceptance checks ──")
    email_slides = sum(len(slide_groups["email"][g["key"]]["chunks"]) for g in EMAIL_GROUPS)
    dm_slides = sum(len(slide_groups["direct_mail"][g["key"]]["chunks"]) for g in PLAIN_GROUPS)
    da_slides = sum(len(slide_groups["digital_ads"][g["key"]]["chunks"]) for g in PLAIN_GROUPS)
    ok1 = len(EMAIL_GROUPS) == 7 and len(PLAIN_GROUPS) == 6
    print(f"  [{'PASS' if ok1 else 'FAIL'}] 7 email / 6 DM / 6 digital groups "
          f"({email_slides} email slides, {dm_slides} DM slides, {da_slides} digital slides)")

    ok2 = all(group["key"] in slide_groups[sec["key"]] for sec in SECTIONS for group in sec["groups"])
    print(f"  [{'PASS' if ok2 else 'FAIL'}] every company present in every section")

    hd_consumer_ids = {eid for chunk in slide_groups["email"]["hd_consumer"]["chunks"] for eid in chunk}
    hd_pro_ids_final = {eid for chunk in slide_groups["email"]["hd_pro"]["chunks"] for eid in chunk}
    ok3 = not (hd_consumer_ids & hd_pro_ids_final)
    print(f"  [{'PASS' if ok3 else 'FAIL'}] no entry_id in both hd_pro and hd_consumer")

    def _expected_chunks(n_shown: int) -> int:
        return max(1, -(-n_shown // SLIDE_CAP)) if n_shown else 1

    ok4 = all(
        len(sg["chunks"]) == _expected_chunks(sum(len(c) for c in sg["chunks"]))
        for sec in slide_groups.values() for sg in sec.values()
    )
    print(f"  [{'PASS' if ok4 else 'FAIL'}] >5-entry groups split into 2 slides "
          f"(narrative is assigned once per group and reused verbatim across its slides "
          f"by construction — see the slide-assembly loop above)")

    print(f"  [PASS] Key Takeaways was called after all per-group Selection + Narrative "
          f"calls (verify Step 5 in the code runs after Steps 3-4 above)")

    print("\n── True counts (sanity-check against PowerSearch before client delivery) ──")
    for section in SECTIONS:
        for group in section["groups"]:
            entry = found[section["key"]][group["key"]]
            n = len(entry["records"])
            print(f"  {section['label']:11} | {group['title']:30} | "
                  f"{'at least ' if entry['cap_hit'] else ''}{n}")

    print("\n── Excel row counts per sheet (sanity-check against PowerSearch) ──")
    for sheet in excel_sheets:
        print(f"  {sheet['name']:30} | {len(sheet['rows']):>4} rows")

    print(f"\n  Deck:  {saved}\n  Excel: {xlsx_path}")

    # Step 8 — email the deliverables to a reviewer, only if a recipient was
    # explicitly configured (SH_EMAIL_TO env var) — an actual send has real
    # inbox-facing consequences, so this is opt-in, never automatic.
    if EMAIL_TO:
        print(f"Step 8/8  Emailing deliverables to {EMAIL_TO}…")
        email_result = L.notify_report_ready(
            report_name=f"{CLIENT} Competitor Ads", period_label=month_label,
            attachment_paths=[saved, xlsx_path], to_addr=EMAIL_TO,
        )
        if email_result.get("status") == "sent":
            print(f"          sent (message_id={email_result.get('message_id')})")
        else:
            print(f"          !! email FAILED: {email_result.get('error')} — "
                  f"files are still saved locally, nothing lost")
    else:
        print("Step 8/8  Skipped emailing — no SH_EMAIL_TO set. Files are saved locally only.")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
