#!/usr/bin/env python3
"""
cs_api.py — the Competiscan Platform API client
═══════════════════════════════════════════════════════════════════════════════════════

One client, shared by Pipelines Studio v3 and by every pipeline it generates. If both
sides resolve the same filter names through the same code, a Preview in the Studio and a
run of the exported file cannot disagree.

WHY STDLIB AND NOT requests
    The Studio is what a researcher launches, and it must start under whatever `python`
    they happen to have. requests/pandas/boto3 live in one conda env here; the default
    interpreter has none of them. This API is plain JSON over HTTPS with a single header,
    so requests buys nothing worth a hard dependency. urllib it is.

ONE ENDPOINT
    Everything goes to POST /v1/search/enhanced. The service documents it as a superset
    of /v1/search with a byte-identical response shape — "no reason to integrate against
    both". Sending an enhanced body with no enhanced fields set IS a core call.

WHAT THIS MODULE WILL NOT DO
    It does not interpret cs-api's errors. An error envelope becomes an ApiError carrying
    the service's own code and message, because the service words them better than a
    wrapper can and the codes are the documented stable contract.

RUN
    python pipelines/cs_api.py --selftest      # health, catalog, taxonomy, lookup, probe
"""

from __future__ import annotations

import gzip
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://platform-api.competiscan.com"
ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(__file__).resolve().parent / "generated" / "_cache"
CATALOG_CACHE = CACHE_DIR / "filters.json"

# The service's own numbers, quoted so callers do not have to re-read the docs.
LIMIT_MAX = 10_000          # rows in one non-streaming search
COUNT_CAP = 25_000          # where a bounded total stops and total_is_capped goes true
DEFAULT_TIMEOUT = 180       # "a wide query genuinely can run for minutes"
COUNT_TIMEOUT = 600         # exact_count, or a stream

# The filters that may legally be a request's ONLY narrowing filter. Sending a body with
# none of these is a 400 no_filters before it reaches the database.
STANDALONE_FILTERS = ("sector", "media_channel", "audience", "company",
                      "panelist_id", "entry_id", "date_from", "date_to")

# Vocabularies /v1/filters/enhanced truncates because they are too large to inline. These
# are the only ones that need /v1/lookup typeahead rather than a rendered option list.
LOOKUP_FIELDS = ("company", "affinity_name", "publication", "dma")


# ═══════════════════════════════════════════════════════════════════════════════════════
# .env — mirrors report_lib, minus the hard dotenv dependency
# ═══════════════════════════════════════════════════════════════════════════════════════

def _load_env(path: Path | None = None) -> None:
    """Get .env into os.environ. python-dotenv when it is installed, otherwise a small
    parser of our own — the Studio has to run on interpreters that lack dotenv, and
    .env here is written `KEY = "value"`, which a naive split would keep the quotes of."""
    path = path or (ROOT / ".env")
    try:
        from dotenv import load_dotenv
        load_dotenv(path)
        return
    except Exception:
        pass
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        os.environ.setdefault(key, val)


_load_env()


def api_key() -> str:
    """The key, from the environment. Never printed, never written into a generated file —
    keys are bearer credentials and anyone holding one can spend the quota."""
    for name in ("CS_API_NAVIGATOR_KEY", "COMPETISCAN_API_KEY", "CS_API_KEY"):
        val = (os.environ.get(name) or "").strip()
        if val:
            return val
    raise ApiError(
        "missing_credentials",
        "No API key found. Put CS_API_NAVIGATOR_KEY=<key> in the project-root .env "
        "(or export COMPETISCAN_API_KEY).")


# ═══════════════════════════════════════════════════════════════════════════════════════
# Errors — cs-api's envelope, passed through
# ═══════════════════════════════════════════════════════════════════════════════════════

class ApiError(RuntimeError):
    """One failure. `code` is the stable thing to branch on; `message` is human text the
    service may reword. field/valid_options are present on unknown_filter_value, which is
    the error a researcher actually needs to read."""

    def __init__(self, code: str, message: str, *, status: int | None = None,
                 field: str | None = None, valid_options=None,
                 request_id: str | None = None, retry_after: float | None = None,
                 payload: dict | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status = status
        self.field = field
        self.valid_options = valid_options or []
        self.request_id = request_id
        self.retry_after = retry_after
        self.payload = payload or {}

    def hint(self) -> str:
        """One line a researcher can act on."""
        if self.code == "unknown_filter_value" and self.field:
            opts = ", ".join(str(o) for o in self.valid_options[:8])
            more = f" (+{len(self.valid_options) - 8} more)" if len(self.valid_options) > 8 else ""
            return f'{self.field}: {self.message} Valid: {opts}{more}' if opts else self.message
        if self.code == "no_filters":
            return (f"{self.message} Every section needs at least one of "
                    f"{', '.join(STANDALONE_FILTERS)}.")
        if self.code == "quota_exceeded":
            used, quota = self.payload.get("used"), self.payload.get("quota")
            return (f"{self.message} Retrying cannot help until "
                    f"{self.payload.get('period') or 'the period'} rolls over"
                    + (f" ({used}/{quota} used)." if used and quota else "."))
        return self.message


def _as_error(status: int, body: bytes, headers) -> ApiError:
    request_id = headers.get("X-Request-Id") if headers else None
    try:
        env = json.loads(body.decode("utf-8", "replace")).get("error") or {}
    except Exception:
        env = {}
    if not env:
        text = body.decode("utf-8", "replace").strip()
        if status in (502, 503, 504) or text.lstrip().lower().startswith("<html"):
            # A gateway error, not the service's own envelope — the body is an HTML
            # page. 504 in particular means the query outran the gateway, which is a
            # signal to narrow the window, not to retry: the query is still running.
            msg = {502: "The gateway could not reach the archive.",
                   503: "The archive is unreachable.",
                   504: ("The query outran the gateway timeout. Narrow the date range "
                         "— a month is fast where a year is not.")
                   }.get(status, f"Gateway error {status}.")
            return ApiError(f"gateway_{status}", msg, status=status,
                            request_id=request_id)
        return ApiError(f"http_{status}", text[:300] or f"HTTP {status}",
                        status=status, request_id=request_id)
    return ApiError(env.get("code") or f"http_{status}",
                    env.get("message") or f"HTTP {status}",
                    status=status,
                    field=env.get("field"),
                    valid_options=env.get("valid_options"),
                    request_id=request_id,
                    retry_after=env.get("retry_after_seconds"),
                    payload=env)


# ═══════════════════════════════════════════════════════════════════════════════════════
# Transport
# ═══════════════════════════════════════════════════════════════════════════════════════

_SSL = ssl.create_default_context()
CALLS = {"n": 0}  # every authenticated request counts against the monthly quota, errors too


def _open(method: str, url: str, body: bytes | None, timeout: int):
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("X-API-Key", api_key())
    req.add_header("Accept", "application/json")
    # About 6x fewer bytes on the wire. urllib will not ask for this on its own, and it
    # will not decode it either, so the caller below unwraps the gzip itself.
    req.add_header("Accept-Encoding", "gzip")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    CALLS["n"] += 1
    return urllib.request.urlopen(req, timeout=timeout, context=_SSL)


def _read(resp) -> dict:
    raw = resp.read()
    if (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
        raw = gzip.decompress(raw)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def request(method: str, path: str, *, body: dict | None = None,
            params: dict | None = None, timeout: int = DEFAULT_TIMEOUT,
            attempts: int = 4) -> dict:
    """One call, with the documented retry policy and nothing more.

    429 and 503 are worth retrying; any other 4xx will fail identically next time, so it
    is raised at once. quota_exceeded is a 429 that retrying cannot fix — it stops here.
    """
    url = BASE + path
    if params:
        clean = {k: v for k, v in params.items() if v not in (None, "")}
        if clean:
            url += "?" + urllib.parse.urlencode(clean)
    payload = json.dumps(body).encode("utf-8") if body is not None else None

    last: ApiError | None = None
    for attempt in range(attempts):
        try:
            with _open(method, url, payload, timeout) as resp:
                return _read(resp)
        except urllib.error.HTTPError as exc:
            err = _as_error(exc.code, exc.read(), exc.headers)
            if err.code == "quota_exceeded":
                raise err
            if err.code == "rate_limit_exceeded":
                time.sleep(min(float(err.retry_after or 1.0), 30.0))
                last = err
                continue
            if exc.code == 503:
                time.sleep(2 ** attempt)
                last = err
                continue
            raise err
        except urllib.error.URLError as exc:
            # DNS, TLS, a dropped connection. Worth one backoff, not a whole schedule.
            last = ApiError("unreachable", f"Could not reach {BASE}: {exc.reason}")
            time.sleep(2 ** attempt)
        except TimeoutError:
            # Do NOT retry a slow query on a short timeout: the query keeps running and
            # you have paid for it twice. Surface it and let the caller widen the timeout.
            raise ApiError("timeout",
                           f"No response within {timeout}s. A wide query can legitimately "
                           f"run for minutes — narrow the date range or raise the timeout.")
    raise last or ApiError("exhausted", "Retries exhausted with no error recorded.")


# ═══════════════════════════════════════════════════════════════════════════════════════
# Search
# ═══════════════════════════════════════════════════════════════════════════════════════

def search(body: dict, *, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """POST /v1/search/enhanced. Returns the response verbatim: total, total_is_capped,
    truncated, resolved_filters, took_ms, cached, results."""
    return request("POST", "/v1/search/enhanced", body=body, timeout=timeout)


def count(body: dict, *, exact: bool = False, timeout: int | None = None) -> dict:
    """The exact total for a filter set, without paying for the rows.

    limit is forced to 1: the count runs BEFORE the rows, so this is a cheap probe. It is
    also the thing that repairs the limit cliff — an exact, uncapped total no greater than
    the row limit proves the limit cannot fill, which licenses the server to drive the row
    query off the date index instead of walking the primary key to the end of it.
    """
    probe = {**body, "limit": 1, "include_total": True}
    if exact:
        probe["exact_count"] = True
    return search(probe, timeout=timeout or (COUNT_TIMEOUT if exact else DEFAULT_TIMEOUT))


def has_narrowing_filter(body: dict) -> bool:
    """The unbounded-query guard, checked before spending a request on a certain 400."""
    return any(body.get(k) for k in STANDALONE_FILTERS)


# ═══════════════════════════════════════════════════════════════════════════════════════
# Discovery — vocabulary, taxonomy, lookup
# ═══════════════════════════════════════════════════════════════════════════════════════

def taxonomy(parent: str | int | None = None) -> dict:
    """Children of a taxonomy node, or the top-level sectors when parent is omitted. Only
    nodes PowerSearch actually exposes are listed, so this is a menu of valid selections
    rather than everything /v1/search would accept."""
    return request("GET", "/v1/taxonomy",
                   params={"parent": None if parent in (None, "") else str(parent)},
                   timeout=60)


def lookup(field: str, q: str = "", limit: int = 50) -> dict:
    """Autocomplete one large vocabulary. Matching runs against cached names, so this
    never touches the database."""
    return request("GET", f"/v1/lookup/{urllib.parse.quote(field)}",
                   params={"q": q, "limit": max(1, min(int(limit), 500))}, timeout=60)


def health() -> dict:
    """Unauthenticated liveness. Does not count against the quota, so probe freely."""
    with urllib.request.urlopen(BASE + "/health", timeout=15, context=_SSL) as resp:
        return _read(resp)


def catalog(refresh: bool = False, max_age: int = 3600) -> dict:
    """The full filter surface: core filters, the enhanced groups, and the taxonomy roots.

    Cached on disk so the Studio opens with no network — a stale vocabulary that renders
    beats a blank screen — and stamped so the UI can say how old it is. The API refreshes
    its own reference data about every 15 minutes; names can be stale, ids never are.
    """
    if not refresh:
        cached = _read_cache()
        if cached and (time.time() - cached.get("fetched_at", 0)) < max_age:
            return {**cached, "source": "cache"}

    try:
        core = request("GET", "/v1/filters", timeout=60)
        enhanced = request("GET", "/v1/filters/enhanced", timeout=60)
        roots = taxonomy()
    except ApiError as exc:
        cached = _read_cache()
        if cached:
            return {**cached, "source": "cache", "error": exc.hint()}
        raise

    data = {
        "fetched_at": time.time(),
        "source": "live",
        "core": core,
        "groups": enhanced.get("groups") or {},
        "enhanced_notes": enhanced.get("notes") or {},
        "sectors": roots.get("children") or [],
    }
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        CATALOG_CACHE.write_text(json.dumps(data, indent=1), encoding="utf-8")
    except OSError:
        pass
    return data


def _read_cache() -> dict | None:
    try:
        data = json.loads(CATALOG_CACHE.read_text(encoding="utf-8"))
        return data if data.get("core") else None
    except (OSError, ValueError):
        return None


def flat_filters(cat: dict | None = None) -> dict:
    """{field_name: spec} across every enhanced group, with the group name folded into
    each spec. The Studio's filter picker and validate() both need one flat index."""
    cat = cat or catalog()
    out: dict[str, dict] = {}
    for group, items in (cat.get("groups") or {}).items():
        for name, spec in (items or {}).items():
            out[name] = {**spec, "group": group, "field": name}
    return out


def filter_fields(spec: dict) -> list[str]:
    """The request field names one filter occupies. A range is TWO fields, not one."""
    if spec.get("type") == "range":
        return list(spec.get("fields") or [f"{spec['field']}_min", f"{spec['field']}_max"])
    return [spec["field"]]


# ═══════════════════════════════════════════════════════════════════════════════════════
# Selftest
# ═══════════════════════════════════════════════════════════════════════════════════════

def selftest() -> int:
    bad = 0

    def step(label, fn):
        nonlocal bad
        try:
            t0 = time.time()
            val = fn()
            print(f"  {label:34} ok    {(time.time() - t0) * 1000:>6.0f}ms   {val}")
            return True
        except ApiError as exc:
            print(f"  {label:34} FAIL  {exc.code}: {exc.hint()[:110]}")
            bad += 1
            return False
        except Exception as exc:
            print(f"  {label:34} FAIL  {type(exc).__name__}: {exc}")
            bad += 1
            return False

    print(f"cs_api selftest — {BASE}\n")
    step("GET /health", lambda: health().get("version"))
    try:
        api_key()
        print(f"  {'API key':34} ok    present ({len(api_key())} chars)")
    except ApiError as exc:
        print(f"  {'API key':34} FAIL  {exc.message}")
        return 1

    cat = None

    def get_catalog():
        nonlocal cat
        cat = catalog(refresh=True)
        fl = flat_filters(cat)
        return (f"{len(cat['core'])} core keys, {len(cat['groups'])} groups, "
                f"{len(fl)} enhanced filters, {len(cat['sectors'])} sectors")

    step("catalog (live)", get_catalog)
    step("catalog (cache)", lambda: f"source={catalog()['source']}")
    step("GET /v1/taxonomy?parent=Credit Cards",
         lambda: ", ".join(c["name"] for c in taxonomy("Credit Cards")["children"]))
    step("GET /v1/lookup/company",
         lambda: lookup("company", "harborstone")["matches"])
    step("count probe (Banking/DM/July 2026)", lambda: (lambda r: (
        f"total={r['total']} capped={r['total_is_capped']} took={r['took_ms']}ms"))(
        count({"sector": ["Banking"], "media_channel": ["Direct Mail"],
               "audience": ["Consumer"], "credit_union": True,
               "date_field": "search_date",
               "date_from": "2026-07-01", "date_to": "2026-07-31"})))

    if cat:
        ranges = [f for f, s in flat_filters(cat).items() if s.get("type") == "range"]
        flags = [f for f, s in flat_filters(cat).items() if s.get("type") == "boolean"]
        trunc = [f for f, s in flat_filters(cat).items()
                 if s.get("count") and len(s.get("options") or []) < s["count"]]
        print(f"\n  {len(flags)} flags, {len(ranges)} ranges, "
              f"{len(trunc)} truncated vocabularies ({', '.join(trunc)})")

    print(f"\n  {CALLS['n']} quota units spent")
    print("SELFTEST", "FAILED" if bad else "PASSED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(selftest())


# ═══════════════════════════════════════════════════════════════════════════════════════
# Request bodies — the one place a section's filters become a request
# ═══════════════════════════════════════════════════════════════════════════════════════

# Core request fields that carry a list of names or ids.
_CORE_LISTS = ("sector", "category", "subcategory", "subsubcategory",
               "media_channel", "audience", "company", "ocr_text",
               "entry_id", "panelist_id")
# Core request fields that carry a single scalar, with the API's own default. Sending the
# default is harmless but noisy, so a value equal to it is left out.
_CORE_SCALARS = {"company_match": "exact", "ocr_text_match": "all",
                 "panelist_type": "all", "country": None,
                 "credit_score_type": "fico"}
# Keys the Studio keeps on a section's `search` that are NOT API filters. They are
# post-filters or execution settings, and must never reach the request body.
NON_API_KEYS = ("enhanced", "filters", "sort", "row_cap", "company_must_not_match",
                "collapse_repeats", "max_per_creative")


def build_body(search: dict, *, channel: str | None = None,
               date_field: str = "search_date",
               date_from=None, date_to=None,
               limit: int | None = None, include_total: bool | None = None,
               sort: str | None = None) -> dict:
    """One section's `search` spec -> one request body.

    The Studio's Preview and the pipeline it generates both call THIS function, so a
    previewed count and a real run cannot drift apart.

    `channel` overrides media_channel with a single value. That is deliberate and it is
    the load-bearing decision in this whole client: channels DO OR correctly inside one
    filter, but when the result truncates the cut is taken in date order, so one shared
    cap starves the low-volume channel. Measured on Banking/Consumer/credit unions over
    July 2026 — one call for three channels at limit 300 returned 258 Social Media, 41
    Email and 1 Direct Mail, against true totals of 1583 / 367 / 116.
    """
    body: dict = {}

    for key in _CORE_LISTS:
        vals = [v for v in (search.get(key) or []) if str(v).strip() != ""]
        if vals:
            body[key] = vals
    if channel:
        body["media_channel"] = [channel]

    for key, default in _CORE_SCALARS.items():
        val = search.get(key)
        if val not in (None, "", default):
            body[key] = val

    # Enhanced filters go through exactly as the researcher set them. A flag left on
    # "Any" is ABSENT from this dict, not False — flags are tri-state and omitting is the
    # default, so `false` would narrow to pieces explicitly recorded as not carrying it.
    for key, val in (search.get("enhanced") or {}).items():
        if val is None or val == [] or val == "":
            continue
        body[key] = val

    if date_field:
        body["date_field"] = date_field
    if date_from:
        body["date_from"] = str(date_from)
    if date_to:
        body["date_to"] = str(date_to)

    if sort:
        body["sort"] = sort
    if limit is not None:
        body["limit"] = max(1, min(int(limit), LIMIT_MAX))
    if include_total is not None:
        body["include_total"] = bool(include_total)
    return body


def month_slices(start, end) -> list[tuple]:
    """[(start, end), ...] one per calendar month the window touches, clipped to it.

    The way to get a result set larger than one query can return. There is no cursor and
    no offset to loop over, and narrowing is the cheap path anyway — every date column is
    indexed, so a month is fast where six years is not.
    """
    from datetime import date, timedelta
    out, cur = [], date(start.year, start.month, 1)
    while cur <= end:
        nxt = date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)
        out.append((max(cur, start), min(nxt - timedelta(days=1), end)))
        cur = nxt
    return out


# ═══════════════════════════════════════════════════════════════════════════════════════
# Reading one piece — the inverse of the ocr_text filter
# ═══════════════════════════════════════════════════════════════════════════════════════
#
# ocr_text asks "which pieces contain these words"; this asks "what does this piece
# say". They are not substitutes and most real work needs both, in that order: search to
# find the ids, read to see the page.
#
# The text is not a column on a search row because it is a longtext — the largest single
# chunk in the archive is over a million characters — and attaching that to every row of
# a 10,000-row result would dominate the response for a field almost nobody reads.

def ocr(entry_id: str, *, include_documents: bool = False,
        timeout: int = 60) -> dict | None:
    """One piece's scanned text, or None when there is nothing to read.

    Three outcomes get folded into one return value here, deliberately, because callers
    all do the same thing with them — skip the piece:
      * 404 entry_id_not_found — no approved piece carries that id
      * ocr_available: false   — a real piece that never went through OCR
      * no results at all
    A `truncated` result is NOT one of them: that is text, just less of it than exists,
    and the caller is told so it can say the count came from a cut page.
    """
    try:
        res = request("GET", "/v1/ocr",
                      params={"entry_id": entry_id,
                              "include_documents": "true" if include_documents else None},
                      timeout=timeout)
    except ApiError as exc:
        if exc.code == "entry_id_not_found":
            return None
        raise
    # `results` is a list because entry_id is indexed but not declared unique. Over the
    # approved archive it holds exactly one row — a property of the data, not a promise.
    rows = res.get("results") or []
    if not rows or not rows[0].get("ocr_available") or not rows[0].get("text"):
        return None
    return rows[0]


def ocr_texts(entry_ids, *, cap: int | None = None,
              on_progress=None) -> tuple[dict, list]:
    """{entry_id: text} for as many ids as the cap allows, plus a list of notes.

    There is no batch form, and the docs are explicit that there is no point wishing for
    one: each call is two keyed lookups, so a loop costs what a batch would and keeps a
    single million-character piece from holding up the rest. What a loop DOES cost is one
    quota unit per piece, which is why `cap` exists and why the caller is told when it
    bit rather than being handed a quietly short dictionary.
    """
    ids = [e for e in dict.fromkeys(entry_ids) if e]      # de-duplicated, order kept
    notes: list[str] = []
    if cap is not None and len(ids) > cap:
        notes.append(f"{len(ids)} pieces to read but the OCR cap is {cap} — read the "
                     f"first {cap}. Raise the cap or narrow the section.")
        ids = ids[:cap]

    out, missing, cut = {}, 0, 0
    for eid in ids:
        try:
            row = ocr(eid)
        except ApiError as exc:
            notes.append(f"{eid}: {exc.code} — {exc.hint()}")
            if exc.code == "quota_exceeded":
                raise
            continue
        if not row:
            missing += 1
            continue
        out[eid] = row.get("text") or ""
        if row.get("truncated"):
            cut += 1
        if on_progress:
            on_progress(len(out), len(ids))
    if missing:
        notes.append(f"{missing} of {len(ids)} piece(s) had no scanned text on file.")
    if cut:
        notes.append(f"{cut} piece(s) came back cut at the server's character cap.")
    return out, notes
