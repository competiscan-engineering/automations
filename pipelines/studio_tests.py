#!/usr/bin/env python3
"""
studio_tests.py — drive Pipelines Studio over HTTP, exactly as the browser does
═══════════════════════════════════════════════════════════════════════════════════════

pipeline_studio3.py --selftest proves the generator emits sound Python. This proves the
STUDIO works: that pressing Run in each of the four modes reaches the right stage of the
generated pipeline, that mode 3 really pauses and really resumes, that a download cannot
be walked out of, that a stop kills a live process, and that a run-time email address is
kept nowhere afterwards.

Every assertion goes through the same endpoints the page calls, so a pass means the
plumbing between the browser and the generated pipeline works — not merely that the
functions exist.

    python pipelines/studio_tests.py

It is also run by:

    python pipelines/pipeline_studio3.py --selftest --offline

Nothing here touches the archive, Bedrock, the tunnel, the deck builder or SES: the child
processes are started with pipelines/mock_archive.py installed. The generated pipelines
themselves are completely unmodified.

The browser half of the page — that boot() completes, that every render path survives a
real project, that the panel draws — is pipelines/studio_dom_test.js, which needs jsdom:

    npm install jsdom && node pipelines/studio_dom_test.js
"""
import io
import json
import os
import sys
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRATCH = ROOT / "pipelines" / "generated" / "_offline"
SCRATCH.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT))

# The mock has to be on the CHILD's path: _spawn copies os.environ, so setting it here
# is how a Studio-launched pipeline gets it.
shim = SCRATCH / "shim"
shim.mkdir(parents=True, exist_ok=True)
import pipelines.mock_archive as M  # noqa: E402
(shim / "sitecustomize.py").write_text(M.SITECUSTOMIZE, encoding="utf-8")
os.environ["PYTHONPATH"] = os.pathsep.join([str(shim), str(ROOT)])
os.environ["RS_MOCK_ROOT"] = str(ROOT)
os.environ["RS_MOCK_ROWS"] = "20"

import pipeline_studio3 as S  # noqa: E402
from http.server import ThreadingHTTPServer  # noqa: E402

PORT = int(os.environ.get("RS_TEST_PORT") or 8799)
BASE = f"http://127.0.0.1:{PORT}"
srv = ThreadingHTTPServer(("127.0.0.1", PORT), S.Handler)
threading.Thread(target=srv.serve_forever, daemon=True).start()

FAIL = []


def ok(label, cond, extra=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  — ' + str(extra)) if extra else ''}")
    if not cond:
        FAIL.append(label)


def get(path, raw=False):
    with urllib.request.urlopen(BASE + path, timeout=120) as r:
        body = r.read()
        return (r.status, body) if raw else json.loads(body)


def post(path, obj):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(obj).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.loads(r.read())


def post_maybe(path, obj):
    """post(), except that a refusal comes back as its body instead of an exception.
    Half of what the shelf has to get right is what it says no to."""
    try:
        return post(path, obj)
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read())


def get_maybe(path):
    """get(), same bargain."""
    try:
        return get(path)
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read())


def wait(run_id, limit=600):
    t0 = time.time()
    while time.time() - t0 < limit:
        s = get(f"/api/run/status?id={run_id}")
        if s.get("done"):
            return s
        time.sleep(0.4)
    raise TimeoutError("run never finished")


print("\n=== spec: the four modes, in order, with the agreed wording ===")
spec = get("/api/spec")
labels = [m["label"] for m in spec["modes"]]
want = ["Run the searches", "Run the workbook", "Run and edit the deliverables",
        "Run the pipeline"]
ok("the four run modes, in order, with the agreed wording", labels == want, labels)
ok("templates are offered separately from saved reports",
   len(spec["templates"]) >= 6 and all(t.get("note") for t in spec["templates"]),
   [t["label"] for t in spec["templates"]])

print("\n=== a one-time report, blank project to downloaded deck ===")
proj = get("/api/template?name=blank")["project"]
proj["client"] = "Acme One Off"
proj["name"] = "acme one off"
proj["window"] = {"mode": "range", "start": "2026-04-01", "end": "2026-04-30"}
sec = proj["sections"][0]
sec["title"] = "Checking offers"
sec["search"]["media_channel"] = ["Direct Mail", "Email"]
sec["search"]["sector"] = ["Banking"]
sec["feature"]["count"] = 3

chk = post("/api/check", {"project": proj})
ok("a blank project with a fixed range validates", chk["errors"] == 0,
   [i["msg"][:70] for i in chk["issues"] if i["level"] == "error"])
ok("the badge starts at Draft", chk["badge"]["state"] == "draft", chk["badge"]["label"])
ok("the ad-hoc range reaches the window the run will use",
   chk["window"] == {"start": "2026-04-01", "end": "2026-04-30", "mode": "range"},
   chk["window"])
ok("a fixed range is warned about in the editor",
   any("fixed window" in i["msg"] for i in chk["issues"]),
   [i["msg"][:60] for i in chk["issues"] if i["level"] == "warn"][:2])

print("\n--- mode 1: Run the searches ---")
r = post("/api/run", {"project": proj, "mode": "search"})
st = wait(r["run_id"])
ok("mode 1 exits cleanly", st["rc"] == 0, st["lines"][-3:])
ok("mode 1 makes no model call and writes no file",
   not st["files"] and not any("Step 5" in x for x in st["lines"]))
pan = get(f"/api/run/panel?id={r['run_id']}")
n = sum(len(s["pieces"]) for s in pan["sections"])
ok("the panel lists retrieved entry_ids grouped by section",
   len(pan["sections"]) == 1 and n > 0, f"{n} pieces")
ok("every piece carries the detail the panel promises",
   all(p["entry_id"] and p["company"] and p["channel"] and p["date"]
       for s in pan["sections"] for p in s["pieces"]))
ok("no --limit was injected", not any("--limit" in x for x in st["lines"]))

print("\n--- a capped search reads as 'at least N' ---")
os.environ["RS_MOCK_CAPPED"] = "1"
rc = post("/api/run", {"project": proj, "mode": "search"})
stc = wait(rc["run_id"])
panc = get(f"/api/run/panel?id={rc['run_id']}")
ok("the panel marks a capped section as a lower bound",
   all(s["at_least"] for s in panc["sections"]),
   [(s["title"], s["archive_total"], s["at_least"]) for s in panc["sections"]])
ok("the log says 'at least' rather than quoting the figure",
   any("at least" in x for x in stc["lines"]))
os.environ.pop("RS_MOCK_CAPPED", None)

print("\n--- mode 2: Run the workbook ---")
r = post("/api/run", {"project": proj, "mode": "excel"})
st = wait(r["run_id"])
ok("mode 2 exits cleanly", st["rc"] == 0, st["lines"][-3:])
ok("mode 2 writes a workbook", any(f["name"].endswith(".xlsx") for f in st["files"]),
   [f["name"] for f in st["files"]])
ok("mode 2 makes no model call", not any("Step 5" in x for x in st["lines"]))
book = [f for f in st["files"] if f["name"].endswith(".xlsx")][0]
code, body = get(f"/api/run/file?id={r['run_id']}&name="
                 + urllib.parse.quote(book["name"]), raw=True)
ok("the workbook downloads", code == 200 and body[:2] == b"PK" and len(body) > 3000,
   f"{len(body)} bytes")

print("\n--- the download endpoint cannot be walked out of ---")
for probe in ["../../../../.env", "..%2F..%2F.env", "../state.json",
              "pipeline.py", "..\\..\\.env"]:
    try:
        c, _ = get(f"/api/run/file?id={r['run_id']}&name="
                   + urllib.parse.quote(probe, safe=""), raw=True)
        got = c
    except urllib.error.HTTPError as e:
        got = e.code
    ok(f"refuses {probe!r}", got == 404, got)
try:
    c, _ = get("/api/run/file?id=../../&name=x", raw=True)
    got = c
except urllib.error.HTTPError as e:
    got = e.code
ok("refuses a run id that is not a run id", got == 404, got)

print("\n--- mode 3: Run and edit the deliverables ---")
r = post("/api/run", {"project": proj, "mode": "curate"})
rid = r["run_id"]
st = wait(rid)
ok("mode 3 pauses instead of finishing", st.get("paused") is True and st["rc"] == 0,
   st["lines"][-3:])
ok("it paused BEFORE the write-ups",
   any("Step 5 " in x for x in st["lines"])
   and not any("Step 6" in x for x in st["lines"]))
pan = get(f"/api/run/panel?id={rid}")
psec = pan["sections"][0]
picks = [p["entry_id"] for p in psec["picks"]]
ok("the panel lists the picks with their detail", len(picks) == 3, picks)
ok("the workbook is already downloadable at the pause",
   any(f["name"].endswith(".xlsx") for f in pan["files"]),
   [f["name"] for f in pan["files"]])

rejected = picks[0]
res = post("/api/run/replace", {"run_id": rid, "section": psec["id"],
                                "keep": picks[1:], "reject": [rejected], "used": []})
rep = (res.get("replacement") or {}).get("entry_id")
pool = {p["entry_id"] for p in psec["pieces"]}
ok("a rejection is replaced from the cached pool", rep in pool, rep)
ok("the replacement is not the rejected piece and not one already shown",
   rep != rejected and rep not in picks[1:], rep)

final = [rep] + picks[1:]
cont = post("/api/run/continue", {"run_id": rid, "approved": {psec["id"]: final}})
ok("the run continues from the pause", cont.get("ok") is True, cont)
st = wait(rid)
ok("the build half exits cleanly", st["rc"] == 0, st["lines"][-4:])
ok("the build half never searched again",
   not any("Step 1  Searching" in x for x in st["lines"][-40:]))
ok("mode 3 produces a deck and offers it for download",
   any(f["name"].endswith(".pptx") for f in st["files"]),
   [f["name"] for f in st["files"]])

slides = json.loads((S.run_dir(rid) / "output" / [
    f["name"] for f in st["files"] if f["name"].endswith(".slides.json")][0])
    .read_text("utf-8"))
on_slide = [e for s in slides if s["type"] == "entry_ids" for e in s["data"]["entryIds"]]
ok("the deck holds exactly the approved pieces", sorted(on_slide) == sorted(final),
   on_slide)
ok("the rejected piece is not on a slide", rejected not in on_slide)
insight = [s["data"]["insight"] for s in slides if s["type"] == "entry_ids"][0]
ok("the write-up describes the approved set, not the original picks",
   all(c["company"] in insight
       for c in psec["picks"] if c["entry_id"] in picks[1:]) and len(insight) > 40,
   insight[:90])

print("\n--- mode 4: Run the pipeline, with a run-time recipient ---")
r = post("/api/run", {"project": proj, "mode": "full",
                      "email_to": "someone.else@competiscan.com"})
st = wait(r["run_id"])
ok("mode 4 runs end to end", st["rc"] == 0, st["lines"][-3:])
ok("mode 4 emails AND offers downloads",
   any("Emailing deliverables" in x for x in st["lines"])
   and any(f["name"].endswith(".pptx") for f in st["files"])
   and any(f["name"].endswith(".xlsx") for f in st["files"]),
   [f["name"] for f in st["files"]])
sent = json.loads((S.run_dir(r["run_id"]) / "output" / "_email.json").read_text("utf-8"))
ok("it delivered to the address typed at run time",
   sent["to"] == "someone.else@competiscan.com", sent)
ok("the address is nowhere in the project", "someone.else" not in json.dumps(proj))
saved = post("/api/projects/save", {"project": proj})
reloaded = get("/api/projects/load?name=" + saved["name"])["project"]
ok("reloading the project shows no recipient anywhere",
   "someone.else" not in json.dumps(reloaded)
   and "to_addr" not in json.dumps(reloaded.get("email")),
   reloaded.get("email"))

print("\n--- every finished run offers its deliverables as one download ---")
# The reason this endpoint exists: a finished run leaves four or five files behind and
# the researcher was fetching them one at a time, or waiting for the email. The names
# on the status payload and the names inside the zip have to be the same set, or the
# bar in the page is offering something the zip does not contain.
full_id = r["run_id"]
listed = sorted(f["name"] for f in st["files"])
req = urllib.request.Request(BASE + "/api/run/zip?id=" + full_id)
with urllib.request.urlopen(req, timeout=120) as resp:
    zbody = resp.read()
    zdisp = resp.headers.get("Content-Disposition") or ""
    ztype = resp.headers.get("Content-Type") or ""
ok("the zip is served as a zip attachment",
   ztype == "application/zip" and "attachment;" in zdisp, ztype + " | " + zdisp)
ok("it is named after the report, not after the run alone",
   "acme-one-off" in zdisp.lower() and full_id in zdisp, zdisp)
with zipfile.ZipFile(io.BytesIO(zbody)) as z:
    inside = sorted(z.namelist())
    sizes = {i.filename: i.file_size for i in z.infolist()}
ok("the zip holds exactly the files the run listed", inside == listed, inside)
ok("and every one of them has its bytes, not an empty entry",
   bool(sizes) and all(v > 0 for v in sizes.values()), sizes)
ok("the deck and the workbook are both in it",
   any(n.endswith(".pptx") for n in inside)
   and any(n.endswith(".xlsx") for n in inside), inside)

# The zip is built in memory on purpose. Written into the output directory it would be
# picked up as one of the run's own deliverables and land inside itself the next time
# somebody asked for it.
after = sorted(f["name"] for f in get("/api/run/files?id=" + full_id)["files"])
ok("asking for the zip does not add a file to the run", after == listed, after)

for probe in ["../../", "0" * 12, "not-a-run", ""]:
    try:
        c, _ = get("/api/run/zip?id=" + urllib.parse.quote(probe, safe=""), raw=True)
        got = c
    except urllib.error.HTTPError as e:
        got = e.code
    ok("the zip endpoint refuses " + repr(probe), got == 404, got)

search_run = post("/api/run", {"project": proj, "mode": "search"})
sst = wait(search_run["run_id"])
ok("a search-only run finishes with nothing to download",
   sst["rc"] == 0 and not sst["files"], sst["files"])
try:
    c, _ = get("/api/run/zip?id=" + search_run["run_id"], raw=True)
    got = c
except urllib.error.HTTPError as e:
    got = e.code
ok("and its zip is a 404 rather than an empty archive", got == 404, got)

print("\n--- the page has somewhere to put them ---")
# The bar is the point of the change: before it, a deck from mode 4 was on disk and in
# an email and nowhere the researcher could click. It is asserted against the served
# page because a JavaScript name that no longer matches its element is invisible to
# every other test in this file.
page = get("/", raw=True)[1].decode("utf-8")
for needle in ['<div id="deliv"', '<div id="delivTab"', "#deliv{", "#delivTab{",
               "function showFiles(", "function clearFiles(", "function zipLink(",
               "function toggleDeliv(", "/api/run/zip?id="]:
    ok("the page carries " + repr(needle), needle in page)
ok("the strip is on the right edge, next to the results strip",
   page.index('id="panelTab"') < page.index('id="delivTab"')
   < page.index('id="logbar"'))
ok("and the results strip only appears for the mode that stops for review",
   "if(s.paused)loadPanel();" in page and "function killPanel(" in page)
ok("the bar is emptied when the next run starts", "clearLog();clearFiles();" in page)
ok("every finished run fills it, not only the one that pauses",
   "showFiles(s.files||[],RUNID," in page)

print("\n--- a mode a report cannot do is explained, not crashed into ---")
noBook = json.loads(json.dumps(proj))
noBook["workbook"]["enabled"] = False
try:
    r2 = post("/api/run", {"project": noBook, "mode": "excel"})
    got = r2
except urllib.error.HTTPError as e:
    got = json.loads(e.read())
ok("running the workbook on a report that has none is refused in plain words",
   "does not build a workbook" in str(got.get("error", "")), got)
noDeck = json.loads(json.dumps(proj))
noDeck["deck"]["enabled"] = False
try:
    r2 = post("/api/run", {"project": noDeck, "mode": "curate"})
    got = r2
except urllib.error.HTTPError as e:
    got = json.loads(e.read())
ok("curating a report with nothing on a slide is refused in plain words",
   "no pieces to approve" in str(got.get("error", "")), got)
try:
    r2 = post("/api/run", {"project": proj, "mode": "nonsense"})
    got = r2
except urllib.error.HTTPError as e:
    got = json.loads(e.read())
ok("an unknown mode is refused", "unknown run mode" in str(got.get("error", "")), got)
try:
    r2 = post("/api/run", {"project": proj, "mode": "full", "email_to": "not-an-email"})
    got = r2
except urllib.error.HTTPError as e:
    got = json.loads(e.read())
ok("a run-time address that is not an address is refused",
   "does not look like an email" in str(got.get("error", "")), got)

print("\n--- the guardrails, in the file the Studio actually runs ---")
gen = post("/api/check", {"project": proj})
src = None
for f in sorted((S.GENERATED_DIR).glob("_run_*.py"), key=lambda q: q.stat().st_mtime):
    src = f.read_text("utf-8")
ok("a generated file was written and kept", src is not None)
ok("searches run one channel at a time, never in parallel",
   'for channel in sec["search"]["media_channel"]:' in src
   and "run_parallel" not in src.split("def stage_search")[1].split("def print_counts")[0])
ok("a capped total is said as a lower bound", "at least " in src)
ok("cap-hit with nothing in the window is SUSPECT, not a zero", "SUSPECT" in src)
ok("slides hold at most five, and overflow rolls onto (cont.)",
   "SLIDE_CAP    = 5" in src and "(cont.)" in src and "chunk_ids" in src)
ok("write-ups are trimmed to whole sentences", "L.fit_text" in src)
ok("an invented entry_id cannot reach a slide", "L.pick_ids" in src)
ok("the selection rules live in exactly one place",
   src.count("def _eligible") == 1 and src.count("def _one_per_company") == 1)
ok("no write-up is generated in the selection stage",
   "_writeup" not in src.split("def stage_select")[1].split("def stage_deliver")[0])
ok("no literal recipient is baked in",
   all("@" not in ln for ln in src.splitlines() if ln.startswith("EMAIL_TO")))
ok("the settings block is still at the top",
   src.index("# ── Report settings") < src.index("SECTIONS = ["))

print("\n--- a long run can be stopped ---")
big = json.loads(json.dumps(proj))
big["sections"] = []
for i in range(6):
    s2 = get("/api/section")["section"]
    s2["title"] = f"Section {i}"
    s2["search"]["media_channel"] = ["Direct Mail", "Email", "Social Media"]
    s2["search"]["sector"] = ["Banking"]
    big["sections"].append(s2)
# Genuinely slow, so the process really is alive when Stop is pressed.
os.environ["RS_MOCK_SLOW"] = "1.5"
r = post("/api/run", {"project": big, "mode": "excel"})
time.sleep(6)
mid = get(f"/api/run/status?id={r['run_id']}")
ok("the run is still going when Stop is pressed", mid["done"] is False
   and mid["running"] is True, mid["done"])
stop = post("/api/run/stop", {"run_id": r["run_id"]})
ok("stop killed a live process", stop.get("stopped") is True
   and stop.get("was_running") is True, stop)
st = wait(r["run_id"], limit=60)
ok("the studio knows the run is over", st["done"] is True and st["running"] is False)
ok("it is recorded as stopped, not as a clean finish",
   st["stopped"] is True and not st.get("paused"))
ok("the log says so", any("Stopped" in x for x in st["lines"]), st["lines"][-2:])
ok("partial output survives the stop and is still listed",
   isinstance(st["files"], list))
os.environ.pop("RS_MOCK_SLOW", None)
os.environ["RS_MOCK_ROWS"] = "20"

print("\n--- notes for Engineering are lifted into the generated file ---")
noted = json.loads(json.dumps(proj))
noted["notes"] = ("Split the lending section by application type.\n"
                  "Ask Meg about the Q3 comparison before this goes out.")
post("/api/run", {"project": noted, "mode": "search"})
time.sleep(0.6)
newest = sorted(S.GENERATED_DIR.glob("_run_*.py"), key=lambda q: q.stat().st_mtime)[-1]
doc = newest.read_text("utf-8").split('"""')[1]
ok("both note lines reach the docstring, unreflowed",
   "Split the lending section by application type." in doc
   and "Ask Meg about the Q3 comparison before this goes out." in doc,
   doc.count("NOTES FOR ENGINEERING"))

print("\n--- status: draft until Engineering has it, then delivered ---")
proj["status"]["runs"] = [{"id": "a" * 12, "mode": "full", "at": "2026-08-25T10:00:00",
                           "rc": 0, "stopped": False, "emailed": False,
                           "produced": ["Acme_Report.pptx"]}]
b = post("/api/check", {"project": proj})["badge"]
ok("running it is not delivering it — a produced deck is still a Draft",
   b["state"] == "draft" and b["label"] == "Draft", b["label"])
ok("but the run it did have is on the badge, not thrown away",
   "Acme_Report.pptx" in b["detail"], b["detail"])
h = post("/api/check", {"project": proj})["content_hash"]
proj["status"]["sent"] = {"at": "2026-08-26T09:00:00", "file": "Acme.py", "hash": h}
b = post("/api/check", {"project": proj})["badge"]
ok("only Send to Eng. makes it Delivered",
   b["state"] == "sent" and b["label"].startswith("Delivered"), b["label"])
proj["sections"][0]["title"] = "Checking offers (revised)"
b = post("/api/check", {"project": proj})["badge"]
ok("editing a sent report moves it to Edited since sent", b["state"] == "edited",
   b["label"])
proj["status"]["runs"].append({"id": "b" * 12, "mode": "full",
                               "at": "2026-08-27T10:00:00", "rc": 0, "stopped": False,
                               "emailed": False, "produced": ["x.pptx"]})
b = post("/api/check", {"project": proj})["badge"]
ok("running it again does not clear the edited state", b["state"] == "edited")

print("\n--- promote a one-off to recurring ---")
one = json.loads(json.dumps(proj))
one["window"] = {"mode": "range", "start": "2026-04-01", "end": "2026-06-30"}
pr = post("/api/promote", {"project": one})
np = pr["project"]
ok("the fixed range is gone", np["window"]["mode"] == "cadence"
   and not np["window"]["start"], np["window"])
ok("it lands on a repeatable anchor", np["anchor"] == "prior_complete"
   and np["cadence"] in ("week", "month"), (np["cadence"], np["anchor"]))
ok("it says what it changed", any("fixed range" in c for c in pr["changes"]),
   pr["changes"][:1])
ok("it warns that the dates are gone",
   any("2026-04-01 .. 2026-06-30" in w for w in pr["warnings"]))
code, _ = get("/api/spec", raw=True)

print("\n--- run history ---")
runs = get("/api/run/files?id=" + r["run_id"])
ok("the files of a past run are listable", "files" in runs, runs)
gone = get("/api/run/files?id=" + "0" * 12)
ok("a pruned run degrades to an empty list rather than an error",
   gone.get("files") == [], gone)

print("\n--- thumbnails: the guards, always; the pictures, when the tunnel is up ---")
# The guards need nothing but the Studio, so they run everywhere.
thumb_run = r["run_id"]
for probe in ["../../../.env", "..%2F..%2Fstate", "../output/x", "a/b", "'; DROP",
              "..\\..\\.env"]:
    try:
        c, _ = get(f"/api/thumb?id={thumb_run}&entry_id="
                   + urllib.parse.quote(probe, safe=""), raw=True)
        got = c
    except urllib.error.HTTPError as e:
        got = e.code
    ok(f"the thumbnail endpoint refuses {probe!r}", got == 404, got)
try:
    c, _ = get(f"/api/thumb?id=../../&entry_id=2026-01-01-0001", raw=True)
    got = c
except urllib.error.HTTPError as e:
    got = e.code
ok("and refuses a run id that is not a run id", got == 404, got)
ok("it will not fetch for an unknown run",
   bool(post("/api/thumbs", {"run_id": "not-a-run",
                             "entry_ids": ["2026-01-01-0001"]}).get("error")))

# The pictures themselves need the database and S3. There is no image URL on a search
# row — pdf_url is a PowerSearch page behind a login — so a cover image costs a query
# and a credentialed read. Absent either, this reports SKIP rather than failing a
# machine that simply has no tunnel open.
REAL = os.environ.get("RS_TEST_ENTRY_IDS", "2026-08-13-1578").split(",")
tres = post("/api/thumbs", {"run_id": thumb_run, "entry_ids": REAL})
if tres.get("error"):
    print(f"  SKIP  live cover images — {str(tres['error'])[:90]}")
elif not (tres.get("thumbs") or {}):
    print(f"  SKIP  live cover images — the archive holds none for {REAL}")
else:
    eid = next(iter(tres["thumbs"]))
    code, body = get(f"/api/thumb?id={thumb_run}&entry_id="
                     + urllib.parse.quote(eid), raw=True)
    ok("a real piece's cover image is fetched and served as a JPEG",
       code == 200 and body[:2] == b"\xff\xd8" and len(body) > 2000,
       f"{code} {len(body)}b")
    again = post("/api/thumbs", {"run_id": thumb_run, "entry_ids": REAL})
    ok("and the second ask is served from cache", again.get("cached", 0) >= 1,
       again.get("cached"))

print("\n--- what the model cost, per run, on the operator's terminal ---")
# The prices are asserted against the literal in report_lib rather than an imported
# value: this suite runs under the stdlib-only interpreter the Studio itself uses, and
# report_lib needs requests, pandas and boto3. Reading the line is also the more honest
# test of "use these prices".
import io
import contextlib
import re as _re

lib = (ROOT / "pipelines" / "report_lib.py").read_text("utf-8")
m_in = _re.search(r"_MODEL_IN_PRICE\s*=\s*([0-9.]+)\s*/\s*1_000_000", lib)
m_out = _re.search(r"_MODEL_OUT_PRICE\s*=\s*([0-9.]+)\s*/\s*1_000_000", lib)
ok("input tokens are priced at $3.00 per million",
   bool(m_in) and float(m_in.group(1)) == 3.00, m_in and m_in.group(1))
ok("output tokens are priced at $15.00 per million",
   bool(m_out) and float(m_out.group(1)) == 15.00, m_out and m_out.group(1))
ok("every model call reports its own cost, machine-readably",
   'cost=${cost:.6f}' in lib)
ok("and the trace is off unless the variable is set",
   'LLM_TRACE = os.environ.get("RS_LLM_TRACE") == "1"' in lib)

IN_P, OUT_P = float(m_in.group(1)) / 1e6, float(m_out.group(1)) / 1e6


def cost(a, b):
    return a * IN_P + b * OUT_P


# Two phases of one run, the way a curated run really works, with the trace lines a
# real pipeline would emit.
cost_run = "0123456789ab"
S.RUNS[cost_run] = {"lines": [], "done": False, "rc": None, "mode": "curate",
                    "proc": None, "at": "", "stopped": False, "target": "",
                    "emailed": False, "llm": S._llm_zero()}
cout = S.run_dir(cost_run) / "output"
cout.mkdir(parents=True, exist_ok=True)
PICK = [(1841, 96), (1602, 88), (1750, 91)]
BUILD = [(2400, 310), (2210, 288), (900, 120)]


def emit(calls):
    return "\n".join(
        f"print('[LLM] -- m in={a} out={b} stop=end_turn cost=${cost(a, b):.6f}')"
        for a, b in calls) + "\nprint('Step 8  Deck...')"


def drive(rid, calls):
    buf = io.StringIO()
    with S.RUNS_LOCK:
        S.RUNS[rid]["done"] = False
    with contextlib.redirect_stdout(buf):
        S._spawn(rid, [None, "-c", emit(calls)])
        for _ in range(400):
            with S.RUNS_LOCK:
                if S.RUNS[rid]["done"]:
                    break
            time.sleep(0.05)
    return buf.getvalue()


p1 = drive(cost_run, PICK)
ok("a paused run holds its total back — it is only half finished", "TOTAL" not in p1)
(cout / "Acme_Report.pptx").write_bytes(b"x")
(cout / "Acme_Data.xlsx").write_bytes(b"x")
with S.RUNS_LOCK:
    S.RUNS[cost_run]["mode"] = "full"
p2 = drive(cost_run, BUILD)

want = sum(cost(a, b) for a, b in PICK + BUILD)
acc = dict(S.RUNS[cost_run]["llm"])
ok("both halves of the run are in one tally", acc["calls"] == len(PICK) + len(BUILD),
   acc["calls"])
ok("the dollars add up across them", abs(acc["usd"] - want) < 1e-6,
   f"${acc['usd']:.6f} vs ${want:.6f}")
ok("a total is printed when the run is over, in dollars",
   "TOTAL" in p2 and f"${want:.4f}" in p2, f"${want:.4f}")
ok("it names the deliverable the spend bought",
   "Acme_Report.pptx" in p2 and "Acme_Data.xlsx" in p2)
ok("none of it leaks into the researcher's output panel",
   not any("[LLM]" in l for l in S.RUNS[cost_run]["lines"] if not l.startswith("$ ")))
ok("ordinary output still does", any("Step 8  Deck..." in l
                                    for l in S.RUNS[cost_run]["lines"]))

import shutil
shutil.rmtree(S.run_dir(cost_run), ignore_errors=True)

print("\n--- every saved project still opens over HTTP ---")
listing = get("/api/projects")["projects"]
ok("the projects list carries a badge per row",
   all(p.get("badge") for p in listing), len(listing))
bad = []
for row in listing:
    d = get("/api/projects/load?name=" + urllib.parse.quote(row["name"]))
    if d.get("error") or not d.get("project"):
        bad.append(row["name"])
ok(f"all {len(listing)} saved projects load", not bad, bad)
ok("every row says when it was last saved, so the list can put the newest first",
   all(p.get("modified") for p in listing),
   [p["name"] for p in listing if not p.get("modified")])
ok("the newest is first", [p["name"] for p in listing]
   == [p["name"] for p in sorted(listing, key=lambda r: -r["modified"])],
   [p["name"] for p in listing[:3]])

print("\n--- a report can be copied, and the copy is a report of its own ---")
copy = post("/api/projects/duplicate", {"name": saved["name"]})
ok("the copy is a different file", copy.get("name")
   and copy["name"] != saved["name"], copy.get("name"))
ok("named off the original rather than slugged at the researcher",
   copy.get("title", "").endswith("copy"), copy.get("title"))
ok("it points at its own file, not the original's",
   copy["project"]["status"]["saved_as"] == copy["name"],
   copy["project"]["status"])
ok("and claims none of the original's history — it has produced and sent nothing",
   not copy["project"]["status"].get("runs")
   and not copy["project"]["status"].get("sent"), copy["project"]["status"])
ok("the original is still there, unchanged",
   get("/api/projects/load?name=" + saved["name"]).get("project", {}).get("name")
   == proj["name"])
again = post("/api/projects/duplicate", {"name": saved["name"]})
ok("copying twice does not overwrite the first copy",
   again["name"] != copy["name"], [copy["name"], again["name"]])
ok("copying something that is not there is a refusal, not a traceback",
   post_maybe("/api/projects/duplicate", {"name": "no_such_report"}).get("error")
   == "not found")

print("\n--- delete takes a report off the shelf without destroying it ---")
gone = post("/api/projects/delete", {"name": copy["name"]})
ok("it says where the file went", "_trash" in str(gone.get("trash")), gone)
ok("the file is still there to be put back",
   Path(gone["trash"]).is_file() and json.loads(
       Path(gone["trash"]).read_text("utf-8"))["name"] == copy["title"])
ok("it is off the shelf",
   not any(r["name"] == copy["name"] for r in get("/api/projects")["projects"]))
ok("the trash is not itself a report",
   not any(r["name"].startswith("_trash") for r in get("/api/projects")["projects"]))
ok("opening it afterwards is a refusal, not a half-loaded page",
   get_maybe("/api/projects/load?name=" + copy["name"]).get("error") == "not found")
ok("deleting it twice is a refusal, not a traceback",
   post_maybe("/api/projects/delete", {"name": copy["name"]}).get("error")
   == "not found")
ok("a name cannot climb out of the store",
   post_maybe("/api/projects/delete",
              {"name": "../../pipeline_studio3"}).get("error") == "not found")
ok("and nothing outside the store was touched",
   (Path(S.__file__)).is_file())

# What the test put on the shelf, and in the trash behind it, comes back off. Swept by
# prefix rather than by the two names it knows about: a run that dies halfway through
# the copies above would otherwise leave one on the shelf forever.
for rec in S.STORE.list():
    if rec["name"].startswith(saved["name"]):
        post_maybe("/api/projects/delete", {"name": rec["name"]})
for f in S.STORE.trash.glob(f"{saved['name']}*.json"):
    try:
        f.unlink()
    except OSError:
        pass

srv.shutdown()
print("\n" + ("HTTP TESTS PASSED" if not FAIL else f"HTTP TESTS FAILED: {FAIL}"))
sys.exit(1 if FAIL else 0)
