/* studio_dom_test.js — load the Studio page in a real DOM and drive it.
 *
 *   python pipelines/pipeline_studio3.py        # in one terminal
 *   npm install jsdom && node pipelines/studio_dom_test.js
 *
 * Point it elsewhere with STUDIO_BASE=http://127.0.0.1:8798.
 *
 *
 * node --check proves the script parses. It does not prove that boot() completes, that
 * every render path survives contact with an actual project, or that the panel draws.
 * Those are runtime failures that take the whole page down and are invisible to Python.
 */
const { JSDOM, VirtualConsole } = require("jsdom");

const BASE = process.env.STUDIO_BASE || "http://127.0.0.1:8788";
const fails = [];
const errors = [];
function ok(label, cond, extra) {
  console.log(`  ${cond ? "PASS" : "FAIL"}  ${label}${extra ? "  — " + extra : ""}`);
  if (!cond) fails.push(label);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const html = await (await fetch(BASE + "/")).text();

  const vc = new VirtualConsole();
  vc.on("jsdomError", (e) => errors.push(String(e.message || e)));
  vc.on("error", (...a) => errors.push(a.join(" ")));

  const dom = new JSDOM(html, {
    url: BASE + "/",
    runScripts: "dangerously",
    pretendToBeVisual: true,
    virtualConsole: vc,
    // jsdom ships no fetch, and the page's own boot() runs the moment the script is
    // parsed. Install it before that rather than after, so the page boots exactly once
    // and by itself.
    beforeParse(window) {
      window.fetch = (u, o) => fetch(new URL(u, BASE).href, o);
      window.confirm = () => true;
      window.alert = () => {};
    },
  });
  const w = dom.window;
  Object.defineProperty(w.navigator, "clipboard",
    { value: { writeText: () => Promise.resolve() }, configurable: true });
  for (let i = 0; i < 80 && !w.document.querySelector("#sections .sec"); i++) {
    await sleep(100);
  }
  const $ = (s) => w.document.querySelector(s);

  console.log("\n--- the page boots ---");
  ok("no uncaught errors during boot", errors.length === 0, errors.slice(0, 3).join(" | "));
  ok("the button says Run", $("#runBtn").textContent.trim() === "Run",
     JSON.stringify($("#runBtn").textContent));
  const modes = [...$("#mode").options].map((o) => o.text);
  ok("exactly four modes, in order, with the agreed wording",
     JSON.stringify(modes) === JSON.stringify(["Run the searches", "Run the workbook",
       "Run and edit the deliverables", "Run the pipeline"]), modes.join(" | "));
  ok("the mode help text is written for the researcher",
     $("#modeHelp").textContent.length > 30, $("#modeHelp").textContent.slice(0, 60));
  ok("the settings pane rendered", $("#settings").children.length > 5);
  ok("section cards rendered", $("#sections .sec") !== null);
  ok("Send to Eng. is still on the bar",
     [...w.document.querySelectorAll("#top button")].some(
       (b) => b.textContent.includes("Send to Eng.")));

  console.log("\n--- the badge ---");
  for (let i = 0; i < 40 && !$("#badge").textContent.trim(); i++) await sleep(100);
  ok("a fresh report reads as Draft", $("#badge").textContent.trim() === "Draft",
     $("#badge").textContent.trim());
  ok("the badge carries an explanation on hover",
     ($("#badge .badge").getAttribute("title") || "").length > 20);

  console.log("\n--- the output terminal starts minimised, every time ---");
  ok("starts minimised", $("#log").classList.contains("min"));
  ok("the control offers to show it", $("#logTog").textContent === "show");
  w.eval("toggleLog()");
  ok("it opens when asked", !$("#log").classList.contains("min"));
  ok("and then offers to put it away", $("#logTog").textContent === "minimise");
  w.eval("restoreLog()");
  ok("a reload puts it back, whatever it was left as",
     $("#log").classList.contains("min"));
  ok("the choice is deliberately not carried across page loads",
     w.localStorage.getItem("rs.log") === null, w.localStorage.getItem("rs.log"));
  w.eval("openLog()");
  ok("starting a run expands it again", !$("#log").classList.contains("min"));

  console.log("\n--- the results panel ---");
  ok("hidden until there is something to show", $("#panel").classList.contains("hide"));
  w.eval(`PANEL={run_id:"aabbccddeeff",start:"2026-04-01",end:"2026-04-30",files:[
    {name:"Report.pptx",size:2048}],sections:[{id:"s1",title:"Checking",feature:true,
    archive_total:1200,at_least:true,kept:38,shown:38,reasoning:"",count:3,
    pieces:[{entry_id:"2026-04-02-1111",company:"Northgate",channel:"Email",
      date:"2026-04-02",headline:"Earn 4.35% APY",product:"",pdf_url:"http://x/y.pdf"},
      {entry_id:"2026-04-03-1112",company:"Harbor",channel:"Direct Mail",
      date:"2026-04-03",headline:"Open an account",product:"",pdf_url:""}],
    picks:[]}]};PSTATE={};openPanel();renderPanel();`);
  ok("the panel opens", !$("#panel").classList.contains("hide"));
  ok("it groups pieces under their section",
     $("#panel .psec h4").textContent.includes("Checking"));
  ok('a capped section is marked "at least"',
     $("#panel .atleast") !== null && $("#panel .psec h4").textContent.includes("at least"),
     $("#panel .psec h4").textContent.replace(/\s+/g, " ").trim());
  ok("every retrieved piece is listed", $$(w, "#panel .piece").length >= 2);
  ok("entry_ids are individually copyable", $("#panel .eid") !== null);
  ok("and copyable per section",
     [...w.document.querySelectorAll("#panel button")].some(
       (b) => b.textContent.includes("copy all")));
  ok("a piece with no product id gets no fake link",
     $("#panel").innerHTML.includes("no link"));
  ok("finished files are offered for download",
     $("#panel .dl a") !== null && $("#panel .dl a").getAttribute("href")
       .includes("/api/run/file?id=aabbccddeeff"));

  console.log("\n--- the same panel, in its approve / reject state ---");
  w.eval(`PANEL.sections[0].picks=PANEL.sections[0].pieces.slice();
    PSTATE={s1:{slate:PANEL.sections[0].picks.slice(),ok:{},rejected:[],exhausted:""}};
    renderPanel();`);
  ok("each pick offers Keep, Swap and By ID",
     [...w.document.querySelectorAll("#panel .yn button")].length === 6,
     [...w.document.querySelectorAll("#panel .yn button")]
       .map((b) => b.textContent.trim()).join(" | "));
  ok("the three read as Keep / Swap / By ID, capitalised, Swap still first",
     [...w.document.querySelectorAll("#panel .piece .yn")][0].textContent
       .replace(/\s+/g, " ").trim() === "Keep Swap By ID",
     [...w.document.querySelectorAll("#panel .piece .yn")][0].textContent
       .replace(/\s+/g, " ").trim());
  const confirmBtn = [...w.document.querySelectorAll("#panel button")].find((b) =>
    b.textContent.includes("Build the deck"));
  ok("the confirm action exists", !!confirmBtn);
  ok("it is disabled until every piece is settled", confirmBtn.disabled);
  ok("it counts what is settled out of what is needed",
     $("#panel .warnbox").textContent.includes("0 of 2 pieces settled"),
     $("#panel .warnbox").textContent.replace(/\s+/g, " ").trim().slice(0, 40));
  w.eval(`approve("s1","2026-04-02-1111");approve("s1","2026-04-03-1112");`);
  const confirm2 = [...w.document.querySelectorAll("#panel button")].find((b) =>
    b.textContent.includes("Build the deck"));
  ok("settling every piece enables it", !confirm2.disabled);
  ok("and it says so", $("#panel .okbox") !== null);

  console.log("\n--- a thumbnail beside every piece ---");
  ok("each piece has a picture frame",
     $$(w, "#panel .piece .thumb").length >= 2,
     $$(w, "#panel .piece .thumb").length);
  ok("before anything is known it shows a placeholder, not a broken image",
     $("#panel .thumb .none") !== null && $("#panel .thumb img") === null);
  // What the fetch would have said: one piece has a cover, the archive has none for
  // the other. Only those two are answers about a piece.
  w.eval(`THUMBS={"2026-04-02-1111":"ok","2026-04-03-1112":"none"};renderPanel();`);
  const img = $("#panel .thumb img");
  ok("a piece with a cover image renders it", img !== null,
     img && img.getAttribute("src"));
  ok("the image is served from the Studio, not from a bucket the browser cannot read",
     img.getAttribute("src").startsWith("/api/thumb?id="),
     img.getAttribute("src"));
  ok("it is lazy, so a long list does not fetch a screenful it never shows",
     img.getAttribute("loading") === "lazy");
  ok("a piece the archive has no image for says so rather than showing a broken frame",
     $("#panel").innerHTML.includes("no image"));
  ok("clicking a thumbnail enlarges it", $("#panel .thumb").getAttribute("onclick")
     .includes("zoom("));
  w.eval(`zoom("/api/thumb?id=x&entry_id=y")`);
  ok("the enlarged view opens", $("#lightbox").classList.contains("show"));
  ok("and closes again", (() => {
    $("#lightbox").classList.remove("show");
    return !$("#lightbox").classList.contains("show");
  })());

  console.log("\n--- a fetch that failed is not the archive saying there is no image ---");
  // The bug this covers: a shut tunnel used to be written down as false and rendered
  // as "no image on file" for the rest of the run, on pieces that plainly did have a
  // picture. It only corrected itself if the piece happened to be swapped, because
  // that is what asked a second time.
  w.eval(`THUMBS={"2026-04-02-1111":"retry","2026-04-03-1112":"none"};
    THUMBTRY={"2026-04-02-1111":1};renderPanel();`);
  const frames = $$(w, "#panel .piece .thumb");
  ok("the failed one does not claim the archive has no image",
     !frames[0].textContent.includes("no image"),
     frames[0].textContent.replace(/\s+/g, " ").trim());
  ok("it says the load failed instead",
     frames[0].textContent.replace(/\s+/g, " ").includes("load"),
     frames[0].textContent.replace(/\s+/g, " ").trim());
  ok("and offers to try again", frames[0].classList.contains("bad")
     && (frames[0].getAttribute("onclick") || "").includes("retryThumb"));
  ok("the piece the archive really has no image for still says so",
     frames[1].textContent.includes("no image"),
     frames[1].textContent.replace(/\s+/g, " ").trim());
  ok("the panel header offers to retry every failed picture at once",
     [...w.document.querySelectorAll("#panel .phead button")].some(
       (b) => b.textContent.toLowerCase().includes("retry")),
     [...w.document.querySelectorAll("#panel .phead button")]
       .map((b) => b.textContent.trim()).join(" | "));
  ok("a failed fetch is counted as trouble, a genuinely imageless piece is not",
     w.eval("thumbTrouble()") === 1, w.eval("thumbTrouble()"));
  ok("only the failed one is queued to be asked again",
     w.eval(`JSON.stringify(thumbsPending())`) === '["2026-04-02-1111"]',
     w.eval(`JSON.stringify(thumbsPending())`));
  // A retry clears the verdict so the piece is asked about from scratch.
  w.eval(`retryThumb("2026-04-02-1111")`);
  ok("retrying forgets the failure rather than remembering it",
     w.eval(`THUMBS["2026-04-02-1111"]===undefined`));
  ok("a piece with tries left is never left saying no image on file",
     !$("#panel .piece .thumb").textContent.includes("no image"));
  // An <img> the browser cannot draw is the same kind of event.
  w.eval(`THUMBS={"2026-04-02-1111":"ok"};renderPanel();`);
  ok("a rendered image reports its own failure to draw",
     ($("#panel .thumb img").getAttribute("onerror") || "").includes("thumbBroke"));
  w.eval(`thumbBroke("2026-04-02-1111")`);
  ok("and that turns it into a retry, not a no-image",
     w.eval(`THUMBS["2026-04-02-1111"]`) === "retry",
     w.eval(`THUMBS["2026-04-02-1111"]`));
  w.eval(`THUMBS={"2026-04-02-1111":"ok","2026-04-03-1112":"none"};
    THUMBTRY={};renderPanel();`);

  console.log("\n--- By ID: naming the piece you want, and seeing it first ---");
  // The lookup reads the run's own state file, because the panel only ever receives
  // the first 300 rows of a section and an id copied out of the workbook is very
  // often one the browser has never seen. So the test writes a state file.
  writeFixtureRun();
  w.eval(`RUNID=${JSON.stringify(FIXTURE_RUN)}`);

  ok("the dialog is shut until By ID is pressed",
     !$("#ovById").classList.contains("show"));
  w.eval(`openById("s1","2026-04-02-1111")`);
  ok("By ID opens a dialog rather than wedging a box into the row",
     $("#ovById").classList.contains("show") && $("#byIdEid") !== null);
  ok("it says which section and which slot is being replaced",
     $("#byIdWhere").textContent.includes("Checking")
     && $("#byIdWhere").textContent.includes("slot 1 of 2"),
     $("#byIdWhere").textContent.trim());
  ok("it shows the piece that would leave the slide",
     $("#ovById .byidrow.out .co").textContent.includes("Northgate"),
     $("#ovById .byidrow.out .co").textContent.trim());
  ok("nothing can be committed before a piece is named", $("#byIdGo").disabled);
  ok("and it says what to do", $("#ovById .byidmsg.idle") !== null);

  // The input must survive typing. It is not part of what gets redrawn, because
  // re-creating it per keystroke sends the caret to the end of the line and makes
  // correcting the middle of a pasted id impossible.
  const boxBefore = $("#byIdEid");
  w.eval(`byIdTyped("2026-04-20-9999")`);
  await sleep(60);
  ok("typing does not replace the input element",
     $("#byIdEid") === boxBefore);
  $("#byIdEid").value = "2026-04-20-9999";
  $("#byIdEid").setSelectionRange(5, 5);
  w.eval(`byIdTyped("2026-04-20-9999")`);
  await sleep(60);
  ok("and it leaves the caret where the researcher put it",
     $("#byIdEid").selectionStart === 5, $("#byIdEid").selectionStart);
  w.eval(`byIdTyped("")`);
  await sleep(60);

  // Too short to be an id: no request, no complaint yet.
  w.eval(`byIdTyped("2026")`);
  await sleep(120);
  ok("a half-typed id is not looked up and not called wrong",
     $("#ovById .byidmsg.idle") !== null && $("#byIdGo").disabled);

  // The piece already in the slot, and one already on the slate: both answered
  // locally, without troubling the archive.
  w.eval(`byIdTyped("2026-04-02-1111")`);
  await until(() => ($("#ovById .byidmsg.bad") || {}).textContent
    .includes("already in this slot"));
  ok("naming the piece already in the slot is refused",
     ($("#ovById .byidmsg.bad") || {}).textContent.includes("already in this slot"),
     ($("#ovById .byidmsg.bad") || {}).textContent);
  w.eval(`byIdTyped("2026-04-03-1112")`);
  await until(() => ($("#ovById .byidmsg.bad") || {}).textContent
    .includes("already on this slate"));
  ok("a piece already on the slate is refused",
     ($("#ovById .byidmsg.bad") || {}).textContent.includes("already on this slate"),
     ($("#ovById .byidmsg.bad") || {}).textContent);
  ok("still nothing to commit", $("#byIdGo").disabled);

  // An id this run never fetched. The build phase would drop it silently, so the
  // dialog has to be the thing that says so.
  w.eval(`byIdTyped("1999-01-01-0001")`);
  await until(() => ($("#ovById .byidmsg.bad") || {}).textContent
    .includes("not among"));
  ok("an id this run never fetched is refused, and the count is named",
     ($("#ovById .byidmsg.bad") || {}).textContent.includes("not among the 4 piece"),
     ($("#ovById .byidmsg.bad") || {}).textContent.slice(0, 110));
  ok("the slate was not touched",
     w.eval(`PSTATE.s1.slate[0].entry_id`) === "2026-04-02-1111");

  // Real, but in a different section of the same report.
  w.eval(`byIdTyped("2026-05-05-5555")`);
  await until(() => ($("#ovById .byidmsg.bad") || {}).textContent
    .includes("different section"));
  ok("an id from another section is refused, and says where it actually is",
     ($("#ovById .byidmsg.bad") || {}).textContent.includes("Savings"),
     ($("#ovById .byidmsg.bad") || {}).textContent.slice(0, 130));

  // A real id from this section's records, past the row the panel was ever sent.
  w.eval(`byIdTyped("2026-04-20-9999")`);
  await until(() => $("#ovById .byidrow.in") !== null);
  ok("a valid id previews the actual piece, before anything is committed",
     $("#ovById .byidrow.in") !== null
     && $("#ovById .byidrow.in .co").textContent.includes("Cascadia"),
     ($("#ovById .byidrow.in .co") || {}).textContent);
  ok("the preview carries the headline, which is what makes it checkable",
     $("#ovById .byidrow.in .hl").textContent.includes("5.10%"),
     $("#ovById .byidrow.in .hl").textContent.trim().slice(0, 60));
  ok("the preview shows a picture frame, not just text",
     $("#ovById .byidrow.in .thumb") !== null);
  ok("only now can it be committed", !$("#byIdGo").disabled);
  ok("the slate is still untouched until it is",
     w.eval(`PSTATE.s1.slate[0].entry_id`) === "2026-04-02-1111");

  // Commit.
  w.eval(`useById()`);
  ok("committing replaces the whole row on the slate",
     w.eval(`PSTATE.s1.slate[0].entry_id`) === "2026-04-20-9999",
     w.eval(`PSTATE.s1.slate[0].entry_id`));
  ok("with every field the panel draws, not just an id",
     w.eval(`PSTATE.s1.slate[0].company`) === "Cascadia Credit Union"
     && w.eval(`PSTATE.s1.slate[0].date`) === "2026-04-20",
     w.eval(`PSTATE.s1.slate[0].company + " / " + PSTATE.s1.slate[0].date`));
  ok("the row in the panel is the new piece",
     $("#panel .piece .co").textContent.includes("Cascadia"),
     $("#panel .piece .co").textContent.trim());
  ok("the dialog closes", !$("#ovById").classList.contains("show"));
  ok("the new row is flashed so the change is visible",
     $('#panel .piece[data-eid="2026-04-20-9999"]') !== null);
  ok("it needs settling like any other piece",
     w.eval(`PSTATE.s1.ok["2026-04-20-9999"]===undefined`));
  ok("the displaced piece is remembered, so it can be put back",
     w.eval(`PSTATE.s1.swapped["2026-04-20-9999"].entry_id`) === "2026-04-02-1111",
     w.eval(`PSTATE.s1.swapped["2026-04-20-9999"].entry_id`));
  ok("and the panel offers to put it back",
     $("#panel .swapped") !== null
     && $("#panel .swapped").textContent.includes("put it back"));
  ok("a displaced piece is not treated as rejected, so a swap may still offer it",
     w.eval(`JSON.stringify(PSTATE.s1.rejected)`) === "[]",
     w.eval(`JSON.stringify(PSTATE.s1.rejected)`));

  // One-per-company is the report's rule; naming a piece outright is the override.
  // It is warned about in the dialog BEFORE committing, and again in the log after.
  w.eval(`PANEL.sections[0].one_per_company=true;
    openById("s1","2026-04-03-1112");byIdTyped("2026-04-21-9998")`);
  await until(() => $("#ovById .byidwarn") !== null);
  ok("a pick that would break one-per-company is warned about before committing",
     $("#ovById .byidwarn").textContent.includes("one piece per company")
     && $("#ovById .byidwarn").textContent.includes("Cascadia"),
     $("#ovById .byidwarn").textContent.replace(/\s+/g, " ").trim().slice(0, 100));
  ok("but it is still allowed, because naming a piece IS the override",
     !$("#byIdGo").disabled);
  w.eval(`useById()`);
  ok("and it goes on the slate", w.eval(`PSTATE.s1.slate[1].entry_id`)
     === "2026-04-21-9998");
  ok("with the rule it broke said out loud in the log too",
     $("#log").textContent.includes("one piece per company"),
     ($("#log").textContent.match(/[^\n]*one piece per company[^\n]*/) || [""])[0]
       .trim().slice(0, 110));

  console.log("\n--- Escape and Cancel leave the slate alone ---");
  const before = w.eval(`PSTATE.s1.slate.map(c=>c.entry_id).join(",")`);
  w.eval(`openById("s1","2026-04-20-9999");byIdTyped("2026-04-02-1111")`);
  w.eval(`closeById()`);
  ok("cancelling changes nothing",
     w.eval(`PSTATE.s1.slate.map(c=>c.entry_id).join(",")`) === before
     && !$("#ovById").classList.contains("show"));
  w.eval(`openById("s1","2026-04-20-9999")`);
  w.eval(`byIdKey({key:"Escape",preventDefault(){}})`);
  ok("Escape closes it", !$("#ovById").classList.contains("show"));

  w.eval(`unswap("s1","2026-04-20-9999");unswap("s1","2026-04-21-9998")`);
  ok("putting them back restores both originals",
     w.eval(`PSTATE.s1.slate.map(c=>c.entry_id).join(",")`)
       === "2026-04-02-1111,2026-04-03-1112",
     w.eval(`PSTATE.s1.slate.map(c=>c.entry_id).join(",")`));
  removeFixtureRun();
  w.eval(`RUNID="aabbccddeeff";PSTATE.s1.ok={"2026-04-02-1111":true,
    "2026-04-03-1112":true};renderPanel();`);

  console.log("\n--- a swap can be undone ---");
  w.eval(`PSTATE.s1.slate[0]={entry_id:"2026-04-09-2222",company:"Cascadia",
      channel:"Email",date:"2026-04-09",headline:"New rate",product:"",pdf_url:""};
    PSTATE.s1.swapped={"2026-04-09-2222":PANEL.sections[0].pieces[0]};
    PSTATE.s1.rejected=["2026-04-02-1111"];
    delete PSTATE.s1.ok["2026-04-02-1111"];renderPanel();`);
  ok("the swapped-in piece says what it replaced",
     $("#panel .swapped") !== null
     && $("#panel .swapped").textContent.includes("Northgate"),
     $("#panel .swapped") && $("#panel .swapped").textContent.replace(/\s+/g, " ").trim());
  const back = [...w.document.querySelectorAll("#panel .swapped button")][0];
  ok("and offers to put it back", !!back && back.textContent.includes("put it back"));
  w.eval(`unswap("s1","2026-04-09-2222")`);
  ok("undoing restores the original piece",
     w.eval(`PSTATE.s1.slate[0].entry_id`) === "2026-04-02-1111",
     w.eval(`PSTATE.s1.slate[0].entry_id`));
  ok("the original is no longer treated as rejected, so it is eligible again",
     w.eval(`JSON.stringify(PSTATE.s1.rejected)`) === "[]",
     w.eval(`JSON.stringify(PSTATE.s1.rejected)`));
  ok("and it needs settling again before the deck can be built",
     w.eval(`PSTATE.s1.ok["2026-04-02-1111"]===undefined`));
  ok("the undo note is gone once it is undone", $("#panel .swapped") === null);

  console.log("\n--- the review gets the room: a bigger panel, bigger pictures ---");
  const css = await (await fetch(BASE + "/")).text();
  ok("the results panel is far wider than the old 480px",
     /#panel\{width:min\(58vw,900px\);min-width:520px/.test(css));
  ok("and it can take the whole window",
     /#panel\.full\{width:auto/.test(css)
     && /#body\.panelfull #pane,#body\.panelfull #paneTab,#body\.panelfull #stage/.test(css));
  ok("the thumbnail is big enough to tell two envelopes apart",
     /\.piece \.thumb\{width:136px;height:176px/.test(css));
  ok("and bigger again at full width",
     /#panel\.full \.piece \.thumb\{width:200px;height:258px/.test(css));
  ok("the piece text grew with it", /\.piece\{[^}]*font-size:14px/.test(css));
  ok("the sections column gives up about a fifth of its width",
     /\.wrapper\{max-width:720px/.test(css));

  console.log("\n--- full width, and back again ---");
  w.eval("togglePanelFull()");
  ok("the panel takes the window", $("#panel").classList.contains("full")
     && $("#body").classList.contains("panelfull"));
  ok("the header offers the way back",
     $("#panel .phead").textContent.toLowerCase().includes("exit full width"),
     $("#panel .phead").textContent.replace(/\s+/g, " ").trim());
  w.eval("togglePanelFull()");
  ok("and gives it back", !$("#panel").classList.contains("full")
     && !$("#body").classList.contains("panelfull"));
  w.eval("togglePanelFull();hidePanel()");
  ok("hiding the panel while full does not leave an empty window",
     !$("#body").classList.contains("panelfull")
     && !$("#pane").classList.contains("hide"));
  w.eval("openPanel()");

  console.log("\n--- the report pane minimises like everything else ---");
  ok("it starts open", !$("#pane").classList.contains("hide"));
  ok("its heading carries the control",
     [...w.document.querySelectorAll("#settings button")].some(
       (b) => b.textContent.trim() === "minimise"));
  w.eval("hidePane()");
  ok("it folds away", $("#pane").classList.contains("hide"));
  ok("and leaves a strip to bring it back", $("#paneTab").classList.contains("show")
     && $("#paneTab").textContent.includes("THE REPORT"));
  w.eval("togglePane()");
  ok("which does bring it back", !$("#pane").classList.contains("hide")
     && !$("#paneTab").classList.contains("show"));

  console.log("\n--- the ladder sits next to the button that climbs it ---");
  ok("the mode select is the element immediately before Run",
     $("#mode").nextElementSibling === $("#runBtn"),
     ($("#mode").nextElementSibling || {}).id);
  const bar = [...$("#runbar").children].map((e) => e.id || e.className);
  ok("both are at the right-hand end of the run bar",
     bar.indexOf("mode") > bar.indexOf("modehelp")
     && bar.indexOf("mode") > bar.indexOf("runEmail"), bar.join(" | "));

  console.log("\n--- a one-off date range in the editor ---");
  w.eval(`setWin("mode","range");`);
  await sleep(400);
  ok("two date fields appear",
     w.document.querySelectorAll('#settings input[type="date"]').length === 2);
  ok('and a "make it recurring" action alongside them',
     [...w.document.querySelectorAll("#settings button")].some(
       (b) => b.textContent.includes("Make it recurring")));
  w.eval(`setWin("start","2026-04-01");setWin("end","2026-06-30");`);
  await sleep(600);
  ok("the fixed window is warned about in the editor",
     $("#gen").textContent.includes("fixed window"),
     $("#gen").textContent.replace(/\s+/g, " ").slice(0, 70));

  console.log("\n--- Send to Eng. cannot be walked past with a fixed window ---");
  w.eval("openExport()");
  ok("the warning is shown",
     $("#exportBody .warnbox") !== null
     && $("#exportBody").textContent.includes("fixed date range"));
  ok("the send button is blocked until it is acknowledged", $("#exportGo").disabled);
  ok("and it offers to fix it instead",
     $("#exportBody").textContent.includes("repeating cadence"));
  const ack = $("#ackFixed");
  ack.checked = true;
  ack.dispatchEvent(new w.Event("change"));
  ok("acknowledging it unblocks the send", !$("#exportGo").disabled);
  w.eval("hide('Export')");

  console.log("\n--- the Projects modal ---");
  w.eval("openProjects()");
  await sleep(700);
  const heads = [...w.document.querySelectorAll("#ovProjects h2")].map((h) =>
    h.textContent.replace(/\s+/g, " ").trim());
  ok("templates and saved reports are separately labelled",
     heads.length >= 2 && heads[0].startsWith("Templates to start from")
     && heads[1].startsWith("Saved reports to return to"), heads.join(" / "));
  ok("six or more templates are offered as cards",
     w.document.querySelectorAll("#tplList .tsec").length >= 6,
     w.document.querySelectorAll("#tplList .tsec").length);
  ok("every saved report shows a badge",
     [...w.document.querySelectorAll("#savedList li")].every(
       (li) => li.querySelector(".badge")),
     w.document.querySelectorAll("#savedList li").length + " saved");
  ok("and says when it was last saved",
     [...w.document.querySelectorAll("#savedList li")].every(
       (li) => /saved /.test(li.querySelector(".meta2").textContent)),
     w.document.querySelector("#savedList .meta2").textContent);
  ok("every row offers Open, Copy and Delete",
     [...w.document.querySelectorAll("#savedList li")].every(
       (li) => [...li.querySelectorAll("button")].map((b) => b.textContent).join()
         === "Open,Copy,Delete"),
     [...w.document.querySelectorAll("#savedList li button")].slice(0, 3)
       .map((b) => b.textContent).join());
  // Delete is only safe to press if the list says which row is the one on screen.
  const firstSaved = w.eval("SAVED.length ? SAVED[0].name : ''");
  w.eval(`P.status={sent:null,runs:[],saved_as:${JSON.stringify(firstSaved)}}`);
  await w.eval("refreshSaved()");
  await sleep(200);
  ok("the report open on screen is the one marked in the list",
     w.document.querySelectorAll("#savedList li.here").length === 1
     && w.document.querySelector("#savedList li.here b").textContent
          .includes("open now"),
     w.document.querySelectorAll("#savedList li.here").length + " marked");
  w.eval('P.status={sent:null,runs:[],saved_as:""}');
  await w.eval("refreshSaved()");
  await sleep(200);

  console.log("\n--- opening a template gives a new unsaved report ---");
  w.eval('newFrom("categories")');
  await sleep(900);
  ok("the sections came from the template",
     w.document.querySelectorAll("#sections .sec").length === 4,
     w.document.querySelectorAll("#sections .sec").length);
  ok("it is unsaved — nothing points back at a file on disk",
     w.eval("JSON.stringify((P.status||{}).saved_as||'')") === '""');
  ok("the template itself was not modified",
     (await (await fetch(BASE + "/api/template?name=categories")).json())
       .project.sections.length === 4);

  console.log("\n--- run history ---");
  w.eval(`P.status={sent:null,saved_as:"",runs:[
    {id:"aabbccddeeff",mode:"full",at:"2026-08-25T10:00:00",rc:0,stopped:false,
     emailed:false,produced:["Old_Report.pptx"]}]};`);
  await w.eval("openHistory()");
  await sleep(900);
  ok("the run is listed", $("#historyBody").textContent.includes("Run the pipeline"));
  ok("a run whose files are gone says so rather than offering a dead link",
     $("#historyBody").textContent.includes("no longer on disk"),
     $("#historyBody").textContent.replace(/\s+/g, " ").slice(0, 90));

  console.log("\n--- confirming the picks does not reopen the pool ---");
  /* The bug this covers: confirmPicks emptied PSTATE and nothing took its place, so
     renderPanel fell back to sec.pieces. A panel narrowed to two pieces reopened as
     every row the run had retrieved — hundreds of them — at the moment the deck was
     being built from two. */
  const pool = [];
  for (let i = 0; i < 300; i++) {
    pool.push({ entry_id: "2026-04-01-" + (2000 + i), company: "Pool " + i,
                media_channel: "Direct Mail", search_date: "2026-04-01",
                product_headline: "A row nobody chose", product: "Checking",
                pdf_url: "" });
  }
  const chosen = pool.slice(0, 2);
  w.eval(`PANEL={run_id:"aabbccddeeff",start:"2026-04-01",end:"2026-04-30",files:[],
    sections:[{id:"s1",title:"Checking",feature:true,count:2,archive_total:300,
      kept:300,shown:300,reasoning:"the two biggest issuers",
      pieces:${JSON.stringify(pool)},picks:${JSON.stringify(chosen)}},
     {id:"s2",title:"Workbook only",feature:false,count:0,archive_total:40,
      kept:40,shown:40,reasoning:"",pieces:${JSON.stringify(pool.slice(0, 40))},
      picks:[]}]};
    PSTATE={s1:{slate:${JSON.stringify(chosen)},ok:{},rejected:[],exhausted:""}};
    PSTATE.s1.ok[${JSON.stringify(chosen[0].entry_id)}]=true;
    PSTATE.s1.ok[${JSON.stringify(chosen[1].entry_id)}]=true;
    BUILT=null;BUILDING=false;renderPanel();openPanel();`);
  /* Scoped to the featured section: at the pause a workbook-only section does still
     list what it retrieved, which is fine — nothing there was ever up for approval. */
  const featured = () => [...$$(w, "#panel .psec")[0]
    .querySelectorAll(".piece[data-eid]")];
  ok("at the pause the featured section shows the slate, not the pool",
     featured().length === 2, featured().length + " rows");
  ok("and offers to build from them",
     /Build the deck from these 2 piece/.test($("#panel").textContent));

  /* Pressing the button, without letting the request finish: what is on screen the
     instant the pool used to come back. */
  w.eval(`BUILT={s1:PSTATE.s1.slate.slice()};PSTATE={};BUILDING=true;renderPanel();`);
  const rows = $$(w, "#panel .piece[data-eid]");
  ok("building shows the two pieces it is building from, not the 300",
     rows.length === 2, rows.length + " rows");
  ok("and every row is one that was actually chosen",
     rows.every((e) => chosen.some((c) => c.entry_id === e.getAttribute("data-eid"))),
     rows.map((e) => e.getAttribute("data-eid")).join(", "));
  ok("none of the pool leaked back in",
     !$("#panel").textContent.includes("A row nobody chose"));
  ok("the panel is greyed out while it builds",
     $("#panel").classList.contains("building"));
  ok("it says what is happening", /Building the deck from the 2 piece/
     .test($("#panel").textContent.replace(/\s+/g, " ")),
     $("#panel").textContent.replace(/\s+/g, " ").slice(0, 130));
  ok("a section that was never featured is summarised, not listed",
     /Nothing here goes on a slide/.test($("#panel").textContent.replace(/\s+/g, " ")));
  ok("and the way to fetch 300 more pictures is gone with it",
     !$("#panel").textContent.includes("load more pictures"));

  w.eval("BUILDING=false;renderPanel()");
  ok("when it finishes the grey comes off", !$("#panel").classList.contains("building"));
  ok("the picks stay on screen afterwards",
     $$(w, "#panel .piece[data-eid]").length === 2
     && !$("#panel").textContent.includes("A row nobody chose"),
     $$(w, "#panel .piece[data-eid]").length + " rows");
  ok("and it says what the deck was built from",
     /Built from these 2 piece/.test($("#panel").textContent.replace(/\s+/g, " ")),
     $("#panel").textContent.replace(/\s+/g, " ").slice(0, 130));

  /* A second run has to start clean, or the next pause would open on the last run's
     build banner. */
  w.eval("BUILT=null;BUILDING=false;PSTATE={};renderPanel()");
  ok("a fresh run leaves no build behind it",
     !/Built from these/.test($("#panel").textContent)
     && !$("#panel").classList.contains("building"));
  w.eval("killPanel();PANEL=null;BUILT=null;BUILDING=false;PSTATE={}");

  console.log("\n--- the deliverables drawer ---");
  /* A drawer on the right edge beside the results strip, not a line in the terminal
     and not a band above it. Driven directly here because reaching it through a real
     run would need the archive; what is under test is that a list of files becomes
     something clickable — after every mode, not only the one that pauses. */
  w.eval(`RUNID="aabbccddeeff";showFiles([
    {name:"Acme_Report_20260430.pptx",size:284160,kind:"pptx"},
    {name:"Acme_Data_20260430.xlsx",size:7603,kind:"xlsx"},
    {name:"Acme_Report_20260430.slides.json",size:665,kind:"json"}],
    RUNID,"Emailed too. Kept under History for this report.");`);
  const shelf = $("#deliv");
  const strip = $("#delivTab");
  ok("a finished run opens the drawer", !shelf.classList.contains("hide"));
  ok("the strip that brings it back sits at the right edge, after RESULTS",
     strip.previousElementSibling === shelf
     && shelf.previousElementSibling.id === "panel"
     && strip.parentElement.id === "body");
  ok("the strip reads vertically", strip.textContent.trim() === "DELIVERABLES");
  const links = $$(w, "#deliv a");
  ok("one download per file, plus the one that takes all of them",
     links.length === 4, links.length + " links");
  ok("the zip is offered first, before the files it holds",
     links[0].getAttribute("href") === "/api/run/zip?id=aabbccddeeff"
     && /Download all \(3\)/.test(links[0].textContent), links[0].textContent);
  ok("each of the rest points at that run's own file",
     links.slice(1).every((a) => a.getAttribute("href")
       .startsWith("/api/run/file?id=aabbccddeeff&name=")),
     links[1] && links[1].getAttribute("href"));
  ok("and every one asks the browser to save rather than navigate",
     links.every((a) => a.hasAttribute("download")));
  ok("a deck is labelled a deck and a workbook a workbook",
     shelf.textContent.includes("deck") && shelf.textContent.includes("workbook"),
     shelf.textContent.replace(/\s+/g, " ").slice(0, 130));
  ok("sizes are readable rather than raw bytes",
     shelf.textContent.includes("278 KB") && shelf.textContent.includes("7 KB")
     && !shelf.textContent.includes("284160"),
     shelf.textContent.replace(/\s+/g, " ").slice(0, 170));
  ok("it says where the files went and where they stay",
     shelf.textContent.includes("Emailed too")
     && shelf.textContent.includes("History"));

  w.eval("toggleDeliv()");
  ok("it folds away to the strip", shelf.classList.contains("hide")
     && strip.classList.contains("show"));
  w.eval("toggleDeliv()");
  ok("and the strip brings it back", !shelf.classList.contains("hide"));

  /* The reason it is not in the terminal: clearing the terminal is a thing people do
     while a run is going, and it used to take the only link to the deck with it. */
  await w.eval("clearLog()");
  ok("clearing the output does not take the downloads with it",
     $$(w, "#deliv a").length === 4 && $("#log").textContent.trim() === "");

  w.eval(`showFiles([{name:"Acme_Data_20260430.xlsx",size:7603,kind:"xlsx"}],
    RUNID,"Written so far — the deck follows once you confirm the picks.");`);
  ok("a single file is offered on its own, with no zip",
     $$(w, "#deliv a").length === 1,
     $$(w, "#deliv a").map((a) => a.textContent.replace(/\s+/g, " ")).join(" | "));
  ok("and a paused run says the rest is still coming",
     shelf.textContent.includes("the deck follows"));

  w.eval("clearFiles()");
  ok("the next run starts with no drawer and no strip",
     shelf.classList.contains("hide") && shelf.innerHTML === ""
     && !strip.classList.contains("show"));

  /* Reopening the report is the other half: History has always had the files, but
     behind a modal, and the usual ask is the last run's deck right now. A run
     directory of this test's own, because the shared fixture is torn down above and
     a real one is pruned as new runs arrive. */
  const PAST_RUN = "eeeeeeeeeeee";
  writeShelfRun(PAST_RUN);
  w.eval(`P.status={sent:null,saved_as:"",runs:[
    {id:"${PAST_RUN}",mode:"full",at:"2026-08-26T09:30:00",rc:0,stopped:false,
     emailed:false,produced:["fixture.txt"]}]};`);
  await w.eval("restoreFiles()");
  await sleep(600);
  ok("reopening a report puts its last run's files back within reach",
     $$(w, "#deliv a").length >= 1
     && $$(w, "#deliv a")[0].getAttribute("href").includes(PAST_RUN),
     shelf.textContent.replace(/\s+/g, " ").slice(0, 130));
  ok("but only lights the strip — it does not shove the window sideways",
     shelf.classList.contains("hide") && strip.classList.contains("show")
     && strip.classList.contains("ready"));
  ok("and says which run they came from",
     shelf.textContent.includes("Run the pipeline")
     && shelf.textContent.includes("2026-08-26 09:30"),
     shelf.textContent.replace(/\s+/g, " ").slice(-70));
  ok("without replaying that run's output into this session's terminal",
     !$("#log").textContent.includes("fixture.txt"));

  w.eval(`P.status={sent:null,saved_as:"",runs:[
    {id:"bbbbbbbbbbbb",mode:"full",at:"2026-08-25T10:00:00",rc:0,stopped:false,
     emailed:false,produced:["Gone.pptx"]}]};`);
  await w.eval("restoreFiles()");
  await sleep(600);
  ok("a run whose files were pruned leaves neither drawer nor strip",
     shelf.classList.contains("hide") && $$(w, "#deliv a").length === 0
     && !strip.classList.contains("show"));
  removeShelfRun(PAST_RUN);

  console.log("\n--- RESULTS belongs to the mode that stops for review ---");
  /* Every mode was leaving a RESULTS strip on the edge of the window. Three of the
     four have nothing behind it: the panel is where a slate is settled. */
  w.eval("PANEL={run_id:'aabbccddeeff',start:'',end:'',files:[],sections:[]};"
    + "PSTATE={};renderPanel();openPanel();");
  ok("the panel can be opened when there is something to show",
     !$("#panel").classList.contains("hide"));
  w.eval("killPanel()");
  ok("clearing it for a mode that has no review takes the strip too",
     $("#panel").classList.contains("hide")
     && !$("#panelTab").classList.contains("show")
     && $("#panel").innerHTML === "");
  w.eval("openPanel();hidePanel()");
  ok("whereas hiding it by hand does leave the strip to bring it back",
     $("#panel").classList.contains("hide")
     && $("#panelTab").classList.contains("show"));
  w.eval("killPanel()");

  console.log("\n--- nothing blew up along the way ---");
  ok("no uncaught page errors at all", errors.length === 0,
     errors.slice(0, 4).join(" | "));

  console.log("\n" + (fails.length ? "DOM TESTS FAILED: " + fails.join("; ")
    : "DOM TESTS PASSED"));
  process.exit(fails.length ? 1 : 0);
})().catch((e) => {
  console.error("HARNESS ERROR", e);
  process.exit(2);
});

function $$(w, s) {
  return [...w.document.querySelectorAll(s)];
}

// A run directory of this test's own, so the by-hand lookup has records to resolve
// against. Real run directories are pruned as new runs arrive, and a test that leans
// on one fails for reasons that have nothing to do with the code under test.
const FIXTURE_RUN = "dddddddddddd";

function fixtureDir() {
  return require("path").join(__dirname, "generated", "_runs", FIXTURE_RUN);
}

function writeFixtureRun() {
  const fs = require("fs");
  const dir = fixtureDir();
  fs.mkdirSync(dir, { recursive: true });
  const rec = (entry_id, company, date, headline, product_id) => ({
    entry_id, company, media_channel: "Direct Mail", search_date: date,
    product_headline: headline, product_name: "Checking", product_id,
    pdf_url: product_id ? "https://example.invalid/" + product_id : "",
  });
  fs.writeFileSync(require("path").join(dir, "state.json"), JSON.stringify({
    version: 1, period_label: "April 2026", start: "2026-04-01", end: "2026-04-30",
    sections: [
      {
        id: "s1", title: "Checking", tab: "Checking", feature: true, count: 2,
        one_per_company: true, never_reuse: true, archive_total: 3, kept: 3,
        picks: ["2026-04-02-1111", "2026-04-03-1112"],
        records: [
          rec("2026-04-02-1111", "Northgate", "2026-04-02", "Earn 4.35% APY", "p1"),
          rec("2026-04-03-1112", "Harbor", "2026-04-03", "Open an account", "p2"),
          // The two the panel would never have been sent, in a real 1,700-row section.
          rec("2026-04-20-9999", "Cascadia Credit Union", "2026-04-20",
              "Save and spend: 5.10% APY on Cascadia Elevate", "p3"),
          rec("2026-04-21-9998", "Cascadia Credit Union", "2026-04-21",
              "A second Cascadia piece, for the one-per-company rule", "p4"),
        ],
      },
      {
        id: "s2", title: "Savings & CDs", tab: "Savings", feature: true, count: 1,
        archive_total: 1, kept: 1, picks: [],
        records: [rec("2026-05-05-5555", "Summit", "2026-05-05", "13-month CD", "p9")],
      },
    ],
  }, null, 1));
  // The count in the refusal comes from the section's records, not from
  // archive_total: records is the list the build phase actually intersects the
  // approved ids against, so it is the only count that tells the researcher
  // anything useful about why their id was not accepted.
}

// A finished run with one file in it, for the deliverables bar to restore.
function writeShelfRun(id) {
  const fs = require("fs"), path = require("path");
  const out = path.join(__dirname, "generated", "_runs", id, "output");
  fs.mkdirSync(out, { recursive: true });
  fs.writeFileSync(path.join(out, "fixture.txt"), "a past run left this behind");
}

function removeShelfRun(id) {
  try {
    require("fs").rmSync(require("path").join(__dirname, "generated", "_runs", id),
                         { recursive: true, force: true });
  } catch (e) { /* a leftover fixture is pruned with the other runs */ }
}

function removeFixtureRun() {
  try {
    require("fs").rmSync(fixtureDir(), { recursive: true, force: true });
  } catch (e) { /* a leftover fixture is pruned with the other runs */ }
}

// Wait for a condition rather than for a fixed number of milliseconds.
async function until(fn, tries = 60, ms = 100) {
  for (let i = 0; i < tries; i++) {
    let hit = false;
    try { hit = !!fn(); } catch (e) { hit = false; }
    if (hit) return true;
    await new Promise((r) => setTimeout(r, ms));
  }
  return false;
}
