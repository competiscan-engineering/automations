#!/usr/bin/env python3
"""
thumbs.py — fetch the cover image of a piece, for the Studio's results panel
═══════════════════════════════════════════════════════════════════════════════════════

WHY THIS IS NOT JUST AN <img src>
    The platform API publishes exactly one URL per search row, pdf_url, and it is a
    PowerSearch page behind a login — an HTML page, not an asset. There is no public
    image URL on the API at all. The actual pages of a piece live in S3, and where they
    live has to be read out of the database:

        csv2_product.cscan_img_document      one row per rendered page, per piece
            img_document_default = 1         the cover — the one worth showing
            bucket_name / img_document_path / img_document_filename

    Two buckets are in play and they behave differently. The legacy one is world
    readable; the current one answers 403 to anyone without credentials. So a browser
    cannot be pointed at either of them and be relied on, and the Studio fetches the
    bytes itself and serves them from its own origin.

WHY IT IS A SEPARATE FILE
    Pipelines Studio is deliberately stdlib-only, so a researcher can start it with
    whatever `python` is on PATH. This needs boto3, pandas and paramiko. It therefore
    runs the same way a generated pipeline does — as a subprocess under the interpreter
    the Studio already resolved for that purpose — and the Studio itself imports nothing
    new.

RUN
    echo '{"entry_ids": ["2026-07-10-3448"], "out": "some/dir"}' | python pipelines/thumbs.py

    Reads one JSON object on stdin, writes <entry_id>.jpg into `out`, and prints a JSON
    manifest of what it managed to get. It never raises at the top level: a missing
    tunnel, a missing piece and a missing image are all normal, and the panel shows a
    placeholder for each of them.

    The manifest keeps two failure lists apart, because the panel says something
    different about each and one of them is a lie if they are merged:

        missing   the archive genuinely holds no cover image for this piece. Final.
        failed    the image exists but this attempt did not get it — a shut tunnel, a
                  403, a dropped read. Worth asking again.

    Everything that is not "missing" is retryable, so a piece that does have a picture
    is never labelled as one that does not.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "mcp"))

# One image per piece is plenty for a list, and the panel shows at most a screenful.
# The cap is here as well as in the Studio so a stray call cannot walk the archive.
MAX_IDS = 120

# The database still records the bucket under the name it had years ago. S3 has no
# bucket by that name, so an unnormalised value 404s on every single row.
BUCKET_ALIASES = {"competiscan.files": "competiscan-files"}


def _query(entry_ids: list[str]) -> str:
    ids = ", ".join("'" + e.replace("'", "''") + "'" for e in entry_ids)
    # The cover page only. img_document_default marks it; a piece that has none falls
    # back to its first page, which is what the sort column is for.
    return f"""
    SELECT p.entry_id,
           i.productID,
           i.img_document_default,
           i.img_document_sort,
           i.bucket_name,
           i.img_document_path,
           i.img_document_filename
    FROM csv2_product.cscan_product p
    JOIN csv2_product.cscan_img_document i ON i.productID = p.product_id
    WHERE p.entry_id IN ({ids})
      AND i.img_document_filename IS NOT NULL
    ORDER BY p.entry_id, i.img_document_default DESC, i.img_document_sort ASC;
    """.strip()


def main() -> int:
    try:
        req = json.loads(sys.stdin.read() or "{}")
    except ValueError as exc:
        print(json.dumps({"error": f"bad request: {exc}", "thumbs": {}}))
        return 0

    entry_ids = [str(e).strip() for e in (req.get("entry_ids") or []) if str(e).strip()]
    entry_ids = list(dict.fromkeys(entry_ids))[:MAX_IDS]
    out = Path(req.get("out") or ".")
    if not entry_ids:
        print(json.dumps({"thumbs": {}, "missing": []}))
        return 0
    out.mkdir(parents=True, exist_ok=True)

    # Neither of the next two failures says anything about whether a piece has a
    # picture, so neither one reports a missing piece. `failed` lists everything asked
    # for, which is what makes the panel ask again instead of claiming "no image".
    try:
        import boto3
        from mcp_serverv3 import _run_sql
    except Exception as exc:
        print(json.dumps({"error": f"cannot reach the archive: {exc}", "thumbs": {},
                          "missing": [], "failed": entry_ids}))
        return 0

    try:
        df = _run_sql(_query(entry_ids))
    except Exception as exc:
        # A closed tunnel is the ordinary case, not an incident. The panel simply
        # shows no pictures, says why, and tries again.
        print(json.dumps({"error": f"database unavailable: {exc}", "thumbs": {},
                          "missing": [], "failed": entry_ids}))
        return 0

    rows = [] if df is None or getattr(df, "empty", True) else df.to_dict("records")
    first: dict[str, dict] = {}
    for r in rows:                      # already ordered: cover first
        first.setdefault(str(r.get("entry_id")), r)

    try:
        s3 = boto3.client("s3")
    except Exception as exc:
        print(json.dumps({"error": f"no S3 credentials: {exc}", "thumbs": {},
                          "missing": [], "failed": entry_ids}))
        return 0

    thumbs, missing, failed, errors = {}, [], [], {}
    for eid in entry_ids:
        row = first.get(eid)
        if not row:
            # The join found no rendered page for this piece. That is a real answer.
            missing.append(eid)
            continue
        bucket = str(row.get("bucket_name") or "").strip()
        bucket = BUCKET_ALIASES.get(bucket, bucket)
        key = "/".join([str(row.get("img_document_path") or "").strip("/"),
                        str(row.get("img_document_filename") or "").strip("/")])
        if not bucket or not key:
            missing.append(eid)
            continue
        # The filename is not the researcher's to choose and is never joined onto a
        # path by the Studio — it serves these by entry_id, out of this one directory.
        dest = out / f"{eid}.jpg"
        # Downloaded beside the real name and moved into place, so a read that dies
        # halfway cannot leave a truncated file that the cache would then trust.
        tmp = out / f".{eid}.part"
        try:
            s3.download_file(bucket, key, str(tmp))
            if tmp.stat().st_size <= 0:
                raise OSError("empty object")
            tmp.replace(dest)
            thumbs[eid] = dest.name
        except Exception as exc:
            try:
                tmp.unlink()
            except OSError:
                pass
            # The row says the image exists, so this is a fetch that went wrong, not a
            # piece without a picture. Retryable.
            errors[eid] = f"{type(exc).__name__}"
            failed.append(eid)

    print(json.dumps({"thumbs": thumbs, "missing": missing, "failed": failed,
                      "errors": errors, "asked": len(entry_ids)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
