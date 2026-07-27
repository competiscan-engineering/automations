"""
Extends build_query() to pull everything needed for the trend-report Excel,
and provides complete_row() / complete_rows() to map the SQL result rows
into the exact Excel column schema.

CONFIRMED:
  - Panelist junction table: csv2_product.cscan_panelists_product
    (PK: panelist_id, productID, ppdate). State/Income come from
    cscan_panelists directly (ppstateID/pincomeID here are just lookup
    FKs, not usable values).
  - cscan_sector is a confirmed 3-level tree via parentID:
      Sector (parentID = 0) -> Category (parentID = a Sector) ->
      Sub Category (parentID = a Category, whose own parentID <> 0).
    e.g. Mortgage & Loan (6, parentID 0)
         -> Secured (183, parentID 6)
            -> Mortgage (16, parentID 183), Reverse Mortgage (203, ...)

STILL OPEN:
  1. "Market" / "Publication" / "Network Name" -- no source table identified
     anywhere in the schema shared so far -> left blank until confirmed.
"""

from typing import Any


# Junction table confirmed: csv2_product.cscan_panelists_product
# (columns include productID, panelist_id, ppdate). State/Income live on
# cscan_panelists itself (they're empty on this junction table per Hernan),
# so we join through to cscan_panelists for those. Age is pulled from
# cscan_panelists.age for consistency; ppage on the junction table itself
# is the alternative if that's preferred instead.
PANELIST_LINK_TABLE = "csv2_product.cscan_panelists_product"
PANELIST_LINK_PRODUCT_COL = "productID"  # confirmed via SHOW COLUMNS
PANELIST_LINK_PANELIST_COL = "panelist_id"


def build_query(entry_ids: list[str]) -> str:
    """
    Genera la query SQL con todos los entry_id que se pasen en la lista,
    incluyendo toda la informacion necesaria para el reporte Excel.
    """
    ids_str = ", ".join(f"'{eid}'" for eid in entry_ids)

    query = f"""
    SELECT
        p.entry_id,
        p.product_id,
        p.product_name,
        p.product_headline,
        p.added_to_database,

        mc.mChannelName AS media_channel,
        mp.mPanelName AS audience,
        mt.mTypeName AS mailing_type,

        d.bucket_name,
        d.document_path,
        d.document_filename,

        co.companyName AS primary_company,

        -- Additional companies (everything NOT flagged as the primary/default image)
        (
            SELECT GROUP_CONCAT(DISTINCT co2.companyName ORDER BY co2.companyName SEPARATOR '||')
            FROM csv2_product.cscan_product_company_mapping cmap2
            JOIN csv2_master_lookup.cscan_company co2
                ON co2.companyID = cmap2.company_id
            WHERE cmap2.product_id = p.product_id
              AND cmap2.default_img = 0
        ) AS additional_companies,

        -- Primary Sector (top level, parentID = 0)
        (
            SELECT GROUP_CONCAT(DISTINCT s.sectorName ORDER BY s.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm
            JOIN csv2_master_lookup.cscan_sector s
                ON s.sectorID = psm.sector_id
            WHERE psm.product_id = p.product_id
              AND s.parentID = 0
        ) AS sectors,

        -- ASSUMPTION 2: Category = 2nd level of the sector hierarchy
        (
            SELECT GROUP_CONCAT(DISTINCT s2.sectorName ORDER BY s2.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm2
            JOIN csv2_master_lookup.cscan_sector s2
                ON s2.sectorID = psm2.sector_id
            JOIN csv2_master_lookup.cscan_sector top
                ON top.sectorID = s2.parentID AND top.parentID = 0
            WHERE psm2.product_id = p.product_id
              AND s2.parentID <> 0
        ) AS categories,

        -- ASSUMPTION 2: Sub Category = 3rd level of the sector hierarchy
        (
            SELECT GROUP_CONCAT(DISTINCT s3.sectorName ORDER BY s3.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm3
            JOIN csv2_master_lookup.cscan_sector s3
                ON s3.sectorID = psm3.sector_id
            JOIN csv2_master_lookup.cscan_sector mid
                ON mid.sectorID = s3.parentID AND mid.parentID <> 0
            WHERE psm3.product_id = p.product_id
        ) AS sub_categories,

        -- ASSUMPTION 3 — UNCONFIRMED: Sub Sub Category = a 4th level of the
        -- sector hierarchy. Only 3 levels were confirmed above (Sector ->
        -- Category -> Sub Category); this has NOT been verified against the
        -- live schema. If cscan_sector doesn't actually nest this deep, this
        -- will just come back NULL/empty for every row — check that live
        -- before trusting this column.
        (
            SELECT GROUP_CONCAT(DISTINCT s4.sectorName ORDER BY s4.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm4
            JOIN csv2_master_lookup.cscan_sector s4
                ON s4.sectorID = psm4.sector_id
            JOIN csv2_master_lookup.cscan_sector subcat
                ON subcat.sectorID = s4.parentID AND subcat.parentID <> 0
            JOIN csv2_master_lookup.cscan_sector cat
                ON cat.sectorID = subcat.parentID AND cat.parentID <> 0
            WHERE psm4.product_id = p.product_id
        ) AS sub_sub_categories,

        p.is_prescreen,
        p.refinance,
        p.jumbo_ncnfg,
        p.va,
        p.fha,
        p.conventional,
        p.usda,
        p.socialmedia_adtype,

        -- ASSUMPTION 1: state/age/income via a panelist junction table
        (
            SELECT GROUP_CONCAT(DISTINCT pan.state SEPARATOR '||')
            FROM {PANELIST_LINK_TABLE} link
            JOIN csv2_master_lookup.cscan_panelists pan
                ON pan.panelist_id = link.{PANELIST_LINK_PANELIST_COL}
            WHERE link.{PANELIST_LINK_PRODUCT_COL} = p.product_id
        ) AS states,
        (
            SELECT GROUP_CONCAT(DISTINCT pan2.age SEPARATOR '||')
            FROM {PANELIST_LINK_TABLE} link2
            JOIN csv2_master_lookup.cscan_panelists pan2
                ON pan2.panelist_id = link2.{PANELIST_LINK_PANELIST_COL}
            WHERE link2.{PANELIST_LINK_PRODUCT_COL} = p.product_id
        ) AS ages,
        (
            SELECT GROUP_CONCAT(DISTINCT pan3.income SEPARATOR '||')
            FROM {PANELIST_LINK_TABLE} link3
            JOIN csv2_master_lookup.cscan_panelists pan3
                ON pan3.panelist_id = link3.{PANELIST_LINK_PANELIST_COL}
            WHERE link3.{PANELIST_LINK_PRODUCT_COL} = p.product_id
        ) AS incomes

    FROM csv2_product.cscan_document d
    JOIN csv2_product.cscan_product p ON p.product_id = d.productID
    JOIN csv2_product.cscan_product_company_mapping cmap ON cmap.product_id = p.product_id
    JOIN csv2_master_lookup.cscan_company co ON co.companyID = cmap.company_id
    JOIN csv2_master_lookup.cscan_mchannel mc ON mc.mChannelID = p.mchanne_id
    JOIN csv2_master_lookup.cscan_mpanel mp ON mp.mPanelID = p.mpanel_id
    LEFT JOIN csv2_master_lookup.cscan_mtype mt ON mt.mTypeID = p.mtype_id

    WHERE p.entry_id IN ({ids_str}) AND cmap.default_img = 1;
    """
    return query.strip()


def _entry_id_to_quarter(entry_id: str) -> str:
    """entry_id looks like YYYY-MM-DD-NNNN -> '2026 Q3'."""
    try:
        year, month = entry_id.split("-")[0], entry_id.split("-")[1]
        q = (int(month) - 1) // 3 + 1
        return f"{year} Q{q}"
    except (ValueError, IndexError):
        return ""


def _mortgage_loan_application_type(row: dict[str, Any]) -> str:
    flags = {
        "Refinance": row.get("refinance"),
        "Jumbo/Non-Conforming": row.get("jumbo_ncnfg"),
        "VA": row.get("va"),
        "FHA": row.get("fha"),
        "Conventional": row.get("conventional"),
        "USDA": row.get("usda"),
    }
    active = [name for name, val in flags.items() if val]
    return ", ".join(active)


def complete_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    Maps one SQL result row (as returned by build_query, as a dict) into the
    exact Excel column schema.
    """
    has_pdf = bool(row.get("bucket_name") and row.get("document_path"))

    return {
        "Primary Company": row.get("primary_company") or "",
        "Additional Companies": (row.get("additional_companies") or "N/A").replace("||", ", "),
        "Primary Sector": (row.get("sectors") or "").split("||")[0] if row.get("sectors") else "",
        "Primary Category": (row.get("categories") or "").split("||")[0] if row.get("categories") else "",
        "Primary Sub Category": (row.get("sub_categories") or "").split("||")[0] if row.get("sub_categories") else "",
        "Primary Sub Sub Category": (row.get("sub_sub_categories") or "").split("||")[0] if row.get("sub_sub_categories") else "",
        "EntryID": row.get("entry_id") or "",
        "Quarter": _entry_id_to_quarter(row.get("entry_id", "")),
        "Headline": row.get("product_headline") or "",
        "Product": row.get("product_name") or "",
        "PDF Content": "PDF Content" if has_pdf else "",
        "Media Channel": row.get("media_channel") or "",
        "State/Province": (row.get("states") or "").replace("||", ", "),
        "Age": (row.get("ages") or "").replace("||", ", "),
        "Income": (row.get("incomes") or "").replace("||", ", "),
        "Mailing Type": row.get("mailing_type") or "",
        "Pre-Screen": "Yes" if row.get("is_prescreen") else "No",
        "Mortgage & Loan - Application Type": _mortgage_loan_application_type(row),
        "Publication": "",  # TODO: not present in shared schema — print media only?
        "Network Name": "",  # TODO: likely relevant only for Online Video/Social
        "Social Media Ad Type": row.get("socialmedia_adtype") or "",
    }


def complete_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bulk version of complete_row for a full result set."""
    return [complete_row(r) for r in rows]


if __name__ == "__main__":
    # Fake raw SQL row, shaped like build_query()'s output, using the
    # SoFi example from the Excel to sanity-check the mapping.
    fake_row = {
        "entry_id": "2026-07-10-3448",
        "product_id": 999999,
        "product_name": "SoFi Refinance Mailer",
        "product_headline": (
            "How refinancing could help you reach your goals? You don't "
            "have to wait for the perfect time to save money. Start "
            "exploring a mortgage refinance today"
        ),
        "media_channel": "Email",
        "audience": "Consumer",
        "mailing_type": None,
        "bucket_name": "cscan-pdfs",
        "document_path": "2026/07/10/",
        "document_filename": "sofi_refi.pdf",
        "primary_company": "SoFi",
        "additional_companies": None,
        "sectors": "Mortgage & Loan",
        "categories": "Secured",
        "sub_categories": "Mortgage",
        "is_prescreen": 0,
        "refinance": 1,
        "jumbo_ncnfg": 0,
        "va": 0,
        "fha": 0,
        "conventional": 0,
        "usda": 0,
        "socialmedia_adtype": None,
        "states": None,
        "ages": None,
        "incomes": None,
    }

    import json
    print(json.dumps(complete_row(fake_row), indent=2))
