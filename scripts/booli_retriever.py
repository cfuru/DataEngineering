#!/usr/bin/env python3
"""Standalone Booli sold-properties retriever.

Queries the Booli GraphQL API, accumulates paginated results, and
merges them into a local Delta Parquet table (delta-rs / no Spark needed).

Usage:
    python scripts/booli_retriever.py

Environment variables:
    BOOLI_AREA_IDS      Comma-separated area IDs  (default: "1")
    BOOLI_LOOKBACK_DAYS Lookback window in days    (default: 31)
    BOOLI_OUTPUT_DIR    Delta table path           (default: data/booli/delta/sold)
"""

import json
import logging
import os
import time
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Booli GraphQL client
# ---------------------------------------------------------------------------

GRAPHQL_URL = "https://www.booli.se/graphql"
HEADERS = {
    "authority": "www.booli.se",
    "accept": "*/*",
    "accept-language": "sv,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "api-client": "booli.se",
    "content-type": "application/json",
    "origin": "https://www.booli.se",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.77"
    ),
}

SOLD_QUERY = (
    "query searchSold($input: SearchRequest) { search: searchSold(input: $input) {"
    "  pages totalCount result {"
    "    booliId soldPrice{raw} rent{raw} streetAddress constructionYear"
    "    floor{raw} soldSqmPrice{raw} soldPriceAbsoluteDiff{raw}"
    "    soldPricePercentageDiff{raw} listPrice{raw} firstPrice{raw}"
    "    livingArea{raw} additionalArea{raw} rooms{raw}"
    "    objectType descriptiveAreaName soldPriceType"
    "    daysActive soldDate latitude longitude url"
    "    operatingCost{raw} tenureForm plotArea{raw}"
    "    apartmentNumber{raw} mapImage created soldPriceSource"
    "    source{name id type} agent{name}"
    "    energyClass{__typename} housingCoopId housingCoop{name id}"
    "    areas{name id type} __typename"
    "  } __typename } }"
)


def _post(payload: dict) -> dict:
    response = requests.post(
        GRAPHQL_URL, data=json.dumps(payload), headers=HEADERS, timeout=30
    )
    if response.status_code == 200:
        return response.json()
    raise RuntimeError(
        f"GraphQL query failed: {response.status_code} — {response.text[:200]}"
    )


def fetch_sold(area_id: int, min_sold_date: str) -> list:
    """Fetch all pages of sold listings for one area."""
    records = []
    page = 1
    total_pages = None

    while True:
        payload = {
            "operationName": "searchSold",
            "variables": {
                "input": {
                    "filters": [{"key": "minSoldDate", "value": min_sold_date}],
                    "areaId": str(area_id),
                    "sort": "created",
                    "page": page,
                    "ascending": False,
                }
            },
            "query": SOLD_QUERY,
        }
        data = _post(payload)
        search = data["data"]["search"]

        if total_pages is None:
            total_pages = search["pages"]
            log.info(
                "area=%s  total_pages=%d  total_count=%d",
                area_id,
                total_pages,
                search["totalCount"],
            )

        results = search.get("result") or []
        records.extend(results)
        log.info(
            "  fetched page %d/%d (%d records so far)", page, total_pages, len(records)
        )

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    return records


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

# After pd.json_normalize, nested GraphQL objects appear as "<field>.raw" or
# "<field>.<subfield>". Map them to the final column names used in the Delta table.
RENAME = {
    "soldPrice.raw": "soldPrice",
    "rent.raw": "rent",
    "floor.raw": "floor",
    "soldSqmPrice.raw": "soldSqmPrice",
    "soldPriceAbsoluteDiff.raw": "soldPriceAbsoluteDiff",
    "soldPricePercentageDiff.raw": "soldPricePercentageDiff",
    "listPrice.raw": "listPrice",
    "firstPrice.raw": "firstPrice",
    "livingArea.raw": "livingArea",
    "additionalArea.raw": "additionalArea",
    "rooms.raw": "rooms",
    "operatingCost.raw": "operatingCost",
    "plotArea.raw": "plotArea",
    "apartmentNumber.raw": "apartmentNumber",
    "source.name": "brokerFirm",
    "source.id": "brokerFirmId",
    "source.type": "brokerFirmType",
    "agent.name": "agentName",
    "housingCoop.name": "housingCoopName",
    "housingCoop.id": "housingCoopIdStr",
    "energyClass.__typename": "energyClass",
    "__typename": "typeName",
}

# All columns kept in the Delta table (order defines the schema)
KEEP_COLUMNS = [
    "booliId", "streetAddress", "constructionYear", "objectType",
    "descriptiveAreaName", "soldPriceType", "daysActive", "soldDate",
    "latitude", "longitude", "url", "typeName", "soldPrice", "rent",
    "floor", "soldSqmPrice", "soldPriceAbsoluteDiff", "soldPricePercentageDiff",
    "listPrice", "firstPrice", "livingArea", "additionalArea", "rooms",
    "operatingCost", "plotArea", "apartmentNumber", "tenureForm",
    "created", "soldPriceSource", "housingCoopId", "brokerFirm", "brokerFirmId",
    "agentName", "housingCoopName", "energyClass", "ingest_date",
]

# Explicit PyArrow schema keeps Delta types stable across runs
SCHEMA = pa.schema([
    pa.field("booliId",                 pa.int64(),   nullable=True),
    pa.field("streetAddress",           pa.string(),  nullable=True),
    pa.field("constructionYear",        pa.int64(),   nullable=True),
    pa.field("objectType",              pa.string(),  nullable=True),
    pa.field("descriptiveAreaName",     pa.string(),  nullable=True),
    pa.field("soldPriceType",           pa.string(),  nullable=True),
    pa.field("daysActive",              pa.int64(),   nullable=True),
    pa.field("soldDate",                pa.string(),  nullable=True),
    pa.field("latitude",                pa.float32(), nullable=True),
    pa.field("longitude",               pa.float32(), nullable=True),
    pa.field("url",                     pa.string(),  nullable=True),
    pa.field("typeName",                pa.string(),  nullable=True),
    pa.field("soldPrice",               pa.int64(),   nullable=True),
    pa.field("rent",                    pa.int64(),   nullable=True),
    pa.field("floor",                   pa.float32(), nullable=True),
    pa.field("soldSqmPrice",            pa.float32(), nullable=True),
    pa.field("soldPriceAbsoluteDiff",   pa.float32(), nullable=True),
    pa.field("soldPricePercentageDiff", pa.float32(), nullable=True),
    pa.field("listPrice",               pa.int64(),   nullable=True),
    pa.field("firstPrice",              pa.int64(),   nullable=True),
    pa.field("livingArea",              pa.float32(), nullable=True),
    pa.field("additionalArea",          pa.float32(), nullable=True),
    pa.field("rooms",                   pa.float32(), nullable=True),
    pa.field("operatingCost",           pa.float32(), nullable=True),
    pa.field("plotArea",                pa.float32(), nullable=True),
    pa.field("apartmentNumber",         pa.string(),  nullable=True),
    pa.field("tenureForm",              pa.string(),  nullable=True),
    pa.field("created",                 pa.string(),  nullable=True),
    pa.field("soldPriceSource",         pa.string(),  nullable=True),
    pa.field("housingCoopId",           pa.int64(),   nullable=True),
    pa.field("brokerFirm",              pa.string(),  nullable=True),
    pa.field("brokerFirmId",            pa.string(),  nullable=True),
    pa.field("agentName",               pa.string(),  nullable=True),
    pa.field("housingCoopName",         pa.string(),  nullable=True),
    pa.field("energyClass",             pa.string(),  nullable=True),
    pa.field("ingest_date",             pa.string(),  nullable=False),
])


def records_to_arrow(records: list, ingest_date: str) -> pa.Table:
    """Normalize, rename, and cast Booli API records to a typed PyArrow table."""
    df = pd.json_normalize(records)
    df = df.rename(columns=RENAME)
    df["ingest_date"] = ingest_date

    int_cols = [
        "booliId", "constructionYear", "daysActive",
        "soldPrice", "rent", "listPrice", "firstPrice", "housingCoopId",
    ]
    float_cols = [
        "latitude", "longitude", "floor", "soldSqmPrice",
        "soldPriceAbsoluteDiff", "soldPricePercentageDiff",
        "livingArea", "additionalArea", "rooms", "operatingCost", "plotArea",
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # Ensure all expected columns are present (fill missing with null)
    for col in KEEP_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[KEEP_COLUMNS]

    return pa.Table.from_pandas(df, schema=SCHEMA, safe=False)


# ---------------------------------------------------------------------------
# Delta writer
# ---------------------------------------------------------------------------

def merge_into_delta(table: pa.Table, output_path: str) -> None:
    """Upsert records into a local Delta table, keyed on booliId."""
    from deltalake import DeltaTable, write_deltalake

    if not DeltaTable.is_deltatable(output_path):
        log.info("Creating new Delta table at %s", output_path)
        write_deltalake(output_path, table, schema=SCHEMA)
        log.info("Delta table created with %d rows", len(table))
        return

    dt = DeltaTable(output_path)
    before = dt.to_pyarrow_dataset().count_rows()
    log.info(
        "Merging %d incoming records into existing Delta table (%d rows)",
        len(table),
        before,
    )
    (
        dt.merge(
            source=table,
            predicate="target.booliId = source.booliId",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    after = DeltaTable(output_path).to_pyarrow_dataset().count_rows()
    log.info("Merge complete — %d rows (+%d new)", after, after - before)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    area_ids = [
        int(x.strip())
        for x in os.environ.get("BOOLI_AREA_IDS", "1").split(",")
    ]
    lookback_days = int(os.environ.get("BOOLI_LOOKBACK_DAYS", "31"))
    output_dir = os.environ.get("BOOLI_OUTPUT_DIR", "data/booli/delta/sold")

    min_sold_date = (date.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    ingest_date = date.today().strftime("%Y-%m-%d")

    log.info(
        "Starting Booli retriever — areas=%s  lookback=%d days  min_sold_date=%s",
        area_ids,
        lookback_days,
        min_sold_date,
    )

    all_records = []
    for area_id in area_ids:
        log.info("Fetching area_id=%d", area_id)
        records = fetch_sold(area_id, min_sold_date)
        all_records.extend(records)
        log.info("area_id=%d done — %d records", area_id, len(records))

    if not all_records:
        log.warning("No records retrieved — nothing to write")
        return

    log.info("Total records: %d — converting to Arrow", len(all_records))
    table = records_to_arrow(all_records, ingest_date)
    os.makedirs(output_dir, exist_ok=True)
    merge_into_delta(table, output_dir)
    log.info("Done.")


if __name__ == "__main__":
    main()
