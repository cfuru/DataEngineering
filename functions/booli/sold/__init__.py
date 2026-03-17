import datetime
import json
import logging
import os
import time
import azure.functions as func
import pandas as pd

from datetime import date, timedelta
from shared_code import utils

DEFAULT_AREAS = [
    {"area_id": 1, "name": "stockholms_kommun"},
]


def main(mytimer: func.TimerRequest) -> None:
    areas = json.loads(os.environ.get("SOLD_AREAS", json.dumps(DEFAULT_AREAS)))
    lookback_days = int(os.environ.get("SOLD_LOOKBACK_DAYS", "31"))

    min_sold_date = (datetime.datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-booli')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)

    booli = utils.Booli()

    for area in areas:
        area_id = area["area_id"]
        area_name = area["name"]

        try:
            data = booli.run_query_sold(area_id, 1, min_sold_date)
            total_pages = data["data"]["search"]["pages"]
            total_count = data["data"]["search"]["totalCount"]
            logging.info(f"[{area_name}] Found {total_count} sold objects across {total_pages} pages")

            res = []
            for page in range(1, total_pages + 1):
                data = booli.run_query_sold(area_id, page, min_sold_date)
                res.extend(data["data"]["search"]["result"])
                logging.info(f"[{area_name}] Page {page}/{total_pages} — {len(res)}/{total_count} objects")
                time.sleep(0.5)

            if res:
                df = pd.json_normalize(res)
                filename = f"Sold_{area_name}_{area_id}_{date.today()}.csv"
                azure_utils.upload_csv_to_datalake(df, "raw/sold/all", filename)
                logging.info(f"[{area_name}] Uploaded {len(res)} objects to raw/sold/all/{filename}")
            else:
                logging.warning(f"[{area_name}] No results found")

        except Exception as e:
            logging.error(f"[{area_name}] Failed: {e}")

        time.sleep(1)

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s',
                 datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat())
