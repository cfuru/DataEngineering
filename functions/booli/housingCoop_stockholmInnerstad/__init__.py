import datetime
import logging
import time
import azure.functions as func
import pandas as pd

from datetime import date, timedelta
from shared_code import utils


def main(mytimer: func.TimerRequest) -> None:
    object_type = "Lägenhet"
    currentDateTime = datetime.datetime.utcnow() - timedelta(days=365)
    minSoldDate = currentDateTime.strftime("%Y-%m-%d")
    maxSoldDate = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    area_id = 143

    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-booli')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)

    booli_utils = utils.Booli()

    # Collect unique housingCoopIds from sold properties in the last year
    data = booli_utils.run_query_sold(
        object_type, minSoldDate, maxSoldDate, "", "", "", "", area_id, 1
    )
    total_number_of_pages = data["data"]["search"]["pages"]

    housing_coop_ids = set()
    for page in range(1, total_number_of_pages + 1):
        data = booli_utils.run_query_sold(
            object_type, minSoldDate, maxSoldDate, "", "", "", "", area_id, page
        )
        for obj in data["data"]["search"]["result"]:
            coop_id = obj.get("housingCoopId")
            if coop_id:
                housing_coop_ids.add(coop_id)

        logging.info(f"Scanned page {page}/{total_number_of_pages}, unique coops so far: {len(housing_coop_ids)}")
        time.sleep(0.5)

    logging.info(f"Found {len(housing_coop_ids)} unique housing cooperatives")

    # Fetch details for each housing cooperative
    res = []
    for i, coop_id in enumerate(housing_coop_ids, 1):
        try:
            coop_data = booli_utils.run_query_housing_coop(coop_id)
            coop = coop_data["data"]["housingCoop"]
            if coop:
                res.append(coop)
            logging.info(f"Fetched coop {i}/{len(housing_coop_ids)}: {coop_id}")
        except Exception as e:
            logging.error(f"Failed to fetch coop {coop_id}: {e}")
        time.sleep(0.5)

    df = pd.json_normalize(res)

    azure_utils.upload_csv_to_datalake(
        df, "raw/housing_coop", f"HousingCoop_{area_id}_{date.today()}.csv"
    )

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
