import datetime
import logging
import time
import azure.functions as func
import pandas as pd

from datetime import date, timedelta
from shared_code import utils

def main(mytimer: func.TimerRequest) -> None:
    object_type = "Lägenhet"
    currentDateTime = datetime.datetime.utcnow() - timedelta(days = 31)
    currentDate = currentDateTime.strftime("%Y-%m-%d")
    minSoldDate = currentDate
    maxSoldDate = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    rooms = ""
    hasBalcony = ""
    hasFireplace = ""
    hasElevator = ""
    area_id = 143
    
    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-booli')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    storage_account = azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)

    booli_utils = utils.Booli()

    data = booli_utils.run_query_sold(object_type, minSoldDate, maxSoldDate, rooms, hasBalcony, hasFireplace, hasElevator, area_id, 1)
    total_number_of_pages = data["data"]["search"]["pages"]
    total_number_of_objects = data["data"]["search"]["totalCount"]
    total_number_of_pages

    res = []
    for page in range(1, total_number_of_pages + 1):
        print(f'Scraping page: {page} / {total_number_of_pages}')
        data = booli_utils.run_query_sold(object_type, minSoldDate, maxSoldDate, rooms, hasBalcony, hasFireplace, hasElevator, area_id, page)
        
        for object in data["data"]["search"]["result"]:
            res.append(object)
        
        print(f"Total number of objects stored {len(res)} / {total_number_of_objects}")
        print("")
        print("--------------------------------------------")
        print("")
        
    df = pd.json_normalize(res)
    
    azure_utils.upload_csv_to_datalake(df, "raw/sold/all", f"Sold_{object_type}_{rooms}_{area_id}_{date.today()}.csv")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
