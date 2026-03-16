import datetime
import logging
import time
import azure.functions as func
import pandas as pd

from datetime import date
from shared_code import utils

def main(mytimer: func.TimerRequest) -> None:
    object_type = "Lägenhet"
    rooms = 3
    area_id = 143
    
    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-booli')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    storage_account = azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)

    booli_utils = utils.Booli()

    data = booli_utils.run_query_upcoming(object_type, rooms, area_id, 1)
    total_number_of_pages = data["data"]["search"]["pages"]
    total_number_of_pages
    
    res = []
    for page in range(1, total_number_of_pages + 1):
        data = booli_utils.run_query_upcoming(object_type, rooms, area_id, page)
        
        for object in data["data"]["search"]["result"]:
            res.append(object)

    df = pd.json_normalize(res)
    
    azure_utils.upload_csv_to_datalake(df, "raw/upcoming", f"Upcoming_{object_type}_{rooms}_{area_id}_{date.today()}.csv")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
