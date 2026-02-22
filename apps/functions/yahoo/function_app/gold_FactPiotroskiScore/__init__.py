#sys.path.insert(0, 'C:/Users/chris/Documents/GitHub/AzureFunctions/AzureFunctions')
import datetime
import logging
import time
import os
import pandas as pd
import azure.functions as func
import numpy as np

from shared_code import utils
from io import BytesIO
from datetime import date

def main(mytimer: func.TimerRequest) -> None:

    azure_utils = utils.AzureUtils()
    dataCleaning_utils = utils.DataCleaning()
    
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)
    
    fact_piotroski = azure_utils.download_parquet_blob(f"gold/factpiotroski", f"fact_piotroski.parquet").drop_duplicates()
    
    df_piotroski = azure_utils.ingest_bronze_data(f"PiotroskiScore/").drop_duplicates()
    
    fact_piotroski = fact_piotroski.set_index(["Ticker", "Date"])
    df_piotroski = df_piotroski.set_index(["Ticker", "Date"])
    fact_piotroski = pd.concat([df_piotroski[~df_piotroski.index.isin(fact_piotroski.index)], fact_piotroski]).reset_index()

    parquet_file = fact_piotroski.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, f"gold/factpiotroski", f"fact_piotroski.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
