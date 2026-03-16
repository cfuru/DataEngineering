import datetime
import logging
import time
import os
import pandas as pd
import azure.functions as func

from datetime import date
from shared_code import utils
from io import BytesIO

def main(mytimer: func.TimerRequest) -> None:

    azure_utils = utils.AzureUtils()
    dataCleaning_utils = utils.DataCleaning()
    
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)
    
    fact_assetProfile = azure_utils.download_parquet_blob(f"gold/factassetprofile", f"fact_assetProfile.parquet").drop_duplicates()
    
    df_AssetProfile = azure_utils.ingest_bronze_data(f"AssetProfile/").drop_duplicates()
    
    df_AssetProfile = df_AssetProfile.rename(columns =
        {
            "index": "Ticker"
        }
    )
    
    fact_assetProfile = fact_assetProfile.set_index(["Ticker"])
    df_AssetProfile = df_AssetProfile.set_index(["Ticker"])
    fact_assetProfile = pd.concat([df_AssetProfile[~df_AssetProfile.index.isin(fact_assetProfile.index)], fact_assetProfile]).reset_index()
    
    parquet_file = fact_assetProfile.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, "gold/factassetprofile", "fact_assetProfile.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
