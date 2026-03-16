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
    
    fact_priceTarget = azure_utils.download_parquet_blob(f"gold/factpricetarget", f"fact_priceTarget.parquet").drop_duplicates()
    
    df_FinancialData = azure_utils.ingest_bronze_data(f"FinancialData/").drop_duplicates()
    df_priceTargets = df_FinancialData[["currentPrice", "numberOfAnalystOpinions", "recommendationKey", "recommendationMean", "targetLowPrice", "targetMeanPrice", "targetMedianPrice"]].reset_index()
    df_priceTargets = df_priceTargets.rename(columns =
            {
                "index": "Ticker"
            }
        )
    df_priceTargets["observationDate"] = date.today()
    
    fact_priceTarget = fact_priceTarget.set_index(["Ticker", "ObservationDate"])
    df_priceTargets = df_priceTargets.set_index(["Ticker", "ObservationDate"])
    fact_priceTarget = pd.concat([df_priceTargets[~df_priceTargets.index.isin(df_priceTargets.index)], fact_priceTarget]).reset_index()
    
    parquet_file = df_priceTargets.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, "gold/factpricetarget", "fact_priceTarget.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
