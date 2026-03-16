#sys.path.insert(0, 'C:/Users/chris/Documents/GitHub/AzureFunctions/AzureFunctions')
import datetime
import logging
import time
from datetime import date
import os
from io import BytesIO
import pandas as pd

import azure.functions as func
from shared_code import utils

def main(mytimer: func.TimerRequest) -> None:

    azure_utils = utils.AzureUtils()
    dataCleaning_utils = utils.DataCleaning()
    
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)
    
    fact_incomeStatement = azure_utils.download_parquet_blob(f"gold/factincomestatement", f"fact_incomeStatement.parquet").drop_duplicates()
    
    df_IncomeStatement = azure_utils.ingest_bronze_data(f"IncomeStatement/").drop_duplicates()
    
    fact_incomeStatement = fact_incomeStatement.set_index(["Ticker", "asOfDate"])
    df_IncomeStatement = df_IncomeStatement.set_index(["Ticker", "asOfDate"])
    fact_incomeStatement = pd.concat([df_IncomeStatement[~df_IncomeStatement.index.isin(fact_incomeStatement.index)], fact_incomeStatement]).reset_index()
    
    parquet_file = fact_incomeStatement.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, "gold/factincomestatement", "fact_incomeStatement.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
