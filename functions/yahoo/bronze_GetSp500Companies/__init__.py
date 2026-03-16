#sys.path.insert(0, 'C:/Users/chris/Documents/GitHub/AzureFunctions/AzureFunctions')
import datetime
import logging
import time
import os
from io import BytesIO
from datetime import date
import pandas as pd

import azure.functions as func
from shared_code import utils

def main(mytimer: func.TimerRequest) -> None:
    try:
        azure_utils = utils.AzureUtils()
        secret_client = azure_utils.initialize_key_vault()
        
        sa_secret = os.getenv("AZURE_STORAGE_SECRET")#azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
        sa_name = os.getenv("AZURE_STORAGE_NAME")#azure_utils.get_key_vault_secret(secret_client, 'sa-name')
        connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")#azure_utils.get_key_vault_secret(secret_client, 'sa-conn-str')

        azure_utils.initialize_storage_account_ad(sa_secret, sa_name)
        azure_utils.initialize_queue_client(connect_str, "sp500-tickers")

        yahoo_utils = utils.yahooUtils()
        companies = yahoo_utils.scrape_sp500_comapnies()
        
        upload_companies_data(companies, azure_utils)
        
        utc_timestamp = datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc).isoformat()

        logging.info('Python timer trigger function ran at %s', utc_timestamp)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")

def upload_companies_data(companies, azure_utils):
    """Upload companies data to Azure Blob Storage and send company tickers to Azure Queue."""
    try:
        for ticker in companies.Symbol:
            azure_utils.send_queue_message(ticker)
        
        parquet_file = companies.to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, "bronze/companies/sp500", "companies_sp500.parquet")

    except Exception as e:
        logging.error(f'Error occurred while uploading companies data: {str(e)}')
