import datetime
import logging
import time
import azure.functions as func
import pandas as pd

from datetime import date, timedelta
from shared_code import utils


def main(mytimer: func.TimerRequest) -> None:
    
    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()

    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-booli')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    storage_account = azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)
    
    utils_DataFactory = utils.DataFactory()
    data_cleaning = utils_DataFactory.get_formatter("Cleaning")

    columns_to_drop = ["floor", "soldPriceAbsoluteDiff", "soldPricePercentageDiff", "listPrice", "rooms", "soldSqmPrice", "livingArea", "Unnamed: 0"]
    non_numeric_columns = ["streetAddress", "objectType", "descriptiveAreaName", "soldPriceType", "soldDate", "url", "__typename"]

    df_cleaning = (
        azure_utils.ingest_raw_data(f"Sold_")
        .pipe(data_cleaning.drop_dataframe_columns, columns_to_drop)
        .pipe(data_cleaning.set_dtype_to_numeric, non_numeric_columns)
        .pipe(data_cleaning.change_timestamp_format, "soldDate")
    )
    
    parquet_file = df_cleaning.to_parquet(index = False)

    azure_utils.upload_csv_to_datalake(parquet_file, "silver/sold", f"Sold_{date.today()}.parquet")   
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
