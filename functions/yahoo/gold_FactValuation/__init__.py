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
    
    fact_valuation = azure_utils.download_parquet_blob(f"gold/factvaluation", f"fact_valuation.parquet").drop_duplicates()
    dim_company = azure_utils.download_parquet_blob(f"gold/dimcompany", f"dim_company.parquet").drop_duplicates()
    
    df_ValuationMeasure = azure_utils.ingest_bronze_data(f"ValuationMeasure/").drop_duplicates()
    df_ValuationMeasure = df_ValuationMeasure.rename(columns =
        {
            "symbol": "Ticker"
        }
    )
    
    df_valuation = df_ValuationMeasure.merge(dim_company, on = "Ticker")
        
    for sector in df_valuation["Sector"].unique():
        df_valuation.loc[df_valuation['Sector'] == sector, 'MeanSectorPeRatio'] = np.mean(df_valuation[df_valuation['Sector'] == sector]['PeRatio'])
        df_valuation.loc[df_valuation['Sector'] == sector, 'MeanSectorEvEbitdaRatio'] = np.mean(df_valuation[df_valuation['Sector'] == sector]['EnterprisesValueEBITDARatio'])
        df_valuation.loc[df_valuation['Sector'] == sector, 'MeanSectorEvRevenueRatio'] = np.mean(df_valuation[df_valuation['Sector'] == sector]['EnterprisesValueRevenueRatio'])

    df_valuation['PeRatioVsSectorMeanInPercent'] = (df_valuation['PeRatio'] / df_valuation['MeanSectorPeRatio'] - 1) * 100
    df_valuation['EvEbitdaRatioVsSectorMeanInPercent'] = (df_valuation['EnterprisesValueEBITDARatio'] / df_valuation['MeanSectorEvEbitdaRatio'] - 1) * 100
    df_valuation['EvRevenueRationVsSectorMeanInPercent'] = (df_valuation['EnterprisesValueRevenueRatio'] / df_valuation['MeanSectorEvRevenueRatio']-1)*100
    
    fact_valuation = fact_valuation.set_index(["Ticker", "asOfDate"])
    df_valuation = df_valuation.set_index(["Ticker", "asOfDate"])
    fact_valuation = pd.concat([df_valuation[~df_valuation.index.isin(fact_valuation.index)], fact_valuation]).reset_index()

    parquet_file = fact_valuation.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, f"gold/factvaluation", f"fact_valuation.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
