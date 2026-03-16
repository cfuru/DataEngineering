import datetime
import logging
import time
from datetime import date
import os
from io import BytesIO
import pandas as pd

import azure.functions as func
from shared_code import utils

def main(msg: func.QueueMessage) -> None:
    
    try:
        ticker = msg.get_body().decode("utf-8")
        logging.info('Python queue trigger function processed a queue item: %s', ticker)

        # Initialize Azure Utils
        azure_utils = utils.AzureUtils()
        secret_client = azure_utils.initialize_key_vault()

        sa_secret = os.getenv("AZURE_STORAGE_SECRET")#azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
        sa_name = os.getenv("AZURE_STORAGE_NAME")#azure_utils.get_key_vault_secret(secret_client, 'sa-name')
        azure_utils.initialize_storage_account_ad(sa_secret, sa_name)

        # Get financials data and upload to Blob Storage
        fetch_and_upload_financials(ticker, azure_utils)

    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        
def fetch_and_upload_financials(ticker, azure_utils):
    all_scores = []
    
    try:
        stock = utils.StockFundamentals(ticker)
        
        calculator = utils.PiotroskiScoreCalculator(stock)
        scores = calculator.calculate_score()
        scores['Ticker'] = ticker  # add a column for the ticker
        all_scores.append(scores)
        scores_df = pd.concat(all_scores)
        
        parquet_file = stock.income_statement.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/IncomeStatement/sp500/{date.today()}", f"IncomeStatement_{ticker}.parquet")
        
        parquet_file = stock.balance_sheet.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/BalanceSheet/sp500/{date.today()}", f"BalanceSheet_{ticker}.parquet")
        
        parquet_file = stock.cash_flow.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/CashFlow/sp500/{date.today()}", f"CashFlow_{ticker}.parquet")
        
        parquet_file = stock.valuation_measure.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/ValuationMeasure/sp500/{date.today()}", f"ValuationMeasure_{ticker}.parquet")
        
        parquet_file = scores_df.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/PiotroskiScore/sp500/{date.today()}", f"PiotroskiScore_{ticker}.parquet")
        
        parquet_file = stock.asset_profile.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/AssetProfile/sp500/{date.today()}", f"AssetProfile_{ticker}.parquet")
        
        parquet_file = stock.financial_data.reset_index().to_parquet(index = False)
        azure_utils.upload_blob(parquet_file, f"bronze/FinancialData/sp500/{date.today()}", f"FinancialData_{ticker}.parquet")
    
    except Exception as e:
        logging.error(f'Error occurred while fetching and uploading financials data: {str(e)}')
