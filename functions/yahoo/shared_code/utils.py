import queue
import pandas as pd
import logging
import requests as r
import yfinance as yf

from lxml import html
from io import BytesIO
from io import StringIO
from datetime import date
from yahooquery import Ticker
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

class AzureUtils:
    def __init__(self, vault_url=None, storageaccount=None):
        self.vault_url = "https://kv-yahoo-prod-001.vault.azure.net/"

    def initialize_storage_account_ad(self, storage_account_secret, blob):
        try:  
            global blob_service_client_instance
            blob_service_client_instance = BlobServiceClient(
                account_url = "{}://{}.blob.core.windows.net".format("https", blob), 
                credential = storage_account_secret
                )
        except Exception as e:
            logging.error(f"Could not create blob service client: {e}")
            
    def initialize_data_lake(self, storage_account_name, storage_account_secret):
        try:  
            global datalake_service_client
            datalake_service_client = DataLakeServiceClient(
                account_url = "{}://{}.dfs.core.windows.net".format("https", storage_account_name), 
                credential = storage_account_secret
                )
        except Exception as e:
            logging.error(f"Could not create data lake service client: {e}")

    def initialize_queue_client(self, accountUrl, queueName):
        try:  
            global queue_client_instance
            queue_client_instance = QueueClient.from_connection_string(
                conn_str = accountUrl, 
                queue_name = queueName,
                message_encode_policy = BinaryBase64EncodePolicy(),
                message_decode_policy = BinaryBase64DecodePolicy()
                )
            logging.info(f"Connected to queue {queueName} successfully")
        except Exception as e:
            logging.error(f"Error connecting to queue {queueName}: {e}")

    def send_queue_message(self, message):
        try:
            queue_client_instance.send_message(message.encode("utf-8"))
            logging.info(f"Queued message {message} successfully")
        except Exception as e:
            logging.error(f"Error queueing message {message}: {e}")
   
    def upload_blob(self, data, container, blob_name):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, blob_name, snapshot = None)
        try:
            blob_client_instance.upload_blob(data, overwrite = True, encoding = "utf-8", length=len(data))
            logging.info(f"Created blob {blob_name} successfully")
        except Exception as e:
            logging.error(f"Error creating blob {blob_name}: {e}")
            
    def write_dataframe_to_datalake(self, df, dir_name, filename):
        file_system_client = datalake_service_client.get_file_system_client(file_system = "gold")
        directory_client = file_system_client.get_directory_client(dir_name)
        file_client = directory_client.create_file(f'{filename}_{date.today()}.Parquet')
        
        df_parquet = df.to_parquet()
        file_client.append_data(data = df_parquet, offset = 0, length = len(df_parquet))
        file_client.flush_data(len(df_parquet))
        return True
    
    def download_parquet_blob(self, container, blob_name):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, blob_name, snapshot = None)
        try:
            with BytesIO() as input_blob:
                blob_client_instance.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_parquet(input_blob)
            logging.info(f"Downloaded blob {blob_name} successfully")    
        except Exception as e:
            logging.error(f"Error downloadning blob {blob_name}: {e}")
        return df
    
    def download_csv_blob(self, container, blob_name):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, blob_name, snapshot = None)
        try:
            with BytesIO() as input_blob:
                blob_client_instance.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob)
            logging.info(f"Downloaded blob {blob_name} successfully")    
        except Exception as e:
            logging.error(f"Error downloadning blob {blob_name}: {e}")
        return df
    
    def ingest_bronze_data(self, directory):
        blob_list = self.list_blobs("bronze", directory)
        df = pd.concat([self.download_parquet_blob("bronze", blob.name) for blob in blob_list], ignore_index = True)
        return df
    
    def ingest_silver_data(self, directory):
        blob_list = self.list_blobs("silver", directory)
        df = pd.concat([self.download_parquet_blob("silver", blob.name) for blob in blob_list], ignore_index = True)
        return df
    
    def list_blobs(self, container, blob_name_starts_with):
        try:
            container_client_instance = blob_service_client_instance.get_container_client(container)
            blob_list = container_client_instance.list_blobs(blob_name_starts_with)
            logging.info(f"Retreived list of blobs that starts with name {blob_name_starts_with} from container {container}")
        except Exception as e:
            logging.error(f"Error retreiving list of blobs in container {container} with blob name starting with {blob_name_starts_with}: {e}")
        return blob_list
            
    def initialize_key_vault(self):
        credential = DefaultAzureCredential(additionally_allowed_tenants=['*'])
        secret_client = SecretClient(vault_url = self.vault_url, credential=credential)
        return secret_client

    def get_key_vault_secret(self, secret_client, secret_name):
        return secret_client.get_secret(secret_name)

class yahooUtils:
    def __init__(self):
        self.nasdaq_base_url = "http://www.nasdaqomxnordic.com" 
        self.nasdaq_full_url = self.nasdaq_base_url + "/shares/listed-companies/stockholm"
        self.sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    def scrape_nasdaq_companies(self):
        headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
        page = r.get(self.nasdaq_full_url, headers = headers)
        tree = html.fromstring(page.content)
        page.close
        tree.make_links_absolute(self.nasdaq_base_url)
        trs = tree.xpath('//tbody//tr')
        df_companies = pd.DataFrame(
                [[j.text_content() for j in i.getchildren()[:-1]] for i in trs],
                columns = ['name','symbol','currency','isin','sector','icb']
                )
        # here we create the ticker names for the queries to Yahoo Finance
        df_companies['tickers'] = ["-".join(i.split(" "))+".ST" for i in df_companies['symbol'].values]
        return df_companies

    def scrape_sp500_comapnies(self):
        data = pd.read_html(self.sp500_url)
        return data[0]

    def get_ticker_financials(self, ticker):
        try:
            ticker_object = yf.Ticker(ticker)
            temp = pd.DataFrame.from_dict(ticker_object.info, orient = "index")
            temp.reset_index(inplace = True)
            temp.columns = ["Attribute", "Recent"]
            temp["Ticker"] = ticker
            return temp
        except Exception as e:
            print(e)

class StockFundamentals:
    def __init__(self, symbol):
        self.symbol = symbol
        self.ticker = Ticker(self.symbol)
        self.income_statement = None
        self.balance_sheet = None
        self.cash_flow = None
        self.valuation_measure = None
        self.asset_profile = None
        self.financial_data = None

    def fetch_data(self):
        self.income_statement = self.ticker.income_statement(frequency = 'q', trailing = False).sort_values('asOfDate').set_index("asOfDate")
        self.income_statement["Ticker"] = self.symbol
        
        self.balance_sheet = self.ticker.balance_sheet(frequency = 'q', trailing = False).sort_values('asOfDate').set_index("asOfDate")
        self.balance_sheet["Ticker"] = self.symbol
        
        self.cash_flow = self.ticker.cash_flow(frequency = 'q', trailing = False).sort_values('asOfDate').set_index("asOfDate")
        self.cash_flow["Ticker"] = self.symbol
        
        self.valuation_measure = self.ticker.valuation_measures
        self.valuation_measure = self.valuation_measure[~self.valuation_measure.EnterpriseValue.isnull()]
        
        self.asset_profile = pd.DataFrame(self.ticker.asset_profile).T
        self.asset_profile = self.asset_profile.drop(columns = ["companyOfficers"])
        
        self.financial_data = pd.DataFrame(self.ticker.financial_data).T
                
class PiotroskiScoreCalculator:
    def __init__(self, stock):
        self.stock = stock
        self.date = None
        self.prev_date = None

    def set_dates(self, date):
        self.date = date
        self.prev_date = self.stock.balance_sheet.index[self.stock.balance_sheet.index.get_loc(date) - 1]

    def calculate_roa_score(self):
        roa = self.stock.income_statement.loc[self.date, 'NetIncome'] / self.stock.balance_sheet.loc[self.date, 'TotalAssets']
        return 1 if roa > 0 else 0

    def calculate_cfo_score(self):
        cfo = self.stock.cash_flow.loc[self.date, 'OperatingCashFlow']
        return 1 if cfo > 0 else 0

    def calculate_delta_roa_score(self):
        roa = self.stock.income_statement.loc[self.date, 'NetIncome'] / self.stock.balance_sheet.loc[self.date, 'TotalAssets']
        delta_roa = roa - (self.stock.income_statement.loc[self.prev_date, 'NetIncome'] / \
                           self.stock.balance_sheet.loc[self.prev_date, 'TotalAssets'])
        return 1 if delta_roa > 0 else 0

    def calculate_quality_of_earnings_score(self):
        cfo = self.stock.cash_flow.loc[self.date, 'OperatingCashFlow']
        return 1 if cfo > self.stock.income_statement.loc[self.date, 'NetIncome'] else 0

    def calculate_delta_leverage_score(self):
        delta_leverage = self.stock.balance_sheet.loc[self.date, 'LongTermDebt'] / self.stock.balance_sheet.loc[self.date, 'TotalAssets'] - \
                         self.stock.balance_sheet.loc[self.prev_date, 'LongTermDebt'] / self.stock.balance_sheet.loc[self.prev_date, 'TotalAssets']
        return 1 if delta_leverage < 0 else 0

    def calculate_delta_liquidity_score(self):
        delta_liquidity = (self.stock.balance_sheet.loc[self.date, 'CurrentAssets'] / self.stock.balance_sheet.loc[self.date, 'CurrentLiabilities']) - \
                          (self.stock.balance_sheet.loc[self.prev_date, 'CurrentAssets'] / self.stock.balance_sheet.loc[self.prev_date, 'CurrentLiabilities'])
        return 1 if delta_liquidity > 0 else 0

    def calculate_new_equity_score(self):
        new_equity = self.stock.balance_sheet.loc[self.date, 'CommonStock'] - \
                     self.stock.balance_sheet.loc[self.prev_date, 'CommonStock']
        return 1 if new_equity <= 0 else 0

    def calculate_gross_margin_score(self):
        gross_margin_now = (self.stock.income_statement.loc[self.date, 'TotalRevenue'] - \
                            self.stock.income_statement.loc[self.date, 'CostOfRevenue']) / self.stock.income_statement.loc[self.date, 'TotalRevenue']
        gross_margin_prev = (self.stock.income_statement.loc[self.prev_date, 'TotalRevenue'] - \
                             self.stock.income_statement.loc[self.prev_date, 'CostOfRevenue']) / self.stock.income_statement.loc[self.prev_date, 'TotalRevenue']
        return 1 if gross_margin_now > gross_margin_prev else 0

    def calculate_asset_turnover_score(self):
        asset_turnover_now = self.stock.income_statement.loc[self.date, 'TotalRevenue'] / self.stock.balance_sheet.loc[self.date, 'TotalAssets']
        asset_turnover_prev = self.stock.income_statement.loc[self.prev_date,
        'TotalRevenue'] / self.stock.balance_sheet.loc[self.prev_date, 'TotalAssets']
        return 1 if asset_turnover_now > asset_turnover_prev else 0

    def calculate_score(self):
        self.stock.fetch_data()
        score_data = []

        for date in self.stock.balance_sheet.index[1:]:
            self.set_dates(date)

            roa_score = self.calculate_roa_score()
            cfo_score = self.calculate_cfo_score()
            delta_roa_score = self.calculate_delta_roa_score()
            quality_of_earnings_score = self.calculate_quality_of_earnings_score()
            delta_leverage_score = self.calculate_delta_leverage_score()
            delta_liquidity_score = self.calculate_delta_liquidity_score()
            new_equity_score = self.calculate_new_equity_score()
            gross_margin_score = self.calculate_gross_margin_score()
            asset_turnover_score = self.calculate_asset_turnover_score()

            total_score = sum([roa_score, cfo_score, delta_roa_score, quality_of_earnings_score, delta_leverage_score,
                                delta_liquidity_score, new_equity_score, gross_margin_score, asset_turnover_score])
            
            # Save the data
            score_data.append([date, roa_score, cfo_score, delta_roa_score, quality_of_earnings_score, 
                               delta_leverage_score, delta_liquidity_score, new_equity_score, 
                               gross_margin_score, asset_turnover_score, total_score])

        # Convert the data into a DataFrame
        scores_df = pd.DataFrame(score_data, columns=['Date', 'ROA', 'CFO', 'Delta ROA', 'Quality of Earnings', 
                                                      'Delta Leverage', 'Delta Liquidity', 'New Equity', 
                                                      'Gross Margin', 'Asset Turnover', 'Piotroski Score'])

        return scores_df


#Structure created by Sarah Floris
class DataCleaning:
    def __init__(self):
        pass
    
    def pivot_fundamentals_dataframe(self, df, selected_index = ["Ticker", "ObservationDate"], selected_column = "Attribute", selected_value = "Recent"):
        try:
            return df.pivot(index = selected_index, columns = selected_column, values = selected_value).reset_index()
        except Exception as e:
            logging.error(f"Couldn't pivot the dataframe: {e}")
    
    def select_dataframe_columns(self, df, columns):
        try:
            return df[columns]
        except Exception as e:
            logging.error(f"Could not select columns {columns}: {e}")
            
    def set_dtype_to_numeric(self, df, cols_to_exclude):
        try:
            df.loc[:, ~df.columns.isin(cols_to_exclude)] = df.loc[:, ~df.columns.isin(cols_to_exclude)].apply(pd.to_numeric, errors = 'coerce')
        except Exception as e:
            logging.error(f"Could not change data type to numeric for all columns except {cols_to_exclude}: {e}")
        return df
    
    def change_timestamp_to_datetime(self, df, column_name):
        try:
            df[column_name] = df[column_name].apply(lambda x: pd.to_datetime(x, unit = 's'))
        except Exception as e:
            logging.error(f"Could not change timestamp to datetime for column {column_name}: {e}")
        return df
    
    def change_timestamp_format(self, df, column_name, date_format = '%Y-%m-%d'):
        df[column_name] = df[column_name].apply(lambda x: pd.to_datetime(x, format = date_format))
        return df
    
class FeatureEngineering:
    def __init__(self):
        pass
    
class DataFactory:
    def get_formatter(self, format):
        if format == 'Cleaning':
            return DataCleaning()
        elif format == 'Features':
            return FeatureEngineering()
        else:
            ValueError(format)
        












