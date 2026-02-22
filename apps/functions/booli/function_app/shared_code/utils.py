import queue
import pandas as pd
import logging
import requests
import time

from lxml import html
from io import BytesIO
from io import StringIO
from datetime import date
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
        self.vault_url = "https://kv-booli-prod-001.vault.azure.net/"

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
   
    def upload_csv_to_datalake(self, df, container, filename):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, filename)
        try:
            blob_client_instance.upload_blob(df.to_csv(index = False, encoding = "utf-8"), overwrite = True)
            logging.info(f"Successfully uploaded csv file {filename} to container {container}")
        except Exception as e:
            logging.error(f"Error uploading csv file {filename} to container {container}: {e}")
            
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
    
    def ingest_raw_data(self, blob_name_starts_with):
        blob_list = self.list_blobs(f"raw", blob_name_starts_with)
        df = pd.concat([self.download_csv_blob(f"raw", blob.name) for blob in blob_list], ignore_index = True)
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

class Booli:
    def __init__(self):
        self.path = "https://www.booli.se/graphql"
        
    def run_query_upcoming(self, object_type, rooms, area_id, page):
        payload = """{
            "operationName":"searchForSale",
            "variables":{
                "input":{
                    "filters":[
                        {
                            "key":"objectType",
                            "value":"%s"
                        },
                        {
                            "key":"rooms",
                            "value":"%s"
                        },
                        {
                            "key":"isNewConstruction",
                            "value":""
                        },
                        {
                            "key":"priceDecrease",
                            "value":""
                        },
                        {
                            "key":"upcomingSale",
                            "value":""
                        }
                    ],
                    "areaId":"%s",
                    "sort":"published",
                    "page":%s,
                    "ascending":false
                }
            },
            "query":"query searchForSale($input: SearchRequest) { search: searchForSale(input: $input) { pages totalCount result { __typename ... on Listing { booliId descriptiveAreaName constructionYear floor{raw} livingArea{raw} listPrice{raw} rent{raw} listSqmPrice{raw} latitude longitude daysActive objectType rent{raw} operatingCost{raw} estimate{ price{raw} } rooms{raw} streetAddress url isNewConstruction biddingOpen upcomingSale mortgageDeed tenureForm plotArea{raw} hasPatio hasBalcony hasFireplace}} __typename  }}"
        }"""
        headers = {
            'authority': "www.booli.se",
            'accept': "*/*",
            'accept-language': "sv,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            'api-client': "booli.se",
            'content-type': "application/json",
            'origin': "https://www.booli.se",
            'sec-ch-ua-mobile': "?0",
            'sec-ch-ua-platform': "Windows",
            'sec-fetch-dest': "empty",
            'sec-fetch-mode': "cors",
            'sec-fetch-site': "same-origin",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.71"
        }
        response = requests.post(self.path, data = payload % (object_type, rooms, area_id, page), headers=headers) # Throws 400
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Query failed to run: {} - {}".format(response.status_code, response.json()))
        
    def run_query_sold(self, object_type, minSoldDate, maxSoldDate, rooms, hasBalcony, hasFireplace, hasElevator, area_id, page):
        payload = """{    
            "operationName": "searchSold",    
            "variables": {
                "input": {            
                    "filters": [                
                        {                    
                            "key": "objectType",
                            "value": "%s"                
                        },
                        {
					        "key": "minSoldDate",
					        "value": "%s"
				        },
                        {
                            "key": "maxSoldDate",
                            "value": "%s"
                        },                
                        {                    
                            "key": "rooms",                    
                            "value": "%s"                
                        },
                         {                    
                            "key": "hasBalcony",                    
                            "value": "%s"                
                        },
                        {                    
                            "key": "hasFireplace",                    
                            "value": "%s"                
                        },
                        {                    
                            "key": "hasElevator",                    
                            "value": "%s"                
                        }
                    ],        
                    "areaId": "%s",        
                    "sort": "created",        
                    "page": %s,        
                    "ascending": false        
                }    
            },    
            "query": "query searchSold($input: SearchRequest) {  search: searchSold(input: $input) {    pages    totalCount    result {      booliId      soldPrice {raw}   rent{raw}   streetAddress       constructionYear       floor{raw}      soldSqmPrice {raw}      soldPriceAbsoluteDiff {raw}      soldPricePercentageDiff {raw}      listPrice {raw}      livingArea {raw}      rooms {raw}      rooms {raw}      objectType      descriptiveAreaName      soldPriceType      daysActive      soldDate      latitude      longitude      url      __typename    }    __typename  }}"    
        }"""
        headers = {
            'authority': "www.booli.se",
            'accept': "*/*",
            'accept-language': "sv,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            'api-client': "booli.se",
            'content-type': "application/json",
            'origin': "https://www.booli.se",
            'sec-ch-ua-mobile': "?0",
            'sec-ch-ua-platform': "Windows",
            'sec-fetch-dest': "empty",
            'sec-fetch-mode': "cors",
            'sec-fetch-site': "same-origin",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.77"
        }
        response = requests.post(self.path, data = payload % (object_type, minSoldDate, maxSoldDate, rooms, hasBalcony, hasFireplace, hasElevator, area_id, page), headers=headers) # Throws 400
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Query failed to run: {} - {}".format(response.status_code, response.json()))
        

#Structure created by Sarah Floris
class DataCleaning:
    def __init__(self):
        pass
    
    def drop_dataframe_columns(self, df, columns):
        try:
            return df.drop(columns, axis = 1)
        except Exception as e:
            logging.error(f"Failed to drop columns {columns}: {e}")

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
    
    def get_missing_values_percent(self, df):
        return pd.DataFrame({"Missing Values [%]" : df.isna().sum() / len(df.index)*100})
    
    def get_data_types(self, df):
        return pd.DataFrame({"Data Type" : df.dtypes})
    
    def rename_df_columns(self, df, columns_to_rename):
        return df.rename(columns = columns_to_rename)
    
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
        