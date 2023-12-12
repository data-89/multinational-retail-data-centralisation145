import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy.exc import SQLAlchemyError
from tabula import read_pdf
import requests
import boto3
from io import StringIO
from urllib.parse import urlparse

db_connector = DatabaseConnector('db_creds.yaml')

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        try:
            df = pd.read_sql(query, self.db_connector.engine)
            print(f"Reading table: {table_name}: {df}")
            return df
        except SQLAlchemyError as e:
            print(f"Error reading table {table_name}:\n{e}")
            return pd.DataFrame()
        
    def retrieve_pdf_data(self, link):
        dfs = read_pdf(link, multiple_tables=True, pages="all")
        concatenated_df = pd.concat(dfs, ignore_index=True)
        return concatenated_df

    def list_number_of_stores(self, headers, endpoint):
        response = requests.get(endpoint, headers=headers)
        number_of_stores = response.json()
        print(f" Number of stores:\n{number_of_stores}")
        
    def retrieve_stores_data(self, headers, endpoint):
        stores_data = []
        for store_number in range(0, 451):
                final_endpoint = f"{endpoint}/{store_number}"
                response = requests.get(final_endpoint, headers=headers)
                if response.status_code == 200:  # Check if the request was successful
                    store_data = response.json()
                    stores_data.append(store_data)
                else:
                    print(f"Failed to retrieve data for store {store_number}. Status Code: {response.status_code}")
                    break
        df = pd.DataFrame(stores_data)
        print(f"Stores data df:\n{df}")
        return df
    
    def extract_from_s3_csv(self, s3_address): # other parameters: download_path
        bucket_name, key = s3_address.split('//')[1].split('/', 1)
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        #response = s3_client.download_file('data-handling-public', 'products.csv', 'products.csv')
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(f"Data extracted from s3 csv to df:\n{df}")
        return df

    def extract_from_s3_json(self, s3_address):
        parsed_url = urlparse(s3_address)
        bucket = parsed_url.netloc
        key = parsed_url.path[1:]        
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket='data-handling-public', Key=key)
        json_content = response['Body'].read().decode('utf-8')
        df = pd.read_json(StringIO(json_content))
        print(f"Data extracted from s3 json to df:\n{df}")
        return df


        


        