import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy.exc import SQLAlchemyError
from tabula import read_pdf
import requests

db_connector = DatabaseConnector('db_creds.yaml')
list_tables = db_connector.list_db_tables()

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        try:
            df = pd.read_sql(query, self.db_connector.engine)
            print(df)
            return df
        except SQLAlchemyError as e:
            print(f"Error reading table {table_name}: {e}")
            return pd.DataFrame()
        
    def retrieve_pdf_data(self, link):
        df = read_pdf(link, multiple_tables=True, pages="all")
        print(len(df))
        return df

    def list_number_of_stores(self, headers, endpoint):
        response = requests.get(endpoint, headers=headers)
        number_of_stores = response.json()
        print(number_of_stores)
        
    def retrieve_stores_data(self, headers, endpoint):
        stores_data = []
        for store_number in range(0, 450):
                final_endpoint = f"{endpoint}/{store_number}"
                response = requests.get(final_endpoint, headers=headers)
                if response.status_code == 200:  # Check if the request was successful
                    store_data = response.json()
                    stores_data.append(store_data)
                else:
                    print(f"Failed to retrieve data for store {store_number}. Status Code: {response.status_code}")
                    break
        
        df = pd.DataFrame(stores_data)
        print(df)
        return df
        

data_extractor = DataExtractor(db_connector)

retrieve_store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
return_number_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
number_of_stores = data_extractor.list_number_of_stores(header, return_number_url)
stores_data = data_extractor.retrieve_stores_data(header, retrieve_store_url)

        


        