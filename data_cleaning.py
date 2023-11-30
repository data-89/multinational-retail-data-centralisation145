import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

db_connector = DatabaseConnector('db_creds.yaml')
list_tables = db_connector.list_db_tables()
data_extractor = DataExtractor(db_connector)
user_data = data_extractor.read_rds_table(list_tables[1])
card_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

class DataCleaning:
    def clean_card_data(self, card_data):
        card_data.dropna(inplace=True)
        card_data['card_number'] = card_data['card_number'].apply(self.clean_card_number)
        return card_data

    def change_data_type_to_str(self, table_name, Column_name):
        table_name[Column_name] = table_name[Column_name].astype('string')
        print(table_name.info())
        
    def change_type_to_int64(self, table_name, Column_name):
        table_name[Column_name] = table_name[Column_name].astype('int64', errors='ignore')
        print(table_name.info())

    def change_date_type(self, table_name, Column_name):
        table_name[Column_name] = pd.to_datetime(table_name[Column_name])
        print(table_name.info())

    def clean_user_data(self, table_name):
        user_data.dropna(inplace=True)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], errors='coerce')
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], errors='coerce')
        user_data['country_code'] = user_data['country_code'].astype(str)
        user_data['phone_number'] = user_data['phone_number'].astype(str)
        return user_data
    
    def clean_stores_data(self, stores_data):
        stores_data.dropna(inplace=True)

    