import pandas as pd
import numpy as np
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    def clean_card_number(self, card_number):
        # Example: Remove non-numeric characters from card numbers
        cleaned_card_number = ''.join(char for char in str(card_number) if char.isnumeric())
        return cleaned_card_number
    
    def clean_card_data(self, card_data):
        card_data.dropna(inplace=True)
        card_data['card_number'] = card_data['card_number'].apply(self.clean_card_number)
        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], format='%m/%y', errors='ignore')
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors='ignore')
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

    def clean_user_data(self, user_data):
        user_data.dropna(inplace=True)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], errors='coerce')
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], errors='coerce')
        user_data['country_code'] = user_data['country_code'].astype(str)
        user_data['phone_number'] = user_data['phone_number'].astype(str)
        return user_data
    
    def clean_stores_data(self, stores_data):
        stores_data.staff_numbers = stores_data.staff_numbers.astype('int64', errors='ignore')
        stores_data.opening_date = pd.to_datetime(stores_data.opening_date, errors='ignore')
        stores_data['latitude'] = pd.to_numeric(stores_data['latitude'], errors='coerce')
        stores_data['latitude'].fillna('0', inplace=True)
        stores_data.latitude = stores_data.latitude.astype('float64', errors='raise').round(2)
        # stores_data.longitude = stores_data.longitude.astype('float64', errors='raise').round(2)
        stores_data = stores_data.replace('N/A', np.nan)
        print(f"Clean stores data:\n{stores_data.head()}")
        return stores_data

    def convert_product_weights(self, products_df):
        products_df['weight'] = products_df['weight'].astype(str)
        def clean_and_convert(weight_str):
            weight_str = str(weight_str)
            weight_str = ''.join(char for char in weight_str if char.isdigit() or char in ('.', ','))
            weight_str = weight_str.replace(',', '.')
            weight_str = pd.to_numeric(weight_str, errors='coerce')
            print(f"Type of weight string: {type(weight_str)}")
            print(weight_str)
            if 'ml' in weight_str:
                weight_float = float(weight_str)
                weight_float *= 0.001
            elif 'g' in weight_str:
                weight_float = float(weight_str)
                weight_float *= 0.001
            elif 'kg' in weight_str:
                weight_float = float(weight_str)
            return weight_float
        products_df['weight'] = products_df['weight'].apply(clean_and_convert)
        return products_df
    
    def clean_products_data(self, products_df):
        products_df['date_added'] = pd.to_datetime(products_df['dat_added'], errors='coerce')
        products_df['product_price'] = products_df.product_price.astype('float64', errors='coerce')
        products_df['EAN'] = products_df.EAN.astype('int64', errors='ignore')
        products_df['removed'] = products_df['removed'].astype(bool)
        return products_df

    def clean_orders_data(self, orders_df):
        columns_to_drop = ['first_name', 'last_name', '1']
        orders_df_cleaned = orders_df.drop(columns=columns_to_drop, errors='ignore')
        print(f"Cleaned Orders DataFrame:\n{orders_df_cleaned}")
        return orders_df_cleaned
    
    def clean_datetime_data(self, datetime_df):
        print(f"Datetime data info:\n{datetime_df.info()}")
        print(f"Datetime data head:\n{datetime_df.head()}")
