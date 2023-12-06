import pandas as pd
import numpy as np
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

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
        stores_data.dropna(how='all') #substitute with drop lat column
        stores_data.dropna(axis=3)
        stores_data.staff_numbers = stores_data.staff_numbers.astype('int64', errors='ignore')
        stores_data.opening_date = pd.to_datetime(stores_data.opening_date)
        stores_data.latitude = stores_data.latitude.astype('float64', errors='ignore').round(2)
        stores_data.longitude = stores_data.longitude.astype('float64', errors='ignore').round(2)
        stores_data = stores_data.replace('N/A', np.nan)
        print(stores_data.head())
        return stores_data

    def convert_product_weights(products_df):
        products_df['weight'] = products_df['weight'].astype(str)
        def clean_and_convert(weight_str):
            weight_str = ''.join(char for char in weight_str if char.isdigit() or char in ('.', ','))
            weight_str = weight_str.replace(',', '.')
            weight_float = float(weight_str)
            if 'ml' in weight_str:
                weight_float *= 1.0  # adjust this conversion factor based on your estimation
                return weight_float
        products_df['weight'] = products_df['weight'].apply(clean_and_convert)
        return products_df
    
    def clean_products_data(products_df):
        products_df['date_added'] = pd.to_datetime(products_df['dat_added'], errors='coerce')
        products_df['product_price'] = products_df.product_price.astype('float64', errors='coerce')
        products_df['EAN'] = products_df.EAN.astype('int64', errors='ignore')
        products_df['removed'] = products_df['removed'].astype(bool)
        return products_df

    def clean_orders_data(self, orders_df):
        columns_to_drop = ['first_name', 'last_name', '1']
        orders_df_cleaned = orders_df.drop(columns=columns_to_drop, errors='ignore')
        
        # Perform additional cleaning if needed
        # For example, handling missing values, data type conversions, etc.

        # Print information about the cleaned DataFrame
        print("Cleaned Orders DataFrame:")
        print(orders_df_cleaned.info())

        return orders_df_cleaned
