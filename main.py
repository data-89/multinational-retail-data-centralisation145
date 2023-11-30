from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector('db_creds.yaml')
list_tables = db_connector.list_db_tables()

data_extractor = DataExtractor(db_connector)
result_dataframes = data_extractor.read_rds_table(list_tables)
first_table_dataframe = data_extractor.read_rds_table(list_tables[1])
print(first_table_dataframe.dtypes)
print(first_table_dataframe.info())

db_connector = DatabaseConnector('db_creds.yaml')
list_tables = db_connector.list_db_tables()

data_extractor = DataExtractor(db_connector)
user_data = data_extractor.read_rds_table(list_tables[1])
card_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

retrieve_store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
return_number_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
number_of_stores = data_extractor.list_number_of_stores(header, return_number_url)
stores_data = data_extractor.retrieve_stores_data(header, retrieve_store_url)

data_cleaner = DataCleaning()
cleaned_user_data = data_cleaner.clean_user_data(user_data)
cleaned_card_data = data_cleaner.clean_card_data(card_data)
cleaned_stores_data = data_cleaner.clean_stores_data(stores_data)

db_con2 = DatabaseConnector('postgre_creds.yaml')
db_con2.upload_to_db(cleaned_user_data, table_name="dim_users")
db_con2.upload_to_db(cleaned_card_data, table_name="dim_card_details")