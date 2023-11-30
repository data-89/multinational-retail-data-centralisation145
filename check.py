from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector('db_creds.yaml')
list_tables = db_connector.list_db_tables()

data_extractor = DataExtractor(db_connector)

db_connector = DatabaseConnector('db_creds.yaml')

data_extractor = DataExtractor(db_connector)
card_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

retrieve_store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
return_number_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
stores_data = data_extractor.retrieve_stores_data(header, retrieve_store_url)
print(stores_data.dtypes)
print(stores_data.info())

