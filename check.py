from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector('db_creds.yaml')
list_tables = db_connector.list_db_tables()

data_extractor = DataExtractor(db_connector)
#orders_table_dataframe = data_extractor.read_rds_table(list_tables[2])
#print(f"Orders table dtypes: \n{orders_table_dataframe.dtypes}")
#print(f"Orders table head: \n{orders_table_dataframe.head()}")

json_s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
date_details_df = data_extractor.extract_from_s3_json(json_s3_address)
print(f"Date details df dtypes: \n{date_details_df.dtypes}")