from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector('db_creds.yaml')

data_extractor = DataExtractor(db_connector)

address = "s3://data-handling-public/products.csv"
download_path = "/Users/rafal/Downloads"
products_df = data_extractor.extract_from_s3_csv(address) # other parameter: download_path
print(f"Uncleaned products info:\n{products_df.info()}")

data_cleaner = DataCleaning()
cleaned_products_df = data_cleaner.convert_product_weights(products_df)
print(f"Cleaned products:\n{cleaned_products_df.info()}")
