# multinational-retail-data-centralisation145

# Multinational Retail Distribution Centre ETL Pipeline
Overview
This project implements an ETL (Extract, Transform, Load) pipeline for a Multinational Retail Distribution Centre. The pipeline is designed to handle data extraction from various sources, cleaning and transformation, and loading the data into a database for analysis.

Project Structure
The project is organized into three main classes:

1. DataExtractor: Handles the extraction of data from different sources, including databases and PDFs.

2. DatabaseConnector: Manages the connection to the database and provides methods for executing SQL queries and interacting with tables.

3. DataCleaning: Contains methods for cleaning and transforming the extracted data before loading it into the database.

Classes and Methods
1. DataExtractor
   Methods:
read_rds_table(table_name):

Reads a table from the connected database.
retrieve_pdf_data(link):

Extracts data from a PDF file using the Tabula library.
list_number_of_stores(headers, endpoint):

Retrieves the number of stores from an API endpoint.
retrieve_stores_data(headers, endpoint):

Retrieves store data from an API endpoint.
extract_from_s3(s3_address, download_path):

Downloads and extracts data from an S3 bucket.
2. DatabaseConnector
  Methods:
init(file_path):

Initializes the database connector with credentials from a YAML file.
execute_query(query):

Executes a SQL query on the connected database.
execute_many_queries(queries):

Executes a list of SQL queries.
close_connection():

Closes the database connection.

3. DataCleaning
  Methods:
clean_user_data(user_data):

Cleans user data by handling NULL values, date errors, and incorrect data types.
clean_card_data(card_data):

Cleans card data by handling erroneous values, NULL values, and formatting errors.
clean_stores_data(stores_data):

Cleans store data by handling NULL values, data type conversion, and rounding of numeric values.


# Usage
Clone the repository:

git clone https://github.com/your-username/your-repository.git
cd your-repository

Install dependencies:

pip install -r requirements.txt

Update the configuration file (e.g., config.yaml) with your database credentials and other necessary information.

Run the ETL pipeline:

python main.py
