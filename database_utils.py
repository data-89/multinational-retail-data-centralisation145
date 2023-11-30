import yaml
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self,file_path):
         self.credentials = self.read_db_creds(file_path)
         self.engine = self.init_db_engine()

    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            credentials = yaml.safe_load(file)
            return credentials

    def init_db_engine(self):
        engine = db.create_engine(f"postgresql+psycopg2://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}")
        self.connection = engine.connect()
        return engine
        
    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        print(table_names)
        return table_names
        
    def upload_to_db(self, df, table_name):
            df.to_sql(name=table_name, con=self.connection, if_exists='replace', index=False)
            print(f"Data successfully uploaded to table '{table_name}'.")
            return True
    

