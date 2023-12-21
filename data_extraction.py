# data_extraction.py
import boto3
import time
import requests
import tabula
import pandas as pd
from tabula import read_pdf

class DataExtractor:
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        return pd.read_sql_table(table_name, engine) 
    
    def retrieve_pdf_data(self, link):
        # Extract tables from PDF
        tables = read_pdf(link, pages='all', multiple_tables=True)
        # Combine all tables into a single DataFrame
        return pd.concat(tables, ignore_index=True)
    
    def list_number_of_stores(self, number_stores_url, headers):
        response = requests.get(number_stores_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']  # Extract the number of stores
        else:
            raise Exception("Failed to retrieve the number of stores")    

    def retrieve_stores_data(self, store_details_url, headers, number_of_stores):
        stores = []
        for store_number in range(1, number_of_stores + 1):
            response = requests.get(store_details_url.format(store_number), headers=headers)
            if response.status_code == 200:
               store_data = response.json()
               stores.append(store_data)
            else:
                print(f"Failed to retrieve details for store {store_number}. Status code: {response.status_code}, Response: {response.text}")   
            time.sleep(0.1)  # wait for 0.5 second between requests
    
        df = pd.DataFrame(stores)
        # If 'index' is a column in df, set it as the index
        if 'index' in df.columns:
            df.set_index('index', inplace=True)
        return df
    
    def extract_from_s3(self, s3_address):
        import boto3
        import pandas as pd
        from io import BytesIO

        s3_client = boto3.client('s3')
        bucket, key = s3_address.replace("s3://", "").split("/", 1)
        csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = csv_obj['Body'].read()
        return pd.read_csv(BytesIO(body))
    
    def extract_json_data(self, link):
        response = requests.get(link)
        data = response.json()
        return pd.DataFrame.from_dict(data)

