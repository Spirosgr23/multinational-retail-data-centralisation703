import datetime
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    print("Starting process at:", datetime.datetime.now())
    dbcon = DatabaseConnector()
    dbex = DataExtractor()
    dbclean = DataCleaning()
    
    print("Reading data from RDS database...")
    user_df = dbex.read_rds_table(dbcon, 'legacy_users')

    if user_df is None:
        print("Error: No data returned from 'read_rds_table'.")
        return

    print(f"Data read successfully. Number of rows before cleaning: {len(user_df)}")
    clean_user_df = dbclean.clean_user_data(user_df)

    if clean_user_df is None or clean_user_df.empty:
        print("Error: Data cleaning resulted in 'None' or an empty DataFrame.")
        return

    print(f"Data cleaned successfully. Number of rows after cleaning: {len(clean_user_df)}")
    print("Uploading cleaned data to the database...")
    dbcon.upload_to_db(clean_user_df, 'dim_users')
    print("Data uploaded successfully to 'dim_users' table at:", datetime.datetime.now())
    
    print("Retrieving card data at:", datetime.datetime.now())
    card_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_df = dbex.retrieve_pdf_data(card_link)
    print("Card data retrieved at:", datetime.datetime.now())
    print("Cleaning card data at:", datetime.datetime.now())
    clean_card_df = dbclean.clean_card_data(card_df)
    print("Card data cleaned at:", datetime.datetime.now())
    
    print("Uploading card data to the database at:", datetime.datetime.now())
    dbcon.upload_to_db(clean_card_df, 'dim_card_details')
    print("Card data upload completed, data uploaded in 'dim_card_details' table.")

    print("Process completed at:", datetime.datetime.now())

    # Store data extraction and processing
    print("Retrieving store data at:", datetime.datetime.now())
    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_stores_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"

    try:
        number_of_stores = dbex.list_number_of_stores(number_stores_url, headers)
        print(f"Number of stores to extract: {number_of_stores} at: {datetime.datetime.now()}")
    
        store_data = dbex.retrieve_stores_data(store_details_url, headers, number_of_stores)
        if not store_data.empty:
            clean_store_data = dbclean.clean_store_data(store_data)
            # Additional check if 'index' column exists in DataFrame
            if 'index' in clean_store_data.columns:
                clean_store_data.drop(columns=['index'], inplace=True)
            dbcon.upload_to_db(clean_store_data, 'dim_store_details')
            print("Store data uploaded successfully to 'dim_store_details' table at:", datetime.datetime.now())
    
    except Exception as e:
       print(f"An error occurred while processing store data: {e}")
       
    # Step to handle product data from S3
    print("Retrieving product data from S3...")
    product_df = dbex.extract_from_s3('s3://data-handling-public/products.csv')
    print("Cleaning product data...")
    clean_product_df = dbclean.clean_products_data(product_df)
    print("Uploading product data to the database...")
    dbcon.upload_to_db(clean_product_df, 'dim_products')
    print("Product data upload completed, cleaned data uploaded to 'dim_products' table at:", datetime.datetime.now())
    
 # Database table listing to find the orders table
    db_engine = dbcon.init_db_engine()
    tables_in_db = dbcon.list_db_tables(db_engine)
    print("Tables in the database:", tables_in_db)
    
    # Assuming the orders table is named 'orders_table'
    orders_table_name = "orders_table"
    if orders_table_name in tables_in_db:
        print(f"Extracting data from {orders_table_name}...")
        orders_df = dbex.read_rds_table(dbcon, orders_table_name)
        print(f"Orders data extracted. Number of rows: {len(orders_df)}")
        
        # Clean orders data
        clean_orders_df = dbclean.clean_orders_data(orders_df)
        print("Orders data cleaned.")
        
        # Upload cleaned data to the database
        dbcon.upload_to_db(clean_orders_df, orders_table_name)
        print(f"Cleaned orders data uploaded to {orders_table_name} table.")
    else:
        print(f"Table {orders_table_name} not found in the database.")
        
    # Extract JSON data
    print("Extracting JSON data...")
    json_data = dbex.extract_json_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

    # Extract and clean JSON data
    dbex = DataExtractor()
    dbclean = DataCleaning()
    dbcon = DatabaseConnector()

    print("Starting JSON data extraction...")
    json_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    json_data = dbex.extract_json_data(json_link)
    print("JSON data extracted at:", datetime.datetime.now())
    
    print("Cleaning JSON data...")
    cleaned_json_data = dbclean.clean_date_times_data(json_data) 
    print("JSON data cleaned at:", datetime.datetime.now())

    # Upload cleaned data to the database
    print("Uploading cleaned JSON data to the database...")
    dbcon.upload_to_db(cleaned_json_data, 'dim_date_times')
    print("Cleaned JSON data uploaded to 'dim_date_times' table at:", datetime.datetime.now())

if __name__ == "__main__":
    main()