import pandas as pd
import numpy as np

class DataCleaning:
    
    def replace_and_drop_null(self, df):
        '''Replaces the string 'NULL' with NaN and drops rows with null values from the DataFrame.'''
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        return df
    
    def clean_user_data(self, df):
        '''Replaces the string 'NULL' with NaN and drops rows with null values from the DataFrame.'''
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        return df
    
    def clean_card_data(self, card_df):
        # Implement cleaning steps here
        # E.g., Remove NULL values, correct formatting, etc.
        # This is a placeholder, adjust as per your data's needs
        card_df.dropna(inplace=True)  # Example: Drop rows with NULL values
        return card_df
    
    def clean_store_data(self, store_df):
        # Implement your cleaning logic here
        # Example: store_df.dropna(inplace=True)
        return store_df
    
    def convert_product_weights(self, products_df):
        def convert_weight(weight):
            import re
            weight = str(weight).lower().replace(" ", "")
            if 'kg' in weight:
                return float(re.sub(r'[^0-9.]', '', weight))
            elif 'ml' in weight or 'g' in weight:
                return float(re.sub(r'[^0-9.]', '', weight)) / 1000
            elif 'oz' in weight:
                return float(re.sub(r'[^0-9.]', '', weight)) * 0.0283495
            return None  # Handle unexpected formats

        products_df['weight'] = products_df['weight'].apply(convert_weight)
        return products_df
    
    def clean_products_data(self, products_df):
        # ... existing cleaning logic ...
        products_df = self.convert_product_weights(products_df)
        # Add more cleaning steps if needed
        return products_df
    
    def clean_orders_data(self, orders_df):
        # Drop specified columns
        orders_df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        return orders_df
    
    def clean_date_times_data(self, df):
        df.fillna('unknown', inplace=True)  # Replace nulls with 'unknown'
        # Add any additional cleaning logic specific to your data
        return df