import pandas as pd
import numpy as np
from dateutil.parser import parse

class DataCleaning:
    
    def replace_and_drop_null(self, df):
        '''Replaces the string 'NULL' with NaN and drops rows with null values from the DataFrame.'''
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        return df
    
    def drop_rows_containing_mask(self, df, column_name, pattern):
    # Your implementation here
    # For example, using a regular expression to filter rows
        mask = df[column_name].str.contains(pattern, regex=True, na=False)
        return df[~mask]
    
    def custom_date_parser(self, date_str):
        try:
            return parse(date_str)
        except ValueError:
            return None
    
    def clean_user_data(self, df):
        user_df = self.replace_and_drop_null(df.copy())  
        user_df = self.drop_rows_containing_mask(user_df, "first_name", "\d+")
        user_df.loc[:, 'date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], format='mixed', errors='coerce')
        user_df.loc[:, 'email_address'] = user_df['email_address'].str.replace('@@', '@')
        user_df.loc[:, 'country_code'] = user_df['country_code'].str.replace('GG', 'G')
        user_df.loc[:, 'country_code'] = user_df['country_code'].astype('category')
        replacements = {'\(0\)': '', '[\)\(\.\- ]' : '', '^\+': '00'}
        user_df.loc[:, 'phone_number'] = user_df['phone_number'].replace(replacements, regex=True)
        user_df = self.drop_rows_containing_mask(user_df, "phone_number", "[a-zA-Z]")
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], format='mixed', errors='coerce')
        user_df.loc[:, 'phone_number'] = user_df['phone_number'].str.replace('^00\d{2}', '', regex=True)
        user_df = user_df.reset_index(drop=True)
        
        return user_df
    
    def clean_card_data(self, card_df):
        card_df = self.replace_and_drop_null(card_df.copy())
        card_df.loc[:, 'date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], format='mixed', errors='coerce') 
        card_df = card_df[card_df['card_number'].apply(lambda x: str(x).isdigit())]
        card_df['card_number'] = card_df['card_number'].astype('int')
        card_df['card_provider'] = card_df['card_provider'].astype('category')        
        card_df = card_df.reset_index(drop=True)
    
        return card_df

    
    def clean_store_data(self, store_df):
        store_df = store_df.copy()  # To avoid SettingWithCopyWarning
        store_df.drop('lat', axis=1, inplace=True)
        store_df = self.replace_and_drop_null(store_df)
        store_df = self.drop_rows_containing_mask(store_df, "staff_numbers", "[a-zA-Z]")
        store_df.loc[:, 'continent'] = store_df['continent'].str.replace('ee', '')
        store_df.loc[:, 'opening_date'] = pd.to_datetime(store_df['opening_date'], format='mixed', errors='coerce')
        column_to_move = store_df.pop('latitude')
        store_df.insert(2, 'latitude', column_to_move)
        store_df.loc[:, 'longitude'] = store_df['longitude'].astype('float')
        store_df.loc[:, 'latitude'] = store_df['latitude'].astype('float')
        store_df.loc[:, 'staff_numbers'] = store_df['staff_numbers'].astype('int')
        store_df.loc[:, 'store_type'] = store_df['store_type'].astype('category')
        store_df.loc[:, 'country_code'] = store_df['country_code'].astype('category')
        store_df = store_df.reset_index(drop=True)
    
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
    
    def clean_products_data(self, product_df):
        product_df = self.replace_and_drop_null(product_df.copy())  # To avoid SettingWithCopyWarning
        product_df = self.drop_rows_containing_mask(product_df, "product_price", "[a-zA-Z]")
        product_df = product_df[product_df['EAN'].str.len() <= 13]
        product_df.loc[:, 'date_added'] = pd.to_datetime(product_df['date_added'], format='mixed', errors='coerce')
    
    # Correctly calling convert_product_weights
        product_df = self.convert_product_weights(product_df)

        product_df.loc[:, 'product_price'] = product_df['product_price'].str.replace('£', '')
        product_df.loc[:, 'product_price'] = product_df['product_price'].astype('float')
        product_df.loc[:, 'category'] = product_df['category'].astype('category')
        product_df.loc[:, 'removed'] = product_df['removed'].astype('category')
        product_df.rename(columns={'weight': 'weight_kg', 'product_price': 'price_£'}, inplace=True)
        product_df.drop('Unnamed: 0', axis=1, inplace=True)
        product_df = product_df.reset_index(drop=True)
    
        return product_df


    
    def clean_orders_data(self, order_df):
    
        order_df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        
        return order_df
    
    def clean_date_times_data(self, date_times_df):
        
        date_times_df = self.replace_and_drop_null(date_times_df)
        date_times_df = self.drop_rows_containing_mask(date_times_df, "month", "[a-zA-Z]")  
        
        return date_times_df