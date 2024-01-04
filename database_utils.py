# database_utils.py
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    def read_db_creds(self, filename='db_creds.yaml'):
        with open(filename, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self, target=False):
        filename = 'my_creds.yaml' if target else 'db_creds.yaml'
        creds = self.read_db_creds(filename)
        if target:
            # For your PostgreSQL database
            return create_engine(f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        else:
            # For the AWS RDS database
            return create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")

    def list_db_tables(self, engine):
        # Adjusted to receive an engine as an argument
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, dataframe, table_name):
        engine = self.init_db_engine(target=True)  # Connect to your PostgreSQL database
        dataframe.to_sql(table_name, engine, if_exists='append', index=False)